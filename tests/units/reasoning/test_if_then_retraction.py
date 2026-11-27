"""Simple (IF-THEN) retraction rules: a THEN part that removes triples.

A rule with ``action == "retract"`` runs the same inference SELECT as an
assertion, but its rows are collected as ``removed_triples`` (hidden through
the removal overlay) instead of inferred triples. These tests prove:

1. the transpiler renders a retraction as a SPARQL ``DELETE`` with no
   ``sh:rule`` (SHACL-AF has no delete);
2. the engine routes retraction rows to ``removed_triples`` and never inserts;
3. the save-time validator accepts the retraction shapes and rejects the rest;
4. the hand-off to ``ReasoningService`` hides the matched triples with
   ``reasoning`` origin, and DQ / per-entity runs leave retractions alone.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

# Import the package root first: avoids a pre-existing circular import between
# ``back.core.reasoning`` and ``back.core.w3c`` when this module runs alone.
import back.objects  # noqa: F401
from back.core.errors import ValidationError
from back.core.graphdb.overrides import ORIGIN_REASONING
from back.core.reasoning.constants import RDF_TYPE
from back.core.reasoning.ReasoningService import ReasoningService
from back.core.reasoning.results.ReasoningResult import ReasoningResult
from back.core.reasoning.shacl_rules import (
    RETRACT_RULE,
    IfThenToShaclTranspiler,
    ShaclRuleEngine,
)
from back.core.reasoning.shacl_rules.rule_serializer import (
    preview_rule_shacl,
    serialize_rules_to_turtle,
)
from back.core.w3c.shacl.constants import IF_THEN_ACTION_RETRACT
from back.objects.digitaltwin.node.NodeBusinessRuleService import (
    NodeBusinessRuleService,
)
from back.objects.digitaltwin.quality.TwinDataQualityRun import TwinDataQualityRun
from back.objects.ontology.Rules import Rules

pytestmark = pytest.mark.unit

ONTOLOGY = {
    "base_uri": "http://ex.org",
    "classes": [{"name": "Meter"}, {"name": "Customer"}],
    "properties": [{"name": "meterOf"}, {"name": "status"}],
}

#: Remove the meterOf link between a Meter and a Customer.
RETRACT_LINK = {
    "name": "drop_meter_link",
    "action": IF_THEN_ACTION_RETRACT,
    "antecedent": "Meter(?m) \u2227 Customer(?c) \u2227 meterOf(?m, ?c)",
    "consequent": "meterOf(?m, ?c)",
}

#: Remove a literal attribute value from every Meter.
RETRACT_VALUE = {
    "name": "close_status",
    "action": IF_THEN_ACTION_RETRACT,
    "antecedent": "Meter(?m)",
    "consequent": 'status(?m, "closed")',
}

ASSERT_RULE = {
    "name": "meter_customer",
    "antecedent": "Meter(?m) \u2227 meterOf(?m, ?c)",
    "consequent": "Customer(?c)",
}

ROW = {
    "subject": "http://ex.org/m1",
    "predicate": "http://ex.org/meterOf",
    "object": "http://ex.org/c1",
}


def _store(rows=None):
    store = MagicMock()
    store.sql_table_reference.side_effect = lambda t: t
    store.query_dialect = ""
    store.execute_query.return_value = [dict(r) for r in (rows or [])]
    return store


def _queries(store):
    return [c.args[0] for c in store.execute_query.call_args_list]


# -- 1. Transpiler --------------------------------------------------------


class TestTranspiler:
    def test_default_action_is_assert(self):
        shacl = IfThenToShaclTranspiler(ontology=ONTOLOGY).transpile(ASSERT_RULE)
        assert shacl.action == "assert"
        assert shacl.is_retraction is False
        assert shacl.construct_sparql.startswith("CONSTRUCT {")

    def test_retraction_renders_as_delete_where(self):
        shacl = IfThenToShaclTranspiler(ontology=ONTOLOGY).transpile(RETRACT_LINK)
        assert shacl.is_retraction is True
        assert shacl.rule_type == RETRACT_RULE
        assert shacl.construct_sparql.startswith("DELETE {")
        assert "WHERE {" in shacl.construct_sparql
        assert "<http://ex.org/meterOf>" in shacl.construct_sparql

    def test_retraction_turtle_has_no_sh_rule(self):
        shacl = IfThenToShaclTranspiler(ontology=ONTOLOGY).transpile(RETRACT_LINK)
        assert "sh:rule" not in shacl.turtle
        assert 'ex:action "retract"' in shacl.turtle

    def test_literal_retraction_keeps_the_literal_in_the_delete_template(self):
        shacl = IfThenToShaclTranspiler(ontology=ONTOLOGY).transpile(RETRACT_VALUE)
        assert shacl.construct_sparql.startswith("DELETE {")
        assert '"closed"' in shacl.construct_sparql


# -- 2. SQL and engine ----------------------------------------------------


def _inference_sql(rule):
    from back.core.reasoning.shacl_rules.ShaclFlatSqlBackend import ShaclFlatSqlBackend
    from back.core.reasoning.shacl_rules.ShaclRuleSqlTranslator import (
        ShaclRuleSqlTranslator,
    )

    engine = ShaclRuleEngine(ontology=ONTOLOGY)
    shacl = engine._transpiler.transpile(rule)
    ctx = {"base_uri": ONTOLOGY["base_uri"], "uri_map": engine._build_uri_map()}
    return ShaclRuleSqlTranslator(ShaclFlatSqlBackend()).build_inference_sql(
        "triples", shacl, ctx
    )


class TestRemovalSql:
    """A retraction selects the triples that exist; an assertion, the missing ones."""

    def test_relationship_retraction_keeps_existing_triples(self):
        sql = _inference_sql(RETRACT_LINK)
        assert "EXISTS (SELECT 1 FROM triples a4" in sql
        assert "NOT EXISTS" not in sql

    def test_literal_retraction_keeps_existing_values(self):
        sql = _inference_sql(RETRACT_VALUE)
        assert "EXISTS (SELECT 1 FROM triples a2" in sql
        assert "'closed'" in sql
        assert "NOT EXISTS" not in sql

    def test_assertion_still_selects_missing_triples(self):
        sql = _inference_sql(dict(RETRACT_LINK, action="assert", consequent="Customer(?c)"))
        assert "NOT EXISTS" in sql

    def test_retraction_matches_exactly_the_existing_triples(self):
        # Run the generated SQL on real rows: a retraction must return the
        # triples present in the graph, not the missing ones.
        import sqlite3

        e, c = "http://ex.org/", "http://ex.org#"
        db = sqlite3.connect(":memory:")
        db.execute("CREATE TABLE triples (subject TEXT, predicate TEXT, object TEXT)")
        db.executemany(
            "INSERT INTO triples VALUES (?, ?, ?)",
            [
                ("m1", RDF_TYPE, c + "Meter"),
                ("m2", RDF_TYPE, c + "Meter"),
                ("c1", RDF_TYPE, c + "Customer"),
                ("c2", RDF_TYPE, c + "Customer"),
                ("m1", e + "meterOf", "c1"),
                ("m2", e + "meterOf", "c2"),
                ("m1", e + "status", "closed"),
                ("m2", e + "status", "open"),
            ],
        )
        link = _inference_sql(RETRACT_LINK)
        value = _inference_sql(RETRACT_VALUE)
        assert sorted(db.execute(link).fetchall()) == [
            ("m1", e + "meterOf", "c1"),
            ("m2", e + "meterOf", "c2"),
        ]
        assert db.execute(value).fetchall() == [("m1", e + "status", "closed")]

    def test_class_retraction_keeps_existing_memberships(self):
        sql = _inference_sql(dict(RETRACT_LINK, consequent="Meter(?m)"))
        assert "predicate = '" + RDF_TYPE + "'" in sql
        assert "EXISTS (SELECT 1 FROM triples a" in sql
        assert "NOT EXISTS" not in sql

    def test_class_retraction_matches_only_existing_memberships(self):
        import sqlite3

        e, c = "http://ex.org/", "http://ex.org#"
        db = sqlite3.connect(":memory:")
        db.execute("CREATE TABLE triples (subject TEXT, predicate TEXT, object TEXT)")
        db.executemany(
            "INSERT INTO triples VALUES (?, ?, ?)",
            [
                ("m1", RDF_TYPE, c + "Meter"),
                ("m2", RDF_TYPE, c + "Meter"),
                ("m3", RDF_TYPE, c + "Meter"),
                ("c1", RDF_TYPE, c + "Customer"),
                ("c2", RDF_TYPE, c + "Customer"),
                ("m1", e + "meterOf", "c1"),
                ("m2", e + "meterOf", "c2"),
            ],
        )
        sql = _inference_sql(dict(RETRACT_LINK, consequent="Meter(?m)"))
        # m3 has no meterOf link, so the IF part does not bind it: not removed.
        assert sorted(db.execute(sql).fetchall()) == [
            ("m1", RDF_TYPE, c + "Meter"),
            ("m2", RDF_TYPE, c + "Meter"),
        ]

    def test_dangling_object_retraction_emits_no_sql(self):
        # ?z is not bound by the IF part: a removal would match every value.
        rule = dict(RETRACT_LINK, consequent="meterOf(?m, ?z)")
        assert _inference_sql(rule) is None


class TestEngine:
    def test_retraction_rows_become_removed_triples(self):
        store = _store([ROW])
        result = ShaclRuleEngine(ontology=ONTOLOGY).execute_rules(
            [RETRACT_LINK], store, "triples"
        )
        assert result.removed_triples == [ROW]
        assert result.inferred_triples == []

    def test_retraction_never_inserts_even_when_materialising(self):
        store = _store([ROW])
        ShaclRuleEngine(ontology=ONTOLOGY).execute_rules(
            [RETRACT_LINK], store, "triples", materialize=True
        )
        assert not any(q.lstrip().upper().startswith("INSERT") for q in _queries(store))

    def test_retraction_issues_a_select_on_the_triple_table(self):
        store = _store([ROW])
        ShaclRuleEngine(ontology=ONTOLOGY).execute_rules(
            [RETRACT_LINK], store, "triples"
        )
        queries = _queries(store)
        assert queries and all(q.lstrip().upper().startswith("SELECT") for q in queries)
        assert all("triples" in q for q in queries)

    def test_rows_without_a_complete_triple_are_skipped(self):
        store = _store([{"subject": "", "predicate": "p", "object": "o"}])
        result = ShaclRuleEngine(ontology=ONTOLOGY).execute_rules(
            [RETRACT_LINK], store, "triples"
        )
        assert result.removed_triples == []

    def test_assertions_are_unaffected(self):
        store = _store([ROW])
        result = ShaclRuleEngine(ontology=ONTOLOGY).execute_rules(
            [ASSERT_RULE], store, "triples"
        )
        assert result.removed_triples == []
        assert len(result.inferred_triples) == 1
        assert result.inferred_triples[0].provenance == "if_then:meter_customer"


# -- 3. Validation --------------------------------------------------------


class TestValidation:
    def test_relationship_retraction_is_valid(self):
        # Its THEN part restates the IF part on purpose: no tautology error.
        assert Rules.validate_if_then_rule(RETRACT_LINK) == []

    def test_literal_retraction_is_valid(self):
        assert Rules.validate_if_then_rule(RETRACT_VALUE) == []

    def test_retraction_passes_the_reference_gate(self):
        errors = Rules.if_then_reference_errors(
            RETRACT_LINK, {"meter", "customer"}, {"meterof"}
        )
        assert errors == []

    def test_assertion_tautology_is_still_rejected(self):
        rule = {"name": "R", "antecedent": "Meter(?m)", "consequent": "Meter(?m)"}
        errors = Rules.if_then_reference_errors(rule, {"meter"}, set())
        assert any("infers nothing new" in e for e in errors)

    def test_unknown_action_is_rejected(self):
        errors = Rules.validate_if_then_rule(dict(RETRACT_LINK, action="delete"))
        assert any("action must be" in e for e in errors)

    def test_class_membership_in_the_then_part_is_valid(self):
        rule = dict(RETRACT_LINK, consequent="Meter(?m)")
        assert Rules.validate_if_then_rule(rule) == []

    def test_three_argument_atom_in_the_then_part_is_rejected(self):
        rule = dict(RETRACT_LINK, consequent="meterOf(?m, ?c, ?m)")
        errors = Rules.validate_if_then_rule(rule)
        assert any("has 3 arguments" in e for e in errors)

    def test_unbound_variable_in_the_then_part_is_rejected(self):
        rule = dict(RETRACT_LINK, consequent="meterOf(?m, ?z)")
        errors = Rules.validate_if_then_rule(rule)
        assert any("'?z' must be bound" in e for e in errors)

    def test_builtin_in_the_then_part_is_rejected(self):
        rule = dict(RETRACT_LINK, consequent="builtin:greaterThan(?m, 3)")
        errors = Rules.validate_if_then_rule(rule)
        assert any("built-ins or negation" in e for e in errors)


# -- 4. Serialisation, DQ, per-entity, hand-off ---------------------------


class TestSerializerAndUses:
    def test_combined_export_omits_retractions_and_says_so(self):
        text = serialize_rules_to_turtle([ASSERT_RULE, RETRACT_LINK], ONTOLOGY)
        assert "rule_meter_customer" in text
        assert "rule_drop_meter_link" not in text
        assert "1 retraction rule(s) omitted" in text

    def test_preview_returns_the_delete_form(self):
        out = preview_rule_shacl(
            "drop_meter_link",
            RETRACT_LINK["antecedent"],
            RETRACT_LINK["consequent"],
            ONTOLOGY,
            action=IF_THEN_ACTION_RETRACT,
        )
        assert out["rule_type"] == RETRACT_RULE
        assert "DELETE {" in out["construct_sparql"]

    def test_data_quality_checks_skip_retractions(self):
        domain = SimpleNamespace(
            shacl_shapes=[],
            if_then_rules=[ASSERT_RULE, RETRACT_LINK],
            ontology={},
        )
        run = TwinDataQualityRun.from_request(domain, {})
        assert [r["name"] for r in run.if_then_rules] == ["meter_customer"]

    def test_per_entity_run_rejects_a_retraction(self):
        domain = SimpleNamespace(
            if_then_rules=[RETRACT_LINK],
            get_classes=lambda: [],
        )
        matched = {"name": "Meter", "business_rules": [{"name": "drop_meter_link"}]}
        with pytest.raises(ValidationError, match="reasoning pipeline"):
            NodeBusinessRuleService._focus_rule(domain, matched, "drop_meter_link")


def _service(store):
    svc = ReasoningService.__new__(ReasoningService)
    svc._store = store
    svc._get_graph_name = lambda: "graph"
    svc._domain = None
    return svc


def test_retraction_hides_matched_triples_with_reasoning_origin():
    store = _store([ROW])
    store.insert_triples.return_value = 0
    result = ShaclRuleEngine(ontology=ONTOLOGY).execute_rules(
        [RETRACT_LINK], store, "triples"
    )
    _service(store).materialize_inferred(result)
    store.delete_triples.assert_called_once()
    args, kwargs = store.delete_triples.call_args
    assert kwargs["origin"] == ORIGIN_REASONING
    assert args[1] == [ROW]


def test_import_dedupe_tells_an_assertion_from_a_retraction_with_the_same_text():
    from api.routers.internal.ontology import _rule_signature

    assertion = dict(RETRACT_LINK, action="assert")
    assert _rule_signature("if_then_rules", assertion) != _rule_signature(
        "if_then_rules", RETRACT_LINK
    )


class TestEngineEdges:
    def test_disabled_retraction_is_skipped(self):
        store = _store([ROW])
        result = ShaclRuleEngine(ontology=ONTOLOGY).execute_rules(
            [dict(RETRACT_LINK, enabled=False)], store, "triples"
        )
        assert result.removed_triples == []
        store.execute_query.assert_not_called()

    def test_assertion_and_retraction_run_side_by_side(self):
        store = _store([ROW])
        result = ShaclRuleEngine(ontology=ONTOLOGY).execute_rules(
            [ASSERT_RULE, RETRACT_LINK], store, "triples"
        )
        assert len(result.inferred_triples) == 1
        assert result.removed_triples == [ROW]
        assert result.stats["errors"] == 0

    def test_inference_limit_caps_a_retraction(self):
        store = _store([ROW])
        ShaclRuleEngine(ontology=ONTOLOGY).execute_rules(
            [RETRACT_LINK], store, "triples", inference_limit=5
        )
        assert "LIMIT 5" in _queries(store)[0]

    def test_failing_retraction_is_counted_and_does_not_raise(self):
        store = _store()
        store.execute_query.side_effect = RuntimeError("warehouse down")
        result = ShaclRuleEngine(ontology=ONTOLOGY).execute_rules(
            [RETRACT_LINK], store, "triples"
        )
        assert result.removed_triples == []
        assert result.stats["errors"] == 1
        assert "drop_meter_link" in result.stats["error_details"]

    def test_retraction_does_not_count_as_inferred(self):
        store = _store([ROW])
        result = ShaclRuleEngine(ontology=ONTOLOGY).execute_rules(
            [RETRACT_LINK], store, "triples"
        )
        assert result.stats["inferred_count"] == 0
        assert result.stats["rules_count"] == 1


class TestEdgeValidation:
    @pytest.mark.parametrize("action", [None, "", "assert"])
    def test_missing_or_empty_action_means_assert(self, action):
        rule = dict(ASSERT_RULE, action=action)
        assert Rules.validate_if_then_rule(rule) == []
        assert IfThenToShaclTranspiler(ONTOLOGY).transpile(rule).is_retraction is False

    def test_negation_in_the_then_part_is_rejected(self):
        rule = dict(RETRACT_LINK, consequent="not(meterOf(?m, ?c))")
        errors = Rules.validate_if_then_rule(rule)
        assert any("built-ins or negation" in e for e in errors)

    def test_each_problem_is_reported_once(self):
        rule = dict(RETRACT_LINK, consequent="meterOf(?m, ?z) \u2227 status(?m, ?z)")
        errors = Rules.validate_if_then_rule(rule)
        assert len(errors) == len(set(errors))

    def test_retraction_still_needs_known_ontology_terms(self):
        errors = Rules.if_then_reference_errors(RETRACT_LINK, {"meter", "customer"}, set())
        assert any("unknown relationship/property 'meterOf'" in e for e in errors)


def test_data_quality_with_only_retractions_has_nothing_to_check():
    domain = SimpleNamespace(shacl_shapes=[], if_then_rules=[RETRACT_LINK], ontology={})
    with pytest.raises(ValidationError):
        TwinDataQualityRun.from_request(domain, {})


def test_retraction_result_serialises_removed_triples():
    result = ReasoningResult()
    result.removed_triples.append(ROW)
    assert result.to_dict()["removed_triples"] == [ROW]
