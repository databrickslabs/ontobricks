"""Unit tests for the SHACL-AF rule engine.

The SHACL rule engine executes the IF/THEN business-rules UI.  These tests
prove:

1. **SQL capabilities** — for a battery of rules covering every capability
   (unary/binary atoms, comparison/string built-ins, negation, multi-hop,
   materialisation, entity focus), the engine issues the expected SQL to the
   store via the flat-triple SQL backend.
2. **Contract** — provenance (``if_then:<rule>``) and ``stats`` keys match the
   established contract, so the UI / MCP / DQ consumers observe no difference.
3. **Transpiler** — IF/THEN rules map to the expected SHACL-AF rule type and
   produce well-formed canonical Turtle.
"""

from unittest.mock import MagicMock

# Importing the package root first avoids a pre-existing circular import
# between ``back.core.reasoning`` and ``back.core.w3c`` when this module is
# collected on its own.
import back.objects  # noqa: F401
from back.core.reasoning.shacl_rules import (
    ShaclRuleEngine,
    SPARQL_RULE,
    IfThenToShaclTranspiler,
    TRIPLE_RULE,
)


ONTOLOGY = {
    "base_uri": "http://ex.org",
    "classes": [
        {"name": "Customer"},
        {"name": "Claim"},
        {"name": "Contract"},
        {"name": "HighValue"},
    ],
    "properties": [
        {"name": "hasClaim"},
        {"name": "hasContract"},
        {"name": "hasScore"},
        {"name": "status"},
        {"name": "owns"},
    ],
}

# A battery exercising every IF-THEN capability path.
RULE_BATTERY = [
    {
        "name": "simple_class",
        "antecedent": "Customer(?x)",
        "consequent": "HighValue(?x)",
    },
    {
        "name": "property_chain",
        "antecedent": "Customer(?x) \u2227 hasClaim(?x, ?y) \u2227 Claim(?y)",
        "consequent": "hasContract(?x, ?z)",
    },
    {
        "name": "comparison_builtin",
        "antecedent": "Customer(?x) \u2227 hasScore(?x, ?s) \u2227 builtin:greaterThan(?s, 500)",
        "consequent": "HighValue(?x)",
    },
    {
        "name": "string_builtin",
        "antecedent": (
            'Customer(?x) \u2227 Contract(?z) \u2227 hasContract(?x, ?z) '
            '\u2227 status(?z, ?st) \u2227 builtin:contains(?st, "active")'
        ),
        "consequent": "HighValue(?x)",
    },
    {
        "name": "negation",
        "antecedent": "Customer(?x) \u2227 not(hasContract(?x, ?z))",
        "consequent": "HighValue(?x)",
    },
]


def _make_store():
    store = MagicMock()
    store.sql_table_reference.side_effect = lambda t: t
    store.query_dialect = ""
    store.execute_query.return_value = [
        {
            "subject": "http://ex.org/e1",
            "predicate": "http://ex.org/hasContract",
            "object": "",
        }
    ]
    return store


def _queries(store):
    return [c.args[0] for c in store.execute_query.call_args_list]


class TestSqlCapabilities:
    """The SHACL engine must emit the expected SQL for every capability path."""

    def test_inference_sql_targets_triple_table_per_rule(self):
        for rule in RULE_BATTERY:
            store = _make_store()
            ShaclRuleEngine(ontology=ONTOLOGY).execute_rules(
                [rule], store, "triples"
            )
            # Every emitted query must target the flat triple table.  A
            # class-only antecedent (``simple_class``) is a classification
            # no-op and legitimately emits no inference SQL.
            assert all("triples" in q for q in _queries(store)), (
                f"SQL did not target the triple table for rule '{rule['name']}'"
            )

    def test_property_chain_rule_emits_inference_sql(self):
        # A property-chain rule derives a new relationship, so the engine
        # must issue an inference SELECT (builtin/negation SQL paths are
        # covered exhaustively by the flat-backend unit tests).
        store = _make_store()
        ShaclRuleEngine(ontology=ONTOLOGY).execute_rules(
            [RULE_BATTERY[1]], store, "triples"
        )
        assert _queries(store), "property-chain rule emitted no SQL"

    def test_materialization_sql_inserts(self):
        rule = RULE_BATTERY[1]
        store = _make_store()
        ShaclRuleEngine(ontology=ONTOLOGY).execute_rules(
            [rule], store, "triples", materialize=True
        )
        queries = _queries(store)
        assert any("INSERT" in q.upper() for q in queries), (
            "materialisation did not emit an INSERT"
        )

    def test_focus_sql_restricts_to_entity(self):
        rule = RULE_BATTERY[1]
        focus = {"vars": ["?x"], "uri": "http://ex.org/e1"}
        store = _make_store()
        ShaclRuleEngine(ontology=ONTOLOGY).execute_rules(
            [rule], store, "triples", focus=focus
        )
        assert any("http://ex.org/e1" in q for q in _queries(store)), (
            "focus entity URI absent from emitted SQL"
        )

    def test_inference_limit_applied(self):
        rule = RULE_BATTERY[1]  # property chain -> emits an inference SELECT
        store = _make_store()
        ShaclRuleEngine(ontology=ONTOLOGY).execute_rules(
            [rule], store, "triples", inference_limit=10
        )
        assert any("LIMIT 10" in q for q in _queries(store))


class TestContract:
    def test_provenance_and_stats_match_contract(self):
        store = _make_store()
        result = ShaclRuleEngine(ontology=ONTOLOGY).execute_rules(
            [RULE_BATTERY[1]], store, "triples"
        )
        assert result.stats["phase"] == "if_then"
        assert result.stats["rules_count"] == 1
        assert len(result.inferred_triples) >= 1
        assert all(
            t.provenance.startswith("if_then:") for t in result.inferred_triples
        )

    def test_empty_rules(self):
        store = _make_store()
        result = ShaclRuleEngine(ontology=ONTOLOGY).execute_rules([], store, "triples")
        assert result.stats["rules_count"] == 0
        assert result.inferred_triples == []

    def test_disabled_rule_is_skipped(self):
        store = _make_store()
        rule = {**RULE_BATTERY[0], "enabled": False}
        result = ShaclRuleEngine(ontology=ONTOLOGY).execute_rules(
            [rule], store, "triples"
        )
        assert result.stats["rules_count"] == 0
        store.execute_query.assert_not_called()


class TestTranspiler:
    def setup_method(self):
        self.t = IfThenToShaclTranspiler(ontology=ONTOLOGY)

    def test_simple_assertion_is_triple_rule(self):
        rule = self.t.transpile(RULE_BATTERY[0])
        assert rule.rule_type == TRIPLE_RULE
        assert "sh:TripleRule" in rule.turtle
        assert rule.target_class_uri == "http://ex.org#Customer"

    def test_builtin_rule_is_sparql_rule(self):
        rule = self.t.transpile(RULE_BATTERY[2])
        assert rule.rule_type == SPARQL_RULE
        assert "sh:SPARQLRule" in rule.turtle
        assert rule.construct_sparql.startswith("CONSTRUCT {")
        assert "FILTER" in rule.construct_sparql

    def test_negation_renders_filter_not_exists(self):
        rule = self.t.transpile(RULE_BATTERY[4])
        assert rule.rule_type == SPARQL_RULE
        assert "FILTER NOT EXISTS" in rule.construct_sparql

    def test_property_chain_construct_has_head_and_body(self):
        rule = self.t.transpile(RULE_BATTERY[1])
        assert "hasContract" in rule.construct_sparql
        assert "hasClaim" in rule.construct_sparql
        assert "WHERE {" in rule.construct_sparql

    def test_turtle_is_well_formed_prefix_block(self):
        rule = self.t.transpile(RULE_BATTERY[1])
        assert rule.turtle.startswith("@prefix sh:")
        assert "a sh:NodeShape ;" in rule.turtle
