"""Entity business rules: IF-THEN rules referenced from a class and triggered on one entity."""

import sys
from types import SimpleNamespace

import pytest

from back.core.errors import NotFoundError, ValidationError
from back.core.reasoning.shacl_rules.ShaclFlatSqlBackend import ShaclFlatSqlBackend
from back.core.w3c.owl.OntologyGenerator import OntologyGenerator
from back.core.w3c.owl.OntologyParser import OntologyParser
from back.objects.digitaltwin import NodeBusinessRuleService, NodeContextService
from back.objects.digitaltwin.TwinGraphAccess import TwinGraphAccess
from back.objects.digitaltwin.certify import CertifiedGraphWriter
from back.objects.ontology import Rules
from back.objects.ontology.ClassModel import ClassModel

pytestmark = pytest.mark.unit

BASE = "https://example.com/onto#"
ENTITY = "https://example.com/Customer/CUST001"

VIP_RULE = {
    "name": "VipCustomer",
    "description": "Customers with an order become VIP",
    "antecedent": "Customer(?c) ^ hasOrder(?c, ?o) ^ Order(?o)",
    "consequent": "VIP(?c)",
}
POINTS_RULE = {
    "name": "ActiveContract100Points",
    "description": "Active contract grants 100 loyalty points",
    "antecedent": "Customer(?c)",
    "consequent": "loyaltypoints(?c, 100)",
}
ORDER_RULE = {
    "name": "FlagOrder",
    "antecedent": "Order(?o) ^ amount(?o, ?a) ^ builtin:greaterThan(?a, 100)",
    "consequent": "BigOrder(?o)",
}
PERSON_RULE = {
    "name": "PersonRule",
    "antecedent": "Person(?p) ^ hasOrder(?p, ?o)",
    "consequent": "Buyer(?p)",
}

CLASSES = [
    {"name": "Person", "uri": "https://example.com/Person"},
    {
        "name": "Customer",
        "uri": "https://example.com/Customer",
        "parent": "Person",
        "business_rules": [
            {"name": "VipCustomer"},
            {"name": "ActiveContract100Points"},
            {"name": "Gone"},
        ],
    },
    {"name": "Order", "uri": "https://example.com/Order"},
]


# ---------------------------------------------------------------------------
# Rule <-> class matching


def test_if_then_rule_classes_lists_unary_unprefixed_atoms():
    assert Rules.if_then_rule_classes(VIP_RULE) == {"customer", "order", "vip"}
    assert Rules.if_then_rule_classes(ORDER_RULE) == {"order", "bigorder"}


def test_rules_for_class_includes_ancestor_rules():
    rules = [VIP_RULE, ORDER_RULE, PERSON_RULE]
    names = [r["name"] for r in Rules.rules_for_class("Customer", rules, CLASSES)]
    assert names == ["VipCustomer", "PersonRule"]


def test_rules_for_class_matches_consequent_only_class():
    names = [r["name"] for r in Rules.rules_for_class("VIP", [VIP_RULE], [])]
    assert names == ["VipCustomer"]


def test_focus_variables_follow_lineage():
    assert Rules.if_then_focus_variables(VIP_RULE, ["Customer", "Person"]) == ["?c"]
    assert Rules.if_then_focus_variables(PERSON_RULE, ["Customer", "Person"]) == ["?p"]
    assert Rules.if_then_focus_variables(ORDER_RULE, ["Customer"]) == []


# ---------------------------------------------------------------------------
# Persistence: class model, OWL round-trip, rename/delete cascade


def test_class_model_preserves_business_rules_on_partial_update():
    existing = {"name": "Customer", "business_rules": [{"name": "VipCustomer"}]}
    built = ClassModel.build_class_from_data({"label": "C"}, existing)
    assert built["business_rules"] == [{"name": "VipCustomer"}]


def test_owl_round_trip_keeps_business_rules():
    refs = [{"name": "VipCustomer"}]
    owl = OntologyGenerator(
        base_uri=BASE,
        ontology_name="T",
        classes=[{"name": "Customer", "label": "Customer", "business_rules": refs}],
        properties=[],
    ).generate()
    parsed = {c["name"]: c for c in OntologyParser(owl_content=owl).get_classes()}
    assert parsed["Customer"]["business_rules"] == refs


def test_rename_and_drop_cascade():
    classes = [
        {"name": "A", "business_rules": [{"name": "R1"}, {"name": "R2"}]},
        {"name": "B", "business_rules": [{"name": "R1"}]},
    ]
    assert Rules.rename_business_rule_refs(classes, "R1", "R9") == 2
    assert classes[1]["business_rules"] == [{"name": "R9"}]
    assert Rules.drop_business_rule_refs(classes, "R9") == 2
    assert classes[0]["business_rules"] == [{"name": "R2"}]
    assert classes[1]["business_rules"] == []


# ---------------------------------------------------------------------------
# Translator focus


def _params(**extra):
    return {**VIP_RULE, "base_uri": BASE, **extra}


def test_inference_sql_unchanged_without_focus():
    tr = ShaclFlatSqlBackend()
    assert tr.build_inference_sql("t", _params()) == tr.build_inference_sql(
        "t", _params(focus=None)
    )


def test_inference_sql_adds_escaped_focus_predicate():
    sql = ShaclFlatSqlBackend().build_inference_sql(
        "t", _params(focus={"vars": ["?c"], "uri": "https://x/C'1"})
    )
    assert "(a1.subject = 'https://x/C''1')" in sql


def test_inference_sql_ors_multiple_focus_vars():
    rule = {
        "name": "Knows",
        "antecedent": "Person(?a) ^ knows(?a, ?b) ^ Person(?b)",
        "consequent": "friend(?a, ?b)",
        "base_uri": BASE,
        "focus": {"vars": ["?a", "?b"], "uri": "u"},
    }
    sql = ShaclFlatSqlBackend().build_inference_sql("t", rule)
    assert "(a1.subject = 'u' OR a3.subject = 'u')" in sql


def test_inference_sql_none_when_focus_var_unbound():
    assert (
        ShaclFlatSqlBackend().build_inference_sql(
            "t", _params(focus={"vars": ["?zz"], "uri": "u"})
        )
        is None
    )


# ---------------------------------------------------------------------------
# Service


def _domain(rules=None):
    return SimpleNamespace(
        info={"name": "c360"},
        databricks={},
        domain_folder="c360",
        current_version="1",
        ontology={"base_uri": BASE, "classes": CLASSES},
        if_then_rules=(
            rules
            if rules is not None
            else [VIP_RULE, POINTS_RULE, PERSON_RULE]
        ),
        get_classes=lambda: CLASSES,
    )


def test_class_entries_drop_missing_and_disabled_rules():
    disabled = {**VIP_RULE, "enabled": False}
    assert NodeBusinessRuleService.class_entries(CLASSES[1], [VIP_RULE]) == [
        {
            "name": "VipCustomer",
            "description": "Customers with an order become VIP",
            "antecedent": VIP_RULE["antecedent"],
            "consequent": VIP_RULE["consequent"],
        }
    ]
    assert NodeBusinessRuleService.class_entries(CLASSES[1], [disabled]) == []


def test_resolve_rejects_undeclared_rule_and_disabled_feature():
    with pytest.raises(ValidationError):
        NodeContextService.resolve_business_rule(
            _domain(), entity_uri=ENTITY, rule_name="PersonRule"
        )
    with pytest.raises(ValidationError):
        NodeContextService.resolve_business_rule(
            _domain(),
            entity_uri=ENTITY,
            rule_name="VipCustomer",
            context_policy={"business_rules": "disabled"},
        )
    with pytest.raises(NotFoundError):
        NodeContextService.resolve_business_rule(
            _domain(), entity_uri="https://example.com/Nope/1", rule_name="VipCustomer"
        )


def _patch_store(monkeypatch, store):
    module = sys.modules["back.objects.digitaltwin.node.NodeBusinessRuleService"]

    def _get_graphdb(snap, settings, *, for_write=False, variant=None):
        store.for_write = for_write
        store.variant = variant
        return store

    monkeypatch.setattr(module, "get_graphdb", _get_graphdb)
    monkeypatch.setattr(
        TwinGraphAccess, "served_query_table", staticmethod(lambda *a, **k: "g_certified")
    )


class _FakeStore:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []
        self.inserted = []

    def insert_triples(self, table_name, triples):
        self.inserted.extend(triples)
        return len(triples)

    def sql_table_reference(self, table_name):
        return table_name

    def execute_query(self, sql):
        self.queries.append(sql)
        return self.rows


class _FakeWriter:
    """Records the Certified Graph writes of one run."""

    calls: list = []

    def __init__(self, domain, settings):
        pass

    def add_enrichments(self, triples, *, kind, run_id=""):
        _FakeWriter.calls.append(("add", kind, None, list(triples)))
        return len(triples)

    def replace_scope(self, triples, *, kind, provenance, run_id=""):
        _FakeWriter.calls.append(("replace", kind, provenance, list(triples)))
        return len(triples)


@pytest.fixture
def writer(monkeypatch):
    module = sys.modules["back.objects.digitaltwin.node.NodeBusinessRuleService"]
    _FakeWriter.calls = []
    _FakeWriter.KIND_RULE = CertifiedGraphWriter.KIND_RULE
    _FakeWriter.KIND_OVERRIDE = CertifiedGraphWriter.KIND_OVERRIDE
    _FakeWriter.drop_attribute_values = CertifiedGraphWriter.drop_attribute_values
    monkeypatch.setattr(module, "CertifiedGraphWriter", _FakeWriter)
    return _FakeWriter


_VIP_ROW = {
    "subject": ENTITY,
    "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
    "object": BASE + "VIP",
}
_POINTS_ROW = {"subject": ENTITY, "predicate": BASE + "loyaltypoints", "object": "100"}


async def _run(rule="VipCustomer", **kwargs):
    return await NodeContextService.run_business_rule(
        _domain(), object(), entity_uri=ENTITY, rule_name=rule, **kwargs
    )


async def test_the_rule_reads_the_served_certified_graph_scoped_to_the_entity(
    monkeypatch, writer
):
    store = _FakeStore([_VIP_ROW])
    _patch_store(monkeypatch, store)

    await _run()

    assert store.variant == "certified"
    assert "g_certified" in store.queries[0]
    assert f"a1.subject = '{ENTITY}'" in store.queries[0]


async def test_new_facts_are_added_to_the_certified_graph(monkeypatch, writer):
    _patch_store(monkeypatch, _FakeStore([_VIP_ROW]))

    result = await _run()

    provenance = f"if_then:VipCustomer@{ENTITY}"
    assert writer.calls == [("add", "rule", None, [{**_VIP_ROW, "provenance": provenance}])]
    assert result["graph_target"] == "certified"
    assert result["materialized_count"] == 1
    assert result["class_name"] == "Customer"


async def test_a_set_attribute_overrides_the_current_value(monkeypatch, writer):
    _patch_store(monkeypatch, _FakeStore([_POINTS_ROW]))

    result = await _run("ActiveContract100Points")

    assert writer.calls == [
        (
            "replace",
            "override",
            f"if_then:ActiveContract100Points@{ENTITY}",
            [_POINTS_ROW],
        )
    ]
    assert result["triples"][0]["object"] == "100"


async def test_without_new_facts_nothing_is_written(monkeypatch, writer):
    _patch_store(monkeypatch, _FakeStore([]))

    result = await _run()

    assert result["inferred_count"] == 0
    assert result["materialized_count"] == 0
    assert writer.calls == []


async def test_a_preview_returns_the_triples_and_writes_nothing(monkeypatch, writer):
    _patch_store(monkeypatch, _FakeStore([_VIP_ROW]))

    result = await _run(preview=True)

    assert writer.calls == []
    assert result["graph_target"] == "preview"
    assert result["inferred_count"] == 1
    assert result["materialized_count"] == 0
    assert result["triples"] == [_VIP_ROW]


async def test_run_business_rule_on_simulation_uses_sandbox(monkeypatch):
    live = _FakeStore([])
    sandbox_store = _FakeStore(
        [
            {
                "subject": ENTITY,
                "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "object": BASE + "VIP",
            }
        ]
    )
    _patch_store(monkeypatch, live)
    seeded = []
    views = []

    class FakeSandbox:
        def __init__(self, store, graph_name, sim_id):
            assert store is live
            assert sim_id == "sim1"
            self.store = sandbox_store
            self.table = "g__sim_sim1"

        def seed_focus_for_if_then(self, uri, hops=2):
            seeded.append((uri, hops))

        def ensure_explorer_views(self, live_union):
            views.append(live_union)

    class FakeSim:
        def __init__(self, *a, **k):
            pass

        def sandbox_relation(self, sid):
            assert sid == "sim1"
            return "g__sim_sim1"

    class FakeSimReasoning:
        def __init__(self, snap, store, table):
            assert store is sandbox_store
            assert table == "g__sim_sim1"
            self._store = store

        def _get_graph_name(self):
            return "g__sim_sim1"

        def run_if_then_rules(self, rule_names=None, focus=None):
            assert focus["uri"] == ENTITY
            return SimpleNamespace(
                inferred_triples=[
                    SimpleNamespace(
                        subject=ENTITY, predicate=BASE + "p", object=BASE + "o"
                    )
                ],
                stats={},
            )

        def materialize_inferred(self, result):
            rows = [
                {"subject": t.subject, "predicate": t.predicate, "object": t.object}
                for t in result.inferred_triples
            ]
            self._store.insert_triples("g__sim_sim1", rows)
            return len(rows)

    monkeypatch.setattr("back.objects.intelligence.Simulation", FakeSim)
    monkeypatch.setattr("back.objects.intelligence.Sandbox", FakeSandbox)
    monkeypatch.setattr(
        "back.objects.intelligence.Reasoning",
        FakeSimReasoning,
    )
    monkeypatch.setattr(
        "back.core.helpers.effective_graph_name", lambda domain: "g"
    )
    monkeypatch.setattr(
        "back.core.helpers.effective_graph_query_table",
        lambda *a, **k: "g__sync",
    )

    result = await NodeContextService.run_business_rule(
        _domain(),
        object(),
        entity_uri=ENTITY,
        rule_name="VipCustomer",
        simulation_id="sim1",
    )

    assert live.for_write is False
    assert live.inserted == []
    assert sandbox_store.inserted
    assert seeded == [(ENTITY, 2)]
    assert views
    assert result["graph_target"] == "simulation"
    assert result["inferred_count"] == 1
    assert result["materialized_count"] == 1
