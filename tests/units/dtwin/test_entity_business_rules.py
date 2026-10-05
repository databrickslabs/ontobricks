"""Entity business rules: SWRL rules referenced from a class and triggered on one entity."""

import sys
from types import SimpleNamespace

import pytest

from back.core.errors import NotFoundError, ValidationError
from back.core.reasoning.SWRLSQLTranslator import SWRLSQLTranslator
from back.core.w3c.owl.OntologyGenerator import OntologyGenerator
from back.core.w3c.owl.OntologyParser import OntologyParser
from back.objects.digitaltwin import NodeBusinessRuleService, NodeContextService
from back.objects.ontology import OntologyRules
from back.objects.ontology.OntologyClassModel import OntologyClassModel

pytestmark = pytest.mark.unit

BASE = "https://example.com/onto#"
ENTITY = "https://example.com/Customer/CUST001"

VIP_RULE = {
    "name": "VipCustomer",
    "description": "Customers with an order become VIP",
    "antecedent": "Customer(?c) ^ hasOrder(?c, ?o) ^ Order(?o)",
    "consequent": "VIP(?c)",
}
ORDER_RULE = {
    "name": "FlagOrder",
    "antecedent": "Order(?o) ^ amount(?o, ?a) ^ swrlb:greaterThan(?a, 100)",
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
        "business_rules": [{"name": "VipCustomer"}, {"name": "Gone"}],
    },
    {"name": "Order", "uri": "https://example.com/Order"},
]


# ---------------------------------------------------------------------------
# Rule <-> class matching


def test_swrl_rule_classes_lists_unary_unprefixed_atoms():
    assert OntologyRules.swrl_rule_classes(VIP_RULE) == {"customer", "order", "vip"}
    assert OntologyRules.swrl_rule_classes(ORDER_RULE) == {"order", "bigorder"}


def test_rules_for_class_includes_ancestor_rules():
    rules = [VIP_RULE, ORDER_RULE, PERSON_RULE]
    names = [r["name"] for r in OntologyRules.rules_for_class("Customer", rules, CLASSES)]
    assert names == ["VipCustomer", "PersonRule"]


def test_rules_for_class_matches_consequent_only_class():
    names = [r["name"] for r in OntologyRules.rules_for_class("VIP", [VIP_RULE], [])]
    assert names == ["VipCustomer"]


def test_focus_variables_follow_lineage():
    assert OntologyRules.swrl_focus_variables(VIP_RULE, ["Customer", "Person"]) == ["?c"]
    assert OntologyRules.swrl_focus_variables(PERSON_RULE, ["Customer", "Person"]) == ["?p"]
    assert OntologyRules.swrl_focus_variables(ORDER_RULE, ["Customer"]) == []


# ---------------------------------------------------------------------------
# Persistence: class model, OWL round-trip, rename/delete cascade


def test_class_model_preserves_business_rules_on_partial_update():
    existing = {"name": "Customer", "business_rules": [{"name": "VipCustomer"}]}
    built = OntologyClassModel.build_class_from_data({"label": "C"}, existing)
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
    assert OntologyRules.rename_business_rule_refs(classes, "R1", "R9") == 2
    assert classes[1]["business_rules"] == [{"name": "R9"}]
    assert OntologyRules.drop_business_rule_refs(classes, "R9") == 2
    assert classes[0]["business_rules"] == [{"name": "R2"}]
    assert classes[1]["business_rules"] == []


# ---------------------------------------------------------------------------
# Translator focus


def _params(**extra):
    return {**VIP_RULE, "base_uri": BASE, **extra}


def test_inference_sql_unchanged_without_focus():
    tr = SWRLSQLTranslator()
    assert tr.build_inference_sql("t", _params()) == tr.build_inference_sql(
        "t", _params(focus=None)
    )


def test_inference_sql_adds_escaped_focus_predicate():
    sql = SWRLSQLTranslator().build_inference_sql(
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
    sql = SWRLSQLTranslator().build_inference_sql("t", rule)
    assert "(a1.subject = 'u' OR a3.subject = 'u')" in sql


def test_inference_sql_none_when_focus_var_unbound():
    assert (
        SWRLSQLTranslator().build_inference_sql(
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
        swrl_rules=rules if rules is not None else [VIP_RULE, PERSON_RULE],
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
    module = sys.modules["back.objects.digitaltwin.NodeBusinessRuleService"]
    monkeypatch.setattr(module, "get_graphdb", lambda snap, settings: store)


class _FakeStore:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []
        self.inserted = []

    def sql_table_reference(self, table_name):
        return "g"

    def execute_query(self, sql):
        self.queries.append(sql)
        return self.rows

    def insert_triples(self, table_name, triples):
        self.inserted.extend(triples)
        return len(triples)


async def test_run_business_rule_scopes_inference_and_materialises(monkeypatch):
    rows = [
        {
            "subject": ENTITY,
            "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "object": BASE + "VIP",
        }
    ]
    store = _FakeStore(rows)
    _patch_store(monkeypatch, store)

    result = await NodeContextService.run_business_rule(
        _domain(), object(), entity_uri=ENTITY, rule_name="VipCustomer"
    )

    assert f"a1.subject = '{ENTITY}'" in store.queries[0]
    assert store.inserted == rows
    assert result["inferred_count"] == 1
    assert result["materialized_count"] == 1
    assert result["rule"] == "VipCustomer"
    assert result["class_name"] == "Customer"


async def test_run_business_rule_without_new_facts_writes_nothing(monkeypatch):
    store = _FakeStore([])
    _patch_store(monkeypatch, store)
    result = await NodeContextService.run_business_rule(
        _domain(), object(), entity_uri=ENTITY, rule_name="VipCustomer"
    )
    assert result["inferred_count"] == 0
    assert result["materialized_count"] == 0
    assert store.inserted == []
