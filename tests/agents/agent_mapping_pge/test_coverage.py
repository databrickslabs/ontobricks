"""Tests for deterministic coverage enforcement + derived mappings.

These lock in the invariant that broke the pipeline before: coverage is
computed from the ontology, NOT from the Planner's discretionary plan, so every
class and every relationship is always attempted.
"""

from agents.agent_mapping_pge import coverage as cov
from agents.agent_mapping_pge.contracts import (
    CanonicalId,
    MappingPlan,
    SkipItem,
    SourceModel,
    TableRole,
    TableRoleCandidate,
)

# A miniature class hierarchy mirroring the maternity shape:
#   Person (abstract) -> Patient (abstract) -> {Mother, Baby concrete}
#   Encounter (abstract) -> {Visit concrete}
PERSON = "u#Person"
PATIENT = "u#Patient"
MOTHER = "u#Mother"
BABY = "u#Baby"
ENCOUNTER = "u#Encounter"
VISIT = "u#Visit"
ATTENDED = "u#attended"  # Visit -> Mother


def _ontology() -> dict:
    return {
        "entities": [
            {"uri": PERSON, "name": "Person", "parent": "", "attributes": []},
            {"uri": PATIENT, "name": "Patient", "parent": "Person",
             "attributes": [{"name": "nhsnumber"}]},
            {"uri": MOTHER, "name": "Mother", "parent": "Patient",
             "attributes": [{"name": "nhsnumber"}, {"name": "postcode"}]},
            {"uri": BABY, "name": "Baby", "parent": "Patient",
             "attributes": [{"name": "nhsnumber"}]},
            {"uri": ENCOUNTER, "name": "Encounter", "parent": "", "attributes": []},
            {"uri": VISIT, "name": "Visit", "parent": "Encounter", "attributes": []},
        ],
        "relationships": [
            {"uri": ATTENDED, "name": "attended", "domain": VISIT, "range": MOTHER},
        ],
    }


def _source_model(skip_some: bool = False) -> SourceModel:
    # Planner only assigns tables to the concrete leaves + only plans a SUBSET.
    roles = [
        TableRole(table="c.s.mother", ontology_class_candidates=[
            TableRoleCandidate(uri=MOTHER, confidence=0.9)]),
        TableRole(table="c.s.baby", ontology_class_candidates=[
            TableRoleCandidate(uri=BABY, confidence=0.9)]),
        TableRole(table="c.s.visit", ontology_class_candidates=[
            TableRoleCandidate(uri=VISIT, confidence=0.9)]),
    ]
    cids = [
        CanonicalId(ontology_class=MOTHER, canonical_column_per_table={"c.s.mother": "nhs"}),
        CanonicalId(ontology_class=VISIT, canonical_column_per_table={"c.s.visit": "vid"}),
    ]
    plan = MappingPlan(
        entity_order=[MOTHER],            # deliberately incomplete
        relationship_order=[],            # deliberately empty
        skip=[SkipItem(item=BABY, reason="planner skipped it")] if skip_some else [],
    )
    return SourceModel(table_roles=roles, canonical_ids=cids, mapping_plan=plan)


def test_classify_abstract_vs_concrete():
    concrete, abstract = cov.classify(_ontology(), _source_model())
    assert abstract == {PERSON, PATIENT, ENCOUNTER}
    assert {MOTHER, BABY, VISIT} <= concrete


def test_full_entity_order_covers_all_classes_abstracts_last():
    order = cov.full_entity_order(_ontology(), _source_model())
    assert set(order) == {PERSON, PATIENT, MOTHER, BABY, ENCOUNTER, VISIT}
    # Every concrete leaf precedes its abstract ancestors.
    assert order.index(MOTHER) < order.index(PATIENT) < order.index(PERSON)
    assert order.index(VISIT) < order.index(ENCOUNTER)


def test_skip_list_does_not_reduce_coverage():
    # Even when the Planner skips Baby, full coverage still includes it.
    order = cov.full_entity_order(_ontology(), _source_model(skip_some=True))
    assert BABY in order


def test_full_relationship_order_includes_all():
    order = cov.full_entity_order(_ontology(), _source_model())
    rels = cov.full_relationship_order(_ontology(), order, _source_model())
    assert rels == [ATTENDED]


def test_concrete_leaf_descendants():
    concrete, _ = cov.classify(_ontology(), _source_model())
    assert set(cov.concrete_leaf_descendants(PERSON, _ontology(), concrete)) == {MOTHER, BABY}
    assert set(cov.concrete_leaf_descendants(PATIENT, _ontology(), concrete)) == {MOTHER, BABY}
    assert set(cov.concrete_leaf_descendants(ENCOUNTER, _ontology(), concrete)) == {VISIT}


def test_build_abstract_union_mapping_reuses_subclass_sql():
    mother_em = {
        "ontology_class": MOTHER, "id_column": "ID",
        "sql_query": "SELECT nhs AS ID, nhs AS nhsnumber, pc AS postcode FROM c.s.mother",
        "attribute_mappings": {"nhsnumber": "nhsnumber", "postcode": "postcode"},
    }
    baby_em = {
        "ontology_class": BABY, "id_column": "ID",
        "sql_query": "SELECT CONCAT(nhs,'-baby') AS ID, nhs AS nhsnumber FROM c.s.baby",
        "attribute_mappings": {"nhsnumber": "nhsnumber"},
    }
    patient = next(e for e in _ontology()["entities"] if e["uri"] == PATIENT)
    m = cov.build_abstract_union_mapping(PATIENT, patient, [mother_em, baby_em])
    assert m is not None
    assert m["id_column"] == "ID"
    assert m["derived"] == "abstract_union"
    # Patient's own attribute (nhsnumber) is projected; both subclass SQLs reused.
    assert "nhsnumber" in m["attribute_mappings"]
    assert "UNION ALL" in m["sql_query"]
    assert "c.s.mother" in m["sql_query"] and "c.s.baby" in m["sql_query"]


def test_build_abstract_union_mapping_none_when_no_subclasses():
    patient = next(e for e in _ontology()["entities"] if e["uri"] == PATIENT)
    assert cov.build_abstract_union_mapping(PATIENT, patient, []) is None


def test_synthetic_endpoint_mapping_from_canonical_ids():
    em = cov.synthetic_endpoint_mapping(_source_model(), VISIT)
    assert em is not None
    assert em["id_column"] == "ID"
    assert "c.s.visit" in em["sql_query"]
    assert em["derived"] == "synthetic_endpoint"


def test_synthetic_endpoint_mapping_none_for_unknown_class():
    assert cov.synthetic_endpoint_mapping(_source_model(), "u#Nope") is None
