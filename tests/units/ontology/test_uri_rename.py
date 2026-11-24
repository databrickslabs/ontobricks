"""Renaming an ontology item re-derives its URI and keeps its mappings (DC-E06)."""

import copy

from back.objects.ontology.ClassModel import ClassModel
from back.objects.ontology.Ontology import Ontology

BASE = "http://test.org/ontology#"


def _load(ps, ontology, mappings):
    ps._data["ontology"].update(copy.deepcopy(ontology))
    ps._data["assignment"]["entities"] = copy.deepcopy(mappings["entities"])
    ps._data["assignment"]["relationships"] = copy.deepcopy(mappings["relationships"])


def _find(items, name):
    return next(i for i in items if i.get("name") == name)


def _save(ps, cfg):
    return Ontology(ps).save_ontology_config_from_editor({"config": cfg})


def test_renamed_entity_gets_uri_from_new_name_and_mappings_follow(
    domain_session, sample_ontology_config, sample_mapping_config
):
    _load(domain_session, sample_ontology_config, sample_mapping_config)
    cfg = copy.deepcopy(sample_ontology_config)
    customer = _find(cfg["classes"], "Customer")
    customer["name"] = "Client"
    customer["localName"] = "Client"
    for prop in cfg["properties"]:
        if prop["domain"] == "Customer":
            prop["domain"] = "Client"

    out = _save(domain_session, cfg)

    assert out["success"]
    assert out["mappings_cleaned"] == {
        "entity_mappings_removed": 0,
        "relationship_mappings_removed": 0,
    }
    assert _find(domain_session.get_classes(), "Client")["uri"] == BASE + "Client"
    entity_uris = {m["ontology_class"] for m in domain_session.get_entity_mappings()}
    assert BASE + "Client" in entity_uris
    assert BASE + "Customer" not in entity_uris
    rel = domain_session.get_relationship_mappings()[0]
    assert rel["source_class"] == BASE + "Client"
    assert rel["property"] == BASE + "hasOrder"


def test_freed_name_can_be_reused_without_duplicate_uri(
    domain_session, sample_ontology_config, sample_mapping_config
):
    _load(domain_session, sample_ontology_config, sample_mapping_config)
    cfg = copy.deepcopy(sample_ontology_config)
    _find(cfg["classes"], "Customer")["name"] = "Client"
    cfg["classes"].append({"name": "Customer", "label": "Customer", "dataProperties": []})

    _save(domain_session, cfg)

    uris = [c["uri"] for c in domain_session.get_classes()]
    assert len(uris) == len(set(uris))
    assert _find(domain_session.get_classes(), "Customer")["uri"] == BASE + "Customer"


def test_swapped_names_remap_each_mapping_to_its_entity(
    domain_session, sample_ontology_config, sample_mapping_config
):
    _load(domain_session, sample_ontology_config, sample_mapping_config)
    cfg = copy.deepcopy(sample_ontology_config)
    customer, order = _find(cfg["classes"], "Customer"), _find(cfg["classes"], "Order")
    customer["name"], order["name"] = "Order", "Customer"

    _save(domain_session, cfg)

    by_table = {m["table"]: m["ontology_class"] for m in domain_session.get_entity_mappings()}
    # The entity formerly known as Customer (table customers) is now named Order.
    assert by_table["customers"] == BASE + "Order"
    assert by_table["orders"] == BASE + "Customer"


def test_legacy_duplicate_uri_keeps_mappings_on_the_original_item(
    domain_session, sample_ontology_config
):
    cfg = copy.deepcopy(sample_ontology_config)
    cfg["classes"] = [
        {"name": "assignedto2", "uri": BASE + "assignedto", "dataProperties": []},
        {"name": "assignedto", "uri": BASE + "assignedto", "dataProperties": []},
    ]
    domain_session._data["ontology"].update(copy.deepcopy(cfg))
    domain_session._data["assignment"]["entities"] = [
        {"ontology_class": BASE + "assignedto", "table": "t"}
    ]
    edited = copy.deepcopy(cfg)
    _find(edited["classes"], "assignedto2")["name"] = "foo"

    _save(domain_session, edited)

    assert _find(domain_session.get_classes(), "foo")["uri"] == BASE + "foo"
    assert _find(domain_session.get_classes(), "assignedto")["uri"] == BASE + "assignedto"
    assert domain_session.get_entity_mappings()[0]["ontology_class"] == BASE + "assignedto"


def test_imported_entity_rename_keeps_source_namespace(domain_session):
    source_ns = "https://src.example.com/onto#"
    old = [{"name": "Person", "uri": source_ns + "Person", "dataProperties": []}]
    new = [{"name": "Human", "uri": source_ns + "Person", "dataProperties": []}]

    class_renames, property_renames = ClassModel.rewrite_renamed_uris(old, new, [], [])

    assert new[0]["uri"] == source_ns + "Human"
    assert class_renames == {source_ns + "Person": source_ns + "Human"}
    assert property_renames == {}


def test_renamed_attribute_gets_uri_from_new_name(sample_ontology_config):
    old = copy.deepcopy(sample_ontology_config["classes"])
    old[0]["dataProperties"][0]["uri"] = BASE + "firstName"
    new = copy.deepcopy(old)
    new[0]["dataProperties"][0]["name"] = "givenName"

    ClassModel.rewrite_renamed_uris(old, new, [], [])

    assert new[0]["dataProperties"][0]["uri"] == BASE + "givenName"


def test_unchanged_names_keep_their_uris(
    domain_session, sample_ontology_config, sample_mapping_config
):
    _load(domain_session, sample_ontology_config, sample_mapping_config)
    cfg = copy.deepcopy(sample_ontology_config)

    out = _save(domain_session, cfg)

    assert out["uri_renames"] == {"classes": {}, "properties": {}}
    assert out["attribute_uris"] == ClassModel.attribute_uris(sample_ontology_config)


def test_attribute_uris_are_positional_per_class(sample_ontology_config):
    cfg = copy.deepcopy(sample_ontology_config)
    cfg["classes"][0]["dataProperties"][0]["uri"] = BASE + "firstName"

    uris = ClassModel.attribute_uris(cfg)

    assert uris[0][0] == BASE + "firstName"
    assert len(uris) == len(cfg["classes"])


def test_save_reports_renamed_uris_for_the_editor(
    domain_session, sample_ontology_config, sample_mapping_config
):
    _load(domain_session, sample_ontology_config, sample_mapping_config)
    cfg = copy.deepcopy(sample_ontology_config)
    _find(cfg["classes"], "Customer")["name"] = "Client"

    out = _save(domain_session, cfg)

    assert out["uri_renames"]["classes"] == {BASE + "Customer": BASE + "Client"}
