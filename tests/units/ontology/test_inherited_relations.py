"""Outgoing relation inheritance (domain-side, read-time)."""

from back.objects.ontology.ClassModel import ClassModel


def _config():
    return {
        "classes": [
            {"name": "Personne"},
            {"name": "Employe", "parent": "Personne"},
            {"name": "Manager", "parent": "Employe"},
            {"name": "Ville"},
            {"name": "CycleA", "parent": "CycleB"},
            {"name": "CycleB", "parent": "CycleA"},
            {"name": "Orphan", "parent": "Missing"},
        ],
        "properties": [
            {
                "name": "habiteA",
                "label": "habite à",
                "type": "ObjectProperty",
                "domain": "Personne",
                "range": "Ville",
                "uri": "http://ex#habiteA",
            },
            {
                "name": "firstName",
                "type": "DatatypeProperty",
                "domain": "Personne",
                "range": "xsd:string",
            },
            {
                "name": "knows",
                "type": "ObjectProperty",
                "domain": "Employe",
                "range": "Personne",
            },
        ],
    }


def test_own_relation_is_not_marked_inherited():
    rels = ClassModel.outgoing_relations_for_class(_config(), "Personne")
    names = {r["name"]: r for r in rels}
    assert "habiteA" in names
    assert names["habiteA"]["inherited"] is False
    assert names["habiteA"]["inheritedFrom"] == ""
    assert "firstName" not in names


def test_subclass_inherits_parent_object_property():
    rels = ClassModel.outgoing_relations_for_class(_config(), "Employe")
    by_name = {r["name"]: r for r in rels}
    assert by_name["habiteA"]["inherited"] is True
    assert by_name["habiteA"]["inheritedFrom"] == "Personne"
    assert by_name["knows"]["inherited"] is False


def test_grandchild_inherits_from_declaring_class():
    rels = ClassModel.outgoing_relations_for_class(_config(), "Manager")
    habite = next(r for r in rels if r["name"] == "habiteA")
    assert habite["inheritedFrom"] == "Personne"
    knows = next(r for r in rels if r["name"] == "knows")
    assert knows["inheritedFrom"] == "Employe"


def test_parent_cycle_does_not_loop():
    rels = ClassModel.outgoing_relations_for_class(_config(), "CycleA")
    assert rels == []


def test_missing_parent_is_skipped():
    rels = ClassModel.outgoing_relations_for_class(_config(), "Orphan")
    assert rels == []


def test_does_not_mutate_config_properties():
    cfg = _config()
    ClassModel.outgoing_relations_for_class(cfg, "Employe")
    assert "inherited" not in cfg["properties"][0]
