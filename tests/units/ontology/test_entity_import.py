"""Rules for copying entities from another registry domain (Studio Import entity)."""

import pytest

from back.core.errors import ValidationError
from back.objects.ontology.EntityImport import EntityImport

pytestmark = pytest.mark.unit

SRC = "http://src.org/onto#"


def _source():
    return {
        "base_uri": SRC,
        "classes": [
            {"name": "Person", "uri": f"{SRC}Person", "label": "Person", "emoji": "👤",
             "parent": "", "dataProperties": [{"name": "fullName", "uri": f"{SRC}fullName"}]},
            {"name": "Customer", "uri": f"{SRC}Customer", "label": "Customer", "parent": "Person",
             "dataProperties": [{"name": "email", "uri": f"{SRC}email"}],
             "dataset": {"fullName": "main.crm.customers"}, "bridges": [{"x": 1}],
             "actions": [{"y": 1}], "dashboard": "d", "dashboardParams": {"a": 1}},
            {"name": "Order", "uri": f"{SRC}Order", "label": "Order", "parent": "", "dataProperties": []},
            {"name": "Address", "uri": f"{SRC}Address", "label": "Address", "parent": "", "dataProperties": []},
        ],
        "properties": [
            {"name": "places", "uri": f"{SRC}places", "type": "ObjectProperty",
             "domain": "Customer", "range": "Order"},
            {"name": "livesAt", "uri": f"{SRC}livesAt", "type": "ObjectProperty",
             "domain": "Person", "range": "Address"},
            {"name": "amount", "uri": f"{SRC}amount", "type": "DatatypeProperty",
             "domain": "Order", "range": "decimal"},
        ],
    }


@pytest.fixture
def target(domain_session):
    domain_session.ontology["base_uri"] = "http://tgt.org/onto#"
    domain_session.ontology["classes"] = [
        {"name": "Address", "uri": "http://tgt.org/onto#Address", "label": "Address", "parent": ""},
    ]
    domain_session.ontology["properties"] = []
    return domain_session


def _run(session, names, renames=None):
    return EntityImport(session).import_entities(
        _source(), names, renames or {}, source_domain="Sales", source_version="3"
    )


def _cls(session, name):
    return next(c for c in session.ontology["classes"] if c["name"] == name)


class TestCatalog:
    def test_lists_entities_with_exists_flag_and_relationships_only(self, target):
        cat = EntityImport.build_catalog(_source(), target.ontology)
        by_name = {e["name"]: e for e in cat["entities"]}
        assert set(by_name) == {"Person", "Customer", "Order", "Address"}
        assert by_name["Address"]["exists"] is True
        assert by_name["Customer"]["exists"] is False
        assert by_name["Customer"]["parent"] == "Person"
        assert by_name["Person"]["attributes"] == 1
        assert {r["name"] for r in cat["relationships"]} == {"places", "livesAt"}


class TestClassCopy:
    def test_copies_with_rebased_uris_and_provenance(self, target):
        result = _run(target, ["Order"])
        assert result["imported"] == ["Order"]
        order = _cls(target, "Order")
        assert order["uri"] == "http://tgt.org/onto#Order"
        assert order["importedFrom"] == {"domain": "Sales", "version": "3", "uri": f"{SRC}Order"}

    def test_attribute_uris_rebased_and_mapping_fields_dropped(self, target):
        _run(target, ["Customer"])
        customer = _cls(target, "Customer")
        assert customer["dataProperties"][0]["uri"] == "http://tgt.org/onto#email"
        for field in ("dataset", "bridges", "actions", "dashboard", "dashboardParams"):
            assert field not in customer

    def test_top_level_datatype_properties_become_attributes(self, target):
        source = _source()
        source["properties"].append(
            {"name": "status", "uri": f"{SRC}status", "type": "DatatypeProperty",
             "domain": "Address", "range": "string"}
        )
        cat = EntityImport.build_catalog(source, {"classes": []})
        assert next(e for e in cat["entities"] if e["name"] == "Order")["attributes"] == 1
        EntityImport(target).import_entities(
            source, ["Order"], {}, source_domain="Sales", source_version="3"
        )
        attrs = _cls(target, "Order")["dataProperties"]
        assert [(a["name"], a["uri"]) for a in attrs] == [("amount", "http://tgt.org/onto#amount")]

    @pytest.mark.parametrize("base, expected", [
        ("http://tgt.org/onto/", "http://tgt.org/onto/Order"),
        ("http://tgt.org/onto", "http://tgt.org/onto#Order"),
    ])
    def test_base_uri_separator(self, target, base, expected):
        target.ontology["base_uri"] = base
        _run(target, ["Order"])
        assert _cls(target, "Order")["uri"] == expected

    def test_parent_kept_when_imported(self, target):
        _run(target, ["Customer", "Person"])
        assert _cls(target, "Customer")["parent"] == "Person"

    def test_parent_cleared_when_not_resolvable(self, target):
        _run(target, ["Customer"])
        assert _cls(target, "Customer")["parent"] == ""


class TestRelationships:
    def test_between_two_imported_entities(self, target):
        result = _run(target, ["Customer", "Order"])
        assert result["relationships"] == ["places"]
        rel = next(p for p in target.ontology["properties"] if p["name"] == "places")
        assert (rel["domain"], rel["range"]) == ("Customer", "Order")
        assert rel["uri"] == "http://tgt.org/onto#places"

    def test_to_existing_same_name_class_not_selected(self, target):
        result = _run(target, ["Person"])
        assert result["relationships"] == ["livesAt"]

    def test_skipped_when_endpoint_unresolved(self, target):
        result = _run(target, ["Customer"])
        assert result["relationships"] == []

    def test_none_when_both_endpoints_already_exist(self, target):
        target.ontology["classes"].append({"name": "Person", "uri": "http://tgt.org/onto#Person"})
        result = _run(target, ["Person", "Address"])
        assert result["imported"] == []
        assert result["relationships"] == []

    def test_skipped_when_uri_already_used(self, target):
        target.ontology["properties"] = [
            {"name": "places", "uri": "http://tgt.org/onto#places", "domain": "Address", "range": "Address"},
        ]
        result = _run(target, ["Customer", "Order"])
        assert result["skipped_relationships"] == [{"name": "places", "reason": "uri_exists"}]


class TestConflicts:
    def test_existing_entity_is_skipped_and_reused(self, target):
        result = _run(target, ["Address", "Person"])
        assert result["skipped"] == [{"name": "Address", "reason": "exists"}]
        assert result["imported"] == ["Person"]
        assert result["relationships"] == ["livesAt"]
        assert [c["name"] for c in target.ontology["classes"]].count("Address") == 1

    def test_rename_imports_under_new_name_and_remaps_relationships(self, target):
        result = _run(target, ["Address", "Person"], {"Address": "SalesAddress"})
        assert set(result["imported"]) == {"SalesAddress", "Person"}
        assert _cls(target, "SalesAddress")["uri"] == "http://tgt.org/onto#SalesAddress"
        rel = next(p for p in target.ontology["properties"] if p["name"] == "livesAt")
        assert rel["range"] == "SalesAddress"

    @pytest.mark.parametrize("bad", ["", "has space", "1abc", "address"])
    def test_invalid_rename_rejected(self, target, bad):
        with pytest.raises(ValidationError):
            _run(target, ["Address"], {"Address": bad})

    def test_unknown_entity_rejected(self, target):
        with pytest.raises(ValidationError):
            _run(target, ["Ghost"])

    def test_nothing_saved_when_nothing_imported(self, target, monkeypatch):
        calls = []
        monkeypatch.setattr(target, "save", lambda: calls.append(1))
        _run(target, ["Address"])
        assert calls == []
