"""Tests for api.service — external REST API business logic."""
import pytest

from api.service import (
    get_project_info,
    get_ontology_info,
    get_ontology_classes,
    get_ontology_properties,
    get_mapping_info,
    validate_sparql_query,
    generate_sample_queries,
)


VERSIONED_PROJECT = {
    "info": {
        "name": "TestProject",
        "description": "A test",
        "uri": "http://test.org",
        "author": "tester",
    },
    "versions": {
        "2": {
            "ontology": {
                "classes": [
                    {"uri": "http://test.org#A", "name": "A", "label": "A"},
                    {"uri": "http://test.org#B", "name": "B", "label": "B"},
                ],
                "properties": [
                    {"uri": "http://test.org#rel", "name": "rel", "domain": "A", "range": "B"},
                ],
            },
            "assignment": {
                "entities": [{"ontology_class": "A"}],
                "relationships": [{"property": "rel"}],
                "r2rml_output": "<r2rml/>",
            },
        },
        "1": {
            "ontology": {"classes": [], "properties": []},
            "assignment": {"entities": [], "relationships": []},
        },
    },
}

LEGACY_PROJECT = {
    "info": {"name": "Legacy", "version": "1"},
    "ontology": {
        "classes": [{"uri": "http://x.org#C", "name": "C", "label": "C"}],
        "properties": [],
    },
    "assignment": {
        "entities": [],
        "relationships": [],
    },
}


class TestGetProjectInfo:
    def test_versioned_format(self):
        info = get_project_info(VERSIONED_PROJECT)
        assert info["name"] == "TestProject"
        assert info["version"] == "2"
        assert info["statistics"]["classes"] == 2
        assert info["statistics"]["properties"] == 1
        assert info["statistics"]["entities"] == 1
        assert info["statistics"]["relationships"] == 1
        assert info["statistics"]["has_r2rml"] is True

    def test_legacy_format(self):
        info = get_project_info(LEGACY_PROJECT)
        assert info["name"] == "Legacy"
        assert info["version"] == "1"
        assert info["statistics"]["classes"] == 1

    def test_empty_project(self):
        info = get_project_info({})
        assert info["name"] == "Untitled"
        assert info["statistics"]["classes"] == 0


class TestGetOntologyInfo:
    def test_versioned_format(self):
        info = get_ontology_info(VERSIONED_PROJECT)
        assert info["statistics"]["class_count"] == 2
        assert info["statistics"]["property_count"] == 1
        assert len(info["classes"]) == 2

    def test_basic(self):
        data = {
            "ontology": {
                "classes": [{"name": "X"}],
                "properties": [{"name": "p"}],
            },
            "constraints": [{"type": "c"}],
            "swrl_rules": [{"rule": "r"}],
        }
        info = get_ontology_info(data)
        assert info["statistics"]["class_count"] == 1
        assert info["statistics"]["property_count"] == 1
        assert len(info["constraints"]) == 1
        assert len(info["swrl_rules"]) == 1

    def test_empty(self):
        info = get_ontology_info({})
        assert info["classes"] == []


class TestGetOntologyClasses:
    def test_versioned_format(self):
        classes = get_ontology_classes(VERSIONED_PROJECT)
        assert len(classes) == 2
        assert any(c["uri"] == "http://test.org#A" for c in classes)

    def test_basic(self):
        data = {
            "ontology": {
                "classes": [
                    {"uri": "http://x.org#A", "name": "A", "label": "ClassA", "attributes": ["x"]},
                ]
            }
        }
        classes = get_ontology_classes(data)
        assert len(classes) == 1
        assert classes[0]["name"] == "A"
        assert classes[0]["label"] == "ClassA"


class TestGetOntologyProperties:
    def test_basic(self):
        data = {
            "ontology": {
                "properties": [
                    {"uri": "http://x.org#p", "name": "p", "label": "prop", "domain": "A", "range": "B"},
                ]
            }
        }
        props = get_ontology_properties(data)
        assert len(props) == 1
        assert props[0]["domain"] == "A"


class TestGetMappingInfo:
    def test_versioned_format(self):
        info = get_mapping_info(VERSIONED_PROJECT)
        assert info["statistics"]["entity_count"] == 1
        assert info["statistics"]["relationship_count"] == 1

    def test_basic(self):
        data = {
            "assignment": {
                "entities": [{"cls": "A"}, {"cls": "B"}],
                "relationships": [{"rel": "R"}],
            }
        }
        info = get_mapping_info(data)
        assert info["statistics"]["entity_count"] == 2
        assert info["statistics"]["relationship_count"] == 1

    def test_legacy_keys(self):
        data = {
            "mapping": {
                "data_source_mappings": [{"cls": "A"}],
                "relationship_mappings": [],
            }
        }
        info = get_mapping_info(data)
        assert info["statistics"]["entity_count"] == 1


class TestValidateSparqlQuery:
    def test_valid_query(self):
        ok, err = validate_sparql_query("SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
        assert ok is True
        assert err is None

    def test_invalid_query(self):
        ok, err = validate_sparql_query("NOT A QUERY AT ALL !!!")
        assert ok is False
        assert err is not None


class TestGenerateSampleQueries:
    def test_versioned_format(self):
        samples = generate_sample_queries(VERSIONED_PROJECT)
        assert len(samples) >= 2
        assert any("http://test.org#A" in s.get("query", "") for s in samples)

    def test_basic(self):
        data = {
            "ontology": {
                "classes": [{"uri": "http://x.org#A", "name": "A"}],
                "properties": [{"uri": "http://x.org#p", "name": "p"}],
            }
        }
        samples = generate_sample_queries(data)
        assert len(samples) >= 2
        assert any("Select All" in s["name"] for s in samples)
        assert any("A" in s["name"] for s in samples)

    def test_empty_ontology(self):
        samples = generate_sample_queries({})
        assert len(samples) >= 1
