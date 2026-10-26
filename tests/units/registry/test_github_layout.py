"""Tests for DomainRepoLayout — registry version document <-> repository files."""

from __future__ import annotations

import copy
import json

import pytest
from rdflib import Graph

from back.core.errors import ValidationError
from back.objects.registry.gitsync import DomainRepoLayout


def _doc(with_mapping: bool = True) -> dict:
    entities = (
        [
            {
                "ontology_class": "http://test.org/ontology#Customer",
                "ontology_class_label": "Customer",
                "sql_query": "SELECT * FROM main.sales.customers",
                "id_column": "customer_id",
                "label_column": "name",
                "attribute_mappings": {},
            }
        ]
        if with_mapping
        else []
    )
    return {
        "info": {
            "name": "Sales",
            "description": "Sales domain",
            "status": "DRAFT",
            "last_update": "2026-10-12T10:00:00Z",
            "last_build": "2026-10-11T10:00:00Z",
            "graph_cache_state": "ready",
            "connection": {"password": "s3cr3t", "user": "bob"},
        },
        "versions": {
            "2": {
                "ontology": {
                    "name": "Sales",
                    "base_uri": "http://test.org/ontology#",
                    "classes": [
                        {
                            "uri": "http://test.org/ontology#Customer",
                            "name": "Customer",
                            "label": "Customer",
                            "dataProperties": [],
                        }
                    ],
                    "properties": [],
                    "constraints": [],
                    "swrl_rules": [{"name": "r1", "body": "Customer(?x)"}],
                    "axioms": [],
                    "expressions": [],
                    "groups": [],
                    "decision_tables": [],
                },
                "assignment": {"entities": entities, "relationships": []},
                "design_layout": {
                    "current_view": "Main view",
                    "views": {
                        "Main view": {"entities": [{"id": "Customer", "x": 1, "y": 2}]},
                        "main/view": {"entities": []},
                    },
                    "map": {"zoom": 3},
                },
                "metadata": {"catalog": "main", "schema": "sales", "tables": []},
            }
        },
    }


def _expected_round_trip(doc: dict) -> dict:
    expected = copy.deepcopy(doc)
    for key in DomainRepoLayout.VOLATILE_INFO_KEYS:
        expected["info"].pop(key, None)
    expected["info"]["connection"].pop("password")
    return expected


class TestSplitMerge:
    def test_round_trip_reproduces_document(self):
        doc = _doc()
        files, errors = DomainRepoLayout.split_version(doc, "Sales", "2")
        assert errors == []
        assert DomainRepoLayout.merge_version(files, "2") == _expected_round_trip(doc)

    def test_round_trip_minimal_read_version_shape(self):
        doc = {
            "info": {"name": "Empty"},
            "versions": {
                "1": {"ontology": {}, "assignment": {}, "design_layout": {}, "metadata": {}}
            },
        }
        files, _ = DomainRepoLayout.split_version(doc, "Empty", "1")
        assert DomainRepoLayout.merge_version(files, "1") == doc

    def test_one_file_per_concern(self):
        files, _ = DomainRepoLayout.split_version(_doc(), "Sales", "2")
        for rel in (
            "manifest.json",
            "info.json",
            "ontology/ontology.json",
            "ontology/rules/swrl.json",
            "ontology/rules/decision_tables.json",
            "mapping/assignment.json",
            "layout/design.json",
            "layout/map.json",
            "layout/views/Main_view.json",
            "layout/views/main_view_2.json",
            "metadata/tables.json",
        ):
            assert rel in files, rel
        assert "swrl_rules" not in json.loads(files["ontology/ontology.json"])

    def test_secrets_and_volatile_fields_are_stripped(self):
        files, _ = DomainRepoLayout.split_version(_doc(), "Sales", "2")
        info = json.loads(files["info.json"])
        assert "s3cr3t" not in "".join(files.values())
        assert "last_update" not in info and "graph_cache_state" not in info
        manifest = json.loads(files["manifest.json"])
        assert manifest["last_update"] == "2026-10-12T10:00:00Z"

    def test_output_is_deterministic(self):
        a, _ = DomainRepoLayout.split_version(_doc(), "Sales", "2")
        b, _ = DomainRepoLayout.split_version(_doc(), "Sales", "2")
        assert a == b


class TestTurtleFiles:
    def test_owl_and_r2rml_turtle_are_written_and_parse(self):
        files, errors = DomainRepoLayout.split_version(_doc(), "Sales", "2")
        assert errors == []
        owl = files["ontology/Sales.owl.ttl"]
        r2rml = files["mapping/Sales.r2rml.ttl"]
        assert len(Graph().parse(data=owl, format="turtle")) > 0
        assert len(Graph().parse(data=r2rml, format="turtle")) > 0
        assert "customer_id" in r2rml
        manifest = json.loads(files["manifest.json"])
        assert manifest["generated"] == [
            "mapping/Sales.r2rml.ttl",
            "ontology/Sales.owl.ttl",
        ]
        assert manifest["files"]["ontology/Sales.owl.ttl"] == DomainRepoLayout.sha256(owl)

    def test_no_r2rml_without_mappings(self):
        files, _ = DomainRepoLayout.split_version(_doc(with_mapping=False), "Sales", "2")
        assert "ontology/Sales.owl.ttl" in files
        assert "mapping/Sales.r2rml.ttl" not in files

    def test_generation_failure_is_reported_not_raised(self, monkeypatch):
        from back.objects.ontology import Owl

        def boom(*_a, **_k):
            raise RuntimeError("bad ontology")

        monkeypatch.setattr(Owl, "generate_owl", staticmethod(boom))
        files, errors = DomainRepoLayout.split_version(_doc(), "Sales", "2")
        assert "ontology/Sales.owl.ttl" not in files
        assert "info.json" in files
        assert any("OWL generation failed" in e for e in errors)

    def test_merge_ignores_turtle_files(self):
        files, _ = DomainRepoLayout.split_version(_doc(), "Sales", "2")
        files["ontology/Sales.owl.ttl"] = "not turtle at all"
        assert DomainRepoLayout.merge_version(files, "2") == _expected_round_trip(_doc())


class TestPaths:
    def test_parse_version_path(self):
        assert DomainRepoLayout.parse_version_path(
            "domains/Sales/versions/V2/ontology/ontology.json"
        ) == ("Sales", "2", "ontology/ontology.json")
        assert DomainRepoLayout.parse_version_path("domains/Sales/domain.json") is None

    def test_newer_layout_format_is_refused(self):
        files, _ = DomainRepoLayout.split_version(_doc(), "Sales", "2")
        manifest = json.loads(files["manifest.json"])
        manifest["format_version"] = DomainRepoLayout.FORMAT_VERSION + 1
        files["manifest.json"] = json.dumps(manifest)
        with pytest.raises(ValidationError):
            DomainRepoLayout.merge_version(files, "2")
