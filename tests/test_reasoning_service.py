"""Tests for the ReasoningService orchestration layer.

Complements test_reasoning.py (models + OWLRL) and test_swrl_engine.py
(SWRL translators + engine) by covering the service-level orchestration,
phase skipping, graph reasoning, constraint checks, and helper functions.
"""
import importlib

import pytest
from unittest.mock import MagicMock, PropertyMock, patch

from back.core.reasoning.models import InferredTriple, ReasoningResult, RuleViolation
from back.core.reasoning.ReasoningService import ReasoningService

_local_name = ReasoningService._local_name
_namespace_parts = ReasoningService._namespace_parts

_has_owlrl = importlib.util.find_spec("owlrl") is not None
requires_owlrl = pytest.mark.skipif(not _has_owlrl, reason="owlrl not installed")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _domain_session(
    ontology=None,
    owl_content="",
    swrl_rules=None,
    info=None,
):
    """Build a lightweight mock DomainSession."""
    domain = MagicMock()
    domain.ontology = ontology or {
        "name": "TestOntology",
        "base_uri": "http://test.org/ontology#",
        "classes": [
            {"uri": "http://test.org/ontology#Customer", "name": "Customer"},
            {"uri": "http://test.org/ontology#Order", "name": "Order"},
        ],
        "properties": [
            {
                "uri": "http://test.org/ontology#hasOrder",
                "name": "hasOrder",
                "characteristics": [],
            },
        ],
        "constraints": [],
        "swrl_rules": swrl_rules or [],
        "shacl_shapes": [],
    }
    domain.generated_owl = owl_content
    domain.swrl_rules = swrl_rules or []
    domain.info = info or {"name": "test_graph"}
    return domain


# ===========================================================================
# Utility function tests
# ===========================================================================

class TestLocalName:
    def test_fragment(self):
        assert _local_name("http://example.org/ontology#Customer") == "Customer"

    def test_path(self):
        assert _local_name("http://example.org/ontology/Customer") == "Customer"

    def test_no_separator(self):
        assert _local_name("Customer") == "Customer"


class TestNamespaceParts:
    def test_hash_ending(self):
        data_ns, sep = _namespace_parts("http://example.org/ontology#")
        assert sep == ""
        assert data_ns == "http://example.org/ontology/"

    def test_slash_ending(self):
        data_ns, sep = _namespace_parts("http://example.org/ontology/")
        assert sep == ""
        assert data_ns == "http://example.org/ontology/"

    def test_no_ending(self):
        data_ns, sep = _namespace_parts("http://example.org/ontology")
        assert sep == "#"
        assert data_ns == "http://example.org/ontology/"

    def test_empty(self):
        data_ns, sep = _namespace_parts("")
        assert sep == "#"
        assert data_ns == ""


# ===========================================================================
# Normalize property URI
# ===========================================================================

class TestNormalizePropertyUri:
    def test_empty_uri_uses_name(self):
        uri = ReasoningService._normalize_property_uri(
            "", "http://ex.org/data/", "http://ex.org#", "#", "myProp"
        )
        assert uri == "http://ex.org/data/myProp"

    def test_uri_not_in_data_ns_gets_rewritten(self):
        uri = ReasoningService._normalize_property_uri(
            "http://other.org/myProp", "http://ex.org/data/", "http://ex.org#", "#", "myProp"
        )
        assert uri == "http://ex.org/data/myProp"

    def test_uri_already_in_data_ns_unchanged(self):
        uri = ReasoningService._normalize_property_uri(
            "http://ex.org/data/myProp", "http://ex.org/data/", "http://ex.org#", "#", "myProp"
        )
        assert uri == "http://ex.org/data/myProp"


# ===========================================================================
# Find properties by characteristic
# ===========================================================================

class TestFindPropertiesByCharacteristic:
    def test_finds_transitive(self):
        ontology = {
            "base_uri": "http://ex.org#",
            "properties": [
                {"name": "isPartOf", "uri": "http://ex.org#isPartOf", "characteristics": ["transitive"]},
                {"name": "hasOrder", "uri": "http://ex.org#hasOrder", "characteristics": []},
            ],
        }
        result = ReasoningService._find_properties_by_characteristic(ontology, "transitive")
        assert len(result) == 1
        assert "isPartOf" in result[0]

    def test_finds_symmetric(self):
        ontology = {
            "base_uri": "http://ex.org#",
            "properties": [
                {"name": "hasSibling", "uri": "http://ex.org#hasSibling", "characteristics": ["Symmetric"]},
            ],
        }
        result = ReasoningService._find_properties_by_characteristic(ontology, "symmetric")
        assert len(result) == 1

    def test_no_matching_characteristic(self):
        ontology = {
            "base_uri": "http://ex.org#",
            "properties": [
                {"name": "hasOrder", "uri": "http://ex.org#hasOrder", "characteristics": []},
            ],
        }
        result = ReasoningService._find_properties_by_characteristic(ontology, "transitive")
        assert len(result) == 0


# ===========================================================================
# run_full_reasoning — phase orchestration
# ===========================================================================

class TestRunFullReasoning:
    def test_all_phases_disabled(self):
        svc = ReasoningService(_domain_session())
        result = svc.run_full_reasoning({"tbox": False, "swrl": False, "graph": False})
        assert result.stats.get("tbox_skipped") is True
        assert result.stats.get("swrl_skipped") is True
        assert result.stats.get("graph_skipped") is True
        assert "total_duration_seconds" in result.stats

    def test_tbox_skipped_when_no_owl(self):
        svc = ReasoningService(_domain_session(owl_content=""))
        result = svc.run_full_reasoning({"swrl": False, "graph": False})
        assert result.stats.get("tbox_skipped") is True

    def test_swrl_skipped_when_no_rules(self):
        svc = ReasoningService(_domain_session(swrl_rules=[]))
        result = svc.run_full_reasoning({"tbox": False, "graph": False})
        assert result.stats.get("swrl_skipped") is True

    def test_graph_skipped_when_no_store(self):
        svc = ReasoningService(_domain_session(), triplestore_backend=None)
        result = svc.run_full_reasoning({"tbox": False, "swrl": False})
        assert result.stats.get("graph_skipped") is True

    @requires_owlrl
    def test_tbox_phase_runs(self):
        owl = (
            "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n"
            "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n"
            "@prefix ex: <http://test.org/ontology#> .\n"
            "ex:A a owl:Class .\n"
            "ex:B a owl:Class ; rdfs:subClassOf ex:A .\n"
        )
        svc = ReasoningService(_domain_session(owl_content=owl))
        result = svc.run_full_reasoning({"swrl": False, "graph": False})
        assert "tbox_duration_seconds" in result.stats
        assert result.stats.get("tbox_skipped") is not True

    def test_phase_error_captured_in_stats(self):
        domain = _domain_session()
        domain.generated_owl = "invalid turtle!!!"
        svc = ReasoningService(domain)
        result = svc.run_full_reasoning({"swrl": False, "graph": False})
        has_error = result.stats.get("tbox_error") or result.stats.get("tbox_skipped")
        assert has_error


# ===========================================================================
# run_tbox_reasoning
# ===========================================================================

class TestRunTboxReasoning:
    def test_no_owl_content(self):
        svc = ReasoningService(_domain_session(owl_content=""))
        result = svc.run_tbox_reasoning()
        assert result.stats.get("skipped") is True

    @requires_owlrl
    def test_with_valid_owl(self):
        owl = (
            "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n"
            "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n"
            "@prefix ex: <http://test.org/ontology#> .\n"
            "ex:Animal a owl:Class .\n"
            "ex:Dog a owl:Class ; rdfs:subClassOf ex:Animal .\n"
        )
        svc = ReasoningService(_domain_session(owl_content=owl))
        result = svc.run_tbox_reasoning()
        assert result.stats["phase"] == "tbox"
        assert result.stats["original_count"] > 0


# ===========================================================================
# run_swrl_rules
# ===========================================================================

class TestRunSwrlRules:
    def test_no_rules(self):
        svc = ReasoningService(_domain_session(swrl_rules=[]))
        result = svc.run_swrl_rules()
        assert result.stats.get("skipped") is True

    def test_no_store(self):
        rules = [{"name": "r1", "antecedent": "A(?x)", "consequent": "B(?x)"}]
        svc = ReasoningService(_domain_session(swrl_rules=rules), triplestore_backend=None)
        result = svc.run_swrl_rules()
        assert result.stats.get("skipped") is True

    def test_with_rules_and_store(self):
        rules = [{
            "name": "test_rule",
            "antecedent": "Customer(?x) \u2227 hasOrder(?x, ?y)",
            "consequent": "hasOrder(?x, ?z)",
        }]
        store = MagicMock()
        store.execute_query.return_value = [{"s": "http://test.org/data/c1"}]
        svc = ReasoningService(
            _domain_session(swrl_rules=rules),
            triplestore_backend=store,
        )
        result = svc.run_swrl_rules()
        assert result.stats["phase"] == "swrl"


# ===========================================================================
# run_graph_reasoning
# ===========================================================================

class TestRunGraphReasoning:
    def test_no_store(self):
        svc = ReasoningService(_domain_session(), triplestore_backend=None)
        result = svc.run_graph_reasoning()
        assert result.stats.get("skipped") is True

    def test_transitive_closure(self):
        store = MagicMock()
        store.transitive_closure.return_value = [
            {"subject": "http://ex.org/a", "predicate": "http://ex.org/isPartOf", "object": "http://ex.org/c"},
        ]
        store.symmetric_expand.return_value = []

        ontology = {
            "base_uri": "http://ex.org#",
            "classes": [],
            "properties": [
                {"name": "isPartOf", "uri": "http://ex.org#isPartOf", "characteristics": ["transitive"]},
            ],
            "constraints": [],
            "swrl_rules": [],
            "shacl_shapes": [],
        }
        svc = ReasoningService(
            _domain_session(ontology=ontology),
            triplestore_backend=store,
        )
        result = svc.run_graph_reasoning()
        assert result.stats["phase"] == "graph"
        assert len(result.inferred_triples) == 1
        assert result.inferred_triples[0].provenance == "graph:transitive"

    def test_symmetric_expand(self):
        store = MagicMock()
        store.transitive_closure.return_value = []
        store.symmetric_expand.return_value = [
            {"subject": "http://ex.org/b", "predicate": "http://ex.org/hasSibling", "object": "http://ex.org/a"},
        ]

        ontology = {
            "base_uri": "http://ex.org#",
            "classes": [],
            "properties": [
                {"name": "hasSibling", "uri": "http://ex.org#hasSibling", "characteristics": ["symmetric"]},
            ],
            "constraints": [],
            "swrl_rules": [],
            "shacl_shapes": [],
        }
        svc = ReasoningService(
            _domain_session(ontology=ontology),
            triplestore_backend=store,
        )
        result = svc.run_graph_reasoning()
        assert len(result.inferred_triples) == 1
        assert result.inferred_triples[0].provenance == "graph:symmetric"

    def test_store_method_failure_handled(self):
        store = MagicMock()
        store.transitive_closure.side_effect = Exception("Cypher error")
        store.symmetric_expand.return_value = []

        ontology = {
            "base_uri": "http://ex.org#",
            "classes": [],
            "properties": [
                {"name": "isPartOf", "uri": "http://ex.org#isPartOf", "characteristics": ["transitive"]},
            ],
            "constraints": [],
            "swrl_rules": [],
            "shacl_shapes": [],
        }
        svc = ReasoningService(
            _domain_session(ontology=ontology),
            triplestore_backend=store,
        )
        result = svc.run_graph_reasoning()
        assert result.stats["phase"] == "graph"
        assert len(result.inferred_triples) == 0


# ===========================================================================
# run_constraint_checks
# ===========================================================================

class TestRunConstraintChecks:
    def test_no_constraints_and_no_shapes(self):
        svc = ReasoningService(_domain_session())
        result = svc.run_constraint_checks()
        assert result.stats.get("skipped") is True

    def test_no_store(self):
        ontology = {
            "base_uri": "http://ex.org#",
            "classes": [],
            "properties": [],
            "constraints": [{"type": "minCardinality", "className": "X", "property": "p", "cardinalityValue": 1}],
            "swrl_rules": [],
            "shacl_shapes": [],
        }
        svc = ReasoningService(_domain_session(ontology=ontology), triplestore_backend=None)
        result = svc.run_constraint_checks()
        assert result.stats.get("skipped") is True

    def test_non_ladybug_store_skipped(self):
        ontology = {
            "base_uri": "http://ex.org#",
            "classes": [],
            "properties": [],
            "constraints": [{"type": "minCardinality", "className": "X", "property": "p", "cardinalityValue": 1}],
            "swrl_rules": [],
            "shacl_shapes": [],
        }
        store = MagicMock()  # not LadybugBase
        svc = ReasoningService(_domain_session(ontology=ontology), triplestore_backend=store)
        result = svc.run_constraint_checks()
        assert result.stats.get("skipped") is True
        assert "Cypher-capable" in result.stats.get("reason", "")


# ===========================================================================
# materialize_inferred
# ===========================================================================

class TestMaterializeInferred:
    def test_no_store(self):
        svc = ReasoningService(_domain_session(), triplestore_backend=None)
        result = ReasoningResult(inferred_triples=[
            InferredTriple("s1", "p1", "o1", "test"),
        ])
        assert svc.materialize_inferred(result) == 0

    def test_empty_triples(self):
        store = MagicMock()
        svc = ReasoningService(_domain_session(), triplestore_backend=store)
        result = ReasoningResult()
        assert svc.materialize_inferred(result) == 0
        store.insert_triples.assert_not_called()

    def test_inserts_triples(self):
        store = MagicMock()
        store.insert_triples.return_value = 2
        svc = ReasoningService(_domain_session(), triplestore_backend=store)
        result = ReasoningResult(inferred_triples=[
            InferredTriple("http://ex.org/a", "http://ex.org/p", "http://ex.org/b", "test"),
            InferredTriple("http://ex.org/c", "http://ex.org/p", "http://ex.org/d", "test"),
        ])
        count = svc.materialize_inferred(result)
        assert count == 2
        store.insert_triples.assert_called_once()

    def test_skips_batch_subjects(self):
        store = MagicMock()
        store.insert_triples.return_value = 1
        svc = ReasoningService(_domain_session(), triplestore_backend=store)
        result = ReasoningResult(inferred_triples=[
            InferredTriple("(batch)", "p", "o", "test"),
            InferredTriple("http://ex.org/a", "p", "o", "test"),
        ])
        svc.materialize_inferred(result)
        triples_arg = store.insert_triples.call_args[0][1]
        assert len(triples_arg) == 1
        assert triples_arg[0]["subject"] == "http://ex.org/a"
