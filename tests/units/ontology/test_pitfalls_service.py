"""Tests for PitfallsService and the vendored OntologyPatternToolkit utils."""

import tempfile
import os
from unittest.mock import MagicMock, patch

import pytest
from rdflib import Graph

# Marker: tests that require the pitfalls optional deps (sentence-transformers, sklearn, nltk)
_PITFALLS_DEPS = pytest.mark.skipif(
    not __import__("importlib").util.find_spec("nltk"),
    reason="pitfalls optional deps not installed (pip install .[pitfalls])",
)


# ── Synthetic TTL fixture ─────────────────────────────────────────────────────

SAMPLE_TURTLE = """
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix : <http://test.org/onto#> .

<http://test.org/onto> a owl:Ontology ;
    rdfs:label "TestOntology" .

:Animal a owl:Class ;
    rdfs:label "Animal" .

:Dog a owl:Class ;
    rdfs:subClassOf :Animal ;
    rdfs:label "Dog" .

:Cat a owl:Class ;
    rdfs:subClassOf :Animal ;
    rdfs:label "Cat" .

:Puppy a owl:Class ;
    rdfs:subClassOf :Dog ;
    rdfs:label "Puppy" .

:hasOwner a owl:ObjectProperty ;
    rdfs:label "hasOwner" ;
    rdfs:domain :Animal ;
    rdfs:range :Animal .

:hasName a owl:DatatypeProperty ;
    rdfs:label "hasName" ;
    rdfs:domain :Animal ;
    rdfs:range xsd:string .
"""

DISJOINT_TURTLE = """
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix : <http://test.org/disjoint#> .

<http://test.org/disjoint> a owl:Ontology .

:Parent a owl:Class .
:Child a owl:Class ;
    rdfs:subClassOf :Parent ;
    owl:disjointWith :Parent .
"""


@pytest.fixture
def sample_graph():
    g = Graph()
    g.parse(data=SAMPLE_TURTLE, format="turtle")
    return g


@pytest.fixture
def disjoint_graph():
    g = Graph()
    g.parse(data=DISJOINT_TURTLE, format="turtle")
    return g


@pytest.fixture
def sample_ttl_file(tmp_path):
    f = tmp_path / "test.ttl"
    f.write_text(SAMPLE_TURTLE)
    return str(f)


# ── PitfallsService: taxonomy ─────────────────────────────────────────────────

class TestPitfallsServiceTaxonomy:
    def test_get_taxonomy_returns_list(self):
        from back.core.external.pitfalls.PitfallsService import PitfallsService

        svc = PitfallsService()
        # Taxonomy is loaded from the vendored runner — no ML deps needed
        taxonomy = svc.get_taxonomy()
        assert isinstance(taxonomy, list)
        assert len(taxonomy) == 19

    def test_taxonomy_has_required_keys(self):
        from back.core.external.pitfalls.PitfallsService import PitfallsService

        svc = PitfallsService()
        taxonomy = svc.get_taxonomy()
        for entry in taxonomy:
            assert "pitfall_id" in entry
            assert "title" in entry
            assert "category" in entry

    def test_taxonomy_pitfall_ids(self):
        from back.core.external.pitfalls.PitfallsService import PitfallsService

        svc = PitfallsService()
        ids = {e["pitfall_id"] for e in svc.get_taxonomy()}
        expected = {f"P{major}.{minor}" for major in range(1, 5) for minor in range(1, 8)}
        # Keep only those that exist in the 19-pitfall spec
        for pid in ["P1.1", "P1.2", "P1.3", "P2.1", "P2.2", "P2.3", "P2.4", "P2.5", "P2.6",
                    "P3.1", "P3.2", "P3.3", "P4.1", "P4.2", "P4.3", "P4.4", "P4.5", "P4.6", "P4.7"]:
            assert pid in ids

    def test_get_available_patterns(self):
        from back.core.external.pitfalls.PitfallsService import PitfallsService

        patterns = PitfallsService().get_available_patterns()
        assert isinstance(patterns, list)
        assert len(patterns) == 19
        assert patterns == sorted(patterns, key=lambda p: tuple(int(x) for x in p[1:].split(".")))


# ── PitfallsService.run_analysis — fast patterns only (no ML) ─────────────────

@_PITFALLS_DEPS
class TestPitfallsServiceRunAnalysis:
    FAST_PATTERNS = ["P1.1", "P1.2", "P2.2", "P2.4", "P3.1", "P3.2", "P3.3", "P4.7"]

    def test_run_analysis_returns_required_keys(self, sample_graph):
        from back.core.external.pitfalls.PitfallsService import PitfallsService

        svc = PitfallsService()
        result = svc.run_analysis(sample_graph, patterns=self.FAST_PATTERNS)

        assert "metadata" in result
        assert "selected_pitfalls" in result
        assert "results" in result
        assert "grouped_results" in result

    def test_metadata_reflects_graph(self, sample_graph):
        from back.core.external.pitfalls.PitfallsService import PitfallsService

        result = PitfallsService().run_analysis(sample_graph, patterns=["P1.1"])
        meta = result["metadata"]
        assert meta["classes"] >= 4
        assert meta["object_properties"] >= 1
        assert meta["datatype_properties"] >= 1

    def test_selected_pitfalls_match_request(self, sample_graph):
        from back.core.external.pitfalls.PitfallsService import PitfallsService

        patterns = ["P1.1", "P2.2"]
        result = PitfallsService().run_analysis(sample_graph, patterns=patterns)
        assert set(result["selected_pitfalls"]) == set(patterns)

    def test_results_per_pattern_has_count(self, sample_graph):
        from back.core.external.pitfalls.PitfallsService import PitfallsService

        result = PitfallsService().run_analysis(sample_graph, patterns=["P2.2"])
        r = result["results"]["P2.2"]
        assert "count" in r
        assert isinstance(r["count"], int)

    def test_grouped_results_categories(self, sample_graph):
        from back.core.external.pitfalls.PitfallsService import PitfallsService

        result = PitfallsService().run_analysis(sample_graph, patterns=["P1.1", "P2.2", "P3.1"])
        grouped = result["grouped_results"]
        assert "Logical Issues" in grouped
        assert "Structural Issues" in grouped
        assert "Redundancy / Naming Issues" in grouped

    def test_p1_1_detects_disjoint_child(self, disjoint_graph):
        """P1.1 must flag the Child-disjointWith-Parent pattern."""
        from back.core.external.pitfalls.PitfallsService import PitfallsService

        result = PitfallsService().run_analysis(disjoint_graph, patterns=["P1.1"])
        r = result["results"]["P1.1"]
        assert r["count"] >= 1
        labels = [i["child_label"] for i in r["items"]]
        assert any("Child" in lbl for lbl in labels)

    def test_p1_2_detects_redundant_subclass(self):
        """P1.2 fires when a class is subclass of both parent and grandparent."""
        redundant_ttl = """
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix : <http://t.org/r#> .
<http://t.org/r> a owl:Ontology .
:A a owl:Class .
:B a owl:Class ; rdfs:subClassOf :A .
:C a owl:Class ; rdfs:subClassOf :B ; rdfs:subClassOf :A .
"""
        g = Graph()
        g.parse(data=redundant_ttl, format="turtle")

        from back.core.external.pitfalls.PitfallsService import PitfallsService

        result = PitfallsService().run_analysis(g, patterns=["P1.2"])
        r = result["results"]["P1.2"]
        assert r["count"] >= 1

    def test_run_all_keyword(self, sample_graph):
        """Passing ['all'] runs all 19 pitfalls (with ML stubs)."""
        from back.core.external.pitfalls.PitfallsService import PitfallsService

        # Stub SentenceTransformer to avoid downloading model weights in CI
        import numpy as np
        fake_encode = MagicMock(return_value=np.zeros((5, 384)))

        with patch("back.core.external.pitfalls.runner.SentenceTransformer") as MockST:
            instance = MockST.return_value
            instance.encode = fake_encode

            result = PitfallsService().run_analysis(sample_graph, patterns=["all"])

        assert len(result["selected_pitfalls"]) == 19
        for pid in result["selected_pitfalls"]:
            assert pid in result["results"]

    def test_invalid_pattern_raises(self, sample_graph):
        from back.core.external.pitfalls.PitfallsService import PitfallsService

        with pytest.raises(ValueError, match="Unknown pitfall"):
            PitfallsService().run_analysis(sample_graph, patterns=["P9.9"])


# ── _group_results_by_category helper ────────────────────────────────────────

@_PITFALLS_DEPS
class TestGroupResultsByCategory:
    def test_grouping_preserves_all_pitfalls(self):
        from back.core.external.pitfalls.PitfallsService import _group_results_by_category
        from back.core.external.pitfalls.runner import PITFALL_TAXONOMY

        selected = ["P1.1", "P2.2", "P3.1"]
        results = {pid: {"count": 0, "items": []} for pid in selected}
        grouped = _group_results_by_category(selected, results, list(PITFALL_TAXONOMY))

        all_pitfalls_in_groups = [
            pid
            for cat_dict in grouped.values()
            for pid in cat_dict.keys()
        ]
        assert set(all_pitfalls_in_groups) == set(selected)

    def test_grouping_attaches_title(self):
        from back.core.external.pitfalls.PitfallsService import _group_results_by_category
        from back.core.external.pitfalls.runner import PITFALL_TAXONOMY

        selected = ["P1.1"]
        results = {"P1.1": {"count": 1, "items": []}}
        grouped = _group_results_by_category(selected, results, list(PITFALL_TAXONOMY))

        assert grouped["Logical Issues"]["P1.1"]["title"] == "Parent disjoint with children"


# ── OntologyPatternToolkit utils ──────────────────────────────────────────────

class TestPitfallUtils:
    def test_camel_case_split(self):
        from back.core.external.pitfalls.utils import camel_case_split

        assert camel_case_split("CamelCase") == ["Camel", "Case"]
        assert camel_case_split("hasOwner") == ["has", "Owner"]
        assert camel_case_split("") == []

    def test_normalize_name(self):
        from back.core.external.pitfalls.utils import normalize_name

        assert normalize_name("hasOwner") == "hasowner"
        assert normalize_name("Has Owner!") == "hasowner"

    def test_extract_label_fragment(self):
        from back.core.external.pitfalls.utils import extract_label

        assert extract_label("http://example.org/onto#MyClass") == "MyClass"

    def test_extract_label_clean(self):
        from back.core.external.pitfalls.utils import extract_label

        assert extract_label("http://example.org/onto#MyClass", clean=True) == "My Class"

    def test_sort_pattern_ids(self):
        from back.core.external.pitfalls.utils import sort_pattern_ids

        ids = ["P4.1", "P1.3", "P2.2", "P1.1"]
        assert sort_pattern_ids(ids) == ["P1.1", "P1.3", "P2.2", "P4.1"]

    def test_parse_pattern_selection_all(self):
        from back.core.external.pitfalls.utils import parse_pattern_selection
        from back.core.external.pitfalls.runner import OntologyPatternToolkit

        available = OntologyPatternToolkit.available_patterns()
        result = parse_pattern_selection(
            ["all"], available, normalizer=OntologyPatternToolkit.normalize_pitfall_id
        )
        assert result == available

    def test_parse_pattern_selection_subset(self):
        from back.core.external.pitfalls.utils import parse_pattern_selection
        from back.core.external.pitfalls.runner import OntologyPatternToolkit

        available = OntologyPatternToolkit.available_patterns()
        result = parse_pattern_selection(
            ["P1.1", "P2.2"], available, normalizer=OntologyPatternToolkit.normalize_pitfall_id
        )
        assert result == ["P1.1", "P2.2"]
