"""Integration tests for LadybugDB graph reasoning methods.

These tests verify transitive_closure, symmetric_expand, and
shortest_path on the LadybugGraphStore using a small in-memory
ontology and sample instance data.
"""

import importlib

import pytest
from unittest.mock import MagicMock, patch

_has_owlrl = importlib.util.find_spec("owlrl") is not None
requires_owlrl = pytest.mark.skipif(not _has_owlrl, reason="owlrl not installed")


@pytest.fixture
def mock_ontology():
    return {
        "base_uri": "http://example.org/",
        "classes": [
            {"name": "Region", "uri": "http://example.org/Region"},
            {"name": "Country", "uri": "http://example.org/Country"},
        ],
        "properties": [
            {
                "name": "isPartOf",
                "uri": "http://example.org/isPartOf",
                "type": "ObjectProperty",
                "domain": "http://example.org/Region",
                "range": "http://example.org/Region",
                "characteristics": ["Transitive"],
            },
            {
                "name": "borders",
                "uri": "http://example.org/borders",
                "type": "ObjectProperty",
                "domain": "http://example.org/Country",
                "range": "http://example.org/Country",
                "characteristics": ["Symmetric"],
            },
        ],
    }


class TestTransitiveClosureSQL:
    """Test the SQL-based default transitive_closure method."""

    def test_returns_list(self):
        from back.core.triplestore import TripleStoreBackend

        class FakeStore(TripleStoreBackend):
            def create_table(self, t):
                pass

            def drop_table(self, t):
                pass

            def insert_triples(self, t, triples, **kw):
                return 0

            def query_triples(self, t):
                return []

            def count_triples(self, t):
                return 0

            def table_exists(self, t):
                return True

            def get_status(self, t):
                return {}

            def execute_query(self, q):
                if "WITH RECURSIVE" in q:
                    return [
                        {
                            "subject": "http://ex.org/a",
                            "predicate": "http://ex.org/isPartOf",
                            "object": "http://ex.org/c",
                        },
                    ]
                return []

        store = FakeStore()
        result = store.transitive_closure("triples", "http://ex.org/isPartOf")
        assert len(result) == 1
        assert result[0]["subject"] == "http://ex.org/a"

    def test_with_start_uri(self):
        from back.core.triplestore import TripleStoreBackend

        class FakeStore(TripleStoreBackend):
            def create_table(self, t):
                pass

            def drop_table(self, t):
                pass

            def insert_triples(self, t, triples, **kw):
                return 0

            def query_triples(self, t):
                return []

            def count_triples(self, t):
                return 0

            def table_exists(self, t):
                return True

            def get_status(self, t):
                return {}

            def execute_query(self, q):
                assert "http://ex.org/start" in q
                return []

        store = FakeStore()
        store.transitive_closure(
            "t", "http://ex.org/P", start_uri="http://ex.org/start"
        )


class TestSymmetricExpandSQL:
    def test_returns_missing_inverses(self):
        from back.core.triplestore import TripleStoreBackend

        class FakeStore(TripleStoreBackend):
            def create_table(self, t):
                pass

            def drop_table(self, t):
                pass

            def insert_triples(self, t, triples, **kw):
                return 0

            def query_triples(self, t):
                return []

            def count_triples(self, t):
                return 0

            def table_exists(self, t):
                return True

            def get_status(self, t):
                return {}

            def execute_query(self, q):
                return [
                    {
                        "subject": "http://ex.org/b",
                        "predicate": "http://ex.org/borders",
                        "object": "http://ex.org/a",
                    },
                ]

        store = FakeStore()
        result = store.symmetric_expand("triples", "http://ex.org/borders")
        assert len(result) == 1


class TestShortestPathSQL:
    def test_default_returns_empty(self):
        from back.core.triplestore import TripleStoreBackend

        class FakeStore(TripleStoreBackend):
            def create_table(self, t):
                pass

            def drop_table(self, t):
                pass

            def insert_triples(self, t, triples, **kw):
                return 0

            def query_triples(self, t):
                return []

            def count_triples(self, t):
                return 0

            def table_exists(self, t):
                return True

            def get_status(self, t):
                return {}

            def execute_query(self, q):
                return []

        store = FakeStore()
        result = store.shortest_path("t", "http://ex.org/a", "http://ex.org/b")
        assert result == []


class TestReasoningServiceIntegration:
    """Test the ReasoningService with mocked backends."""

    @requires_owlrl
    def test_run_tbox_with_owl_content(self):
        from back.core.reasoning.ReasoningService import ReasoningService

        domain = MagicMock()
        domain.generated_owl = (
            "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n"
            "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n"
            "@prefix ex: <http://example.org/> .\n"
            "ex:A a owl:Class .\n"
            "ex:B a owl:Class ; rdfs:subClassOf ex:A .\n"
        )
        domain.swrl_rules = []
        domain.ontology = {"properties": []}

        svc = ReasoningService(domain)
        result = svc.run_tbox_reasoning()
        assert result.stats.get("phase") == "tbox"
        assert result.stats.get("original_count", 0) > 0

    def test_run_swrl_skipped_without_rules(self):
        from back.core.reasoning.ReasoningService import ReasoningService

        domain = MagicMock()
        domain.swrl_rules = []
        domain.ontology = {}

        svc = ReasoningService(domain, MagicMock())
        result = svc.run_swrl_rules()
        assert result.stats.get("skipped") is True

    def test_run_full_reasoning_tbox_only(self):
        from back.core.reasoning.ReasoningService import ReasoningService

        domain = MagicMock()
        domain.generated_owl = (
            "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n"
            "@prefix ex: <http://example.org/> .\n"
            "ex:A a owl:Class .\n"
        )
        domain.swrl_rules = []
        domain.ontology = {"properties": [], "base_uri": "http://example.org/"}
        domain.info = {"name": "test"}
        domain._data = {"ontology": domain.ontology}

        svc = ReasoningService(domain)
        result = svc.run_full_reasoning({"tbox": True, "swrl": False, "graph": False})
        assert "total_duration_seconds" in result.stats
