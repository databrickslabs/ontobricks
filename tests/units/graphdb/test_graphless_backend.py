"""Contracts for the ontology-only ("No Backend") graph backend.

A domain may declare ``graph_backend == "none"``: no graph is ever built and
the factory must never construct a store for it. The value has to be a
first-class, valid backend (so it round-trips through normalization) while the
legacy "invalid -> lakebase" fallback keeps existing domains working.
"""

from types import SimpleNamespace

import pytest

from back.core.graphdb.GraphDBFactory import (
    GRAPH_BACKENDS,
    GRAPHLESS_BACKEND,
    GraphDBFactory,
    is_graphless_backend,
    normalize_graph_backend,
)

pytestmark = pytest.mark.unit


def test_none_is_a_valid_backend_value():
    assert GRAPHLESS_BACKEND == "none"
    assert "none" in GRAPH_BACKENDS
    assert normalize_graph_backend("none") == "none"
    assert normalize_graph_backend("NONE") == "none"


def test_invalid_values_still_default_to_lakebase():
    """Existing domains without a stored backend keep working."""
    assert normalize_graph_backend("") == "lakebase"
    assert normalize_graph_backend("bogus") == "lakebase"
    assert normalize_graph_backend(None) == "lakebase"


def test_is_graphless_backend_detects_only_none():
    assert is_graphless_backend("none") is True
    assert is_graphless_backend("NONE") is True
    for backend in ("lakebase", "databricks", "neo4j", "", "bogus"):
        assert is_graphless_backend(backend) is False


def test_auto_create_returns_none_for_a_graphless_domain():
    factory = GraphDBFactory()
    domain = SimpleNamespace(info={"name": "Onto", "graph_backend": "none"})
    assert factory.create(domain) is None
