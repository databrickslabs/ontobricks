"""SPARQL over a ready simulation sandbox snapshot."""

import importlib
from unittest.mock import MagicMock

import pytest

from back.core.errors import ValidationError
from back.objects.digitaltwin.query.TwinSandboxSparql import TwinSandboxSparql

pytestmark = pytest.mark.unit

_MOD = importlib.import_module("back.objects.digitaltwin.query.TwinSandboxSparql")


def test_execute_runs_select_on_loaded_triples(monkeypatch):
    store = MagicMock()
    store.paginated_count.return_value = 1
    store.paginated_triples.return_value = [
        {
            "subject": "http://ex/p1",
            "predicate": "http://ex/score",
            "object": "420",
        }
    ]
    monkeypatch.setattr(_MOD, "get_graphdb", lambda *a, **k: store)
    sim = MagicMock()
    sim.sandbox_relation.return_value = "graph__sim_s1"
    monkeypatch.setattr(_MOD, "Simulation", lambda *a, **k: sim)
    out = TwinSandboxSparql(MagicMock(), MagicMock()).execute(
        "SELECT ?s ?p ?o WHERE { ?s ?p ?o }",
        10,
        "s1abc",
    )
    assert out["success"] is True
    assert out["engine"] == "sandbox"
    assert out["count"] == 1
    assert out["results"][0]["s"].endswith("/p1")


def test_execute_refuses_huge_sandbox(monkeypatch):
    store = MagicMock()
    store.paginated_count.return_value = TwinSandboxSparql.MAX_TRIPLES + 1
    monkeypatch.setattr(_MOD, "get_graphdb", lambda *a, **k: store)
    sim = MagicMock()
    sim.sandbox_relation.return_value = "graph__sim_s1"
    monkeypatch.setattr(_MOD, "Simulation", lambda *a, **k: sim)
    with pytest.raises(ValidationError, match="too large"):
        TwinSandboxSparql(MagicMock(), MagicMock()).execute(
            "SELECT ?s WHERE { ?s ?p ?o }", None, "s1abc"
        )
