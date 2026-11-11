"""CommunityMaterializer replace/load/clear against an in-memory store."""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from back.core.graph_analysis.CommunityMaterializer import CommunityMaterializer
from back.core.graph_analysis.CommunityVocabulary import CommunityVocabulary

pytestmark = pytest.mark.unit

BASE = "http://example.org/onto"


class _MemStore:
    query_dialect = "cypher"
    supports_adjacency = False

    def __init__(self) -> None:
        self.rows: List[Dict[str, str]] = []

    def query_triples(self, _name: str) -> List[Dict[str, str]]:
        return list(self.rows)

    def insert_triples(self, _name: str, triples: List[Dict[str, str]]) -> int:
        self.rows.extend(triples)
        return len(triples)

    def delete_community_triples(
        self, _name: str, prefix: str, predicate: str
    ) -> int:
        before = len(self.rows)
        self.rows = [
            t
            for t in self.rows
            if not (
                str(t.get("subject", "")).startswith(prefix)
                or (
                    t.get("predicate") == predicate
                    and str(t.get("object", "")).startswith(prefix)
                )
            )
        ]
        return before - len(self.rows)


def _mat(store: Any | None = None) -> tuple[CommunityMaterializer, _MemStore]:
    mem = store or _MemStore()
    return CommunityMaterializer(mem, "g", BASE), mem


def test_materialize_then_load_round_trip():
    mat, _store = _mat()
    clusters = [
        {"id": 0, "members": ["http://ex/a", "http://ex/b"], "size": 2},
        {"id": 1, "members": ["http://ex/c"], "size": 1},
    ]
    inserted = mat.materialize(clusters, {"algorithm": "louvain", "resolution": 1.2})
    assert inserted > 0
    out = mat.load_current()
    assert out["stats"]["algorithm"] == "louvain"
    assert out["stats"]["cluster_count"] == 2
    assert {c["id"] for c in out["clusters"]} == {0, 1}
    members_0 = next(c for c in out["clusters"] if c["id"] == 0)["members"]
    assert members_0 == ["http://ex/a", "http://ex/b"]
    assert CommunityVocabulary.in_cluster(BASE) in {
        t["predicate"] for t in _store.rows
    }


def test_rematerialize_replaces_previous_partition():
    mat, store = _mat()
    mat.materialize([{"id": 0, "members": ["http://ex/old"], "size": 1}])
    mat.materialize([{"id": 0, "members": ["http://ex/new"], "size": 1}])
    subjects = [t["subject"] for t in store.rows]
    assert "http://ex/old" not in subjects
    assert "http://ex/new" in subjects


def test_clear_empties_partition():
    mat, _store = _mat()
    mat.materialize([{"id": 0, "members": ["http://ex/a"], "size": 1}])
    mat.clear()
    out = mat.load_current()
    assert out["clusters"] == []
    assert out["stats"] is None


def test_empty_load_when_store_has_unrelated_triples():
    store = _MemStore()
    store.rows.append(
        {"subject": "http://ex/a", "predicate": "http://ex/p", "object": "v"}
    )
    mat, _ = _mat(store)
    out = mat.load_current()
    assert out["clusters"] == []
