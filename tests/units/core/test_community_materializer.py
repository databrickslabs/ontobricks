"""CommunityMaterializer: set-based writes, bounded reads, both store kinds."""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List

import pytest

from back.core.graph_analysis.CommunityMaterializer import (
    ASSIGNMENTS_MAX,
    CommunityMaterializer,
)
from back.core.graph_analysis.CommunityVocabulary import CommunityVocabulary
from back.core.graphdb.constants import RDFS_LABEL, RDF_TYPE

pytestmark = pytest.mark.unit

BASE = "http://example.org/onto"
STATS = {"algorithm": "label_propagation", "modularity": 0.42, "node_count": 3, "cluster_count": 2}


class _Communities:
    """Stands in for JobCommunities: two clusters over three nodes."""

    output_table = "cat.sch.graph_communities_x"

    def iter_memberships(self, _batch):
        yield [("http://ex/a", 0), ("http://ex/b", 0)]
        yield [("http://ex/c", 1)]

    def iter_sizes(self, _batch):
        yield [(0, 2), (1, 1)]


class _SqlStore:
    """SQLite-backed store speaking the SQL dialect the materializer uses."""

    query_dialect = "sql"
    supports_adjacency = False

    def __init__(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("CREATE TABLE g (subject TEXT, predicate TEXT, object TEXT)")
        self.queries: List[str] = []

    def _sql_relation(self, name: str) -> str:
        return name

    def execute_query(self, sql: str) -> List[Dict[str, Any]]:
        self.queries.append(sql)
        return [dict(r) for r in self.conn.execute(sql).fetchall()]

    def insert_triples(self, name: str, triples: List[Dict[str, str]]) -> int:
        self.conn.executemany(
            f"INSERT INTO {name} VALUES (?, ?, ?)",
            [(t["subject"], t["predicate"], t["object"]) for t in triples],
        )
        return len(triples)

    def delete_community_triples(self, name: str, prefix: str, predicate: str) -> int:
        cur = self.conn.execute(
            f"DELETE FROM {name} WHERE subject LIKE ? OR (predicate = ? AND object LIKE ?)",
            (prefix + "%", predicate, prefix + "%"),
        )
        return cur.rowcount

    def all(self) -> List[Dict[str, str]]:
        return [dict(r) for r in self.conn.execute("SELECT * FROM g").fetchall()]


class _DeltaStore(_SqlStore):
    def __init__(self) -> None:
        super().__init__()
        self.selects: List[str] = []

    def insert_triples_from_select(self, name: str, select_sql: str) -> None:
        self.selects.append(select_sql)


class _ScanStore:
    query_dialect = "cypher"
    supports_adjacency = False

    def __init__(self) -> None:
        self.rows: List[Dict[str, str]] = []

    def query_triples(self, _name):
        return list(self.rows)

    def insert_triples(self, _name, triples):
        self.rows.extend(triples)
        return len(triples)

    def delete_community_triples(self, _name, prefix, predicate):
        before = len(self.rows)
        self.rows = [
            t for t in self.rows
            if not (t["subject"].startswith(prefix) or t["predicate"] == predicate)
        ]
        return before - len(self.rows)


@pytest.fixture(params=["sql", "scan"])
def store(request):
    return _SqlStore() if request.param == "sql" else _ScanStore()


def test_batched_materialize_then_load(store):
    mat = CommunityMaterializer(store, "g", BASE)
    inserted = mat.materialize(_Communities(), STATS)
    assert inserted == 3 + 2 * 5
    current = mat.load_current()
    assert [c["id"] for c in current["clusters"]] == [0, 1]
    assert [c["size"] for c in current["clusters"]] == [2, 1]
    assert "members" not in current["clusters"][0]
    assert current["stats"]["cluster_count"] == 2
    assert current["stats"]["node_count"] == 3
    assert current["stats"]["algorithm"] == "label_propagation"
    assert current["stats"]["modularity"] == pytest.approx(0.42)


def test_assignments_and_members(store):
    mat = CommunityMaterializer(store, "g", BASE)
    mat.materialize(_Communities(), STATS)
    assert mat.assignments(["http://ex/a", "http://ex/c", "http://ex/zzz"]) == {
        "http://ex/a": 0,
        "http://ex/c": 1,
    }
    page = mat.members(0, offset=0, limit=1)
    assert [m["uri"] for m in page["members"]] == ["http://ex/a"]
    assert page["has_more"] is True
    page2 = mat.members(0, offset=1, limit=1)
    assert [m["uri"] for m in page2["members"]] == ["http://ex/b"]
    assert page2["has_more"] is False


def test_rematerialize_replaces_and_clear_empties(store):
    mat = CommunityMaterializer(store, "g", BASE)
    mat.materialize(_Communities(), STATS)
    mat.materialize(_Communities(), STATS)
    assert mat.load_current()["stats"]["node_count"] == 3
    mat.clear()
    assert mat.load_current() == {"clusters": [], "stats": None}


def test_failed_materialize_leaves_no_partition():
    class _Boom(_Communities):
        def iter_sizes(self, _batch):
            raise RuntimeError("warehouse went away")
            yield  # pragma: no cover

    store = _SqlStore()
    mat = CommunityMaterializer(store, "g", BASE)
    with pytest.raises(RuntimeError):
        mat.materialize(_Boom(), STATS)
    assert store.all() == []


def test_delta_store_writes_with_insert_select():
    store = _DeltaStore()
    mat = CommunityMaterializer(store, "g", BASE)
    inserted = mat.materialize(_Communities(), STATS)
    assert inserted == 3 + 2 * 5
    membership, metadata = store.selects
    out = _Communities.output_table
    assert f"FROM {out}" in membership
    assert CommunityVocabulary.in_cluster(BASE) in membership
    assert CommunityVocabulary.cluster_prefix(BASE) + "c-" in membership
    assert metadata.count(f"FROM {out}_sizes") == 5
    for pred in (RDF_TYPE, RDFS_LABEL, CommunityVocabulary.cluster_size(BASE)):
        assert pred in metadata
    assert store.all() == []


def test_sql_reads_are_bounded():
    store = _SqlStore()
    mat = CommunityMaterializer(store, "g", BASE)
    mat.materialize(_Communities(), STATS)
    store.queries.clear()
    mat.load_current(limit=10_000)
    assert any("LIMIT 200" in q for q in store.queries)
    in_cluster = CommunityVocabulary.in_cluster(BASE)
    # No read pulls every membership triple.
    assert not any(
        f"predicate = '{in_cluster}'" in q and "LIMIT" not in q and " IN (" not in q
        for q in store.queries
    )


def test_assignments_are_capped():
    store = _SqlStore()
    mat = CommunityMaterializer(store, "g", BASE)
    mat.assignments([f"http://ex/n{i}" for i in range(ASSIGNMENTS_MAX + 50)])
    assert store.queries[-1].count("'http://ex/n") == ASSIGNMENTS_MAX
