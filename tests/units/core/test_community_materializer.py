"""CommunityMaterializer: partition SQL (Spark) and bounded reads on both store kinds."""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List

import pytest

from back.core.graph_analysis.community.CommunityMaterializer import (
    ASSIGNMENTS_MAX,
    CommunityMaterializer,
)
from back.core.graph_analysis.community.CommunityVocabulary import CommunityVocabulary
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

    def sql_flavor(self) -> str:
        return "postgres"

    def execute_query(self, sql: str) -> List[Dict[str, Any]]:
        self.queries.append(sql)
        return [dict(r) for r in self.conn.execute(sql).fetchall()]

    def insert_triples(self, name: str, triples: List[Dict[str, str]]) -> int:
        self.conn.executemany(
            f"INSERT INTO {name} VALUES (?, ?, ?)",
            [(t["subject"], t["predicate"], t["object"]) for t in triples],
        )
        return len(triples)


    def all(self) -> List[Dict[str, str]]:
        return [dict(r) for r in self.conn.execute("SELECT * FROM g").fetchall()]


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



@pytest.fixture(params=["sql", "scan"])
def store(request):
    return _SqlStore() if request.param == "sql" else _ScanStore()


def _seed(store) -> None:
    """Write the partition of ``_Communities`` + ``STATS`` the way it is materialized."""
    triples = []
    for batch in _Communities().iter_memberships(0):
        for uri, cid in batch:
            triples.append(
                {
                    "subject": uri,
                    "predicate": CommunityVocabulary.in_cluster(BASE),
                    "object": CommunityVocabulary.cluster(BASE, cid),
                }
            )
    for batch in _Communities().iter_sizes(0):
        for cid, size in batch:
            uri = CommunityVocabulary.cluster(BASE, cid)
            for pred, obj in (
                (RDF_TYPE, CommunityVocabulary.cluster_class(BASE)),
                (RDFS_LABEL, f"Cluster #{cid}"),
                (CommunityVocabulary.from_algorithm(BASE), STATS["algorithm"]),
                (CommunityVocabulary.cluster_size(BASE), str(size)),
                (CommunityVocabulary.modularity(BASE), str(STATS["modularity"])),
            ):
                triples.append({"subject": uri, "predicate": pred, "object": obj})
    store.insert_triples("g", triples)


def test_load_current_reads_the_materialized_partition(store):
    _seed(store)
    mat = CommunityMaterializer(store, "g", BASE)
    current = mat.load_current()
    assert [c["id"] for c in current["clusters"]] == [0, 1]
    assert [c["size"] for c in current["clusters"]] == [2, 1]
    assert "members" not in current["clusters"][0]
    assert current["stats"]["cluster_count"] == 2
    assert current["stats"]["node_count"] == 3
    assert current["stats"]["algorithm"] == "label_propagation"
    assert current["stats"]["modularity"] == pytest.approx(0.42)


def test_assignments_and_members(store):
    _seed(store)
    mat = CommunityMaterializer(store, "g", BASE)
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


def test_an_empty_graph_has_no_partition(store):
    assert CommunityMaterializer(store, "g", BASE).load_current() == {
        "clusters": [],
        "stats": None,
    }


def test_partition_sql_reads_the_job_tables():
    mat = CommunityMaterializer(None, "g", BASE)
    out = _Communities.output_table
    membership = mat.membership_select_sql(out)
    metadata = mat.metadata_select_sql(out, "label_propagation", "0.42")
    assert f"FROM {out}" in membership
    assert CommunityVocabulary.in_cluster(BASE) in membership
    assert CommunityVocabulary.cluster_prefix(BASE) + "c-" in membership
    assert metadata.count(f"FROM {out}_sizes") == 5
    for pred in (RDF_TYPE, RDFS_LABEL, CommunityVocabulary.cluster_size(BASE)):
        assert pred in metadata


def test_partition_sql_is_spark_escaped_whatever_the_store():
    mat = CommunityMaterializer(_SqlStore(), "g", "http://ex/o\\'x")
    sql = mat.membership_select_sql("c.s.t")
    # Spark doubles the backslash before doubling the quote; the Postgres
    # store's own dialect would not.
    assert "o\\\\''x" in sql


def test_sql_reads_are_bounded():
    store = _SqlStore()
    _seed(store)
    mat = CommunityMaterializer(store, "g", BASE)
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
