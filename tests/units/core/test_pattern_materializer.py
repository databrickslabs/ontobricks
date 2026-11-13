"""PatternMaterializer: named saves, bounded reads, clear-one semantics."""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List

import pytest

from back.core.graph_analysis.PatternMaterializer import (
    PatternMaterializer,
    slug_saved_id,
)
from back.core.graph_analysis.PatternVocabulary import PatternVocabulary
from back.core.graphdb.constants import RDFS_LABEL, RDF_TYPE

pytestmark = pytest.mark.unit

BASE = "http://example.org/onto"
PID = "abcdef0123456789"


class _Patterns:
    output_table = "cat.sch.graph_patterns_x"

    def shape(self, pattern_id):
        assert pattern_id == PID
        return {"support": 2, "pattern_id": pattern_id}

    def members(self, pattern_id, offset=0, limit=100):
        return {"total": 2, "members": []}

    def iter_memberships(self, pattern_id, batch_size=50000):
        yield [
            {"uri": "http://ex/Alice", "pattern_id": pattern_id},
            {"uri": "http://ex/Bob", "pattern_id": pattern_id},
        ]


class _SqlStore:
    query_dialect = "sql"
    supports_adjacency = False

    def __init__(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("CREATE TABLE g (subject TEXT, predicate TEXT, object TEXT)")

    def _sql_relation(self, name: str) -> str:
        return name

    def execute_query(self, sql: str) -> List[Dict[str, Any]]:
        return [dict(r) for r in self.conn.execute(sql).fetchall()]

    def insert_triples(self, name: str, triples: List[Dict[str, str]]) -> int:
        self.conn.executemany(
            f"INSERT INTO {name} VALUES (?, ?, ?)",
            [(t["subject"], t["predicate"], t["object"]) for t in triples],
        )
        return len(triples)

    def delete_cohort_triples(self, name: str, prefix: str, predicate: str) -> int:
        cur = self.conn.execute(
            f"DELETE FROM {name} WHERE subject = ? OR (predicate = ? AND object = ?)",
            (prefix, predicate, prefix),
        )
        return cur.rowcount

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


def test_slug_saved_id():
    assert slug_saved_id("FR buyers!", "ab12cd34ffff") == "fr-buyers-ab12cd34"


def test_materialize_writes_metadata_and_memberships():
    store = _SqlStore()
    mat = PatternMaterializer(store, "g", BASE)
    result = mat.materialize(_Patterns(), PID, "FR buyers")
    assert result["size"] == 2
    assert result["saved_id"].startswith("fr-buyers-")
    uri = result["uri"]
    triples = store.all()
    assert any(t["subject"] == uri and t["predicate"] == RDF_TYPE for t in triples)
    assert any(t["predicate"] == RDFS_LABEL and t["object"] == "FR buyers" for t in triples)
    members = [
        t["subject"]
        for t in triples
        if t["predicate"] == PatternVocabulary.in_pattern(BASE) and t["object"] == uri
    ]
    assert set(members) == {"http://ex/Alice", "http://ex/Bob"}
    saved = mat.list_saved()
    assert len(saved) == 1
    assert saved[0]["size"] == 2


def test_second_materialize_keeps_the_first():
    store = _SqlStore()
    mat = PatternMaterializer(store, "g", BASE)
    first = mat.materialize(_Patterns(), PID, "FR buyers")
    second = mat.materialize(_Patterns(), PID, "FR buyers")
    assert first["saved_id"] != second["saved_id"]
    assert len(mat.list_saved()) == 2


def test_delete_removes_only_one_pattern():
    store = _SqlStore()
    mat = PatternMaterializer(store, "g", BASE)
    a = mat.materialize(_Patterns(), PID, "A")
    b = mat.materialize(_Patterns(), PID, "B")
    mat.delete(a["saved_id"])
    remaining = {s["id"] for s in mat.list_saved()}
    assert remaining == {b["saved_id"]}
    page = mat.members(b["saved_id"], offset=0, limit=10)
    assert page["total"] == 2
    assert len(page["members"]) == 2


def test_scan_store_materialize_and_list():
    store = _ScanStore()
    mat = PatternMaterializer(store, "g", BASE)
    result = mat.materialize(_Patterns(), PID, "Scan")
    assert len(mat.list_saved()) == 1
    page = mat.members(result["saved_id"], offset=0, limit=1)
    assert page["total"] == 2
    assert page["has_more"] is True
