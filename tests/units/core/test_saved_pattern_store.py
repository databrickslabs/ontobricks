"""SavedPatternStore: Detect catalog snapshots in a Delta table, never in the graph."""

from __future__ import annotations

import json
import sqlite3
import sys
from types import SimpleNamespace

import pytest

from back.core.errors import ValidationError
from back.core.graph_analysis.pattern.JobPatterns import (
    pattern_output_table,
    saved_pattern_table,
)
from back.core.graph_analysis.pattern.SavedPatternStore import (
    SavedPatternStore,
    slug_saved_id,
)

pytestmark = pytest.mark.unit

# The package re-exports the class under the module's name, so reach the module itself.
_STORE_MODULE = sys.modules[SavedPatternStore.__module__]

OUT = "out_tbl"
SAVED = "saved_tbl"
PID = "abcdef0123456789"
PID2 = "fedcba9876543210"
CUSTOMER = "http://ex/Customer"
CLAIM = "http://ex/Claim"


class _Warehouse:
    """sqlite stand-in for the Build warehouse (Delta-only spellings shimmed)."""

    def __init__(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(
            "CREATE TABLE out_tbl (anchor_uri TEXT, pattern_id TEXT, label TEXT, type_uri TEXT)"
        )
        self.conn.executemany(
            "INSERT INTO out_tbl VALUES (?, ?, ?, ?)",
            [
                ("http://ex/Alice", PID, "Alice", CUSTOMER),
                ("http://ex/Bob", PID, "Bob", CUSTOMER),
                ("http://ex/Carol", PID2, "Carol", CUSTOMER),
                ("http://ex/Other", "zzz", "Other", CUSTOMER),
            ],
        )
        self.conn.execute(
            "CREATE TABLE out_tbl_shapes "
            "(pattern_id TEXT, hop_count INT, support INT, seed_label TEXT, "
            "shape_json TEXT, refined INT, near_duplicate_count INT)"
        )
        self.conn.executemany(
            "INSERT INTO out_tbl_shapes VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                (PID, 2, 2, "Customer · hasClaim", "[]", 0, 1),
                (PID2, 1, 1, "Customer · hasOrder", "[]", 0, 1),
            ],
        )
        self.conn.execute(
            "CREATE TABLE out_tbl_summary "
            "(pattern_count INT, hop_count INT, anchor_type TEXT, bins INT, "
            "anchor_count INT, population_count INT)"
        )
        self.conn.execute(
            "INSERT INTO out_tbl_summary VALUES (?, ?, ?, ?, ?, ?)",
            (2, 3, CUSTOMER, 8, 4, 10),
        )
        self.conn.execute(
            "CREATE TABLE out_tbl_shape_edges "
            "(pattern_id TEXT, hop INT, parent_via TEXT, parent_type TEXT, via TEXT, "
            "child_type TEXT, member_count INT, edge_total INT)"
        )
        self.conn.executemany(
            "INSERT INTO out_tbl_shape_edges VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (PID, 1, "", CUSTOMER, "http://ex/hasClaim", CLAIM, 2, 2),
                (PID, 2, "http://ex/hasClaim", CLAIM, "http://ex/onPolicy",
                 "http://ex/Policy", 2, 2),
                (PID2, 1, "", CUSTOMER, "http://ex/hasOrder", "http://ex/Order", 1, 1),
            ],
        )
        self.statements: list[str] = []

    @staticmethod
    def _dialect(sql: str) -> str:
        return sql.replace(" USING DELTA", "").replace("current_timestamp()", "CURRENT_TIMESTAMP")

    def query(self, sql: str):
        try:
            return [dict(r) for r in self.conn.execute(self._dialect(sql)).fetchall()]
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc):  # Delta's spelling of the same fact
                raise RuntimeError(f"[TABLE_OR_VIEW_NOT_FOUND] {exc}") from exc
            raise

    def execute(self, sql: str):
        self.statements.append(sql)
        self.conn.execute(self._dialect(sql))
        self.conn.commit()
        return []

    def store(self) -> SavedPatternStore:
        return SavedPatternStore(OUT, SAVED, query=self.query, execute=self.execute)


@pytest.fixture
def wh() -> _Warehouse:
    return _Warehouse()


def test_slug_saved_id():
    assert slug_saved_id("FR buyers!", "ab12cd34ffff") == "fr-buyers-ab12cd34"


def test_save_writes_one_row_per_pattern_not_members(wh):
    result = wh.store().save(PID, "FR buyers")
    assert result["size"] == 3  # support 2 + 1
    assert result["pattern_count"] == 2
    assert result["saved_id"].startswith("fr-buyers-")
    rows = wh.query(
        f"SELECT pattern_id, anchor_uri, support, saved_label FROM {SAVED} "
        "ORDER BY pattern_id"
    )
    assert len(rows) == 2
    assert {r["pattern_id"] for r in rows} == {PID, PID2}
    assert all(r["anchor_uri"] is None or r["anchor_uri"] == "" for r in rows)
    assert {r["saved_label"] for r in rows} == {"FR buyers"}
    by_pid = {r["pattern_id"]: r["support"] for r in rows}
    assert by_pid[PID] == 2
    assert by_pid[PID2] == 1


def test_save_issues_two_inserts_whatever_the_pattern_count(wh):
    wh.store().save(PID, "FR buyers")
    inserts = [s for s in wh.statements if s.lstrip().upper().startswith("INSERT")]
    # eager rows (with diagrams) + one server-side INSERT … SELECT for the rest
    assert len(inserts) == 2
    assert "SELECT" in inserts[1].upper()


def _add_patterns(wh, count: int) -> None:
    rows = [(f"p{i:05d}", 1, 100 + i, f"seed {i}", "[]", 0, 1) for i in range(count)]
    wh.conn.executemany("INSERT INTO out_tbl_shapes VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
    wh.conn.executemany(
        "INSERT INTO out_tbl_shape_edges VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [(f"p{i:05d}", 1, "", CUSTOMER, "http://ex/hasOrder", "http://ex/Order", 1, 1)
         for i in range(count)],
    )
    wh.conn.execute("UPDATE out_tbl_summary SET pattern_count = pattern_count + ?", (count,))
    wh.conn.commit()


def test_save_cost_does_not_grow_with_the_pattern_count(wh):
    """A 46k-pattern run once took an hour: 2 warehouse queries per pattern."""
    queries: list[str] = []
    real = wh.query
    wh.query = lambda sql: (queries.append(sql), real(sql))[1]  # type: ignore[assignment]
    wh.store().save(PID, "small")
    small = len(queries)
    queries.clear()
    _add_patterns(wh, 400)
    result = wh.store().save(PID, "big")
    assert result["pattern_count"] == 402
    assert len(queries) <= small + 2
    assert len(queries) < 20


def test_save_stores_every_pattern_with_sql_computed_kpis(wh, monkeypatch):
    monkeypatch.setattr(_STORE_MODULE, "EAGER_DIAGRAMS", 1)
    store = wh.store()
    sid = store.save(PID, "Customer")["saved_id"]
    rows = {
        r["pattern_id"]: r
        for r in wh.query(f"SELECT * FROM {SAVED} WHERE saved_id = '{sid}'")
    }
    assert set(rows) == {PID, PID2}
    assert rows[PID]["diagram_json"]  # focus pattern: eager diagram
    assert not rows[PID2]["diagram_json"]  # beyond the eager cap
    assert (rows[PID2]["type_count"], rows[PID2]["relation_count"]) == (2, 1)
    assert (rows[PID]["type_count"], rows[PID]["relation_count"]) == (3, 2)
    assert rows[PID2]["hop_count"] == 1 and rows[PID2]["support"] == 1
    assert rows[PID2]["seed_label"] == "Customer · hasOrder"
    assert rows[PID2]["pattern_count"] == 2


def test_pattern_beyond_the_eager_cap_draws_on_first_open(wh, monkeypatch):
    monkeypatch.setattr(_STORE_MODULE, "EAGER_DIAGRAMS", 1)
    store = wh.store()
    sid = store.save(PID, "Customer")["saved_id"]
    shape = store.shape(sid, pattern_id=PID2)
    assert shape is not None and shape["diagram"]["nodes"]
    stored = wh.query(
        f"SELECT diagram_json FROM {SAVED} WHERE saved_id = '{sid}' "
        f"AND pattern_id = '{PID2}'"
    )
    assert stored[0]["diagram_json"]  # persisted: the next open is a plain read


def test_save_without_shape_edges_table_still_writes_the_catalog(wh):
    wh.conn.execute("DROP TABLE out_tbl_shape_edges")
    wh.conn.commit()
    result = wh.store().save(PID, "No edges")
    assert result["pattern_count"] == 2
    assert result["type_count"] == 1 and result["relation_count"] == 0


def test_save_never_writes_to_the_graph(wh):
    wh.store().save(PID, "FR buyers")
    writes = [
        s for s in wh.statements if s.lstrip().upper().startswith(("INSERT", "DELETE", "CREATE"))
    ]
    assert writes, "a save must write the snapshot table"
    for sql in wh.statements:
        assert "triple" not in sql.lower()
        assert SAVED in sql or OUT in sql


def test_list_reports_one_item_per_saved_pattern(wh):
    store = wh.store()
    store.save(PID, "FR buyers")
    items = store.list_saved()
    assert len(items) == 1
    assert items[0]["label"] == "FR buyers"
    assert items[0]["size"] == 3
    assert items[0]["pattern_id"] == PID
    assert items[0]["id"].startswith("fr-buyers-")
    assert items[0]["hop_count"] == 2
    assert items[0]["support"] == 2
    assert items[0]["type_count"] == 3
    assert items[0]["relation_count"] == 2
    assert items[0]["pattern_count"] == 2
    assert items[0]["saved_at"]


def test_save_snapshots_discover_kpis(wh):
    result = wh.store().save(PID, "FR buyers")
    assert result["hop_count"] == 2
    assert result["support"] == 2
    assert result["type_count"] == 3
    assert result["relation_count"] == 2
    assert result["pattern_count"] == 2
    assert result["has_diagram"] is True


def test_save_snapshots_every_pattern_in_the_detect_run(wh):
    store = wh.store()
    sid = store.save(PID, "Customer run")["saved_id"]
    catalog = store.patterns(sid)
    assert {p["id"] for p in catalog["patterns"]} == {PID, PID2}
    assert catalog["focus_pattern_id"] == PID
    assert catalog["count"] == 2
    # Wipe live Detect — catalog + diagrams stay; members come from Detect and
    # are empty once that run is gone.
    wh.conn.execute("DELETE FROM out_tbl")
    wh.conn.execute("DELETE FROM out_tbl_shapes")
    wh.conn.execute("DELETE FROM out_tbl_shape_edges")
    wh.conn.commit()
    for pid, min_nodes in ((PID, 2), (PID2, 2)):
        shape = store.shape(sid, pattern_id=pid)
        assert shape is not None
        assert shape["pattern_id"] == pid
        assert shape["diagram"] and len(shape["diagram"]["nodes"]) >= min_nodes
        members = store.members(sid, pattern_id=pid)
        assert members["total"] == 0


def test_save_snapshots_diagram_and_shape_survives_new_detect(wh):
    store = wh.store()
    sid = store.save(PID, "Customer")["saved_id"]
    shape = store.shape(sid, pattern_id=PID)
    assert shape is not None
    assert shape["pattern_id"] == PID
    assert shape["diagram"]
    assert shape["diagram"]["nodes"]
    assert shape["_fromSaved"] is True
    wh.conn.execute("DELETE FROM out_tbl_shapes")
    wh.conn.execute("DELETE FROM out_tbl_shape_edges")
    wh.conn.commit()
    again = store.shape(sid, pattern_id=PID)
    assert again is not None
    assert again["diagram"]["nodes"]
    assert len(again["diagram"]["nodes"]) >= 2


def test_list_backfills_kpis_for_rows_saved_before_snapshot_columns(wh):
    """Older saves have NULL KPI columns; list_saved fills them from the live Detect tables."""
    store = wh.store()
    store._ensure_table()
    wh.execute(
        f"INSERT INTO {SAVED} "
        "(saved_id, saved_label, pattern_id, anchor_uri, anchor_label, type_uri, saved_at) "
        f"VALUES ('legacy-pat', 'Legacy', '{PID}', 'http://ex/Alice', 'Alice', "
        f"'{CUSTOMER}', CURRENT_TIMESTAMP)"
    )
    wh.execute(
        f"INSERT INTO {SAVED} "
        "(saved_id, saved_label, pattern_id, anchor_uri, anchor_label, type_uri, saved_at) "
        f"VALUES ('legacy-pat', 'Legacy', '{PID}', 'http://ex/Bob', 'Bob', "
        f"'{CUSTOMER}', CURRENT_TIMESTAMP)"
    )
    items = store.list_saved()
    legacy = next(i for i in items if i["id"] == "legacy-pat")
    assert legacy["hop_count"] == 2
    assert legacy["support"] == 2
    assert legacy["type_count"] == 3
    assert legacy["relation_count"] == 2
    assert legacy["pattern_count"] == 1
    rows = wh.query(
        f"SELECT hop_count, type_count, relation_count, pattern_count "
        f"FROM {SAVED} WHERE saved_id = 'legacy-pat' LIMIT 1"
    )
    assert rows[0]["hop_count"] == 2
    assert rows[0]["type_count"] == 3
    assert rows[0]["relation_count"] == 2
    assert rows[0]["pattern_count"] == 1


def test_members_are_paged_from_live_detect(wh):
    store = wh.store()
    sid = store.save(PID, "FR buyers")["saved_id"]
    first = store.members(sid, offset=0, limit=1, pattern_id=PID)
    assert first["total"] == 2
    assert first["has_more"] is True
    assert first["members"][0] == {
        "uri": "http://ex/Alice",
        "label": "Alice",
        "type": CUSTOMER,
        "type_label": "Customer",
    }
    second = store.members(sid, offset=1, limit=1, pattern_id=PID)
    assert second["members"][0]["uri"] == "http://ex/Bob"
    assert second["has_more"] is False


def test_legacy_member_rows_still_page_from_saved(wh):
    store = wh.store()
    store._ensure_table()
    wh.execute(
        f"INSERT INTO {SAVED} "
        "(saved_id, saved_label, pattern_id, focus_pattern_id, anchor_uri, "
        "anchor_label, type_uri, hop_count, support, type_count, relation_count, "
        "pattern_count, seed_label, shape_json, diagram_json, saved_at) "
        f"VALUES ('legacy-mem', 'Legacy', '{PID}', '{PID}', 'http://ex/Alice', "
        f"'Alice', '{CUSTOMER}', 2, 2, 3, 2, 1, 'seed', '[]', '', CURRENT_TIMESTAMP)"
    )
    wh.execute(
        f"INSERT INTO {SAVED} "
        "(saved_id, saved_label, pattern_id, focus_pattern_id, anchor_uri, "
        "anchor_label, type_uri, hop_count, support, type_count, relation_count, "
        "pattern_count, seed_label, shape_json, diagram_json, saved_at) "
        f"VALUES ('legacy-mem', 'Legacy', '{PID}', '{PID}', 'http://ex/Bob', "
        f"'Bob', '{CUSTOMER}', 2, 2, 3, 2, 1, 'seed', '[]', '', CURRENT_TIMESTAMP)"
    )
    # Wipe Detect so a catalog-style fallback would return nothing.
    wh.conn.execute("DELETE FROM out_tbl")
    wh.conn.commit()
    page = store.members("legacy-mem", pattern_id=PID)
    assert page["total"] == 2
    assert {m["uri"] for m in page["members"]} == {
        "http://ex/Alice",
        "http://ex/Bob",
    }


def test_second_save_keeps_the_first(wh):
    store = wh.store()
    first = store.save(PID, "FR buyers")
    second = store.save(PID, "FR buyers")
    assert first["saved_id"] != second["saved_id"]
    assert len(store.list_saved()) == 2


def test_delete_removes_only_one_saved_pattern(wh):
    store = wh.store()
    a = store.save(PID, "A")["saved_id"]
    b = store.save(PID, "B")["saved_id"]
    assert store.delete(a) == 2  # one catalog row per pattern
    assert {item["id"] for item in store.list_saved()} == {b}
    assert store.members(b)["total"] == 3


def test_delete_unknown_pattern_is_a_no_op(wh):
    assert wh.store().delete("never-saved") == 0


def test_catalog_survives_a_new_detect_run(wh):
    store = wh.store()
    sid = store.save(PID, "FR buyers")["saved_id"]
    wh.conn.execute("DELETE FROM out_tbl")
    wh.conn.commit()
    assert store.members(sid)["total"] == 0  # members live on Detect only
    assert len(store.list_saved()) == 1
    catalog = store.patterns(sid)
    assert catalog["count"] == 2
    # Run-level coverage stats are persisted in the snapshot: population=10
    # (all entities of the type), covered = support 2 + 1 = 3 → Coverage 30%.
    assert catalog["stats"]["anchor_count"] == 4
    assert catalog["stats"]["population_count"] == 10
    assert catalog["stats"]["covered_count"] == 3
    assert catalog["stats"]["pattern_count"] == 2
    assert store.shape(sid, pattern_id=PID)["diagram"]["nodes"]


def test_save_rejects_unknown_or_empty_pattern(wh):
    with pytest.raises(ValidationError, match="Unknown or empty"):
        wh.store().save("nope", "X")
    assert not any(s.startswith("INSERT") for s in wh.statements)


def test_save_requires_a_pattern_id(wh):
    with pytest.raises(ValidationError, match="pattern_id is required"):
        wh.store().save("  ", "X")


def test_label_quotes_are_escaped(wh):
    store = wh.store()
    store.save(PID, "O'Brien")
    assert store.list_saved()[0]["label"] == "O'Brien"


def test_invalid_saved_id_is_rejected(wh):
    store = wh.store()
    with pytest.raises(ValidationError, match="Invalid saved pattern id"):
        store.members("Bad Id'; DROP")
    with pytest.raises(ValidationError, match="Invalid saved pattern id"):
        store.delete("../x")


def test_table_names_follow_the_domain_version():
    domain = SimpleNamespace(uc_domain_folder="Ex", current_version="1.0")
    assert pattern_output_table("cat.sch", domain, "g") == "cat.sch.graph_patterns_ex_1_0"
    assert saved_pattern_table("cat.sch", domain, "g") == "cat.sch.graph_pattern_saved_ex_1_0"


def test_save_draws_hops_from_legacy_shape_edges_without_parent_via(wh):
    """Warehouse Detect tables still on the pre-parent_via schema must not
    collapse the snapshot diagram to the anchor alone."""
    rows = wh.query(
        "SELECT pattern_id, hop, parent_type, via, child_type, "
        "member_count, edge_total FROM out_tbl_shape_edges"
    )
    wh.conn.execute("DROP TABLE out_tbl_shape_edges")
    wh.conn.execute(
        "CREATE TABLE out_tbl_shape_edges ("
        "pattern_id TEXT, hop INT, parent_type TEXT, via TEXT, child_type TEXT, "
        "member_count INT, edge_total INT, direction TEXT)"
    )
    wh.conn.executemany(
        "INSERT INTO out_tbl_shape_edges VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                r["pattern_id"],
                r["hop"],
                r["parent_type"],
                r["via"],
                r["child_type"],
                r["member_count"],
                r["edge_total"],
                "out",
            )
            for r in rows
        ],
    )
    wh.conn.commit()
    store = wh.store()
    sid = store.save(PID, "Legacy edges")["saved_id"]
    shape = store.shape(sid, pattern_id=PID)
    assert shape is not None
    assert len(shape["diagram"]["nodes"]) >= 2
    assert any(int(n.get("hop") or 0) > 0 for n in shape["diagram"]["nodes"])


def test_shape_rebuilds_anchor_only_snapshot_from_live_detect(wh):
    """A Save that stored only the Entity node (failed edge query) recovers
    hops when the live Detect edges are still available."""
    store = wh.store()
    sid = store.save(PID, "Thin")["saved_id"]
    thin = {
        "nodes": [
            {
                "id": f"0||{CUSTOMER}",
                "hop": 0,
                "type": CUSTOMER,
                "type_label": "Customer",
                "attributes": [],
            }
        ],
        "edges": [],
        "truncated": False,
    }
    wh.conn.execute(
        f"UPDATE {SAVED} SET diagram_json = ? WHERE saved_id = ? AND pattern_id = ?",
        (json.dumps(thin), sid, PID),
    )
    wh.conn.commit()
    shape = store.shape(sid, pattern_id=PID)
    assert shape is not None
    assert len(shape["diagram"]["nodes"]) >= 2
    assert any(int(n.get("hop") or 0) > 0 for n in shape["diagram"]["nodes"])


def test_shape_rebuilds_snapshot_with_orphan_hop_roots(wh):
    """Legacy parent_via mismatch left hop-1 nodes with no incoming edge."""
    store = wh.store()
    sid = store.save(PID, "Orphans")["saved_id"]
    broken = {
        "nodes": [
            {
                "id": f"0||{CUSTOMER}",
                "hop": 0,
                "type": CUSTOMER,
                "type_label": "Customer",
                "attributes": [],
            },
            {
                "id": f"1||{CLAIM}",
                "hop": 1,
                "type": CLAIM,
                "type_label": "Claim",
                "via": "",
                "attributes": [],
            },
            {
                "id": f"1|http://ex/hasClaim|{CLAIM}",
                "hop": 1,
                "type": CLAIM,
                "type_label": "Claim",
                "via": "http://ex/hasClaim",
                "attributes": [],
            },
        ],
        "edges": [
            {
                "source": f"0||{CUSTOMER}",
                "target": f"1|http://ex/hasClaim|{CLAIM}",
                "via": "http://ex/hasClaim",
                "via_label": "hasClaim",
                "share": 1.0,
                "avg": 1.0,
            },
            {
                "source": f"1||{CLAIM}",
                "target": "2|http://ex/onPolicy|http://ex/Policy",
                "via": "http://ex/onPolicy",
                "via_label": "onPolicy",
                "share": 1.0,
                "avg": 1.0,
            },
        ],
        "truncated": False,
    }
    wh.conn.execute(
        f"UPDATE {SAVED} SET diagram_json = ? WHERE saved_id = ? AND pattern_id = ?",
        (json.dumps(broken), sid, PID),
    )
    wh.conn.commit()
    shape = store.shape(sid, pattern_id=PID)
    assert shape is not None
    targets = {e["target"] for e in shape["diagram"]["edges"]}
    orphans = [
        n for n in shape["diagram"]["nodes"]
        if int(n.get("hop") or 0) > 0 and n["id"] not in targets
    ]
    assert orphans == []
