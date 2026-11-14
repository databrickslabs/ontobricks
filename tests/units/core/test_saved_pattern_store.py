"""SavedPatternStore: pattern snapshots in a Delta table, never in the graph."""

from __future__ import annotations

import sqlite3
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
            "(pattern_count INT, hop_count INT, anchor_type TEXT, bins INT)"
        )
        self.conn.execute(
            "INSERT INTO out_tbl_summary VALUES (?, ?, ?, ?)",
            (2, 3, CUSTOMER, 8),
        )
        self.conn.execute(
            "CREATE TABLE out_tbl_shape_edges "
            "(pattern_id TEXT, hop INT, parent_type TEXT, via TEXT, child_type TEXT, "
            "direction TEXT, member_count INT, edge_total INT)"
        )
        self.conn.executemany(
            "INSERT INTO out_tbl_shape_edges VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (PID, 1, CUSTOMER, "http://ex/hasClaim", CLAIM, "out", 2, 2),
                (PID, 2, CLAIM, "http://ex/onPolicy", "http://ex/Policy", "out", 2, 2),
                (PID2, 1, CUSTOMER, "http://ex/hasOrder", "http://ex/Order", "out", 1, 1),
            ],
        )
        self.statements: list[str] = []

    @staticmethod
    def _dialect(sql: str) -> str:
        return sql.replace(" USING DELTA", "").replace("current_timestamp()", "CURRENT_TIMESTAMP")

    def query(self, sql: str):
        return [dict(r) for r in self.conn.execute(self._dialect(sql)).fetchall()]

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


def test_save_snapshots_members_with_their_label_and_type(wh):
    result = wh.store().save(PID, "FR buyers")
    assert result["size"] == 3  # Alice+Bob (PID) + Carol (PID2)
    assert result["saved_id"].startswith("fr-buyers-")
    rows = wh.query(
        f"SELECT anchor_uri, anchor_label, type_uri, saved_label FROM {SAVED} "
        f"WHERE pattern_id = '{PID}' ORDER BY anchor_uri"
    )
    assert [r["anchor_uri"] for r in rows] == ["http://ex/Alice", "http://ex/Bob"]
    assert rows[0]["anchor_label"] == "Alice"
    assert rows[0]["type_uri"] == CUSTOMER
    assert {r["saved_label"] for r in rows} == {"FR buyers"}


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
    assert items[0]["pattern_count"] == 2  # whole Detect run snapshotted
    assert items[0]["pattern_id"] == PID  # focus from Save
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
    # Wipe live Detect — both patterns must still open with diagrams + members.
    wh.conn.execute("DELETE FROM out_tbl")
    wh.conn.execute("DELETE FROM out_tbl_shapes")
    wh.conn.execute("DELETE FROM out_tbl_shape_edges")
    wh.conn.commit()
    for pid, min_nodes, min_members in ((PID, 2, 2), (PID2, 2, 1)):
        shape = store.shape(sid, pattern_id=pid)
        assert shape is not None
        assert shape["pattern_id"] == pid
        assert shape["diagram"] and len(shape["diagram"]["nodes"]) >= min_nodes
        members = store.members(sid, pattern_id=pid)
        assert members["total"] == min_members


def test_save_snapshots_diagram_and_shape_survives_new_detect(wh):
    store = wh.store()
    sid = store.save(PID, "Customer")["saved_id"]
    shape = store.shape(sid, pattern_id=PID)
    assert shape is not None
    assert shape["pattern_id"] == PID
    assert shape["diagram"]
    assert shape["diagram"]["nodes"]
    assert shape["_fromSaved"] is True
    # Overwrite live Detect tables — the snapshot must still draw.
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
    assert legacy["pattern_count"] == 1  # distinct pattern_ids in that legacy save
    rows = wh.query(
        f"SELECT hop_count, type_count, relation_count, pattern_count "
        f"FROM {SAVED} WHERE saved_id = 'legacy-pat' LIMIT 1"
    )
    assert rows[0]["hop_count"] == 2
    assert rows[0]["type_count"] == 3
    assert rows[0]["relation_count"] == 2
    assert rows[0]["pattern_count"] == 1


def test_members_are_paged_and_shaped_like_discovered_members(wh):
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
    assert store.delete(a) == 3  # both patterns' members from the Detect run
    assert {item["id"] for item in store.list_saved()} == {b}
    assert store.members(b)["total"] == 3


def test_delete_unknown_pattern_is_a_no_op(wh):
    assert wh.store().delete("never-saved") == 0


def test_saved_snapshot_survives_a_new_detect_run(wh):
    store = wh.store()
    sid = store.save(PID, "FR buyers")["saved_id"]
    # A new Detect overwrites the output table.
    wh.conn.execute("DELETE FROM out_tbl")
    wh.conn.commit()
    assert store.members(sid)["total"] == 3  # all patterns in the run
    assert len(store.list_saved()) == 1


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
