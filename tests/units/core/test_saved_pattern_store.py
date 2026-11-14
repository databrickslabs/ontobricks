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
CUSTOMER = "http://ex/Customer"


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
                ("http://ex/Other", "zzz", "Other", CUSTOMER),
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
    assert result["size"] == 2
    assert result["saved_id"].startswith("fr-buyers-")
    rows = wh.query(
        f"SELECT anchor_uri, anchor_label, type_uri, saved_label FROM {SAVED} ORDER BY anchor_uri"
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
    assert items[0]["size"] == 2
    assert items[0]["pattern_id"] == PID
    assert items[0]["id"].startswith("fr-buyers-")


def test_members_are_paged_and_shaped_like_discovered_members(wh):
    store = wh.store()
    sid = store.save(PID, "FR buyers")["saved_id"]
    first = store.members(sid, offset=0, limit=1)
    assert first["total"] == 2
    assert first["has_more"] is True
    assert first["members"][0] == {
        "uri": "http://ex/Alice",
        "label": "Alice",
        "type": CUSTOMER,
        "type_label": "Customer",
    }
    second = store.members(sid, offset=1, limit=1)
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
    assert store.delete(a) == 2
    assert {item["id"] for item in store.list_saved()} == {b}
    assert store.members(b)["total"] == 2


def test_delete_unknown_pattern_is_a_no_op(wh):
    assert wh.store().delete("never-saved") == 0


def test_saved_snapshot_survives_a_new_detect_run(wh):
    store = wh.store()
    sid = store.save(PID, "FR buyers")["saved_id"]
    # A new Detect overwrites the output table.
    wh.conn.execute("DELETE FROM out_tbl")
    wh.conn.commit()
    assert store.members(sid)["total"] == 2
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
