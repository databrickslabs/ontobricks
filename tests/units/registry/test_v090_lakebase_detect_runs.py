"""Real SQL of the community and pattern detection run history.

:class:`DetectRuns` (append-only ``community_runs`` / ``pattern_runs``) driven
through the scripted cursor from ``_v090_fake_pg``, the same harness the
graph-analytics run history is tested with.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from ._v090_fake_pg import (
    QSCHEMA,
    REGISTRY_ID,
    FakeDB,
    fake_psycopg,  # noqa: F401
    make_store,
)

pytestmark = pytest.mark.unit

_T0 = datetime(2026, 11, 16, 9, 0, tzinfo=timezone.utc)

KINDS = [
    # kind, table, flag, index
    ("community", "community_runs", "_community_runs_ready", "idx_community_runs_domain_version"),
    ("pattern", "pattern_runs", "_pattern_runs_ready", "idx_pattern_runs_domain_version"),
]


def _community_entry(**over):
    entry = {
        "status": "completed",
        "class_filter": ["http://x/Person"],
        "node_count": "90",
        "cluster_count": 4,
        "converged": True,
        "duration_ms": 1200,
        "task_id": "t-1",
        "error": "",
        "run_page_url": "https://dbx/run/1",
        "computed_at": _T0.isoformat(),
    }
    entry.update(over)
    return entry


def _pattern_entry(**over):
    entry = {
        "status": "completed",
        "anchor_type": "http://x/Person",
        "hop_count": 3,
        "min_support": 2,
        "detail_level": "full",
        "anchor_count": 500,
        "pattern_count": 12,
        "duration_ms": 900,
        "task_id": "t-2",
        "error": "",
        "run_page_url": "",
        "computed_at": _T0.isoformat(),
    }
    entry.update(over)
    return entry


def _record(store, kind, folder, version, entry):
    if kind == "community":
        store.record_community_run(folder, version, entry)
    else:
        store.record_pattern_run(folder, version, entry)


def _load_all(store, kind, **kw):
    if kind == "community":
        return store.load_all_community_runs(**kw)
    return store.load_all_pattern_runs(**kw)


# ---------------------------------------------------------------------------
# Lazy tables
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("kind,table,flag,index", KINDS)
def test_lazy_table_created_when_absent(kind, table, flag, index):
    db = FakeDB()  # information_schema returns no row -> table absent

    assert make_store(db, ready=False)._ensure_detect_runs_table(kind) is True

    create = db.one("CREATE TABLE IF NOT EXISTS")
    assert f"{QSCHEMA}.{table}" in create.sql
    assert "domain_id" in create.sql and "computed_at" in create.sql
    assert "REFERENCES" in create.sql and "ON DELETE CASCADE" in create.sql
    idx = db.one("CREATE INDEX IF NOT EXISTS")
    assert index in idx.sql


@pytest.mark.parametrize("kind,table,flag,index", KINDS)
def test_lazy_table_skips_ddl_when_present_and_fails_soft(kind, table, flag, index):
    present = FakeDB().on("information_schema", rows=[{"?column?": 1}])
    store = make_store(present, ready=False)
    assert store._ensure_detect_runs_table(kind) is True
    assert not present.sql_with("CREATE TABLE")
    assert getattr(store, flag) is True

    broken = FakeDB().on("information_schema", raises=RuntimeError("no"))
    store = make_store(broken, ready=False)
    assert store._ensure_detect_runs_table(kind) is False
    assert getattr(store, flag) is False


@pytest.mark.parametrize("kind,table,flag,index", KINDS)
def test_flag_short_circuits_without_touching_the_database(kind, table, flag, index):
    db = FakeDB()

    assert make_store(db, ready=True)._ensure_detect_runs_table(kind) is True

    assert db.calls == []


# ---------------------------------------------------------------------------
# Record
# ---------------------------------------------------------------------------


def test_record_community_run_binds_coerced_values():
    db = FakeDB()

    make_store(db).record_community_run("acme", 2, _community_entry())

    ins = db.one("INSERT INTO")
    assert f"INSERT INTO {QSCHEMA}.community_runs" in ins.sql
    assert "%s::jsonb" in ins.sql
    p = ins.params
    # [version, class_filter, node_count, cluster_count, converged,
    #  status, duration_ms, task_id, error, run_page_url, computed_at,
    #  registry_id, folder]
    assert p[0] == "2"
    assert p[1] == json.dumps(["http://x/Person"])
    assert p[2] == 90  # node_count coerced from "90"
    assert p[3] == 4
    assert p[4] is True
    assert p[5] == "completed"
    assert p[6] == 1200
    assert p[-3] == _T0.isoformat()
    assert p[-2:] == (REGISTRY_ID, "acme")


def test_record_pattern_run_binds_coerced_values():
    db = FakeDB()

    make_store(db).record_pattern_run("acme", "3", _pattern_entry())

    ins = db.one("INSERT INTO")
    assert f"INSERT INTO {QSCHEMA}.pattern_runs" in ins.sql
    p = ins.params
    assert p[0] == "3"
    assert p[1] == "http://x/Person"
    assert p[2] == 3  # hop_count
    assert p[3] == 2  # min_support
    assert p[4] == "full"
    assert p[5] == 500
    assert p[6] == 12
    assert p[-3] == _T0.isoformat()
    assert p[-2:] == (REGISTRY_ID, "acme")


@pytest.mark.parametrize("kind,table,flag,index", KINDS)
def test_record_prunes_history_beyond_cap_per_version(kind, table, flag, index):
    db = FakeDB()

    _record(make_store(db), kind, "acme", "2", {})

    prune = db.one("DELETE FROM")
    assert table in prune.sql
    assert "OFFSET %s" in prune.sql
    assert prune.params[:3] == (REGISTRY_ID, "acme", "2")
    assert prune.params[3] == 100


@pytest.mark.parametrize("kind,table,flag,index", KINDS)
def test_record_is_a_noop_without_the_table(kind, table, flag, index):
    db = FakeDB().on("information_schema", raises=RuntimeError())

    _record(make_store(db, ready=False), kind, "acme", "2", {})

    assert not db.sql_with("INSERT")
    assert not db.sql_with("DELETE")


@pytest.mark.parametrize("kind,table,flag,index", KINDS)
def test_record_swallows_write_errors(kind, table, flag, index):
    db = FakeDB().on("INSERT INTO", raises=RuntimeError("boom"))

    _record(make_store(db), kind, "acme", "2", {})  # must not raise


# ---------------------------------------------------------------------------
# Cross-domain reads
# ---------------------------------------------------------------------------


def _community_row(**over):
    row = {
        "id": 5,
        "domain": "acme",
        "version": "2",
        "status": "completed",
        "class_filter": ["http://x/Person"],
        "node_count": 90,
        "cluster_count": 4,
        "converged": True,
        "duration_ms": 1200,
        "task_id": "t-1",
        "error": None,
        "run_page_url": "https://dbx/run/1",
        "computed_at": _T0,
    }
    row.update(over)
    return row


def test_load_all_community_runs_paginates_and_maps(fake_psycopg):
    db = FakeDB()
    db.on("SELECT COUNT(*) AS total", rows=[{"total": 9}])
    db.on("d.folder AS domain", rows=[_community_row()])

    entries, total = make_store(db).load_all_community_runs(folder="acme", limit=1, offset=2)

    assert total == 9
    assert entries == [
        {
            "id": 5,
            "domain": "acme",
            "version": "2",
            "computed_at": _T0.isoformat(),
            "status": "completed",
            "class_filter": ["http://x/Person"],
            "node_count": 90,
            "cluster_count": 4,
            "converged": True,
            "duration_ms": 1200,
            "task_id": "t-1",
            "error": "",
            "run_page_url": "https://dbx/run/1",
        }
    ]
    select = db.calls[1]
    assert "FROM" in select.sql and "community_runs r" in select.sql
    assert select.params == (REGISTRY_ID, "acme", 1, 2)


def test_load_all_pattern_runs_maps_pattern_columns(fake_psycopg):
    row = {
        "id": 8,
        "domain": "acme",
        "version": "2",
        "anchor_type": "http://x/Person",
        "hop_count": 3,
        "min_support": 2,
        "detail_level": "full",
        "anchor_count": 500,
        "pattern_count": 12,
        "status": "failed",
        "duration_ms": 900,
        "task_id": "t-2",
        "error": "boom",
        "run_page_url": "",
        "computed_at": _T0,
    }
    db = FakeDB()
    db.on("SELECT COUNT(*) AS total", rows=[{"total": 1}])
    db.on("d.folder AS domain", rows=[row])

    entries, total = make_store(db).load_all_pattern_runs()

    assert total == 1
    entry = entries[0]
    assert entry["anchor_type"] == "http://x/Person"
    assert entry["hop_count"] == 3
    assert entry["min_support"] == 2
    assert entry["detail_level"] == "full"
    assert entry["anchor_count"] == 500
    assert entry["pattern_count"] == 12
    assert entry["status"] == "failed"
    assert entry["error"] == "boom"
    assert entry["domain"] == "acme"
    assert db.calls[0].params == (REGISTRY_ID,)


@pytest.mark.parametrize("kind,table,flag,index", KINDS)
def test_load_all_error_and_missing_table_return_an_empty_page(fake_psycopg, kind, table, flag, index):
    db = FakeDB().on("SELECT COUNT(*)", raises=RuntimeError())
    assert _load_all(make_store(db), kind) == ([], 0)

    bad = make_store(FakeDB().on("information_schema", raises=RuntimeError()), ready=False)
    assert _load_all(bad, kind) == ([], 0)
