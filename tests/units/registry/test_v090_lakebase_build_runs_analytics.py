"""Real SQL of the build-run trace and graph-analytics mixins.

:class:`BuildRuns` (append-only build trace, last-build
stamp, analytics roll-up) and :class:`Analytics` (cached
graph metrics + capped run history) driven through a scripted cursor.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from ._v090_fake_pg import (
    DICT_ROW,
    QSCHEMA,
    REGISTRY_ID,
    SCHEMA,
    FakeDB,
    fake_psycopg,  # noqa: F401
    make_store,
)

pytestmark = pytest.mark.unit

_T0 = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)
_T1 = datetime(2026, 9, 1, 8, 5, tzinfo=timezone.utc)


def _run_row(**over):
    row = {
        "id": 7,
        "version": "2",
        "build_kind": "session",
        "status": "success",
        "message": None,
        "error": None,
        "started_at": _T0,
        "finished_at": _T1,
        "duration_s": 12.5,
        "triple_count": 1000,
        "entity_count": 10,
        "relationship_count": 20,
        "sql_chars": 300,
        "graph_engine": None,
        "sync_mode": "full",
        "view_table": "c.s.v",
        "graph_name": "g",
        "task_id": "t1",
        "phase_times": {"sql": 1.0},
        "stats": None,
    }
    row.update(over)
    return row


# ---------------------------------------------------------------------------
# Lazy tables (build_runs, graph_analytics, graph_analytics_runs)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "method,flag,table,index",
    [
        ("_ensure_build_runs_table", "_build_runs_ready", "build_runs", "idx_build_runs_domain_version"),
        ("_ensure_graph_analytics_table", "_graph_analytics_ready", "graph_analytics", None),
        ("_ensure_graph_analytics_runs_table", "_graph_analytics_runs_ready", "graph_analytics_runs", "idx_graph_analytics_runs_domain_version"),
    ],
)
def test_lazy_table_created_when_absent(method, flag, table, index):
    db = FakeDB()
    store = make_store(db, ready=False)

    assert getattr(store, method)() is True
    assert f"table_name = '{table}'" in db.calls[0].sql
    assert db.calls[0].params == (SCHEMA,)
    assert db.sql_with(f"CREATE TABLE IF NOT EXISTS {QSCHEMA}.{table} (")
    if index:
        assert db.sql_with(index)
    assert getattr(store, flag) is True


@pytest.mark.parametrize(
    "method,flag",
    [
        ("_ensure_build_runs_table", "_build_runs_ready"),
        ("_ensure_graph_analytics_table", "_graph_analytics_ready"),
        ("_ensure_graph_analytics_runs_table", "_graph_analytics_runs_ready"),
    ],
)
def test_lazy_table_skips_ddl_when_present_and_fails_soft(method, flag):
    db = FakeDB().on("information_schema.tables", rows=[(1,)])
    store = make_store(db, ready=False)
    assert getattr(store, method)() is True
    assert not db.sql_with("CREATE")

    broken = make_store(FakeDB().on("information_schema", raises=RuntimeError("x")), ready=False)
    assert getattr(broken, method)() is False
    assert getattr(broken, flag) is False


# ---------------------------------------------------------------------------
# Build runs
# ---------------------------------------------------------------------------


def test_record_build_run_binds_coerced_values():
    db = FakeDB().on("INSERT INTO", rowcount=1)
    entry = {
        "version": 3,
        "status": "success",
        "message": None,
        "started_at": "2026-09-01T08:00:00Z",
        "duration_s": "1.5",
        "triple_count": None,
        "phase_times": {"sql": 1},
    }

    make_store(db).record_build_run("acme", entry)

    call = db.one("INSERT INTO")
    assert f"INSERT INTO {QSCHEMA}.build_runs" in call.sql
    assert "COALESCE(%s::timestamptz, now())" in call.sql
    p = call.params
    assert p[:5] == ("3", "session", "success", "", "")
    assert p[5] == "2026-09-01T08:00:00Z" and p[6] is None
    assert p[7] == 1.5 and p[8] == 0
    assert json.loads(p[17]) == {"sql": 1} and json.loads(p[18]) == {}
    assert p[-2:] == (REGISTRY_ID, "acme")


def test_record_build_run_tolerates_missing_domain_and_errors():
    make_store(FakeDB().on("INSERT INTO", rowcount=0)).record_build_run("ghost", {})
    make_store(FakeDB().on("INSERT INTO", raises=RuntimeError("e"))).record_build_run("a", {})


def test_record_build_run_noop_when_table_unavailable():
    db = FakeDB().on("information_schema", raises=RuntimeError("x"))

    make_store(db, ready=False).record_build_run("acme", {})

    assert not db.sql_with("INSERT")


def test_stamp_last_build_targets_single_column():
    db = FakeDB()
    store = make_store(db)

    assert store.stamp_last_build("acme", "2", "2026-09-01") == (True, "")
    call = db.one("UPDATE")
    assert "SET last_build = %s" in call.sql
    assert call.params == ("2026-09-01", REGISTRY_ID, "acme", "2")

    db.on("UPDATE", raises=RuntimeError("e"))
    assert store.stamp_last_build("acme", "2", "x") == (False, "e")


def test_load_build_runs_filters_version_and_maps_rows(fake_psycopg):
    db = FakeDB().on("FROM \"ontobricks_registry\".build_runs b", rows=[_run_row()])

    runs = make_store(db).load_build_runs("acme", version="2", limit="5")

    assert runs == [
        {
            "id": 7,
            "version": "2",
            "build_kind": "session",
            "status": "success",
            "message": "",
            "error": "",
            "started_at": _T0.isoformat(),
            "finished_at": _T1.isoformat(),
            "duration_s": 12.5,
            "triple_count": 1000,
            "entity_count": 10,
            "relationship_count": 20,
            "sql_chars": 300,
            "graph_engine": "",
            "sync_mode": "full",
            "view_table": "c.s.v",
            "graph_name": "g",
            "task_id": "t1",
            "phase_times": {"sql": 1.0},
            "stats": {},
        }
    ]
    call = db.calls[0]
    assert "b.version = %s" in call.sql
    assert "ORDER BY b.started_at DESC, b.id DESC LIMIT %s" in call.sql
    assert call.params == (REGISTRY_ID, "acme", "2", 5)
    assert call.row_factory is DICT_ROW


def test_load_build_runs_without_version_and_on_error(fake_psycopg):
    db = FakeDB()
    store = make_store(db)
    assert store.load_build_runs("acme") == []
    assert "b.version" not in db.calls[0].sql.split("WHERE")[1]
    assert db.calls[0].params == (REGISTRY_ID, "acme", 100)

    db.on("build_runs b", raises=RuntimeError("e"))
    assert store.load_build_runs("acme") == []
    unavailable = make_store(FakeDB().on("information_schema", raises=RuntimeError()), ready=False)
    assert unavailable.load_build_runs("acme") == []


def test_load_all_build_runs_paginates_and_counts(fake_psycopg):
    db = FakeDB()
    db.on("SELECT COUNT(*) AS total", rows=[{"total": 42}])
    db.on("d.folder AS domain", rows=[_run_row(domain="acme"), _run_row(id=8, domain=None)])

    entries, total = make_store(db).load_all_build_runs(folder="acme", limit=2, offset=4)

    assert total == 42
    assert [e["domain"] for e in entries] == ["acme", ""]
    count, page = db.calls
    assert count.params == (REGISTRY_ID, "acme")
    assert "LIMIT %s OFFSET %s" in page.sql
    assert page.params == (REGISTRY_ID, "acme", 2, 4)


def test_load_all_build_runs_tuple_count_rows_and_errors(fake_psycopg):
    db = FakeDB().on("SELECT COUNT(*) AS total", rows=[(3,)])
    entries, total = make_store(db).load_all_build_runs()
    assert (entries, total) == ([], 3)
    assert "d.folder = %s" not in db.calls[0].sql

    db = FakeDB().on("SELECT COUNT(*)", raises=RuntimeError("e"))
    assert make_store(db).load_all_build_runs() == ([], 0)
    unavailable = make_store(FakeDB().on("information_schema", raises=RuntimeError()), ready=False)
    assert unavailable.load_all_build_runs() == ([], 0)


def test_build_analytics_rolls_up_success_rate_active_build_and_versions(fake_psycopg):
    db = FakeDB()
    db.on(
        "AS total_runs, count(*) FILTER",
        rows=[
            {
                "total_runs": 4,
                "success_runs": 3,
                "failed_runs": 1,
                "avg_duration_s": 10.0,
                "min_duration_s": 5,
                "max_duration_s": None,
            }
        ],
    )
    db.on("AND b.status = 'success' ORDER BY", rows=[_run_row(triple_count=555)])
    db.on(
        "GROUP BY b.version",
        rows=[
            {"version": "2", "total_runs": 3, "success_runs": 2, "last_status": "success", "last_triple_count": 555, "last_run": _T1},
            {"version": "1", "total_runs": 1, "success_runs": 1, "last_status": None, "last_triple_count": None, "last_run": None},
        ],
    )

    result = make_store(db).build_analytics("acme", version="2")

    assert result["total_runs"] == 4
    assert result["success_rate"] == 0.75
    assert result["max_duration_s"] == 0.0
    assert result["active_build"]["triple_count"] == 555
    assert result["last_triple_count"] == 555
    assert result["per_version"] == [
        {"version": "2", "total_runs": 3, "success_runs": 2, "last_status": "success", "last_triple_count": 555, "last_run": _T1.isoformat()},
        {"version": "1", "total_runs": 1, "success_runs": 1, "last_status": "", "last_triple_count": 0, "last_run": ""},
    ]
    assert all(c.params == (REGISTRY_ID, "acme", "2") for c in db.calls)


def test_build_analytics_empty_scope_and_errors(fake_psycopg):
    result = make_store(FakeDB()).build_analytics("acme")
    assert result["total_runs"] == 0 and result["success_rate"] == 0.0
    assert result["active_build"] is None and result["per_version"] == []

    db = FakeDB().on("AS total_runs", raises=RuntimeError("e"))
    assert make_store(db).build_analytics("acme") == make_store(db)._empty_analytics()
    unavailable = make_store(FakeDB().on("information_schema", raises=RuntimeError()), ready=False)
    assert unavailable.build_analytics("acme")["total_runs"] == 0


# ---------------------------------------------------------------------------
# Graph analytics cache
# ---------------------------------------------------------------------------


def test_save_graph_analytics_upserts_one_row_per_version():
    db = FakeDB().on("INSERT INTO", rowcount=1)
    entry = {"stats": {"nodes": 3}, "top_pagerank": [["a", 0.5]], "duration_ms": "12"}

    make_store(db).save_graph_analytics("acme", 2, entry)

    call = db.one("INSERT INTO")
    assert f"INSERT INTO {QSCHEMA}.graph_analytics" in call.sql
    assert "ON CONFLICT (domain_id, version) DO UPDATE" in call.sql
    p = call.params
    assert p[:3] == ("2", "completed", "")
    assert json.loads(p[3]) == [] and json.loads(p[4]) == {"nodes": 3}
    assert json.loads(p[5]) == [["a", 0.5]] and json.loads(p[6]) == {}
    assert p[9] == 12 and p[10] is None
    assert p[-2:] == (REGISTRY_ID, "acme")


def test_save_graph_analytics_tolerates_missing_domain_errors_and_missing_table():
    make_store(FakeDB().on("INSERT", rowcount=0)).save_graph_analytics("g", "1", {})
    make_store(FakeDB().on("INSERT", raises=RuntimeError())).save_graph_analytics("g", "1", {})
    db = FakeDB().on("information_schema", raises=RuntimeError())
    make_store(db, ready=False).save_graph_analytics("g", "1", {})
    assert not db.sql_with("INSERT")


def test_load_graph_analytics_maps_row(fake_psycopg):
    row = {
        "version": "2",
        "status": None,
        "graph_name": None,
        "class_filter": ["A"],
        "stats": None,
        "top_pagerank": None,
        "result": {"r": 1},
        "error": None,
        "task_id": None,
        "duration_ms": None,
        "computed_at": _T0,
    }
    db = FakeDB().on("FROM \"ontobricks_registry\".graph_analytics g", rows=[row])

    out = make_store(db).load_graph_analytics("acme", "2")

    assert out == {
        "version": "2",
        "status": "completed",
        "graph_name": "",
        "class_filter": ["A"],
        "stats": {},
        "top_pagerank": [],
        "result": {"r": 1},
        "error": "",
        "task_id": "",
        "duration_ms": 0,
        "computed_at": _T0.isoformat(),
    }
    assert db.calls[0].params == (REGISTRY_ID, "acme", "2")


def test_load_graph_analytics_miss_error_and_missing_table(fake_psycopg):
    db = FakeDB()
    assert make_store(db).load_graph_analytics("a", "1") is None
    db.on("graph_analytics g", raises=RuntimeError())
    assert make_store(db).load_graph_analytics("a", "1") is None
    bad = make_store(FakeDB().on("information_schema", raises=RuntimeError()), ready=False)
    assert bad.load_graph_analytics("a", "1") is None


# ---------------------------------------------------------------------------
# Graph analytics run history
# ---------------------------------------------------------------------------


def test_record_graph_analytics_run_inserts_history_row():
    db = FakeDB()
    entry = {"node_count": "5", "avg_degree": "1.5", "class_filter": ["A"]}

    make_store(db).record_graph_analytics_run("acme", 2, entry)

    ins = db.one("INSERT INTO")
    assert f"INSERT INTO {QSCHEMA}.graph_analytics_runs" in ins.sql
    p = ins.params
    assert p[:4] == ("2", "completed", json.dumps(["A"]), 5)
    assert p[6] == 1.5
    assert p[-2:] == (REGISTRY_ID, "acme")


def test_record_graph_analytics_run_prunes_history_beyond_cap():
    db = FakeDB()

    make_store(db).record_graph_analytics_run("acme", "2", {})

    prune = db.one("DELETE FROM")
    assert "OFFSET %s" in prune.sql
    assert prune.params[:3] == (REGISTRY_ID, "acme", "2")


def test_record_graph_analytics_run_noop_without_table():
    db = FakeDB().on("information_schema", raises=RuntimeError())

    make_store(db, ready=False).record_graph_analytics_run("acme", "2", {})

    assert not db.sql_with("INSERT")


def _analytics_run_row(**over):
    row = {
        "id": 3,
        "version": "2",
        "status": None,
        "class_filter": None,
        "node_count": 10,
        "edge_count": None,
        "connected_components": 2,
        "avg_degree": 1.25,
        "density": None,
        "duration_ms": 50,
        "task_id": None,
        "error": None,
        "computed_at": None,
    }
    row.update(over)
    return row


def test_load_graph_analytics_runs_filters_and_maps(fake_psycopg):
    db = FakeDB().on("graph_analytics_runs r", rows=[_analytics_run_row()])

    runs = make_store(db).load_graph_analytics_runs("acme", "2", limit=3)

    assert runs == [
        {
            "id": 3,
            "version": "2",
            "status": "completed",
            "class_filter": [],
            "node_count": 10,
            "edge_count": 0,
            "connected_components": 2,
            "avg_degree": 1.25,
            "density": 0.0,
            "duration_ms": 50,
            "task_id": "",
            "error": "",
            "computed_at": "",
        }
    ]
    call = db.calls[0]
    assert "AND r.version = %s" in call.sql
    assert "ORDER BY r.computed_at DESC, r.id DESC LIMIT %s" in call.sql
    assert call.params == (REGISTRY_ID, "acme", "2", 3)


def test_load_graph_analytics_runs_all_versions_error_and_missing_table(fake_psycopg):
    db = FakeDB()
    assert make_store(db).load_graph_analytics_runs("acme") == []
    assert db.calls[0].params == (REGISTRY_ID, "acme", 100)
    db.on("graph_analytics_runs r", raises=RuntimeError())
    assert make_store(db).load_graph_analytics_runs("acme") == []
    bad = make_store(FakeDB().on("information_schema", raises=RuntimeError()), ready=False)
    assert bad.load_graph_analytics_runs("acme") == []


def test_load_all_graph_analytics_runs_paginates(fake_psycopg):
    db = FakeDB()
    db.on("SELECT COUNT(*) AS total", rows=[{"total": 9}])
    db.on("d.folder AS domain", rows=[_analytics_run_row(domain="acme", computed_at=_T0)])

    entries, total = make_store(db).load_all_graph_analytics_runs(folder="acme", limit=1, offset=2)

    assert total == 9
    assert entries[0]["domain"] == "acme"
    assert entries[0]["computed_at"] == _T0.isoformat()
    assert db.calls[1].params == (REGISTRY_ID, "acme", 1, 2)


def test_load_all_graph_analytics_runs_unscoped_error_and_missing_table(fake_psycopg):
    db = FakeDB()
    assert make_store(db).load_all_graph_analytics_runs() == ([], 0)
    assert db.calls[0].params == (REGISTRY_ID,)
    db.on("SELECT COUNT(*)", raises=RuntimeError())
    assert make_store(db).load_all_graph_analytics_runs() == ([], 0)
    bad = make_store(FakeDB().on("information_schema", raises=RuntimeError()), ready=False)
    assert bad.load_all_graph_analytics_runs() == ([], 0)
