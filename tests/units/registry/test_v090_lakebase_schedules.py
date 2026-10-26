"""Real SQL of :class:`Schedules` against a scripted cursor.

Complements ``test_schedule_store.py`` with the failure paths and the
legacy cohort import edge cases (key fallback, malformed keys, migration
unavailable) of the generic scheduled-task table.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from ._v090_fake_pg import (
    DICT_ROW,
    REGISTRY_ID,
    FakeDB,
    fake_psycopg,  # noqa: F401
    make_store,
)

pytestmark = pytest.mark.unit

_T0 = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------


def test_load_schedules_keys_rows_by_type_domain_target(fake_psycopg):
    rows = [
        {
            "task_type": None,
            "domain_name": "acme",
            "target_key": None,
            "interval_minutes": 30,
            "enabled": True,
            "version": None,
            "config": None,
            "last_run": _T0,
            "last_status": "success",
            "last_message": "ok",
            "last_count": None,
        },
        {
            "task_type": "cohort",
            "domain_name": "acme",
            "target_key": "rule-1",
            "interval_minutes": 60,
            "enabled": False,
            "version": "3",
            "config": {"output_uc": True},
            "last_run": None,
            "last_status": None,
            "last_message": None,
            "last_count": 9,
        },
    ]
    db = FakeDB().on("FROM \"ontobricks_registry\".schedules WHERE registry_id", rows=rows)

    out = make_store(db).load_schedules()

    build_key, cohort_key = list(out)
    assert out[build_key] == {
        "task_type": "build",
        "domain_name": "acme",
        "target_key": "",
        "interval_minutes": 30,
        "enabled": True,
        "version": "latest",
        "config": {},
        "last_run": _T0.isoformat(),
        "last_status": "success",
        "last_message": "ok",
        "last_count": 0,
    }
    assert out[cohort_key]["target_key"] == "rule-1"
    assert out[cohort_key]["last_run"] is None
    assert db.calls[0].params == (REGISTRY_ID,)
    assert db.calls[0].row_factory is DICT_ROW


def test_load_schedules_returns_empty_on_error(fake_psycopg):
    db = FakeDB().on("schedules WHERE", raises=RuntimeError("down"))

    assert make_store(db).load_schedules() == {}


def test_save_schedules_replaces_rows_and_folds_legacy_drop_flag():
    db = FakeDB()
    schedules = {
        "acme": {"interval_minutes": "15", "drop_existing": False},
        "cohort::acme::rule-1": {"enabled": False, "config": {"output_uc": True}, "last_count": None},
    }

    ok, msg = make_store(db).save_schedules(schedules)

    assert (ok, msg) == (True, "Schedules saved")
    assert db.one("DELETE FROM").params == (REGISTRY_ID,)
    first, second = db.sql_with("INSERT INTO")
    assert first.params[:6] == (REGISTRY_ID, "build", "acme", "", 15, False)
    assert json.loads(first.params[8]) == {"drop_existing": False}
    assert first.params[7] == "latest"
    assert second.params[1:4] == ("cohort", "acme", "rule-1")
    assert second.params[5:7] == (True, False)
    assert second.params[-1] == 0


def test_save_schedules_reports_errors():
    db = FakeDB().on("DELETE FROM", raises=RuntimeError("perm"))

    assert make_store(db).save_schedules({}) == (False, "perm")


def test_load_schedule_history_maps_runs_in_time_order(fake_psycopg):
    rows = [{"run_ts": _T0, "status": "success", "message": None, "duration_s": None, "triple_count": 5, "detail": None}]
    db = FakeDB().on("FROM \"ontobricks_registry\".schedule_runs", rows=rows)

    hist = make_store(db).load_schedule_history("cohort::acme::rule-1")

    assert hist == [
        {
            "timestamp": _T0.isoformat(),
            "status": "success",
            "message": "",
            "duration_s": 0.0,
            "triple_count": 5,
            "detail": {},
        }
    ]
    assert "ORDER BY run_ts ASC" in db.calls[0].sql
    assert db.calls[0].params == (REGISTRY_ID, "cohort", "acme", "rule-1")


def test_load_schedule_history_returns_empty_on_error(fake_psycopg):
    db = FakeDB().on("schedule_runs", raises=RuntimeError())

    assert make_store(db).load_schedule_history("acme") == []


def test_append_schedule_history_inserts_then_caps_per_task():
    db = FakeDB()

    make_store(db).append_schedule_history(
        "acme", {"status": "success", "duration_s": "2", "detail": {"x": 1}}, max_entries=5
    )

    ins = db.one("INSERT INTO")
    assert ins.params[:5] == (REGISTRY_ID, "build", "acme", "", None)
    assert ins.params[7] == 2.0
    assert json.loads(ins.params[9]) == {"x": 1}
    prune = db.one("DELETE FROM")
    assert "ORDER BY run_ts DESC LIMIT %s" in prune.sql
    assert prune.params == (REGISTRY_ID, "build", "acme", "", REGISTRY_ID, "build", "acme", "", 5)


def test_append_schedule_history_swallows_errors():
    db = FakeDB().on("INSERT INTO", raises=RuntimeError())

    make_store(db).append_schedule_history("acme", {})

    assert not db.sql_with("DELETE FROM")


# ---------------------------------------------------------------------------
# Legacy cohort import
# ---------------------------------------------------------------------------


def _import_store(db, *, columns_ready=True):
    store = make_store(db)
    store._cohort_schedules_imported = False
    store._schedule_columns_ready = columns_ready
    return store


def test_cohort_import_falls_back_to_legacy_key_and_skips_malformed(fake_psycopg):
    blob = {
        "cohort_schedules": {
            "acme::rule-1": {"interval_minutes": 10, "output_graph": False},
            "nodomain": {},
            "beta::rule-2": {"domain_name": "beta-x", "rule_id": "r2"},
        },
        "cohort_schedule_history": {
            "acme::rule-1": [{"timestamp": "2026-01-01T00:00:00Z", "status": "success", "uc_rows_written": 3}],
            "bad-key": [{"status": "x"}],
        },
    }
    db = FakeDB().on("SELECT config FROM", rows=[{"config": blob}])
    store = _import_store(db)

    store._import_legacy_cohort_schedules()

    sched = db.sql_with("INSERT INTO \"ontobricks_registry\".schedules")
    assert [c.params[1:3] for c in sched] == [("acme", "rule-1"), ("beta-x", "r2")]
    assert "ON CONFLICT ON CONSTRAINT schedules_type_domain_target_key DO NOTHING" in sched[0].sql
    assert json.loads(sched[0].params[6]) == {"output_graph": False, "output_uc": True}
    runs = db.sql_with("INSERT INTO \"ontobricks_registry\".schedule_runs")
    assert len(runs) == 1
    assert runs[0].params[1:3] == ("acme", "rule-1")
    assert json.loads(runs[0].params[-1]) == {"materialized_triples": 0, "uc_rows_written": 3}
    assert db.sql_with("UPDATE \"ontobricks_registry\".global_config")
    assert store._cohort_schedules_imported is True


def test_cohort_import_noop_when_blob_has_no_cohort_keys(fake_psycopg):
    db = FakeDB().on("SELECT config FROM", rows=[{"config": {"warehouse_id": "w"}}])
    store = _import_store(db)

    store._import_legacy_cohort_schedules()

    assert store._cohort_schedules_imported is True
    assert not db.sql_with("INSERT")


def test_cohort_import_waits_for_schedule_migration(fake_psycopg):
    db = FakeDB()
    db.on("SELECT config FROM", rows=[{"config": {"cohort_schedules": {"a::r": {}}}}])
    db.on("information_schema.columns", raises=RuntimeError("not owner"))
    store = _import_store(db, columns_ready=False)

    store._import_legacy_cohort_schedules()

    assert store._cohort_schedules_imported is False, "retried on next start"
    assert not db.sql_with("INSERT")


def test_cohort_import_failure_leaves_blob_intact(fake_psycopg):
    db = FakeDB().on("SELECT config FROM", raises=RuntimeError("down"))
    store = _import_store(db)

    store._import_legacy_cohort_schedules()

    assert store._cohort_schedules_imported is False
    assert not db.sql_with("UPDATE")


# ---------------------------------------------------------------------------
# Generic-task migration
# ---------------------------------------------------------------------------


def test_schedule_migration_swaps_legacy_unique_constraint():
    db = FakeDB().on("FROM pg_constraint", rows=[("schedules_registry_id_domain_name_key",)])
    store = make_store(db)
    store._schedule_columns_ready = False

    assert store._ensure_schedule_task_columns() is True
    drop = db.one("DROP CONSTRAINT IF EXISTS")
    assert '"schedules_registry_id_domain_name_key"' in drop.sql
    assert db.sql_with("ADD CONSTRAINT schedules_type_domain_target_key")
    assert db.sql_with("jsonb_build_object( 'drop_existing'")
    assert store._schedule_columns_ready is True


def test_schedule_migration_failure_returns_false():
    db = FakeDB().on("ALTER TABLE", raises=RuntimeError("must be owner"))
    store = make_store(db)
    store._schedule_columns_ready = False

    assert store._ensure_schedule_task_columns() is False
    assert store._schedule_columns_ready is False
