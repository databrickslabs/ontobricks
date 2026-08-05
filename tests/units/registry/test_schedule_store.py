"""The scheduled-task store contract.

Schedules used to be one row per domain, build-only. They are now keyed
on ``(task_type, domain, target_key)`` so a domain can host a build, a
cohort materialisation, an analytics run, and an inference run at once.
These tests pin the three parts of that change that a live registry
depends on: the key encoding, the SQL that reads and writes the wider
rows, and the two migrations that move existing deployments onto it
(the column/constraint widening, and lifting cohort schedules out of
the ``global_config`` JSONB blob).

The Lakebase store is driven through a scripted cursor, so nothing here
needs a real Postgres.
"""

from __future__ import annotations

import json
from contextlib import contextmanager

import pytest

from back.objects.registry.RegistryService import RegistryCfg
from back.objects.registry.store.base import parse_schedule_key, schedule_key

pytestmark = pytest.mark.unit

CFG = RegistryCfg(catalog="cat", schema="sch", volume="vol")


class _Cursor:
    """Cursor stand-in that records SQL and replays scripted results."""

    def __init__(self, script=None):
        self._script = list(script or [])
        self.executed = []
        self._one = None
        self._all = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        for entry in self._script:
            if entry["contains"] in sql and not entry.get("_used"):
                entry["_used"] = True
                self._one = entry.get("fetchone")
                self._all = entry.get("fetchall", [])
                self.rowcount = entry.get("rowcount", 0)
                return
        self._one = None
        self._all = []
        self.rowcount = 0

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._all

    def sql_containing(self, needle):
        return [sql for sql, _ in self.executed if needle in sql]

    def params_for(self, needle):
        return [params for sql, params in self.executed if needle in sql]


class _Conn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self, *args, **kwargs):
        return self._cursor


def _store(monkeypatch, cursor, *, migrated=True, cohorts_imported=True):
    """A LakebaseRegistryStore wired to *cursor* instead of Postgres."""
    monkeypatch.setenv("PGHOST", "test-host")
    monkeypatch.setenv("PGPORT", "5432")
    monkeypatch.setenv("PGDATABASE", "ontobricks_registry")
    monkeypatch.setenv("PGUSER", "sp-test")

    from back.objects.registry.store.lakebase import LakebaseRegistryStore

    store = LakebaseRegistryStore(registry_cfg=CFG, schema="reg")
    store._registry_id = "rid-1"
    store._schedule_columns_ready = migrated
    store._cohort_schedules_imported = cohorts_imported

    @contextmanager
    def _connect():
        yield _Conn(cursor)

    monkeypatch.setattr(store, "_connect", _connect)
    return store


# ---------------------------------------------------------------------
# Key encoding
# ---------------------------------------------------------------------


class TestScheduleKey:
    def test_round_trips(self):
        key = schedule_key("cohort", "Acme", "rule_42")
        assert parse_schedule_key(key) == ("cohort", "Acme", "rule_42")

    def test_a_typeless_target_is_an_empty_segment(self):
        assert parse_schedule_key(schedule_key("build", "Acme")) == (
            "build",
            "Acme",
            "",
        )

    def test_a_bare_domain_reads_as_a_build(self):
        """Pre-generic deployments (and the Volume migration script) key
        schedules on the domain name alone."""
        assert parse_schedule_key("Acme") == ("build", "Acme", "")

    def test_four_types_can_coexist_on_one_domain(self):
        keys = {
            schedule_key(t, "Acme")
            for t in ("build", "cohort", "analytics", "reasoning")
        }
        assert len(keys) == 4


# ---------------------------------------------------------------------
# Read / write
# ---------------------------------------------------------------------


class TestLoadSchedules:
    def test_rows_are_keyed_on_type_domain_and_target(self, monkeypatch):
        cur = _Cursor(
            [
                {
                    "contains": "FROM \"reg\".schedules",
                    "fetchall": [
                        {
                            "task_type": "cohort",
                            "domain_name": "Acme",
                            "target_key": "rule_1",
                            "interval_minutes": 30,
                            "enabled": True,
                            "version": "latest",
                            "config": {"output_graph": True, "output_uc": False},
                            "last_run": None,
                            "last_status": None,
                            "last_message": None,
                            "last_count": 7,
                        }
                    ],
                }
            ]
        )
        store = _store(monkeypatch, cur)

        out = store.load_schedules()

        assert list(out) == ["cohort::Acme::rule_1"]
        entry = out["cohort::Acme::rule_1"]
        assert entry["task_type"] == "cohort"
        assert entry["target_key"] == "rule_1"
        assert entry["config"] == {"output_graph": True, "output_uc": False}
        assert entry["last_count"] == 7


class TestSaveSchedules:
    def test_config_is_written_as_json(self, monkeypatch):
        cur = _Cursor()
        store = _store(monkeypatch, cur)

        ok, _msg = store.save_schedules(
            {
                "reasoning::Acme::": {
                    "task_type": "reasoning",
                    "domain_name": "Acme",
                    "target_key": "",
                    "interval_minutes": 120,
                    "enabled": True,
                    "version": "3",
                    "config": {"phases": {"swrl": True}, "materialize_graph": True},
                }
            }
        )

        assert ok
        params = cur.params_for("INSERT INTO")[0]
        assert params[1] == "reasoning"
        assert params[2] == "Acme"
        assert params[3] == ""
        assert json.loads(params[8]) == {
            "phases": {"swrl": True},
            "materialize_graph": True,
        }

    def test_the_key_supplies_the_identity_when_the_entry_omits_it(self, monkeypatch):
        """The Volume → Lakebase migration script writes bare domain keys."""
        cur = _Cursor()
        store = _store(monkeypatch, cur)

        store.save_schedules({"Acme": {"interval_minutes": 60, "enabled": True}})

        params = cur.params_for("INSERT INTO")[0]
        assert (params[1], params[2], params[3]) == ("build", "Acme", "")

    def test_a_legacy_top_level_drop_existing_is_folded_into_config(self, monkeypatch):
        cur = _Cursor()
        store = _store(monkeypatch, cur)

        store.save_schedules(
            {"Acme": {"interval_minutes": 60, "drop_existing": False}}
        )

        params = cur.params_for("INSERT INTO")[0]
        assert json.loads(params[8]) == {"drop_existing": False}


class TestScheduleHistory:
    def test_reads_are_scoped_to_the_whole_composite_key(self, monkeypatch):
        cur = _Cursor()
        store = _store(monkeypatch, cur)

        store.load_schedule_history("cohort::Acme::rule_1")

        sql, params = cur.executed[0]
        assert "task_type = %s" in sql and "target_key = %s" in sql
        assert params == ("rid-1", "cohort", "Acme", "rule_1")

    def test_type_specific_counters_live_in_detail(self, monkeypatch):
        cur = _Cursor()
        store = _store(monkeypatch, cur)

        store.append_schedule_history(
            "cohort::Acme::rule_1",
            {
                "timestamp": "2026-01-01T00:00:00+00:00",
                "status": "success",
                "triple_count": 5,
                "detail": {"uc_rows_written": 12},
            },
        )

        params = cur.params_for("INSERT INTO")[0]
        assert json.loads(params[-1]) == {"uc_rows_written": 12}

    def test_the_cap_only_trims_the_same_schedule(self, monkeypatch):
        cur = _Cursor()
        store = _store(monkeypatch, cur)

        store.append_schedule_history(
            "build::Acme::", {"status": "success"}, max_entries=3
        )

        delete_sql = cur.sql_containing("DELETE FROM")[0]
        assert delete_sql.count("task_type = %s") == 2
        assert delete_sql.count("target_key = %s") == 2
        assert cur.params_for("DELETE FROM")[0][-1] == 3


# ---------------------------------------------------------------------
# Migrations
# ---------------------------------------------------------------------


class TestColumnMigration:
    def test_no_ddl_when_the_column_already_exists(self, monkeypatch):
        cur = _Cursor([{"contains": "information_schema.columns", "fetchone": (1,)}])
        store = _store(monkeypatch, cur, migrated=False)

        assert store._ensure_schedule_task_columns() is True
        assert cur.sql_containing("ALTER TABLE") == []

    def test_the_widening_adds_every_column_and_swaps_the_constraint(
        self, monkeypatch
    ):
        cur = _Cursor(
            [
                {
                    "contains": "pg_get_constraintdef",
                    "fetchall": [("schedules_registry_id_domain_name_key",)],
                }
            ]
        )
        store = _store(monkeypatch, cur, migrated=False)

        assert store._ensure_schedule_task_columns() is True

        all_sql = " ".join(sql for sql, _ in cur.executed)
        for column in ("task_type", "target_key", "config", "last_count", "detail"):
            assert column in all_sql
        assert "DROP CONSTRAINT IF EXISTS" in all_sql
        assert "schedules_type_domain_target_key" in all_sql

    def test_the_legacy_drop_existing_column_is_folded_into_config(self, monkeypatch):
        cur = _Cursor()
        store = _store(monkeypatch, cur, migrated=False)

        store._ensure_schedule_task_columns()

        update = [s for s in cur.sql_containing("UPDATE") if "config" in s]
        assert update, "the legacy build flag should move into the config blob"
        assert "drop_existing" in update[0]

    def test_a_permission_failure_is_reported_not_raised(self, monkeypatch):
        class _Boom(_Cursor):
            def execute(self, sql, params=None):
                if "ALTER TABLE" in sql:
                    raise RuntimeError("must be owner of table schedules")
                super().execute(sql, params)

        store = _store(monkeypatch, _Boom(), migrated=False)
        assert store._ensure_schedule_task_columns() is False


class TestLegacyCohortImport:
    def _blob(self):
        return {
            "contains": "FROM \"reg\".global_config",
            "fetchone": {
                "config": {
                    "cohort_schedules": {
                        "Acme::rule_1": {
                            "domain_name": "Acme",
                            "rule_id": "rule_1",
                            "interval_minutes": 45,
                            "enabled": True,
                            "version": "2",
                            "output_graph": True,
                            "output_uc": False,
                            "last_status": "success",
                        }
                    },
                    "cohort_schedule_history": {
                        "Acme::rule_1": [
                            {
                                "timestamp": "2026-01-01T00:00:00+00:00",
                                "status": "success",
                                "materialized_triples": 4,
                                "uc_rows_written": 0,
                            }
                        ]
                    },
                }
            },
        }

    def test_blob_entries_become_cohort_rows(self, monkeypatch):
        cur = _Cursor([self._blob()])
        store = _store(monkeypatch, cur, cohorts_imported=False)

        store._import_legacy_cohort_schedules()

        inserts = cur.params_for("INSERT INTO \"reg\".schedules")
        assert len(inserts) == 1
        params = inserts[0]
        assert params[1] == "Acme"
        assert params[2] == "rule_1"
        assert params[3] == 45
        assert json.loads(params[6]) == {"output_graph": True, "output_uc": False}

    def test_run_history_comes_across_too(self, monkeypatch):
        cur = _Cursor([self._blob()])
        store = _store(monkeypatch, cur, cohorts_imported=False)

        store._import_legacy_cohort_schedules()

        runs = cur.params_for("INSERT INTO \"reg\".schedule_runs")
        assert len(runs) == 1
        assert json.loads(runs[0][-1])["materialized_triples"] == 4

    def test_the_blob_is_scrubbed_afterwards(self, monkeypatch):
        cur = _Cursor([self._blob()])
        store = _store(monkeypatch, cur, cohorts_imported=False)

        store._import_legacy_cohort_schedules()

        scrub = [s for s in cur.sql_containing("UPDATE") if "global_config" in s]
        assert scrub
        assert "cohort_schedules" in scrub[0]
        assert "cohort_schedule_history" in scrub[0]

    def test_an_empty_blob_writes_nothing(self, monkeypatch):
        cur = _Cursor([{"contains": "global_config", "fetchone": {"config": {}}}])
        store = _store(monkeypatch, cur, cohorts_imported=False)

        store._import_legacy_cohort_schedules()

        assert cur.sql_containing("INSERT INTO") == []
        assert store._cohort_schedules_imported is True

    def test_the_reader_hides_every_legacy_schedule_key(self, monkeypatch):
        """``load_global_config`` must not hand schedule state back to
        callers — the tables own it now."""
        cur = _Cursor(
            [
                {
                    "contains": "global_config",
                    "fetchone": {
                        "config": {
                            "warehouse_id": "w1",
                            "schedules": {"a": {}},
                            "schedule_history": {"a": []},
                            "cohort_schedules": {"b": {}},
                            "cohort_schedule_history": {"b": []},
                        }
                    },
                }
            ]
        )
        store = _store(monkeypatch, cur)

        assert store.load_global_config() == {"warehouse_id": "w1"}
