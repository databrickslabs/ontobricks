"""Contract tests for the registry-store abstraction.

These tests use a lightweight in-memory fake (no Databricks/Postgres
dependencies) to validate:

- :class:`RegistryFactory` returns the Lakebase store without eagerly
  importing :mod:`psycopg`.
- Every concrete :class:`RegistryStore` agrees on the public interface
  (method names + return shapes).
- :class:`LakebaseRegistryStore` honours the registry-identity model
  (one schema = one registry, with legacy adoption) and surfaces
  initialisation problems explicitly via :meth:`init_status`.

Lakebase-only behaviour that requires a real Postgres connection lives
in the gated ``tests/integration/`` suite.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple
from unittest.mock import MagicMock

import pytest

from back.objects.registry import RegistryCfg
from back.objects.registry.store import (
    RegistryFactory,
    RegistryStore,
)
from back.objects.registry.store.base import DomainSummary, ScheduleHistoryEntry


CFG = RegistryCfg(catalog="cat", schema="sch", volume="vol")


# ---------------------------------------------------------------------
# Fake in-memory store — used as both source and destination
# ---------------------------------------------------------------------


class _InMemoryStore(RegistryStore):
    """Minimal in-memory implementation used for migration round-trips.

    Just enough behaviour to exercise the public surface used by
    the store contract tests below.
    """

    def __init__(self, tag: str = "memory"):
        self._tag = tag
        self._versions: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._perms: Dict[str, Dict[str, Any]] = {}
        self._schedules: Dict[str, Dict[str, Any]] = {}
        self._history: Dict[str, List[ScheduleHistoryEntry]] = {}
        self._build_runs: Dict[str, List[Dict[str, Any]]] = {}
        self._review_events: List[Dict[str, Any]] = []
        self._comments: List[Dict[str, Any]] = []
        self._tasks: List[Dict[str, Any]] = []
        self._global: Dict[str, Any] = {}
        self._initialized = False

    @property
    def backend(self) -> str:
        return self._tag

    @property
    def cache_key(self) -> str:
        return f"{self._tag}:cat.sch.vol"

    def is_initialized(self) -> bool:
        return self._initialized

    def initialize(self, *, client: Any = None) -> Tuple[bool, str]:
        self._initialized = True
        return True, "ok"

    def list_domain_folders(self) -> Tuple[bool, List[str], str]:
        folders = sorted({f for (f, _) in self._versions.keys()})
        return True, folders, "ok"

    def list_domains_with_metadata(self) -> Tuple[bool, List[DomainSummary], str]:
        ok, folders, msg = self.list_domain_folders()
        return ok, [{"name": f, "versions": []} for f in folders], msg

    def domain_exists(self, folder: str) -> bool:
        return any(f == folder for (f, _) in self._versions.keys())

    def get_domain_quorum(self, folder: str) -> int:
        for (f, v), data in self._versions.items():
            if f == folder:
                info = data.get("info", {}) or {}
                return max(1, int(info.get("review_quorum") or 1))
        return 1

    def delete_domain(self, folder: str) -> List[str]:
        for key in [k for k in self._versions if k[0] == folder]:
            self._versions.pop(key, None)
        self._perms.pop(folder, None)
        self._history.pop(folder, None)
        return []

    def list_versions(self, folder: str) -> Tuple[bool, List[str], str]:
        versions = sorted(v for (f, v) in self._versions if f == folder)
        return True, versions, "ok"

    def read_version(
        self, folder: str, version: str
    ) -> Tuple[bool, Dict[str, Any], str]:
        data = self._versions.get((folder, version))
        if data is None:
            return False, {}, f"missing {folder}/{version}"
        return True, dict(data), "ok"

    def write_version(
        self, folder: str, version: str, data: Dict[str, Any]
    ) -> Tuple[bool, str]:
        self._versions[(folder, version)] = dict(data)
        return True, "ok"

    def delete_version(self, folder: str, version: str) -> Tuple[bool, str]:
        self._versions.pop((folder, version), None)
        return True, "ok"

    def update_version_status(
        self, folder: str, version: str, status: str
    ) -> Tuple[bool, str]:
        data = self._versions.get((folder, version))
        if data is None:
            return False, f"missing {folder}/{version}"
        data.setdefault("info", {})["status"] = status
        return True, "ok"

    def load_domain_permissions(self, folder: str) -> Dict[str, Any]:
        return dict(self._perms.get(folder, {"version": 1, "permissions": []}))

    def save_domain_permissions(
        self, folder: str, data: Dict[str, Any]
    ) -> Tuple[bool, str]:
        self._perms[folder] = dict(data)
        return True, "ok"

    def load_schedules(self) -> Dict[str, Dict[str, Any]]:
        return {k: dict(v) for k, v in self._schedules.items()}

    def save_schedules(
        self, schedules: Dict[str, Dict[str, Any]]
    ) -> Tuple[bool, str]:
        self._schedules = {k: dict(v) for k, v in schedules.items()}
        return True, "ok"

    def load_schedule_history(self, folder: str) -> List[ScheduleHistoryEntry]:
        return list(self._history.get(folder, []))

    def append_schedule_history(
        self, folder: str, entry: ScheduleHistoryEntry, *, max_entries: int = 50
    ) -> None:
        bucket = self._history.setdefault(folder, [])
        bucket.append(dict(entry))
        if len(bucket) > max_entries:
            del bucket[: len(bucket) - max_entries]

    def record_build_run(self, folder: str, entry: Dict[str, Any]) -> None:
        runs = self._build_runs.setdefault(folder, [])
        row = dict(entry)
        row.setdefault("id", len(runs) + 1)
        runs.append(row)

    def load_build_runs(
        self, folder: str, *, version=None, limit: int = 100
    ) -> List[Dict[str, Any]]:
        runs = [
            dict(r)
            for r in self._build_runs.get(folder, [])
            if version is None or r.get("version") == version
        ]
        runs.reverse()  # newest-first
        return runs[:limit]

    def build_analytics(self, folder: str, *, version=None) -> Dict[str, Any]:
        runs = [
            r
            for r in self._build_runs.get(folder, [])
            if version is None or r.get("version") == version
        ]
        total = len(runs)
        successes = [r for r in runs if r.get("status") == "success"]
        durations = [float(r.get("duration_s", 0) or 0) for r in successes]
        active = successes[-1] if successes else None
        return {
            "total_runs": total,
            "success_runs": len(successes),
            "failed_runs": total - len(successes),
            "success_rate": (len(successes) / total) if total else 0.0,
            "avg_duration_s": (sum(durations) / len(durations)) if durations else 0.0,
            "min_duration_s": min(durations) if durations else 0.0,
            "max_duration_s": max(durations) if durations else 0.0,
            "last_triple_count": int(active.get("triple_count", 0)) if active else 0,
            "active_build": dict(active) if active else None,
            "per_version": [],
        }

    def record_review_event(
        self,
        folder: str,
        version: str,
        actor: str,
        action: str,
        *,
        from_status: str = "",
        to_status: str = "",
        comment: str = "",
        meta=None,
    ) -> Tuple[bool, str]:
        self._review_events.append(
            {
                "id": str(len(self._review_events) + 1),
                "folder": folder,
                "version": version,
                "actor": actor,
                "action": action,
                "from_status": from_status,
                "to_status": to_status,
                "comment": comment,
                "meta": dict(meta or {}),
                "created_at": f"2026-01-01T00:00:{len(self._review_events):02d}",
            }
        )
        return True, "ok"

    def list_review_events(
        self, folder: str, version=None
    ) -> List[Dict[str, Any]]:
        return [
            dict(e)
            for e in self._review_events
            if e["folder"] == folder
            and (version is None or e["version"] == version)
        ]

    def list_all_review_events(self) -> List[Dict[str, Any]]:
        return [dict(e) for e in self._review_events]

    def insert_comment(
        self,
        folder: str,
        version: str,
        *,
        author: str,
        body: str,
        parent_id=None,
    ):
        row = {
            "id": str(len(self._comments) + 1),
            "folder": folder,
            "version": version,
            "parent_id": str(parent_id) if parent_id else "",
            "author": author or "",
            "body": body or "",
            "resolved": False,
            "created_at": f"2026-01-01T00:00:{len(self._comments):02d}",
        }
        self._comments.append(row)
        return dict(row)

    def list_comments(
        self,
        folder: str,
        version=None,
        *,
        include_resolved: bool = True,
    ) -> List[Dict[str, Any]]:
        return [
            dict(c)
            for c in self._comments
            if c["folder"] == folder
            and (version is None or c["version"] == version)
            and (include_resolved or not c["resolved"])
        ]

    def resolve_comment(
        self, folder: str, comment_id: str, *, resolved: bool = True
    ) -> Tuple[bool, str]:
        for c in self._comments:
            if c["folder"] == folder and c["id"] == str(comment_id):
                c["resolved"] = resolved
                return True, ""
        return False, "Comment not found"

    def insert_task(
        self,
        folder: str,
        version: str,
        *,
        assignee: str,
        created_by: str,
        title: str,
        description: str = "",
        due_date=None,
        comment_id=None,
    ):
        row = {
            "id": str(len(self._tasks) + 1),
            "folder": folder,
            "version": version,
            "assignee": assignee or "",
            "created_by": created_by or "",
            "title": title or "",
            "description": description or "",
            "status": "open",
            "due_date": due_date or "",
            "comment_id": str(comment_id) if comment_id else "",
            "created_at": f"2026-01-01T00:00:{len(self._tasks):02d}",
            "updated_at": f"2026-01-01T00:00:{len(self._tasks):02d}",
        }
        self._tasks.append(row)
        return dict(row)

    def list_tasks(self, folder: str, version=None) -> List[Dict[str, Any]]:
        rows = [
            dict(t)
            for t in self._tasks
            if t["folder"] == folder
            and (version is None or t["version"] == version)
        ]
        rows.reverse()  # newest-first
        return rows

    def list_tasks_for_assignee(self, assignee: str) -> List[Dict[str, Any]]:
        rows = [
            dict(t)
            for t in self._tasks
            if t["assignee"].lower() == (assignee or "").lower()
        ]
        rows.reverse()
        return rows

    def update_task_status(
        self, folder: str, task_id: str, status: str
    ) -> Tuple[bool, str]:
        for t in self._tasks:
            if t["folder"] == folder and t["id"] == str(task_id):
                t["status"] = status
                return True, ""
        return False, "Task not found"

    def load_global_config(self) -> Dict[str, Any]:
        return dict(self._global)

    def save_global_config(self, updates: Dict[str, Any]) -> Tuple[bool, str]:
        self._global.update(updates)
        return True, "ok"

    def domain_folder_id(self, folder: str):
        return folder


# ---------------------------------------------------------------------
# RegistryFactory — single facing entry point for store construction
# ---------------------------------------------------------------------


class TestRegistryFactory:
    def test_lakebase_factory_does_not_eagerly_import_psycopg(self, monkeypatch):
        """The Lakebase backend must be import-safe even when ``psycopg``
        is missing — the actual driver is only required when a method
        that touches Postgres runs (``initialize``/connect/…).
        """
        monkeypatch.setenv("PGHOST", "test-host")
        monkeypatch.setenv("PGPORT", "5432")
        monkeypatch.setenv("PGDATABASE", "ontobricks_registry")
        monkeypatch.setenv("PGUSER", "sp-test")

        store = RegistryFactory.lakebase(
            registry_cfg=CFG, schema="ontobricks_registry"
        )
        from back.objects.registry.store.lakebase import LakebaseRegistryStore

        assert isinstance(store, LakebaseRegistryStore)
        assert store.backend == "lakebase"
        assert store.cache_key.startswith("lakebase:")

    def test_lakebase_database_override_propagates_to_store(self, monkeypatch):
        """``RegistryFactory.lakebase(database=...)`` must store the
        override on the resulting store and surface it both via
        ``describe()`` and the (effective) ``cache_key`` so callers
        like ``RegistryService._build_store`` can route Browse traffic
        to the database the admin actually picked in Settings.
        """
        monkeypatch.setenv("PGHOST", "test-host")
        monkeypatch.setenv("PGPORT", "5432")
        monkeypatch.setenv("PGDATABASE", "ontobricks_registry")
        monkeypatch.setenv("PGUSER", "sp-test")

        store = RegistryFactory.lakebase(
            registry_cfg=CFG,
            schema="ontobricks_registry",
            database="ontobricks_other",
        )
        info = store.describe()
        assert info["database"] == "ontobricks_registry"
        assert info["database_override"] == "ontobricks_other"
        assert info["effective_database"] == "ontobricks_other"
        # The pool key bakes the effective database in, so a cache
        # entry built for the bound DB cannot leak across to a store
        # that points at a different database.
        assert "ontobricks_other" in store.cache_key

    def test_from_cfg_plumbs_database_override(self, monkeypatch):
        """``RegistryFactory.from_cfg`` must forward the
        ``lakebase_database`` override to the store — this is the
        entry point ``RegistryService._build_store`` uses.
        """
        monkeypatch.setenv("PGHOST", "test-host")
        monkeypatch.setenv("PGPORT", "5432")
        monkeypatch.setenv("PGDATABASE", "ontobricks_registry")
        monkeypatch.setenv("PGUSER", "sp-test")

        cfg = RegistryCfg(
            catalog="c",
            schema="s",
            volume="v",
            lakebase_schema="ontobricks_registry",
            lakebase_database="ontobricks_other",
        )

        from back.objects.registry.store.lakebase import LakebaseRegistryStore

        store = RegistryFactory.from_cfg(cfg)
        assert isinstance(store, LakebaseRegistryStore)
        assert store.describe()["effective_database"] == "ontobricks_other"


# ---------------------------------------------------------------------
# RegistryStore contract — every concrete store must satisfy these
# ---------------------------------------------------------------------


@pytest.fixture
def store() -> RegistryStore:
    s = _InMemoryStore("memory")
    s.initialize()
    return s


class TestStoreContract:
    """Behavioural contract every :class:`RegistryStore` implementation
    must honour. Run here against the in-memory fake — the same suite
    is reused against a live Lakebase in ``tests/integration/``.
    """

    def test_initialize_is_idempotent(self, store):
        ok1, _ = store.initialize()
        ok2, _ = store.initialize()
        assert ok1 and ok2 and store.is_initialized()

    def test_unknown_version_returns_false_without_raising(self, store):
        ok, data, msg = store.read_version("ghost", "1")
        assert ok is False
        assert data == {}
        assert msg

    def test_write_then_read_round_trip(self, store):
        payload = {"info": {"name": "demo"}, "versions": [{"version": "1"}]}
        ok, _ = store.write_version("demo", "1", payload)
        assert ok
        ok, got, _ = store.read_version("demo", "1")
        assert ok
        assert got == payload

    def test_domain_listing_excludes_deleted_versions(self, store):
        store.write_version("a", "1", {"info": {}})
        store.write_version("a", "2", {"info": {}})
        store.delete_version("a", "1")
        ok, versions, _ = store.list_versions("a")
        assert ok and versions == ["2"]

    def test_update_version_status_round_trip(self, store):
        store.write_version("demo", "1", {"info": {"name": "demo"}})
        ok, msg = store.update_version_status("demo", "1", "IN-REVIEW")
        assert ok, msg
        ok, got, _ = store.read_version("demo", "1")
        assert ok
        assert got["info"]["status"] == "IN-REVIEW"

    def test_update_version_status_unknown_version(self, store):
        ok, msg = store.update_version_status("ghost", "9", "PUBLISHED")
        assert ok is False
        assert msg

    def test_permissions_default_shape(self, store):
        out = store.load_domain_permissions("nobody")
        assert out == {"version": 1, "permissions": []}

    def test_global_config_merge_is_last_write_wins(self, store):
        store.save_global_config({"warehouse_id": "w1", "schedules": {}})
        store.save_global_config({"warehouse_id": "w2"})
        cfg = store.load_global_config()
        assert cfg["warehouse_id"] == "w2"
        assert cfg["schedules"] == {}

    def test_schedule_history_is_capped(self, store):
        for i in range(5):
            store.append_schedule_history(
                "a", {"timestamp": str(i), "status": "ok"}, max_entries=3
            )
        history = store.load_schedule_history("a")
        assert len(history) == 3
        assert [h["timestamp"] for h in history] == ["2", "3", "4"]

    def test_build_runs_round_trip_newest_first(self, store):
        for i in range(3):
            store.record_build_run(
                "demo",
                {
                    "version": "1",
                    "build_kind": "session",
                    "status": "success",
                    "started_at": str(i),
                    "duration_s": float(i),
                    "triple_count": i * 10,
                },
            )
        runs = store.load_build_runs("demo")
        assert len(runs) == 3
        # Newest-first (last recorded comes back first).
        assert [r["started_at"] for r in runs] == ["2", "1", "0"]

    def test_build_runs_filter_by_version(self, store):
        store.record_build_run("demo", {"version": "1", "status": "success"})
        store.record_build_run("demo", {"version": "2", "status": "success"})
        store.record_build_run("demo", {"version": "2", "status": "error"})
        v2 = store.load_build_runs("demo", version="2")
        assert len(v2) == 2
        assert all(r["version"] == "2" for r in v2)

    def test_build_analytics_aggregates_and_active(self, store):
        store.record_build_run(
            "demo",
            {"version": "1", "status": "success", "duration_s": 2.0,
             "triple_count": 100, "started_at": "1"},
        )
        store.record_build_run(
            "demo",
            {"version": "1", "status": "error", "duration_s": 0.0,
             "started_at": "2"},
        )
        store.record_build_run(
            "demo",
            {"version": "1", "status": "success", "duration_s": 4.0,
             "triple_count": 200, "started_at": "3"},
        )
        a = store.build_analytics("demo")
        assert a["total_runs"] == 3
        assert a["success_runs"] == 2
        assert a["failed_runs"] == 1
        assert abs(a["success_rate"] - (2 / 3)) < 1e-9
        assert abs(a["avg_duration_s"] - 3.0) < 1e-9
        # Active build = latest successful run (triple_count=200).
        assert a["active_build"] is not None
        assert a["last_triple_count"] == 200

    def test_build_analytics_empty_domain(self, store):
        a = store.build_analytics("ghost")
        assert a["total_runs"] == 0
        assert a["active_build"] is None
        assert a["success_rate"] == 0.0

    def test_review_events_round_trip_oldest_first(self, store):
        store.record_review_event(
            "demo", "1", "alice@a.com", "submitted",
            from_status="DRAFT", to_status="IN-REVIEW", comment="go",
        )
        store.record_review_event(
            "demo", "1", "bob@a.com", "approved", comment="lgtm",
        )
        events = store.list_review_events("demo", "1")
        assert [e["action"] for e in events] == ["submitted", "approved"]
        assert events[0]["from_status"] == "DRAFT"
        assert events[0]["to_status"] == "IN-REVIEW"
        assert events[1]["actor"] == "bob@a.com"

    def test_review_events_filter_by_version(self, store):
        store.record_review_event("demo", "1", "a@a.com", "submitted")
        store.record_review_event("demo", "2", "a@a.com", "submitted")
        assert len(store.list_review_events("demo", "1")) == 1
        assert len(store.list_review_events("demo")) == 2

    def test_list_all_review_events_spans_domains(self, store):
        store.record_review_event("demo", "1", "a@a.com", "submitted")
        store.record_review_event("other", "1", "a@a.com", "submitted")
        allev = store.list_all_review_events()
        folders = {e["folder"] for e in allev}
        assert folders == {"demo", "other"}

    def test_comments_round_trip_oldest_first(self, store):
        store.insert_comment(
            "demo", "1", author="a@a.com", body="first",
        )
        store.insert_comment(
            "demo", "1", author="b@a.com", body="second",
        )
        comments = store.list_comments("demo", "1")
        assert [c["body"] for c in comments] == ["first", "second"]
        assert comments[0]["resolved"] is False

    def test_resolve_comment_flips_flag_and_filters(self, store):
        c = store.insert_comment(
            "demo", "1", author="a@a.com", body="hi",
        )
        ok, _ = store.resolve_comment("demo", c["id"], resolved=True)
        assert ok
        assert store.list_comments("demo", "1", include_resolved=False) == []
        assert len(store.list_comments("demo", "1")) == 1

    def test_resolve_unknown_comment_returns_false(self, store):
        ok, msg = store.resolve_comment("demo", "999")
        assert ok is False and msg

    def test_tasks_round_trip_newest_first(self, store):
        store.insert_task(
            "demo", "1", assignee="x@a.com", created_by="a@a.com",
            title="task one",
        )
        store.insert_task(
            "demo", "1", assignee="y@a.com", created_by="a@a.com",
            title="task two",
        )
        tasks = store.list_tasks("demo", "1")
        assert [t["title"] for t in tasks] == ["task two", "task one"]
        assert tasks[0]["status"] == "open"

    def test_tasks_for_assignee_is_case_insensitive_and_cross_domain(self, store):
        store.insert_task(
            "demo", "1", assignee="X@a.com", created_by="a@a.com", title="t1",
        )
        store.insert_task(
            "other", "1", assignee="x@a.com", created_by="a@a.com", title="t2",
        )
        store.insert_task(
            "demo", "1", assignee="z@a.com", created_by="a@a.com", title="t3",
        )
        mine = store.list_tasks_for_assignee("x@a.com")
        assert {t["title"] for t in mine} == {"t1", "t2"}
        assert {t["folder"] for t in mine} == {"demo", "other"}

    def test_update_task_status_round_trip(self, store):
        t = store.insert_task(
            "demo", "1", assignee="x@a.com", created_by="a@a.com", title="t",
        )
        ok, _ = store.update_task_status("demo", t["id"], "done")
        assert ok
        assert store.list_tasks("demo", "1")[0]["status"] == "done"

    def test_update_unknown_task_returns_false(self, store):
        ok, msg = store.update_task_status("demo", "999", "done")
        assert ok is False and msg

    def test_table_row_counts_defaults_to_zero(self, store):
        # The base class returns zero for every requested table — only
        # Lakebase overrides this. Ensures the admin UI can call the
        # helper unconditionally without backend-specific guards.
        counts = store.table_row_counts(("registries", "domains", "schedules"))
        assert counts == {"registries": 0, "domains": 0, "schedules": 0}

    def test_table_row_counts_handles_empty_input(self, store):
        assert store.table_row_counts(()) == {}


# ---------------------------------------------------------------------
# Lakebase identity model: 1 schema = 1 registry, with legacy adoption
# ---------------------------------------------------------------------


class _ScriptedCursor:
    """Tiny psycopg-cursor stand-in driven by a queue of scripted
    ``(predicate, fetchone, fetchall)`` triples. Each call to
    :meth:`execute` consumes the first matching script entry and pins
    its return values for the next ``fetchone`` / ``fetchall``.
    """

    def __init__(self, script):
        # script: list of dicts with keys: contains, fetchone, fetchall,
        # rowcount
        self._script = list(script)
        self.executed = []  # captured (sql, params) tuples
        self._next_one = None
        self._next_all = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        for entry in self._script:
            if entry["contains"] in sql and not entry.get("_used"):
                entry["_used"] = True
                self._next_one = entry.get("fetchone")
                self._next_all = entry.get("fetchall", [])
                self.rowcount = entry.get("rowcount", 0)
                return
        # Default to "no row" so unscripted queries don't accidentally
        # return stale data from the previous script entry.
        self._next_one = None
        self._next_all = []
        self.rowcount = 0

    def fetchone(self):
        return self._next_one

    def fetchall(self):
        return self._next_all


class _ScriptedConn:
    def __init__(self, cursor):
        self._cursor = cursor
        self.closed = False

    def cursor(self, *args, **kwargs):
        # Accept ``row_factory=dict_row`` (used by the list/insert paths)
        # while staying compatible with the no-arg callers.
        return self._cursor

    def close(self):
        self.closed = True


def _make_lakebase_store(monkeypatch, schema="ontobricks_registry"):
    """Build a real :class:`LakebaseRegistryStore` whose ``_connect``
    is patched to yield a scripted cursor — so the registry-name and
    legacy-adoption logic can be tested without a real Postgres.
    """
    monkeypatch.setenv("PGHOST", "test-host")
    monkeypatch.setenv("PGPORT", "5432")
    monkeypatch.setenv("PGDATABASE", "ontobricks_registry")
    monkeypatch.setenv("PGUSER", "sp-test")

    from back.objects.registry.store.lakebase import LakebaseRegistryStore

    return LakebaseRegistryStore(registry_cfg=CFG, schema=schema)


class TestLakebaseRegistryIdentity:
    """The Lakebase registry name is keyed on the *schema*, not the
    Volume triplet. This decouples dev/prod apps that share a Lakebase
    binding from their (unrelated) Volume bindings, and lets a single
    legacy row migrated under the old ``catalog.schema.volume`` naming
    be transparently adopted on first access.
    """

    def test_registry_name_is_the_schema(self, monkeypatch):
        store = _make_lakebase_store(monkeypatch, schema="my_lb_schema")
        # _registry_name is the new identity. Critically, it does NOT
        # depend on the cfg's catalog/schema/volume — two apps with
        # different Volume bindings but the same Lakebase schema must
        # see the same registry.
        assert store._registry_name() == "my_lb_schema"
        cfg2 = RegistryCfg(catalog="other", schema="other", volume="other")
        store2 = _make_lakebase_store(monkeypatch, schema="my_lb_schema")
        store2._cfg = cfg2
        assert store2._registry_name() == "my_lb_schema"

    def test_fetch_returns_id_when_named_row_exists(self, monkeypatch):
        store = _make_lakebase_store(monkeypatch)
        cur = _ScriptedCursor(
            [{"contains": "WHERE name = %s", "fetchone": ("rid-123",)}]
        )
        from contextlib import contextmanager

        @contextmanager
        def fake_connect():
            yield _ScriptedConn(cur)

        monkeypatch.setattr(store, "_connect", fake_connect)

        assert store._fetch_registry_id() == "rid-123"
        # Single SELECT, no UPDATE — the row is already in the new shape.
        kinds = [s for s, _ in cur.executed]
        assert any("WHERE name = %s" in s for s in kinds)
        assert not any("UPDATE" in s for s in kinds)

    def test_fetch_adopts_lone_legacy_row(self, monkeypatch):
        """Pre-existing schemas keyed by ``catalog.schema.volume`` must
        be silently renamed to the new schema-based identity on first
        access — that's what makes the dev app start seeing the data
        the production app migrated, without any manual SQL.
        """
        store = _make_lakebase_store(monkeypatch)
        cur = _ScriptedCursor(
            [
                {"contains": "WHERE name = %s", "fetchone": None},
                {
                    "contains": "ORDER BY created_at",
                    "fetchone": ("legacy-id", "cat.sch.vol", 1),
                },
                {"contains": "UPDATE", "fetchone": None},
            ]
        )
        from contextlib import contextmanager

        @contextmanager
        def fake_connect():
            yield _ScriptedConn(cur)

        monkeypatch.setattr(store, "_connect", fake_connect)

        assert store._fetch_registry_id() == "legacy-id"
        # The rename SQL must have been issued with the *new* name.
        update_sql = [(s, p) for s, p in cur.executed if "UPDATE" in s]
        assert len(update_sql) == 1
        _, params = update_sql[0]
        assert params[0] == store._registry_name()  # new name
        assert params[1] == "legacy-id"  # row id

    def test_fetch_returns_none_when_schema_is_empty(self, monkeypatch):
        store = _make_lakebase_store(monkeypatch)
        cur = _ScriptedCursor(
            [
                {"contains": "WHERE name = %s", "fetchone": None},
                {"contains": "ORDER BY created_at", "fetchone": None},
            ]
        )
        from contextlib import contextmanager

        @contextmanager
        def fake_connect():
            yield _ScriptedConn(cur)

        monkeypatch.setattr(store, "_connect", fake_connect)

        assert store._fetch_registry_id() is None
        # Must NOT have issued an UPDATE if there was nothing to adopt.
        assert not any("UPDATE" in s for s, _ in cur.executed)

    def test_fetch_adopts_oldest_when_multiple_legacy_rows(self, monkeypatch):
        """When several legacy registry rows are present (old multi-
        tenant data), pick the oldest deterministically and warn so
        the admin can clean up the rest. We rely on Postgres's
        ``ORDER BY created_at ASC LIMIT 1`` for the determinism — the
        unit test verifies the warning is emitted by patching the
        store's logger directly (caplog occasionally misses records
        when other tests alter root-logger configuration).
        """
        from back.objects.registry.store.lakebase import store as lb_store

        store = _make_lakebase_store(monkeypatch)
        cur = _ScriptedCursor(
            [
                {"contains": "WHERE name = %s", "fetchone": None},
                {
                    "contains": "ORDER BY created_at",
                    "fetchone": ("oldest-id", "cat1.sch1.vol1", 3),
                },
                {"contains": "UPDATE", "fetchone": None},
            ]
        )
        from contextlib import contextmanager

        @contextmanager
        def fake_connect():
            yield _ScriptedConn(cur)

        monkeypatch.setattr(store, "_connect", fake_connect)
        warn_mock = MagicMock()
        monkeypatch.setattr(lb_store.logger, "warning", warn_mock)

        assert store._fetch_registry_id() == "oldest-id"

        warn_mock.assert_called_once()
        rendered = warn_mock.call_args[0][0] % warn_mock.call_args[0][1:]
        assert "3 registry rows" in rendered


class TestLakebaseInitStatus:
    """``init_status`` is the detailed companion to ``is_initialized``.

    The bare bool used to swallow the most common silent failure
    mode — *the app's service principal lacks ``USAGE`` on the
    registry schema* — and report it as a generic "not
    initialized", which sent operators chasing phantom data loss
    instead of running the bootstrap-perms script. The new method
    returns a stable ``reason`` token so the admin UI can render
    the actual cause.
    """

    def _patch_connect(self, monkeypatch, store, cur):
        from contextlib import contextmanager

        @contextmanager
        def fake_connect():
            yield _ScriptedConn(cur)

        monkeypatch.setattr(store, "_connect", fake_connect)

    def test_no_usage_is_surfaced_explicitly(self, monkeypatch):
        """Schema USAGE missing — must NOT report "not initialised"."""
        store = _make_lakebase_store(monkeypatch)
        cur = _ScriptedCursor(
            [
                {
                    "contains": "has_schema_privilege",
                    "fetchone": ("databricks_postgres", "sp-uuid", False, True),
                }
            ]
        )
        self._patch_connect(monkeypatch, store, cur)

        status = store.init_status()
        assert status["initialized"] is False
        assert status["reason"] == "no_usage"
        assert "USAGE" in status["error"]
        # New diagnostic surface — error must name the live db + role so
        # operators can spot grants that landed on a different database.
        assert "databricks_postgres" in status["error"]
        assert "sp-uuid" in status["error"]

    def test_no_usage_when_schema_missing_in_bound_database(self, monkeypatch):
        """Schema absent from the bound DB — surfaces a different hint."""
        store = _make_lakebase_store(monkeypatch)
        cur = _ScriptedCursor(
            [
                {
                    "contains": "has_schema_privilege",
                    "fetchone": ("databricks_postgres", "sp-uuid", False, False),
                }
            ]
        )
        self._patch_connect(monkeypatch, store, cur)

        status = store.init_status()
        assert status["initialized"] is False
        assert status["reason"] == "no_usage"
        assert "does not exist" in status["error"]
        assert "databricks_postgres" in status["error"]
        # Must short-circuit — no ``to_regclass`` query when the
        # SP can't even see the schema.
        assert not any("to_regclass" in s for s, _ in cur.executed)

    def test_no_registries_table_when_schema_is_fresh(self, monkeypatch):
        store = _make_lakebase_store(monkeypatch)
        cur = _ScriptedCursor(
            [
                {
                    "contains": "has_schema_privilege",
                    "fetchone": ("databricks_postgres", "sp-uuid", True, True),
                },
                {"contains": "to_regclass", "fetchone": (False,)},
            ]
        )
        self._patch_connect(monkeypatch, store, cur)

        status = store.init_status()
        assert status["initialized"] is False
        assert status["reason"] == "no_registries_table"

    def test_no_registry_row_when_table_exists_but_empty(self, monkeypatch):
        store = _make_lakebase_store(monkeypatch)
        cur = _ScriptedCursor(
            [
                {
                    "contains": "has_schema_privilege",
                    "fetchone": ("databricks_postgres", "sp-uuid", True, True),
                },
                {"contains": "to_regclass", "fetchone": (True,)},
                # ``_fetch_registry_id`` runs after — both queries return
                # nothing, so the schema is initialised but unseeded.
                {"contains": "WHERE name = %s", "fetchone": None},
                {"contains": "ORDER BY created_at", "fetchone": None},
            ]
        )
        self._patch_connect(monkeypatch, store, cur)

        status = store.init_status()
        assert status["initialized"] is False
        assert status["reason"] == "no_registry_row"

    def test_ok_when_everything_is_in_place(self, monkeypatch):
        store = _make_lakebase_store(monkeypatch)
        cur = _ScriptedCursor(
            [
                {
                    "contains": "has_schema_privilege",
                    "fetchone": ("databricks_postgres", "sp-uuid", True, True),
                },
                {"contains": "to_regclass", "fetchone": (True,)},
                {"contains": "WHERE name = %s", "fetchone": ("rid-42",)},
            ]
        )
        self._patch_connect(monkeypatch, store, cur)

        status = store.init_status()
        assert status == {"initialized": True, "reason": "ok", "error": None}
        # ``is_initialized`` is the bool wrapper — must agree.
        # Reset cached id so the second probe re-runs the script
        # against a new cursor instance.
        store._registry_id = None
        cur2 = _ScriptedCursor(
            [
                {
                    "contains": "has_schema_privilege",
                    "fetchone": ("databricks_postgres", "sp-uuid", True, True),
                },
                {"contains": "to_regclass", "fetchone": (True,)},
                {"contains": "WHERE name = %s", "fetchone": ("rid-42",)},
            ]
        )
        self._patch_connect(monkeypatch, store, cur2)
        assert store.is_initialized() is True

    def test_connect_failure_is_reported_not_swallowed_silently(self, monkeypatch):
        """A pool/auth blow-up must surface as ``connect_failed`` —
        not the legacy "all good, just empty" false negative.
        """
        store = _make_lakebase_store(monkeypatch)
        from contextlib import contextmanager

        @contextmanager
        def boom():
            raise RuntimeError("Lakebase pool exhausted")

        monkeypatch.setattr(store, "_connect", boom)

        status = store.init_status()
        assert status["initialized"] is False
        assert status["reason"] == "connect_failed"
        assert "pool exhausted" in status["error"]


class TestLakebaseTableRowCountsErrors:
    """``table_row_counts`` used to swallow every exception and return
    all-zeros, which masked real deployment problems (service principal
    missing USAGE on the schema, instance unreachable, …). It now
    propagates so the admin UI can surface a clear error instead of a
    misleading "0 rows everywhere" inventory.
    """

    def test_propagates_connection_error(self, monkeypatch):
        store = _make_lakebase_store(monkeypatch)
        from contextlib import contextmanager

        @contextmanager
        def boom():
            raise RuntimeError("Lakebase pool exhausted")

        monkeypatch.setattr(store, "_connect", boom)

        with pytest.raises(RuntimeError, match="Lakebase pool exhausted"):
            store.table_row_counts(("registries", "domains"))

    def test_returns_zero_for_known_tables_when_schema_is_empty(self, monkeypatch):
        # Schema exists but is empty: information_schema.tables returns
        # no rows for our requested whitelist; we still get a clean
        # ``{table: 0}`` mapping without raising. This is the
        # legitimate "schema not initialised" signal — distinct from
        # "could not connect" which now raises.
        store = _make_lakebase_store(monkeypatch)
        cur = _ScriptedCursor(
            [{"contains": "information_schema.tables", "fetchall": []}]
        )
        from contextlib import contextmanager

        @contextmanager
        def fake_connect():
            yield _ScriptedConn(cur)

        monkeypatch.setattr(store, "_connect", fake_connect)

        counts = store.table_row_counts(("registries", "domains"))
        assert counts == {"registries": 0, "domains": 0}

    def test_counts_only_present_tables(self, monkeypatch):
        store = _make_lakebase_store(monkeypatch)
        cur = _ScriptedCursor(
            [
                # Only "domains" is returned by information_schema, so
                # we must NOT issue a count query for "registries".
                {
                    "contains": "information_schema.tables",
                    "fetchall": [("domains",)],
                },
                {"contains": "SELECT count(*)", "fetchone": (42,)},
            ]
        )
        from contextlib import contextmanager

        @contextmanager
        def fake_connect():
            yield _ScriptedConn(cur)

        monkeypatch.setattr(store, "_connect", fake_connect)

        counts = store.table_row_counts(("registries", "domains"))
        assert counts == {"registries": 0, "domains": 42}
        # Exactly one ``count(*)`` query — guards against accidentally
        # querying tables the schema doesn't have, which would error
        # under ``relation does not exist``.
        count_calls = [s for s, _ in cur.executed if "SELECT count(*)" in s]
        assert len(count_calls) == 1


class TestFetchLakebaseRegistryTriplet:
    """``fetch_lakebase_registry_triplet`` is the source of truth used by
    :meth:`RegistryCfg.from_domain` to align catalog/schema/volume with
    the Lakebase row. It must:

    - return the row triplet on success,
    - cache positive results (no second SELECT),
    - return ``None`` on Postgres / auth errors and on missing rows,
    - cache negative results briefly so we don't hammer Lakebase on
      every page render during a cold start.
    """

    def setup_method(self):
        # Each test starts from a clean cache so positive results from
        # one case don't leak into the next (the cache is process-wide).
        from back.objects.registry.store.lakebase.store import (
            reset_lakebase_triplet_cache,
        )

        reset_lakebase_triplet_cache()

    def _patch_pool(self, monkeypatch, cur):
        """Replace ``_get_pool`` with a stub that yields ``cur`` on
        ``connection()``. Avoids any real Lakebase / psycopg call.
        """
        from contextlib import contextmanager
        from back.objects.registry.store.lakebase import store as _lb_store

        @contextmanager
        def fake_connection():
            yield _ScriptedConn(cur)

        class _FakePool:
            def connection(self):
                return fake_connection()

        monkeypatch.setattr(_lb_store, "_get_pool", lambda *a, **kw: _FakePool())
        monkeypatch.setattr(
            _lb_store, "get_lakebase_auth", lambda: object()
        )

    def test_returns_row_triplet(self, monkeypatch):
        from back.objects.registry.store.lakebase.store import (
            fetch_lakebase_registry_triplet,
        )

        cur = _ScriptedCursor(
            [
                {
                    "contains": "registries",
                    "fetchone": (
                        "benoit_cayla",
                        "ontobricks",
                        "OntoBricksRegistry",
                    ),
                }
            ]
        )
        self._patch_pool(monkeypatch, cur)

        out = fetch_lakebase_registry_triplet("ontobricks_registry")
        assert out == ("benoit_cayla", "ontobricks", "OntoBricksRegistry")

    def test_caches_positive_result(self, monkeypatch):
        from back.objects.registry.store.lakebase.store import (
            fetch_lakebase_registry_triplet,
        )

        cur = _ScriptedCursor(
            [{"contains": "registries", "fetchone": ("c", "s", "v")}]
        )
        self._patch_pool(monkeypatch, cur)

        fetch_lakebase_registry_triplet("schema_a")
        fetch_lakebase_registry_triplet("schema_a")
        fetch_lakebase_registry_triplet("schema_a")
        # Second & third call must hit the cache, not the database.
        # ``_ScriptedCursor.executed`` records every SELECT; only the
        # first call should have landed.
        assert len(cur.executed) == 1

    def test_returns_none_when_row_missing(self, monkeypatch):
        from back.objects.registry.store.lakebase.store import (
            fetch_lakebase_registry_triplet,
        )

        cur = _ScriptedCursor([])  # no scripted entry → fetchone() → None
        self._patch_pool(monkeypatch, cur)

        out = fetch_lakebase_registry_triplet("schema_b")
        assert out is None

    def test_returns_none_on_pool_failure(self, monkeypatch):
        from back.objects.registry.store.lakebase import store as _lb_store
        from back.objects.registry.store.lakebase.store import (
            fetch_lakebase_registry_triplet,
        )

        def _boom(*a, **kw):
            raise RuntimeError("Lakebase cold-start timeout")

        monkeypatch.setattr(_lb_store, "_get_pool", _boom)
        monkeypatch.setattr(_lb_store, "get_lakebase_auth", lambda: object())

        # Must NOT raise — ``RegistryCfg.from_domain`` calls this on
        # every request and a hard failure here would 500 the whole app.
        assert fetch_lakebase_registry_triplet("schema_c") is None

    def test_returns_none_on_auth_failure(self, monkeypatch):
        from back.objects.registry.store.lakebase import store as _lb_store
        from back.objects.registry.store.lakebase.store import (
            fetch_lakebase_registry_triplet,
        )

        def _boom():
            raise RuntimeError("LakebaseAuth: PG* env vars missing")

        monkeypatch.setattr(_lb_store, "get_lakebase_auth", _boom)
        assert fetch_lakebase_registry_triplet("schema_d") is None

    def test_distinct_databases_have_distinct_cache_entries(self, monkeypatch):
        from back.objects.registry.store.lakebase.store import (
            fetch_lakebase_registry_triplet,
        )

        cur = _ScriptedCursor(
            [
                {"contains": "registries", "fetchone": ("c1", "s1", "v1")},
                {"contains": "registries", "fetchone": ("c2", "s2", "v2")},
            ]
        )
        self._patch_pool(monkeypatch, cur)

        first = fetch_lakebase_registry_triplet("schema_e", database="db_a")
        # Cache is keyed on ``(schema, database)`` so a different
        # database must trigger a fresh SELECT (consuming the second
        # scripted entry above).
        second = fetch_lakebase_registry_triplet("schema_e", database="db_b")

        assert first == ("c1", "s1", "v1")
        assert second == ("c2", "s2", "v2")


# ---------------------------------------------------------------------
# Lakebase collaborative comments + tasks — real SQL code paths
#
# The in-memory fake above proves the *contract*; these tests drive the
# concrete Lakebase implementation (row mappers, the lazy
# ``_ensure_collab_tables`` short-circuit, and the rowcount-based
# not-found handling) without a real Postgres, via the scripted cursor.
# ---------------------------------------------------------------------


from datetime import datetime  # noqa: E402


def _collab_store(monkeypatch, cur):
    """A Lakebase store whose ``_connect`` yields *cur* and whose collab
    tables are already marked present (skips the lazy DDL probe).

    Also stubs ``_require_psycopg`` so the tests pass without psycopg
    installed: _ScriptedConn.cursor() already accepts and ignores the
    ``row_factory`` kwarg, so (None, None) is a safe stub.
    """
    from contextlib import contextmanager
    import back.objects.registry.store.lakebase.store as _store_mod

    store = _make_lakebase_store(monkeypatch)
    store._registry_id = "rid-1"          # skip registry-id resolution
    store._collab_tables_ready = True     # skip the ensure/CREATE probe

    @contextmanager
    def fake_connect():
        yield _ScriptedConn(cur)

    monkeypatch.setattr(store, "_connect", fake_connect)
    # Stub out the psycopg import guard — _ScriptedConn.cursor() ignores
    # row_factory so None is a safe placeholder for dict_row.
    monkeypatch.setattr(_store_mod, "_require_psycopg", lambda: (None, None))
    return store


class TestLakebaseCollab:
    def test_ensure_collab_tables_short_circuits_when_present(self, monkeypatch):
        from contextlib import contextmanager

        store = _make_lakebase_store(monkeypatch)
        store._registry_id = "rid-1"
        cur = _ScriptedCursor(
            [{"contains": "information_schema.tables", "fetchone": (1,)}]
        )

        @contextmanager
        def fake_connect():
            yield _ScriptedConn(cur)

        monkeypatch.setattr(store, "_connect", fake_connect)

        assert store._ensure_collab_tables() is True
        assert store._collab_tables_ready is True
        # Table already exists → must NOT issue any CREATE TABLE.
        assert not any("CREATE TABLE" in s for s, _ in cur.executed)

    def test_insert_comment_maps_returning_row(self, monkeypatch):
        cur = _ScriptedCursor(
            [
                {
                    "contains": "INSERT INTO",
                    "fetchone": {
                        "id": "c-1", "version": "2",
                        "parent_id": None,
                        "author": "a@a.com", "body": "rename",
                        "resolved": False,
                        "created_at": datetime(2026, 1, 1, 0, 0, 0),
                    },
                }
            ]
        )
        store = _collab_store(monkeypatch, cur)
        out = store.insert_comment(
            "demo", "2", author="a@a.com", body="rename",
        )
        assert out["id"] == "c-1"
        assert out["body"] == "rename"
        assert out["folder"] == "demo"
        assert out["resolved"] is False
        assert out["created_at"].startswith("2026-01-01")

    def test_insert_comment_returns_none_when_domain_missing(self, monkeypatch):
        # No RETURNING row (unknown domain) → fetchone None → None.
        cur = _ScriptedCursor([])
        store = _collab_store(monkeypatch, cur)
        assert store.insert_comment(
            "ghost", "2", author="a@a.com", body="x",
        ) is None

    def test_list_comments_maps_rows_and_filters(self, monkeypatch):
        cur = _ScriptedCursor(
            [
                {
                    "contains": "FROM",
                    "fetchall": [
                        {
                            "id": "c-1", "folder": "demo", "version": "2",
                            "parent_id": None, "author": "a@a.com",
                            "body": "hi", "resolved": False,
                            "created_at": datetime(2026, 1, 1),
                        }
                    ],
                }
            ]
        )
        store = _collab_store(monkeypatch, cur)
        rows = store.list_comments(
            "demo", "2", include_resolved=False,
        )
        assert [c["body"] for c in rows] == ["hi"]
        # The version + resolved filters must reach the SQL.
        sql, params = cur.executed[-1]
        assert "c.version = %s" in sql
        assert "resolved = false" in sql
        assert "2" in params

    def test_resolve_comment_not_found_returns_false(self, monkeypatch):
        cur = _ScriptedCursor([{"contains": "UPDATE", "rowcount": 0}])
        store = _collab_store(monkeypatch, cur)
        ok, msg = store.resolve_comment("demo", "c-9")
        assert ok is False and msg == "Comment not found"

    def test_resolve_comment_success(self, monkeypatch):
        cur = _ScriptedCursor([{"contains": "UPDATE", "rowcount": 1}])
        store = _collab_store(monkeypatch, cur)
        ok, msg = store.resolve_comment("demo", "c-1", resolved=True)
        assert ok is True and msg == ""
        _, params = cur.executed[-1]
        assert params[0] is True  # resolved flag forwarded

    def test_insert_task_maps_returning_row(self, monkeypatch):
        cur = _ScriptedCursor(
            [
                {
                    "contains": "INSERT INTO",
                    "fetchone": {
                        "id": "t-1", "version": "2",
                        "assignee": "bob@a.com", "created_by": "a@a.com",
                        "title": "fix", "description": "", "status": "open",
                        "due_date": None, "comment_id": "c-1",
                        "created_at": datetime(2026, 1, 1),
                        "updated_at": datetime(2026, 1, 1),
                    },
                }
            ]
        )
        store = _collab_store(monkeypatch, cur)
        out = store.insert_task(
            "demo", "2", assignee="bob@a.com", created_by="a@a.com",
            title="fix", comment_id="c-1",
        )
        assert out["id"] == "t-1"
        assert out["assignee"] == "bob@a.com"
        assert out["comment_id"] == "c-1"
        assert out["status"] == "open"
        assert out["due_date"] == ""

    def test_list_tasks_for_assignee_maps_rows(self, monkeypatch):
        cur = _ScriptedCursor(
            [
                {
                    "contains": "lower(t.assignee) = lower(%s)",
                    "fetchall": [
                        {
                            "id": "t-1", "folder": "demo", "version": "2",
                            "assignee": "bob@a.com", "created_by": "a@a.com",
                            "title": "fix", "description": "", "status": "open",
                            "due_date": None, "comment_id": None,
                            "created_at": datetime(2026, 1, 1),
                            "updated_at": datetime(2026, 1, 1),
                        }
                    ],
                }
            ]
        )
        store = _collab_store(monkeypatch, cur)
        rows = store.list_tasks_for_assignee("BOB@a.com")
        assert [t["title"] for t in rows] == ["fix"]
        assert rows[0]["folder"] == "demo"
        _, params = cur.executed[-1]
        assert params == ("rid-1", "BOB@a.com")

    def test_update_task_status_not_found(self, monkeypatch):
        cur = _ScriptedCursor([{"contains": "UPDATE", "rowcount": 0}])
        store = _collab_store(monkeypatch, cur)
        ok, msg = store.update_task_status("demo", "t-9", "done")
        assert ok is False and msg == "Task not found"

    def test_update_task_status_success(self, monkeypatch):
        cur = _ScriptedCursor([{"contains": "UPDATE", "rowcount": 1}])
        store = _collab_store(monkeypatch, cur)
        ok, _ = store.update_task_status("demo", "t-1", "in_progress")
        assert ok is True
        sql, params = cur.executed[-1]
        assert "SET status = %s" in sql
        assert params[0] == "in_progress"
