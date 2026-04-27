"""Contract tests for the registry-store abstraction and migration helper.

These tests use lightweight in-memory fakes (no Databricks/Postgres
dependencies) to validate:

- :class:`RegistryFactory` returns the correct concrete store and
  gracefully surfaces a missing ``psycopg`` install for the Lakebase
  backend.
- :class:`VolumeRegistryStore` and :class:`LakebaseRegistryStore` agree
  on the public ``RegistryStore`` interface (method names + return
  shapes).
- The Volume → Lakebase migration helper copies every JSON-shaped
  artefact through the destination store and produces an idempotent
  ``MigrationReport``.

Lakebase-only behaviour that requires a real Postgres connection lives
in the gated ``tests/integration/`` suite.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pytest

from back.objects.registry import RegistryCfg
from back.objects.registry.store import (
    MigrationReport,
    RegistryFactory,
    RegistryStore,
    VolumeRegistryStore,
    build_store,
    migrate_volume_to_lakebase,
    summarize_migration,
)
from back.objects.registry.store.base import DomainSummary, ScheduleHistoryEntry


CFG = RegistryCfg(catalog="cat", schema="sch", volume="vol")


# ---------------------------------------------------------------------
# Fake in-memory store — used as both source and destination
# ---------------------------------------------------------------------


class _InMemoryStore(RegistryStore):
    """Minimal in-memory implementation used for migration round-trips.

    Just enough behaviour to exercise the public surface used by
    :func:`migrate_volume_to_lakebase` and the contract tests below.
    """

    def __init__(self, tag: str = "memory"):
        self._tag = tag
        self._versions: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._perms: Dict[str, Dict[str, Any]] = {}
        self._schedules: Dict[str, Dict[str, Any]] = {}
        self._history: Dict[str, List[ScheduleHistoryEntry]] = {}
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
    def test_for_backend_volume_default(self):
        store = RegistryFactory.for_backend(
            "volume", registry_cfg=CFG, host="h", token="t"
        )
        assert isinstance(store, VolumeRegistryStore)
        assert store.backend == "volume"
        assert store.cache_key.startswith("volume:")

    def test_for_backend_unknown_falls_back_to_volume(self):
        store = RegistryFactory.for_backend(
            "nope", registry_cfg=CFG, host="h", token="t"
        )
        assert isinstance(store, VolumeRegistryStore)

    def test_volume_explicit_constructor(self):
        store = RegistryFactory.volume(registry_cfg=CFG, host="h", token="t")
        assert isinstance(store, VolumeRegistryStore)

    def test_from_cfg_dispatches_on_backend_attr(self):
        cfg_volume = RegistryCfg(catalog="c", schema="s", volume="v")
        store = RegistryFactory.from_cfg(cfg_volume, host="h", token="t")
        assert isinstance(store, VolumeRegistryStore)

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


class TestBuildStoreShim:
    """The deprecated ``build_store`` function must still work as a
    backward-compatible alias for :meth:`RegistryFactory.for_backend`.
    """

    def test_volume_default(self):
        store = build_store("volume", registry_cfg=CFG, host="h", token="t")
        assert isinstance(store, VolumeRegistryStore)


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

    def test_volume_store_cache_key_is_tagged(self):
        s = VolumeRegistryStore(registry_cfg=CFG)
        assert s.cache_key.startswith("volume:"), (
            "Volume cache key must be backend-tagged so a runtime switch "
            "to Lakebase invalidates the registry-level TTL cache."
        )

    def test_table_row_counts_defaults_to_zero(self, store):
        # The base class returns zero for every requested table — only
        # Lakebase overrides this. Ensures the admin UI can call the
        # helper unconditionally without backend-specific guards.
        counts = store.table_row_counts(("registries", "domains", "schedules"))
        assert counts == {"registries": 0, "domains": 0, "schedules": 0}

    def test_table_row_counts_handles_empty_input(self, store):
        assert store.table_row_counts(()) == {}


# ---------------------------------------------------------------------
# Migration: Volume → Lakebase
# ---------------------------------------------------------------------


def _seed(src: _InMemoryStore) -> None:
    src.save_global_config({"warehouse_id": "wh-1"})
    src.save_schedules(
        {
            "demo": {"cron": "0 * * * *", "enabled": True, "version": "1"},
            "other": {"cron": "@daily", "enabled": False, "version": "2"},
        }
    )
    src.write_version(
        "demo",
        "1",
        {
            "info": {"name": "demo", "base_uri": "http://x/"},
            "versions": [{"version": "1", "active": True}],
        },
    )
    src.write_version(
        "demo",
        "2",
        {
            "info": {"name": "demo", "base_uri": "http://x/"},
            "versions": [{"version": "2", "active": True}],
        },
    )
    src.save_domain_permissions(
        "demo", {"version": 1, "permissions": [{"user": "alice", "role": "admin"}]}
    )
    for ts in range(3):
        src.append_schedule_history(
            "demo", {"timestamp": str(ts), "status": "ok"}, max_entries=10
        )


class TestMigration:
    def test_round_trip_copies_everything(self):
        src, dst = _InMemoryStore("volume"), _InMemoryStore("lakebase")
        _seed(src)

        report = migrate_volume_to_lakebase(src, dst)

        assert report.ok
        assert report.global_config is True
        assert report.schedules == 2
        assert report.domains == 1
        assert report.versions == 2
        assert report.permission_sets == 1
        assert report.history_entries == 3
        assert report.errors == []

        assert dst.load_global_config() == {"warehouse_id": "wh-1"}
        assert dst.load_schedules().keys() == {"demo", "other"}
        ok, versions, _ = dst.list_versions("demo")
        assert ok and versions == ["1", "2"]
        assert dst.load_domain_permissions("demo")["permissions"][0]["user"] == "alice"
        assert len(dst.load_schedule_history("demo")) == 3

    def test_idempotent_when_run_twice(self):
        src, dst = _InMemoryStore("volume"), _InMemoryStore("lakebase")
        _seed(src)

        first = migrate_volume_to_lakebase(src, dst)
        second = migrate_volume_to_lakebase(src, dst)

        assert first.ok and second.ok
        ok, versions, _ = dst.list_versions("demo")
        assert ok and versions == ["1", "2"]

    def test_destination_initialize_failure_short_circuits(self):
        class _Broken(_InMemoryStore):
            def initialize(self, *, client: Any = None):
                return False, "ddl failed"

        src = _InMemoryStore("volume")
        _seed(src)
        report = migrate_volume_to_lakebase(src, _Broken("lakebase"))

        assert not report.ok
        assert any("initialize destination" in e for e in report.errors)
        assert report.versions == 0

    def test_summarize_renders_human_readable(self):
        report = MigrationReport(
            domains=2, versions=5, permission_sets=1,
            schedules=1, history_entries=4, global_config=True,
        )
        ok, msg = summarize_migration(report)
        assert ok
        assert "domains=2" in msg
        assert "versions=5" in msg
        assert "global_config=yes" in msg

    def test_summarize_flags_errors(self):
        report = MigrationReport(errors=["boom"])
        ok, msg = summarize_migration(report)
        assert ok is False
        assert "errors=1" in msg
        assert "boom" in msg

    def test_summarize_flags_multiple_errors(self):
        report = MigrationReport(errors=["boom", "kaboom", "fizzle"])
        ok, msg = summarize_migration(report)
        assert ok is False
        assert "errors=3" in msg
        assert "boom" in msg
        assert "+2 more" in msg
