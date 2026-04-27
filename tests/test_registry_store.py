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
from unittest.mock import MagicMock

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

    def test_lakebase_factory_for_backend_plumbs_database(self, monkeypatch):
        """End-to-end check that ``for_backend`` and ``from_cfg`` both
        forward the override to the store — these are the entry points
        ``RegistryService._build_store`` uses.
        """
        monkeypatch.setenv("PGHOST", "test-host")
        monkeypatch.setenv("PGPORT", "5432")
        monkeypatch.setenv("PGDATABASE", "ontobricks_registry")
        monkeypatch.setenv("PGUSER", "sp-test")

        cfg = RegistryCfg(
            catalog="c",
            schema="s",
            volume="v",
            backend="lakebase",
            lakebase_schema="ontobricks_registry",
            lakebase_database="ontobricks_other",
        )

        from back.objects.registry.store.lakebase import LakebaseRegistryStore

        s1 = RegistryFactory.for_backend(
            "lakebase",
            registry_cfg=cfg,
            lakebase_schema=cfg.lakebase_schema,
            lakebase_database=cfg.lakebase_database,
        )
        assert isinstance(s1, LakebaseRegistryStore)
        assert s1.describe()["effective_database"] == "ontobricks_other"

        s2 = RegistryFactory.from_cfg(cfg)
        assert isinstance(s2, LakebaseRegistryStore)
        assert s2.describe()["effective_database"] == "ontobricks_other"


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
# Lakebase identity model: 1 schema = 1 registry, with legacy adoption
# ---------------------------------------------------------------------


class _ScriptedCursor:
    """Tiny psycopg-cursor stand-in driven by a queue of scripted
    ``(predicate, fetchone, fetchall)`` triples. Each call to
    :meth:`execute` consumes the first matching script entry and pins
    its return values for the next ``fetchone`` / ``fetchall``.
    """

    def __init__(self, script):
        # script: list of dicts with keys: contains, fetchone, fetchall
        self._script = list(script)
        self.executed = []  # captured (sql, params) tuples
        self._next_one = None
        self._next_all = []

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
                return
        # Default to "no row" so unscripted queries don't accidentally
        # return stale data from the previous script entry.
        self._next_one = None
        self._next_all = []

    def fetchone(self):
        return self._next_one

    def fetchall(self):
        return self._next_all


class _ScriptedConn:
    def __init__(self, cursor):
        self._cursor = cursor
        self.closed = False

    def cursor(self):
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
            [{"contains": "has_schema_privilege", "fetchone": (False,)}]
        )
        self._patch_connect(monkeypatch, store, cur)

        status = store.init_status()
        assert status["initialized"] is False
        assert status["reason"] == "no_usage"
        assert "USAGE" in status["error"]
        # Must short-circuit — no ``to_regclass`` query when the
        # SP can't even see the schema.
        assert not any("to_regclass" in s for s, _ in cur.executed)

    def test_no_registries_table_when_schema_is_fresh(self, monkeypatch):
        store = _make_lakebase_store(monkeypatch)
        cur = _ScriptedCursor(
            [
                {"contains": "has_schema_privilege", "fetchone": (True,)},
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
                {"contains": "has_schema_privilege", "fetchone": (True,)},
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
                {"contains": "has_schema_privilege", "fetchone": (True,)},
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
                {"contains": "has_schema_privilege", "fetchone": (True,)},
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
