"""Initialize must pin the registry row even when the app SP cannot run DDL."""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

from back.objects.registry.RegistryService import RegistryCfg

pytestmark = pytest.mark.unit

CFG = RegistryCfg(catalog="cat", schema="sch", volume="vol")


def _store(monkeypatch):
    monkeypatch.setenv("PGHOST", "test-host")
    monkeypatch.setenv("PGPORT", "5432")
    monkeypatch.setenv("PGDATABASE", "ontobricks_registry")
    monkeypatch.setenv("PGUSER", "sp-test")

    from back.objects.registry.store.lakebase import LakebaseRegistryStore

    store = LakebaseRegistryStore(registry_cfg=CFG, schema="reg")

    @contextmanager
    def _connect():
        cur = MagicMock()
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        conn.cursor.return_value.__exit__.return_value = False
        yield conn

    monkeypatch.setattr(store, "_connect", _connect)
    monkeypatch.setattr(store, "_scrub_global_config_legacy_keys", lambda: None)
    monkeypatch.setattr(store, "_ensure_domain_versions_status_column", lambda: True)
    monkeypatch.setattr(store, "_ensure_domains_review_quorum_column", lambda: True)
    monkeypatch.setattr(store, "_ensure_domains_mcp_policy_column", lambda: True)
    monkeypatch.setattr(store, "_ensure_schedule_task_columns", lambda: True)
    monkeypatch.setattr(store, "_import_legacy_cohort_schedules", lambda: None)
    return store


def test_initialize_continues_when_ddl_fails_as_non_owner(monkeypatch):
    store = _store(monkeypatch)
    monkeypatch.setattr(
        store,
        "_apply_ddl",
        lambda: (_ for _ in ()).throw(
            RuntimeError("must be owner of table registries")
        ),
    )
    monkeypatch.setattr(store, "_ensure_registry_row", lambda: "reg-9")

    ok, msg = store.initialize()

    assert ok is True
    assert store._registry_id == "reg-9"
    assert "initialized/upgraded" in msg


def test_initialize_still_fails_when_dml_fails(monkeypatch):
    store = _store(monkeypatch)
    monkeypatch.setattr(store, "_apply_ddl", lambda: None)
    monkeypatch.setattr(
        store,
        "_ensure_registry_row",
        lambda: (_ for _ in ()).throw(RuntimeError("perm")),
    )

    ok, msg = store.initialize()

    assert ok is False
    assert msg == "Failed to initialise Lakebase registry: perm"
