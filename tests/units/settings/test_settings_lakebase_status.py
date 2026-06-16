"""Unit tests for ``SettingsService._lakebase_schema_status``.

The status probe surfaces ``initialized`` + ``populated`` flags on
the admin Registry Location page so operators can tell at a glance
whether the Lakebase schema is ready and holding data.

Both signals are best-effort: the probe must never raise. Any
psycopg / connection / permission failure degrades to ``False``.
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

from back.objects.domain.SettingsService import SettingsService
from back.objects.registry.RegistryService import RegistryCfg


def _rcfg() -> RegistryCfg:
    return RegistryCfg(
        catalog="cat",
        schema="sch",
        volume="vol",
        lakebase_schema="ontobricks_registry",
        lakebase_database="",
    )


@pytest.fixture
def psycopg_installed(monkeypatch):
    """Stub ``psycopg`` so the optional-extra gate inside
    ``_lakebase_schema_status`` lets the test reach the mocked factory.

    The production code does ``import psycopg`` at function scope and
    bails to ``{initialized: False, populated: False}`` if the package
    is missing. The ``[lakebase]`` extra is not part of the base venv,
    so every test that wants to assert behaviour past the gate must
    opt into this fixture.
    """
    monkeypatch.setitem(sys.modules, "psycopg", MagicMock())
    yield


class TestLakebaseSchemaStatus:
    def test_returns_false_pair_when_psycopg_missing(self, monkeypatch):
        # Simulate the optional extra not installed: ``import psycopg``
        # at module scope inside the helper raises ImportError.
        import builtins

        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "psycopg":
                raise ImportError("no psycopg in this venv")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)
        assert SettingsService._lakebase_schema_status(_rcfg()) == {
            "initialized": False,
            "populated": False,
        }

    def test_uninitialized_skips_row_count_probe(self, psycopg_installed):
        # When ``is_initialized`` is False the helper must short-circuit
        # — no point hitting ``table_row_counts`` against a schema that
        # doesn't have the registry tables yet (it'd just round-trip
        # for nothing and pollute the logs).
        store = MagicMock()
        store.is_initialized.return_value = False
        with patch(
            "back.objects.registry.store.RegistryFactory.lakebase",
            return_value=store,
        ):
            status = SettingsService._lakebase_schema_status(_rcfg())
        assert status == {"initialized": False, "populated": False}
        store.table_row_counts.assert_not_called()

    def test_initialized_but_empty_tables_reports_unpopulated(
        self, psycopg_installed
    ):
        store = MagicMock()
        store.is_initialized.return_value = True
        store.table_row_counts.return_value = {
            "domains": 0,
            "permission_sets": 0,
            "scheduled_builds": 0,
            "scheduled_history": 0,
        }
        with patch.dict(sys.modules, {"psycopg": MagicMock()}), \
             patch(
                 "back.objects.registry.store.RegistryFactory.lakebase",
                 return_value=store,
             ):
            assert SettingsService._lakebase_schema_status(_rcfg()) == {
                "initialized": True,
                "populated": False,
            }

    def test_initialized_with_any_table_populated_reports_true(
        self, psycopg_installed
    ):
        # A single non-empty table is enough — the UI only needs the
        # has-data signal, not exact counts.
        store = MagicMock()
        store.is_initialized.return_value = True
        store.table_row_counts.return_value = {
            "domains": 7,
            "permission_sets": 0,
            "scheduled_builds": 0,
            "scheduled_history": 0,
        }
        with patch.dict(sys.modules, {"psycopg": MagicMock()}), \
             patch(
                 "back.objects.registry.store.RegistryFactory.lakebase",
                 return_value=store,
             ):
            assert SettingsService._lakebase_schema_status(_rcfg()) == {
                "initialized": True,
                "populated": True,
            }

    def test_factory_failure_is_swallowed(self, psycopg_installed):
        # Mirrors the failure mode where ``RegistryFactory.lakebase``
        # itself raises (e.g. broken config). The probe must keep the
        # admin UI rendering — never raise.
        with patch(
            "back.objects.registry.store.RegistryFactory.lakebase",
            side_effect=RuntimeError("factory boom"),
        ):
            assert SettingsService._lakebase_schema_status(_rcfg()) == {
                "initialized": False,
                "populated": False,
            }

    def test_row_count_failure_keeps_initialized_true(self, psycopg_installed):
        # ``table_row_counts`` now propagates connection / permission
        # errors (intentional, it surfaces deployment misconfigurations).
        # The status probe still wants to keep ``initialized=True`` so
        # the admin can see "schema is up but row counts unavailable",
        # while gracefully reporting ``populated=False``.
        store = MagicMock()
        store.is_initialized.return_value = True
        store.table_row_counts.side_effect = RuntimeError("permission denied")
        with patch.dict(sys.modules, {"psycopg": MagicMock()}), \
             patch(
                 "back.objects.registry.store.RegistryFactory.lakebase",
                 return_value=store,
             ):
            assert SettingsService._lakebase_schema_status(_rcfg()) == {
                "initialized": True,
                "populated": False,
            }

    def test_legacy_helper_delegates_to_status(self):
        # ``_lakebase_schema_initialized`` is kept as a thin wrapper for
        # callers that only need the boolean. It must read the
        # canonical ``status['initialized']`` so we don't grow two
        # divergent code paths.
        with patch.object(
            SettingsService,
            "_lakebase_schema_status",
            return_value={"initialized": True, "populated": True},
        ):
            assert SettingsService._lakebase_schema_initialized(_rcfg()) is True
        with patch.object(
            SettingsService,
            "_lakebase_schema_status",
            return_value={"initialized": False, "populated": False},
        ):
            assert SettingsService._lakebase_schema_initialized(_rcfg()) is False
