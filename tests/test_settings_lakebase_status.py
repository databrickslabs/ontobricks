"""Unit tests for ``SettingsService._lakebase_schema_status``.

The status probe drives two distinct UI affordances on the admin
Registry Location page:

- ``initialized`` toggles the *Migrate to Lakebase* button visibility
  when on the Lakebase backend (uninitialised ⇒ keep button visible
  so the admin can populate the schema).
- ``populated`` flips the same button between *Migrate* (clean
  target) and *Re-sync from Volume* (overwrite warning) and hides it
  outright when Lakebase is the active backend and already holds
  data — running a migration there would silently overwrite live
  rows.

Both signals are best-effort: the probe must never raise. Any
psycopg / connection / permission failure degrades to ``False``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from back.objects.domain.SettingsService import SettingsService
from back.objects.registry.RegistryService import RegistryCfg


def _rcfg() -> RegistryCfg:
    return RegistryCfg(
        catalog="cat",
        schema="sch",
        volume="vol",
        backend="lakebase",
        lakebase_schema="ontobricks_registry",
        lakebase_database="",
    )


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

    def test_uninitialized_skips_row_count_probe(self):
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

    def test_initialized_but_empty_tables_reports_unpopulated(self):
        store = MagicMock()
        store.is_initialized.return_value = True
        store.table_row_counts.return_value = {
            "domains": 0,
            "permission_sets": 0,
            "scheduled_builds": 0,
            "scheduled_history": 0,
        }
        with patch(
            "back.objects.registry.store.RegistryFactory.lakebase",
            return_value=store,
        ):
            assert SettingsService._lakebase_schema_status(_rcfg()) == {
                "initialized": True,
                "populated": False,
            }

    def test_initialized_with_any_table_populated_reports_true(self):
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
        with patch(
            "back.objects.registry.store.RegistryFactory.lakebase",
            return_value=store,
        ):
            assert SettingsService._lakebase_schema_status(_rcfg()) == {
                "initialized": True,
                "populated": True,
            }

    def test_factory_failure_is_swallowed(self):
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

    def test_row_count_failure_keeps_initialized_true(self):
        # ``table_row_counts`` now propagates connection / permission
        # errors (intentional, it surfaces deployment misconfigurations).
        # The status probe still wants to keep ``initialized=True`` so
        # the admin can see "schema is up but row counts unavailable",
        # while gracefully reporting ``populated=False``.
        store = MagicMock()
        store.is_initialized.return_value = True
        store.table_row_counts.side_effect = RuntimeError("permission denied")
        with patch(
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
