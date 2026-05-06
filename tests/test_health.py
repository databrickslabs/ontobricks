"""Unit tests for the readiness probes in ``shared.fastapi.health``.

The probes are exercised in isolation with mocks so they run in any
environment, including CI machines with no Databricks credentials.
``test_routes.py::TestHealthRoutes`` already covers the FastAPI route
end-to-end against the real (degraded) test environment.
"""

import os
import shutil
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from shared.fastapi import health


# ---------------------------------------------------------------------------
# _safely_run
# ---------------------------------------------------------------------------


class TestSafelyRun:
    def test_returns_status_and_detail(self):
        result = health._safely_run("x", "X", lambda: ("ok", "fine"))
        assert result["status"] == "ok"
        assert result["detail"] == "fine"
        assert result["name"] == "x"
        assert result["label"] == "X"
        assert isinstance(result["duration_ms"], int)
        assert result["duration_ms"] >= 0

    def test_catches_exceptions(self):
        def boom():
            raise RuntimeError("kaboom")

        result = health._safely_run("x", "X", boom)
        assert result["status"] == "error"
        assert "kaboom" in result["detail"]


# ---------------------------------------------------------------------------
# Filesystem probes
# ---------------------------------------------------------------------------


class TestCheckDirectoryWritable:
    def test_ok_when_writable_and_space(self, tmp_path):
        status, detail = health._check_directory_writable(
            str(tmp_path), low_warn_gb=0.0, low_err_gb=0.0
        )
        assert status == "ok"
        assert "Writable" in detail

    def test_creates_missing_directory(self, tmp_path):
        target = tmp_path / "nested" / "deep"
        status, _ = health._check_directory_writable(
            str(target), low_warn_gb=0.0, low_err_gb=0.0
        )
        assert status == "ok"
        assert target.exists()

    def test_warning_below_threshold(self, tmp_path):
        # Force the disk-usage path to look low without actually filling
        # the test runner's filesystem.
        with patch.object(
            health.shutil,
            "disk_usage",
            return_value=SimpleNamespace(total=10 ** 9, used=10 ** 9 - 10 ** 8, free=10 ** 8),
        ):
            status, detail = health._check_directory_writable(
                str(tmp_path), low_warn_gb=1.0, low_err_gb=0.05
            )
        assert status == "warning"
        assert "Low disk space" in detail

    def test_error_below_critical(self, tmp_path):
        with patch.object(
            health.shutil,
            "disk_usage",
            return_value=SimpleNamespace(total=10 ** 9, used=10 ** 9 - 10 ** 6, free=10 ** 6),
        ):
            status, detail = health._check_directory_writable(
                str(tmp_path), low_warn_gb=1.0, low_err_gb=0.5
            )
        assert status == "error"
        assert "Critically" in detail


# ---------------------------------------------------------------------------
# Databricks auth + warehouse
# ---------------------------------------------------------------------------


class TestCheckDatabricksAuth:
    def test_app_mode_missing_creds_is_error(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_APP_PORT", "8000")
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        status, detail = health._check_databricks_auth()
        assert status == "error"
        assert "DATABRICKS_CLIENT_ID" in detail

    def test_local_mode_missing_token_is_error(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        status, detail = health._check_databricks_auth()
        assert status == "error"
        assert "DATABRICKS_TOKEN" in detail

    def test_local_mode_with_pat(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        monkeypatch.setenv("DATABRICKS_TOKEN", "pat-xyz")
        status, detail = health._check_databricks_auth()
        assert status == "ok"
        assert "Personal Access Token" in detail


class TestCheckWarehouse:
    def test_no_client_is_warning(self):
        with patch.object(health, "_build_health_client", return_value=None):
            status, detail = health._check_warehouse()
        assert status == "warning"
        assert "credentials" in detail

    def test_no_warehouse_id_is_warning(self):
        client = MagicMock(warehouse_id="")
        with patch.object(health, "_build_health_client", return_value=client):
            status, detail = health._check_warehouse()
        assert status == "warning"
        assert "WAREHOUSE_ID" in detail

    def test_test_connection_ok(self):
        client = MagicMock(warehouse_id="abc123")
        client.test_connection.return_value = (True, "Connection successful")
        with patch.object(health, "_build_health_client", return_value=client):
            status, detail = health._check_warehouse()
        assert status == "ok"
        assert "successful" in detail.lower()

    def test_test_connection_error(self):
        client = MagicMock(warehouse_id="abc123")
        client.test_connection.return_value = (False, "Cannot connect")
        with patch.object(health, "_build_health_client", return_value=client):
            status, detail = health._check_warehouse()
        assert status == "error"


# ---------------------------------------------------------------------------
# Registry probes
# ---------------------------------------------------------------------------


def _fake_cfg(catalog="main", schema="bronze", volume="reg", backend="volume"):
    return SimpleNamespace(
        catalog=catalog,
        schema=schema,
        volume=volume,
        backend=backend,
        lakebase_schema="ontobricks_registry",
        lakebase_database="",
    )


class TestCheckRegistryCfg:
    def test_resolved(self):
        with patch.object(health, "_resolve_registry_cfg", return_value=_fake_cfg()):
            status, detail = health._check_registry_cfg(MagicMock())
        assert status == "ok"
        assert "main" in detail and "bronze" in detail

    def test_missing_is_warning(self):
        with patch.object(
            health,
            "_resolve_registry_cfg",
            return_value=_fake_cfg(catalog="", schema="", volume=""),
        ):
            status, detail = health._check_registry_cfg(MagicMock())
        assert status == "warning"


class TestCheckRegistryVolumeReadWrite:
    def _patch_svc(self, svc):
        return patch(
            "back.core.databricks.VolumeFileService.VolumeFileService",
            return_value=svc,
        )

    def test_read_ok(self):
        svc = MagicMock()
        svc.is_configured.return_value = True
        svc.list_directory.return_value = (True, [{"name": "f"}], "ok")
        with patch.object(health, "_resolve_registry_cfg", return_value=_fake_cfg()), \
             self._patch_svc(svc):
            status, detail = health._check_registry_volume_read(MagicMock())
        assert status == "ok"
        assert "1 entries" in detail

    def test_read_skipped_when_unconfigured(self):
        with patch.object(
            health,
            "_resolve_registry_cfg",
            return_value=_fake_cfg(catalog="", schema="", volume=""),
        ):
            status, detail = health._check_registry_volume_read(MagicMock())
        assert status == "warning"

    def test_write_ok_with_cleanup(self):
        svc = MagicMock()
        svc.is_configured.return_value = True
        svc.write_file.return_value = (True, "saved")
        svc.delete_file.return_value = (True, "deleted")
        with patch.object(health, "_resolve_registry_cfg", return_value=_fake_cfg()), \
             self._patch_svc(svc):
            status, detail = health._check_registry_volume_write(MagicMock())
        assert status == "ok"
        assert "Wrote+deleted" in detail
        # Sentinel path includes the volume root.
        write_args = svc.write_file.call_args[0]
        assert write_args[0].startswith("/Volumes/main/bronze/reg/")

    def test_write_failure_surfaces(self):
        svc = MagicMock()
        svc.is_configured.return_value = True
        svc.write_file.return_value = (False, "403 Forbidden")
        with patch.object(health, "_resolve_registry_cfg", return_value=_fake_cfg()), \
             self._patch_svc(svc):
            status, detail = health._check_registry_volume_write(MagicMock())
        assert status == "error"
        assert "403" in detail

    def test_write_ok_but_cleanup_failed_is_warning(self):
        svc = MagicMock()
        svc.is_configured.return_value = True
        svc.write_file.return_value = (True, "saved")
        svc.delete_file.return_value = (False, "kaboom")
        with patch.object(health, "_resolve_registry_cfg", return_value=_fake_cfg()), \
             self._patch_svc(svc):
            status, detail = health._check_registry_volume_write(MagicMock())
        assert status == "warning"
        assert "cleanup failed" in detail


class TestCheckRegistryUcSchemaDdl:
    def test_skipped_when_no_warehouse(self):
        client = MagicMock(warehouse_id="")
        with patch.object(health, "_resolve_registry_cfg", return_value=_fake_cfg()), \
             patch.object(health, "_build_health_client", return_value=client):
            status, _ = health._check_registry_uc_schema_ddl()
        assert status == "warning"

    def test_create_drop_ok(self):
        client = MagicMock(warehouse_id="wh")
        client.execute_statement.return_value = True
        with patch.object(health, "_resolve_registry_cfg", return_value=_fake_cfg()), \
             patch.object(health, "_build_health_client", return_value=client):
            status, detail = health._check_registry_uc_schema_ddl()
        assert status == "ok"
        # Two calls: CREATE OR REPLACE VIEW + DROP VIEW
        assert client.execute_statement.call_count == 2
        first_sql = client.execute_statement.call_args_list[0][0][0]
        assert "CREATE OR REPLACE VIEW" in first_sql
        assert "`main`.`bronze`" in first_sql

    def test_create_failure_is_error(self):
        client = MagicMock(warehouse_id="wh")
        client.execute_statement.side_effect = Exception(
            "PERMISSION_DENIED: lacks CREATE on schema"
        )
        with patch.object(health, "_resolve_registry_cfg", return_value=_fake_cfg()), \
             patch.object(health, "_build_health_client", return_value=client):
            status, detail = health._check_registry_uc_schema_ddl()
        assert status == "error"
        assert "PERMISSION_DENIED" in detail

    def test_drop_failure_is_warning(self):
        client = MagicMock(warehouse_id="wh")
        client.execute_statement.side_effect = [True, Exception("lock contention")]
        with patch.object(health, "_resolve_registry_cfg", return_value=_fake_cfg()), \
             patch.object(health, "_build_health_client", return_value=client):
            status, detail = health._check_registry_uc_schema_ddl()
        assert status == "warning"
        assert "DROP failed" in detail


# ---------------------------------------------------------------------------
# Lakebase
# ---------------------------------------------------------------------------


class TestCheckLakebase:
    def test_skipped_when_pg_not_bound(self):
        auth = MagicMock(is_available=False)
        with patch(
            "back.core.databricks.LakebaseAuth.get_lakebase_auth",
            return_value=auth,
        ):
            status, detail = health._check_lakebase(MagicMock())
        assert status == "ok"
        assert "Volume backend" in detail

    def test_initialized_returns_ok(self):
        auth = MagicMock(is_available=True)
        store = MagicMock(schema="ontobricks_registry")
        store.init_status.return_value = {"initialized": True, "reason": "ok"}
        with patch(
            "back.core.databricks.LakebaseAuth.get_lakebase_auth",
            return_value=auth,
        ), patch.object(health, "_resolve_registry_cfg", return_value=_fake_cfg()), \
           patch(
               "back.objects.registry.store.lakebase.store.LakebaseRegistryStore",
               return_value=store,
           ):
            status, detail = health._check_lakebase(MagicMock())
        assert status == "ok"

    def test_no_usage_is_error(self):
        auth = MagicMock(is_available=True)
        store = MagicMock(schema="s")
        store.init_status.return_value = {
            "initialized": False,
            "reason": "no_usage",
            "error": "Role lacks USAGE on schema",
        }
        with patch(
            "back.core.databricks.LakebaseAuth.get_lakebase_auth",
            return_value=auth,
        ), patch.object(health, "_resolve_registry_cfg", return_value=_fake_cfg()), \
           patch(
               "back.objects.registry.store.lakebase.store.LakebaseRegistryStore",
               return_value=store,
           ):
            status, detail = health._check_lakebase(MagicMock())
        assert status == "error"
        assert "USAGE" in detail

    def test_no_registries_table_is_warning(self):
        auth = MagicMock(is_available=True)
        store = MagicMock(schema="s")
        store.init_status.return_value = {
            "initialized": False,
            "reason": "no_registries_table",
            "error": "schema has no registries table",
        }
        with patch(
            "back.core.databricks.LakebaseAuth.get_lakebase_auth",
            return_value=auth,
        ), patch.object(health, "_resolve_registry_cfg", return_value=_fake_cfg()), \
           patch(
               "back.objects.registry.store.lakebase.store.LakebaseRegistryStore",
               return_value=store,
           ):
            status, _ = health._check_lakebase(MagicMock())
        assert status == "warning"


# ---------------------------------------------------------------------------
# Aggregator + route shape
# ---------------------------------------------------------------------------


class TestRunReadinessChecks:
    def test_shape(self):
        result = health.run_readiness_checks()
        assert {"status", "version", "service", "framework", "summary", "checks"} <= result.keys()
        assert result["service"] == "OntoBricks"
        assert result["framework"] == "FastAPI"
        assert result["status"] in ("ok", "warning", "error")
        names = {c["name"] for c in result["checks"]}
        # The probes the user explicitly requested must always be present
        # in the response, even when the underlying dependency is missing
        # (in which case they degrade to a warning rather than vanishing).
        assert {
            "filesystem.tmp",
            "registry.cfg",
            "registry.volume_read",
            "registry.volume_write",
            "registry.uc_schema_ddl",
            "lakebase",
        } <= names

    def test_overall_status_is_worst(self):
        # Run with a forced ERROR probe; aggregator must elevate the
        # top-level status accordingly.
        with patch.object(health, "_check_tmp", return_value=("error", "boom")):
            result = health.run_readiness_checks()
        assert result["status"] == "error"
        assert result["summary"]["errors"] >= 1
