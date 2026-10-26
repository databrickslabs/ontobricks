"""WarehouseSettings (0.9.0): current config payload, separate Build vs Query
(Delta / Lakehouse//RT) SQL warehouses, App-resource locks, connection test."""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from back.core.errors import AuthorizationError, InfrastructureError, ValidationError
from back.objects.domain.settings.SettingsService import SettingsService
from back.objects.domain.settings.WarehouseSettings import WarehouseSettings

pytestmark = pytest.mark.unit

_ss = importlib.import_module("back.objects.domain.settings.SettingsService")

CFG = {"catalog": "cat", "schema": "sch"}


async def _inline_run_blocking(func, *args, **kwargs):
    return func(*args, **kwargs)


def _settings(**kw):
    base = dict(
        sql_warehouse_id="",
        databricks_host="",
        databricks_token="",
        ontobricks_app_name="app",
    )
    base.update(kw)
    return SimpleNamespace(**base)


@pytest.fixture
def domain(monkeypatch):
    d = MagicMock()
    d.databricks = {}
    monkeypatch.setattr(_ss, "get_domain", lambda sm: d)
    monkeypatch.setattr(
        SettingsService,
        "_resolve_context",
        staticmethod(lambda sm, st: (d, "h", "t", CFG)),
    )
    return d


@pytest.fixture
def gcs(monkeypatch):
    svc = MagicMock()
    svc.set_warehouse_id.return_value = (True, "ok")
    svc.set_build_warehouse.return_value = (True, "ok")
    svc.set_delta_warehouse_id.return_value = (True, "ok")
    monkeypatch.setattr(_ss, "global_config_service", svc)
    return svc


@pytest.fixture
def local_mode(monkeypatch):
    monkeypatch.setattr(_ss, "is_databricks_app", lambda: False)


@pytest.fixture
def app_mode(monkeypatch):
    monkeypatch.setattr(_ss, "is_databricks_app", lambda: True)
    perm = MagicMock()
    perm.is_admin.return_value = True
    monkeypatch.setattr(_ss, "permission_service", perm)
    return perm


# ---------------------------------------------------------------------------
# Lock / current config
# ---------------------------------------------------------------------------


class TestWarehouseLock:
    def test_locked_only_in_app_with_resource_warehouse(self, app_mode):
        assert WarehouseSettings.is_warehouse_locked(_settings(sql_warehouse_id="wh")) is True
        assert WarehouseSettings.is_warehouse_locked(_settings(sql_warehouse_id="")) is False

    def test_never_locked_locally(self, local_mode):
        assert WarehouseSettings.is_warehouse_locked(_settings(sql_warehouse_id="wh")) is False


class TestBuildCurrentConfig:
    @pytest.fixture(autouse=True)
    def _resolvers(self, monkeypatch, local_mode):
        monkeypatch.setattr(_ss, "resolve_warehouse_id", lambda d, s: "wh-build")
        monkeypatch.setattr(_ss, "resolve_use_cloud_fetch", lambda d, s: True)

    def test_token_auth_masks_token(self, domain):
        domain.databricks = {"host": "https://ws", "token": "dapi"}
        cfg = WarehouseSettings.build_current_config(MagicMock(), _settings())
        assert cfg["token"] == "***"
        assert cfg["auth_mode"] == "token"
        assert cfg["auth_display"] == "Personal Access Token"
        assert cfg["has_config"] is True
        assert cfg["warehouse_id"] == "wh-build"
        assert cfg["use_cloud_fetch"] is True
        assert cfg["warehouse_use_sea"] is False

    def test_app_auth_when_host_from_env_and_no_token(self, domain):
        cfg = WarehouseSettings.build_current_config(
            MagicMock(), _settings(databricks_host="https://ws")
        )
        assert cfg["token"] is None
        assert cfg["auth_mode"] == "app"
        assert cfg["auth_display"] == "Databricks App"
        assert cfg["is_app_mode"] is True and cfg["from_env"] is True
        assert cfg["has_config"] is False

    def test_unconfigured(self, domain):
        cfg = WarehouseSettings.build_current_config(MagicMock(), _settings())
        assert cfg["auth_mode"] == "none"
        assert cfg["auth_display"] == "Not configured"
        assert cfg["warehouse_locked"] is False


# ---------------------------------------------------------------------------
# apply_config_save / select_warehouse
# ---------------------------------------------------------------------------


class TestApplyConfigSave:
    def test_host_saved_to_session_and_posted_token_ignored(self, domain, gcs, local_mode):
        """A domain never holds secrets (.cursor/07 §Domain Security)."""
        result = WarehouseSettings.apply_config_save(
            {"host": "https://ws", "token": "dapi"}, "", "", MagicMock(), _settings()
        )
        assert result == {"success": True, "message": "Configuration saved"}
        assert domain.databricks == {"host": "https://ws"}
        gcs.set_warehouse_id.assert_not_called()
        domain.save.assert_called_once()

    def test_locked_warehouse_cannot_be_changed(self, domain, gcs, app_mode):
        with pytest.raises(ValidationError, match="Databricks App resources"):
            WarehouseSettings.apply_config_save(
                {"warehouse_id": "other"}, "", "", MagicMock(), _settings(sql_warehouse_id="wh")
            )
        gcs.set_warehouse_id.assert_not_called()
        domain.save.assert_not_called()

    def test_non_admin_cannot_change_warehouse(self, domain, gcs, app_mode):
        app_mode.is_admin.return_value = False
        with pytest.raises(AuthorizationError):
            WarehouseSettings.apply_config_save({"warehouse_id": "w"}, "u", "t", MagicMock(), _settings())
        assert "warehouse_id" not in domain.databricks

    def test_warehouse_persisted_globally_with_session_fallback(self, domain, gcs, local_mode):
        gcs.set_warehouse_id.return_value = (False, "registry not configured")
        result = WarehouseSettings.apply_config_save({"warehouse_id": "w1"}, "", "", MagicMock(), _settings())
        assert result["success"] is True
        assert domain.databricks["warehouse_id"] == "w1"
        gcs.set_warehouse_id.assert_called_once_with("h", "t", CFG, "w1")


class TestSelectWarehouse:
    def test_locked_rejected(self, domain, gcs, app_mode):
        with pytest.raises(ValidationError, match="cannot be changed"):
            WarehouseSettings.select_warehouse("w", "", "", MagicMock(), _settings(sql_warehouse_id="wh"))

    def test_missing_id_rejected(self, domain, gcs, local_mode):
        with pytest.raises(ValidationError, match="No warehouse ID"):
            WarehouseSettings.select_warehouse("", "", "", MagicMock(), _settings())

    def test_non_admin_rejected(self, domain, gcs, app_mode):
        app_mode.is_admin.return_value = False
        with pytest.raises(AuthorizationError):
            WarehouseSettings.select_warehouse("w", "", "", MagicMock(), _settings())
        gcs.set_warehouse_id.assert_not_called()

    def test_persisted_globally(self, domain, gcs, local_mode):
        assert WarehouseSettings.select_warehouse("w", "", "", MagicMock(), _settings()) == {
            "success": True,
            "message": "Warehouse selected",
        }
        assert domain.databricks["warehouse_id"] == "w"
        domain.save.assert_called_once()

    def test_global_failure_keeps_session_selection(self, domain, gcs, local_mode):
        gcs.set_warehouse_id.return_value = (False, "no registry")
        result = WarehouseSettings.select_warehouse("w", "", "", MagicMock(), _settings())
        assert result["success"] is True
        assert "stored in session" in result["message"]
        assert domain.databricks["warehouse_id"] == "w"


# ---------------------------------------------------------------------------
# Build warehouse
# ---------------------------------------------------------------------------


class TestSelectBuildWarehouse:
    def test_missing_id_rejected(self, domain, gcs, local_mode):
        with pytest.raises(ValidationError, match="No Build SQL Warehouse"):
            WarehouseSettings.select_build_warehouse("  ", None, False, "", "", MagicMock(), _settings())

    def test_lakehouse_rt_rejected_for_build(self, domain, gcs, local_mode):
        with pytest.raises(ValidationError, match="does not support build DDL"):
            WarehouseSettings.select_build_warehouse("w", " reyden ", False, "", "", MagicMock(), _settings())
        gcs.set_build_warehouse.assert_not_called()

    def test_sea_flag_is_ignored_build_always_thrift(self, domain, gcs, local_mode):
        result = WarehouseSettings.select_build_warehouse(" w1 ", "PRO", True, "", "", MagicMock(), _settings())
        assert result == {"success": True, "warehouse_id": "w1", "use_sea": False}
        gcs.set_build_warehouse.assert_called_once_with("h", "t", CFG, "w1", use_sea=False)
        assert domain.databricks["warehouse_id"] == "w1"

    def test_non_admin_rejected(self, domain, gcs, app_mode):
        app_mode.is_admin.return_value = False
        with pytest.raises(AuthorizationError):
            WarehouseSettings.select_build_warehouse("w", "PRO", False, "", "", MagicMock(), _settings())

    def test_store_failure(self, domain, gcs, local_mode):
        gcs.set_build_warehouse.return_value = (False, "x")
        with pytest.raises(InfrastructureError, match="Build SQL Warehouse"):
            WarehouseSettings.select_build_warehouse("w", "PRO", False, "", "", MagicMock(), _settings())


# ---------------------------------------------------------------------------
# Query (Delta) warehouse
# ---------------------------------------------------------------------------


class TestSelectDeltaWarehouse:
    @pytest.fixture(autouse=True)
    def _wiring(self, monkeypatch):
        monkeypatch.setattr(_ss, "resolve_warehouse_id", lambda d, s: "wh-build")
        monkeypatch.setattr(_ss, "resolve_delta_warehouse_id", lambda d, s: "effective")
        self.mirror = MagicMock()
        monkeypatch.setattr(SettingsService, "_mirror_graph_engine_to_domain_registry", self.mirror)

    def test_none_rejected(self, domain, gcs, local_mode):
        with pytest.raises(ValidationError, match="No warehouse ID"):
            WarehouseSettings.select_delta_warehouse(None, "", "", MagicMock(), _settings())

    def test_rt_requires_a_warehouse(self, domain, gcs, local_mode):
        with pytest.raises(ValidationError, match="Select a Query SQL Warehouse"):
            WarehouseSettings.select_delta_warehouse(" ", "", "", MagicMock(), _settings(), use_sea=True)

    def test_rt_query_must_differ_from_build(self, domain, gcs, local_mode):
        with pytest.raises(ValidationError, match="must be different"):
            WarehouseSettings.select_delta_warehouse("wh-build", "", "", MagicMock(), _settings(), use_sea=True)
        gcs.set_delta_warehouse_id.assert_not_called()

    def test_rt_selection_persisted_and_mirrored(self, domain, gcs, local_mode):
        sm = MagicMock()
        result = WarehouseSettings.select_delta_warehouse(" wh-rt ", "", "", sm, _settings(), use_sea=1)
        assert result == {
            "success": True,
            "message": "Delta SQL Warehouse selected",
            "delta_warehouse_id": "wh-rt",
            "use_sea": True,
            "effective_delta_warehouse_id": "effective",
        }
        gcs.set_delta_warehouse_id.assert_called_once_with("h", "t", CFG, "wh-rt", use_sea=True)
        gcs.load.assert_called_once_with("h", "t", CFG, force=True)
        self.mirror.assert_called_once_with(sm, delta_warehouse_id="wh-rt")

    def test_non_rt_clears_override_to_share_build(self, domain, gcs, local_mode):
        result = WarehouseSettings.select_delta_warehouse("wh-other", "", "", MagicMock(), _settings())
        assert result["delta_warehouse_id"] == ""
        assert result["message"].startswith("Query SQL Warehouse cleared")
        gcs.set_delta_warehouse_id.assert_called_once_with("h", "t", CFG, "", use_sea=False)

    def test_non_admin_rejected(self, domain, gcs, app_mode):
        app_mode.is_admin.return_value = False
        with pytest.raises(AuthorizationError):
            WarehouseSettings.select_delta_warehouse("wh-rt", "", "", MagicMock(), _settings(), use_sea=True)
        gcs.set_delta_warehouse_id.assert_not_called()

    def test_store_failure_is_validation_error(self, domain, gcs, local_mode):
        gcs.set_delta_warehouse_id.return_value = (False, "warehouse not found")
        with pytest.raises(ValidationError, match="warehouse not found"):
            WarehouseSettings.select_delta_warehouse("w", "", "", MagicMock(), _settings())
        self.mirror.assert_not_called()


class TestGetDeltaWarehouse:
    def test_reports_selection_and_registry_location(self, domain, gcs, monkeypatch):
        monkeypatch.setattr(_ss, "resolve_delta_warehouse_id", lambda d, s: "eff")
        gcs.get_delta_warehouse_id.return_value = "wh-rt"
        gcs.get_delta_warehouse_use_sea.return_value = True

        result = WarehouseSettings.get_delta_warehouse_result(MagicMock(), _settings())

        assert result == {
            "success": True,
            "delta_warehouse_id": "wh-rt",
            "use_sea": True,
            "effective_delta_warehouse_id": "eff",
            "registry_catalog": "cat",
            "registry_schema": "sch",
            "storage_location": "cat.sch",
            "registry_configured": True,
        }

    def test_unconfigured_registry(self, gcs, monkeypatch):
        monkeypatch.setattr(_ss, "get_domain", lambda sm: MagicMock())
        monkeypatch.setattr(
            SettingsService, "_resolve_context",
            staticmethod(lambda sm, st: (MagicMock(), "h", "t", None)),
        )
        monkeypatch.setattr(_ss, "resolve_delta_warehouse_id", lambda d, s: "")
        result = WarehouseSettings.get_delta_warehouse_result(MagicMock(), _settings())
        assert result["storage_location"] == ""
        assert result["registry_configured"] is False


# ---------------------------------------------------------------------------
# Connection test / warehouse listing
# ---------------------------------------------------------------------------


@pytest.fixture
def client(monkeypatch):
    fake = MagicMock()
    monkeypatch.setattr(_ss, "get_domain", lambda sm: MagicMock())
    monkeypatch.setattr(_ss, "get_databricks_client", lambda d, s: fake)
    monkeypatch.setattr(_ss, "run_blocking", _inline_run_blocking)
    return fake


class TestTestConnection:
    async def test_success_counts_warehouses(self, client):
        client.get_warehouses.return_value = [{}, {}, {}]
        assert await WarehouseSettings.test_connection(MagicMock(), _settings()) == {
            "success": True,
            "message": "Connection successful. Found 3 warehouses.",
        }

    async def test_no_client(self, client, monkeypatch):
        monkeypatch.setattr(_ss, "get_databricks_client", lambda d, s: None)
        with pytest.raises(ValidationError, match="Databricks not configured"):
            await WarehouseSettings.test_connection(MagicMock(), _settings())

    async def test_sdk_not_initialized_is_validation(self, client):
        client.get_warehouses.side_effect = AttributeError("'NoneType' object has no attribute 'request'")
        with pytest.raises(ValidationError, match="not properly initialized"):
            await WarehouseSettings.test_connection(MagicMock(), _settings())

    async def test_other_attribute_error_is_infrastructure(self, client):
        client.get_warehouses.side_effect = AttributeError("bad attr")
        with pytest.raises(InfrastructureError) as exc_info:
            await WarehouseSettings.test_connection(MagicMock(), _settings())
        assert exc_info.value.detail == "bad attr"

    async def test_generic_error_is_infrastructure(self, client):
        client.get_warehouses.side_effect = RuntimeError("timeout")
        with pytest.raises(InfrastructureError, match="Test connection failed"):
            await WarehouseSettings.test_connection(MagicMock(), _settings())


class TestFetchWarehouses:
    async def test_success(self, client):
        client.get_warehouses.return_value = [{"id": "w"}]
        assert await WarehouseSettings.fetch_warehouses(MagicMock(), _settings()) == {"warehouses": [{"id": "w"}]}

    async def test_no_client(self, client, monkeypatch):
        monkeypatch.setattr(_ss, "get_databricks_client", lambda d, s: None)
        with pytest.raises(ValidationError):
            await WarehouseSettings.fetch_warehouses(MagicMock(), _settings())

    async def test_sdk_not_initialized_is_validation(self, client):
        client.get_warehouses.side_effect = AttributeError("'NoneType' object has no attribute 'request'")
        with pytest.raises(ValidationError, match="not properly initialized"):
            await WarehouseSettings.fetch_warehouses(MagicMock(), _settings())

    async def test_other_attribute_error_is_infrastructure(self, client):
        client.get_warehouses.side_effect = AttributeError("bad attr")
        with pytest.raises(InfrastructureError, match="Failed to list SQL warehouses"):
            await WarehouseSettings.fetch_warehouses(MagicMock(), _settings())

    async def test_generic_error_is_infrastructure(self, client):
        client.get_warehouses.side_effect = RuntimeError("x")
        with pytest.raises(InfrastructureError, match="Failed to list SQL warehouses"):
            await WarehouseSettings.fetch_warehouses(MagicMock(), _settings())
