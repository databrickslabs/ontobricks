"""Tests for Delta-specific SQL warehouse global config."""

from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import ValidationError
from back.core.helpers.DatabricksHelpers import DatabricksHelpers
from back.core.helpers import (
    get_delta_databricks_credentials,
    get_triplestore_sql_credentials,
    resolve_delta_warehouse_id,
)
from back.objects.domain.SettingsService import SettingsService
from back.objects.session.GlobalConfigService import GlobalConfigService

REGISTRY_CFG = {"catalog": "cat", "schema": "sch", "volume": "vol"}


class TestGlobalConfigDeltaWarehouse:
    def test_empty_defaults_contain_delta_warehouse_id(self):
        empty = GlobalConfigService._empty()
        assert empty.get("delta_warehouse_id") == ""

    def test_get_and_set_delta_warehouse_id(self):
        svc = GlobalConfigService()
        with patch.object(svc, "load", return_value=GlobalConfigService._empty()):
            assert svc.get_delta_warehouse_id("h", "t", REGISTRY_CFG) == ""
        with patch.object(svc, "_save", return_value=(True, "ok")) as mock_save:
            ok, _ = svc.set_delta_warehouse_id("h", "t", REGISTRY_CFG, "wh-delta")
        assert ok
        mock_save.assert_called_once_with(
            "h", "t", REGISTRY_CFG, {"delta_warehouse_id": "wh-delta"}
        )


class TestResolveDeltaWarehouseId:
    def test_prefers_delta_warehouse_over_global(self):
        domain = MagicMock()
        settings = MagicMock()
        with patch.object(
            DatabricksHelpers,
            "get_databricks_host_and_token",
            return_value=("https://h", "tok"),
        ), patch.object(
            DatabricksHelpers,
            "_resolve_registry_cfg",
            return_value=REGISTRY_CFG,
        ), patch(
            "back.objects.session.global_config_service.get_delta_warehouse_id",
            return_value="wh-delta",
        ), patch(
            "back.core.helpers.DatabricksHelpers.DatabricksHelpers.resolve_warehouse_id",
            return_value="wh-global",
        ) as global_resolve:
            wid = resolve_delta_warehouse_id(domain, settings)
        assert wid == "wh-delta"
        global_resolve.assert_not_called()

    def test_falls_back_to_global_when_delta_unset(self):
        domain = MagicMock()
        settings = MagicMock()
        with patch.object(
            DatabricksHelpers,
            "get_databricks_host_and_token",
            return_value=("https://h", "tok"),
        ), patch.object(
            DatabricksHelpers,
            "_resolve_registry_cfg",
            return_value=REGISTRY_CFG,
        ), patch(
            "back.objects.session.global_config_service.get_delta_warehouse_id",
            return_value="",
        ), patch(
            "back.core.helpers.DatabricksHelpers.DatabricksHelpers.resolve_warehouse_id",
            return_value="wh-global",
        ):
            wid = resolve_delta_warehouse_id(domain, settings)
        assert wid == "wh-global"


class TestDeltaDatabricksCredentials:
    def test_get_delta_databricks_credentials(self):
        domain = MagicMock()
        settings = MagicMock()
        with patch.object(
            DatabricksHelpers,
            "get_databricks_host_and_token",
            return_value=("https://h", "tok"),
        ), patch.object(
            DatabricksHelpers,
            "resolve_delta_warehouse_id",
            return_value="wh-delta",
        ):
            host, token, wid = get_delta_databricks_credentials(domain, settings)
        assert host == "https://h"
        assert token == "tok"
        assert wid == "wh-delta"

    def test_triplestore_sql_credentials_uses_delta_when_backend_databricks(self):
        domain = MagicMock()
        settings = MagicMock()
        with patch(
            "back.core.graphdb.GraphDBFactory.GraphDBFactory._resolve_triple_store_backend",
            return_value="databricks",
        ), patch.object(
            DatabricksHelpers,
            "get_delta_databricks_credentials",
            return_value=("h", "t", "wh-d"),
        ) as delta_creds, patch.object(
            DatabricksHelpers,
            "get_databricks_credentials",
            return_value=("h", "t", "wh-g"),
        ):
            creds = get_triplestore_sql_credentials(domain, settings)
        assert creds == ("h", "t", "wh-d")
        delta_creds.assert_called_once()

    def test_triplestore_sql_credentials_uses_global_when_backend_lakebase(self):
        domain = MagicMock()
        settings = MagicMock()
        with patch(
            "back.core.graphdb.GraphDBFactory.GraphDBFactory._resolve_triple_store_backend",
            return_value="lakebase",
        ), patch.object(
            DatabricksHelpers,
            "get_databricks_credentials",
            return_value=("h", "t", "wh-g"),
        ) as global_creds:
            creds = get_triplestore_sql_credentials(domain, settings)
        assert creds == ("h", "t", "wh-g")
        global_creds.assert_called_once()


class TestSettingsServiceDeltaWarehouse:
    def test_get_triple_store_backend_includes_delta_warehouse(self):
        domain = MagicMock()
        with patch.object(
            SettingsService, "_resolve_context", return_value=(domain, "h", "t", REGISTRY_CFG)
        ), patch(
            "back.objects.domain.SettingsService.global_config_service.load"
        ), patch(
            "back.objects.domain.SettingsService.global_config_service.get_triple_store_backend",
            return_value="databricks",
        ), patch(
            "back.objects.domain.SettingsService.global_config_service.get_delta_warehouse_id",
            return_value="wh-delta",
        ), patch(
            "back.objects.domain.SettingsService.get_domain",
            return_value=domain,
        ), patch(
            "back.objects.domain.SettingsService.resolve_delta_warehouse_id",
            return_value="wh-delta",
        ):
            result = SettingsService.get_triple_store_backend_result(
                MagicMock(), MagicMock()
            )
        assert result["delta_warehouse_id"] == "wh-delta"
        assert result["effective_delta_warehouse_id"] == "wh-delta"

    def test_select_delta_warehouse_requires_id(self):
        with pytest.raises(ValidationError, match="No warehouse ID"):
            SettingsService.select_delta_warehouse(
                None, "a@b.com", "tok", MagicMock(), MagicMock()
            )

    def test_select_delta_warehouse_allows_clear(self):
        domain = MagicMock()
        with patch.object(
            SettingsService, "_resolve_context", return_value=(domain, "h", "t", REGISTRY_CFG)
        ), patch.object(SettingsService, "require_admin_error"), patch(
            "back.objects.domain.SettingsService.global_config_service.set_delta_warehouse_id",
            return_value=(True, "ok"),
        ) as mock_set, patch(
            "back.objects.domain.SettingsService.global_config_service.load"
        ), patch.object(
            SettingsService, "_mirror_graph_engine_to_domain_registry"
        ), patch(
            "back.objects.domain.SettingsService.resolve_delta_warehouse_id",
            return_value="wh-global",
        ):
            result = SettingsService.select_delta_warehouse(
                "", "a@b.com", "tok", MagicMock(), MagicMock()
            )
        assert result["success"]
        assert result["delta_warehouse_id"] == ""
        mock_set.assert_called_once_with("h", "t", REGISTRY_CFG, "")

    def test_select_delta_warehouse_persists(self):
        domain = MagicMock()
        with patch.object(
            SettingsService, "_resolve_context", return_value=(domain, "h", "t", REGISTRY_CFG)
        ), patch.object(SettingsService, "require_admin_error"), patch(
            "back.objects.domain.SettingsService.global_config_service.set_delta_warehouse_id",
            return_value=(True, "ok"),
        ) as mock_set, patch.object(
            SettingsService, "_mirror_graph_engine_to_domain_registry"
        ) as mock_mirror, patch(
            "back.objects.domain.SettingsService.resolve_delta_warehouse_id",
            return_value="wh-delta",
        ):
            result = SettingsService.select_delta_warehouse(
                "wh-delta", "a@b.com", "tok", MagicMock(), MagicMock()
            )
        assert result["success"]
        mock_set.assert_called_once_with("h", "t", REGISTRY_CFG, "wh-delta")
        mock_mirror.assert_called_once()
        assert mock_mirror.call_args.kwargs.get("delta_warehouse_id") == "wh-delta"

    def test_set_triple_store_backend_persists_delta_warehouse(self):
        domain = MagicMock()
        session_mgr = MagicMock()
        with patch.object(
            SettingsService, "_resolve_context", return_value=(domain, "h", "t", REGISTRY_CFG)
        ), patch.object(SettingsService, "require_admin_error"), patch(
            "back.objects.domain.SettingsService.global_config_service.set_triple_store_backend",
            return_value=(True, "ok"),
        ), patch(
            "back.objects.domain.SettingsService.global_config_service.get_triple_store_backend",
            return_value="databricks",
        ), patch(
            "back.objects.domain.SettingsService.global_config_service.get_delta_warehouse_id",
            return_value="wh-delta",
        ), patch(
            "back.objects.domain.SettingsService.global_config_service.set_delta_warehouse_id",
            return_value=(True, "ok"),
        ) as mock_set_delta, patch.object(
            SettingsService, "_mirror_graph_engine_to_domain_registry"
        ) as mock_mirror, patch(
            "back.objects.domain.SettingsService.resolve_delta_warehouse_id",
            return_value="wh-delta",
        ):
            result = SettingsService.set_triple_store_backend_result(
                "databricks",
                "a@b.com",
                "tok",
                session_mgr,
                MagicMock(),
                delta_warehouse_id="wh-delta",
                persist_delta_warehouse=True,
            )
        assert result["delta_warehouse_id"] == "wh-delta"
        mock_set_delta.assert_called_once_with("h", "t", REGISTRY_CFG, "wh-delta")
        mock_mirror.assert_called_once_with(
            session_mgr,
            triple_store_backend="databricks",
            delta_warehouse_id="wh-delta",
        )
