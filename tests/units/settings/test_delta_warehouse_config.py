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
    def test_empty_defaults_have_no_top_level_delta_warehouse(self):
        empty = GlobalConfigService._empty()
        assert "delta_warehouse_id" not in empty

    def test_get_and_set_delta_warehouse_id_uses_lakehouse_bucket(self):
        svc = GlobalConfigService()
        with patch.object(svc, "load", return_value=GlobalConfigService._empty()):
            assert svc.get_delta_warehouse_id("h", "t", REGISTRY_CFG) == ""
        with patch.object(svc, "load", return_value=GlobalConfigService._empty()), patch.object(
            svc, "_save", return_value=(True, "ok")
        ) as mock_save:
            ok, _ = svc.set_delta_warehouse_id("h", "t", REGISTRY_CFG, "wh-delta")
        assert ok
        updates = mock_save.call_args[0][3]
        assert "delta_warehouse_id" not in updates
        assert updates["graph_engine_config"]["lakehouse"]["warehouse_id"] == "wh-delta"

    def test_get_delta_warehouse_use_sea_defaults_false(self):
        svc = GlobalConfigService()
        with patch.object(svc, "load", return_value=GlobalConfigService._empty()):
            assert svc.get_delta_warehouse_use_sea("h", "t", REGISTRY_CFG) is False

    def test_set_delta_warehouse_id_persists_use_sea(self):
        svc = GlobalConfigService()
        with patch.object(svc, "load", return_value=GlobalConfigService._empty()), patch.object(
            svc, "_save", return_value=(True, "ok")
        ) as mock_save:
            ok, _ = svc.set_delta_warehouse_id(
                "h", "t", REGISTRY_CFG, "wh-rt", use_sea=True
            )
        assert ok
        lakehouse = mock_save.call_args[0][3]["graph_engine_config"]["lakehouse"]
        assert lakehouse["warehouse_id"] == "wh-rt"
        assert lakehouse["use_sea"] is True

    def test_set_delta_warehouse_id_preserves_use_sea_when_omitted(self):
        svc = GlobalConfigService()
        stored = GlobalConfigService._empty()
        stored["graph_engine_config"] = {
            "lakehouse": {"warehouse_id": "wh-rt", "use_sea": True}
        }
        with patch.object(svc, "load", return_value=stored), patch.object(
            svc, "_save", return_value=(True, "ok")
        ) as mock_save:
            ok, _ = svc.set_delta_warehouse_id("h", "t", REGISTRY_CFG, "wh-2")
        assert ok
        lakehouse = mock_save.call_args[0][3]["graph_engine_config"]["lakehouse"]
        assert lakehouse["warehouse_id"] == "wh-2"
        assert lakehouse["use_sea"] is True


class TestGlobalConfigBuildWarehouse:
    def test_build_transport_defaults_false(self):
        svc = GlobalConfigService()
        with patch.object(svc, "load", return_value=GlobalConfigService._empty()):
            assert svc.get_build_warehouse_use_sea("h", "t", REGISTRY_CFG) is False

    def test_set_build_warehouse_persists_independent_transport(self):
        svc = GlobalConfigService()
        with patch.object(
            svc, "_save", return_value=(True, "ok")
        ) as mock_save:
            ok, _ = svc.set_build_warehouse(
                "h", "t", REGISTRY_CFG, "wh-build", use_sea=True
            )
        assert ok
        assert mock_save.call_args[0][3] == {
            "warehouse_id": "wh-build",
            "warehouse_use_sea": True,
        }


class TestResolveBuildWarehouse:
    def test_resolves_build_warehouse_without_query_fallback(self):
        domain = MagicMock()
        settings = MagicMock(sql_warehouse_id="wh-bound")
        with patch.object(
            DatabricksHelpers,
            "resolve_warehouse_id",
            return_value="wh-build",
        ), patch.object(
            DatabricksHelpers,
            "resolve_delta_warehouse_id",
            return_value="wh-rt",
        ) as query_resolver:
            assert (
                DatabricksHelpers.resolve_build_warehouse_id(domain, settings)
                == "wh-build"
            )
        query_resolver.assert_not_called()

    def test_resolves_build_transport_independently(self):
        with patch.object(
            DatabricksHelpers,
            "get_databricks_host_and_token",
            return_value=("https://h", "tok"),
        ), patch.object(
            DatabricksHelpers,
            "_resolve_registry_cfg",
            return_value=REGISTRY_CFG,
        ), patch(
            "back.objects.session.global_config_service.get_build_warehouse_use_sea",
            return_value=True,
        ):
            assert DatabricksHelpers.resolve_build_use_sea(
                MagicMock(), MagicMock()
            ) is True

    def test_build_credentials_require_warehouse(self):
        with patch.object(
            DatabricksHelpers,
            "get_databricks_host_and_token",
            return_value=("https://h", "tok"),
        ), patch.object(
            DatabricksHelpers,
            "resolve_build_warehouse_id",
            return_value="",
        ):
            with pytest.raises(ValidationError, match="Build SQL Warehouse"):
                DatabricksHelpers.get_build_sql_credentials(MagicMock(), MagicMock())


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


class TestResolveLakehouseUseSea:
    def test_reads_global_use_sea(self):
        with patch.object(
            DatabricksHelpers,
            "get_databricks_host_and_token",
            return_value=("https://h", "tok"),
        ), patch.object(
            DatabricksHelpers,
            "_resolve_registry_cfg",
            return_value=REGISTRY_CFG,
        ), patch(
            "back.objects.session.global_config_service.get_delta_warehouse_use_sea",
            return_value=True,
        ):
            assert DatabricksHelpers.resolve_lakehouse_use_sea(MagicMock(), MagicMock()) is True

    def test_defaults_false_without_registry(self):
        with patch.object(
            DatabricksHelpers,
            "get_databricks_host_and_token",
            return_value=("", ""),
        ), patch.object(
            DatabricksHelpers,
            "_resolve_registry_cfg",
            return_value={},
        ):
            assert DatabricksHelpers.resolve_lakehouse_use_sea(MagicMock(), MagicMock()) is False


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

    def test_triplestore_sql_credentials_uses_build_warehouse_for_databricks(self):
        domain = MagicMock()
        settings = MagicMock()
        with patch.object(
            DatabricksHelpers,
            "get_build_sql_credentials",
            return_value=("h", "t", "wh-build"),
        ) as build_creds:
            creds = get_triplestore_sql_credentials(domain, settings)
        assert creds == ("h", "t", "wh-build")
        build_creds.assert_called_once()

    def test_triplestore_sql_credentials_uses_build_warehouse_for_lakebase(self):
        domain = MagicMock()
        settings = MagicMock()
        with patch.object(
            DatabricksHelpers,
            "get_build_sql_credentials",
            return_value=("h", "t", "wh-build"),
        ) as build_creds:
            creds = get_triplestore_sql_credentials(domain, settings)
        assert creds == ("h", "t", "wh-build")
        build_creds.assert_called_once()


class TestSettingsServiceDeltaWarehouse:
    def test_get_delta_warehouse_result_includes_warehouse_and_location(self):
        domain = MagicMock()
        with patch.object(
            SettingsService, "_resolve_context", return_value=(domain, "h", "t", REGISTRY_CFG)
        ), patch(
            "back.objects.domain.SettingsService.global_config_service.load"
        ), patch(
            "back.objects.domain.SettingsService.global_config_service.get_delta_warehouse_id",
            return_value="wh-delta",
        ), patch(
            "back.objects.domain.SettingsService.global_config_service.get_delta_warehouse_use_sea",
            return_value=True,
        ), patch(
            "back.objects.domain.SettingsService.get_domain",
            return_value=domain,
        ), patch(
            "back.objects.domain.SettingsService.resolve_delta_warehouse_id",
            return_value="wh-delta",
        ):
            result = SettingsService.get_delta_warehouse_result(
                MagicMock(), MagicMock()
            )
        assert result["delta_warehouse_id"] == "wh-delta"
        assert result["use_sea"] is True
        assert result["effective_delta_warehouse_id"] == "wh-delta"
        assert result["storage_location"] == "cat.sch"
        assert result["registry_configured"] is True

    def test_select_delta_warehouse_requires_id(self):
        with pytest.raises(ValidationError, match="No warehouse ID"):
            SettingsService.select_delta_warehouse(
                None, "a@b.com", "tok", MagicMock(), MagicMock()
            )

    def test_select_delta_warehouse_requires_override_when_rt_enabled(self):
        with pytest.raises(ValidationError, match="Query SQL Warehouse"):
            SettingsService.select_delta_warehouse(
                "", "a@b.com", "tok", MagicMock(), MagicMock(), use_sea=True
            )

    def test_select_delta_warehouse_requires_distinct_rt_warehouse(self):
        with patch(
            "back.objects.domain.SettingsService.resolve_warehouse_id",
            return_value="wh-build",
        ):
            with pytest.raises(ValidationError, match="different"):
                SettingsService.select_delta_warehouse(
                    "wh-build",
                    "a@b.com",
                    "tok",
                    MagicMock(),
                    MagicMock(),
                    use_sea=True,
                )

    def test_disabling_rt_clears_query_override(self):
        domain = MagicMock()
        with patch.object(
            SettingsService,
            "_resolve_context",
            return_value=(domain, "h", "t", REGISTRY_CFG),
        ), patch.object(SettingsService, "require_admin_error"), patch(
            "back.objects.domain.SettingsService.global_config_service.set_delta_warehouse_id",
            return_value=(True, "ok"),
        ) as mock_set, patch(
            "back.objects.domain.SettingsService.global_config_service.load"
        ), patch.object(
            SettingsService, "_mirror_graph_engine_to_domain_registry"
        ), patch(
            "back.objects.domain.SettingsService.resolve_delta_warehouse_id",
            return_value="wh-build",
        ):
            result = SettingsService.select_delta_warehouse(
                "wh-old-rt",
                "a@b.com",
                "tok",
                MagicMock(),
                MagicMock(),
                use_sea=False,
            )
        assert result["delta_warehouse_id"] == ""
        assert result["use_sea"] is False
        mock_set.assert_called_once_with(
            "h", "t", REGISTRY_CFG, "", use_sea=False
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
        assert result["use_sea"] is False
        mock_set.assert_called_once_with("h", "t", REGISTRY_CFG, "", use_sea=False)

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
                "wh-delta", "a@b.com", "tok", MagicMock(), MagicMock(), use_sea=True
            )
        assert result["success"]
        assert result["use_sea"] is True
        mock_set.assert_called_once_with(
            "h", "t", REGISTRY_CFG, "wh-delta", use_sea=True
        )
        mock_mirror.assert_called_once()
        assert mock_mirror.call_args.kwargs.get("delta_warehouse_id") == "wh-delta"


class TestSettingsServiceBuildWarehouse:
    def test_current_config_includes_build_transport(self):
        domain = MagicMock()
        domain.databricks = {"host": "https://h", "token": "tok"}
        settings = MagicMock(
            databricks_host="https://h",
            databricks_token="",
            sql_warehouse_id="wh-bound",
        )
        with patch(
            "back.objects.domain.SettingsService.get_domain",
            return_value=domain,
        ), patch(
            "back.objects.domain.SettingsService.resolve_warehouse_id",
            return_value="wh-build",
        ), patch(
            "back.objects.domain.SettingsService.resolve_build_use_sea",
            return_value=True,
        ), patch(
            "back.objects.domain.SettingsService.resolve_use_cloud_fetch",
            return_value=True,
        ), patch.object(
            SettingsService,
            "is_warehouse_locked",
            return_value=True,
        ):
            result = SettingsService.build_current_config(MagicMock(), settings)
        assert result["warehouse_id"] == "wh-build"
        assert result["warehouse_use_sea"] is True

    def test_select_build_warehouse_rejects_rt(self):
        with pytest.raises(ValidationError, match="does not support build"):
            SettingsService.select_build_warehouse(
                "wh-rt",
                "REYDEN",
                True,
                "a@b.com",
                "tok",
                MagicMock(),
                MagicMock(),
            )

    def test_select_build_warehouse_persists_transport(self):
        domain = MagicMock()
        settings = MagicMock(sql_warehouse_id="")
        with patch.object(
            SettingsService,
            "_resolve_context",
            return_value=(domain, "h", "t", REGISTRY_CFG),
        ), patch.object(
            SettingsService, "require_admin_error"
        ), patch.object(
            SettingsService, "is_warehouse_locked", return_value=False
        ), patch(
            "back.objects.domain.SettingsService.global_config_service.set_build_warehouse",
            return_value=(True, "ok"),
        ) as mock_set:
            result = SettingsService.select_build_warehouse(
                "wh-build",
                "PRO",
                True,
                "a@b.com",
                "tok",
                MagicMock(),
                settings,
            )
        assert result == {
            "success": True,
            "warehouse_id": "wh-build",
            "use_sea": True,
        }
        mock_set.assert_called_once_with(
            "h", "t", REGISTRY_CFG, "wh-build", use_sea=True
        )

    def test_select_build_warehouse_can_override_bound_default(self):
        domain = MagicMock()
        settings = MagicMock(sql_warehouse_id="wh-bound")
        with patch.object(
            SettingsService,
            "_resolve_context",
            return_value=(domain, "h", "t", REGISTRY_CFG),
        ), patch.object(
            SettingsService, "require_admin_error"
        ), patch.object(
            SettingsService, "is_warehouse_locked", return_value=True
        ), patch(
            "back.objects.domain.SettingsService.global_config_service.set_build_warehouse",
            return_value=(True, "ok"),
        ) as mock_set:
            result = SettingsService.select_build_warehouse(
                "wh-other",
                "PRO",
                False,
                "a@b.com",
                "tok",
                MagicMock(),
                settings,
            )
        assert result["warehouse_id"] == "wh-other"
        mock_set.assert_called_once_with(
            "h", "t", REGISTRY_CFG, "wh-other", use_sea=False
        )
