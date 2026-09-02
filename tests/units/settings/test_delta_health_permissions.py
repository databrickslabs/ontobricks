"""Tests for Settings Lakehouse Health schema-permission diagnostics."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from back.core.errors import InfrastructureError
from back.objects.domain.SettingsService import SettingsService

pytestmark = pytest.mark.unit


def _client(*, client_id: str = "app-client-id") -> MagicMock:
    client = MagicMock()
    client.auth.client_id = client_id
    client.workspace.get_current_user_email.return_value = "dev@example.com"
    return client


class TestTripleStoreDatabricksHealthService:
    def test_uses_app_registry_context_and_schema_permissions_only(self):
        settings = MagicMock()
        session_mgr = MagicMock()
        client = _client(client_id="app-client-id")
        client.catalog.get_effective_schema_permissions.return_value = {
            "accessible": True,
            "assignments": [
                {"privilege": "USE_CATALOG", "inherited_from": "main"},
                {"privilege": "USE_SCHEMA", "inherited_from": ""},
                {"privilege": "CREATE_TABLE", "inherited_from": ""},
                {"privilege": "CREATE_VIEW", "inherited_from": ""},
                {"privilege": "SELECT", "inherited_from": ""},
                {"privilege": "MODIFY", "inherited_from": ""},
            ],
            "error": None,
        }
        app_ctx = ("https://adb", "dapi-token", {"catalog": "main", "schema": "graph"})

        with patch(
            "back.objects.domain.SettingsService.resolve_app_registry_context",
            return_value=app_ctx,
        ) as mock_ctx, patch(
            "back.objects.domain.SettingsService.DatabricksClient",
            return_value=client,
            create=True,
        ) as mock_ctor, patch(
            "back.objects.domain.SettingsService.get_domain",
            side_effect=AssertionError("get_domain must not be called"),
        ), patch(
            "back.core.graphdb.delta.DeltaBase.create_databricks_client",
            side_effect=AssertionError("create_databricks_client must not be called"),
        ), patch(
            "back.core.graphdb.delta._table_naming.view_fqn",
            side_effect=AssertionError("table naming must not be called"),
        ), patch(
            "back.core.graphdb.delta._table_naming.data_table_fqn",
            side_effect=AssertionError("table naming must not be called"),
        ), patch(
            "back.core.graphdb.delta._table_naming.inferred_table_fqn",
            side_effect=AssertionError("table naming must not be called"),
        ):
            result = SettingsService.triple_store_databricks_health_result(
                session_mgr, settings
            )

        assert result.keys() >= {
            "success",
            "registry_configured",
            "registry_catalog",
            "registry_schema",
            "storage_location",
            "principal",
            "accessible",
            "operational",
            "permissions",
            "error",
        }
        assert "active_domain" not in result
        assert "view_fqn" not in result
        assert "data_table" not in result
        assert "warehouse_id" not in result
        assert result["success"] is True
        assert result["storage_location"] == "main.graph"
        assert result["principal"] == "app-client-id"
        assert result["accessible"] is True
        assert result["operational"] is True
        assert result["error"] is None
        mock_ctx.assert_called_once_with(settings)
        mock_ctor.assert_called_once_with(host="https://adb", token="dapi-token")
        client.catalog.get_effective_schema_permissions.assert_called_once_with(
            "main", "graph", "app-client-id"
        )

    def test_falls_back_to_authenticated_user_when_app_client_id_is_missing(self):
        client = _client(client_id="")
        client.workspace.get_current_user_email.return_value = "me@databricks.com"
        client.catalog.get_effective_schema_permissions.return_value = {
            "accessible": True,
            "assignments": [{"privilege": "USE_SCHEMA", "inherited_from": ""}],
            "error": None,
        }

        with patch(
            "back.objects.domain.SettingsService.resolve_app_registry_context",
            return_value=("https://adb", "tok", {"catalog": "main", "schema": "graph"}),
        ), patch(
            "back.objects.domain.SettingsService.DatabricksClient",
            return_value=client,
            create=True,
        ):
            result = SettingsService.triple_store_databricks_health_result(
                MagicMock(), MagicMock()
            )

        assert result["principal"] == "me@databricks.com"
        client.catalog.get_effective_schema_permissions.assert_called_once_with(
            "main", "graph", "me@databricks.com"
        )

    def test_returns_registry_not_configured_diagnostic_without_rest_call(self):
        client = _client(client_id="app-client-id")

        with patch(
            "back.objects.domain.SettingsService.resolve_app_registry_context",
            return_value=("https://adb", "tok", {"catalog": "main", "schema": ""}),
        ), patch(
            "back.objects.domain.SettingsService.DatabricksClient",
            return_value=client,
            create=True,
        ) as mock_ctor:
            result = SettingsService.triple_store_databricks_health_result(
                MagicMock(), MagicMock()
            )

        assert result["success"] is True
        assert result["registry_configured"] is False
        assert result["storage_location"] == ""
        assert result["permissions"] == []
        assert "not configured" in str(result["error"]).lower()
        mock_ctor.assert_not_called()
        client.catalog.get_effective_schema_permissions.assert_not_called()

    def test_missing_grants_stays_a_http_200_diagnostic(self):
        client = _client(client_id="app-client-id")
        client.catalog.get_effective_schema_permissions.return_value = {
            "accessible": True,
            "assignments": [{"privilege": "USE_SCHEMA", "inherited_from": ""}],
            "error": None,
        }
        with patch(
            "back.objects.domain.SettingsService.resolve_app_registry_context",
            return_value=("https://adb", "tok", {"catalog": "main", "schema": "graph"}),
        ), patch(
            "back.objects.domain.SettingsService.DatabricksClient",
            return_value=client,
            create=True,
        ):
            result = SettingsService.triple_store_databricks_health_result(
                MagicMock(), MagicMock()
            )

        assert result["success"] is True
        assert result["accessible"] is True
        assert result["operational"] is False
        assert result["error"] is None

    def test_request_failures_are_mapped_to_infrastructure_error(self):
        client = _client(client_id="app-client-id")
        client.catalog.get_effective_schema_permissions.side_effect = RuntimeError("boom")

        with patch(
            "back.objects.domain.SettingsService.resolve_app_registry_context",
            return_value=("https://adb", "tok", {"catalog": "main", "schema": "graph"}),
        ), patch(
            "back.objects.domain.SettingsService.DatabricksClient",
            return_value=client,
            create=True,
        ):
            with pytest.raises(InfrastructureError):
                SettingsService.triple_store_databricks_health_result(
                    MagicMock(), MagicMock()
                )


class TestTripleStoreDatabricksHealthRoute:
    def test_endpoint_keeps_map_route_errors_wrapper(self):
        from shared.fastapi.main import app

        client = TestClient(app, raise_server_exceptions=False)
        with patch(
            "api.routers.internal.settings.config_service.triple_store_databricks_health_result",
            side_effect=RuntimeError("socket timeout"),
        ):
            response = client.get("/settings/triple-store/databricks-health")

        assert response.status_code == 502
        body = response.json()
        assert body["error"] == "infrastructure"
        assert body["message"] == "Databricks triple store health"
