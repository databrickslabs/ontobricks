"""Tests for back.core.helpers — Databricks client/credentials helpers."""

import pytest
from unittest.mock import patch, MagicMock, PropertyMock

from back.core.helpers import (
    get_databricks_client,
    get_databricks_credentials,
    get_databricks_host_and_token,
    resolve_use_cloud_fetch,
)
from back.core.helpers.DatabricksHelpers import DatabricksHelpers


def _make_domain(**overrides):
    """Build a minimal domain-session-like object for credential resolution."""
    data = {"host": "", "token": "", "warehouse_id": ""}
    data.update(overrides)
    domain = MagicMock()
    domain.databricks = data
    return domain


def _make_settings(**overrides):
    defaults = {
        "databricks_host": "https://test.databricks.com",
        "databricks_token": "tok-123",
        "databricks_sql_warehouse_id": "wh-1",
    }
    defaults.update(overrides)
    s = MagicMock()
    for k, v in defaults.items():
        setattr(s, k, v)
    return s


class TestGetDatabricksClient:
    @patch("back.core.databricks.is_databricks_app", return_value=False)
    def test_returns_client_with_credentials(self, _):
        domain = _make_domain()
        settings = _make_settings()
        client = get_databricks_client(domain, settings)
        assert client is not None

    @patch("back.core.databricks.is_databricks_app", return_value=False)
    def test_returns_none_without_credentials(self, _):
        domain = _make_domain()
        settings = _make_settings(databricks_host="", databricks_token="")
        client = get_databricks_client(domain, settings)
        assert client is None

    @patch("back.core.databricks.is_databricks_app", return_value=True)
    def test_returns_client_in_app_mode(self, _):
        domain = _make_domain()
        settings = _make_settings(databricks_host="", databricks_token="")
        client = get_databricks_client(domain, settings)
        assert client is not None

    @patch("back.core.databricks.is_databricks_app", return_value=False)
    def test_domain_overrides_settings(self, _):
        domain = _make_domain(host="https://proj.databricks.com", token="proj-tok")
        settings = _make_settings()
        client = get_databricks_client(domain, settings)
        assert client is not None
        assert "proj.databricks.com" in client.host


class TestGetDatabricksCredentials:
    @patch("back.core.databricks.is_databricks_app", return_value=False)
    def test_returns_three_values(self, _):
        domain = _make_domain()
        settings = _make_settings()
        host, token, wh = get_databricks_credentials(domain, settings)
        assert host
        assert token
        assert wh


class TestGetDatabricksHostAndToken:
    @patch("back.core.databricks.is_databricks_app", return_value=False)
    def test_normalizes_host(self, _):
        domain = _make_domain(host="test.databricks.com")
        settings = _make_settings(databricks_host="", databricks_token="")
        host, token = get_databricks_host_and_token(domain, settings)
        assert host.startswith("https://")

    @patch("back.core.databricks.is_databricks_app", return_value=False)
    def test_settings_fallback(self, _):
        domain = _make_domain()
        settings = _make_settings()
        host, token = get_databricks_host_and_token(domain, settings)
        assert "test.databricks.com" in host
        assert token == "tok-123"

    @patch("back.core.databricks.is_databricks_app", return_value=False)
    def test_none_domain_falls_back_to_settings(self, _):
        # Session-less callers (the readiness probe, MCP, scheduled jobs)
        # pass ``domain=None``.  Helpers must not blow up on
        # ``domain.databricks.get(...)``; they should fall through to the
        # Pydantic Settings defaults instead.
        settings = _make_settings()
        host, token = get_databricks_host_and_token(None, settings)
        assert "test.databricks.com" in host
        assert token == "tok-123"


class TestResolveUseCloudFetch:
    """Admin-disabled CloudFetch must propagate through the helper.

    Regression for a bug where ``resolve_use_cloud_fetch`` went through
    ``_resolve_global_setting`` whose ``if val: return val`` short-circuit
    treated a legitimate ``False`` as "not configured" and silently
    returned the ``True`` default — re-enabling CloudFetch despite the
    admin toggle being off.
    """

    @patch.object(
        DatabricksHelpers,
        "_resolve_registry_cfg",
        return_value={"catalog": "main", "schema": "ob"},
    )
    @patch.object(
        DatabricksHelpers,
        "get_databricks_host_and_token",
        return_value=("https://test.databricks.com", "tok-123"),
    )
    def test_returns_false_when_globally_disabled(self, _hst, _rcfg):
        with patch(
            "back.objects.session.global_config_service.get_use_cloud_fetch",
            return_value=False,
        ):
            assert resolve_use_cloud_fetch(_make_domain(), _make_settings()) is False

    @patch.object(
        DatabricksHelpers,
        "_resolve_registry_cfg",
        return_value={"catalog": "main", "schema": "ob"},
    )
    @patch.object(
        DatabricksHelpers,
        "get_databricks_host_and_token",
        return_value=("https://test.databricks.com", "tok-123"),
    )
    def test_returns_true_when_globally_enabled(self, _hst, _rcfg):
        with patch(
            "back.objects.session.global_config_service.get_use_cloud_fetch",
            return_value=True,
        ):
            assert resolve_use_cloud_fetch(_make_domain(), _make_settings()) is True

    @patch.object(
        DatabricksHelpers,
        "_resolve_registry_cfg",
        return_value={"catalog": "", "schema": ""},
    )
    @patch.object(
        DatabricksHelpers,
        "get_databricks_host_and_token",
        return_value=("https://test.databricks.com", "tok-123"),
    )
    def test_defaults_to_true_when_registry_unconfigured(self, _hst, _rcfg):
        assert resolve_use_cloud_fetch(_make_domain(), _make_settings()) is True


class TestGetDatabricksClientCloudFetch:
    """End-to-end: a disabled global toggle must reach ``DatabricksAuth``."""

    @patch("back.core.databricks.is_databricks_app", return_value=False)
    @patch.object(DatabricksHelpers, "resolve_use_cloud_fetch", return_value=False)
    def test_disabled_propagates_to_auth(self, _rcf, _app):
        client = get_databricks_client(_make_domain(), _make_settings())
        assert client is not None
        assert client.auth.use_cloud_fetch is False
        assert client.auth.can_use_cloud_fetch() is False


class TestNoneDomainSafety:
    """Regression coverage: every credential helper must accept ``domain=None``."""

    @patch("back.core.databricks.is_databricks_app", return_value=False)
    def test_get_databricks_client_with_none_domain(self, _):
        client = get_databricks_client(None, _make_settings())
        assert client is not None
        assert "test.databricks.com" in client.host

    @patch("back.core.databricks.is_databricks_app", return_value=False)
    def test_get_databricks_credentials_with_none_domain(self, _):
        host, token, wh = get_databricks_credentials(None, _make_settings())
        assert host and token and wh

    @patch("back.core.databricks.is_databricks_app", return_value=False)
    def test_get_databricks_client_none_domain_no_creds_returns_none(self, _):
        settings = _make_settings(databricks_host="", databricks_token="")
        assert get_databricks_client(None, settings) is None
