"""Tests for back.core.helpers — Databricks client/credentials helpers."""

import pytest
from unittest.mock import patch, MagicMock, PropertyMock

from back.core.helpers import (
    get_databricks_client,
    get_databricks_credentials,
    get_databricks_host_and_token,
)


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
