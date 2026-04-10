"""Tests for back.core.helpers — Databricks client/credentials helpers."""
import pytest
from unittest.mock import patch, MagicMock, PropertyMock

from back.core.helpers import (
    get_databricks_client,
    get_databricks_credentials,
    get_databricks_host_and_token,
)


def _make_project(**overrides):
    """Build a minimal project-like object."""
    data = {"host": "", "token": "", "warehouse_id": ""}
    data.update(overrides)
    proj = MagicMock()
    proj.databricks = data
    return proj


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
        proj = _make_project()
        settings = _make_settings()
        client = get_databricks_client(proj, settings)
        assert client is not None

    @patch("back.core.databricks.is_databricks_app", return_value=False)
    def test_returns_none_without_credentials(self, _):
        proj = _make_project()
        settings = _make_settings(databricks_host="", databricks_token="")
        client = get_databricks_client(proj, settings)
        assert client is None

    @patch("back.core.databricks.is_databricks_app", return_value=True)
    def test_returns_client_in_app_mode(self, _):
        proj = _make_project()
        settings = _make_settings(databricks_host="", databricks_token="")
        client = get_databricks_client(proj, settings)
        assert client is not None

    @patch("back.core.databricks.is_databricks_app", return_value=False)
    def test_project_overrides_settings(self, _):
        proj = _make_project(host="https://proj.databricks.com", token="proj-tok")
        settings = _make_settings()
        client = get_databricks_client(proj, settings)
        assert client is not None
        assert "proj.databricks.com" in client.host


class TestGetDatabricksCredentials:
    @patch("back.core.databricks.is_databricks_app", return_value=False)
    def test_returns_three_values(self, _):
        proj = _make_project()
        settings = _make_settings()
        host, token, wh = get_databricks_credentials(proj, settings)
        assert host
        assert token
        assert wh


class TestGetDatabricksHostAndToken:
    @patch("back.core.databricks.is_databricks_app", return_value=False)
    def test_normalizes_host(self, _):
        proj = _make_project(host="test.databricks.com")
        settings = _make_settings(databricks_host="", databricks_token="")
        host, token = get_databricks_host_and_token(proj, settings)
        assert host.startswith("https://")

    @patch("back.core.databricks.is_databricks_app", return_value=False)
    def test_settings_fallback(self, _):
        proj = _make_project()
        settings = _make_settings()
        host, token = get_databricks_host_and_token(proj, settings)
        assert "test.databricks.com" in host
        assert token == "tok-123"
