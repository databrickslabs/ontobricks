"""An explicit on-behalf-of token wins over the app service principal (Apps mode)."""

from __future__ import annotations

import pytest

from back.core.databricks.DatabricksAuth import DatabricksAuth

pytestmark = pytest.mark.unit


@pytest.fixture
def app_mode(monkeypatch):
    monkeypatch.setenv("DATABRICKS_APP_PORT", "8000")
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "sp-id")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "sp-secret")
    monkeypatch.setattr(
        DatabricksAuth,
        "_resolve_global_cloud_fetch_default",
        staticmethod(lambda h, t: False),
    )


def _auth(**kw):
    return DatabricksAuth(host="https://ws.example", warehouse_id="wh", **kw)


def test_on_behalf_of_uses_the_caller_token(app_mode):
    auth = _auth(token="user-token", on_behalf_of=True)
    assert auth.uses_app_credentials is False
    assert auth.get_auth_headers()["Authorization"] == "Bearer user-token"
    params = auth.get_sql_connection_params()
    assert params["access_token"] == "user-token"
    assert "credentials_provider" not in params
    assert auth.has_valid_auth() is True


def test_without_the_flag_apps_mode_keeps_the_service_principal(app_mode, monkeypatch):
    monkeypatch.setattr(DatabricksAuth, "get_oauth_token", lambda self: "sp-token")
    auth = _auth(token="some-token")
    assert auth.uses_app_credentials is True
    assert auth.get_auth_headers()["Authorization"] == "Bearer sp-token"
    assert "credentials_provider" in auth.get_sql_connection_params()


def test_flag_without_a_token_does_not_drop_the_service_principal(app_mode):
    auth = _auth(on_behalf_of=True)
    assert auth.on_behalf_of is False and auth.uses_app_credentials is True


def test_data_plane_client_asks_for_on_behalf_of(app_mode, monkeypatch):
    from back.core.databricks.request_identity import RequestIdentity
    import importlib

    helpers_module = importlib.import_module("back.core.helpers.DatabricksHelpers")
    from back.core.helpers.DatabricksHelpers import DatabricksHelpers

    monkeypatch.setattr(
        helpers_module,
        "get_request_identity",
        lambda: RequestIdentity(email="u@x", user_token="user-token"),
    )
    monkeypatch.setattr(
        DatabricksHelpers, "resolve_warehouse_id", staticmethod(lambda d, s: "wh")
    )
    monkeypatch.setattr(
        DatabricksHelpers, "resolve_use_cloud_fetch", staticmethod(lambda d, s: False)
    )
    settings = type("S", (), {"databricks_host": "https://ws.example"})()
    domain = type("D", (), {"databricks": {}})()
    client = DatabricksHelpers.get_data_plane_client(domain, settings)
    assert client.auth.on_behalf_of is True
    assert client.auth.get_auth_headers()["Authorization"] == "Bearer user-token"
