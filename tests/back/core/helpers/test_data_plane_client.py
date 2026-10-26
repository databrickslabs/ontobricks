import pytest
from types import SimpleNamespace

from back.core.databricks.request_identity import (
    RequestIdentity,
    set_request_identity,
    reset_request_identity,
)
from back.core.errors import AuthorizationError
from back.core.helpers import get_data_plane_client


class _FakeClient:
    def __init__(self, host="", token="", warehouse_id="", use_cloud_fetch=False):
        self.host = host
        self.token = token
        self.warehouse_id = warehouse_id
        self.use_cloud_fetch = use_cloud_fetch


def _patch(monkeypatch, app_mode: bool):
    import importlib

    H = importlib.import_module("back.core.helpers.DatabricksHelpers")

    monkeypatch.setattr(H._databricks, "is_databricks_app", lambda: app_mode)
    monkeypatch.setattr(H._databricks, "DatabricksClient", _FakeClient)
    monkeypatch.setattr(H.DatabricksHelpers, "resolve_warehouse_id", lambda d, s: "wh1")
    monkeypatch.setattr(
        H.DatabricksHelpers, "resolve_use_cloud_fetch", lambda d, s: False
    )
    monkeypatch.setattr(H, "_domain_databricks", lambda d: {"host": "https://x"})


def test_app_mode_uses_user_token(monkeypatch):
    _patch(monkeypatch, app_mode=True)
    tok = set_request_identity(
        RequestIdentity(email="u@x", user_token="USER_TOK", domain_role="viewer")
    )
    try:
        client = get_data_plane_client(
            SimpleNamespace(),
            SimpleNamespace(databricks_host="", databricks_token=""),
        )
        assert client.token == "USER_TOK"
        assert client.warehouse_id == "wh1"
    finally:
        reset_request_identity(tok)


def test_app_mode_without_user_token_fails_closed(monkeypatch):
    _patch(monkeypatch, app_mode=True)
    tok = set_request_identity(
        RequestIdentity(email="u@x", user_token="", domain_role="viewer")
    )
    try:
        with pytest.raises(AuthorizationError):
            get_data_plane_client(
                SimpleNamespace(),
                SimpleNamespace(databricks_host="", databricks_token=""),
            )
    finally:
        reset_request_identity(tok)
