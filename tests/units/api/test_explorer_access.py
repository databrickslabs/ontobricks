import pytest
from types import SimpleNamespace

from back.core.errors import AuthorizationError
from api.routers.internal._graph_access import assert_domain_graph_read


def test_non_member_blocked_on_read(monkeypatch):
    monkeypatch.setattr(
        "api.routers.internal._graph_access.is_databricks_app", lambda: True
    )
    req = SimpleNamespace(
        state=SimpleNamespace(user_role="app_user", user_domain_role="none")
    )
    with pytest.raises(AuthorizationError):
        assert_domain_graph_read(req)


def test_member_allowed_on_read(monkeypatch):
    monkeypatch.setattr(
        "api.routers.internal._graph_access.is_databricks_app", lambda: True
    )
    req = SimpleNamespace(
        state=SimpleNamespace(user_role="app_user", user_domain_role="viewer")
    )
    assert_domain_graph_read(req)  # no raise


def test_delta_read_prefers_forwarded_user_token(monkeypatch):
    """A user-driven Delta read binds the caller's forwarded token (OBO)."""
    import back.core.graphdb.delta.DeltaBase as db
    from back.core.databricks.request_identity import (
        RequestIdentity,
        set_request_identity,
        reset_request_identity,
    )

    captured = {}

    class _Client:
        def __init__(self, host="", token="", warehouse_id="", use_sea=False, use_cloud_fetch=False):
            captured["token"] = token

    monkeypatch.setattr(db, "is_databricks_app", lambda: True)
    monkeypatch.setattr(
        db, "get_databricks_host_and_token", lambda d, s: ("https://x", "SP_TOKEN")
    )
    monkeypatch.setattr(db, "resolve_delta_warehouse_id", lambda d, s: "wh")
    monkeypatch.setattr(db, "resolve_lakehouse_use_sea", lambda d, s: False)
    monkeypatch.setattr(db, "resolve_use_cloud_fetch", lambda d, s: False)
    # DatabricksClient is imported inside the function from back.core.databricks.
    monkeypatch.setattr("back.core.databricks.DatabricksClient", _Client)

    tok = set_request_identity(
        RequestIdentity(email="u", user_token="USER_TOKEN", domain_role="viewer")
    )
    try:
        db.create_databricks_client(SimpleNamespace(), SimpleNamespace(), for_write=False)
    finally:
        reset_request_identity(tok)

    assert captured["token"] == "USER_TOKEN"


def test_delta_build_keeps_service_principal(monkeypatch):
    """A build (for_write) read keeps the service-principal credentials."""
    import back.core.graphdb.delta.DeltaBase as db

    captured = {}

    class _Client:
        def __init__(self, host="", token="", warehouse_id="", use_sea=False, use_cloud_fetch=False):
            captured["token"] = token

    monkeypatch.setattr(db, "is_databricks_app", lambda: True)
    monkeypatch.setattr(
        db, "get_build_sql_credentials", lambda d, s: ("https://x", "SP_TOKEN", "wh")
    )
    monkeypatch.setattr(db, "resolve_build_use_sea", lambda d, s: False)
    monkeypatch.setattr(db, "resolve_use_cloud_fetch", lambda d, s: False)
    monkeypatch.setattr("back.core.databricks.DatabricksClient", _Client)

    db.create_databricks_client(SimpleNamespace(), SimpleNamespace(), for_write=True)

    assert captured["token"] == "SP_TOKEN"
