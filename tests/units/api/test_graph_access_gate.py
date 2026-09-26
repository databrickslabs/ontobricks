import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock

from back.core.errors import AuthorizationError
from api.routers.internal._graph_access import (
    assert_domain_graph_read,
    assert_public_graph_read,
)


def _req(app_role="app_user", domain_role="none"):
    st = SimpleNamespace(user_role=app_role, user_domain_role=domain_role)
    return SimpleNamespace(state=st)


def test_local_mode_is_noop(monkeypatch):
    monkeypatch.setattr(
        "api.routers.internal._graph_access.is_databricks_app", lambda: False
    )
    assert_domain_graph_read(_req(domain_role="none"))  # no raise


def test_app_mode_blocks_non_member(monkeypatch):
    monkeypatch.setattr(
        "api.routers.internal._graph_access.is_databricks_app", lambda: True
    )
    with pytest.raises(AuthorizationError):
        assert_domain_graph_read(_req(app_role="app_user", domain_role="none"))


def test_app_mode_allows_viewer(monkeypatch):
    monkeypatch.setattr(
        "api.routers.internal._graph_access.is_databricks_app", lambda: True
    )
    assert_domain_graph_read(_req(app_role="app_user", domain_role="viewer"))


def test_app_admin_bypasses(monkeypatch):
    monkeypatch.setattr(
        "api.routers.internal._graph_access.is_databricks_app", lambda: True
    )
    assert_domain_graph_read(_req(app_role="admin", domain_role="none"))


# ------------------------------------------------------------------
# Public-API gate: resolves the forwarded user's role for the *target*
# (MCP-selected) domain and enforces the same Viewer+ / admin / fail-closed
# rules as the internal (Explorer / Chat) routes.
# ------------------------------------------------------------------


def _pub_req():
    st = SimpleNamespace(user_role="", user_domain_role="", user_email="u@x.com")
    return SimpleNamespace(state=st)


def _patch(monkeypatch, *, app_mode=True, resolved_role="none"):
    monkeypatch.setattr(
        "api.routers.internal._graph_access.is_databricks_app", lambda: app_mode
    )
    monkeypatch.setattr(
        "back.objects.domain.SettingsService.resolve_domain_role",
        lambda request, folder, settings, **kw: resolved_role,
    )


def test_public_local_mode_is_noop(monkeypatch):
    _patch(monkeypatch, app_mode=False, resolved_role="none")
    domain = SimpleNamespace(domain_folder="d")
    assert_public_graph_read(_pub_req(), domain, object())  # no raise


def test_public_blocks_non_member(monkeypatch):
    _patch(monkeypatch, app_mode=True, resolved_role="none")
    domain = SimpleNamespace(domain_folder="d")
    with pytest.raises(AuthorizationError):
        assert_public_graph_read(_pub_req(), domain, object())


def test_public_blocks_when_no_forwarded_user(monkeypatch):
    # Unattended M2M caller: resolve_domain_role returns "" (no email/token).
    _patch(monkeypatch, app_mode=True, resolved_role="")
    domain = SimpleNamespace(domain_folder="d")
    with pytest.raises(AuthorizationError):
        assert_public_graph_read(_pub_req(), domain, object())


def test_public_allows_viewer_and_stamps_state(monkeypatch):
    _patch(monkeypatch, app_mode=True, resolved_role="viewer")
    req = _pub_req()
    domain = SimpleNamespace(domain_folder="d")
    assert_public_graph_read(req, domain, object())  # no raise
    assert req.state.user_domain_role == "viewer"


def test_public_admin_bypasses(monkeypatch):
    _patch(monkeypatch, app_mode=True, resolved_role="admin")
    req = _pub_req()
    domain = SimpleNamespace(domain_folder="d")
    assert_public_graph_read(req, domain, object())  # no raise
    assert req.state.user_role == "admin"


# ------------------------------------------------------------------
# Wiring: the public /api/v1/digitaltwin + /graphql read handlers call the
# gate *before* touching the store, so MCP-driven reads are Team-gated in
# App mode exactly like Explorer / Chat on the internal routes.
# ------------------------------------------------------------------


async def test_public_dt_triples_find_blocks_non_member(monkeypatch):
    from api.routers import digitaltwin as dt

    _patch(monkeypatch, app_mode=True, resolved_role="none")
    monkeypatch.setattr(
        dt.DigitalTwin,
        "resolve_domain",
        classmethod(lambda cls, *a, **k: SimpleNamespace(domain_folder="d")),
    )
    # get_graphdb must never be reached — the gate raises first.
    monkeypatch.setattr(
        dt, "get_graphdb", lambda *a, **k: pytest.fail("store built before gate")
    )
    req = _pub_req()
    with pytest.raises(AuthorizationError):
        await dt.dt_triples_find(
            request=req, search="x", session_mgr=MagicMock(), settings=MagicMock()
        )


async def test_public_graphql_execute_blocks_non_member(monkeypatch):
    import back.fastapi.graphql_routes as gql

    _patch(monkeypatch, app_mode=True, resolved_role="none")
    monkeypatch.setattr(
        gql,
        "_load_domain_from_registry",
        lambda *a, **k: SimpleNamespace(domain_folder="d"),
    )
    monkeypatch.setattr(
        gql,
        "_get_schema_and_context",
        lambda *a, **k: pytest.fail("schema/context built before gate"),
    )
    req = SimpleNamespace(
        state=SimpleNamespace(user_role="", user_domain_role="", user_email="u@x.com"),
        scope={"path": "/graphql/d"},
    )
    body = gql.GraphQLRequest(query="{ __typename }")
    with pytest.raises(AuthorizationError):
        await gql.graphql_execute(
            request=req,
            domain_name="d",
            body=body,
            session_mgr=MagicMock(),
            settings=MagicMock(),
        )
