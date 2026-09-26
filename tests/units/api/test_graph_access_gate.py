import pytest
from types import SimpleNamespace

from back.core.errors import AuthorizationError
from api.routers.internal._graph_access import assert_domain_graph_read


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
