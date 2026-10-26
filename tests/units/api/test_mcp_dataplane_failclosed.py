"""Data-plane routes fail-closed when no end-user identity is forwarded.

When the MCP server (or any caller) reaches a graph/UC read route without a
resolved end-user, the Team gate must deny access rather than silently run as
the service principal. This locks that contract at the gate level.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from api.routers.internal._graph_access import assert_domain_graph_read
from back.core.errors import AuthorizationError
from back.objects.registry import ROLE_ADMIN


def _req(**state) -> SimpleNamespace:
    return SimpleNamespace(state=SimpleNamespace(**state))


def test_failclosed_without_user(monkeypatch):
    monkeypatch.setattr(
        "api.routers.internal._graph_access.is_databricks_app", lambda: True
    )
    req = _req(user_role="app_user", user_domain_role="none")
    with pytest.raises(AuthorizationError):
        assert_domain_graph_read(req)


def test_admin_bypasses_gate(monkeypatch):
    monkeypatch.setattr(
        "api.routers.internal._graph_access.is_databricks_app", lambda: True
    )
    req = _req(user_role=ROLE_ADMIN, user_domain_role="none")
    assert_domain_graph_read(req)  # no raise


def test_local_mode_is_noop(monkeypatch):
    monkeypatch.setattr(
        "api.routers.internal._graph_access.is_databricks_app", lambda: False
    )
    req = _req(user_role="", user_domain_role="")
    assert_domain_graph_read(req)  # no raise
