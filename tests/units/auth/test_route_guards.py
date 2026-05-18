"""Tests for back.objects.registry.guards.require — the declarative
permission FastAPI dependency.

The middleware (:mod:`shared.fastapi.main`) is responsible for
populating ``request.state.user_role`` and
``request.state.user_domain_role``. These tests assume that already
happened and exercise the *guard* in two ways:

1. As a plain callable, against a hand-rolled request object — to
   pin down the comparison logic.
2. End-to-end through a FastAPI ``TestClient``, mounting a sample
   router that uses ``Depends(require(...))`` — to verify the
   FastAPI integration (status code, error shape).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import Depends, FastAPI, Request
from fastapi.testclient import TestClient
from starlette.datastructures import State

from back.core.errors import AuthorizationError, register_exception_handlers
from back.objects.registry import (
    ROLE_ADMIN,
    ROLE_APP_USER,
    ROLE_BUILDER,
    ROLE_EDITOR,
    ROLE_NONE,
    ROLE_VIEWER,
    require,
)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _make_request(
    user_role: str = "",
    user_domain_role: str = "",
) -> Request:
    """Hand-roll a Request-like object with a populated state."""
    req = MagicMock(spec=Request)
    req.state = State()
    req.state.user_role = user_role
    req.state.user_domain_role = user_domain_role
    return req


def _build_test_app(dep) -> TestClient:
    """Mount a tiny app exposing a single guarded endpoint."""
    app = FastAPI()
    register_exception_handlers(app)

    @app.get("/guarded", dependencies=[Depends(dep)])
    def _ok():
        return {"ok": True}

    return TestClient(app)


# ------------------------------------------------------------------
# Sanity checks on the factory
# ------------------------------------------------------------------


class TestRequireFactory:
    def test_invalid_scope_raises(self):
        with pytest.raises(ValueError):
            require(ROLE_ADMIN, scope="user")

    def test_returns_distinct_dep_per_call(self):
        a = require(ROLE_ADMIN)
        b = require(ROLE_ADMIN)
        assert a is not b

    def test_dep_has_descriptive_name(self):
        dep = require(ROLE_BUILDER, scope="domain")
        assert dep.__name__ == "require_domain_builder"


# ------------------------------------------------------------------
# Direct (non-FastAPI) invocation
# ------------------------------------------------------------------


class TestRequireAppScope:
    """``scope='app'`` reads ``request.state.user_role``."""

    @pytest.mark.parametrize(
        "actual",
        [ROLE_ADMIN, ROLE_BUILDER, ROLE_EDITOR, ROLE_VIEWER, ROLE_APP_USER],
    )
    def test_below_admin_rejected(self, actual):
        if actual == ROLE_ADMIN:
            return
        dep = require(ROLE_ADMIN)
        req = _make_request(user_role=actual)
        with pytest.raises(AuthorizationError):
            dep(req)

    def test_admin_passes_admin_gate(self):
        dep = require(ROLE_ADMIN)
        req = _make_request(user_role=ROLE_ADMIN)
        assert dep(req) == ROLE_ADMIN

    def test_role_none_rejected(self):
        """ROLE_NONE is below every gated tier (viewer/editor/builder/admin)."""
        dep = require(ROLE_VIEWER)
        req = _make_request(user_role=ROLE_NONE)
        with pytest.raises(AuthorizationError):
            dep(req)

    def test_missing_role_rejected(self):
        """An unset user_role still resolves to level 0 → rejected for any
        gate above ROLE_NONE."""
        dep = require(ROLE_VIEWER)
        req = _make_request()
        with pytest.raises(AuthorizationError):
            dep(req)

    def test_app_user_role_constant_has_no_gating_level(self):
        """``ROLE_APP_USER`` is intentionally outside the hierarchy
        (its level is 0 like ``ROLE_NONE``). Document that here so a
        future contributor doesn't try to gate on it."""
        from back.objects.registry import role_level

        assert role_level(ROLE_APP_USER) == 0
        assert role_level(ROLE_NONE) == 0

    def test_app_scope_ignores_domain_role(self):
        """A high domain role must NOT satisfy an app-scope check."""
        dep = require(ROLE_ADMIN, scope="app")
        req = _make_request(user_role=ROLE_APP_USER, user_domain_role=ROLE_ADMIN)
        with pytest.raises(AuthorizationError):
            dep(req)


class TestRequireDomainScope:
    """``scope='domain'`` reads ``user_domain_role`` with admin fallback."""

    @pytest.mark.parametrize(
        "actual,allowed",
        [
            (ROLE_ADMIN, True),
            (ROLE_BUILDER, True),
            (ROLE_EDITOR, False),
            (ROLE_VIEWER, False),
            (ROLE_NONE, False),
        ],
    )
    def test_builder_gate(self, actual, allowed):
        dep = require(ROLE_BUILDER, scope="domain")
        req = _make_request(user_domain_role=actual)
        if allowed:
            assert dep(req) == actual
        else:
            with pytest.raises(AuthorizationError):
                dep(req)

    def test_admin_app_role_satisfies_domain_gate(self):
        """Admins get a free pass on every domain gate via the
        ``user_role`` fallback (they bypass the domain ACL)."""
        dep = require(ROLE_BUILDER, scope="domain")
        req = _make_request(user_role=ROLE_ADMIN, user_domain_role="")
        assert dep(req) == ROLE_ADMIN

    def test_viewer_blocked_on_editor_gate(self):
        dep = require(ROLE_EDITOR, scope="domain")
        req = _make_request(user_domain_role=ROLE_VIEWER)
        with pytest.raises(AuthorizationError):
            dep(req)

    def test_editor_passes_editor_gate(self):
        dep = require(ROLE_EDITOR, scope="domain")
        req = _make_request(user_domain_role=ROLE_EDITOR)
        assert dep(req) == ROLE_EDITOR


# ------------------------------------------------------------------
# End-to-end through FastAPI's dependency stack
# ------------------------------------------------------------------


class TestRequireEndToEnd:
    """Mount a tiny FastAPI app and verify the dep returns 403 with a
    structured error body when the caller is below the required role.
    """

    @staticmethod
    def _set_role_middleware(role: str, domain_role: str = ""):
        """Build a Starlette middleware-like callable that sets the
        roles on request.state before the route runs."""

        async def _set_roles(request: Request, call_next):
            request.state.user_role = role
            request.state.user_domain_role = domain_role
            return await call_next(request)

        return _set_roles

    def _client_with_roles(
        self,
        guard_role: str,
        actual_role: str,
        *,
        scope: str = "app",
        domain_role: str = "",
    ) -> TestClient:
        from starlette.middleware.base import BaseHTTPMiddleware

        app = FastAPI()
        register_exception_handlers(app)
        app.add_middleware(
            BaseHTTPMiddleware,
            dispatch=self._set_role_middleware(actual_role, domain_role),
        )

        @app.get(
            "/guarded",
            dependencies=[Depends(require(guard_role, scope=scope))],
        )
        def _ok():
            return {"ok": True}

        return TestClient(app)

    def test_admin_passes(self):
        client = self._client_with_roles(ROLE_ADMIN, ROLE_ADMIN)
        resp = client.get("/guarded")
        assert resp.status_code == 200
        assert resp.json() == {"ok": True}

    def test_app_user_blocked_on_admin_route(self):
        client = self._client_with_roles(ROLE_ADMIN, ROLE_APP_USER)
        resp = client.get("/guarded")
        assert resp.status_code == 403
        body = resp.json()
        assert body.get("error") == "authorization"
        assert "admin" in body.get("message", "")

    def test_viewer_blocked_on_builder_domain_route(self):
        client = self._client_with_roles(
            ROLE_BUILDER,
            actual_role=ROLE_APP_USER,
            scope="domain",
            domain_role=ROLE_VIEWER,
        )
        resp = client.get("/guarded")
        assert resp.status_code == 403

    def test_builder_passes_builder_domain_route(self):
        client = self._client_with_roles(
            ROLE_BUILDER,
            actual_role=ROLE_APP_USER,
            scope="domain",
            domain_role=ROLE_BUILDER,
        )
        resp = client.get("/guarded")
        assert resp.status_code == 200

    def test_admin_passes_builder_domain_route_via_fallback(self):
        """The domain-scope dep falls back to ``user_role`` so admins
        (who never have a domain entry) still pass."""
        client = self._client_with_roles(
            ROLE_BUILDER,
            actual_role=ROLE_ADMIN,
            scope="domain",
            domain_role="",
        )
        resp = client.get("/guarded")
        assert resp.status_code == 200
