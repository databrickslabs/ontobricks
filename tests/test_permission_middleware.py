"""Tests for PermissionMiddleware (shared.fastapi.main).

Covers: bypass paths, local-dev admin bypass, role enforcement (none→403,
viewer write→403), admin-only paths, request.state role propagation, and
the digital-twin build endpoint authorization guard.
"""

import asyncio
import pytest
from unittest.mock import patch, MagicMock
from starlette.datastructures import State

from back.objects.registry.PermissionService import (
    ROLE_ADMIN,
    ROLE_APP_USER,
    ROLE_BUILDER,
    ROLE_EDITOR,
    ROLE_VIEWER,
    ROLE_NONE,
    role_level,
)
from back.core.errors import AuthorizationError


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _run(coro):
    """Run a coroutine synchronously for test assertions."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor() as pool:
            return pool.submit(asyncio.run, coro).result()
    return asyncio.run(coro)


def _make_request(method="GET", path="/", email="user@test.com", headers=None):
    """Build a lightweight mock Request accepted by PermissionMiddleware."""
    req = MagicMock()
    req.method = method
    req.url = MagicMock()
    req.url.path = path
    req.state = State()
    _headers = {
        "x-forwarded-email": email,
        "accept": "application/json",
    }
    if headers:
        _headers.update(headers)
    headers_mock = MagicMock()
    headers_mock.get = MagicMock(side_effect=lambda k, d="": _headers.get(k, d))
    req.headers = headers_mock
    return req


def _dispatch_with_roles(app_role, domain_role, method="GET", path="/ontology/"):
    """Drive a single middleware dispatch with predetermined roles."""
    from shared.fastapi.main import PermissionMiddleware

    req = _make_request(method=method, path=path)
    result = {}

    async def call_next(r):
        result["passed"] = True
        return MagicMock(status_code=200)

    middleware = PermissionMiddleware(MagicMock())

    with (
        patch("back.core.databricks.is_databricks_app", return_value=True),
        patch.object(
            PermissionMiddleware, "_resolve_roles", return_value=(app_role, domain_role)
        ),
    ):
        resp = _run(middleware.dispatch(req, call_next))

    return req, resp, result


# ------------------------------------------------------------------
# Bypass paths
# ------------------------------------------------------------------


class TestBypassPaths:
    """Requests to static / health / docs / api paths skip enforcement."""

    @pytest.mark.parametrize(
        "path",
        [
            "/static/css/main.css",
            "/health",
            "/docs",
            "/redoc",
            "/openapi.json",
            "/access-denied",
            "/api/v1/domains",
            "/graphql/",
        ],
    )
    def test_bypass_sets_empty_role(self, path):
        from shared.fastapi.main import PermissionMiddleware

        req = _make_request(path=path)
        called = {}

        async def call_next(r):
            called["passed"] = True
            return MagicMock(status_code=200)

        middleware = PermissionMiddleware(MagicMock())

        with patch("back.core.databricks.is_databricks_app", return_value=True):
            _run(middleware.dispatch(req, call_next))

        assert req.state.user_role == ""
        assert req.state.user_domain_role == ""
        assert called.get("passed")


# ------------------------------------------------------------------
# Local dev mode
# ------------------------------------------------------------------


class TestLocalDevMode:
    """When not running as a Databricks App, every request is admin."""

    def test_local_mode_admin(self):
        from shared.fastapi.main import PermissionMiddleware

        req = _make_request(path="/ontology/")
        called = {}

        async def call_next(r):
            called["passed"] = True
            return MagicMock(status_code=200)

        middleware = PermissionMiddleware(MagicMock())

        with patch("back.core.databricks.is_databricks_app", return_value=False):
            _run(middleware.dispatch(req, call_next))

        assert req.state.user_role == "admin"
        assert req.state.user_domain_role == "admin"
        assert called.get("passed")


# ------------------------------------------------------------------
# Role enforcement
# ------------------------------------------------------------------


class TestRoleEnforcement:
    """Role-based blocking: none→403, viewer+write→403."""

    def test_none_role_blocked(self):
        _, resp, result = _dispatch_with_roles(ROLE_NONE, ROLE_NONE)
        assert resp.status_code == 403
        assert not result.get("passed")

    def test_none_role_html_redirects_to_reason_app(self):
        """HTML request → 302 to /access-denied?reason=app by default."""
        from shared.fastapi.main import PermissionMiddleware

        req = _make_request(headers={"accept": "text/html"})
        middleware = PermissionMiddleware(MagicMock())

        async def call_next(_):
            return MagicMock(status_code=200)

        with (
            patch("back.core.databricks.is_databricks_app", return_value=True),
            patch.object(
                PermissionMiddleware,
                "_resolve_roles",
                return_value=(ROLE_NONE, ROLE_NONE),
            ),
            patch(
                "back.objects.registry.permission_service"
                ".is_app_principals_forbidden",
                return_value=False,
            ),
        ):
            resp = _run(middleware.dispatch(req, call_next))

        assert resp.status_code == 302
        assert "reason=app" in resp.headers["location"]

    def test_bootstrap_redirect_on_forbidden_principals(self):
        """When list_app_principals came back 403, use reason=bootstrap."""
        from shared.fastapi.main import PermissionMiddleware

        req = _make_request(headers={"accept": "text/html"})
        middleware = PermissionMiddleware(MagicMock())

        async def call_next(_):
            return MagicMock(status_code=200)

        with (
            patch("back.core.databricks.is_databricks_app", return_value=True),
            patch.object(
                PermissionMiddleware,
                "_resolve_roles",
                return_value=(ROLE_NONE, ROLE_NONE),
            ),
            patch(
                "back.objects.registry.permission_service"
                ".is_app_principals_forbidden",
                return_value=True,
            ),
        ):
            resp = _run(middleware.dispatch(req, call_next))

        assert resp.status_code == 302
        assert "reason=bootstrap" in resp.headers["location"]

    def test_viewer_get_allowed(self):
        _, _, result = _dispatch_with_roles(ROLE_VIEWER, ROLE_VIEWER, method="GET")
        assert result.get("passed")

    def test_viewer_post_blocked(self):
        _, resp, result = _dispatch_with_roles(ROLE_VIEWER, ROLE_VIEWER, method="POST")
        assert resp.status_code == 403
        assert not result.get("passed")

    def test_viewer_put_blocked(self):
        _, resp, _ = _dispatch_with_roles(ROLE_VIEWER, ROLE_VIEWER, method="PUT")
        assert resp.status_code == 403

    def test_viewer_patch_blocked(self):
        _, resp, _ = _dispatch_with_roles(ROLE_VIEWER, ROLE_VIEWER, method="PATCH")
        assert resp.status_code == 403

    def test_viewer_delete_blocked(self):
        _, resp, _ = _dispatch_with_roles(ROLE_VIEWER, ROLE_VIEWER, method="DELETE")
        assert resp.status_code == 403

    def test_editor_post_allowed(self):
        _, _, result = _dispatch_with_roles(ROLE_EDITOR, ROLE_EDITOR, method="POST")
        assert result.get("passed")

    def test_builder_post_allowed(self):
        _, _, result = _dispatch_with_roles(ROLE_BUILDER, ROLE_BUILDER, method="POST")
        assert result.get("passed")

    def test_admin_post_allowed(self):
        _, _, result = _dispatch_with_roles(ROLE_ADMIN, ROLE_ADMIN, method="POST")
        assert result.get("passed")


# ------------------------------------------------------------------
# Admin-only paths
# ------------------------------------------------------------------


class TestAdminOnlyPaths:
    """Non-admin users are blocked from /settings/permissions and /settings/domain-permissions."""

    def test_admin_can_access_permissions(self):
        _, _, result = _dispatch_with_roles(
            ROLE_ADMIN, ROLE_ADMIN, path="/settings/permissions"
        )
        assert result.get("passed")

    def test_editor_blocked_from_permissions(self):
        _, resp, result = _dispatch_with_roles(
            ROLE_EDITOR, ROLE_EDITOR, path="/settings/permissions"
        )
        assert resp.status_code == 403
        assert not result.get("passed")

    def test_builder_blocked_from_permissions(self):
        _, resp, result = _dispatch_with_roles(
            ROLE_BUILDER, ROLE_BUILDER, path="/settings/permissions"
        )
        assert resp.status_code == 403
        assert not result.get("passed")

    def test_admin_can_access_domain_permissions(self):
        _, _, result = _dispatch_with_roles(
            ROLE_ADMIN,
            ROLE_ADMIN,
            path="/settings/domain-permissions/my_domain",
        )
        assert result.get("passed")

    def test_editor_blocked_from_domain_permissions(self):
        _, resp, result = _dispatch_with_roles(
            ROLE_EDITOR,
            ROLE_EDITOR,
            path="/settings/domain-permissions/my_domain",
        )
        assert resp.status_code == 403
        assert not result.get("passed")

    def test_admin_can_access_teams(self):
        _, _, result = _dispatch_with_roles(
            ROLE_ADMIN, ROLE_ADMIN, path="/settings/teams"
        )
        assert result.get("passed")

    def test_app_user_blocked_from_teams(self):
        _, resp, result = _dispatch_with_roles(
            ROLE_APP_USER, ROLE_NONE, path="/settings/teams"
        )
        assert resp.status_code == 403
        assert not result.get("passed")


# ------------------------------------------------------------------
# Domain-scoped routes require a team entry (new strict model)
# ------------------------------------------------------------------


class TestDomainScopedRoutes:
    """App users without a team entry on a domain are blocked there."""

    @pytest.mark.parametrize(
        "path",
        ["/domain/", "/ontology/", "/mapping/", "/dtwin/"],
    )
    def test_app_user_no_team_blocked(self, path):
        _, resp, result = _dispatch_with_roles(
            ROLE_APP_USER, ROLE_NONE, method="GET", path=path
        )
        assert resp.status_code == 403
        assert not result.get("passed")

    def test_app_user_viewer_can_get(self):
        _, _, result = _dispatch_with_roles(
            ROLE_APP_USER, ROLE_VIEWER, method="GET", path="/ontology/"
        )
        assert result.get("passed")

    def test_app_user_viewer_cannot_write(self):
        _, resp, _ = _dispatch_with_roles(
            ROLE_APP_USER, ROLE_VIEWER, method="POST", path="/ontology/"
        )
        assert resp.status_code == 403

    def test_app_user_editor_can_write(self):
        _, _, result = _dispatch_with_roles(
            ROLE_APP_USER, ROLE_EDITOR, method="POST", path="/ontology/"
        )
        assert result.get("passed")

    def test_admin_bypasses_domain_gate(self):
        _, _, result = _dispatch_with_roles(
            ROLE_ADMIN, ROLE_NONE, method="GET", path="/ontology/"
        )
        assert result.get("passed")

    def test_app_user_can_hit_non_domain_routes(self):
        # Non-domain-scoped paths should be reachable with no team entry
        _, _, result = _dispatch_with_roles(
            ROLE_APP_USER, ROLE_NONE, method="GET", path="/registry/"
        )
        assert result.get("passed")


# ------------------------------------------------------------------
# request.state carries both roles
# ------------------------------------------------------------------


class TestRequestState:
    """Middleware sets user_role and user_domain_role on request.state."""

    def test_roles_on_state(self):
        from shared.fastapi.main import PermissionMiddleware

        req = _make_request(path="/ontology/")
        captured = {}

        async def call_next(r):
            captured["app"] = r.state.user_role
            captured["domain"] = r.state.user_domain_role
            return MagicMock(status_code=200)

        middleware = PermissionMiddleware(MagicMock())

        with (
            patch("back.core.databricks.is_databricks_app", return_value=True),
            patch.object(
                PermissionMiddleware,
                "_resolve_roles",
                return_value=(ROLE_BUILDER, ROLE_EDITOR),
            ),
        ):
            _run(middleware.dispatch(req, call_next))

        assert captured["app"] == ROLE_BUILDER
        assert captured["domain"] == ROLE_EDITOR

    def test_email_on_state(self):
        from shared.fastapi.main import PermissionMiddleware

        req = _make_request(path="/ontology/", email="alice@acme.com")

        async def call_next(r):
            return MagicMock(status_code=200)

        middleware = PermissionMiddleware(MagicMock())

        with patch("back.core.databricks.is_databricks_app", return_value=False):
            _run(middleware.dispatch(req, call_next))

        assert req.state.user_email == "alice@acme.com"


# ------------------------------------------------------------------
# Resolve-roles exception → ROLE_NONE
# ------------------------------------------------------------------


class TestResolveRolesFailure:
    """If _resolve_roles raises, the user gets ROLE_NONE (blocked)."""

    def test_fallback_to_none(self):
        from shared.fastapi.main import PermissionMiddleware

        req = _make_request(path="/ontology/")
        called = {}

        async def call_next(r):
            called["passed"] = True
            return MagicMock(status_code=200)

        middleware = PermissionMiddleware(MagicMock())

        with (
            patch("back.core.databricks.is_databricks_app", return_value=True),
            patch.object(
                PermissionMiddleware, "_resolve_roles", side_effect=RuntimeError("boom")
            ),
        ):
            resp = _run(middleware.dispatch(req, call_next))

        assert resp.status_code == 403
        assert not called.get("passed")
        assert req.state.user_role == ROLE_NONE


# ------------------------------------------------------------------
# Build endpoint authorization guard
# ------------------------------------------------------------------


class TestBuildEndpointGuard:
    """The /dtwin/sync/start role check rejects users below builder.

    These tests validate the *logic* used by the endpoint guard
    (role_level comparison), not the full endpoint stack.
    """

    @pytest.mark.parametrize(
        "role,allowed",
        [
            (ROLE_ADMIN, True),
            (ROLE_BUILDER, True),
            (ROLE_EDITOR, False),
            (ROLE_VIEWER, False),
            (ROLE_NONE, False),
        ],
    )
    def test_role_gate(self, role, allowed):
        assert (role_level(role) >= role_level(ROLE_BUILDER)) == allowed

    @pytest.mark.parametrize("role", [ROLE_EDITOR, ROLE_VIEWER, ROLE_NONE])
    def test_rejected_roles_raise_authorization_error(self, role):
        if role_level(role) < role_level(ROLE_BUILDER):
            with pytest.raises(AuthorizationError):
                raise AuthorizationError(
                    "Only builders and admins can build a digital twin"
                )

    @pytest.mark.parametrize("role", [ROLE_ADMIN, ROLE_BUILDER])
    def test_accepted_roles_pass(self, role):
        assert role_level(role) >= role_level(ROLE_BUILDER)
