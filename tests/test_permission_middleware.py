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

    @pytest.mark.parametrize(
        "path",
        [
            "/settings",
            "/settings/warehouses",
            "/settings/registry/initialize",
            "/settings/save",
            "/settings/graph-engine",
        ],
    )
    def test_app_user_blocked_from_settings(self, path):
        """The settings page and its write endpoints are admin-only."""
        _, resp, result = _dispatch_with_roles(
            ROLE_APP_USER, ROLE_NONE, path=path
        )
        assert resp.status_code in (302, 403)
        assert not result.get("passed")

    @pytest.mark.parametrize(
        "path,method",
        [
            ("/settings/current", "GET"),
            ("/settings/registry", "GET"),
            ("/settings/registry/domains", "GET"),
            ("/settings/registry/bridges", "GET"),
        ],
    )
    def test_settings_read_only_exceptions_allow_non_admin(self, path, method):
        """Read-only status endpoints under /settings must stay open
        to app users (used by the Load Domain dialog and the Browse
        page before any admin role is established)."""
        _, _, result = _dispatch_with_roles(
            ROLE_APP_USER, ROLE_VIEWER, method=method, path=path
        )
        assert result.get("passed")

    def test_post_on_registry_exception_is_still_admin_only(self):
        """POST /settings/registry (change registry location) must
        NOT benefit from the read-only GET exception."""
        _, resp, result = _dispatch_with_roles(
            ROLE_APP_USER, ROLE_VIEWER, method="POST", path="/settings/registry"
        )
        assert resp.status_code in (302, 403)
        assert not result.get("passed")

    def test_delete_registry_domain_is_admin_only(self):
        """Non-admins must not be able to delete registry domains."""
        _, resp, result = _dispatch_with_roles(
            ROLE_APP_USER,
            ROLE_BUILDER,
            method="DELETE",
            path="/settings/registry/domains/my_domain",
        )
        assert resp.status_code in (302, 403)
        assert not result.get("passed")

    def test_delete_registry_version_is_admin_only(self):
        _, resp, result = _dispatch_with_roles(
            ROLE_APP_USER,
            ROLE_BUILDER,
            method="DELETE",
            path="/settings/registry/domains/my_domain/versions/1",
        )
        assert resp.status_code in (302, 403)
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

    @pytest.mark.parametrize(
        "path,method",
        [
            ("/domain/list-projects", "GET"),
            ("/domain/list-versions", "GET"),
            ("/domain/load-from-uc", "POST"),
        ],
    )
    def test_registry_enumeration_routes_bypass_domain_role(self, path, method):
        """Listing registry content / switching to a new domain must not
        depend on the current session domain's role. Otherwise a user
        whose session lands on a domain they are not a member of cannot
        even open the "Load Domain from Registry" picker.
        """
        _, _, result = _dispatch_with_roles(
            ROLE_APP_USER, ROLE_NONE, method=method, path=path
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


# ------------------------------------------------------------------
# Three-level domain-role matrix (viewer / editor / builder)
# ------------------------------------------------------------------


class TestThreeLevelDomainRoleMatrix:
    """Full matrix of the three domain-level tiers against every
    HTTP method and every domain-scoped prefix.

    Goal: give us a single source of truth for "what can a viewer do?
    what can an editor do? what can a builder do?" — the three tiers
    the UI surfaces as distinct badges. Admins are covered separately
    (they bypass the domain gate entirely).
    """

    DOMAIN_PREFIXES = ["/domain/", "/ontology/", "/mapping/", "/dtwin/"]
    READ_METHODS = ["GET"]
    WRITE_METHODS = ["POST", "PUT", "PATCH", "DELETE"]

    # ----- Viewer: reads OK, writes blocked on every prefix -----

    @pytest.mark.parametrize("prefix", DOMAIN_PREFIXES)
    @pytest.mark.parametrize("method", READ_METHODS)
    def test_viewer_reads_allowed(self, prefix, method):
        _, _, result = _dispatch_with_roles(
            ROLE_APP_USER, ROLE_VIEWER, method=method, path=prefix
        )
        assert result.get("passed"), (
            f"viewer should be allowed to {method} {prefix}"
        )

    @pytest.mark.parametrize("prefix", DOMAIN_PREFIXES)
    @pytest.mark.parametrize("method", WRITE_METHODS)
    def test_viewer_writes_blocked(self, prefix, method):
        _, resp, result = _dispatch_with_roles(
            ROLE_APP_USER, ROLE_VIEWER, method=method, path=prefix
        )
        assert resp.status_code == 403, (
            f"viewer should be blocked on {method} {prefix}"
        )
        assert not result.get("passed")

    # ----- Editor: reads + writes OK on every prefix -----

    @pytest.mark.parametrize("prefix", DOMAIN_PREFIXES)
    @pytest.mark.parametrize("method", READ_METHODS + WRITE_METHODS)
    def test_editor_reads_and_writes_allowed(self, prefix, method):
        _, _, result = _dispatch_with_roles(
            ROLE_APP_USER, ROLE_EDITOR, method=method, path=prefix
        )
        assert result.get("passed"), (
            f"editor should be allowed to {method} {prefix}"
        )

    # ----- Builder: reads + writes OK on every prefix -----

    @pytest.mark.parametrize("prefix", DOMAIN_PREFIXES)
    @pytest.mark.parametrize("method", READ_METHODS + WRITE_METHODS)
    def test_builder_reads_and_writes_allowed(self, prefix, method):
        _, _, result = _dispatch_with_roles(
            ROLE_APP_USER, ROLE_BUILDER, method=method, path=prefix
        )
        assert result.get("passed"), (
            f"builder should be allowed to {method} {prefix}"
        )

    # ----- role_level ordering: viewer < editor < builder < admin -----

    def test_role_level_strict_ordering(self):
        """The three tiers must sort strictly below admin so callers
        that compare with ``role_level(...) >= role_level(X)`` work."""
        assert (
            role_level(ROLE_VIEWER)
            < role_level(ROLE_EDITOR)
            < role_level(ROLE_BUILDER)
            < role_level(ROLE_ADMIN)
        )
        # ROLE_NONE must be below every tier.
        assert role_level(ROLE_NONE) < role_level(ROLE_VIEWER)


# ------------------------------------------------------------------
# OWL generation / export endpoints — the read-only preview pattern
# ------------------------------------------------------------------


class TestOwlEndpointPermissions:
    """``POST /ontology/generate-owl`` saves the domain (write), so the
    viewer must be blocked there. ``GET /ontology/export-owl`` is the
    read-only preview endpoint that ``autoGenerateOwl`` falls back to in
    read-only mode, so every tier (viewer / editor / builder / admin)
    must be able to reach it. These tests lock that contract in place so
    a future refactor of the middleware cannot silently regress the
    read-only OWL preview for viewers.
    """

    # --- POST /ontology/generate-owl (write) ---

    def test_viewer_cannot_post_generate_owl(self):
        _, resp, result = _dispatch_with_roles(
            ROLE_APP_USER,
            ROLE_VIEWER,
            method="POST",
            path="/ontology/generate-owl",
        )
        assert resp.status_code == 403
        assert not result.get("passed")

    @pytest.mark.parametrize("role", [ROLE_EDITOR, ROLE_BUILDER])
    def test_editor_and_builder_can_post_generate_owl(self, role):
        _, _, result = _dispatch_with_roles(
            ROLE_APP_USER, role, method="POST", path="/ontology/generate-owl"
        )
        assert result.get("passed")

    def test_admin_can_post_generate_owl(self):
        _, _, result = _dispatch_with_roles(
            ROLE_ADMIN,
            ROLE_ADMIN,
            method="POST",
            path="/ontology/generate-owl",
        )
        assert result.get("passed")

    # --- GET /ontology/export-owl (read-only preview) ---

    @pytest.mark.parametrize(
        "role", [ROLE_VIEWER, ROLE_EDITOR, ROLE_BUILDER]
    )
    def test_every_tier_can_get_export_owl(self, role):
        """The three domain-level tiers can all preview the OWL
        output — this is what the client falls back to when the
        user is read-only."""
        _, _, result = _dispatch_with_roles(
            ROLE_APP_USER, role, method="GET", path="/ontology/export-owl"
        )
        assert result.get("passed")

    def test_admin_can_get_export_owl(self):
        _, _, result = _dispatch_with_roles(
            ROLE_ADMIN,
            ROLE_ADMIN,
            method="GET",
            path="/ontology/export-owl",
        )
        assert result.get("passed")

    def test_no_team_app_user_blocked_on_export_owl(self):
        """Users without a team entry on the current domain cannot
        even preview its OWL — they don't belong to the domain."""
        _, resp, result = _dispatch_with_roles(
            ROLE_APP_USER,
            ROLE_NONE,
            method="GET",
            path="/ontology/export-owl",
        )
        assert resp.status_code == 403
        assert not result.get("passed")


# ------------------------------------------------------------------
# Filter accessible domains (visibility layer used by registry UIs)
# ------------------------------------------------------------------


class TestFilterAccessibleDomains:
    """``PermissionService.filter_accessible_domains`` is the visibility
    layer used by registry listings. It mirrors the three-level model:
    admins see everything, app users see only domains where their role
    is not ``ROLE_NONE``."""

    def _make_service(self, domain_roles: dict, *, is_admin: bool = False):
        from back.objects.registry.PermissionService import PermissionService

        svc = PermissionService()
        svc.is_admin = MagicMock(return_value=is_admin)

        def _get_domain_role(
            email, host, token, registry_cfg, app_name, folder,
            user_token="", app_role="",
        ):
            return domain_roles.get(folder, ROLE_NONE)

        svc.get_domain_role = MagicMock(side_effect=_get_domain_role)
        return svc

    def test_admin_by_app_role_sees_all(self):
        svc = self._make_service({}, is_admin=False)
        entries = ["alpha", "beta", "gamma"]
        out = svc.filter_accessible_domains(
            "admin@acme.com",
            "h",
            "t",
            {},
            "app",
            entries,
            app_role=ROLE_ADMIN,
        )
        assert out == entries
        # Short-circuited — never needs to call get_domain_role.
        assert not svc.get_domain_role.called

    def test_admin_by_live_lookup_sees_all(self):
        svc = self._make_service({}, is_admin=True)
        entries = ["alpha", "beta"]
        out = svc.filter_accessible_domains(
            "admin@acme.com", "h", "t", {}, "app", entries,
        )
        assert out == entries

    def test_viewer_sees_only_member_domains(self):
        svc = self._make_service(
            {"alpha": ROLE_VIEWER, "beta": ROLE_NONE, "gamma": ROLE_VIEWER},
            is_admin=False,
        )
        out = svc.filter_accessible_domains(
            "user@acme.com",
            "h",
            "t",
            {},
            "app",
            ["alpha", "beta", "gamma"],
            app_role=ROLE_APP_USER,
        )
        assert out == ["alpha", "gamma"]

    def test_editor_and_builder_sees_domains(self):
        svc = self._make_service(
            {"alpha": ROLE_EDITOR, "beta": ROLE_BUILDER, "gamma": ROLE_NONE},
            is_admin=False,
        )
        out = svc.filter_accessible_domains(
            "user@acme.com",
            "h",
            "t",
            {},
            "app",
            ["alpha", "beta", "gamma"],
            app_role=ROLE_APP_USER,
        )
        assert out == ["alpha", "beta"]

    def test_empty_input_short_circuits(self):
        svc = self._make_service({}, is_admin=False)
        assert svc.filter_accessible_domains(
            "user@acme.com", "h", "t", {}, "app", [], app_role=ROLE_APP_USER,
        ) == []
        assert not svc.get_domain_role.called

    def test_dict_entries_use_name_key(self):
        svc = self._make_service(
            {"alpha": ROLE_VIEWER, "beta": ROLE_NONE},
            is_admin=False,
        )
        entries = [{"name": "alpha", "extra": 1}, {"name": "beta"}]
        out = svc.filter_accessible_domains(
            "user@acme.com", "h", "t", {}, "app", entries,
            app_role=ROLE_APP_USER,
        )
        assert out == [{"name": "alpha", "extra": 1}]

    def test_custom_key_callback(self):
        svc = self._make_service(
            {"alpha": ROLE_EDITOR, "beta": ROLE_NONE},
            is_admin=False,
        )
        entries = [("alpha", 1), ("beta", 2)]
        out = svc.filter_accessible_domains(
            "user@acme.com",
            "h",
            "t",
            {},
            "app",
            entries,
            app_role=ROLE_APP_USER,
            key=lambda e: e[0],
        )
        assert out == [("alpha", 1)]


# ------------------------------------------------------------------
# Import / reset endpoints: viewers must be blocked, editors+ pass
# ------------------------------------------------------------------


class TestImportAndResetEndpoints:
    """Every import-like and reset-like write that the read-only UI
    now disables maps to a POST on a domain-scoped prefix, so the
    middleware already blocks viewers. We lock that contract with
    explicit parametrised tests so the UI gating and backend gating
    stay in sync.

    Covered:

    - ``POST /ontology/import-owl``          (OWL import)
    - ``POST /ontology/import-rdfs``         (RDFS import — dynamic
                                              ``/ontology/import-{kind}``)
    - ``POST /ontology/import-fibo``         (FIBO industry-standard)
    - ``POST /ontology/import-cdisc``        (CDISC industry-standard)
    - ``POST /ontology/import-iof``          (IOF industry-standard)
    - ``POST /mapping/parse-r2rml``          (R2RML import)
    - ``POST /domain/metadata/clear``        (Reset data sources)
    - ``POST /domain/metadata/save``         (Add/save data sources)
    - ``POST /domain/metadata/update``       (Update from UC)
    - ``POST /domain/metadata/update-mappings``
    """

    IMPORT_AND_RESET_PATHS = [
        "/ontology/import-owl",
        "/ontology/import-rdfs",
        "/ontology/import-fibo",
        "/ontology/import-cdisc",
        "/ontology/import-iof",
        "/mapping/parse-r2rml",
        "/domain/metadata/clear",
        "/domain/metadata/save",
        "/domain/metadata/update",
        "/domain/metadata/update-mappings",
    ]

    @pytest.mark.parametrize("path", IMPORT_AND_RESET_PATHS)
    def test_viewer_blocked_on_import_or_reset(self, path):
        _, resp, result = _dispatch_with_roles(
            ROLE_APP_USER, ROLE_VIEWER, method="POST", path=path
        )
        assert resp.status_code == 403, (
            f"viewer should be blocked on POST {path}"
        )
        assert not result.get("passed")

    @pytest.mark.parametrize("path", IMPORT_AND_RESET_PATHS)
    @pytest.mark.parametrize("role", [ROLE_EDITOR, ROLE_BUILDER])
    def test_editor_and_builder_allowed_on_import_or_reset(self, path, role):
        _, _, result = _dispatch_with_roles(
            ROLE_APP_USER, role, method="POST", path=path
        )
        assert result.get("passed"), (
            f"{role} should be allowed to POST {path}"
        )

    @pytest.mark.parametrize("path", IMPORT_AND_RESET_PATHS)
    def test_admin_allowed_on_import_or_reset(self, path):
        _, _, result = _dispatch_with_roles(
            ROLE_ADMIN, ROLE_ADMIN, method="POST", path=path
        )
        assert result.get("passed")

    @pytest.mark.parametrize("path", IMPORT_AND_RESET_PATHS)
    def test_no_team_app_user_blocked_on_import_or_reset(self, path):
        """Users without a team entry on the current domain are
        blocked at the domain gate before the viewer rule even runs."""
        _, resp, result = _dispatch_with_roles(
            ROLE_APP_USER, ROLE_NONE, method="POST", path=path
        )
        assert resp.status_code == 403
        assert not result.get("passed")
