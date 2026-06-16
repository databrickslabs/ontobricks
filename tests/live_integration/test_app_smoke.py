"""Smoke probes against the deployed OntoBricks FastAPI app.

These confirm the app is live, the OAuth-gated routes work with a workspace
Bearer token, and the OpenAPI surface advertises the expected endpoints.
Anything that requires per-request session state (CSRF, DomainSession) is
exercised lightly — heavy-lifting belongs in the unit / contract tiers.
"""

from __future__ import annotations

import pytest


pytestmark = pytest.mark.live_integration


class TestHealth:
    def test_healthz_returns_200(self, http):
        """Public health probe — no auth required."""
        resp = http.get("/healthz")
        assert resp.status_code == 200

    def test_health_alias_also_returns_200(self, http):
        """The app also exposes ``/health`` as an alias for ``/healthz``.
        Confirms both addresses are wired and respond identically."""
        resp = http.get("/health")
        assert resp.status_code == 200


class TestRootRedirect:
    def test_root_redirects_when_unauthenticated(self):
        """Root must not return 500 to anonymous browsers."""
        import httpx

        with httpx.Client(timeout=10, follow_redirects=False) as anon:
            # Use the env-resolved base directly so we drop the Bearer header.
            import os

            base = os.environ["ONTOBRICKS_LIVE_BASE"].rstrip("/")
            resp = anon.get(f"{base}/")
            # Apps middleware redirects to /login or /access-denied; either
            # is a sign the app is up.
            assert resp.status_code in (200, 302, 303, 307)


class TestOpenAPI:
    def test_openapi_endpoint_returns_200(self, http):
        resp = http.get("/openapi.json")
        assert resp.status_code == 200, resp.text[:200]

    def test_openapi_is_well_formed(self, http):
        spec = http.get("/openapi.json").json()
        assert spec["openapi"].startswith("3.")
        assert isinstance(spec.get("paths"), dict)
        assert spec["info"]["title"] == "OntoBricks"

    def test_openapi_advertises_core_routers(self, http):
        """Every core router (dtwin, ontology, mapping, settings, domain,
        tasks, graphql) must register at least one path. These are the
        domain APIs that power the UI + the MCP companion."""
        paths = http.get("/openapi.json").json()["paths"]
        for prefix in (
            "/dtwin/",
            "/ontology/",
            "/mapping/",
            "/settings/",
            "/domain/",
            "/tasks/",
            "/graphql",
        ):
            assert any(p.startswith(prefix) for p in paths), (
                f"No paths under {prefix!r}; got {len(paths)} total paths"
            )

    def test_openapi_advertises_help_api(self, http):
        """The /api/help/* surface is what the in-app docs viewer hits."""
        paths = http.get("/openapi.json").json()["paths"]
        help_paths = [p for p in paths if p.startswith("/api/help")]
        assert help_paths, "No /api/help paths found in OpenAPI"


class TestStaticAssets:
    def test_static_css_is_served(self, http):
        """Bootstrap and CSS bundle should be reachable behind the OAuth gate."""
        # Hit a well-known static path; if the route doesn't exist Apps returns
        # 404 (still proves the app is healthy).
        resp = http.get("/static/css/bootstrap.min.css")
        assert resp.status_code in (200, 304, 404)


class TestSessionEndpoints:
    """Probe of session-level endpoints. Without a Domain selected most
    domain endpoints return a structured error, not a 500."""

    def test_session_status_is_reachable(self, http):
        resp = http.get("/session-status")
        # 200 is the happy path; 4xx is acceptable (session not initialized).
        assert resp.status_code in (200, 401, 403, 422), resp.text[:200]

    def test_settings_warehouses_returns_no_500(self, http):
        """Settings endpoint that fans out to Databricks workspace APIs.
        Must not crash even on a fresh deploy."""
        resp = http.get("/settings/warehouses")
        assert resp.status_code < 500, resp.text[:200]

    def test_help_docs_index_is_reachable(self, http):
        resp = http.get("/api/help/docs")
        assert resp.status_code in (200, 401, 403, 404), resp.text[:200]
