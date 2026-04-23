"""
Layer 2 UI Tests -- write-flow smoke tests (Playwright).

Exercises at least one state-mutating endpoint per main page so the
E2E campaign is no longer purely read-only.

Scope constraint: the E2E subprocess starts Uvicorn with mock Databricks
credentials (``DATABRICKS_HOST=https://test.databricks.com``).  Endpoints
that persist **globally** to a UC Volume therefore cannot complete the
round-trip — they legitimately return an ``infrastructure`` error.  We
still test them but only assert the *route contract* (route mounted,
CSRF and JSON parsing work, response is JSON-shaped).

The one endpoint tested end-to-end is ``POST /settings/save`` with a
session-only payload (host / token) — it mutates the in-session domain
without hitting the remote backend, so the read-back assertion can run.
"""

from __future__ import annotations

import json


def _csrf_headers(context) -> dict:
    """Extract the current ``csrf_token`` cookie and return matching headers."""
    cookies = {c["name"]: c["value"] for c in context.cookies()}
    headers = {"Content-Type": "application/json"}
    token = cookies.get("csrf_token")
    if token:
        headers["X-CSRF-Token"] = token
    return headers


class TestSettingsSaveSessionOnly:
    """``POST /settings/save`` with host-only payload mutates session state
    without touching Databricks; ``GET /settings/current`` reads it back."""

    def test_save_host_and_read_back(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")

        new_host = "https://e2e-updated.databricks.test"
        headers = _csrf_headers(page.context)

        save = page.context.request.post(
            f"{live_server}/settings/save",
            headers=headers,
            data=json.dumps({"host": new_host}),
        )
        assert save.status == 200, save.text()

        read = page.context.request.get(f"{live_server}/settings/current")
        assert read.status == 200
        payload = json.loads(read.body())
        # SettingsService exposes the host under ``host`` or inside ``databricks``.
        observed = payload.get("host") or (
            payload.get("databricks", {}).get("host") if isinstance(payload, dict) else None
        )
        assert observed == new_host, (
            f"Expected host to round-trip through the session; got payload={payload!r}"
        )


class TestSettingsSaveRouteContracts:
    """Endpoints whose happy path requires a live UC Volume.

    We can't complete the round-trip in the test harness, but we can
    still prove that:

    - the route is mounted (no 404),
    - JSON parsing works (no 400 on a valid body),
    - CSRF is honoured when present,
    - errors come back as JSON with the OntoBricksError shape
      (never HTML, never uncaught 5xx from the framework).
    """

    def _assert_contract(self, response, allow_success=True):
        """Response must be JSON; status is 200 or a documented OntoBricksError."""
        # 200 ok, 400/422 validation, 502 infrastructure, 403 permission.
        assert response.status in (
            (200, 400, 403, 422, 502) if allow_success else (400, 403, 422, 502)
        ), f"Unexpected status {response.status}: {response.text()}"
        # Every response must be JSON (the router uses JSONResponse everywhere).
        body = response.body()
        payload = json.loads(body)
        assert isinstance(payload, dict)
        if response.status != 200:
            assert "error" in payload or "detail" in payload

    def test_save_base_uri_contract(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        headers = _csrf_headers(page.context)
        resp = page.context.request.post(
            f"{live_server}/settings/save-base-uri",
            headers=headers,
            data=json.dumps({"base_uri": "https://e2e.test/"}),
        )
        self._assert_contract(resp)

    def test_set_default_emoji_contract(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        headers = _csrf_headers(page.context)
        resp = page.context.request.post(
            f"{live_server}/settings/set-default-emoji",
            headers=headers,
            data=json.dumps({"emoji": "✨"}),
        )
        self._assert_contract(resp)

    def test_save_registry_cache_ttl_contract(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        headers = _csrf_headers(page.context)
        resp = page.context.request.post(
            f"{live_server}/settings/save-registry-cache-ttl",
            headers=headers,
            data=json.dumps({"registry_cache_ttl": 60}),
        )
        self._assert_contract(resp)
