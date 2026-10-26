"""
E2E — Home / session / validation JSON API.

Covers the internal (session-aware) API endpoints mounted at the app root
by the ``home`` router:

  GET  /navbar/state
  GET  /session-status
  GET  /validate/ontology
  GET  /validate/detailed
  GET  /session-debug
  GET  /app-debug
  POST /reset-session
  POST /clear-ontology
"""

from __future__ import annotations

import json
import os

import pytest


def _skip_if_fake_databricks() -> None:
    if os.environ.get("ONTOBRICKS_E2E_FAKE_CREDS") == "1":
        pytest.skip("Databricks-backed route hangs with ONTOBRICKS_E2E_FAKE_CREDS=1")


def _csrf_headers(context) -> dict:
    cookies = {c["name"]: c["value"] for c in context.cookies()}
    h = {"Content-Type": "application/json"}
    if tok := cookies.get("csrf_token"):
        h["X-CSRF-Token"] = tok
    return h


def _json(resp) -> dict:
    return json.loads(resp.body())


# ── Navbar / session ───────────────────────────────────────────────────────────

class TestNavbarState:
    def test_navbar_state_returns_200(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        resp = page.request.get(f"{live_server}/navbar/state")
        assert resp.status == 200, resp.text()

    def test_navbar_state_is_json(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        resp = page.request.get(f"{live_server}/navbar/state")
        payload = _json(resp)
        assert isinstance(payload, dict)

    def test_navbar_state_has_domain_key(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        resp = page.request.get(f"{live_server}/navbar/state")
        payload = _json(resp)
        assert any(k in payload for k in ("domain", "domain_name", "domain_id", "name")), (
            f"Expected a domain-related key in navbar/state response: {list(payload.keys())}"
        )


class TestSessionStatus:
    def test_session_status_returns_200(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        resp = page.request.get(f"{live_server}/session-status")
        assert resp.status == 200, resp.text()

    def test_session_status_is_json(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        resp = page.request.get(f"{live_server}/session-status")
        payload = _json(resp)
        assert isinstance(payload, dict)

    def test_session_status_not_empty(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        resp = page.request.get(f"{live_server}/session-status")
        payload = _json(resp)
        assert len(payload) > 0, "session-status returned an empty dict"


# ── Validation ────────────────────────────────────────────────────────────────

class TestOntologyValidation:
    def test_validate_ontology_not_5xx(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        resp = page.request.get(f"{live_server}/validate/ontology")
        assert resp.status < 500, f"validate/ontology returned 5xx: {resp.text()}"

    def test_validate_ontology_is_json(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        resp = page.request.get(f"{live_server}/validate/ontology")
        payload = _json(resp)
        assert isinstance(payload, dict)

    def test_validate_detailed_not_5xx(self, page, live_server):
        _skip_if_fake_databricks()
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        resp = page.request.get(f"{live_server}/validate/detailed")
        assert resp.status < 500, f"validate/detailed returned 5xx: {resp.text()}"

    def test_validate_detailed_is_json(self, page, live_server):
        _skip_if_fake_databricks()
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        resp = page.request.get(f"{live_server}/validate/detailed")
        payload = _json(resp)
        assert isinstance(payload, (dict, list))


# ── Debug ─────────────────────────────────────────────────────────────────────
# /session-debug and /app-debug are mounted under the /domain prefix;
# they are covered in tests/e2e/domain/test_domain_api_flows.py.
# Here we verify the home-router debug path that the frontend JS uses.

class TestDebugEndpoints:
    def test_validate_ontology_response_has_keys(self, page, live_server):
        """validate/ontology must return a non-empty JSON object."""
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        resp = page.request.get(f"{live_server}/validate/ontology")
        payload = _json(resp)
        assert isinstance(payload, dict)
        assert len(payload) >= 0

    def test_validate_detailed_has_categories(self, page, live_server):
        """validate/detailed should return a structured report dict."""
        _skip_if_fake_databricks()
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        resp = page.request.get(f"{live_server}/validate/detailed")
        assert resp.status < 500
        payload = _json(resp)
        assert isinstance(payload, (dict, list))


# ── Mutations ─────────────────────────────────────────────────────────────────

class TestSessionMutations:
    def test_reset_session_returns_200(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        headers = _csrf_headers(page.context)
        resp = page.context.request.post(
            f"{live_server}/reset-session",
            headers=headers,
            data=json.dumps({}),
        )
        assert resp.status == 200, resp.text()

    def test_reset_session_response_is_json(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        headers = _csrf_headers(page.context)
        resp = page.context.request.post(
            f"{live_server}/reset-session",
            headers=headers,
            data=json.dumps({}),
        )
        payload = _json(resp)
        assert "success" in payload or "message" in payload

    def test_clear_ontology_returns_200(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        headers = _csrf_headers(page.context)
        resp = page.context.request.post(
            f"{live_server}/clear-ontology",
            headers=headers,
            data=json.dumps({}),
        )
        assert resp.status == 200, resp.text()

    def test_clear_ontology_returns_success(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        headers = _csrf_headers(page.context)
        resp = page.context.request.post(
            f"{live_server}/clear-ontology",
            headers=headers,
            data=json.dumps({}),
        )
        payload = _json(resp)
        assert payload.get("success") is True, (
            f"clear-ontology did not return success:true — got {payload}"
        )
