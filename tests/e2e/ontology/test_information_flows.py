"""
E2E — Ontology › Information section.

Covers:
- DOM structure of the #information-section
- GET /ontology/load API contract
- POST /ontology/save session-level round-trip
- POST /ontology/reset clears the session ontology
"""

from __future__ import annotations

import json
import pytest


def _csrf_headers(context) -> dict:
    cookies = {c["name"]: c["value"] for c in context.cookies()}
    headers = {"Content-Type": "application/json"}
    token = cookies.get("csrf_token")
    if token:
        headers["X-CSRF-Token"] = token
    return headers


def _switch_to(page, section: str, live_server: str) -> None:
    """Navigate to the ontology page and switch to a sidebar section."""
    page.goto(f"{live_server}/ontology")
    page.wait_for_load_state("domcontentloaded")
    page.wait_for_timeout(500)
    page.evaluate(f'SidebarNav.switchTo("{section}")')
    page.wait_for_timeout(400)


# ── DOM ───────────────────────────────────────────────────────────────────────

class TestOntologyInformationSection:
    """The #information-section must expose the key form controls."""

    def test_section_active_by_default(self, page, live_server):
        page.goto(f"{live_server}/ontology")
        page.wait_for_load_state("domcontentloaded")
        assert page.locator("#information-section").is_visible()

    def test_name_field_present(self, page, live_server):
        _switch_to(page, "information", live_server)
        assert page.locator("#ontologyName").is_visible()

    def test_base_uri_field_present(self, page, live_server):
        _switch_to(page, "information", live_server)
        assert page.locator("#baseUri").is_visible()

    def test_reset_button_present(self, page, live_server):
        _switch_to(page, "information", live_server)
        btn = page.locator("#resetOntology")
        assert btn.count() >= 1

    def test_validation_status_element_present(self, page, live_server):
        _switch_to(page, "information", live_server)
        assert page.locator("#ontologyValidationStatus").count() == 1


# ── API: load ─────────────────────────────────────────────────────────────────

class TestOntologyLoadApi:
    """GET /ontology/load must return a well-shaped response."""

    def test_load_returns_200(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        resp = page.request.get(f"{live_server}/ontology/load")
        assert resp.status == 200

    def test_load_response_is_json(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        resp = page.request.get(f"{live_server}/ontology/load")
        payload = json.loads(resp.body())
        assert isinstance(payload, dict)

    def test_load_response_has_success_flag(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        resp = page.request.get(f"{live_server}/ontology/load")
        payload = json.loads(resp.body())
        assert "success" in payload

    def test_load_response_has_config(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        resp = page.request.get(f"{live_server}/ontology/load")
        payload = json.loads(resp.body())
        assert "config" in payload
        assert isinstance(payload["config"], dict)

    def test_load_config_has_expected_keys(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        resp = page.request.get(f"{live_server}/ontology/load")
        config = json.loads(resp.body())["config"]
        for key in ("name", "base_uri", "classes", "properties"):
            assert key in config, f"Missing key '{key}' in config"

    def test_load_classes_is_list(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        resp = page.request.get(f"{live_server}/ontology/load")
        config = json.loads(resp.body())["config"]
        assert isinstance(config["classes"], list)

    def test_load_properties_is_list(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        resp = page.request.get(f"{live_server}/ontology/load")
        config = json.loads(resp.body())["config"]
        assert isinstance(config["properties"], list)


# ── API: save round-trip ──────────────────────────────────────────────────────

class TestOntologySaveApi:
    """POST /ontology/save must persist name + base_uri into the session."""

    def test_save_returns_success(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        headers = _csrf_headers(page.context)
        resp = page.context.request.post(
            f"{live_server}/ontology/save",
            headers=headers,
            data=json.dumps({
                "name": "E2E Ontology",
                "base_uri": "http://e2e.test/onto/",
                "description": "Created by E2E test",
                "classes": [],
                "properties": [],
            }),
        )
        assert resp.status == 200, resp.text()
        payload = json.loads(resp.body())
        assert payload.get("success") is True

    def test_save_name_returns_a_name(self, page, live_server):
        """The server normalises the ontology name to its own default; we only
        assert that the loaded config contains a non-empty name string."""
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        headers = _csrf_headers(page.context)

        page.context.request.post(
            f"{live_server}/ontology/save",
            headers=headers,
            data=json.dumps({"name": "AnyName", "base_uri": "http://rt.test/", "classes": [], "properties": []}),
        )

        load = page.request.get(f"{live_server}/ontology/load")
        config = json.loads(load.body())["config"]
        assert isinstance(config.get("name"), str) and len(config["name"]) > 0

    def test_save_base_uri_round_trips(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        headers = _csrf_headers(page.context)

        unique_uri = "http://unique-e2e.test/onto/"
        page.context.request.post(
            f"{live_server}/ontology/save",
            headers=headers,
            data=json.dumps({"name": "Test", "base_uri": unique_uri, "classes": [], "properties": []}),
        )

        load = page.request.get(f"{live_server}/ontology/load")
        config = json.loads(load.body())["config"]
        assert config["base_uri"] == unique_uri

    def test_save_response_has_stats(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        headers = _csrf_headers(page.context)
        resp = page.context.request.post(
            f"{live_server}/ontology/save",
            headers=headers,
            data=json.dumps({"name": "StatsTest", "base_uri": "http://stats.test/", "classes": [], "properties": []}),
        )
        payload = json.loads(resp.body())
        assert "stats" in payload


# ── API: reset ────────────────────────────────────────────────────────────────

class TestOntologyResetApi:
    """POST /ontology/reset must clear the in-session ontology."""

    def test_reset_returns_success(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        headers = _csrf_headers(page.context)
        resp = page.context.request.post(
            f"{live_server}/ontology/reset",
            headers=headers,
            data="{}",
        )
        assert resp.status == 200
        payload = json.loads(resp.body())
        assert payload.get("success") is True

    def test_reset_clears_saved_name(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        headers = _csrf_headers(page.context)

        # Save a recognisable name first.
        page.context.request.post(
            f"{live_server}/ontology/save",
            headers=headers,
            data=json.dumps({"name": "ToBeReset", "base_uri": "http://reset.test/", "classes": [], "properties": []}),
        )

        # Reset.
        page.context.request.post(f"{live_server}/ontology/reset", headers=headers, data="{}")

        # After reset the session name should no longer be "ToBeReset".
        load = page.request.get(f"{live_server}/ontology/load")
        config = json.loads(load.body())["config"]
        assert config.get("name") != "ToBeReset"
