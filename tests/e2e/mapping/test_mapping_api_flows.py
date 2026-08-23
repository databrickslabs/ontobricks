"""
E2E — Mapping internal API (/mapping/…).

Verifies route contracts for the mapping layer.  Endpoints that require
a live SQL warehouse / UC Volume return infrastructure errors in the test
harness — we assert JSON shape and no framework 5xx.

Covered:
  GET  /mapping/load
  GET  /mapping/download
  GET  /mapping/diagnostics
  GET  /mapping/schema-drift
  GET  /mapping/wizard/llm-endpoints
  POST /mapping/generate
  POST /mapping/reset
  POST /mapping/clear
  POST /mapping/entity/add
  POST /mapping/entity/delete
  POST /mapping/save
"""

from __future__ import annotations

import json


_CONTRACT_STATUSES = (200, 400, 403, 404, 422, 502)


def _csrf_headers(context) -> dict:
    cookies = {c["name"]: c["value"] for c in context.cookies()}
    h = {"Content-Type": "application/json"}
    if tok := cookies.get("csrf_token"):
        h["X-CSRF-Token"] = tok
    return h


def _json(resp):
    return json.loads(resp.body())


def _assert_contract(resp):
    assert resp.status in _CONTRACT_STATUSES, (
        f"Unexpected {resp.status}: {resp.text()}"
    )
    payload = _json(resp)
    assert isinstance(payload, (dict, list))


# ── Read endpoints ─────────────────────────────────────────────────────────────

class TestMappingReadEndpoints:
    def _get(self, page, live_server, path):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        return page.request.get(f"{live_server}{path}")

    def test_load_returns_200(self, page, live_server):
        resp = self._get(page, live_server, "/mapping/load")
        assert resp.status == 200, resp.text()

    def test_load_is_json(self, page, live_server):
        resp = self._get(page, live_server, "/mapping/load")
        assert isinstance(_json(resp), dict)

    def test_load_has_config_key(self, page, live_server):
        resp = self._get(page, live_server, "/mapping/load")
        payload = _json(resp)
        assert "config" in payload or "mapping" in payload or "entities" in payload, (
            f"Expected a mapping config key in /mapping/load response: {list(payload.keys())}"
        )

    def test_download_not_5xx(self, page, live_server):
        resp = self._get(page, live_server, "/mapping/download")
        assert resp.status < 500, f"/mapping/download 5xx: {resp.text()}"

    def test_diagnostics_no_5xx(self, page, live_server):
        resp = self._get(page, live_server, "/mapping/diagnostics")
        assert resp.status < 500

    def test_schema_drift_no_5xx(self, page, live_server):
        resp = self._get(page, live_server, "/mapping/schema-drift")
        assert resp.status < 500

    def test_schema_drift_has_entity_and_relationship_keys(self, page, live_server):
        resp = self._get(page, live_server, "/mapping/schema-drift")
        if resp.status != 200:
            return
        payload = _json(resp)
        assert "entities" in payload and "relationships" in payload

    def test_wizard_llm_endpoints_contract(self, page, live_server):
        resp = self._get(page, live_server, "/mapping/wizard/llm-endpoints")
        assert resp.status < 500
        assert isinstance(_json(resp), (dict, list))


# ── Write endpoints ────────────────────────────────────────────────────────────

class TestMappingWriteEndpoints:
    def _post(self, page, live_server, path, body=None):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        return page.context.request.post(
            f"{live_server}{path}",
            headers=_csrf_headers(page.context),
            data=json.dumps(body or {}),
        )

    def test_generate_contract(self, page, live_server):
        _assert_contract(self._post(page, live_server, "/mapping/generate"))

    def test_reset_returns_200(self, page, live_server):
        resp = self._post(page, live_server, "/mapping/reset")
        assert resp.status == 200, resp.text()

    def test_clear_returns_200(self, page, live_server):
        resp = self._post(page, live_server, "/mapping/clear")
        assert resp.status == 200, resp.text()

    def test_entity_add_contract(self, page, live_server):
        _assert_contract(self._post(
            page, live_server, "/mapping/entity/add",
            {"class_name": "TestClass", "table_name": "test_table"},
        ))

    def test_entity_delete_contract(self, page, live_server):
        _assert_contract(self._post(
            page, live_server, "/mapping/entity/delete",
            {"class_name": "TestClass"},
        ))

    def test_save_contract(self, page, live_server):
        _assert_contract(self._post(page, live_server, "/mapping/save", {}))
