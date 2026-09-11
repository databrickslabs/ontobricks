"""
E2E — Domain internal API (/domain/…).

Covers read + write API endpoints that the Domain page and the registry
use for persisting domain state in the session and (optionally) UC.

Endpoints that interact with UC Volumes return an infrastructure error
in the test harness (mock credentials) — we assert route contracts, not
round-trips.

Covered:
  GET  /domain/info
  GET  /domain/config
  GET  /domain/check-name
  GET  /domain/design-views
  GET  /domain/design-views/current
  GET  /domain/map-layout
  GET  /domain/list-versions
  GET  /domain/version-status
  GET  /domain/session-debug
  GET  /domain/app-debug
  GET  /domain/current-user
  POST /domain/info
  POST /domain/reset
  POST /domain/clear
  POST /domain/design-views/create
  POST /domain/design-views/save-current
  POST /domain/metadata/removal-impact
"""

from __future__ import annotations

import json


_CONTRACT_STATUSES = (200, 400, 403, 422, 502)


def _csrf_headers(context) -> dict:
    cookies = {c["name"]: c["value"] for c in context.cookies()}
    h = {"Content-Type": "application/json"}
    if tok := cookies.get("csrf_token"):
        h["X-CSRF-Token"] = tok
    return h


def _json(resp) -> dict:
    return json.loads(resp.body())


def _assert_contract(resp):
    assert resp.status in _CONTRACT_STATUSES, (
        f"Unexpected status {resp.status}: {resp.text()}"
    )
    payload = _json(resp)
    assert isinstance(payload, (dict, list, str))
    if resp.status != 200 and isinstance(payload, dict):
        assert "error" in payload or "detail" in payload


# ── Read endpoints ─────────────────────────────────────────────────────────────

class TestDomainReadEndpoints:
    def _get(self, page, live_server, path, params=""):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        return page.request.get(f"{live_server}{path}{params}")

    def test_domain_info_returns_200(self, page, live_server):
        resp = self._get(page, live_server, "/domain/info")
        assert resp.status == 200, resp.text()

    def test_domain_info_is_json(self, page, live_server):
        resp = self._get(page, live_server, "/domain/info")
        assert isinstance(_json(resp), dict)

    def test_domain_config_returns_200(self, page, live_server):
        resp = self._get(page, live_server, "/domain/config")
        assert resp.status == 200, resp.text()

    def test_domain_config_is_json(self, page, live_server):
        resp = self._get(page, live_server, "/domain/config")
        assert isinstance(_json(resp), dict)

    def test_check_name_with_valid_name(self, page, live_server):
        resp = self._get(page, live_server, "/domain/check-name", "?name=testdomain")
        assert resp.status < 500, resp.text()
        assert isinstance(_json(resp), dict)

    def test_check_name_empty_returns_not_5xx(self, page, live_server):
        resp = self._get(page, live_server, "/domain/check-name", "?name=")
        assert resp.status < 500

    def test_design_views_returns_list(self, page, live_server):
        resp = self._get(page, live_server, "/domain/design-views")
        assert resp.status == 200, resp.text()
        payload = _json(resp)
        assert isinstance(payload, (list, dict))

    def test_design_views_current_not_5xx(self, page, live_server):
        resp = self._get(page, live_server, "/domain/design-views/current")
        assert resp.status < 500

    def test_map_layout_returns_200(self, page, live_server):
        resp = self._get(page, live_server, "/domain/map-layout")
        assert resp.status == 200, resp.text()

    def test_session_debug_returns_200(self, page, live_server):
        resp = self._get(page, live_server, "/domain/session-debug")
        assert resp.status == 200, resp.text()

    def test_app_debug_returns_200(self, page, live_server):
        resp = self._get(page, live_server, "/domain/app-debug")
        assert resp.status == 200, resp.text()

    def test_current_user_not_5xx(self, page, live_server):
        resp = self._get(page, live_server, "/domain/current-user")
        assert resp.status < 500

    def test_list_versions_not_5xx(self, page, live_server):
        resp = self._get(page, live_server, "/domain/list-versions")
        assert resp.status < 500


# ── Write endpoints ────────────────────────────────────────────────────────────

class TestDomainWriteEndpoints:
    def _post(self, page, live_server, path, body=None):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        return page.context.request.post(
            f"{live_server}{path}",
            headers=_csrf_headers(page.context),
            data=json.dumps(body or {}),
        )

    def test_domain_info_save_contract(self, page, live_server):
        resp = self._post(page, live_server, "/domain/info", {"name": "e2e_domain"})
        _assert_contract(resp)

    def test_domain_reset_returns_200(self, page, live_server):
        resp = self._post(page, live_server, "/domain/reset")
        assert resp.status == 200, resp.text()

    def test_domain_clear_returns_200(self, page, live_server):
        resp = self._post(page, live_server, "/domain/clear")
        assert resp.status == 200, resp.text()

    def test_design_view_create_contract(self, page, live_server):
        resp = self._post(page, live_server, "/domain/design-views/create", {"name": "e2eview"})
        _assert_contract(resp)

    def test_design_view_save_current_contract(self, page, live_server):
        resp = self._post(page, live_server, "/domain/design-views/save-current", {})
        _assert_contract(resp)

    def test_removal_impact_contract(self, page, live_server):
        resp = self._post(
            page,
            live_server,
            "/domain/metadata/removal-impact",
            {"table_names": ["cat.sch.customers"]},
        )
        _assert_contract(resp)

    def test_removal_impact_reports_no_impact_on_empty_session(
        self, page, live_server
    ):
        """A fresh session has no mappings, so nothing can be orphaned."""
        resp = self._post(
            page,
            live_server,
            "/domain/metadata/removal-impact",
            {"table_names": ["cat.sch.customers"]},
        )
        assert resp.status == 200, resp.text()
        payload = _json(resp)
        assert payload["impact"] == {}
        assert payload["affected_table_count"] == 0
        assert payload["affected_mapping_count"] == 0

    def test_removal_impact_empty_list_contract(self, page, live_server):
        resp = self._post(
            page, live_server, "/domain/metadata/removal-impact", {"table_names": []}
        )
        _assert_contract(resp)
