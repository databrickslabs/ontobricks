"""
E2E — Knowledge Graph internal API (/dtwin/…).

Covers read + write API endpoints that the Knowledge Graph page uses for
syncing, querying, and running analytics on the knowledge graph.

In the test harness the graph store is empty (no Databricks backend),
so endpoints that require a populated triplestore legitimately return
non-200 statuses.  We assert:
  - the route is mounted (not 404),
  - the response is JSON,
  - the status is never a framework 5xx.

Covered:
  GET  /dtwin/sync/status
  GET  /dtwin/sync/stats
  GET  /dtwin/sync/info
  GET  /dtwin/sync/dt-existence
  GET  /dtwin/sync/changes
  GET  /dtwin/groups
  POST /dtwin/execute          — SPARQL execution contract
  POST /dtwin/translate        — NL-to-SPARQL contract
  POST /dtwin/sync/filter      — filter contract
"""

from __future__ import annotations

import json

import pytest


_CONTRACT_STATUSES = (200, 400, 403, 404, 422, 502)

_VALID_SPARQL = "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 5"


def _csrf_headers(context) -> dict:
    cookies = {c["name"]: c["value"] for c in context.cookies()}
    h = {"Content-Type": "application/json"}
    if tok := cookies.get("csrf_token"):
        h["X-CSRF-Token"] = tok
    return h


def _json(resp) -> dict | list:
    return json.loads(resp.body())


def _assert_contract(resp):
    assert resp.status in _CONTRACT_STATUSES, (
        f"Unexpected status {resp.status}: {resp.text()}"
    )
    payload = _json(resp)
    assert isinstance(payload, (dict, list))


# ── Sync read endpoints ────────────────────────────────────────────────────────

class TestDtwinSyncRead:
    def _get(self, page, live_server, path):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        return page.request.get(f"{live_server}{path}")

    def test_sync_status_route_mounted(self, page, live_server):
        resp = self._get(page, live_server, "/dtwin/sync/status")
        assert resp.status != 404, "/dtwin/sync/status is not mounted"

    def test_sync_status_is_json(self, page, live_server):
        resp = self._get(page, live_server, "/dtwin/sync/status")
        assert isinstance(_json(resp), (dict, list))

    def test_sync_status_no_5xx(self, page, live_server):
        resp = self._get(page, live_server, "/dtwin/sync/status")
        assert resp.status < 500, f"sync/status 5xx: {resp.text()}"

    def test_sync_stats_route_mounted(self, page, live_server):
        resp = self._get(page, live_server, "/dtwin/sync/stats")
        assert resp.status != 404, "/dtwin/sync/stats is not mounted"

    def test_sync_stats_contract(self, page, live_server):
        """502 is acceptable: graph backend not configured in test harness."""
        resp = self._get(page, live_server, "/dtwin/sync/stats")
        assert resp.status in _CONTRACT_STATUSES, f"sync/stats unexpected status: {resp.text()}"

    def test_sync_info_no_5xx(self, page, live_server):
        resp = self._get(page, live_server, "/dtwin/sync/info")
        assert resp.status < 500, f"sync/info 5xx: {resp.text()}"

    def test_sync_dt_existence_no_5xx(self, page, live_server):
        resp = self._get(page, live_server, "/dtwin/sync/dt-existence")
        assert resp.status < 500, f"sync/dt-existence 5xx: {resp.text()}"

    def test_sync_changes_no_5xx(self, page, live_server):
        resp = self._get(page, live_server, "/dtwin/sync/changes")
        assert resp.status < 500, f"sync/changes 5xx: {resp.text()}"

    def test_groups_no_5xx(self, page, live_server):
        resp = self._get(page, live_server, "/dtwin/groups")
        assert resp.status < 500, f"/dtwin/groups 5xx: {resp.text()}"


# ── SPARQL / translate ────────────────────────────────────────────────────────

class TestDtwinSparqlExecute:
    def _post(self, page, live_server, path, body):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        return page.context.request.post(
            f"{live_server}{path}",
            headers=_csrf_headers(page.context),
            data=json.dumps(body),
        )

    def test_execute_route_mounted(self, page, live_server):
        resp = self._post(page, live_server, "/dtwin/execute", {"query": _VALID_SPARQL})
        assert resp.status != 404, "/dtwin/execute is not mounted"

    def test_execute_valid_sparql_contract(self, page, live_server):
        resp = self._post(page, live_server, "/dtwin/execute", {"query": _VALID_SPARQL})
        _assert_contract(resp)

    def test_execute_missing_query_returns_422(self, page, live_server):
        resp = self._post(page, live_server, "/dtwin/execute", {})
        assert resp.status in (400, 422), (
            f"Expected 400/422 for missing query, got {resp.status}"
        )

    def test_execute_response_is_json(self, page, live_server):
        resp = self._post(page, live_server, "/dtwin/execute", {"query": _VALID_SPARQL})
        assert isinstance(_json(resp), (dict, list))

    def test_translate_contract(self, page, live_server):
        resp = self._post(
            page, live_server, "/dtwin/translate",
            {"text": "list all classes", "query": ""},
        )
        _assert_contract(resp)


# ── Filter ────────────────────────────────────────────────────────────────────

class TestDtwinSyncFilter:
    def test_filter_contract(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        resp = page.context.request.post(
            f"{live_server}/dtwin/sync/filter",
            headers=_csrf_headers(page.context),
            data=json.dumps({"class_uris": []}),
        )
        assert resp.status in _CONTRACT_STATUSES, (
            f"Unexpected status {resp.status}: {resp.text()}"
        )
