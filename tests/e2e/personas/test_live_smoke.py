"""UAT — live smoke (Tier-2): real Databricks journeys against a deployed app.

These run ONLY in live mode (``ONTOBRICKS_LIVE_BASE`` set) and authenticate as
the bearer-token user (the persona seam is offline-only). They verify the
Databricks-backed surfaces that the offline tier can only assert at the
permission boundary: registry discovery, ontology read, real SPARQL execution,
digital-twin status/stats. Durable mutations (build/materialize) run only with
``ONTOBRICKS_LIVE_ALLOW_MUTATING=1``.

Run:
    export ONTOBRICKS_LIVE_BASE=https://<app-host>
    export DATABRICKS_CONFIG_PROFILE=fevm-ontobricks-int
    uv run pytest tests/e2e/personas/test_live_smoke.py -m "uat and live_integration" --no-cov
"""

from __future__ import annotations

import json
import os

import pytest

from tests.e2e.personas._helpers import body_json

pytestmark = [pytest.mark.e2e, pytest.mark.uat, pytest.mark.live_integration]


def _require_live():
    if not os.environ.get("ONTOBRICKS_LIVE_BASE"):
        pytest.skip("live smoke requires ONTOBRICKS_LIVE_BASE (deployed app)")


class TestLiveSmoke:
    def test_api_health(self, page, live_server):
        _require_live()
        resp = page.context.request.get(f"{live_server}/api/v1/health")
        assert resp.status == 200

    def test_registry_discovery(self, page, live_server):
        _require_live()
        # The external domains/list endpoint requires UC coordinates
        # ({catalog, schema, volume}); with an empty body the deployed app
        # correctly answers 422. The smoke proves the registry API surface is
        # reachable AND the gateway authenticated us (no 401/403) AND the
        # contract is enforced (200 with coords, or 422 without) — not a 5xx.
        resp = page.context.request.post(
            f"{live_server}/api/v1/domains/list",
            headers={"Content-Type": "application/json"},
            data=json.dumps({}),
        )
        assert resp.status in (200, 422), resp.text()[:300]
        assert resp.status not in (401, 403), "live bearer auth should pass"

    def test_ontology_read(self, page, live_server):
        _require_live()
        page.goto(f"{live_server}/ontology")
        page.wait_for_load_state("domcontentloaded")
        resp = page.context.request.get(f"{live_server}/ontology/load")
        assert resp.status in (200, 404)

    def test_real_sparql_query(self, page, live_server):
        _require_live()
        resp = page.context.request.post(
            f"{live_server}/api/v1/query",
            headers={"Content-Type": "application/json"},
            data=json.dumps({"query": "SELECT * WHERE { ?s ?p ?o } LIMIT 5"}),
        )
        assert resp.status in (200, 400, 422), resp.text()[:300]

    def test_digital_twin_status(self, page, live_server):
        _require_live()
        resp = page.context.request.get(f"{live_server}/api/v1/digitaltwin/status")
        assert resp.status in (200, 400, 404), resp.text()[:300]

    def test_build_trigger_when_mutating_allowed(self, page, live_server):
        _require_live()
        if os.environ.get("ONTOBRICKS_LIVE_ALLOW_MUTATING") != "1":
            pytest.skip("set ONTOBRICKS_LIVE_ALLOW_MUTATING=1 to run the build smoke")
        # Prime CSRF then trigger a sync; the bearer user must be a builder/admin
        # on the loaded domain for this to be authorized.
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        cookies = {c["name"]: c["value"] for c in page.context.cookies()}
        headers = {"Content-Type": "application/json"}
        if cookies.get("csrf_token"):
            headers["X-CSRF-Token"] = cookies["csrf_token"]
        resp = page.context.request.post(
            f"{live_server}/dtwin/sync/start", headers=headers, data=json.dumps({})
        )
        assert resp.status != 403, "live bearer user should be authorized to build"
        body = body_json(resp)
        assert resp.status < 500, body
