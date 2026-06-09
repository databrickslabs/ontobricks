"""UAT — REST v1 API surface per persona (characterization).

The external REST API lives under ``/api/`` which is in the middleware's bypass
list, so it is **not** role-gated by ``PermissionMiddleware`` — it is a
programmatic surface authorized at the Databricks Apps gateway. These tests
*characterize and assert that actual behavior*: every persona (including the
viewer) can reach the API and validate a query. If product intent is to gate
writes here, that's a follow-up finding — not a test to force.
"""

from __future__ import annotations

import json

import pytest

from tests.e2e.personas import personas as P
from tests.e2e.personas._helpers import body_json

pytestmark = [pytest.mark.e2e, pytest.mark.uat]

_VALID_SPARQL = "SELECT * WHERE { ?s ?p ?o } LIMIT 1"


class TestApiV1Personas:
    @pytest.mark.parametrize("persona", P.ALL_PERSONAS, ids=P.ids)
    def test_health_open_to_all(self, persona_page, live_server, persona):
        page = persona_page(persona)
        resp = page.context.request.get(
            f"{live_server}/api/v1/health", headers={"Accept": "application/json"}
        )
        assert resp.status == 200, resp.text()[:200]

    @pytest.mark.parametrize("persona", P.ALL_PERSONAS, ids=P.ids)
    def test_query_validate_open_to_all(self, persona_page, live_server, persona):
        page = persona_page(persona)
        resp = page.context.request.post(
            f"{live_server}/api/v1/query/validate",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            data=json.dumps({"query": _VALID_SPARQL}),
        )
        # /api/ is CSRF- and permission-bypassed: no persona is blocked.
        assert resp.status != 403, resp.text()[:200]
        assert resp.status == 200, resp.text()[:200]

    @pytest.mark.parametrize("persona", P.ALL_PERSONAS, ids=P.ids)
    def test_domains_list_not_role_gated(self, persona_page, live_server, persona):
        page = persona_page(persona)
        resp = page.context.request.post(
            f"{live_server}/api/v1/domains/list",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            data=json.dumps({}),
        )
        # Open programmatic surface — never an authorization 403. (Backend
        # errors are possible offline; the assertion is the gating contract.)
        assert resp.status != 403, resp.text()[:200]
        # Sanity: a JSON body comes back when the registry is reachable.
        if resp.status == 200:
            assert isinstance(body_json(resp), (dict, list))
