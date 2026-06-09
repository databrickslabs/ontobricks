"""UAT — Business Consumer (Cory, viewer) end-to-end journey.

Cory only reads. This journey confirms a viewer can browse seeded ontology
content and load the explore pages, and that *every* write surface is refused
(403) at the API. (The seeded content is imported via an admin-override request
inside Cory's own session so a read-only persona has something to look at.)
"""

from __future__ import annotations

import json

import pytest

from tests.e2e.personas import personas as P
from tests.e2e.personas._helpers import (
    body_json,
    body_role,
    csrf_headers,
    open_page,
    prime,
    seed_ontology,
    SEED_OWL,
)

pytestmark = [pytest.mark.e2e, pytest.mark.uat]

PERSONA = P.CONSUMER

# Representative write endpoints across areas — all must be refused for a viewer.
WRITE_ENDPOINTS = [
    ("/ontology/class/add", {"name": "Nope"}),
    ("/ontology/import-owl", {"content": SEED_OWL}),
    ("/ontology/generate-owl", {"classes": []}),
    ("/mapping/save", {"mappings": {}}),
    ("/mapping/entity/add", {"class_uri": "x"}),
    ("/dtwin/execute", {"query": "SELECT * WHERE { ?s ?p ?o } LIMIT 1"}),
    ("/dtwin/sync/start", {}),
]


class TestConsumerCanBrowse:
    def test_lands_as_viewer(self, persona_page, live_server):
        page = persona_page(PERSONA)
        open_page(page, live_server, "/ontology")
        app_role, domain_role = body_role(page)
        assert app_role == "app_user" and domain_role == "viewer"

    def test_browse_seeded_ontology(self, persona_page, live_server):
        page = persona_page(PERSONA)
        seed_ontology(page, live_server)  # admin-override import in viewer session
        loaded = body_json(
            page.context.request.get(
                f"{live_server}/ontology/load", headers={"Accept": "application/json"}
            )
        )
        names = [c.get("name", "") for c in loaded.get("config", {}).get("classes", [])]
        assert "Customer" in names and "Order" in names

    def test_explore_page_loads(self, persona_page, live_server):
        page = persona_page(PERSONA)
        resp = page.goto(f"{live_server}/dtwin")
        page.wait_for_load_state("domcontentloaded")
        assert resp is not None and resp.status == 200

    def test_mapping_read_allowed(self, persona_page, live_server):
        page = persona_page(PERSONA)
        prime(page, live_server)
        resp = page.context.request.get(
            f"{live_server}/mapping/load", headers={"Accept": "application/json"}
        )
        assert resp.status != 403


class TestConsumerCannotWrite:
    @pytest.mark.parametrize(
        "path,payload", WRITE_ENDPOINTS, ids=[e[0] for e in WRITE_ENDPOINTS]
    )
    def test_write_refused(self, persona_page, live_server, path, payload):
        page = persona_page(PERSONA)
        prime(page, live_server)
        resp = page.context.request.post(
            f"{live_server}{path}",
            headers=csrf_headers(page.context),
            data=json.dumps(payload),
        )
        assert resp.status == 403, (
            f"viewer write to {path} must be 403, got {resp.status}: "
            f"{resp.text()[:200]}"
        )
