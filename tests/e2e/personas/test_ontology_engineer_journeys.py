"""UAT — Ontology Engineer (Olu, builder) end-to-end journey.

Olu owns ontology design. This journey imports an ontology, performs CRUD on
classes/properties, exercises the data-quality and rules surfaces, and verifies
pure-compute OWL generation — all session-local and deterministic offline.
Build / submit-for-review (Databricks/registry-backed) are time-boxed and skip
cleanly when no workspace creds are present.
"""

from __future__ import annotations

import json

import pytest

from tests.e2e.personas import personas as P
from tests.e2e.personas._helpers import (
    body_json,
    body_role,
    open_page,
    prime,
    switch_section,
    SEED_OWL,
)

pytestmark = [pytest.mark.e2e, pytest.mark.uat]

PERSONA = P.ONTOLOGY_ENGINEER


class TestOntologyEngineerJourney:
    def test_lands_on_ontology_page_as_builder(self, persona_page, live_server):
        page = persona_page(PERSONA)
        open_page(page, live_server, "/ontology")
        app_role, domain_role = body_role(page)
        assert app_role == "app_user" and domain_role == "builder"

    def test_import_ontology(self, persona_page, live_server):
        page = persona_page(PERSONA)
        headers = prime(page, live_server)
        resp = page.context.request.post(
            f"{live_server}/ontology/import-owl",
            headers=headers,
            data=json.dumps({"content": SEED_OWL}),
        )
        assert resp.status == 200, resp.text()[:300]
        loaded = body_json(
            page.context.request.get(
                f"{live_server}/ontology/load", headers={"Accept": "application/json"}
            )
        )
        names = [c.get("name", "") for c in loaded.get("config", {}).get("classes", [])]
        assert "Customer" in names and "Order" in names

    def test_add_class(self, persona_page, live_server):
        page = persona_page(PERSONA)
        headers = prime(page, live_server)
        resp = page.context.request.post(
            f"{live_server}/ontology/class/add",
            headers=headers,
            data=json.dumps({"name": "Invoice", "description": "UAT class"}),
        )
        assert resp.status == 200, resp.text()[:300]
        assert body_json(resp).get("success") is True

    def test_add_property_is_authorized(self, persona_page, live_server):
        page = persona_page(PERSONA)
        headers = prime(page, live_server)
        # Seed first so domain/range URIs resolve.
        page.context.request.post(
            f"{live_server}/ontology/import-owl",
            headers=headers,
            data=json.dumps({"content": SEED_OWL}),
        )
        resp = page.context.request.post(
            f"{live_server}/ontology/property/add",
            headers=headers,
            data=json.dumps(
                {
                    "name": "billedTo",
                    "domain": "http://uat.ontobricks.test/Order",
                    "range": "http://uat.ontobricks.test/Customer",
                }
            ),
        )
        # Builder is authorized; payload may be incomplete (4xx) but never 403/5xx.
        assert resp.status not in (403,) and resp.status < 500, resp.text()[:300]

    def test_data_quality_shape_authorized(self, persona_page, live_server):
        page = persona_page(PERSONA)
        headers = prime(page, live_server)
        resp = page.context.request.post(
            f"{live_server}/ontology/dataquality/save",
            headers=headers,
            data=json.dumps({"shapes": []}),
        )
        assert resp.status != 403 and resp.status < 500, resp.text()[:300]

    def test_generate_owl_includes_classes(self, persona_page, live_server):
        page = persona_page(PERSONA)
        headers = prime(page, live_server)
        page.context.request.post(
            f"{live_server}/ontology/import-owl",
            headers=headers,
            data=json.dumps({"content": SEED_OWL}),
        )
        loaded = body_json(
            page.context.request.get(
                f"{live_server}/ontology/load", headers={"Accept": "application/json"}
            )
        )
        config = loaded.get("config", {})
        resp = page.context.request.post(
            f"{live_server}/ontology/generate-owl",
            headers=headers,
            data=json.dumps(config),
        )
        assert resp.status == 200, resp.text()[:300]
        owl = body_json(resp).get("owl", "")
        assert "Customer" in owl and "Order" in owl

    def test_entities_section_renders(self, persona_page, live_server):
        page = persona_page(PERSONA)
        open_page(page, live_server, "/ontology")
        switch_section(page, "entities")
        assert page.locator("#entities-section").is_visible()

    def test_build_trigger_is_authorized(self, persona_page, live_server):
        """Builders may trigger a sync; offline this needs Databricks → skip."""
        page = persona_page(PERSONA)
        headers = prime(page, live_server)
        try:
            resp = page.context.request.post(
                f"{live_server}/dtwin/sync/start",
                headers=headers,
                data=json.dumps({}),
                timeout=20000,
            )
        except Exception as exc:  # noqa: BLE001
            pytest.skip(f"build needs Databricks creds (offline): {exc}")
        assert (
            resp.status != 403
        ), f"builder must be authorized to build: {resp.text()[:200]}"
