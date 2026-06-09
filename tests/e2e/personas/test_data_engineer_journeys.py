"""UAT — Data Engineer (Dana, builder) end-to-end journey.

Dana wires data to the ontology. This journey exercises the mapping surfaces
(save, entity add, R2RML generation, diagnostics) which are session-local and
deterministic, then attempts the Databricks-backed steps (UC tables, SQL test,
build/sync) which are time-boxed and skip offline.
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
)

pytestmark = [pytest.mark.e2e, pytest.mark.uat]

PERSONA = P.DATA_ENGINEER


class TestDataEngineerJourney:
    def test_lands_on_mapping_page_as_builder(self, persona_page, live_server):
        page = persona_page(PERSONA)
        open_page(page, live_server, "/mapping")
        app_role, domain_role = body_role(page)
        assert app_role == "app_user" and domain_role == "builder"

    def test_save_mapping_is_authorized(self, persona_page, live_server):
        page = persona_page(PERSONA)
        seed_ontology(page, live_server)
        resp = page.context.request.post(
            f"{live_server}/mapping/save",
            headers=csrf_headers(page.context),
            data=json.dumps({"mappings": {}}),
        )
        assert resp.status != 403 and resp.status < 500, resp.text()[:300]

    def test_add_entity_mapping_authorized(self, persona_page, live_server):
        page = persona_page(PERSONA)
        headers = prime(page, live_server)
        resp = page.context.request.post(
            f"{live_server}/mapping/entity/add",
            headers=headers,
            data=json.dumps({"class_uri": "http://uat.ontobricks.test/Customer"}),
        )
        assert resp.status != 403 and resp.status < 500, resp.text()[:300]

    def test_generate_r2rml(self, persona_page, live_server):
        page = persona_page(PERSONA)
        seed_ontology(page, live_server)
        resp = page.context.request.post(
            f"{live_server}/mapping/generate",
            headers=prime(page, live_server),
            data=json.dumps({}),
        )
        # R2RML generation is pure-compute; authorized and not a crash.
        assert resp.status != 403 and resp.status < 500, resp.text()[:300]
        if resp.status == 200:
            assert "r2rml" in body_json(resp)

    def test_diagnostics_readable(self, persona_page, live_server):
        page = persona_page(PERSONA)
        prime(page, live_server)
        resp = page.context.request.get(
            f"{live_server}/mapping/diagnostics", headers={"Accept": "application/json"}
        )
        assert resp.status != 403

    def test_uc_tables_browse(self, persona_page, live_server):
        """Browsing UC tables needs Databricks → skip offline."""
        page = persona_page(PERSONA)
        headers = prime(page, live_server)
        try:
            resp = page.context.request.post(
                f"{live_server}/mapping/tables",
                headers=headers,
                data=json.dumps({}),
                timeout=20000,
            )
        except Exception as exc:  # noqa: BLE001
            pytest.skip(f"UC metadata needs Databricks creds (offline): {exc}")
        assert resp.status != 403

    def test_build_sync_authorized(self, persona_page, live_server):
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
        assert resp.status != 403, "builder must be authorized to build"
