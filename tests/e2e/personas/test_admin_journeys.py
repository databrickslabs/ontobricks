"""UAT — Platform Admin (Priya) end-to-end journey.

Priya governs the platform. This journey confirms the admin-only surfaces are
visible and reachable (Settings, warehouse, Teams) and that the admin is
authorized everywhere a non-admin is blocked. Registry/Databricks-backed writes
are time-boxed and skip offline; the authorization outcome is the assertion.
"""

from __future__ import annotations

import json

import pytest

from tests.e2e.personas import personas as P
from tests.e2e.personas._helpers import body_role, open_page, prime

pytestmark = [pytest.mark.e2e, pytest.mark.uat]

PERSONA = P.ADMIN


class TestAdminJourney:
    def test_lands_as_admin(self, persona_page, live_server):
        page = persona_page(PERSONA)
        open_page(page, live_server, "/")
        app_role, domain_role = body_role(page)
        assert app_role == "admin" and domain_role == "admin"

    def test_warehouse_icon_visible(self, persona_page, live_server):
        page = persona_page(PERSONA)
        open_page(page, live_server, "/")
        assert page.locator("#warehouseStatusLink").is_visible()

    def test_settings_page_reachable(self, persona_page, live_server):
        page = persona_page(PERSONA)
        resp = page.goto(f"{live_server}/settings")
        page.wait_for_load_state("domcontentloaded")
        assert resp is not None and resp.status == 200
        assert "/access-denied" not in page.url

    def test_admin_can_write_settings(self, persona_page, live_server):
        page = persona_page(PERSONA)
        headers = prime(page, live_server)
        try:
            resp = page.context.request.post(
                f"{live_server}/settings/save-base-uri",
                headers=headers,
                data=json.dumps({"base_uri": "https://uat-admin.test/"}),
                timeout=20000,
            )
        except Exception as exc:  # noqa: BLE001
            pytest.skip(f"settings write needs backend (offline): {exc}")
        assert resp.status != 403, "admin must be authorized for settings writes"

    def test_admin_can_reach_teams_save(self, persona_page, live_server):
        page = persona_page(PERSONA)
        headers = prime(page, live_server)
        try:
            resp = page.context.request.post(
                f"{live_server}/settings/teams",
                headers=headers,
                data=json.dumps({}),
                timeout=20000,
            )
        except Exception as exc:  # noqa: BLE001
            pytest.skip(f"teams save needs registry (offline): {exc}")
        assert resp.status != 403, "admin must be authorized for the Teams matrix"

    def test_admin_can_edit_domain(self, persona_page, live_server):
        page = persona_page(PERSONA)
        headers = prime(page, live_server)
        resp = page.context.request.post(
            f"{live_server}/ontology/class/add",
            headers=headers,
            data=json.dumps({"name": "AdminClass"}),
        )
        assert resp.status != 403, resp.text()[:200]

    def test_admin_can_trigger_build(self, persona_page, live_server):
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
        assert resp.status != 403
