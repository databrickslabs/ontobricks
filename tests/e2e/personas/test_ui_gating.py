"""UAT — UI permission gating per persona (offline / Tier-1).

Asserts the deterministic, server-rendered gating signals:

* ``<body data-app-role / data-domain-role>`` reflect the persona, and
* ``[data-requires-app="admin"]`` controls (warehouse status icon, Teams nav,
  registry admin actions) are visible only to the admin persona — this is pure
  CSS keyed on ``body[data-app-role]`` (``permissions.css``), so it holds even
  though the offline subprocess is not in "app mode".

The JS-driven indicators (``body.role-viewer`` stamp and the navbar role pill)
only run in app mode, so they are verified in the live-smoke suite, not here.
"""

from __future__ import annotations

import pytest

from tests.e2e.personas import personas as P
from tests.e2e.personas._helpers import body_role, open_page

pytestmark = [pytest.mark.e2e, pytest.mark.uat]


class TestBodyRoleAttributes:
    @pytest.mark.parametrize("persona", P.ALL_PERSONAS, ids=P.ids)
    def test_body_carries_persona_roles(self, persona_page, live_server, persona):
        page = persona_page(persona)
        open_page(page, live_server, "/")
        app_role, domain_role = body_role(page)
        assert (
            app_role == persona.app_role
        ), f"{persona.key}: data-app-role={app_role!r}"
        assert (
            domain_role == persona.domain_role
        ), f"{persona.key}: data-domain-role={domain_role!r}"


class TestAdminOnlyControls:
    @pytest.mark.parametrize("persona", P.ALL_PERSONAS, ids=P.ids)
    def test_warehouse_icon_admin_only(self, persona_page, live_server, persona):
        page = persona_page(persona)
        open_page(page, live_server, "/")
        warehouse = page.locator("#warehouseStatusLink")
        if persona.is_admin:
            assert warehouse.is_visible(), "admin should see the warehouse icon"
        else:
            assert (
                not warehouse.is_visible()
            ), f"{persona.key} must not see the admin-only warehouse icon"

    @pytest.mark.parametrize("persona", P.ALL_PERSONAS, ids=P.ids)
    def test_admin_only_elements_hidden_for_non_admin(
        self, persona_page, live_server, persona
    ):
        page = persona_page(persona)
        open_page(page, live_server, "/")
        gated = page.locator('[data-requires-app="admin"]')
        total = gated.count()
        assert total > 0, "expected some data-requires-app=admin controls on home"
        visible = sum(1 for i in range(total) if gated.nth(i).is_visible())
        if persona.is_admin:
            assert visible >= 1, "admin should see at least one admin-only control"
        else:
            assert (
                visible == 0
            ), f"{persona.key} sees {visible} admin-only control(s) — must be 0"
