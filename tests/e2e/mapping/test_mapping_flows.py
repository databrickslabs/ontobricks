"""
E2E — Mapping page sidebar navigation.

Verifies that each of the six sidebar sections becomes visible when
activated via ``SidebarNav.switchTo()``.
"""

import pytest


class TestMappingSidebar:
    @pytest.mark.parametrize(
        "section",
        ["information", "design", "manual", "autoassign", "r2rml", "sparksql"],
    )
    def test_sidebar_switches_section(self, page, live_server, section):
        page.goto(f"{live_server}/mapping")
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(500)
        page.evaluate(f'SidebarNav.switchTo("{section}")')
        page.wait_for_timeout(400)
        section_div = page.locator(f"#{section}-section")
        assert (
            section_div.is_visible()
        ), f"Section #{section}-section not visible after click"
