"""
E2E — Domain page sidebar navigation.

Sidebar links use ``pointer-events:none`` when no domain is loaded;
tests bypass CSS by switching via ``SidebarNav.switchTo()`` directly.
"""

import pytest


class TestDomainSidebar:
    @pytest.mark.parametrize(
        "section",
        ["information", "metadata", "documents", "validation", "owl-content", "r2rml"],
    )
    def test_sidebar_switches_section(self, page, live_server, section):
        page.goto(f"{live_server}/domain")
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(500)
        page.evaluate(f'SidebarNav.switchTo("{section}")')
        page.wait_for_timeout(400)
        section_div = page.locator(f"#{section}-section")
        assert (
            section_div.is_visible()
        ), f"Section #{section}-section not visible after click"
