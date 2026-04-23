"""
Layer 2 UI Tests -- Digital Twin page sidebar parity (Playwright).

Extends the pre-existing minimal ``TestDigitalTwinSidebar`` coverage in
``test_e2e_flows.py`` by parametrising over *every* sidebar section
declared in ``templates/dtwin.html`` (insight, dataquality, reasoning,
sigmagraph, graphql, chat) — so a new section cannot be added without
the E2E campaign noticing.
"""

import pytest


DTWIN_SECTIONS = [
    "insight",
    "dataquality",
    "reasoning",
    "sigmagraph",
    "graphql",
    "chat",
]


class TestDigitalTwinSidebarParity:
    """Every dtwin sidebar section must be reachable via ``SidebarNav``."""

    @pytest.mark.parametrize("section", DTWIN_SECTIONS)
    def test_sidebar_switches_section(self, page, live_server, section):
        page.goto(f"{live_server}/dtwin/")
        page.wait_for_load_state("domcontentloaded")
        # Let SidebarNav bootstrap first.
        page.wait_for_timeout(500)
        # Some sidebar links are disabled when no domain is loaded; bypass
        # CSS pointer-events by switching programmatically like
        # TestMappingSidebar / TestDomainSidebar already do.
        page.evaluate(f'SidebarNav.switchTo("{section}")')
        page.wait_for_timeout(400)
        section_div = page.locator(f"#{section}-section")
        assert (
            section_div.count() == 1
        ), f"Section #{section}-section is not declared in dtwin.html"
        assert (
            section_div.is_visible()
        ), f"Section #{section}-section is not visible after SidebarNav.switchTo"

    def test_graph_chat_section_has_input(self, page, live_server):
        """The chat panel must expose an input area for the user prompt."""
        page.goto(f"{live_server}/dtwin/")
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(500)
        page.evaluate('SidebarNav.switchTo("chat")')
        page.wait_for_timeout(400)
        panel = page.locator("#chat-section")
        assert panel.is_visible()
        # Accept either a textarea or an input; chat UIs differ.
        interactable = panel.locator("textarea, input[type='text'], [contenteditable]")
        assert interactable.count() >= 1, "Chat section has no input field"
