"""
E2E — Knowledge Graph page.

Merges two previously separate test files:

* Basic sidebar checks (default section, Knowledge Graph nav link).
* Full sidebar parity — every section declared in the template
  (insight, dataquality, reasoning, sigmagraph, graphql, chat) must be
  reachable via ``SidebarNav.switchTo()``.  A section added to the
  template without a matching ``#{section}-section`` ``<div>`` will
  cause this suite to fail.
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


class TestDigitalTwinSidebar:
    """Basic structural checks for the Knowledge Graph page."""

    def test_sigmagraph_section_visible_by_default(self, page, live_server):
        page.goto(f"{live_server}/dtwin/")
        page.wait_for_load_state("domcontentloaded")
        assert page.locator("#sigmagraph-section").is_visible()

    def test_sidebar_knowledge_graph_link(self, page, live_server):
        page.goto(f"{live_server}/dtwin/")
        page.wait_for_load_state("domcontentloaded")
        link = page.locator('a[data-section="sigmagraph"]')
        assert link.is_visible()
        assert (
            "knowledge" in (link.text_content() or "").lower()
            or "graph" in (link.text_content() or "").lower()
        )


class TestDigitalTwinSidebarParity:
    """Every dtwin sidebar section must be reachable via ``SidebarNav``."""

    @pytest.mark.parametrize("section", DTWIN_SECTIONS)
    def test_sidebar_switches_section(self, page, live_server, section):
        page.goto(f"{live_server}/dtwin/")
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(500)
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
        interactable = panel.locator("textarea, input[type='text'], [contenteditable]")
        assert interactable.count() >= 1, "Chat section has no input field"
