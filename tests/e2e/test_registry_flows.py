"""
Layer 2 UI Tests -- Registry page (Playwright).

Covers the /registry HTML page that was previously absent from the E2E
campaign. All assertions are read-only -- no domain is created or
mutated -- so the tests can run in any environment.
"""

class TestRegistryPage:
    """Smoke tests for the Registry HTML page."""

    def test_registry_loads(self, page, live_server):
        page.goto(f"{live_server}/registry/")
        page.wait_for_load_state("domcontentloaded")
        assert "Registry" in page.title() or "OntoBricks" in page.title()

    def test_registry_has_content(self, page, live_server):
        """Registry page should render a non-empty body."""
        page.goto(f"{live_server}/registry/")
        page.wait_for_load_state("domcontentloaded")
        body_text = page.text_content("body") or ""
        assert len(body_text.strip()) > 0

    def test_registry_navbar_visible(self, page, live_server):
        """Shared navbar should still render on /registry."""
        page.goto(f"{live_server}/registry/")
        page.wait_for_load_state("domcontentloaded")
        assert page.locator("a.navbar-brand").is_visible()

    def test_registry_link_exists_in_dom(self, page, live_server):
        """A link to ``/registry/...`` must exist somewhere in the DOM
        (it may be hidden behind a dropdown toggle, which is why we do
        not click it here — we just assert discoverability)."""
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        assert page.locator('a[href^="/registry"]').count() >= 1
