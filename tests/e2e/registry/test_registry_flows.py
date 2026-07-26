"""
Layer 2 UI Tests -- Registry modal (Playwright).

Covers the navbar Registry icon + Browse/Bridges modal. All assertions
are read-only so the tests can run in any environment.
"""


class TestRegistryModal:
    """Smoke tests for the Registry modal entry point."""

    def test_registry_legacy_url_opens_home(self, page, live_server):
        page.goto(f"{live_server}/registry/")
        page.wait_for_load_state("domcontentloaded")
        assert "open=registry" in page.url or page.locator("#registryModal").count() >= 1

    def test_registry_toggle_in_navbar(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        assert page.locator("#registryModalToggle").count() >= 1

    def test_registry_modal_in_dom(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        assert page.locator("#registryModal").count() >= 1
        assert page.locator("#registryDomainsSection").count() >= 1

    def test_registry_navbar_visible(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        assert page.locator("a.navbar-brand").is_visible()
