"""
Layer 2 UI Tests -- Registry on the Home page (Playwright).

Covers the Home launch tiles that open the 80% Registry modal
(Contexts · Bridges · My tasks). All assertions are read-only so the
tests can run in any environment.
"""


class TestRegistryHomeTiles:
    """Smoke tests for the Registry tiles + modal on Home."""

    def test_registry_legacy_url_opens_home_browse(self, page, live_server):
        page.goto(f"{live_server}/registry/")
        page.wait_for_load_state("domcontentloaded")
        assert "section=domains" in page.url
        page.wait_for_selector("#homeRegistryModal.show", state="visible")

    def test_registry_tiles_in_dom(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        assert page.locator("#homeTabs").count() == 1
        assert page.locator("#homeRegistryModal").count() == 1
        assert page.locator("#registryDomainsSection").count() >= 1

    def test_navbar_registry_icon_removed(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        assert page.locator("#registryModalToggle").count() == 0

    def test_tile_opens_modal_and_updates_url(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        page.locator('[data-home-section="tasks"]').first.click()
        page.wait_for_selector("#homeRegistryModal.show", state="visible")
        page.wait_for_selector("#home-pane-tasks.active")
        assert "section=tasks" in page.url

    def test_my_tasks_pane_shows_content_or_empty_state(self, page, live_server):
        page.goto(f"{live_server}/?section=tasks")
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_selector("#homeRegistryModal.show", state="visible")
        page.wait_for_selector("#home-pane-tasks.active #homeTasksContainer > *")
        assert page.locator("#homeTasksContainer").count() == 1

    def test_registry_navbar_visible(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        assert page.locator("a.navbar-brand").is_visible()
