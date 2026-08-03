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

    def test_confirm_stacks_over_registry_modal(self, page, live_server):
        """Confirm over Registry must mark the Registry as underlying and
        clean up after cancel — covers white-on-white stacking."""
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")

        page.locator("#registryModalToggle").click()
        page.wait_for_selector("#registryModal.show", state="visible")

        page.evaluate(
            """() => {
                window.__obConfirmPromise = showConfirmDialog({
                    title: 'Load Domain',
                    message: 'Load <strong>fibo</strong> version <strong>v1</strong>?',
                    confirmText: 'Load',
                    cancelText: 'Cancel',
                });
            }"""
        )

        page.wait_for_selector(".modal.ob-modal-stacked.show", state="visible")
        assert page.locator("#registryModal.ob-modal-underlying").count() == 1
        assert page.locator(".modal-backdrop.ob-modal-stacked-backdrop").count() >= 1

        page.locator(".modal.ob-modal-stacked .btn-secondary").click()
        page.wait_for_selector(".modal.ob-modal-stacked", state="detached")
        page.wait_for_selector(
            ".modal-backdrop.ob-modal-stacked-backdrop", state="detached"
        )

        assert page.locator("#registryModal.show").count() == 1
        assert page.locator("#registryModal.ob-modal-underlying").count() == 0
