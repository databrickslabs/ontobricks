"""
E2E — Settings page UI.

Covers the Databricks section, host display, base-URI field, and Save button.
"""


class TestSettingsPage:
    def test_databricks_section_visible(self, page, live_server):
        page.goto(f"{live_server}/settings")
        page.wait_for_load_state("domcontentloaded")
        assert page.locator("#databricks-section").is_visible()

    def test_host_display(self, page, live_server):
        page.goto(f"{live_server}/settings")
        page.wait_for_load_state("domcontentloaded")
        assert page.locator("#currentHostDisplay").is_visible()

    def test_base_uri_field(self, page, live_server):
        page.goto(f"{live_server}/settings")
        page.wait_for_load_state("domcontentloaded")
        page.click(".sidebar-nav .nav-link[data-section='global']")
        page.wait_for_timeout(400)
        field = page.locator("#baseUriDefault")
        assert field.is_visible()
        assert field.input_value() != ""

    def test_save_button_clickable(self, page, live_server):
        page.goto(f"{live_server}/settings")
        page.wait_for_load_state("domcontentloaded")
        btn = page.locator(".btn-save-settings").first
        assert btn.is_visible()
        assert btn.is_enabled()
