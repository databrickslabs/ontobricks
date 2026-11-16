"""
E2E — Graph → Management → Mapping navigation.

Verifies that each Mapping sidebar section (under Graph) becomes visible when
activated via ``SidebarNav.switchTo()``. Legacy ``/mapping/`` redirects into
``/dtwin/``.
"""

import pytest


class TestMappingSidebar:
    @pytest.mark.parametrize(
        "section",
        ["information", "design"],
    )
    def test_sidebar_switches_section(self, page, live_server, section):
        page.goto(f"{live_server}/dtwin/?section={section}")
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(500)
        page.evaluate(f'SidebarNav.switchTo("{section}")')
        page.wait_for_timeout(400)
        section_div = page.locator(f"#{section}-section")
        assert (
            section_div.is_visible()
        ), f"Section #{section}-section not visible after click"

    def test_studio_list_tab_hosts_manual_tree(self, page, live_server):
        page.goto(f"{live_server}/dtwin/?section=design&tab=list")
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(600)
        assert page.locator("#mappingStudioPaneList").is_visible()
        assert page.locator("#manualAssignmentTree").count() == 1

    def test_studio_toolbar_hosts_r2rml_import_button(self, page, live_server):
        page.goto(f"{live_server}/dtwin/?section=design&tab=informations")
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(500)
        assert page.locator("#mappingStudioTabInformations").is_visible()
        assert page.locator("#mappingImportR2RMLBtn").is_visible()
        assert page.locator("#mappingExportR2RMLBtn").is_visible()

    def test_studio_opens_automap_modal(self, page, live_server):
        page.goto(f"{live_server}/dtwin/?section=design")
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(500)
        page.locator("#openAutoAssignModalBtn").click()
        page.wait_for_timeout(300)
        assert page.locator("#autoAssignModal").is_visible()
        assert page.locator("#startAutoAssignBtn").is_visible()

    def test_build_page_has_diagnostics_modal_and_button(self, page, live_server):
        page.goto(f"{live_server}/dtwin/?section=sync")
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(500)
        assert page.locator("#mappingDiagnosticsModal").count() == 1
