"""
E2E — Graph → Management → Mapping.

Covers the Mapping page as it ships in 0.10.0: a single ``design`` section
(sidebar label "Mapping", right after Build) with Informations / Visual /
List tabs sharing one bottom panel. Legacy ``/mapping/``,
``?section=information`` and ``?section=manual`` URLs redirect into it.

``needs_domain`` makes local runs seed a session domain — otherwise every
workspace URL 303s to Home.
"""

import pytest

pytestmark = pytest.mark.needs_domain

ONTOLOGY_JS = """
() => {
    MappingState.loadedOntology = {
        classes: [
            {uri: 'http://e2e.test/Customer', name: 'Customer', label: 'Customer', dataProperties: []},
            {uri: 'http://e2e.test/Order', name: 'Order', label: 'Order', dataProperties: []},
        ],
        properties: [
            {uri: 'http://e2e.test/places', name: 'places', label: 'places',
             type: 'ObjectProperty', domain: 'Customer', range: 'Order'},
        ],
    };
    MappingState.config = {entities: [], relationships: []};
    MappingState.initialized = true;
}
"""


def _open_mapping(page, live_server, query="section=design"):
    page.goto(f"{live_server}/dtwin/?{query}")
    page.wait_for_load_state("domcontentloaded")
    page.wait_for_function("() => typeof MappingStudioTabs !== 'undefined'")
    page.wait_for_timeout(400)


def _tab_param(page):
    return page.evaluate("new URL(location.href).searchParams.get('tab')")


def _section_param(page):
    return page.evaluate("new URL(location.href).searchParams.get('section')")


def _load_fake_ontology(page):
    page.wait_for_function(
        "() => typeof MappingState !== 'undefined' && MappingState.initialized",
        timeout=15000,
    )
    page.evaluate(ONTOLOGY_JS)


class TestMappingNavigation:
    def test_sidebar_shows_mapping_right_after_build(self, page, live_server):
        _open_mapping(page, live_server)
        order = page.evaluate(
            """() => Array.from(document.querySelectorAll('a[data-section]'))
                     .map(a => a.dataset.section)"""
        )
        assert "information" not in order
        assert "manual" not in order
        idx = order.index("sync")
        assert order[idx : idx + 3] == ["sync", "design", "insight"]
        labels = page.evaluate(
            """() => Array.from(document.querySelectorAll('a[data-section="design"]'))
                     .map(a => a.textContent.trim())"""
        )
        assert any("Mapping" in label for label in labels), labels
        assert not any("Studio" in label for label in labels), labels

    def test_design_section_defaults_to_informations_tab(self, page, live_server):
        _open_mapping(page, live_server)
        assert page.locator("#design-section").is_visible()
        assert "active" in page.locator("#mappingStudioTabInformations").get_attribute("class")
        assert page.evaluate("MappingStudioTabs.current()") == "informations"

    @pytest.mark.parametrize(
        "url, expected_tab",
        [
            ("/mapping/", "informations"),
            ("/dtwin/?section=information", "informations"),
            ("/dtwin/?section=manual", "list"),
        ],
    )
    def test_legacy_urls_land_on_mapping_tab(
        self, page, live_server, url, expected_tab
    ):
        page.goto(f"{live_server}{url}")
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_function("() => typeof MappingStudioTabs !== 'undefined'")
        page.wait_for_timeout(500)
        assert "/dtwin" in page.url
        assert _section_param(page) == "design"
        assert _tab_param(page) == expected_tab
        assert page.evaluate("MappingStudioTabs.current()") == expected_tab


class TestMappingTabs:
    def test_tab_clicks_sync_url_and_bottom_panel(self, page, live_server):
        _open_mapping(page, live_server, "section=design&tab=informations")
        panel = page.locator("#mappingRightPanel")
        assert "d-none" in (panel.get_attribute("class") or "")

        page.locator("#mappingStudioTabVisual").click()
        page.wait_for_timeout(300)
        assert _tab_param(page) == "visual"
        assert "d-none" not in (panel.get_attribute("class") or "")

        page.locator("#mappingStudioTabList").click()
        page.wait_for_timeout(300)
        assert _tab_param(page) == "list"
        assert page.locator("#mappingStudioPaneList").is_visible()
        assert page.locator("#manualAssignmentTree").count() == 1
        assert "d-none" not in (panel.get_attribute("class") or "")

        page.locator("#mappingStudioTabInformations").click()
        page.wait_for_timeout(300)
        assert _tab_param(page) == "informations"
        assert "d-none" in (panel.get_attribute("class") or "")

    def test_informations_tab_hosts_r2rml_import_export(self, page, live_server):
        _open_mapping(page, live_server, "section=design&tab=informations")
        assert page.locator("#mappingImportR2RMLBtn").is_visible()
        assert page.locator("#mappingExportR2RMLBtn").is_visible()

    def test_opens_automap_modal(self, page, live_server):
        _open_mapping(page, live_server)
        btn = page.locator("#openAutoAssignModalBtn")
        assert btn.count() == 1
        if btn.get_attribute("aria-disabled") == "true":
            pytest.skip("Auto-Map is gated on an LLM serving endpoint (none configured)")
        btn.click()
        page.wait_for_timeout(300)
        assert page.locator("#autoAssignModal").is_visible()
        assert page.locator("#startAutoAssignBtn").is_visible()

    def test_build_page_has_diagnostics_modal(self, page, live_server):
        _open_mapping(page, live_server, "section=sync")
        assert page.locator("#mappingDiagnosticsModal").count() == 1


class TestVisualListSelectionSync:
    """Selecting an item in Visual highlights it in List, and vice versa."""

    def _render(self, page, live_server):
        _open_mapping(page, live_server, "section=design&tab=visual")
        _load_fake_ontology(page)
        page.evaluate("refreshMappingDesign()")
        page.wait_for_function(
            "() => document.querySelectorAll('.mapping-map-node').length === 2",
            timeout=15000,
        )
        page.evaluate("ManualModule.initialized = true; ManualModule.buildTree()")

    def _map_selected(self, page):
        return page.evaluate(
            """() => d3.selectAll('.mapping-map-node.selected').data()
                     .map(d => d.uri || d.name)"""
        )

    def _list_selected(self, page):
        return page.evaluate(
            """() => Array.from(document.querySelectorAll(
                    '#manualAssignmentTree .manual-tree-node-content.selected'))
                 .map(el => el.closest('.manual-tree-node').dataset.uri)"""
        )

    def test_visual_diagram_is_centered_on_render(self, page, live_server):
        self._render(page, live_server)
        page.wait_for_timeout(600)
        transform = page.evaluate(
            "() => { const t = d3.zoomTransform(mappingMapSvg.node());"
            " return {x: t.x, y: t.y, k: t.k}; }"
        )
        assert transform["k"] <= 1.0
        assert (transform["x"], transform["y"]) != (0, 0)

    def test_visual_click_selects_row_in_list(self, page, live_server):
        self._render(page, live_server)
        page.evaluate(
            """() => {
                const node = d3.selectAll('.mapping-map-node')
                    .filter(d => d.name === 'Order').node();
                node.dispatchEvent(new MouseEvent('click', {bubbles: true}));
            }"""
        )
        page.wait_for_timeout(300)
        assert self._map_selected(page) == ["http://e2e.test/Order"]
        assert self._list_selected(page) == ["http://e2e.test/Order"]

        page.locator("#mappingStudioTabList").click()
        page.wait_for_timeout(500)
        assert self._list_selected(page) == ["http://e2e.test/Order"]

    def test_list_click_selects_node_in_visual(self, page, live_server):
        self._render(page, live_server)
        page.locator("#mappingStudioTabList").click()
        page.wait_for_timeout(500)
        page.evaluate("ManualModule.buildTree()")
        page.locator(
            '#manualAssignmentTree .manual-tree-node.item-node[data-type="entity"]'
            '[data-uri="http://e2e.test/Customer"] > .manual-tree-node-content'
        ).click()
        page.wait_for_timeout(300)
        assert self._list_selected(page) == ["http://e2e.test/Customer"]
        assert self._map_selected(page) == ["http://e2e.test/Customer"]

    def test_select_in_tree_expands_collapsed_parent(self, page, live_server):
        self._render(page, live_server)
        page.locator("#mappingStudioTabList").click()
        page.wait_for_timeout(500)
        page.evaluate(
            """() => {
                ManualModule.buildTree();
                ManualModule.collapseAll();
                ManualModule.selectInTree('entity', 'http://e2e.test/Order', 'Order');
            }"""
        )
        row = page.locator(
            '#manualAssignmentTree .manual-tree-node.item-node[data-uri="http://e2e.test/Order"]'
            " > .manual-tree-node-content"
        )
        assert "selected" in (row.get_attribute("class") or "")
        assert row.is_visible()
