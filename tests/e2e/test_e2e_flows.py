"""
Layer 2 UI Tests -- End-to-End Browser Tests (Playwright)

Critical user flows exercised against a live Uvicorn server.
"""
import pytest


# =====================================================
# NAVIGATION
# =====================================================

class TestNavigation:
    """Verify top-level page navigation works."""

    def test_home_loads(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        assert "OntoBricks" in page.title()

    @pytest.mark.parametrize("path,title_fragment", [
        ("/settings", "Settings"),
        ("/ontology", "Ontology"),
        ("/mapping", "Mapping"),
        ("/domain", "Domain"),
        ("/dtwin/", "Digital Twin"),
        ("/about", "About"),
    ])
    def test_page_loads(self, page, live_server, path, title_fragment):
        page.goto(f"{live_server}{path}")
        page.wait_for_load_state("domcontentloaded")
        assert title_fragment in page.title()

    def test_navbar_brand_navigates_home(self, page, live_server):
        page.goto(f"{live_server}/settings")
        page.wait_for_load_state("domcontentloaded")
        page.click("a.navbar-brand")
        page.wait_for_load_state("domcontentloaded")
        assert page.url.rstrip("/") == live_server.rstrip("/") or page.url == f"{live_server}/"

    def test_settings_link_in_navbar(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        page.click('a.nav-link[href="/settings"]')
        page.wait_for_load_state("domcontentloaded")
        assert "Settings" in page.title()


# =====================================================
# HOME PAGE
# =====================================================

class TestHomePage:
    def test_hero_visible(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        hero = page.locator(".home-hero")
        assert hero.is_visible()
        assert "OntoBricks" in hero.text_content()

    def test_domain_panel_visible(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        assert page.locator("#sessionPanel").is_visible()

    def test_workflow_cards_present(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        cards = page.locator(".workflow-card")
        assert cards.count() == 3

    def test_stat_items_present(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        assert page.locator("#classCount").is_visible()
        assert page.locator("#propCount").is_visible()
        assert page.locator("#mappingCount").is_visible()


# =====================================================
# SETTINGS PAGE
# =====================================================

class TestSettingsPage:
    def test_databricks_tab_visible(self, page, live_server):
        page.goto(f"{live_server}/settings")
        page.wait_for_load_state("domcontentloaded")
        assert page.locator("#pane-databricks").is_visible()

    def test_host_display(self, page, live_server):
        page.goto(f"{live_server}/settings")
        page.wait_for_load_state("domcontentloaded")
        assert page.locator("#currentHostDisplay").is_visible()

    def test_base_uri_field(self, page, live_server):
        page.goto(f"{live_server}/settings")
        page.wait_for_load_state("domcontentloaded")
        page.click("#tab-global")
        page.wait_for_timeout(400)
        field = page.locator("#baseUriDefault")
        assert field.is_visible()
        assert field.input_value() != ""

    def test_save_button_clickable(self, page, live_server):
        page.goto(f"{live_server}/settings")
        page.wait_for_load_state("domcontentloaded")
        btn = page.locator("#btnSaveAllSettings")
        assert btn.is_visible()
        assert btn.is_enabled()


# =====================================================
# ONTOLOGY PAGE -- SIDEBAR NAVIGATION
# =====================================================

class TestOntologySidebar:
    """Click each sidebar item and verify the correct section becomes visible."""

    @pytest.mark.parametrize("section", [
        "information", "import", "wizard", "map", "design",
        "entities", "relationships", "dataquality", "swrl", "axioms", "owl"
    ])
    def test_sidebar_switches_section(self, page, live_server, section):
        page.goto(f"{live_server}/ontology")
        page.wait_for_load_state("domcontentloaded")
        page.click(f'a[data-section="{section}"]')
        page.wait_for_timeout(400)
        section_div = page.locator(f"#{section}-section")
        assert section_div.is_visible(), f"Section #{section}-section not visible after click"

    def test_wizard_select_all_checkbox_exists(self, page, live_server):
        page.goto(f"{live_server}/ontology")
        page.wait_for_load_state("domcontentloaded")
        page.click('a[data-section="wizard"]')
        page.wait_for_timeout(400)
        cb = page.locator("#wizardSelectAllCheckbox")
        assert cb.count() == 1


# =====================================================
# MAPPING PAGE -- SIDEBAR NAVIGATION
# =====================================================

class TestMappingSidebar:
    @pytest.mark.parametrize("section", [
        "information", "design", "manual", "autoassign", "r2rml", "sparksql"
    ])
    def test_sidebar_switches_section(self, page, live_server, section):
        page.goto(f"{live_server}/mapping")
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(500)
        page.evaluate(f'SidebarNav.switchTo("{section}")')
        page.wait_for_timeout(400)
        section_div = page.locator(f"#{section}-section")
        assert section_div.is_visible(), f"Section #{section}-section not visible after click"


# =====================================================
# DOMAIN PAGE -- SIDEBAR NAVIGATION
# =====================================================

class TestDomainSidebar:
    @pytest.mark.parametrize("section", [
        "information", "metadata", "documents", "validation",
        "owl-content", "r2rml"
    ])
    def test_sidebar_switches_section(self, page, live_server, section):
        page.goto(f"{live_server}/domain")
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(500)
        # Some links get sidebar-disabled (pointer-events:none) when no
        # domain is saved; bypass CSS by using JS to switch directly.
        page.evaluate(f'SidebarNav.switchTo("{section}")')
        page.wait_for_timeout(400)
        section_div = page.locator(f"#{section}-section")
        assert section_div.is_visible(), f"Section #{section}-section not visible after click"


# =====================================================
# DIGITAL TWIN PAGE
# =====================================================

class TestDigitalTwinSidebar:
    def test_sigmagraph_section_visible_by_default(self, page, live_server):
        page.goto(f"{live_server}/dtwin/")
        page.wait_for_load_state("domcontentloaded")
        assert page.locator("#sigmagraph-section").is_visible()

    def test_sidebar_knowledge_graph_link(self, page, live_server):
        page.goto(f"{live_server}/dtwin/")
        page.wait_for_load_state("domcontentloaded")
        link = page.locator('a[data-section="sigmagraph"]')
        assert link.is_visible()
        assert "knowledge" in (link.text_content() or "").lower() or "graph" in (link.text_content() or "").lower()


# =====================================================
# ABOUT PAGE
# =====================================================

class TestAboutPage:
    def test_page_content(self, page, live_server):
        page.goto(f"{live_server}/about")
        page.wait_for_load_state("domcontentloaded")
        assert "OntoBricks" in page.text_content("body")

    def test_features_listed(self, page, live_server):
        page.goto(f"{live_server}/about")
        page.wait_for_load_state("domcontentloaded")
        assert "R2RML" in page.text_content("body")
