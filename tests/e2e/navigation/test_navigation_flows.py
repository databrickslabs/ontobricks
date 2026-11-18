"""
E2E — Navigation, Home page, About page.

Covers:
- All six top-level routes load with the correct <title>.
- Navbar brand navigates back to home.
- Settings nav-link works.
- Home page hero, All Domains gateway, New Domain CTA.
- About page content.
"""

import pytest


class TestNavigation:
    """Verify top-level page navigation works."""

    def test_home_loads(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        assert "OntoBricks" in page.title()

    @pytest.mark.needs_domain
    @pytest.mark.parametrize(
        "path,title_fragment",
        [
            ("/settings", "Settings"),
            ("/ontology", "Ontology"),
            # Legacy /mapping redirects into Graph → Management → Mapping.
            ("/mapping", "Graph"),
            ("/domain", "Configuration"),
            ("/dtwin/", "Graph"),
            ("/about", "About"),
        ],
    )
    def test_page_loads(self, page, live_server, path, title_fragment):
        page.goto(f"{live_server}{path}")
        page.wait_for_load_state("domcontentloaded")
        assert title_fragment in page.title()

    def test_navbar_brand_navigates_home(self, page, live_server):
        page.goto(f"{live_server}/settings")
        page.wait_for_load_state("domcontentloaded")
        page.click("a.navbar-brand")
        page.wait_for_load_state("domcontentloaded")
        assert (
            page.url.rstrip("/") == live_server.rstrip("/")
            or page.url == f"{live_server}/"
        )

    def test_settings_link_in_navbar(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        page.click('a.nav-link[href="/settings"]')
        page.wait_for_load_state("domcontentloaded")
        assert "Settings" in page.title()


class TestHomePage:
    def test_hero_visible(self, page, live_server):
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        hero = page.locator(".home-hero")
        assert hero.is_visible()
        assert "OntoBricks" in hero.text_content()

    def test_registry_tabs_visible(self, page, live_server):
        # The KPI band moved to Domain → General and the Settings / About
        # quick links were removed; Home is the Registry (Browse · Bridges ·
        # My tasks).
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        assert page.locator("#homeTabs").is_visible()
        assert page.locator("a.quick-link-sm").count() == 0

    def test_new_domain_cta_present(self, page, live_server):
        # Create lives in the Browse tab of the Registry card.
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")
        assert page.locator("#btnHomeNewDomain").is_visible()


class TestAboutPage:
    def test_page_content(self, page, live_server):
        page.goto(f"{live_server}/about")
        page.wait_for_load_state("domcontentloaded")
        assert "OntoBricks" in page.text_content("body")

    def test_features_listed(self, page, live_server):
        page.goto(f"{live_server}/about")
        page.wait_for_load_state("domcontentloaded")
        assert "R2RML" in page.text_content("body")
