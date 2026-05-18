"""
E2E — Ontology page sidebar navigation.

Verifies that clicking each sidebar link makes the corresponding
``#{section}-section`` ``<div>`` visible.

Sections covered: information, import, wizard, map, design, entities,
relationships, dataquality, swrl, axioms, owl.
"""

import pytest


class TestOntologySidebar:
    """Click each sidebar item and verify the correct section becomes visible."""

    @pytest.mark.parametrize(
        "section",
        [
            "information",
            "import",
            "wizard",
            "map",
            "design",
            "entities",
            "relationships",
            "dataquality",
            "swrl",
            "axioms",
            "owl",
        ],
    )
    def test_sidebar_switches_section(self, page, live_server, section):
        page.goto(f"{live_server}/ontology")
        page.wait_for_load_state("domcontentloaded")
        page.click(f'a[data-section="{section}"]')
        page.wait_for_timeout(400)
        section_div = page.locator(f"#{section}-section")
        assert (
            section_div.is_visible()
        ), f"Section #{section}-section not visible after click"

    def test_wizard_select_all_checkbox_exists(self, page, live_server):
        page.goto(f"{live_server}/ontology")
        page.wait_for_load_state("domcontentloaded")
        page.click('a[data-section="wizard"]')
        page.wait_for_timeout(400)
        cb = page.locator("#wizardSelectAllCheckbox")
        assert cb.count() == 1
