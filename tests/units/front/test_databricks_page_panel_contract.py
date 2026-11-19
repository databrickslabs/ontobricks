"""Sidebar pages render like Databricks pages: one white bordered panel that
holds the title, subtitle and body, with Databricks page typography."""

from pathlib import Path
import re

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
CSS = REPO_ROOT / "src/front/static"
SIDEBAR_CSS = CSS / "global/css/sidebar-layout.css"
MAIN_CSS = CSS / "global/css/main.css"

PADDED_SECTIONS = {
    "global/css/sidebar-layout.css": "#logs-section.active",
    "global/css/mapping.css": "#design-section.active",
    "ontology/css/ontology-map.css": "#map-section.active",
    "query/css/query-sigmagraph.css": "#sigmagraph-section.active",
    "global/css/query.css": "#graphql-section.active",
}


def _block(css: str, selector: str) -> str:
    bodies = re.findall(rf"(?m)^{re.escape(selector)}\s*\{{([^}}]*)\}}", css)
    assert bodies, f"missing rule {selector}"
    return "\n".join(bodies)


def test_active_section_is_a_white_bordered_page_panel():
    css = SIDEBAR_CSS.read_text(encoding="utf-8")
    body = _block(css, ".sidebar-section.active")
    assert "background: var(--db-surface-warm)" in body
    assert "border: 1px solid var(--db-border)" in body
    assert "border-radius: var(--ob-page-panel-radius) 0 0 0" in body
    assert "padding: var(--ob-page-padding)" in body
    layout = _block(css, ".sidebar-layout")
    assert "--ob-page-padding:" in layout
    assert "--ob-page-panel-radius:" in layout


@pytest.mark.parametrize("relative_path,selector", PADDED_SECTIONS.items())
def test_full_height_sections_reuse_the_page_padding(relative_path, selector):
    css = (CSS / relative_path).read_text(encoding="utf-8")
    assert "padding: var(--ob-page-padding)" in _block(css, selector)


def test_section_title_and_subtitle_use_databricks_typography():
    css = MAIN_CSS.read_text(encoding="utf-8")
    title = re.search(
        r"\.section-header h4\s*\{(?P<body>[^}]*)\}", css
    ).group("body")
    assert "font-size: 1.375rem" in title
    assert "font-weight: 600" in title
    subtitle = _block(css, ".section-header p")
    assert "font-size: 0.8125rem" in subtitle
    assert "color: var(--db-text-secondary)" in subtitle


def test_tabs_and_tab_cards_have_no_global_borders():
    """Tabs and their enclosing cards inside the page panel have no global borders."""
    components = (CSS / "global/css/components.css").read_text(encoding="utf-8")
    assert "border-bottom: none" in _block(components, ".nav-tabs.ob-tabs")
    assert "border: none" in components and "background: transparent" in components
    main = MAIN_CSS.read_text(encoding="utf-8")
    assert "border-bottom: none" in _block(main, ".nav-tabs")
