"""SidebarNav must not lock the first URL section or leak Build ``?tab=``."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
SIDEBAR_JS = REPO_ROOT / "src/front/static/global/js/sidebar-nav.js"
MAPPING_INIT = REPO_ROOT / "src/front/static/mapping/js/mapping-init.js"
MAPPING_INIT_SECTION = REPO_ROOT / "src/front/static/mapping/js/mapping-init-section.js"
DTWIN_HTML = REPO_ROOT / "src/front/templates/dtwin.html"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_graph_page_loads_pre_paint_section_style():
    html = _read(DTWIN_HTML)
    assert "mapping-init-section.js" in html


def test_pre_paint_style_uses_important_display_lock():
    js = _read(MAPPING_INIT_SECTION)
    assert "initial-section-style" in js
    assert "display: none !important" in js
    assert "display: flex !important" in js


def test_sidebar_nav_clears_pre_paint_style_on_section_change():
    js = _read(SIDEBAR_JS)
    assert "_clearInitialSectionStyle" in js
    assert "initial-section-style" in js
    show = js[js.index("_showSection: function") : js.index("_activateSection: function")]
    assert "_clearInitialSectionStyle" in show
    click = js[js.index("link.addEventListener('click'") : js.index("_pushSectionState(section)")]
    assert "_clearInitialSectionStyle" in click


def test_sidebar_nav_drops_foreign_tab_query_param():
    js = _read(SIDEBAR_JS)
    assert "_TAB_OWNERS" in js
    owners = js[js.index("_TAB_OWNERS") : js.index("_pushSectionState: function")]
    assert "sync" in owners
    assert "build" in owners
    push = js[js.index("_pushSectionState: function") : js.index("_onPopState: function")]
    assert "searchParams.delete('tab')" in push


def test_mapping_init_clears_pre_paint_style_when_hosted_on_graph():
    js = _read(MAPPING_INIT)
    hosted = js[js.index("if (hostedOnGraph)") :]
    assert "initial-section-style" in hosted or "_clearInitialSectionStyle" in hosted
