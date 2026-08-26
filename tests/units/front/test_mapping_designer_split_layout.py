"""Contract: Mapping Designer uses Ontology-style split cards in column mode."""

from pathlib import Path
import re

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
MAPPING_DESIGN_TEMPLATE = (
    REPO_ROOT / "src/front/templates/partials/mapping/_mapping_design.html"
)
MAPPING_PAGE_TEMPLATE = REPO_ROOT / "src/front/templates/mapping.html"
SHARED_PANELS_CSS = REPO_ROOT / "src/front/static/ontology/css/ontology-shared-panels.css"
MAPPING_CSS = REPO_ROOT / "src/front/static/global/css/mapping.css"
MAPPING_DESIGN_CSS = REPO_ROOT / "src/front/static/mapping/css/mapping-design.css"
MAPPING_DESIGN_JS = REPO_ROOT / "src/front/static/mapping/js/mapping-design.js"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _block(css: str, selector: str) -> str:
    stripped = re.sub(r"/\*.*?\*/", "", css, flags=re.DOTALL)
    for selectors, declarations in re.findall(
        r"([^{}]+)\{([^{}]*)\}", stripped, flags=re.DOTALL
    ):
        if any(part.strip() == selector for part in selectors.split(",")):
            return declarations
    raise AssertionError(f"Missing rule for {selector}")


def _declaration(block: str, prop: str) -> str | None:
    match = re.search(rf"(?<![a-z-]){re.escape(prop)}\s*:\s*([^;]+);", block)
    return match.group(1).strip() if match else None


def test_mapping_designer_template_uses_column_split_shell():
    html = _read(MAPPING_DESIGN_TEMPLATE)

    assert re.search(
        r'<div[^>]*id="mappingDesignerContainer"[^>]*class="[^"]*\bob-split-shell\b[^"]*\bob-split-shell--column\b|'
        r'<div[^>]*class="[^"]*\bob-split-shell\b[^"]*\bob-split-shell--column\b[^"]*"[^>]*id="mappingDesignerContainer"',
        html,
    )
    assert re.search(
        r'<div[^>]*id="mappingMapCard"[^>]*class="[^"]*\bob-split-pane\b|'
        r'<div[^>]*class="[^"]*\bob-split-pane\b[^"]*"[^>]*id="mappingMapCard"',
        html,
    )
    assert re.search(
        r'class="detail-panel-resize-handle"[^>]*>\s*<div class="resize-handle-bar"></div>\s*</div>',
        html,
        flags=re.DOTALL,
    )
    assert re.search(
        r'<div[^>]*id="mappingRightPanel"[^>]*class="[^"]*\bmapping-right-panel\b[^"]*\bob-split-pane\b|'
        r'<div[^>]*class="[^"]*\bmapping-right-panel\b[^"]*\bob-split-pane\b[^"]*"[^>]*id="mappingRightPanel"',
        html,
    )


def test_mapping_designer_canvas_wrapper_drops_shadow_sm():
    html = _read(MAPPING_DESIGN_TEMPLATE)
    map_card_open = re.search(r'<div[^>]*id="mappingMapCard"[^>]*>', html)
    assert map_card_open, "mappingMapCard wrapper is missing"
    assert "shadow-sm" not in map_card_open.group(0)


def test_mapping_designer_template_ships_placeholder():
    html = _read(MAPPING_DESIGN_TEMPLATE)
    assert re.search(
        r'id="panelBody"[\s\S]*class="panel-placeholder"',
        html,
    )


def test_mapping_designer_ships_active_dot_grid_toggle():
    html = _read(MAPPING_DESIGN_TEMPLATE)

    toggle = re.search(r'<button[^>]*id="mappingMapToggleGrid"[^>]*>', html)
    assert toggle, "Mapping Designer grid toggle is missing"
    assert "active" in toggle.group(0)
    assert 'aria-pressed="true"' in toggle.group(0)
    assert 'title="Hide dot grid"' in toggle.group(0)
    assert 'bi-grid-3x3-gap' in html
    assert re.search(
        r'id="mapping-map-container"[^>]*class="[^"]*\bmapping-grid-visible\b|'
        r'class="[^"]*\bmapping-grid-visible\b[^"]*"[^>]*id="mapping-map-container"',
        html,
    )


def test_mapping_designer_dot_grid_matches_ontology_grid():
    css = _read(MAPPING_DESIGN_CSS)
    grid = _block(css, "#mapping-map-container.mapping-grid-visible")

    assert "radial-gradient" in grid
    assert "var(--db-text)" in grid
    assert _declaration(grid, "background-size") == "24px 24px"


def test_mapping_designer_grid_toggle_is_persisted_and_initialised():
    js = _read(MAPPING_DESIGN_JS)

    assert "mappingMapGridVisible" in js
    assert "function isMappingMapGridVisible()" in js
    assert "function applyMappingMapGridVisibility()" in js
    assert "function initMappingMapGridToggle()" in js
    assert "initMappingMapGridToggle();" in js
    assert "mappingMapToggleGrid" in js
    assert "mapping-grid-visible" in js
    assert "aria-pressed" in js


def test_mapping_page_links_shared_split_panel_css():
    html = _read(MAPPING_PAGE_TEMPLATE)
    assert (
        "static', filename='ontology/css/ontology-shared-panels.css'"
        in html
    )


def test_shared_css_defines_column_split_orientation_and_handle():
    css = _read(SHARED_PANELS_CSS)
    shell = _block(css, ".ob-split-shell--column")
    assert _declaration(shell, "display") == "flex"
    assert _declaration(shell, "flex-direction") == "column"
    assert _declaration(shell, "gap") == "0.5rem"

    handle = _block(css, ".ob-split-shell--column > .detail-panel-resize-handle")
    assert _declaration(handle, "width") == "100%"
    assert _declaration(handle, "height") == "8px"
    assert _declaration(handle, "cursor") == "row-resize"
    assert _declaration(handle, "order") == "0"

    bar = _block(css, ".ob-split-shell--column > .detail-panel-resize-handle .resize-handle-bar")
    assert _declaration(bar, "width") == "40px"
    assert _declaration(bar, "height") == "3px"


def test_mapping_designer_panel_css_uses_fixed_open_height_not_zero():
    css = _read(MAPPING_CSS)

    panel = _block(css, ".mapping-designer-container .mapping-right-panel")
    assert _declaration(panel, "height") == "320px"
    assert _declaration(panel, "min-height") == "200px"
    assert _declaration(panel, "max-height") == "50%"
    assert _declaration(panel, "width") == "100%"

    assert "height: 0;" not in panel
    assert ".mapping-designer-container.panel-open .mapping-right-panel" not in css
    assert ".mapping-designer-container.panel-open #mappingMapCard" not in css


def test_manual_mapping_bottom_panel_rules_remain_present():
    css = _read(MAPPING_CSS)
    assert ".manual-bottom-panel" in css


def test_open_mapping_panel_does_not_toggle_panel_open_class():
    js = _read(MAPPING_DESIGN_JS)
    match = re.search(r"function openMappingPanel\([^)]*\)\s*\{([\s\S]*?)\n\}", js)
    assert match, "openMappingPanel() must exist"
    assert "classList.add('panel-open')" not in match.group(1)


def test_close_mapping_panel_does_not_hide_panel_and_restores_placeholder():
    js = _read(MAPPING_DESIGN_JS)
    match = re.search(r"function closeMappingPanel\([^)]*\)\s*\{([\s\S]*?)\n\}", js)
    assert match, "closeMappingPanel() must exist"
    body = match.group(1)
    assert "classList.remove('panel-open')" not in body
    assert "renderMappingPanelPlaceholder()" in body
    assert "panel-placeholder" in js
    assert "releaseMappingPanel()" in body
    assert re.search(
        r"panelTitle\.innerHTML\s*=\s*'<span id=\"panelItemName\">Select Item</span>'",
        body,
    )
    assert "<i " not in body


def test_mapping_panel_resize_persistence_key_and_row_resize_cursor_exist():
    js = _read(MAPPING_DESIGN_JS)
    assert "mappingDesignerPanelHeight" in js
    assert "row-resize" in js


def test_mapping_panel_resizes_only_while_primary_button_is_held():
    js = _read(MAPPING_DESIGN_JS)
    setup = re.search(
        r"function setupMappingDesignerResizeHandle\(\)\s*\{([\s\S]*?)\n\}",
        js,
    )
    assert setup, "setupMappingDesignerResizeHandle() must exist"
    body = setup.group(1)

    assert "event.button !== 0" in body
    assert "event.buttons & 1" in body
    assert "onMouseUp()" in body


def test_canvas_background_click_still_uses_guarded_close_path():
    js = _read(MAPPING_DESIGN_JS)
    assert re.search(
        r"svg\.on\('click',\s*function\(\)\s*\{[\s\S]*guardedCloseMappingPanel\(\)",
        js,
    )


def test_resize_handle_setup_function_exists_and_is_called():
    js = _read(MAPPING_DESIGN_JS)
    assert "function setupMappingDesignerResizeHandle()" in js
    assert "setupMappingDesignerResizeHandle();" in js
