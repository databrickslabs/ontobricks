"""Mapping Studio hosts Visual (canvas) and List (former Manual) tabs."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
MENU = REPO_ROOT / "src/front/config/menu_config.json"
DESIGN = REPO_ROOT / "src/front/templates/partials/mapping/_mapping_design.html"
MANUAL = REPO_ROOT / "src/front/templates/partials/mapping/_mapping_manual.html"
DTWIN = REPO_ROOT / "src/front/templates/dtwin.html"
INIT_JS = REPO_ROOT / "src/front/static/mapping/js/mapping-init.js"
INIT_SECTION = REPO_ROOT / "src/front/static/mapping/js/mapping-init-section.js"
SIDEBAR_JS = REPO_ROOT / "src/front/static/global/js/sidebar-nav.js"
MANUAL_JS = REPO_ROOT / "src/front/static/mapping/js/mapping-manual.js"
DESIGN_JS = REPO_ROOT / "src/front/static/mapping/js/mapping-design.js"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_menu_has_studio_but_no_manual_sidebar_item():
    menu = _read(MENU)
    management = menu[
        menu.index('"id": "digitaltwin-management"') : menu.index('"id": "digitaltwin-data"')
    ]
    assert '"id": "design"' in management
    assert '"id": "manual"' not in menu
    assert "Manual" not in management


def test_dtwin_has_no_manual_section():
    html = _read(DTWIN)
    assert 'id="manual-section"' not in html
    assert "_mapping_design.html" in html


def test_studio_has_visual_and_list_tabs():
    html = _read(DESIGN)
    assert 'id="mappingStudioTabs"' in html
    assert 'id="mappingStudioTabInformations"' in html
    assert 'id="mappingStudioTabVisual"' in html
    assert 'id="mappingStudioTabList"' in html
    assert 'id="mappingStudioPaneInformations"' in html
    assert 'id="mappingStudioPaneVisual"' in html
    assert 'id="mappingStudioPaneList"' in html
    assert "_mapping_information_content.html" in html
    assert "_mapping_manual.html" in html
    assert 'id="mapping-map-container"' in html
    assert "ob-tabs" in html
    assert html.index("mappingStudioTabInformations") < html.index("mappingStudioTabVisual")


def test_studio_has_import_and_export_buttons():
    html = _read(DESIGN)
    assert 'id="mappingImportR2RMLBtn"' in html
    assert 'id="mappingExportR2RMLBtn"' in html
    assert 'id="mappingStudioImportR2rmlFileInput"' in html


def test_studio_clean_embedding_no_round_corners_inside_tab():
    css = (REPO_ROOT / "src/front/static/mapping/css/mapping-design.css").read_text(encoding="utf-8")
    assert "#design-section #mappingStudioMainPane" in css
    assert "border-radius: 0" in css
    assert "#design-section .mapping-right-panel" in css


def test_visual_and_list_share_the_studio_bottom_pane():
    html = _read(DESIGN)
    visual = html[html.index('id="mappingStudioPaneVisual"') : html.index('id="mappingStudioPaneList"')]
    assert 'id="mappingRightPanel"' not in visual
    assert html.index("mappingStudioTabContent") < html.index("mappingRightPanel")
    assert 'id="mappingDesignerContainer"' in html
    list_html = _read(MANUAL)
    assert 'id="manualBottomPanel"' not in list_html
    assert 'id="manualPanelBody"' not in list_html
    assert 'id="manualAssignmentTree"' in list_html


def test_manual_partial_is_list_body_not_standalone_page():
    html = _read(MANUAL)
    assert 'id="mappingStudioListRoot"' in html
    assert 'id="manualAssignmentTree"' in html
    assert "Manual Mapping" not in html
    assert 'id="openAutoAssignModalManualBtn"' not in html


def test_legacy_manual_section_opens_studio_list_tab():
    js = _read(INIT_SECTION)
    assert "=== 'manual'" in js
    assert "__OB_MAPPING_STUDIO_TAB" in js
    assert "'list'" in js
    init = _read(INIT_JS)
    assert "__OB_MAPPING_STUDIO_TAB" in init
    assert "ManualModule" in init


def test_studio_tab_url_owned_by_design_section():
    js = _read(SIDEBAR_JS)
    start = js.index("_TAB_OWNERS:")
    owners = js[start : start + 280]
    assert "design:" in owners or "design :" in owners
    assert "visual" in owners
    assert "list" in owners


def test_list_opens_the_shared_studio_panel():
    js = _read(MANUAL_JS)
    open_panel = js[js.index("openPanel: function") : js.index("initBootstrapTabs")]
    assert "openEntityMappingFromDesign" in open_panel
    assert "openRelationshipMappingFromDesign" in open_panel
    assert "manualPanelBody" not in open_panel


def test_design_js_syncs_studio_tabs():
    js = _read(DESIGN_JS)
    assert "mappingStudioTab" in js or "MappingStudioTabs" in js or "studioTab" in js


def test_visual_centers_diagram_by_default():
    js = _read(DESIGN_JS)
    assert "function centerMappingDiagram(" in js
    assert "window.centerMappingDiagram = centerMappingDiagram" in js
    assert "centerMappingDiagram(false)" in js
    assert "centerMappingDiagram(true)" in js
    # Reset view centers diagram
    assert "mappingMapResetView" in js
