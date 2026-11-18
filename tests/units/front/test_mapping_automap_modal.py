"""Batch Auto-Map is a modal opened from Studio / Manual / Build — not a page."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
MENU = REPO_ROOT / "src/front/config/menu_config.json"
DTWIN_HTML = REPO_ROOT / "src/front/templates/dtwin.html"
MODAL = REPO_ROOT / "src/front/templates/partials/mapping/_mapping_autoassign_modal.html"
PAGE = REPO_ROOT / "src/front/templates/partials/mapping/_mapping_autoassign.html"
DESIGN = REPO_ROOT / "src/front/templates/partials/mapping/_mapping_design.html"
MANUAL = REPO_ROOT / "src/front/templates/partials/mapping/_mapping_manual.html"
AUTO_JS = REPO_ROOT / "src/front/static/mapping/js/mapping-autoassign.js"
SYNC_HTML = REPO_ROOT / "src/front/templates/partials/dtwin/_query_sync.html"
DBX_JS = REPO_ROOT / "src/front/static/query/js/query-databricks-build.js"
HELP = REPO_ROOT / "src/front/templates/partials/layout/help_modal.html"


def test_mapping_menu_has_no_automap_page():
    menu = MENU.read_text(encoding="utf-8")
    assert '"id": "assignment"' not in menu
    mapping = menu[menu.index('"id": "digitaltwin-management"') : menu.index('"id": "digitaltwin-data"')]
    assert '"id": "autoassign"' not in mapping
    assert "Auto-Map" not in mapping


def test_graph_page_includes_modal_not_section():
    html = DTWIN_HTML.read_text(encoding="utf-8")
    assert 'id="autoassign-section"' not in html
    assert "_mapping_autoassign_modal.html" in html
    assert PAGE.exists() is False
    assert MODAL.exists()


def test_modal_hosts_batch_automap_controls():
    html = MODAL.read_text(encoding="utf-8")
    assert 'id="autoAssignModal"' in html
    assert 'id="startAutoAssignBtn"' in html
    assert 'id="reassignAttrsBtn"' in html
    assert 'id="autoAssignReportSection"' in html
    assert 'id="autoAssignPreparing"' in html
    assert 'id="autoAssignPreparingText"' in html
    assert "modal-xl" in html
    assert "modal-dialog-scrollable" in html


def test_autoassign_js_guards_early_start_and_single_completion_toast():
    """Start must wait for prepare; completion must not spam duplicate toasts."""
    js = AUTO_JS.read_text(encoding="utf-8")
    assert "_preparing" in js
    assert "_prepareModal" in js
    assert "_setPreparing" in js
    assert "_monitoringTaskId" in js
    assert "Already monitoring task" in js
    assert "Auto-Map is still preparing" in js
    assert "showNotification('Auto-map completed!'" not in js
    assert 'showNotification("Auto-map completed!"' not in js
    assert "Auto-mapped ${successCount} item(s) successfully" in js
    assert "showReport({ notify: true })" in js
    assert "showReport({ notify: false })" in js


def test_studio_header_opens_the_modal():
    design = DESIGN.read_text(encoding="utf-8")
    assert 'id="openAutoAssignModalBtn"' in design
    assert "AutoAssignModule.open" in design or 'js-open-autoassign-modal' in design
    assert "data-requires-llm" in design[design.index("openAutoAssignModalBtn") :]
    # List tab (former Manual) shares the Studio header Auto-Map button.
    assert 'id="openAutoAssignModalManualBtn"' not in MANUAL.read_text(encoding="utf-8")


def test_autoassign_js_exposes_open_and_uses_modal_host():
    js = AUTO_JS.read_text(encoding="utf-8")
    assert "open:" in js or "open =" in js or "open: function" in js
    assert "autoAssignModal" in js
    assert "autoassign-modal-body" in js
    assert "section === 'autoassign'" not in js


def test_autoassign_js_blocks_when_uc_metadata_missing():
    js = AUTO_JS.read_text(encoding="utf-8")
    assert "_showMissingMetadataWarning" in js
    assert "_metadataMissing" in js
    assert "No Unity Catalog metadata loaded" in js
    assert "/domain/?section=metadata" in js


def test_build_ctas_open_automap_modal():
    sync = SYNC_HTML.read_text(encoding="utf-8")
    dbx = DBX_JS.read_text(encoding="utf-8")
    assert "AutoAssignModule.open" in sync
    assert 'href="/mapping/?section=autoassign"' not in sync
    assert "AutoAssignModule.open" in dbx
    assert 'href="/mapping/?section=autoassign"' not in dbx


def test_help_no_longer_points_at_mapping_automap_page():
    help_html = HELP.read_text(encoding="utf-8")
    assert "Mapping &rarr; Auto-Map" not in help_html
    assert "Mapping → Auto-Map" not in help_html


DESIGN_JS = REPO_ROOT / "src/front/static/mapping/js/mapping-design.js"
MANUAL_JS = REPO_ROOT / "src/front/static/mapping/js/mapping-manual.js"
CORE_JS = REPO_ROOT / "src/front/static/global/js/mapping-core.js"


def test_running_automap_streams_items_into_visual_and_list():
    auto = AUTO_JS.read_text(encoding="utf-8")
    assert "applyLiveMappings" in auto
    assert "live_entity_mappings" in auto and "live_relationship_mappings" in auto
    assert "refreshMappingLiveStatus" in auto
    design = DESIGN_JS.read_text(encoding="utf-8")
    assert "function refreshMappingMapStatus" in design
    assert "window.refreshMappingLiveStatus = refreshMappingLiveStatus" in design
    assert "refreshLive: function" in MANUAL_JS.read_text(encoding="utf-8")


def test_mapping_section_stays_reachable_while_automap_runs():
    menu = MENU.read_text(encoding="utf-8")
    assert '"disabled_during": "autoassign"' not in menu
    assert 'id="runInBackgroundBtn"' in MODAL.read_text(encoding="utf-8")
    assert "_setBackgroundBtnVisible(true)" in AUTO_JS.read_text(encoding="utf-8")
    assert "AutoAssignModule.isRunning" in CORE_JS.read_text(encoding="utf-8")
    init_js = (REPO_ROOT / "src/front/static/mapping/js/mapping-init.js").read_text(
        encoding="utf-8"
    )
    assert "section !== 'design' && section !== 'manual'" in init_js
    assert "Please wait until Auto-Map finishes before switching sections." in init_js
