"""Graph Explorer contracts for entity SHACL facets (SPEC §3.5)."""

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
FRONT = REPO_ROOT / "src/front"
SHACL_JS = FRONT / "static/query/js/query-shacl-facets.js"
SHACL_CSS = FRONT / "static/query/css/query-shacl-facets.css"
MODAL_HTML = FRONT / "templates/partials/dtwin/_query_shacl_facets_modal.html"
SIGMA_HTML = FRONT / "templates/partials/dtwin/_query_sigmagraph.html"
SIGMA_JS = FRONT / "static/query/js/query-sigmagraph.js"
LOADERS_JS = FRONT / "static/query/js/query-loaders.js"
DTWIN_HTML = FRONT / "templates/dtwin.html"

pytestmark = pytest.mark.unit


def _read(path: Path) -> str:
    if not path.exists():
        pytest.fail(f"{path.relative_to(REPO_ROOT)} does not exist yet")
    return path.read_text(encoding="utf-8")


def _function_body(js: str, name: str) -> str:
    """Source of ``function <name>`` up to the next sibling function."""
    start = js.find(f"function {name}(")
    assert start != -1, f"function {name} not found"
    nxt = re.search(r"\n    function \w+\(", js[start + 1:])
    return js[start: start + 1 + nxt.start()] if nxt else js[start:]


# ---------------------------------------------------------------------------
# Assets and module surface (§3.5.1)
# ---------------------------------------------------------------------------


def test_explorer_loads_the_module_and_its_styles():
    html = _read(DTWIN_HTML)
    assert "query/js/query-shacl-facets.js" in html
    assert "query/css/query-shacl-facets.css" in html


def test_module_exports_its_three_entry_points():
    js = _read(SHACL_JS)
    assert "window.openShaclFacetsModal = openShaclFacetsModal" in js
    assert "window.renderShaclFacetSection = renderShaclFacetSection" in js
    assert "window.getShaclFacetStatus = getShaclFacetStatus" in js


def test_module_calls_the_internal_endpoint():
    js = _read(SHACL_JS)
    assert "/dtwin/nodes/shacl/start" in js
    assert "JSON.stringify({ entity_uri: entityUri })" in js
    assert "/tasks/" in js
    assert "log_entries" in js
    assert "TaskProgressUI.renderActivityLog" in js


def test_loading_state_shows_a_scrollable_activity_log():
    html = _read(MODAL_HTML)
    assert 'id="shaclFacetsProgressLog"' in html
    assert "task-activity-log" in html
    css = _read(SHACL_CSS)
    assert "#shaclFacetsProgressLog" in css
    assert "overflow-y: auto" in css
    assert "max-height: 9.5rem" in css
    js = _read(SHACL_JS)
    assert "_shaclRenderProgressLog" in js
    assert "tick()" in js


def test_completed_checks_are_broadcast_not_pushed():
    """Panel and canvas listen; the module never calls them directly."""
    js = _read(SHACL_JS)
    assert "new CustomEvent('shacl-facets:updated'" in js
    sigma = _read(SIGMA_JS)
    assert "'shacl-facets:updated'" in sigma


def test_rendered_text_is_escaped_and_failures_are_notified():
    js = _read(SHACL_JS)
    assert "escapeHtml" in js
    assert "showNotification(" in js
    assert "alert(" not in js


def test_styles_use_the_shared_status_tokens_without_hard_coded_colours():
    css = _read(SHACL_CSS)
    for status in ("success", "warning", "danger", "info", "draft"):
        assert f"--db-status-{status}" in css
    assert not re.search(r"#[0-9a-fA-F]{3,6}\b", css)


# ---------------------------------------------------------------------------
# Context menu (§3.5.2)
# ---------------------------------------------------------------------------


def test_context_menu_item_is_gated_on_the_shape_count():
    body = _function_body(_read(SIGMA_JS), "_showNodeContextMenu")
    assert "shaclShapeCount" in body
    assert 'data-sg-node-action="shacl-facets"' in body
    assert "Data Quality Checks (" in body
    assert "SHACL facets" not in body
    assert "'Data quality'" in body or ">Data quality<" in body


def test_context_menu_item_opens_the_popup():
    js = _read(SIGMA_JS)
    assert "action === 'shacl-facets'" in js
    assert "openShaclFacetsModal(" in js


def test_loader_attaches_a_shape_count_to_each_class():
    js = _read(LOADERS_JS)
    assert "/ontology/dataquality/list" in js
    assert "shaclShapeCount" in js


def test_client_count_follows_the_server_selection_rule():
    """Own class + ancestors (via ``parent``) + global shapes, enabled only."""
    js = _read(SHACL_JS)
    assert "function shaclShapeCountFor(" in js
    body = js[js.find("function shaclShapeCountFor("):]
    assert ".parent" in body
    assert "enabled !== false" in body
    assert "target_class_uri" in body


# ---------------------------------------------------------------------------
# Popup (§3.5.3)
# ---------------------------------------------------------------------------


def test_modal_partial_is_included_exactly_once():
    html = _read(SIGMA_HTML)
    assert html.count('include "partials/dtwin/_query_shacl_facets_modal.html"') == 1


def test_modal_follows_the_canonical_bootstrap_pattern():
    html = _read(MODAL_HTML)
    assert 'id="shaclFacetsModal"' in html
    for cls in ("modal-lg", "modal-dialog-scrollable", "modal-fullscreen-md-down"):
        assert cls in html
    assert 'data-bs-dismiss="modal"' in html
    assert 'style="' not in html


def test_modal_is_opened_idempotently():
    js = _read(SHACL_JS)
    assert "bootstrap.Modal.getOrCreateInstance(" in js
    assert "new bootstrap.Modal(" not in js


def test_popup_is_titled_data_quality_checks():
    html = _read(MODAL_HTML)
    assert "Data Quality Checks" in html
    assert "SHACL facets" not in html
    js = _read(SHACL_JS)
    assert "Checking SHACL facets" not in js


def test_popup_shows_one_gauge_per_dimension():
    """Score = passed / (passed + failed); unchecked shapes do not count."""
    assert 'id="shaclFacetsGauges"' in _read(MODAL_HTML)
    js = _read(SHACL_JS)
    body = js[js.find("function _shaclGaugesHtml("):]
    assert "function _shaclGaugesHtml(" in js
    assert "data-shacl-gauge" in body
    assert "<svg" in body and 'aria-hidden="true"' in body
    assert "passed" in body


def test_gauge_colours_come_from_the_status_tokens():
    css = _read(SHACL_CSS)
    assert ".shacl-gauge" in css
    assert not re.search(r"#[0-9a-fA-F]{3,6}\b", css)


def test_dimension_sections_start_collapsed():
    js = _read(SHACL_JS)
    body = js[js.find("function _shaclCategoriesHtml("):js.find("function _shaclFacetRowHtml(")]
    assert 'accordion-button collapsed' in body
    assert 'aria-expanded="false"' in body
    assert "collapse show" not in body


def test_modal_has_recheck_and_data_quality_link():
    html = _read(MODAL_HTML)
    assert 'id="shaclFacetsRecheck"' in html
    assert "Open Data Quality" in html


@pytest.mark.parametrize(
    "status,icon,label",
    [
        ("fail", "bi-x-circle-fill", "failed"),
        ("pass", "bi-check-circle-fill", "passed"),
        ("not_applicable", "bi-dash-circle", "not applicable"),
        ("not_evaluated", "bi-question-circle", "not evaluated"),
        ("error", "bi-exclamation-triangle-fill", "error"),
    ],
)
def test_every_status_has_an_icon_and_a_label(status, icon, label):
    """Status must never be carried by colour alone (.cursor/11)."""
    js = _read(SHACL_JS)
    assert status in js
    assert icon in js
    assert label in js


def test_status_label_is_announced_to_screen_readers():
    assert "visually-hidden" in _read(SHACL_JS)


def test_chips_filter_the_facet_list():
    js = _read(SHACL_JS)
    assert "data-shacl-filter" in js


def test_facet_row_states_inheritance_and_observed_values():
    js = _read(SHACL_JS)
    assert "inherited from" in js
    assert "(none)" in js
    assert "more" in js


def test_categories_reuse_the_data_quality_tab_icons():
    js = _read(SHACL_JS)
    for icon in ("bi-check2-all", "bi-123", "bi-fingerprint",
                 "bi-link-45deg", "bi-regex", "bi-diagram-3"):
        assert icon in js


def test_empty_state_names_the_class():
    assert "No SHACL shape applies to" in _read(SHACL_JS)


def test_results_are_cached_per_entity_and_rechecked_explicitly():
    js = _read(SHACL_JS)
    assert "_shaclCache" in js
    assert "checkedAt" in js


# ---------------------------------------------------------------------------
# Detail panel (§3.5.4)
# ---------------------------------------------------------------------------


def test_detail_panel_renders_the_data_quality_section():
    js = _read(SIGMA_JS)
    assert "renderShaclFacetSection(entity.id" in js


def test_panel_section_offers_check_then_details_and_recheck():
    js = _read(SHACL_JS)
    body = js[js.find("function renderShaclFacetSection("):]
    assert "SHACL shapes apply" in body
    assert "Check" in body
    assert "Details" in body
    assert "Re-check" in body


# ---------------------------------------------------------------------------
# Canvas ring (§3.5.5)
# ---------------------------------------------------------------------------


def test_quality_toggle_lives_in_canvas_help_modal():
    html = _read(SIGMA_HTML)
    modal = _read(FRONT / "templates/partials/dtwin/_query_sigmagraph_canvas_help_modal.html")
    assert 'id="sgShowQualityRings"' not in html.split("visualization-layout", 1)[0]
    assert 'id="sgShowQualityRings"' in modal
    assert 'data-sg-change="toggleQualityRings"' in modal


def test_ring_is_drawn_before_highlights_so_they_win():
    body = _function_body(_read(SIGMA_JS), "_nodeReducer")
    assert "getShaclFacetStatus(" in body
    assert body.index("getShaclFacetStatus(") < body.index("_highlightedSeeds")
    assert body.index("getShaclFacetStatus(") < body.index("_pathHighlightNodes")


def test_ring_colours_come_from_the_status_tokens():
    js = _read(SIGMA_JS) + _read(SHACL_JS)
    assert "getComputedStyle" in js
    assert "--db-status-danger" in js


def test_node_status_follows_the_ring_table():
    """Violation → danger; Warning/Info or error → warning; all clear → success;
    anything unchecked without a failure → no ring."""
    js = _read(SHACL_JS)
    body = js[js.find("function getShaclFacetStatus("):]
    for token in ("'danger'", "'warning'", "'success'", "sh:Violation", "not_evaluated"):
        assert token in body
