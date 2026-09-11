"""Contracts for Explorer's latest-search timing breakdown."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SIGMA_JS = REPO_ROOT / "src/front/static/query/js/query-sigmagraph.js"
SIGMA_CSS = REPO_ROOT / "src/front/static/query/css/query-sigmagraph.css"


def _js() -> str:
    return SIGMA_JS.read_text(encoding="utf-8")


def test_latest_timing_record_has_all_phases() -> None:
    js = _js()
    assert "var _searchTiming = {" in js
    assert "previewMs: null" in js
    assert "expansionMs: null" in js
    assert "displayMs: null" in js
    assert "totalMs: null" in js
    assert "function _resetSearchTiming()" in js


def test_preview_and_expansion_measure_request_through_parse() -> None:
    js = _js()
    assert "var previewStartedAt = performance.now();" in js
    assert "_searchTiming.previewMs = performance.now() - previewStartedAt;" in js
    assert "var expansionStartedAt = performance.now();" in js
    assert "_searchTiming.expansionMs = performance.now() - expansionStartedAt;" in js


def test_display_and_total_finish_after_render() -> None:
    js = _js()
    expand_start = js.index("async function _expandAndRenderGraph")
    render_call = js.index("_render();", expand_start)
    display_done = js.index(
        "_searchTiming.displayMs = performance.now() - displayStartedAt;",
        render_call,
    )
    total_done = js.index("_searchTiming.totalMs = _searchTimerStop();", display_done)
    assert render_call < display_done < total_done
    assert "_showSearchTiming(_searchTiming);" in js[total_done:]


def test_timer_pauses_before_the_seed_selection_modal_opens() -> None:
    js = _js()
    search_block = js.split(
        "async function _executeGraphSearch()", 1
    )[1].split("async function _expandAndRenderGraph", 1)[0]
    pause = search_block.index("_searchTimerPause();")
    modal_show = search_block.index("modal.show();")
    assert pause < modal_show


def test_timer_resumes_after_the_selection_modal_has_closed() -> None:
    js = _js()
    hide_block = js.split(
        "function _hideSeedPreviewModal()", 1
    )[1].split("// -- Phase 2:", 1)[0]
    hidden_listener = hide_block.index("'hidden.bs.modal'")
    modal_hide = hide_block.index("modal.hide();", hidden_listener)
    assert "{ once: true }" in hide_block[hidden_listener:modal_hide]

    select_block = js.split(
        "async function _expandSelectedSeeds()", 1
    )[1].split("function _clearGraphFilter()", 1)[0]
    modal_closed = select_block.index("await _hideSeedPreviewModal();")
    resume = select_block.index("_searchTimerResume();", modal_closed)
    expansion = select_block.index(
        "await _expandAndRenderGraph(selectedUris);", resume
    )
    assert modal_closed < resume < expansion


def test_starting_search_resets_latest_details() -> None:
    js = _js()
    start = js.index("async function _executeGraphSearch()")
    timer = js.index("_searchTimerStart();", start)
    assert "_resetSearchTiming();" in js[start:timer]
    reset_block = js.split("function _resetSearchTiming()", 1)[1].split(
        "var _visibleTypes", 1
    )[0]
    assert "timingButton.hidden = true;" in reset_block
    assert "el.hidden = false;" in js.split("function _showSearchTiming", 1)[1]


def test_timing_indicator_is_an_accessible_button() -> None:
    js = _js()
    assert "document.createElement('button')" in js
    assert "el.type = 'button';" in js
    assert "el.setAttribute('aria-controls', 'sgSearchTimingDetails');" in js
    assert "el.setAttribute('aria-expanded', 'false');" in js
    assert "function _setSearchTimingExpanded(expanded)" in js


def test_details_support_click_escape_and_outside_dismissal() -> None:
    js = _js()
    assert "el.addEventListener('click'" in js
    assert "event.key === 'Escape'" in js
    assert "container.contains(event.target)" in js


def test_indicator_uses_classes_instead_of_inline_visual_styles() -> None:
    js = _js()
    css = SIGMA_CSS.read_text(encoding="utf-8")
    show_block = js.split("function _showSearchTiming", 1)[1].split(
        "// -----------------------------------------------------------", 1
    )[0]
    assert "style.cssText" not in show_block
    assert "getComputedStyle" not in show_block
    assert ".sg-search-timing" in css
    assert ".sg-search-timing-details" in css


def test_details_render_latest_phase_labels() -> None:
    js = _js()
    for label in ("Preview request", "Expansion request", "Display", "Total"):
        assert label in js
    assert "aria-label" in js
