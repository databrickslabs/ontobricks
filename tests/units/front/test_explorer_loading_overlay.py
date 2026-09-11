"""Contracts for Explorer's shared blocking canvas loading overlay."""

from pathlib import Path

import pytest


pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
PARTIAL = REPO_ROOT / "src/front/templates/partials/dtwin/_query_sigmagraph.html"
SIGMA_CSS = REPO_ROOT / "src/front/static/query/css/query-sigmagraph.css"
SIGMA_JS = REPO_ROOT / "src/front/static/query/js/query-sigmagraph.js"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_canvas_contains_one_accessible_shared_loading_overlay() -> None:
    html = _read(PARTIAL)
    assert 'id="sgContainer" aria-busy="false"' in html
    assert html.count('id="sgLoading"') == 1
    assert 'class="sg-loading-overlay"' in html
    assert 'id="sgLoadingLabel"' in html
    assert 'class="ob-loading-spinner"' in html
    assert 'class="ob-spinner-svg"' in html
    assert 'aria-live="polite"' in html


def test_obsolete_filter_and_neighbor_indicators_are_removed() -> None:
    html = _read(PARTIAL)
    assert 'id="sgGraphFilterInfo"' not in html
    assert 'id="sgGraphFilterInfoText"' not in html
    assert 'id="sgExpandSpinner"' not in html
    assert 'id="sgExpandSpinnerLabel"' not in html


def test_overlay_css_blocks_the_whole_canvas_with_shared_tokens() -> None:
    css = _read(SIGMA_CSS)
    block = css.split(".sg-loading-overlay", 1)[1].split(".sg-search-timing", 1)[0]
    assert "position: absolute;" in block
    assert "inset: 0;" in block
    assert "display: none;" in block
    assert "pointer-events: auto;" in block
    assert "background: var(--db-surface-warm);" in block
    assert "color-mix" not in block
    assert "transparent" not in block
    assert ".sg-loading-overlay.is-active" in css
    hide_block = css.split('#sgContainer[aria-busy="true"] > :not(#sgLoading)', 1)[1]
    assert "visibility: hidden;" in hide_block.split("}", 1)[0]
    assert ".sg-expand-spinner" not in css


def test_loading_helpers_own_overlay_accessibility_and_search_state() -> None:
    js = _read(SIGMA_JS)
    assert "function _showGraphLoading(label)" in js
    assert "function _setGraphLoadingStep(label)" in js
    assert "function _hideGraphLoading()" in js
    assert "loading.classList.add('is-active');" in js
    assert "loading.classList.remove('is-active');" in js
    assert "container.setAttribute('aria-busy', 'true');" in js
    assert "container.setAttribute('aria-busy', 'false');" in js
    assert "searchButton.disabled = true;" in js
    assert "searchButton.disabled = false;" in js


def test_phase_updates_yield_a_browser_paint_before_graph_work() -> None:
    js = _read(SIGMA_JS)
    assert "function _waitForGraphLoadingPaint()" in js
    assert "window.requestAnimationFrame" in js

    expand = js.split("async function _expandAndRenderGraph", 1)[1].split(
        "function _hideSeedPreviewModal", 1
    )[0]
    render_step = expand.index("_setGraphLoadingStep('Rendering graph…');")
    paint = expand.index("await _waitForGraphLoadingPaint();", render_step)
    render = expand.index("_render();", paint)
    assert render_step < paint < render


def test_filter_search_uses_preview_expand_and_render_steps() -> None:
    js = _read(SIGMA_JS)
    search = js.split("async function _executeGraphSearch()", 1)[1].split(
        "async function _expandAndRenderGraph", 1
    )[0]
    expand = js.split("async function _expandAndRenderGraph", 1)[1].split(
        "function _hideSeedPreviewModal", 1
    )[0]
    assert "_showGraphLoading('Searching…');" in search
    assert "_setGraphLoadingStep('1 entity found — expanding…');" in search
    assert "_hideGraphLoading();" in search[: search.index("modal.show();")]
    assert "'Expanding ' + uris.length + ' entities…'" in expand
    assert "_setGraphLoadingStep('Rendering graph…');" in expand


def test_render_reload_focus_and_inferred_refresh_use_overlay() -> None:
    js = _read(SIGMA_JS)
    render = js.split("function _render(", 1)[1].split(
        "// -----------------------------------------------------------", 1
    )[0]
    public_api = js.split("return {", 1)[1]
    assert "_setGraphLoadingStep('Rendering graph…');" in render
    assert "_hideGraphLoading();" in render
    assert "reload: async function ()" in public_api
    assert "_showGraphLoading('Loading graph data…');" in public_api
    assert "_showGraphLoading('Loading graph data…');" in js.split(
        "function init(focusUri)", 1
    )[1].split("function _hasData", 1)[0]
    assert "_showGraphLoading('Loading graph data…');" in public_api.split(
        "toggleInferred: async function", 1
    )[1].split("expandHop: async function", 1)[0]


def test_neighbor_expansion_uses_overlay_and_error_notification() -> None:
    js = _read(SIGMA_JS)
    hop = js.split("expandHop: async function", 1)[1].split(
        "// --- Group expand / collapse ---", 1
    )[0]
    assert "'Expanding neighbours (' + depth + ' hop)…'" in hop
    assert "_setGraphLoadingStep('Rendering graph…');" in hop
    assert "finally {" in hop
    assert "_hideGraphLoading();" in hop.split("finally {", 1)[1]
    assert "'danger'" not in hop
    assert "'error'" in hop
    assert "sgExpandSpinner" not in hop
    assert "sgGraphFilterInfo" not in hop


def test_refresh_and_inferred_failures_release_the_overlay() -> None:
    js = _read(SIGMA_JS)
    refresh = js.split("refreshCurrentExpansion: async function", 1)[1].split(
        "selectEntity: function", 1
    )[0]
    inferred = js.split("toggleInferred: async function", 1)[1].split(
        "expandHop: async function", 1
    )[0]
    assert "_hideGraphLoading();" in refresh
    assert "showNotification" in refresh
    assert "'error'" in refresh
    assert "catch (error)" in inferred
    assert "showNotification" in inferred
    assert "finally {" in inferred
    assert "_hideGraphLoading();" in inferred


def test_filter_status_dom_is_not_referenced() -> None:
    js = _read(SIGMA_JS)
    assert "sgGraphFilterInfo" not in js
    assert "sgGraphFilterInfoText" not in js
