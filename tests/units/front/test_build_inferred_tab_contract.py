"""Contract tests for the Build → Inferred tab markup and loader wiring."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]
BUILD_HTML = (ROOT / "src/front/templates/partials/dtwin/_query_build.html").read_text(encoding="utf-8")
DTWIN_HTML = (ROOT / "src/front/templates/dtwin.html").read_text(encoding="utf-8")
JS = (ROOT / "src/front/static/query/js/query-build-inferred.js").read_text(encoding="utf-8")
SYNC_JS = (ROOT / "src/front/static/query/js/query-sync.js").read_text(encoding="utf-8")


def _pane_html() -> str:
    """The Inferred pane, from its opening id to the end of its table."""
    start = BUILD_HTML.index('id="buildPaneInferred"')
    end = BUILD_HTML.index("</table>", start) + len("</table>")
    return BUILD_HTML[start:end]


def test_inferred_tab_is_labelled_overrides():
    start = BUILD_HTML.index('id="buildTabInferredBtn"')
    end = BUILD_HTML.index("</button>", start)
    label = BUILD_HTML[start:end]
    assert label.rstrip().endswith("Overrides")
    assert ">Inferred<" not in label


def test_inferred_tab_follows_runs_tab():
    assert BUILD_HTML.index('id="buildTabInferredBtn"') > BUILD_HTML.index('id="buildTabRunsBtn"')


def test_inferred_pane_follows_runs_pane():
    assert BUILD_HTML.index('id="buildPaneInferred"') > BUILD_HTML.index('id="buildPaneRuns"')


def test_inferred_tab_targets_its_pane():
    assert 'data-bs-target="#buildPaneInferred"' in BUILD_HTML
    assert 'aria-controls="buildPaneInferred"' in BUILD_HTML


@pytest.mark.parametrize(
    "element_id",
    [
        "buildInferredRefreshBtn",
        "buildInferredFilter",
        "buildInferredCount",
        "buildInferredLoading",
        "buildInferredNotice",
        "buildInferredEmpty",
        "buildInferredError",
        "buildInferredErrorMessage",
        "buildInferredTableWrapper",
        "buildInferredBody",
        "buildInferredTotal",
    ],
)
def test_pane_exposes_hooks_used_by_js(element_id):
    assert f'id="{element_id}"' in BUILD_HTML
    assert element_id in JS


def test_pane_has_no_inline_style():
    assert "style=" not in _pane_html()


def test_pane_uses_the_runs_list_style():
    """Same list chrome as the Runs tab: filter bar, count, ob-runs-table."""
    pane = _pane_html()
    assert 'class="ob-filter-bar"' in pane
    assert 'class="ob-filter-search"' in pane
    assert 'class="ob-runs-count"' in pane
    assert "table-responsive" in pane
    assert "table table-hover align-middle ob-runs-table" in pane


def test_js_binds_the_shared_list_filter():
    assert "ObListFilter.bind(" in JS


def test_pane_has_the_four_table_headers():
    pane = _pane_html()
    for header in ("Category", "Triples", "Entities", "Groups"):
        assert f">{header}<" in pane


def test_script_is_loaded_by_dtwin_page():
    assert "query/js/query-build-inferred.js" in DTWIN_HTML


def test_js_calls_breakdown_endpoint_with_credentials():
    assert "/dtwin/reasoning/inferred/breakdown" in JS
    assert "credentials: 'include'" in JS


def test_js_shows_errors_inline_like_runs_not_native_dialogs():
    assert "buildInferredErrorMessage" in JS
    assert "alert(" not in JS
    assert "confirm(" not in JS


def test_js_reloads_on_every_tab_show():
    assert "shown.bs.tab" in JS


def test_js_shows_loading_spinner_state():
    assert "buildInferredLoading" in JS
    assert "spinner-border" in JS
    assert "showOnly('buildInferredLoading')" in JS


def test_build_tab_renders_graph_cache_status_helper_still_in_sync_js():
    assert "function renderBuildGraphCacheStatus" in SYNC_JS
    assert "renderBuildGraphCacheStatus(cache)" in SYNC_JS or "renderBuildGraphCacheStatus(cacheSource)" in SYNC_JS


def test_cache_updated_falls_back_to_last_build_not_never():
    """Missing stamp falls back to last Build; the UI must not print 'Never'."""
    body = SYNC_JS.split("function renderBuildGraphCacheStatus", 1)[1].split(
        "async function initSyncSection", 1
    )[0]
    assert ">Never<" not in body
    assert "'Never'" not in body
    assert '"Never"' not in body
    assert "last_built" in body
    assert "Cache updated" in BUILD_HTML


def test_header_actions_stay_visible_on_every_build_tab():
    """Refresh / Refresh cache / Build must not disappear on Runs or Inferred."""
    runs_js = (ROOT / "src/front/static/domain/js/domain-runs.js").read_text(encoding="utf-8")
    sync_fn = runs_js.split("function _syncBuildPageTab", 1)[1].split("function _showBuildPageTab", 1)[0]
    assert "buildHeaderActions" in sync_fn
    assert "classList.toggle('d-none'" not in sync_fn
    assert "actions.classList.remove('d-none')" in sync_fn
    assert "buildTabInferredBtn" in runs_js
    assert "_normalizeBuildPageTab" in runs_js
