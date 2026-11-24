"""Contract tests for the Build → Cache tab markup and loader wiring."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]
BUILD_HTML = (ROOT / "src/front/templates/partials/dtwin/_query_build.html").read_text(encoding="utf-8")
SYNC_HTML = (ROOT / "src/front/templates/partials/dtwin/_query_sync.html").read_text(encoding="utf-8")
DTWIN_HTML = (ROOT / "src/front/templates/dtwin.html").read_text(encoding="utf-8")
CACHE_JS = (ROOT / "src/front/static/query/js/query-build-cache.js").read_text(encoding="utf-8")
RUNS_JS = (ROOT / "src/front/static/domain/js/domain-runs.js").read_text(encoding="utf-8")


def test_cache_tab_follows_inferred_tab():
    assert BUILD_HTML.index('id="buildTabCacheBtn"') > BUILD_HTML.index('id="buildTabInferredBtn"')


def test_cache_pane_follows_inferred_pane():
    assert BUILD_HTML.index('id="buildPaneCache"') > BUILD_HTML.index('id="buildPaneInferred"')


def test_refresh_cache_buttons_live_in_cache_tab_not_header():
    header = BUILD_HTML.split('id="buildHeaderActions"', 1)[1].split('id="buildPageTabs"', 1)[0]
    assert "syncAdjacencyRefreshBtn" not in header
    assert "dbxAdjacencyRefreshBtn" not in header
    pane = BUILD_HTML.split('id="buildPaneCache"', 1)[1]
    assert 'id="syncAdjacencyRefreshBtn"' in pane
    assert 'id="dbxAdjacencyRefreshBtn"' in pane


def test_build_card_no_longer_embeds_cache_status():
    assert 'id="dtGraphCacheActivated"' not in SYNC_HTML
    assert 'id="dtGraphCacheActivated"' in BUILD_HTML.split('id="buildPaneCache"', 1)[1]


def test_activate_deactivate_link_points_at_context_settings():
    assert 'id="buildCacheSettingsLink"' in BUILD_HTML
    assert 'href="/domain/?section=information#searchCacheSection"' in BUILD_HTML


@pytest.mark.parametrize(
    "element_id",
    [
        "buildCacheReloadBtn",
        "buildCacheLoading",
        "buildCacheNotice",
        "buildCacheError",
        "buildCacheContent",
        "buildCacheCompanionsBody",
        "dtGraphCacheActivated",
        "dtGraphCacheState",
        "dtGraphCacheRefreshed",
    ],
)
def test_cache_pane_exposes_hooks(element_id):
    assert f'id="{element_id}"' in BUILD_HTML


def test_script_is_loaded_and_fetches_stats():
    assert "query/js/query-build-cache.js" in DTWIN_HTML
    assert "/dtwin/sync/stats" in CACHE_JS
    assert "shown.bs.tab" in CACHE_JS


def test_domain_runs_wires_cache_tab():
    assert "buildTabCacheBtn" in RUNS_JS
    assert "'cache'" in RUNS_JS or '"cache"' in RUNS_JS
