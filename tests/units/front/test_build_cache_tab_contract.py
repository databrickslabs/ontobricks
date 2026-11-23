"""Contract tests for the Certified Graph → Cache tab markup and loader wiring."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]
BUILD_HTML = (ROOT / "src/front/templates/partials/dtwin/_query_certified.html").read_text(encoding="utf-8")
SYNC_HTML = (ROOT / "src/front/templates/partials/dtwin/_query_sync.html").read_text(encoding="utf-8")
DTWIN_HTML = (ROOT / "src/front/templates/dtwin.html").read_text(encoding="utf-8")
CACHE_JS = (ROOT / "src/front/static/query/js/query-build-cache.js").read_text(encoding="utf-8")
SYNC_JS = (ROOT / "src/front/static/query/js/query-sync.js").read_text(encoding="utf-8")


def test_cache_tab_follows_runs_tab():
    assert BUILD_HTML.index('id="certTabCacheBtn"') > BUILD_HTML.index('id="certTabRunsBtn"')
    assert BUILD_HTML.index('id="certPaneCache"') > BUILD_HTML.index('id="certPaneRuns"')


def test_refresh_cache_buttons_live_in_cache_tab_not_header():
    header = BUILD_HTML.split("section-header", 1)[1].split('id="certPageTabs"', 1)[0]
    assert "syncAdjacencyRefreshBtn" not in header
    assert "dbxAdjacencyRefreshBtn" not in header
    pane = BUILD_HTML.split('id="certPaneCache"', 1)[1]
    assert 'id="syncAdjacencyRefreshBtn"' in pane
    assert 'id="dbxAdjacencyRefreshBtn"' in pane


def test_build_card_no_longer_embeds_cache_status():
    assert 'id="dtGraphCacheActivated"' not in SYNC_HTML
    assert 'id="dtGraphCacheActivated"' in BUILD_HTML.split('id="certPaneCache"', 1)[1]


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
    assert "/dtwin/sync/stats?simulation_id=certified" in CACHE_JS
    assert "shown.bs.tab" in CACHE_JS


def test_the_loader_binds_the_certified_cache_tab():
    assert "el('certTabCacheBtn')" in CACHE_JS


def test_cache_status_helper_stays_in_sync_js():
    assert "function renderBuildGraphCacheStatus" in SYNC_JS
    assert "renderBuildGraphCacheStatus(cacheSource)" in SYNC_JS


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

