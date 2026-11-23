"""Contract tests for the Build page Cache tab: one section per graph (Original, Certified)."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]
DTWIN = ROOT / "src/front/templates/partials/dtwin"
ORIGINAL_HTML = (DTWIN / "_query_build.html").read_text(encoding="utf-8")
PANE_HTML = (DTWIN / "_graph_cache_pane.html").read_text(encoding="utf-8")
SYNC_HTML = (DTWIN / "_query_sync.html").read_text(encoding="utf-8")
DTWIN_HTML = (ROOT / "src/front/templates/dtwin.html").read_text(encoding="utf-8")
CACHE_JS = (ROOT / "src/front/static/query/js/query-build-cache.js").read_text(encoding="utf-8")
SYNC_JS = (ROOT / "src/front/static/query/js/query-sync.js").read_text(encoding="utf-8")
RUNS_JS = (ROOT / "src/front/static/domain/js/domain-runs.js").read_text(encoding="utf-8")


def test_the_cache_tab_follows_runs_and_holds_one_section_per_graph():
    assert ORIGINAL_HTML.index('id="buildTabCacheBtn"') > ORIGINAL_HTML.index('id="buildTabRunsBtn"')
    pane = ORIGINAL_HTML.split('id="buildPaneCache"', 1)[1].split('id="buildPaneObjects"', 1)[0]
    original, certified = pane.split('id="buildCacheCertifiedHeading"', 1)
    assert "Original Graph" in original
    assert "{% set cache_id = 'orig' %}" in original
    assert "{% set cache_refresh = 'dbx' %}" in original
    assert "Certified Graph" in certified
    assert "{% set cache_id = 'cert' %}" in certified
    assert "{% set cache_refresh = 'sync' %}" in certified
    assert pane.count("partials/dtwin/_graph_cache_pane.html") == 2


@pytest.mark.parametrize(
    "suffix",
    [
        "CacheReloadBtn",
        "CacheLoading",
        "CacheNotice",
        "CacheError",
        "CacheContent",
        "CacheCompanionsBody",
        "CacheActivated",
        "CacheState",
        "CacheRefreshed",
    ],
)
def test_pane_ids_are_prefixed_per_graph(suffix):
    assert f'id="{{{{ cache_id }}}}{suffix}"' in PANE_HTML


def test_the_pane_offers_the_refresh_of_its_own_graph():
    assert "'dbxAdjacencyRefreshBtn' if cache_refresh == 'dbx' else 'syncAdjacencyRefreshBtn'" in PANE_HTML
    assert 'data-sync-action="refresh-adjacency-only"' in PANE_HTML


def test_activate_deactivate_link_points_at_context_settings():
    assert 'href="/domain/?section=information#searchCacheSection"' in PANE_HTML


def test_build_card_no_longer_embeds_cache_status():
    assert "CacheActivated" not in SYNC_HTML


def test_one_loader_per_graph_reads_its_own_stats():
    assert "query/js/query-build-cache.js" in DTWIN_HTML
    assert "'/dtwin/sync/stats?simulation_id=' + encodeURIComponent(source)" in CACHE_JS
    assert "prefix: 'orig', source: 'original', tabBtnId: 'buildTabCacheBtn'" in CACHE_JS
    assert "prefix: 'cert', source: 'certified', tabBtnId: 'buildTabCacheBtn'" in CACHE_JS
    assert "shown.bs.tab" in CACHE_JS


def test_only_the_original_pane_feeds_the_overview():
    assert "feedsOverview: true" in CACHE_JS.split("prefix: 'orig'", 1)[1].split("})", 1)[0]
    assert "feedsOverview: false" in CACHE_JS.split("prefix: 'cert'", 1)[1].split("})", 1)[0]


def test_the_build_page_routes_its_cache_and_objects_tabs():
    assert "buildTabCacheBtn" in RUNS_JS
    assert "buildTabObjectsBtn" in RUNS_JS
    assert "['runs', 'cache', 'objects']" in RUNS_JS


def test_cache_status_helper_paints_the_pane_of_its_prefix():
    assert "function renderBuildGraphCacheStatus(source, prefix)" in SYNC_JS
    assert "renderBuildGraphCacheStatus(cacheSource, 'cert')" in SYNC_JS
    assert "renderBuildGraphCacheStatus(cache, 'orig')" in SYNC_JS


def test_cache_updated_falls_back_to_last_build_not_never():
    """Missing stamp falls back to last Build; the UI must not print 'Never'."""
    body = SYNC_JS.split("function renderBuildGraphCacheStatus", 1)[1].split(
        "async function initSyncSection", 1
    )[0]
    assert ">Never<" not in body
    assert "'Never'" not in body
    assert '"Never"' not in body
    assert "last_built" in body
    assert "Cache updated" in PANE_HTML


def test_each_pane_says_where_its_cache_is_stored():
    assert 'id="{{ cache_id }}CacheLocation"' in PANE_HTML
    assert "Stored in" in PANE_HTML
    assert "databricks: 'Lakehouse'" in CACHE_JS
    assert "cache.location" in CACHE_JS


def test_reload_bypasses_the_stats_cache():
    assert "ENDPOINT + '&refresh=true'" in CACHE_JS
    assert "() => load(true)" in CACHE_JS
