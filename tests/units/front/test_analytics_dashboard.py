"""The Analytics section follows the project's asset-split convention.

The section used to carry ~1100 lines of inline JS and an inline <style> block,
both forbidden by .cursor/11-frontend-design.mdc. Everything else in this area
(query-sync, query-cohorts, ...) is already split; this pins that.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
PANEL = REPO_ROOT / "src/front/templates/partials/dtwin/_query_analytics.html"
DTWIN = REPO_ROOT / "src/front/templates/dtwin.html"
CSS = REPO_ROOT / "src/front/static/query/css/query-analytics.css"
JS = REPO_ROOT / "src/front/static/query/js/query-analytics.js"


@pytest.fixture(scope="module")
def panel() -> str:
    return PANEL.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def js() -> str:
    return JS.read_text(encoding="utf-8")


class TestAssetsAreSplitOut:
    def test_css_and_js_files_exist(self):
        assert CSS.is_file()
        assert JS.is_file()

    def test_partial_has_no_inline_style_block(self, panel):
        assert "<style>" not in panel

    def test_partial_has_no_inline_script_block(self, panel):
        # The metric-explanation modal markup stays; executable JS does not.
        assert "<script>" not in panel

    def test_dtwin_loads_both_assets_cache_busted(self):
        html = DTWIN.read_text(encoding="utf-8")
        for asset in ("query/css/query-analytics.css",
                      "query/js/query-analytics.js"):
            assert asset in html
        block = html[html.index("query-analytics.js") - 400:]
        assert "asset_version" in block[:600]

    def test_cross_file_globals_keep_their_names(self, js):
        """query.js and query-sigmagraph.js call these by name."""
        for name in (
            "window.analyticsLoadTypes",
            "window.analyticsCompute",
            "window.analyticsRenderCharts",
            "window.analyticsResume",
            "window.analyticsLoadLatest",
            "window.analyticsInterpret",
            "window.analyticsAddToAuditTrail",
            "window._analyticsDrillURI",
            "window._showMetricInfo",
        ):
            assert name in js, f"{name} must survive the move"

    def test_the_metric_explanation_modal_stays_in_the_partial(self, panel):
        assert 'id="analyticsMetricModal"' in panel


class TestTabStripIsThreeTabs:
    REMOVED = (
        "atab-btn-pagerank", "atab-pagerank",
        "atab-btn-betweenness", "atab-betweenness",
        "atab-btn-degree", "atab-degree",
        "atab-btn-closeness", "atab-closeness",
        "atab-btn-clustering", "atab-clustering",
    )

    @pytest.mark.parametrize("marker", REMOVED)
    def test_per_metric_tabs_are_gone(self, panel, marker):
        assert marker not in panel

    @pytest.mark.parametrize("marker", (
        "atab-btn-dashboard", "atab-dashboard",
        "atab-btn-health", "atab-health",
        "atab-btn-insights", "atab-insights",
    ))
    def test_the_three_surviving_tabs_are_present(self, panel, marker):
        assert marker in panel

    def test_dashboard_is_the_default_tab(self, panel):
        strip = panel[panel.index('id="analyticsTabs"'):]
        strip = strip[: strip.index("</ul>")]
        assert strip.count("nav-link active") == 1
        dashboard_at = strip.index("atab-btn-dashboard")
        active_at = strip.index("nav-link active")
        assert active_at < dashboard_at, "the active class must be on Dashboard"

    def test_the_strip_uses_the_shared_ob_tabs_treatment(self, panel):
        assert 'class="nav nav-tabs ob-tabs' in panel

    def test_the_strip_does_not_re_apply_baked_in_utilities(self, panel):
        strip = panel[panel.index('id="analyticsTabs"'):]
        strip = strip[: strip.index(">")]
        for banned in ("px-3", "pt-2", "pt-3", "bg-white", "font-size"):
            assert banned not in strip


class TestDashboardShell:
    def test_the_dashboard_pane_holds_the_strip_and_ranking_containers(self, panel):
        pane = panel[panel.index('id="atab-dashboard"'):]
        pane = pane[: pane.index('id="atab-health"')]
        assert 'id="analyticsDistStrip"' in pane
        assert 'id="analyticsRankingCard"' in pane
        assert 'id="pagerankDetailTable"' in pane, "detail table moves here"


class TestKpiRowUsesTheSharedTile:
    def test_kpi_tiles_use_the_ob_kpi_tile_component(self, panel):
        row = panel[panel.index('id="analyticsStatsRow"'):]
        row = row[: row.index('id="analyticsDistStrip"')]
        assert "ob-kpi-tile" in row

    def test_the_hand_rolled_tile_markup_is_gone(self, panel):
        row = panel[panel.index('id="analyticsStatsRow"'):]
        row = row[: row.index('id="analyticsDistStrip"')]
        assert "border-0 bg-light" not in row

    @pytest.mark.parametrize("stat_id", (
        "aStatNodes", "aStatEdges", "aStatComponents",
        "aStatAvgDegree", "aStatDensity", "aStatElapsed", "aStatGraphNodes",
    ))
    def test_every_stat_id_survives_the_rebuild(self, panel, stat_id):
        """_renderAnalyticsData writes to these by id."""
        assert stat_id in panel
