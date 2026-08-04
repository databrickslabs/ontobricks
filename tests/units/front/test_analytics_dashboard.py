"""The Analytics section follows the project's asset-split convention.

The section used to carry ~1100 lines of inline JS and an inline <style> block,
both forbidden by .cursor/11-frontend-design.mdc. Everything else in this area
(query-sync, query-cohorts, ...) is already split; this pins that.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


def _fn(source: str, name: str) -> str:
    """The body of a named function, up to the next declaration at its level.

    The section's JS is one IIFE of 4-space-indented declarations, so the next
    `\\n    function ` or `\\n    window.` is a reliable terminator.
    """
    header = "function " + name
    start = source.index(header)
    rest = source[start + len(header):]
    ends = [i for i in (rest.find("\n    function "),
                        rest.find("\n    window.")) if i != -1]
    return rest[: min(ends)] if ends else rest

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


class TestDistributionStrip:
    def test_the_strip_is_rendered_from_the_distributions_payload(self, js):
        assert "function _renderDistributionStrip" in js
        assert "_analyticsData.distributions" in js

    def test_the_metric_table_lists_all_five_in_display_order(self, js):
        """PageRank was absent from the old _METRICS list because its tab held a
        table, not a chart. The strip charts all five."""
        table = js[js.index("_ALL_METRICS = ["):]
        table = table[: table.index("];")]
        keys = re.findall(r"key:\s*'(\w+)'", table)
        assert keys == ["pagerank", "betweenness", "degree",
                        "closeness", "clustering"]

    def test_every_metric_in_the_table_has_a_colour_and_an_icon(self, js):
        table = js[js.index("_ALL_METRICS = ["):]
        table = table[: table.index("];")]
        assert len(re.findall(r"color:", table)) == 5
        assert len(re.findall(r"icon:", table)) == 5

    def test_pagerank_is_selected_on_load(self, js):
        assert "_selectedMetric = 'pagerank'" in js

    def test_a_missing_distribution_renders_an_empty_state_not_a_chart(self, js):
        """A legacy payload has no distributions at all; an unavailable metric
        has none for that key. Neither may reach Chart.js."""
        fn = _fn(js, "_renderDistributionStrip")
        assert "Re-run the analysis" in fn
        assert "Not computed for this run" in fn
        # Legacy cached results omit distributions entirely, so dist is
        # undefined on first load. The !dist || check must run before
        # !dist.bins or every metric throws on that path.
        undefined_guard_at = fn.index("!dist ||")
        bins_guard_at = fn.index("!dist.bins")
        assert undefined_guard_at < bins_guard_at
        # The guard must return before any chart is constructed.
        guard_at = fn.index("!dist.bins")
        assert guard_at < fn.index("new Chart")
        between = fn[guard_at:fn.index("new Chart")]
        assert "return" in between

    def test_tiles_are_buttons_so_selection_is_keyboard_reachable(self, js):
        fn = _fn(js, "_renderDistributionStrip")
        assert "<button" in fn

    def test_approximate_metrics_are_badged_in_the_strip(self, js):
        fn = _fn(js, "_renderDistributionStrip")
        assert "asymp" in fn or "estimate" in fn.lower()

    def test_the_median_is_labelled_as_approximate(self, js):
        """Interpolated from bins; accurate to one bin width, so the caption
        must not present the median as an exact figure."""
        fn = _fn(js, "_renderDistributionStrip")
        caption_line = next(
            line for line in fn.splitlines()
            if "caption.innerHTML" in line and "median" in line.lower()
        )
        assert re.search(r"median\s+(&asymp;|~)", caption_line), (
            "the approximation marker must sit on the median caption line, "
            "not only in the unrelated approximate-metric badge"
        )

    def test_selecting_a_tile_redraws_the_ranking_chart(self, js):
        fn = _fn(js, "_selectMetric")
        assert "_renderRankingChart" in fn
