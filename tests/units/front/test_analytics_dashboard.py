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


@pytest.fixture(scope="module")
def css() -> str:
    return CSS.read_text(encoding="utf-8")


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
        # Asserted as whole literals: a window around the tag also spans the
        # neighbouring <script>s, which carry their own ?v={{ asset_version }}.
        for literal in (
            "query/css/query-analytics.css') }}?v={{ asset_version }}",
            "query/js/query-analytics.js') }}?v={{ asset_version }}",
        ):
            assert literal in html

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
        # class= sits before id= on the <ul>, so the slice has to start at the
        # opening tag or the class list is never examined.
        strip = panel[panel.rindex("<ul", 0, panel.index('id="analyticsTabs"')):]
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


class TestTheKpiStripLinesUpWithTheTabs:
    """`.ob-tabs` and `.ob-tab-content` are inset 12px from the right.

    A Bootstrap `.row` is not, so the tiles overhung the tab strip by 12px
    (measured in Chrome at 1500/1100/800/500px viewports before the change).
    Its negative gutters also offset both edges by 4px, which is why the strip is
    a grid rather than a row carrying a compensating margin.
    """

    def test_the_strip_is_a_grid_not_a_bootstrap_row(self, panel):
        row = panel[panel.index('id="analyticsStatsRow"') - 200:]
        row = row[: row.index('id="analyticsTabs"')]
        assert "analytics-kpi-strip" in row
        assert "row g-2" not in row
        assert "col-lg-2" not in row, "column wrappers are gone with the row"

    def test_the_strip_carries_the_same_right_inset_as_the_tabs(self, css):
        block = css[css.index(".analytics-kpi-strip {"):]
        block = block[: block.index("}")]
        assert "margin-right: 12px" in block

    def test_the_inset_matches_what_ob_tab_content_uses(self):
        """If the shared 12px ever changes, this pairing has to change with it."""
        main_css = (
            REPO_ROOT / "src/front/static/global/css/main.css"
        ).read_text(encoding="utf-8")
        block = main_css[main_css.index(".ob-tab-content {"):]
        block = block[: block.index("}")]
        assert "margin-right: 12px" in block

    def test_the_tracks_can_shrink_below_their_content(self, css):
        """Bare 1fr means minmax(auto, 1fr) and would floor the track widths."""
        block = css[css.index(".analytics-kpi-strip {"):]
        block = block[: block.index("}")]
        assert "repeat(6, minmax(0, 1fr))" in block

    @pytest.mark.parametrize("breakpoint_px,tracks", (("991.98px", 3), ("767.98px", 2)))
    def test_the_old_column_breakpoints_are_preserved(self, css, breakpoint_px, tracks):
        """col-6 / col-md-4 / col-lg-2 gave 2 / 3 / 6 tiles across."""
        query = f"@media (max-width: {breakpoint_px})"
        assert query in css
        block = css[css.index(query):]
        block = block[: block.index("\n}")]
        assert f"repeat({tracks}, minmax(0, 1fr))" in block
        assert "analytics-kpi-strip" in block


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
        assert "analytics-dist-badge" in fn
        assert "approximate.indexOf(m.key)" in fn

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


class TestRankingCard:
    def test_one_ranking_chart_is_rendered_for_the_selected_metric(self, js):
        assert "function _renderRankingChart" in js
        assert "analyticsRankingChart" in js

    def test_the_segmented_control_offers_all_five_metrics(self, js):
        fn = _fn(js, "_renderRankingChart")
        assert "rankSeg_" in fn

    def test_clicking_a_segment_selects_that_metric(self, js):
        fn = _fn(js, "_renderRankingChart")
        assert "_selectMetric" in fn

    def test_click_through_to_the_graph_viewer_survives(self, js):
        fn = _fn(js, "_renderRankingChart")
        assert "_navigateToGraph" in fn

    def test_the_top_n_input_still_drives_the_chart(self, js):
        assert "analyticsTopN" in js

    def test_the_estimate_notice_survives(self, js):
        assert "Estimate." in js

    def test_the_all_zero_notice_survives(self, js):
        assert "All values are 0." in js

    def test_the_not_computed_notice_survives(self, js):
        assert "Not computed for this graph." in js

    def test_the_detail_table_is_still_rendered(self, js):
        assert "_renderPagerankTable" in js


class TestLogScaleToggle:
    def test_the_toggle_exists_in_the_section_header(self, panel):
        header = panel[: panel.index('id="analyticsSpinner"')]
        assert 'id="analyticsLogScale"' in header
        switch = header[header.index('id="analyticsLogScale"') - 120 :]
        assert "checked" in switch[:220]

    def test_the_toggle_is_labelled(self, panel):
        assert "Log scale" in panel

    def test_the_toggle_redraws_the_strip(self, js):
        fn = _fn(js, "analyticsToggleLogScale")
        assert "_renderDistributionStrip" in fn

    def test_it_defaults_to_logarithmic(self, js):
        assert "_logScale = true" in js

    def test_the_axis_type_follows_the_flag(self, js):
        assert "'logarithmic'" in js
        assert "_logScale ? 'logarithmic' : 'linear'" in js

    def test_the_caption_states_when_log_is_active(self, js):
        """Bar heights stop being proportional to counts; an unlabelled log
        chart misleads."""
        fn = _fn(js, "_renderDistributionStrip")
        assert re.search(r"_logScale \?[^\n]*<em>log</em>", fn), (
            "the caption must name the log scale, not merely read the flag"
        )


class TestInterpretPayloadExcludesDistributions:
    """Sending them would change an LLM prompt, which needs the eval gate."""

    @staticmethod
    def _interpret(js: str) -> str:
        """The body of window.analyticsInterpret, which is an assigned
        expression rather than a declaration, so _fn does not apply."""
        start = js.index("window.analyticsInterpret")
        rest = js[start + 25:]
        end = rest.find("\n    window.")
        return rest if end == -1 else rest[:end]

    def test_distributions_are_deleted_from_the_interpret_body(self, js):
        assert "delete payload.distributions" in self._interpret(js)

    def test_the_deletion_happens_after_the_payload_is_built(self, js):
        """Deleting before the Object.assign would be a no-op that reads as
        protection."""
        body = self._interpret(js)
        assert body.index("Object.assign") < body.index("delete payload.distributions")

    def test_the_deletion_is_before_the_fetch(self, js):
        body = self._interpret(js)
        assert body.index("delete payload.distributions") < body.index("fetch(")

    def test_the_reason_is_recorded_at_the_deletion(self, js):
        """Without the reason, a later reader restores the field to 'give the
        agent more context' and trips the eval gate unknowingly."""
        body = self._interpret(js)
        near = body[body.index("delete payload.distributions") - 400:
                    body.index("delete payload.distributions")]
        assert "eval" in near.lower()
        assert "metrics_payload" in near or "prompt" in near.lower()


class TestScopeIsAskedForAtLaunch:
    """Entity type only takes effect when a run starts, so it is collected in a
    modal instead of sitting in the toolbar between runs, where it could be
    changed without re-running and misdescribe the result on screen."""

    @staticmethod
    def _modal(panel: str) -> str:
        start = panel.index('id="analyticsScopeModal"')
        return panel[start: panel.index("Metric explanation modal", start)]

    @staticmethod
    def _toolbar(panel: str) -> str:
        return panel[: panel.index('id="analyticsResults"')]

    def test_the_select_is_no_longer_in_the_toolbar(self, panel):
        assert "analyticsTypeSelect" not in self._toolbar(panel)

    def test_the_select_lives_in_the_scope_modal(self, panel):
        """Moved rather than duplicated, so _loadEntityTypes still finds it."""
        assert panel.count('id="analyticsTypeSelect"') == 1
        assert 'id="analyticsTypeSelect"' in self._modal(panel)

    def test_run_analysis_opens_the_modal_instead_of_computing(self, panel):
        toolbar = self._toolbar(panel)
        assert 'onclick="analyticsOpenScope()"' in toolbar
        assert 'onclick="analyticsCompute()"' not in toolbar

    def test_the_modal_offers_run_and_cancel(self, panel):
        modal = self._modal(panel)
        assert 'onclick="analyticsCompute()"' in modal
        assert 'data-bs-dismiss="modal"' in modal
        assert "Cancel" in modal

    def test_the_modal_resets_to_the_full_graph_on_open(self, js):
        """An inherited scope is easy to miss in a dialog you dismiss with Run."""
        fn = _fn(js, "analyticsOpenScope")
        assert "sel.value = ''" in fn
        assert ".show()" in fn
        assert fn.index("sel.value = ''") < fn.index(".show()")

    def test_the_modal_does_not_open_when_the_job_cannot_run(self, js):
        fn = _fn(js, "analyticsOpenScope")
        assert "_jobAvailable" in fn
        assert fn.index("_jobAvailable") < fn.index(".show()")

    def test_the_scope_is_read_before_the_modal_is_dismissed(self, js):
        """Reading the select after hiding risks Bootstrap having reset it."""
        body = js[js.index("window.analyticsCompute"):]
        body = body[: body.index("\n    window.")]
        assert body.index("_getSelectedTypes()") < body.index(".hide()")

    def test_the_run_sends_the_scope_that_was_picked(self, js):
        body = js[js.index("window.analyticsCompute"):]
        body = body[: body.index("\n    window.")]
        assert "class_filter: requested" in body


class TestResultScopeDescribesWhatIsDisplayed:
    """Three consumers used to read the live select, so changing it without
    re-running made them describe a scope nobody had computed."""

    def test_the_scope_comes_from_the_rendered_result(self, js):
        assert "_resultScope = (meta.class_filter && meta.class_filter.length)" in js

    def test_a_full_graph_result_clears_the_scope(self, js):
        """Without the else-null, a full-graph re-run keeps advertising the
        previous run's entity type."""
        start = js.index("_resultScope = (meta.class_filter")
        assert ": null" in js[start: start + 200]

    def test_the_subtitle_reads_the_result_scope(self, js):
        fn = _fn(js, "_renderAnalyticsData")
        assert "_scopeLabel(_resultScope)" in fn
        assert "_selEl.options[_selEl.selectedIndex]" not in fn

    def test_data_model_health_reads_the_result_scope(self, js):
        assert "_renderTypeProfiles(data.entity_type_profiles, !!_resultScope)" in js

    def test_interpret_sends_the_result_scope(self, js):
        assert "class_filter: _resultScope ? [_resultScope] : null" in js

    def test_nothing_but_the_run_request_reads_the_live_select(self, js):
        """_getSelectedTypes is the request being composed; anything that
        describes the displayed result must use _resultScope."""
        readers = [ln for ln in js.splitlines()
                   if "_getSelectedTypes()" in ln and "function" not in ln]
        assert len(readers) == 1, readers
        assert "requested" in readers[0]

    def test_the_dropdown_and_the_subtitle_share_one_label_format(self, js):
        """Two copies of the label expression drift; the subtitle would then
        disagree with the option the user picked."""
        assert "function _typeLabel" in js
        assert "_typeLabel(t)" in _fn(js, "_populateTypeSelect")
        assert "_typeLabel(match)" in _fn(js, "_scopeLabel")


class TestAllFiveTilesStayVisible:
    """All five tiles must be reachable at any usable window width."""

    @staticmethod
    def _rule(css: str, selector: str) -> str:
        """The declaration block following a selector."""
        start = css.index(selector)
        return css[start: css.index("}", start)]

    def test_the_five_column_track_can_shrink_below_the_canvas_width(self, css):
        """A bare `1fr` is minmax(auto, 1fr), so the canvas's 300px intrinsic
        width floors each track; five of them overflowed the container and
        clipped the last tile off the right edge."""
        rule = self._rule(css, ".analytics-dist-strip {")
        assert "repeat(5, minmax(0, 1fr))" in rule
        assert "repeat(5, 1fr)" not in css

    def test_the_reason_for_minmax_is_recorded(self, css):
        """Without it, 'minmax(0, 1fr)' reads as noise and gets simplified back
        to 1fr, silently restoring the clipping."""
        before = css[: css.index(".analytics-dist-strip {")]
        assert "minmax" in before
        assert "clip" in before.lower() or "overflow" in before.lower()

    def test_the_mid_width_layout_is_two_rows_of_two_then_three(self, css):
        """Five tiles in a 2-column grid wrapped to three ragged rows."""
        assert "repeat(6, minmax(0, 1fr))" in css
        assert "nth-child(-n + 2)" in css
        assert "nth-child(n + 3)" in css
        first = css.index("nth-child(-n + 2)")
        rest = css.index("nth-child(n + 3)")
        assert "span 3" in css[first: first + 120]
        assert "span 2" in css[rest: rest + 120]

    def test_the_two_column_wrap_is_gone(self, css):
        """The rule that produced the 2 + 2 + 1 wrap."""
        assert "repeat(2, 1fr)" not in css

    def test_the_narrowest_width_still_falls_back_to_one_column(self, css):
        """At phone widths three across leaves ~110px per tile, which wraps the
        caption and makes the two rows different heights."""
        assert "max-width: 576px" in css
        rule = self._rule(css, "@media (max-width: 576px)")
        assert "1fr" in rule

    def test_the_two_row_layout_does_not_apply_at_phone_widths(self, css):
        """The 2+3 rule and the single-column rule must not both match."""
        assert "(max-width: 1200px) and (min-width: 577px)" in css


class TestElapsedIsHumanReadable:
    """A Lakeflow run takes seconds to minutes, so the raw millisecond count
    ('134210 ms') forced the reader to do the arithmetic."""

    def test_the_kpi_tile_does_not_concatenate_raw_milliseconds(self, js):
        assert "s.elapsed_ms + ' ms'" not in js

    def test_the_kpi_tile_formats_the_elapsed_value(self, js):
        assert "_setText('aStatElapsed'" in js
        line = next(ln for ln in js.splitlines() if "aStatElapsed'" in ln)
        assert "_fmtElapsed(s.elapsed_ms)" in line

    def test_the_completion_toast_formats_it_too(self, js):
        """The toast and the tile show the same number; only one of them being
        converted is worse than neither."""
        assert "+ 'ms'" not in js
        assert "' in ' + _fmtElapsed(ms)" in js

    def test_the_formatter_converts_milliseconds_to_seconds(self, js):
        """formatTaskSeconds takes seconds. Handing it milliseconds would
        report a 2-minute run as 37 hours."""
        fn = _fn(js, "_fmtElapsed")
        assert "ms / 1000" in fn

    def test_the_formatter_reuses_the_shared_scale(self, js):
        """Duplicating the scale here would let the Analytics tile drift from
        the task tracker and the Runs page."""
        fn = _fn(js, "_fmtElapsed")
        assert "formatTaskSeconds" in fn

    def test_the_shared_formatter_is_exported(self):
        """_fmtElapsed depends on this global, and task-tracker.js previously
        marked the function 'Internal' and did not export it."""
        tracker = (REPO_ROOT / "src/front/static/global/js/task-tracker.js").read_text(
            encoding="utf-8"
        )
        assert "window.formatTaskSeconds = formatSeconds" in tracker

    def test_the_shared_scale_reaches_minutes_and_hours(self):
        """The whole point of the change: durations past 60s must not stay in
        seconds."""
        tracker = (REPO_ROOT / "src/front/static/global/js/task-tracker.js").read_text(
            encoding="utf-8"
        )
        body = tracker[tracker.index("function formatSeconds"):]
        body = body[: body.index("\nfunction ")]
        assert "m ${remSec}s" in body
        assert "h ${remMin}m" in body
