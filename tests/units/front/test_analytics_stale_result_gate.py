"""Analytics must not show a stored result for a graph that no longer exists.

Drop the Unity Catalog objects and reopen **Intelligence → Discovery → Analytics** and
the page used to contradict itself: the "No Knowledge Graph has been built yet"
banner appeared (the live probe correctly failed) while the KPI tiles, the
distribution strip and the ranking chart stayed populated from the last
persisted run.

The two came from independent sources. The banner is driven by a live check in
``_loadEntityTypes`` (``__TRIPLESTORE_CONFIG.graph_name``, then
``/dtwin/sync/stats``). The charts are driven by ``analyticsResume`` →
``analyticsLoadLatest`` → ``/dtwin/metrics/latest``, which is a pure registry
read and by design knows nothing about whether the graph objects still exist.
Nothing connected the live signal to the rendering, so the presentation layer
kept asserting numbers about a graph that had been deleted.

The contract is that the live signal gates the dashboard: ``_loadEntityTypes``
records whether the graph is missing, and ``analyticsResume`` clears the
dashboard and skips the stored-result fetch when it is.
"""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
ANALYTICS_JS = REPO_ROOT / "src/front/static/query/js/query-analytics.js"
ANALYTICS_HTML = REPO_ROOT / "src/front/templates/partials/intelligence/_intel_analytics.html"

FLAG = "_graphMissing"


def _source() -> str:
    return ANALYTICS_JS.read_text(encoding="utf-8")


def _block(source: str, signature: str, span: int) -> str:
    return source[source.index(signature) : source.index(signature) + span]


class TestTheLiveSignalIsRecorded:
    def test_a_missing_graph_flag_exists(self):
        assert f"var {FLAG}" in _source()

    def test_the_no_config_path_records_it(self):
        """``graph_name`` empty: no build has ever produced a graph."""
        body = _block(_source(), "async function _loadEntityTypes()", 900)
        head = body[: body.index("try {")]
        assert f"{FLAG} = true" in head

    def test_the_failed_probe_path_records_it(self):
        """``/dtwin/sync/stats`` failing is how deleted objects surface."""
        body = _block(_source(), "async function _loadEntityTypes()", 2200)
        probe = body[body.index("if (!data.success)") :]
        assert f"{FLAG} = true" in probe[:400]

    def test_the_success_path_clears_it(self):
        """A rebuilt graph has to bring the dashboard back."""
        body = _block(_source(), "async function _loadEntityTypes()", 2200)
        assert f"{FLAG} = false" in body


class TestTheDashboardIsGated:
    def test_resume_consults_the_flag(self):
        body = _block(_source(), "window.analyticsResume = async function", 700)
        assert FLAG in body

    def test_resume_checks_before_it_loads(self):
        """Fetching first and hiding after would still flash stale charts."""
        body = _block(_source(), "window.analyticsResume = async function", 700)
        assert body.index(FLAG) < body.index("analyticsLoadLatest")

    def test_resume_clears_the_dashboard_when_the_graph_is_gone(self):
        body = _block(_source(), "window.analyticsResume = async function", 700)
        assert "_clearAnalyticsResults()" in body

    def test_the_clear_helper_hides_the_results_container(self):
        """`#analyticsResults` starts hidden, so only a clear can re-hide it."""
        assert 'id="analyticsResults" class="d-none"' in ANALYTICS_HTML.read_text(
            encoding="utf-8"
        )
        body = _block(_source(), "function _clearAnalyticsResults()", 900)
        assert "analyticsResults" in body
        assert "add('d-none')" in body

    def test_the_clear_helper_drops_the_cached_payload(self):
        """``_analyticsData`` gates Interpret; a stale payload would still send."""
        body = _block(_source(), "function _clearAnalyticsResults()", 900)
        assert "_analyticsData = null" in body

    def test_the_clear_helper_drops_metric_series_cache(self):
        body = _block(_source(), "function _clearAnalyticsResults()", 1400)
        assert "_metricSeriesCache = {}" in body


class TestTheBannerExplainsTheDisappearance:
    def test_it_says_previous_results_are_not_shown(self):
        """Charts vanishing with no explanation reads as a broken page."""
        body = _block(_source(), "function _showNoGraphBanner(", 1400)
        assert "no longer" in body

    def test_it_points_at_the_retained_history(self):
        body = _block(_source(), "function _showNoGraphBanner(", 1400)
        assert "Runs" in body


class TestSpinnerOnSectionOpen:
    """The graph probe can take seconds: feedback must not wait for it."""

    def test_spinner_is_shown_before_the_graph_probe(self):
        body = _block(_source(), "window.analyticsLoadTypes = function", 900)
        assert body.index("_showLoadingLatestSpinner()") < body.index("_loadEntityTypes()")

    def test_latest_result_is_prefetched_during_the_probe(self):
        body = _block(_source(), "window.analyticsLoadTypes = function", 900)
        assert body.index("_fetchLatestMetrics()") < body.index("_loadEntityTypes()")
        assert "analyticsResume(latest)" in body

    def test_missing_graph_path_hides_the_spinner(self):
        body = _block(_source(), "window.analyticsResume = async function", 700)
        assert "_hideLoadingLatestSpinner()" in body


class TestEmptyStateMatchesRunsTab:
    """No stored analysis: same icon + message as Intelligence → Analytics → Runs."""

    RUNS_HTML = Path(__file__).resolve().parents[3] / (
        "src/front/templates/partials/domain/_domain_analytics_runs.html"
    )

    def _empty_block(self) -> str:
        html = ANALYTICS_HTML.read_text(encoding="utf-8")
        start = html.index('id="analyticsEmpty"')
        return html[start : html.index("</div>", start)]

    def test_empty_state_reuses_the_runs_icon_and_message(self):
        runs = self.RUNS_HTML.read_text(encoding="utf-8")
        block = self._empty_block()
        assert "No analytics runs found" in runs and "No analytics runs found" in block
        assert '<i class="bi bi-graph-up-arrow text-muted fs-1"></i>' in runs
        assert '<i class="bi bi-graph-up-arrow text-muted fs-1"></i>' in block

    def test_empty_state_starts_hidden(self):
        html = ANALYTICS_HTML.read_text(encoding="utf-8")
        assert 'id="analyticsEmpty" class="text-center py-5 d-none"' in html

    def test_resume_shows_it_only_when_nothing_is_stored_or_running(self):
        body = _block(_source(), "window.analyticsResume = async function", 1100)
        assert "_showAnalyticsEmpty(_noStoredResult && !running)" in body
        assert body.index("_findRunningAnalyticsTask()") < body.index(
            "_showAnalyticsEmpty(_noStoredResult && !running)"
        )

    def test_rendering_busy_and_errors_hide_it(self):
        src = _source()
        for anchor in (
            "function _renderAnalyticsData(data, meta)",
            "function _setAnalysisBusy(busy)",
            "function _showAnalyticsError(msg)",
        ):
            assert "_showAnalyticsEmpty(false)" in _block(src, anchor, 300)

