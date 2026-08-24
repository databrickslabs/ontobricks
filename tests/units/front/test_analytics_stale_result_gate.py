"""Analytics must not show a stored result for a graph that no longer exists.

Drop the Unity Catalog objects and reopen **Knowledge Graph → Analytics** and
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
ANALYTICS_HTML = REPO_ROOT / "src/front/templates/partials/dtwin/_query_analytics.html"

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


class TestTheBannerExplainsTheDisappearance:
    def test_it_says_previous_results_are_not_shown(self):
        """Charts vanishing with no explanation reads as a broken page."""
        body = _block(_source(), "function _showNoGraphBanner(", 1400)
        assert "no longer" in body

    def test_it_points_at_the_retained_history(self):
        body = _block(_source(), "function _showNoGraphBanner(", 1400)
        assert "Runs" in body
