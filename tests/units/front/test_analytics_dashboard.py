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
