"""Build / Analytics embed their own Runs tabs (no Graph → Runs sidebar).

Build runs live under Graph → Management → Build → Runs.
Analytics runs live under Intelligence → Discovery → Analytics → Runs.
Settings → Automation → Runs remains the cross-domain admin view.
"""

import re
from pathlib import Path

import pytest

from tests.units.api.test_ui_rendering import _find, _html, _tags

pytestmark = pytest.mark.unit

_BUILD_SHELL = Path("src/front/templates/partials/dtwin/_query_build.html")
_BUILD_RUNS = Path("src/front/templates/partials/domain/_domain_build_runs.html")
_ANALYTICS_RUNS = Path("src/front/templates/partials/domain/_domain_analytics_runs.html")
_DTWIN = Path("src/front/templates/dtwin.html")
_INTEL = Path("src/front/templates/intelligence.html")
_ANALYTICS = Path("src/front/templates/partials/intelligence/_intel_analytics.html")
_ANALYTICS_JS = Path("src/front/static/query/js/query-analytics.js")
_JS = Path("src/front/static/domain/js/domain-runs.js")
_SHARED_JS = Path("src/front/static/global/js/runs-render.js")
_PERMISSIONS_CSS = Path("src/front/static/global/css/permissions.css")
_DTWIN_ROUTE = Path("src/front/routes/dtwin.py")
_MENU = Path("src/front/config/menu_config.json")


def _js() -> str:
    return _JS.read_text(encoding="utf-8")


def _shared_js() -> str:
    return _SHARED_JS.read_text(encoding="utf-8")


def _analytics_partial() -> str:
    return _ANALYTICS.read_text(encoding="utf-8") + "\n" + _ANALYTICS_JS.read_text(encoding="utf-8")


class TestBuildPageRunsTab:
    def test_shell_has_build_and_runs_tabs(self, client):
        html = _html(client, "/dtwin/")
        tags = _tags(html)
        for button_id, pane_id in (
            ("buildTabBuildBtn", "buildPaneBuild"),
            ("buildTabRunsBtn", "buildPaneRuns"),
        ):
            btn = _find(tags, id_=button_id)
            assert btn is not None
            assert btn.get("data-bs-target") == f"#{pane_id}"
            assert _find(tags, id_=pane_id) is not None

    def test_build_is_the_default_tab(self, client):
        html = _html(client, "/dtwin/")
        tags = _tags(html)
        assert "active" in (_find(tags, id_="buildTabBuildBtn").get("class") or "")
        assert "active" in (_find(tags, id_="buildPaneBuild").get("class") or "")

    def test_build_runs_table_lives_in_runs_pane(self, client):
        html = _html(client, "/dtwin/")
        runs_pane = html.index('id="buildPaneRuns"')
        assert runs_pane < html.index('id="runsTableBody"')
        assert "analyticsRunsTableBody" not in html

    def test_refresh_calls_load_build_runs(self, client):
        html = _html(client, "/dtwin/")
        btn = _find(_tags(html), id_="btnReloadBuildRuns")
        assert btn is not None
        assert btn.get("onclick") == "loadBuildRuns()"

    def test_runs_is_not_a_sidebar_item(self):
        menu = _MENU.read_text(encoding="utf-8")
        assert '"id": "digitaltwin-management"' in menu
        # Management items no longer include a top-level runs entry.
        mgmt = menu[menu.index('"id": "digitaltwin-management"'):]
        mgmt = mgmt[: mgmt.index('"id": "digitaltwin-data"')]
        assert '"id": "runs"' not in mgmt
        assert 'id="runs-section"' not in _DTWIN.read_text(encoding="utf-8")

    def test_legacy_runs_section_redirects_to_build_runs_tab(self):
        route = _DTWIN_ROUTE.read_text(encoding="utf-8")
        assert 'section") == "runs"' in route
        assert '/dtwin/?section=sync&tab=runs' in route


class TestAnalyticsPageRunsTab:
    def test_shell_has_analytics_and_runs_tabs(self, client):
        html = _html(client, "/intelligence/")
        tags = _tags(html)
        for button_id, pane_id in (
            ("analyticsPageTabMainBtn", "analyticsPagePaneMain"),
            ("analyticsPageTabRunsBtn", "analyticsPagePaneRuns"),
        ):
            btn = _find(tags, id_=button_id)
            assert btn is not None
            assert btn.get("data-bs-target") == f"#{pane_id}"
            assert _find(tags, id_=pane_id) is not None

    def test_analytics_is_the_default_page_tab(self, client):
        html = _html(client, "/intelligence/")
        tags = _tags(html)
        assert "active" in (_find(tags, id_="analyticsPageTabMainBtn").get("class") or "")
        assert "active" in (_find(tags, id_="analyticsPagePaneMain").get("class") or "")

    def test_analytics_runs_table_lives_in_runs_pane(self, client):
        html = _html(client, "/intelligence/")
        runs_pane = html.index('id="analyticsPagePaneRuns"')
        assert runs_pane < html.index('id="analyticsRunsTableBody"')
        assert "runsTableBody" not in html

    def test_inner_result_tabs_remain(self, client):
        html = _html(client, "/intelligence/")
        tags = _tags(html)
        tab_button_ids = [
            a.get("id")
            for t, a in tags
            if t == "button" and (a.get("id") or "").startswith("atab-btn-")
        ]
        assert tab_button_ids == [
            "atab-btn-dashboard",
            "atab-btn-health",
            "atab-btn-insights",
        ]

    def test_refresh_calls_load_analytics_runs(self, client):
        html = _html(client, "/intelligence/")
        btn = _find(_tags(html), id_="btnReloadAnalyticsRuns")
        assert btn is not None
        assert btn.get("onclick") == "loadAnalyticsRuns()"

    def test_analytics_table_names_its_version_column(self, client):
        html = _html(client, "/intelligence/")
        analytics = html[html.index("analyticsRunsTableWrapper"):]
        for header in ("Scope", "Version", "Nodes", "Edges", "Components", "Density"):
            assert f">{header}<" in analytics


class TestAnalyticsModal:
    def test_modal_renders_on_intelligence_page(self, client):
        html = _html(client, "/intelligence/")
        tags = _tags(html)
        assert _find(tags, id_="analyticsRunDetailsModal") is not None
        assert _find(tags, id_="analyticsRunDetailsBody") is not None

    def test_modal_is_page_level_on_intelligence(self):
        assert 'id="analyticsRunDetailsModal"' in _INTEL.read_text(encoding="utf-8")
        assert "analyticsRunDetailsModal" not in _ANALYTICS_RUNS.read_text(encoding="utf-8")
        assert "analyticsRunDetailsModal" not in _DTWIN.read_text(encoding="utf-8")

    def test_modal_aria_labelledby_references_existing_id(self, client):
        html = _html(client, "/intelligence/")
        tags = _tags(html)
        modal = _find(tags, id_="analyticsRunDetailsModal")
        assert modal is not None
        label_id = modal.get("aria-labelledby")
        assert label_id
        assert _find(tags, id_=label_id) is not None


class TestPermissionsCssCleanup:
    def test_runs_version_filter_selector_is_gone(self):
        css = _PERMISSIONS_CSS.read_text(encoding="utf-8")
        assert "#runsVersionFilter" not in css

    def test_audit_version_filter_selector_survives(self):
        css = _PERMISSIONS_CSS.read_text(encoding="utf-8")
        assert "#auditVersionFilter" in css


class TestRunsScript:
    def test_it_fetches_both_sources(self):
        src = _js()
        assert "/domain/build-runs" in src
        assert "/dtwin/metrics/history" in src

    def test_the_two_fetches_are_independent(self):
        src = _js()
        assert "Promise.all(" not in src
        assert src.count("async function _loadBuildRuns") == 1
        assert src.count("async function _loadAnalyticsRuns") == 1

    def test_public_loaders_latch_their_own_flags(self):
        src = _js()
        assert "_buildRunsLoaded = await _loadBuildRuns()" in src
        assert "_analyticsRunsLoaded = await _loadAnalyticsRuns()" in src
        assert re.search(r"_runsLoaded\s*=\s*true\s*;", src) is None

    def test_tab_chrome_wires_build_and_analytics_pages(self):
        src = _js()
        assert "buildTabRunsBtn" in src
        assert "analyticsPageTabRunsBtn" in src
        assert "_showBuildPageTab" in src
        assert "_showAnalyticsPageTab" in src

    def test_build_header_actions_stay_visible_on_runs(self):
        src = _js()
        sync_fn = src.split("function _syncBuildPageTab", 1)[1].split("function _showBuildPageTab", 1)[0]
        assert "buildHeaderActions" in sync_fn
        assert "classList.toggle('d-none'" not in sync_fn
        assert "actions.classList.remove('d-none')" in sync_fn

    def test_analytics_status_has_its_own_badge_helper(self):
        src = _shared_js()
        assert "analyticsStatusBadge" in src
        assert "'completed'" in src or '"completed"' in src

    def test_failed_analytics_rows_do_not_show_zeroed_metrics(self):
        src = _shared_js()
        match = re.search(
            r"function analyticsRunCells\(run, idx, opts\) \{(.*?)\n    \}",
            src,
            re.DOTALL,
        )
        assert match is not None
        body = match.group(1)
        assert "const failed = " in body
        assert "const dash = " in body
        for metric in ("node_count", "edge_count", "connected_components"):
            assert f"failed ? dash : num(run.{metric})" in body
        assert 'failed ? dash : esc((Number(run.avg_degree)' in body
        assert 'failed ? dash : esc((Number(run.density)' in body

    def test_loaders_report_failure_on_both_error_paths(self):
        src = _js()
        for name in ("_loadBuildRuns", "_loadAnalyticsRuns"):
            match = re.search(
                r"async function " + name + r"\(\) \{(.*?)\n\}",
                src,
                re.DOTALL,
            )
            assert match is not None, f"{name} function not found"
            body = match.group(1)
            error_branch = body[body.index("if (!data.success)"): body.index("catch (err)")]
            catch_branch = body[body.index("catch (err)"):]
            assert "return false;" in error_branch
            assert "return false;" in catch_branch
            assert "return true;" in body


class TestHistoryTabRemoved:
    @pytest.mark.parametrize(
        "marker",
        [
            "atab-btn-history",
            "atab-history",
            "analyticsLoadHistory",
            "analyticsHistoryBody",
            "analyticsHistoryEmpty",
        ],
    )
    def test_no_trace_of_the_history_tab_in_the_source(self, marker):
        assert marker not in _analytics_partial()

    def test_helpers_the_other_tabs_still_use_survive(self):
        src = _analytics_partial()
        assert "function _formatComputedAt" in src
        assert "function _localName" in src
