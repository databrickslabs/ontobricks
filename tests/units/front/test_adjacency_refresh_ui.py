"""Frontend contracts for adjacency-only refresh actions."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
BUILD_HTML = REPO_ROOT / "src/front/templates/partials/dtwin/_query_build.html"
SYNC_JS = REPO_ROOT / "src/front/static/query/js/query-sync.js"
DBX_JS = REPO_ROOT / "src/front/static/query/js/query-databricks-build.js"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


class TestAdjacencyButtonsExist:
    def test_cache_tab_hosts_refresh_cache_buttons(self):
        html = _read(BUILD_HTML)
        pane = html.split('id="buildPaneCache"', 1)[1]
        header = html.split('id="buildHeaderActions"', 1)[1].split('id="buildPageTabs"', 1)[0]
        assert 'id="syncAdjacencyRefreshBtn"' in pane
        assert 'id="dbxAdjacencyRefreshBtn"' in pane
        assert 'id="syncAdjacencyRefreshBtn"' not in header
        assert 'id="dbxAdjacencyRefreshBtn"' not in header
        # Header kebab also offers Refresh cache (via shared partial).
        assert '_build_more_actions.html' in header
        # Role-gated: builder + locked variants for Lakebase and Lakehouse.
        assert pane.count(">Refresh cache") == 4
        assert ">Refresh adjacency" not in html

    def test_dbx_js_binds_header_kebab_refresh_cache(self):
        js = _read(DBX_JS)
        assert "js-dbx-adjacency-refresh" in js
        assert "startDatabricksAdjacencyRefresh" in js

    def test_sync_panel_has_refresh_adjacency_action(self):
        html = _read(BUILD_HTML)
        assert 'id="syncAdjacencyRefreshBtn"' in html
        assert 'data-sync-action="refresh-adjacency-only"' in html
        assert "btn btn-sm btn-outline-secondary" in html

    def test_databricks_panel_has_refresh_adjacency_action(self):
        html = _read(BUILD_HTML)
        assert 'id="dbxAdjacencyRefreshBtn"' in html
        assert "btn btn-sm btn-outline-secondary" in html

    def test_both_buttons_are_role_gated_like_build(self):
        html = _read(BUILD_HTML)
        assert "{% set _dr = get_user_domain_role() %}" in html
        assert "{% if _dr not in ('viewer', 'editor', 'none') %}" in html
        assert '<i class="bi bi-lock me-1"></i>' in html


class TestAdjacencyRefreshFlow:
    def test_sync_js_posts_to_endpoint_and_polls_tasks(self):
        js = _read(SYNC_JS)
        assert "const ADJ_REFRESH_TASK_KEY = 'ontobricks_adjacency_refresh_task';" in js
        assert "fetch('/dtwin/adjacency/refresh'" in js
        assert "fetch(`/tasks/${taskId}`" in js
        assert "sessionStorage.setItem(ADJ_REFRESH_TASK_KEY, data.task_id);" in js

    def test_databricks_js_disables_while_running_and_without_graph(self):
        js = _read(DBX_JS)
        assert "dbxAdjacencyRefreshRunning" in js
        assert "dbxBuildRunning || dbxAdjacencyRefreshRunning" in js
        assert "dbxGraphHasData" in js

    def test_sync_visibility_is_explicitly_lakebase_only(self):
        sync_js = _read(SYNC_JS)
        assert "return backend === 'lakebase';" in sync_js

    def test_lakehouse_resume_checks_task_status_before_polling(self):
        dbx_js = _read(DBX_JS)
        assert "async function checkAndResumeDatabricksTask(" in dbx_js
        assert "const resp = await fetch('/tasks/' + encodeURIComponent(taskId)" in dbx_js
        assert "if (task.status === 'running' || task.status === 'pending')" in dbx_js

    def test_lakehouse_refresh_polling_never_overlaps_requests(self):
        dbx_js = _read(DBX_JS)
        refresh_polling = dbx_js.split(
            "function pollDatabricksAdjacencyTask(taskId) {", 1
        )[1].split("function _finishDbxBuild(task) {", 1)[0]

        assert "setInterval(" not in refresh_polling
        assert "async function pollOnce()" in refresh_polling
        assert "setTimeout(pollOnce, 1500);" in refresh_polling

    def test_lakehouse_button_shows_for_every_backend(self):
        """The Original Graph is a Lakehouse virtual graph for every domain."""
        dbx_js = _read(DBX_JS)
        fn = dbx_js.split("function _updateDbxAdjacencyButton()", 1)[1].split(
            "function _applyDbxStorageKind", 1
        )[0]
        assert "backend === 'databricks'" not in fn
        assert "const visible = true;" in fn
        assert "js-dbx-adjacency-refresh" in fn
