"""Contract tests for the shared timed task-log renderer wiring."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
DTWIN_HTML = REPO_ROOT / "src/front/templates/dtwin.html"
QUERY_SYNC_JS = REPO_ROOT / "src/front/static/query/js/query-sync.js"
QUERY_SYNC_CSS = REPO_ROOT / "src/front/static/query/css/query-sync.css"
SYNC_PARTIAL = REPO_ROOT / "src/front/templates/partials/dtwin/_query_sync.html"
DBX_BUILD_JS = REPO_ROOT / "src/front/static/query/js/query-databricks-build.js"
DBX_BUILD_HTML = REPO_ROOT / "src/front/templates/partials/dtwin/_query_databricks_build.html"


def test_shared_renderer_loads_before_backend_build_scripts() -> None:
    html = DTWIN_HTML.read_text(encoding="utf-8")
    assert html.index("timed-task-log.js") < html.index("query-databricks-build.js")
    assert html.index("timed-task-log.js") < html.index("query-sync.js")


def test_lakebase_uses_shared_timed_renderer() -> None:
    js = QUERY_SYNC_JS.read_text(encoding="utf-8")
    assert "TimedTaskLog.create" in js
    assert 'cardId: "syncBuildLogCard"' in js


def test_generic_log_styles_use_shared_card_class() -> None:
    css = QUERY_SYNC_CSS.read_text(encoding="utf-8")
    assert ".timed-task-log-card .sync-build-log" in css
    assert "#syncBuildLogCard .sync-build-log" not in css


def test_lakebase_log_card_declares_shared_class() -> None:
    html = SYNC_PARTIAL.read_text(encoding="utf-8")
    assert 'id="syncBuildLogCard"' in html
    assert 'class="card d-none mb-3 timed-task-log-card"' in html


def test_lakehouse_build_panel_has_timed_log_card() -> None:
    html = DBX_BUILD_HTML.read_text(encoding="utf-8")
    for element_id in (
        "dbxBuildLogCard",
        "dbxBuildLogList",
        "dbxBuildLogTotal",
        "dbxBuildLogBadge",
        "dbxBuildLogExport",
        "dbxBuildLogHide",
    ):
        assert f'id="{element_id}"' in html
    assert "timed-task-log-card" in html


def test_lakehouse_poll_renders_full_task_log() -> None:
    js = DBX_BUILD_JS.read_text(encoding="utf-8")
    assert "TimedTaskLog.create" in js
    assert 'cardId: "dbxBuildLogCard"' in js
    assert "_dbxTimedBuildLog.render(task)" in js


def _lakehouse_poll_body() -> str:
    source = DBX_BUILD_JS.read_text(encoding="utf-8")
    start = source.index("function pollDatabricksBuildTask(")
    end = source.index("\nasync function checkAndResumeDatabricksTask", start)
    return source[start:end]


def test_lakehouse_build_poll_starts_immediately_without_interval() -> None:
    body = _lakehouse_poll_body()
    assert "pollOnce();" in body
    assert "setInterval(" not in body
    assert "setTimeout(pollOnce, _dbxBuildPollDelay(task))" in body


def test_lakehouse_build_poll_uses_adaptive_delays() -> None:
    js = DBX_BUILD_JS.read_text(encoding="utf-8")
    assert "const DBX_BUILD_FAST_POLL_MS = 300;" in js
    assert "const DBX_BUILD_FINAL_POLL_MS = 1000;" in js
    start = js.index("function _dbxBuildPollDelay(")
    end = js.index("\nfunction applyTripleStoreBackendPanels", start)
    body = js[start:end]
    assert "current_step" in body
    assert "steps.length - 1" in body


def test_lakehouse_build_poll_stops_after_terminal_or_error() -> None:
    body = _lakehouse_poll_body()
    terminal_start = body.index("if (task.status === 'completed'")
    schedule_start = body.index(
        "setTimeout(pollOnce, _dbxBuildPollDelay(task))"
    )
    assert "return;" in body[terminal_start:schedule_start]
    catch_start = body.index("} catch (e)")
    assert "setTimeout(" not in body[catch_start:]
