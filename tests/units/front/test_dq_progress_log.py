"""Data Quality run UI shows a scrollable activity log from task.log_entries."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
HTML = REPO_ROOT / "src/front/templates/partials/dtwin/_query_dataquality.html"
JS = REPO_ROOT / "src/front/static/query/js/query-dataquality.js"
PAGE = REPO_ROOT / "src/front/templates/dtwin.html"


CSS = REPO_ROOT / "src/front/static/query/css/query-dataquality.css"


def test_progress_area_has_a_scrollable_activity_log():
    html = HTML.read_text(encoding="utf-8")
    assert 'id="dqExecProgressLog"' in html
    assert "task-activity-log" in html
    css = CSS.read_text(encoding="utf-8")
    assert "#dqExecProgressLog" in css
    assert "overflow-y: auto" in css
    assert "max-height: 9.5rem" in css


def test_poll_renders_task_log_entries():
    js = JS.read_text(encoding="utf-8")
    assert "log_entries" in js
    assert "_renderProgressLog" in js
    assert "dqExecProgressLog" in js
    assert "TaskProgressUI.renderActivityLog" in js
    assert "keepProgress" in js
    assert "tick()" in js


def test_knowledge_graph_page_loads_the_shared_activity_log_renderer():
    page = PAGE.read_text(encoding="utf-8")
    assert "global/js/task-progress-ui.js" in page
    assert page.find("task-progress-ui.js") < page.find("query-dataquality.js")
    assert page.find("task-progress-ui.js") < page.find("query-shacl-facets.js")
