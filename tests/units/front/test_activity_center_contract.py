"""Navbar Activity Center combines tasks and notifications."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO = Path(__file__).resolve().parents[3]
BASE = REPO / "src/front/templates/base.html"
CSS = REPO / "src/front/static/global/css/main.css"
UTILS = REPO / "src/front/static/global/js/utils.js"
TASKS = REPO / "src/front/static/global/js/task-tracker.js"
HANDLERS = REPO / "src/front/static/global/js/base-ui-handlers.js"


def test_one_activity_icon_and_one_panel():
    html = BASE.read_text(encoding="utf-8")
    assert 'id="activityCenterToggle"' in html
    assert 'bi-inboxes' in html
    assert 'id="activityCenterDropdown"' in html
    assert 'id="taskTrackerList"' in html
    assert 'id="notifCenterList"' in html
    assert html.index("taskTrackerList") < html.index("notifCenterList")
    assert 'id="taskTrackerToggle"' not in html
    assert 'id="notifCenterToggle"' not in html
    assert 'id="taskTrackerDropdown"' not in html
    assert 'id="notifCenterDropdown"' not in html
    assert html.count("activityCenterToggle") == 1


def test_activity_center_js_owns_toggle_and_combined_badge():
    utils = UTILS.read_text(encoding="utf-8")
    tasks = TASKS.read_text(encoding="utf-8")
    handlers = HANDLERS.read_text(encoding="utf-8")
    assert "const ActivityCenter" in utils
    assert "window.ActivityCenter" in utils
    assert "getActiveTaskCount" in tasks
    assert "hasRunningTask" in tasks
    assert "activityCenterToggle" in handlers
    assert "taskTrackerToggle" not in handlers
    assert "notifCenterToggle" not in handlers


def test_activity_center_hidden_on_home_page():
    html = BASE.read_text(encoding="utf-8")
    assert 'id="activityCenterNavItem"' in html
    css = CSS.read_text(encoding="utf-8")
    assert 'body[data-page="home"] #activityCenterNavItem' in css
    assert "display: none !important" in css
