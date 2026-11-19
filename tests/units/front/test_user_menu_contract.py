"""L1 user menu: Databricks-style avatar + Help / Report issue items."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO = Path(__file__).resolve().parents[3]
BASE = REPO / "src/front/templates/base.html"
CSS = REPO / "src/front/static/global/css/main.css"


def test_user_menu_uses_initials_avatar_not_person_icon():
    html = BASE.read_text(encoding="utf-8")
    assert "ob-user-avatar" in html
    assert "bi-person-circle" not in html
    assert "_initials" in html
    css = CSS.read_text(encoding="utf-8")
    assert ".ob-user-avatar" in css
    assert "border-radius: 50%" in css


def test_help_and_report_issue_live_in_user_dropdown():
    html = BASE.read_text(encoding="utf-8")
    menu = html[html.index("ob-user-menu") : html.index("</ul>", html.index("ob-user-menu"))]
    assert 'id="helpCenterToggle"' in menu
    assert 'id="navReportIssue"' in menu
    assert "Help" in menu
    assert "Report an issue" in menu
    # No standalone L1 icons for help / bug outside the user menu.
    before = html[: html.index("userMenuDropdown")]
    assert "helpCenterToggle" not in before
    assert "navReportIssue" not in before
