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


def test_user_menu_shows_name_and_email_databricks_style():
    html = BASE.read_text(encoding="utf-8")
    menu = html[html.index("ob-user-menu-identity") : html.index("ob-user-menu-badges")]
    assert "ob-user-menu-name" in menu
    assert "ob-user-menu-email" in menu
    assert "_display_name" in menu
    assert "ob-user-avatar-lg" in menu
    css = CSS.read_text(encoding="utf-8")
    assert ".ob-user-menu .ob-user-menu-name" in css
    # Email stays on one line; avatar stays a true circle under flex layout.
    assert "white-space: nowrap" in css
    assert "flex-shrink: 0" in css
    assert "aspect-ratio: 1 / 1" in css
    assert ".ob-subnav .dropdown-menu.ob-user-menu" in css


@pytest.mark.parametrize(
    "email, expected",
    [
        ("benoit.cayla@databricks.com", "Benoit Cayla"),
        ("first_last@x.io", "First Last"),
        ("jane-doe@x.io", "Jane Doe"),
        ("solo@x.io", "Solo"),
        ("", ""),
        ("@x.io", "@x.io"),
    ],
)
def test_display_name_derived_from_email(email, expected):
    from front.fastapi.dependencies import get_user_display_name

    class _State:
        user_email = email

    class _Req:
        state = _State()

    assert get_user_display_name({"request": _Req()}) == expected


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
