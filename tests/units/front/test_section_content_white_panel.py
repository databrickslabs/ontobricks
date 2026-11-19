"""Sidebar section bodies must sit on a white card under the title.

Reference: Domain → Versions (``.section-header`` then ``.card``).
Domain → Validation previously left tiles on the warm canvas — that is the
defect this contract guards against.
"""

from pathlib import Path
import re

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
TEMPLATES = REPO_ROOT / "src/front/templates"

# Previously bare-on-canvas sections that must now wrap body content in a card
# (title/header stays outside on the warm canvas).
WHITE_PANEL_PARTIALS = (
    "partials/layout/_workspace_home.html",
    "partials/domain/_domain_validation.html",
    "partials/domain/_domain_review.html",
    "partials/domain/_domain_mytasks.html",
    "partials/domain/_domain_discussions.html",
    "partials/domain/_domain_versions.html",
    "partials/settings/_settings_teams.html",
    "partials/settings/_settings_github.html",
    "partials/settings/_settings_schedule.html",
    "partials/settings/_settings_runs.html",
    "partials/dtwin/_query_api.html",
    "partials/dtwin/_query_build.html",
    "partials/dtwin/_query_insights.html",
    "partials/dtwin/_query_chat.html",
)


def _read(rel: str) -> str:
    return (TEMPLATES / rel).read_text(encoding="utf-8")


@pytest.mark.parametrize("relative_path", WHITE_PANEL_PARTIALS)
def test_section_body_uses_white_card_under_title(relative_path: str):
    html = _read(relative_path)

    assert "section-header" in html, f"{relative_path} missing section-header"
    assert re.search(r'class="[^"]*\bcard\b', html), (
        f"{relative_path} must wrap body content in a .card under the title"
    )


def test_settings_locks_and_logs_wrap_includes_in_cards():
    """Locks/Logs headers live in settings.html; body includes must be carded."""
    settings = (TEMPLATES / "settings.html").read_text(encoding="utf-8")

    for include_name in (
        "partials/settings/_settings_locks.html",
        "partials/settings/_settings_logs.html",
    ):
        # The include must sit inside a card > card-body block.
        pattern = (
            r'<div class="card">\s*'
            r'<div class="card-body">\s*'
            rf'\{{%\s*include\s+"{re.escape(include_name)}"\s*%\}}'
        )
        assert re.search(pattern, settings), (
            f"{include_name} must be wrapped in card/card-body in settings.html"
        )


def test_validation_keeps_title_outside_the_white_card():
    """Title stays on the warm canvas; the card starts after the header."""
    html = _read("partials/domain/_domain_validation.html")
    header_pos = html.find("section-header")
    card_pos = html.find('<div class="card">')
    assert header_pos != -1 and card_pos != -1
    assert header_pos < card_pos


def test_overview_title_is_overview_and_has_white_card():
    """Graph/Overview title must be Overview and its body in a white card."""
    html = _read("partials/dtwin/_query_insights.html")
    assert "Overview" in html
    assert "Insights" not in html.split('<div class="section-header', 1)[1].split("</h4>", 1)[0]
    assert '<div class="card">' in html
    header_pos = html.find("section-header")
    card_pos = html.find('<div class="card">')
    assert header_pos < card_pos


def test_sidebar_nav_links_have_sidebar_title_data_attribute():
    """Sidebar nav partial sets data-sidebar-title for tooltip support when collapsed."""
    html = (TEMPLATES / "partials/layout/_sidebar_nav.html").read_text(encoding="utf-8")
    assert 'data-sidebar-title="{{ item.label }}"' in html


def test_sidebar_nav_js_manages_collapsed_tooltips():
    """SidebarNav._updateTooltips configures placement='right' hover tooltips when collapsed."""
    js = (REPO_ROOT / "src/front/static/global/js/sidebar-nav.js").read_text(encoding="utf-8")
    assert "_updateTooltips:" in js
    assert "placement: 'right'" in js
    assert "trigger: 'hover'" in js
    assert "data-sidebar-title" in js
