"""Guard Home / Explorer markup so e2e assertions cannot drift again.

The Playwright home/dtwin flows previously asserted a 3-card workflow strip
and a Knowledge Graph sidebar label. Those surfaces were replaced (All
Domains gateway + New Domain CTA; sigmagraph labelled Explorer) but the
e2e tests were not updated on ``develop``. This module pins the current
templates so ``pytest -m "not scenario"`` fails if they regress.
"""

from __future__ import annotations

import json
from pathlib import Path

_FRONT = Path(__file__).resolve().parents[3] / "src" / "front"
_HOME = _FRONT / "templates" / "home.html"
_MENU = _FRONT / "config" / "menu_config.json"


def test_home_is_the_registry_tabs_without_gateway():
    html = _HOME.read_text(encoding="utf-8")
    assert 'id="homeTabs"' in html
    assert "partials/registry/_registry_domains.html" in html
    assert 'id="domainGateway"' not in html
    assert "workflow-card" not in html
    assert 'id="classCount"' not in html
    assert 'id="propCount"' not in html
    assert 'id="mappingCount"' not in html


def test_home_shows_app_version_next_to_title():
    html = _HOME.read_text(encoding="utf-8")
    title_start = html.index("<h1>")
    title_end = html.index("</h1>", title_start)
    hero_title = html[title_start:title_end]
    assert 'class="home-hero-version"' in hero_title
    assert "v{{ app_version }}" in hero_title
    assert ".home-hero-version" in (
        _FRONT / "static" / "global" / "css" / "pages.css"
    ).read_text(encoding="utf-8")


def test_home_has_centered_databrickslabs_credit_at_bottom():
    html = _HOME.read_text(encoding="utf-8")
    credit_start = html.index('class="home-labs-credit"')
    credit = html[credit_start:html.index("</p>", credit_start)]
    assert "https://github.com/databrickslabs" in credit
    assert "Databricks Labs" in credit
    assert "https://www.databricks.com" in credit
    assert "100% powered by" in credit
    # Sits after the resources strip, before the modals.
    assert credit_start > html.index('class="home-resources"')
    assert credit_start < html.index('id="homeRegistryModal"')
    assert ".home-labs-credit" in (
        _FRONT / "static" / "home" / "css" / "home.css"
    ).read_text(encoding="utf-8")


def test_navbar_has_report_issue_link():
    base_html = (_FRONT / "templates" / "base.html").read_text(encoding="utf-8")
    assert 'id="navReportIssue"' in base_html
    assert 'href="https://github.com/databrickslabs/ontobricks/issues"' in base_html
    assert "bi-bug" in base_html
    # Help + Report issue live in the user dropdown (right-most chrome control).
    assert base_html.index("activityCenterToggle") < base_html.index("userMenuDropdown")
    assert base_html.index("userMenuDropdown") < base_html.index('id="helpCenterToggle"')
    assert base_html.index('id="helpCenterToggle"') < base_html.index('id="navReportIssue"')
    assert "Report an issue" in base_html
    assert "ob-user-avatar" in base_html


def test_home_has_resources_strip_with_help_website_github():
    html = _HOME.read_text(encoding="utf-8")
    assert 'id="homeOpenHelp"' in html
    assert "https://ontobricks.org/" in html
    assert "https://github.com/databrickslabs/ontobricks" in html
    assert "HelpCenter.open" in (
        (_FRONT / "static" / "home" / "js" / "home.js").read_text(encoding="utf-8")
    )


def test_home_launch_tiles_use_databricks_jobs_card_pattern():
    """Launch tiles mirror Workspace Home / Databricks Jobs create tiles."""
    html = _HOME.read_text(encoding="utf-8")
    assert "home-nav-tile-head" in html
    assert "home-nav-tile-icon" in html
    assert "home-nav-tile-art" not in html
    assert "home-nav-tile-points" not in html
    assert "home-nav-tile-arrow" not in html
    assert 'id="homeCountContexts"' in html
    assert 'id="homeCountBridges"' in html
    assert 'id="homeCountTasks"' in html

    css = (_FRONT / "static" / "home" / "css" / "home.css").read_text(encoding="utf-8")
    # Hover = primary border + faint ring (like .ob-rule-card), no drop shadow.
    assert "0 2px 8px" not in css
    assert "border-color: var(--db-primary, var(--bs-primary))" in css
    assert "translateY" not in css
    assert "--home-tile-radius" in css


def test_home_resources_strip_links_to_settings():
    html = _HOME.read_text(encoding="utf-8")
    strip = html[html.index('class="home-resources-grid"'):html.index('class="home-labs-credit"')]
    card_start = strip.index('data-home-resource="settings"')
    card = strip[strip.rindex("<a ", 0, card_start):strip.index("</a>", card_start)]
    assert 'href="/settings"' in card
    assert "Settings" in card
    assert "bi-gear-wide-connected" in card
    # Internal navigation: no new tab.
    assert "target=" not in card


def test_home_resources_grid_is_four_columns_on_desktop():
    css = (_FRONT / "static" / "home" / "css" / "home.css").read_text(encoding="utf-8")
    assert "grid-template-columns: repeat(4, minmax(0, 1fr));" in css
    assert "grid-template-columns: repeat(2, minmax(0, 1fr));" in css


def test_sigmagraph_sidebar_label_is_explorer():
    cfg = json.loads(_MENU.read_text(encoding="utf-8"))
    dtwin = next(menu for menu in cfg["menus"] if menu["id"] == "digitaltwin")
    items = [item for group in dtwin["groups"] for item in group["items"]]
    sigma = next(item for item in items if item["id"] == "sigmagraph")
    assert sigma["label"] == "Explorer"
