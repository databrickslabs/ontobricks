"""Unified-chrome Databricks-style app switcher (9-dot, after Close/Save)."""

from pathlib import Path
import re

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
BASE_HTML = REPO_ROOT / "src/front/templates/base.html"
MAIN_CSS = REPO_ROOT / "src/front/static/global/css/main.css"
NAVBAR_JS = REPO_ROOT / "src/front/static/global/js/navbar.js"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_app_switcher_sits_after_close_save_and_lists_main_workspaces():
    html = _read(BASE_HTML)

    assert 'id="obAppSwitcher"' in html
    assert "ob-subnav-app-switcher" in html
    assert "ob-nav-icon" in html
    assert "ob-app-switcher-menu" in html
    assert "ob-app-switcher-list" in html
    assert "ob-app-switcher-grid" not in html
    assert "ob-app-switcher-tile" not in html

    # Fixed right-side order: Context → Close/Save → app switcher → Activity → User.
    assert html.index("ob-subnav-flex-spacer") < html.index("domainContextTrigger")
    assert html.index("domainContextTrigger") < html.index("ob-subnav-close-btn")
    assert html.index("ob-subnav-close-btn") < html.index("ob-subnav-save-btn")
    assert html.index("ob-subnav-save-btn") < html.index("obAppSwitcher")
    assert html.index("obAppSwitcher") < html.index("activityCenterToggle")
    assert html.index("activityCenterToggle") < html.index("userMenuDropdown")

    # Main workspaces only (Mapping lives under Graph). Labels/routes come
    # from menu_config via Jinja (domain / ontology / kg / intel).
    menu = html[html.index("ob-app-switcher-list") : html.index("activityCenterToggle")]
    assert len(re.findall(r'class="ob-app-switcher-item(?:\s[^"]*)?"', menu)) == 5
    assert menu.count("ob-app-switcher-item-check") == 5
    for ns in ("domain_m", "ontology_m", "kg_m", "intel_m", "settings_m"):
        assert f"{ns}.v.route" in menu and "?section=home" in menu, (
            f"App switcher must land on {ns} home"
        )
        assert f"{ns}.v.label" in menu, f"Missing app label for {ns}"
        assert f"{ns}.v.icon" in menu, f"Missing app icon for {ns}"
        assert f"{ns}.v.description" in menu, f"Missing app subtitle for {ns}"
    assert menu.count("ob-app-switcher-item-desc") == 5
    assert menu.count("ob-app-switcher-item-copy") == 5
    assert menu.count("nav-requires-graph") == 2
    assert 'data-requires-app="admin"' in menu
    # Settings is the only admin-gated app-switcher row.
    assert menu.count('data-requires-app="admin"') == 1
    assert "settings_m.v.route" in menu


def test_app_switcher_uses_databricks_line_item_styles():
    """Popover is a vertical list of icon+label rows (not a 2×2 tile grid)."""
    css = _read(MAIN_CSS)

    nav_icon = re.search(r"\.ob-nav-icon\s*\{([^}]*)\}", css)
    assert nav_icon, "Missing .ob-nav-icon (Databricks 9-dot nav-icon)"
    assert "grid-template-columns" in nav_icon.group(1)
    assert "repeat(3" in nav_icon.group(1)

    trigger = re.search(r"\.ob-subnav-app-switcher\s*\{([^}]*)\}", css)
    assert trigger and "transparent" in trigger.group(1)

    listing = re.search(r"\.ob-app-switcher-list\s*\{([^}]*)\}", css)
    assert listing and "flex-direction: column" in listing.group(1)
    assert "grid-template-columns" not in listing.group(1)

    menu_box = re.search(r"\.ob-app-switcher-menu\s*\{([^}]*)\}", css)
    assert menu_box and "min-width: 22rem" in menu_box.group(1)
    assert "border-radius: 0.5rem" in menu_box.group(1)

    item = re.search(r"\.ob-app-switcher-item\s*\{([^}]*)\}", css)
    assert item
    assert "display: flex" in item.group(1)
    assert "flex-direction: column" not in item.group(1)
    assert "align-items: center" in item.group(1)
    assert "border-radius: 0.5rem" in item.group(1)

    icon = re.search(r"\.ob-app-switcher-item-icon\s*\{([^}]*)\}", css)
    assert icon and "border-radius: 0.5rem" in icon.group(1)
    assert "border: 1px solid var(--db-border)" in icon.group(1)

    copy = re.search(r"\.ob-app-switcher-item-copy\s*\{([^}]*)\}", css)
    assert copy and "flex-direction: column" in copy.group(1)
    desc = re.search(r"\.ob-app-switcher-item-desc\s*\{([^}]*)\}", css)
    assert desc and "font-size: 0.72rem" in desc.group(1)

    check = re.search(
        r"\.ob-app-switcher-item\.is-current\s+\.ob-app-switcher-item-check\s*\{([^}]*)\}",
        css,
    )
    assert check and "visibility: visible" in check.group(1)


def test_app_switcher_trigger_is_labelled_feature_selector():
    """The 9-dot trigger is the "Feature selector" (tooltip + accessible name)."""
    html = _read(BASE_HTML)
    trigger = re.search(r'<button\b[^>]*id="obAppSwitcher"[^>]*>', html)
    assert trigger, "Missing #obAppSwitcher trigger"
    tag = trigger.group(0)
    assert 'title="Feature selector"' in tag
    assert 'aria-label="Open feature selector"' in tag
    assert "Switch workspace" not in html


def test_app_switcher_marks_current_workspace_from_route():
    js = _read(NAVBAR_JS)
    assert "ob-app-switcher-item" in js
    assert "initAppSwitcherActiveState" in js
