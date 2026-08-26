"""Static contract for the segmented level-2 workspace navigation."""

from pathlib import Path
import re

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
BASE_HTML = REPO_ROOT / "src/front/templates/base.html"
MAIN_CSS = REPO_ROOT / "src/front/static/global/css/main.css"
NAVBAR_JS = REPO_ROOT / "src/front/static/global/js/navbar.js"
SIDEBAR_LAYOUT_CSS = REPO_ROOT / "src/front/static/global/css/sidebar-layout.css"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _rule(css: str, selector: str) -> str:
    match = re.search(rf"{re.escape(selector)}\s*\{{([^}}]*)\}}", css, re.DOTALL)
    assert match, f"Missing CSS rule for {selector}"
    return match.group(1)


def _mobile_block(css: str) -> str:
    match = re.search(
        r"@media\s*\(max-width:\s*767\.98px\)\s*\{(.*?)\n\}",
        css,
        flags=re.DOTALL,
    )
    assert match, "Missing mobile subnav breakpoint"
    return match.group(1)


def test_workspace_targets_are_grouped_before_context_and_actions():
    html = _read(BASE_HTML)
    assert 'id="obSubnav"' in html
    group_start = html.index('<li class="ob-subnav-workspaces">')
    group_end = html.index("<!-- Flex spacer", group_start)
    group = html[group_start:group_end]

    ids = [
        "subnavDomainDropdown",
        "subnavOntologyDropdown",
        "subnavMappingDropdown",
        "subnavKgDropdown",
    ]
    positions = [group.index(element_id) for element_id in ids]

    assert positions == sorted(positions)
    assert 'class="ob-subnav-workspace-list"' in group
    assert "obBreadcrumbWrap" not in group
    assert "menuSaveDomain" not in group


def test_subnav_surface_is_transparent_and_borderless():
    css = _read(MAIN_CSS)
    block = _rule(css, ".ob-subnav")

    assert re.search(r"background(?:-color)?\s*:\s*transparent", block)
    assert re.search(r"border-bottom\s*:\s*(?:0|none)", block)


def test_subnav_left_edge_matches_sidebar_panel_gutter():
    main_css = _read(MAIN_CSS)
    sidebar_css = _read(SIDEBAR_LAYOUT_CSS)
    subnav_container = _rule(main_css, "#obSubnav .container-fluid")
    sidebar_layout = _rule(sidebar_css, ".sidebar-layout")

    assert re.search(r"padding\s*:\s*0\.5rem\s*;", sidebar_layout)
    assert re.search(
        r"padding-left\s*:\s*0\.5rem\s*!important\s*;",
        subnav_container,
    )


def test_workspace_group_uses_shared_segmented_control_tokens():
    css = _read(MAIN_CSS)
    group = _rule(css, ".ob-subnav-workspace-list")
    active = _rule(css, ".ob-subnav-link.active")
    focus = _rule(css, ".ob-subnav-link:focus-visible")

    assert "background: var(--db-surface-warm)" in group
    assert "border: 1px solid var(--db-border)" in group
    assert "border-radius: var(--db-radius-control)" in group
    assert "background: var(--db-primary-light)" in active
    assert "color: var(--db-primary-darker)" in active
    assert "outline: 2px solid transparent" in focus
    assert "box-shadow: var(--db-focus-ring)" in focus


def test_active_target_disables_only_its_dropdown():
    html = _read(BASE_HTML)
    js = _read(NAVBAR_JS)

    assert "if (route && path.startsWith(route))" in js
    assert "disableCurrentSubnavDropdown(link)" in js
    assert "toggle.removeAttribute('data-bs-toggle')" in js
    assert "toggle.classList.remove('dropdown-toggle')" in js
    assert "toggle.setAttribute('aria-current', 'page')" in js
    assert "event.preventDefault()" in js
    assert "if (menu) menu.remove()" in js
    for element_id in (
        "subnavDomainDropdown",
        "subnavOntologyDropdown",
        "subnavMappingDropdown",
        "subnavKgDropdown",
    ):
        tag = re.search(
            rf'<a\b[^>]*id="{element_id}"[^>]*>',
            html,
            flags=re.DOTALL,
        )
        assert tag, element_id
        assert 'data-bs-toggle="dropdown"' in tag.group(0)


def test_save_is_the_only_primary_filled_domain_action():
    css = _read(MAIN_CSS)
    save = _rule(css, ".ob-subnav-save-btn")
    switch = _rule(css, ".ob-subnav-switch-btn")
    switch_hover = _rule(css, ".ob-subnav-switch-btn:hover")
    close = _rule(css, ".ob-subnav-close-btn")

    assert "background-color: var(--db-primary)" in save
    assert "background-color: var(--db-surface-warm)" in switch
    assert "background-color: var(--db-hover-indigo)" in switch_hover
    assert "background-color: var(--db-surface-warm)" in close
    assert "color: var(--db-status-danger)" in close


def test_mobile_contract_does_not_add_horizontal_scrolling():
    mobile = _mobile_block(_read(MAIN_CSS))

    assert ".ob-subnav-label" in mobile
    assert "overflow-x: auto" not in mobile
