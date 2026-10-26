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


def _constrained_block(css: str) -> str:
    match = re.search(
        r"@media\s*\(max-width:\s*1199\.98px\)\s*\{(.*?)\n\}",
        css,
        flags=re.DOTALL,
    )
    assert match, "Missing constrained subnav breakpoint"
    return match.group(1)


def test_workspace_targets_are_grouped_before_context_and_actions():
    html = _read(BASE_HTML)
    assert 'id="obSubnav"' in html
    context_li = re.search(
        r'<li\b[^>]*class="[^"]*\bob-subnav-context\b[^"]*"[^>]*>',
        html,
        flags=re.DOTALL,
    )
    assert context_li, "Missing .ob-subnav-context wrapper"
    context_classes = context_li.group(0)
    assert "ob-subnav-item" in context_classes
    assert html.index(context_li.group(0)) < html.index('class="ob-subnav-workspaces"')
    assert html.count('class="ob-subnav-ordinal"') == 4
    workspace_match = re.search(
        r'<li\b[^>]*class="[^"]*\bob-subnav-workspaces\b[^"]*"[^>]*>',
        html,
    )
    assert workspace_match, "Missing .ob-subnav-workspaces wrapper"
    group_start = html.index(workspace_match.group(0))
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
    assert "ob-subnav-save-btn" not in group
    assert "currentDomainName" in html
    assert "domainContextVersion" in html
    assert html.index("ob-subnav-switch-btn") < html.index("ob-subnav-close-btn")
    assert html.index("ob-subnav-close-btn") < html.index("ob-subnav-save-btn")


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

    assert re.search(r"padding\s*:\s*0\.25rem\s+0\.5rem\s+0\.5rem\s*;", sidebar_layout)
    assert re.search(
        r"padding-left\s*:\s*0\.5rem\s*!important\s*;",
        subnav_container,
    )


def test_workspace_targets_stretch_to_near_full_inner_height():
    css = _read(MAIN_CSS)
    group = _rule(css, ".ob-subnav-workspace-list")
    stretch = re.search(
        r"\.ob-subnav-workspace-list\s*>\s*\.ob-subnav-item\s*,\s*"
        r"\.ob-subnav-workspace-list\s*>\s*\.ob-subnav-item\s*>\s*\.ob-subnav-link\s*\{([^}]*)\}",
        css,
        flags=re.DOTALL,
    )

    assert "padding: 0.2rem" in group
    assert stretch, "Missing workspace target stretch rule"
    assert "height: 100%" in stretch.group(1)


def test_workspace_group_uses_shared_segmented_control_tokens():
    css = _read(MAIN_CSS)
    group = _rule(css, ".ob-subnav-workspace-list")
    context_trigger = _rule(css, ".ob-subnav-context-trigger")
    context_icon = _rule(css, ".ob-subnav-context-trigger > .bi")
    context_hover = _rule(css, ".ob-subnav-context-trigger:hover")
    context_copy = _rule(css, ".ob-subnav-context-copy")
    context_name = _rule(css, ".ob-subnav-context-name")
    context_version = _rule(css, ".ob-subnav-context-version")
    context_status = _rule(css, ".ob-subnav-context-copy .domain-status-badge")
    context_focus = _rule(css, ".ob-subnav-context-trigger:focus-visible")
    ordinal = _rule(css, ".ob-subnav-ordinal")
    active = _rule(css, ".ob-subnav-link.active")
    active_ordinal = _rule(css, ".ob-subnav-link.active .ob-subnav-ordinal")
    focus = _rule(css, ".ob-subnav-link:focus-visible")

    assert "background: var(--db-surface-warm)" in group
    assert "border: 1px solid var(--db-border)" in group
    assert "border-radius: var(--db-radius-control)" in group
    assert "background: var(--db-primary-soft)" in context_trigger
    assert "border: 0" in context_trigger
    assert "border: 1px solid var(--db-primary)" not in context_trigger
    assert "border-radius: var(--db-radius-control)" in context_trigger
    assert "color: var(--db-primary-darker)" in context_trigger
    assert "background: var(--db-primary-soft-hover)" in context_hover
    assert "border-color:" not in context_hover
    assert "color: var(--db-primary-darker)" in context_hover
    assert "width: 200px" in context_trigger
    assert "min-width: 200px" in context_trigger
    assert "max-width: 200px" in context_trigger
    assert "justify-content: center" in context_trigger
    assert "position: relative" in context_trigger
    assert "padding-left: 1.75rem" in context_trigger
    assert "padding-right: 0.55rem" in context_trigger
    assert "position: absolute" in context_icon
    assert "left: 0.65rem" in context_icon
    assert "display: grid" in context_copy
    assert "grid-template-columns: minmax(0, 1fr) auto" in context_copy
    assert "grid-template-rows: auto auto" in context_copy
    assert "row-gap: 0.12rem" in context_copy
    assert "align-items: center" in context_copy
    assert "justify-items: center" in context_copy
    assert "width: 100%" in context_copy
    assert "column-gap: 0.4rem" in context_copy
    assert "grid-column: 1" in context_status
    assert "grid-row: 1" in context_status
    assert "justify-self: center" in context_status
    assert "margin-left: 0 !important" in context_status
    assert "grid-column: 1" in context_name
    assert "grid-row: 2" in context_name
    assert "text-align: center" in context_name
    assert "min-width: 0" in context_name
    assert "width: 100%" in context_name
    assert "max-width: 100%" in context_name
    assert "white-space: nowrap" in context_name
    assert "overflow: hidden" in context_name
    assert "text-overflow: ellipsis" in context_name
    assert "grid-column: 2" in context_version
    assert "grid-row: 1 / span 2" in context_version
    assert "align-self: center" in context_version
    assert "justify-self: center" in context_version
    assert "font-size: 0.88rem" in context_version
    assert "font-weight: 700" in context_version
    assert "background: var(--db-primary-darker)" in context_version
    assert "color: var(--db-on-primary)" in context_version
    assert "border-radius: var(--db-radius-control)" in context_version
    assert "white-space: nowrap" in context_version
    assert "box-shadow: var(--db-focus-ring)" in context_focus
    assert "border: 1px solid var(--db-border)" in ordinal
    assert "color: var(--db-text-muted)" in ordinal
    assert "background: var(--db-gradient-primary)" in active
    assert "color: var(--db-on-primary)" in active
    assert "background: var(--db-on-primary)" in active_ordinal
    assert "color: var(--db-primary)" in active_ordinal
    assert "outline: 2px solid transparent" in focus
    assert "box-shadow: var(--db-focus-ring)" in focus
    assert "--ob-subnav-rail-height: 3rem" in css
    nav = _rule(css, ".ob-subnav-nav")
    assert "padding: 0.25rem 0 0" in nav
    assert re.search(
        r"\.ob-subnav-context-trigger\s*,\s*\.ob-subnav-workspace-list\s*\{[^}]*height\s*:\s*var\(--ob-subnav-rail-height\)",
        css,
    )
    assert re.search(
        r"\.ob-subnav-context-trigger\s*,\s*\.ob-subnav-workspace-list\s*\{[^}]*min-height\s*:\s*var\(--ob-subnav-rail-height\)",
        css,
    )
    shared_height_rule = re.search(
        r"\.ob-subnav-context-trigger\s*,\s*\.ob-subnav-workspace-list\s*\{([^}]*)\}",
        css,
        flags=re.DOTALL,
    )
    assert shared_height_rule
    assert "box-sizing: border-box" in shared_height_rule.group(1)
    assert "border-radius: var(--db-radius-control)" in shared_height_rule.group(1)
    assert "background:" not in shared_height_rule.group(1)
    assert "border: 1px solid" not in shared_height_rule.group(1)


def test_obsolete_l1_domain_rules_are_removed():
    css = _read(MAIN_CSS)

    assert "#domainL1Link.active" not in css
    assert "#domainL1Link.active:hover" not in css
    assert ".ob-nav-path-sep" not in css


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
    assert "domainContextVersion.textContent = hasDomain ? String(version) : '';" in js


def test_save_is_the_only_primary_filled_domain_action():
    css = _read(MAIN_CSS)
    save = _rule(css, ".ob-subnav-save-btn")
    switch = _rule(css, ".ob-subnav-switch-btn")
    switch_hover = _rule(css, ".ob-subnav-switch-btn:hover")
    close = _rule(css, ".ob-subnav-close-btn")

    assert "background: var(--db-gradient-primary)" in save
    assert "box-shadow: var(--db-shadow-primary)" in save
    assert "background-color: var(--db-surface-warm)" in switch
    assert "background-color: var(--db-hover-indigo)" in switch_hover
    assert "background-color: var(--db-surface-warm)" in close
    assert "color: var(--db-status-danger)" in close


def test_mobile_contract_does_not_add_horizontal_scrolling():
    mobile = _mobile_block(_read(MAIN_CSS))

    assert re.search(r"\.ob-subnav-context-copy\s*\{[^}]*display\s*:\s*none", mobile)
    assert re.search(r"\.ob-subnav-ordinal\s*\{[^}]*display\s*:\s*none", mobile)
    assert re.search(r"\.ob-subnav-label\s*\{[^}]*display\s*:\s*none", mobile)
    assert ".ob-subnav" in mobile
    assert ".ob-subnav-nav" in mobile
    assert ".ob-subnav-workspace-list" in mobile
    assert re.search(r"\.ob-subnav-context-trigger\s*\{[^}]*width\s*:\s*auto", mobile)
    assert re.search(r"\.ob-subnav-context-trigger\s*\{[^}]*min-width\s*:\s*2rem", mobile)
    assert re.search(r"\.ob-subnav-context-trigger\s*\{[^}]*max-width\s*:\s*none", mobile)
    assert re.search(r"\.ob-subnav-context-trigger\s*\{[^}]*padding-inline\s*:\s*0\.5rem", mobile)
    assert not re.search(r"\.ob-subnav-context-trigger\s*\{[^}]*padding-left\s*:", mobile)
    assert not re.search(r"\.ob-subnav-context-trigger\s*\{[^}]*padding-right\s*:", mobile)
    assert re.search(r"\.ob-subnav-context-trigger\s*>\s*\.bi\s*\{[^}]*position\s*:\s*static", mobile)
    assert re.search(r"\.ob-subnav-context-trigger\s*>\s*\.bi\s*\{[^}]*transform\s*:\s*none", mobile)
    assert "overflow: visible" in mobile
    assert "overflow-x: auto" not in mobile


def test_constrained_contract_hides_breadcrumb_and_clips_action_labels():
    css = _read(MAIN_CSS)
    constrained = _constrained_block(css)

    assert re.search(r"#obBreadcrumbWrap\s*\{[^}]*display\s*:\s*none\s*!important", constrained)
    assert ".ob-subnav-context-trigger" not in constrained
    for declaration in (
        "position: absolute",
        "width: 1px",
        "height: 1px",
        "padding: 0",
        "margin: -1px",
        "overflow: hidden",
        "clip: rect(0, 0, 0, 0)",
        "white-space: nowrap",
        "border: 0",
    ):
        assert declaration in constrained
    assert not re.search(
        r"\.ob-subnav-(?:resume-btn|switch-btn|close-btn|save-btn)\s+\.ob-subnav-label\s*\{[^}]*display\s*:\s*none",
        constrained,
    )
    assert "visibility: hidden" not in constrained
