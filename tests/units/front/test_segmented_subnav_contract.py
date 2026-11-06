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
    assert "ob-subnav-item" in context_li.group(0)
    assert html.index(context_li.group(0)) < html.index('class="ob-subnav-workspaces"')
    # Workspace numbering (1-5 ordinals) is retired in the C direction.
    assert 'class="ob-subnav-ordinal"' not in html
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
        "subnavSimDropdown",
    ]
    positions = [group.index(element_id) for element_id in ids]

    assert positions == sorted(positions)
    assert 'class="ob-subnav-workspace-list"' in group
    assert "ob-subnav-save-btn" not in group
    assert "currentDomainName" in html
    assert "domainContextVersion" in html
    # The version crumb owns the switcher (Versions merged into vN ▾).
    version_crumb = re.search(r'<a\b[^>]*class="[^"]*ob-subnav-version-crumb[^"]*"[^>]*>', html)
    assert version_crumb, "Missing version crumb"
    # The id is rendered from menu_config.json (navbar action element_id).
    assert 'id="{{ switch_action_ctx.v.element_id }}"' in version_crumb.group(0)
    assert html.index("ob-subnav-version-crumb") < html.index("ob-subnav-close-btn")
    assert html.index("ob-subnav-close-btn") < html.index("ob-subnav-save-btn")
    assert "ob-subnav-switch-btn" not in html
    assert "ob-subnav-save-dirty" in html
    assert "ob-subnav-save-kbd" in html


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


def test_workspace_group_uses_quiet_segmented_tokens():
    css = _read(MAIN_CSS)
    group = _rule(css, ".ob-subnav-workspace-list")
    crumb = _rule(css, ".ob-subnav-crumb")
    crumb_hover = _rule(css, ".ob-subnav-crumb:hover")
    active = _rule(css, ".ob-subnav-link.active")
    focus = _rule(css, ".ob-subnav-link:focus-visible")

    assert "background: transparent" in group
    assert "border: 0" in group
    assert "border-radius: var(--db-radius-control)" in group
    assert "var(--db-surface-warm)" not in group
    assert "color: var(--db-text)" in crumb
    assert "background: var(--db-hover-indigo)" in crumb_hover
    assert "color: var(--db-primary-darker)" in active
    assert "background: transparent" in active
    assert "box-shadow" not in active
    assert "gradient" not in active
    assert "outline: 2px solid transparent" in focus
    assert "box-shadow: var(--db-focus-ring)" in focus
    assert "--ob-subnav-rail-height: 3rem" in css
    nav = _rule(css, ".ob-subnav-nav")
    assert "padding: 0.25rem 0 0" in nav


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
        "subnavSimDropdown",
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
    dirty = _rule(css, ".ob-subnav-save-dirty")
    close = _rule(css, ".ob-subnav-close-btn")

    assert "background: var(--db-primary)" in save
    assert "color: var(--db-on-primary)" in save
    assert "background: var(--db-status-warning)" in dirty
    assert "display: none" in dirty
    assert re.search(r"\.ob-subnav-save-btn\.is-dirty\s+\.ob-subnav-save-dirty\s*\{[^}]*display:\s*inline-block", css)
    assert "background: transparent" in close
    assert "border: 0" in close
    assert "color: var(--db-status-danger)" in close


def test_mobile_contract_does_not_add_horizontal_scrolling():
    mobile = _mobile_block(_read(MAIN_CSS))

    assert re.search(r"\.ob-subnav-context-copy\s*\{[^}]*display\s*:\s*none", mobile)
    assert re.search(r"\.ob-subnav-save-kbd\s*,\s*\.ob-subnav-crumb-sep\s*\{[^}]*display\s*:\s*none", mobile)
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


def test_constrained_contract_clips_action_labels():
    css = _read(MAIN_CSS)
    constrained = _constrained_block(css)

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
