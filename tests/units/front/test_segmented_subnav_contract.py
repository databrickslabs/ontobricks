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


def test_chrome_layout_is_brand_context_actions_switcher_utilities():
    """Workspace dropdown rail is gone; app switcher owns Context→Intelligence nav."""
    html = _read(BASE_HTML)
    assert 'id="obSubnav"' in html
    context_li = re.search(
        r'<li\b[^>]*class="[^"]*\bob-subnav-context\b[^"]*"[^>]*>',
        html,
        flags=re.DOTALL,
    )
    assert context_li, "Missing .ob-subnav-context wrapper"
    assert "ob-subnav-item" in context_li.group(0)

    # Retired segmented workspace rail + ordinals.
    assert 'class="ob-subnav-workspaces"' not in html
    assert 'class="ob-subnav-workspace-list"' not in html
    assert 'class="ob-subnav-ordinal"' not in html
    for element_id in (
        "subnavDomainDropdown",
        "subnavOntologyDropdown",
        "subnavKgDropdown",
        "subnavSimDropdown",
        "domainWorkspaceSubnavNav",
        "domainSectionName",
    ):
        assert f'id="{element_id}"' not in html

    assert "currentDomainName" in html
    assert "domainContextVersion" in html
    assert "ob-subnav-version-crumb" not in html
    assert "ob-subnav-crumb-sep" not in html
    assert re.search(
        r'id="domainContextVersion"[^>]*class="[^"]*\bdomain-version-badge\b',
        html,
    )
    assert html.index("obSidebarToggle") < html.index("brandLogoImg")
    assert html.index("brandLogoImg") < html.index("ob-subnav-flex-spacer")
    assert html.index("ob-subnav-flex-spacer") < html.index("domainContextTrigger")
    assert html.index("domainContextTrigger") < html.index("ob-subnav-close-btn")
    assert html.index("ob-subnav-close-btn") < html.index("ob-subnav-save-btn")
    assert html.index("ob-subnav-save-btn") < html.index("obAppSwitcher")
    assert html.index("obAppSwitcher") < html.index("activityCenterToggle")
    assert html.index("activityCenterToggle") < html.index("userMenuDropdown")
    assert "ob-subnav-switch-btn" not in html
    assert "ob-subnav-save-dirty" in html
    assert 'aria-label="' in html  # icon-only Save/Close expose their names


def test_subnav_surface_is_transparent_and_borderless():
    css = _read(MAIN_CSS)
    # Unified chrome keeps former L2 look (canvas grey, no border) even when
    # the element also carries Bootstrap `.navbar`.
    block = _rule(css, ".ob-subnav,\n.ob-subnav.navbar")
    if "transparent" not in block:
        block = _rule(css, ".ob-subnav.navbar")
    assert re.search(r"background(?:-color)?\s*:\s*transparent", block)
    assert re.search(r"border-bottom\s*:\s*(?:0|none)", block)


def test_subnav_left_edge_matches_sidebar_panel_gutter():
    main_css = _read(MAIN_CSS)
    sidebar_css = _read(SIDEBAR_LAYOUT_CSS)
    subnav_container = _rule(main_css, "#obSubnav .container-fluid")
    sidebar_layout = _rule(sidebar_css, ".sidebar-layout")

    assert re.search(r"padding\s*:\s*0\.15rem\s+0\.25rem\s+0\.25rem\s*;", sidebar_layout)
    assert re.search(
        r"padding-left\s*:\s*0\.25rem\s*!important\s*;",
        subnav_container,
    )


def test_context_crumb_and_rail_use_quiet_tokens():
    css = _read(MAIN_CSS)
    crumb = _rule(css, ".ob-subnav-crumb")
    crumb_hover = _rule(css, ".ob-subnav-crumb:hover")

    assert "color: var(--db-text)" in crumb
    assert "background: var(--db-hover-indigo)" in crumb_hover
    assert "--ob-subnav-rail-height: 2.25rem" in css
    nav = _rule(css, ".ob-subnav-nav")
    assert "padding: 0.15rem 0" in nav
    # Segmented workspace rail styles must not return.
    assert ".ob-subnav-workspace-list" not in css
    assert ".ob-subnav-link" not in css


def test_obsolete_l1_domain_rules_are_removed():
    css = _read(MAIN_CSS)

    assert "#domainL1Link.active" not in css
    assert "#domainL1Link.active:hover" not in css
    assert ".ob-nav-path-sep" not in css


def test_app_switcher_owns_current_workspace_state():
    html = _read(BASE_HTML)
    js = _read(NAVBAR_JS)

    assert "initAppSwitcherActiveState" in js
    assert "disableCurrentSubnavDropdown" not in js
    assert 'id="obAppSwitcher"' in html
    assert "`V${version}`" in js
    assert "domainContextVersion" in js
    assert "domain-version-badge" in js or "domainContextVersion" in js


def test_chrome_actions_use_genie_chip_style():
    css = _read(MAIN_CSS)
    dirty = _rule(css, ".ob-subnav-save-dirty")
    action = _rule(css, ".ob-subnav-action")
    action_hover = _rule(css, ".ob-subnav-action:hover")

    assert "--ob-subnav-btn-height: var(--ob-subnav-icon-hit)" in css
    assert "--ob-subnav-btn-radius: var(--ob-subnav-icon-radius)" in css
    assert "--ob-subnav-icon-hit: 2rem;" in css
    assert "--db-btn-radius: 0.25rem;" in css
    assert "height: var(--ob-subnav-btn-height)" in action
    assert "width: var(--ob-subnav-btn-height)" in action
    assert "border-radius: var(--ob-subnav-btn-radius)" in action
    assert "border: 1px solid var(--db-border)" in action
    assert "background: var(--db-surface-warm)" in action
    # Hover matches Activity (no gradient, no Create/Share fill)
    assert "background: var(--db-hover-indigo)" in action_hover
    assert "gradient" not in action_hover
    assert "var(--db-btn-primary)" not in action
    assert "var(--db-btn-primary)" not in action_hover
    # Dirty indicator on Save
    assert "background: var(--db-status-warning)" in dirty
    assert "display: none" in dirty
    assert re.search(r"\.ob-subnav-save-btn\.is-dirty\s+\.ob-subnav-save-dirty\s*\{[^}]*display:\s*inline-block", css)


def test_navbar_icons_share_one_hover_square():
    """Save, Close, Activity and the feature switcher use the same hit square."""
    css = _read(MAIN_CSS)
    assert "--ob-subnav-icon-hit: 2rem;" in css
    assert "--ob-subnav-icon-radius: var(--db-btn-radius);" in css

    for selector in (
        ".ob-subnav-utility-link",
        ".ob-subnav-app-switcher",
        ".ob-subnav-action",
    ):
        block = _rule(css, selector)
        assert "var(--ob-subnav-icon-hit)" in block or "var(--ob-subnav-btn-height)" in block, selector
        assert (
            "var(--ob-subnav-icon-radius)" in block
            or "var(--ob-subnav-btn-radius)" in block
        ), selector

    # Save/Close: bare at rest, Activity hover on hover (no border either way).
    rest = re.search(
        r"\.ob-subnav-save-btn,\s*\.ob-subnav-close-btn\s*\{([^}]*)\}",
        css,
    )
    assert rest and "background: transparent" in rest.group(1) and "border: 0" in rest.group(1)
    hover = re.search(
        r"\.ob-subnav-save-btn:hover,\s*\.ob-subnav-close-btn:hover\s*\{([^}]*)\}",
        css,
    )
    assert hover and "background: var(--db-hover-indigo)" in hover.group(1)
    assert "border: 0" in hover.group(1)


def test_activity_and_user_have_room_for_the_notification_badge():
    css = _read(MAIN_CSS)
    assert (
        ".ob-subnav-nav .ob-subnav-item:has(#activityCenterToggle) + .ob-subnav-item"
        in css
    )
    rule = re.search(
        r"\.ob-subnav-nav\s+\.ob-subnav-item:has\(#activityCenterToggle\)\s*\+\s*\.ob-subnav-item\s*\{([^}]*)\}",
        css,
    )
    assert rule and "margin-inline-start: 0.85rem" in rule.group(1)


def test_chrome_actions_share_databricks_button_class():
    html = _read(BASE_HTML)
    assert 'class="ob-subnav-action ob-subnav-close-btn' in html
    assert 'class="ob-subnav-action ob-subnav-save-btn' in html
    js = (REPO_ROOT / "src/front/static/global/js/edit-lock.js").read_text(
        encoding="utf-8"
    )
    assert "ob-subnav-action ob-subnav-resume-btn" in js
    css = _read(MAIN_CSS)
    resume = _rule(css, ".ob-subnav-resume-btn")
    assert "color: var(--db-status-warning)" in resume
    assert "width: auto" in resume
    resume_hover = _rule(css, ".ob-subnav-resume-btn:hover")
    assert "background: var(--db-hover-indigo)" in resume_hover


def test_mobile_contract_does_not_add_horizontal_scrolling():
    mobile = _mobile_block(_read(MAIN_CSS))

    assert re.search(r"\.ob-subnav-context-copy\s*\{[^}]*display\s*:\s*none", mobile)
    assert re.search(
        r"\.ob-subnav-action:not\(\.ob-subnav-resume-btn\)\s+\.ob-subnav-label\s*\{[^}]*display\s*:\s*none",
        mobile,
    )
    assert ".ob-subnav" in mobile
    assert ".ob-subnav-nav" in mobile
    assert ".ob-subnav-workspace-list" not in mobile
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
