"""Clarity design contract for shell tokens and immutable chrome."""

import json
from pathlib import Path
import re

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
MAIN_CSS = REPO_ROOT / "src/front/static/global/css/main.css"
COMPONENTS_CSS = REPO_ROOT / "src/front/static/global/css/components.css"
BASE_HTML = REPO_ROOT / "src/front/templates/base.html"
SIDEBAR_LAYOUT_CSS = REPO_ROOT / "src/front/static/global/css/sidebar-layout.css"
ONTOLOGY_MAP_CSS = REPO_ROOT / "src/front/static/ontology/css/ontology-map.css"
FRONTEND_RULE = REPO_ROOT / ".cursor/11-frontend-design.mdc"
MENU_CONFIG = REPO_ROOT / "src/front/config/menu_config.json"
SIDEBAR_NAV_PARTIAL = (
    REPO_ROOT / "src/front/templates/partials/layout/_sidebar_nav.html"
)
ONTOVIZ_CSS = REPO_ROOT / "src/front/static/global/ontoviz/css/ontoviz.css"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _assert_css_ordered_tokens(css_text: str, tokens: list[str]) -> None:
    cursor = 0
    for token in tokens:
        match = re.search(re.escape(token), css_text[cursor:])
        assert match, f"Missing token assignment: {token}"
        cursor += match.end()


def _iter_rule_blocks(css_text: str) -> list[tuple[str, str]]:
    return re.findall(r"([^{}]+)\{([^{}]*)\}", css_text, flags=re.DOTALL)


def _rule_blocks_for_exact_class(css_text: str, class_selector: str) -> list[str]:
    class_pattern = re.compile(
        rf"(?<![A-Za-z0-9_-]){re.escape(class_selector)}(?![A-Za-z0-9_-])"
    )
    blocks: list[str] = []
    for selectors, declarations in _iter_rule_blocks(css_text):
        selector_list = [selector.strip() for selector in selectors.split(",")]
        if any(class_pattern.search(selector) for selector in selector_list):
            blocks.append(declarations)
    return blocks


def _any_block_has_declaration(
    blocks: list[str], property_pattern: str, value_pattern: str
) -> bool:
    return any(
        re.search(
            rf"{property_pattern}\s*:\s*{value_pattern}\s*;",
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        for block in blocks
    )


def _menu_config() -> dict:
    return json.loads(_read(MENU_CONFIG))


def _strip_root_palette_block(css_text: str) -> str:
    return re.sub(r":root\s*\{[^}]*\}", "", css_text, flags=re.DOTALL)


def test_main_css_declares_clarity_primary_and_warm_tokens():
    css = _read(MAIN_CSS)
    # Contract keeps canonical uppercase hex for the primary indigo token.
    assert re.search(r"--db-primary\s*:\s*#4F46E5\s*;", css)
    # Regression coverage from review fix: muted text token must remain accessible.
    assert re.search(r"--db-text-muted\s*:\s*#6F6B65\s*;", css)

    expected_tokens = [
        "--db-canvas-warm:",
        "--db-surface-warm:",
        "--db-line-soft:",
        "--db-status-success-soft-bg:",
        "--db-status-warning-soft-bg:",
        "--db-status-danger-soft-bg:",
        "--db-radius-sm:",
        "--db-radius-md:",
        "--db-radius-lg:",
    ]
    for token in expected_tokens:
        assert token in css


def test_components_css_keeps_only_primary_button_filled():
    css = _read(COMPONENTS_CSS)
    primary_blocks = _rule_blocks_for_exact_class(css, ".btn-primary")
    secondary_blocks = _rule_blocks_for_exact_class(css, ".btn-secondary")
    outline_secondary_blocks = _rule_blocks_for_exact_class(css, ".btn-outline-secondary")
    outline_primary_blocks = _rule_blocks_for_exact_class(css, ".btn-outline-primary")

    assert primary_blocks, "No CSS rule targets .btn-primary"
    assert _any_block_has_declaration(
        primary_blocks, r"(?:background|background-color)", r"var\(--db-primary\)"
    )
    assert _any_block_has_declaration(
        primary_blocks, r"color", r"var\(--db-on-primary\)"
    )

    # Semantic outcome: neutral and outline button variants stay visually unfilled.
    assert secondary_blocks, "No CSS rule targets .btn-secondary"
    assert _any_block_has_declaration(
        secondary_blocks, r"(?:background|background-color)", r"transparent"
    )
    assert outline_secondary_blocks, "No CSS rule targets .btn-outline-secondary"
    assert _any_block_has_declaration(
        outline_secondary_blocks, r"(?:background|background-color)", r"transparent"
    )
    assert outline_primary_blocks, "No CSS rule targets .btn-outline-primary"
    assert _any_block_has_declaration(
        outline_primary_blocks, r"(?:background|background-color)", r"transparent"
    )


def test_components_css_keeps_btn_link_unfilled_in_all_states():
    css = _read(COMPONENTS_CSS)
    link_blocks = _rule_blocks_for_exact_class(css, ".btn-link")
    assert link_blocks, "No CSS rule targets .btn-link"
    assert _any_block_has_declaration(
        link_blocks, r"(?:background|background-color)", r"transparent"
    )
    assert _any_block_has_declaration(link_blocks, r"border-color", r"transparent")


def test_components_css_keeps_primary_disabled_after_generic_disabled():
    css = _read(COMPONENTS_CSS)
    generic_disabled_pos = css.find(".btn:disabled,")
    primary_disabled_pos = css.find(".btn-primary:disabled,")
    assert generic_disabled_pos != -1, "Generic disabled rule missing"
    assert primary_disabled_pos != -1, "Primary disabled rule missing"
    assert primary_disabled_pos > generic_disabled_pos


def test_components_css_ob_tabs_active_uses_clarity_primary_tokens():
    css = _read(COMPONENTS_CSS)
    assert re.search(
        r"\.nav-tabs\.ob-tabs\s+\.nav-link\.active,\s*"
        r"\.nav-tabs\.ob-tabs\s+\.nav-item\.show\s+\.nav-link\s*\{"
        r"[^}]*color\s*:\s*var\(--db-primary-darker[^;]*;"
        r"[^}]*background\s*:\s*var\(--db-primary-light[^;]*;"
        r"[^}]*border-bottom-color\s*:\s*var\(--db-primary[^;]*;"
        r"[^}]*border-radius\s*:\s*0\s*!important;",
        css,
        flags=re.DOTALL,
    )

def test_components_css_ob_tabs_strip_rounds_its_top_corners():
    """The strip is the head of the tabs + body shape, so it carries the outer
    rounding on top while the selected tab itself stays square."""
    css = _read(COMPONENTS_CSS)
    strip_blocks = _rule_blocks_for_exact_class(css, ".nav-tabs.ob-tabs")
    assert _any_block_has_declaration(
        strip_blocks,
        r"border-radius",
        r"var\(--db-radius-card[^)]*\)\s+var\(--db-radius-card[^)]*\)\s+0\s+0",
    ), "ob-tabs strip must round only its top-left / top-right corners"

    # The body below stays squared on top so the two form one shape.
    assert re.search(
        r"\.nav-tabs\.ob-tabs\s*\+\s*\.card,[^{]*\{"
        r"[^}]*border-top-left-radius\s*:\s*0\s*!important;"
        r"[^}]*border-top-right-radius\s*:\s*0\s*!important;",
        css,
        flags=re.DOTALL,
    )


def test_ob_tabs_following_card_shares_the_strip_right_inset():
    """Domain / Information uses `ul.ob-tabs` + `.card`. The strip is inset
    12px for the stable scrollbar gutter; `.sidebar-section .card { width:
    100% }` made the card ignore that inset, so the tab head sat short of
    the card's top and right borders."""
    css = _read(COMPONENTS_CSS)
    match = re.search(
        r"\.nav-tabs\.ob-tabs\s*\+\s*\.card,[^{]*\{([^}]+)\}",
        css,
        flags=re.DOTALL,
    )
    assert match, "missing adjacent-sibling rule for ob-tabs + card"
    block = match.group(1)
    assert re.search(r"margin-right\s*:\s*12px\s*;", block)
    assert re.search(r"margin-top\s*:\s*0\s*;", block)
    assert re.search(r"width\s*:\s*auto\s*;", block)


def test_designer_canvas_selection_uses_clarity_indigo():
    """The Designer canvas highlighted selection with Bootstrap blue (#0d6efd),
    which is outside the palette."""
    css = _read(ONTOLOGY_MAP_CSS)

    assert "0d6efd" not in css.lower()
    assert "13, 110, 253" not in css

    for selector in (".map-node.selected .map-node-hitarea", ".map-link.highlighted"):
        blocks = _rule_blocks_for_exact_class(css, selector)
        assert blocks, f"missing rule for {selector}"
        assert any("--db-primary" in block for block in blocks), selector


def test_main_css_l2_and_nav_tabs_use_square_indigo_soft_selection():
    css = _read(MAIN_CSS)
    assert re.search(
        r"\.ob-subnav-link\.active\s*\{"
        r"[^}]*background\s*:\s*var\(--db-primary-light\)\s*;"
        r"[^}]*border-radius\s*:\s*0\s*;",
        css,
        flags=re.DOTALL,
    )
    assert re.search(
        r"\.nav-tabs\s+\.nav-link\s*\{[^}]*border-radius\s*:\s*0\s*;",
        css,
        flags=re.DOTALL,
    )


def test_nav_hover_indigo_token_is_lighter_than_active_fill():
    """Hover tint must stay below the 0.12 active fill so the two read apart."""
    css = _read(MAIN_CSS)
    hover_match = re.search(
        r"--db-hover\s*:\s*rgba\(\s*79\s*,\s*70\s*,\s*229\s*,\s*(0\.[0-9]+)\s*\)\s*;",
        css,
    )
    assert hover_match, "--db-hover token missing"
    assert float(hover_match.group(1)) < 0.12
    assert re.search(r"--db-hover-indigo\s*:\s*var\(--db-hover\)\s*;", css)


def test_navbar_dropdown_hover_matches_sidebar():
    css = _read(MAIN_CSS)
    sidebar = _read(SIDEBAR_LAYOUT_CSS)
    assert re.search(
        r"\.navbar\s+\.dropdown-menu\s+\.dropdown-item:hover,\s*"
        r"\.navbar\s+\.dropdown-menu\s+\.dropdown-item:focus\s*\{"
        r"[^}]*background\s*:\s*var\(--db-hover-indigo\)\s*;",
        css,
        flags=re.DOTALL,
    )
    assert re.search(
        r"\.navbar\s+\.dropdown-menu\s+\.dropdown-item\s*\{"
        r"[^}]*border-radius\s*:\s*9px\s*;",
        css,
        flags=re.DOTALL,
    )
    assert re.search(
        r"\.sidebar-nav\s+\.nav-link:hover\s*\{[^}]*background\s*:\s*var\(--db-hover-indigo\)\s*;",
        sidebar,
        flags=re.DOTALL,
    )


def test_l2_dropdown_items_match_sidebar_hover_and_active():
    css = _read(MAIN_CSS)
    assert re.search(
        r"\.ob-subnav\s+\.dropdown-menu\s+\.dropdown-item:hover,\s*"
        r"\.ob-subnav\s+\.dropdown-menu\s+\.dropdown-item:focus\s*\{"
        r"[^}]*background\s*:\s*var\(--db-hover-indigo\)\s*;",
        css,
        flags=re.DOTALL,
    )
    assert re.search(
        r"\.ob-subnav\s+\.dropdown-menu\s+\.dropdown-item\s*\{"
        r"[^}]*border-radius\s*:\s*9px\s*;",
        css,
        flags=re.DOTALL,
    )
    assert re.search(
        r"\.ob-subnav\s+\.dropdown-menu\s+\.dropdown-item\.active\s*\{"
        r"[^}]*background\s*:\s*var\(--db-primary-light\)\s*;",
        css,
        flags=re.DOTALL,
    )


def test_base_template_keeps_logo_nav_structure_and_l1_before_l2():
    html = _read(BASE_HTML)
    menu = _menu_config()
    domain_menu = next(menu_item for menu_item in menu["menus"] if menu_item["id"] == "domain")
    save_action = next(
        action for action in domain_menu["navbar_actions"] if action["action"] == "domainSave"
    )

    assert 'id="brandLogoImg"' in html
    assert 'data-brand-icon' in html
    assert 'data-brand-title' in html
    assert "{{ _branding.logo_url }}" in html
    assert 'id="obSubnav"' in html
    assert save_action["element_id"] == "menuSaveDomain"
    # Base template contract is dynamic: id comes from save_action.v.element_id.
    assert 'class="ob-subnav-save-btn btn-requires-domain"' in html
    assert '{% if save_action.v.element_id %}id="{{ save_action.v.element_id }}"{% endif %}' in html

    l1_pos = html.index('<nav class="navbar navbar-expand-lg navbar-light">')
    l2_pos = html.index('<nav id="obSubnav"')
    assert l1_pos < l2_pos


def test_sidebar_layout_preserves_geometry_and_clarity_active_tokens():
    css = _read(SIDEBAR_LAYOUT_CSS)

    _assert_css_ordered_tokens(
        css,
        [
            "width: 200px;",
            "min-width: 200px;",
            "width: 48px;",
            "min-width: 48px;",
        ],
    )

    assert re.search(
        r"\.sidebar-nav\s+\.nav-link\.active\s*\{[^}]*background\s*:\s*var\(--db-primary-light\)\s*;[^}]*\}",
        css,
        flags=re.DOTALL,
    )
    assert re.search(
        r"\.sidebar-nav\s+\.nav-link\.active\s*\{[^}]*color\s*:\s*var\(--db-primary-selected-text\)\s*;[^}]*\}",
        css,
        flags=re.DOTALL,
    )


def test_sidebar_is_a_framed_card_like_the_split_panel_panes():
    """The gutter is what lets all four corners be rounded — flush against the
    viewport, a radius would just open a gap."""
    css = _read(SIDEBAR_LAYOUT_CSS)
    blocks = _rule_blocks_for_exact_class(css, ".sidebar-nav")

    assert _any_block_has_declaration(blocks, r"margin", r"0\.5rem")
    assert _any_block_has_declaration(
        blocks, r"border-radius", r"var\(--db-radius-card\)"
    )
    assert _any_block_has_declaration(blocks, r"border", r"1px solid var\(--db-border\)")
    # Margins eat into a fixed-height, overflow-hidden parent.
    assert _any_block_has_declaration(blocks, r"height", r"calc\(100% - 1rem\)")


def test_user_identity_lives_in_the_navbar_not_the_sidebar():
    """One place for the current user, and it is the far right of the navbar."""
    partial = _read(SIDEBAR_NAV_PARTIAL)
    html = _read(BASE_HTML)

    assert "sidebar-user" not in partial
    assert 'id="userMenuDropdown"' in html
    assert "ob-user-menu-email" in html

    # Declared after the right-aligned group opens and after the last control
    # in it, or it is not the right-most item.
    assert (
        html.index('<ul class="navbar-nav ms-auto">')
        < html.index("settingsGearDropdown")
        < html.index("userMenuDropdown")
    )

    # The sidebar CSS for the old footer must go with it.
    assert "sidebar-user" not in _read(SIDEBAR_LAYOUT_CSS)


def test_ontoviz_entity_headers_use_indigo_soft_not_red():
    """Business Views entity cards were tinted light red, outside the palette."""
    css = _read(ONTOVIZ_CSS)

    assert "fdeaea" not in css.lower()
    blocks = _rule_blocks_for_exact_class(css, ".ovz-entity-header")
    assert _any_block_has_declaration(blocks, r"background", r"var\(--db-primary-light\)")


def test_primary_consumers_use_runtime_tokens_outside_root_fallback():
    css_files = [MAIN_CSS, COMPONENTS_CSS, SIDEBAR_LAYOUT_CSS, ONTOVIZ_CSS]
    forbidden = [
        "#4f46e5",
        "rgba(79, 70, 229",
        "79, 70, 229",
    ]

    for css_file in css_files:
        stripped = _strip_root_palette_block(_read(css_file)).lower()
        for literal in forbidden:
            assert literal not in stripped, f"{css_file} still uses literal {literal}"


def test_ontology_map_highlight_reads_primary_token_from_css():
    js = _read(REPO_ROOT / "src/front/static/ontology/js/ontology-map.js")
    assert "--db-primary" in js
    assert "getComputedStyle(document.documentElement)" in js
    # Keep exactly one explicit fallback literal scoped to the helper.
    assert js.count("#4F46E5") == 1
    assert "return value || '#4F46E5';" in js


def test_frontend_design_rule_documents_new_palette_and_immutable_structure():
    rule_text = _read(FRONTEND_RULE)

    required_statements = [
        "#4F46E5",
        "warm",
        "filled primary",
        "semantic tinted status",
        "one-pixel",
        "system font stack",
        "L1",
        "L2",
        "logo",
    ]
    for statement in required_statements:
        assert statement in rule_text


def test_base_template_preserves_css_and_js_contract_ordering():
    html = _read(BASE_HTML)

    css_links = re.findall(r'filename=\'([^\']+\.css)\'', html)
    assert css_links, "No stylesheet references found in base.html"
    assert css_links[-1] == "global/css/permissions.css"
    assert html.index("global/css/permissions.css") < html.index('id="uiBrandingTokens"')
    assert html.index('id="uiBrandingTokens"') < html.index("{% block extra_css %}{% endblock %}")

    defer_scripts = re.findall(
        r"<script\s+defer\s+src=\"\{\{\s*url_for\('static',\s*filename='([^']+\.js)'",
        html,
    )
    assert defer_scripts, "No deferred script references found in base.html"
    assert defer_scripts[0] == "global/js/permissions.js"
