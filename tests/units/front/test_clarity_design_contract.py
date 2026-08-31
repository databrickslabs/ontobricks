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
ONTOLOGY_PAGE_CSS = REPO_ROOT / "src/front/static/global/css/ontology.css"
MAPPING_PAGE_CSS = REPO_ROOT / "src/front/static/global/css/mapping.css"
MAPPING_DESIGN_JS = REPO_ROOT / "src/front/static/mapping/js/mapping-design.js"
PAGES_CSS = REPO_ROOT / "src/front/static/global/css/pages.css"
QUERY_PAGE_CSS = REPO_ROOT / "src/front/static/global/css/query.css"
QUERY_SYNC_CSS = REPO_ROOT / "src/front/static/query/css/query-sync.css"
SIGMAGRAPH_CSS = REPO_ROOT / "src/front/static/query/css/query-sigmagraph.css"
QUERY_CHAT_CSS = REPO_ROOT / "src/front/static/query/css/query-chat.css"
QUERY_DATAQUALITY_CSS = (
    REPO_ROOT / "src/front/static/query/css/query-dataquality.css"
)
CONFIG_CSS = REPO_ROOT / "src/front/static/global/css/config.css"
SIGMAGRAPH_TEMPLATE = (
    REPO_ROOT / "src/front/templates/partials/dtwin/_query_sigmagraph.html"
)
ONTOLOGY_COHORT_TEMPLATE = (
    REPO_ROOT / "src/front/templates/partials/ontology/_ontology_cohorts.html"
)
QUERY_COHORT_TEMPLATE = (
    REPO_ROOT / "src/front/templates/partials/dtwin/_query_cohorts.html"
)
ONTOLOGY_WIZARD_CSS = REPO_ROOT / "src/front/static/ontology/css/ontology-wizard.css"
ONTOLOGY_WIZARD_TEMPLATE = (
    REPO_ROOT / "src/front/templates/partials/ontology/_ontology_wizard.html"
)
ONTOLOGY_PITFALLS_CSS = (
    REPO_ROOT / "src/front/static/ontology/css/ontology-pitfalls.css"
)
ONTOLOGY_DATAQUALITY_CSS = (
    REPO_ROOT / "src/front/static/ontology/css/ontology-dataquality.css"
)
ONTOLOGY_BUSINESS_RULES_CSS = (
    REPO_ROOT / "src/front/static/ontology/css/ontology-business-rules.css"
)
REGISTRY_TEAMS_CSS = (
    REPO_ROOT / "src/front/static/registry/css/registry-teams.css"
)
FRONTEND_RULE = REPO_ROOT / ".cursor/11-frontend-design.mdc"
CLAUDE_GUIDE = REPO_ROOT / "CLAUDE.md"
FRONTEND_SKILL = REPO_ROOT / ".claude/skills/frontend-design/SKILL.md"
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


def _strip_comments(css_text: str) -> str:
    return re.sub(r"/\*.*?\*/", "", css_text, flags=re.DOTALL)


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


def _rule_blocks_for_exact_selector(css_text: str, selector: str) -> list[str]:
    """Declaration blocks whose selector list contains ``selector`` verbatim.

    Unlike :func:`_rule_blocks_for_exact_class`, a combinator or ``:has()``
    override (e.g. ``.sidebar-content:has(#foo.active)``) does NOT match the
    base selector, so a base-layout contract cannot be satisfied by a
    page-specific override.
    """
    blocks: list[str] = []
    for selectors, declarations in _iter_rule_blocks(_strip_comments(css_text)):
        selector_list = [part.strip() for part in selectors.split(",")]
        if selector in selector_list:
            blocks.append(declarations)
    return blocks


def _media_query_body(css_text: str, feature: str = "max-width") -> str | None:
    """Return the body of the first ``@media`` block whose header mentions
    ``feature``, extracted with balanced-brace matching (not greedy-to-EOF)."""
    css_text = _strip_comments(css_text)
    for match in re.finditer(r"@media[^{]*\{", css_text):
        if feature not in match.group(0):
            continue
        depth = 1
        index = match.end()
        while index < len(css_text) and depth:
            char = css_text[index]
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
            index += 1
        return css_text[match.end() : index - 1]
    return None


def _viewport_height_allowed(selector_parts: list[str], prop: str, value: str) -> bool:
    """Only the root shell may own viewport-height arithmetic.

    ``.sidebar-layout`` may set ``height`` / ``max-height`` to
    ``calc(100vh - var(--ob-chrome-height, ...))``; ``.sidebar-content`` may
    mirror it as ``max-height`` when the approved contract needs it. Every
    other rule / property / value that references ``100vh`` is a pane-level
    offender.
    """
    chrome_calc = re.compile(
        r"^calc\(\s*100vh\s*-\s*var\(\s*--ob-chrome-height\b[^)]*\)\s*\)$"
    )
    if not chrome_calc.match(value):
        return False
    if selector_parts == [".sidebar-layout"] and prop in {"height", "max-height"}:
        return True
    if selector_parts == [".sidebar-content"] and prop == "max-height":
        return True
    return False


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


def test_components_css_ob_tabs_active_is_transparent_with_primary_underline():
    css = _read(COMPONENTS_CSS)
    assert re.search(
        r"\.nav-tabs\.ob-tabs\s+\.nav-link\.active,\s*"
        r"\.nav-tabs\.ob-tabs\s+\.nav-item\.show\s+\.nav-link\s*\{"
        r"[^}]*color\s*:\s*var\(--db-primary-darker[^;]*;"
        r"[^}]*background\s*:\s*transparent\s*!important;"
        r"[^}]*border-bottom-color\s*:\s*var\(--db-primary[^;]*;"
        r"[^}]*border-radius\s*:\s*0\s*!important;",
        css,
        flags=re.DOTALL,
    )


def test_tab_hover_uses_the_brand_aware_indigo_token():
    components = _read(COMPONENTS_CSS)
    component_hover = _rule_blocks_for_exact_class(
        components, ".nav-tabs.ob-tabs .nav-link:hover:not(.active):not(.disabled)"
    )
    assert _any_block_has_declaration(
        component_hover,
        r"background",
        r"var\(--db-hover-indigo[^;]*\)\s*!important",
    )

    main = _read(MAIN_CSS)
    fallback_hover = _rule_blocks_for_exact_class(main, ".nav-tabs .nav-link:hover")
    assert _any_block_has_declaration(
        fallback_hover, r"background", r"var\(--db-hover-indigo[^;]*\)"
    )


def test_components_css_ob_tabs_strip_is_a_transparent_scrollable_rail():
    css = _read(COMPONENTS_CSS)
    strip_blocks = _rule_blocks_for_exact_class(css, ".nav-tabs.ob-tabs")
    for property_name, expected in (
        (r"background", r"transparent"),
        (r"border-radius", r"0"),
        (r"flex-wrap", r"nowrap"),
        (r"overflow-x", r"auto"),
        (r"overflow-y", r"hidden"),
    ):
        assert _any_block_has_declaration(strip_blocks, property_name, expected)
    assert _any_block_has_declaration(
        strip_blocks,
        r"border-bottom",
        r"1px\s+solid\s+var\(--db-border[^;]*\)",
    )


def test_components_css_ob_tabs_has_visible_keyboard_focus():
    css = _read(COMPONENTS_CSS)
    focus_blocks = _rule_blocks_for_exact_class(
        css, ".nav-tabs.ob-tabs .nav-link:focus-visible"
    )
    assert focus_blocks, "ob-tabs must expose a keyboard-only focus state"
    assert _any_block_has_declaration(
        focus_blocks, r"outline", r"2px\s+solid\s+var\(--db-primary[^;]*\)"
    )
    assert _any_block_has_declaration(focus_blocks, r"outline-offset", r"-3px")


def test_components_css_ob_tabs_exposes_standard_and_compact_densities():
    css = _read(COMPONENTS_CSS)
    standard = _rule_blocks_for_exact_class(css, ".nav-tabs.ob-tabs .nav-link")
    compact = _rule_blocks_for_exact_class(
        css, ".nav-tabs.ob-tabs.ob-tabs--compact .nav-link"
    )
    assert _any_block_has_declaration(standard, r"font-size", r"0\.9rem")
    assert _any_block_has_declaration(standard, r"padding", r"0\.5rem\s+0\.875rem")
    assert _any_block_has_declaration(compact, r"font-size", r"0\.75rem")
    assert _any_block_has_declaration(compact, r"padding", r"0\.35rem\s+0\.6rem")

    main = _read(MAIN_CSS)
    fallback = _rule_blocks_for_exact_class(main, ".nav-tabs .nav-link")
    assert _any_block_has_declaration(fallback, r"font-size", r"0\.9rem")
    assert _any_block_has_declaration(
        fallback, r"padding", r"0\.5rem\s+0\.875rem"
    )


def test_data_quality_tabs_do_not_override_the_shared_density():
    css = _read(ONTOLOGY_DATAQUALITY_CSS)
    assert "#dqTabs .nav-link" not in css


def test_ontology_generate_uses_the_card_integrated_tab_pattern():
    template = _read(ONTOLOGY_WIZARD_TEMPLATE)
    assert '<div class="card h-100">' in template
    assert '<div class="card-body p-0 ob-tabs-wrap">' in template
    assert (
        'class="nav nav-tabs ob-tabs nav-fill" id="wizardTabs"'
        in template
    )
    anchor = template.index('id="wizardTabContent"')
    content_tag = template[template.rindex("<div", 0, anchor) : anchor]
    assert "tab-content p-3" in content_tag
    assert "ob-tab-content" not in content_tag


def test_frontend_rule_defines_card_integrated_page_tabs():
    rule = _read(FRONTEND_RULE)
    assert "Card-integrated page tabs" in rule
    assert '<div class="card h-100">' in rule
    assert '<div class="card-body p-0 ob-tabs-wrap">' in rule
    assert 'class="nav nav-tabs ob-tabs nav-fill"' in rule
    assert 'class="tab-content p-3"' in rule
    assert "Domain → Information" in rule
    assert "Ontology → Generate" in rule


def test_claude_frontend_skill_points_to_the_canonical_rule():
    skill = _read(FRONTEND_SKILL)
    assert ".cursor/11-frontend-design.mdc" in skill
    assert "browser" in skill.lower()
    assert "desktop" in skill.lower()
    assert "mobile" in skill.lower()
    assert 'uv run --frozen pytest -q -m "not scenario"' in skill
    assert "frontend-design" in _read(CLAUDE_GUIDE)


def test_ob_tabs_and_content_are_independent_surfaces():
    components = _read(COMPONENTS_CSS)
    assert ".nav-tabs.ob-tabs + .card" not in components
    assert ".nav-tabs.ob-tabs + .ob-tab-content" not in components

    main = _read(MAIN_CSS)
    content = _rule_blocks_for_exact_class(main, ".ob-tab-content")
    assert _any_block_has_declaration(
        content, r"border", r"1px\s+solid\s+var\(--db-border\)"
    )
    assert _any_block_has_declaration(
        content, r"border-radius", r"var\(--db-radius-card\)"
    )


def test_sigmagraph_panel_uses_the_shared_compact_tab_rail():
    template = _read(SIGMAGRAPH_TEMPLATE)
    assert (
        'class="nav nav-tabs ob-tabs ob-tabs--compact sg-panel-tabs"' in template
    )

    css = _read(SIGMAGRAPH_CSS)
    assert ".sg-panel-tabs .nav-link" not in css


def test_mapping_designer_panels_use_the_shared_compact_tab_rail():
    script = _read(MAPPING_DESIGN_JS)
    assert script.count(
        'class="nav nav-tabs ob-tabs ob-tabs--compact"'
    ) == 2


def test_cohort_cards_do_not_masquerade_as_tab_strips():
    for template_path in (ONTOLOGY_COHORT_TEMPLATE, QUERY_COHORT_TEMPLATE):
        template = _read(template_path)
        assert "cohort-tabs-wrap" not in template
        assert 'class="cohort-content-card card"' in template
    ontology_template = _read(ONTOLOGY_COHORT_TEMPLATE)
    assert 'role="tabpanel"' not in ontology_template
    assert 'class="tab-pane' not in ontology_template


def test_orphan_tab_styles_are_removed():
    pages = _read(PAGES_CSS)
    mapping = _read(MAPPING_PAGE_CSS)
    query_sync = _read(QUERY_SYNC_CSS)
    assert ".custom-tabs" not in pages
    assert ".panel-tabs" not in mapping
    assert ".rel-mapping-tabs" not in mapping
    assert ".ob-nav-tabs-gap" not in query_sync


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


def test_main_css_l2_segment_and_nav_tabs_use_indigo_soft_selection():
    css = _read(MAIN_CSS)
    assert re.search(
        r"\.ob-subnav-link\.active\s*\{"
        r"[^}]*background\s*:\s*var\(--db-primary-light\)\s*;"
        r"[^}]*border-radius\s*:\s*calc\(var\(--db-radius-control\)\s*-\s*2px\)\s*;",
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
    """The shell supplies the gutter that exposes all four rounded corners."""
    css = _read(SIDEBAR_LAYOUT_CSS)
    blocks = _rule_blocks_for_exact_class(css, ".sidebar-nav")

    assert _any_block_has_declaration(
        blocks, r"border-radius", r"var\(--db-radius-card\)"
    )
    assert _any_block_has_declaration(blocks, r"border", r"1px solid var\(--db-border\)")


def test_sidebar_layout_centralizes_the_shared_outer_gutter():
    """The shell owns the 0.5rem viewport gutter and content starts flush with
    the sidebar top while retaining its horizontal inset.

    Exact-selector parsing so a ``:has()`` page override cannot satisfy the
    base-layout contract.
    """
    css = _read(SIDEBAR_LAYOUT_CSS)
    layout = _rule_blocks_for_exact_selector(css, ".sidebar-layout")
    sidebar = _rule_blocks_for_exact_selector(css, ".sidebar-nav")
    content = _rule_blocks_for_exact_selector(css, ".sidebar-content")

    assert _any_block_has_declaration(layout, r"gap", r"0\.5rem")
    assert _any_block_has_declaration(layout, r"padding", r"0\.5rem")
    assert _any_block_has_declaration(layout, r"box-sizing", r"border-box")
    assert _any_block_has_declaration(sidebar, r"margin", r"0")
    assert _any_block_has_declaration(sidebar, r"height", r"100%")
    assert _any_block_has_declaration(
        content, r"padding", r"0\s+0\.5rem"
    )


def test_level_two_rail_and_content_share_the_same_vertical_gutter():
    """L2 owns only its top gutter; the shell owns the gutter below it."""
    main_css = _read(MAIN_CSS)
    subnav = _rule_blocks_for_exact_selector(main_css, ".ob-subnav-nav")

    assert _any_block_has_declaration(
        subnav,
        r"padding",
        r"0\.5rem\s+0\s+0",
    )


def test_desktop_sidebar_titles_remove_the_global_top_inset():
    css = _read(SIDEBAR_LAYOUT_CSS)
    desktop_rule = re.search(
        r"@media\s*\(\s*min-width\s*:\s*769px\s*\)\s*\{"
        r"(?P<body>.*?)"
        r"\n\}",
        css,
        flags=re.DOTALL,
    )
    assert desktop_rule, "Missing desktop sidebar-title alignment rules"
    body = desktop_rule.group("body")
    assert re.search(
        r"\.sidebar-layout\s+\.section-header\s*\{"
        r"[^}]*padding-top\s*:\s*0\s*;",
        body,
        flags=re.DOTALL,
    )
    assert "translateY" not in body

    header_blocks = _rule_blocks_for_exact_selector(
        css,
        ".sidebar-layout .section-header",
    )
    assert _any_block_has_declaration(
        header_blocks,
        r"margin-bottom",
        r"0\.5rem\s*!important",
    )


def test_sidebar_layout_has_a_min_height_safe_flex_chain():
    css = _read(SIDEBAR_LAYOUT_CSS)
    content = _rule_blocks_for_exact_selector(css, ".sidebar-content")
    active = _rule_blocks_for_exact_selector(css, ".sidebar-section.active")

    assert _any_block_has_declaration(content, r"display", r"flex")
    assert _any_block_has_declaration(content, r"flex-direction", r"column")
    assert _any_block_has_declaration(content, r"min-height", r"0")
    assert _any_block_has_declaration(active, r"display", r"flex")
    assert _any_block_has_declaration(active, r"flex", r"1(?:\s+1\s+(?:0|auto))?")
    assert _any_block_has_declaration(active, r"min-height", r"0")


def test_only_the_root_shell_owns_viewport_height():
    """Tightened contract: any ``100vh`` reference (bare, or inside a ``calc``
    with any unit/var) is forbidden at pane level. Only ``.sidebar-layout``
    (and ``.sidebar-content`` for a matching ``max-height``) may key off the
    viewport, always through ``--ob-chrome-height``."""
    shell_css = _read(SIDEBAR_LAYOUT_CSS)
    assert re.search(
        r"\.sidebar-layout[^{]*\{[^}]*height\s*:\s*calc\(\s*100vh\s*-\s*"
        r"var\(\s*--ob-chrome-height\b",
        shell_css,
        flags=re.DOTALL,
    ), "sidebar shell must own height via --ob-chrome-height"

    sidebar_page_styles = (
        SIDEBAR_LAYOUT_CSS,
        ONTOLOGY_PAGE_CSS,
        ONTOLOGY_MAP_CSS,
        MAPPING_PAGE_CSS,
        QUERY_PAGE_CSS,
        SIGMAGRAPH_CSS,
        QUERY_CHAT_CSS,
        QUERY_DATAQUALITY_CSS,
        CONFIG_CSS,
        ONTOLOGY_WIZARD_CSS,
        ONTOLOGY_PITFALLS_CSS,
        ONTOLOGY_DATAQUALITY_CSS,
        ONTOLOGY_BUSINESS_RULES_CSS,
        REGISTRY_TEAMS_CSS,
    )
    offenders: list[tuple[str, str, str]] = []
    for path in sidebar_page_styles:
        rel = str(path.relative_to(REPO_ROOT))
        for selectors, declarations in _iter_rule_blocks(_strip_comments(_read(path))):
            selector_parts = [part.strip() for part in selectors.split(",")]
            for declaration in declarations.split(";"):
                if "100vh" not in declaration:
                    continue
                prop, _, value = declaration.partition(":")
                prop = prop.strip().lower()
                value = value.strip()
                if not _viewport_height_allowed(selector_parts, prop, value):
                    offenders.append((rel, selectors.strip(), declaration.strip()))
    assert not offenders, f"pane-level viewport arithmetic remains: {offenders}"


def test_sidebar_layout_restores_natural_flow_on_mobile():
    css = _read(SIDEBAR_LAYOUT_CSS)
    mobile_css = _media_query_body(css, feature="max-width")
    assert mobile_css, "sidebar layout needs a mobile natural-flow override"

    layout = _rule_blocks_for_exact_selector(mobile_css, ".sidebar-layout")
    content = _rule_blocks_for_exact_selector(mobile_css, ".sidebar-content")
    assert _any_block_has_declaration(layout, r"height", r"auto")
    assert _any_block_has_declaration(layout, r"max-height", r"none")
    assert _any_block_has_declaration(layout, r"overflow", r"visible")
    assert _any_block_has_declaration(content, r"max-height", r"none")
    assert _any_block_has_declaration(content, r"overflow(?:-y)?", r"visible")


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
