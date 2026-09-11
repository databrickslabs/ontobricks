"""Static contracts for full-height sidebar section content."""

from html.parser import HTMLParser
from pathlib import Path
import re

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
TEMPLATES = REPO_ROOT / "src/front/templates"
SIDEBAR_CSS = REPO_ROOT / "src/front/static/global/css/sidebar-layout.css"
CONFIG_CSS = REPO_ROOT / "src/front/static/global/css/config.css"
WIZARD_CSS = REPO_ROOT / "src/front/static/ontology/css/ontology-wizard.css"
PITFALLS_CSS = REPO_ROOT / "src/front/static/ontology/css/ontology-pitfalls.css"
ONTOLOGY_DQ_CSS = REPO_ROOT / "src/front/static/ontology/css/ontology-dataquality.css"
QUERY_DQ_CSS = REPO_ROOT / "src/front/static/query/css/query-dataquality.css"
QUERY_CHAT_CSS = REPO_ROOT / "src/front/static/query/css/query-chat.css"
BUSINESS_RULES_CSS = (
    REPO_ROOT / "src/front/static/ontology/css/ontology-business-rules.css"
)
TEAMS_CSS = REPO_ROOT / "src/front/static/registry/css/registry-teams.css"


SECTION_INVENTORY = {
    "domain.html": {
        "information",
        "metadata",
        "validation",
        "review",
        "mytasks",
        "discussions",
        "versions",
        "documents",
        "owl-content",
        "r2rml",
        "sync",
        "audit",
    },
    "ontology.html": {
        "wizard",
        "information",
        "import",
        "design",
        "map",
        "entities",
        "relationships",
        "groups",
        "dataquality",
        "swrl",
        "cohorts",
        "axioms",
        "owl",
        "pitfalls",
    },
    "mapping.html": {
        "information",
        "import",
        "design",
        "manual",
        "autoassign",
        "diagnostics",
        "r2rml",
        "sparksql",
    },
    "dtwin.html": {
        "sync",
        "runs",
        "insight",
        "dataquality",
        "reasoning",
        "cohorts",
        "analytics",
        "sigmagraph",
        "graphql",
        "chat",
    },
    "settings.html": {
        "databricks",
        "delta",
        "lakebase",
        "neo4j",
        "global",
        "ui",
        "teams",
        "locks",
        "registry",
        "health",
        "logs",
        "runs",
        "schedule",
        "api",
    },
}

# These section bodies use the shared direct-child .content-section flex chain.
# Long-form pages still enter through the same root so their headers align.
# Domain Documents adds a neutral inner stack so its cards remain in natural
# flow rather than inheriting the direct-child card stretch/overflow contract.
STRETCH_PARTIALS = (
    "partials/domain/_domain_information.html",
    "partials/domain/_domain_metadata.html",
    "partials/domain/_domain_validation.html",
    "partials/domain/_domain_review.html",
    "partials/domain/_domain_mytasks.html",
    "partials/domain/_domain_discussions.html",
    "partials/domain/_domain_versions.html",
    "partials/domain/_domain_documents.html",
    "partials/domain/_domain_owl_content.html",
    "partials/domain/_domain_r2rml.html",
    "partials/domain/_domain_audit.html",
    "partials/ontology/_ontology_wizard.html",
    "partials/ontology/_ontology_information.html",
    "partials/ontology/_ontology_import.html",
    "partials/ontology/_ontology_design.html",
    "partials/ontology/_ontology_entities.html",
    "partials/ontology/_ontology_relationships.html",
    "partials/ontology/_ontology_groups.html",
    "partials/ontology/_ontology_dataquality.html",
    "partials/ontology/_ontology_business_rules.html",
    "partials/ontology/_ontology_cohorts.html",
    "partials/ontology/_ontology_axioms.html",
    "partials/ontology/_ontology_owl_content.html",
    "partials/ontology/_pitfalls.html",
    "partials/mapping/_mapping_information.html",
    "partials/mapping/_mapping_import.html",
    "partials/mapping/_mapping_design.html",
    "partials/mapping/_mapping_manual.html",
    "partials/mapping/_mapping_autoassign.html",
    "partials/mapping/_mapping_diagnostics.html",
    "partials/mapping/_mapping_r2rml.html",
    "partials/mapping/_mapping_sparksql.html",
    "partials/dtwin/_query_sync.html",
    "partials/domain/_domain_runs.html",
    "partials/dtwin/_query_insights.html",
    "partials/dtwin/_query_reasoning.html",
    "partials/dtwin/_query_cohorts.html",
    "partials/dtwin/_query_analytics.html",
    "partials/settings/_settings_teams.html",
    "partials/settings/_settings_runs.html",
    "partials/settings/_settings_schedule.html",
    "partials/settings/_settings_api.html",
)

INLINE_SETTINGS_SECTIONS = {
    "databricks",
    "delta",
    "lakebase",
    "neo4j",
    "global",
    "ui",
    "locks",
    "registry",
    "health",
    "logs",
}

class _RootElementParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._stack: list[str] = []
        self._class_stack: list[set[str]] = []
        self.roots: list[tuple[str, set[str]]] = []
        self.content_root_children: list[tuple[str, set[str]]] = []
        self.modal_count = 0
        self.mismatched_tags: list[tuple[str | None, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = dict(attrs)
        classes = set((attr_map.get("class") or "").split())
        if "modal" in classes:
            self.modal_count += 1
        if not self._stack:
            self.roots.append((tag, classes))
        elif len(self._stack) == 1 and "content-section" in self._class_stack[0]:
            self.content_root_children.append((tag, classes))
        if tag not in {"br", "hr", "img", "input", "link", "meta", "source"}:
            self._stack.append(tag)
            self._class_stack.append(classes)

    def handle_endtag(self, tag: str) -> None:
        if not self._stack or self._stack[-1] != tag:
            self.mismatched_tags.append(
                (self._stack[-1] if self._stack else None, tag)
            )
        if tag in self._stack:
            reverse_index = self._stack[::-1].index(tag)
            start = len(self._stack) - reverse_index - 1
            del self._stack[start:]
            del self._class_stack[start:]


def _read(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8")


def _resolve_include_only_partial(relative_path: str) -> str:
    text = _read(f"src/front/templates/{relative_path}")
    includes = re.findall(r'{%\s*include\s+"([^"]+)"\s*%}', text)
    if "<" not in re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL) and len(includes) == 1:
        return _resolve_include_only_partial(includes[0])
    return text


def _root_elements(relative_path: str) -> list[tuple[str, set[str]]]:
    parser = _RootElementParser()
    parser.feed(_resolve_include_only_partial(relative_path))
    return parser.roots


def _rule_blocks_for_exact_selector(css_text: str, selector: str) -> list[str]:
    blocks = []
    css_text = re.sub(r"/\*.*?\*/", "", css_text, flags=re.DOTALL)
    for selectors, declarations in re.findall(
        r"([^{}]+)\{([^{}]*)\}", css_text, flags=re.DOTALL
    ):
        if selector in [part.strip() for part in selectors.split(",")]:
            blocks.append(declarations)
    return blocks


def _media_body(css_text: str, condition: str) -> str:
    marker = f"@media {condition}"
    assert marker in css_text, f"Missing media query: {condition}"
    start = css_text.index(marker)
    opening_brace = css_text.index("{", start)
    depth = 1
    for index in range(opening_brace + 1, len(css_text)):
        if css_text[index] == "{":
            depth += 1
        elif css_text[index] == "}":
            depth -= 1
            if depth == 0:
                return css_text[opening_brace + 1 : index]
    raise AssertionError(f"Unclosed media query: {condition}")


def _assert_declarations(
    css_path: Path, selector: str, declarations: dict[str, str]
) -> None:
    blocks = _rule_blocks_for_exact_selector(
        css_path.read_text(encoding="utf-8"), selector
    )
    assert blocks, f"{css_path.name}: missing {selector}"
    for prop, value in declarations.items():
        assert any(
            re.search(rf"{re.escape(prop)}\s*:\s*{value}\s*;", block)
            for block in blocks
        ), f"{css_path.name}: {selector} missing {prop}: {value}"


def _assert_winning_declarations(
    css_path: Path, selector: str, declarations: dict[str, str]
) -> None:
    blocks = _rule_blocks_for_exact_selector(
        css_path.read_text(encoding="utf-8"), selector
    )
    assert blocks, f"{css_path.name}: missing {selector}"
    for prop, value in declarations.items():
        matches = [
            match.group(1).strip()
            for block in blocks
            for match in re.finditer(
                rf"(?:^|;)\s*{re.escape(prop)}\s*:\s*([^;]+)",
                block,
            )
        ]
        assert matches, f"{css_path.name}: {selector} missing {prop}"
        assert re.fullmatch(value, matches[-1]), (
            f"{css_path.name}: {selector} winning {prop} is {matches[-1]!r}, "
            f"expected {value}"
        )


@pytest.mark.parametrize("relative_path", STRETCH_PARTIALS)
def test_stretch_partial_markup_is_balanced(relative_path):
    parser = _RootElementParser()
    parser.feed(_resolve_include_only_partial(relative_path))
    assert not parser.mismatched_tags, relative_path
    assert not parser._stack, relative_path


def test_sidebar_section_inventory_is_complete():
    for page_name, expected_ids in SECTION_INVENTORY.items():
        html = _read(f"src/front/templates/{page_name}")
        actual_ids = set(
            re.findall(
                r'id="([^"]+)-section"\s+class="[^"]*\bsidebar-section\b',
                html,
            )
        )
        assert actual_ids == expected_ids, page_name


@pytest.mark.parametrize("relative_path", STRETCH_PARTIALS)
def test_primary_partial_uses_root_content_section(relative_path):
    roots = _root_elements(relative_path)
    assert roots, f"{relative_path} has no root element"
    assert roots[0][0] == "div"
    assert "content-section" in roots[0][1], (
        f"{relative_path} primary surface bypasses the shared stretch chain"
    )
    unexpected_siblings = [
        (tag, classes)
        for tag, classes in roots[1:]
        if tag != "script"
        and "modal" not in classes
        and "swrl-context-menu" not in classes
    ]
    assert not unexpected_siblings, (
        f"{relative_path} leaves primary content outside the stretch root: "
        f"{unexpected_siblings}"
    )


def test_inline_settings_sections_use_direct_content_section():
    html = _read("src/front/templates/settings.html")
    for section_id in INLINE_SETTINGS_SECTIONS:
        marker = f'id="{section_id}-section"'
        section_start = html.index(marker)
        opening_tag_end = html.index(">", section_start) + 1
        next_section = html.find('class="sidebar-section', opening_tag_end)
        body = html[opening_tag_end : next_section if next_section != -1 else len(html)]
        assert re.search(
            r'^\s*<div class="[^"]*\bcontent-section\b',
            body,
        ), f"settings:{section_id} bypasses the shared stretch chain"


def test_domain_documents_cards_keep_natural_flow_inside_neutral_stack():
    parser = _RootElementParser()
    parser.feed(
        _resolve_include_only_partial("partials/domain/_domain_documents.html")
    )
    direct_child_classes = [classes for _, classes in parser.content_root_children]
    assert {"domain-documents-stack"} in direct_child_classes
    assert not any("card" in classes for classes in direct_child_classes)


@pytest.mark.parametrize("relative_path", STRETCH_PARTIALS)
def test_every_modal_remains_a_sibling_of_primary_stretch_root(relative_path):
    html = _resolve_include_only_partial(relative_path)
    parser = _RootElementParser()
    parser.feed(html)
    roots = _root_elements(relative_path)
    assert "content-section" in roots[0][1]
    root_modal_count = sum("modal" in classes for _, classes in roots[1:])
    assert root_modal_count == parser.modal_count, (
        f"{relative_path} has {parser.modal_count - root_modal_count} modal(s) "
        "inside the stretch root"
    )


def test_shared_content_section_chain_stays_min_height_safe():
    css = SIDEBAR_CSS.read_text(encoding="utf-8")
    assert re.search(
        r"\.sidebar-section\s*>\s*\.content-section\s*\{"
        r"[^}]*flex\s*:\s*1\s*;"
        r"[^}]*display\s*:\s*flex\s*;"
        r"[^}]*flex-direction\s*:\s*column\s*;"
        r"[^}]*min-height\s*:\s*0\s*;",
        css,
        flags=re.DOTALL,
    )


def test_fixed_height_dataquality_and_logs_use_shell_vertical_padding():
    _assert_winning_declarations(
        SIDEBAR_CSS,
        ".sidebar-content:has(#dataquality-section.active)",
        {"padding": r"0\s+0\.5rem"},
    )
    _assert_winning_declarations(
        SIDEBAR_CSS,
        "#logs-section.active",
        {"padding": r"0\s+1rem"},
    )


def test_mobile_dataquality_section_exact_selector_restores_natural_flow():
    mobile_css = _media_body(
        QUERY_DQ_CSS.read_text(encoding="utf-8"), "(max-width: 768px)"
    )
    blocks = _rule_blocks_for_exact_selector(
        mobile_css, "#dataquality-section.active"
    )
    assert blocks, "mobile CSS missing exact #dataquality-section.active selector"
    declarations = blocks[-1]
    assert re.search(r"(?:^|;)\s*height\s*:\s*auto\s*;", declarations)
    assert re.search(r"(?:^|;)\s*overflow\s*:\s*visible\s*;", declarations)


def test_mobile_chat_section_exact_selector_restores_natural_flow():
    mobile_css = _media_body(
        QUERY_CHAT_CSS.read_text(encoding="utf-8"), "(max-width: 768px)"
    )
    blocks = _rule_blocks_for_exact_selector(
        mobile_css, "#chat-section.sidebar-section.active"
    )
    assert blocks, (
        "mobile CSS missing exact #chat-section.sidebar-section.active selector"
    )
    declarations = blocks[-1]
    assert re.search(r"(?:^|;)\s*height\s*:\s*auto\s*;", declarations)
    assert re.search(r"(?:^|;)\s*overflow\s*:\s*visible\s*;", declarations)


def test_logs_console_scroll_contract_is_css_owned():
    _assert_declarations(
        CONFIG_CSS,
        "#logsConsoleWrap",
        {"flex": "1", "min-height": "100px", "overflow-y": "auto"},
    )
    template = _read("src/front/templates/partials/settings/_settings_logs.html")
    opening_tag = re.search(r'<div id="logsConsoleWrap"[^>]*>', template)
    assert opening_tag
    assert "style=" not in opening_tag.group(0)


def test_existing_internal_scroll_variants_have_complete_flex_chains():
    chains = (
        (
            REPO_ROOT / "src/front/static/ontology/css/ontology-map.css",
            (
                ("#map-section > .card", {"flex": "1", "min-height": "0"}),
                (
                    "#map-section > .card > .card-body",
                    {"flex": "1", "min-height": "0"},
                ),
                (
                    "#ontology-map-container",
                    {"flex": "1", "overflow": "hidden"},
                ),
            ),
        ),
        (
            QUERY_DQ_CSS,
            (
                (
                    "#dataquality-section .dq-card-fill",
                    {"flex": "1", "min-height": "0", "overflow": "hidden"},
                ),
                (
                    "#dataquality-section .dq-card-fill > .card-body",
                    {"flex": "1", "min-height": "0", "overflow-y": "auto"},
                ),
            ),
        ),
        (
            REPO_ROOT / "src/front/static/query/css/query-sigmagraph.css",
            (
                (
                    "#sigmagraph-section > .visualization-layout",
                    {"flex": "1", "min-height": "0", "overflow": "hidden"},
                ),
                (
                    "#sigmagraph-section .graph-wrapper",
                    {"flex": "1", "min-height": "0", "overflow": "hidden"},
                ),
                (
                    "#sigmagraph-section .graph-panel .card-body",
                    {"flex": "1", "min-height": "0", "overflow": "hidden"},
                ),
            ),
        ),
        (
            REPO_ROOT / "src/front/static/global/css/query.css",
            (
                (
                    "#graphql-section.active #graphiql-container",
                    {"flex": "1", "min-height": "0"},
                ),
                (
                    "#graphiql-container .graphiql-container",
                    {"flex": "1", "min-height": "0"},
                ),
            ),
        ),
        (
            REPO_ROOT / "src/front/static/query/css/query-chat.css",
            (
                (
                    "#chat-section.sidebar-section.active",
                    {"min-height": "0", "overflow": "hidden"},
                ),
                (
                    ".graph-chat-container",
                    {
                        "flex": r"1\s+1\s+auto",
                        "min-height": "0",
                        "overflow": "hidden",
                    },
                ),
                (
                    ".graph-chat-messages",
                    {
                        "flex": r"1\s+1\s+auto",
                        "min-height": "0",
                        "overflow-y": "auto",
                    },
                ),
            ),
        ),
    )
    for css_path, selectors in chains:
        for selector, declarations in selectors:
            _assert_declarations(css_path, selector, declarations)


def test_affected_sidebar_components_use_scoped_internal_scroll_chains():
    chains = (
        (
            WIZARD_CSS,
            (
                (
                    "#wizardTabContent",
                    {"flex": "1", "min-height": "0", "overflow-y": "auto"},
                ),
                (
                    ".wizard-metadata-table-wrap",
                    {"max-height": "none", "overflow-y": "visible"},
                ),
            ),
        ),
        (
            PITFALLS_CSS,
            (
                (
                    "#pitfallsTabContent",
                    {"flex": "1", "min-height": "0", "overflow-y": "auto"},
                ),
                (
                    "#pitfalls-results-pane",
                    {"max-height": "none", "overflow-y": "visible"},
                ),
            ),
        ),
        (
            ONTOLOGY_DQ_CSS,
            (
                (
                    "#dataquality-section .card-body",
                    {
                        "display": "flex",
                        "flex-direction": "column",
                        "min-height": "0",
                        "overflow": "hidden",
                    },
                ),
                (
                    "#dqTabContent",
                    {"flex": "1", "min-height": "0", "overflow-y": "auto"},
                ),
                (
                    ".dq-shapes-list",
                    {"max-height": "none", "overflow-y": "visible"},
                ),
            ),
        ),
        (
            BUSINESS_RULES_CSS,
            (
                (
                    "#swrl-section .card-body",
                    {
                        "display": "flex",
                        "flex-direction": "column",
                        "min-height": "0",
                        "overflow": "hidden",
                    },
                ),
                (
                    "#brTabContent",
                    {"flex": "1", "min-height": "0", "overflow-y": "auto"},
                ),
                (
                    ".br-rules-list",
                    {"max-height": "none", "overflow-y": "visible"},
                ),
                (
                    "#swrlRulesList",
                    {"max-height": "none", "overflow-y": "visible"},
                ),
            ),
        ),
        (
            TEAMS_CSS,
            (
                (
                    ".teams-matrix-wrapper",
                    {"flex": "1", "min-height": "0", "overflow": "auto"},
                ),
            ),
        ),
    )
    for css_path, selectors in chains:
        for selector, declarations in selectors:
            _assert_declarations(css_path, selector, declarations)
