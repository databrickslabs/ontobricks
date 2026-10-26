"""Static contract for Knowledge Graph Query -> SPARQL."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]
DTWIN = ROOT / "src/front/templates/dtwin.html"
SHELL = ROOT / "src/front/templates/partials/dtwin/_query_query.html"
GRAPHQL = ROOT / "src/front/templates/partials/dtwin/_query_graphql.html"
SPARQL = ROOT / "src/front/templates/partials/dtwin/_query_sparql.html"
CSS = ROOT / "src/front/static/query/css/query-sparql.css"
GLOBAL_QUERY_CSS = ROOT / "src/front/static/global/css/query.css"
QUERY_JS = ROOT / "src/front/static/query/js/query.js"
JS = ROOT / "src/front/static/query/js/query-execute.js"
SYNC_JS = ROOT / "src/front/static/query/js/query-sync.js"
SIGMA_JS = ROOT / "src/front/static/query/js/query-sigmagraph.js"
MENU = ROOT / "src/front/config/menu_config.json"


def read(path):
    return path.read_text(encoding="utf-8")


def _selector_blocks(css_text, selector):
    css_text = __import__("re").sub(r"/\*.*?\*/", "", css_text, flags=__import__("re").DOTALL)
    blocks = []
    for selectors, declarations in __import__("re").findall(r"([^{}]+)\{([^{}]*)\}", css_text):
        parts = [chunk.strip() for chunk in selectors.split(",")]
        if selector in parts:
            blocks.append(declarations)
    return blocks


def _winning_declaration(css_text, selector, prop):
    import re

    value = None
    for block in _selector_blocks(css_text, selector):
        for match in re.finditer(rf"(?:^|;)\s*{re.escape(prop)}\s*:\s*([^;]+)", block):
            value = match.group(1).strip()
    return value


def _media_block(css_text, condition):
    start = css_text.index(f"@media {condition}")
    opening_brace = css_text.index("{", start)
    depth = 1
    cursor = opening_brace + 1
    while depth:
        if css_text[cursor] == "{":
            depth += 1
        elif css_text[cursor] == "}":
            depth -= 1
        cursor += 1
    return css_text[opening_brace + 1 : cursor - 1]


def _effective_declaration(stylesheets, matching_selectors, prop):
    import re

    winner = None
    source_order = 0
    for css_text in stylesheets:
        css_text = re.sub(r"/\*.*?\*/", "", css_text, flags=re.DOTALL)
        for selectors, declarations in re.findall(r"([^{}]+)\{([^{}]*)\}", css_text):
            for selector in (chunk.strip() for chunk in selectors.split(",")):
                source_order += 1
                if selector not in matching_selectors:
                    continue
                specificity = (
                    len(re.findall(r"#[\w-]+", selector)),
                    len(re.findall(r"\.[\w-]+|\[[^\]]+\]|:(?!:)[\w-]+", selector)),
                    len(re.findall(r"(?:^|[\s>+~])([a-zA-Z][\w-]*)", selector)),
                )
                for match in re.finditer(
                    rf"(?:^|;)\s*{re.escape(prop)}\s*:\s*([^;]+)", declarations
                ):
                    candidate = (specificity, source_order, match.group(1).strip())
                    if winner is None or candidate[:2] >= winner[:2]:
                        winner = candidate
    return winner[2] if winner else None


def test_query_shell_hosts_graphql_and_sparql_tabs():
    shell = read(SHELL)
    assert "GraphQL" in shell
    assert "SPARQL" in shell
    assert 'id="queryGraphqlTab"' in shell
    assert 'id="querySparqlTab"' in shell
    assert 'id="queryGraphqlPane"' in shell
    assert 'id="querySparqlPane"' in shell
    assert "_query_graphql.html" in shell
    assert "_query_sparql.html" in shell


def test_sparql_workspace_has_editor_results_and_actions():
    html = read(SPARQL)
    for element_id in (
        "sparqlPlaygroundQuery",
        "sparqlSample",
        "sparqlResultLimit",
        "sparqlRunBtn",
        "sparqlResultsContainer",
        "sparqlGeneratedSql",
        "sparqlDownloadBtn",
        "sparqlExploreBtn",
    ):
        assert f'id="{element_id}"' in html
    assert 'style="' not in html
    assert "onclick=" not in html


def test_generated_sql_pane_is_persistent_below_editor_and_outside_results():
    html = read(SPARQL)
    editor_start = html.index('<section class="sparql-editor-pane"')
    results_start = html.index('<section class="sparql-results-pane"')
    editor_html = html[editor_start:results_start]
    results_html = html[results_start:]

    assert editor_html.index('id="sparqlPlaygroundQuery"') < editor_html.index(
        '<section class="sparql-sql-pane"'
    )
    assert 'aria-labelledby="sparqlSqlHeading"' in editor_html
    assert 'id="sparqlGeneratedSql">No SQL generated.</code>' in editor_html
    assert "sparql-sql-pane" not in results_html
    assert "sparqlGeneratedSql" not in results_html
    assert "<details" not in html
    assert "<summary" not in html


def test_query_shell_actions_stay_declarative_without_inline_handlers():
    shell = read(SHELL)
    for action in ("ontology", "discussion"):
        assert f'data-query-action="{action}"' in shell
    assert 'data-query-action="switch-domain"' not in shell
    assert "Switch domain" not in shell
    assert "Graph DB:" not in shell
    assert "onclick=" not in shell


def test_graphql_ids_are_preserved():
    html = read(GRAPHQL)
    for element_id in (
        "graphqlLoading",
        "graphqlError",
        "graphqlErrorMsg",
        "graphiql-container",
        "graphqlDepthSelect",
        "graphqlOpenNewTab",
    ):
        assert f'id="{element_id}"' in html


def test_dtwin_wires_query_shell_and_css():
    html = read(DTWIN)
    assert "_query_query.html" in html
    assert "query/css/query-sparql.css" in html


def test_menu_keeps_one_query_item():
    menu = read(MENU)
    assert menu.count('"id": "graphql"') == 1
    block = menu[menu.index('"id": "graphql"') : menu.index('"id": "chat"')]
    assert '"label": "Query"' in block


def test_sparql_css_uses_shared_tokens_and_responsive_stack():
    css = read(CSS)
    assert ".sparql-playground" in css
    assert "var(--db-" in css
    assert "@media" in css
    global_css = read(GLOBAL_QUERY_CSS)
    assert "#sparqlResultsContainer > .gridjs-container" in global_css
    assert "#resultsContainer" not in global_css
    assert (
        _winning_declaration(global_css, "#sparqlResultsContainer", "overflow")
        == "auto"
    )
    assert (
        _winning_declaration(
            global_css, "#sparqlResultsContainer .results-empty-state", "color"
        )
        == "var(--db-text-muted)"
    )


def test_mobile_sparql_stack_restores_natural_flow_and_result_scroll_owner():
    mobile_css = _media_block(read(CSS), "(max-width: 991.98px)")
    assert (
        _winning_declaration(
            mobile_css,
            ".query-language-content:has(> #querySparqlPane.active)",
            "flex",
        )
        == "none"
    )
    assert (
        _winning_declaration(
            mobile_css,
            ".query-language-content:has(> #querySparqlPane.active)",
            "overflow",
        )
        == "visible"
    )
    assert (
        _winning_declaration(mobile_css, "#querySparqlPane.active", "flex")
        == "none"
    )
    assert _winning_declaration(mobile_css, ".query-language-content", "flex") is None
    assert (
        _winning_declaration(
            mobile_css, ".query-language-content > .tab-pane.active", "flex"
        )
        is None
    )
    assert _winning_declaration(mobile_css, ".sparql-playground", "overflow") == "visible"
    assert (
        _winning_declaration(
            mobile_css, ".sparql-playground #sparqlResultsContainer", "overflow"
        )
        == "auto"
    )


def test_mobile_result_minimum_wins_actual_stylesheet_cascade():
    template = read(DTWIN)
    assert template.index("query/css/query-sparql.css") < template.index(
        "global/css/query.css"
    )

    local_mobile_css = _media_block(read(CSS), "(max-width: 991.98px)")
    effective_minimum = _effective_declaration(
        (local_mobile_css, read(GLOBAL_QUERY_CSS)),
        (
            ".sparql-playground #sparqlResultsContainer",
            "#sparqlResultsContainer",
        ),
        "min-height",
    )
    assert effective_minimum == "15rem"


def test_local_results_container_leaves_flex_layout_to_global_owner():
    local_css = read(CSS)
    for prop in ("display", "flex", "min-height", "flex-direction"):
        assert _winning_declaration(local_css, ".sparql-results-container", prop) is None

    assert (
        _winning_declaration(
            local_css, ".sparql-results-container .results-empty-state", "display"
        )
        == "flex"
    )


def test_sparql_grid_scope_uses_only_canonical_shell_color_tokens():
    import re

    css = read(GLOBAL_QUERY_CSS)
    scoped_blocks = []
    for selectors, declarations in re.findall(r"([^{}]+)\{([^{}]*)\}", css):
        if any(
            selector.strip().startswith("#sparqlResultsContainer")
            for selector in selectors.split(",")
        ):
            scoped_blocks.append(declarations)

    scoped_css = "\n".join(scoped_blocks).lower()
    for hardcoded in ("#ffffff", "#fff", "#212529", "#e9ecef", "#6c757d"):
        assert hardcoded not in scoped_css
    for token in (
        "var(--db-surface-warm)",
        "var(--db-text)",
        "var(--db-border)",
        "var(--db-text-muted)",
    ):
        assert token in scoped_css
    assert "var(--db-surface)" not in scoped_css


def test_sparql_keyboard_targets_have_explicit_visible_focus_styles():
    css = read(CSS)
    tab_selector = ".nav-tabs.ob-tabs.query-language-tabs .nav-link:focus-visible"
    assert (
        _winning_declaration(css, tab_selector, "outline")
        == "2px solid var(--db-primary)"
    )
    assert _winning_declaration(css, tab_selector, "box-shadow") == "none"

    control_selectors = (
        "#sparqlSample:focus-visible",
        "#sparqlResultLimit:focus-visible",
        "#sparqlDownloadBtn:focus-visible",
        "#sparqlExploreBtn:focus-visible",
        "#sparqlPlaygroundQuery:focus-visible",
    )
    for selector in control_selectors:
        assert (
            _winning_declaration(css, selector, "outline")
            == "2px solid var(--db-primary)"
        )
        assert (
            _winning_declaration(css, selector, "box-shadow")
            == "var(--db-focus-ring)"
        )


def test_generated_sql_pane_has_fixed_basis_and_owns_internal_scroll():
    css = read(CSS)
    assert (
        _winning_declaration(css, ".sparql-sql-pane", "flex")
        == "0 0 12rem"
    )
    assert (
        _winning_declaration(css, ".sparql-sql-pane", "background")
        == "var(--db-surface-warm)"
    )
    assert (
        _winning_declaration(css, ".sparql-sql-pane", "color")
        == "var(--db-text)"
    )
    assert (
        _winning_declaration(css, ".sparql-sql-pane pre", "overflow")
        == "auto"
    )
    assert (
        _winning_declaration(css, ".sparql-sql-pane pre", "background")
        == "var(--db-canvas-warm)"
    )
    assert (
        _winning_declaration(css, ".sparql-sql-pane pre", "color")
        == "var(--db-text)"
    )
    assert (
        _winning_declaration(css, ".sparql-sql-pane pre code", "color")
        == "var(--db-text)"
    )


def test_query_shell_action_mappings_are_wired_in_js():
    js = read(QUERY_JS)
    assert "switch-domain" not in js
    assert "ontology" in js
    assert "OntologyViewer.open" in js
    assert "discussion" in js
    assert "openTwinDiscussion" in js


def test_sparql_controller_has_samples_and_no_automatic_explorer_switch():
    js = read(JS)
    assert "const SPARQLPlayground" in js
    assert "async function init()" in js
    assert "async function execute()" in js
    assert "function applySample(" in js
    assert "function isTripleProjection(" in js
    execute_body = js[js.index("async function execute()") :]
    execute_body = execute_body[: execute_body.index("function displayResults(")]
    assert "SidebarNav.switchTo('sigmagraph')" not in execute_body


def test_query_page_initializes_only_the_selected_language_tab():
    js = read(QUERY_JS)
    assert "initQueryPlayground" in js
    assert "GraphQLPlayground.init()" in js
    assert "SPARQLPlayground.init()" in js


def test_query_entrypoint_has_no_orphaned_result_state_or_actions():
    import re

    js = read(QUERY_JS)
    assert re.search(r"\b(?:let|const|var)\s+queryResults\b", js) is None
    assert re.search(r"\b(?:let|const|var)\s+generatedSql\b", js) is None
    assert re.search(r"\bfunction\s+copyGeneratedSql\s*\(", js) is None
    assert re.search(r"\bfunction\s+downloadResults\s*\(", js) is None

    # CSV remains owned by the SPARQLPlayground closure.
    assert "function downloadResults()" in read(JS)


def test_sync_load_retains_graph_flow_without_stale_query_ui_coupling():
    js = read(SYNC_JS)
    body = js[js.index("async function loadTripleStore(") :]
    body = body[: body.index("\n/**\n * Update the standalone Insight")]

    for retained in (
        "tripleStoreHasData = count > 0",
        "updateDataMenus()",
        "graphJustBuilt = true",
        "showNotification(`Loaded ${count} triples from triple store`",
        "SidebarNav.switchTo('sigmagraph')",
    ):
        assert retained in body

    for stale in (
        "queryResults",
        "generatedSql",
        "resultCountBadge",
        "resultCount",
        "displayResults(",
    ):
        assert stale not in body


def test_query_reentry_without_pending_tab_does_not_force_graphql():
    js = read(QUERY_JS)
    assert "function getActiveQueryTabName()" in js
    assert "tabName === 'sparql' || tabName === 'graphql'" in js
    init_body = js[js.index("function initQueryPlayground(") :]
    init_body = init_body[: init_body.index("async function _initQueryPage(")]
    assert "const activeTabName = getActiveQueryTabName();" in init_body
    assert "bootstrap.Tab.getOrCreateInstance(tab).show();" in init_body
    assert "if (requestedTab) {" in init_body
    assert "if (activeTabName === 'sparql')" in init_body


def test_execute_failure_does_not_clear_generated_sql():
    js = read(JS)
    execute_body = js[js.index("async function execute()") :]
    execute_body = execute_body[: execute_body.index("function displayResults(")]
    catch_body = execute_body[execute_body.index("} catch (error) {") :]
    catch_body = catch_body[: catch_body.index("} finally {")]
    assert 'displayGeneratedSql("")' not in catch_body
    assert "displayGeneratedSql('')" not in catch_body


def test_generated_sql_display_only_updates_text():
    js = read(JS)
    start = js.index("function displayGeneratedSql(sql)")
    body = js[start : js.index("\n    function displayError(", start)]
    assert 'target.textContent = sql || "No SQL generated.";' in body
    assert 'target.closest("details")' not in body
    assert "disclosure.open" not in body


def test_explorer_switch_uses_local_double_quote_style():
    js = read(JS)
    show_body = js[js.index("function showInExplorer()") :]
    show_body = show_body[: show_body.index("\n    return {")]
    assert 'SidebarNav.switchTo("sigmagraph")' in show_body
    assert "SidebarNav.switchTo('sigmagraph')" not in show_body


def test_explorer_bridge_is_explicit_and_sigma_accepts_query_rows():
    sparql_js = read(JS)
    sigma_js = read(SIGMA_JS)
    assert "SigmaGraph.loadQueryResults" in sparql_js
    assert "loadQueryResults:" in sigma_js
    assert 'SidebarNav.switchTo("sigmagraph")' in sparql_js

    # The bridge must hand off to Sigma directly (no timer race with
    # SigmaGraph's own ~100ms section-entry init) and must switch the
    # section before it loads the rows.
    show_body = sparql_js[sparql_js.index("function showInExplorer()") :]
    show_body = show_body[: show_body.index("\n    return {")]
    assert "setTimeout" not in show_body
    switch_idx = show_body.index('SidebarNav.switchTo("sigmagraph")')
    load_idx = show_body.index("SigmaGraph.loadQueryResults(")
    assert switch_idx < load_idx


def test_load_query_results_starts_its_own_lib_load_before_waiting():
    """`loadQueryResults` must be self-sufficient: it kicks off
    `_loadGraphLibs()` itself, before it awaits `_waitForGraphLibs()`,
    rather than depending on `SigmaGraph.init()` (scheduled by the
    sidebarSectionChanged listener in query.js) having run first."""
    sigma_js = read(SIGMA_JS)
    start = sigma_js.index("loadQueryResults: async function")
    body = sigma_js[start:]
    body = body[: body.index("\n        reload: async function")]
    load_libs_idx = body.index("_loadGraphLibs()")
    wait_libs_idx = body.index("_waitForGraphLibs(10000)")
    assert load_libs_idx < wait_libs_idx


def test_load_query_results_claims_graph_filter_state_before_any_await():
    """The public loader must set `_graphFilterActive` / `lastQueryResults`
    synchronously, before its first `await`, so a concurrently scheduled
    `SigmaGraph.init()` sees the flag already set and takes its
    "already active" branch instead of wiping d3NodesData/d3LinksData out
    from under the rows we are about to render."""
    sigma_js = read(SIGMA_JS)
    start = sigma_js.index("loadQueryResults: async function")
    body = sigma_js[start:]
    body = body[: body.index("\n        reload: async function")]
    first_await_idx = body.index("await ")
    filter_flag_idx = body.index("_graphFilterActive = true;")
    assert filter_flag_idx < first_await_idx
