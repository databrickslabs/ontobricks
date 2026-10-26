"""
E2E — Knowledge Graph page.

Merges two previously separate test files:

* Basic sidebar checks (default section, Knowledge Graph nav link).
* Full sidebar parity — every section declared in the template
  (insight, dataquality, reasoning, sigmagraph, graphql, chat) must be
  reachable via ``SidebarNav.switchTo()``.  A section added to the
  template without a matching ``#{section}-section`` ``<div>`` will
  cause this suite to fail.
"""

import json

import pytest


DTWIN_SECTIONS = [
    "insight",
    "dataquality",
    "reasoning",
    "sigmagraph",
    "graphql",
    "chat",
]


class TestDigitalTwinSidebar:
    """Basic structural checks for the Knowledge Graph page."""

    def test_sigmagraph_section_visible_by_default(self, page, live_server):
        page.goto(f"{live_server}/dtwin/")
        page.wait_for_load_state("domcontentloaded")
        assert page.locator("#sigmagraph-section").is_visible()

    def test_sidebar_knowledge_graph_link(self, page, live_server):
        page.goto(f"{live_server}/dtwin/")
        page.wait_for_load_state("domcontentloaded")
        link = page.locator('a[data-section="sigmagraph"]')
        assert link.is_visible()
        label = (link.text_content() or "").lower()
        # Sidebar label is "Explorer" (Graph Explorer); older builds said
        # "Knowledge Graph" / "Graph".
        assert (
            "explorer" in label
            or "knowledge" in label
            or "graph" in label
        )


class TestDigitalTwinSidebarParity:
    """Every dtwin sidebar section must be reachable via ``SidebarNav``."""

    @pytest.mark.parametrize("section", DTWIN_SECTIONS)
    def test_sidebar_switches_section(self, page, live_server, section):
        page.goto(f"{live_server}/dtwin/")
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(500)
        page.evaluate(f'SidebarNav.switchTo("{section}")')
        page.wait_for_timeout(400)
        section_div = page.locator(f"#{section}-section")
        assert (
            section_div.count() == 1
        ), f"Section #{section}-section is not declared in dtwin.html"
        assert (
            section_div.is_visible()
        ), f"Section #{section}-section is not visible after SidebarNav.switchTo"

    def test_graph_chat_section_has_input(self, page, live_server):
        """The chat panel must expose an input area for the user prompt."""
        page.goto(f"{live_server}/dtwin/")
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(500)
        page.evaluate('SidebarNav.switchTo("chat")')
        page.wait_for_timeout(400)
        panel = page.locator("#chat-section")
        assert panel.is_visible()
        interactable = panel.locator("textarea, input[type='text'], [contenteditable]")
        assert interactable.count() >= 1, "Chat section has no input field"


class TestQueryPlayground:
    def test_query_section_has_both_language_tabs(self, page, live_server):
        page.goto(f"{live_server}/dtwin/?section=graphql")
        page.wait_for_load_state("domcontentloaded")
        page.locator("#queryGraphqlTab").wait_for()
        assert page.locator("#queryGraphqlTab").is_visible()
        assert page.locator("#querySparqlTab").is_visible()

    def test_sparql_deep_link_selects_tab_and_loads_default_query(
        self, page, live_server
    ):
        page.goto(f"{live_server}/dtwin/?section=graphql&tab=sparql")
        page.wait_for_load_state("domcontentloaded")
        page.locator("#sparqlPlaygroundQuery").wait_for(state="visible")
        page.wait_for_function(
            "() => (document.querySelector('#sparqlPlaygroundQuery') || {})"
            ".value.includes('SELECT')"
        )
        query = page.locator("#sparqlPlaygroundQuery").input_value()
        assert "SELECT ?subject ?predicate ?object" in query
        assert "LIMIT 100" in query

    def test_triple_projection_detection(self, page, live_server):
        page.goto(f"{live_server}/dtwin/?section=graphql&tab=sparql")
        page.wait_for_load_state("domcontentloaded")
        page.locator("#sparqlPlaygroundQuery").wait_for(state="visible")
        assert page.evaluate(
            "SPARQLPlayground.isTripleProjection(['subject','predicate','object'])"
        )
        assert page.evaluate(
            "SPARQLPlayground.isTripleProjection(['s','p','o'])"
        )
        assert not page.evaluate(
            "SPARQLPlayground.isTripleProjection(['type','count'])"
        )

    def test_show_in_explorer_switches_then_loads_sigma_bridge_with_query_rows(
        self, page, live_server
    ):
        """Behavioral coverage for the explicit Show-in-Explorer bridge.

        Mocks ``/dtwin/execute`` with a triple-shaped result so the flow
        stays deterministic and network-independent, runs the query through
        the real UI to prove ``#sparqlExploreBtn`` starts disabled and
        becomes enabled only after a triple projection, then stubs
        ``SidebarNav.switchTo`` and ``SigmaGraph.loadQueryResults`` to prove
        the click (a) switches section before loading, and (b) hands the
        loader the exact rows/columns from the executed query — with no
        timer in between.
        """
        page.goto(f"{live_server}/dtwin/?section=graphql&tab=sparql")
        page.wait_for_load_state("domcontentloaded")
        page.locator("#sparqlPlaygroundQuery").wait_for(state="visible")
        page.wait_for_function(
            "() => (document.querySelector('#sparqlPlaygroundQuery') || {})"
            ".value.includes('SELECT')"
        )

        explore_btn = page.locator("#sparqlExploreBtn")
        sql_pane = page.locator(".sparql-sql-pane")
        assert explore_btn.is_disabled()
        assert sql_pane.is_visible()
        assert page.locator(".sparql-results-pane .sparql-sql-pane").count() == 0
        assert page.locator(".sparql-editor-pane .sparql-sql-pane").count() == 1

        triple_result = {
            "success": True,
            "results": [
                {
                    "subject": "http://example.com/a",
                    "predicate": "http://example.com/rel",
                    "object": "http://example.com/b",
                }
            ],
            "columns": ["subject", "predicate", "object"],
            "generated_sql": "SELECT * FROM t",
        }
        page.route(
            "**/dtwin/execute",
            lambda route: route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(triple_result),
            ),
        )

        # Stub the two collaborators the bridge calls so this test never
        # touches the real graph-rendering/CDN-library pipeline.
        page.evaluate(
            """() => {
                window.__explorerCalls = [];
                // SidebarNav is a top-level `const` (no `window.` property);
                // SigmaGraph is a top-level `var`. Reference both as bare
                // globals so the stub actually replaces the binding the
                // bridge closures call through.
                SidebarNav.switchTo = function (section) {
                    window.__explorerCalls.push({ fn: 'switchTo', section });
                };
                SigmaGraph.loadQueryResults = function (rows, columns) {
                    window.__explorerCalls.push({
                        fn: 'loadQueryResults',
                        rows: JSON.parse(JSON.stringify(rows)),
                        columns: JSON.parse(JSON.stringify(columns)),
                    });
                    return Promise.resolve(true);
                };
            }"""
        )

        page.locator("#sparqlRunBtn").click()
        page.locator("#sparqlExploreBtn:not([disabled])").wait_for(state="visible")
        assert not explore_btn.is_disabled()
        assert page.locator("#sparqlGeneratedSql").text_content() == "SELECT * FROM t"

        explore_btn.click()
        page.wait_for_function("window.__explorerCalls.length >= 2")

        calls = page.evaluate("() => window.__explorerCalls")
        assert [c["fn"] for c in calls] == ["switchTo", "loadQueryResults"]
        assert calls[0]["section"] == "sigmagraph"
        assert calls[1]["rows"] == triple_result["results"]
        assert calls[1]["columns"] == triple_result["columns"]
