"""Query and Chat reuse Explorer's live / inferred / sandbox graph picker."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
PARTIAL = REPO_ROOT / "src/front/templates/partials/dtwin"
STATIC_JS = REPO_ROOT / "src/front/static/query/js"
DTWIN_HTML = REPO_ROOT / "src/front/templates/dtwin.html"


def test_query_and_chat_headers_include_graph_source() -> None:
    query = (PARTIAL / "_query_query.html").read_text(encoding="utf-8")
    chat = (PARTIAL / "_query_chat.html").read_text(encoding="utf-8")
    shared = (PARTIAL / "_kg_graph_source.html").read_text(encoding="utf-8")
    query_header = query.split('<div class="section-header', 1)[1].split(
        "query-language-tabs", 1
    )[0]
    chat_header = chat.split('<div class="section-header', 1)[1].split(
        "graph-chat-container", 1
    )[0]
    assert 'select_id="queryGraphSource"' in query_header
    assert 'select_id="chatGraphSource"' in chat_header
    assert 'class="form-select' in shared
    assert "kg-graph-source" in shared
    assert "Live graph (inferred)" in shared
    assert "Live graph" in shared
    assert "onclick=" not in query_header
    assert "onclick=" not in chat_header.split("onto-discuss-btn", 1)[0]


def test_shared_graph_source_script_is_loaded_before_consumers() -> None:
    html = DTWIN_HTML.read_text(encoding="utf-8")
    source_idx = html.find("query/js/query-graph-source.js")
    sigma_idx = html.find("query/js/query-sigmagraph.js")
    graphql_idx = html.find("query/js/query-graphql.js")
    chat_idx = html.find("query/js/query-chat.js")
    execute_idx = html.find("query/js/query-execute.js")
    assert source_idx != -1
    assert source_idx < sigma_idx
    assert source_idx < graphql_idx
    assert source_idx < chat_idx
    assert source_idx < execute_idx


def test_graph_source_module_persists_and_applies_payload() -> None:
    js = (STATIC_JS / "query-graph-source.js").read_text(encoding="utf-8")
    assert "ontobricks_kg_graph_source" in js
    assert "kg-graph-source-change" in js
    assert "function applyToBody(" in js
    assert "simulation_id" in js
    assert "include_inferred" in js
    assert "live:inferred" in js
    assert "/simulation/list" in js
    wanted = js.split("function wanted(explicit)", 1)[1].split("function selects", 1)[0]
    assert wanted.index("wantedFromUrl()") < wanted.index("readStored()")
    assert "if (!stored || stored === LIVE_INFERRED)" not in wanted


def test_graphql_and_chat_and_sparql_send_graph_source() -> None:
    graphql = (STATIC_JS / "query-graphql.js").read_text(encoding="utf-8")
    chat = (STATIC_JS / "query-chat.js").read_text(encoding="utf-8")
    execute = (STATIC_JS / "query-execute.js").read_text(encoding="utf-8")
    tools = (
        REPO_ROOT / "src/agents/agent_dtwin_chat/tools.py"
    ).read_text(encoding="utf-8")
    assert "KgGraphSource.applyToBody" in graphql
    assert "KgGraphSource.payload" in chat
    assert "KgGraphSource.applyToBody" in execute
    assert "_graph_source_query" in tools
    assert "_graph_source_body" in tools
    assert "dtwin_simulation_id" in tools
    assert "include_inferred" in tools


def test_explorer_select_uses_shared_partial() -> None:
    sigma = (PARTIAL / "_query_sigmagraph.html").read_text(encoding="utf-8")
    header = sigma.split('<div class="section-header', 1)[1].split(
        "visualization-layout", 1
    )[0]
    assert 'select_id="sgSimulationSelect"' in header
    assert "_kg_graph_source.html" in header
