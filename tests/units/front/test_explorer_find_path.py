"""Contracts for KG Explorer Find Path (shortest paths on the loaded graph)."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SIGMA_HTML = REPO_ROOT / "src/front/templates/partials/dtwin/_query_sigmagraph.html"
SIGMA_JS = REPO_ROOT / "src/front/static/query/js/query-sigmagraph.js"
FIND_PATH_JS = REPO_ROOT / "src/front/static/query/js/query-find-path.js"
DTWIN_HTML = REPO_ROOT / "src/front/templates/dtwin.html"


def test_find_path_header_button_and_context_menu() -> None:
    html = SIGMA_HTML.read_text(encoding="utf-8")
    header = html.split('<div class="section-header', 1)[1].split("visualization-layout", 1)[0]
    assert 'data-sg-action="openFindPathModal"' in header
    assert "bi-signpost-split" in header
    assert "Find Path" in header
    assert 'data-sg-ctx="openFindPathModal"' in html
    ctx = html[html.index('data-sg-ctx="openFindPathModal"') : html.index('data-sg-ctx="openFindPathModal"') + 180]
    assert "bi-signpost-split" in ctx


def test_find_path_modal_uses_search_not_select() -> None:
    html = SIGMA_HTML.read_text(encoding="utf-8")
    start = html.index('id="sgFindPathModal"')
    modal = html[start:]
    assert 'id="sgFindPathSource"' in modal
    assert 'id="sgFindPathTarget"' in modal
    assert "sgFindPathMaxHops" not in modal
    assert "Maximum hops" not in modal
    assert 'data-sg-action="applyFindPath"' in modal
    assert "Highlight path" in modal
    source_block = modal[modal.index('id="sgFindPathSource"') : modal.index('id="sgFindPathTarget"')]
    dest_block = modal[modal.index('id="sgFindPathTarget"') :]
    assert "<select" not in source_block.lower()
    assert "<select" not in dest_block.lower()


def test_find_path_script_is_wired_after_sigmagraph() -> None:
    html = DTWIN_HTML.read_text(encoding="utf-8")
    sigma = html.index("query/js/query-sigmagraph.js")
    find_path = html.index("query/js/query-find-path.js")
    assert find_path > sigma


def test_find_path_module_exposes_bfs_and_search() -> None:
    js = FIND_PATH_JS.read_text(encoding="utf-8")
    assert "function findUndirectedShortestPaths" in js
    assert "function searchLoadedNodes" in js
    assert "function resolveDisplayedNodeId" in js
    assert "function forEachUndirectedNeighbor" in js
    assert "forEachInboundNeighbor" in js
    assert "_memberIds" in js
    path_fn = js[js.index("function findUndirectedShortestPaths") : js.index("function _graph")]
    assert "if (!_isInstanceNode(attrs)) return" not in path_fn
    assert "undirectedEdgeKey" in js


def test_sigmagraph_path_highlight_uses_edge_set() -> None:
    js = SIGMA_JS.read_text(encoding="utf-8")
    assert "_pathHighlightEdges" in js
    assert "_pathHighlightNodes" in js
    assert "openFindPathModal:" in js
    assert "applyPathHighlight:" in js
    reducer = js[js.index("// Find Path: only edges") : js.index("// Search: show edges")]
    assert "_pathHighlightEdges" in reducer
    assert "undirectedEdgeKey" in reducer


def test_click_node_or_canvas_clears_path_highlight() -> None:
    js = SIGMA_JS.read_text(encoding="utf-8")
    click_node = js[js.index("_renderer.on('clickNode'") : js.index("_renderer.on('doubleClickNode'")]
    click_stage = js[js.index("_renderer.on('clickStage'") : js.index("_renderer.on('enterNode'")]
    select_entity = js[js.index("selectEntity: function (entityId)") : js.index("discussNode:")]
    assert "_clearPathHighlight(false)" in click_node
    assert "_clearPathHighlight(false)" in click_stage
    assert "_clearPathHighlight(false)" in select_entity


def test_entity_context_menu_find_path_from_and_to() -> None:
    js = SIGMA_JS.read_text(encoding="utf-8")
    builder = js[js.index("function _showNodeContextMenu") : js.index("function _showExpandInfoBubble")]
    from_idx = builder.index('data-sg-node-action="find-path-from"')
    to_idx = builder.index('data-sg-node-action="find-path-to"')
    expand_idx = builder.index('data-sg-node-action="expandHop"')
    assert from_idx < to_idx < expand_idx
    assert "Find path (From)" in builder
    assert "Find path (To)" in builder
    handler = js[js.index("action === 'find-path-from'") : js.index("action === 'expandHop'")]
    assert "role: action === 'find-path-to' ? 'to' : 'from'" in handler
    assert "getFilterAnchorNodeId:" in js
    assert "_lastExpandedSeedUris" in js[js.index("getFilterAnchorNodeId:") : js.index("applyPathHighlight:")]
    open_js = FIND_PATH_JS.read_text(encoding="utf-8")
    assert "function open(opts)" in open_js
    assert "role === 'to'" in open_js
    assert "_runDirect(filterId, selectedId)" in open_js
    to_branch = open_js[open_js.index("if (role === 'to')") : open_js.index("if (selected) _pick('source'")]
    assert "modal.show" not in to_branch
    assert "getFilterAnchorNodeId" in open_js
