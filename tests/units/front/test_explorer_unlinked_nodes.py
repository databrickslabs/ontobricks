"""Explorer must not keep typed subjects that have no remaining edges."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
D3_JS = REPO_ROOT / "src/front/static/query/js/query-d3graph.js"


def test_build_graph_drops_unlinked_nodes_when_edges_exist() -> None:
    js = D3_JS.read_text(encoding="utf-8")
    assert "const hasEdges = filteredRelationships.length > 0;" in js
    assert "Filtering out unlinked entity:" in js
    keep_type = js.index("// Keep if it has a type (it's a real instance)")
    has_edges = js.index("const hasEdges = filteredRelationships.length > 0;")
    assert has_edges < keep_type
