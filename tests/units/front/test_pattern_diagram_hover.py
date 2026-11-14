"""Static contract for the Patterns diagram hover highlight.

Hovering a node lights up its lineage: ancestors toward the entity (hop 0),
descendants toward further hops, and every relationship on those paths.
Unrelated branches dim. The JS toggles classes only; CSS owns the look.
"""

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

FRONT = Path(__file__).resolve().parents[3] / "src/front"
JS = FRONT / "static/intelligence/js/intelligence-patterns.js"
CSS = FRONT / "static/intelligence/css/intelligence.css"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _method(js: str, name: str) -> str:
    """Body of an object-literal method, up to the next method at the same indent."""
    start = js.index(f"    {name}(")
    rest = js[start + 1 :]
    nxt = re.search(r"\n    (?:async )?[A-Za-z_]\w*\(.*\) \{", rest)
    return js[start : start + 1 + (nxt.start() if nxt else len(rest))]


def test_nodes_are_grouped_with_their_id():
    js = _read(JS)
    node = _method(js, "_diagramNode")
    assert 'class="pat-dg-node-g"' in node
    assert 'data-node-id="${this._esc(n.id)}"' in node


def test_edges_and_labels_carry_their_endpoints():
    js = _read(JS)
    edge = _method(js, "_diagramEdge")
    assert 'data-source="${this._esc(e.source)}"' in edge
    assert 'data-target="${this._esc(e.target)}"' in edge
    # Both the path and its label group must be highlighted together.
    assert edge.count("${ends}") == 2


def test_relationship_name_is_separate_from_the_count_badge():
    js = _read(JS)
    edge = _method(js, "_diagramEdge")
    assert 'class="pat-dg-edge-name"' in edge
    assert "pat-dg-edge-badge-${chip.kind}" in edge
    assert "kind: 'avg'" in edge
    assert "_trim(e.via_label" not in edge
    assert "via_label || this._shortLabel(e.via)" in edge


def test_init_binds_the_delegated_hover():
    js = _read(JS)
    init = _method(js, "init")
    assert "this._bindDiagramHover()" in init
    assert "this._bindDiagramTooltips()" in init


def test_hover_is_delegated_on_document_not_per_svg():
    js = _read(JS)
    bind = _method(js, "_bindDiagramHover")
    assert "document.addEventListener('mouseover'" in bind
    assert "document.addEventListener('mouseout'" in bind
    assert "relatedTarget" in bind


def test_layout_reserves_height_for_parallel_edge_labels():
    js = _read(JS)
    layout = _method(js, "_layoutDiagram")
    assert "edgeSlot" in layout
    assert "labelPad" in layout
    assert "fanIn" in layout
    assert "bandH" in layout


def test_edge_endpoints_stay_on_the_entity_box():
    """Parallel-edge lane offsets must not pull arrows into the empty band."""
    js = _read(JS)
    attach = _method(js, "_edgeAttachY")
    assert "p.y + p.h / 2" in attach
    assert "p.h - 2 * m" in attach
    edge = _method(js, "_diagramEdge")
    assert "this._edgeAttachY(a, outSlot, nOut)" in edge
    assert "this._edgeAttachY(b, inSlot, nIn)" in edge
    # Lane only bends the curve / label, not the endpoint Y.
    assert "b.y + b.h / 2 + dy" not in edge
    assert "lane * 0.35" in edge


def test_relationship_and_badge_carry_tooltip_copy():
    js = _read(JS)
    edge = _method(js, "_diagramEdge")
    assert "data-pat-tip=" in edge
    assert "Average neighbours of this type per matching entity" in edge
    assert "Share of matching entities that have this relationship" in edge
    assert "Relationship" in edge
    tip_bind = _method(js, "_bindDiagramTooltips")
    assert "data-pat-tip" in tip_bind
    assert "_showEdgeTip" in tip_bind
    assert "_hideEdgeTip" in tip_bind


def test_highlight_walks_ancestors_and_descendants_not_undirected():
    """Pattern graphs are one component; undirected BFS would light everything."""
    js = _read(JS)
    hl = _method(js, "_highlightDiagram")
    assert "link(down, s, t)" in hl
    assert "link(up, t, s)" in hl
    assert "walk(id, up)" in hl
    assert "walk(id, down)" in hl
    # Must not fall back to a single undirected adjacency walk.
    assert "link(p.dataset.target, p.dataset.source)" not in hl
    assert "is-focus" in hl and "is-on" in hl and "is-hovering" in hl
    # Edges on the lineage are marked, not every edge between reached nodes
    # from an undirected component.
    assert "onPath" in hl


def test_highlight_reads_data_attrs_via_getAttribute():
    """SVG dataset is uneven across engines; attributes are the source of truth."""
    js = _read(JS)
    hl = _method(js, "_highlightDiagram")
    assert "this._attr(node, 'data-node-id')" in hl
    assert "this._attr(p, 'data-source')" in hl
    assert "this._attr(p, 'data-target')" in hl


def test_clear_resets_state_so_the_same_node_can_be_hovered_again():
    js = _read(JS)
    clr = _method(js, "_clearDiagramHighlight")
    assert "removeAttribute('data-hover-node')" in clr
    assert "is-hovering" in clr
    assert "'is-on', 'is-focus'" in clr


def test_css_dims_unrelated_and_accents_related_paths():
    css = _read(CSS)
    assert ".pat-diagram-svg.is-hovering .pat-dg-node-g:not(.is-on)" in css
    assert ".pat-diagram-svg.is-hovering .pat-dg-edge:not(.is-on)" in css
    assert ".pat-diagram-svg.is-hovering .pat-dg-edge-label:not(.is-on)" in css
    assert ".pat-dg-node-g.is-focus .pat-dg-node" in css
    assert ".pat-diagram-svg.is-hovering .pat-dg-edge.is-on" in css
    assert ".pat-diagram-svg.is-hovering .pat-dg-edge-label.is-on .pat-dg-edge-name" in css
    assert ".pat-dg-edge-badge-bg" in css
    assert "fill: var(--db-primary" in css.split(".pat-dg-edge-badge-bg", 1)[1].split("}", 1)[0]
    assert ".pat-dg-tip" in css
    dim = css.split(".pat-diagram-svg.is-hovering .pat-dg-node-g:not(.is-on)", 1)[1]
    assert "opacity: 0.08" in dim.split("}", 1)[0]
    assert "blur(" in dim.split("}", 1)[0]
    accent = css.split(".pat-diagram-svg.is-hovering .pat-dg-edge.is-on", 1)[1]
    assert "var(--db-primary" in accent.split("}", 1)[0]


def test_no_inline_handlers_added_to_the_diagram():
    js = _read(JS)
    diagram = _method(js, "_diagramSvg")
    assert "onmouse" not in diagram
    assert "onclick" not in diagram


def test_each_hop_column_has_a_dotted_lane_not_gutters():
    js = _read(JS)
    diagram = _method(js, "_diagramSvg")
    assert 'class="pat-dg-lane"' in diagram
    assert "L.maxHop + 1" in diagram
    assert "pat-dg-lane-gutter" not in diagram
    css = _read(CSS)
    assert ".pat-dg-lane" in css
    assert ".pat-dg-lane-gutter" not in css
    lane = css.split(".pat-dg-lane", 1)[1].split("}", 1)[0]
    assert "stroke-dasharray" in lane
    assert "pointer-events: none" in lane
    assert "#F4F2EF" not in lane
    assert "non-scaling-stroke" in lane
