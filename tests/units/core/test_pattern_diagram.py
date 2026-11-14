"""pattern_diagram: shape-edge rows + shape tokens -> layered diagram model."""

import pytest

from back.core.graph_analysis.pattern_diagram import (
    MAX_DIAGRAM_EDGES,
    build_pattern_diagram,
    local_label,
    node_id,
)

pytestmark = pytest.mark.unit

NS = "http://ex.org/"
CUSTOMER, ORDER, PRODUCT = NS + "Customer", NS + "Order", NS + "Product"


def _row(hop, parent, via, child, direction="out", members=4, total=8):
    return {
        "hop": hop,
        "parent_type": parent,
        "via": via,
        "child_type": child,
        "direction": direction,
        "member_count": members,
        "edge_total": total,
    }


def _diagram(rows, tokens=(), support=4, bins=8):
    return build_pattern_diagram(
        anchor_type=CUSTOMER, support=support, edge_rows=rows, tokens=tokens, bins=bins
    )


def test_local_label_strips_path_and_fragment():
    assert local_label(NS + "a/b#Thing") == "Thing"
    assert local_label(NS + "hasOrder") == "hasOrder"
    assert local_label("") == ""


def test_isolated_pattern_is_just_the_anchor():
    d = _diagram([])
    assert [n["id"] for n in d["nodes"]] == [node_id(0, CUSTOMER)]
    assert d["edges"] == []


def test_nodes_are_hop_and_type_and_edges_follow_the_walk():
    d = _diagram(
        [
            _row(1, CUSTOMER, NS + "hasOrder", ORDER),
            _row(2, ORDER, NS + "hasProduct", PRODUCT, members=2, total=2),
        ]
    )
    assert [(n["hop"], n["type_label"]) for n in d["nodes"]] == [
        (0, "Customer"), (1, "Order"), (2, "Product"),
    ]
    first, second = d["edges"]
    assert (first["source"], first["target"]) == (
        node_id(0, CUSTOMER), node_id(1, ORDER)
    )
    assert first["via_label"] == "hasOrder"
    assert first["share"] == 1.0 and first["avg"] == 2.0
    assert second["share"] == 0.5 and second["avg"] == 1.0


def test_same_type_at_two_hops_gives_two_nodes():
    d = _diagram(
        [
            _row(1, CUSTOMER, NS + "hasOrder", ORDER),
            _row(2, ORDER, NS + "hasOrder", CUSTOMER, direction="in"),
        ]
    )
    ids = {n["id"] for n in d["nodes"]}
    assert {node_id(0, CUSTOMER), node_id(2, CUSTOMER)} <= ids
    assert d["edges"][1]["direction"] == "in"


def test_attribute_tokens_become_chips_on_their_node():
    tokens = [
        f"0||{CUSTOMER}|self|{NS}segment|pro",
        f"0||{CUSTOMER}|self|{NS}score|q3",
        f"0||{CUSTOMER}|self||",
        f"1|{NS}hasOrder|{ORDER}|out|{NS}status|open",
        f"1|{NS}hasOrder|{ORDER}|out",
    ]
    d = _diagram([_row(1, CUSTOMER, NS + "hasOrder", ORDER)], tokens=tokens)
    by_id = {n["id"]: n for n in d["nodes"]}
    assert by_id[node_id(0, CUSTOMER)]["attributes"] == [
        {"prop": "segment", "value": "pro"},
        {"prop": "score", "value": "bin 4/8"},
    ]
    assert by_id[node_id(1, ORDER)]["attributes"] == [
        {"prop": "status", "value": "open"}
    ]


def test_edges_are_capped():
    rows = [_row(1, CUSTOMER, NS + f"p{i}", NS + f"T{i}") for i in range(MAX_DIAGRAM_EDGES + 5)]
    d = _diagram(rows)
    assert len(d["edges"]) == MAX_DIAGRAM_EDGES
    assert d["truncated"] is True
