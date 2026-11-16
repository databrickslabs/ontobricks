"""pattern_diagram: shape-edge rows + shape tokens -> layered diagram model."""

import pytest

from back.core.graph_analysis.pattern.pattern_diagram import (
    MAX_DIAGRAM_EDGES,
    build_pattern_diagram,
    local_label,
    node_id,
)

pytestmark = pytest.mark.unit

NS = "http://ex.org/"
CUSTOMER, ORDER, PRODUCT = NS + "Customer", NS + "Order", NS + "Product"
CONTRACT = NS + "Contract"


def _row(hop, parent, via, child, members=4, total=8, parent_via=""):
    return {
        "hop": hop,
        "parent_via": parent_via,
        "parent_type": parent,
        "via": via,
        "child_type": child,
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
            _row(
                2, ORDER, NS + "hasProduct", PRODUCT, members=2, total=2,
                parent_via=NS + "hasOrder",
            ),
        ]
    )
    assert [(n["hop"], n["type_label"]) for n in d["nodes"]] == [
        (0, "Customer"), (1, "Order"), (2, "Product"),
    ]
    first, second = d["edges"]
    assert (first["source"], first["target"]) == (
        node_id(0, CUSTOMER), node_id(1, ORDER)
    )
    assert (second["source"], second["target"]) == (
        node_id(1, ORDER), node_id(2, PRODUCT)
    )
    assert first["via_label"] == "hasOrder"
    assert first["share"] == 1.0 and first["avg"] == 2.0
    assert second["share"] == 0.5 and second["avg"] == 1.0


def test_same_type_at_two_hops_gives_two_nodes():
    d = _diagram(
        [
            _row(1, CUSTOMER, NS + "hasOrder", ORDER),
            _row(
                2, ORDER, NS + "hasCustomer", CUSTOMER,
                parent_via=NS + "hasOrder",
            ),
        ]
    )
    ids = {n["id"] for n in d["nodes"]}
    assert {node_id(0, CUSTOMER), node_id(2, CUSTOMER)} <= ids
    assert "direction" not in d["edges"][1]


def test_same_type_reached_by_two_predicates_is_one_box():
    """One entity type per hop column — two predicates to Contract merge."""
    hc, hp = NS + "hasContract", NS + "hasPolicy"
    d = _diagram(
        [
            _row(1, CUSTOMER, hc, CONTRACT, members=1, total=1),
            _row(1, CUSTOMER, hp, CONTRACT, members=1, total=1),
            _row(2, CONTRACT, hc, CUSTOMER, members=1, total=1, parent_via=hp),
        ]
    )
    hop1 = [n for n in d["nodes"] if n["hop"] == 1]
    assert len(hop1) == 1
    assert hop1[0]["id"] == node_id(1, CONTRACT)
    by_target = {(e["source"], e["target"], e["via_label"]) for e in d["edges"]}
    assert (node_id(0, CUSTOMER), node_id(1, CONTRACT), "hasContract") in by_target
    assert (node_id(0, CUSTOMER), node_id(1, CONTRACT), "hasPolicy") in by_target
    assert (node_id(1, CONTRACT), node_id(2, CUSTOMER), "hasContract") in by_target


def test_attribute_tokens_become_chips_on_their_node():
    tokens = [
        f"0||{CUSTOMER}|self|{NS}segment|pro",
        f"0||{CUSTOMER}|self|{NS}score|q3",
        f"0||{CUSTOMER}|self||",
        f"1|{NS}hasOrder|{ORDER}|{NS}status|open",
        f"1|{NS}hasOrder|{ORDER}",
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


def test_legacy_edges_without_parent_via_keep_a_single_root():
    """Detect tables that lack parent_via must not invent hop-1 roots."""
    knows, does = NS + "knows", NS + "does"
    agent, interaction = NS + "Agent", NS + "Interaction"
    d = _diagram(
        [
            _row(1, CUSTOMER, knows, agent, members=2, total=2),
            _row(1, CUSTOMER, NS + "hasContract", CONTRACT, members=2, total=2),
            _row(2, agent, does, interaction, members=2, total=2),
            _row(2, CONTRACT, NS + "hasContract", CUSTOMER, members=1, total=1),
        ]
    )
    targets = {e["target"] for e in d["edges"]}
    orphans = [
        n for n in d["nodes"] if n["hop"] > 0 and n["id"] not in targets
    ]
    assert orphans == [], [(o["id"], o["type_label"]) for o in orphans]
    # One box per type per hop — no via-keyed duplicates.
    hop1_types = [n["type"] for n in d["nodes"] if n["hop"] == 1]
    assert hop1_types == sorted(set(hop1_types))
    by_target = {(e["source"], e["target"]) for e in d["edges"]}
    assert (node_id(0, CUSTOMER), node_id(1, agent)) in by_target
    assert (node_id(1, agent), node_id(2, interaction)) in by_target
