"""Pattern diagram -- the typed neighbourhood one discovered pattern stands for.

Pure: rows of ``<out>_shape_edges`` plus the pattern's shape tokens in, a small
node / edge model the Patterns page draws as a layered diagram out. A node is a
``(hop, type)`` pair, so one node stands for every neighbour of that type at
that distance from the anchor; an edge carries how many members have it
(``share``) and how many such neighbours a member has on average (``avg``).
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

MAX_DIAGRAM_EDGES = 60
MAX_NODE_ATTRIBUTES = 6


def local_label(uri: str) -> str:
    """Display name of a URI: its last path or fragment segment."""
    if not uri:
        return ""
    tail = uri.rstrip("/").rsplit("/", 1)[-1]
    return tail.rsplit("#", 1)[-1] or uri


def node_id(hop: int, type_uri: str) -> str:
    return f"{int(hop)}|{type_uri or ''}"


def _bin_label(value: str, bins: int) -> str:
    """``q3`` -> ``bin 4/8``; categorical values pass through."""
    if len(value) > 1 and value[0] == "q" and value[1:].isdigit():
        n = int(value[1:]) + 1
        return f"bin {n}/{bins}" if bins > 0 else f"bin {n}"
    return value


def _attribute_of(token: str) -> Optional[tuple]:
    """``(node_id, prop, value)`` for an attribute token, else ``None``.

    Hop-0 tokens are ``0||type|self|prop|bin``; neighbour tokens are
    ``hop|via|type|direction|prop|bin``. Structural tokens have 4 fields.
    """
    parts = str(token).split("|")
    if len(parts) != 6 or not parts[4]:
        return None
    if parts[3] == "self":
        return node_id(0, parts[2]), parts[4], parts[5]
    if not parts[0].isdigit():
        return None
    return node_id(int(parts[0]), parts[2]), parts[4], parts[5]


def _node(nodes: Dict[str, Dict[str, Any]], hop: int, type_uri: str) -> str:
    nid = node_id(hop, type_uri)
    if nid not in nodes:
        nodes[nid] = {
            "id": nid,
            "hop": int(hop),
            "type": type_uri,
            "type_label": local_label(type_uri) or "?",
            "attributes": [],
        }
    return nid


def build_pattern_diagram(
    *,
    anchor_type: str,
    support: int,
    edge_rows: Iterable[Dict[str, Any]],
    tokens: Iterable[str],
    bins: int = 0,
) -> Dict[str, Any]:
    """Layered ``{nodes, edges, truncated}`` model of one pattern."""
    nodes: Dict[str, Dict[str, Any]] = {}
    _node(nodes, 0, anchor_type)
    rows = sorted(
        edge_rows,
        key=lambda r: (int(r.get("hop") or 0), -int(r.get("member_count") or 0)),
    )
    edges: List[Dict[str, Any]] = []
    for row in rows[:MAX_DIAGRAM_EDGES]:
        hop = int(row.get("hop") or 0)
        members = int(row.get("member_count") or 0)
        via = str(row.get("via") or "")
        source = _node(nodes, hop - 1, str(row.get("parent_type") or ""))
        target = _node(nodes, hop, str(row.get("child_type") or ""))
        edges.append(
            {
                "source": source,
                "target": target,
                "via": via,
                "via_label": local_label(via),
                "direction": str(row.get("direction") or "out"),
                "share": round(members / support, 3) if support else 0.0,
                "avg": round(int(row.get("edge_total") or 0) / members, 2)
                if members
                else 0.0,
            }
        )
    for token in tokens:
        attr = _attribute_of(token)
        if attr is None or attr[0] not in nodes:
            continue
        chips = nodes[attr[0]]["attributes"]
        if len(chips) < MAX_NODE_ATTRIBUTES:
            chips.append({"prop": local_label(attr[1]), "value": _bin_label(attr[2], bins)})
    return {
        "nodes": sorted(nodes.values(), key=lambda n: (n["hop"], n["type_label"])),
        "edges": edges,
        "truncated": len(rows) > MAX_DIAGRAM_EDGES,
    }
