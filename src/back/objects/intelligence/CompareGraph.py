"""Graph projection of a sandbox-vs-live comparison (Compare section)."""

from __future__ import annotations

from collections import Counter
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from back.core.helpers import extract_local_name, is_uri
from back.objects.digitaltwin.constants import RDF_TYPE, RDFS_LABEL
from back.objects.intelligence.Diff import Diff, TripleKey


class CompareGraph:
    """Project a live-vs-sandbox triple diff into ``{nodes, edges, summary}``.

    Triples are classified ``unchanged`` (in both), ``added`` (sandbox only)
    or ``removed`` (live only). Only URI-valued, non-``rdf:type`` triples are
    drawn as edges; ``rdf:type`` sets the node type and literals are attached
    to their subject node as properties (only changed ones are listed).

    The payload is bounded: nodes touched by a change are kept first, then the
    best-connected unchanged nodes fill the remaining budget. ``summary``
    always carries the exact triple counts, whatever the cap.
    """

    MAX_NODES = 400
    MAX_EDGES = 1500
    MAX_PROPS_PER_NODE = 25

    @classmethod
    def build(
        cls,
        live: Iterable[Dict[str, Any]],
        sandbox: Iterable[Dict[str, Any]],
        *,
        patch_added: Optional[Set[TripleKey]] = None,
        provenance: Optional[Dict[TripleKey, str]] = None,
        max_nodes: int = MAX_NODES,
        max_edges: int = MAX_EDGES,
    ) -> Dict[str, Any]:
        live_rows = list(live)
        sandbox_rows = list(sandbox)
        live_keys = Diff.keys(live_rows)
        sandbox_keys = Diff.keys(sandbox_rows)
        diff = Diff.compute(
            live_rows,
            sandbox_rows,
            patch_added=patch_added or set(),
            provenance=provenance,
            row_cap=None,
        )
        origins = {
            Diff.key(r): (r.get("origin"), r.get("provenance"))
            for r in diff["added"]
        }
        status_of = cls._status_resolver(live_keys, sandbox_keys)
        keys = live_keys | sandbox_keys

        labels, types = cls._labels_and_types(keys)
        edge_keys = [k for k in keys if cls._is_edge(k)]
        node_ids = {k[0] for k in keys} | {k[2] for k in edge_keys}
        in_live = {k[0] for k in live_keys} | {
            k[2] for k in live_keys if cls._is_edge(k)
        }
        in_sandbox = {k[0] for k in sandbox_keys} | {
            k[2] for k in sandbox_keys if cls._is_edge(k)
        }

        delta, node_added, node_removed, props = cls._delta_stats(keys, status_of)

        degree: Counter = Counter()
        for k in edge_keys:
            degree[k[0]] += 1
            degree[k[2]] += 1

        selected = cls._select_nodes(
            node_ids, delta, degree, edge_keys, max(1, int(max_nodes))
        )
        edges, edges_capped = cls._select_edges(
            edge_keys, selected, status_of, origins, max(1, int(max_edges))
        )
        nodes = [
            cls._node(
                uri,
                labels,
                types,
                in_live,
                in_sandbox,
                node_added,
                node_removed,
                props,
            )
            for uri in sorted(selected)
        ]
        summary = dict(diff["summary"])
        summary.update(
            {
                "live_count": summary.pop("before_count"),
                "sandbox_count": summary.pop("after_count"),
                "unchanged_count": len(live_keys & sandbox_keys),
                "entity_count": len({k[0] for k in keys}),
                "node_count": len(nodes),
                "edge_count": len(edges),
                "capped": len(selected) < len(node_ids) or edges_capped,
                "node_total": len(node_ids),
                "edge_total": len(edge_keys),
            }
        )
        summary.pop("row_cap", None)
        return {"nodes": nodes, "edges": edges, "summary": summary}

    @classmethod
    def _delta_stats(
        cls, keys: Iterable[TripleKey], status_of: Any
    ) -> Tuple[Counter, Counter, Counter, Dict[str, List[Dict[str, str]]]]:
        """Per-node change counters and the changed attribute/type triples."""
        delta: Counter = Counter()
        node_added: Counter = Counter()
        node_removed: Counter = Counter()
        props: Dict[str, List[Dict[str, str]]] = {}
        for k in keys:
            status = status_of(k)
            if status == "unchanged":
                continue
            delta[k[0]] += 1
            if cls._is_edge(k):
                delta[k[2]] += 1
            (node_added if status == "added" else node_removed)[k[0]] += 1
            if cls._is_edge(k):
                continue
            bucket = props.setdefault(k[0], [])
            if len(bucket) < cls.MAX_PROPS_PER_NODE:
                bucket.append(
                    {
                        "predicate": k[1],
                        "predicate_label": extract_local_name(k[1]) or k[1],
                        "object": k[2],
                        "status": status,
                    }
                )
        return delta, node_added, node_removed, props

    @staticmethod
    def empty_summary() -> Dict[str, Any]:
        return {
            "live_count": 0,
            "sandbox_count": 0,
            "added_count": 0,
            "removed_count": 0,
            "unchanged_count": 0,
            "entity_count": 0,
            "node_count": 0,
            "edge_count": 0,
            "capped": False,
        }

    # -- classification -----------------------------------------------------

    @staticmethod
    def _status_resolver(live_keys: Set[TripleKey], sandbox_keys: Set[TripleKey]):
        def status(key: TripleKey) -> str:
            if key in live_keys and key in sandbox_keys:
                return "unchanged"
            return "added" if key in sandbox_keys else "removed"

        return status

    @staticmethod
    def _is_edge(key: TripleKey) -> bool:
        return key[1] != RDF_TYPE and key[1] != RDFS_LABEL and is_uri(key[2])

    @staticmethod
    def _labels_and_types(
        keys: Iterable[TripleKey],
    ) -> Tuple[Dict[str, str], Dict[str, str]]:
        labels: Dict[str, str] = {}
        types: Dict[str, str] = {}
        for s, p, o in sorted(keys):
            if p == RDFS_LABEL and o:
                labels.setdefault(s, o)
            elif p == RDF_TYPE and o:
                types.setdefault(s, extract_local_name(o) or o)
        return labels, types

    # -- selection ----------------------------------------------------------

    @staticmethod
    def _select_nodes(
        node_ids: Set[str],
        delta: Counter,
        degree: Counter,
        edge_keys: List[TripleKey],
        max_nodes: int,
    ) -> Set[str]:
        changed = sorted((u for u in node_ids if delta[u]), key=lambda u: (-delta[u], u))
        selected: Set[str] = set(changed[:max_nodes])
        if len(selected) >= max_nodes:
            return selected
        links: Counter = Counter()
        for s, _p, o in edge_keys:
            if s in selected and o not in selected:
                links[o] += 1
            elif o in selected and s not in selected:
                links[s] += 1
        rest = sorted(
            (u for u in node_ids if u not in selected),
            key=lambda u: (-links[u], -degree[u], u),
        )
        selected.update(rest[: max_nodes - len(selected)])
        return selected

    @staticmethod
    def _select_edges(
        edge_keys: List[TripleKey],
        selected: Set[str],
        status_of: Any,
        origins: Dict[TripleKey, Tuple[Any, Any]],
        max_edges: int,
    ) -> Tuple[List[Dict[str, Any]], bool]:
        eligible = [k for k in edge_keys if k[0] in selected and k[2] in selected]
        eligible.sort(key=lambda k: (status_of(k) == "unchanged", k))
        capped = len(eligible) > max_edges
        edges: List[Dict[str, Any]] = []
        for k in eligible[:max_edges]:
            origin, prov = origins.get(k, (None, None))
            edge = {
                "id": f"{k[0]}|{k[1]}|{k[2]}",
                "source": k[0],
                "target": k[2],
                "predicate": k[1],
                "label": extract_local_name(k[1]) or k[1],
                "status": status_of(k),
            }
            if origin:
                edge["origin"] = origin
            if prov:
                edge["provenance"] = prov
            edges.append(edge)
        return edges, capped

    @staticmethod
    def _node(
        uri: str,
        labels: Dict[str, str],
        types: Dict[str, str],
        in_live: Set[str],
        in_sandbox: Set[str],
        node_added: Counter,
        node_removed: Counter,
        props: Dict[str, List[Dict[str, str]]],
    ) -> Dict[str, Any]:
        if uri in in_live and uri in in_sandbox:
            presence = "shared"
        elif uri in in_sandbox:
            presence = "sandbox_only"
        else:
            presence = "live_only"
        node = {
            "id": uri,
            "label": labels.get(uri) or extract_local_name(uri) or uri,
            "type": types.get(uri, ""),
            "presence": presence,
            "added": node_added[uri],
            "removed": node_removed[uri],
            "changed": bool(node_added[uri] or node_removed[uri]),
        }
        if uri in props:
            node["changes"] = props[uri]
        return node
