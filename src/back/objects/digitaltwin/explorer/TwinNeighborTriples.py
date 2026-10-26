"""Neighbour-expansion triple filter for the Knowledge Graph explorer.

Extracted from ``api.routers.internal.dtwin`` (Fowler Extract Class).
The route module keeps ``_filter_neighbor_triples`` aliases for tests.
"""

from __future__ import annotations


class TwinNeighborTriples:
    """Keep rdf:type triples when expanding neighbours (issue #52)."""

    RDF_TYPE_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

    @staticmethod
    def is_type_predicate(predicate: str) -> bool:
        """Return True for ``rdf:type`` predicates (full URI or ``#type``/``/type``)."""
        if not predicate:
            return False
        return (
            predicate == TwinNeighborTriples.RDF_TYPE_URI
            or predicate.endswith("#type")
            or predicate.endswith("/type")
        )

    @staticmethod
    def filter_neighbor_triples(
        rows: list[dict[str, str]],
        visited: set[str],
        limit: int,
    ) -> list[dict[str, str]]:
        """Reduce raw store rows to the triples the knowledge graph can render.

        A triple is kept when its object is a literal, when its object URI is
        part of *visited*, or when it is an ``rdf:type`` triple.
        """
        triples: list[dict[str, str]] = []
        seen: set = set()
        for r in rows:
            s = r.get("subject", "") or ""
            p = r.get("predicate", "") or ""
            o = r.get("object", "") or ""
            key = (s, p, o)
            if key in seen:
                continue
            is_uri_obj = o.startswith("http://") or o.startswith("https://")
            if (
                is_uri_obj
                and o not in visited
                and not TwinNeighborTriples.is_type_predicate(p)
            ):
                continue
            seen.add(key)
            triples.append({"subject": s, "predicate": p, "object": o})
            if len(triples) >= limit:
                break
        return triples
