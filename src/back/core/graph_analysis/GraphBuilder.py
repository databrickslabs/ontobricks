"""Shared graph-building base for graph analysis services."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

import networkx as nx

from back.core.triplestore.constants import RDF_TYPE, RDFS_LABEL

# High-cardinality predicates that create noise in graph structure analysis
_DEFAULT_EXCLUDED_PREDICATES: Set[str] = {
    RDF_TYPE,
    RDFS_LABEL,
    "http://www.w3.org/2000/01/rdf-schema#comment",
    "http://www.w3.org/2000/01/rdf-schema#seeAlso",
}


class GraphBuilder:
    """Base class that loads triples and builds a NetworkX graph.

    Subclasses receive a triplestore backend and an optional graph/table name.
    ``_load_triples`` and ``_build_graph`` are shared by all graph analysis
    services; they encapsulate predicate exclusion, class filtering, and the
    max-triples safety guard.
    """

    def __init__(self, store: Any, graph_name: str) -> None:
        self._store = store
        self._graph_name = graph_name

    # ------------------------------------------------------------------
    # Shared helpers (used by CommunityDetector and GraphMetrics)
    # ------------------------------------------------------------------

    def _load_triples(self, request: Any) -> List[Dict[str, str]]:
        """Query triples from the store with a max_triples guard."""
        triples = self._store.query_triples(self._graph_name)
        # NOTE: the guard is on the *full* triple load — class/predicate filters
        # are applied later in ``_build_graph`` and do NOT reduce what is read
        # here, so the only levers are shrinking the synced graph or raising the
        # limit (``ONTOBRICKS_ANALYTICS_MAX_TRIPLES``).
        if len(triples) > request.max_triples:
            raise ValueError(
                f"Triple count ({len(triples)}) exceeds max_triples "
                f"({request.max_triples}). Reduce the synced graph (exclude "
                f"entity types in KG \u2192 Sync) or raise the analytics limit "
                f"(ONTOBRICKS_ANALYTICS_MAX_TRIPLES)."
            )
        return triples

    def _build_graph(
        self,
        triples: List[Dict[str, str]],
        request: Any,
    ) -> nx.Graph:
        """Build an undirected NetworkX graph from SPO triples.

        Only entity–entity edges are included: triples whose object is a
        literal (not a URI starting with http/https) are silently dropped so
        that attribute values ("John", "42", …) never become graph nodes.
        High-cardinality predicates (rdf:type, rdfs:label, …) are also
        excluded, and optionally filters by predicate or class.
        """
        excluded = set(_DEFAULT_EXCLUDED_PREDICATES)
        if request.predicate_filter:
            excluded.update(request.predicate_filter)

        class_filter: Optional[Set[str]] = None
        if request.class_filter:
            class_filter = set(request.class_filter)

        allowed_subjects: Optional[Set[str]] = None
        if class_filter:
            allowed_subjects = {
                t["subject"]
                for t in triples
                if t.get("predicate") == RDF_TYPE and t.get("object") in class_filter
            }

        g = nx.Graph()
        for t in triples:
            pred = t.get("predicate", "")
            if pred in excluded:
                continue

            subj = t.get("subject", "")
            obj = t.get("object", "")
            if not subj or not obj:
                continue

            # Drop attribute triples — object must be a URI (entity reference)
            if not (obj.startswith("http://") or obj.startswith("https://")):
                continue

            if allowed_subjects is not None:
                if subj not in allowed_subjects and obj not in allowed_subjects:
                    continue

            g.add_edge(subj, obj)

        return g
