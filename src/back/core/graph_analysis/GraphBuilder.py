"""Shared graph-building base for graph analysis services."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

import networkx as nx

from back.core.graphdb.constants import RDF_TYPE, RDFS_LABEL

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
        """Query triples from the store with a max_triples guard.

        Class and predicate filters are pushed down to the store when it
        supports it, so a filtered analysis reads only the selected subgraph
        and can succeed on a store whose full triple count is over the limit.
        """
        class_filter = list(getattr(request, "class_filter", None) or [])
        predicate_filter = list(getattr(request, "predicate_filter", None) or [])

        pushdown = getattr(self._store, "query_triples_for_analysis", None)
        if (class_filter or predicate_filter) and pushdown is not None:
            triples = pushdown(
                self._graph_name,
                class_filter=class_filter or None,
                predicate_filter=predicate_filter or None,
            )
        else:
            triples = self._store.query_triples(self._graph_name)

        if len(triples) > request.max_triples:
            hint = (
                "Narrow the analysis with an entity-type filter, reduce the "
                "synced graph (exclude entity types in KG \u2192 Sync), or raise "
                "the analytics limit (ONTOBRICKS_ANALYTICS_MAX_TRIPLES)."
            )
            if class_filter:
                hint = (
                    "Select a smaller entity type, reduce the synced graph "
                    "(exclude entity types in KG \u2192 Sync), or raise the "
                    "analytics limit (ONTOBRICKS_ANALYTICS_MAX_TRIPLES)."
                )
            raise ValueError(
                f"Triple count ({len(triples)}) exceeds max_triples "
                f"({request.max_triples}). {hint}"
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
