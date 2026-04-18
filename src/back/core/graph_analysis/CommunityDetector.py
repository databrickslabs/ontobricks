"""Community detection service using NetworkX algorithms."""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Set

import networkx as nx

from back.core.logging import get_logger
from back.core.triplestore.constants import RDF_TYPE, RDFS_LABEL
from back.core.graph_analysis.models import (
    ClusterRequest,
    ClusterResult,
    DetectionResult,
    DetectionStats,
)

logger = get_logger(__name__)

# High-cardinality predicates that create noise in community structure
_DEFAULT_EXCLUDED_PREDICATES: Set[str] = {
    RDF_TYPE,
    RDFS_LABEL,
    "http://www.w3.org/2000/01/rdf-schema#comment",
    "http://www.w3.org/2000/01/rdf-schema#seeAlso",
}

_SUPPORTED_ALGORITHMS = {"louvain", "label_propagation", "greedy_modularity"}


class CommunityDetector:
    """Detect communities in a knowledge graph using NetworkX algorithms.

    Constructor receives a triplestore backend instance and an optional
    graph/table name.  The detector queries all triples, builds an
    undirected ``networkx.Graph``, and runs the selected algorithm.
    """

    def __init__(self, store: Any, graph_name: str) -> None:
        self._store = store
        self._graph_name = graph_name

    def detect(self, request: ClusterRequest) -> DetectionResult:
        """Run community detection and return clusters with statistics.

        Raises ``ValueError`` when the algorithm is unsupported or the
        triple count exceeds ``request.max_triples``.
        """
        algorithm = request.algorithm
        if algorithm not in _SUPPORTED_ALGORITHMS:
            raise ValueError(
                f"Unsupported algorithm '{algorithm}'. "
                f"Choose from: {', '.join(sorted(_SUPPORTED_ALGORITHMS))}"
            )

        t0 = time.time()

        triples = self._load_triples(request)

        nxg = self._build_graph(triples, request)
        if nxg.number_of_nodes() == 0:
            logger.warning("CommunityDetector: graph has 0 nodes after filtering")
            return DetectionResult(
                stats=DetectionStats(
                    algorithm=algorithm, elapsed_ms=self._elapsed_ms(t0)
                ),
            )

        logger.info(
            "CommunityDetector: built nx.Graph with %d nodes, %d edges",
            nxg.number_of_nodes(),
            nxg.number_of_edges(),
        )

        communities = self._run_algorithm(nxg, request)

        clusters = self._communities_to_clusters(communities)
        modularity = self._compute_modularity(nxg, communities)

        elapsed_ms = self._elapsed_ms(t0)
        stats = DetectionStats(
            node_count=nxg.number_of_nodes(),
            edge_count=nxg.number_of_edges(),
            cluster_count=len(clusters),
            modularity=round(modularity, 4),
            algorithm=algorithm,
            elapsed_ms=elapsed_ms,
        )

        logger.info(
            "CommunityDetector: %d clusters (modularity=%.4f) in %dms",
            len(clusters),
            modularity,
            elapsed_ms,
        )

        return DetectionResult(clusters=clusters, stats=stats)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_triples(self, request: ClusterRequest) -> List[Dict[str, str]]:
        """Query triples from the store with a max_triples guard."""
        triples = self._store.query_triples(self._graph_name)
        if len(triples) > request.max_triples:
            raise ValueError(
                f"Triple count ({len(triples)}) exceeds max_triples "
                f"({request.max_triples}). Use a predicate or class filter, "
                f"or increase max_triples."
            )
        return triples

    def _build_graph(
        self,
        triples: List[Dict[str, str]],
        request: ClusterRequest,
    ) -> nx.Graph:
        """Build an undirected NetworkX graph from SPO triples.

        Excludes high-cardinality predicates (rdf:type, rdfs:label, ...)
        and optionally filters by predicate or class.
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

            if allowed_subjects is not None:
                if subj not in allowed_subjects and obj not in allowed_subjects:
                    continue

            g.add_edge(subj, obj)

        return g

    def _run_algorithm(
        self,
        g: nx.Graph,
        request: ClusterRequest,
    ) -> List[set]:
        """Dispatch to the selected NetworkX community algorithm."""
        algo = request.algorithm
        if algo == "louvain":
            return nx.community.louvain_communities(
                g,
                resolution=request.resolution,
                seed=42,
            )
        if algo == "label_propagation":
            return list(nx.community.label_propagation_communities(g))
        if algo == "greedy_modularity":
            return list(nx.community.greedy_modularity_communities(g))
        raise ValueError(f"Unsupported algorithm: {algo}")

    @staticmethod
    def _communities_to_clusters(communities: List[set]) -> List[ClusterResult]:
        """Convert a list of node-sets into sorted ``ClusterResult`` objects."""
        clusters = []
        for idx, members in enumerate(sorted(communities, key=len, reverse=True)):
            clusters.append(
                ClusterResult(id=idx, members=sorted(members), size=len(members))
            )
        return clusters

    @staticmethod
    def _compute_modularity(g: nx.Graph, communities: List[set]) -> float:
        """Compute Newman modularity for the partition."""
        try:
            return nx.community.modularity(g, communities)
        except Exception:
            return 0.0

    @staticmethod
    def _elapsed_ms(t0: float) -> int:
        return int((time.time() - t0) * 1000)
