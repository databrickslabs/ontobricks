"""Community detection service using NetworkX algorithms."""

from __future__ import annotations

import time
from typing import List, Optional

import networkx as nx

from back.core.logging import get_logger
from back.core.graph_analysis.GraphBuilder import GraphBuilder
from back.core.graph_analysis.models import (
    ClusterRequest,
    ClusterResult,
    DetectionResult,
    DetectionStats,
)

logger = get_logger(__name__)

_SUPPORTED_ALGORITHMS = {"louvain", "label_propagation", "greedy_modularity"}


class CommunityDetector(GraphBuilder):
    """Detect communities in a knowledge graph using NetworkX algorithms.

    Constructor receives a triplestore backend instance and an optional
    graph/table name.  The detector queries all triples, builds an
    undirected ``networkx.Graph``, and runs the selected algorithm.
    """

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
