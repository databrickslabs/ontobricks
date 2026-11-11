"""Community detection service using NetworkX algorithms."""

from __future__ import annotations

import time
from typing import Any, Dict, Iterable, List, Set

import networkx as nx

from back.core.graphdb.constants import RDF_TYPE, RDFS_LABEL
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
# Cap for the Intelligence Community light explorer (not the materialize payload).
_PREVIEW_MAX_NODES = 1_500


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
        member_uris = {m for cl in clusters for m in cl.members}
        entity_meta = self._entity_meta(triples, member_uris)
        preview = self._build_preview(nxg, clusters, entity_meta=entity_meta)

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

        return DetectionResult(
            clusters=clusters,
            stats=stats,
            preview=preview,
            entity_meta=entity_meta,
        )

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
    def _local_label(uri: str) -> str:
        if not uri:
            return ""
        tail = uri.rstrip("/").rsplit("/", 1)[-1]
        return tail.rsplit("#", 1)[-1] or uri

    @classmethod
    def _entity_meta(
        cls,
        triples: Iterable[Dict[str, str]],
        uris: Set[str],
    ) -> Dict[str, Dict[str, str]]:
        """Collect ``rdfs:label`` / ``rdf:type`` for *uris* from loaded triples."""
        meta: Dict[str, Dict[str, str]] = {
            uri: {"label": "", "type": "", "type_label": ""} for uri in uris
        }
        if not uris:
            return meta
        for triple in triples:
            subject = triple.get("subject") or ""
            cell = meta.get(subject)
            if cell is None:
                continue
            predicate = triple.get("predicate") or ""
            obj = triple.get("object") or ""
            if not obj:
                continue
            if predicate == RDFS_LABEL and not cell["label"]:
                cell["label"] = obj
            elif predicate == RDF_TYPE and not cell["type"]:
                cell["type"] = obj
                cell["type_label"] = cls._local_label(obj)
        for uri, cell in meta.items():
            if not cell["label"]:
                cell["label"] = cls._local_label(uri)
        return meta

    @classmethod
    def _build_preview(
        cls,
        nxg: nx.Graph,
        clusters: List[ClusterResult],
        max_nodes: int = _PREVIEW_MAX_NODES,
        entity_meta: Dict[str, Dict[str, str]] | None = None,
    ) -> Dict[str, Any]:
        """Build a capped entity graph coloured by cluster for the UI preview."""
        selected: List[tuple[str, int]] = []
        for cl in clusters:
            for member in cl.members:
                if len(selected) >= max_nodes:
                    break
                selected.append((member, cl.id))
            if len(selected) >= max_nodes:
                break

        node_set = {uri for uri, _ in selected}
        cluster_of = {uri: cid for uri, cid in selected}
        meta = entity_meta or {}
        nodes = []
        for uri in sorted(node_set):
            info = meta.get(uri) or {}
            nodes.append(
                {
                    "id": uri,
                    "label": info.get("label") or cls._local_label(uri),
                    "type": info.get("type") or "",
                    "type_label": info.get("type_label") or "",
                    "cluster": cluster_of[uri],
                }
            )
        edges: List[Dict[str, str]] = []
        for a, b in nxg.edges():
            if a not in node_set or b not in node_set:
                continue
            edges.append({"id": f"{a}|{b}", "source": a, "target": b})
        return {
            "nodes": nodes,
            "edges": edges,
            "capped": nxg.number_of_nodes() > len(node_set),
            "node_total": nxg.number_of_nodes(),
            "edge_total": nxg.number_of_edges(),
        }

    @staticmethod
    def _elapsed_ms(t0: float) -> int:
        return int((time.time() - t0) * 1000)
