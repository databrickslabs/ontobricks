"""Graph centrality and structural metrics service using NetworkX."""

from __future__ import annotations

import time
from collections import defaultdict
from statistics import mean
from typing import Dict, List, Set

import networkx as nx

from back.core.logging import get_logger
from back.core.triplestore.constants import RDF_TYPE, RDFS_LABEL
from back.core.graph_analysis.GraphBuilder import GraphBuilder, _DEFAULT_EXCLUDED_PREDICATES
from back.core.graph_analysis.models import (
    EntityTypeProfile,
    MetricsRequest,
    MetricsResult,
    MetricsStats,
    NodeMetrics,
)

logger = get_logger(__name__)

_TOP_N = 10

# Predicate local-name fragments that suggest time-series / temporal data
_TEMPORAL_KEYWORDS: Set[str] = {
    "time", "date", "timestamp", "ts", "at", "created", "modified", "dt",
    "start", "end", "recorded", "occurred", "measured",
}


def _local_name(uri: str) -> str:
    """Extract the local name from a URI (last path/fragment segment, lower-cased)."""
    return (uri or "").rstrip("/").split("/")[-1].split("#")[-1].lower()


class GraphMetrics(GraphBuilder):
    """Compute centrality and structural metrics over a knowledge graph.

    Constructor receives a triplestore backend and an optional graph/table name.
    Call ``compute(request)`` to run all metrics in one pass.
    """

    def compute(self, request: MetricsRequest) -> MetricsResult:
        """Compute graph metrics and return per-node scores plus aggregate stats.

        Raises ``ValueError`` when the triple count exceeds ``request.max_triples``.
        """
        t0 = time.time()

        triples = self._load_triples(request)
        g = self._build_graph(triples, request)

        if g.number_of_nodes() == 0:
            logger.warning("GraphMetrics: graph has 0 nodes after filtering")
            return MetricsResult(stats=MetricsStats(elapsed_ms=self._elapsed_ms(t0)))

        logger.info(
            "GraphMetrics: built nx.Graph with %d nodes, %d edges",
            g.number_of_nodes(),
            g.number_of_edges(),
        )

        degree = nx.degree_centrality(g)
        pagerank = self._pagerank(g)
        betweenness = self._betweenness(g, request.max_nodes_betweenness)
        closeness = nx.closeness_centrality(g)
        clustering = nx.clustering(g)

        nodes: Dict[str, NodeMetrics] = {
            uri: NodeMetrics(
                degree=round(degree.get(uri, 0.0), 6),
                pagerank=round(pagerank.get(uri, 0.0), 6),
                betweenness=round(betweenness.get(uri, 0.0), 6),
                closeness=round(closeness.get(uri, 0.0), 6),
                clustering=round(clustering.get(uri, 0.0), 6),
            )
            for uri in g.nodes()
        }

        graph_node_count = g.number_of_nodes()
        edge_count = g.number_of_edges()
        components = nx.number_connected_components(g)
        avg_degree = (2 * edge_count / graph_node_count) if graph_node_count else 0.0
        density = nx.density(g)

        # Build node → class_uri mapping from rdf:type triples
        node_types: Dict[str, str] = {
            t["subject"]: t["object"]
            for t in triples
            if t.get("predicate") == RDF_TYPE
            and t.get("subject") in nodes
        }

        # Build node → rdfs:label mapping (first label wins if multiple)
        node_labels: Dict[str, str] = {}
        for t in triples:
            if t.get("predicate") == RDFS_LABEL:
                subj = t.get("subject", "")
                if subj in nodes and subj not in node_labels:
                    node_labels[subj] = t.get("object", "")

        # When a class_filter was applied, restrict returned nodes to instances of
        # the selected type(s).  Metrics are still computed on the full connected
        # subgraph (for accuracy), but only the requested entities are surfaced.
        # Instances that have no entity-entity edges (isolated nodes) are added
        # with zero scores so the count always matches the full type population.
        if request.class_filter:
            filter_set = set(request.class_filter)
            allowed = {
                t["subject"]
                for t in triples
                if t.get("predicate") == RDF_TYPE and t.get("object") in filter_set
            }
            nodes = {uri: m for uri, m in nodes.items() if uri in allowed}
            # Add isolated instances that never made it into the graph
            for uri in allowed:
                if uri not in nodes:
                    nodes[uri] = NodeMetrics()

        node_count = len(nodes)

        top_pagerank = sorted(nodes, key=lambda u: nodes[u].pagerank, reverse=True)[:_TOP_N]

        elapsed_ms = self._elapsed_ms(t0)

        stats = MetricsStats(
            node_count=node_count,
            graph_node_count=graph_node_count,
            edge_count=edge_count,
            connected_components=components,
            avg_degree=round(avg_degree, 4),
            density=round(density, 6),
            elapsed_ms=elapsed_ms,
        )

        logger.info(
            "GraphMetrics: %d/%d nodes returned, %d edges, %d components in %dms",
            node_count,
            graph_node_count,
            edge_count,
            components,
            elapsed_ms,
        )

        profiles = self._build_type_profiles(triples, nodes, node_types, stats.graph_node_count)
        logger.info(
            "GraphMetrics: %d entity type profiles built (%d flat)",
            len(profiles),
            sum(1 for p in profiles.values() if p.is_flat),
        )
        return MetricsResult(nodes=nodes, stats=stats, top_pagerank=top_pagerank, node_types=node_types, node_labels=node_labels, entity_type_profiles=profiles)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _pagerank(g: nx.Graph) -> Dict[str, float]:
        """Compute PageRank, falling back to the pure-Python power-iteration
        implementation when scipy is not available."""
        try:
            return nx.pagerank(g, max_iter=200)
        except (ImportError, ModuleNotFoundError):
            from networkx.algorithms.link_analysis.pagerank_alg import _pagerank_python
            return _pagerank_python(g, 0.85, None, 200, 1.0e-6, None, "weight")

    @staticmethod
    def _betweenness(g: nx.Graph, max_nodes: int) -> Dict[str, float]:
        """Compute betweenness centrality, sampling on large graphs.

        When the graph exceeds *max_nodes* nodes, uses ``k``-sample
        approximation (capped at 500) to avoid the O(VE) cost.
        """
        n = g.number_of_nodes()
        if n > max_nodes:
            k = min(500, n)
            logger.info(
                "GraphMetrics: betweenness sampling (k=%d) for %d-node graph", k, n
            )
            return nx.betweenness_centrality(g, k=k, seed=42)
        return nx.betweenness_centrality(g)

    @staticmethod
    def _elapsed_ms(t0: float) -> int:
        return int((time.time() - t0) * 1000)

    @staticmethod
    def _build_type_profiles(
        triples: List[Dict],
        nodes: Dict[str, NodeMetrics],
        node_types: Dict[str, str],
        graph_node_count: int,
    ) -> Dict[str, EntityTypeProfile]:
        """Build per-entity-type structural profiles and apply flat-dataset heuristics.

        Runs a single pass over ``triples`` to collect the set of distinct
        predicates used by each node, then aggregates per class URI.
        No additional DB query is needed.
        """
        if not node_types:
            return {}

        # Collect distinct predicates per node (both directions):
        # a node counts a predicate whether it is the subject OR the object.
        predicates_by_node: Dict[str, Set[str]] = defaultdict(set)
        for t in triples:
            pred = t.get("predicate", "")
            if not pred or pred in _DEFAULT_EXCLUDED_PREDICATES:
                continue
            subj = t.get("subject", "")
            obj  = t.get("object",  "")
            if subj in nodes:
                predicates_by_node[subj].add(pred)
            if obj in nodes and (obj.startswith("http://") or obj.startswith("https://")):
                predicates_by_node[obj].add(pred)

        # Per-type rollup
        profiles: Dict[str, EntityTypeProfile] = {}
        classes = set(node_types.values())
        for class_uri in classes:
            instances = [u for u, c in node_types.items() if c == class_uri and u in nodes]
            if not instances:
                continue

            avg_deg  = round(mean(nodes[u].degree      for u in instances), 6)
            avg_clust = round(mean(nodes[u].clustering  for u in instances), 6)
            avg_bet  = round(mean(nodes[u].betweenness  for u in instances), 6)

            all_preds: Set[str] = set()
            for u in instances:
                all_preds.update(predicates_by_node.get(u, set()))

            has_temporal = any(
                kw in _local_name(p) for p in all_preds for kw in _TEMPORAL_KEYWORDS
            )

            # Heuristic rules — degree centrality is normalised by (N-1) so we
            # rely on predicate diversity as the primary flat-dataset signal.
            reasons: List[str] = []
            n = len(instances)
            n_preds = len(all_preds)

            if n_preds == 0:
                reasons.append("no entity-entity relationships (fully isolated instances)")
            elif n_preds == 1 and n > 20:
                reasons.append(f"only 1 distinct relationship predicate across {n} instances")

            profiles[class_uri] = EntityTypeProfile(
                uri=class_uri,
                count=n,
                avg_degree=avg_deg,
                avg_clustering=avg_clust,
                avg_betweenness=avg_bet,
                distinct_predicates=n_preds,
                has_temporal_predicates=has_temporal,
                is_flat=bool(reasons),
                flat_reasons=reasons,
            )

        return profiles
