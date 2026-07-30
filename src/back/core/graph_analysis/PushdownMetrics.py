"""Graph metrics computed inside the storage engine, for graphs too large to load.

The in-memory :class:`~back.core.graph_analysis.GraphMetrics.GraphMetrics` path
materialises every triple and builds one ``networkx.Graph``, which is why it is
capped by ``ONTOBRICKS_ANALYTICS_MAX_TRIPLES``.  This module computes the
subset of the same result that can be expressed as SQL aggregations, so it has
no graph-size limit:

* structure counts (nodes, edges, average degree, density),
* degree centrality for the highest-degree nodes,
* per-entity-type profiles and the flat-dataset heuristic.

What it deliberately does **not** produce is the metrics that need an iterative
pass over the graph — PageRank, clustering coefficient, betweenness, closeness
and the connected-component count.  Those are reported through
``MetricsResult.unavailable_metrics`` rather than silently returned as zeros or
as approximations dressed up as exact values.  To get them on a large store,
narrow the analysis with an entity-type filter: the filter is pushed down to
the engine, so the selected subgraph usually fits the in-memory path.

``nodes`` is bounded to the top *top_n* by degree.  The UI only charts a top-N
slice (its selector is capped well below this), and an exhaustive per-node map
would defeat the purpose on a graph of this size.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Set

from back.core.logging import get_logger
from back.core.graph_analysis.GraphBuilder import _DEFAULT_EXCLUDED_PREDICATES
from back.core.graph_analysis.models import (
    MODE_PUSHDOWN,
    EntityTypeProfile,
    MetricsRequest,
    MetricsResult,
    MetricsStats,
    NodeMetrics,
)
from back.core.graph_analysis.profiles import flat_reasons, has_temporal_predicates

logger = get_logger(__name__)

#: Metrics the SQL pushdown path cannot compute (they need graph iteration).
UNAVAILABLE_METRICS = ("pagerank", "betweenness", "closeness", "clustering")

#: Engine methods a store must expose to support pushdown analytics.
_REQUIRED_METHODS = (
    "get_graph_structure_stats",
    "get_top_nodes_by_degree",
    "get_type_edge_stats",
    "get_type_predicate_pairs",
    "get_type_distribution",
)


def supports_pushdown(store: Any) -> bool:
    """Whether *store* can run the SQL-pushdown analytics."""
    if store is None:
        return False
    if getattr(store, "query_dialect", "sql") != "sql":
        return False
    return all(hasattr(store, m) for m in _REQUIRED_METHODS)


class PushdownMetrics:
    """Compute graph metrics via engine-side SQL aggregations."""

    def __init__(self, store: Any, graph_name: str, *, top_n: int = 100) -> None:
        self._store = store
        self._graph_name = graph_name
        self._top_n = max(1, int(top_n))

    def compute(self, request: MetricsRequest) -> MetricsResult:
        """Run the aggregations and assemble a :class:`MetricsResult`."""
        t0 = time.time()

        excluded = sorted(
            set(_DEFAULT_EXCLUDED_PREDICATES) | set(request.predicate_filter or [])
        )
        class_filter = list(request.class_filter or []) or None
        kwargs: Dict[str, Any] = {
            "excluded_predicates": excluded,
            "class_filter": class_filter,
        }

        structure = self._store.get_graph_structure_stats(self._graph_name, **kwargs)
        graph_node_count = int(structure.get("graph_node_count", 0) or 0)
        edge_count = int(structure.get("edge_count", 0) or 0)
        node_count = int(structure.get("node_count", 0) or 0)

        if graph_node_count == 0:
            logger.warning(
                "PushdownMetrics: no entity-entity edges found in %s", self._graph_name
            )
            return MetricsResult(
                stats=MetricsStats(
                    node_count=node_count,
                    connected_components=None,
                    elapsed_ms=self._elapsed_ms(t0),
                ),
                mode=MODE_PUSHDOWN,
                unavailable_metrics=list(UNAVAILABLE_METRICS),
            )

        # Degree centrality normalises the raw degree by the number of other
        # nodes, exactly as ``nx.degree_centrality`` does.
        divisor = float(graph_node_count - 1) if graph_node_count > 1 else 1.0

        top_rows = self._store.get_top_nodes_by_degree(
            self._graph_name, top_n=self._top_n, **kwargs
        )
        nodes: Dict[str, NodeMetrics] = {}
        node_types: Dict[str, str] = {}
        node_labels: Dict[str, str] = {}
        for row in top_rows:
            uri = row.get("node_uri") or ""
            if not uri:
                continue
            nodes[uri] = NodeMetrics(
                degree=round(int(row.get("degree", 0) or 0) / divisor, 6)
            )
            if row.get("type_uri"):
                node_types[uri] = row["type_uri"]
            if row.get("label"):
                node_labels[uri] = row["label"]

        stats = MetricsStats(
            node_count=node_count,
            graph_node_count=graph_node_count,
            edge_count=edge_count,
            connected_components=None,
            avg_degree=round(2.0 * edge_count / graph_node_count, 4),
            density=round(2.0 * edge_count / (graph_node_count * divisor), 6),
            elapsed_ms=self._elapsed_ms(t0),
        )

        profiles = self._build_type_profiles(kwargs, class_filter, divisor)

        logger.info(
            "PushdownMetrics: %s nodes / %s edges, %d profiles, top %d returned in %dms",
            f"{graph_node_count:,}",
            f"{edge_count:,}",
            len(profiles),
            len(nodes),
            stats.elapsed_ms,
        )

        return MetricsResult(
            nodes=nodes,
            stats=stats,
            node_types=node_types,
            node_labels=node_labels,
            entity_type_profiles=profiles,
            mode=MODE_PUSHDOWN,
            unavailable_metrics=list(UNAVAILABLE_METRICS),
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_type_profiles(
        self,
        kwargs: Dict[str, Any],
        class_filter: Optional[List[str]],
        divisor: float,
    ) -> Dict[str, EntityTypeProfile]:
        """Assemble per-type profiles from the three per-type aggregations."""
        edge_stats = self._store.get_type_edge_stats(self._graph_name, **kwargs)
        pairs = self._store.get_type_predicate_pairs(self._graph_name, **kwargs)
        instance_counts = {
            r.get("type_uri"): int(r.get("cnt", 0) or 0)
            for r in (self._store.get_type_distribution(self._graph_name) or [])
        }

        wanted = set(class_filter) if class_filter else None

        predicates_by_type: Dict[str, Set[str]] = {}
        for row in pairs:
            type_uri = row.get("type_uri") or ""
            predicate = row.get("predicate") or ""
            if not type_uri or not predicate:
                continue
            if wanted is not None and type_uri not in wanted:
                continue
            predicates_by_type.setdefault(type_uri, set()).add(predicate)

        profiles: Dict[str, EntityTypeProfile] = {}
        for row in edge_stats:
            type_uri = row.get("type_uri") or ""
            if not type_uri:
                continue
            if wanted is not None and type_uri not in wanted:
                continue

            connected = int(row.get("connected_count", 0) or 0)
            degree_sum = int(row.get("degree_sum", 0) or 0)
            # A class-filtered run surfaces every instance of the selected
            # type, isolated ones included, so its count is the full
            # population rather than only the connected instances.
            count = (
                instance_counts.get(type_uri, connected)
                if wanted is not None
                else connected
            )
            preds = predicates_by_type.get(type_uri, set())
            avg_degree = (
                round((degree_sum / connected) / divisor, 6) if connected else 0.0
            )
            reasons = flat_reasons(count, len(preds))

            profiles[type_uri] = EntityTypeProfile(
                uri=type_uri,
                count=count,
                avg_degree=avg_degree,
                # Both need triangle counts / shortest paths, which this path
                # does not compute — see ``unavailable_metrics``.
                avg_clustering=0.0,
                avg_betweenness=0.0,
                distinct_predicates=len(preds),
                has_temporal_predicates=has_temporal_predicates(preds),
                is_flat=bool(reasons),
                flat_reasons=reasons,
            )

        return profiles

    @staticmethod
    def _elapsed_ms(t0: float) -> int:
        return int((time.time() - t0) * 1000)
