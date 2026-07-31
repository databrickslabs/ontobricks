"""Graph metrics, computed entirely by the Databricks graph analytics job.

This is the *only* analytics compute path. The job reads the ``…_data``
snapshot the Build materialises from the R2RML VIEW, writes four tables, and
this module assembles them into a :class:`MetricsResult`:

* ``<out>``                  one row per scored node, with ``type_uri`` and ``label``
* ``<out>_summary``          node / edge / component counts, pivot and BFS flags
* ``<out>_type_profiles``    per-entity-type counts, degree sum, mean clustering
* ``<out>_type_predicates``  the distinct relationship predicates per type

Nothing here computes a metric. The one piece of logic the app still applies is
the flat-dataset heuristic in :mod:`back.core.graph_analysis.profiles`, which
turns ``(instance_count, distinct_predicates)`` into a human-readable verdict —
string matching on predicate names, not graph computation.

Betweenness and closeness are Brandes-Pich *estimates* sampled from pivots, so
they are flagged as approximate. They drop to unavailable when the pivot BFS was
truncated by its depth cap, since the distance sums would be biased — raise
``analytics_job_max_depth`` and re-run when that happens.

The read-back keeps the bounded-payload contract: only the union of the top-N
per metric is returned, never a row per node.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from back.core.errors import InfrastructureError
from back.core.graph_analysis.models import (
    MODE_JOB,
    EntityTypeProfile,
    MetricsRequest,
    MetricsResult,
    NodeMetrics,
)
from back.core.graph_analysis.profiles import flat_reasons, has_temporal_predicates
from back.core.helpers.SQLHelpers import SQLHelpers
from back.core.logging import get_logger

logger = get_logger(__name__)

#: Betweenness and closeness are sampled from a subset of source nodes rather
#: than computed exactly, so they are reported as approximate rather than as
#: plain values. Nothing is unavailable in this mode when pivots were sampled.
APPROXIMATE_METRICS = ("betweenness", "closeness")

#: What is missing when the job ran with no pivots, or when its BFS was
#: truncated and the estimates would be biased.
UNAVAILABLE_METRICS = ("betweenness", "closeness")


# ---------------------------------------------------------------------------
# Source resolution
# ---------------------------------------------------------------------------


def resolve_analytics_source(domain: Any, settings: Any) -> Tuple[str, str]:
    """Resolve the Unity Catalog table analytics reads.

    Always the ``…_data`` snapshot the Build materialises from the R2RML VIEW,
    never the engine's own graph relation. That is what makes a KPI identical
    on Lakehouse, Lakebase and Neo4j: the engines differ, the mapped snapshot
    does not.

    Returns ``(table, "")`` or ``("", reason)``, where the reason is written for
    whoever reads it in the UI.
    """
    table = (SQLHelpers.effective_databricks_table(domain, settings) or "").strip()
    table = table.replace("`", "")
    if not table:
        return "", (
            "No mapped-triples table could be resolved for this domain. Run "
            "Knowledge Graph → Build to materialise it."
        )
    if table.count(".") != 2:
        return "", (
            f"The mapped-triples table resolves to {table!r}, which is not a "
            f"catalog.schema.table name the Databricks job can read."
        )
    return table, ""


# ---------------------------------------------------------------------------
# Read-back SQL
# ---------------------------------------------------------------------------


def summary_query(output_table: str) -> str:
    """One-row run summary written by the job."""
    return (
        "SELECT node_count, total_node_count, edge_count, component_count, "
        "components_converged, pivot_count, bfs_complete "
        f"FROM {output_table}_summary"
    )


def top_nodes_query(output_table: str, top_n: int) -> str:
    """Union of the top *top_n* by each ranked metric.

    Degree is ranked here too: with the pushdown path gone, this query is the
    only source of the node payload. Ties break on ``node_uri`` so the result
    is deterministic across runs.
    """
    k = max(1, int(top_n))
    return (
        "WITH ranked AS (\n"
        "  SELECT node_uri, degree, pagerank, clustering, betweenness, closeness,\n"
        "         component_id, type_uri, label,\n"
        "         ROW_NUMBER() OVER (ORDER BY degree DESC, node_uri) AS rn_dg,\n"
        "         ROW_NUMBER() OVER (ORDER BY pagerank DESC, node_uri) AS rn_pr,\n"
        "         ROW_NUMBER() OVER (ORDER BY clustering DESC, node_uri) AS rn_cl,\n"
        "         ROW_NUMBER() OVER (ORDER BY betweenness DESC, node_uri) AS rn_bc,\n"
        "         ROW_NUMBER() OVER (ORDER BY closeness DESC, node_uri) AS rn_cn\n"
        f"  FROM {output_table}\n"
        ")\n"
        "SELECT node_uri, degree, pagerank, clustering, betweenness, closeness,\n"
        "       component_id, type_uri, label\n"
        "FROM ranked\n"
        f"WHERE rn_dg <= {k} OR rn_pr <= {k} OR rn_cl <= {k}\n"
        f"   OR rn_bc <= {k} OR rn_cn <= {k}\n"
        "ORDER BY pagerank DESC, node_uri"
    )


def type_profiles_query(output_table: str) -> str:
    """Per-entity-type rollups, as written by the job."""
    return (
        "SELECT type_uri, instance_count, connected_count, degree_sum,\n"
        "       avg_clustering, avg_betweenness\n"
        f"FROM {output_table}_type_profiles"
    )


def type_predicates_query(output_table: str) -> str:
    """Distinct ``(type, predicate)`` pairs, as written by the job."""
    return f"SELECT type_uri, predicate FROM {output_table}_type_predicates"


# ---------------------------------------------------------------------------
# JobMetrics
# ---------------------------------------------------------------------------


class JobMetrics:
    """Run the Lakeflow job, then assemble its output into a MetricsResult."""

    def __init__(
        self,
        source_table: str,
        *,
        runner: Any,
        query: Callable[[str], List[Dict[str, Any]]],
        output_table: str,
        top_n: int = 100,
        pagerank_iterations: int = 20,
        pivots: int = 64,
        max_depth: int = 32,
    ) -> None:
        self._source_table = source_table
        self._runner = runner
        self._query = query
        self._output_table = output_table
        self._top_n = max(1, int(top_n))
        self._pagerank_iterations = max(1, int(pagerank_iterations))
        self._pivots = max(0, int(pivots))
        self._max_depth = max(1, int(max_depth))

    def compute(
        self,
        request: MetricsRequest,
        on_progress: Optional[Callable[[int, str], None]] = None,
    ) -> MetricsResult:
        """Trigger the job and read every metric back."""
        t0 = time.time()

        outcome = self._runner.run_and_wait(
            source_table=self._source_table,
            output_table=self._output_table,
            exclude_predicates=list(request.predicate_filter or []) or None,
            class_filter=list(request.class_filter or []) or None,
            pagerank_iterations=self._pagerank_iterations,
            pivots=self._pivots,
            max_depth=self._max_depth,
            on_progress=on_progress,
        )
        if not outcome.get("success"):
            raise InfrastructureError(
                "The graph analytics job did not complete successfully",
                detail=(
                    f"{outcome.get('life_cycle_state', '')} "
                    f"{outcome.get('result_state', '')} "
                    f"{outcome.get('message', '')}".strip()
                    + (
                        f" — {outcome['run_page_url']}"
                        if outcome.get("run_page_url")
                        else ""
                    )
                ),
            )

        if on_progress:
            on_progress(85, "Reading results back")

        result = MetricsResult(mode=MODE_JOB)
        self._read_summary(result)
        self._read_nodes(result)
        self._read_type_profiles(result)
        result.stats.elapsed_ms = int((time.time() - t0) * 1000)

        logger.info(
            "JobMetrics: %s nodes / %s edges, %s components, "
            "%d profiles, %d nodes returned in %dms",
            f"{result.stats.graph_node_count:,}",
            f"{result.stats.edge_count:,}",
            result.stats.connected_components,
            len(result.entity_type_profiles),
            len(result.nodes),
            result.stats.elapsed_ms,
        )
        return result

    # ------------------------------------------------------------------

    def _read_summary(self, result: MetricsResult) -> None:
        """Fill the structure counts and decide on the sampled metrics."""
        rows = self._query(summary_query(self._output_table)) or []
        if not rows:
            # No summary means no run to trust: withhold the sampled metrics
            # rather than charting zeros as real centrality values.
            result.approximate_metrics = []
            result.unavailable_metrics = list(UNAVAILABLE_METRICS)
            result.pivot_count = 0
            return

        row = rows[0]
        graph_node_count = int(row.get("node_count", 0) or 0)
        edge_count = int(row.get("edge_count", 0) or 0)
        divisor = float(graph_node_count - 1) if graph_node_count > 1 else 1.0

        result.stats.node_count = int(row.get("total_node_count", 0) or 0)
        result.stats.graph_node_count = graph_node_count
        result.stats.edge_count = edge_count
        result.stats.connected_components = int(row.get("component_count", 0) or 0)
        if graph_node_count:
            result.stats.avg_degree = round(2.0 * edge_count / graph_node_count, 4)
            result.stats.density = round(
                2.0 * edge_count / (graph_node_count * divisor), 6
            )

        if not row.get("components_converged", True):
            # Surfaced rather than silently trusted: an unconverged label
            # propagation over-counts components.
            logger.warning(
                "Component labelling did not converge for %s — the component "
                "count is a lower bound",
                self._output_table,
            )

        pivot_count = int(row.get("pivot_count", 0) or 0)
        bfs_complete = bool(row.get("bfs_complete", True))
        if pivot_count > 0 and bfs_complete:
            result.approximate_metrics = list(APPROXIMATE_METRICS)
            result.unavailable_metrics = []
            result.pivot_count = pivot_count
        else:
            result.approximate_metrics = []
            result.unavailable_metrics = list(UNAVAILABLE_METRICS)
            result.pivot_count = 0
            if pivot_count > 0 and not bfs_complete:
                logger.warning(
                    "The pivot BFS for %s hit its depth cap; betweenness and "
                    "closeness are reported as unavailable rather than truncated",
                    self._output_table,
                )

    def _read_nodes(self, result: MetricsResult) -> None:
        """Fill the bounded node payload from the top-N union."""
        rows = self._query(top_nodes_query(self._output_table, self._top_n)) or []
        for row in rows:
            uri = row.get("node_uri") or ""
            if not uri:
                continue
            node = NodeMetrics(
                degree=round(float(row.get("degree", 0.0) or 0.0), 6),
                pagerank=round(float(row.get("pagerank", 0.0) or 0.0), 8),
                clustering=round(float(row.get("clustering", 0.0) or 0.0), 6),
            )
            if result.approximate_metrics:
                node.betweenness = round(float(row.get("betweenness", 0.0) or 0.0), 8)
                node.closeness = round(float(row.get("closeness", 0.0) or 0.0), 6)
            result.nodes[uri] = node
            if row.get("type_uri"):
                result.node_types[uri] = row["type_uri"]
            if row.get("label"):
                result.node_labels[uri] = row["label"]

        result.top_pagerank = [
            uri
            for uri, _ in sorted(
                result.nodes.items(), key=lambda kv: (-kv[1].pagerank, kv[0])
            )
        ][: self._top_n]

    def _read_type_profiles(self, result: MetricsResult) -> None:
        """Assemble the per-type profiles and label the flat ones."""
        profiles = self._query(type_profiles_query(self._output_table)) or []
        pairs = self._query(type_predicates_query(self._output_table)) or []

        predicates_by_type: Dict[str, Set[str]] = {}
        for row in pairs:
            type_uri = row.get("type_uri") or ""
            predicate = row.get("predicate") or ""
            if type_uri and predicate:
                predicates_by_type.setdefault(type_uri, set()).add(predicate)

        graph_node_count = result.stats.graph_node_count or 0
        divisor = float(graph_node_count - 1) if graph_node_count > 1 else 1.0

        for row in profiles:
            type_uri = row.get("type_uri") or ""
            if not type_uri:
                continue
            count = int(row.get("instance_count", 0) or 0)
            connected = int(row.get("connected_count", 0) or 0)
            degree_sum = int(row.get("degree_sum", 0) or 0)
            preds = predicates_by_type.get(type_uri, set())
            reasons = flat_reasons(count, len(preds))

            result.entity_type_profiles[type_uri] = EntityTypeProfile(
                uri=type_uri,
                count=count,
                # Normalised the same way ``nx.degree_centrality`` does, so a
                # type's average degree is comparable with a node's.
                avg_degree=(
                    round((degree_sum / connected) / divisor, 6) if connected else 0.0
                ),
                avg_clustering=round(float(row.get("avg_clustering", 0.0) or 0.0), 6),
                avg_betweenness=round(float(row.get("avg_betweenness", 0.0) or 0.0), 8),
                distinct_predicates=len(preds),
                has_temporal_predicates=has_temporal_predicates(preds),
                is_flat=bool(reasons),
                flat_reasons=reasons,
            )
