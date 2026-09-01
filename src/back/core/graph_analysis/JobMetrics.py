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
from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Tuple

from back.core.errors import InfrastructureError, ValidationError
from back.core.graph_analysis.models import (
    DEFAULT_DISTRIBUTION_BINS,
    MODE_JOB,
    NODE_METRIC_KEYS,
    EntityTypeProfile,
    MetricDistribution,
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

METRIC_SERIES_COLUMNS = frozenset(
    {"pagerank", "betweenness", "degree", "closeness", "clustering"}
)
METRIC_SERIES_SAMPLE_THRESHOLD = 5_000
METRIC_SERIES_SAMPLE_SIZE = 2_000


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


@contextmanager
def analytics_snapshot(domain: Any, settings: Any, source_table: str) -> Iterator[str]:
    """Yield a table the analytics job can scan repeatedly, cleaning up after.

    In the default materialization ``source_table`` is already a Delta table
    and this yields it unchanged. Under view-only materialization it is a
    pass-through view over the R2RML gateway, and the job's iterative BFS would
    re-run that whole query on every scan — so a disposable snapshot is
    materialized for the run and dropped on the way out, including when the run
    raises.

    A failure to drop is logged rather than raised: the run's result matters
    more than the leftover, which Settings → Lakehouse lists for purging.
    """
    from back.core.graphdb.GraphDBFactory import GraphDBFactory

    if GraphDBFactory.resolve_lakehouse_materialization(domain, settings) != "view":
        yield source_table
        return

    from back.core.graphdb.delta import _table_naming, materialize
    from back.core.graphdb.delta.DeltaBase import create_databricks_client

    snapshot = _table_naming.analytics_snapshot_fqn(domain, settings)
    client = create_databricks_client(domain, settings)
    if not snapshot or client is None:
        raise InfrastructureError(
            "Graph analytics needs a temporary Delta snapshot for a view-only "
            "domain, and it could not be prepared",
            detail=(
                "No snapshot table name could be derived"
                if not snapshot
                else "No SQL warehouse is configured for this domain"
            ),
        )

    logger.info("Materializing analytics snapshot %s from %s", snapshot, source_table)
    materialize.materialize_from_view(client, source_table, snapshot)
    try:
        yield snapshot
    finally:
        logger.info("Dropping analytics snapshot %s", snapshot)
        materialize.drop_relation(client, snapshot, kind="table")


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


def validate_metric_series_column(metric: str) -> str:
    """Return a validated metric column name for metric-series SQL."""
    metric_name = str(metric or "").strip()
    if metric_name not in METRIC_SERIES_COLUMNS:
        raise ValidationError("Unsupported graph metric")
    return metric_name


def metric_series_query(output_table: str, metric: str) -> str:
    """One exhaustive series query for a validated metric column."""
    metric_name = validate_metric_series_column(metric)
    return (
        "SELECT node_uri, label, "
        f"{metric_name} AS score\n"
        f"FROM {output_table}\n"
        f"ORDER BY {metric_name} DESC, node_uri ASC"
    )


def sample_metric_series(
    rows: List[Dict[str, Any]],
    *,
    threshold: int = METRIC_SERIES_SAMPLE_THRESHOLD,
    sample_size: int = METRIC_SERIES_SAMPLE_SIZE,
) -> Tuple[List[Dict[str, Any]], List[int], bool]:
    """Return sampled rows and their original one-based ranks.

    Uses pure LTTB above *threshold*. For *threshold* or fewer rows, returns
    the full input with identity ranks.
    """
    total = len(rows)
    if total <= max(1, int(threshold)):
        return list(rows), [i + 1 for i in range(total)], False

    target = min(total, max(3, int(sample_size)))
    if target >= total:
        return list(rows), [i + 1 for i in range(total)], False

    def _score(index: int) -> float:
        try:
            return float(rows[index].get("score", 0.0) or 0.0)
        except (TypeError, ValueError):
            return 0.0

    bucket_width = float(total - 2) / float(target - 2)
    sampled_indexes: List[int] = [0]
    a = 0

    for bucket in range(target - 2):
        avg_start = int((bucket + 1) * bucket_width) + 1
        avg_end = int((bucket + 2) * bucket_width) + 1
        avg_start = min(max(1, avg_start), total - 1)
        avg_end = min(max(avg_start + 1, avg_end), total)

        avg_count = max(1, avg_end - avg_start)
        avg_x = 0.0
        avg_y = 0.0
        for idx in range(avg_start, avg_end):
            avg_x += float(idx)
            avg_y += _score(idx)
        avg_x /= float(avg_count)
        avg_y /= float(avg_count)

        range_start = int(bucket * bucket_width) + 1
        range_end = int((bucket + 1) * bucket_width) + 1
        range_start = min(max(1, range_start), total - 1)
        range_end = min(max(range_start + 1, range_end), total - 1)

        ax = float(a)
        ay = _score(a)
        max_area = -1.0
        best = range_start
        for idx in range(range_start, range_end):
            area = abs((ax - avg_x) * (_score(idx) - ay) - (ax - float(idx)) * (avg_y - ay))
            if area > max_area:
                max_area = area
                best = idx
        sampled_indexes.append(best)
        a = best

    sampled_indexes.append(total - 1)
    sampled_rows = [rows[idx] for idx in sampled_indexes]
    ranks = [idx + 1 for idx in sampled_indexes]
    return sampled_rows, ranks, True


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


def distribution_bounds_query(output_table: str) -> str:
    """Per-metric ``MIN`` / ``MAX`` / ``AVG`` over every scored node.

    Split from :func:`distributions_query` rather than folded into it: combining
    them forces the bounds into either a repeated ``CASE`` expression in the
    ``GROUP BY`` or a constant-ordinal ``GROUP BY``, and the portable spelling of
    each differs across SQLite, Spark and Postgres. Two plain aggregates are
    individually obvious, and the extra round trip is nothing next to a job that
    runs for minutes.
    """
    columns = ",\n".join(
        f"       MIN({m}) AS lo_{m},\n"
        f"       MAX({m}) AS hi_{m},\n"
        f"       AVG({m}) AS mean_{m}"
        for m in NODE_METRIC_KEYS
    )
    return f"SELECT\n{columns}\nFROM {output_table}"


def _bin_index_expression(metric: str, bins: int) -> str:
    """Portable bucket assignment for one metric.

    Written as a ``CASE`` rather than with ``least`` / ``greatest``: the *job*
    test harness registers those as custom SQLite functions, but this read-back
    executes on SQLite (tests) and Databricks/Spark (production), parses under
    all three sqlglot dialects, and would need the ``ROUND`` argument cast to
    ``numeric`` to execute on Postgres.

    The three branches are the two degenerate cases plus the general one:

    * ``hi <= lo`` — every node scores identically (a fully regular graph, or a
      metric the run left all-zero). The general expression would divide by
      zero, so everything collapses into bin 0.
    * ``value >= hi`` — the top-scoring node would otherwise compute ``bins``,
      one index past the last bucket.
    * otherwise, scale into ``[0, bins)`` via ``CAST(ROUND(..., 10) AS INTEGER)``.
      Every metric is non-negative by construction, which is what makes integer
      truncation equivalent to ``floor`` and saves needing a ``floor`` function.

    The ``ROUND(..., 10)`` is load-bearing. A value sitting exactly on a bin
    boundary is common — any metric quantised to a few distinct values, which
    ``clustering`` and ``degree`` routinely are — and binary64 puts the scaled
    result a few parts in 10^16 below the integer it should equal, so bare
    truncation drops it into the bin below. Rounding at the tenth decimal
    absorbs that while leaving genuine fractional positions untouched.
    """
    return (
        f"CASE\n"
        f"      WHEN b.hi <= b.lo THEN 0\n"
        f"      WHEN t.{metric} >= b.hi THEN {int(bins) - 1}\n"
        f"      ELSE CAST(ROUND((t.{metric} - b.lo) * {int(bins)}"
        f" / (b.hi - b.lo), 10) AS INTEGER)\n"
        f"    END"
    )


def distributions_query(
    output_table: str, bins: int = DEFAULT_DISTRIBUTION_BINS
) -> str:
    """Node counts per histogram bin, for all five metrics.

    One ``SELECT`` per metric, stacked with ``UNION ALL``. Each groups inside a
    subquery so the ``GROUP BY`` targets a plain column name — grouping directly
    on the ``CASE`` expression or on a constant ordinal is spelled differently
    across the three engines this has to satisfy.

    Bins containing no nodes produce no row; the caller pads them to zero.
    """
    # zero bins would divide by zero in _bin_index_expression's general branch.
    bins = max(1, int(bins))
    parts = [
        f"SELECT '{metric}' AS metric, bin_index, COUNT(*) AS node_count\n"
        f"FROM (\n"
        f"  SELECT {_bin_index_expression(metric, bins)} AS bin_index\n"
        f"  FROM {output_table} t\n"
        f"  CROSS JOIN (\n"
        f"    SELECT MIN({metric}) AS lo, MAX({metric}) AS hi"
        f" FROM {output_table}\n"
        f"  ) b\n"
        f") q\n"
        f"GROUP BY bin_index"
        for metric in NODE_METRIC_KEYS
    ]
    return "\nUNION ALL\n".join(parts)


def interpolate_quantile(
    bins: List[int], lo: float, hi: float, q: float
) -> float:
    """Estimate the *q*-th quantile from histogram bin counts.

    The read-back has to run on engines without a percentile function (see
    :func:`distribution_bounds_query`), so the quantile is recovered from the
    bins by assuming each bucket's mass is spread evenly across its width. The
    error is therefore bounded by one bin width, which is why the UI presents
    the result as an approximation rather than as a measured median.

    Returns *lo* for an empty or zero-total distribution, and for the degenerate
    ``hi == lo`` case where every node scored the same.
    """
    total = sum(bins)
    if not bins or total <= 0 or hi <= lo:
        return float(lo)

    width = (hi - lo) / len(bins)
    target = total * q
    cumulative = 0
    for index, count in enumerate(bins):
        if count <= 0:
            continue
        if cumulative + count >= target:
            # Clamped because q == 0 puts the target at or below the first
            # bucket's left edge, and q == 1 at its right edge.
            fraction = min(1.0, max(0.0, (target - cumulative) / count))
            return lo + (index + fraction) * width
        cumulative += count
    return float(hi)


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
        distribution_bins: int = DEFAULT_DISTRIBUTION_BINS,
    ) -> None:
        self._source_table = source_table
        self._runner = runner
        self._query = query
        self._output_table = output_table
        self._top_n = max(1, int(top_n))
        self._pagerank_iterations = max(1, int(pagerank_iterations))
        self._pivots = max(0, int(pivots))
        self._max_depth = max(1, int(max_depth))
        # Injectable so tests can assert against a hand-computable count;
        # production has one call site which never overrides this.
        self._distribution_bins = max(1, int(distribution_bins))

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
        self._read_distributions(result)
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

    def _read_distributions(self, result: MetricsResult) -> None:
        """Summarise every scored node into per-metric histograms.

        Called after :meth:`_read_summary` because it depends on that method's
        verdict on which metrics are trustworthy.

        Never raises. A distribution is a presentation aggregate, and an analysis
        that successfully scored the whole graph must not be discarded because
        the histogram query failed — the UI already renders an explanatory empty
        state for a result without one.
        """
        try:
            bounds_rows = self._query(
                distribution_bounds_query(self._output_table)
            ) or []
            if not bounds_rows:
                return
            bounds = bounds_rows[0]

            counts: Dict[str, Dict[int, int]] = {}
            for row in self._query(
                distributions_query(self._output_table, self._distribution_bins)
            ) or []:
                metric = row.get("metric") or ""
                if metric not in NODE_METRIC_KEYS:
                    continue
                counts.setdefault(metric, {})[
                    int(row.get("bin_index", 0) or 0)
                ] = int(row.get("node_count", 0) or 0)

            for metric in NODE_METRIC_KEYS:
                # A metric this run could not compute is stored as zeros. Its
                # histogram would be a single tall bar at zero, which reads as a
                # measurement rather than as an absence.
                if metric in result.unavailable_metrics:
                    continue

                lo = float(bounds.get(f"lo_{metric}", 0.0) or 0.0)
                hi = float(bounds.get(f"hi_{metric}", 0.0) or 0.0)
                mean = float(bounds.get(f"mean_{metric}", 0.0) or 0.0)

                per_bin = counts.get(metric, {})
                # Padded rather than sparse: the front end indexes positionally,
                # and a bin with no nodes is a real, meaningful zero.
                bins = [
                    per_bin.get(i, 0) for i in range(self._distribution_bins)
                ]

                result.distributions[metric] = MetricDistribution(
                    bins=bins,
                    bin_count=self._distribution_bins,
                    lo=round(lo, 8),
                    hi=round(hi, 8),
                    mean=round(mean, 8),
                    median=round(interpolate_quantile(bins, lo, hi, 0.5), 8),
                    p90=round(interpolate_quantile(bins, lo, hi, 0.9), 8),
                )
        except Exception:  # noqa: BLE001 — a chart is never worth failing a run
            logger.warning(
                "could not read metric distributions for %s; the result is "
                "complete but the Analytics page will show no histograms",
                self._output_table,
                exc_info=True,
            )
            result.distributions.clear()
