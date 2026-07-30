"""Graph metrics for oversized graphs, with the iterative parts run on Databricks.

This is the third compute mode, above ``in_memory`` and ``pushdown``:

* aggregates (structure counts, degree centrality, type profiles) come from the
  engine-side SQL of :class:`~back.core.graph_analysis.PushdownMetrics`, reused
  verbatim so the two modes cannot drift apart;
* PageRank, connected components and the clustering coefficient come from the
  serverless job in ``resources/graph_analytics.job.yml``, which the app
  triggers and then reads back from a Delta table.

That leaves only betweenness and closeness unavailable — both need all-pairs
shortest paths, which is not worth a distributed implementation here. Narrow
the analysis with an entity-type filter to get those from the exact in-memory
path.

**Prerequisite.** The graph must be readable from Spark as a Unity Catalog
table, which is not true for every engine —
:func:`resolve_spark_source` is where that is decided, and it refuses rather
than guessing.

The read-back keeps the bounded-payload contract: only the union of the top-N
per metric is returned, never a row per node.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from back.core.errors import InfrastructureError
from back.core.graph_analysis.PushdownMetrics import PushdownMetrics
from back.core.graph_analysis.models import (
    MODE_JOB,
    MetricsRequest,
    MetricsResult,
    NodeMetrics,
)
from back.core.logging import get_logger

logger = get_logger(__name__)

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

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


def resolve_spark_source(store: Any, graph_name: str) -> Tuple[str, str]:
    """Resolve the Unity Catalog table the Spark job should read.

    Returns ``(table, "")`` when the graph is reachable from Spark, or
    ``("", reason)`` when it is not. The reason is written for the person
    reading it in the UI, because "the job can't see your data" is a
    configuration problem only they can fix.

    The three cases that matter:

    * **Delta** — the graph already *is* a UC table; use it directly.
    * **Lakebase managed_synced** — Postgres holds a synced copy of a UC
      table, so point Spark at that upstream source rather than at Postgres.
    * **Lakebase app_managed / Neo4j** — the triples live only in a store
      Spark cannot read. Refused.
    """
    if store is None:
        return "", "No graph store is configured."

    if getattr(store, "query_dialect", "sql") != "sql":
        return "", (
            "This engine is not SQL-based, so the Databricks job cannot read "
            "its data. Use an entity-type filter to analyse a subgraph in-app."
        )

    # Lakebase: only the managed_synced mode has a UC table upstream.
    if hasattr(store, "sync_mode"):
        if not getattr(store, "is_synced", False):
            return "", (
                "This Lakebase graph is in app_managed mode, so its triples "
                "exist only in Postgres and are not readable from Spark. "
                "Switch the graph to managed_synced to use the job."
            )
        source = _lakebase_uc_source(store, graph_name)
        if not source:
            return "", (
                "Could not determine the Unity Catalog source table behind "
                "this synced Lakebase graph. Rebuild the graph so the synced "
                "table is registered, then retry."
            )
        return source, ""

    # Delta and anything else exposing a fully-qualified relation.
    try:
        relation = store.sql_table_reference(graph_name)
    except Exception as exc:  # noqa: BLE001
        return "", f"Could not resolve the graph's table name: {exc}"

    relation = (relation or "").replace("`", "").strip()
    if relation.count(".") != 2:
        return "", (
            f"The graph resolves to {relation!r}, which is not a "
            f"catalog.schema.table name the Databricks job can read."
        )
    return relation, ""


def _lakebase_uc_source(store: Any, graph_name: str) -> str:
    """Read ``spec.source_table_full_name`` off the synced-table object."""
    try:
        manager = store.synced_manager()
        synced = manager.get(store.synced_uc_name(graph_name))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not fetch the synced table for %s: %s", graph_name, exc)
        return ""
    if synced is None:
        return ""
    spec = getattr(synced, "spec", None)
    if spec is not None:
        value = getattr(spec, "source_table_full_name", "") or ""
        if value:
            return str(value)
    if isinstance(synced, dict):
        return str((synced.get("spec") or {}).get("source_table_full_name", "") or "")
    return ""


# ---------------------------------------------------------------------------
# Read-back SQL
# ---------------------------------------------------------------------------


def _quote(value: str) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def summary_query(output_table: str) -> str:
    """One-row run summary written by the job."""
    return (
        "SELECT node_count, edge_count, component_count, components_converged, "
        "pivot_count, bfs_complete "
        f"FROM {output_table}_summary"
    )


def top_nodes_query(output_table: str, top_n: int) -> str:
    """Union of the top *top_n* by PageRank and by clustering coefficient.

    Degree ranking is deliberately absent: the pushdown base result already
    carries the top nodes by degree, and this query's job is to add what the
    engine-side SQL could not rank. Ties break on ``node_uri`` so the result is
    deterministic across runs.
    """
    k = max(1, int(top_n))
    return (
        "WITH ranked AS (\n"
        "  SELECT node_uri, degree, pagerank, clustering, betweenness, closeness,\n"
        "         component_id,\n"
        "         ROW_NUMBER() OVER (ORDER BY pagerank DESC, node_uri) AS rn_pr,\n"
        "         ROW_NUMBER() OVER (ORDER BY clustering DESC, node_uri) AS rn_cl,\n"
        "         ROW_NUMBER() OVER (ORDER BY betweenness DESC, node_uri) AS rn_bc,\n"
        "         ROW_NUMBER() OVER (ORDER BY closeness DESC, node_uri) AS rn_cn\n"
        f"  FROM {output_table}\n"
        ")\n"
        "SELECT node_uri, degree, pagerank, clustering, betweenness, closeness,\n"
        "       component_id\n"
        "FROM ranked\n"
        f"WHERE rn_pr <= {k} OR rn_cl <= {k} OR rn_bc <= {k} OR rn_cn <= {k}\n"
        "ORDER BY pagerank DESC, node_uri"
    )


def metrics_for_nodes_query(output_table: str, node_uris: List[str]) -> str:
    """Fetch the job's metrics for an explicit node set.

    Used to fill in the nodes the pushdown pass already selected by degree, so
    they are charted with a real PageRank rather than a zero.
    """
    in_list = ", ".join(_quote(u) for u in node_uris)
    return (
        "SELECT node_uri, degree, pagerank, clustering, betweenness, closeness,\n"
        "       component_id\n"
        f"FROM {output_table}\n"
        f"WHERE node_uri IN ({in_list})"
    )


def type_clustering_query(output_table: str, source_table: str) -> str:
    """Mean clustering coefficient per entity type.

    Joins the per-node output back to ``rdf:type`` in the source triples. This
    is what lets the job mode fill in ``EntityTypeProfile.avg_clustering``,
    which the pushdown mode has to leave at zero.
    """
    return (
        "SELECT t.object AS type_uri,\n"
        "       AVG(m.clustering) AS avg_clustering,\n"
        "       COUNT(*) AS instance_count\n"
        f"FROM {output_table} m\n"
        f"JOIN {source_table} t\n"
        f"  ON t.subject = m.node_uri AND t.predicate = {_quote(RDF_TYPE)}\n"
        "GROUP BY t.object"
    )


# ---------------------------------------------------------------------------
# JobMetrics
# ---------------------------------------------------------------------------


class JobMetrics:
    """Run the Lakeflow job, then merge its output onto the pushdown result."""

    def __init__(
        self,
        store: Any,
        graph_name: str,
        *,
        runner: Any,
        query: Callable[[str], List[Dict[str, Any]]],
        output_table: str,
        top_n: int = 100,
        pagerank_iterations: int = 20,
        pivots: int = 64,
    ) -> None:
        self._store = store
        self._graph_name = graph_name
        self._runner = runner
        self._query = query
        self._output_table = output_table
        self._top_n = max(1, int(top_n))
        self._pagerank_iterations = max(1, int(pagerank_iterations))
        self._pivots = max(0, int(pivots))

    def compute(
        self,
        request: MetricsRequest,
        on_progress: Optional[Callable[[int, str], None]] = None,
    ) -> MetricsResult:
        """Compute the full metric set, offloading the iterative parts."""
        t0 = time.time()

        source_table, reason = resolve_spark_source(self._store, self._graph_name)
        if not source_table:
            raise InfrastructureError(
                "The graph analytics job cannot read this graph", detail=reason
            )

        # The engine-side aggregations run while the job is being scheduled
        # anyway, and reusing them keeps one definition of the type profiles.
        if on_progress:
            on_progress(20, "Aggregating graph structure")
        base = PushdownMetrics(
            self._store, self._graph_name, top_n=self._top_n
        ).compute(request)

        outcome = self._runner.run_and_wait(
            source_table=source_table,
            output_table=self._output_table,
            exclude_predicates=None,
            pagerank_iterations=self._pagerank_iterations,
            pivots=self._pivots,
            on_progress=on_progress,
        )
        if not outcome.get("success"):
            raise InfrastructureError(
                "The graph analytics job did not complete successfully",
                detail=(
                    f"{outcome.get('life_cycle_state', '')} "
                    f"{outcome.get('result_state', '')} "
                    f"{outcome.get('message', '')}".strip()
                    + (f" — {outcome['run_page_url']}" if outcome.get("run_page_url") else "")
                ),
            )

        if on_progress:
            on_progress(85, "Reading results back")
        self._merge_job_results(base, source_table)

        base.mode = MODE_JOB
        base.stats.elapsed_ms = int((time.time() - t0) * 1000)

        logger.info(
            "JobMetrics: %s nodes / %s edges, %s components, %d nodes returned in %dms",
            f"{base.stats.graph_node_count:,}",
            f"{base.stats.edge_count:,}",
            base.stats.connected_components,
            len(base.nodes),
            base.stats.elapsed_ms,
        )
        return base

    # ------------------------------------------------------------------

    def _merge_job_results(self, base: MetricsResult, source_table: str) -> None:
        """Overlay the job's per-node scores and component count onto *base*."""
        summary = self._query(summary_query(self._output_table)) or []
        # Default to "not computed" so a summary the job could not write never
        # results in sampled zeros being charted as real centrality values.
        pivot_count = 0
        bfs_complete = True
        if summary:
            row = summary[0]
            base.stats.connected_components = int(row.get("component_count", 0) or 0)
            pivot_count = int(row.get("pivot_count", 0) or 0)
            bfs_complete = bool(row.get("bfs_complete", True))
            if not row.get("components_converged", True):
                # Surfaced rather than silently trusted: an unconverged label
                # propagation over-counts components.
                logger.warning(
                    "Component labelling did not converge for %s — the component "
                    "count is a lower bound",
                    self._graph_name,
                )

        # A truncated BFS biases both estimates, so they are withheld rather
        # than published with a caveat nobody would read.
        if pivot_count > 0 and bfs_complete:
            base.approximate_metrics = list(APPROXIMATE_METRICS)
            base.unavailable_metrics = []
            base.pivot_count = pivot_count
        else:
            base.approximate_metrics = []
            base.unavailable_metrics = list(UNAVAILABLE_METRICS)
            base.pivot_count = 0
            if pivot_count > 0 and not bfs_complete:
                logger.warning(
                    "The pivot BFS for %s hit its depth cap; betweenness and "
                    "closeness are reported as unavailable rather than truncated",
                    self._graph_name,
                )

        rows = list(self._query(top_nodes_query(self._output_table, self._top_n)) or [])
        # Fill in the degree-ranked nodes the pushdown pass already chose, so
        # they are not charted with a zero PageRank.
        known = [u for u in base.nodes if u not in {r.get("node_uri") for r in rows}]
        if known:
            rows += list(
                self._query(metrics_for_nodes_query(self._output_table, known)) or []
            )

        for row in rows:
            uri = row.get("node_uri") or ""
            if not uri:
                continue
            node = base.nodes.get(uri) or NodeMetrics()
            node.pagerank = round(float(row.get("pagerank", 0.0) or 0.0), 8)
            node.clustering = round(float(row.get("clustering", 0.0) or 0.0), 6)
            if base.approximate_metrics:
                node.betweenness = round(float(row.get("betweenness", 0.0) or 0.0), 8)
                node.closeness = round(float(row.get("closeness", 0.0) or 0.0), 6)
            # The job's ``degree`` is already normalised, but the pushdown pass
            # computed it too; prefer the existing value so a node's degree
            # never changes depending on which query returned it.
            if uri not in base.nodes:
                node.degree = round(float(row.get("degree", 0.0) or 0.0), 6)
            base.nodes[uri] = node

        base.top_pagerank = [
            uri
            for uri, _ in sorted(
                base.nodes.items(), key=lambda kv: (-kv[1].pagerank, kv[0])
            )
        ][: self._top_n]

        self._merge_type_clustering(base, source_table)

    def _merge_type_clustering(self, base: MetricsResult, source_table: str) -> None:
        """Fill ``avg_clustering`` on the type profiles the job can now supply."""
        try:
            rows = self._query(
                type_clustering_query(self._output_table, source_table)
            ) or []
        except Exception as exc:  # noqa: BLE001
            # A missing per-type rollup is not worth failing an otherwise
            # complete run over.
            logger.warning("Could not read per-type clustering: %s", exc)
            return
        for row in rows:
            type_uri = row.get("type_uri") or ""
            profile = base.entity_type_profiles.get(type_uri)
            if profile is None:
                continue
            profile.avg_clustering = round(float(row.get("avg_clustering", 0.0) or 0.0), 6)
