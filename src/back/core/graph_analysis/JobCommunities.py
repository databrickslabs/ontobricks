"""Community detection, computed by the Databricks community-detection job.

The job (``src/jobs/graph_analytics_job.py --task communities``) runs label
propagation over the ``…_data`` snapshot and writes five tables:

* ``<out>``                one row per node: ``community_rank``, degree, type, label
* ``<out>_sizes``          one row per community, ranked by size
* ``<out>_preview_nodes``  the capped sample the Community page draws
* ``<out>_preview_edges``  edges among the preview nodes
* ``<out>_summary``        counts, modularity, convergence, run timestamp

This module triggers the run and reads those tables back. Every read is
bounded — top-N clusters, one page of members, keyset batches — because a
partition of a 100M-node graph never fits in the app or in a response.

``community_rank`` (0 = largest) is the cluster id everywhere: in the API, in
the UI and in the materialized ``…/cluster/c-{id}`` URIs.
"""

from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from back.core.errors import InfrastructureError
from back.core.graph_analysis.preflight import names_a_missing_object
from back.core.logging import get_logger

logger = get_logger(__name__)

#: Upper bounds on what one request may read.
TOP_CLUSTERS_MAX = 200
MEMBERS_PAGE_MAX = 500
DEFAULT_TOP_CLUSTERS = 50
DEFAULT_MEMBERS_PAGE = 100


def community_output_table(output_schema: str, domain: Any, graph_name: str) -> str:
    """Per-domain-version output table name for the community job.

    Only ``[A-Za-z0-9_]`` survives: the name is interpolated unquoted into
    generated SQL on both the job and the read-back side.
    """
    folder = str(getattr(domain, "uc_domain_folder", "") or "") or graph_name
    version = str(getattr(domain, "current_version", "") or "")
    slug = re.sub(r"[^A-Za-z0-9_]+", "_", f"{folder}_{version}").strip("_").lower()
    return f"{output_schema}.graph_communities_{slug or 'default'}"


def _local_label(uri: str) -> str:
    if not uri:
        return ""
    tail = uri.rstrip("/").rsplit("/", 1)[-1]
    return tail.rsplit("#", 1)[-1] or uri


def _bounded(value: Any, default: int, upper: int) -> int:
    try:
        n = int(value)
    except (TypeError, ValueError):
        n = default
    return max(1, min(n, upper))


# ---------------------------------------------------------------------------
# Read-back SQL
# ---------------------------------------------------------------------------


def summary_query(out: str) -> str:
    return (
        "SELECT node_count, edge_count, community_count, modularity, iterations, "
        "converged, algorithm, class_filter, source_table, computed_at "
        f"FROM {out}_summary"
    )


def size_stats_query(out: str) -> str:
    return (
        "SELECT COUNT(*) AS n, MIN(size) AS min_size, MAX(size) AS max_size, "
        f"AVG(size) AS avg_size FROM {out}_sizes"
    )


def size_at_query(out: str, offset: int) -> str:
    """Size at a position in ascending order — the median without a percentile function."""
    return (
        f"SELECT size FROM {out}_sizes ORDER BY size, community_rank "
        f"LIMIT 1 OFFSET {max(0, int(offset))}"
    )


def top_clusters_query(out: str, limit: int) -> str:
    return (
        "SELECT community_rank, size, seed_uri, seed_label "
        f"FROM {out}_sizes WHERE community_rank < {int(limit)} "
        "ORDER BY community_rank"
    )


def preview_nodes_query(out: str) -> str:
    return (
        "SELECT node_uri, community_rank, label, type_uri "
        f"FROM {out}_preview_nodes ORDER BY community_rank, node_uri"
    )


def preview_edges_query(out: str) -> str:
    return f"SELECT src, dst FROM {out}_preview_edges"


def cluster_size_query(out: str, cluster_id: int) -> str:
    return f"SELECT size FROM {out}_sizes WHERE community_rank = {int(cluster_id)}"


def members_query(out: str, cluster_id: int, offset: int, limit: int) -> str:
    """One page of a community, highest degree first."""
    return (
        "SELECT node_uri, label, type_uri "
        f"FROM {out} WHERE community_rank = {int(cluster_id)} "
        "ORDER BY degree_raw DESC, node_uri "
        f"LIMIT {int(limit)} OFFSET {max(0, int(offset))}"
    )


def membership_batch_query(out: str, after_uri: str, limit: int) -> str:
    """Keyset page of ``(node_uri, community_rank)`` — no OFFSET scan."""
    esc = (after_uri or "").replace("'", "''")
    return (
        f"SELECT node_uri, community_rank FROM {out} "
        f"WHERE node_uri > '{esc}' ORDER BY node_uri LIMIT {int(limit)}"
    )


def sizes_batch_query(out: str, after_rank: int, limit: int) -> str:
    return (
        f"SELECT community_rank, size FROM {out}_sizes "
        f"WHERE community_rank > {int(after_rank)} "
        f"ORDER BY community_rank LIMIT {int(limit)}"
    )


# ---------------------------------------------------------------------------
# JobCommunities
# ---------------------------------------------------------------------------


class JobCommunities:
    """Run the community job and read its output with bounded queries."""

    def __init__(
        self,
        source_table: str,
        *,
        runner: Any,
        query: Callable[[str], List[Dict[str, Any]]],
        output_table: str,
        lp_iterations: int = 30,
    ) -> None:
        self._source_table = source_table
        self._runner = runner
        self._query = query
        self._out = output_table
        self._lp_iterations = max(1, int(lp_iterations))

    @property
    def output_table(self) -> str:
        return self._out

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------

    def run(
        self,
        *,
        class_filter: Optional[List[str]] = None,
        exclude_predicates: Optional[List[str]] = None,
        on_progress: Optional[Callable[[int, str], None]] = None,
    ) -> Dict[str, Any]:
        """Trigger the job, wait, and return the run summary."""
        t0 = time.time()
        outcome = self._runner.run_params_and_wait(
            {
                "source_table": self._source_table,
                "output_table": self._out,
                "exclude_predicates": ",".join(exclude_predicates or []),
                "class_filter": ",".join(class_filter or []),
                "lp_iterations": str(self._lp_iterations),
                "computed_at": datetime.now(timezone.utc).isoformat(),
            },
            on_progress=on_progress,
        )
        if not outcome.get("success"):
            detail = (
                f"{outcome.get('life_cycle_state', '')} "
                f"{outcome.get('result_state', '')} "
                f"{outcome.get('message', '')}"
            ).strip()
            if outcome.get("run_page_url"):
                detail += f" — {outcome['run_page_url']}"
            raise InfrastructureError(
                "The community detection job did not complete successfully",
                detail=detail,
            )
        if on_progress:
            on_progress(90, "Reading communities back")
        stats = self.summary() or {}
        stats["run_page_url"] = outcome.get("run_page_url", "")
        stats["elapsed_ms"] = int((time.time() - t0) * 1000)
        return stats

    # ------------------------------------------------------------------
    # Read-back
    # ------------------------------------------------------------------

    def _safe_rows(self, sql: str) -> Optional[List[Dict[str, Any]]]:
        """Rows, or ``None`` when the job has never written the table."""
        try:
            return self._query(sql) or []
        except Exception as exc:  # noqa: BLE001
            if names_a_missing_object(exc):
                return None
            raise

    def summary(self) -> Optional[Dict[str, Any]]:
        """The last run's summary, or ``None`` when there is no run."""
        rows = self._safe_rows(summary_query(self._out))
        if not rows:
            return None
        row = rows[0]
        stats: Dict[str, Any] = {
            "node_count": int(row.get("node_count", 0) or 0),
            "edge_count": int(row.get("edge_count", 0) or 0),
            "cluster_count": int(row.get("community_count", 0) or 0),
            "modularity": round(float(row.get("modularity", 0.0) or 0.0), 4),
            "iterations": int(row.get("iterations", 0) or 0),
            "converged": bool(row.get("converged", True)),
            "algorithm": str(row.get("algorithm") or "label_propagation"),
            "class_filter": [
                c for c in str(row.get("class_filter") or "").split(",") if c
            ],
            "computed_at": str(row.get("computed_at") or ""),
        }
        stats.update(self._size_stats())
        return stats

    def _size_stats(self) -> Dict[str, Any]:
        rows = self._safe_rows(size_stats_query(self._out)) or [{}]
        row = rows[0] if rows else {}
        count = int(row.get("n", 0) or 0)
        if not count:
            return {"size_min": None, "size_max": None, "size_avg": None, "size_median": None}
        median = self._size_at(count // 2)
        if count % 2 == 0:
            median = (median + self._size_at(count // 2 - 1)) / 2.0
        return {
            "size_min": int(row.get("min_size", 0) or 0),
            "size_max": int(row.get("max_size", 0) or 0),
            "size_avg": round(float(row.get("avg_size", 0.0) or 0.0), 2),
            "size_median": median,
        }

    def _size_at(self, offset: int) -> float:
        rows = self._safe_rows(size_at_query(self._out, offset)) or []
        return float(rows[0].get("size", 0) or 0) if rows else 0.0

    def top_clusters(self, limit: int = DEFAULT_TOP_CLUSTERS) -> List[Dict[str, Any]]:
        k = _bounded(limit, DEFAULT_TOP_CLUSTERS, TOP_CLUSTERS_MAX)
        rows = self._safe_rows(top_clusters_query(self._out, k)) or []
        clusters = []
        for row in rows:
            cid = int(row.get("community_rank", 0) or 0)
            seed = str(row.get("seed_uri") or "")
            clusters.append(
                {
                    "id": cid,
                    "size": int(row.get("size", 0) or 0),
                    "seed_uri": seed,
                    "label": str(row.get("seed_label") or "") or _local_label(seed),
                }
            )
        return clusters

    def preview(self, node_total: int = 0, edge_total: int = 0) -> Dict[str, Any]:
        node_rows = self._safe_rows(preview_nodes_query(self._out)) or []
        edge_rows = self._safe_rows(preview_edges_query(self._out)) or []
        nodes = []
        for row in node_rows:
            uri = str(row.get("node_uri") or "")
            type_uri = str(row.get("type_uri") or "")
            nodes.append(
                {
                    "id": uri,
                    "label": str(row.get("label") or "") or _local_label(uri),
                    "type": type_uri,
                    "type_label": _local_label(type_uri),
                    "cluster": int(row.get("community_rank", 0) or 0),
                }
            )
        edges = [
            {"id": f"{r['src']}|{r['dst']}", "source": r["src"], "target": r["dst"]}
            for r in edge_rows
            if r.get("src") and r.get("dst")
        ]
        return {
            "nodes": nodes,
            "edges": edges,
            "capped": node_total > len(nodes),
            "node_total": node_total,
            "edge_total": edge_total,
        }

    def latest(self, top_n: int = DEFAULT_TOP_CLUSTERS) -> Dict[str, Any]:
        """Summary, top clusters and preview of the last run, or empty."""
        stats = self.summary()
        if stats is None:
            return {"clusters": [], "stats": None, "graph": None}
        return {
            "clusters": self.top_clusters(top_n),
            "stats": stats,
            "graph": self.preview(stats["node_count"], stats["edge_count"]),
        }

    def members(
        self, cluster_id: int, offset: int = 0, limit: int = DEFAULT_MEMBERS_PAGE
    ) -> Dict[str, Any]:
        """One page of a community's members, with label and type."""
        page = _bounded(limit, DEFAULT_MEMBERS_PAGE, MEMBERS_PAGE_MAX)
        start = max(0, int(offset or 0))
        size_rows = self._safe_rows(cluster_size_query(self._out, cluster_id)) or []
        size = int(size_rows[0].get("size", 0) or 0) if size_rows else 0
        rows = self._safe_rows(members_query(self._out, cluster_id, start, page)) or []
        members = []
        for row in rows:
            uri = str(row.get("node_uri") or "")
            type_uri = str(row.get("type_uri") or "")
            members.append(
                {
                    "uri": uri,
                    "label": str(row.get("label") or "") or _local_label(uri),
                    "type": type_uri,
                    "type_label": _local_label(type_uri),
                }
            )
        return {
            "cluster_id": int(cluster_id),
            "size": size,
            "offset": start,
            "members": members,
            "has_more": start + len(members) < size,
        }

    def iter_memberships(self, batch_size: int = 50_000) -> Iterator[List[Tuple[str, int]]]:
        """Yield ``(node_uri, cluster_id)`` batches in URI order (keyset paging)."""
        last = ""
        while True:
            rows = self._query(membership_batch_query(self._out, last, batch_size)) or []
            if not rows:
                return
            batch = [
                (str(r.get("node_uri") or ""), int(r.get("community_rank", 0) or 0))
                for r in rows
            ]
            yield batch
            last = batch[-1][0]
            if len(rows) < batch_size:
                return

    def iter_sizes(self, batch_size: int = 50_000) -> Iterator[List[Tuple[int, int]]]:
        """Yield ``(cluster_id, size)`` batches in id order (keyset paging)."""
        last = -1
        while True:
            rows = self._query(sizes_batch_query(self._out, last, batch_size)) or []
            if not rows:
                return
            batch = [
                (int(r.get("community_rank", 0) or 0), int(r.get("size", 0) or 0))
                for r in rows
            ]
            yield batch
            last = batch[-1][0]
            if len(rows) < batch_size:
                return
