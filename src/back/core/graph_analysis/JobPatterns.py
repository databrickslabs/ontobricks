"""Pattern discovery, computed by the Databricks pattern-detection job.

The job (``src/jobs/graph_analytics_job.py --task patterns``) builds egocentric
neighbourhood signatures over the ``…_data`` snapshot and writes five tables:

* ``<out>``                one row per surviving anchor: ``pattern_id``, type, label
* ``<out>_shapes``         one row per pattern (support, shape_json, …)
* ``<out>_preview_nodes``  the capped sample the Patterns page draws
* ``<out>_preview_edges``  edges among the preview nodes
* ``<out>_summary``        counts, hops, bins, refine flag, run timestamp

This module triggers the run and reads those tables back. Every read is
bounded — top-N patterns, one page of members, keyset batches.
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional

from back.core.errors import InfrastructureError
from back.core.graph_analysis.preflight import names_a_missing_object
from back.core.logging import get_logger

logger = get_logger(__name__)

TOP_PATTERNS_MAX = 200
MEMBERS_PAGE_MAX = 500
DEFAULT_TOP_PATTERNS = 50
DEFAULT_MEMBERS_PAGE = 100


def pattern_output_table(output_schema: str, domain: Any, graph_name: str) -> str:
    """Per-domain-version output table name for the pattern job."""
    folder = str(getattr(domain, "uc_domain_folder", "") or "") or graph_name
    version = str(getattr(domain, "current_version", "") or "")
    slug = re.sub(r"[^A-Za-z0-9_]+", "_", f"{folder}_{version}").strip("_").lower()
    return f"{output_schema}.graph_patterns_{slug or 'default'}"


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


def _sql_str(value: str) -> str:
    return "'" + (value or "").replace("'", "''") + "'"


def summary_query(out: str) -> str:
    return f"SELECT * FROM {out}_summary"


def top_patterns_query(out: str, limit: int) -> str:
    return (
        "SELECT pattern_id, support, hop_count, seed_label, refined, "
        "near_duplicate_count, shape_json "
        f"FROM {out}_shapes ORDER BY support DESC, pattern_id "
        f"LIMIT {int(limit)}"
    )


def preview_nodes_query(out: str) -> str:
    return (
        "SELECT node_uri, pattern_id, label, type_uri "
        f"FROM {out}_preview_nodes ORDER BY pattern_id, node_uri"
    )


def preview_edges_query(out: str) -> str:
    return f"SELECT src, dst FROM {out}_preview_edges"


def pattern_support_query(out: str, pattern_id: str) -> str:
    return (
        f"SELECT support FROM {out}_shapes "
        f"WHERE pattern_id = {_sql_str(pattern_id)}"
    )


def members_query(out: str, pattern_id: str, offset: int, limit: int) -> str:
    return (
        "SELECT anchor_uri, label, type_uri "
        f"FROM {out} WHERE pattern_id = {_sql_str(pattern_id)} "
        "ORDER BY anchor_uri "
        f"LIMIT {int(limit)} OFFSET {max(0, int(offset))}"
    )


def shape_query(out: str, pattern_id: str) -> str:
    return (
        "SELECT pattern_id, support, hop_count, shape_json, seed_label, "
        "refined, near_duplicate_count "
        f"FROM {out}_shapes WHERE pattern_id = {_sql_str(pattern_id)}"
    )


def membership_batch_query(out: str, pattern_id: str, after_uri: str, limit: int) -> str:
    return (
        f"SELECT anchor_uri, pattern_id FROM {out} "
        f"WHERE pattern_id = {_sql_str(pattern_id)} "
        f"AND anchor_uri > {_sql_str(after_uri)} "
        f"ORDER BY anchor_uri LIMIT {int(limit)}"
    )


class JobPatterns:
    """Run the pattern job and read its output with bounded queries."""

    def __init__(
        self,
        source_table: str,
        *,
        runner: Any,
        query: Callable[[str], List[Dict[str, Any]]],
        output_table: str,
    ) -> None:
        self._source_table = source_table
        self._runner = runner
        self._query = query
        self._out = output_table

    @property
    def output_table(self) -> str:
        return self._out

    def run(
        self,
        *,
        class_filter: str,
        hops: int = 3,
        bins: int = 8,
        min_support: int = 3,
        refine_top: int = 0,
        exclude_predicates: Optional[List[str]] = None,
        on_progress: Optional[Callable[[int, str], None]] = None,
    ) -> Dict[str, Any]:
        """Trigger the job, wait, and return the run summary."""
        if not (class_filter or "").strip():
            raise ValueError("class_filter is required")
        t0 = time.time()
        outcome = self._runner.run_params_and_wait(
            {
                "source_table": self._source_table,
                "output_table": self._out,
                "exclude_predicates": ",".join(exclude_predicates or []),
                "class_filter": class_filter.strip(),
                "pattern_hops": str(int(hops)),
                "pattern_bins": str(int(bins)),
                "pattern_min_support": str(int(min_support)),
                "pattern_frontier": "50000",
                "refine_top": str(int(refine_top)),
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
                "The pattern detection job did not complete successfully",
                detail=detail,
            )
        if on_progress:
            on_progress(90, "Reading patterns back")
        stats = self.summary() or {}
        stats["run_page_url"] = outcome.get("run_page_url", "")
        if stats.get("elapsed_ms") is None:
            stats["elapsed_ms"] = int((time.time() - t0) * 1000)
        return stats

    def _safe_rows(self, sql: str) -> Optional[List[Dict[str, Any]]]:
        try:
            return self._query(sql) or []
        except Exception as exc:  # noqa: BLE001
            if names_a_missing_object(exc):
                return None
            raise

    def summary(self) -> Optional[Dict[str, Any]]:
        rows = self._safe_rows(summary_query(self._out))
        if not rows:
            return None
        row = rows[0]
        elapsed_raw = row.get("elapsed_ms")
        try:
            elapsed_ms = int(elapsed_raw) if elapsed_raw is not None else None
        except (TypeError, ValueError):
            elapsed_ms = None
        return {
            "anchor_type": str(row.get("anchor_type") or ""),
            "anchor_count": int(row.get("anchor_count", 0) or 0),
            "pattern_count": int(row.get("pattern_count", 0) or 0),
            "hop_count": int(row.get("hop_count", 0) or 0),
            "bins": int(row.get("bins", 0) or 0),
            "refined": int(row.get("refined", 0) or 0),
            "min_support": int(row.get("min_support", 0) or 0),
            "algorithm": str(row.get("algorithm") or "egocentric_signature"),
            "class_filter": [
                c for c in str(row.get("class_filter") or "").split(",") if c
            ],
            "computed_at": str(row.get("computed_at") or ""),
            "elapsed_ms": elapsed_ms,
        }

    def top_patterns(self, limit: int = DEFAULT_TOP_PATTERNS) -> List[Dict[str, Any]]:
        k = _bounded(limit, DEFAULT_TOP_PATTERNS, TOP_PATTERNS_MAX)
        rows = self._safe_rows(top_patterns_query(self._out, k)) or []
        out = []
        for row in rows:
            pid = str(row.get("pattern_id") or "")
            out.append(
                {
                    "id": pid,
                    "support": int(row.get("support", 0) or 0),
                    "hop_count": int(row.get("hop_count", 0) or 0),
                    "label": str(row.get("seed_label") or "") or pid[:8],
                    "refined": int(row.get("refined", 0) or 0),
                    "near_duplicate_count": int(
                        row.get("near_duplicate_count", 1) or 1
                    ),
                    "shape_json": str(row.get("shape_json") or "[]"),
                }
            )
        return out

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
                    "pattern": str(row.get("pattern_id") or ""),
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

    def latest(self, top_n: int = DEFAULT_TOP_PATTERNS) -> Dict[str, Any]:
        stats = self.summary()
        if stats is None:
            return {
                "patterns": [],
                "stats": None,
                "graph": {
                    "nodes": [],
                    "edges": [],
                    "capped": False,
                    "node_total": 0,
                    "edge_total": 0,
                },
            }
        return {
            "patterns": self.top_patterns(top_n),
            "stats": stats,
            "graph": self.preview(stats["anchor_count"], 0),
        }

    def members(
        self,
        pattern_id: str,
        offset: int = 0,
        limit: int = DEFAULT_MEMBERS_PAGE,
    ) -> Dict[str, Any]:
        page = _bounded(limit, DEFAULT_MEMBERS_PAGE, MEMBERS_PAGE_MAX)
        start = max(0, int(offset or 0))
        size_rows = self._safe_rows(pattern_support_query(self._out, pattern_id)) or []
        size = int(size_rows[0].get("support", 0) or 0) if size_rows else 0
        rows = self._safe_rows(members_query(self._out, pattern_id, start, page)) or []
        members = []
        for row in rows:
            uri = str(row.get("anchor_uri") or "")
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
            "pattern_id": pattern_id,
            "total": size,
            "offset": start,
            "limit": page,
            "members": members,
            "has_more": start + len(members) < size,
        }

    def shape(self, pattern_id: str) -> Optional[Dict[str, Any]]:
        rows = self._safe_rows(shape_query(self._out, pattern_id)) or []
        if not rows:
            return None
        row = rows[0]
        raw = str(row.get("shape_json") or "[]")
        try:
            tokens = json.loads(raw)
        except json.JSONDecodeError:
            tokens = []
        if not isinstance(tokens, list):
            tokens = []
        return {
            "pattern_id": str(row.get("pattern_id") or pattern_id),
            "support": int(row.get("support", 0) or 0),
            "hop_count": int(row.get("hop_count", 0) or 0),
            "seed_label": str(row.get("seed_label") or ""),
            "refined": int(row.get("refined", 0) or 0),
            "near_duplicate_count": int(row.get("near_duplicate_count", 1) or 1),
            "shape": tokens,
            "shape_json": raw,
        }

    def iter_memberships(
        self, pattern_id: str, batch_size: int = 50_000
    ) -> Iterator[List[Dict[str, str]]]:
        """Yield ``{uri, pattern_id}`` batches in URI order (keyset paging)."""
        last = ""
        while True:
            rows = (
                self._query(
                    membership_batch_query(self._out, pattern_id, last, batch_size)
                )
                or []
            )
            if not rows:
                return
            batch = [
                {
                    "uri": str(r.get("anchor_uri") or ""),
                    "pattern_id": str(r.get("pattern_id") or pattern_id),
                }
                for r in rows
                if r.get("anchor_uri")
            ]
            if not batch:
                return
            yield batch
            last = batch[-1]["uri"]
            if len(rows) < batch_size:
                return
