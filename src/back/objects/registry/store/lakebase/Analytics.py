"""Graph analytics cache and run history on the Lakebase registry.

Extracted from :class:`LakebaseRegistryStore` (Fowler Extract Class / mixin).
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

from back.core.logging import get_logger
from ..base import GraphAnalyticsResult, GraphAnalyticsRun

logger = get_logger("back.objects.registry.store.lakebase.store")


def _require_psycopg():
    """Defer to the store module so tests can monkeypatch that name."""
    from back.objects.registry.store.lakebase.store import _require_psycopg as _rp

    return _rp()


class Analytics:
    """Cached graph metrics plus append-only analytics run rows."""

    _GRAPH_ANALYTICS_RUNS_CAP = 100

    def _ensure_graph_analytics_table(self) -> bool:
        """Lazily create ``graph_analytics`` if it is missing.

        Self-heals deployments created before the async graph-analytics
        cache existed (the full DDL only runs from the Settings
        *Initialize* action). Idempotent and guarded by a per-instance
        flag so we only pay the round-trip once per store. Best-effort:
        on failure (e.g. missing GRANT) it logs and returns ``False`` so
        callers can no-op instead of breaking the analytics task. Mirrors
        :meth:`_ensure_build_runs_table`.
        """
        if self._graph_analytics_ready:
            return True
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                # Check first: CREATE on an existing table we don't own
                # would raise even with IF NOT EXISTS (see build_runs).
                cur.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = %s AND table_name = 'graph_analytics'",
                    (self._schema,),
                )
                if cur.fetchone():
                    self._graph_analytics_ready = True
                    return True
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {sch}.graph_analytics (
                        domain_id    uuid NOT NULL
                                     REFERENCES {sch}.domains(id)
                                     ON DELETE CASCADE,
                        version      text NOT NULL,
                        status       text NOT NULL DEFAULT 'completed',
                        graph_name   text NOT NULL DEFAULT '',
                        class_filter jsonb NOT NULL DEFAULT '[]'::jsonb,
                        stats        jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                        top_pagerank jsonb NOT NULL DEFAULT '[]'::jsonb,
                        result       jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                        error        text NOT NULL DEFAULT '',
                        task_id      text NOT NULL DEFAULT '',
                        duration_ms  bigint NOT NULL DEFAULT 0,
                        computed_at  timestamptz NOT NULL DEFAULT now(),
                        PRIMARY KEY (domain_id, version)
                    )
                    """
                )
            self._graph_analytics_ready = True
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "could not create graph_analytics table — "
                "run `make bootstrap-lakebase` as the schema owner to apply "
                "the migration: %s",
                exc,
            )
            return False

    def save_graph_analytics(
        self, folder: str, version: str, entry: GraphAnalyticsResult
    ) -> None:
        if not self._ensure_graph_analytics_table():
            return
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {sch}.graph_analytics
                        (domain_id, version, status, graph_name, class_filter,
                         stats, top_pagerank, result, error, task_id,
                         duration_ms, computed_at)
                    SELECT d.id, %s, %s, %s, %s::jsonb,
                           %s::jsonb, %s::jsonb, %s::jsonb, %s, %s,
                           %s, COALESCE(%s::timestamptz, now())
                    FROM {sch}.domains d
                    WHERE d.registry_id = %s AND d.folder = %s
                    ON CONFLICT (domain_id, version) DO UPDATE SET
                        status       = EXCLUDED.status,
                        graph_name   = EXCLUDED.graph_name,
                        class_filter = EXCLUDED.class_filter,
                        stats        = EXCLUDED.stats,
                        top_pagerank = EXCLUDED.top_pagerank,
                        result       = EXCLUDED.result,
                        error        = EXCLUDED.error,
                        task_id      = EXCLUDED.task_id,
                        duration_ms  = EXCLUDED.duration_ms,
                        computed_at  = EXCLUDED.computed_at
                    """,
                    (
                        str(version),
                        str(entry.get("status", "completed")),
                        str(entry.get("graph_name", "") or ""),
                        json.dumps(entry.get("class_filter") or []),
                        json.dumps(entry.get("stats") or {}),
                        json.dumps(entry.get("top_pagerank") or []),
                        json.dumps(entry.get("result") or {}),
                        str(entry.get("error", "") or ""),
                        str(entry.get("task_id", "") or ""),
                        int(entry.get("duration_ms", 0) or 0),
                        entry.get("computed_at"),
                        self._registry(),
                        folder,
                    ),
                )
                if cur.rowcount == 0:
                    logger.warning(
                        "save_graph_analytics(%s): no domain row matched — "
                        "result not stored",
                        folder,
                    )
        except Exception as exc:  # noqa: BLE001
            logger.warning("save_graph_analytics(%s) failed: %s", folder, exc)

    @staticmethod
    def _graph_analytics_row_to_entry(r: Dict[str, Any]) -> GraphAnalyticsResult:
        return {
            "version": r["version"],
            "status": r["status"] or "completed",
            "graph_name": r["graph_name"] or "",
            "class_filter": list(r.get("class_filter") or []),
            "stats": dict(r.get("stats") or {}),
            "top_pagerank": list(r.get("top_pagerank") or []),
            "result": dict(r.get("result") or {}),
            "error": r["error"] or "",
            "task_id": r["task_id"] or "",
            "duration_ms": int(r["duration_ms"] or 0),
            "computed_at": (
                r["computed_at"].isoformat() if r.get("computed_at") else ""
            ),
        }

    def load_graph_analytics(
        self, folder: str, version: str
    ) -> Optional[GraphAnalyticsResult]:
        if not self._ensure_graph_analytics_table():
            return None
        try:
            _psycopg, dict_row = _require_psycopg()
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT g.version, g.status, g.graph_name, g.class_filter,
                           g.stats, g.top_pagerank, g.result, g.error,
                           g.task_id, g.duration_ms, g.computed_at
                    FROM {sch}.graph_analytics g
                    JOIN {sch}.domains d ON d.id = g.domain_id
                    WHERE d.registry_id = %s AND d.folder = %s
                      AND g.version = %s
                    """,
                    (self._registry(), folder, version),
                )
                row = cur.fetchone()
            if not row:
                return None
            return self._graph_analytics_row_to_entry(row)
        except Exception as exc:  # noqa: BLE001
            logger.debug("load_graph_analytics(%s) failed: %s", folder, exc)
            return None

    def _ensure_graph_analytics_runs_table(self) -> bool:
        """Lazily create ``graph_analytics_runs`` (+ index) if missing.

        Mirrors :meth:`_ensure_build_runs_table`: best-effort, idempotent,
        guarded by a per-instance flag.
        """
        if self._graph_analytics_runs_ready:
            return True
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = %s AND table_name = 'graph_analytics_runs'",
                    (self._schema,),
                )
                if cur.fetchone():
                    self._graph_analytics_runs_ready = True
                    return True
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {sch}.graph_analytics_runs (
                        id                  bigserial PRIMARY KEY,
                        domain_id           uuid NOT NULL
                                            REFERENCES {sch}.domains(id)
                                            ON DELETE CASCADE,
                        version             text NOT NULL,
                        status              text NOT NULL DEFAULT 'completed',
                        class_filter        jsonb NOT NULL DEFAULT '[]'::jsonb,
                        node_count          bigint NOT NULL DEFAULT 0,
                        edge_count          bigint NOT NULL DEFAULT 0,
                        connected_components integer NOT NULL DEFAULT 0,
                        avg_degree          double precision NOT NULL DEFAULT 0,
                        density             double precision NOT NULL DEFAULT 0,
                        duration_ms         bigint NOT NULL DEFAULT 0,
                        task_id             text NOT NULL DEFAULT '',
                        error               text NOT NULL DEFAULT '',
                        computed_at         timestamptz NOT NULL DEFAULT now()
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_graph_analytics_runs_domain_version
                        ON {sch}.graph_analytics_runs(domain_id, version, computed_at DESC)
                    """
                )
            self._graph_analytics_runs_ready = True
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "could not create graph_analytics_runs table — "
                "run `make bootstrap-lakebase` as the schema owner to apply "
                "the migration: %s",
                exc,
            )
            return False

    def record_graph_analytics_run(
        self, folder: str, version: str, entry: GraphAnalyticsRun
    ) -> None:
        if not self._ensure_graph_analytics_runs_table():
            return
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {sch}.graph_analytics_runs
                        (domain_id, version, status, class_filter, node_count,
                         edge_count, connected_components, avg_degree, density,
                         duration_ms, task_id, error, computed_at)
                    SELECT d.id, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s,
                           COALESCE(%s::timestamptz, now())
                    FROM {sch}.domains d
                    WHERE d.registry_id = %s AND d.folder = %s
                    """,
                    (
                        str(version),
                        str(entry.get("status", "completed")),
                        json.dumps(entry.get("class_filter") or []),
                        int(entry.get("node_count", 0) or 0),
                        int(entry.get("edge_count", 0) or 0),
                        int(entry.get("connected_components", 0) or 0),
                        float(entry.get("avg_degree", 0) or 0),
                        float(entry.get("density", 0) or 0),
                        int(entry.get("duration_ms", 0) or 0),
                        str(entry.get("task_id", "") or ""),
                        str(entry.get("error", "") or ""),
                        entry.get("computed_at"),
                        self._registry(),
                        folder,
                    ),
                )
                # Prune older rows beyond the cap for this (domain, version).
                cur.execute(
                    f"""
                    DELETE FROM {sch}.graph_analytics_runs
                    WHERE id IN (
                        SELECT r.id
                        FROM {sch}.graph_analytics_runs r
                        JOIN {sch}.domains d ON d.id = r.domain_id
                        WHERE d.registry_id = %s AND d.folder = %s
                          AND r.version = %s
                        ORDER BY r.computed_at DESC, r.id DESC
                        OFFSET %s
                    )
                    """,
                    (
                        self._registry(),
                        folder,
                        str(version),
                        int(self._GRAPH_ANALYTICS_RUNS_CAP),
                    ),
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("record_graph_analytics_run(%s) failed: %s", folder, exc)

    @staticmethod
    def _graph_analytics_run_row_to_entry(r: Dict[str, Any]) -> GraphAnalyticsRun:
        return {
            "id": int(r.get("id") or 0),
            "version": r["version"],
            "status": r["status"] or "completed",
            "class_filter": list(r.get("class_filter") or []),
            "node_count": int(r["node_count"] or 0),
            "edge_count": int(r["edge_count"] or 0),
            "connected_components": int(r["connected_components"] or 0),
            "avg_degree": float(r["avg_degree"] or 0),
            "density": float(r["density"] or 0),
            "duration_ms": int(r["duration_ms"] or 0),
            "task_id": r["task_id"] or "",
            "error": r["error"] or "",
            "computed_at": (
                r["computed_at"].isoformat() if r.get("computed_at") else ""
            ),
        }

    def load_graph_analytics_runs(
        self, folder: str, version: Optional[str] = None, *, limit: int = 100
    ) -> List[GraphAnalyticsRun]:
        if not self._ensure_graph_analytics_runs_table():
            return []
        try:
            _psycopg, dict_row = _require_psycopg()
            sch = self._q(self._schema)
            where = "WHERE d.registry_id = %s AND d.folder = %s"
            params: List[Any] = [self._registry(), folder]
            if version is not None:
                where += " AND r.version = %s"
                params.append(version)
            params.append(int(limit))
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT r.id, r.version, r.status, r.class_filter, r.node_count,
                           r.edge_count, r.connected_components, r.avg_degree,
                           r.density, r.duration_ms, r.task_id, r.error,
                           r.computed_at
                    FROM {sch}.graph_analytics_runs r
                    JOIN {sch}.domains d ON d.id = r.domain_id
                    {where}
                    ORDER BY r.computed_at DESC, r.id DESC
                    LIMIT %s
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
            return [self._graph_analytics_run_row_to_entry(r) for r in rows]
        except Exception as exc:  # noqa: BLE001
            logger.debug("load_graph_analytics_runs(%s) failed: %s", folder, exc)
            return []

    def load_all_graph_analytics_runs(
        self,
        *,
        folder: Optional[str] = None,
        limit: int = 25,
        offset: int = 0,
    ) -> Tuple[List[GraphAnalyticsRun], int]:
        if not self._ensure_graph_analytics_runs_table():
            return [], 0
        try:
            _psycopg, dict_row = _require_psycopg()
            sch = self._q(self._schema)
            where = "WHERE d.registry_id = %s"
            params: List[Any] = [self._registry()]
            if folder:
                where += " AND d.folder = %s"
                params.append(folder)
            source = f"""
                FROM {sch}.graph_analytics_runs r
                JOIN {sch}.domains d ON d.id = r.domain_id
                {where}
            """
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(f"SELECT COUNT(*) AS total {source}", tuple(params))
                total = self._scalar_total(cur.fetchone())
                cur.execute(
                    f"""
                    SELECT r.id, d.folder AS domain, r.version, r.status,
                           r.class_filter, r.node_count, r.edge_count,
                           r.connected_components, r.avg_degree, r.density,
                           r.duration_ms, r.task_id, r.error, r.computed_at
                    {source}
                    ORDER BY r.computed_at DESC, r.id DESC
                    LIMIT %s OFFSET %s
                    """,
                    tuple(params) + (int(limit), int(offset)),
                )
                rows = cur.fetchall()
            entries = []
            for r in rows:
                entry = self._graph_analytics_run_row_to_entry(r)
                entry["domain"] = r.get("domain") or ""
                entries.append(entry)
            return entries, total
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "load_all_graph_analytics_runs(folder=%s) failed: %s", folder, exc
            )
            return [], 0
