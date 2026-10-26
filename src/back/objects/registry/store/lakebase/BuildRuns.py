"""Knowledge Graph build-run trace on the Lakebase registry.

Extracted from :class:`LakebaseRegistryStore` (Fowler Extract Class / mixin).
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

from ..base import BuildRunEntry

from back.core.logging import get_logger

logger = get_logger("back.objects.registry.store.lakebase.store")


def _require_psycopg():
    """Defer to the store module so tests can monkeypatch that name."""
    from back.objects.registry.store.lakebase.store import _require_psycopg as _rp

    return _rp()


class BuildRuns:
    """Append-only Knowledge Graph build-run trace."""

    def _ensure_build_runs_table(self) -> bool:
        """Lazily create ``build_runs`` (+ index) if it is missing.

        Self-heals deployments created before the build-run trace
        existed: the full DDL only runs from the Settings *Initialize*
        action, so without this an upgraded instance would have no
        table until an admin re-ran Initialize. Idempotent (every
        statement uses ``IF NOT EXISTS``) and guarded by a per-instance
        flag so we only pay the round-trip once per store. Best-effort:
        on failure (e.g. missing GRANT) it logs and returns ``False``
        so callers can no-op instead of breaking a build.
        """
        if self._build_runs_ready:
            return True
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                # Check first: if the table already exists (created by
                # bootstrap as the schema owner), skip all DDL.  CREATE
                # INDEX requires table ownership in Postgres — running it
                # when we don't own the table raises "must be owner of
                # table build_runs" even with IF NOT EXISTS.
                cur.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = %s AND table_name = 'build_runs'",
                    (self._schema,),
                )
                if cur.fetchone():
                    self._build_runs_ready = True
                    return True
                # Table is absent — SP has CREATE ON SCHEMA so it can
                # create the table (and will own it, allowing the index).
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {sch}.build_runs (
                        id                  bigserial PRIMARY KEY,
                        domain_id           uuid NOT NULL
                                            REFERENCES {sch}.domains(id)
                                            ON DELETE CASCADE,
                        version             text NOT NULL,
                        build_kind          text NOT NULL DEFAULT 'session',
                        status              text NOT NULL,
                        message             text NOT NULL DEFAULT '',
                        error               text NOT NULL DEFAULT '',
                        started_at          timestamptz NOT NULL DEFAULT now(),
                        finished_at         timestamptz,
                        duration_s          double precision NOT NULL DEFAULT 0,
                        triple_count        bigint NOT NULL DEFAULT 0,
                        entity_count        integer NOT NULL DEFAULT 0,
                        relationship_count  integer NOT NULL DEFAULT 0,
                        sql_chars           integer NOT NULL DEFAULT 0,
                        graph_engine        text NOT NULL DEFAULT '',
                        sync_mode           text NOT NULL DEFAULT '',
                        view_table          text NOT NULL DEFAULT '',
                        graph_name          text NOT NULL DEFAULT '',
                        task_id             text NOT NULL DEFAULT '',
                        phase_times         jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                        stats               jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                        created_at          timestamptz NOT NULL DEFAULT now()
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_build_runs_domain_version
                        ON {sch}.build_runs(domain_id, version, started_at DESC)
                    """
                )
            self._build_runs_ready = True
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "could not create build_runs table — "
                "run `make bootstrap-lakebase` as the schema owner to apply the migration: %s",
                exc,
            )
            return False

    def record_build_run(self, folder: str, entry: BuildRunEntry) -> None:
        if not self._ensure_build_runs_table():
            return
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {sch}.build_runs
                        (domain_id, version, build_kind, status, message,
                         error, started_at, finished_at, duration_s,
                         triple_count, entity_count, relationship_count,
                         sql_chars, graph_engine, sync_mode, view_table,
                         graph_name, task_id, phase_times, stats)
                    SELECT d.id, %s, %s, %s, %s, %s,
                           COALESCE(%s::timestamptz, now()),
                           %s::timestamptz, %s, %s, %s, %s, %s, %s, %s, %s,
                           %s, %s, %s::jsonb, %s::jsonb
                    FROM {sch}.domains d
                    WHERE d.registry_id = %s AND d.folder = %s
                    """,
                    (
                        str(entry.get("version", "")),
                        str(entry.get("build_kind", "session")),
                        str(entry.get("status", "")),
                        str(entry.get("message", "") or ""),
                        str(entry.get("error", "") or ""),
                        entry.get("started_at"),
                        entry.get("finished_at"),
                        float(entry.get("duration_s", 0) or 0),
                        int(entry.get("triple_count", 0) or 0),
                        int(entry.get("entity_count", 0) or 0),
                        int(entry.get("relationship_count", 0) or 0),
                        int(entry.get("sql_chars", 0) or 0),
                        str(entry.get("graph_engine", "") or ""),
                        str(entry.get("sync_mode", "") or ""),
                        str(entry.get("view_table", "") or ""),
                        str(entry.get("graph_name", "") or ""),
                        str(entry.get("task_id", "") or ""),
                        json.dumps(entry.get("phase_times") or {}),
                        json.dumps(entry.get("stats") or {}),
                        self._registry(),
                        folder,
                    ),
                )
                if cur.rowcount == 0:
                    logger.warning(
                        "record_build_run(%s): no domain row matched — "
                        "build trace not stored",
                        folder,
                    )
        except Exception as exc:  # noqa: BLE001
            logger.warning("record_build_run(%s) failed: %s", folder, exc)

    @staticmethod
    def _build_run_row_to_entry(r: Dict[str, Any]) -> BuildRunEntry:
        return {
            "id": int(r.get("id") or 0),
            "version": r["version"],
            "build_kind": r["build_kind"],
            "status": r["status"],
            "message": r["message"] or "",
            "error": r["error"] or "",
            "started_at": (
                r["started_at"].isoformat() if r.get("started_at") else ""
            ),
            "finished_at": (
                r["finished_at"].isoformat() if r.get("finished_at") else ""
            ),
            "duration_s": float(r["duration_s"] or 0),
            "triple_count": int(r["triple_count"] or 0),
            "entity_count": int(r["entity_count"] or 0),
            "relationship_count": int(r["relationship_count"] or 0),
            "sql_chars": int(r["sql_chars"] or 0),
            "graph_engine": r["graph_engine"] or "",
            "sync_mode": r["sync_mode"] or "",
            "view_table": r["view_table"] or "",
            "graph_name": r["graph_name"] or "",
            "task_id": r["task_id"] or "",
            "phase_times": dict(r["phase_times"] or {}),
            "stats": dict(r["stats"] or {}),
        }

    def stamp_last_build(
        self, folder: str, version: str, ts: str
    ) -> Tuple[bool, str]:
        """Targeted UPDATE for ``domain_versions.last_build``.

        Avoids a full read + re-write of the JSONB blobs: only the scalar
        ``last_build`` column is touched.  Returns ``(ok, message)``.
        """
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {self._q(self._schema)}.domain_versions v
                       SET last_build = %s
                      FROM {self._q(self._schema)}.domains d
                     WHERE d.id         = v.domain_id
                       AND d.registry_id = %s
                       AND d.folder     = %s
                       AND v.version    = %s
                    """,
                    (ts, self._registry(), folder, version),
                )
            return True, ""
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)

    def load_build_runs(
        self,
        folder: str,
        *,
        version: Optional[str] = None,
        limit: int = 100,
    ) -> List[BuildRunEntry]:
        if not self._ensure_build_runs_table():
            return []
        try:
            psycopg, dict_row = _require_psycopg()
            sch = self._q(self._schema)
            clauses = ["d.registry_id = %s", "d.folder = %s"]
            params: List[Any] = [self._registry(), folder]
            if version:
                clauses.append("b.version = %s")
                params.append(version)
            params.append(int(limit))
            where = " AND ".join(clauses)
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT b.id, b.version, b.build_kind, b.status, b.message,
                           b.error, b.started_at, b.finished_at, b.duration_s,
                           b.triple_count, b.entity_count, b.relationship_count,
                           b.sql_chars, b.graph_engine, b.sync_mode,
                           b.view_table, b.graph_name, b.task_id,
                           b.phase_times, b.stats
                    FROM {sch}.build_runs b
                    JOIN {sch}.domains d ON d.id = b.domain_id
                    WHERE {where}
                    ORDER BY b.started_at DESC, b.id DESC
                    LIMIT %s
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
            return [self._build_run_row_to_entry(r) for r in rows]
        except Exception as exc:  # noqa: BLE001
            logger.debug("load_build_runs(%s) failed: %s", folder, exc)
            return []

    def load_all_build_runs(
        self,
        *,
        folder: Optional[str] = None,
        limit: int = 25,
        offset: int = 0,
    ) -> Tuple[List[BuildRunEntry], int]:
        if not self._ensure_build_runs_table():
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
                FROM {sch}.build_runs b
                JOIN {sch}.domains d ON d.id = b.domain_id
                {where}
            """
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(f"SELECT COUNT(*) AS total {source}", tuple(params))
                total = self._scalar_total(cur.fetchone())
                cur.execute(
                    f"""
                    SELECT b.id, d.folder AS domain, b.version, b.build_kind,
                           b.status, b.message, b.error, b.started_at,
                           b.finished_at, b.duration_s, b.triple_count,
                           b.entity_count, b.relationship_count, b.sql_chars,
                           b.graph_engine, b.sync_mode, b.view_table,
                           b.graph_name, b.task_id, b.phase_times, b.stats
                    {source}
                    ORDER BY b.started_at DESC, b.id DESC
                    LIMIT %s OFFSET %s
                    """,
                    tuple(params) + (int(limit), int(offset)),
                )
                rows = cur.fetchall()
            entries = []
            for r in rows:
                entry = self._build_run_row_to_entry(r)
                entry["domain"] = r.get("domain") or ""
                entries.append(entry)
            return entries, total
        except Exception as exc:  # noqa: BLE001
            logger.debug("load_all_build_runs(folder=%s) failed: %s", folder, exc)
            return [], 0

    @staticmethod
    def _empty_analytics() -> Dict[str, Any]:
        return {
            "total_runs": 0,
            "success_runs": 0,
            "failed_runs": 0,
            "success_rate": 0.0,
            "avg_duration_s": 0.0,
            "min_duration_s": 0.0,
            "max_duration_s": 0.0,
            "last_triple_count": 0,
            "active_build": None,
            "per_version": [],
        }

    def build_analytics(
        self, folder: str, *, version: Optional[str] = None
    ) -> Dict[str, Any]:
        if not self._ensure_build_runs_table():
            return self._empty_analytics()
        try:
            psycopg, dict_row = _require_psycopg()
            sch = self._q(self._schema)
            scope = ["d.registry_id = %s", "d.folder = %s"]
            scope_params: List[Any] = [self._registry(), folder]
            if version:
                scope.append("b.version = %s")
                scope_params.append(version)
            where = " AND ".join(scope)

            result = self._empty_analytics()
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                # Headline aggregates.
                cur.execute(
                    f"""
                    SELECT
                        count(*)                                   AS total_runs,
                        count(*) FILTER (WHERE b.status = 'success') AS success_runs,
                        count(*) FILTER (WHERE b.status <> 'success') AS failed_runs,
                        COALESCE(avg(b.duration_s)
                                 FILTER (WHERE b.status = 'success'), 0) AS avg_duration_s,
                        COALESCE(min(b.duration_s)
                                 FILTER (WHERE b.status = 'success'), 0) AS min_duration_s,
                        COALESCE(max(b.duration_s)
                                 FILTER (WHERE b.status = 'success'), 0) AS max_duration_s
                    FROM {sch}.build_runs b
                    JOIN {sch}.domains d ON d.id = b.domain_id
                    WHERE {where}
                    """,
                    tuple(scope_params),
                )
                agg = cur.fetchone() or {}
                total = int(agg.get("total_runs") or 0)
                success = int(agg.get("success_runs") or 0)
                result.update(
                    {
                        "total_runs": total,
                        "success_runs": success,
                        "failed_runs": int(agg.get("failed_runs") or 0),
                        "success_rate": (success / total) if total else 0.0,
                        "avg_duration_s": float(agg.get("avg_duration_s") or 0),
                        "min_duration_s": float(agg.get("min_duration_s") or 0),
                        "max_duration_s": float(agg.get("max_duration_s") or 0),
                    }
                )

                # Active build = latest successful run in scope.
                cur.execute(
                    f"""
                    SELECT b.version, b.build_kind, b.status, b.message,
                           b.error, b.started_at, b.finished_at, b.duration_s,
                           b.triple_count, b.entity_count, b.relationship_count,
                           b.sql_chars, b.graph_engine, b.sync_mode,
                           b.view_table, b.graph_name, b.task_id,
                           b.phase_times, b.stats
                    FROM {sch}.build_runs b
                    JOIN {sch}.domains d ON d.id = b.domain_id
                    WHERE {where} AND b.status = 'success'
                    ORDER BY b.started_at DESC, b.id DESC
                    LIMIT 1
                    """,
                    tuple(scope_params),
                )
                active = cur.fetchone()
                if active:
                    entry = self._build_run_row_to_entry(active)
                    result["active_build"] = entry
                    result["last_triple_count"] = entry["triple_count"]

                # Per-version rollup (newest version first).
                cur.execute(
                    f"""
                    SELECT b.version,
                           count(*) AS total_runs,
                           count(*) FILTER (WHERE b.status = 'success')
                               AS success_runs,
                           max(b.started_at) AS last_run,
                           (array_agg(b.status ORDER BY b.started_at DESC,
                                      b.id DESC))[1] AS last_status,
                           (array_agg(b.triple_count ORDER BY b.started_at DESC,
                                      b.id DESC))[1] AS last_triple_count
                    FROM {sch}.build_runs b
                    JOIN {sch}.domains d ON d.id = b.domain_id
                    WHERE {where}
                    GROUP BY b.version
                    ORDER BY max(b.started_at) DESC
                    """,
                    tuple(scope_params),
                )
                result["per_version"] = [
                    {
                        "version": r["version"],
                        "total_runs": int(r["total_runs"] or 0),
                        "success_runs": int(r["success_runs"] or 0),
                        "last_status": r["last_status"] or "",
                        "last_triple_count": int(r["last_triple_count"] or 0),
                        "last_run": (
                            r["last_run"].isoformat() if r.get("last_run") else ""
                        ),
                    }
                    for r in cur.fetchall()
                ]
            return result
        except Exception as exc:  # noqa: BLE001
            logger.debug("build_analytics(%s) failed: %s", folder, exc)
            return self._empty_analytics()
