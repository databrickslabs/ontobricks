"""Simulation (what-if sandbox) metadata on the Lakebase registry.

Extracted from :class:`LakebaseRegistryStore` (Fowler Extract Class / mixin).
Only the scenario definitions and run history live here; sandbox triples
live in the graph schema as ``<graph>__sim_<id>`` tables.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from back.core.errors import InfrastructureError
from back.core.logging import get_logger

logger = get_logger("back.objects.registry.store.lakebase.store")


def _require_psycopg():
    """Defer to the store module so tests can monkeypatch that name."""
    from back.objects.registry.store.lakebase.store import _require_psycopg as _rp

    return _rp()


def _iso(value: Any) -> str:
    return value.isoformat() if value else ""


class Simulations:
    """Named simulations + append-only simulation run trace."""

    def _ensure_simulation_tables(self) -> bool:
        """Lazily create ``simulations`` / ``simulation_runs`` if missing.

        Same self-heal pattern as ``_ensure_build_runs_table``: upgraded
        deployments get the tables without re-running Settings → Initialize.
        """
        if getattr(self, "_simulation_tables_ready", False):
            return True
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM information_schema.tables "
                    "WHERE table_schema = %s "
                    "AND table_name IN ('simulations', 'simulation_runs')",
                    (self._schema,),
                )
                row = cur.fetchone()
                if row and int(row[0]) == 2:
                    self._simulation_tables_ready = True
                    return True
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {sch}.simulations (
                        domain_id     uuid NOT NULL
                                      REFERENCES {sch}.domains(id) ON DELETE CASCADE,
                        version       text NOT NULL,
                        id            text NOT NULL,
                        label         text NOT NULL DEFAULT '',
                        status        text NOT NULL DEFAULT 'draft',
                        spec          jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                        diff_summary  jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                        diff          jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                        owner         text NOT NULL DEFAULT '',
                        task_id       text NOT NULL DEFAULT '',
                        error         text NOT NULL DEFAULT '',
                        created_at    timestamptz NOT NULL DEFAULT now(),
                        updated_at    timestamptz NOT NULL DEFAULT now(),
                        last_run_at   timestamptz,
                        PRIMARY KEY (domain_id, version, id)
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {sch}.simulation_runs (
                        id             bigserial PRIMARY KEY,
                        domain_id      uuid NOT NULL
                                       REFERENCES {sch}.domains(id) ON DELETE CASCADE,
                        version        text NOT NULL,
                        simulation_id  text NOT NULL,
                        status         text NOT NULL,
                        message        text NOT NULL DEFAULT '',
                        error          text NOT NULL DEFAULT '',
                        task_id        text NOT NULL DEFAULT '',
                        started_at     timestamptz NOT NULL DEFAULT now(),
                        finished_at    timestamptz,
                        duration_s     double precision NOT NULL DEFAULT 0,
                        stats          jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                        owner          text NOT NULL DEFAULT ''
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_simulation_runs_domain_version
                        ON {sch}.simulation_runs(domain_id, version, started_at DESC)
                    """
                )
            self._simulation_tables_ready = True
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "could not create simulation tables — "
                "run `make bootstrap-lakebase` as the schema owner to apply the migration: %s",
                exc,
            )
            return False

    def _require_simulation_tables(self) -> None:
        if not self._ensure_simulation_tables():
            raise InfrastructureError(
                "Simulation tables are unavailable in the Lakebase registry",
                detail="Run Settings → Registry → Initialize as the schema owner.",
            )

    @staticmethod
    def _simulation_row_to_entry(r: Dict[str, Any], *, full: bool) -> Dict[str, Any]:
        entry = {
            "id": r["id"],
            "version": r["version"],
            "label": r["label"] or "",
            "status": r["status"] or "draft",
            "spec": dict(r["spec"] or {}),
            "diff_summary": dict(r["diff_summary"] or {}),
            "owner": r["owner"] or "",
            "task_id": r["task_id"] or "",
            "error": r["error"] or "",
            "created_at": _iso(r.get("created_at")),
            "updated_at": _iso(r.get("updated_at")),
            "last_run_at": _iso(r.get("last_run_at")),
        }
        if full:
            entry["diff"] = dict(r.get("diff") or {})
        return entry

    def list_simulations(self, folder: str, version: str) -> List[Dict[str, Any]]:
        self._require_simulation_tables()
        _psycopg, dict_row = _require_psycopg()
        sch = self._q(self._schema)
        with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT s.id, s.version, s.label, s.status, s.spec,
                       s.diff_summary, s.owner, s.task_id, s.error,
                       s.created_at, s.updated_at, s.last_run_at
                FROM {sch}.simulations s
                JOIN {sch}.domains d ON d.id = s.domain_id
                WHERE d.registry_id = %s AND d.folder = %s AND s.version = %s
                ORDER BY s.updated_at DESC
                """,
                (self._registry(), folder, str(version)),
            )
            rows = cur.fetchall()
        return [self._simulation_row_to_entry(r, full=False) for r in rows]

    def load_simulation(
        self, folder: str, version: str, sim_id: str
    ) -> Optional[Dict[str, Any]]:
        self._require_simulation_tables()
        _psycopg, dict_row = _require_psycopg()
        sch = self._q(self._schema)
        with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT s.id, s.version, s.label, s.status, s.spec,
                       s.diff_summary, s.diff, s.owner, s.task_id, s.error,
                       s.created_at, s.updated_at, s.last_run_at
                FROM {sch}.simulations s
                JOIN {sch}.domains d ON d.id = s.domain_id
                WHERE d.registry_id = %s AND d.folder = %s
                  AND s.version = %s AND s.id = %s
                """,
                (self._registry(), folder, str(version), sim_id),
            )
            row = cur.fetchone()
        return self._simulation_row_to_entry(row, full=True) if row else None

    def save_simulation(self, folder: str, version: str, entry: Dict[str, Any]) -> None:
        """UPSERT one simulation row. Raises InfrastructureError on failure."""
        self._require_simulation_tables()
        sch = self._q(self._schema)
        replace_diff = "diff" in entry or "diff_summary" in entry
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {sch}.simulations
                    (domain_id, version, id, label, status, spec, diff_summary,
                     diff, owner, task_id, error, last_run_at, updated_at)
                SELECT d.id, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb,
                       %s, %s, %s, %s::timestamptz, now()
                FROM {sch}.domains d
                WHERE d.registry_id = %s AND d.folder = %s
                ON CONFLICT (domain_id, version, id) DO UPDATE SET
                    label        = EXCLUDED.label,
                    status       = EXCLUDED.status,
                    spec         = EXCLUDED.spec,
                    diff_summary = CASE WHEN %s THEN EXCLUDED.diff_summary
                                        ELSE {sch}.simulations.diff_summary END,
                    diff         = CASE WHEN %s THEN EXCLUDED.diff
                                        ELSE {sch}.simulations.diff END,
                    task_id      = EXCLUDED.task_id,
                    error        = EXCLUDED.error,
                    last_run_at  = COALESCE(EXCLUDED.last_run_at,
                                            {sch}.simulations.last_run_at),
                    updated_at   = now()
                """,
                (
                    str(version),
                    str(entry["id"]),
                    str(entry.get("label", "") or ""),
                    str(entry.get("status", "draft") or "draft"),
                    json.dumps(entry.get("spec") or {}),
                    json.dumps(entry.get("diff_summary") or {}),
                    json.dumps(entry.get("diff") or {}),
                    str(entry.get("owner", "") or ""),
                    str(entry.get("task_id", "") or ""),
                    str(entry.get("error", "") or ""),
                    entry.get("last_run_at") or None,
                    self._registry(),
                    folder,
                    replace_diff,
                    replace_diff,
                ),
            )
            if cur.rowcount == 0:
                raise InfrastructureError(
                    "Domain is not registered — save it to the registry first",
                    detail=folder,
                )

    def delete_simulation(self, folder: str, version: str, sim_id: str) -> bool:
        self._require_simulation_tables()
        sch = self._q(self._schema)
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                DELETE FROM {sch}.simulations s
                USING {sch}.domains d
                WHERE d.id = s.domain_id AND d.registry_id = %s
                  AND d.folder = %s AND s.version = %s AND s.id = %s
                """,
                (self._registry(), folder, str(version), sim_id),
            )
            return bool(cur.rowcount and cur.rowcount > 0)

    def record_simulation_run(
        self, folder: str, version: str, entry: Dict[str, Any]
    ) -> None:
        """Append one run-history row. Best-effort: logs and never raises."""
        if not self._ensure_simulation_tables():
            return
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {sch}.simulation_runs
                        (domain_id, version, simulation_id, status, message,
                         error, task_id, started_at, finished_at, duration_s,
                         stats, owner)
                    SELECT d.id, %s, %s, %s, %s, %s, %s,
                           COALESCE(%s::timestamptz, now()), %s::timestamptz,
                           %s, %s::jsonb, %s
                    FROM {sch}.domains d
                    WHERE d.registry_id = %s AND d.folder = %s
                    """,
                    (
                        str(version),
                        str(entry.get("simulation_id", "")),
                        str(entry.get("status", "")),
                        str(entry.get("message", "") or ""),
                        str(entry.get("error", "") or ""),
                        str(entry.get("task_id", "") or ""),
                        entry.get("started_at"),
                        entry.get("finished_at"),
                        float(entry.get("duration_s", 0) or 0),
                        json.dumps(entry.get("stats") or {}),
                        str(entry.get("owner", "") or ""),
                        self._registry(),
                        folder,
                    ),
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("record_simulation_run(%s) failed: %s", folder, exc)

    def load_simulation_runs(
        self,
        folder: str,
        version: str,
        *,
        sim_id: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        if not self._ensure_simulation_tables():
            return []
        try:
            _psycopg, dict_row = _require_psycopg()
            sch = self._q(self._schema)
            clauses = ["d.registry_id = %s", "d.folder = %s", "r.version = %s"]
            params: List[Any] = [self._registry(), folder, str(version)]
            if sim_id:
                clauses.append("r.simulation_id = %s")
                params.append(sim_id)
            params.append(int(limit))
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT r.id, r.simulation_id, r.status, r.message, r.error,
                           r.task_id, r.started_at, r.finished_at, r.duration_s,
                           r.stats, r.owner
                    FROM {sch}.simulation_runs r
                    JOIN {sch}.domains d ON d.id = r.domain_id
                    WHERE {" AND ".join(clauses)}
                    ORDER BY r.started_at DESC, r.id DESC
                    LIMIT %s
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
            return [
                {
                    "id": int(r["id"] or 0),
                    "simulation_id": r["simulation_id"],
                    "status": r["status"],
                    "message": r["message"] or "",
                    "error": r["error"] or "",
                    "task_id": r["task_id"] or "",
                    "started_at": _iso(r.get("started_at")),
                    "finished_at": _iso(r.get("finished_at")),
                    "duration_s": float(r["duration_s"] or 0),
                    "stats": dict(r["stats"] or {}),
                    "owner": r["owner"] or "",
                }
                for r in rows
            ]
        except Exception as exc:  # noqa: BLE001
            logger.debug("load_simulation_runs(%s) failed: %s", folder, exc)
            return []
