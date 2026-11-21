"""Ontology Evolve runs on the Lakebase registry.

Two tables: ``evolution_runs`` holds one mutable row per run (intent,
coverage, proposal, candidate content, evaluation), guarded by an integer
``revision`` for compare-and-swap updates; ``evolution_results`` is an
append-only log of per-question, per-arm outcomes.

Unlike run *history* tables these writes are part of the feature's state,
so every failure raises instead of being logged and dropped.

Only portable PostgreSQL is used (``ON CONFLICT DO NOTHING RETURNING``,
``UPDATE … WHERE revision = %s RETURNING``) so the same code runs on every
Lakebase major version.

Extracted from :class:`LakebaseRegistryStore` (mixin, same as ``Simulations``).
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from back.core.errors import ConflictError, InfrastructureError, NotFoundError
from back.core.logging import get_logger

logger = get_logger("back.objects.registry.store.lakebase.store")

EVOLUTION_JSON_FIELDS = (
    "manifest",
    "intent",
    "coverage",
    "proposal",
    "candidate",
    "evaluation",
)
EVOLUTION_TEXT_FIELDS = (
    "status",
    "stage",
    "task_id",
    "accepted_version",
    "error_code",
    "error",
)
_RUN_COLUMNS = (
    "id, parent_version, status, stage, revision, idempotency_key, owner, "
    "task_id, cancel_requested, manifest, intent, coverage, proposal, "
    "candidate, evaluation, accepted_version, error_code, error, "
    "heartbeat_at, created_at, updated_at"
)


def _require_psycopg():
    """Defer to the store module so tests can monkeypatch that name."""
    from back.objects.registry.store.lakebase.store import _require_psycopg as _rp

    return _rp()


def _iso(value: Any) -> str:
    return value.isoformat() if hasattr(value, "isoformat") else str(value or "")


class EvolutionRuns:
    """Evolve run aggregate rows plus the per-question result log."""

    def _ensure_evolution_tables(self) -> bool:
        """Lazily create the two evolution tables if missing. Idempotent."""
        if getattr(self, "_evolution_tables_ready", False):
            return True
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM information_schema.tables "
                    "WHERE table_schema = %s "
                    "AND table_name IN ('evolution_runs', 'evolution_results')",
                    (self._schema,),
                )
                row = cur.fetchone()
                if row and int(row[0]) == 2:
                    self._evolution_tables_ready = True
                    return True
                for statement in self.evolution_ddl(sch):
                    cur.execute(statement)
            self._evolution_tables_ready = True
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "could not create evolution tables — run `make bootstrap-lakebase` "
                "as the schema owner to apply the migration: %s",
                exc,
            )
            return False

    @staticmethod
    def evolution_ddl(sch: str) -> List[str]:
        """``CREATE … IF NOT EXISTS`` statements for both evolution tables."""
        return [
            f"""
            CREATE TABLE IF NOT EXISTS {sch}.evolution_runs (
                domain_id        uuid NOT NULL
                                 REFERENCES {sch}.domains(id) ON DELETE CASCADE,
                id               text NOT NULL,
                parent_version   text NOT NULL,
                status           text NOT NULL DEFAULT 'CREATED',
                stage            text NOT NULL DEFAULT '',
                revision         integer NOT NULL DEFAULT 1,
                idempotency_key  text NOT NULL,
                owner            text NOT NULL DEFAULT '',
                task_id          text NOT NULL DEFAULT '',
                cancel_requested boolean NOT NULL DEFAULT false,
                manifest         jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                intent           jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                coverage         jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                proposal         jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                candidate        jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                evaluation       jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                accepted_version text NOT NULL DEFAULT '',
                error_code       text NOT NULL DEFAULT '',
                error            text NOT NULL DEFAULT '',
                heartbeat_at     timestamptz,
                created_at       timestamptz NOT NULL DEFAULT now(),
                updated_at       timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (domain_id, id),
                UNIQUE (domain_id, idempotency_key)
            )
            """,
            f"""
            CREATE INDEX IF NOT EXISTS idx_evolution_runs_domain_parent
                ON {sch}.evolution_runs(domain_id, parent_version, created_at DESC)
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {sch}.evolution_results (
                id           bigserial PRIMARY KEY,
                domain_id    uuid NOT NULL
                             REFERENCES {sch}.domains(id) ON DELETE CASCADE,
                run_id       text NOT NULL,
                question_id  text NOT NULL,
                arm          text NOT NULL,
                status       text NOT NULL,
                observed     jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                created_at   timestamptz NOT NULL DEFAULT now()
            )
            """,
            f"""
            CREATE INDEX IF NOT EXISTS idx_evolution_results_run
                ON {sch}.evolution_results(domain_id, run_id, created_at)
            """,
        ]

    def _require_evolution_tables(self) -> None:
        if not self._ensure_evolution_tables():
            raise InfrastructureError(
                "Evolution tables are unavailable in the Lakebase registry",
                detail="Run `make bootstrap-lakebase` as the schema owner.",
            )

    @staticmethod
    def _evolution_row(r: Dict[str, Any]) -> Dict[str, Any]:
        entry = {
            "id": r["id"],
            "parent_version": r["parent_version"],
            "status": r["status"],
            "stage": r["stage"] or "",
            "revision": int(r["revision"]),
            "idempotency_key": r["idempotency_key"],
            "owner": r["owner"] or "",
            "task_id": r["task_id"] or "",
            "cancel_requested": bool(r["cancel_requested"]),
            "accepted_version": r["accepted_version"] or "",
            "error_code": r["error_code"] or "",
            "error": r["error"] or "",
            "heartbeat_at": _iso(r["heartbeat_at"]) if r["heartbeat_at"] else "",
            "created_at": _iso(r["created_at"]),
            "updated_at": _iso(r["updated_at"]),
        }
        for name in EVOLUTION_JSON_FIELDS:
            entry[name] = dict(r[name] or {})
        return entry

    def create_evolution_run(self, folder: str, run: Dict[str, Any]) -> Dict[str, Any]:
        """Insert a run, or return the existing one with the same idempotency key.

        Returns the stored row plus ``created`` (``False`` when an earlier
        request with the same key already created it).
        """
        self._require_evolution_tables()
        _psycopg, dict_row = _require_psycopg()
        sch = self._q(self._schema)
        with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                INSERT INTO {sch}.evolution_runs
                    (domain_id, id, parent_version, status, stage,
                     idempotency_key, owner, manifest, intent)
                SELECT d.id, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb
                FROM {sch}.domains d
                WHERE d.registry_id = %s AND d.folder = %s
                ON CONFLICT (domain_id, idempotency_key) DO NOTHING
                RETURNING {_RUN_COLUMNS}
                """,
                (
                    str(run["id"]),
                    str(run["parent_version"]),
                    str(run.get("status") or "CREATED"),
                    str(run.get("stage") or ""),
                    str(run["idempotency_key"]),
                    str(run.get("owner") or ""),
                    json.dumps(run.get("manifest") or {}),
                    json.dumps(run.get("intent") or {}),
                    self._registry(),
                    folder,
                ),
            )
            row = cur.fetchone()
            if row:
                return {**self._evolution_row(row), "created": True}
            cur.execute(
                f"""
                SELECT r.{_RUN_COLUMNS.replace(', ', ', r.')}
                FROM {sch}.evolution_runs r
                JOIN {sch}.domains d ON d.id = r.domain_id
                WHERE d.registry_id = %s AND d.folder = %s
                  AND r.idempotency_key = %s
                """,
                (self._registry(), folder, str(run["idempotency_key"])),
            )
            row = cur.fetchone()
        if not row:
            raise NotFoundError(
                "Context is not registered — save it to the registry first",
                detail=folder,
            )
        return {**self._evolution_row(row), "created": False}

    def load_evolution_run(self, folder: str, run_id: str) -> Optional[Dict[str, Any]]:
        self._require_evolution_tables()
        _psycopg, dict_row = _require_psycopg()
        sch = self._q(self._schema)
        with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT r.{_RUN_COLUMNS.replace(', ', ', r.')}
                FROM {sch}.evolution_runs r
                JOIN {sch}.domains d ON d.id = r.domain_id
                WHERE d.registry_id = %s AND d.folder = %s AND r.id = %s
                """,
                (self._registry(), folder, str(run_id)),
            )
            row = cur.fetchone()
        return self._evolution_row(row) if row else None

    def list_evolution_runs(
        self, folder: str, parent_version: Optional[str] = None, limit: int = 50
    ) -> List[Dict[str, Any]]:
        self._require_evolution_tables()
        _psycopg, dict_row = _require_psycopg()
        sch = self._q(self._schema)
        version_clause = "AND r.parent_version = %s" if parent_version else ""
        params: List[Any] = [self._registry(), folder]
        if parent_version:
            params.append(str(parent_version))
        params.append(int(limit))
        with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT r.{_RUN_COLUMNS.replace(', ', ', r.')}
                FROM {sch}.evolution_runs r
                JOIN {sch}.domains d ON d.id = r.domain_id
                WHERE d.registry_id = %s AND d.folder = %s {version_clause}
                ORDER BY r.created_at DESC
                LIMIT %s
                """,
                tuple(params),
            )
            rows = cur.fetchall()
        return [self._evolution_row(r) for r in rows]

    def update_evolution_run(
        self,
        folder: str,
        run_id: str,
        expected_revision: int,
        changes: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Compare-and-swap update; raises ConflictError on a stale revision.

        ``changes`` may contain the JSON fields, the text fields,
        ``cancel_requested`` and ``touch_heartbeat``. Unknown keys raise.
        """
        allowed = set(EVOLUTION_JSON_FIELDS) | set(EVOLUTION_TEXT_FIELDS)
        allowed |= {"cancel_requested", "touch_heartbeat"}
        unknown = set(changes) - allowed
        if unknown:
            raise ValueError(f"Unknown evolution run fields: {sorted(unknown)}")
        self._require_evolution_tables()
        _psycopg, dict_row = _require_psycopg()
        sch = self._q(self._schema)
        sets: List[str] = []
        params: List[Any] = []
        for name in EVOLUTION_JSON_FIELDS:
            if name in changes:
                sets.append(f"{name} = %s::jsonb")
                params.append(json.dumps(changes[name] or {}))
        for name in EVOLUTION_TEXT_FIELDS:
            if name in changes:
                sets.append(f"{name} = %s")
                params.append(str(changes[name] or ""))
        if "cancel_requested" in changes:
            sets.append("cancel_requested = %s")
            params.append(bool(changes["cancel_requested"]))
        if changes.get("touch_heartbeat"):
            sets.append("heartbeat_at = now()")
        sets += ["revision = r.revision + 1", "updated_at = now()"]
        with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                UPDATE {sch}.evolution_runs r
                SET {", ".join(sets)}
                FROM {sch}.domains d
                WHERE d.id = r.domain_id
                  AND d.registry_id = %s AND d.folder = %s
                  AND r.id = %s AND r.revision = %s
                RETURNING r.{_RUN_COLUMNS.replace(', ', ', r.')}
                """,
                (
                    *params,
                    self._registry(),
                    folder,
                    str(run_id),
                    int(expected_revision),
                ),
            )
            row = cur.fetchone()
        if row:
            return self._evolution_row(row)
        current = self.load_evolution_run(folder, run_id)
        if current is None:
            raise NotFoundError("Evolution run not found", detail=str(run_id))
        raise ConflictError(
            "The run changed since it was read — reload and retry",
            detail=f"expected revision {expected_revision}, current {current['revision']}",
        )

    def delete_evolution_run(self, folder: str, run_id: str) -> bool:
        self._require_evolution_tables()
        sch = self._q(self._schema)
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                DELETE FROM {sch}.evolution_results x
                USING {sch}.domains d
                WHERE d.id = x.domain_id AND d.registry_id = %s
                  AND d.folder = %s AND x.run_id = %s
                """,
                (self._registry(), folder, str(run_id)),
            )
            cur.execute(
                f"""
                DELETE FROM {sch}.evolution_runs r
                USING {sch}.domains d
                WHERE d.id = r.domain_id AND d.registry_id = %s
                  AND d.folder = %s AND r.id = %s
                """,
                (self._registry(), folder, str(run_id)),
            )
            return cur.rowcount > 0

    def append_evolution_result(
        self, folder: str, run_id: str, result: Dict[str, Any]
    ) -> None:
        self._require_evolution_tables()
        sch = self._q(self._schema)
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {sch}.evolution_results
                    (domain_id, run_id, question_id, arm, status, observed)
                SELECT d.id, %s, %s, %s, %s, %s::jsonb
                FROM {sch}.domains d
                WHERE d.registry_id = %s AND d.folder = %s
                """,
                (
                    str(run_id),
                    str(result["question_id"]),
                    str(result["arm"]),
                    str(result["status"]),
                    json.dumps(result.get("observed") or {}),
                    self._registry(),
                    folder,
                ),
            )
            if cur.rowcount == 0:
                raise NotFoundError(
                    "Context is not registered — save it to the registry first",
                    detail=folder,
                )

    def list_evolution_results(self, folder: str, run_id: str) -> List[Dict[str, Any]]:
        self._require_evolution_tables()
        _psycopg, dict_row = _require_psycopg()
        sch = self._q(self._schema)
        with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT x.question_id, x.arm, x.status, x.observed, x.created_at
                FROM {sch}.evolution_results x
                JOIN {sch}.domains d ON d.id = x.domain_id
                WHERE d.registry_id = %s AND d.folder = %s AND x.run_id = %s
                ORDER BY x.created_at, x.id
                """,
                (self._registry(), folder, str(run_id)),
            )
            rows = cur.fetchall()
        return [
            {
                "question_id": r["question_id"],
                "arm": r["arm"],
                "status": r["status"],
                "observed": dict(r["observed"] or {}),
                "created_at": _iso(r["created_at"]),
            }
            for r in rows
        ]

    def mark_interrupted_evolution_runs(self, stale_after_s: int) -> int:
        """Mark RUNNING runs with a stale heartbeat INTERRUPTED. Returns the count."""
        self._require_evolution_tables()
        sch = self._q(self._schema)
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {sch}.evolution_runs r
                SET status = 'INTERRUPTED', revision = r.revision + 1,
                    updated_at = now(), error_code = 'worker_lost'
                FROM {sch}.domains d
                WHERE d.id = r.domain_id AND d.registry_id = %s
                  AND r.status = 'RUNNING'
                  AND (r.heartbeat_at IS NULL
                       OR r.heartbeat_at < now() - make_interval(secs => %s))
                """,
                (self._registry(), int(stale_after_s)),
            )
            return int(cur.rowcount or 0)

    def touch_evolution_heartbeat(self, folder: str, run_id: str) -> None:
        """Refresh a RUNNING run's heartbeat without bumping its revision."""
        self._require_evolution_tables()
        sch = self._q(self._schema)
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {sch}.evolution_runs r SET heartbeat_at = now()
                FROM {sch}.domains d
                WHERE d.id = r.domain_id AND d.registry_id = %s
                  AND d.folder = %s AND r.id = %s AND r.status = 'RUNNING'
                """,
                (self._registry(), folder, str(run_id)),
            )

    # -- review link ------------------------------------------------------

    def _evolution_review_meta(self, folder: str, version: str) -> Dict[str, Any]:
        """Evolve evidence of *version*; ValidationError when it went stale."""
        from back.objects.ontology.evolution.EvolutionReviewLink import (
            EvolutionReviewLink,
        )

        ok, data, _msg = self.read_version(folder, version, include_simulations=False)
        return EvolutionReviewLink.review_meta(data, version) if ok else {}

    def update_version_status(self, folder: str, version: str, status: str):
        """Refuse review of an Evolve version that changed after its evaluation.

        Every route into review (lifecycle change, review submit) reaches
        the store through here, so the gate cannot be bypassed.
        """
        if (status or "").upper() == "IN-REVIEW":
            self._evolution_review_meta(folder, version)
        return super().update_version_status(folder, version, status)

    def with_evolution_review_meta(
        self, folder: str, version: str, meta: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """*meta* plus the evaluation summary; best-effort so the audit row lands."""
        try:
            return {**(meta or {}), **self._evolution_review_meta(folder, version)}
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "evolution review meta skipped for %s/%s: %s", folder, version, exc
            )
            return dict(meta or {})

    # -- accept -----------------------------------------------------------

    def accept_evolution_run(
        self,
        folder: str,
        run_id: str,
        expected_revision: int,
        version_row: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Insert the next DRAFT version from an evaluated run, exactly once.

        One transaction: lock the domain row (serialises concurrent
        allocators), lock the run, allocate ``max(numeric version) + 1``,
        insert the version *without* touching the ``domains`` row (so
        ``review_quorum`` / ``mcp_policy`` are kept), then mark the run
        ACCEPTED. A repeated accept of the same run returns the version it
        already created. Returns ``{"version", "created", "run"}``.
        """
        from back.objects.registry.registry_cache import invalidate_registry_cache

        self._require_evolution_tables()
        _psycopg, dict_row = _require_psycopg()
        with self._connect() as conn, conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                domain_id, run = self._lock_run_for_accept(cur, folder, run_id)
                if run["accepted_version"]:
                    return {
                        "version": run["accepted_version"],
                        "created": False,
                        "run": self._evolution_row(run),
                    }
                if int(run["revision"]) != int(expected_revision):
                    raise ConflictError(
                        "The run changed since it was read — reload and retry",
                        detail=f"expected revision {expected_revision}, "
                        f"current {run['revision']}",
                    )
                if run["status"] != "EVALUATED":
                    raise ConflictError(f"A {run['status']} run cannot be accepted")
                version = self._insert_next_version(cur, domain_id, version_row)
                cur.execute(
                    f"""
                    UPDATE {self._q(self._schema)}.evolution_runs
                    SET status = 'ACCEPTED', accepted_version = %s,
                        revision = revision + 1, updated_at = now()
                    WHERE domain_id = %s AND id = %s
                    RETURNING {_RUN_COLUMNS}
                    """,
                    (version, domain_id, str(run_id)),
                )
                accepted = cur.fetchone()
        invalidate_registry_cache(self.cache_key)
        return {
            "version": version,
            "created": True,
            "run": self._evolution_row(accepted),
        }

    def _lock_run_for_accept(self, cur, folder: str, run_id: str):
        sch = self._q(self._schema)
        cur.execute(
            f"SELECT id FROM {sch}.domains "
            "WHERE registry_id = %s AND folder = %s FOR UPDATE",
            (self._registry(), folder),
        )
        domain = cur.fetchone()
        if not domain:
            raise NotFoundError("Context not found", detail=folder)
        cur.execute(
            f"SELECT {_RUN_COLUMNS} FROM {sch}.evolution_runs "
            "WHERE domain_id = %s AND id = %s FOR UPDATE",
            (domain["id"], str(run_id)),
        )
        run = cur.fetchone()
        if not run:
            raise NotFoundError("Evolution run not found", detail=str(run_id))
        return domain["id"], run

    def _insert_next_version(
        self, cur, domain_id: Any, version_row: Dict[str, Any]
    ) -> str:
        sch = self._q(self._schema)
        cur.execute(
            f"""
            SELECT COALESCE(MAX(version::integer), 0) + 1 AS next
            FROM {sch}.domain_versions
            WHERE domain_id = %s AND version ~ '^[0-9]+$'
            """,
            (domain_id,),
        )
        version = str(cur.fetchone()["next"])
        info = dict(version_row.get("info") or {})
        cur.execute(
            f"""
            INSERT INTO {sch}.domain_versions
                (domain_id, version, info, ontology, assignment,
                 design_layout, metadata, mcp_enabled, status,
                 last_update, last_build)
            VALUES (%s, %s, %s::jsonb, %s::jsonb, %s::jsonb,
                    %s::jsonb, %s::jsonb, %s, 'DRAFT', %s, '')
            ON CONFLICT (domain_id, version) DO NOTHING
            RETURNING version
            """,
            (
                domain_id,
                version,
                json.dumps(info),
                json.dumps(version_row.get("ontology") or {}),
                json.dumps(version_row.get("assignment") or {}),
                json.dumps(version_row.get("design_layout") or {}),
                json.dumps(version_row.get("metadata") or {}),
                bool(info.get("mcp_enabled")),
                str(info.get("last_update") or ""),
            ),
        )
        if not cur.fetchone():
            raise ConflictError(
                "Another version was created at the same time — retry", detail=version
            )
        return version
