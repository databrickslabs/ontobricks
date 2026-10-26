"""Review/validation audit log on the Lakebase registry.

Extracted from :class:`LakebaseRegistryStore` (Fowler Extract Class / mixin).
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

from ..base import ReviewEvent

from back.core.logging import get_logger

logger = get_logger("back.objects.registry.store.lakebase.store")


def _require_psycopg():
    """Defer to the store module so tests can monkeypatch that name."""
    from back.objects.registry.store.lakebase.store import _require_psycopg as _rp

    return _rp()


class ReviewEvents:
    """Review/validation audit rows for a domain version."""

    def _ensure_review_events_table(self) -> bool:
        """Lazily create ``domain_review_events`` (+ index) if missing.

        Self-heals deployments created before the review/validation audit
        log existed — same ownership-safe pattern as
        :meth:`_ensure_build_runs_table`: check first, only attempt DDL
        when the table is genuinely absent. Best-effort: on failure it
        logs and returns ``False`` so callers no-op rather than breaking
        a transition.
        """
        if self._review_events_ready:
            return True
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = %s "
                    "AND table_name = 'domain_review_events'",
                    (self._schema,),
                )
                if cur.fetchone():
                    self._review_events_ready = True
                    return True
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {sch}.domain_review_events (
                        id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                        domain_id       uuid NOT NULL
                                        REFERENCES {sch}.domains(id)
                                        ON DELETE CASCADE,
                        version         text NOT NULL,
                        actor           text NOT NULL,
                        action          text NOT NULL,
                        from_status     text NOT NULL DEFAULT '',
                        to_status       text NOT NULL DEFAULT '',
                        comment         text NOT NULL DEFAULT '',
                        meta            jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                        created_at      timestamptz NOT NULL DEFAULT now()
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_review_events_domain_version
                        ON {sch}.domain_review_events
                           (domain_id, version, created_at)
                    """
                )
            self._review_events_ready = True
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "could not create domain_review_events table — "
                "run `make bootstrap-lakebase` as the schema owner to "
                "apply the migration: %s",
                exc,
            )
            return False

    def record_review_event(
        self,
        folder: str,
        version: str,
        actor: str,
        action: str,
        *,
        from_status: str = "",
        to_status: str = "",
        comment: str = "",
        meta: Optional[Dict[str, Any]] = None,
    ) -> Tuple[bool, str]:
        if not self._ensure_review_events_table():
            return False, "review audit log unavailable"
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {sch}.domain_review_events
                        (domain_id, version, actor, action, from_status,
                         to_status, comment, meta)
                    SELECT d.id, %s, %s, %s, %s, %s, %s, %s::jsonb
                    FROM {sch}.domains d
                    WHERE d.registry_id = %s AND d.folder = %s
                    """,
                    (
                        version,
                        actor or "",
                        action or "",
                        from_status or "",
                        to_status or "",
                        comment or "",
                        json.dumps(meta or {}),
                        self._registry(),
                        folder,
                    ),
                )
                if cur.rowcount == 0:
                    return False, f"Domain '{folder}' not found"
            return True, ""
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "record_review_event(%s/%s) failed: %s", folder, version, exc
            )
            return False, str(exc)

    @staticmethod
    def _review_row_to_event(r: Dict[str, Any]) -> ReviewEvent:
        return {
            "id": str(r.get("id") or ""),
            "folder": r.get("folder", "") or "",
            "version": r["version"],
            "actor": r["actor"] or "",
            "action": r["action"] or "",
            "from_status": r["from_status"] or "",
            "to_status": r["to_status"] or "",
            "comment": r["comment"] or "",
            "meta": dict(r["meta"] or {}),
            "created_at": (
                r["created_at"].isoformat() if r.get("created_at") else ""
            ),
        }

    def list_review_events(
        self, folder: str, version: Optional[str] = None
    ) -> List[ReviewEvent]:
        if not self._ensure_review_events_table():
            return []
        try:
            psycopg, dict_row = _require_psycopg()
            sch = self._q(self._schema)
            clauses = ["d.registry_id = %s", "d.folder = %s"]
            params: List[Any] = [self._registry(), folder]
            if version:
                clauses.append("e.version = %s")
                params.append(version)
            where = " AND ".join(clauses)
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT e.id, d.folder, e.version, e.actor, e.action,
                           e.from_status, e.to_status, e.comment, e.meta,
                           e.created_at
                    FROM {sch}.domain_review_events e
                    JOIN {sch}.domains d ON d.id = e.domain_id
                    WHERE {where}
                    ORDER BY e.created_at ASC, e.id ASC
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
            return [self._review_row_to_event(r) for r in rows]
        except Exception as exc:  # noqa: BLE001
            logger.debug("list_review_events(%s) failed: %s", folder, exc)
            return []

    def list_all_review_events(self) -> List[ReviewEvent]:
        if not self._ensure_review_events_table():
            return []
        try:
            psycopg, dict_row = _require_psycopg()
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT e.id, d.folder, e.version, e.actor, e.action,
                           e.from_status, e.to_status, e.comment, e.meta,
                           e.created_at
                    FROM {sch}.domain_review_events e
                    JOIN {sch}.domains d ON d.id = e.domain_id
                    WHERE d.registry_id = %s
                    ORDER BY e.created_at ASC, e.id ASC
                    """,
                    (self._registry(),),
                )
                rows = cur.fetchall()
            return [self._review_row_to_event(r) for r in rows]
        except Exception as exc:  # noqa: BLE001
            logger.debug("list_all_review_events failed: %s", exc)
            return []
