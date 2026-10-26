"""Ontology/mapping change audit log on the Lakebase registry.

Extracted from :class:`LakebaseRegistryStore` (Fowler Extract Class / mixin).
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

from ..base import ChangeEvent

from back.core.logging import get_logger

logger = get_logger("back.objects.registry.store.lakebase.store")


def _require_psycopg():
    """Defer to the store module so tests can monkeypatch that name."""
    from back.objects.registry.store.lakebase.store import _require_psycopg as _rp

    return _rp()


class ChangeEvents:
    """Ontology/mapping change audit rows."""

    def _ensure_change_events_table(self) -> bool:
        """Lazily create ``domain_change_events`` (+ index) if missing.

        Self-heals deployments created before the ontology/mapping change
        audit log existed — same ownership-safe pattern as
        :meth:`_ensure_review_events_table`. Best-effort: on failure it
        logs and returns ``False`` so callers no-op rather than breaking
        a save.
        """
        if self._change_events_ready:
            return True
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = %s "
                    "AND table_name = 'domain_change_events'",
                    (self._schema,),
                )
                if cur.fetchone():
                    self._change_events_ready = True
                    return True
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {sch}.domain_change_events (
                        id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                        domain_id       uuid NOT NULL
                                        REFERENCES {sch}.domains(id)
                                        ON DELETE CASCADE,
                        version         text NOT NULL,
                        actor           text NOT NULL DEFAULT '',
                        source          text NOT NULL DEFAULT 'user',
                        action          text NOT NULL,
                        entity_type     text NOT NULL DEFAULT '',
                        entity_ref      text NOT NULL DEFAULT '',
                        summary         text NOT NULL DEFAULT '',
                        meta            jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                        occurred_at     timestamptz NOT NULL DEFAULT now(),
                        created_at      timestamptz NOT NULL DEFAULT now()
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_change_events_domain_version
                        ON {sch}.domain_change_events
                           (domain_id, version, occurred_at)
                    """
                )
            self._change_events_ready = True
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "could not create domain_change_events table — "
                "run `make bootstrap-lakebase` as the schema owner to "
                "apply the migration: %s",
                exc,
            )
            return False

    def record_change_events(
        self,
        folder: str,
        version: str,
        actor: str,
        events: List[Dict[str, Any]],
    ) -> Tuple[bool, str]:
        if not events:
            return True, ""
        if not self._ensure_change_events_table():
            return False, "change audit log unavailable"
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT id FROM {sch}.domains
                    WHERE registry_id = %s AND folder = %s
                    """,
                    (self._registry(), folder),
                )
                row = cur.fetchone()
                if not row:
                    return False, f"Domain '{folder}' not found"
                domain_id = row[0]
                params = [
                    (
                        domain_id,
                        version,
                        actor or "",
                        (e.get("source") or "user"),
                        (e.get("action") or ""),
                        (e.get("entity_type") or ""),
                        (e.get("entity_ref") or ""),
                        (e.get("summary") or ""),
                        json.dumps(e.get("meta") or {}),
                        (e.get("ts") or e.get("occurred_at") or None),
                    )
                    for e in events
                ]
                cur.executemany(
                    f"""
                    INSERT INTO {sch}.domain_change_events
                        (domain_id, version, actor, source, action,
                         entity_type, entity_ref, summary, meta, occurred_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb,
                            COALESCE(%s::timestamptz, now()))
                    """,
                    params,
                )
            return True, ""
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "record_change_events(%s/%s) failed: %s", folder, version, exc
            )
            return False, str(exc)

    @staticmethod
    def _change_row_to_event(r: Dict[str, Any]) -> ChangeEvent:
        return {
            "id": str(r.get("id") or ""),
            "folder": r.get("folder", "") or "",
            "version": r["version"],
            "actor": r.get("actor") or "",
            "source": r.get("source") or "user",
            "action": r.get("action") or "",
            "entity_type": r.get("entity_type") or "",
            "entity_ref": r.get("entity_ref") or "",
            "summary": r.get("summary") or "",
            "meta": dict(r.get("meta") or {}),
            "occurred_at": (
                r["occurred_at"].isoformat() if r.get("occurred_at") else ""
            ),
            "created_at": (
                r["created_at"].isoformat() if r.get("created_at") else ""
            ),
        }

    def list_change_events(
        self, folder: str, version: Optional[str] = None, limit: int = 500
    ) -> List[ChangeEvent]:
        if not self._ensure_change_events_table():
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
            params.append(int(limit) if limit else 500)
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT e.id, d.folder, e.version, e.actor, e.source,
                           e.action, e.entity_type, e.entity_ref, e.summary,
                           e.meta, e.occurred_at, e.created_at
                    FROM {sch}.domain_change_events e
                    JOIN {sch}.domains d ON d.id = e.domain_id
                    WHERE {where}
                    ORDER BY e.occurred_at ASC, e.id ASC
                    LIMIT %s
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
            return [self._change_row_to_event(r) for r in rows]
        except Exception as exc:  # noqa: BLE001
            logger.debug("list_change_events(%s) failed: %s", folder, exc)
            return []
