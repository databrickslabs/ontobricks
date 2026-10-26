"""Collaborative comments and tasks on the Lakebase registry.

Extracted from :class:`LakebaseRegistryStore` (Fowler Extract Class / mixin).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from ..base import DomainComment, DomainTask

from back.core.logging import get_logger

# Keep log records on the store module name (tests / operators grep it).
logger = get_logger("back.objects.registry.store.lakebase.store")


def _require_psycopg():
    """Defer to the store module so tests can monkeypatch that name."""
    from back.objects.registry.store.lakebase.store import _require_psycopg as _rp

    return _rp()


class Collab:
    """Comments and tasks for a domain version."""

    def _ensure_collab_tables(self) -> bool:
        """Lazily create ``domain_comments`` + ``domain_tasks`` (+ indexes).

        Self-heals deployments created before the collaborative comments
        and tasks feature existed — same ownership-safe pattern as
        :meth:`_ensure_review_events_table`. Best-effort: on failure it
        logs and returns ``False`` so callers no-op rather than breaking.
        """
        if self._collab_tables_ready:
            return True
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = %s "
                    "AND table_name = 'domain_comments'",
                    (self._schema,),
                )
                if cur.fetchone():
                    self._collab_tables_ready = True
                    return True
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {sch}.domain_comments (
                        id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                        domain_id   uuid NOT NULL
                                    REFERENCES {sch}.domains(id)
                                    ON DELETE CASCADE,
                        version     text NOT NULL,
                        parent_id   uuid
                                    REFERENCES {sch}.domain_comments(id)
                                    ON DELETE CASCADE,
                        author      text NOT NULL,
                        body        text NOT NULL DEFAULT '',
                        resolved    boolean NOT NULL DEFAULT false,
                        created_at  timestamptz NOT NULL DEFAULT now()
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_domain_comments_lookup
                        ON {sch}.domain_comments (domain_id, version, created_at)
                    """
                )
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {sch}.domain_tasks (
                        id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                        domain_id   uuid NOT NULL
                                    REFERENCES {sch}.domains(id)
                                    ON DELETE CASCADE,
                        version     text NOT NULL,
                        assignee    text NOT NULL,
                        created_by  text NOT NULL,
                        title       text NOT NULL,
                        description text NOT NULL DEFAULT '',
                        status      text NOT NULL DEFAULT 'open',
                        due_date    date,
                        comment_id  uuid
                                    REFERENCES {sch}.domain_comments(id)
                                    ON DELETE SET NULL,
                        created_at  timestamptz NOT NULL DEFAULT now(),
                        updated_at  timestamptz NOT NULL DEFAULT now()
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_domain_tasks_assignee
                        ON {sch}.domain_tasks (lower(assignee), status)
                    """
                )
                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_domain_tasks_domain
                        ON {sch}.domain_tasks (domain_id, version)
                    """
                )
            self._collab_tables_ready = True
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "could not create domain_comments/domain_tasks tables — "
                "run `make bootstrap-lakebase` as the schema owner to "
                "apply the migration: %s",
                exc,
            )
            return False

    @staticmethod
    def _comment_row_to_dict(
        r: Dict[str, Any], folder: str = ""
    ) -> DomainComment:
        return {
            "id": str(r.get("id") or ""),
            "folder": r.get("folder", folder) or folder,
            "version": r["version"],
            "parent_id": str(r["parent_id"]) if r.get("parent_id") else "",
            "author": r["author"] or "",
            "body": r["body"] or "",
            "resolved": bool(r["resolved"]),
            "created_at": (
                r["created_at"].isoformat() if r.get("created_at") else ""
            ),
        }

    def insert_comment(
        self,
        folder: str,
        version: str,
        *,
        author: str,
        body: str,
        parent_id: Optional[str] = None,
    ) -> Optional[DomainComment]:
        if not self._ensure_collab_tables():
            return None
        try:
            psycopg, dict_row = _require_psycopg()
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    INSERT INTO {sch}.domain_comments
                        (domain_id, version, parent_id, author, body)
                    SELECT d.id, %s, %s, %s, %s
                    FROM {sch}.domains d
                    WHERE d.registry_id = %s AND d.folder = %s
                    RETURNING id, version, parent_id, author, body,
                              resolved, created_at
                    """,
                    (
                        version,
                        parent_id or None,
                        author or "",
                        body or "",
                        self._registry(),
                        folder,
                    ),
                )
                row = cur.fetchone()
            if not row:
                return None
            return self._comment_row_to_dict(row, folder)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "insert_comment(%s/%s) failed: %s", folder, version, exc
            )
            return None

    def list_comments(
        self,
        folder: str,
        version: Optional[str] = None,
        *,
        include_resolved: bool = True,
    ) -> List[DomainComment]:
        if not self._ensure_collab_tables():
            return []
        try:
            psycopg, dict_row = _require_psycopg()
            sch = self._q(self._schema)
            clauses = ["d.registry_id = %s", "d.folder = %s"]
            params: List[Any] = [self._registry(), folder]
            if version:
                clauses.append("c.version = %s")
                params.append(version)
            if not include_resolved:
                clauses.append("c.resolved = false")
            where = " AND ".join(clauses)
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT c.id, d.folder, c.version, c.parent_id,
                           c.author, c.body, c.resolved, c.created_at
                    FROM {sch}.domain_comments c
                    JOIN {sch}.domains d ON d.id = c.domain_id
                    WHERE {where}
                    ORDER BY c.created_at ASC, c.id ASC
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
            return [self._comment_row_to_dict(r) for r in rows]
        except Exception as exc:  # noqa: BLE001
            logger.debug("list_comments(%s) failed: %s", folder, exc)
            return []

    def resolve_comment(
        self, folder: str, comment_id: str, *, resolved: bool = True
    ) -> Tuple[bool, str]:
        if not self._ensure_collab_tables():
            return False, "comments backend unavailable"
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {sch}.domain_comments c
                    SET resolved = %s
                    FROM {sch}.domains d
                    WHERE c.domain_id = d.id
                      AND d.registry_id = %s AND d.folder = %s
                      AND c.id = %s
                    """,
                    (resolved, self._registry(), folder, comment_id),
                )
                if cur.rowcount == 0:
                    return False, "Comment not found"
            return True, ""
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "resolve_comment(%s/%s) failed: %s", folder, comment_id, exc
            )
            return False, str(exc)

    @staticmethod
    def _task_row_to_dict(r: Dict[str, Any], folder: str = "") -> DomainTask:
        return {
            "id": str(r.get("id") or ""),
            "folder": r.get("folder", folder) or folder,
            "version": r["version"],
            "assignee": r["assignee"] or "",
            "created_by": r["created_by"] or "",
            "title": r["title"] or "",
            "description": r["description"] or "",
            "status": r["status"] or "open",
            "due_date": r["due_date"].isoformat() if r.get("due_date") else "",
            "comment_id": str(r["comment_id"]) if r.get("comment_id") else "",
            "created_at": (
                r["created_at"].isoformat() if r.get("created_at") else ""
            ),
            "updated_at": (
                r["updated_at"].isoformat() if r.get("updated_at") else ""
            ),
        }

    def insert_task(
        self,
        folder: str,
        version: str,
        *,
        assignee: str,
        created_by: str,
        title: str,
        description: str = "",
        due_date: Optional[str] = None,
        comment_id: Optional[str] = None,
    ) -> Optional[DomainTask]:
        if not self._ensure_collab_tables():
            return None
        try:
            psycopg, dict_row = _require_psycopg()
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    INSERT INTO {sch}.domain_tasks
                        (domain_id, version, assignee, created_by, title,
                         description, due_date, comment_id)
                    SELECT d.id, %s, %s, %s, %s, %s, %s, %s
                    FROM {sch}.domains d
                    WHERE d.registry_id = %s AND d.folder = %s
                    RETURNING id, version, assignee, created_by, title,
                              description, status, due_date, comment_id,
                              created_at, updated_at
                    """,
                    (
                        version,
                        assignee or "",
                        created_by or "",
                        title or "",
                        description or "",
                        due_date or None,
                        comment_id or None,
                        self._registry(),
                        folder,
                    ),
                )
                row = cur.fetchone()
            if not row:
                return None
            return self._task_row_to_dict(row, folder)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "insert_task(%s/%s) failed: %s", folder, version, exc
            )
            return None

    def list_tasks(
        self, folder: str, version: Optional[str] = None
    ) -> List[DomainTask]:
        if not self._ensure_collab_tables():
            return []
        try:
            psycopg, dict_row = _require_psycopg()
            sch = self._q(self._schema)
            clauses = ["d.registry_id = %s", "d.folder = %s"]
            params: List[Any] = [self._registry(), folder]
            if version:
                clauses.append("t.version = %s")
                params.append(version)
            where = " AND ".join(clauses)
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT t.id, d.folder, t.version, t.assignee, t.created_by,
                           t.title, t.description, t.status, t.due_date,
                           t.comment_id, t.created_at, t.updated_at
                    FROM {sch}.domain_tasks t
                    JOIN {sch}.domains d ON d.id = t.domain_id
                    WHERE {where}
                    ORDER BY t.created_at DESC, t.id DESC
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
            return [self._task_row_to_dict(r) for r in rows]
        except Exception as exc:  # noqa: BLE001
            logger.debug("list_tasks(%s) failed: %s", folder, exc)
            return []

    def list_tasks_for_assignee(self, assignee: str) -> List[DomainTask]:
        if not self._ensure_collab_tables():
            return []
        try:
            psycopg, dict_row = _require_psycopg()
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT t.id, d.folder, t.version, t.assignee, t.created_by,
                           t.title, t.description, t.status, t.due_date,
                           t.comment_id, t.created_at, t.updated_at
                    FROM {sch}.domain_tasks t
                    JOIN {sch}.domains d ON d.id = t.domain_id
                    WHERE d.registry_id = %s AND lower(t.assignee) = lower(%s)
                    ORDER BY t.created_at DESC, t.id DESC
                    """,
                    (self._registry(), assignee or ""),
                )
                rows = cur.fetchall()
            return [self._task_row_to_dict(r) for r in rows]
        except Exception as exc:  # noqa: BLE001
            logger.debug("list_tasks_for_assignee(%s) failed: %s", assignee, exc)
            return []

    def update_task_status(
        self, folder: str, task_id: str, status: str
    ) -> Tuple[bool, str]:
        if not self._ensure_collab_tables():
            return False, "tasks backend unavailable"
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {sch}.domain_tasks t
                    SET status = %s, updated_at = now()
                    FROM {sch}.domains d
                    WHERE t.domain_id = d.id
                      AND d.registry_id = %s AND d.folder = %s
                      AND t.id = %s
                    """,
                    (status, self._registry(), folder, task_id),
                )
                if cur.rowcount == 0:
                    return False, "Task not found"
            return True, ""
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "update_task_status(%s/%s) failed: %s", folder, task_id, exc
            )
            return False, str(exc)
