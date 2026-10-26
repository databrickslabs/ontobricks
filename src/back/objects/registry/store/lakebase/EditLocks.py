"""Single-editor domain locks on the Lakebase registry.

Extracted from :class:`LakebaseRegistryStore` (Fowler Extract Class / mixin).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


from back.core.logging import get_logger

# Keep log records on the store module name (tests / operators grep it).
logger = get_logger("back.objects.registry.store.lakebase.store")


def _require_psycopg():
    """Defer to the store module so tests can monkeypatch that name."""
    from back.objects.registry.store.lakebase.store import _require_psycopg as _rp

    return _rp()


class EditLocks:
    """DRAFT single-editor lock (acquire, renew, release, list)."""

    def _ensure_domain_edit_locks_table(self) -> bool:
        """Lazily create ``domain_edit_locks`` (self-heal old deployments).

        Same ownership-safe pattern as :meth:`_ensure_collab_tables`:
        probe ``information_schema``, create idempotently, and on failure
        log + return ``False`` so callers no-op (the lock simply becomes a
        no-op rather than breaking domain loads).
        """
        if self._edit_locks_ready:
            return True
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = %s "
                    "AND table_name = 'domain_edit_locks'",
                    (self._schema,),
                )
                if cur.fetchone():
                    self._edit_locks_ready = True
                    return True
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {sch}.domain_edit_locks (
                        domain_id      uuid NOT NULL
                                       REFERENCES {sch}.domains(id)
                                       ON DELETE CASCADE,
                        version        text NOT NULL,
                        holder_email   text NOT NULL,
                        holder_name    text NOT NULL DEFAULT '',
                        holder_session text NOT NULL DEFAULT '',
                        acquired_at    timestamptz NOT NULL DEFAULT now(),
                        heartbeat_at   timestamptz NOT NULL DEFAULT now(),
                        PRIMARY KEY (domain_id, version)
                    )
                    """
                )
            self._edit_locks_ready = True
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "could not create domain_edit_locks table — run "
                "`make bootstrap-lakebase` as the schema owner to apply "
                "the migration: %s",
                exc,
            )
            return False

    @staticmethod
    def _edit_lock_row_to_dict(r: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "holder_email": r.get("holder_email") or "",
            "holder_name": r.get("holder_name") or "",
            "holder_session": r.get("holder_session") or "",
            "acquired_at": (
                r["acquired_at"].isoformat() if r.get("acquired_at") else ""
            ),
            "heartbeat_at": (
                r["heartbeat_at"].isoformat() if r.get("heartbeat_at") else ""
            ),
            # Populated only by reads that pass a positive lease TTL (the
            # ``is_stale`` SQL expression); absent/false otherwise.
            "is_stale": bool(r.get("is_stale")),
        }

    def acquire_edit_lock(
        self,
        folder: str,
        version: str,
        *,
        holder_email: str,
        holder_name: str = "",
        holder_session: str = "",
        force: bool = False,
        ttl_seconds: int = 0,
    ) -> Dict[str, Any]:
        """Atomically take the (domain, version) edit lock when available.

        The lock is granted when it is free, already held by the **same**
        ``holder_email`` (refresh), ``force`` (admin take-over), or its lease
        has gone **stale** — ``ttl_seconds > 0`` and the current holder has
        not renewed (``heartbeat_at``) within the TTL. A live lock held by
        another user whose lease is still fresh is *never* reclaimed. With
        ``ttl_seconds == 0`` the lease is disabled and the lock is held until
        an explicit release / take-over (the pre-lease behaviour).

        On a successful grant ``heartbeat_at`` is bumped to ``now()``;
        ``acquired_at`` is reset only when the holder actually changes (a
        same-holder refresh keeps the original session start).

        Returns ``{acquired, is_self, holder_email, holder_name,
        acquired_at}`` describing the *live* lock after the attempt.
        ``acquired`` is ``True`` only when the caller now holds it.
        """
        if not self._ensure_domain_edit_locks_table():
            return {"acquired": False, "is_self": False, "holder_email": ""}
        try:
            psycopg, dict_row = _require_psycopg()
            sch = self._q(self._schema)
            ttl = max(0, int(ttl_seconds or 0))
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    INSERT INTO {sch}.domain_edit_locks
                        (domain_id, version, holder_email, holder_name,
                         holder_session)
                    SELECT d.id, %s, %s, %s, %s
                    FROM {sch}.domains d
                    WHERE d.registry_id = %s AND d.folder = %s
                    ON CONFLICT (domain_id, version) DO UPDATE SET
                        holder_email   = EXCLUDED.holder_email,
                        holder_name    = EXCLUDED.holder_name,
                        holder_session = EXCLUDED.holder_session,
                        acquired_at    = CASE
                            WHEN {sch}.domain_edit_locks.holder_email
                                     = EXCLUDED.holder_email
                            THEN {sch}.domain_edit_locks.acquired_at
                            ELSE now()
                        END,
                        heartbeat_at   = now()
                    WHERE {sch}.domain_edit_locks.holder_email
                              = EXCLUDED.holder_email
                       OR %s
                       OR (%s > 0 AND now() - {sch}.domain_edit_locks.heartbeat_at
                                     > make_interval(secs => %s))
                    RETURNING holder_email
                    """,
                    (
                        version,
                        holder_email or "",
                        holder_name or "",
                        holder_session or "",
                        self._registry(),
                        folder,
                        bool(force),
                        ttl,
                        ttl,
                    ),
                )
                cur.fetchone()  # row present only when the upsert won
                live = self._get_edit_lock_row(
                    cur, sch, folder, version, ttl_seconds=ttl
                )
            if not live:
                return {"acquired": False, "is_self": False, "holder_email": ""}
            d = self._edit_lock_row_to_dict(live)
            is_self = (d["holder_email"] or "").lower() == (
                holder_email or ""
            ).lower()
            return {
                "acquired": is_self,
                "is_self": is_self,
                "holder_email": d["holder_email"],
                "holder_name": d["holder_name"],
                "acquired_at": d["acquired_at"],
            }
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "acquire_edit_lock(%s/%s) failed: %s", folder, version, exc
            )
            return {"acquired": False, "is_self": False, "holder_email": ""}

    def _delete_edit_lock(
        self,
        cur,
        sch: str,
        folder: str,
        version: str,
        *,
        holder_email: str | None = None,
    ) -> int:
        """Delete the ``(folder, version)`` lock row; return affected rowcount.

        Holder-scoped (case-insensitive) when *holder_email* is provided;
        unconditional when ``None`` (admin force-release). Shared execution
        core for :meth:`release_edit_lock` / :meth:`force_release_edit_lock`.
        """
        where = (
            "l.domain_id = d.id AND d.registry_id = %s "
            "AND d.folder = %s AND l.version = %s"
        )
        params: list = [self._registry(), folder, version]
        if holder_email is not None:
            where += " AND lower(l.holder_email) = lower(%s)"
            params.append(holder_email or "")
        cur.execute(
            f"DELETE FROM {sch}.domain_edit_locks l USING {sch}.domains d "
            f"WHERE {where}",
            tuple(params),
        )
        return cur.rowcount

    def release_edit_lock(
        self, folder: str, version: str, *, holder_email: str
    ) -> bool:
        """Release the lock iff the caller holds it. Idempotent no-op else."""
        if not self._ensure_domain_edit_locks_table():
            return False
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                return (
                    self._delete_edit_lock(
                        cur, sch, folder, version, holder_email=holder_email
                    )
                    > 0
                )
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "release_edit_lock(%s/%s) failed: %s", folder, version, exc
            )
            return False

    def force_release_edit_lock(self, folder: str, version: str) -> bool:
        """Unconditionally drop the lock for (domain, version) — admin only."""
        if not self._ensure_domain_edit_locks_table():
            return False
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                return self._delete_edit_lock(cur, sch, folder, version) > 0
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "force_release_edit_lock(%s/%s) failed: %s",
                folder,
                version,
                exc,
            )
            return False

    def renew_edit_lock(
        self, folder: str, version: str, *, holder_email: str
    ) -> bool:
        """Bump ``heartbeat_at`` iff the caller still holds the lock.

        This is the lease keep-alive (called periodically by the holder's
        browser). Returns ``False`` when the caller no longer holds the lock
        — because someone reclaimed a stale lease or it was released/taken
        over — which the client reads as "your editing session expired".
        """
        if not self._ensure_domain_edit_locks_table():
            return False
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {sch}.domain_edit_locks l
                    SET heartbeat_at = now()
                    FROM {sch}.domains d
                    WHERE l.domain_id = d.id
                      AND d.registry_id = %s AND d.folder = %s
                      AND l.version = %s
                      AND lower(l.holder_email) = lower(%s)
                    """,
                    (self._registry(), folder, version, holder_email or ""),
                )
                return cur.rowcount > 0
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "renew_edit_lock(%s/%s) failed: %s", folder, version, exc
            )
            return False

    def get_edit_lock(
        self, folder: str, version: str, ttl_seconds: int = 0
    ) -> Optional[Dict[str, Any]]:
        """Return the live lock row or ``None`` when the lock is free.

        When ``ttl_seconds > 0`` the returned dict carries ``is_stale`` — the
        lease has lapsed (no renew within the TTL) and the lock is reclaimable
        — so callers (e.g. the permission gate) can ignore an abandoned lock.
        """
        if not self._ensure_domain_edit_locks_table():
            return None
        try:
            psycopg, dict_row = _require_psycopg()
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                row = self._get_edit_lock_row(
                    cur, sch, folder, version, ttl_seconds=ttl_seconds
                )
            return self._edit_lock_row_to_dict(row) if row else None
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "get_edit_lock(%s/%s) failed: %s", folder, version, exc
            )
            return None

    def _get_edit_lock_row(
        self,
        cur: Any,
        sch: str,
        folder: str,
        version: str,
        ttl_seconds: int = 0,
    ) -> Optional[Dict[str, Any]]:
        """Fetch the live lock row for (folder, version) via an open cursor.

        ``is_stale`` is computed server-side: true only when
        ``ttl_seconds > 0`` and the lease has lapsed past the TTL.
        """
        ttl = max(0, int(ttl_seconds or 0))
        cur.execute(
            f"""
            SELECT l.holder_email, l.holder_name, l.holder_session,
                   l.acquired_at, l.heartbeat_at,
                   (%s > 0 AND now() - l.heartbeat_at
                             > make_interval(secs => %s)) AS is_stale
            FROM {sch}.domain_edit_locks l
            JOIN {sch}.domains d ON d.id = l.domain_id
            WHERE d.registry_id = %s AND d.folder = %s AND l.version = %s
            """,
            (ttl, ttl, self._registry(), folder, version),
        )
        return cur.fetchone()

    def list_all_edit_locks(self, ttl_seconds: int = 0) -> List[Dict[str, Any]]:
        """List every active edit lock across the registry (admin overview).

        Joins ``domains`` for the folder and ``domain_versions`` for the
        current lifecycle status, newest lock first. When ``ttl_seconds > 0``
        each row carries ``is_stale`` so the admin Locks panel can flag an
        abandoned lease that will auto-reclaim. Returns ``[]`` when the lock
        backend is unavailable so the admin UI degrades to "no locks".
        """
        if not self._ensure_domain_edit_locks_table():
            return []
        try:
            psycopg, dict_row = _require_psycopg()
            sch = self._q(self._schema)
            ttl = max(0, int(ttl_seconds or 0))
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT d.folder, l.version,
                           l.holder_email, l.holder_name, l.holder_session,
                           l.acquired_at, l.heartbeat_at, v.status,
                           (%s > 0 AND now() - l.heartbeat_at
                                     > make_interval(secs => %s)) AS is_stale
                    FROM {sch}.domain_edit_locks l
                    JOIN {sch}.domains d ON d.id = l.domain_id
                    LEFT JOIN {sch}.domain_versions v
                           ON v.domain_id = l.domain_id AND v.version = l.version
                    WHERE d.registry_id = %s
                    ORDER BY l.acquired_at DESC
                    """,
                    (ttl, ttl, self._registry()),
                )
                rows = cur.fetchall()
            return [
                {
                    "folder": r.get("folder") or "",
                    "version": r.get("version") or "",
                    "status": (r.get("status") or "DRAFT"),
                    **self._edit_lock_row_to_dict(r),
                }
                for r in rows
            ]
        except Exception as exc:  # noqa: BLE001
            logger.debug("list_all_edit_locks failed: %s", exc)
            return []
