"""Knowledge Store document rows on the Lakebase registry.

Extracted from :class:`LakebaseRegistryStore` (Fowler Extract Class /
mixin). The store inherits this class so ``upsert_document`` and friends
stay on the public store type.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from back.core.logging import get_logger
from ..base import StoreError

# Keep log records on the store module name (tests / operators grep it).
logger = get_logger("back.objects.registry.store.lakebase.store")


def _require_psycopg():
    """Defer to the store module so tests can monkeypatch that name."""
    from back.objects.registry.store.lakebase.store import _require_psycopg as _rp

    return _rp()


class Documents:
    """Parse-only document corpus CRUD (no UC Volume)."""

    def _ensure_domain_documents_table(self) -> bool:
        """Lazily create ``domain_documents`` (+ index) if missing.

        Mirrors :meth:`_ensure_graph_analytics_table`: best-effort,
        idempotent, guarded by a per-instance flag so we only pay the
        round-trip once per store.
        """
        if self._domain_documents_ready:
            return True
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = %s AND table_name = 'domain_documents'",
                    (self._schema,),
                )
                if not cur.fetchone():
                    cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {sch}.domain_documents (
                            id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                            domain_id     uuid NOT NULL
                                          REFERENCES {sch}.domains(id)
                                          ON DELETE CASCADE,
                            version       text NOT NULL,
                            filename      text NOT NULL,
                            source_hash   text NOT NULL,
                            parser        text NOT NULL,
                            status        text NOT NULL DEFAULT 'pending'
                                          CHECK (status IN ('pending', 'ready', 'failed')),
                            parsed_text   text NOT NULL DEFAULT '',
                            source_bytes  bytea,
                            size_bytes    bigint NOT NULL DEFAULT 0,
                            output_schema text NOT NULL DEFAULT '',
                            error         text NOT NULL DEFAULT '',
                            parsed_at     timestamptz,
                            created_at    timestamptz NOT NULL DEFAULT now(),
                            updated_at    timestamptz NOT NULL DEFAULT now(),
                            UNIQUE (domain_id, version, filename)
                        )
                        """
                    )
                    cur.execute(
                        f"CREATE INDEX IF NOT EXISTS idx_domain_documents_lookup "
                        f"ON {sch}.domain_documents(domain_id, version)"
                    )
            self._domain_documents_ready = True
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "could not create domain_documents table — "
                "run `make bootstrap-lakebase` as the schema owner to apply "
                "the migration: %s",
                exc,
            )
            return False

    @staticmethod
    def _document_row(r: Dict[str, Any]) -> Dict[str, Any]:
        """Metadata-only projection returned by list/get (no text/bytes)."""
        return {
            "filename": r["filename"],
            "source_hash": r["source_hash"] or "",
            "parser": r["parser"] or "",
            "status": r["status"] or "pending",
            "size_bytes": int(r["size_bytes"] or 0),
            "output_schema": r["output_schema"] or "",
            "error": r["error"] or "",
            "parsed_at": (
                r["parsed_at"].isoformat() if r.get("parsed_at") else ""
            ),
        }

    def upsert_document(
        self,
        folder: str,
        version: str,
        *,
        filename: str,
        source_hash: str,
        parser: str,
        status: str,
        size_bytes: int,
        output_schema: str = "",
        source_bytes: Optional[bytes] = None,
        parsed_text: str = "",
        error: str = "",
    ) -> Tuple[bool, str]:
        """Insert or replace one document row for ``(folder, version, filename)``."""
        if not self._ensure_domain_documents_table():
            return False, "domain_documents table unavailable"
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {sch}.domain_documents
                        (domain_id, version, filename, source_hash, parser,
                         status, parsed_text, source_bytes, size_bytes,
                         output_schema, error, parsed_at, updated_at)
                    SELECT d.id, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                           CASE WHEN %s = 'ready' THEN now() ELSE NULL END, now()
                    FROM {sch}.domains d
                    WHERE d.registry_id = %s AND d.folder = %s
                    ON CONFLICT (domain_id, version, filename) DO UPDATE SET
                        source_hash   = EXCLUDED.source_hash,
                        parser        = EXCLUDED.parser,
                        status        = EXCLUDED.status,
                        parsed_text   = EXCLUDED.parsed_text,
                        source_bytes  = EXCLUDED.source_bytes,
                        size_bytes    = EXCLUDED.size_bytes,
                        output_schema = EXCLUDED.output_schema,
                        error         = EXCLUDED.error,
                        parsed_at     = EXCLUDED.parsed_at,
                        updated_at    = now()
                    """,
                    (
                        str(version),
                        str(filename),
                        str(source_hash),
                        str(parser),
                        str(status),
                        str(parsed_text or ""),
                        source_bytes,
                        int(size_bytes or 0),
                        str(output_schema or ""),
                        str(error or ""),
                        str(status),
                        self._registry(),
                        folder,
                    ),
                )
                if cur.rowcount == 0:
                    return False, f"No domain row matched folder {folder!r}"
            return True, ""
        except Exception as exc:  # noqa: BLE001
            logger.warning("upsert_document(%s/%s) failed: %s", folder, filename, exc)
            return False, str(exc)

    def list_documents(
        self, folder: str, version: str, *, strict: bool = False
    ) -> List[Dict[str, Any]]:
        """Metadata rows for ``(folder, version)`` — never text/bytes.

        ``strict`` distinguishes infrastructure failures from a genuinely
        empty result for destructive cleanup.  Read-only callers retain the
        historical tolerant behavior.
        """
        if not self._ensure_domain_documents_table():
            if strict:
                raise StoreError("Knowledge Store document table unavailable")
            return []
        try:
            _psycopg, dict_row = _require_psycopg()
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT dd.filename, dd.source_hash, dd.parser, dd.status,
                           dd.size_bytes, dd.output_schema, dd.error, dd.parsed_at
                    FROM {sch}.domain_documents dd
                    JOIN {sch}.domains d ON d.id = dd.domain_id
                    WHERE d.registry_id = %s AND d.folder = %s AND dd.version = %s
                    ORDER BY dd.filename
                    """,
                    (self._registry(), folder, str(version)),
                )
                return [self._document_row(r) for r in cur.fetchall()]
        except Exception as exc:  # noqa: BLE001
            if strict:
                raise StoreError(
                    f"Knowledge Store document listing failed: {exc}"
                ) from exc
            logger.debug("list_documents(%s/%s) failed: %s", folder, version, exc)
            return []

    def get_document(
        self, folder: str, version: str, filename: str
    ) -> Optional[Dict[str, Any]]:
        """Single metadata row, or ``None`` when absent."""
        if not self._ensure_domain_documents_table():
            return None
        try:
            _psycopg, dict_row = _require_psycopg()
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT dd.filename, dd.source_hash, dd.parser, dd.status,
                           dd.size_bytes, dd.output_schema, dd.error, dd.parsed_at
                    FROM {sch}.domain_documents dd
                    JOIN {sch}.domains d ON d.id = dd.domain_id
                    WHERE d.registry_id = %s AND d.folder = %s
                      AND dd.version = %s AND dd.filename = %s
                    """,
                    (self._registry(), folder, str(version), str(filename)),
                )
                row = cur.fetchone()
            return self._document_row(row) if row else None
        except Exception as exc:  # noqa: BLE001
            logger.debug("get_document(%s/%s) failed: %s", folder, filename, exc)
            return None

    def read_document_text(
        self, folder: str, version: str, filename: str
    ) -> Optional[Tuple[str, str, str]]:
        """Return ``(parsed_text, parser, status)`` or ``None`` when absent."""
        if not self._ensure_domain_documents_table():
            return None
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT dd.parsed_text, dd.parser, dd.status
                    FROM {sch}.domain_documents dd
                    JOIN {sch}.domains d ON d.id = dd.domain_id
                    WHERE d.registry_id = %s AND d.folder = %s
                      AND dd.version = %s AND dd.filename = %s
                    """,
                    (self._registry(), folder, str(version), str(filename)),
                )
                row = cur.fetchone()
            if not row:
                return None
            return (row[0] or "", row[1] or "", row[2] or "pending")
        except Exception as exc:  # noqa: BLE001
            logger.debug("read_document_text(%s/%s) failed: %s", folder, filename, exc)
            return None

    def read_document_bytes(
        self, folder: str, version: str, filename: str
    ) -> Optional[bytes]:
        """Return the transient original bytes, or ``None`` when absent/cleared."""
        if not self._ensure_domain_documents_table():
            return None
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT dd.source_bytes
                    FROM {sch}.domain_documents dd
                    JOIN {sch}.domains d ON d.id = dd.domain_id
                    WHERE d.registry_id = %s AND d.folder = %s
                      AND dd.version = %s AND dd.filename = %s
                    """,
                    (self._registry(), folder, str(version), str(filename)),
                )
                row = cur.fetchone()
            if not row or row[0] is None:
                return None
            return bytes(row[0])
        except Exception as exc:  # noqa: BLE001
            logger.debug("read_document_bytes(%s/%s) failed: %s", folder, filename, exc)
            return None

    def set_document_ready(
        self, folder: str, version: str, filename: str, *, parsed_text: str
    ) -> Tuple[bool, str]:
        """Mark ready, persist parsed text, and NULL the transient bytes."""
        return self._update_document_status(
            folder,
            version,
            filename,
            status="ready",
            parsed_text=parsed_text,
            error="",
            clear_bytes=True,
            set_parsed_at=True,
        )

    def set_document_failed(
        self, folder: str, version: str, filename: str, *, error: str
    ) -> Tuple[bool, str]:
        """Mark failed with a safe error; keep bytes so retry can run."""
        return self._update_document_status(
            folder,
            version,
            filename,
            status="failed",
            error=error,
            set_parsed_at=True,
        )

    def set_document_pending(
        self, folder: str, version: str, filename: str
    ) -> Tuple[bool, str]:
        """Return a failed/ready row to pending for a retry."""
        return self._update_document_status(
            folder,
            version,
            filename,
            status="pending",
            error="",
            set_parsed_at=False,
        )

    def _update_document_status(
        self,
        folder: str,
        version: str,
        filename: str,
        *,
        status: str,
        parsed_text: Optional[str] = None,
        error: Optional[str] = None,
        clear_bytes: bool = False,
        set_parsed_at: bool = False,
    ) -> Tuple[bool, str]:
        if not self._ensure_domain_documents_table():
            return False, "domain_documents table unavailable"
        sets = ["status = %s", "updated_at = now()"]
        params: List[Any] = [str(status)]
        if parsed_text is not None:
            sets.append("parsed_text = %s")
            params.append(str(parsed_text))
        if error is not None:
            sets.append("error = %s")
            params.append(str(error))
        if clear_bytes:
            sets.append("source_bytes = NULL")
        sets.append(
            "parsed_at = now()" if set_parsed_at else "parsed_at = parsed_at"
        )
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {sch}.domain_documents AS dd
                    SET {", ".join(sets)}
                    FROM {sch}.domains d
                    WHERE dd.domain_id = d.id
                      AND d.registry_id = %s AND d.folder = %s
                      AND dd.version = %s AND dd.filename = %s
                    """,
                    (*params, self._registry(), folder, str(version), str(filename)),
                )
                if cur.rowcount == 0:
                    return False, f"Document {filename!r} not found"
            return True, ""
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "_update_document_status(%s/%s) failed: %s", folder, filename, exc
            )
            return False, str(exc)

    def delete_documents(
        self, folder: str, version: str, filenames: List[str]
    ) -> List[str]:
        """Delete 1..N document rows. Returns a list of error messages."""
        if not filenames:
            return []
        if not self._ensure_domain_documents_table():
            return ["domain_documents table unavailable"]
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    DELETE FROM {sch}.domain_documents dd
                    USING {sch}.domains d
                    WHERE dd.domain_id = d.id
                      AND d.registry_id = %s AND d.folder = %s
                      AND dd.version = %s AND dd.filename = ANY(%s)
                    """,
                    (
                        self._registry(),
                        folder,
                        str(version),
                        [str(f) for f in filenames],
                    ),
                )
            return []
        except Exception as exc:  # noqa: BLE001
            logger.warning("delete_documents(%s) failed: %s", folder, exc)
            return [str(exc)]

    def copy_documents_to_version(
        self, folder: str, from_version: str, to_version: str
    ) -> Tuple[bool, str]:
        """Copy parsed docs forward to a new version (text, not bytes).

        Also carries what-if scenario specs and run history when the
        simulations mixin is present — new versions must not drop them.
        """
        def _carry_simulations() -> None:
            copy_sims = getattr(self, "copy_simulations_to_version", None)
            if callable(copy_sims):
                copy_sims(folder, from_version, to_version)

        if not self._ensure_domain_documents_table():
            _carry_simulations()
            return False, "domain_documents table unavailable"
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {sch}.domain_documents
                        (domain_id, version, filename, source_hash, parser,
                         status, parsed_text, source_bytes, size_bytes,
                         output_schema, error, parsed_at, updated_at)
                    SELECT dd.domain_id, %s, dd.filename, dd.source_hash,
                           dd.parser, dd.status, dd.parsed_text, NULL,
                           dd.size_bytes, dd.output_schema, dd.error,
                           dd.parsed_at, now()
                    FROM {sch}.domain_documents dd
                    JOIN {sch}.domains d ON d.id = dd.domain_id
                    WHERE d.registry_id = %s AND d.folder = %s
                      AND dd.version = %s
                    ON CONFLICT (domain_id, version, filename) DO NOTHING
                    """,
                    (
                        str(to_version),
                        self._registry(),
                        folder,
                        str(from_version),
                    ),
                )
            _carry_simulations()
            return True, ""
        except Exception as exc:  # noqa: BLE001
            logger.warning("copy_documents_to_version(%s) failed: %s", folder, exc)
            return False, str(exc)

    def count_documents(self, folder: str, version: str) -> int:
        """Number of document rows for ``(folder, version)``."""
        if not self._ensure_domain_documents_table():
            return 0
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT count(*)
                    FROM {sch}.domain_documents dd
                    JOIN {sch}.domains d ON d.id = dd.domain_id
                    WHERE d.registry_id = %s AND d.folder = %s AND dd.version = %s
                    """,
                    (self._registry(), folder, str(version)),
                )
                row = cur.fetchone()
            return int(row[0]) if row else 0
        except Exception as exc:  # noqa: BLE001
            logger.debug("count_documents(%s/%s) failed: %s", folder, version, exc)
            return 0
