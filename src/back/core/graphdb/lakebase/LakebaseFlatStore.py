"""Lakebase Postgres flat triple store (single subject/predicate/object table per graph).

Two operating modes are supported:

* ``app_managed`` (default) -- the app owns one writable PG table per graph
  version. All inserts / deletes go through the FastAPI process via psycopg
  (small ``executemany`` payloads) or ``COPY FROM STDIN`` (bulk).

* ``managed_synced`` -- the bulk R2RML data movement is delegated to a
  Lakeflow synced-table pipeline (Databricks data plane only). The PG layout
  becomes a triad: a read-only ``_sync`` table owned by Lakeflow, a writable
  ``__app`` companion that absorbs reasoning + cohort writes, and a UNION
  view (named after the legacy single-table) that readers query. Direct
  writes through this class always target the companion; reads always go
  through the union view, so callers stay unchanged.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Tuple

from back.core.errors import InfrastructureError
from back.core.graphdb.lakebase import _companion_ddl
from back.core.graphdb.lakebase.LakebaseBase import LakebaseBase
from back.core.graphdb.lakebase.SyncedTableManager import (
    DEFAULT_TIMEOUT_S as _SYNC_DEFAULT_TIMEOUT_S,
)
from back.core.helpers import validate_table_name
from back.core.logging import get_logger

logger = get_logger(__name__)

_BULK_INSERT_THRESHOLD = 50
_BULK_DELETE_THRESHOLD = 50

_COPY_INSERT_TEMP = "_ob_copy_stage"
_COPY_DELETE_TEMP = "_ob_del_stage"

SYNC_MODE_APP = "app_managed"
SYNC_MODE_MANAGED = "managed_synced"


class LakebaseFlatStore(LakebaseBase):
    """Flat-model triple store on Lakebase Postgres.

    In ``app_managed`` mode, one physical table per logical graph name lives
    under the configured Postgres *schema* and the app drives every write.

    In ``managed_synced`` mode, that same logical name resolves to a UNION
    view fronting the Lakeflow-managed ``_sync`` table and the app-owned
    ``__app`` companion. Direct writes go to the companion; the synced side
    is populated by the Databricks data plane via
    :class:`SyncedTableManager`.

    ``search_path`` is set on every pooled connection so generated SQL from
    :class:`TripleStoreBackend` helpers resolves correctly.
    """

    def __init__(
        self,
        auth: Any,
        schema: str,
        database_override: str = "",
        *,
        sync_mode: str = SYNC_MODE_APP,
        sync_table_mode: str = "snapshot",
        sync_timeout_s: int = _SYNC_DEFAULT_TIMEOUT_S,
        sync_uc_catalog: str = "",
        sync_uc_schema: str = "",
        synced_manager: Optional[Any] = None,
    ) -> None:
        super().__init__(auth, schema, database_override)
        self._sync_mode = (
            sync_mode if sync_mode in (SYNC_MODE_APP, SYNC_MODE_MANAGED) else SYNC_MODE_APP
        )
        self._sync_table_mode = sync_table_mode or "snapshot"
        self._sync_timeout_s = int(sync_timeout_s) if sync_timeout_s else _SYNC_DEFAULT_TIMEOUT_S
        self._sync_uc_catalog = sync_uc_catalog or ""
        # UC schema segment for the synced-table FQN. Normally set to the
        # registry Volume schema so the Lakeflow object lives in the same UC
        # namespace as registry artefacts. Falls back to the Postgres graph
        # schema when the registry is unreachable.
        self._sync_uc_schema = sync_uc_schema or ""
        self._synced_manager = synced_manager

    # -- Mode introspection ------------------------------------------------

    @property
    def sync_mode(self) -> str:
        return self._sync_mode

    @property
    def is_synced(self) -> bool:
        return self._sync_mode == SYNC_MODE_MANAGED

    @property
    def sync_table_mode(self) -> str:
        return self._sync_table_mode

    @property
    def sync_timeout_s(self) -> int:
        return self._sync_timeout_s

    @property
    def sync_uc_catalog(self) -> str:
        return self._sync_uc_catalog

    def synced_manager(self) -> Any:
        if self._synced_manager is None:
            raise InfrastructureError(
                "managed_synced mode active but no SyncedTableManager was wired — "
                "check GraphDBFactory configuration"
            )
        return self._synced_manager

    # -- Table-name resolution --------------------------------------------

    def _writable_table_id(self, name: str) -> str:
        """Return the Postgres table that direct app writes target.

        Both ``app_managed`` and ``managed_synced`` use the companion table
        (``*__app``) for reasoning and cohort writes so that bulk warehouse
        data in ``*_sync`` is never modified by the app post-build.
        """
        return _companion_ddl.companion_phy(name)

    def _readable_table_id(self, name: str) -> str:
        """Return the table / view that direct reads should query."""
        # Both modes resolve to the same identifier — in synced mode it is the
        # union view name (which equals the legacy table name).
        return self.physical_table_id(name)

    def synced_table_name(self, table_name: str) -> str:
        """Return the ``_sync`` table name so callers can query without materialised triples."""
        return _companion_ddl.synced_phy(table_name)

    def synced_phy(self, name: str) -> str:
        """Postgres table name for the read-only synced side (managed_synced only)."""
        return _companion_ddl.synced_phy(name)

    def companion_phy(self, name: str) -> str:
        """Postgres table name for the writable companion (managed_synced only)."""
        return _companion_ddl.companion_phy(name)

    def synced_uc_name(self, name: str, fallback_catalog: str = "") -> str:
        """Build the UC fully-qualified name for the synced table.

        ``fallback_catalog`` is consulted when ``sync_uc_catalog`` is not set
        in ``engine_config``. Prefer passing the result of
        :func:`resolve_sync_uc_fallback_catalog` so the default UC catalog follows
        Settings → Registry rather than only ``domain.delta``.
        """
        catalog = (self._sync_uc_catalog or fallback_catalog or "").strip()
        if not catalog:
            raise InfrastructureError(
                "Cannot build synced UC name: no catalog configured "
                "(set graph_engine_config.sync_uc_catalog or pass a Delta catalog)"
            )
        uc_schema = (self._sync_uc_schema or self._schema).strip()
        return f"{catalog}.{uc_schema}.{self.synced_phy(name)}"

    # -- Companion + union view DDL --------------------------------------

    def ensure_synced_companion(self, name: str) -> None:
        """Create Postgres schema + writable companion before the ``_sync`` table exists.

        Lakeflow creates the read-only ``_sync`` table only after the first snapshot
        progresses; the union view references both sides and must run **after** sync.
        """
        if not self.is_synced:
            return
        companion = self.companion_phy(name)
        with self._cursor() as cur:
            _companion_ddl.ensure_companion(cur, self._schema, companion)

    def ensure_synced_union_view(
        self,
        name: str,
        *,
        wait_s: int = 0,
        poll_interval_s: float = 5.0,
        synced_phy_override: str = "",
    ) -> None:
        """Create the union view once the ``_sync`` table exists (after Lakeflow snapshot).

        The ``_sync`` table is placed by Lakebase in the Postgres schema that
        corresponds to the UC schema segment of the synced-table FQN
        (``_sync_uc_schema``). When that differs from the Postgres graph schema
        (e.g. registry schema ``ontobricks`` vs graph schema ``ontobricks_graph``),
        the ``_sync`` reference is schema-qualified so the DDL is independent of
        the active ``search_path``.

        After the Lakeflow pipeline reaches ONLINE there can be a short lag before
        the Postgres ``_sync`` table becomes visible.  This method polls for the
        table's existence up to *wait_s* seconds before giving up with a clear
        error (rather than silently proceeding and hitting a Postgres
        "relation does not exist" at view-creation time).

        *wait_s* defaults to the ``ONTOBRICKS_SYNC_VIEW_WAIT_S`` env var
        (default 300s).  ``poll_interval_s`` can be overridden via
        ``ONTOBRICKS_SYNC_VIEW_POLL_S`` (default 5s).

        *synced_phy_override* lets callers supply the actual Postgres table name
        when ``SyncedTableManager.ensure()`` used a ghost-state fallback suffix
        (e.g. ``cust360auto_v4_sync_b`` instead of ``cust360auto_v4_sync``).
        """
        import os
        import time as _time

        if not self.is_synced:
            return

        if wait_s == 0:
            wait_s = int(os.environ.get("ONTOBRICKS_SYNC_VIEW_WAIT_S", "") or 300)
        if poll_interval_s == 5.0:
            poll_interval_s = float(
                os.environ.get("ONTOBRICKS_SYNC_VIEW_POLL_S", "") or 5.0
            )

        companion = self.companion_phy(name)
        synced_bare = synced_phy_override or self.synced_phy(name)
        view = self._readable_table_id(name)
        sync_pg_schema = (self._sync_uc_schema or self._schema).strip()
        synced = (
            f'"{sync_pg_schema}".{synced_bare}'
            if sync_pg_schema != self._schema
            else synced_bare
        )

        # Poll until the _sync table is visible in Postgres (propagation lag after ONLINE).
        deadline = _time.time() + max(1, int(wait_s))
        attempt = 0
        while True:
            attempt += 1
            with self._cursor() as cur:
                exists = self._sync_table_exists(cur, sync_pg_schema, synced_bare)
            if exists:
                if attempt > 1:
                    logger.info(
                        "ensure_synced_union_view: _sync table %r visible "
                        "after %d attempt(s)",
                        synced_bare,
                        attempt,
                    )
                break
            remaining = deadline - _time.time()
            if remaining <= 0:
                raise RuntimeError(
                    f"_sync table {synced_bare!r} not found in Postgres schema "
                    f"{sync_pg_schema!r} after {wait_s}s. "
                    f"The Lakeflow pipeline reported ONLINE but Lakebase has not "
                    f"yet materialised the Postgres table. "
                    f"Set ONTOBRICKS_SYNC_VIEW_WAIT_S to a higher value if this "
                    f"workspace consistently needs more time."
                )
            logger.info(
                "ensure_synced_union_view: _sync table %r not yet visible "
                "(attempt %d, %.0fs remaining) — retrying in %.1fs",
                synced_bare,
                attempt,
                remaining,
                min(poll_interval_s, remaining),
            )
            _time.sleep(min(poll_interval_s, remaining))

        with self._cursor() as cur:
            _companion_ddl.ensure_union_view(cur, view, synced, companion)

    @staticmethod
    def _sync_table_exists(cur: Any, schema: str, table: str) -> bool:
        """Return True if *schema.table* is visible to the current Postgres session."""
        cur.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s LIMIT 1",
            (schema, table),
        )
        return cur.fetchone() is not None

    def ensure_synced_layout(self, name: str) -> None:
        """Create the writable companion and the union view if they do not exist.

        For builds, prefer :meth:`ensure_synced_companion` before Lakeflow sync and
        :meth:`ensure_synced_union_view` after — the ``_sync`` table does not exist until the
        snapshot pipeline materializes it.
        """
        self.ensure_synced_companion(name)
        self.ensure_synced_union_view(name)

    def truncate_companion(self, name: str) -> None:
        """Truncate the companion table (used on full rebuild in synced mode)."""
        if not self.is_synced:
            return
        companion = self.companion_phy(name)
        with self._cursor() as cur:
            _companion_ddl.truncate_companion(cur, companion)

    def _idx_name(self, phy: str, suffix: str) -> str:
        base = f"g_{phy}_{suffix}".lower()
        return base[:63]

    @staticmethod
    def _literal_meta(t: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
        """Normalize optional RDF literal metadata for storage (NULL when absent)."""
        dt = t.get("datatype")
        lang = t.get("lang")
        if dt is not None and not isinstance(dt, str):
            dt = str(dt)
        if lang is not None and not isinstance(lang, str):
            lang = str(lang)
        dt_val = (dt or "").strip() or None
        lang_val = (lang or "").strip() or None
        return dt_val, lang_val

    @staticmethod
    def _row_to_triple(row: Dict[str, Any]) -> Dict[str, str]:
        out: Dict[str, str] = {
            "subject": row["subject"] or "",
            "predicate": row["predicate"] or "",
            "object": row["object"] or "",
        }
        dt = row.get("datatype")
        lang = row.get("lang")
        if dt:
            out["datatype"] = str(dt)
        if lang:
            out["lang"] = str(lang)
        return out

    def _ensure_legacy_columns(self, cur: Any, phy: str) -> None:
        """Add ``datatype`` / ``lang`` when upgrading tables created before those columns."""
        cur.execute(
            f"ALTER TABLE {phy} ADD COLUMN IF NOT EXISTS datatype TEXT"
        )
        cur.execute(f"ALTER TABLE {phy} ADD COLUMN IF NOT EXISTS lang TEXT")

    @staticmethod
    def _require_pg():
        from back.core.graphdb.lakebase.pool import _require_psycopg

        return _require_psycopg()

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        _, dict_row = self._require_pg()
        pool = self._pool()
        with pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(f'SET search_path TO "{self._schema}", public')
                cur.execute(query)
                if cur.description:
                    return [dict(row) for row in cur.fetchall()]
                return []

    def create_table(self, table_name: str) -> None:
        validate_table_name(table_name)
        if self.is_synced:
            # In managed_synced mode the *_sync table is provisioned by Lakebase/
            # Lakeflow via SyncedTableManager. The companion and union view are
            # created by ensure_synced_companion / ensure_synced_union_view at
            # the appropriate points in _apply_via_synced_pipeline.
            logger.debug(
                "create_table %s is a no-op in managed_synced mode "
                "(provisioning deferred to SyncedTableManager)",
                table_name,
            )
            return
        # app_managed: create the full 3-object layout so reasoning / materialise
        # can write to the companion while bulk warehouse data lives in *_sync.
        synced = _companion_ddl.synced_phy(table_name)
        companion = _companion_ddl.companion_phy(table_name)
        view = self._readable_table_id(table_name)
        with self._cursor() as cur:
            _companion_ddl.ensure_synced(cur, self._schema, synced)
            _companion_ddl.ensure_companion(cur, self._schema, companion)
            _companion_ddl.ensure_union_view(cur, view, synced, companion)
        logger.info(
            "Lakebase graph layout ready: %s.[%s | %s | view %s]",
            self._schema,
            synced,
            companion,
            view,
        )

    def drop_table(self, table_name: str) -> None:
        validate_table_name(table_name)
        if self.is_synced:
            view = self._readable_table_id(table_name)
            companion = self.companion_phy(table_name)
            with self._cursor() as cur:
                _companion_ddl.drop_view(cur, view)
                _companion_ddl.drop_companion(cur, companion)
            try:
                mgr = self.synced_manager()
                synced_uc = self.synced_uc_name(table_name)
                mgr.delete(synced_uc, purge_data=True)
            except InfrastructureError as exc:
                # No catalog configured / SDK unavailable -- best-effort cleanup
                # so callers can still drop the PG side without aborting.
                logger.warning(
                    "drop_table %s: synced-table delete skipped (%s)",
                    table_name,
                    exc,
                )
            logger.info(
                "Dropped Lakebase synced trio for %s.%s", self._schema, table_name
            )
            return
        view = self._readable_table_id(table_name)
        companion = _companion_ddl.companion_phy(table_name)
        synced = _companion_ddl.synced_phy(table_name)
        with self._cursor() as cur:
            _companion_ddl.drop_view(cur, view)
            _companion_ddl.drop_companion(cur, companion)
            _companion_ddl.drop_synced(cur, synced)
        logger.info(
            "Dropped Lakebase graph layout %s.[%s | %s | view %s]",
            self._schema,
            synced,
            companion,
            view,
        )

    @contextmanager
    def _txn_cursor(self) -> Iterator[Tuple[Any, Any]]:
        """Yield ``(conn, cur)`` inside an explicit transaction.

        The pool runs connections with ``autocommit=True`` so that read paths
        and small DDL/DML stay round-trip-cheap. Bulk paths that rely on
        ``ON COMMIT DROP`` temp tables (COPY-based insert / delete) need an
        explicit transaction boundary to keep the staging table alive across
        the COPY → INSERT/DELETE steps and ensure it is released when the
        block exits.
        """
        from back.core.graphdb.lakebase.pool import _require_psycopg

        _, dict_row = _require_psycopg()
        pool = self._pool()
        with pool.connection() as conn:
            with conn.transaction():
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(f'SET search_path TO "{self._schema}", public')
                    yield conn, cur

    def _copy_insert_batch_phy(self, phy: str, batch: List[Dict[str, str]]) -> int:
        """COPY *batch* into a temp staging table then ``INSERT … ON CONFLICT DO NOTHING``.

        Takes the resolved physical Postgres table name directly so callers can
        target either the writable companion (``*__app``) or the bulk-data sync
        table (``*_sync``) without going through ``_writable_table_id``.
        """
        if not batch:
            return 0
        with self._txn_cursor() as (_, cur):
            cur.execute(
                f"CREATE TEMP TABLE {_COPY_INSERT_TEMP} ("
                "subject TEXT, predicate TEXT, object TEXT, "
                "datatype TEXT, lang TEXT) ON COMMIT DROP"
            )
            copy_sql = (
                f"COPY {_COPY_INSERT_TEMP} "
                "(subject, predicate, object, datatype, lang) FROM STDIN"
            )
            with cur.copy(copy_sql) as cp:
                for t in batch:
                    dt, lg = self._literal_meta(t)
                    cp.write_row(
                        (
                            (t.get("subject", "") or ""),
                            (t.get("predicate", "") or ""),
                            (t.get("object", "") or ""),
                            dt,
                            lg,
                        )
                    )
            cur.execute(
                f"INSERT INTO {phy} (subject, predicate, object, datatype, lang) "
                f"SELECT subject, predicate, object, datatype, lang "
                f"FROM {_COPY_INSERT_TEMP} ON CONFLICT DO NOTHING"
            )
        return len(batch)

    def _copy_insert_batch(
        self, table_name: str, batch: List[Dict[str, str]]
    ) -> int:
        """Route a COPY batch to the writable companion table for *table_name*."""
        if not batch:
            return 0
        validate_table_name(table_name)
        return self._copy_insert_batch_phy(self._writable_table_id(table_name), batch)

    def _copy_delete_batch(
        self, table_name: str, batch: List[Dict[str, str]]
    ) -> int:
        """COPY *batch* into a temp staging table then ``DELETE … USING`` join.

        Replaces the per-row ``DELETE`` loop on the incremental remove path:
        the join executes server-side in Postgres so the app does not pay a
        round-trip per triple to be removed. In ``managed_synced`` mode the
        target is the writable companion (the synced side is immutable).
        """
        if not batch:
            return 0
        validate_table_name(table_name)
        phy = self._writable_table_id(table_name)
        with self._txn_cursor() as (_, cur):
            cur.execute(
                f"CREATE TEMP TABLE {_COPY_DELETE_TEMP} ("
                "subject TEXT, predicate TEXT, object TEXT) ON COMMIT DROP"
            )
            copy_sql = (
                f"COPY {_COPY_DELETE_TEMP} "
                "(subject, predicate, object) FROM STDIN"
            )
            with cur.copy(copy_sql) as cp:
                for t in batch:
                    cp.write_row(
                        (
                            (t.get("subject", "") or ""),
                            (t.get("predicate", "") or ""),
                            (t.get("object", "") or ""),
                        )
                    )
            cur.execute(
                f"DELETE FROM {phy} USING {_COPY_DELETE_TEMP} d "
                f"WHERE {phy}.subject = d.subject "
                f"AND {phy}.predicate = d.predicate "
                f"AND {phy}.object = d.object"
            )
            return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    def _insert_triples_executemany(
        self,
        table_name: str,
        triples: List[Dict[str, str]],
        batch_size: int = 2000,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """Insert rows with ``executemany`` (small-payload fallback, < ``_BULK_INSERT_THRESHOLD``)."""
        validate_table_name(table_name)
        if not triples:
            return 0
        phy = self._writable_table_id(table_name)
        sql = (
            f"INSERT INTO {phy} (subject, predicate, object, datatype, lang) "
            f"VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"
        )
        total = 0
        with self._cursor() as cur:
            for i in range(0, len(triples), batch_size):
                batch = triples[i : i + batch_size]
                rows = []
                for t in batch:
                    dt, lg = self._literal_meta(t)
                    rows.append(
                        (
                            (t.get("subject", "") or ""),
                            (t.get("predicate", "") or ""),
                            (t.get("object", "") or ""),
                            dt,
                            lg,
                        )
                    )
                cur.executemany(sql, rows)
                total += len(batch)
                if on_progress:
                    on_progress(total, len(triples))
        logger.info("Inserted %d triple rows into %s.%s", total, self._schema, phy)
        return total

    def insert_triples(
        self,
        table_name: str,
        triples: List[Dict[str, str]],
        batch_size: int = 2000,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        validate_table_name(table_name)
        if not triples:
            return 0
        if len(triples) >= _BULK_INSERT_THRESHOLD:
            return self.bulk_insert_iter(
                table_name,
                iter(triples),
                batch_size=batch_size,
                on_progress=on_progress,
            )
        return self._insert_triples_executemany(
            table_name, triples, batch_size=batch_size, on_progress=on_progress
        )

    def bulk_insert_iter(
        self,
        table_name: str,
        triple_iterator: Iterable[Dict[str, str]],
        batch_size: int = 2000,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """Insert triples from an iterator in fixed-size batches via ``COPY FROM STDIN``.

        Bounded memory: only one ``batch_size`` window is held in RAM at any
        time. Each batch is its own transaction (COPY into ``_ob_copy_stage``
        then ``INSERT … ON CONFLICT DO NOTHING``) so progress callbacks fire
        per-batch and a single bad batch does not abort the entire load.
        """
        validate_table_name(table_name)
        batch: List[Dict[str, str]] = []
        total = 0
        for t in triple_iterator:
            batch.append(t)
            if len(batch) >= batch_size:
                total += self._copy_insert_batch(table_name, batch)
                if on_progress:
                    on_progress(total, total)
                batch = []
        if batch:
            total += self._copy_insert_batch(table_name, batch)
            if on_progress:
                on_progress(total, total)
        if total:
            logger.info(
                "COPY-inserted %d triple rows into %s.%s",
                total,
                self._schema,
                self.physical_table_id(table_name),
            )
        return total

    def bulk_load_into_sync(
        self,
        table_name: str,
        triple_iterator: Iterable[Dict[str, str]],
        batch_size: int = 5000,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """Bulk-load warehouse data into the ``*_sync`` table for *table_name*.

        Used by the ``app_managed`` full-rebuild path where the app streams
        triples from the Delta warehouse view directly into the sync table,
        mirroring what Lakeflow does automatically in ``managed_synced`` mode.

        Writes target ``synced_phy(table_name)`` (``*_sync``) so that
        post-build app writes (reasoning / materialise) continue to use the
        companion (``*__app``) via :meth:`_writable_table_id` and are not
        mixed with the warehouse snapshot.
        """
        validate_table_name(table_name)
        sync_phy = _companion_ddl.synced_phy(table_name)
        batch: List[Dict[str, str]] = []
        total = 0
        for t in triple_iterator:
            batch.append(t)
            if len(batch) >= batch_size:
                total += self._copy_insert_batch_phy(sync_phy, batch)
                if on_progress:
                    on_progress(total, total)
                batch = []
        if batch:
            total += self._copy_insert_batch_phy(sync_phy, batch)
            if on_progress:
                on_progress(total, total)
        if total:
            logger.info(
                "COPY-loaded %d triple rows into %s.%s (sync table)",
                total,
                self._schema,
                sync_phy,
            )
        return total

    def bulk_delete_iter(
        self,
        table_name: str,
        triple_iterator: Iterable[Dict[str, str]],
        batch_size: int = 2000,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """Delete triples from an iterator in fixed-size batches via temp-table JOIN.

        Mirror of :meth:`bulk_insert_iter` for the incremental remove path.
        """
        validate_table_name(table_name)
        batch: List[Dict[str, str]] = []
        deleted = 0
        for t in triple_iterator:
            batch.append(t)
            if len(batch) >= batch_size:
                deleted += self._copy_delete_batch(table_name, batch)
                if on_progress:
                    on_progress(deleted, deleted)
                batch = []
        if batch:
            deleted += self._copy_delete_batch(table_name, batch)
            if on_progress:
                on_progress(deleted, deleted)
        if deleted:
            logger.info(
                "Bulk-deleted %d triple rows from %s.%s",
                deleted,
                self._schema,
                self.physical_table_id(table_name),
            )
        return deleted

    def delete_triples(
        self,
        table_name: str,
        triples: List[Dict[str, str]],
        batch_size: int = 2000,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        validate_table_name(table_name)
        if not triples:
            return 0
        if len(triples) >= _BULK_DELETE_THRESHOLD:
            return self.bulk_delete_iter(
                table_name,
                iter(triples),
                batch_size=batch_size,
                on_progress=on_progress,
            )
        phy = self._writable_table_id(table_name)
        sql = f"DELETE FROM {phy} WHERE subject = %s AND predicate = %s AND object = %s"
        deleted = 0
        with self._cursor() as cur:
            for t in triples:
                cur.execute(
                    sql,
                    (
                        (t.get("subject", "") or ""),
                        (t.get("predicate", "") or ""),
                        (t.get("object", "") or ""),
                    ),
                )
                deleted += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
            if on_progress:
                on_progress(len(triples), len(triples))
        logger.info("Deleted %d triple rows from %s.%s", deleted, self._schema, phy)
        return deleted

    def query_triples(self, table_name: str) -> List[Dict[str, str]]:
        validate_table_name(table_name)
        phy = self._readable_table_id(table_name)
        with self._cursor() as cur:
            cur.execute(
                f"SELECT subject, predicate, object, datatype, lang FROM {phy} "
                f"ORDER BY subject, predicate"
            )
            rows = cur.fetchall()
        return [self._row_to_triple(r) for r in rows]

    def iter_triples(
        self,
        table_name: str,
        batch_size: int = 5000,
    ) -> Iterator[Dict[str, str]]:
        """Yield triple rows in sort order without loading the full graph into memory."""
        validate_table_name(table_name)
        phy = self._readable_table_id(table_name)
        offset = 0
        while True:
            with self._cursor() as cur:
                cur.execute(
                    f"SELECT subject, predicate, object, datatype, lang FROM {phy} "
                    f"ORDER BY subject, predicate "
                    f"LIMIT %s OFFSET %s",
                    (batch_size, offset),
                )
                rows = cur.fetchall()
            if not rows:
                break
            for r in rows:
                yield self._row_to_triple(r)
            offset += len(rows)
            if len(rows) < batch_size:
                break

    def count_triples(self, table_name: str) -> int:
        validate_table_name(table_name)
        phy = self._readable_table_id(table_name)
        with self._cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS cnt FROM {phy}")
            row = cur.fetchone()
        return int(row["cnt"]) if row else 0

    def table_exists(self, table_name: str) -> bool:
        if not table_name or not table_name.strip():
            return False
        phy = self._readable_table_id(table_name)
        with self._cursor() as cur:
            # In synced mode the readable target is a VIEW; check both
            # ``information_schema.tables`` and ``information_schema.views``
            # so the existence probe works in either mode.
            cur.execute(
                """
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = current_schema() AND table_name = %s
                UNION ALL
                SELECT 1 FROM information_schema.views
                WHERE table_schema = current_schema() AND table_name = %s
                LIMIT 1
                """,
                (phy, phy),
            )
            return cur.fetchone() is not None

    def get_status(self, table_name: str) -> Dict[str, Any]:
        validate_table_name(table_name)
        count = self.count_triples(table_name)
        return {
            "count": count,
            "last_modified": None,
            "path": None,
            "format": "lakebase",
            "schema": self._schema,
            "database": self._effective_database_display(),
            "sync_mode": self._sync_mode,
        }

    def _effective_database_display(self) -> str:
        if self._database_override:
            return self._database_override
        try:
            return str(self._auth.database)
        except Exception:  # noqa: BLE001
            return ""

    def optimize_table(self, table_name: str) -> None:
        validate_table_name(table_name)
        # managed_synced: vacuum the companion only (*_sync is Lakeflow-managed).
        # app_managed: vacuum *_sync (just bulk-loaded) and the companion.
        companion = self.companion_phy(table_name)
        with self._cursor() as cur:
            if not self.is_synced:
                synced = self.synced_phy(table_name)
                cur.execute(f"VACUUM ANALYZE {synced}")
            cur.execute(f"VACUUM ANALYZE {companion}")


def resolve_lakebase_graph_schema(
    domain: Any,
    settings: Optional[Any],
    config_schema: str,
) -> str:
    """Postgres / UC schema segment for Lakebase triple tables.

    When **Settings → Registry** resolves to a non-empty Unity Catalog **Volume**
    schema (``RegistryCfg.schema``, the middle part of ``catalog.schema.volume``),
    that value **always** wins over ``graph_engine_config.schema``. Managed-synced
    tables must register in UC as ``catalog.schema.table`` where ``schema`` matches
    the Lakebase Postgres schema; aligning with the registry Volume keeps graph
    triples and synced metadata under the same UC namespace as artefacts.

    Falls back to *config_schema* (validated) when the registry triplet has no
    schema or resolution fails.
    """
    from back.core.graphdb.lakebase.LakebaseBase import (
        DEFAULT_GRAPH_SCHEMA,
        validate_graph_schema,
    )

    raw = (config_schema or "").strip()
    if raw:
        # Explicit schema configured in graph_engine_config — always honour it.
        return validate_graph_schema(raw)

    # No explicit schema: derive from the Registry Volume middle segment so
    # managed-sync UC names stay aligned with the registry UC namespace.
    try:
        from back.objects.registry import RegistryCfg

        rc = RegistryCfg.from_domain(domain, settings)
        reg_schema = (rc.schema or "").strip()
        if reg_schema:
            try:
                validated = validate_graph_schema(reg_schema)
            except ValueError as exc:
                logger.warning(
                    "Registry Volume schema %r is not a valid Lakebase identifier (%s) — "
                    "falling back to default graph schema",
                    reg_schema,
                    exc,
                )
            else:
                logger.info(
                    "Lakebase graph schema=%s from Registry Volume triplet "
                    "(graph_engine_config.schema was not set)",
                    validated,
                )
                return validated
    except Exception as exc:  # noqa: BLE001
        logger.debug(
            "resolve_lakebase_graph_schema: registry unavailable: %s",
            exc,
        )
    return validate_graph_schema(DEFAULT_GRAPH_SCHEMA)


def resolve_sync_uc_fallback_catalog(
    domain: Any,
    settings: Optional[Any],
    delta_cfg: Optional[Dict[str, Any]] = None,
) -> str:
    """Unity Catalog name for synced-table registration when ``sync_uc_catalog`` is unset.

    Resolution order:

    1. Environment variable ``ONTOBRICKS_SYNC_UC_CATALOG`` — optional deployment-
       wide pin so every domain falls back to the same UC catalog (e.g. a shared
       ``main`` catalog) when the JSON config leaves ``sync_uc_catalog`` empty.
       The legacy spelling ``ONTBRICKS_SYNC_UC_CATALOG`` (without the ``O``) is
       still honoured for backwards compatibility but is deprecated.
    2. ``RegistryCfg.from_domain`` — the catalog from **Settings → Registry**
       (the Volume / registry UC triplet). This matches where operators expect
       managed assets to live.
    3. ``domain.delta["catalog"]`` — per-domain Delta triple-store catalog
       (often a personal or workspace-default catalog).

    ``LakebaseFlatStore.synced_uc_name`` still prefers ``graph_engine_config.sync_uc_catalog``
    when set; this helper supplies *fallback_catalog* only.
    """
    import os

    pin = os.getenv("ONTOBRICKS_SYNC_UC_CATALOG", "").strip()
    if not pin:
        # Backwards-compat with the original (typoed) env var name. Warn so
        # operators migrate away from it.
        legacy = os.getenv("ONTBRICKS_SYNC_UC_CATALOG", "").strip()
        if legacy:
            logger.warning(
                "Using deprecated env var ONTBRICKS_SYNC_UC_CATALOG=%r — "
                "rename to ONTOBRICKS_SYNC_UC_CATALOG (added 'O').",
                legacy,
            )
            pin = legacy
    if pin:
        logger.info(
            "resolve_sync_uc_fallback_catalog: using ONTOBRICKS_SYNC_UC_CATALOG=%r",
            pin,
        )
        return pin

    try:
        from back.objects.registry import RegistryCfg

        rc = RegistryCfg.from_domain(domain, settings)
        cat = (rc.catalog or "").strip()
        if cat:
            logger.info(
                "resolve_sync_uc_fallback_catalog: using registry catalog %r "
                "(no ONTOBRICKS_SYNC_UC_CATALOG set)",
                cat,
            )
            return cat
    except Exception as exc:  # noqa: BLE001
        logger.debug(
            "resolve_sync_uc_fallback_catalog: registry catalog unavailable: %s",
            exc,
        )
    dc = delta_cfg or {}
    fallback = str(dc.get("catalog") or "").strip()
    if fallback:
        logger.info(
            "resolve_sync_uc_fallback_catalog: using domain.delta catalog %r "
            "(no env var or registry catalog available)",
            fallback,
        )
    return fallback


