"""Lakebase user-override operations (add / hide / partition delete / stats).

Mixed into :class:`FlatStore`, which supplies the connection plumbing
(``_cursor``, ``_txn_cursor``), the table-name resolution and the bulk-insert
paths. Kept out of ``FlatStore`` so that class stays within its size budget.

The overrides table follows ``back.core.graphdb.overrides``: one row per triple
and operation, ``add`` rows for added / derived triples, ``del`` rows for hidden
ones. Every edit runs in one Postgres transaction, so an interrupted call leaves
no half-applied change.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from back.core.graphdb.inferred_breakdown import legacy_origin_expr
from back.core.graphdb.lakebase import _companion_ddl
from back.core.graphdb.overrides import (
    OP_ADD,
    OP_DEL,
    ORIGIN_COHORT,
    ORIGIN_COMMUNITY,
    ORIGIN_USER,
    unique_triples,
    validate_hide_origin,
)
from back.core.helpers import validate_table_name
from back.core.logging import get_logger

logger = get_logger(__name__)

_COPY_INSERT_TEMP = "_ob_copy_stage"


def _triple_match(left: str, right: str) -> str:
    """SQL predicate matching two aliases on ``(subject, predicate, object)``."""
    return (
        f"{left}.subject = {right}.subject AND {left}.predicate = {right}.predicate "
        f"AND {left}.object = {right}.object"
    )


class OverrideOpsMixin:
    """Overrides-table lifecycle and user edits for the Lakebase flat store."""

    def synced_table_name(self, table_name: str) -> str:
        """Relation holding the asserted graph (mapped data plus user overrides).

        The ``…_asserted`` view when it exists; the bare ``_sync`` table before the
        overrides layer is set up.
        """
        asserted = _companion_ddl.asserted_phy(table_name)
        try:
            with self._cursor() as cur:
                if _companion_ddl.relation_exists(cur, asserted):
                    return asserted
        except Exception as exc:  # noqa: BLE001
            logger.debug("asserted view probe failed for %s: %s", table_name, exc)
        return _companion_ddl.synced_phy(table_name)

    def get_inferred_triple_count(self, table_name: str) -> int:
        """Number of derived ``add`` rows (reasoning, cohorts, communities).

        Queries the overrides table directly so callers can tell whether any
        derived data exists independently of the union view total. Returns 0 on any
        error (e.g. the table is not created yet).
        """
        companion = _companion_ddl.companion_phy(table_name)
        try:
            with self._cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*) AS cnt FROM {companion} "
                    f"WHERE op = '{OP_ADD}' AND origin <> '{ORIGIN_USER}'"
                )
                row = cur.fetchone()
            return int(row["cnt"]) if row else 0
        except Exception:  # noqa: BLE001
            return 0

    def _ensure_companion_table(self, cur: Any, name: str) -> None:
        """Create ``__overrides``; seed a new one from the legacy ``__app`` table."""
        _companion_ddl.ensure_companion(
            cur,
            self._schema,
            self.companion_phy(name),
            legacy=_companion_ddl.legacy_companion_phy(name),
            origin_expr=legacy_origin_expr(self._base_uri(), self._sql_escape),
        )

    def _synced_ref(self, synced_bare: str) -> str:
        """``_sync`` reference, schema-qualified when it lives outside the graph schema."""
        sync_pg_schema = (self._sync_uc_schema or self._schema).strip()
        if sync_pg_schema != self._schema:
            return f'"{sync_pg_schema}".{synced_bare}'
        return synced_bare

    def _ensure_override_layout(self, name: str) -> None:
        """Make sure the overrides table and the views exist before a write.

        Cheap after the first call per store. It also upgrades a graph built
        before the overrides layer: its union view is re-pointed at ``__overrides``
        without waiting for the next Build.
        """
        if name in self._override_ready:
            return
        synced_bare = self.synced_phy(name)
        with self._cursor() as cur:
            self._ensure_companion_table(cur, name)
            synced = self._synced_ref(synced_bare)
            if _companion_ddl.relation_exists(cur, synced):
                _companion_ddl.ensure_union_view(
                    cur,
                    self._readable_table_id(name),
                    synced,
                    self.companion_phy(name),
                    _companion_ddl.asserted_phy(name),
                )
            else:
                logger.warning(
                    "Lakebase overrides table ready for %s but %s does not exist yet: "
                    "the union view is not re-pointed until the next Build",
                    name,
                    synced,
                )
        self._override_ready.add(name)

    def truncate_companion(self, name: str) -> None:
        """Clear the derived rows on a full rebuild in synced mode (user rows stay)."""
        if not self.is_synced:
            return
        companion = self.companion_phy(name)
        with self._cursor() as cur:
            _companion_ddl.clear_derived(cur, companion)

    def reset_for_rebuild(self, table_name: str) -> None:
        """Recreate the bulk layout for a full Build; user overrides survive.

        The ``__overrides`` table is kept (only its derived rows are cleared);
        the sync table and the views are dropped and recreated.
        """
        validate_table_name(table_name)
        self.drop_table(table_name, keep_overrides=True)
        self.create_table(table_name)
        companion = self.companion_phy(table_name)
        with self._cursor() as cur:
            if _companion_ddl.relation_exists(cur, companion):
                _companion_ddl.clear_derived(cur, companion)
        self._override_ready.discard(table_name)

    def purge_materialized_triples(self, table_name: str) -> int:
        """Delete the derived rows; mapped source triples and user rows stay."""
        companion = self.companion_phy(table_name)
        count = self.get_inferred_triple_count(table_name)
        with self._cursor() as cur:
            _companion_ddl.clear_derived(cur, companion)
        from back.core.graphdb.search_cache import rebuild_graph_cache_if_enabled

        rebuild_graph_cache_if_enabled(
            self, table_name, getattr(self, "_domain", None)
        )
        logger.info("Purged %d materialized triples from %s", count, companion)
        return count

    def _copy_insert_override_rows(
        self,
        phy: str,
        batch: List[Dict[str, str]],
        origin: str,
        *,
        op: str = OP_ADD,
        author: str = "",
        change_id: str = "",
    ) -> int:
        """COPY *batch* into a staging table, then insert it as override rows.

        Same shape as :meth:`_copy_insert_batch_phy` for an ``__overrides`` table:
        every row carries *op*, *origin* and the optional audit columns.
        """
        if not batch:
            return 0
        try:
            with self._txn_cursor() as (_, cur):
                self._stage_triples(cur, batch)
                cur.execute(
                    f"INSERT INTO {phy} (subject, predicate, object, datatype, lang, "
                    "op, origin, change_id, author) "
                    "SELECT subject, predicate, object, datatype, lang, "
                    f"%s, %s, %s, %s FROM {_COPY_INSERT_TEMP} ON CONFLICT DO NOTHING",
                    (op, origin, change_id or None, author or None),
                )
        except Exception as exc:
            from back.core.graphdb.lakebase.FlatStore import (
                _reraise_lakebase_index_limit,
            )

            _reraise_lakebase_index_limit(exc, batch)
        return len(batch)

    def _stage_triples(self, cur: Any, batch: List[Dict[str, str]]) -> None:
        """Create the ``ON COMMIT DROP`` staging table and COPY *batch* into it."""
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

    def add_triples(
        self,
        table_name: str,
        triples: List[Dict[str, str]],
        batch_size: int = 2000,
        on_progress: Optional[Callable[[int, int], None]] = None,
        *,
        author: str = "",
        change_id: str = "",
    ) -> int:
        """Add triples to the graph as user overrides.

        Un-hides a hidden triple (drops its ``del`` row) and inserts a user ``add``
        row only for a triple absent from the effective graph, all in one
        transaction. Returns the number of triples handled.
        """
        validate_table_name(table_name)
        if not triples:
            return 0
        self._ensure_override_layout(table_name)
        overrides = self.companion_phy(table_name)
        graph = self._readable_table_id(table_name)
        restore_sql = (
            f"DELETE FROM {overrides} d USING {_COPY_INSERT_TEMP} t "
            f"WHERE d.op = '{OP_DEL}' AND d.subject = t.subject "
            "AND d.predicate = t.predicate AND d.object = t.object"
        )
        add_sql = (
            f"INSERT INTO {overrides} (subject, predicate, object, datatype, lang, "
            "op, origin, change_id, author) "
            f"SELECT t.subject, t.predicate, t.object, t.datatype, t.lang, "
            f"'{OP_ADD}', '{ORIGIN_USER}', %s, %s FROM {_COPY_INSERT_TEMP} t "
            f"WHERE NOT EXISTS (SELECT 1 FROM {graph} g WHERE g.subject = t.subject "
            "AND g.predicate = t.predicate AND g.object = t.object) "
            "ON CONFLICT DO NOTHING"
        )

        def statements(cur: Any) -> None:
            cur.execute(restore_sql)
            cur.execute(add_sql, (change_id or None, author or None))

        return self._run_override_batches(triples, batch_size, on_progress, statements)

    def _run_override_batches(
        self,
        triples: List[Dict[str, str]],
        batch_size: int,
        on_progress: Optional[Callable[[int, int], None]],
        statements: Callable[[Any], None],
    ) -> int:
        """Stage each batch of unique triples and run *statements* in one transaction."""
        unique = unique_triples(triples)
        handled = 0
        for i in range(0, len(unique), batch_size):
            batch = unique[i : i + batch_size]
            with self._txn_cursor() as (_, cur):
                self._stage_triples(cur, batch)
                statements(cur)
            handled += len(batch)
            if on_progress:
                on_progress(handled, len(unique))
        if handled:
            self._mark_cache_stale()
        return handled

    def delete_cohort_triples(
        self,
        table_name: str,
        cohort_uri_prefix: str,
        in_cohort_predicate: str,
    ) -> int:
        """Delete the cohort rows of the overrides table (idempotent)."""
        return self._delete_partition(
            table_name, ORIGIN_COHORT, cohort_uri_prefix, in_cohort_predicate
        )

    def delete_community_triples(
        self,
        table_name: str,
        cluster_uri_prefix: str,
        in_cluster_predicate: str,
    ) -> int:
        """Delete the community rows of the overrides table (idempotent)."""
        return self._delete_partition(
            table_name, ORIGIN_COMMUNITY, cluster_uri_prefix, in_cluster_predicate
        )

    def _delete_partition(
        self, table_name: str, origin: str, prefix: str, member_predicate: str
    ) -> int:
        """Delete one cohort / community partition (subject prefix + membership rows)."""
        if not prefix or not self.supports_overrides:
            return 0
        overrides = self.companion_phy(table_name)
        width = len(prefix)
        try:
            with self._cursor() as cur:
                cur.execute(
                    f"DELETE FROM {overrides} WHERE origin = %s AND "
                    f"(substr(subject, 1, {width}) = %s "
                    f"OR (predicate = %s AND substr(object, 1, {width}) = %s))",
                    (origin, prefix, member_predicate, prefix),
                )
                return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "delete %s triples failed on %s (%s): %s",
                origin,
                table_name,
                prefix,
                exc,
            )
            return 0

    def get_override_stats(self, table_name: str) -> Dict[str, int]:
        """User overrides at a glance: ``user_adds``, ``user_dels``, ``dormant_dels``.

        A *dormant* ``del`` hides a triple that no longer exists in the mapped data
        nor in a derived row (the source changed). It is harmless, and worth showing.
        """
        overrides = self.companion_phy(table_name)
        synced = self._synced_ref(self.synced_phy(table_name))
        counts = self.execute_query(
            "SELECT "
            f"COUNT(*) FILTER (WHERE op = '{OP_ADD}' AND origin = '{ORIGIN_USER}') "
            "AS user_adds, "
            f"COUNT(*) FILTER (WHERE op = '{OP_DEL}') AS user_dels "
            f"FROM {overrides}"
        )
        dormant = self.execute_query(
            f"SELECT COUNT(*) AS cnt FROM {overrides} o WHERE o.op = '{OP_DEL}' "
            f"AND NOT EXISTS (SELECT 1 FROM {synced} s WHERE s.subject = o.subject "
            "AND s.predicate = o.predicate AND s.object = o.object) "
            f"AND NOT EXISTS (SELECT 1 FROM {overrides} a WHERE a.op = '{OP_ADD}' "
            "AND a.subject = o.subject AND a.predicate = o.predicate "
            "AND a.object = o.object)"
        )
        row = counts[0] if counts else {}
        return {
            "user_adds": int(row.get("user_adds") or 0),
            "user_dels": int(row.get("user_dels") or 0),
            "dormant_dels": int((dormant[0] if dormant else {}).get("cnt") or 0),
        }

    def _hide_triples(
        self,
        table_name: str,
        triples: List[Dict[str, str]],
        batch_size: int,
        on_progress: Optional[Callable[[int, int], None]],
        author: str,
        change_id: str,
        origin: str = ORIGIN_USER,
    ) -> int:
        """Hide *triples* (``del`` rows) in one transaction.

        ``origin='user'`` also drops the user ``add`` rows of the triples. With
        ``origin='reasoning'`` (removal rules) a triple that has a user ``add`` row is
        skipped and no user row is dropped (D2).
        """
        validate_hide_origin(origin)
        self._ensure_override_layout(table_name)
        overrides = self.companion_phy(table_name)
        synced = self._synced_ref(self.synced_phy(table_name))
        key = _triple_match
        user_add_guard = (
            ""
            if origin == ORIGIN_USER
            else (
                f"AND NOT EXISTS (SELECT 1 FROM {overrides} u "
                f"WHERE u.op = '{OP_ADD}' AND u.origin = '{ORIGIN_USER}' "
                f"AND {key('u', 't')}) "
            )
        )
        hide_sql = (
            f"INSERT INTO {overrides} (subject, predicate, object, op, origin, "
            "change_id, author) "
            f"SELECT t.subject, t.predicate, t.object, '{OP_DEL}', '{origin}', "
            f"%s, %s FROM {_COPY_INSERT_TEMP} t "
            f"WHERE (EXISTS (SELECT 1 FROM {synced} s WHERE {key('s', 't')}) "
            f"OR EXISTS (SELECT 1 FROM {overrides} a WHERE a.op = '{OP_ADD}' "
            f"AND a.origin <> '{ORIGIN_USER}' AND {key('a', 't')})) "
            f"{user_add_guard}"
            "ON CONFLICT DO NOTHING"
        )
        drop_sql = (
            f"DELETE FROM {overrides} a USING {_COPY_INSERT_TEMP} t "
            f"WHERE a.op = '{OP_ADD}' AND a.origin = '{ORIGIN_USER}' "
            f"AND {key('a', 't')}"
        )

        def statements(cur: Any) -> None:
            cur.execute(hide_sql, (change_id or None, author or None))
            if origin == ORIGIN_USER:
                cur.execute(drop_sql)

        return self._run_override_batches(triples, batch_size, on_progress, statements)
