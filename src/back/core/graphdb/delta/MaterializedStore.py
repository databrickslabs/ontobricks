"""Materialized Delta (Unity Catalog) triple store on Databricks SQL Warehouse.

A :class:`MaterializedGraphBackend` that owns a physical copy of the triples: a
clustered Delta ``table`` snapshot in Unity Catalog, an inferred companion table
for app writes, a union graph VIEW, and the adjacency / entity-search / props
companions.  It therefore carries the full write + build surface (table
lifecycle, triple ingestion, companion rebuild, inference purge, OPTIMIZE).

The SQL **read** surface is shared with the no-copy
:class:`back.core.graphdb.delta.ViewStore.ViewStore` via
:class:`back.core.graphdb.delta.ReadStore.ReadStore`.
"""

from __future__ import annotations

import concurrent.futures
import time
from typing import Any, Callable, Dict, List, Optional

from back.core.errors import ValidationError
from back.core.graphdb.interfaces.MaterializedGraphBackend import MaterializedGraphBackend
from back.core.graphdb.delta import _table_naming, materialize
from back.core.graphdb.inferred_breakdown import (
    build_breakdown_sql,
    legacy_origin_expr,
    normalise_breakdown,
)
from back.core.graphdb.delta.ReadStore import ReadStore
from back.core.graphdb.overrides import (
    OP_ADD,
    OP_DEL,
    ORIGIN_COHORT,
    ORIGIN_COMMUNITY,
    ORIGIN_REASONING,
    ORIGIN_USER,
    unique_triples,
    validate_derived_origin,
)
from back.core.graphdb.props import forget_missing_props
from back.core.helpers import sql_escape as _escape_sql_string, validate_table_name
from back.core.logging import get_logger

logger = get_logger(__name__)


class MaterializedStore(ReadStore, MaterializedGraphBackend):
    """GraphDB backend that queries materialized Delta triple tables in UC.

    With a bound *domain* it routes app writes through an inferred companion
    table and a union graph VIEW.  Constructed with ``domain=None`` it behaves
    as a raw store against the exact table FQNs passed in — used for direct
    materialisation and build-time writes.
    """

    supports_materialized_inference_purge = True
    supports_inferred_breakdown = True
    supports_adjacency = True
    supports_entity_search = True
    supports_props = True
    supports_overrides = True

    def __init__(
        self, client: Any, domain: Any = None, settings: Any = None
    ) -> None:
        super().__init__(client, domain, settings)
        # Overrides tables whose companion objects were ensured by this instance:
        # a batch loop must not re-issue the DDL for every batch.
        self._companions_ready: set[str] = set()

    # ------------------------------------------------------------------
    # Companion rebuild (adjacency / entity-search / props)
    # ------------------------------------------------------------------

    def _try_optimize(self, table_fqn: str, kind: str) -> None:
        """Run OPTIMIZE best-effort; log a warning with *kind* context on failure."""
        try:
            materialize.optimize_table(self._client, table_fqn)
        except Exception as exc:  # noqa: BLE001
            logger.warning("OPTIMIZE %s failed for %s: %s", kind, table_fqn, exc)

    def _run_rebuild_job(self, name: str, rebuild: Callable[[], None]) -> None:
        started = time.perf_counter()
        rebuild()
        logger.info(
            "Graph-index companion rebuilt: %s in %.0f ms",
            name,
            (time.perf_counter() - started) * 1000,
        )

    def rebuild_adjacency(self, table_name: str) -> None:
        relation = self._sql_relation(table_name)
        adj_out, adj_in = self.adjacency_table_ids(table_name)
        search = self.entity_search_table_id(table_name)
        search_asserted = self.entity_search_asserted_table_id(table_name)
        props = self.props_table_id(table_name)
        if not adj_out or not adj_in or not search or not props:
            logger.warning("Skipping graph-index rebuild, unresolved table ids for %s", table_name)
            return
        asserted_spo = self.synced_table_name(table_name)
        jobs: Dict[str, Callable[[], None]] = {
            "adj_out": lambda: self._rebuild_adjacency_table(relation, adj_out, "out"),
            "adj_in": lambda: self._rebuild_adjacency_table(relation, adj_in, "in"),
            "entity_search": lambda: self._rebuild_entity_search_table(relation, search),
            "props": lambda: self._rebuild_props_table(relation, props),
        }
        if search_asserted and asserted_spo:
            jobs["entity_search_asserted"] = lambda: self._rebuild_entity_search_table(
                asserted_spo, search_asserted
            )
        started = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(5, len(jobs))) as executor:
            future_names = {
                executor.submit(self._run_rebuild_job, name, rebuild): name
                for name, rebuild in jobs.items()
            }
            for future in concurrent.futures.as_completed(future_names):
                try:
                    future.result()
                except Exception:
                    for pending in future_names:
                        pending.cancel()
                    logger.exception(
                        "Graph-index companion rebuild failed: %s", future_names[future]
                    )
                    raise
        logger.info(
            "Graph-index rebuild completed: %s companions in %.0f ms",
            len(jobs),
            (time.perf_counter() - started) * 1000,
        )

    def _rebuild_adjacency_table(
        self, relation: str, adj_fqn: str, direction: str
    ) -> None:
        materialize.drop_relation(self._client, adj_fqn, kind="view")
        self._client.execute_statement(
            materialize.build_adj_ctas_sql(relation, adj_fqn, direction)
        )
        self._try_optimize(adj_fqn, "adjacency table")

    def _rebuild_props_table(self, relation: str, props: str) -> None:
        materialize.drop_relation(self._client, props, kind="view")
        self._client.execute_statement(materialize.build_props_ctas_sql(relation, props))
        # CTAS succeeded; OPTIMIZE below is best-effort only.
        self._try_optimize(props, "property table")
        forget_missing_props(props)

    def _rebuild_entity_search_table(self, spo_fqn: str, search_fqn: str) -> None:
        materialize.drop_relation(self._client, search_fqn, kind="view")
        self._client.execute_statement(
            materialize.build_entity_search_ctas_sql(spo_fqn, search_fqn)
        )
        materialize.set_bloom_filter_columns(
            self._client, search_fqn, "label_lc,uri_lc"
        )
        self._try_optimize(search_fqn, "entity-search table")

    def _writable_table_fqn(self, table_name: str) -> str:
        """Route app writes to the ``…_overrides`` table (Lakebase ``__overrides`` analogue)."""
        if table_name.endswith(_table_naming.overrides_suffix()):
            return table_name
        if self._domain is not None:
            overrides = _table_naming.overrides_fqn(self._domain, self._settings)
            if overrides:
                return overrides
        if "." in table_name and table_name.count(".") == 2:
            cat, sch, base = table_name.split(".", 2)
            for suffix in (
                _table_naming.data_suffix(),
                _table_naming.graph_suffix(),
                _table_naming.asserted_suffix(),
                _table_naming.inferred_suffix(),
            ):
                if base.endswith(suffix):
                    base = base[: -len(suffix)]
                    break
            return f"{cat}.{sch}.{base}{_table_naming.overrides_suffix()}"
        raise ValueError(f"Cannot resolve overrides table for {table_name!r}")

    def _migrate_legacy_inferred(self, overrides: str) -> None:
        """Copy ``…_inferred`` into the overrides table that was just created."""
        if self._domain is None:
            return
        legacy = _table_naming.inferred_table_fqn(self._domain, self._settings)
        origin_expr = legacy_origin_expr(self._base_uri(), self._sql_escape)
        if materialize.migrate_inferred(self._client, overrides, legacy, origin_expr):
            logger.info("Legacy inferred rows migrated into %s", overrides)

    def _ensure_companion_objects(self, table_name: str) -> str:
        """Ensure the overrides TABLE and the graph / asserted VIEWs exist.

        A freshly created overrides table is seeded from the legacy ``…_inferred``
        table first, so no derived triple disappears when the views are replaced.
        """
        overrides = self._writable_table_fqn(table_name)
        if overrides in self._companions_ready:
            return overrides
        created = materialize.ensure_overrides_table(self._client, overrides)
        if created:
            self._migrate_legacy_inferred(overrides)
        if self._domain is not None:
            data = _table_naming.data_table_fqn(self._domain, self._settings)
            graph = _table_naming.graph_view_fqn(self._domain, self._settings)
            asserted = _table_naming.asserted_view_fqn(self._domain, self._settings)
            if data and graph:
                materialize.ensure_graph_view(self._client, graph, data, overrides)
            if data and asserted:
                materialize.ensure_asserted_view(
                    self._client, asserted, data, overrides
                )
        self._companions_ready.add(overrides)
        return overrides

    # ------------------------------------------------------------------
    # Raw SQL CRUD against Databricks Delta tables (operate on the exact
    # FQN passed in — no logical-name resolution).
    # ------------------------------------------------------------------

    def create_table(self, table_name: str) -> None:
        """Create the (subject, predicate, object) Delta table with Liquid Clustering."""
        validate_table_name(table_name)
        query = (
            f"CREATE TABLE IF NOT EXISTS {table_name} "
            "(subject STRING, predicate STRING, object STRING) USING DELTA "
            "CLUSTER BY (predicate, subject)"
        )
        logger.info("Creating Delta table: %s", table_name)
        self._client.execute_statement(query)

    def drop_table(self, table_name: str) -> None:
        """Drop table if exists."""
        validate_table_name(table_name)
        query = f"DROP TABLE IF EXISTS {table_name}"
        logger.info("Dropping Delta table: %s", table_name)
        self._client.execute_statement(query)

    def _uses_inline_sql(self) -> bool:
        """True when the warehouse is Apps INLINE Statement Execution."""
        transport = getattr(self._client, "sql", self._client)
        return getattr(type(transport), "uses_inline_statements", False) is True

    @staticmethod
    def _insert_sql(
        table_name: str, batch: List[Dict[str, str]], origin: Optional[str] = None
    ) -> str:
        """``INSERT`` for *batch*.

        With *origin* the target is an ``…_overrides`` table and every row is
        written as a derived ``add`` row. Without it the target is a plain
        three-column triple table (sandbox, raw materialisation).
        """
        if origin is None:
            values_sql = ",\n".join(
                "('{s}', '{p}', '{o}')".format(
                    s=_escape_sql_string(t.get("subject", "") or ""),
                    p=_escape_sql_string(t.get("predicate", "") or ""),
                    o=_escape_sql_string(t.get("object", "") or ""),
                )
                for t in batch
            )
            return (
                f"INSERT INTO {table_name} (subject, predicate, object) VALUES\n"
                f"{values_sql}"
            )
        values_sql = ",\n".join(
            "('{s}', '{p}', '{o}', '{op}', '{origin}', NULL, NULL, current_timestamp())".format(
                s=_escape_sql_string(t.get("subject", "") or ""),
                p=_escape_sql_string(t.get("predicate", "") or ""),
                o=_escape_sql_string(t.get("object", "") or ""),
                op=OP_ADD,
                origin=origin,
            )
            for t in batch
        )
        return (
            f"INSERT INTO {table_name} ({materialize.OVERRIDE_COLUMNS}) VALUES\n"
            f"{values_sql}"
        )

    def _execute_insert_triples(
        self,
        table_name: str,
        triples: List[Dict[str, str]],
        batch_size: int,
        on_progress: Optional[Callable[[int, int], None]],
        origin: Optional[str] = None,
    ) -> int:
        """Batch insert triples into *table_name* using one persistent connection."""
        validate_table_name(table_name)
        if not triples:
            return 0

        if self._uses_inline_sql():
            return self._execute_insert_triples_inline(
                table_name, triples, batch_size, on_progress, origin
            )

        from databricks import sql

        total = 0
        conn_params = self._client.get_sql_connection_params()
        with sql.connect(**conn_params) as connection:
            with connection.cursor() as cursor:
                for i in range(0, len(triples), batch_size):
                    batch = triples[i : i + batch_size]
                    cursor.execute(self._insert_sql(table_name, batch, origin))
                    total += len(batch)
                    if on_progress:
                        on_progress(total, len(triples))
                    logger.debug(
                        "Inserted batch %d-%d of %d",
                        i + 1,
                        i + len(batch),
                        len(triples),
                    )

        logger.info("Inserted %d triples into %s", total, table_name)
        return total

    def _execute_insert_triples_inline(
        self,
        table_name: str,
        triples: List[Dict[str, str]],
        batch_size: int,
        on_progress: Optional[Callable[[int, int], None]],
        origin: Optional[str] = None,
    ) -> int:
        """INSERT via Statement Execution (no Thrift session in Databricks Apps)."""
        total = 0
        for i in range(0, len(triples), batch_size):
            batch = triples[i : i + batch_size]
            self._client.execute_statement(self._insert_sql(table_name, batch, origin))
            total += len(batch)
            if on_progress:
                on_progress(total, len(triples))
        logger.info(
            "Inserted %d triples into %s via Statement Execution", total, table_name
        )
        return total

    def insert_triples(
        self,
        table_name: str,
        triples: List[Dict[str, str]],
        batch_size: int = 2000,
        on_progress: Optional[Callable[[int, int], None]] = None,
        origin: str = ORIGIN_REASONING,
    ) -> int:
        """Insert derived triples as ``add`` rows of the overrides table.

        *origin* says which feature derived them (``reasoning``, ``cohort`` or
        ``community``); Build and purge remove exactly these rows. User edits go
        through :meth:`add_triples` instead. In raw mode (``domain is None``)
        writes go directly to the three-column *table_name*.
        """
        if self._domain is None:
            return self._execute_insert_triples(
                table_name, triples, batch_size, on_progress
            )
        validate_derived_origin(origin)
        target = self._ensure_companion_objects(table_name)
        return self._execute_insert_triples(
            target, triples, batch_size, on_progress, origin
        )

    def insert_triples_from_select(
        self, table_name: str, select_sql: str, origin: str = ORIGIN_REASONING
    ) -> None:
        """``INSERT INTO <overrides> SELECT …`` on the warehouse.

        *select_sql* must project ``subject, predicate, object``. Used for
        bulk writes whose rows already live in Delta (community partitions),
        so no row transits the app.
        """
        if self._domain is None:
            validate_table_name(table_name)
            self._client.execute_statement(
                f"INSERT INTO {table_name} (subject, predicate, object)\n{select_sql}"
            )
            return
        validate_derived_origin(origin)
        target = self._ensure_companion_objects(table_name)
        validate_table_name(target)
        self._client.execute_statement(
            f"INSERT INTO {target} ({materialize.OVERRIDE_COLUMNS})\n"
            f"SELECT subject, predicate, object, '{OP_ADD}', '{origin}', "
            "NULL, NULL, current_timestamp() "
            f"FROM ({select_sql}) AS derived"
        )

    # ------------------------------------------------------------------
    # User overrides: add / delete / modify triples over the mapped graph
    # ------------------------------------------------------------------

    def _run_batches(
        self,
        triples: List[Dict[str, str]],
        batch_size: int,
        on_progress: Optional[Callable[[int, int], None]],
        build_statements: Callable[[List[Dict[str, str]]], List[str]],
    ) -> int:
        """Run *build_statements* per batch of unique triples; return triples handled."""
        unique = unique_triples(triples)
        total = 0
        for i in range(0, len(unique), batch_size):
            batch = unique[i : i + batch_size]
            for statement in build_statements(batch):
                self._client.execute_statement(statement)
            total += len(batch)
            if on_progress:
                on_progress(total, len(unique))
        return total

    def add_triples(
        self,
        table_name: str,
        triples: List[Dict[str, str]],
        batch_size: int = 500,
        on_progress: Optional[Callable[[int, int], None]] = None,
        *,
        author: str = "",
        change_id: str = "",
    ) -> int:
        """Add triples to the graph as user overrides.

        Un-hides a hidden triple (drops its ``del`` row) and inserts an ``add`` row
        only for triples absent from the effective graph, so the table keeps its
        invariant (never an ``add`` and a ``del`` for the same triple). Returns the
        number of triples handled.
        """
        self._require_domain("add_triples")
        overrides = self._ensure_companion_objects(table_name)
        graph = _table_naming.graph_view_fqn(self._domain, self._settings)

        def statements(batch: List[Dict[str, str]]) -> List[str]:
            return [
                materialize.build_restore_sql(overrides, batch),
                materialize.build_add_user_sql(
                    overrides, graph, batch, author=author, change_id=change_id
                ),
            ]

        handled = self._run_batches(triples, batch_size, on_progress, statements)
        if handled:
            self._mark_cache_stale()
        return handled

    def delete_triples(
        self,
        table_name: str,
        triples: List[Dict[str, str]],
        batch_size: int = 500,
        on_progress: Optional[Callable[[int, int], None]] = None,
        *,
        author: str = "",
        change_id: str = "",
    ) -> int:
        """Hide triples from the graph.

        A triple that comes from the mapped data or from a derived row gets a
        ``del`` row; a user ``add`` row is simply dropped. The ``del`` row is written
        first, so an interrupted call leaves the triple hidden rather than visible.

        In raw mode (``domain is None``, a sandbox table) the rows are physically
        deleted from *table_name*.
        """
        if self._domain is None:
            return self._delete_raw_triples(
                table_name, triples, batch_size, on_progress
            )
        overrides = self._ensure_companion_objects(table_name)
        data = _table_naming.data_table_fqn(self._domain, self._settings)

        def statements(batch: List[Dict[str, str]]) -> List[str]:
            return [
                materialize.build_hide_sql(
                    overrides, data, batch, author=author, change_id=change_id
                ),
                materialize.build_drop_user_adds_sql(overrides, batch),
            ]

        handled = self._run_batches(triples, batch_size, on_progress, statements)
        if handled:
            self._mark_cache_stale()
        return handled

    def _delete_raw_triples(
        self,
        table_name: str,
        triples: List[Dict[str, str]],
        batch_size: int,
        on_progress: Optional[Callable[[int, int], None]],
    ) -> int:
        validate_table_name(table_name)
        return self._run_batches(
            triples,
            batch_size,
            on_progress,
            lambda batch: [
                f"MERGE INTO {table_name} t USING "
                f"(VALUES {materialize.build_values_sql(batch)}) "
                "AS v(subject, predicate, object) "
                "ON t.subject = v.subject AND t.predicate = v.predicate "
                "AND t.object = v.object WHEN MATCHED THEN DELETE"
            ],
        )

    def _require_domain(self, operation: str) -> None:
        if self._domain is None:
            raise ValidationError(
                f"{operation} needs a store bound to a domain "
                "(a raw store has no overrides table)"
            )

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
        if not prefix or self._domain is None:
            return 0
        overrides = self._writable_table_fqn(table_name)
        try:
            self._client.execute_statement(
                materialize.build_clear_origin_sql(
                    overrides, origin, prefix, member_predicate
                )
            )
            return -1
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "delete %s triples failed on %s (%s): %s",
                origin,
                table_name,
                prefix,
                exc,
            )
            return 0

    def optimize_inferred_companion(self, table_name: str) -> None:
        """Compact and re-cluster the overrides table after app writes."""
        try:
            overrides = self._writable_table_fqn(table_name)
            materialize.optimize_table(self._client, overrides)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "OPTIMIZE overrides table failed for %s: %s", table_name, exc
            )

    def purge_materialized_triples(self, table_name: str) -> int:
        """Delete the derived rows and preserve mapped source triples and user rows."""
        overrides = self._writable_table_fqn(table_name)
        try:
            count = self.get_inferred_triple_count(table_name, raise_errors=True)
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            if (
                "TABLE_OR_VIEW_NOT_FOUND" in msg
                or "does not exist" in msg.lower()
            ):
                logger.info(
                    "Purged 0 materialized triples; overrides table missing: %s",
                    overrides,
                )
                return 0
            raise
        materialize.clear_derived(self._client, overrides)
        from back.core.graphdb.search_cache import rebuild_graph_cache_if_enabled

        rebuild_graph_cache_if_enabled(self, table_name, self._domain)
        logger.info("Purged %d materialized triples from %s", count, overrides)
        return count

    def optimize_table(self, table_name: str) -> None:
        """Run OPTIMIZE to trigger Liquid Clustering compaction."""
        validate_table_name(table_name)
        logger.info("Optimizing Delta table: %s", table_name)
        self._client.execute_statement(f"OPTIMIZE {table_name}")

    def get_inferred_triple_count(
        self, table_name: str, *, raise_errors: bool = False
    ) -> int:
        """Number of derived ``add`` rows (reasoning, cohorts, communities)."""
        if self._domain is None:
            return 0
        try:
            overrides = self._writable_table_fqn(table_name)
            validate_table_name(overrides)
            rows = self._client.execute_query(
                f"SELECT COUNT(*) AS cnt FROM {overrides} "
                f"WHERE op = '{OP_ADD}' AND COALESCE(origin, '') <> '{ORIGIN_USER}'"
            )
            return int(rows[0].get("cnt", 0)) if rows else 0
        except Exception:  # noqa: BLE001
            if raise_errors:
                raise
            return 0

    def get_override_stats(self, table_name: str) -> Dict[str, int]:
        """User overrides at a glance: ``user_adds``, ``user_dels``, ``dormant_dels``.

        A *dormant* ``del`` hides a triple that no longer exists in the mapped data
        nor in a derived row (the source changed). It is harmless, and worth showing.
        """
        stats = {"user_adds": 0, "user_dels": 0, "dormant_dels": 0}
        if self._domain is None:
            return stats
        overrides = self._writable_table_fqn(table_name)
        data = _table_naming.data_table_fqn(self._domain, self._settings)
        validate_table_name(overrides)
        validate_table_name(data)
        counts = self.execute_query(
            "SELECT "
            f"COUNT(CASE WHEN op = '{OP_ADD}' AND origin = '{ORIGIN_USER}' "
            "THEN 1 END) AS user_adds, "
            f"COUNT(CASE WHEN op = '{OP_DEL}' THEN 1 END) AS user_dels "
            f"FROM {overrides}"
        )
        dormant = self.execute_query(
            f"SELECT COUNT(*) AS cnt FROM {overrides} o WHERE o.op = '{OP_DEL}' "
            f"AND NOT EXISTS (SELECT 1 FROM {data} d WHERE d.subject = o.subject "
            "AND d.predicate = o.predicate AND d.object = o.object) "
            f"AND NOT EXISTS (SELECT 1 FROM {overrides} a WHERE a.op = '{OP_ADD}' "
            "AND a.subject = o.subject AND a.predicate = o.predicate "
            "AND a.object = o.object)"
        )
        row = counts[0] if counts else {}
        stats["user_adds"] = int(row.get("user_adds") or 0)
        stats["user_dels"] = int(row.get("user_dels") or 0)
        stats["dormant_dels"] = int((dormant[0] if dormant else {}).get("cnt") or 0)
        return stats

    def get_inferred_breakdown(self, table_name: str, base_uri: str) -> List[Dict[str, Any]]:
        """Per-category triples / entities / groups over the derived override rows."""
        if self._domain is None:
            return normalise_breakdown([])
        overrides = self._writable_table_fqn(table_name)
        validate_table_name(overrides)
        # The overrides table is created by the first Build or edit. Until then
        # nothing is materialized, so report zero rows instead of failing.
        if not materialize.relation_exists(self._client, overrides):
            return normalise_breakdown([])
        sql = build_breakdown_sql(
            overrides, base_uri, self._sql_escape, has_origin=True
        )
        return normalise_breakdown(self.execute_query(sql))
