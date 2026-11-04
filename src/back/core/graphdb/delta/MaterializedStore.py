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
from typing import Callable, Dict, List, Optional

from back.core.graphdb.delta import _table_naming, materialize
from back.core.graphdb.delta.GraphRelation import GraphRelation
from back.core.graphdb.delta.ReadStore import ReadStore
from back.core.graphdb.interfaces.MaterializedGraphBackend import MaterializedGraphBackend
from back.core.graphdb.props import forget_missing_props
from back.core.helpers import sql_escape as _escape_sql_string
from back.core.helpers import validate_table_name
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
    supports_adjacency = True
    supports_entity_search = True
    supports_props = True

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
        # The FQN as given (never the live slots): a rebuild follows a refreshed ``…_data``.
        relation = table_name if table_name.count(".") == 2 else self._sql_relation(table_name)
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
        """Route app writes to the inferred companion table (Lakebase ``__app`` analogue)."""
        if table_name.endswith(_table_naming.inferred_suffix()):
            return table_name
        if self._domain is not None:
            inferred = _table_naming.inferred_table_fqn(self._domain, self._settings)
            if inferred:
                return inferred
        if "." in table_name and table_name.count(".") == 2:
            cat, sch, base = table_name.split(".", 2)
            for suffix in (
                _table_naming.data_suffix(),
                _table_naming.graph_suffix(),
            ):
                if base.endswith(suffix):
                    base = base[: -len(suffix)]
                    break
            return f"{cat}.{sch}.{base}{_table_naming.inferred_suffix()}"
        raise ValueError(f"Cannot resolve inferred companion table for {table_name!r}")

    def _ensure_companion_objects(self, table_name: str) -> str:
        """Ensure inferred TABLE + union graph VIEW (a graph MV is kept) exist before app writes."""
        inferred = self._writable_table_fqn(table_name)
        materialize.ensure_inferred_table(self._client, inferred)
        if self._domain is not None:
            data = _table_naming.data_table_fqn(self._domain, self._settings)
            graph = _table_naming.graph_view_fqn(self._domain, self._settings)
            if data and graph:
                GraphRelation.ensure_view(self._client, graph, data, inferred)
        return inferred

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
    def _insert_sql(table_name: str, batch: List[Dict[str, str]]) -> str:
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

    def _execute_insert_triples(
        self,
        table_name: str,
        triples: List[Dict[str, str]],
        batch_size: int,
        on_progress: Optional[Callable[[int, int], None]],
    ) -> int:
        """Batch insert triples into *table_name* using one persistent connection."""
        validate_table_name(table_name)
        if not triples:
            return 0

        if self._uses_inline_sql():
            return self._execute_insert_triples_inline(
                table_name, triples, batch_size, on_progress
            )

        from databricks import sql

        total = 0
        conn_params = self._client.get_sql_connection_params()
        with sql.connect(**conn_params) as connection:
            with connection.cursor() as cursor:
                for i in range(0, len(triples), batch_size):
                    batch = triples[i : i + batch_size]
                    cursor.execute(self._insert_sql(table_name, batch))
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
    ) -> int:
        """INSERT via Statement Execution (no Thrift session in Databricks Apps)."""
        total = 0
        for i in range(0, len(triples), batch_size):
            batch = triples[i : i + batch_size]
            self._client.execute_statement(self._insert_sql(table_name, batch))
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
    ) -> int:
        """Insert triples, routing app writes to the inferred companion table.

        In raw mode (``domain is None``) writes go directly to *table_name*.
        A ``…_graph`` materialized view over the companion written is then
        refreshed (``GraphRelation.refresh_after_write``).
        """
        target = (
            table_name
            if self._domain is None
            else self._ensure_companion_objects(table_name)
        )
        written = self._execute_insert_triples(target, triples, batch_size, on_progress)
        GraphRelation.refresh_after_write(self._client, target)
        return written

    def optimize_inferred_companion(self, table_name: str) -> None:
        """Compact and re-cluster the inferred companion after app writes."""
        try:
            inferred = self._writable_table_fqn(table_name)
            materialize.optimize_table(self._client, inferred)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "OPTIMIZE inferred companion failed for %s: %s", table_name, exc
            )

    def purge_materialized_triples(self, table_name: str) -> int:
        """Truncate the inferred companion and preserve mapped source triples."""
        inferred = self._writable_table_fqn(table_name)
        try:
            count = self.count_triples(inferred)
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            if (
                "TABLE_OR_VIEW_NOT_FOUND" in msg
                or "does not exist" in msg.lower()
            ):
                logger.info(
                    "Purged 0 materialized triples; companion missing: %s",
                    inferred,
                )
                return 0
            raise
        materialize.truncate_table(self._client, inferred)
        GraphRelation.refresh_after_write(self._client, inferred)
        from back.core.graphdb.search_cache import rebuild_graph_cache_if_enabled

        rebuild_graph_cache_if_enabled(self, table_name, self._domain)
        logger.info("Purged %d materialized triples from %s", count, inferred)
        return count

    def optimize_table(self, table_name: str) -> None:
        """Run OPTIMIZE to trigger Liquid Clustering compaction."""
        validate_table_name(table_name)
        logger.info("Optimizing Delta table: %s", table_name)
        self._client.execute_statement(f"OPTIMIZE {table_name}")

    def get_inferred_triple_count(self, table_name: str) -> int:
        if self._domain is None:
            return 0
        try:
            inferred = self._writable_table_fqn(table_name)
            return self.count_triples(inferred)
        except Exception:  # noqa: BLE001
            return 0
