"""Delta (Unity Catalog) flat triple store on Databricks SQL Warehouse."""

from __future__ import annotations

import concurrent.futures
import re
import time
from typing import Any, Callable, Dict, List, Optional

from back.core.graphdb.GraphDBBackend import GraphDBBackend
from back.core.graphdb.adjacency import expand_and_fetch_sql
from back.core.graphdb.constants import RDF_TYPE, RDFS_LABEL
from back.core.graphdb.delta import _table_naming, materialize
from back.core.graphdb.delta.DeltaBase import create_databricks_client
from back.core.graphdb.delta.DeltaOverviewStats import DeltaOverviewStats
from back.core.graphdb.delta.GraphReadRelation import GraphReadRelation
from back.core.graphdb.delta.GraphRelation import GraphRelation
from back.core.graphdb.entity_search import (
    is_missing_relation_error,
    is_view_not_table_error,
)
from back.core.graphdb.props import (
    execute_expand_with_props_fallback,
    forget_missing_props,
)
from back.core.helpers import (
    resolve_build_warehouse_id,
    sql_escape as _escape_sql_string,
    validate_table_name,
)
from back.core.logging import get_logger

logger = get_logger(__name__)

# Lower-case substrings of a read error that mean the Lakehouse//RT query
# warehouse refused or aborted the statement (guardrails: 2-minute runtime,
# 300 GB shuffle, statement timeout, unsupported feature).  Such a read is
# retried once on the build warehouse; any other error propagates unchanged.
# Only SELECT / WITH statements are retried (see ``_is_read_statement``).
RT_FALLBACK_MARKERS = (
    "guardrail",
    "exceeds",
    "statement_timeout",
    "query timed out",
    "501",
    "lakehouse//rt",
    "not supported on this warehouse",
)

_READ_STATEMENT = re.compile(r"(?:SELECT|WITH)\b", re.IGNORECASE)


class DeltaFlatStore(GraphDBBackend):
    """GraphDB backend that queries materialized Delta triple tables in UC.

    With a bound *domain* it routes app writes through an inferred companion
    table and a union graph VIEW.  Constructed with ``domain=None`` it behaves
    as a raw store against the exact table FQNs passed in — used for read-only
    health probes (the ``"view"`` engine) and direct materialisation.
    """

    supports_materialized_inference_purge = True
    supports_adjacency = True
    supports_entity_search = True
    supports_props = True

    def __init__(
        self,
        client: Any,
        domain: Any = None,
        settings: Any = None,
    ) -> None:
        self._client = client
        self._domain = domain
        self._settings = settings
        self._fallback_client: Any = None
        self._last_read_route = "query"

    @property
    def last_read_route(self) -> str:
        """Warehouse path that answered the last read: ``query`` or ``build-fallback``."""
        return self._last_read_route

    @property
    def query_dialect(self) -> str:
        return "sql"

    def _distinct_count_expr(self, column: str) -> str:
        """Use Spark's HyperLogLog counter for the stats overview.

        ``approx_count_distinct`` is O(1) memory and avoids the full distinct
        shuffle an exact ``COUNT(DISTINCT ...)`` triggers over multi-million-row
        triple tables — turning a multi-second stats query into sub-second. The
        default ~5% relative error is acceptable for an at-a-glance overview.
        """
        return f"approx_count_distinct({column})"

    def get_connection(self) -> Any:
        return self._client

    def close(self) -> None:
        return

    def physical_table_id(self, graph_name: str) -> str:
        """Resolve logical graph name (``Domain_V5``) to a UC FQN: names only, never the live relation."""
        return graph_name if graph_name.count(".") == 2 else (self._readable_table_fqn() or graph_name)

    def _readable_table_fqn(self) -> str:
        """UC object for read queries (union VIEW, or ``_data`` if VIEW not built yet)."""
        if self._domain is None:
            return ""
        graph = _table_naming.graph_view_fqn(self._domain, self._settings)
        data = _table_naming.data_table_fqn(self._domain, self._settings)
        if graph:
            try:
                if self.table_exists(graph):
                    return graph
            except Exception:  # noqa: BLE001
                pass
        return data or graph or ""

    def _sql_relation(self, table_name: str) -> str:
        """Map logical names to UC ``…_graph`` / ``…_data``; an FQN passes (``GraphReadRelation`` may swap it).

        Read-only SQL: a swapped FQN is a subquery (a write must pass a logical name or the table).
        """
        if table_name.count(".") == 2:
            return GraphReadRelation.for_read(table_name, self._domain, self._settings, self._client)
        resolved = self._readable_table_fqn()
        if resolved:
            return resolved
        return table_name

    def sql_flavor(self) -> str:
        return "spark"

    def adjacency_table_ids(self, table_name: str) -> tuple[str, str]:
        if self._domain is not None:
            adj_out = _table_naming.adj_out_fqn(self._domain, self._settings)
            adj_in = _table_naming.adj_in_fqn(self._domain, self._settings)
            if adj_out and adj_in:
                return (adj_out, adj_in)
        if "." not in table_name or table_name.count(".") != 2:
            return ("", "")
        cat, sch, base = table_name.split(".", 2)
        base = _table_naming.strip_graph_leaf_suffix(base)
        return (
            f"{cat}.{sch}.{base}{_table_naming.adj_out_suffix()}",
            f"{cat}.{sch}.{base}{_table_naming.adj_in_suffix()}",
        )

    def entity_search_table_id(self, table_name: str) -> str:
        if self._domain is not None:
            search = _table_naming.entity_search_fqn(self._domain, self._settings)
            if search:
                return search
        if "." not in table_name or table_name.count(".") != 2:
            return ""
        cat, sch, base = table_name.split(".", 2)
        base = _table_naming.strip_graph_leaf_suffix(base)
        return f"{cat}.{sch}.{base}{_table_naming.entity_search_suffix()}"

    def entity_search_asserted_table_id(self, table_name: str) -> str:
        if self._domain is not None:
            search = _table_naming.entity_search_asserted_fqn(
                self._domain, self._settings
            )
            if search:
                return search
        if "." not in table_name or table_name.count(".") != 2:
            return ""
        cat, sch, base = table_name.split(".", 2)
        base = _table_naming.strip_graph_leaf_suffix(base)
        return f"{cat}.{sch}.{base}{_table_naming.entity_search_asserted_suffix()}"

    def props_table_id(self, table_name: str) -> str:
        if self._domain is not None:
            props = _table_naming.props_fqn(self._domain, self._settings)
            if props:
                return props
        if "." not in table_name or table_name.count(".") != 2:
            return ""
        cat, sch, base = table_name.split(".", 2)
        base = _table_naming.strip_graph_leaf_suffix(base)
        return f"{cat}.{sch}.{base}{_table_naming.props_suffix()}"

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

    def synced_table_name(self, table_name: str) -> str:
        """Base data table without inferred companion rows."""
        if table_name.endswith(_table_naming.data_suffix()):
            return table_name
        if self._domain is not None:
            data = _table_naming.data_table_fqn(self._domain, self._settings)
            if data:
                return data
        if "." in table_name and table_name.count(".") == 2:
            cat, sch, base = table_name.split(".", 2)
            if base.endswith(_table_naming.inferred_suffix()):
                base = base[: -len(_table_naming.inferred_suffix())]
            elif base.endswith(_table_naming.graph_suffix()):
                base = base[: -len(_table_naming.graph_suffix())]
            return f"{cat}.{sch}.{base}{_table_naming.data_suffix()}"
        return table_name

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

    def query_triples(self, table_name: str) -> List[Dict[str, str]]:
        """SELECT all triples (a dump: no statement timeout, but the RT fallback)."""
        validate_table_name(table_name)
        sql = f"SELECT subject, predicate, object FROM {self._sql_relation(table_name)}"
        return self.execute_query(sql, bounded=False)

    def count_triples(self, table_name: str) -> int:
        """Count triples (no statement timeout, but the RT fallback)."""
        validate_table_name(table_name)
        sql = f"SELECT COUNT(*) AS cnt FROM {self._sql_relation(table_name)}"
        rows = self.execute_query(sql, bounded=False)
        if rows and len(rows) > 0:
            return int(rows[0].get("cnt", 0))
        return 0

    def describe_detail(self, table_name: str) -> Dict[str, Any]:
        """Return the first ``DESCRIBE DETAIL`` row, or ``{}`` if it fails.

        A view (zero-copy ``…_data``) has no Delta detail, so it also yields ``{}``.
        """
        if not table_name or not table_name.strip():
            return {}
        validate_table_name(table_name)
        try:
            rows = self._client.execute_query(f"DESCRIBE DETAIL {table_name}")
        except Exception as exc:  # noqa: BLE001
            if is_missing_relation_error(exc) or is_view_not_table_error(exc):
                return {}
            raise
        return rows[0] if rows else {}

    def _graph_index_probe_fqn(self, table_name: str) -> str:
        """Prefer ``…_entity_search`` when *table_name* is the union ``…_graph`` VIEW."""
        leaf = table_name.rsplit(".", 1)[-1]
        if leaf.endswith(_table_naming.graph_suffix()):
            search = self.entity_search_table_id(table_name)
            if search:
                return search
        return table_name

    def table_exists(self, table_name: str) -> bool:
        """Check existence with catalog metadata, not a row scan.

        For the union ``…_graph`` VIEW, describe ``…_entity_search`` first.
        ``DESCRIBE DETAIL`` on a pass-through mapping view can fail the same
        way a ``COUNT(*)`` does (timeout / cloud I/O), which the KG badge
        treats as unknown.
        """
        if not table_name or not table_name.strip():
            return False
        validate_table_name(table_name)
        probe = self._graph_index_probe_fqn(table_name)
        if probe != table_name and self._describe_exists(probe) is True:
            return True
        exists = self._describe_exists(table_name)
        if exists is not None:
            return exists
        return False

    def _describe_exists(self, table_name: str) -> Optional[bool]:
        try:
            self._client.execute_query(f"DESCRIBE DETAIL {table_name}")
            return True
        except Exception as exc:
            if is_view_not_table_error(exc):
                return True
            if is_missing_relation_error(exc):
                return False
            raise

    def get_status(self, table_name: str) -> Dict[str, Any]:
        """Return count/metadata without touching a live mapping VIEW."""
        validate_table_name(table_name)
        probe = self._graph_index_probe_fqn(table_name)
        status: Dict[str, Any] = {"count": 0, "last_modified": None}
        row = self.describe_detail(probe)
        if row:
            status["last_modified"] = row.get("lastModified")
            status["path"] = row.get("path")
            status["format"] = row.get("format")
        total = (
            DeltaOverviewStats(self).triple_count(table_name)
            if probe != table_name
            else None
        )
        status["count"] = total if total else self._companion_has_row_count(probe)
        return status

    def _companion_has_row_count(self, table_name: str) -> int:
        """``1``/``0`` from ``LIMIT 1`` on *table_name* (already a probe FQN)."""
        try:
            rows = self._client.execute_query(f"SELECT 1 FROM {table_name} LIMIT 1")
            return 1 if rows else 0
        except Exception as exc:  # noqa: BLE001
            if is_missing_relation_error(exc):
                return 0
            raise

    def get_aggregate_stats(self, table_name: str) -> Dict[str, int]:
        return DeltaOverviewStats(self).aggregate(table_name)

    def get_type_distribution(self, table_name: str) -> List[Dict[str, Any]]:
        return DeltaOverviewStats(self).type_distribution(table_name)

    def get_predicate_distribution(self, table_name: str) -> List[Dict[str, Any]]:
        return DeltaOverviewStats(self).predicate_distribution(table_name)

    def optimize_table(self, table_name: str) -> None:
        """Run OPTIMIZE to trigger Liquid Clustering compaction."""
        validate_table_name(table_name)
        logger.info("Optimizing Delta table: %s", table_name)
        self._client.execute_statement(f"OPTIMIZE {table_name}")

    def execute_query(self, query: str, *, bounded: bool = True) -> List[Dict[str, Any]]:
        """Execute a graph read query, bounded by the graph statement timeout.

        Routed through :meth:`SQLWarehouse.execute_query` with a
        ``statement_timeout_s`` so a runaway traversal (e.g. the recursive BFS
        CTE) is cancelled server-side instead of pinning a warehouse session.
        Full-graph dumps and counts pass ``bounded=False`` (the unbounded
        ``client.execute_query``, as the build pipeline uses).

        A read refused by a Lakehouse//RT guardrail (see
        :data:`RT_FALLBACK_MARKERS`) is retried once on the build warehouse;
        :attr:`last_read_route` records which path answered.
        """
        self._last_read_route = "query"
        try:
            rows = self._run_read(self._client, query, bounded)
        except Exception as exc:
            fallback = self._rt_fallback_client(exc, query)
            if fallback is None:
                raise
            logger.warning(
                "Delta read refused by the query warehouse, retrying on the "
                "build warehouse (sql_len=%d, sql=%s): %s",
                len(query),
                query[:120],
                exc,
            )
            rows = self._run_read(fallback, query, bounded)
            self._last_read_route = "build-fallback"
            return rows
        return rows

    @staticmethod
    def _run_read(client: Any, query: str, bounded: bool = True) -> List[Dict[str, Any]]:
        """Run *query* on *client*, bounded by the graph statement timeout when *bounded*."""
        from back.core.query_limits import get_graph_query_timeout_s

        sql_service = getattr(client, "sql", None)
        if bounded and sql_service is not None and hasattr(sql_service, "execute_query"):
            return sql_service.execute_query(query, statement_timeout_s=get_graph_query_timeout_s())
        return client.execute_query(query)

    @staticmethod
    def _is_read_statement(query: str) -> bool:
        """True when *query* starts with SELECT or WITH after whitespace/comments."""
        rest = query
        while True:
            rest = rest.lstrip()
            if rest.startswith("--"):
                end = rest.find("\n")
                if end < 0:
                    return False
                rest = rest[end + 1 :]
            elif rest.startswith("/*"):
                end = rest.find("*/")
                if end < 0:
                    return False
                rest = rest[end + 2 :]
            else:
                return _READ_STATEMENT.match(rest) is not None

    def _rt_fallback_client(self, exc: Exception, query: str) -> Optional[Any]:
        """Build-warehouse client for a refused *query*, or ``None`` for no retry.

        Only guardrail-marked errors on a domain-bound store, for a read
        statement, qualify.  The retry keeps the caller's identity (the read
        branch of ``create_databricks_client``, OBO token included) and only
        swaps the warehouse; it is skipped when the build warehouse is unset
        or already the one that refused the statement.  The client is created
        lazily and cached for later reads.
        """
        message = str(exc).lower()
        if not any(marker in message for marker in RT_FALLBACK_MARKERS):
            return None
        if self._domain is None or not self._is_read_statement(query):
            return None
        if self._fallback_client is None:
            build_wh = resolve_build_warehouse_id(self._domain, self._settings)
            primary_wh = getattr(self._client, "warehouse_id", None)
            if not build_wh or build_wh == primary_wh:
                return None
            self._fallback_client = create_databricks_client(
                self._domain, self._settings, for_write=False, warehouse_id=build_wh
            )
        return self._fallback_client

    @staticmethod
    def _expansion_level_ctes(
        relation: str, depth: int, max_entities: int
    ) -> list[str]:
        """Build fixed-depth, de-duplicated BFS levels for one SQL statement."""
        ctes: list[str] = []
        for level in range(1, depth + 1):
            previous = f"level_{level - 1}"
            visited = f"visited_{level}"
            visited_union = " UNION ALL ".join(
                f"SELECT entity FROM level_{prior}" for prior in range(level)
            )
            ctes.extend(
                [
                    f"{visited} AS ({visited_union})",
                    (
                        f"level_{level}_candidates AS ("
                        f"SELECT t.object AS entity FROM {relation} t "
                        f"JOIN {previous} frontier "
                        f"ON t.subject = frontier.entity "
                        f"WHERE t.object LIKE 'http%' "
                        f"AND t.predicate != '{RDF_TYPE}' "
                        f"AND t.predicate != '{RDFS_LABEL}' "
                        f"UNION ALL "
                        f"SELECT t.subject AS entity FROM {relation} t "
                        f"JOIN {previous} frontier "
                        f"ON t.object = frontier.entity "
                        f"WHERE t.predicate != '{RDF_TYPE}' "
                        f"AND t.predicate != '{RDFS_LABEL}')"
                    ),
                    (
                        f"level_{level} AS ("
                        f"SELECT DISTINCT candidate.entity "
                        f"FROM level_{level}_candidates candidate "
                        f"JOIN {relation} typed "
                        f"ON typed.subject = candidate.entity "
                        f"AND typed.predicate = '{RDF_TYPE}' "
                        f"LEFT ANTI JOIN {visited} "
                        f"ON {visited}.entity = candidate.entity "
                        f"LIMIT {max_entities})"
                    ),
                ]
            )
        return ctes

    def expand_and_fetch_subgraph(
        self,
        table_name: str,
        selected_uris: list[str],
        depth: int,
        max_entities: int,
        max_triples: int,
    ) -> dict[str, Any]:
        """Expand and fetch a bounded Delta subgraph in one SQL statement."""
        if not selected_uris:
            raise ValueError("At least one selected URI is required")
        depth = max(0, int(depth))
        max_entities = max(1, int(max_entities))
        max_triples = max(1, int(max_triples))

        relation = self._sql_relation(table_name)
        props = ""
        if self.adjacency_ready(table_name):
            adj_out, adj_in = self.adjacency_table_ids(table_name)
            props = self._props_table_for_read(table_name)
            sql = expand_and_fetch_sql(
                flavor=self.sql_flavor(),
                adj_out=self._sql_relation(adj_out),
                adj_in=self._sql_relation(adj_in),
                spo=relation,
                props=self._sql_relation(props) if props else None,
                selected_uris=selected_uris,
                depth=depth,
                max_entities=max_entities,
                max_triples=max_triples,
                escape=self._sql_escape,
            )
        else:
            seed_values = ", ".join(
                f"('{self._sql_escape(uri)}')" for uri in dict.fromkeys(selected_uris)
            )
            ctes = [f"level_0(entity) AS (VALUES {seed_values})"]
            ctes.extend(self._expansion_level_ctes(relation, depth, max_entities))

            levels = " UNION ALL ".join(
                f"SELECT entity FROM level_{level}" for level in range(depth + 1)
            )
            ctes.extend(
                [
                    (
                        "entity_probe AS ("
                        f"SELECT DISTINCT entity FROM ({levels}) discovered "
                        f"LIMIT {max_entities + 1})"
                    ),
                    (
                        "entities AS ("
                        f"SELECT entity FROM entity_probe LIMIT {max_entities})"
                    ),
                    (
                        "entity_stats AS ("
                        "SELECT COUNT(*) AS _ob_expanded_count FROM entity_probe)"
                    ),
                ]
            )
            sql = (
                "WITH "
                + ", ".join(ctes)
                + " "
                + "SELECT triples.subject, triples.predicate, triples.object, "
                + "stats._ob_expanded_count "
                + f"FROM {relation} triples "
                + "JOIN entities ON entities.entity = triples.subject "
                + "CROSS JOIN entity_stats stats "
                + f"LIMIT {max_triples + 1}"
            )

        if props:
            adj_out, adj_in = self.adjacency_table_ids(table_name)
            fallback_sql = expand_and_fetch_sql(
                flavor=self.sql_flavor(),
                adj_out=self._sql_relation(adj_out),
                adj_in=self._sql_relation(adj_in),
                spo=relation,
                selected_uris=selected_uris,
                depth=depth,
                max_entities=max_entities,
                max_triples=max_triples,
                escape=self._sql_escape,
            )
            rows = execute_expand_with_props_fallback(
                execute_query=self.execute_query,
                sql=sql,
                props_table=props,
                fallback_sql=fallback_sql,
            )
        else:
            rows = self.execute_query(sql) or []
        discovered_count = (
            int(rows[0].get("_ob_expanded_count") or 0)
            if rows
            else min(len(set(selected_uris)), max_entities)
        )
        triple_capped = len(rows) > max_triples
        triples = [
            {
                "subject": row.get("subject", ""),
                "predicate": row.get("predicate", ""),
                "object": row.get("object", ""),
            }
            for row in rows[:max_triples]
        ]
        entity_capped = discovered_count > max_entities
        return {
            "results": triples,
            "count": len(triples),
            "expanded_count": min(discovered_count, max_entities),
            "capped": entity_capped or triple_capped,
            "timeout_capped": False,
        }

    def get_inferred_triple_count(self, table_name: str) -> int:
        if self._domain is None:
            return 0
        try:
            inferred = self._writable_table_fqn(table_name)
            return self.count_triples(inferred)
        except Exception:  # noqa: BLE001
            return 0
