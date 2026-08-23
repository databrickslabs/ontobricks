"""Delta (Unity Catalog) flat triple store on Databricks SQL Warehouse."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from back.core.graphdb.GraphDBBackend import GraphDBBackend
from back.core.graphdb.delta import _table_naming, materialize
from back.core.helpers import sql_escape as _escape_sql_string, validate_table_name
from back.core.logging import get_logger

logger = get_logger(__name__)


class DeltaFlatStore(GraphDBBackend):
    """GraphDB backend that queries materialized Delta triple tables in UC.

    With a bound *domain* it routes app writes through an inferred companion
    table and a union graph VIEW.  Constructed with ``domain=None`` it behaves
    as a raw store against the exact table FQNs passed in — used for read-only
    health probes (the ``"view"`` engine) and direct materialisation.
    """

    def __init__(
        self,
        client: Any,
        domain: Any = None,
        settings: Any = None,
    ) -> None:
        self._client = client
        self._domain = domain
        self._settings = settings

    @property
    def query_dialect(self) -> str:
        return "sql"

    def get_connection(self) -> Any:
        return self._client

    def close(self) -> None:
        return

    def physical_table_id(self, graph_name: str) -> str:
        """Resolve logical graph name (``Domain_V5``) to a UC FQN for SQL."""
        return self._sql_relation(graph_name)

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
        """Map Lakebase-style logical names to UC ``…_graph`` / ``…_data`` FQNs."""
        if "." in table_name and table_name.count(".") == 2:
            return table_name
        resolved = self._readable_table_fqn()
        if resolved:
            return resolved
        return table_name

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
        """Ensure inferred TABLE + union graph VIEW exist before app writes."""
        inferred = self._writable_table_fqn(table_name)
        materialize.ensure_inferred_table(self._client, inferred)
        if self._domain is not None:
            data = _table_naming.data_table_fqn(self._domain, self._settings)
            graph = _table_naming.graph_view_fqn(self._domain, self._settings)
            if data and graph:
                materialize.ensure_graph_view(self._client, graph, data, inferred)
        return inferred

    def synced_table_name(self, table_name: str) -> str:
        """Base data table without inferred companion rows."""
        if table_name.endswith(_table_naming.data_suffix()):
            return table_name
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

        from databricks import sql

        total = 0
        conn_params = self._client.get_sql_connection_params()
        with sql.connect(**conn_params) as connection:
            with connection.cursor() as cursor:
                for i in range(0, len(triples), batch_size):
                    batch = triples[i : i + batch_size]
                    values_list = []
                    for t in batch:
                        s = _escape_sql_string(t.get("subject", "") or "")
                        p = _escape_sql_string(t.get("predicate", "") or "")
                        o = _escape_sql_string(t.get("object", "") or "")
                        values_list.append(f"('{s}', '{p}', '{o}')")
                    values_sql = ",\n".join(values_list)
                    query = f"INSERT INTO {table_name} (subject, predicate, object) VALUES\n{values_sql}"
                    cursor.execute(query)
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

    def insert_triples(
        self,
        table_name: str,
        triples: List[Dict[str, str]],
        batch_size: int = 2000,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """Insert triples, routing app writes to the inferred companion table.

        In raw mode (``domain is None``) writes go directly to *table_name*.
        """
        target = (
            table_name
            if self._domain is None
            else self._ensure_companion_objects(table_name)
        )
        return self._execute_insert_triples(target, triples, batch_size, on_progress)

    def optimize_inferred_companion(self, table_name: str) -> None:
        """Compact and re-cluster the inferred companion after app writes."""
        try:
            inferred = self._writable_table_fqn(table_name)
            materialize.optimize_table(self._client, inferred)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "OPTIMIZE inferred companion failed for %s: %s", table_name, exc
            )

    def query_triples(self, table_name: str) -> List[Dict[str, str]]:
        """SELECT all triples."""
        validate_table_name(table_name)
        return self._client.execute_query(
            f"SELECT subject, predicate, object FROM {table_name}"
        )

    def count_triples(self, table_name: str) -> int:
        """Count triples."""
        validate_table_name(table_name)
        rows = self._client.execute_query(f"SELECT COUNT(*) AS cnt FROM {table_name}")
        if rows and len(rows) > 0:
            return int(rows[0].get("cnt", 0))
        return 0

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists by trying count, catch TABLE_OR_VIEW_NOT_FOUND."""
        if not table_name or not table_name.strip():
            return False
        try:
            self.count_triples(table_name)
            return True
        except Exception as e:
            error_msg = str(e)
            if (
                "TABLE_OR_VIEW_NOT_FOUND" in error_msg
                or "does not exist" in error_msg.lower()
            ):
                return False
            raise

    def get_status(self, table_name: str) -> Dict[str, Any]:
        """Return dict with count, last_modified, etc."""
        validate_table_name(table_name)
        count = self.count_triples(table_name)
        status: Dict[str, Any] = {"count": count, "last_modified": None}
        try:
            detail = self._client.execute_query(f"DESCRIBE DETAIL {table_name}")
            if detail and len(detail) > 0:
                row = detail[0]
                status["last_modified"] = row.get("lastModified")
                status["path"] = row.get("path")
                status["format"] = row.get("format")
        except Exception as e:
            logger.debug("Could not get DESCRIBE DETAIL for %s: %s", table_name, e)
        return status

    def optimize_table(self, table_name: str) -> None:
        """Run OPTIMIZE to trigger Liquid Clustering compaction."""
        validate_table_name(table_name)
        logger.info("Optimizing Delta table: %s", table_name)
        self._client.execute_statement(f"OPTIMIZE {table_name}")

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a graph read query, bounded by the graph statement timeout.

        Routed through :meth:`SQLWarehouse.execute_query` with a
        ``statement_timeout_s`` so a runaway traversal (e.g. the recursive BFS
        CTE) is cancelled server-side instead of pinning a warehouse session.
        The unbounded ``client.execute_query`` is intentionally reserved for
        full-graph dumps (``query_triples``) and the build pipeline.
        """
        from back.core.query_limits import get_graph_query_timeout_s

        timeout_s = get_graph_query_timeout_s()
        sql_service = getattr(self._client, "sql", None)
        if sql_service is not None and hasattr(sql_service, "execute_query"):
            return sql_service.execute_query(query, statement_timeout_s=timeout_s)
        return self._client.execute_query(query)

    def get_inferred_triple_count(self, table_name: str) -> int:
        if self._domain is None:
            return 0
        try:
            inferred = self._writable_table_fqn(table_name)
            return self.count_triples(inferred)
        except Exception:  # noqa: BLE001
            return 0
