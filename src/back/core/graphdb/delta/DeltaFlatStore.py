"""Delta (Unity Catalog) flat triple store on Databricks SQL Warehouse."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from back.core.graphdb.GraphDBBackend import GraphDBBackend
from back.core.graphdb.delta import _table_naming, materialize
from back.core.logging import get_logger
from back.core.triplestore.delta.DeltaTripleStore import DeltaTripleStore

logger = get_logger(__name__)


class DeltaFlatStore(GraphDBBackend):
    """GraphDB backend that queries materialized Delta triple tables in UC."""

    def __init__(
        self,
        client: Any,
        domain: Any = None,
        settings: Any = None,
    ) -> None:
        self._client = client
        self._domain = domain
        self._settings = settings
        self._store = DeltaTripleStore(client)

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

    def create_table(self, table_name: str) -> None:
        self._store.create_table(table_name)

    def drop_table(self, table_name: str) -> None:
        self._store.drop_table(table_name)

    def insert_triples(
        self,
        table_name: str,
        triples: List[Dict[str, str]],
        batch_size: int = 2000,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        target = self._ensure_companion_objects(table_name)
        return self._store.insert_triples(
            target, triples, batch_size=batch_size, on_progress=on_progress
        )

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
        return self._store.query_triples(table_name)

    def count_triples(self, table_name: str) -> int:
        return self._store.count_triples(table_name)

    def table_exists(self, table_name: str) -> bool:
        return self._store.table_exists(table_name)

    def get_status(self, table_name: str) -> Dict[str, Any]:
        return self._store.get_status(table_name)

    def optimize_table(self, table_name: str) -> None:
        self._store.optimize_table(table_name)

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        return self._store.execute_query(query)

    def get_inferred_triple_count(self, table_name: str) -> int:
        try:
            inferred = self._writable_table_fqn(table_name)
            return self.count_triples(inferred)
        except Exception:  # noqa: BLE001
            return 0
