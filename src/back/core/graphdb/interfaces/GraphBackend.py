"""Shared read root for all graph database backends.

A graph DB backend is a graph-capable triple store (e.g. Lakebase Postgres,
Databricks Delta, or any future Cypher / Gremlin engine implementing the
``back.core.graphdb.interfaces`` contract) operating on the
``(subject, predicate, object)`` triple
model.  It is the single storage abstraction used across OntoBricks for
querying, reasoning, graph traversal, and analytics.

``GraphBackend`` is the common **read** contract shared by both backend
families.  Concrete engines do not subclass it directly — they subclass one of
the two "hat" interfaces:

* :class:`MaterializedGraphBackend` — engines that own a physical copy of the
  triples (Lakebase, Delta ``table``, Neo4j): full write / companion / sync
  surface.
* :class:`VirtualBackend` — read-only pass-through engines over a live source
  (Delta ``view``): no writes, no companions, no sync.

``GraphBackend`` provides the core read abstract methods plus the capability
flags, connection/schema/sync hooks, and the query-translator selection. Its
large set of **default SQL implementations** (named queries, stats, traversal,
reasoning, companion probing) lives in focused mixins mixed in below — SQL
backends inherit them; non-SQL engines (Cypher, Gremlin) override as needed:

* :class:`~back.core.graphdb.interfaces.ReadStatsMixin.ReadStatsMixin`
* :class:`~back.core.graphdb.interfaces.EntityQueryMixin.EntityQueryMixin`
* :class:`~back.core.graphdb.interfaces.TraversalMixin.TraversalMixin`
* :class:`~back.core.graphdb.interfaces.SearchCacheMixin.SearchCacheMixin`
"""

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple

from back.core.helpers import SQL_DIALECT_SPARK
from back.core.helpers import sql_escape as _shared_sql_escape
from back.core.graphdb.interfaces.ReadStatsMixin import ReadStatsMixin
from back.core.graphdb.interfaces.EntityQueryMixin import EntityQueryMixin
from back.core.graphdb.interfaces.TraversalMixin import TraversalMixin
from back.core.graphdb.interfaces.SearchCacheMixin import SearchCacheMixin


class GraphBackend(
    ReadStatsMixin,
    EntityQueryMixin,
    TraversalMixin,
    SearchCacheMixin,
    ABC,
):
    """Shared read root for graph DB engines (see module docstring).

    Concrete engines subclass one of the two hats —
    :class:`MaterializedGraphBackend` or :class:`VirtualBackend` — never this
    class directly.  Subclasses must implement the core read abstract methods
    plus connection management (``get_connection`` / ``close``).
    """

    supports_materialized_inference_purge = False
    supports_adjacency = False
    supports_entity_search = False
    supports_props = False
    # auto | force_on | force_off — see back.core.graphdb.search_cache
    _search_cache_mode = "auto"

    # ------------------------------------------------------------------
    # Core read abstract methods
    # ------------------------------------------------------------------

    def delete_triples(
        self,
        table_name: str,
        triples: List[Dict[str, str]],
        batch_size: int = 500,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """Remove specific triples from the store. Returns count deleted.

        Default implementation raises NotImplementedError.
        Backends that support incremental sync should override this.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not support delete_triples"
        )

    def purge_materialized_triples(self, table_name: str) -> int:
        """Remove generated triples while preserving mapped source triples."""
        raise NotImplementedError(
            f"{type(self).__name__} cannot safely purge generated triples"
        )

    def synced_table_name(self, table_name: str) -> str:
        """Return the table name that contains only synced (non-materialized) triples.

        For backends that separate synced bulk data from app-written/inferred data
        (e.g. FlatStore with its ``_sync`` / ``__app`` companion layout),
        this returns the synced-only side so callers can query without materialised
        triples.  The default returns *table_name* unchanged (no distinction).
        """
        return table_name

    @abstractmethod
    def query_triples(self, table_name: str) -> List[Dict[str, str]]:
        """SELECT all triples."""
        ...

    @abstractmethod
    def count_triples(self, table_name: str) -> int:
        """Count triples."""
        ...

    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists."""
        ...

    @abstractmethod
    def get_status(self, table_name: str) -> Dict[str, Any]:
        """Return dict with count, last_modified, etc."""
        ...

    def optimize_table(self, table_name: str) -> None:
        """Run backend-specific optimization after bulk writes.

        Default is a no-op; backends that benefit from post-write
        optimization (e.g. Delta OPTIMIZE) should override this.
        """

    @abstractmethod
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute arbitrary SQL and return results."""
        ...

    # ------------------------------------------------------------------
    # SQL helpers shared by the default-implementation mixins
    # ------------------------------------------------------------------

    def _sql_escape(self, value: str) -> str:
        """Escape *value* for a ``'...'`` literal in this backend's SQL dialect."""
        return _shared_sql_escape(value, self.sql_flavor() or SQL_DIALECT_SPARK)

    def _sql_relation(self, table_name: str) -> str:
        """SQL relation fragment for *table_name* in generated queries.

        Delta passes fully-qualified ``catalog.schema.table`` unchanged.
        Postgres backends resolve to a physical identifier under ``search_path``.
        """
        return table_name

    def sql_table_reference(self, graph_name: str) -> str:
        """Stable identifier for translators (SWRL, SPARQL, aggregate/DT SQL)."""
        return self._sql_relation(graph_name)

    def sql_flavor(self) -> Optional[Literal["spark", "postgres"]]:
        """SQL dialect for adjacency helpers, or *None* when not applicable."""
        return None

    # ------------------------------------------------------------------
    # Capability flags — reasoning engines use these instead of isinstance
    # ------------------------------------------------------------------

    @property
    def supports_cypher(self) -> bool:
        """Whether this backend speaks Cypher (vs SQL)."""
        return False

    @property
    def supports_graph_model(self) -> bool:
        """Whether this backend uses a typed graph schema (node/rel tables)."""
        return False

    @property
    def query_dialect(self) -> str:
        """Return ``'sql'``, ``'cypher'``, or another dialect identifier."""
        return "sql"

    @staticmethod
    def is_cypher_backend(store) -> bool:
        """Check if *store* is a Cypher-capable graph DB backend."""
        return isinstance(store, GraphBackend) and store.supports_cypher

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    @abstractmethod
    def get_connection(self) -> Any:
        """Return (and lazily open) the native database connection."""
        ...

    @abstractmethod
    def close(self) -> None:
        """Release the database connection and any related resources."""
        ...

    # ------------------------------------------------------------------
    # Schema helpers
    # ------------------------------------------------------------------

    def get_node_table(self, table_name: str) -> str:
        """Return the node-table identifier for *table_name*.

        Default returns *table_name* unchanged.  Backends with naming
        constraints (reserved words, character restrictions) should override.
        """
        return table_name

    def get_graph_schema(self) -> Optional[Any]:
        """Return the graph schema object, or *None* if not available."""
        return None

    # ------------------------------------------------------------------
    # Sync to/from remote storage (UC Volume)
    # ------------------------------------------------------------------

    def sync_to_remote(
        self,
        uc_path: str,
        volume_service: Any,
    ) -> Tuple[bool, str]:
        """Upload local DB to remote storage.  No-op by default."""
        return False, "Not supported by this backend"

    def sync_from_remote(
        self,
        uc_path: str,
        volume_service: Any,
    ) -> Tuple[bool, str]:
        """Download DB from remote storage.  No-op by default."""
        return False, "Not supported by this backend"

    def local_path(self) -> Optional[str]:
        """Return the local file/directory path, or *None* for remote-only."""
        return None

    def remote_archive_path(self, uc_domain_path: str) -> Optional[str]:
        """Return the remote archive path for sync, or *None*."""
        return None

    # ------------------------------------------------------------------
    # Reasoning support
    # ------------------------------------------------------------------

    def get_query_translator(self, table_name: str = "") -> Any:
        """Return the appropriate SWRL/rule query translator for this backend.

        SQL-based backends should return an ``SWRLSQLTranslator``.
        Cypher-based backends should return the matching Cypher translator.
        """
        from back.core.reasoning.swrl.SWRLSQLTranslator import SWRLSQLTranslator

        return SWRLSQLTranslator(dialect=self.sql_flavor() or SQL_DIALECT_SPARK)
