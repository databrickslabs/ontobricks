"""Abstract base class for graph database backends.

A graph DB backend is a local/embedded graph engine (e.g. LadybugDB, KuzuDB,
Neo4j) used for fast Cypher queries, graph traversal, reasoning, and
analytics.  It is **separate** from the triple store (Delta views in Unity
Catalog), which is the permanent storage layer.

``GraphDBBackend`` extends ``TripleStoreBackend`` because both operate on the
same triple data model (subject, predicate, object) and share the same named
query interface.  The extension adds graph-specific concerns: connection
management, schema introspection, sync to/from UC Volumes, capability flags,
and query-translator selection for reasoning engines.
"""

from abc import abstractmethod
from typing import Any, Optional, Tuple

from back.core.logging import get_logger
from back.core.triplestore.TripleStoreBackend import TripleStoreBackend

logger = get_logger(__name__)


class GraphDBBackend(TripleStoreBackend):
    """Abstract base for graph DB engines (LadybugDB, KuzuDB, Neo4j, ...).

    Subclasses must implement the core ``TripleStoreBackend`` abstract methods
    **plus** the graph-specific abstract methods declared here.
    """

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
        return isinstance(store, GraphDBBackend) and store.supports_cypher

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
        constraints (e.g. LadybugDB safe identifiers) should override.
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
        from back.core.reasoning.SWRLSQLTranslator import SWRLSQLTranslator

        return SWRLSQLTranslator()
