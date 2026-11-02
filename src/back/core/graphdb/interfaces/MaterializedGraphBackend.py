"""Hat interface for materialized graph DB backends.

A *materialized* backend owns a physical copy of the triples — Lakebase
Postgres synced/app tables, a Databricks Delta ``table`` snapshot, or a Neo4j
property graph.  Because it owns the data it supports the full write surface:
table lifecycle, triple ingestion, companion (adjacency / entity-search /
props) materialization, inference purge, and remote sync.

Read-only pass-through engines (e.g. a Delta ``view``) use
:class:`back.core.graphdb.interfaces.VirtualBackend.VirtualBackend` instead, which exposes
no write surface.

Every concrete materialized engine (``FlatStore``,
``DeltaMaterializedStore``, ``Store``) subclasses this hat, guaranteeing a
uniform write contract so the factory can add or drop a backend without the
rest of the codebase caring which engine is active.
"""

from abc import abstractmethod
from typing import Callable, Dict, List, Optional

from back.core.graphdb.interfaces.GraphBackend import GraphBackend


class MaterializedGraphBackend(GraphBackend):
    """Shared write/companion/sync contract for copy-owning graph engines."""

    # ------------------------------------------------------------------
    # Core write abstract methods — every materialized engine implements these.
    # ------------------------------------------------------------------

    @abstractmethod
    def create_table(self, table_name: str) -> None:
        """Create the (subject, predicate, object) table."""
        ...

    @abstractmethod
    def drop_table(self, table_name: str) -> None:
        """Drop if exists."""
        ...

    @abstractmethod
    def insert_triples(
        self,
        table_name: str,
        triples: List[Dict[str, str]],
        batch_size: int = 500,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """Batch insert triples, returns count inserted."""
        ...
