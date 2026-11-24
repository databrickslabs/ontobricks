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
``MaterializedStore``, ``Store``) subclasses this hat, guaranteeing a
uniform write contract so the factory can add or drop a backend without the
rest of the codebase caring which engine is active.
"""

from abc import abstractmethod
from typing import Callable, Dict, List, Optional

from back.core.errors import ValidationError
from back.core.graphdb.interfaces.GraphBackend import GraphBackend
from back.core.graphdb.overrides import new_change_id, triple_key


class MaterializedGraphBackend(GraphBackend):
    """Shared write/companion/sync contract for copy-owning graph engines."""

    # ``True`` when the engine keeps user edits in an overrides layer
    # (``back.core.graphdb.overrides``): ``add_triples`` / ``delete_triples`` /
    # ``modify_triple`` then change the effective graph without touching the
    # mapped data, and survive a Build.
    supports_overrides = False

    # ------------------------------------------------------------------
    # User overrides (add / delete / modify) — see back.core.graphdb.overrides
    # ------------------------------------------------------------------

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
        """Add triples to the graph as user overrides. Returns the count handled."""
        raise NotImplementedError(f"{type(self).__name__} does not support add_triples")

    def modify_triple(
        self,
        table_name: str,
        old: Dict[str, str],
        new: Dict[str, str],
        *,
        author: str = "",
    ) -> str:
        """Replace *old* by *new* (any of subject, predicate, object may change).

        Both writes share one ``change_id``, which is returned. The new value is
        added first, so an interrupted call never loses data.
        """
        if not self.supports_overrides:
            raise NotImplementedError(
                f"{type(self).__name__} does not support modify_triple"
            )
        if triple_key(old) == triple_key(new):
            raise ValidationError("modify_triple: old and new triples are identical")
        change_id = new_change_id()
        self.add_triples(table_name, [new], author=author, change_id=change_id)
        self.delete_triples(table_name, [old], author=author, change_id=change_id)
        return change_id

    def reset_for_rebuild(self, table_name: str) -> None:
        """Recreate the graph storage for a full Build, keeping user overrides.

        The default drops and recreates the table; engines with an overrides
        layer override this so the user's edits survive the rebuild.
        """
        self.drop_table(table_name)
        self.create_table(table_name)

    def replay_overrides(self, table_name: str) -> int:
        """Re-apply user edits after the mapped triples were loaded (default: none).

        Engines whose Build rewrites the whole graph (Neo4j) keep a journal of the
        user's edits and replay it here; the others apply overrides at read time.
        """
        return 0

    def _mark_cache_stale(self) -> None:
        """Flag the search cache as out of date: reads fall back to the live graph."""
        from back.core.graphdb.SearchCachePolicy import SearchCachePolicy

        SearchCachePolicy.mark_graph_cache_pending(
            getattr(self, "_domain", None), persist=True
        )

    def _base_uri(self) -> str:
        """Ontology base URI of the bound domain (legacy-origin vocabulary)."""
        from shared.config.constants import DEFAULT_BASE_URI

        ontology = getattr(getattr(self, "_domain", None), "ontology", None) or {}
        uri = ontology.get("base_uri") if isinstance(ontology, dict) else None
        return (uri or DEFAULT_BASE_URI).strip()

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
        origin: str = "reasoning",
    ) -> int:
        """Batch insert derived triples, returns count inserted.

        *origin* (``reasoning``, ``cohort`` or ``community``) tells the store which
        feature derived the rows, so Build and purge can remove exactly those. User
        edits never use this method: see ``add_triples``.
        """
        ...
