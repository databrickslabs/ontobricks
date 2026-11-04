"""The Delta ``…_inferred`` table as the list of the inferred triples a Neo4j store holds."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from back.core.errors import InfrastructureError, OntoBricksError
from back.core.logging import get_logger

logger = get_logger(__name__)


class InferredTripleLedger:
    """Lists in ``…_inferred`` the triples the app writes into a store that cannot tell them apart.

    A Neo4j property graph has no place to mark a triple as inferred, so its
    app writers (reasoning, cohorts) record each batch here *before* writing
    it to the store. A net-change build then deletes from the store every
    listed triple that is not also in ``…_data``
    (``TwinNetChangeApply``) and empties the list. A triple listed but never
    written (the store write failed) is harmless: deleting it removes nothing.
    """

    def __init__(self, client_factory: Callable[[], Any], inferred_fqn: str) -> None:
        self._client_factory = client_factory
        self.inferred_fqn = inferred_fqn

    @classmethod
    def for_domain(cls, domain: Any, settings: Any) -> InferredTripleLedger | None:
        """The ledger of a domain whose builds may apply net changes.

        ``None`` for a ``full`` domain (every build drops and reloads the
        store, inferences included) and when the ``…_inferred`` name cannot be
        resolved. The warehouse client is created on the first record.
        """
        from back.core.graphdb.delta import _table_naming
        from back.core.graphdb.delta.Base import create_databricks_client
        from back.core.graphdb.GraphDBFactory import (
            GraphDBFactory,
            normalize_materialization_mode,
        )

        mode = GraphDBFactory.resolve_materialization_mode(domain, settings)
        if normalize_materialization_mode(mode) == "full":
            return None
        inferred = _table_naming.inferred_table_fqn(domain, settings)
        if not inferred:
            return None
        return cls(lambda: create_databricks_client(domain, settings, for_write=True), inferred)

    @staticmethod
    def record_for(store: Any, triples: list) -> None:
        """Record *triples* when *store* keeps a ledger (Neo4j); a no-op for any other store."""
        ledger = getattr(store, "inferred_ledger", None)
        if isinstance(ledger, InferredTripleLedger) and triples:
            ledger.record(triples)

    def record(self, triples: list) -> int:
        """Append *triples* to ``…_inferred``; :class:`InfrastructureError` when that fails.

        The caller writes them to the store only afterwards, so an inference
        is never in the store without being listed.
        """
        from back.core.graphdb.delta import materialize
        from back.core.graphdb.delta.MaterializedStore import MaterializedStore

        client = self._client_factory()
        if client is None:
            raise InfrastructureError(
                f"No SQL warehouse to list the inferred triples in {self.inferred_fqn}"
            )
        try:
            materialize.ensure_inferred_table(client, self.inferred_fqn)
            written = MaterializedStore(client).insert_triples(self.inferred_fqn, triples)
        except OntoBricksError:
            raise
        except Exception as exc:  # noqa: BLE001 — translated, not swallowed
            raise InfrastructureError(
                f"Could not list the inferred triples in {self.inferred_fqn}: {exc}",
                detail=str(exc),
            ) from exc
        logger.info("Listed %d inferred triple(s) in %s", written, self.inferred_fqn)
        return written
