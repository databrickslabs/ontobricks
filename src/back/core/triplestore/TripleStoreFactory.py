"""Factory for creating triple store backends from domain session configuration.

Supports:
- ``"view"``  -- DeltaTripleStore (SQL against a Unity Catalog VIEW via warehouse)
- ``"graph"`` -- delegates to :class:`GraphDBFactory` (embedded graph database)
"""
from typing import Any, Optional

from back.core.databricks import is_databricks_app
from back.core.helpers import (
    get_databricks_host_and_token,
    resolve_warehouse_id,
)
from back.core.logging import get_logger

logger = get_logger(__name__)


class TripleStoreFactory:
    """Construct triple-store backend instances from domain session configuration."""

    LADYBUG_AVAILABLE = False

    def create(
        self,
        domain: Any,
        settings: Optional[Any] = None,
        backend: Optional[str] = None,
    ) -> Optional[Any]:
        """Create a triple store backend.

        Args:
            domain: Domain session with info and databricks config.
            settings: Optional application settings (for sql_warehouse_id fallback).
            backend: ``"view"`` for DeltaTripleStore, ``"graph"`` delegates to
                     GraphDBFactory.  Defaults to ``"graph"`` when *None*.

        Returns:
            Backend instance or *None* if configuration is incomplete.
        """
        if backend is None:
            backend = "graph"

        if backend == "view":
            return self._create_delta(domain, settings)

        if backend == "graph":
            from back.core.graphdb import get_graphdb
            return get_graphdb(domain, settings)

        logger.warning("Unknown triplestore backend: %s", backend)
        return None

    def _create_delta(self, domain: Any, settings: Optional[Any]) -> Optional[Any]:
        """Instantiate a DeltaTripleStore backed by a Databricks SQL warehouse."""
        try:
            from back.core.databricks import DatabricksClient
            from back.core.triplestore.delta.DeltaTripleStore import DeltaTripleStore

            if settings is not None:
                host, token = get_databricks_host_and_token(domain, settings)
                warehouse_id = resolve_warehouse_id(domain, settings)
            else:
                db = domain.databricks or {}
                host = db.get("host", "")
                token = db.get("token", "")
                warehouse_id = ""
            if not host and not is_databricks_app():
                logger.warning("Delta triplestore: missing host")
                return None
            if not token and not is_databricks_app():
                logger.warning("Delta triplestore: missing token")
                return None
            if not warehouse_id:
                logger.warning("Delta triplestore: missing sql_warehouse_id")
                return None
            client = DatabricksClient(
                host=host,
                token=token,
                warehouse_id=warehouse_id,
            )
            return DeltaTripleStore(client)
        except Exception as e:
            logger.exception("Failed to create DeltaTripleStore: %s", e)
            return None

    @classmethod
    def get_triplestore(
        cls,
        domain: Any,
        settings: Optional[Any] = None,
        backend: Optional[str] = None,
    ) -> Optional[Any]:
        """Convenience wrapper using the package singleton factory instance."""
        return _get_factory_singleton().create(
            domain, settings=settings, backend=backend,
        )


_factory_singleton: Optional[TripleStoreFactory] = None


def _get_factory_singleton() -> TripleStoreFactory:
    global _factory_singleton
    if _factory_singleton is None:
        _factory_singleton = TripleStoreFactory()
    return _factory_singleton


try:
    from back.core.graphdb import GRAPHDB_AVAILABLE
    TripleStoreFactory.LADYBUG_AVAILABLE = GRAPHDB_AVAILABLE
except ImportError:
    logger.debug("Graph DB backends not available (optional dependency)")
