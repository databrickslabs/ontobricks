"""Factory for creating graph database backends from domain session configuration.

Supports multiple engine types (currently only ``"ladybug"``).  The *engine*
parameter is the extension point for future graph DB engines (Neo4j,
KuzuDB, etc.).
"""
from typing import Any, Callable, Optional, Tuple

from back.core.databricks import is_databricks_app
from back.core.helpers import (
    effective_uc_version_path,
    get_databricks_host_and_token,
)
from back.core.logging import get_logger
from shared.config.constants import DEFAULT_GRAPH_NAME, DEFAULT_LADYBUG_PATH

logger = get_logger(__name__)


class GraphDBFactory:
    """Construct graph DB backend instances from domain session configuration."""

    LADYBUG_AVAILABLE = False

    def create(
        self,
        domain: Any,
        settings: Optional[Any] = None,
        engine: Optional[str] = None,
    ) -> Optional[Any]:
        """Create a graph DB backend.

        Args:
            domain: Domain session with info and databricks config.
            settings: Optional application settings.
            engine: ``"ladybug"`` (default).  Future values: ``"neo4j"``,
                     ``"kuzu"``, etc.

        Returns:
            GraphDBBackend instance or *None* if configuration is incomplete.
        """
        if engine is None:
            engine = "ladybug"

        if engine == "ladybug":
            return self._create_ladybug(domain, settings)

        logger.warning("Unknown graph DB engine: %s", engine)
        return None

    @staticmethod
    def _build_auto_restore(
        domain: Any,
        settings: Optional[Any],
        db_name: str,
        db_path: str,
    ) -> Optional[Callable[[], Tuple[bool, str]]]:
        """Build a callback that restores a graph DB file from the registry.

        Returns *None* when the registry or Databricks credentials are
        not configured — in that case auto-restore is simply disabled.
        """
        uc_domain_path = effective_uc_version_path(domain)
        if not uc_domain_path:
            return None

        if settings is not None:
            host, token = get_databricks_host_and_token(domain, settings)
        else:
            db_cfg = getattr(domain, 'databricks', None) or {}
            host = db_cfg.get('host', '')
            token = db_cfg.get('token', '')

        if not host and not is_databricks_app():
            return None
        if not token and not is_databricks_app():
            return None

        def _restore() -> Tuple[bool, str]:
            from back.core.databricks import VolumeFileService
            from back.core.graphdb.ladybugdb.GraphSyncService import GraphSyncService

            uc = VolumeFileService(host=host, token=token)
            svc = GraphSyncService(uc, db_name, local_base=db_path)
            return svc.sync_from_volume(uc_domain_path)

        return _restore

    def _create_ladybug(
        self, domain: Any, settings: Optional[Any] = None,
    ) -> Optional[Any]:
        """Instantiate a LadybugDB store, choosing graph or flat model."""
        try:
            db_path = DEFAULT_LADYBUG_PATH
            base_name = (domain.info or {}).get("name", DEFAULT_GRAPH_NAME)
            version = getattr(domain, 'current_version', '1') or '1'
            db_name = f"{base_name}_V{version}"
            ontology = getattr(domain, 'ontology', None)
            if callable(ontology):
                ontology = None

            auto_restore = self._build_auto_restore(
                domain, settings, db_name, db_path,
            )

            if ontology:
                from back.core.graphdb.ladybugdb.LadybugGraphStore import LadybugGraphStore
                return LadybugGraphStore(
                    db_path=db_path, db_name=db_name,
                    ontology=ontology, auto_restore=auto_restore,
                )
            else:
                from back.core.graphdb.ladybugdb.LadybugFlatStore import LadybugFlatStore
                return LadybugFlatStore(
                    db_path=db_path, db_name=db_name,
                    auto_restore=auto_restore,
                )
        except ImportError as e:
            logger.warning("LadybugDB requires real_ladybug: %s", e)
            return None
        except Exception as e:
            logger.exception("Failed to create LadybugDB store: %s", e)
            return None

    @classmethod
    def get_graphdb(
        cls,
        domain: Any,
        settings: Optional[Any] = None,
        engine: Optional[str] = None,
    ) -> Optional[Any]:
        """Convenience wrapper using the package singleton factory instance."""
        return _get_factory_singleton().create(
            domain, settings=settings, engine=engine,
        )


_factory_singleton: Optional[GraphDBFactory] = None


def _get_factory_singleton() -> GraphDBFactory:
    global _factory_singleton
    if _factory_singleton is None:
        _factory_singleton = GraphDBFactory()
    return _factory_singleton


try:
    from back.core.graphdb.ladybugdb.LadybugFlatStore import LadybugFlatStore  # noqa: F401
    from back.core.graphdb.ladybugdb.LadybugGraphStore import LadybugGraphStore  # noqa: F401

    GraphDBFactory.LADYBUG_AVAILABLE = True
except ImportError:
    logger.debug("LadybugDB backends not available (optional dependency)")
