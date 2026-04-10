"""Factory for creating triple store backends from project configuration.

Supports two backend types:
- ``"view"``  -- DeltaTripleStore (SQL against a Unity Catalog VIEW via warehouse)
- ``"graph"`` -- LadybugTripleStore (embedded graph database)
"""
from typing import Any, Callable, Optional, Tuple

from back.core.databricks import is_databricks_app
from back.core.helpers import get_databricks_host_and_token, resolve_warehouse_id
from back.core.logging import get_logger
from shared.config.constants import DEFAULT_GRAPH_NAME

logger = get_logger(__name__)


class TripleStoreFactory:
    """Construct triple-store backend instances from project configuration."""

    LADYBUG_AVAILABLE = False

    def create(
        self,
        project: Any,
        settings: Optional[Any] = None,
        backend: Optional[str] = None,
    ) -> Optional[Any]:
        """Create a triple store backend.

        Args:
            project: Project session with info and databricks config.
            settings: Optional application settings (for sql_warehouse_id fallback).
            backend: ``"view"`` for DeltaTripleStore, ``"graph"`` for LadybugDB.
                     Defaults to ``"graph"`` when *None*.

        Returns:
            TripleStoreBackend instance or *None* if configuration is incomplete.
        """
        if backend is None:
            backend = "graph"

        if backend == "view":
            return self._create_delta(project, settings)

        if backend == "graph":
            return self._create_ladybug(project, settings)

        logger.warning("Unknown triplestore backend: %s", backend)
        return None

    def _create_delta(self, project: Any, settings: Optional[Any]) -> Optional[Any]:
        """Instantiate a DeltaTripleStore backed by a Databricks SQL warehouse."""
        try:
            from back.core.databricks import DatabricksClient
            from back.core.triplestore.delta.DeltaTripleStore import DeltaTripleStore

            if settings is not None:
                host, token = get_databricks_host_and_token(project, settings)
                warehouse_id = resolve_warehouse_id(project, settings)
            else:
                db = project.databricks or {}
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

    @staticmethod
    def _build_auto_restore(
        project: Any,
        settings: Optional[Any],
        db_name: str,
        db_path: str,
    ) -> Optional[Callable[[], Tuple[bool, str]]]:
        """Build a callback that restores a .lbug file from the registry.

        Returns *None* when the registry or Databricks credentials are
        not configured — in that case auto-restore is simply disabled.
        """
        uc_project_path = getattr(project, 'uc_project_path', '') or ''
        if not uc_project_path:
            return None

        if settings is not None:
            host, token = get_databricks_host_and_token(project, settings)
        else:
            db_cfg = getattr(project, 'databricks', None) or {}
            host = db_cfg.get('host', '')
            token = db_cfg.get('token', '')

        if not host and not is_databricks_app():
            return None
        if not token and not is_databricks_app():
            return None

        def _restore() -> Tuple[bool, str]:
            from back.core.databricks import VolumeFileService
            from back.core.triplestore.ladybugdb.GraphSyncService import GraphSyncService

            uc = VolumeFileService(host=host, token=token)
            svc = GraphSyncService(uc, db_name, local_base=db_path)
            return svc.sync_from_volume(uc_project_path)

        return _restore

    def _create_ladybug(
        self, project: Any, settings: Optional[Any] = None,
    ) -> Optional[Any]:
        """Instantiate a LadybugDB store, choosing graph or flat model."""
        try:
            lb_cfg = getattr(project, 'ladybug', None) or {}
            if not lb_cfg and hasattr(project, 'triplestore'):
                lb_cfg = (project.triplestore or {}).get('ladybug', {})
            db_path = lb_cfg.get("db_path", "/tmp/ontobricks")
            base_name = (project.info or {}).get("name", DEFAULT_GRAPH_NAME)
            version = getattr(project, 'current_version', '1') or '1'
            db_name = f"{base_name}_V{version}"
            ontology = getattr(project, 'ontology', None)
            if callable(ontology):
                ontology = None

            auto_restore = self._build_auto_restore(
                project, settings, db_name, db_path,
            )

            if ontology:
                from back.core.triplestore.ladybugdb.LadybugGraphStore import LadybugGraphStore
                return LadybugGraphStore(
                    db_path=db_path, db_name=db_name,
                    ontology=ontology, auto_restore=auto_restore,
                )
            else:
                from back.core.triplestore.ladybugdb.LadybugFlatStore import LadybugFlatStore
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
    def get_triplestore(
        cls,
        project: Any,
        settings: Optional[Any] = None,
        backend: Optional[str] = None,
    ) -> Optional[Any]:
        """Convenience wrapper using the package singleton factory instance."""
        return _get_factory_singleton().create(
            project, settings=settings, backend=backend,
        )


_factory_singleton: Optional[TripleStoreFactory] = None


def _get_factory_singleton() -> TripleStoreFactory:
    global _factory_singleton
    if _factory_singleton is None:
        _factory_singleton = TripleStoreFactory()
    return _factory_singleton


try:
    from back.core.triplestore.ladybugdb.LadybugFlatStore import LadybugFlatStore  # noqa: F401
    from back.core.triplestore.ladybugdb.LadybugGraphStore import LadybugGraphStore  # noqa: F401

    TripleStoreFactory.LADYBUG_AVAILABLE = True
except ImportError:
    logger.debug("LadybugDB backends not available (optional dependency)")
