"""Factory for creating triple store backends from domain session configuration.

Supports:
- ``"view"``  -- DeltaTripleStore (SQL against a Unity Catalog VIEW via warehouse)
- ``"graph"`` -- delegates to :class:`GraphDBFactory` (currently Lakebase only,
  pluggable via ``back/core/graphdb/<engine>/``)
"""

from typing import Any, Dict, Optional, Tuple

from back.core.databricks import is_databricks_app
from back.core.helpers import (
    get_databricks_host_and_token,
    resolve_warehouse_id,
)
from back.core.logging import get_logger

logger = get_logger(__name__)


class TripleStoreFactory:
    """Construct triple-store backend instances from domain session configuration."""

    GRAPHDB_AVAILABLE = False

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

            engine = self._resolve_graph_engine(domain, settings) or "lakebase"
            engine_config = self._resolve_graph_engine_config(domain, settings)
            return get_graphdb(
                domain, settings, engine=engine, engine_config=engine_config or {}
            )

        logger.warning("Unknown triplestore backend: %s", backend)
        return None

    @staticmethod
    def _read_global_config(domain: Any, settings: Optional[Any], accessor, *, force: bool = False):
        """Call *accessor(global_config_service, host, token, registry_cfg)*.

        Returns ``None`` on any error (registry not configured, etc.).

        When *force* is ``True`` the ``GlobalConfigService`` in-memory cache is
        bypassed and a fresh read is performed against the backing store.  This
        is important for build-time resolution: the cache may hold the empty
        template (``_empty()``) from a cold-start race where Lakebase was
        briefly unavailable, while the Settings UI correctly shows the saved
        value because it always uses ``force=True``.  Without bypassing the
        cache the build would silently fall back to ``app_managed`` even though
        ``managed_synced`` is configured.
        """
        try:
            from back.objects.session.GlobalConfigService import global_config_service

            if settings is not None:
                host, token = get_databricks_host_and_token(domain, settings)
            else:
                db = getattr(domain, "databricks", None) or {}
                host = db.get("host", "")
                token = db.get("token", "")
            from back.objects.registry import RegistryCfg

            registry_cfg = RegistryCfg.from_domain(domain, settings).as_dict()
            if force:
                global_config_service.load(host, token, registry_cfg, force=True)
            return accessor(global_config_service, host, token, registry_cfg)
        except Exception as exc:
            logger.debug("Could not read global config: %s", exc)
            return None

    @staticmethod
    def _registry_graph_engine_mirror(domain: Any) -> Tuple[Optional[str], Dict[str, Any]]:
        """Best-effort read of graph DB fields mirrored under ``domain.settings['registry']``.

        ``SettingsService`` copies the persisted engine choice here after a
        successful admin save.  The mirror is checked as a fallback when
        :meth:`GlobalConfigService.load` returns the empty template (e.g.
        registry catalog/schema not yet wired into the dict passed to ``load``).
        """
        try:
            blob = getattr(domain, "settings", None)
            if not isinstance(blob, dict):
                return None, {}
            reg = blob.get("registry")
            if not isinstance(reg, dict):
                return None, {}
            from back.objects.session.GlobalConfigService import GlobalConfigService

            allowed = GlobalConfigService.ALLOWED_GRAPH_ENGINES
            raw_eng = (reg.get("graph_engine") or "").strip().lower()
            eng = raw_eng if raw_eng in allowed else None
            cfg_raw = reg.get("graph_engine_config")
            cfg: Dict[str, Any] = dict(cfg_raw) if isinstance(cfg_raw, dict) else {}
            return eng, cfg
        except Exception as exc:  # noqa: BLE001
            logger.debug("Could not read registry graph_engine mirror: %s", exc)
            return None, {}

    @staticmethod
    def _resolve_graph_engine(domain: Any, settings: Optional[Any], *, force: bool = False) -> Optional[str]:
        """Read the configured graph engine from ``GlobalConfigService``.

        Falls back to the domain-level registry mirror when global resolution
        is unavailable (e.g. registry not yet wired up).

        Pass *force=True* to bypass the in-memory cache (required for build-time
        calls to avoid a cold-start race where the cache holds ``_empty()``).
        """
        gcs_val = TripleStoreFactory._read_global_config(
            domain,
            settings,
            lambda gcs, h, t, r: gcs.get_graph_engine(h, t, r),
            force=force,
        )
        if gcs_val is not None:
            return gcs_val
        mirrored_eng, _ = TripleStoreFactory._registry_graph_engine_mirror(domain)
        return mirrored_eng

    @staticmethod
    def _resolve_graph_engine_config(
        domain: Any, settings: Optional[Any], *, force: bool = False
    ) -> Optional[dict]:
        """Read the engine-specific JSON config from ``GlobalConfigService``.

        Pass *force=True* to bypass the in-memory cache (required for build-time
        calls to avoid a cold-start race where the cache holds ``_empty()``).
        """
        raw = TripleStoreFactory._read_global_config(
            domain,
            settings,
            lambda gcs, h, t, r: gcs.get_graph_engine_config(h, t, r),
            force=force,
        )
        gcs_cfg: Dict[str, Any] = raw if isinstance(raw, dict) else {}
        _, mirrored_cfg = TripleStoreFactory._registry_graph_engine_mirror(domain)

        # Persisted global keys win on overlap; mirror fills gaps when load()
        # returned the empty template or the config read failed.
        if mirrored_cfg:
            return {**mirrored_cfg, **gcs_cfg}
        return gcs_cfg

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
            domain,
            settings=settings,
            backend=backend,
        )


_factory_singleton: Optional[TripleStoreFactory] = None


def _get_factory_singleton() -> TripleStoreFactory:
    global _factory_singleton
    if _factory_singleton is None:
        _factory_singleton = TripleStoreFactory()
    return _factory_singleton


try:
    from back.core.graphdb.GraphDBFactory import GraphDBFactory

    TripleStoreFactory.GRAPHDB_AVAILABLE = bool(GraphDBFactory.LAKEBASE_AVAILABLE)
except ImportError:
    logger.debug("Graph DB backends not available (optional dependency)")
