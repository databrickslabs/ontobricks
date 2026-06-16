"""Factory for creating graph database backends from domain session configuration.

The default (and currently only) engine is ``"lakebase"`` (flat triple
tables on Lakebase Postgres). The factory keeps a pluggable
dispatcher so other engines can be added without rewriting callers —
copy ``_starter_kit/`` into ``back/core/graphdb/<engine>/`` and register
a new ``_create_<engine>`` branch below. The *engine_config* JSON is
engine-specific (admin: Settings → Graph DB).
"""

from typing import Any, Dict, Optional

from back.core.logging import get_logger

logger = get_logger(__name__)


class GraphDBFactory:
    """Construct graph DB backend instances from domain session configuration."""

    LAKEBASE_AVAILABLE = False

    def create(
        self,
        domain: Any,
        settings: Optional[Any] = None,
        engine: Optional[str] = None,
        engine_config: Optional[Dict[str, Any]] = None,
    ) -> Optional[Any]:
        """Create a graph DB backend.

        Args:
            domain: Domain session with info and databricks config.
            settings: Optional application settings.
            engine: ``"lakebase"`` (default — currently the only supported engine).
            engine_config: Engine-specific JSON configuration set by the
                           admin in Settings > Graph DB.

        Returns:
            GraphDBBackend instance or *None* if configuration is incomplete.
        """
        if engine is None:
            engine = "lakebase"
        if engine_config is None:
            engine_config = {}

        if engine == "lakebase":
            return self._create_lakebase(domain, settings, engine_config=engine_config)

        logger.warning("Unknown graph DB engine: %s", engine)
        return None

    def _create_lakebase(
        self,
        domain: Any,
        settings: Optional[Any] = None,
        *,
        engine_config: Optional[Dict[str, Any]] = None,
    ) -> Optional[Any]:
        """Instantiate :class:`LakebaseFlatStore` on the bound Lakebase instance."""
        try:
            from back.core.graphdb.lakebase import LAKEBASE_AVAILABLE
            from back.core.graphdb.lakebase.LakebaseFlatStore import (
                LakebaseFlatStore,
                SYNC_MODE_APP,
                SYNC_MODE_MANAGED,
                resolve_lakebase_graph_schema,
            )
            from back.core.graphdb.lakebase.SyncedTableManager import (
                DEFAULT_TIMEOUT_S as _SYNC_DEFAULT_TIMEOUT_S,
            )
            from back.core.databricks import get_lakebase_auth
        except ImportError as e:
            logger.warning("Lakebase graph engine requires psycopg: %s", e)
            return None

        if not LAKEBASE_AVAILABLE:
            logger.warning("Lakebase graph backend unavailable (psycopg not installed)")
            return None

        cfg = engine_config or {}
        schema_raw = (cfg.get("schema") or "").strip()
        database_override = str(cfg.get("database") or "").strip()
        sync_mode = str(cfg.get("sync_mode") or SYNC_MODE_APP).strip() or SYNC_MODE_APP
        if sync_mode not in (SYNC_MODE_APP, SYNC_MODE_MANAGED):
            logger.warning(
                "Unknown sync_mode %r in graph_engine_config — falling back to %s",
                sync_mode,
                SYNC_MODE_APP,
            )
            sync_mode = SYNC_MODE_APP
        sync_table_mode = str(cfg.get("sync_table_mode") or "snapshot").strip() or "snapshot"
        sync_timeout_s = int(cfg.get("sync_timeout_s") or _SYNC_DEFAULT_TIMEOUT_S)
        sync_uc_catalog = str(cfg.get("sync_uc_catalog") or "").strip()
        sync_uc_schema_override = str(cfg.get("sync_uc_schema") or "").strip()

        try:
            schema = resolve_lakebase_graph_schema(domain, settings, str(schema_raw))
        except ValueError as exc:
            logger.warning("Invalid lakebase graph schema: %s", exc)
            return None

        # UC schema segment for the synced-table FQN.
        # Priority:
        #   1. Explicit graph_engine_config.sync_uc_schema (user override via Settings UI)
        #   2. Postgres graph schema — Lakebase places the _sync foreign table in the
        #      Postgres schema that matches this UC segment, so it must equal the graph
        #      schema where all other graph tables live.
        sync_uc_schema = sync_uc_schema_override or schema

        branch_path = str(cfg.get("lakebase_branch") or "").strip()
        try:
            if branch_path:
                from back.core.databricks.LakebaseAuth import BranchLakebaseAuth

                auth = BranchLakebaseAuth(branch_path, database_override)
                logger.info(
                    "Graph engine using explicit branch %r (database=%r)",
                    branch_path,
                    database_override,
                )
            else:
                auth = get_lakebase_auth()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Lakebase auth unavailable for graph engine: %s", exc)
            return None

        if not getattr(auth, "is_available", False):
            logger.warning(
                "Lakebase graph engine selected but PGHOST/PGUSER are not configured"
                " (branch=%r)",
                branch_path or "<bound>",
            )
            return None

        synced_manager = None
        if sync_mode == SYNC_MODE_MANAGED:
            synced_manager = self._build_synced_manager(
                auth, database_override
            )
            if synced_manager is None:
                logger.warning(
                    "managed_synced requested but SyncedTableManager could not be built — "
                    "falling back to app_managed for this store"
                )
                sync_mode = SYNC_MODE_APP

        try:
            return LakebaseFlatStore(
                auth,
                schema=schema,
                database_override=database_override,
                sync_mode=sync_mode,
                sync_table_mode=sync_table_mode,
                sync_timeout_s=sync_timeout_s,
                sync_uc_catalog=sync_uc_catalog,
                sync_uc_schema=sync_uc_schema,
                synced_manager=synced_manager,
            )
        except Exception as e:
            logger.exception("Failed to create Lakebase graph store: %s", e)
            return None

    @staticmethod
    def _build_synced_manager(auth: Any, database_override: str) -> Optional[Any]:
        """Build a SyncedTableManager with Autoscaling project + branch targeting.

        Passes ``database_project`` + ``database_branch`` (not
        ``database_instance_name``) so the Lakebase control-plane creates the
        synced table in the exact branch the catalog is connected to (e.g.
        ``demo``) rather than the project's default/production branch.
        """
        try:
            from back.core.graphdb.lakebase.SyncedTableManager import (
                SyncedTableManager,
            )

            project_name = auth.instance_name  # e.g. "ontobricks-app"
            branch_name = auth.branch_name      # e.g. "demo"
            logical_db = (database_override or auth.database or "").strip()
            logger.info(
                "Building SyncedTableManager for project=%r branch=%r logical_db=%r",
                project_name,
                branch_name,
                logical_db,
            )
            return SyncedTableManager(
                project_name=project_name,
                branch_name=branch_name,
                logical_db=logical_db,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Could not build SyncedTableManager (%s) — managed_synced disabled "
                "for this store",
                exc,
            )
            return None

    @classmethod
    def get_graphdb(
        cls,
        domain: Any,
        settings: Optional[Any] = None,
        engine: Optional[str] = None,
        engine_config: Optional[Dict[str, Any]] = None,
    ) -> Optional[Any]:
        """Convenience wrapper using the package singleton factory instance."""
        return _get_factory_singleton().create(
            domain,
            settings=settings,
            engine=engine,
            engine_config=engine_config,
        )


_factory_singleton: Optional[GraphDBFactory] = None


def _get_factory_singleton() -> GraphDBFactory:
    global _factory_singleton
    if _factory_singleton is None:
        _factory_singleton = GraphDBFactory()
    return _factory_singleton


try:
    from back.core.graphdb.lakebase import LAKEBASE_AVAILABLE as _LB_AVAIL  # noqa: F401

    GraphDBFactory.LAKEBASE_AVAILABLE = bool(_LB_AVAIL)
except ImportError:
    logger.debug("Lakebase graph backends not available (optional dependency)")
