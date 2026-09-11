"""Factory for creating graph database backends from domain session configuration.

A single entry point (:meth:`GraphDBFactory.create` / :func:`get_graphdb`)
constructs the graph DB backend for a domain:

* ``engine=None`` — auto-resolve the engine from global/registry config
  (``lakebase`` by default, ``delta`` when ``triple_store_backend`` is
  ``databricks``).  This is the common path callers use.
* ``engine="lakebase"`` — flat triple tables on Lakebase Postgres.
* ``engine="delta"`` — materialized Delta triple tables in Unity Catalog.
* ``engine="view"`` — a raw, read-only Delta store bound to a SQL warehouse
  (health probes against a UC view/table).

New engines are pluggable — copy ``_starter_kit/`` into
``back/core/graphdb/<engine>/`` and register a ``_create_<engine>`` branch.
The *engine_config* JSON is engine-specific (admin: Settings → Graph DB).
"""

from typing import Any, Dict, Optional, Tuple

from back.core.logging import get_logger

logger = get_logger(__name__)

# Unified per-domain graph backend vocabulary.  A domain stores exactly one of
# these in ``DomainSession.info['graph_backend']`` (mandatory).  Each value maps
# to a ``triple_store_backend`` + ``graph_engine`` pair used internally by the
# factory:
#   ``lakebase``   -> triple_store_backend=lakebase,  graph_engine=lakebase
#   ``databricks`` -> triple_store_backend=databricks (Delta)
#   ``neo4j``      -> triple_store_backend=lakebase,  graph_engine=neo4j
#   ``none``       -> ontology-only domain: no graph is ever built, no store is
#                     created, and the graph-dependent features are disabled.
GRAPHLESS_BACKEND = "none"
GRAPH_BACKENDS: Tuple[str, ...] = ("lakebase", "databricks", "neo4j", GRAPHLESS_BACKEND)
DEFAULT_GRAPH_BACKEND = "lakebase"

# How the Lakehouse backend shapes the ``…_data`` relation, stored per domain in
# ``DomainSession.info['lakehouse_materialization']``:
#   ``table`` -> CTAS the gateway VIEW into a clustered Delta TABLE (a copy)
#   ``view``  -> a pass-through VIEW over the gateway VIEW (no copy)
# Only ever ``view`` for the ``databricks`` backend: Lakebase and Neo4j domains
# need the materialized snapshot for Graph Analytics.
LAKEHOUSE_MATERIALIZATIONS: Tuple[str, ...] = ("table", "view")
DEFAULT_LAKEHOUSE_MATERIALIZATION = "table"


def normalize_graph_backend(value: Optional[str]) -> str:
    """Return a valid per-domain graph backend, defaulting to ``lakebase``."""
    v = (value or "").strip().lower()
    return v if v in GRAPH_BACKENDS else DEFAULT_GRAPH_BACKEND


def is_graphless_backend(value: Optional[str]) -> bool:
    """True when the (normalized) backend is the ontology-only ``none`` type."""
    return normalize_graph_backend(value) == GRAPHLESS_BACKEND


def normalize_lakehouse_materialization(value: Optional[str]) -> str:
    """Return a valid Lakehouse materialization mode, defaulting to ``table``."""
    v = (value or "").strip().lower()
    return v if v in LAKEHOUSE_MATERIALIZATIONS else DEFAULT_LAKEHOUSE_MATERIALIZATION


class GraphDBFactory:
    """Construct graph DB backend instances from domain session configuration."""

    LAKEBASE_AVAILABLE = False
    NEO4J_AVAILABLE = False

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
            engine: One of ``None`` (auto-resolve from config), ``"lakebase"``,
                    ``"delta"``, or ``"view"`` (raw read-only Delta store).
            engine_config: Engine-specific JSON configuration set by the
                           admin in Settings > Graph DB.

        Returns:
            GraphDBBackend instance or *None* if configuration is incomplete.
        """
        if engine is None:
            return self._create_auto(domain, settings)

        if engine == "view":
            return self._create_delta_view(domain, settings)

        if engine_config is None:
            engine_config = {}

        from back.core.graphdb.engine_config import lakebase_section, neo4j_section

        if engine == "lakebase":
            return self._create_lakebase(
                domain, settings, engine_config=lakebase_section(engine_config)
            )

        if engine == "neo4j":
            return self._create_neo4j(
                domain, settings, engine_config=neo4j_section(engine_config)
            )

        if engine == "delta":
            return self._create_delta(domain, settings)

        logger.warning("Unknown graph DB engine: %s", engine)
        return None

    def _create_neo4j(
        self,
        domain: Any,
        settings: Optional[Any] = None,
        *,
        engine_config: Optional[Dict[str, Any]] = None,
    ) -> Optional[Any]:
        """Instantiate :class:`Neo4jStore` against a named Settings connection.

        Resolves ``domain.info.neo4j_connection`` against
        ``engine_config.connections`` (or a nested graph_engine_config root).
        The matched profile supplies URI / database / auth fields.
        """
        try:
            from back.core.graphdb.engine_config import (
                resolve_neo4j_connection,
            )
            from back.core.graphdb.neo4j import NEO4J_AVAILABLE
            from back.core.graphdb.neo4j.Neo4jStore import Neo4jStore
            from shared.config.constants import DEFAULT_GRAPH_NAME
        except ImportError as e:
            logger.warning("Neo4j graph engine requires the 'neo4j' driver: %s", e)
            return None

        if not NEO4J_AVAILABLE:
            logger.warning("Neo4j graph backend unavailable (neo4j driver not installed)")
            return None

        info = domain.info or {}
        conn_name = str(info.get("neo4j_connection") or "").strip()
        if not conn_name:
            logger.warning(
                "Neo4jStore: domain has no neo4j_connection — pick one in "
                "Domain → Information → Knowledge Graph"
            )
            return None

        # engine_config may be the neo4j section or the nested root.
        root_or_section = engine_config if isinstance(engine_config, dict) else {}
        profile = resolve_neo4j_connection(root_or_section, conn_name)
        if not profile and (
            "lakebase" in root_or_section
            or "neo4j" in root_or_section
            or "connections" not in root_or_section
        ):
            # When create() already passed neo4j_section(...), wrap it so
            # resolve_neo4j_connection can still see connections[].
            profile = resolve_neo4j_connection({"neo4j": root_or_section}, conn_name)
        if not profile:
            logger.warning(
                "Neo4jStore: connection %r not found in Settings → Neo4j",
                conn_name,
            )
            return None

        cfg = dict(profile)
        base_name = info.get("name", DEFAULT_GRAPH_NAME)
        version = getattr(domain, "current_version", "1") or "1"
        db_name = "%s_V%s" % (base_name, version)
        try:
            return Neo4jStore(db_name=db_name, engine_config=cfg)
        except (ValueError, NotImplementedError) as exc:
            logger.warning("Neo4jStore configuration error: %s", exc)
            return None
        except Exception as e:  # noqa: BLE001
            logger.exception("Failed to create Neo4jStore: %s", e)
            return None

    def _create_auto(
        self, domain: Any, settings: Optional[Any] = None
    ) -> Optional[Any]:
        """Resolve the engine from global/registry config and dispatch.

        Mirrors the former ``TripleStoreFactory`` ``backend="graph"`` behaviour.
        """
        if is_graphless_backend(self._resolve_graph_backend(domain)):
            # Ontology-only domain: there is no graph store to build or query.
            logger.debug("Graph backend is 'none'; no store is created.")
            return None
        ts_backend = self._resolve_triple_store_backend(domain, settings)
        if ts_backend == "databricks":
            return self.create(domain, settings, engine="delta", engine_config={})

        engine = self._resolve_graph_engine(domain, settings) or "lakebase"
        engine_config = self._resolve_graph_engine_config(domain, settings)
        return self.create(
            domain, settings, engine=engine, engine_config=engine_config or {}
        )

    # ------------------------------------------------------------------
    # Config resolution (formerly on TripleStoreFactory)
    # ------------------------------------------------------------------

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
            from back.core.helpers import get_databricks_host_and_token

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
    def _resolve_graph_backend(domain: Any) -> str:
        """Return the mandatory per-domain graph backend.

        The choice lives in ``DomainSession.info['graph_backend']`` (set from the
        Domain Information -> Knowledge Graph tab) and is versioned with the
        domain.  Missing/invalid values default to ``lakebase`` so pre-existing
        domains keep working.
        """
        try:
            info = getattr(domain, "info", None)
            if isinstance(info, dict):
                return normalize_graph_backend(info.get("graph_backend"))
        except Exception as exc:  # noqa: BLE001
            logger.debug("Could not read per-domain graph_backend: %s", exc)
        return DEFAULT_GRAPH_BACKEND

    @staticmethod
    def _resolve_graph_engine(
        domain: Any, settings: Optional[Any] = None, *, force: bool = False
    ) -> Optional[str]:
        """Resolve the graph engine from the per-domain backend choice.

        ``settings``/``force`` are accepted for call-site compatibility but no
        longer consulted — the selection is purely per-domain now.
        """
        backend = GraphDBFactory._resolve_graph_backend(domain)
        return "neo4j" if backend == "neo4j" else "lakebase"

    @staticmethod
    def _resolve_triple_store_backend(
        domain: Any, settings: Optional[Any] = None, *, force: bool = False
    ) -> str:
        """Resolve the triple-store backend from the per-domain backend choice."""
        backend = GraphDBFactory._resolve_graph_backend(domain)
        return "databricks" if backend == "databricks" else "lakebase"

    @staticmethod
    def resolve_lakehouse_materialization(
        domain: Any, settings: Optional[Any] = None
    ) -> str:
        """Return how this domain's ``…_data`` relation must be built.

        ``view`` only when the domain runs on the Lakehouse backend and asked
        for it; every other combination materializes, because Lakebase and
        Neo4j domains have no other source for Graph Analytics.
        """
        if GraphDBFactory._resolve_triple_store_backend(domain, settings) != "databricks":
            return DEFAULT_LAKEHOUSE_MATERIALIZATION
        try:
            info = getattr(domain, "info", None)
            if isinstance(info, dict):
                return normalize_lakehouse_materialization(
                    info.get("lakehouse_materialization")
                )
        except Exception as exc:  # noqa: BLE001
            logger.debug("Could not read lakehouse_materialization: %s", exc)
        return DEFAULT_LAKEHOUSE_MATERIALIZATION

    @staticmethod
    def _resolve_graph_engine_config(
        domain: Any, settings: Optional[Any] = None, *, force: bool = False
    ) -> Optional[dict]:
        """Read the engine-specific connection JSON config from ``GlobalConfigService``.

        Engine *connection* configuration (Neo4j Bolt creds, Lakebase schema /
        sync options) remains workspace-global — only the backend *selection*
        moved per-domain.  Pass *force=True* to bypass the in-memory cache.
        """
        raw = GraphDBFactory._read_global_config(
            domain,
            settings,
            lambda gcs, h, t, r: gcs.get_graph_engine_config(h, t, r),
            force=force,
        )
        return raw if isinstance(raw, dict) else {}

    # ------------------------------------------------------------------
    # Engine constructors
    # ------------------------------------------------------------------

    def _create_delta_view(
        self, domain: Any, settings: Optional[Any] = None
    ) -> Optional[Any]:
        """Instantiate a raw, read-only :class:`DeltaFlatStore` on a SQL warehouse.

        Bound with ``domain=None`` so it operates directly on the FQNs passed in
        (health probes against a UC view/table).  Formerly ``backend="view"``.
        """
        try:
            from back.core.databricks import DatabricksClient, is_databricks_app
            from back.core.helpers import (
                get_databricks_host_and_token,
                resolve_delta_warehouse_id,
            )
            from back.core.graphdb.delta.DeltaFlatStore import DeltaFlatStore

            if settings is not None:
                host, token = get_databricks_host_and_token(domain, settings)
                warehouse_id = resolve_delta_warehouse_id(domain, settings)
            else:
                db = domain.databricks or {}
                host = db.get("host", "")
                token = db.get("token", "")
                warehouse_id = ""
            if not host and not is_databricks_app():
                logger.warning("Delta view store: missing host")
                return None
            if not token and not is_databricks_app():
                logger.warning("Delta view store: missing token")
                return None
            if not warehouse_id:
                logger.warning("Delta view store: missing sql_warehouse_id")
                return None
            client = DatabricksClient(
                host=host,
                token=token,
                warehouse_id=warehouse_id,
            )
            return DeltaFlatStore(client)
        except Exception as e:
            logger.exception("Failed to create Delta view store: %s", e)
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
            from back.core.graphdb.lakebase.LakebaseBase import (
                resolve_postgres_database_override,
            )
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
        database_override = resolve_postgres_database_override(cfg)
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
                from back.core.databricks.lakebase import BranchLakebaseAuth

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

    def _create_delta(
        self,
        domain: Any,
        settings: Optional[Any] = None,
    ) -> Optional[Any]:
        """Instantiate :class:`DeltaFlatStore` on SQL Warehouse."""
        try:
            from back.core.graphdb.delta.DeltaBase import create_databricks_client
            from back.core.graphdb.delta.DeltaFlatStore import DeltaFlatStore
        except ImportError as exc:
            logger.warning("Delta graph engine unavailable: %s", exc)
            return None

        client = create_databricks_client(domain, settings)
        if client is None:
            return None
        try:
            return DeltaFlatStore(client, domain=domain, settings=settings)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to create DeltaFlatStore: %s", exc)
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

try:
    from back.core.graphdb.neo4j import NEO4J_AVAILABLE as _NEO4J_AVAIL  # noqa: F401

    GraphDBFactory.NEO4J_AVAILABLE = bool(_NEO4J_AVAIL)
except ImportError:
    logger.debug("Neo4j graph backend not available (optional dependency)")
