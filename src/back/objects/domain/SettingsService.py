"""Databricks settings, registry, permissions, and schedule orchestration."""

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple

from back.core.errors import (
    AuthorizationError,
    InfrastructureError,
    NotFoundError,
    OntoBricksError,
    ValidationError,
)
from shared.config.settings import Settings
from back.core.databricks import is_databricks_app
from back.core.helpers import (
    get_databricks_client,
    get_databricks_host_and_token,
    resolve_warehouse_id,
    run_blocking,
)
from back.core.logging import get_logger
from back.objects.registry import (
    ASSIGNABLE_ROLES,
    RegistryCfg,
    RegistryService,
    permission_service,
    invalidate_registry_cache,
    obx_format,
)
from back.objects.session import (
    SessionManager,
    get_domain,
    global_config_service,
    sanitize_domain_folder,
)

logger = get_logger(__name__)


class SettingsService:
    """Configuration, registry, permissions, and build schedules."""

    @staticmethod
    def _get_scheduler():
        """Defer APScheduler import until schedule endpoints run."""
        from back.objects.registry import get_scheduler as _gs

        return _gs()

    @staticmethod
    def is_warehouse_locked(settings: Settings) -> bool:
        """True when the SQL Warehouse is supplied by a Databricks App resource."""
        return is_databricks_app() and bool(settings.sql_warehouse_id)

    @staticmethod
    def is_registry_locked(settings: Settings) -> bool:
        """True when registry params are injected by Apps (not editable via .env).

        Covers two binding styles:
        - Volume backend: Apps injects REGISTRY_VOLUME_PATH.
        - Lakebase backend: Apps injects PGHOST from the database resource.
        """
        if not is_databricks_app():
            return False
        import os
        return bool(
            getattr(settings, "registry_volume_path", "")
            or os.environ.get("PGHOST", "")
        )

    @staticmethod
    def _resolve_context(session_mgr: SessionManager, settings: Settings):
        """Return the (domain, host, token, registry_cfg_dict) tuple used by most endpoints."""
        domain = get_domain(session_mgr)
        host, token = get_databricks_host_and_token(domain, settings)
        registry_cfg = RegistryCfg.from_domain(domain, settings).as_dict()
        return domain, host, token, registry_cfg

    @staticmethod
    def _mirror_graph_engine_to_domain_registry(
        session_mgr: SessionManager,
        *,
        engine: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Copy graph DB settings into ``domain.settings['registry']`` (best-effort).

        Authoritative persistence is :class:`GlobalConfigService` via
        :meth:`RegistryStore.save_global_config` (Volume ``.global_config.json``
        or Lakebase ``global_config`` JSONB). Mirroring keeps the domain JSON
        export aligned with the catalog/schema/volume block for operators.
        """
        if engine is None and config is None:
            return
        try:
            domain = get_domain(session_mgr)
            reg = domain.settings.setdefault("registry", {})
            if engine is not None:
                reg["graph_engine"] = engine
            if config is not None:
                reg["graph_engine_config"] = dict(config)
            domain.save()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Could not mirror graph engine fields to domain.settings.registry: %s",
                exc,
            )

    @staticmethod
    def require_admin_error(
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> None:
        """Raise :class:`AuthorizationError` if the caller is not an admin in Databricks App mode."""
        if not is_databricks_app():
            return

        _, host, token, _ = SettingsService._resolve_context(session_mgr, settings)
        if not permission_service.is_admin(
            email,
            host,
            token,
            settings.ontobricks_app_name,
            user_token=user_token,
        ):
            raise AuthorizationError(
                "Only admins (CAN MANAGE) can change the SQL Warehouse"
            )

    @staticmethod
    def build_current_config(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """Build the payload for GET /settings/current."""
        domain = get_domain(session_mgr)

        host = domain.databricks.get("host") or settings.databricks_host
        token = domain.databricks.get("token") or settings.databricks_token
        warehouse_id = resolve_warehouse_id(domain, settings)

        has_config = bool(host and (token or settings.databricks_token))
        is_app_mode = bool(settings.databricks_host)

        auth_mode = "none"
        auth_display = "Not configured"
        if token:
            auth_mode = "token"
            auth_display = "Personal Access Token"
        elif is_app_mode:
            auth_mode = "app"
            auth_display = "Databricks App"

        warehouse_locked = SettingsService.is_warehouse_locked(settings)

        return {
            "host": host,
            "token": "***" if token else None,
            "warehouse_id": warehouse_id,
            "from_env": is_app_mode,
            "is_app_mode": is_app_mode,
            "auth_mode": auth_mode,
            "auth_display": auth_display,
            "has_config": has_config,
            "warehouse_locked": warehouse_locked,
        }

    @staticmethod
    def apply_config_save(
        data: Dict[str, Any],
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Apply POST /settings/save body to session and optional global warehouse."""
        domain = get_domain(session_mgr)

        if data.get("host"):
            domain.databricks["host"] = data["host"]
        if data.get("token"):
            domain.databricks["token"] = data["token"]

        if data.get("warehouse_id"):
            if SettingsService.is_warehouse_locked(settings):
                raise ValidationError(
                    "SQL Warehouse is configured via Databricks App resources and cannot be changed here.",
                )

            SettingsService.require_admin_error(
                email, user_token, session_mgr, settings
            )
            domain.databricks["warehouse_id"] = data["warehouse_id"]

            _, host, token, registry_cfg = SettingsService._resolve_context(
                session_mgr, settings
            )
            ok, msg = global_config_service.set_warehouse_id(
                host,
                token,
                registry_cfg,
                data["warehouse_id"],
            )
            if not ok:
                logger.warning(
                    "Warehouse saved in session only (global config write failed: %s). "
                    "Session fallback active — catalog dropdown will still work.",
                    msg,
                )

        domain.save()
        return {"success": True, "message": "Configuration saved"}

    @staticmethod
    async def test_connection(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """Test Databricks connectivity; returns success/message dict."""
        try:
            client = get_databricks_client(get_domain(session_mgr), settings)

            if not client:
                raise ValidationError(
                    "Databricks not configured. Please set DATABRICKS_HOST and DATABRICKS_TOKEN.",
                )

            warehouses = await run_blocking(client.get_warehouses)
            return {
                "success": True,
                "message": f"Connection successful. Found {len(warehouses)} warehouses.",
            }
        except OntoBricksError:
            raise
        except AttributeError as e:
            logger.exception("Test connection AttributeError: %s", e)
            error_msg = str(e)
            if "NoneType" in error_msg and "request" in error_msg:
                raise ValidationError(
                    "Databricks SDK not properly initialized. Check your authentication configuration.",
                ) from e
            raise InfrastructureError("Test connection failed", detail=error_msg) from e
        except Exception as e:
            logger.exception("Test connection failed: %s", e)
            raise InfrastructureError("Test connection failed", detail=str(e)) from e

    @staticmethod
    async def fetch_warehouses(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """List warehouses from Databricks (``warehouses`` key on success)."""
        try:
            client = get_databricks_client(get_domain(session_mgr), settings)
            if not client:
                raise ValidationError("Databricks not configured")
            return {"warehouses": await run_blocking(client.get_warehouses)}
        except OntoBricksError:
            raise
        except AttributeError as e:
            error_msg = str(e)
            if "NoneType" in error_msg and "request" in error_msg:
                logger.warning("Warehouses HTTP client error: %s", e)
                raise ValidationError(
                    "Databricks SDK not properly initialized. Check your authentication configuration.",
                ) from e
            logger.exception("Get warehouses AttributeError: %s", e)
            raise InfrastructureError(
                "Failed to list SQL warehouses", detail=error_msg
            ) from e
        except Exception as e:
            logger.exception("Get warehouses failed: %s", e)
            raise InfrastructureError(
                "Failed to list SQL warehouses", detail=str(e)
            ) from e

    @staticmethod
    def select_warehouse(
        warehouse_id: Optional[str],
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Persist warehouse selection in session and attempt global registry update."""
        if SettingsService.is_warehouse_locked(settings):
            raise ValidationError(
                "SQL Warehouse is configured via Databricks App resources and cannot be changed here.",
            )

        if not warehouse_id:
            raise ValidationError("No warehouse ID provided")

        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        domain, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        domain.databricks["warehouse_id"] = warehouse_id
        domain.save()

        ok, msg = global_config_service.set_warehouse_id(
            host,
            token,
            registry_cfg,
            warehouse_id,
        )
        if not ok:
            logger.warning(
                "Warehouse stored in session only (global save failed: %s). "
                "Session fallback active — catalog dropdown will still work.",
                msg,
            )
            return {
                "success": True,
                "message": "Warehouse selected (stored in session — will persist globally once the registry is configured)",
            }
        return {"success": True, "message": "Warehouse selected"}

    @staticmethod
    async def fetch_catalogs(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        try:
            client = get_databricks_client(get_domain(session_mgr), settings)
            if not client:
                raise ValidationError("Databricks not configured")
            return {"catalogs": await run_blocking(client.get_catalogs)}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("Get catalogs failed: %s", e)
            raise InfrastructureError(
                "Failed to list Unity Catalog catalogs", detail=str(e)
            ) from e

    @staticmethod
    async def fetch_schemas(
        catalog: str,
        session_mgr: SessionManager,
        settings: Settings,
        *,
        log_label: str = "Get schemas",
    ) -> Dict[str, Any]:
        try:
            client = get_databricks_client(get_domain(session_mgr), settings)
            if not client:
                raise ValidationError("Databricks not configured")
            return {"schemas": await run_blocking(client.get_schemas, catalog)}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("%s failed: %s", log_label, e)
            raise InfrastructureError(f"{log_label} failed", detail=str(e)) from e

    @staticmethod
    async def fetch_volumes(
        catalog: str,
        schema: str,
        session_mgr: SessionManager,
        settings: Settings,
        log_label: str = "Get volumes",
    ) -> Dict[str, Any]:
        try:
            client = get_databricks_client(get_domain(session_mgr), settings)
            if not client:
                raise ValidationError("Databricks not configured")
            return {"volumes": await run_blocking(client.get_volumes, catalog, schema)}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("%s failed: %s", log_label, e)
            raise InfrastructureError(f"{log_label} failed", detail=str(e)) from e

    @staticmethod
    def build_registry_get_payload(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """Payload for GET /settings/registry.

        Includes the registry triplet (catalog/schema/volume) used for
        binary artefacts, the configured ``lakebase_schema`` and
        optional ``lakebase_database`` override, the **graph_engine** /
        **graph_engine_config** read from the registry global-config
        blob (same persistence as Settings → Graph DB), and a read-only
        ``lakebase`` block that surfaces the runtime-injected Postgres
        connection parameters (``PGHOST``/``PGPORT``/``PGDATABASE``/
        ``PGUSER``) plus availability/health for the admin UI.

        Lakebase is the sole registry backend: there is no
        ``available_backends`` field anymore.
        """
        rcfg = RegistryCfg.from_session(session_mgr, settings)
        initialized = False

        if rcfg.is_configured:
            try:
                svc = RegistryService.from_context(get_domain(session_mgr), settings)
                initialized = svc.is_initialized()
            except Exception:
                logger.debug("Could not check registry marker")

        graph_engine = "lakebase"
        graph_engine_config: Dict[str, Any] = {}
        if rcfg.is_configured:
            try:
                _, host, token, registry_cfg = SettingsService._resolve_context(
                    session_mgr, settings
                )
                global_config_service.load(host, token, registry_cfg)
                graph_engine = global_config_service.get_graph_engine(
                    host, token, registry_cfg
                )
                graph_engine_config = global_config_service.get_graph_engine_config(
                    host, token, registry_cfg
                )
            except Exception:
                logger.debug(
                    "Could not load graph engine for registry GET payload",
                    exc_info=True,
                )

        return {
            "success": True,
            **rcfg.as_dict(),
            "configured": initialized,
            "registry_locked": SettingsService.is_registry_locked(settings),
            "lakebase": SettingsService._lakebase_runtime_info(rcfg),
            "graph_engine": graph_engine,
            "graph_engine_config": graph_engine_config,
        }

    @staticmethod
    def _lakebase_runtime_info(rcfg: RegistryCfg) -> Dict[str, Any]:
        """Surface the read-only Lakebase connection params for the UI.

        Returns an empty block when the Lakebase resource is not bound.
        Never raises and never includes the OAuth token.

        Accepts two binding styles:
        - Apps runtime: ``PGHOST``/``PGPORT``/``PGDATABASE``/``PGUSER``
          auto-injected by the platform.
        - Local dev: ``LAKEBASE_PROJECT`` + ``LAKEBASE_BRANCH``
          + ``LAKEBASE_DATABASE`` + ``PGUSER`` — endpoint resolved via
          the Postgres API by :class:`LakebaseAuth`.

        When bound, also tries to enrich the payload with Databricks
        metadata about the bound instance (name, tier, state,
        pg_version, node_count). The lookup is best-effort and
        degrades silently on failure.

        ``database`` is the bound ``PGDATABASE`` / ``LAKEBASE_DATABASE``.
        ``database_override`` is the (optional) admin-selected override
        stored in the registry config. ``effective_database`` is
        whichever of the two the store actually connects to — the
        override wins when set, otherwise the bound database is used.
        """
        import os
        from back.core.databricks import get_lakebase_auth

        auth = get_lakebase_auth()
        override_db = getattr(rcfg, "lakebase_database", "") or ""

        if not auth.is_available:
            return {
                "project": "",
                "host": "",
                "port": "",
                "branch": "",
                "database": "",
                "database_override": override_db,
                "effective_database": override_db,
                "user": "",
                "schema": rcfg.lakebase_schema,
                "bound": False,
                "initialized": False,
                "populated": False,
                "instance": None,
            }

        host = os.environ.get("PGHOST", "")
        bound_db = os.environ.get("PGDATABASE", "") or os.environ.get("LAKEBASE_DATABASE", "")
        branch = os.environ.get("LAKEBASE_BRANCH", "")
        project = os.environ.get("LAKEBASE_PROJECT", "")
        effective_db = override_db or bound_db

        # Single probe: returns ``{initialized, populated}``. ``populated``
        # is true when the schema has the registry tables AND any of the
        # canonical data tables (domains, permission_sets, scheduled_*)
        # has at least one row. Used by the admin UI to:
        #   - hide *Migrate to Lakebase* when the admin is already on
        #     Lakebase and the tables hold data (the button doesn't make
        #     sense — it would silently overwrite live rows),
        #   - keep the button visible on Volume but downgrade it to a
        #     red *Re-sync* with a hard warning popup when Lakebase
        #     already holds data from a previous migration.
        status = SettingsService._lakebase_schema_status(rcfg)
        return {
            "project": project,
            "host": host,
            "port": os.environ.get("PGPORT", "5432"),
            "branch": branch,
            "database": bound_db,
            "database_override": override_db,
            "effective_database": effective_db,
            "user": os.environ.get("PGUSER", ""),
            "schema": rcfg.lakebase_schema,
            "bound": True,
            "initialized": status["initialized"],
            "populated": status["populated"],
            "instance": None,
        }

    @staticmethod
    def _lakebase_schema_initialized(rcfg: RegistryCfg) -> bool:
        """Best-effort probe of ``store.is_initialized()``. Never raises.

        Kept for callers that only need the boolean — internally
        :meth:`_lakebase_schema_status` is the canonical entry point
        because it returns both ``initialized`` and ``populated`` from
        a single store instance.
        """
        return SettingsService._lakebase_schema_status(rcfg)["initialized"]

    @staticmethod
    def _lakebase_schema_status(rcfg: RegistryCfg) -> Dict[str, bool]:
        """Probe ``initialized`` + ``populated`` for the Lakebase schema.

        ``initialized`` mirrors :meth:`RegistryStore.is_initialized` —
        true when the registry tables exist and a registry row matches
        this schema. ``populated`` is true when at least one of the
        canonical data tables (``domains``, ``domain_versions``,
        ``domain_permissions``, ``schedules``, ``schedule_runs``)
        carries one or more rows. Both default to ``False`` when
        psycopg is missing, the Lakebase resource is unbound, or any
        error occurs — this is purely informational UI plumbing.
        """
        result = {"initialized": False, "populated": False}
        try:
            import psycopg  # noqa: F401  -- gate on optional extra
        except ImportError:
            return result
        try:
            from back.objects.registry.store import RegistryFactory

            store = RegistryFactory.lakebase(
                registry_cfg=rcfg,
                schema=rcfg.lakebase_schema,
                database=rcfg.lakebase_database,
            )
            result["initialized"] = bool(store.is_initialized())
        except Exception as exc:  # noqa: BLE001 -- purely informational
            logger.debug("Lakebase schema init probe failed: %s", exc)
            return result
        if not result["initialized"]:
            return result
        # Cheap row-count probe across the canonical tables. The store
        # already short-circuits unknown table names so this is safe
        # even for partial schemas.
        try:
            counts = store.table_row_counts(
                (
                    "domains",
                    "domain_versions",
                    "domain_permissions",
                    "schedules",
                    "schedule_runs",
                )
            )
            result["populated"] = any((counts.get(t) or 0) > 0 for t in counts)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Lakebase populated probe failed: %s", exc)
        return result


    @staticmethod
    def lakebase_stats_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """Return per-table row counts for the Lakebase registry schema.

        Used by the admin Registry Location panel to give a quick at-a-
        glance inventory of what currently lives in Lakebase. Never
        raises — surfaces failures via ``success=False`` + ``message``.
        """
        from back.core.databricks import get_lakebase_auth

        auth = get_lakebase_auth()
        if not auth.is_available:
            return {
                "success": False,
                "message": "Lakebase resource not bound (PGHOST/PGUSER missing)",
                "tables": [],
            }

        try:
            domain = get_domain(session_mgr)
            cfg = RegistryCfg.from_domain(domain, settings)
            host, token = get_databricks_host_and_token(domain, settings)
        except Exception as exc:  # noqa: BLE001
            return {
                "success": False,
                "message": f"Could not resolve registry context: {exc}",
                "tables": [],
            }

        try:
            from back.objects.registry.store import RegistryFactory

            lakebase_cfg = RegistryCfg(
                catalog=cfg.catalog,
                schema=cfg.schema,
                volume=cfg.volume,
                lakebase_schema=cfg.lakebase_schema,
                lakebase_database=cfg.lakebase_database,
            )
            store = RegistryFactory.lakebase(
                registry_cfg=lakebase_cfg,
                schema=cfg.lakebase_schema,
                database=cfg.lakebase_database,
            )
        except ImportError:
            return {
                "success": False,
                "message": "Lakebase backend not installed (missing psycopg)",
                "tables": [],
            }
        except Exception as exc:  # noqa: BLE001
            return {
                "success": False,
                "message": f"Could not build Lakebase store: {exc}",
                "tables": [],
            }

        tables = (
            "registries",
            "global_config",
            "domains",
            "domain_versions",
            "domain_permissions",
            "schedules",
            "schedule_runs",
        )
        try:
            counts = store.table_row_counts(tables)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Lakebase table_row_counts failed")
            # Surface the *kind* of failure so the admin can
            # distinguish "schema not initialised yet" from "service
            # principal lacks USAGE" or "instance unreachable" — the
            # 0-rows-everywhere alternative used to mask real
            # deployment problems.
            return {
                "success": False,
                "schema": cfg.lakebase_schema,
                "initialized": False,
                "reason": "table_count_failed",
                "message": f"Could not query Lakebase: {exc}",
                "tables": [{"name": t, "rows": 0} for t in tables],
            }
        # Use the detailed probe so the UI can distinguish "missing
        # USAGE on the schema" (silent before — looked like an empty
        # registry) from genuine first-run states. Falls back to the
        # plain bool for stores that haven't grown ``init_status``.
        if hasattr(store, "init_status"):
            status = store.init_status()
            initialized = bool(status.get("initialized"))
            reason = status.get("reason") or ("ok" if initialized else "unknown")
            error = status.get("error")
        else:
            initialized = bool(store.is_initialized())
            reason = "ok" if initialized else "unknown"
            error = None
        payload: Dict[str, Any] = {
            "success": True,
            "schema": cfg.lakebase_schema,
            "initialized": initialized,
            "reason": reason,
            "tables": [{"name": t, "rows": counts.get(t, 0)} for t in tables],
        }
        if error:
            payload["message"] = error
        return payload

    @staticmethod
    def initialize_registry_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        try:
            domain = get_domain(session_mgr)
            # ``prefer_volume_binding=True`` so the Initialize flow
            # pins the registry triplet to the *current* Volume binding
            # (not the cached Lakebase ``registries`` row). Without
            # this, re-binding the Volume resource and re-clicking
            # Initialize would silently no-op the row update — the row
            # is the source of truth for read paths, so callers would
            # keep seeing the stale catalog/schema/volume.
            svc = RegistryService.from_context(
                domain, settings, prefer_volume_binding=True
            )
            if not svc.cfg.is_configured:
                raise ValidationError(
                    "Registry catalog, schema, and volume must be configured first"
                )

            client = get_databricks_client(domain, settings)
            if not client:
                raise ValidationError("Databricks not configured")

            ok, msg = svc.initialize(client)
            if not ok:
                raise InfrastructureError("Registry initialization failed", detail=msg)
            # Drop the process-local Lakebase triplet cache so the next
            # ``RegistryCfg.from_domain`` reads the freshly-upserted
            # ``registries`` row instead of returning the stale triplet
            # captured before this Initialize.
            try:
                from back.objects.registry.store.lakebase.store import (
                    reset_lakebase_triplet_cache,
                )

                reset_lakebase_triplet_cache()
            except Exception:  # noqa: BLE001
                logger.debug(
                    "reset_lakebase_triplet_cache unavailable; skipping",
                    exc_info=True,
                )
            try:
                _, host, token, registry_cfg = SettingsService._resolve_context(
                    session_mgr, settings
                )
                blob = global_config_service.load(host, token, registry_cfg, force=True)
                if isinstance(blob, dict) and "graph_engine" not in blob:
                    ok_seed, msg_seed = global_config_service._save(
                        host,
                        token,
                        registry_cfg,
                        {
                            "graph_engine": "lakebase",
                            "graph_engine_config": (
                                blob["graph_engine_config"]
                                if isinstance(blob.get("graph_engine_config"), dict)
                                else {}
                            ),
                        },
                    )
                    if not ok_seed:
                        logger.warning(
                            "Could not seed graph_engine in registry global config: %s",
                            msg_seed,
                        )
            except Exception:
                logger.debug(
                    "Skipping graph_engine seed after registry init",
                    exc_info=True,
                )
            return {"success": ok, "message": msg}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("Initialize registry failed: %s", e)
            raise InfrastructureError(
                "Initialize registry failed", detail=str(e)
            ) from e

    @staticmethod
    def list_registry_domains_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        try:
            domain = get_domain(session_mgr)
            svc = RegistryService.from_context(domain, settings)
            if not svc.cfg.is_configured:
                raise ValidationError("Registry not configured")

            ok, result, msg = svc.list_domain_details_cached()
            if not ok:
                raise InfrastructureError("Failed to list registry domains", detail=msg)
            return {"success": True, "domains": result}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("List registry domains failed: %s", e)
            raise InfrastructureError(
                "Failed to list registry domains", detail=str(e)
            ) from e

    @staticmethod
    def list_registry_bridges_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """Return all bridges across every domain in the registry."""
        try:
            domain = get_domain(session_mgr)
            svc = RegistryService.from_context(domain, settings)
            if not svc.cfg.is_configured:
                raise ValidationError("Registry not configured")

            ok, result, msg = svc.list_all_bridges()
            if not ok:
                raise InfrastructureError("Failed to list registry bridges", detail=msg)
            return {"success": True, "domains": result}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("List registry bridges failed: %s", e)
            raise InfrastructureError(
                "Failed to list registry bridges", detail=str(e)
            ) from e

    @staticmethod
    def delete_registry_domain_result(
        domain_name: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        try:
            domain = get_domain(session_mgr)
            svc = RegistryService.from_context(domain, settings)
            if not svc.cfg.is_configured:
                raise ValidationError("Registry not configured")

            errors = svc.delete_domain(domain_name)

            if errors:
                joined = "; ".join(errors)
                raise InfrastructureError(
                    "Registry domain was only partially deleted",
                    detail=joined,
                )

            return {
                "success": True,
                "message": f'Domain "{domain_name}" deleted from registry',
            }
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("Delete registry domain failed: %s", e)
            raise InfrastructureError(
                "Delete registry domain failed", detail=str(e)
            ) from e

    @staticmethod
    def delete_registry_version_result(
        domain_name: str,
        version: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        try:
            domain = get_domain(session_mgr)
            svc = RegistryService.from_context(domain, settings)
            if not svc.cfg.is_configured:
                raise ValidationError("Registry not configured")

            d_ok, d_msg = svc.delete_version(domain_name, version)
            if not d_ok:
                raise InfrastructureError(
                    "Failed to delete registry version", detail=d_msg
                )

            return {
                "success": True,
                "message": f'Version {version} deleted from "{domain_name}"',
            }
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("Delete registry version failed: %s", e)
            raise InfrastructureError(
                "Delete registry version failed", detail=str(e)
            ) from e

    @staticmethod
    def set_registry_version_active_result(
        domain_name: str,
        version: str,
        enabled: bool,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Toggle the *active* (``mcp_enabled``) flag for a specific version.

        Works on any domain in the registry — the domain does not need to be
        loaded in the current session.  Only one version per domain may be
        active; enabling one automatically disables the others.
        """
        try:
            domain = get_domain(session_mgr)
            svc = RegistryService.from_context(domain, settings)
            if not svc.cfg.is_configured:
                raise ValidationError("Registry not configured")

            sorted_versions = svc.list_versions_sorted(domain_name)
            if version not in sorted_versions:
                raise NotFoundError(f'Version {version} not found in "{domain_name}"')

            if enabled:
                for ver in sorted_versions:
                    if ver == version:
                        continue
                    ok, data, _ = svc.read_version(domain_name, ver)
                    if not ok:
                        continue
                    if data.get("info", {}).get("mcp_enabled"):
                        data["info"]["mcp_enabled"] = False
                        svc.write_version(domain_name, ver, json.dumps(data))

            ok, data, msg = svc.read_version(domain_name, version)
            if not ok:
                raise InfrastructureError("Failed to read registry version", detail=msg)

            data.setdefault("info", {})["mcp_enabled"] = enabled
            svc.write_version(domain_name, version, json.dumps(data))

            invalidate_registry_cache()

            if (
                domain.domain_folder == domain_name
                and domain.current_version == version
            ):
                domain.info["mcp_enabled"] = enabled

            return {"success": True, "version": version, "active": enabled}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("Set registry version active failed: %s", e)
            raise InfrastructureError(
                "Set registry version active failed", detail=str(e)
            ) from e

    @staticmethod
    def set_default_emoji_result(
        emoji: str,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        ok, msg = global_config_service.set_default_emoji(
            host, token, registry_cfg, emoji
        )
        if not ok:
            raise InfrastructureError("Failed to save default emoji", detail=msg)
        return {"success": True, "emoji": emoji}

    @staticmethod
    def save_base_uri_result(
        base_uri: str,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        ok, msg = global_config_service.set_default_base_uri(
            host, token, registry_cfg, base_uri
        )
        if not ok:
            raise InfrastructureError("Failed to save default base URI", detail=msg)
        return {"success": True, "base_uri": base_uri}

    # Recommended upload size & format for the top-bar logo.
    # The navbar renders the image at 24×24 CSS pixels; keeping the source
    # at 64×64 (≈2.7×) gives crisp rendering on retina displays without
    # bloating the global config blob.
    NAVBAR_LOGO_RECOMMENDED_SIZE = "64×64 px"
    NAVBAR_LOGO_DEFAULT_PATH = "/static/global/img/favicon.svg"
    _NAVBAR_LOGO_ALLOWED_MIME = {
        "image/svg+xml",
        "image/png",
        "image/jpeg",
        "image/webp",
        "image/gif",
    }
    _NAVBAR_LOGO_MAX_BYTES = 1024 * 1024  # 1 MB — way more than a 64×64 icon needs

    @staticmethod
    def get_navbar_logo_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Return the configured navbar logo (data URL) or the bundled default."""
        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        custom = global_config_service.get_navbar_logo(host, token, registry_cfg)
        return {
            "success": True,
            "logo_url": custom or SettingsService.NAVBAR_LOGO_DEFAULT_PATH,
            "is_custom": bool(custom),
            "default_url": SettingsService.NAVBAR_LOGO_DEFAULT_PATH,
            "recommended_size": SettingsService.NAVBAR_LOGO_RECOMMENDED_SIZE,
            "max_bytes": SettingsService._NAVBAR_LOGO_MAX_BYTES,
            "allowed_mime": sorted(SettingsService._NAVBAR_LOGO_ALLOWED_MIME),
        }

    @staticmethod
    def upload_navbar_logo_result(
        content: bytes,
        content_type: str,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Validate and persist an uploaded navbar logo (admin only, stored globally)."""
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        if not content:
            raise ValidationError("Empty file — pick an image to upload")
        if len(content) > SettingsService._NAVBAR_LOGO_MAX_BYTES:
            raise ValidationError(
                f"Logo too large ({len(content)} bytes); "
                f"max {SettingsService._NAVBAR_LOGO_MAX_BYTES} bytes"
            )

        mime = (content_type or "").split(";", 1)[0].strip().lower()
        if mime not in SettingsService._NAVBAR_LOGO_ALLOWED_MIME:
            raise ValidationError(
                f"Unsupported image type '{mime}'. "
                f"Allowed: {', '.join(sorted(SettingsService._NAVBAR_LOGO_ALLOWED_MIME))}"
            )

        import base64

        b64 = base64.b64encode(content).decode("ascii")
        data_url = f"data:{mime};base64,{b64}"

        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        ok, msg = global_config_service.set_navbar_logo(
            host, token, registry_cfg, data_url
        )
        if not ok:
            raise InfrastructureError("Failed to save navbar logo", detail=msg)
        return {
            "success": True,
            "logo_url": data_url,
            "is_custom": True,
            "size_bytes": len(content),
            "mime": mime,
        }

    @staticmethod
    def reset_navbar_logo_result(
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Clear the custom navbar logo so the bundled default is used again."""
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        ok, msg = global_config_service.set_navbar_logo(
            host, token, registry_cfg, ""
        )
        if not ok:
            raise InfrastructureError("Failed to reset navbar logo", detail=msg)
        return {
            "success": True,
            "logo_url": SettingsService.NAVBAR_LOGO_DEFAULT_PATH,
            "is_custom": False,
        }

    @staticmethod
    def get_registry_cache_ttl_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        ttl = global_config_service.get_registry_cache_ttl(host, token, registry_cfg)
        return {"success": True, "registry_cache_ttl": ttl}

    @staticmethod
    def save_registry_cache_ttl_result(
        ttl: int,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        ok, msg = global_config_service.set_registry_cache_ttl(
            host, token, registry_cfg, ttl
        )
        if not ok:
            raise InfrastructureError("Failed to save registry cache TTL", detail=msg)
        return {"success": True, "registry_cache_ttl": max(10, int(ttl))}

    # ------------------------------------------------------------------
    #  Graph DB Engine
    # ------------------------------------------------------------------

    @staticmethod
    def get_graph_engine_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        # Bypass per-process TTL so the Settings Graph DB tab reflects the store
        # immediately after save (multi-worker and cross-tab).
        global_config_service.load(host, token, registry_cfg, force=True)
        engine = global_config_service.get_graph_engine(host, token, registry_cfg)
        allowed = list(global_config_service.ALLOWED_GRAPH_ENGINES)
        return {"success": True, "graph_engine": engine, "allowed_engines": allowed}

    @staticmethod
    def set_graph_engine_result(
        engine: str,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        ok, msg = global_config_service.set_graph_engine(
            host, token, registry_cfg, engine
        )
        if not ok:
            raise ValidationError(msg)
        persisted = global_config_service.get_graph_engine(host, token, registry_cfg)
        SettingsService._mirror_graph_engine_to_domain_registry(
            session_mgr, engine=persisted
        )
        return {"success": True, "graph_engine": persisted}

    @staticmethod
    def get_graph_engine_config_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Return the engine-specific JSON configuration."""
        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        global_config_service.load(host, token, registry_cfg, force=True)
        cfg = global_config_service.get_graph_engine_config(host, token, registry_cfg)
        return {"success": True, "graph_engine_config": cfg}

    @staticmethod
    def set_graph_engine_config_result(
        config: Dict[str, Any],
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Persist the engine-specific JSON configuration (admin only)."""
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        ok, msg = global_config_service.set_graph_engine_config(
            host, token, registry_cfg, config
        )
        if not ok:
            raise ValidationError(msg)
        persisted_cfg = global_config_service.get_graph_engine_config(
            host, token, registry_cfg
        )
        SettingsService._mirror_graph_engine_to_domain_registry(
            session_mgr, config=persisted_cfg
        )
        return {"success": True, "graph_engine_config": persisted_cfg}

    @staticmethod
    def graph_engine_lakebase_health_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Probe Lakebase Postgres for the configured graph schema (read-only).

        Uses ``graph_engine_config.database`` (optional) and ``schema`` from
        registry global config. Never raises — failures become ``success=False``.
        """
        import os

        from back.core.databricks import get_lakebase_auth
        from back.core.graphdb.lakebase.LakebaseBase import (
            default_schema,
            validate_graph_schema,
        )

        auth = get_lakebase_auth()
        port = int(os.environ.get("PGPORT", "5432") or "5432")
        bound_db = os.environ.get("PGDATABASE", "").strip()
        out: Dict[str, Any] = {
            "success": False,
            "reason": "",
            "message": "",
            "host": os.environ.get("PGHOST", "") or os.environ.get("LAKEBASE_PROJECT", "")
                    + ("/" + os.environ.get("LAKEBASE_BRANCH", "") if os.environ.get("LAKEBASE_BRANCH") else ""),
            "port": port,
            "bound_database": bound_db or "",
            "effective_database": "",
            "graph_schema": default_schema(),
            "schema_exists": False,
            "tables_in_schema": 0,
        }

        if not auth.is_available:
            out["reason"] = "no_binding"
            out["message"] = (
                "Lakebase not available — set LAKEBASE_PROJECT + LAKEBASE_BRANCH + PGUSER "
                "in .env (local), or bind a Databricks App postgres resource (deployed)."
            )
            return out

        try:
            _, host, token, registry_cfg = SettingsService._resolve_context(
                session_mgr, settings
            )
            global_config_service.load(host, token, registry_cfg, force=True)
            gcfg = global_config_service.get_graph_engine_config(
                host, token, registry_cfg
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("graph_engine_lakebase_health context failed: %s", exc)
            out["reason"] = "context"
            out["message"] = f"Could not load graph engine config: {exc}"
            return out

        db_override = ""
        schema_raw = ""
        if isinstance(gcfg, dict):
            db_override = (gcfg.get("database") or "").strip()
            schema_raw = (gcfg.get("schema") or "").strip()

        try:
            schema = validate_graph_schema(schema_raw or default_schema())
        except ValueError as exc:
            out["reason"] = "bad_schema"
            out["message"] = str(exc)
            out["graph_schema"] = schema_raw or default_schema()
            return out

        out["graph_schema"] = schema
        base_db = bound_db or auth.database
        effective_db = db_override or base_db
        out["bound_database"] = base_db
        out["effective_database"] = effective_db

        try:
            from back.core.graphdb.lakebase.pool import _require_psycopg

            psycopg, _ = _require_psycopg()
        except ImportError as exc:
            out["reason"] = "no_psycopg"
            out["message"] = str(exc)
            return out

        kwargs = auth.kwargs(application_name="ontobricks-graph-health")
        kwargs["dbname"] = effective_db

        try:
            with psycopg.connect(**kwargs) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_namespace
                            WHERE nspname = %s
                        )
                        """,
                        (schema,),
                    )
                    row = cur.fetchone()
                    schema_exists = bool(row[0]) if row else False
                    out["schema_exists"] = schema_exists
                    table_count = 0
                    if schema_exists:
                        cur.execute(
                            """
                            SELECT COUNT(*)
                            FROM pg_catalog.pg_class c
                            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                            WHERE n.nspname = %s AND c.relkind = 'r'
                            """,
                            (schema,),
                        )
                        row2 = cur.fetchone()
                        table_count = int(row2[0]) if row2 else 0
                    out["tables_in_schema"] = table_count
        except Exception as exc:  # noqa: BLE001
            logger.warning("graph_engine_lakebase_health probe failed: %s", exc)
            out["reason"] = "connect_failed"
            out["message"] = str(exc)
            return out

        out["success"] = True
        out["reason"] = "ok"
        if schema_exists:
            out["message"] = (
                f"Connected to database {effective_db!r}; schema {schema!r} exists "
                f"({out['tables_in_schema']} table(s))."
            )
        else:
            out["message"] = (
                f"Connected to database {effective_db!r}, but schema {schema!r} "
                "does not exist yet — run a Digital Twin build or create the schema."
            )
        return out

    @staticmethod
    def graph_engine_uc_catalogs_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """List Unity Catalog names (``SHOW CATALOGS``) for the Lakebase UC picker.

        Read-only; uses the configured SQL warehouse. Returns an empty list with
        ``success=False`` when the warehouse is missing or the query fails.
        """
        out: Dict[str, Any] = {"success": False, "catalogs": [], "message": ""}
        try:
            domain, host, token, registry_cfg = SettingsService._resolve_context(
                session_mgr, settings
            )
            global_config_service.load(host, token, registry_cfg, force=True)
            warehouse_id = global_config_service.get_warehouse_id(
                host, token, registry_cfg
            )
            if not warehouse_id:
                warehouse_id = (
                    (domain.databricks or {}).get("warehouse_id") or ""
                )
            if not warehouse_id:
                warehouse_id = settings.sql_warehouse_id or ""
            if not warehouse_id:
                out["message"] = (
                    "Configure a SQL warehouse under Settings → Databricks first."
                )
                return out
            from back.core.databricks.DatabricksAuth import DatabricksAuth
            from back.core.databricks.UnityCatalog import UnityCatalog

            auth = DatabricksAuth(host=host, token=token, warehouse_id=warehouse_id)
            uc = UnityCatalog(auth)
            catalogs = uc.get_catalogs()
            out["success"] = True
            out["catalogs"] = sorted(catalogs) if catalogs else []
            return out
        except Exception as exc:  # noqa: BLE001
            logger.warning("graph_engine_uc_catalogs failed: %s", exc)
            out["message"] = str(exc)
            return out

    @staticmethod
    def graph_engine_lakebase_projects_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """List all Lakebase Autoscaling projects visible in the workspace."""
        out: Dict[str, Any] = {"success": False, "projects": [], "message": ""}
        try:
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            api = getattr(w, "api_client", None)
            if api is None or not hasattr(api, "do"):
                out["message"] = "Databricks SDK api_client unavailable"
                return out
            raw = (api.do("GET", "/api/2.0/postgres/projects") or {}).get("projects") or []
            projects = []
            for p in raw:
                name = p.get("name") or ""
                if not name:
                    continue
                short = name.rsplit("/", 1)[-1]
                status = p.get("status") or {}
                projects.append({
                    "name": name,
                    "short_name": short,
                    "state": status.get("state") or "",
                })
            out["success"] = True
            out["projects"] = projects
            return out
        except Exception as exc:  # noqa: BLE001
            logger.warning("graph_engine_lakebase_projects failed: %s", exc)
            out["message"] = str(exc)
            return out

    @staticmethod
    def graph_engine_lakebase_branches_result(
        project_path: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """List branches for a Lakebase Autoscaling project."""
        out: Dict[str, Any] = {"success": False, "branches": [], "message": ""}
        if not project_path:
            out["message"] = "project_path is required"
            return out
        try:
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            api = getattr(w, "api_client", None)
            if api is None or not hasattr(api, "do"):
                out["message"] = "Databricks SDK api_client unavailable"
                return out
            # Normalise: accept both short name and full resource path
            if not project_path.startswith("projects/"):
                project_path = f"projects/{project_path}"
            raw = (
                api.do("GET", f"/api/2.0/postgres/{project_path}/branches") or {}
            ).get("branches") or []
            branches = []
            for b in raw:
                name = b.get("name") or ""
                if not name:
                    continue
                short = name.rsplit("/", 1)[-1]
                status = b.get("status") or {}
                branches.append({
                    "name": name,
                    "short_name": short,
                    "state": status.get("state") or "",
                })
            out["success"] = True
            out["branches"] = branches
            return out
        except Exception as exc:  # noqa: BLE001
            logger.warning("graph_engine_lakebase_branches failed: %s", exc)
            out["message"] = str(exc)
            return out

    @staticmethod
    def graph_engine_lakebase_pg_databases_result(
        branch_path: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """List Postgres databases on a Lakebase branch endpoint."""
        out: Dict[str, Any] = {"success": False, "databases": [], "message": ""}
        if not branch_path:
            out["message"] = "branch_path is required"
            return out
        try:
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            api = getattr(w, "api_client", None)
            if api is None or not hasattr(api, "do"):
                out["message"] = "Databricks SDK api_client unavailable"
                return out
            raw = (
                api.do("GET", f"/api/2.0/postgres/{branch_path}/databases") or {}
            ).get("databases") or []
            databases = []
            for db in raw:
                status = db.get("status") or {}
                pg_name = status.get("postgres_database") or ""
                if pg_name:
                    databases.append(pg_name)
            out["success"] = True
            out["databases"] = sorted(databases)
            return out
        except Exception as exc:  # noqa: BLE001
            logger.warning("graph_engine_lakebase_pg_databases failed: %s", exc)
            out["message"] = str(exc)
            return out

    @staticmethod
    def graph_engine_lakebase_pg_schemas_result(
        database: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """List Postgres schemas in a Lakebase database (using the bound instance)."""
        out: Dict[str, Any] = {"success": False, "schemas": [], "message": ""}
        try:
            from back.core.databricks import get_lakebase_auth
            from back.core.graphdb.lakebase.pool import _require_psycopg

            auth = get_lakebase_auth()
            if not auth.is_available:
                out["message"] = "Lakebase resource not bound (LAKEBASE_PROJECT/LAKEBASE_BRANCH/PGUSER missing)"
                return out
            psycopg, _ = _require_psycopg()
            kwargs = auth.kwargs(application_name="ontobricks-schema-list")
            if database:
                kwargs["dbname"] = database
            with psycopg.connect(**kwargs) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT nspname FROM pg_catalog.pg_namespace
                        WHERE nspname NOT LIKE 'pg_%'
                          AND nspname NOT IN ('information_schema')
                        ORDER BY nspname
                        """
                    )
                    schemas = [row[0] for row in cur.fetchall()]
            out["success"] = True
            out["schemas"] = schemas
            return out
        except Exception as exc:  # noqa: BLE001
            logger.warning("graph_engine_lakebase_pg_schemas failed: %s", exc)
            out["message"] = str(exc)
            return out

    @staticmethod
    def _lakebase_kwargs_for_branch(
        branch_path: str,
        database: str,
        application_name: str,
    ) -> Dict[str, Any]:
        """Resolve psycopg connect kwargs directly from a Lakebase branch resource path.

        Uses the Databricks API to find the primary endpoint for ``branch_path``
        (format ``projects/<proj>/branches/<branch>``), mints a fresh JWT, and
        returns kwargs ready to pass to ``psycopg.connect()``.
        Raises on any resolution failure so the caller can return a clean error.
        """
        import os

        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        api = getattr(w, "api_client", None)
        if api is None or not hasattr(api, "do"):
            raise RuntimeError("Databricks SDK api_client unavailable")

        endpoints = (
            api.do("GET", f"/api/2.0/postgres/{branch_path}/endpoints") or {}
        ).get("endpoints") or []

        host = ""
        endpoint_resource = ""
        for ep in endpoints:
            h = ((ep.get("status") or {}).get("hosts") or {}).get("host", "").strip()
            if h:
                host = h
                endpoint_resource = ep.get("name") or ""
                break

        if not host:
            raise RuntimeError(
                f"No active endpoint found for branch path {branch_path!r}"
            )

        token_resp = api.do(
            "POST",
            "/api/2.0/postgres/credentials",
            body={"endpoint": endpoint_resource},
        ) or {}
        jwt = token_resp.get("token", "")
        if not jwt:
            raise RuntimeError(
                f"Failed to mint Lakebase JWT for endpoint {endpoint_resource!r}"
            )

        pguser = os.environ.get("PGUSER", "").strip()
        if not pguser:
            raise RuntimeError(
                "PGUSER is not set — required for Lakebase psycopg connections"
            )

        kwargs: Dict[str, Any] = {
            "host": host,
            "port": int(os.environ.get("PGPORT", "5432")),
            "user": pguser,
            "password": jwt,
            "dbname": database or "postgres",
            "sslmode": "require",
            "connect_timeout": 10,
            "application_name": application_name,
        }
        return kwargs

    @staticmethod
    def graph_engine_lakebase_objects_result(
        database: str,
        branch_path: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """List all user schemas, tables and views in a Lakebase database.

        Uses ``branch_path`` (the form's current branch selection) when provided
        so the result reflects the live form state rather than the saved config.
        Falls back to the bound Lakebase auth when ``branch_path`` is empty.
        Returns the Postgres ``current_user`` so the frontend can display it.
        Never raises — failures become ``success=False``.
        """
        out: Dict[str, Any] = {
            "success": False,
            "current_user": "",
            "schemas": [],
            "tables": [],
            "views": [],
            "message": "",
        }
        try:
            from back.core.graphdb.lakebase.pool import _require_psycopg

            psycopg, _ = _require_psycopg()

            if branch_path:
                kwargs = SettingsService._lakebase_kwargs_for_branch(
                    branch_path, database, "ontobricks-obj-list"
                )
            else:
                from back.core.databricks import get_lakebase_auth

                auth = get_lakebase_auth()
                if not auth.is_available:
                    out["message"] = (
                        "Lakebase resource not bound "
                        "(LAKEBASE_PROJECT/LAKEBASE_BRANCH/PGUSER missing)"
                    )
                    return out
                kwargs = auth.kwargs(application_name="ontobricks-obj-list")
                if database:
                    kwargs["dbname"] = database
            with psycopg.connect(**kwargs) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT current_user")
                    out["current_user"] = (cur.fetchone() or ("",))[0]

                    cur.execute(
                        """
                        SELECT nspname,
                               pg_catalog.pg_get_userbyid(nspowner) AS owner
                        FROM pg_catalog.pg_namespace
                        WHERE nspname NOT LIKE 'pg_%%'
                          AND nspname NOT IN ('information_schema')
                          AND pg_catalog.pg_get_userbyid(nspowner) = current_user
                        ORDER BY nspname
                        """
                    )
                    out["schemas"] = [
                        {"name": r[0], "owner": r[1]} for r in cur.fetchall()
                    ]

                    cur.execute(
                        """
                        SELECT t.schemaname,
                               t.tablename,
                               pg_catalog.pg_get_userbyid(c.relowner) AS owner
                        FROM pg_catalog.pg_tables t
                        JOIN pg_catalog.pg_class c
                             ON c.relname = t.tablename
                        JOIN pg_catalog.pg_namespace n
                             ON n.oid = c.relnamespace
                            AND n.nspname = t.schemaname
                        WHERE t.schemaname NOT LIKE 'pg_%%'
                          AND t.schemaname NOT IN ('information_schema')
                          AND pg_catalog.pg_get_userbyid(c.relowner) = current_user
                        ORDER BY t.schemaname, t.tablename
                        """
                    )
                    out["tables"] = [
                        {"schema": r[0], "name": r[1], "owner": r[2]}
                        for r in cur.fetchall()
                    ]

                    cur.execute(
                        """
                        SELECT v.schemaname,
                               v.viewname,
                               pg_catalog.pg_get_userbyid(c.relowner) AS owner
                        FROM pg_catalog.pg_views v
                        JOIN pg_catalog.pg_class c
                             ON c.relname = v.viewname
                        JOIN pg_catalog.pg_namespace n
                             ON n.oid = c.relnamespace
                            AND n.nspname = v.schemaname
                        WHERE v.schemaname NOT LIKE 'pg_%%'
                          AND v.schemaname NOT IN ('information_schema')
                          AND pg_catalog.pg_get_userbyid(c.relowner) = current_user
                        ORDER BY v.schemaname, v.viewname
                        """
                    )
                    out["views"] = [
                        {"schema": r[0], "name": r[1], "owner": r[2]}
                        for r in cur.fetchall()
                    ]

            out["success"] = True
            return out
        except Exception as exc:  # noqa: BLE001
            logger.warning("graph_engine_lakebase_objects failed: %s", exc)
            out["message"] = str(exc)
            return out

    @staticmethod
    def graph_engine_lakebase_drop_object_result(
        kind: str,
        schema: str,
        name: str,
        database: str,
        branch_path: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Drop a Postgres schema, table or view in the connected Lakebase database.

        ``kind`` must be one of ``schema``, ``table``, ``view``.
        Schemas are dropped with CASCADE.  Uses ``branch_path`` when provided
        so the drop targets the form's current connection, not the saved config.
        Never raises — failures become ``success=False``.
        """
        out: Dict[str, Any] = {"success": False, "message": ""}
        allowed_kinds = {"schema", "table", "view"}
        if kind not in allowed_kinds:
            out["message"] = f"kind must be one of {allowed_kinds}, got: {kind!r}"
            return out

        def _q(ident: str) -> str:
            return '"' + ident.replace('"', '""') + '"'

        if kind == "schema":
            ddl = f"DROP SCHEMA {_q(name)} CASCADE"
        elif kind == "table":
            if not schema:
                out["message"] = "schema is required for kind=table"
                return out
            ddl = f"DROP TABLE {_q(schema)}.{_q(name)}"
        else:
            if not schema:
                out["message"] = "schema is required for kind=view"
                return out
            ddl = f"DROP VIEW {_q(schema)}.{_q(name)}"

        try:
            from back.core.graphdb.lakebase.pool import _require_psycopg

            psycopg, _ = _require_psycopg()

            if branch_path:
                kwargs = SettingsService._lakebase_kwargs_for_branch(
                    branch_path, database, "ontobricks-obj-drop"
                )
            else:
                from back.core.databricks import get_lakebase_auth

                auth = get_lakebase_auth()
                if not auth.is_available:
                    out["message"] = (
                        "Lakebase resource not bound "
                        "(LAKEBASE_PROJECT/LAKEBASE_BRANCH/PGUSER missing)"
                    )
                    return out
                kwargs = auth.kwargs(application_name="ontobricks-obj-drop")
                if database:
                    kwargs["dbname"] = database

            with psycopg.connect(**kwargs) as conn:
                with conn.cursor() as cur:
                    cur.execute(ddl)
            out["success"] = True
            out["message"] = f"Dropped {kind}: {ddl}"
            return out
        except Exception as exc:  # noqa: BLE001
            logger.warning("graph_engine_lakebase_drop_object failed: %s", exc)
            out["message"] = str(exc)
            return out

    @staticmethod
    def graph_engine_uc_schemas_result(
        catalog: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """List Unity Catalog schemas in a given catalog."""
        out: Dict[str, Any] = {"success": False, "schemas": [], "message": ""}
        if not catalog:
            out["message"] = "catalog is required"
            return out
        try:
            domain, host, token, registry_cfg = SettingsService._resolve_context(
                session_mgr, settings
            )
            global_config_service.load(host, token, registry_cfg, force=True)
            warehouse_id = global_config_service.get_warehouse_id(
                host, token, registry_cfg
            )
            if not warehouse_id:
                warehouse_id = (
                    (domain.databricks or {}).get("warehouse_id") or ""
                )
            if not warehouse_id:
                warehouse_id = settings.sql_warehouse_id or ""
            if not warehouse_id:
                out["message"] = "Configure a SQL warehouse under Settings → Databricks first."
                return out
            from back.core.databricks.DatabricksAuth import DatabricksAuth
            from back.core.databricks.UnityCatalog import UnityCatalog

            auth = DatabricksAuth(host=host, token=token, warehouse_id=warehouse_id)
            uc = UnityCatalog(auth)
            schemas = uc.get_schemas(catalog)
            out["success"] = True
            out["schemas"] = sorted(schemas) if schemas else []
            return out
        except Exception as exc:  # noqa: BLE001
            logger.warning("graph_engine_uc_schemas failed: %s", exc)
            out["message"] = str(exc)
            return out

    @staticmethod
    def build_permissions_me(
        email: str,
        display_name: str,
        user_token: str,
        user_role: str,
        user_domain_role: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        if not is_databricks_app():
            return {
                "email": email or "local-user",
                "display_name": display_name or "Local User",
                "role": "admin",
                "is_app_mode": False,
            }

        role = "none"
        is_app_admin = False
        domain_role = user_domain_role or ""
        domain_folder = ""
        try:
            domain, host, token, registry_cfg = SettingsService._resolve_context(
                session_mgr, settings
            )
            domain_folder = getattr(domain, "domain_folder", "") or ""

            permission_service.clear_admin_cache(email)
            is_app_admin = permission_service.is_admin(
                email,
                host,
                token,
                settings.ontobricks_app_name,
                user_token=user_token,
            )
            role = permission_service.get_user_role(
                email,
                host,
                token,
                registry_cfg,
                settings.ontobricks_app_name,
                user_token=user_token,
            )
            # Re-resolve domain role fresh so it matches what the
            # middleware sees on the next request (useful for debugging
            # why a viewer can/can't write).
            domain_role = permission_service.get_domain_role(
                email,
                host,
                token,
                registry_cfg,
                settings.ontobricks_app_name,
                domain_folder,
                user_token=user_token,
                app_role=role,
            )
        except Exception as e:
            logger.error(
                "permissions/me: error resolving role for %s (middleware app/domain role=%r/%r): %s",
                email,
                user_role,
                user_domain_role,
                e,
                exc_info=True,
            )

        return {
            "email": email,
            "display_name": display_name,
            "role": role,
            "is_app_admin": is_app_admin,
            "is_app_mode": True,
            "domain_folder": domain_folder,
            "domain_role": domain_role,
        }

    @staticmethod
    def build_permissions_diag(
        email: str,
        display_name: str,
        user_token: str,
        user_role: str,
        user_domain_role: str,
        settings: Settings,
    ) -> Dict[str, Any]:
        from databricks.sdk import WorkspaceClient
        import requests as _req

        app_name = settings.ontobricks_app_name
        diag: dict = {
            "email": email,
            "app_name": app_name,
            "is_app_mode": is_databricks_app(),
            "user_token_present": bool(user_token),
            "display_name": display_name,
            "state_user_role": user_role,
            "state_user_domain_role": user_domain_role,
        }

        # ── SDK path (SP token) ──
        try:
            w = WorkspaceClient()
            diag["sdk_host"] = str(getattr(w.config, "host", ""))
            diag["sdk_auth_type"] = str(getattr(w.config, "auth_type", ""))
            raw = w.api_client.do("GET", f"/api/2.0/permissions/apps/{app_name}")
            acl_list = raw.get("access_control_list", [])
            managers = []
            for acl in acl_list:
                principal = (
                    acl.get("user_name")
                    or acl.get("group_name")
                    or acl.get("service_principal_name")
                    or ""
                )
                for p in acl.get("all_permissions", []):
                    if p.get("permission_level") == "CAN_MANAGE":
                        managers.append(principal)
            diag["sdk_can_manage"] = managers
            diag["sdk_error"] = None
        except Exception as e:
            diag["sdk_error"] = f"{type(e).__name__}: {e}"
            diag["sdk_can_manage"] = []

        # ── User-token path (preferred at runtime) ──
        if user_token:
            try:
                host = diag.get("sdk_host", "").rstrip("/")
                resp = _req.get(
                    f"{host}/api/2.0/permissions/apps/{app_name}",
                    headers={"Authorization": f"Bearer {user_token}"},
                    timeout=5,
                )
                resp.raise_for_status()
                acl_list = resp.json().get("access_control_list", [])
                managers = []
                for acl in acl_list:
                    principal = (
                        acl.get("user_name")
                        or acl.get("group_name")
                        or acl.get("service_principal_name")
                        or ""
                    )
                    for p in acl.get("all_permissions", []):
                        if p.get("permission_level") == "CAN_MANAGE":
                            managers.append(principal)
                diag["user_token_can_manage"] = managers
                diag["email_is_manager"] = email.lower() in [
                    m.lower() for m in managers
                ]
                diag["user_token_error"] = None
            except Exception as e:
                diag["user_token_error"] = f"{type(e).__name__}: {e}"
                diag["user_token_can_manage"] = []
                diag["email_is_manager"] = False
        else:
            diag["email_is_manager"] = email.lower() in [
                m.lower() for m in diag.get("sdk_can_manage", [])
            ]

        diag["admin_cache"] = {
            k: {"result": v[0], "age_s": round(time.time() - v[1], 1)}
            for k, v in permission_service._admin_cache.items()
        }

        return diag

    @staticmethod
    def list_app_principals_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """Return the Databricks App principals (users + groups).

        Used by Settings → Permissions (read-only view) and as the row
        source for the Registry → Teams matrix picker.
        """
        _, host, token, _ = SettingsService._resolve_context(session_mgr, settings)
        app_name = settings.ontobricks_app_name
        permission_service.clear_principals_cache()
        result = permission_service.list_app_principals(host, token, app_name)
        return {
            "success": True,
            "users": result.get("users", []),
            "groups": result.get("groups", []),
        }

    @staticmethod
    def list_principals_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """Alias kept for the Teams picker dropdown."""
        return SettingsService.list_app_principals_result(session_mgr, settings)

    @staticmethod
    def search_workspace_principals(
        query: str,
        principal_type: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Search users or groups that have access to the Databricks App.

        Fetches the full app-permission principal list (cached by
        ``PermissionService``) and applies a case-insensitive *contains*
        filter on the client side.  This avoids SCIM calls that the app
        service-principal typically cannot perform and ensures only
        app-visible principals are returned.
        """
        _, host, token, _ = SettingsService._resolve_context(session_mgr, settings)
        app_name = settings.ontobricks_app_name
        all_principals = permission_service.list_app_principals(host, token, app_name)

        q = query.lower()

        if principal_type == "group":
            groups = [
                g
                for g in all_principals.get("groups", [])
                if q in (g.get("display_name") or "").lower()
            ]
            return {"success": True, "results": groups}

        users = [
            u
            for u in all_principals.get("users", [])
            if q in (u.get("email") or "").lower()
            or q in (u.get("display_name") or "").lower()
        ]
        return {"success": True, "results": users}

    @staticmethod
    def list_domain_permissions_result(
        domain_name: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        entries = permission_service.list_domain_entries(
            host, token, registry_cfg, domain_name
        )
        return {"success": True, "domain": domain_name, "permissions": entries}

    @staticmethod
    def add_domain_permission_result(
        domain_name: str,
        data: Dict[str, Any],
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        principal = data.get("principal", "").strip()
        principal_type = data.get("principal_type", "user")
        display_name = data.get("display_name", principal)
        role = data.get("role", "viewer")

        if not principal:
            raise ValidationError("Principal (email or group name) is required")
        if role not in ASSIGNABLE_ROLES:
            raise ValidationError('Role must be "viewer", "editor", or "builder"')
        if not domain_name:
            raise ValidationError("Domain name is required")

        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        if not registry_cfg.get("catalog") or not registry_cfg.get("schema"):
            raise ValidationError("Registry not configured")

        ok, msg = permission_service.add_or_update_domain_entry(
            host,
            token,
            registry_cfg,
            domain_name,
            principal,
            principal_type,
            display_name,
            role,
        )
        if not ok:
            raise InfrastructureError(
                "Failed to add or update domain permission", detail=msg
            )
        return {"success": ok, "message": msg}

    @staticmethod
    def delete_domain_permission_result(
        domain_name: str,
        principal: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        if not registry_cfg.get("catalog") or not registry_cfg.get("schema"):
            raise ValidationError("Registry not configured")

        ok, msg = permission_service.remove_domain_entry(
            host,
            token,
            registry_cfg,
            domain_name,
            principal,
        )
        if not ok:
            raise InfrastructureError("Failed to remove domain permission", detail=msg)
        return {"success": ok, "message": msg}

    # ------------------------------------------------------------------
    # Teams matrix (Registry → Teams)
    # ------------------------------------------------------------------

    @staticmethod
    def build_teams_matrix_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """Return the Teams matrix payload: domains, principals, assignments.

        Payload shape::

            {
              "success": true,
              "domains": ["acme", "beta", ...],
              "principals": [
                {"principal": "alice@acme", "principal_type": "user",
                 "display_name": "Alice"},
                {"principal": "data-eng", "principal_type": "group",
                 "display_name": "data-eng"}
              ],
              "assignments": {
                "acme": {"alice@acme": "editor"},
                "beta": {"data-eng": "viewer"}
              }
            }
        """
        domain_obj, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        app_name = settings.ontobricks_app_name

        # Domains
        domains: List[str] = []
        try:
            svc = RegistryService.from_context(domain_obj, settings)
            ok, names, _msg = svc.list_domains_cached()
            if ok:
                domains = sorted(names)
        except Exception as exc:
            logger.warning("Teams matrix: failed to list domains: %s", exc)

        # Principals from Databricks App ACL
        permission_service.clear_principals_cache()
        app_principals = permission_service.list_app_principals(host, token, app_name)

        principals: List[Dict[str, Any]] = []
        for u in app_principals.get("users", []):
            email = u.get("email") or ""
            if not email:
                continue
            principals.append(
                {
                    "principal": email,
                    "principal_type": "user",
                    "display_name": u.get("display_name") or email,
                }
            )
        for g in app_principals.get("groups", []):
            name = g.get("display_name") or g.get("id") or ""
            if not name:
                continue
            principals.append(
                {
                    "principal": name,
                    "principal_type": "group",
                    "display_name": name,
                }
            )

        # Assignments per domain (key: domain -> {principal: role})
        assignments: Dict[str, Dict[str, str]] = {}
        for domain_name in domains:
            try:
                entries = permission_service.list_domain_entries(
                    host, token, registry_cfg, domain_name
                )
                row: Dict[str, str] = {}
                for e in entries:
                    principal = e.get("principal", "")
                    role = e.get("role", "")
                    if principal and role:
                        row[principal] = role
                if row:
                    assignments[domain_name] = row
            except Exception as exc:
                logger.warning(
                    "Teams matrix: failed to read team for %s: %s", domain_name, exc
                )

        return {
            "success": True,
            "domains": domains,
            "principals": principals,
            "assignments": assignments,
        }

    @staticmethod
    def save_teams_batch_result(
        data: Dict[str, Any],
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Persist a batch of team changes across multiple domains.

        Body shape::

            {
              "changes": [
                {"domain_folder": "acme",
                 "principal": "alice@acme",
                 "principal_type": "user",
                 "display_name": "Alice",
                 "role": "editor"},
                {"domain_folder": "beta",
                 "principal": "bob@acme",
                 "principal_type": "user",
                 "display_name": "Bob",
                 "role": null}           # null = remove
              ]
            }
        """
        changes = data.get("changes") or []
        if not isinstance(changes, list):
            raise ValidationError("Body must include a 'changes' array")

        validated: List[Dict[str, Any]] = []
        for idx, ch in enumerate(changes):
            if not isinstance(ch, dict):
                raise ValidationError(f"Change #{idx} is not an object")
            domain_folder = (ch.get("domain_folder") or "").strip()
            principal = (ch.get("principal") or "").strip()
            principal_type = ch.get("principal_type") or "user"
            display_name = ch.get("display_name") or principal
            role = ch.get("role")

            if not domain_folder:
                raise ValidationError(
                    f"Change #{idx}: 'domain_folder' is required"
                )
            if not principal:
                raise ValidationError(f"Change #{idx}: 'principal' is required")
            if principal_type not in ("user", "group"):
                raise ValidationError(
                    f"Change #{idx}: 'principal_type' must be 'user' or 'group'"
                )
            if role is not None and role not in ASSIGNABLE_ROLES:
                raise ValidationError(
                    f"Change #{idx}: 'role' must be one of "
                    f"{list(ASSIGNABLE_ROLES)} or null"
                )

            validated.append(
                {
                    "domain_folder": domain_folder,
                    "principal": principal,
                    "principal_type": principal_type,
                    "display_name": display_name,
                    "role": role,
                }
            )

        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        if not registry_cfg.get("catalog") or not registry_cfg.get("schema"):
            raise ValidationError("Registry not configured")

        saved, failed = permission_service.save_domain_permissions_batch(
            host, token, registry_cfg, validated
        )

        return {
            "success": len(failed) == 0,
            "saved": saved,
            "failed": failed,
            "total_changes": len(validated),
        }

    @staticmethod
    def human_size(nbytes: int) -> str:
        """Return a human-readable file size string."""
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if abs(nbytes) < 1024:
                return f"{nbytes:.1f} {unit}" if unit != "B" else f"{nbytes} B"
            nbytes /= 1024  # type: ignore[assignment]
        return f"{nbytes:.1f} PB"

    @staticmethod
    def list_schedules_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        scheduler = SettingsService._get_scheduler()
        try:
            entries = scheduler.get_all_schedules(host, token, registry_cfg)
            return {"success": True, "schedules": entries}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("list_schedules failed: %s", e)
            raise InfrastructureError("Failed to list schedules", detail=str(e)) from e

    @staticmethod
    def save_schedule_result(
        data: Dict[str, Any],
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        try:
            domain_name = (
                data.get("domain_name") or data.get("project_name") or ""
            ).strip()
            interval_minutes = int(data.get("interval_minutes", 60))
            drop_existing = bool(data.get("drop_existing", True))
            enabled = bool(data.get("enabled", True))
            version = (data.get("version") or "latest").strip()

            if not domain_name:
                raise ValidationError("Domain name is required")

            _, host, token, registry_cfg = SettingsService._resolve_context(
                session_mgr, settings
            )

            scheduler = SettingsService._get_scheduler()
            ok, msg = scheduler.save_schedule(
                host,
                token,
                registry_cfg,
                settings,
                domain_name,
                interval_minutes,
                drop_existing,
                enabled,
                version=version,
            )
            if not ok:
                raise InfrastructureError("Failed to save schedule", detail=msg)
            return {"success": ok, "message": msg}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("save_schedule failed: %s", e)
            raise InfrastructureError("Failed to save schedule", detail=str(e)) from e

    @staticmethod
    def get_schedule_history_result(
        domain_name: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        scheduler = SettingsService._get_scheduler()
        try:
            entries = scheduler.get_schedule_history(
                host, token, registry_cfg, domain_name
            )
            return {"success": True, "domain_name": domain_name, "history": entries}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("get_schedule_history failed for '%s': %s", domain_name, e)
            raise InfrastructureError(
                "Failed to load schedule history", detail=str(e)
            ) from e

    @staticmethod
    def scheduler_status_payload() -> Dict[str, Any]:
        scheduler = SettingsService._get_scheduler()
        return {"success": True, **scheduler.status()}

    @staticmethod
    def delete_schedule_result(
        domain_name: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        try:
            _, host, token, registry_cfg = SettingsService._resolve_context(
                session_mgr, settings
            )

            scheduler = SettingsService._get_scheduler()
            ok, msg = scheduler.remove_schedule(host, token, registry_cfg, domain_name)
            if not ok:
                raise InfrastructureError("Failed to remove schedule", detail=msg)
            return {"success": ok, "message": msg}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("delete_schedule failed: %s", e)
            raise InfrastructureError("Failed to remove schedule", detail=str(e)) from e

    @staticmethod
    def trigger_schedule_now_result(
        domain_name: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Fire the build schedule for *domain_name* immediately."""
        try:
            _, host, token, registry_cfg = SettingsService._resolve_context(
                session_mgr, settings
            )
            scheduler = SettingsService._get_scheduler()
            ok, msg = scheduler.run_schedule_now(
                host, token, registry_cfg, settings, domain_name
            )
            if not ok:
                raise InfrastructureError(
                    "Failed to trigger schedule", detail=msg
                )
            return {"success": True, "message": msg}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("trigger_schedule_now failed: %s", e)
            raise InfrastructureError(
                "Failed to trigger schedule", detail=str(e)
            ) from e

    # ------------------------------------------------------------------
    # Cohort schedules — periodic Cohort analysis + materialisation
    # ------------------------------------------------------------------

    @staticmethod
    def list_cohort_schedules_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        scheduler = SettingsService._get_scheduler()
        try:
            entries = scheduler.get_all_cohort_schedules(host, token, registry_cfg)
            return {"success": True, "schedules": entries}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("list_cohort_schedules failed: %s", e)
            raise InfrastructureError(
                "Failed to list cohort schedules", detail=str(e)
            ) from e

    @staticmethod
    def list_cohort_rules_for_domain_result(
        domain_name: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Return ``[{id, label}]`` for the saved cohort rules of *domain_name*.

        Reads the latest version of the domain headlessly (no session
        switch) so the schedule modal can list rules for any domain
        in the registry.
        """
        try:
            _, host, token, _registry_cfg = SettingsService._resolve_context(
                session_mgr, settings
            )
            domain_obj = get_domain(session_mgr)
            svc = RegistryService.from_context(domain_obj, settings)
            if not svc.cfg.is_configured:
                raise ValidationError("Registry not configured")

            ok, data, version, err = svc.load_latest_domain_data(domain_name)
            if not ok:
                raise NotFoundError(
                    err or f"Domain '{domain_name}' not found in registry"
                )

            doc = data if isinstance(data, dict) else {}

            # Persisted shape (Volume + Lakebase):
            #   { "info": {...},
            #     "versions": { "<v>": { "ontology": { "cohort_rules": [...] }, ... } } }
            # Try the versioned path first, then fall back to the flat
            # legacy shapes for resilience.
            ontology: Dict[str, Any] = {}
            versions = doc.get("versions") or {}
            if isinstance(versions, dict) and versions:
                version_data = versions.get(version) or versions.get(str(version))
                if version_data is None and versions:
                    # Pick the highest version key as a last resort.
                    try:
                        latest_key = max(
                            versions.keys(), key=lambda v: tuple(int(p) for p in str(v).split("."))
                        )
                    except (TypeError, ValueError):
                        latest_key = next(iter(versions))
                    version_data = versions.get(latest_key)
                if isinstance(version_data, dict):
                    ontology = version_data.get("ontology") or {}
            if not ontology:
                ontology = doc.get("ontology") or {}

            rules = (
                ontology.get("cohort_rules")
                or doc.get("cohort_rules")
                or []
            )
            simple = []
            for r in rules:
                rid = r.get("id", "")
                if not rid:
                    continue
                output = r.get("output") or {}
                uc_table = output.get("uc_table") or {}
                simple.append(
                    {
                        "id": rid,
                        "label": r.get("label", "") or rid,
                        "class_uri": r.get("class_uri", ""),
                        "output": {
                            "graph": bool(output.get("graph", True)),
                            "uc_table": (
                                {
                                    "catalog": uc_table.get("catalog", ""),
                                    "schema": uc_table.get("schema", ""),
                                    "table_name": uc_table.get(
                                        "table_name", ""
                                    ),
                                }
                                if uc_table.get("table_name")
                                else None
                            ),
                        },
                    }
                )
            return {"success": True, "rules": simple}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception(
                "list_cohort_rules_for_domain(%s) failed: %s", domain_name, e
            )
            raise InfrastructureError(
                "Failed to list cohort rules", detail=str(e)
            ) from e

    @staticmethod
    def save_cohort_schedule_result(
        data: Dict[str, Any],
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        try:
            domain_name = (data.get("domain_name") or "").strip()
            rule_id = (data.get("rule_id") or "").strip()
            interval_minutes = int(data.get("interval_minutes", 60))
            enabled = bool(data.get("enabled", True))
            version = (data.get("version") or "latest").strip()
            output_graph = bool(data.get("output_graph", True))
            output_uc = bool(data.get("output_uc", True))

            if not domain_name:
                raise ValidationError("Domain name is required")
            if not rule_id:
                raise ValidationError("Cohort rule id is required")
            if not output_graph and not output_uc:
                raise ValidationError(
                    "At least one output target (graph or UC table) is required"
                )

            _, host, token, registry_cfg = SettingsService._resolve_context(
                session_mgr, settings
            )

            scheduler = SettingsService._get_scheduler()
            ok, msg = scheduler.save_cohort_schedule(
                host,
                token,
                registry_cfg,
                settings,
                domain_name,
                rule_id,
                interval_minutes,
                enabled,
                version=version,
                output_graph=output_graph,
                output_uc=output_uc,
            )
            if not ok:
                raise InfrastructureError(
                    "Failed to save cohort schedule", detail=msg
                )
            return {"success": ok, "message": msg}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("save_cohort_schedule failed: %s", e)
            raise InfrastructureError(
                "Failed to save cohort schedule", detail=str(e)
            ) from e

    @staticmethod
    def get_cohort_schedule_history_result(
        domain_name: str,
        rule_id: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        scheduler = SettingsService._get_scheduler()
        try:
            entries = scheduler.get_cohort_schedule_history(
                host, token, registry_cfg, domain_name, rule_id
            )
            return {
                "success": True,
                "domain_name": domain_name,
                "rule_id": rule_id,
                "history": entries,
            }
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception(
                "get_cohort_schedule_history failed for '%s/%s': %s",
                domain_name,
                rule_id,
                e,
            )
            raise InfrastructureError(
                "Failed to load cohort schedule history", detail=str(e)
            ) from e

    @staticmethod
    def delete_cohort_schedule_result(
        domain_name: str,
        rule_id: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        try:
            _, host, token, registry_cfg = SettingsService._resolve_context(
                session_mgr, settings
            )
            scheduler = SettingsService._get_scheduler()
            ok, msg = scheduler.remove_cohort_schedule(
                host, token, registry_cfg, domain_name, rule_id
            )
            if not ok:
                raise InfrastructureError(
                    "Failed to remove cohort schedule", detail=msg
                )
            return {"success": ok, "message": msg}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("delete_cohort_schedule failed: %s", e)
            raise InfrastructureError(
                "Failed to remove cohort schedule", detail=str(e)
            ) from e

    @staticmethod
    def trigger_cohort_schedule_now_result(
        domain_name: str,
        rule_id: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Fire the cohort materialisation schedule for *(domain, rule)* now."""
        try:
            _, host, token, registry_cfg = SettingsService._resolve_context(
                session_mgr, settings
            )
            scheduler = SettingsService._get_scheduler()
            ok, msg = scheduler.run_cohort_schedule_now(
                host, token, registry_cfg, settings, domain_name, rule_id
            )
            if not ok:
                raise InfrastructureError(
                    "Failed to trigger cohort schedule", detail=msg
                )
            return {"success": True, "message": msg}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("trigger_cohort_schedule_now failed: %s", e)
            raise InfrastructureError(
                "Failed to trigger cohort schedule", detail=str(e)
            ) from e

    # ===========================================
    # OBX export / import (Registry → Browse)
    # ===========================================

    # 50 MB cap matches typical Apps upload limits and protects the
    # in-memory JSON parse on the import side.
    OBX_MAX_BYTES = 50 * 1024 * 1024

    @staticmethod
    def _resolve_versions_for_export(
        svc: RegistryService,
        folder: str,
        mode: str,
        explicit: Optional[List[str]],
    ) -> List[str]:
        """Resolve the list of versions to export for a single domain.

        ``mode`` is one of ``"all" | "active" | "latest" | "selected"``.
        For ``"selected"`` the caller must pass *explicit*; the intersection
        with the actually-present versions is returned (silent drop of
        missing versions).
        """
        available = svc.list_versions_sorted(folder)
        if not available:
            return []
        if mode == "all":
            return available
        if mode == "latest":
            return [available[0]]
        if mode == "active":
            mcp_ver, _ = svc.find_mcp_version(folder)
            return [mcp_ver] if mcp_ver else [available[0]]
        if mode == "selected":
            wanted = [str(v) for v in (explicit or [])]
            return [v for v in available if v in set(wanted)]
        raise ValidationError(
            f"Unknown export mode '{mode}' for domain '{folder}' "
            f"(expected one of: all, active, latest, selected)"
        )

    @staticmethod
    def export_registry_obx_result(
        spec: Dict[str, Any],
        session_mgr: SessionManager,
        settings: Settings,
        exported_by: str = "",
    ) -> Dict[str, Any]:
        """Build a `.obx` envelope from the registry for the requested domains.

        ``spec`` shape::

            {
                "domains": [
                    {
                        "name": "claims",
                        "mode": "all" | "active" | "latest" | "selected",
                        "versions": ["1", "2"]   # required when mode == "selected"
                    }
                ]
            }
        """
        try:
            domain_session = get_domain(session_mgr)
            svc = RegistryService.from_context(domain_session, settings)
            if not svc.cfg.is_configured:
                raise ValidationError("Registry not configured")

            entries = (spec or {}).get("domains") or []
            if not entries:
                raise ValidationError("No domains selected for export")

            exported_domains: List[Dict[str, Any]] = []
            errors: List[str] = []
            for entry in entries:
                name = (entry.get("name") or "").strip()
                if not name:
                    errors.append("Domain entry without a name was skipped")
                    continue
                mode = entry.get("mode") or "latest"
                explicit = entry.get("versions")

                versions = SettingsService._resolve_versions_for_export(
                    svc, name, mode, explicit
                )
                if not versions:
                    errors.append(f'No versions to export for domain "{name}"')
                    continue

                version_docs: Dict[str, Any] = {}
                latest_info: Dict[str, Any] = {}
                for ver in versions:
                    ok, data, msg = svc.read_version(name, ver)
                    if not ok:
                        errors.append(f'{name} v{ver}: {msg}')
                        continue
                    version_docs[ver] = data
                    if not latest_info:
                        latest_info = data.get("info", {}) or {}

                if not version_docs:
                    continue

                exported_domains.append(
                    {
                        "name": name,
                        "info": latest_info,
                        "versions": version_docs,
                    }
                )

            if not exported_domains:
                raise ValidationError(
                    "Nothing to export (no readable versions for the selected domains)"
                )

            envelope = obx_format.build_envelope(
                exported_domains, exported_by=exported_by
            )

            today = time.strftime("%Y-%m-%d")
            filename = f"ontobricks-{today}.obx"

            return {
                "success": True,
                "filename": filename,
                "envelope": envelope,
                "domain_count": len(exported_domains),
                "version_count": sum(
                    len(d["versions"]) for d in exported_domains
                ),
                "warnings": errors,
            }
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("OBX export failed: %s", e)
            raise InfrastructureError("OBX export failed", detail=str(e)) from e

    @staticmethod
    def _decode_obx_payload(file_bytes: bytes) -> Dict[str, Any]:
        """Parse + validate the envelope bytes, returning the upgraded envelope."""
        if not file_bytes:
            raise ValidationError("Empty .obx file")
        if len(file_bytes) > SettingsService.OBX_MAX_BYTES:
            raise ValidationError(
                f".obx file too large ({len(file_bytes)} bytes); "
                f"max {SettingsService.OBX_MAX_BYTES} bytes"
            )
        try:
            envelope = json.loads(file_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValidationError(
                f"Invalid .obx file: not valid JSON ({exc})"
            ) from exc
        return obx_format.load(envelope)

    @staticmethod
    def _suggest_rename(svc: RegistryService, folder: str) -> str:
        """Suggest a free folder name by appending ``_imported`` / ``_2`` / ..."""
        base = sanitize_domain_folder(folder + "_imported")
        candidate = base
        idx = 2
        while svc.domain_exists(candidate):
            candidate = f"{base}_{idx}"
            idx += 1
        return candidate

    @staticmethod
    def preview_obx_import_result(
        file_bytes: bytes,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Parse an uploaded `.obx` file and report per-domain conflict status."""
        try:
            domain_session = get_domain(session_mgr)
            svc = RegistryService.from_context(domain_session, settings)
            if not svc.cfg.is_configured:
                raise ValidationError("Registry not configured")

            envelope = SettingsService._decode_obx_payload(file_bytes)

            domains_preview: List[Dict[str, Any]] = []
            for entry in envelope.get("domains", []):
                raw_name = (entry.get("name") or "").strip()
                if not raw_name:
                    continue
                folder = sanitize_domain_folder(raw_name)
                incoming_versions = sorted(
                    (entry.get("versions") or {}).keys(),
                    key=lambda v: [int(x) for x in v.split(".") if x.isdigit()] or [0],
                    reverse=True,
                )

                exists = svc.domain_exists(folder)
                conflicting_versions: List[str] = []
                if exists:
                    existing = set(svc.list_versions_sorted(folder))
                    conflicting_versions = [
                        v for v in incoming_versions if v in existing
                    ]

                domains_preview.append(
                    {
                        "name": folder,
                        "original_name": raw_name,
                        "incoming_versions": incoming_versions,
                        "exists": exists,
                        "conflicting_versions": conflicting_versions,
                        "suggested_new_name": (
                            SettingsService._suggest_rename(svc, folder)
                            if exists
                            else folder
                        ),
                        "info": entry.get("info") or {},
                    }
                )

            return {
                "success": True,
                "format_version": envelope.get("format_version"),
                "ontobricks_version": envelope.get("ontobricks_version", ""),
                "exported_at": envelope.get("exported_at", ""),
                "exported_by": envelope.get("exported_by", ""),
                "domains": domains_preview,
            }
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("OBX import preview failed: %s", e)
            raise InfrastructureError(
                "Failed to read .obx file", detail=str(e)
            ) from e

    @staticmethod
    def import_registry_obx_result(
        file_bytes: bytes,
        decisions: List[Dict[str, Any]],
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Apply per-domain decisions and write the contents of *file_bytes*
        into the registry.

        Each decision: ``{"name": <folder>, "action": "skip"|"overwrite"|"rename",
        "new_name": <str>}``. Missing entries default to ``"skip"`` so callers
        can't accidentally overwrite a domain they didn't review.
        """
        try:
            domain_session = get_domain(session_mgr)
            svc = RegistryService.from_context(domain_session, settings)
            if not svc.cfg.is_configured:
                raise ValidationError("Registry not configured")

            envelope = SettingsService._decode_obx_payload(file_bytes)

            decision_map: Dict[str, Dict[str, Any]] = {}
            for d in decisions or []:
                key = (d.get("name") or "").strip()
                if key:
                    decision_map[key] = d

            summary = {
                "imported_versions": 0,
                "skipped_domains": 0,
                "renamed_domains": 0,
                "overwritten_versions": 0,
                "errors": [],
                "domains": [],
            }

            for entry in envelope.get("domains", []):
                raw_name = (entry.get("name") or "").strip()
                if not raw_name:
                    summary["errors"].append("Domain entry without a name was skipped")
                    continue

                folder = sanitize_domain_folder(raw_name)
                decision = decision_map.get(folder) or decision_map.get(raw_name) or {}
                action = (decision.get("action") or "skip").lower()

                if action == "skip":
                    summary["skipped_domains"] += 1
                    summary["domains"].append({"name": folder, "action": "skipped"})
                    continue

                target_folder = folder
                if action == "rename":
                    candidate = (decision.get("new_name") or "").strip()
                    target_folder = sanitize_domain_folder(
                        candidate or SettingsService._suggest_rename(svc, folder)
                    )
                    if svc.domain_exists(target_folder):
                        summary["errors"].append(
                            f'Rename target "{target_folder}" already exists; '
                            f'"{folder}" was skipped'
                        )
                        summary["skipped_domains"] += 1
                        summary["domains"].append(
                            {"name": folder, "action": "skipped_rename_conflict"}
                        )
                        continue
                    summary["renamed_domains"] += 1
                elif action != "overwrite":
                    raise ValidationError(
                        f"Unknown import action '{action}' for domain '{folder}'"
                    )

                existing = (
                    set(svc.list_versions_sorted(target_folder))
                    if svc.domain_exists(target_folder)
                    else set()
                )
                versions = entry.get("versions") or {}
                wrote = 0
                overwrote = 0
                for ver, doc in versions.items():
                    if not isinstance(doc, dict):
                        summary["errors"].append(
                            f"{target_folder} v{ver}: payload is not an object, skipped"
                        )
                        continue
                    is_overwrite = ver in existing
                    ok, msg = svc.write_version(target_folder, ver, json.dumps(doc))
                    if not ok:
                        summary["errors"].append(
                            f"{target_folder} v{ver}: {msg}"
                        )
                        continue
                    wrote += 1
                    if is_overwrite:
                        overwrote += 1

                summary["imported_versions"] += wrote
                summary["overwritten_versions"] += overwrote
                summary["domains"].append(
                    {
                        "name": target_folder,
                        "original_name": folder,
                        "action": action,
                        "versions_written": wrote,
                        "versions_overwritten": overwrote,
                    }
                )

            invalidate_registry_cache()

            return {
                "success": True,
                "message": (
                    f"Imported {summary['imported_versions']} version(s) "
                    f"across {len(summary['domains'])} domain(s)"
                ),
                **summary,
            }
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("OBX import failed: %s", e)
            raise InfrastructureError("OBX import failed", detail=str(e)) from e
