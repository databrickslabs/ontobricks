"""Unity Catalog browse, Lakebase registry status, initialize, and grants.

Extracted from :class:`SettingsService` (Fowler Extract Class).
``SettingsService`` keeps one-line delegators for callers.
"""

from __future__ import annotations

import importlib
from typing import Any, Dict, List, Optional

from back.core.errors import InfrastructureError, OntoBricksError, ValidationError
from shared.config.settings import Settings
from back.core.databricks.lakebase.grants import resolve_mcp_app_name
from back.core.helpers import get_databricks_client, run_blocking
from back.core.logging import get_logger
from back.objects.registry import RegistryCfg, RegistryService
from back.objects.session import SessionManager, get_domain
from back.objects.domain.settings.SettingsService import SettingsService

# Package ``__init__`` binds ``SettingsService`` as the class, which shadows
# the submodule. Tests patch that module's globals (``global_config_service``).
_ss = importlib.import_module("back.objects.domain.settings.SettingsService")

logger = get_logger(__name__)


class RegistrySettings:
    """Settings → Registry: UC probes, schema status, Initialize, Repair grants."""

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
    async def fetch_uc_assets(
        catalog: str,
        schema: str,
        session_mgr: SessionManager,
        settings: Settings,
        log_label: str = "Get UC assets",
    ) -> Dict[str, Any]:
        """List tables and views in *catalog*.*schema* (with ``table_type``)."""
        try:
            client = get_databricks_client(get_domain(session_mgr), settings)
            if not client:
                raise ValidationError("Databricks not configured")
            assets = await run_blocking(
                client.list_tables_and_views, catalog, schema
            )
            return {"success": True, "assets": assets}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("%s failed: %s", log_label, e)
            raise InfrastructureError(f"{log_label} failed", detail=str(e)) from e

    @staticmethod
    async def fetch_uc_functions(
        catalog: str,
        schema: str,
        session_mgr: SessionManager,
        settings: Settings,
        log_label: str = "Get UC functions",
    ) -> Dict[str, Any]:
        """List user-defined functions in *catalog*.*schema*.

        Used by the ontology *Actions* and *Virtual Attributes* pickers. Both
        only bind functions taking exactly one parameter (the entity ID), so
        ``param_count`` is surfaced for client-side filtering; the virtual
        attribute picker additionally reads ``return_columns`` to derive one
        attribute per result column.
        """
        try:
            client = get_databricks_client(get_domain(session_mgr), settings)
            if not client:
                raise ValidationError("Databricks not configured")
            functions = await run_blocking(client.list_functions, catalog, schema)
            return {"success": True, "functions": functions}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("%s failed: %s", log_label, e)
            raise InfrastructureError(f"{log_label} failed", detail=str(e)) from e

    @staticmethod
    async def fetch_entity_jobs(
        session_mgr: SessionManager,
        settings: Settings,
        *,
        query: str = "",
        log_label: str = "Get entity jobs",
    ) -> Dict[str, Any]:
        """List workspace Jobs that declare an ``entity_id`` parameter."""
        from back.core.databricks.jobs import JobActionRunner

        try:
            domain = get_domain(session_mgr)
            runner = JobActionRunner.from_domain(domain, settings)
            jobs = await run_blocking(runner.list_entity_jobs)
            needle = (query or "").strip().lower()
            if needle:
                jobs = [j for j in jobs if needle in str(j.get("name") or "").lower()]
            return {"success": True, "jobs": jobs}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("%s failed: %s", log_label, e)
            raise InfrastructureError(f"{log_label} failed", detail=str(e)) from e

    @staticmethod
    async def check_lakebase_permissions(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """Run a comprehensive Lakebase permission check for the registry schema.

        Delegates to :meth:`LakebaseRegistryStore.check_permissions` which
        probes connection, schema existence/privileges, and per-table CRUD
        rights in a single round-trip. Raises ``ValidationError`` /
        ``InfrastructureError`` when the registry is unbound or Lakebase is unavailable.
        """
        rcfg = RegistryCfg.from_session(session_mgr, settings)
        if not rcfg.is_configured:
            raise ValidationError(
                "Registry not configured — set REGISTRY_CATALOG / REGISTRY_SCHEMA"
            )
        try:
            from back.objects.registry.store import RegistryFactory  # noqa: PLC0415
            store = RegistryFactory.lakebase(
                registry_cfg=rcfg,
                schema=rcfg.lakebase_schema,
                database=rcfg.lakebase_database,
            )
            return await run_blocking(store.check_permissions)
        except ImportError as exc:
            raise InfrastructureError(
                "psycopg is not installed — Lakebase backend unavailable."
            ) from exc
        except Exception as exc:
            logger.warning("check_lakebase_permissions failed: %s", exc)
            raise InfrastructureError(
                str(exc) or "Lakebase permission check failed", detail=str(exc)
            ) from exc

    @staticmethod
    async def check_registry_access(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """Verify that the configured UC schema and Volume exist and are accessible.

        Performs two independent REST API probes (no warehouse required):

        1. ``catalog.schema`` — checks existence and USE SCHEMA privilege.
        2. ``catalog.schema.volume`` — checks existence and READ VOLUME privilege.

        The result shape::

            {
              "success": True,
              "schema": {
                "path": "my_catalog.my_schema",
                "exists": bool | None,
                "accessible": bool,
                "error": str | None,
              },
              "volume": {
                "name": "OntoBricksRegistry",
                "path": "my_catalog.my_schema.OntoBricksRegistry",
                "exists": bool | None,
                "accessible": bool,
                "error": str | None,
                "volume_type": "MANAGED" | "EXTERNAL",
              },
            }
        """
        rcfg = RegistryCfg.from_session(session_mgr, settings)
        if not rcfg.is_configured:
            raise ValidationError(
                "Registry not configured — set REGISTRY_CATALOG / REGISTRY_SCHEMA"
            )

        client = get_databricks_client(get_domain(session_mgr), settings)
        if not client:
            raise InfrastructureError("Databricks client not available")

        schema_result = await run_blocking(
            client.catalog.check_schema_access, rcfg.catalog, rcfg.schema
        )
        schema_result["path"] = f"{rcfg.catalog}.{rcfg.schema}"

        vol_name = rcfg.volume or "OntoBricksRegistry"
        volume_result = await run_blocking(
            client.catalog.check_volume_access, rcfg.catalog, rcfg.schema, vol_name
        )
        volume_result["name"] = vol_name
        volume_result["path"] = f"{rcfg.catalog}.{rcfg.schema}.{vol_name}"

        return {"success": True, "schema": schema_result, "volume": volume_result}

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

        graph_engine_config: Dict[str, Any] = {}
        delta_warehouse_id = ""
        if rcfg.is_configured:
            try:
                _, host, token, registry_cfg = SettingsService._resolve_context(
                    session_mgr, settings
                )
                _ss.global_config_service.load(host, token, registry_cfg)
                graph_engine_config = _ss.global_config_service.get_graph_engine_config(
                    host, token, registry_cfg
                )
                delta_warehouse_id = _ss.global_config_service.get_delta_warehouse_id(
                    host, token, registry_cfg
                )
            except Exception:
                logger.debug(
                    "Could not load graph engine config for registry GET payload",
                    exc_info=True,
                )

        return {
            "success": True,
            **rcfg.as_dict(),
            "configured": initialized,
            "registry_locked": SettingsService.is_registry_locked(settings),
            "lakebase": SettingsService._lakebase_runtime_info(rcfg),
            "graph_engine_config": graph_engine_config,
            "delta_warehouse_id": delta_warehouse_id,
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
                blob = _ss.global_config_service.load(host, token, registry_cfg, force=True)
                if isinstance(blob, dict) and "graph_engine" not in blob:
                    ok_seed, msg_seed = _ss.global_config_service._save(
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
            # Self-serve the Lakebase grants the app + MCP service principals
            # need (in-app port of scripts/bootstrap-lakebase-perms.sh). The
            # app SP owns the schema it just created, so the Postgres grants
            # always apply; CAN_USE / UC grants are best-effort. Failures are
            # surfaced in the payload, never fatal to Initialize itself.
            result: Dict[str, Any] = {"success": ok, "message": msg}
            try:
                grant_summary = SettingsService._grant_registry_permissions(
                    session_mgr, settings
                )
                if grant_summary is not None:
                    result["permissions"] = grant_summary
            except Exception:  # noqa: BLE001
                logger.debug(
                    "Post-initialize permission grant skipped", exc_info=True
                )
            return result
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("Initialize registry failed: %s", e)
            raise InfrastructureError(
                "Initialize registry failed", detail=str(e)
            ) from e

    @staticmethod
    def _registry_grant_app_names(settings: Settings) -> List[str]:
        """Apps whose service principals receive the registry grants.

        The running app first, then the MCP companion
        (``resolve_mcp_app_name`` — same derivation as
        ``scripts/deploy.config.sh`` / the graph-DB provisioning flow).
        """
        app_name = (getattr(settings, "ontobricks_app_name", "") or "").strip()
        mcp_app_name = resolve_mcp_app_name(app_name)
        names: List[str] = []
        for candidate in (app_name, mcp_app_name):
            if candidate and candidate not in names:
                names.append(candidate)
        return names

    @staticmethod
    def _grant_registry_permissions(
        session_mgr: SessionManager, settings: Settings
    ) -> Optional[Dict[str, Any]]:
        """Apply Lakebase project + registry-schema + UC grants to the app SPs.

        Synchronous core shared by :meth:`initialize_registry_result`
        (auto-run) and :meth:`grant_registry_permissions_result` (the
        explicit *Repair permissions* button). Returns ``None`` when the
        registry is not configured or the Lakebase backend is unavailable;
        otherwise the ``grant_app_permissions`` summary dict.
        """
        rcfg = RegistryCfg.from_session(session_mgr, settings)
        if not rcfg.is_configured:
            return None
        app_names = SettingsService._registry_grant_app_names(settings)
        if not app_names:
            raise ValidationError(
                "Could not determine the app name to grant — set ONTOBRICKS_APP_NAME."
            )
        try:
            from back.objects.registry.store import RegistryFactory  # noqa: PLC0415

            store = RegistryFactory.lakebase(
                registry_cfg=rcfg,
                schema=rcfg.lakebase_schema,
                database=rcfg.lakebase_database,
            )
        except ImportError as exc:
            raise InfrastructureError(
                "psycopg is not installed — Lakebase backend unavailable."
            ) from exc
        return store.grant_app_permissions(
            app_names=app_names,
            uc_catalog=(rcfg.catalog or "").strip(),
        )

    @staticmethod
    async def grant_registry_permissions_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """Explicit *Repair permissions* action for the Registry page.

        In-app equivalent of ``scripts/bootstrap-lakebase-perms.sh`` for the
        registry schema: re-applies CAN_USE on the project, USAGE/DML on the
        schema, and ALL_PRIVILEGES on the UC catalog to the app + MCP service
        principals. Idempotent and safe to re-run after a rebind/redeploy.
        """
        rcfg = RegistryCfg.from_session(session_mgr, settings)
        if not rcfg.is_configured:
            raise ValidationError(
                "Registry not configured — set REGISTRY_CATALOG / REGISTRY_SCHEMA"
            )
        try:
            summary = await run_blocking(
                SettingsService._grant_registry_permissions, session_mgr, settings
            )
        except (ValidationError, InfrastructureError):
            raise
        except Exception as exc:  # noqa: BLE001
            logger.warning("grant_registry_permissions failed: %s", exc)
            raise InfrastructureError(
                str(exc) or "Permission grant failed", detail=str(exc)
            ) from exc
        if summary is None:
            raise InfrastructureError("Lakebase registry backend is not available.")
        return summary
