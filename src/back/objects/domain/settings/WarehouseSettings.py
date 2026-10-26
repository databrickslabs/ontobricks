"""SQL warehouse selection and Databricks connection Settings.

Extracted from :class:`SettingsService` (Fowler Extract Class).
``SettingsService`` keeps one-line delegators for callers.
"""

from __future__ import annotations

import importlib
from typing import Any, Dict, Optional

from back.core.errors import InfrastructureError, OntoBricksError, ValidationError
from shared.config.settings import Settings
from back.core.logging import get_logger
from back.objects.session import SessionManager
from back.objects.domain.settings.SettingsService import SettingsService

# Package ``__init__`` binds ``SettingsService`` as the class, which shadows
# the submodule. Tests patch that module's globals (``get_domain``,
# ``global_config_service``, warehouse resolvers).
_ss = importlib.import_module("back.objects.domain.settings.SettingsService")

logger = get_logger(__name__)


class WarehouseSettings:
    """Build/Query SQL warehouses, connection test, and current-config payload."""

    @staticmethod
    def is_warehouse_locked(settings: Settings) -> bool:
        """True when the SQL Warehouse is supplied by a Databricks App resource."""
        return _ss.is_databricks_app() and bool(settings.sql_warehouse_id)

    @staticmethod
    def build_current_config(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """Build the payload for GET /settings/current."""
        domain = _ss.get_domain(session_mgr)

        host = domain.databricks.get("host") or settings.databricks_host
        token = domain.databricks.get("token") or settings.databricks_token
        warehouse_id = _ss.resolve_warehouse_id(domain, settings)
        use_cloud_fetch = _ss.resolve_use_cloud_fetch(domain, settings)

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
            "warehouse_use_sea": False,
            "use_cloud_fetch": use_cloud_fetch,
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
        domain = _ss.get_domain(session_mgr)

        if data.get("host"):
            domain.databricks["host"] = data["host"]

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
            ok, msg = _ss.global_config_service.set_warehouse_id(
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
            client = _ss.get_databricks_client(_ss.get_domain(session_mgr), settings)

            if not client:
                raise ValidationError(
                    "Databricks not configured. Please set DATABRICKS_HOST and DATABRICKS_TOKEN.",
                )

            warehouses = await _ss.run_blocking(client.get_warehouses)
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
            client = _ss.get_databricks_client(_ss.get_domain(session_mgr), settings)
            if not client:
                raise ValidationError("Databricks not configured")
            return {"warehouses": await _ss.run_blocking(client.get_warehouses)}
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

        ok, msg = _ss.global_config_service.set_warehouse_id(
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
    def select_build_warehouse(
        warehouse_id: Optional[str],
        warehouse_type: Optional[str],
        use_sea: bool,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Persist the non-RT warehouse used for build SQL.

        ``use_sea`` is accepted for backward API compatibility but deliberately
        ignored: build DDL and writes must always use the Thrift transport.
        """
        wid = (warehouse_id or "").strip()
        if not wid:
            raise ValidationError("No Build SQL Warehouse selected")
        if (warehouse_type or "").strip().upper() == "REYDEN":
            raise ValidationError(
                "Lakehouse//RT does not support build DDL or writes. "
                "Select a classic or serverless SQL warehouse."
            )

        SettingsService.require_admin_error(email, user_token, session_mgr, settings)
        domain, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        domain.databricks["warehouse_id"] = wid
        domain.save()
        ok, msg = _ss.global_config_service.set_build_warehouse(
            host,
            token,
            registry_cfg,
            wid,
            use_sea=False,
        )
        if not ok:
            raise InfrastructureError(
                "Failed to save the Build SQL Warehouse", detail=msg
            )
        return {
            "success": True,
            "warehouse_id": wid,
            "use_sea": False,
        }

    @staticmethod
    def select_delta_warehouse(
        warehouse_id: Optional[str],
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
        *,
        use_sea: bool = False,
    ) -> Dict[str, Any]:
        """Persist Delta triple-store warehouse selection in global config.

        *use_sea* is the compatibility key that enables the native Kernel
        Statement Execution API path required by Lakehouse/RT warehouses.
        """
        if warehouse_id is None:
            raise ValidationError("No warehouse ID provided")

        wid = (warehouse_id or "").strip()
        use_sea = bool(use_sea)
        if use_sea:
            if not wid:
                raise ValidationError(
                    "Select a Query SQL Warehouse when Lakehouse//RT is enabled"
                )
            build_wid = _ss.resolve_warehouse_id(_ss.get_domain(session_mgr), settings)
            if wid == build_wid:
                raise ValidationError(
                    "The Lakehouse//RT Query SQL Warehouse must be different "
                    "from the Build SQL Warehouse"
                )
        else:
            # Non-RT reads deliberately share Build; remove any stale override.
            wid = ""
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        domain, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        ok, msg = _ss.global_config_service.set_delta_warehouse_id(
            host,
            token,
            registry_cfg,
            wid,
            use_sea=use_sea,
        )
        if not ok:
            logger.warning(
                "Delta warehouse save failed: %s",
                msg,
            )
            raise ValidationError(msg)
        _ss.global_config_service.load(host, token, registry_cfg, force=True)
        SettingsService._mirror_graph_engine_to_domain_registry(
            session_mgr, delta_warehouse_id=wid
        )
        return {
            "success": True,
            "message": (
                "Delta SQL Warehouse selected"
                if wid
                else "Query SQL Warehouse cleared — using Build SQL Warehouse"
            ),
            "delta_warehouse_id": wid,
            "use_sea": use_sea,
            "effective_delta_warehouse_id": _ss.resolve_delta_warehouse_id(
                domain, settings
            ),
        }

    @staticmethod
    def get_delta_warehouse_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Return the Delta SQL-warehouse selection + registry location.

        Backend *selection* moved per-domain; this endpoint now only surfaces
        the workspace-global Delta connection config (which SQL warehouse
        materializes Delta triples) plus the registry catalog/schema used by the
        Settings Delta panel.
        """
        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        _ss.global_config_service.load(host, token, registry_cfg, force=True)
        domain = _ss.get_domain(session_mgr)
        delta_wid = _ss.global_config_service.get_delta_warehouse_id(
            host, token, registry_cfg
        )
        delta_use_sea = _ss.global_config_service.get_delta_warehouse_use_sea(
            host, token, registry_cfg
        )
        reg = registry_cfg if isinstance(registry_cfg, dict) else {}
        catalog = (reg.get("catalog") or "").strip()
        schema = (reg.get("schema") or "").strip()
        storage_location = f"{catalog}.{schema}" if catalog and schema else ""
        return {
            "success": True,
            "delta_warehouse_id": delta_wid,
            "use_sea": delta_use_sea,
            "effective_delta_warehouse_id": _ss.resolve_delta_warehouse_id(
                domain, settings
            ),
            "registry_catalog": catalog,
            "registry_schema": schema,
            "storage_location": storage_location,
            "registry_configured": bool(storage_location),
        }
