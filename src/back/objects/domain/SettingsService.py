"""Databricks settings, registry, permissions, LadybugDB files, and schedule orchestration."""
from __future__ import annotations

import json
import os
import shutil
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

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
)
from back.objects.session import SessionManager, get_domain, global_config_service

logger = get_logger(__name__)

LADYBUG_BASE_DIR = "/tmp/ontobricks"


class SettingsService:
    """Configuration, registry, permissions, local LadybugDB files, and build schedules."""

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
        """True when the registry is supplied by a Databricks App Volume resource."""
        return is_databricks_app() and bool(getattr(settings, "registry_volume_path", ""))

    @staticmethod
    def _resolve_context(session_mgr: SessionManager, settings: Settings):
        """Return the (domain, host, token, registry_cfg_dict) tuple used by most endpoints."""
        domain = get_domain(session_mgr)
        host, token = get_databricks_host_and_token(domain, settings)
        registry_cfg = RegistryCfg.from_domain(domain, settings).as_dict()
        return domain, host, token, registry_cfg

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
            email, host, token, settings.ontobricks_app_name, user_token=user_token,
        ):
            raise AuthorizationError('Only admins (CAN MANAGE) can change the SQL Warehouse')

    @staticmethod
    def build_current_config(session_mgr: SessionManager, settings: Settings) -> Dict[str, Any]:
        """Build the payload for GET /settings/current."""
        domain = get_domain(session_mgr)

        host = domain.databricks.get('host') or settings.databricks_host
        token = domain.databricks.get('token') or settings.databricks_token
        warehouse_id = resolve_warehouse_id(domain, settings)

        has_config = bool(host and (token or settings.databricks_token))
        is_app_mode = bool(settings.databricks_host)

        auth_mode = 'none'
        auth_display = 'Not configured'
        if token:
            auth_mode = 'token'
            auth_display = 'Personal Access Token'
        elif is_app_mode:
            auth_mode = 'app'
            auth_display = 'Databricks App'

        warehouse_locked = SettingsService.is_warehouse_locked(settings)

        return {
            'host': host,
            'token': '***' if token else None,
            'warehouse_id': warehouse_id,
            'from_env': is_app_mode,
            'is_app_mode': is_app_mode,
            'auth_mode': auth_mode,
            'auth_display': auth_display,
            'has_config': has_config,
            'warehouse_locked': warehouse_locked,
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

        if data.get('host'):
            domain.databricks['host'] = data['host']
        if data.get('token'):
            domain.databricks['token'] = data['token']

        if data.get('warehouse_id'):
            if SettingsService.is_warehouse_locked(settings):
                raise ValidationError(
                    'SQL Warehouse is configured via Databricks App resources and cannot be changed here.',
                )

            SettingsService.require_admin_error(email, user_token, session_mgr, settings)
            domain.databricks['warehouse_id'] = data['warehouse_id']

            _, host, token, registry_cfg = SettingsService._resolve_context(session_mgr, settings)
            ok, msg = global_config_service.set_warehouse_id(
                host, token, registry_cfg, data['warehouse_id'],
            )
            if not ok:
                logger.info("Warehouse saved in session only (global: %s)", msg)

        domain.save()
        return {'success': True, 'message': 'Configuration saved'}

    @staticmethod
    async def test_connection(session_mgr: SessionManager, settings: Settings) -> Dict[str, Any]:
        """Test Databricks connectivity; returns success/message dict."""
        try:
            client = get_databricks_client(get_domain(session_mgr), settings)

            if not client:
                raise ValidationError(
                    'Databricks not configured. Please set DATABRICKS_HOST and DATABRICKS_TOKEN.',
                )

            warehouses = await run_blocking(client.get_warehouses)
            return {'success': True, 'message': f'Connection successful. Found {len(warehouses)} warehouses.'}
        except OntoBricksError:
            raise
        except AttributeError as e:
            logger.exception("Test connection AttributeError: %s", e)
            error_msg = str(e)
            if 'NoneType' in error_msg and 'request' in error_msg:
                raise ValidationError(
                    'Databricks SDK not properly initialized. Check your authentication configuration.',
                ) from e
            raise InfrastructureError('Test connection failed', detail=error_msg) from e
        except Exception as e:
            logger.exception("Test connection failed: %s", e)
            raise InfrastructureError('Test connection failed', detail=str(e)) from e

    @staticmethod
    async def fetch_warehouses(session_mgr: SessionManager, settings: Settings) -> Dict[str, Any]:
        """List warehouses from Databricks (``warehouses`` key on success)."""
        try:
            client = get_databricks_client(get_domain(session_mgr), settings)
            if not client:
                raise ValidationError('Databricks not configured')
            return {'warehouses': await run_blocking(client.get_warehouses)}
        except OntoBricksError:
            raise
        except AttributeError as e:
            error_msg = str(e)
            if 'NoneType' in error_msg and 'request' in error_msg:
                logger.warning("Warehouses HTTP client error: %s", e)
                raise ValidationError(
                    'Databricks SDK not properly initialized. Check your authentication configuration.',
                ) from e
            logger.exception("Get warehouses AttributeError: %s", e)
            raise InfrastructureError('Failed to list SQL warehouses', detail=error_msg) from e
        except Exception as e:
            logger.exception("Get warehouses failed: %s", e)
            raise InfrastructureError('Failed to list SQL warehouses', detail=str(e)) from e

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
                'SQL Warehouse is configured via Databricks App resources and cannot be changed here.',
            )

        if not warehouse_id:
            raise ValidationError('No warehouse ID provided')

        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        domain, host, token, registry_cfg = SettingsService._resolve_context(session_mgr, settings)
        domain.databricks['warehouse_id'] = warehouse_id
        domain.save()

        ok, msg = global_config_service.set_warehouse_id(
            host, token, registry_cfg, warehouse_id,
        )
        if not ok:
            logger.info(
                "Warehouse stored in session only (global save failed: %s)", msg,
            )
            return {'success': True, 'message': 'Warehouse selected (stored in session — will persist globally once the registry is configured)'}
        return {'success': True, 'message': 'Warehouse selected'}

    @staticmethod
    async def fetch_catalogs(session_mgr: SessionManager, settings: Settings) -> Dict[str, Any]:
        try:
            client = get_databricks_client(get_domain(session_mgr), settings)
            if not client:
                raise ValidationError('Databricks not configured')
            return {'catalogs': await run_blocking(client.get_catalogs)}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("Get catalogs failed: %s", e)
            raise InfrastructureError('Failed to list Unity Catalog catalogs', detail=str(e)) from e

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
                raise ValidationError('Databricks not configured')
            return {'schemas': await run_blocking(client.get_schemas, catalog)}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("%s failed: %s", log_label, e)
            raise InfrastructureError(f'{log_label} failed', detail=str(e)) from e

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
                raise ValidationError('Databricks not configured')
            return {'volumes': await run_blocking(client.get_volumes, catalog, schema)}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("%s failed: %s", log_label, e)
            raise InfrastructureError(f'{log_label} failed', detail=str(e)) from e

    @staticmethod
    def build_registry_get_payload(session_mgr: SessionManager, settings: Settings) -> Dict[str, Any]:
        """Payload for GET /settings/registry."""
        rcfg = RegistryCfg.from_session(session_mgr, settings)
        initialized = False

        if rcfg.is_configured:
            try:
                svc = RegistryService.from_context(get_domain(session_mgr), settings)
                initialized = svc.is_initialized()
            except Exception:
                logger.debug("Could not check registry marker")

        return {
            'success': True,
            **rcfg.as_dict(),
            'configured': initialized,
            'registry_locked': SettingsService.is_registry_locked(settings),
        }

    @staticmethod
    def apply_registry_save(data: Dict[str, Any], session_mgr: SessionManager) -> Dict[str, Any]:
        """Persist registry catalog/schema/volume from request body."""
        domain = get_domain(session_mgr)
        reg = domain.settings.setdefault('registry', {})
        if data.get('catalog'):
            reg['catalog'] = data['catalog']
        if data.get('schema'):
            reg['schema'] = data['schema']
        if data.get('volume'):
            reg['volume'] = data['volume']
        domain.save()
        return {'success': True, 'message': 'Registry configuration saved'}

    @staticmethod
    def initialize_registry_result(session_mgr: SessionManager, settings: Settings) -> Dict[str, Any]:
        try:
            domain = get_domain(session_mgr)
            svc = RegistryService.from_context(domain, settings)
            if not svc.cfg.is_configured:
                raise ValidationError('Registry catalog, schema, and volume must be configured first')

            client = get_databricks_client(domain, settings)
            if not client:
                raise ValidationError('Databricks not configured')

            ok, msg = svc.initialize(client)
            if not ok:
                raise InfrastructureError('Registry initialization failed', detail=msg)
            return {'success': ok, 'message': msg}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("Initialize registry failed: %s", e)
            raise InfrastructureError('Initialize registry failed', detail=str(e)) from e

    @staticmethod
    def list_registry_domains_result(session_mgr: SessionManager, settings: Settings) -> Dict[str, Any]:
        try:
            domain = get_domain(session_mgr)
            svc = RegistryService.from_context(domain, settings)
            if not svc.cfg.is_configured:
                raise ValidationError('Registry not configured')

            ok, result, msg = svc.list_domain_details_cached()
            if not ok:
                raise InfrastructureError('Failed to list registry domains', detail=msg)
            return {'success': True, 'domains': result}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("List registry domains failed: %s", e)
            raise InfrastructureError('Failed to list registry domains', detail=str(e)) from e

    @staticmethod
    def list_registry_bridges_result(session_mgr: SessionManager, settings: Settings) -> Dict[str, Any]:
        """Return all bridges across every domain in the registry."""
        try:
            domain = get_domain(session_mgr)
            svc = RegistryService.from_context(domain, settings)
            if not svc.cfg.is_configured:
                raise ValidationError('Registry not configured')

            ok, result, msg = svc.list_all_bridges()
            if not ok:
                raise InfrastructureError('Failed to list registry bridges', detail=msg)
            return {'success': True, 'domains': result}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("List registry bridges failed: %s", e)
            raise InfrastructureError('Failed to list registry bridges', detail=str(e)) from e

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
                raise ValidationError('Registry not configured')

            errors = svc.delete_domain(domain_name)

            if errors:
                joined = '; '.join(errors)
                raise InfrastructureError(
                    'Registry domain was only partially deleted',
                    detail=joined,
                )

            return {'success': True, 'message': f'Domain "{domain_name}" deleted from registry'}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("Delete registry domain failed: %s", e)
            raise InfrastructureError('Delete registry domain failed', detail=str(e)) from e

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
                raise ValidationError('Registry not configured')

            d_ok, d_msg = svc.delete_version(domain_name, version)
            if not d_ok:
                raise InfrastructureError('Failed to delete registry version', detail=d_msg)

            return {'success': True, 'message': f'Version {version} deleted from "{domain_name}"'}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("Delete registry version failed: %s", e)
            raise InfrastructureError('Delete registry version failed', detail=str(e)) from e

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
                raise ValidationError('Registry not configured')

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
                    if data.get('info', {}).get('mcp_enabled'):
                        data['info']['mcp_enabled'] = False
                        svc.write_version(domain_name, ver, json.dumps(data))

            ok, data, msg = svc.read_version(domain_name, version)
            if not ok:
                raise InfrastructureError('Failed to read registry version', detail=msg)

            data.setdefault('info', {})['mcp_enabled'] = enabled
            svc.write_version(domain_name, version, json.dumps(data))

            invalidate_registry_cache()

            if domain.domain_folder == domain_name and domain.current_version == version:
                domain.info['mcp_enabled'] = enabled

            return {'success': True, 'version': version, 'active': enabled}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("Set registry version active failed: %s", e)
            raise InfrastructureError('Set registry version active failed', detail=str(e)) from e

    @staticmethod
    def set_default_emoji_result(
        emoji: str,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        _, host, token, registry_cfg = SettingsService._resolve_context(session_mgr, settings)
        ok, msg = global_config_service.set_default_emoji(host, token, registry_cfg, emoji)
        if not ok:
            raise InfrastructureError('Failed to save default emoji', detail=msg)
        return {'success': True, 'emoji': emoji}

    @staticmethod
    def save_base_uri_result(
        base_uri: str,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        _, host, token, registry_cfg = SettingsService._resolve_context(session_mgr, settings)
        ok, msg = global_config_service.set_default_base_uri(host, token, registry_cfg, base_uri)
        if not ok:
            raise InfrastructureError('Failed to save default base URI', detail=msg)
        return {'success': True, 'base_uri': base_uri}

    @staticmethod
    def get_registry_cache_ttl_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        _, host, token, registry_cfg = SettingsService._resolve_context(session_mgr, settings)
        ttl = global_config_service.get_registry_cache_ttl(host, token, registry_cfg)
        return {'success': True, 'registry_cache_ttl': ttl}

    @staticmethod
    def save_registry_cache_ttl_result(
        ttl: int,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        _, host, token, registry_cfg = SettingsService._resolve_context(session_mgr, settings)
        ok, msg = global_config_service.set_registry_cache_ttl(host, token, registry_cfg, ttl)
        if not ok:
            raise InfrastructureError('Failed to save registry cache TTL', detail=msg)
        return {'success': True, 'registry_cache_ttl': max(10, int(ttl))}

    # ------------------------------------------------------------------
    #  Graph DB Engine
    # ------------------------------------------------------------------

    @staticmethod
    def get_graph_engine_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        _, host, token, registry_cfg = SettingsService._resolve_context(session_mgr, settings)
        engine = global_config_service.get_graph_engine(host, token, registry_cfg)
        allowed = list(global_config_service.ALLOWED_GRAPH_ENGINES)
        return {'success': True, 'graph_engine': engine, 'allowed_engines': allowed}

    @staticmethod
    def set_graph_engine_result(
        engine: str,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        _, host, token, registry_cfg = SettingsService._resolve_context(session_mgr, settings)
        ok, msg = global_config_service.set_graph_engine(host, token, registry_cfg, engine)
        if not ok:
            raise ValidationError(msg)
        return {'success': True, 'graph_engine': engine}

    @staticmethod
    def get_graph_engine_config_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Return the engine-specific JSON configuration."""
        _, host, token, registry_cfg = SettingsService._resolve_context(session_mgr, settings)
        cfg = global_config_service.get_graph_engine_config(host, token, registry_cfg)
        return {'success': True, 'graph_engine_config': cfg}

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

        _, host, token, registry_cfg = SettingsService._resolve_context(session_mgr, settings)
        ok, msg = global_config_service.set_graph_engine_config(host, token, registry_cfg, config)
        if not ok:
            raise ValidationError(msg)
        return {'success': True, 'graph_engine_config': config}

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
                'email': email or 'local-user',
                'display_name': display_name or 'Local User',
                'role': 'admin',
                'is_app_mode': False,
            }

        role = 'none'
        is_app_admin = False
        try:
            _, host, token, registry_cfg = SettingsService._resolve_context(session_mgr, settings)

            permission_service.clear_admin_cache(email)
            is_app_admin = permission_service.is_admin(
                email, host, token, settings.ontobricks_app_name,
                user_token=user_token,
            )
            role = permission_service.get_user_role(
                email, host, token, registry_cfg, settings.ontobricks_app_name,
                user_token=user_token,
            )
        except Exception as e:
            logger.error(
                "permissions/me: error resolving role for %s (middleware app/domain role=%r/%r): %s",
                email, user_role, user_domain_role, e,
                exc_info=True,
            )

        return {
            'email': email,
            'display_name': display_name,
            'role': role,
            'is_app_admin': is_app_admin,
            'is_app_mode': True,
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
                principal = acl.get("user_name") or acl.get("group_name") or acl.get("service_principal_name") or ""
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
                    principal = acl.get("user_name") or acl.get("group_name") or acl.get("service_principal_name") or ""
                    for p in acl.get("all_permissions", []):
                        if p.get("permission_level") == "CAN_MANAGE":
                            managers.append(principal)
                diag["user_token_can_manage"] = managers
                diag["email_is_manager"] = email.lower() in [m.lower() for m in managers]
                diag["user_token_error"] = None
            except Exception as e:
                diag["user_token_error"] = f"{type(e).__name__}: {e}"
                diag["user_token_can_manage"] = []
                diag["email_is_manager"] = False
        else:
            diag["email_is_manager"] = email.lower() in [m.lower() for m in diag.get("sdk_can_manage", [])]

        diag["admin_cache"] = {
            k: {"result": v[0], "age_s": round(time.time() - v[1], 1)}
            for k, v in permission_service._admin_cache.items()
        }

        return diag

    @staticmethod
    def list_permissions_result(session_mgr: SessionManager, settings: Settings) -> Dict[str, Any]:
        _, host, token, registry_cfg = SettingsService._resolve_context(session_mgr, settings)
        entries = permission_service.list_entries(host, token, registry_cfg)
        return {'success': True, 'permissions': entries}

    @staticmethod
    def add_permission_result(
        data: Dict[str, Any],
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        principal = data.get('principal', '').strip()
        principal_type = data.get('principal_type', 'user')
        display_name = data.get('display_name', principal)
        role = data.get('role', 'viewer')

        if not principal:
            raise ValidationError('Principal (email or group name) is required')
        if role not in ASSIGNABLE_ROLES:
            raise ValidationError('Role must be "viewer", "editor", or "builder"')

        _, host, token, registry_cfg = SettingsService._resolve_context(session_mgr, settings)
        if not registry_cfg.get('catalog') or not registry_cfg.get('schema'):
            raise ValidationError(
                'Registry not configured. Initialize the registry in Settings first.',
            )

        ok, msg = permission_service.add_or_update_entry(
            host, token, registry_cfg, principal, principal_type, display_name, role,
        )
        if not ok:
            raise InfrastructureError('Failed to add or update permission', detail=msg)
        return {'success': ok, 'message': msg}

    @staticmethod
    def delete_permission_result(
        principal: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        _, host, token, registry_cfg = SettingsService._resolve_context(session_mgr, settings)
        if not registry_cfg.get('catalog') or not registry_cfg.get('schema'):
            raise ValidationError(
                'Registry not configured. Initialize the registry in Settings first.',
            )

        ok, msg = permission_service.remove_entry(host, token, registry_cfg, principal)
        if not ok:
            raise InfrastructureError('Failed to remove permission', detail=msg)
        return {'success': ok, 'message': msg}

    @staticmethod
    def list_principals_result(session_mgr: SessionManager, settings: Settings) -> Dict[str, Any]:
        _, host, token, _ = SettingsService._resolve_context(session_mgr, settings)
        app_name = settings.ontobricks_app_name
        permission_service.clear_principals_cache()
        result = permission_service.list_app_principals(host, token, app_name)
        return {'success': True, 'users': result.get('users', []), 'groups': result.get('groups', [])}

    @staticmethod
    def search_workspace_principals(
        query: str, principal_type: str, session_mgr: SessionManager, settings: Settings,
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
                g for g in all_principals.get("groups", [])
                if q in (g.get("display_name") or "").lower()
            ]
            return {"success": True, "results": groups}

        users = [
            u for u in all_principals.get("users", [])
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
        _, host, token, registry_cfg = SettingsService._resolve_context(session_mgr, settings)
        entries = permission_service.list_domain_entries(host, token, registry_cfg, domain_name)
        return {'success': True, 'domain': domain_name, 'permissions': entries}

    @staticmethod
    def add_domain_permission_result(
        domain_name: str,
        data: Dict[str, Any],
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        principal = data.get('principal', '').strip()
        principal_type = data.get('principal_type', 'user')
        display_name = data.get('display_name', principal)
        role = data.get('role', 'viewer')

        if not principal:
            raise ValidationError('Principal (email or group name) is required')
        if role not in ASSIGNABLE_ROLES:
            raise ValidationError('Role must be "viewer", "editor", or "builder"')
        if not domain_name:
            raise ValidationError('Domain name is required')

        _, host, token, registry_cfg = SettingsService._resolve_context(session_mgr, settings)
        if not registry_cfg.get('catalog') or not registry_cfg.get('schema'):
            raise ValidationError('Registry not configured')

        ok, msg = permission_service.add_or_update_domain_entry(
            host, token, registry_cfg, domain_name,
            principal, principal_type, display_name, role,
        )
        if not ok:
            raise InfrastructureError('Failed to add or update domain permission', detail=msg)
        return {'success': ok, 'message': msg}

    @staticmethod
    def delete_domain_permission_result(
        domain_name: str,
        principal: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        _, host, token, registry_cfg = SettingsService._resolve_context(session_mgr, settings)
        if not registry_cfg.get('catalog') or not registry_cfg.get('schema'):
            raise ValidationError('Registry not configured')

        ok, msg = permission_service.remove_domain_entry(
            host, token, registry_cfg, domain_name, principal,
        )
        if not ok:
            raise InfrastructureError('Failed to remove domain permission', detail=msg)
        return {'success': ok, 'message': msg}

    @staticmethod
    def human_size(nbytes: int) -> str:
        """Return a human-readable file size string."""
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if abs(nbytes) < 1024:
                return f"{nbytes:.1f} {unit}" if unit != "B" else f"{nbytes} B"
            nbytes /= 1024  # type: ignore[assignment]
        return f"{nbytes:.1f} PB"

    @staticmethod
    def list_ladybugdb_files() -> Dict[str, Any]:
        """List files under the LadybugDB local directory."""
        if not os.path.isdir(LADYBUG_BASE_DIR):
            return {"success": True, "files": [], "base_dir": LADYBUG_BASE_DIR}

        items: List[Dict[str, Any]] = []
        try:
            for entry in sorted(os.scandir(LADYBUG_BASE_DIR), key=lambda e: e.name):
                if not entry.name.endswith(".lbug"):
                    continue
                try:
                    stat = entry.stat(follow_symlinks=False)
                except OSError:
                    continue

                if entry.is_dir(follow_symlinks=False):
                    total_size = 0
                    for dirpath, _dirnames, filenames in os.walk(entry.path):
                        for fname in filenames:
                            try:
                                total_size += os.path.getsize(os.path.join(dirpath, fname))
                            except OSError:
                                pass
                    size = total_size
                else:
                    size = stat.st_size

                mtime_dt = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
                items.append({
                    "name": entry.name,
                    "is_dir": entry.is_dir(follow_symlinks=False),
                    "size_bytes": size,
                    "size_display": SettingsService.human_size(size),
                    "modified_iso": mtime_dt.isoformat(),
                    "modified_display": mtime_dt.strftime("%Y-%m-%d %H:%M:%S UTC"),
                })
        except OSError as exc:
            logger.warning("Failed to list LadybugDB directory: %s", exc)
            raise InfrastructureError('Failed to list LadybugDB files', detail=str(exc)) from exc

        return {"success": True, "files": items, "base_dir": LADYBUG_BASE_DIR}

    @staticmethod
    def delete_ladybugdb_file(filename: str) -> Dict[str, Any]:
        """Delete a file or directory under LadybugDB base dir (basename only)."""
        safe_name = os.path.basename(filename)
        if not safe_name or safe_name in (".", ".."):
            raise ValidationError('Invalid filename')

        target = os.path.join(LADYBUG_BASE_DIR, safe_name)
        if not os.path.exists(target):
            raise NotFoundError(f'Not found: {safe_name}')

        try:
            if os.path.isdir(target):
                shutil.rmtree(target)
            else:
                os.remove(target)
            logger.info("Deleted LadybugDB file: %s", target)
            return {"success": True, "message": f"Deleted {safe_name}"}
        except OSError as exc:
            logger.warning("Failed to delete LadybugDB file %s: %s", target, exc)
            raise InfrastructureError('Failed to delete LadybugDB file', detail=str(exc)) from exc

    @staticmethod
    def list_schedules_result(session_mgr: SessionManager, settings: Settings) -> Dict[str, Any]:
        _, host, token, registry_cfg = SettingsService._resolve_context(session_mgr, settings)
        scheduler = SettingsService._get_scheduler()
        try:
            entries = scheduler.get_all_schedules(host, token, registry_cfg)
            return {"success": True, "schedules": entries}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("list_schedules failed: %s", e)
            raise InfrastructureError('Failed to list schedules', detail=str(e)) from e

    @staticmethod
    def save_schedule_result(
        data: Dict[str, Any],
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        try:
            domain_name = (
                (data.get("domain_name") or data.get("project_name") or "").strip()
            )
            interval_minutes = int(data.get("interval_minutes", 60))
            drop_existing = bool(data.get("drop_existing", True))
            enabled = bool(data.get("enabled", True))
            version = (data.get("version") or "latest").strip()

            if not domain_name:
                raise ValidationError('Domain name is required')

            _, host, token, registry_cfg = SettingsService._resolve_context(session_mgr, settings)

            scheduler = SettingsService._get_scheduler()
            ok, msg = scheduler.save_schedule(
                host, token, registry_cfg, settings, domain_name, interval_minutes,
                drop_existing, enabled, version=version,
            )
            if not ok:
                raise InfrastructureError('Failed to save schedule', detail=msg)
            return {"success": ok, "message": msg}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("save_schedule failed: %s", e)
            raise InfrastructureError('Failed to save schedule', detail=str(e)) from e

    @staticmethod
    def get_schedule_history_result(
        domain_name: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        _, host, token, registry_cfg = SettingsService._resolve_context(session_mgr, settings)
        scheduler = SettingsService._get_scheduler()
        try:
            entries = scheduler.get_schedule_history(host, token, registry_cfg, domain_name)
            return {"success": True, "domain_name": domain_name, "history": entries}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("get_schedule_history failed for '%s': %s", domain_name, e)
            raise InfrastructureError('Failed to load schedule history', detail=str(e)) from e

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
            _, host, token, registry_cfg = SettingsService._resolve_context(session_mgr, settings)

            scheduler = SettingsService._get_scheduler()
            ok, msg = scheduler.remove_schedule(host, token, registry_cfg, domain_name)
            if not ok:
                raise InfrastructureError('Failed to remove schedule', detail=msg)
            return {"success": ok, "message": msg}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("delete_schedule failed: %s", e)
            raise InfrastructureError('Failed to remove schedule', detail=str(e)) from e
