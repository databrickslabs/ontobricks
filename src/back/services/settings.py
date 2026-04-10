"""Configuration business logic for Databricks settings, registry, permissions, and related UI flows.

HTTP routing stays in ``routes``; this module holds orchestration, validation, and data shaping.
"""
from __future__ import annotations

import os
import shutil
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import Request

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
    RegistryCfg,
    RegistryService,
    permission_service,
)
from back.objects.session import SessionManager, get_project, global_config_service

logger = get_logger(__name__)


def _get_scheduler():
    """Defer APScheduler import until schedule endpoints run."""
    from back.objects.registry import get_scheduler as _gs

    return _gs()

LADYBUG_BASE_DIR = "/tmp/ontobricks"


# --- Lock / context helpers ---


def is_warehouse_locked(settings: Settings) -> bool:
    """True when the SQL Warehouse is supplied by a Databricks App resource."""
    return is_databricks_app() and bool(settings.sql_warehouse_id)


def is_registry_locked(settings: Settings) -> bool:
    """True when the registry is supplied by a Databricks App Volume resource."""
    return is_databricks_app() and bool(getattr(settings, "registry_volume_path", ""))


def _resolve_context(session_mgr: SessionManager, settings: Settings):
    """Return the (project, host, token, registry_cfg_dict) tuple used by most endpoints."""
    project = get_project(session_mgr)
    host, token = get_databricks_host_and_token(project, settings)
    registry_cfg = RegistryCfg.from_project(project, settings).as_dict()
    return project, host, token, registry_cfg


def require_admin_error(
    request: Request,
    session_mgr: SessionManager,
    settings: Settings,
) -> Optional[Dict[str, Any]]:
    """Return an error response dict if the caller is not an admin, else None."""
    if not is_databricks_app():
        return None

    email = getattr(request.state, 'user_email', '') or request.headers.get('x-forwarded-email', '')
    _, host, token, _ = _resolve_context(session_mgr, settings)
    user_token = request.headers.get('x-forwarded-access-token', '')
    if not permission_service.is_admin(
        email, host, token, settings.ontobricks_app_name, user_token=user_token,
    ):
        return {'success': False, 'message': 'Only admins (CAN MANAGE) can change the SQL Warehouse'}
    return None


# --- Main configuration ---


def build_current_config(session_mgr: SessionManager, settings: Settings) -> Dict[str, Any]:
    """Build the payload for GET /settings/current."""
    project = get_project(session_mgr)

    host = project.databricks.get('host') or settings.databricks_host
    token = project.databricks.get('token') or settings.databricks_token
    warehouse_id = resolve_warehouse_id(project, settings)

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

    warehouse_locked = is_warehouse_locked(settings)

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


def apply_config_save(
    data: Dict[str, Any],
    request: Request,
    session_mgr: SessionManager,
    settings: Settings,
) -> Dict[str, Any]:
    """Apply POST /settings/save body to session and optional global warehouse."""
    project = get_project(session_mgr)

    if data.get('host'):
        project.databricks['host'] = data['host']
    if data.get('token'):
        project.databricks['token'] = data['token']

    if data.get('warehouse_id'):
        if is_warehouse_locked(settings):
            return {'success': False, 'message': 'SQL Warehouse is configured via Databricks App resources and cannot be changed here.'}

        project.databricks['warehouse_id'] = data['warehouse_id']

        admin_err = require_admin_error(request, session_mgr, settings)
        if admin_err is None:
            _, host, token, registry_cfg = _resolve_context(session_mgr, settings)
            ok, msg = global_config_service.set_warehouse_id(
                host, token, registry_cfg, data['warehouse_id'],
            )
            if not ok:
                logger.info("Warehouse saved in session only (global: %s)", msg)

    project.save()
    return {'success': True, 'message': 'Configuration saved'}


async def test_connection(session_mgr: SessionManager, settings: Settings) -> Dict[str, Any]:
    """Test Databricks connectivity; returns success/message dict."""
    try:
        client = get_databricks_client(get_project(session_mgr), settings)

        if not client:
            return {'success': False, 'message': 'Databricks not configured. Please set DATABRICKS_HOST and DATABRICKS_TOKEN.'}

        warehouses = await run_blocking(client.get_warehouses)
        return {'success': True, 'message': f'Connection successful. Found {len(warehouses)} warehouses.'}
    except AttributeError as e:
        logger.exception("Test connection AttributeError: %s", e)
        error_msg = str(e)
        if 'NoneType' in error_msg and 'request' in error_msg:
            return {'success': False, 'message': 'Databricks SDK not properly initialized. Check your authentication configuration.'}
        return {'success': False, 'message': error_msg}
    except Exception as e:
        logger.exception("Test connection failed: %s", e)
        return {'success': False, 'message': str(e)}


async def fetch_warehouses(session_mgr: SessionManager, settings: Settings) -> Dict[str, Any]:
    """List warehouses or return {'error': ...}."""
    try:
        client = get_databricks_client(get_project(session_mgr), settings)
        if not client:
            return {'error': 'Databricks not configured'}
        return {'warehouses': await run_blocking(client.get_warehouses)}
    except AttributeError as e:
        error_msg = str(e)
        if 'NoneType' in error_msg and 'request' in error_msg:
            logger.warning("Warehouses HTTP client error: %s", e)
            return {'error': 'Databricks SDK not properly initialized. Check your authentication configuration.'}
        logger.exception("Get warehouses AttributeError: %s", e)
        return {'error': error_msg}
    except Exception as e:
        logger.exception("Get warehouses failed: %s", e)
        return {'error': str(e)}


def select_warehouse(
    warehouse_id: Optional[str],
    request: Request,
    session_mgr: SessionManager,
    settings: Settings,
) -> Dict[str, Any]:
    """Persist warehouse selection in session and attempt global registry update."""
    if is_warehouse_locked(settings):
        return {'success': False, 'message': 'SQL Warehouse is configured via Databricks App resources and cannot be changed here.'}

    if not warehouse_id:
        return {'success': False, 'message': 'No warehouse ID provided'}

    admin_err = require_admin_error(request, session_mgr, settings)
    if admin_err:
        return admin_err

    project, host, token, registry_cfg = _resolve_context(session_mgr, settings)
    project.databricks['warehouse_id'] = warehouse_id
    project.save()

    ok, msg = global_config_service.set_warehouse_id(
        host, token, registry_cfg, warehouse_id,
    )
    if not ok:
        logger.info(
            "Warehouse stored in session only (global save failed: %s)", msg,
        )
        return {'success': True, 'message': 'Warehouse selected (stored in session — will persist globally once the registry is configured)'}
    return {'success': True, 'message': 'Warehouse selected'}


# --- Catalog / schema / volume ---


async def fetch_catalogs(session_mgr: SessionManager, settings: Settings) -> Dict[str, Any]:
    try:
        client = get_databricks_client(get_project(session_mgr), settings)
        if not client:
            return {'error': 'Databricks not configured'}
        return {'catalogs': await run_blocking(client.get_catalogs)}
    except Exception as e:
        logger.exception("Get catalogs failed: %s", e)
        return {'error': str(e)}


async def fetch_schemas(
    catalog: str,
    session_mgr: SessionManager,
    settings: Settings,
    *,
    log_label: str = "Get schemas",
) -> Dict[str, Any]:
    try:
        client = get_databricks_client(get_project(session_mgr), settings)
        if not client:
            return {'error': 'Databricks not configured'}
        return {'schemas': await run_blocking(client.get_schemas, catalog)}
    except Exception as e:
        logger.exception("%s failed: %s", log_label, e)
        return {'error': str(e)}


async def fetch_volumes(
    catalog: str,
    schema: str,
    session_mgr: SessionManager,
    settings: Settings,
    log_label: str = "Get volumes",
) -> Dict[str, Any]:
    try:
        client = get_databricks_client(get_project(session_mgr), settings)
        if not client:
            return {'error': 'Databricks not configured'}
        return {'volumes': await run_blocking(client.get_volumes, catalog, schema)}
    except Exception as e:
        logger.exception("%s failed: %s", log_label, e)
        return {'error': str(e)}


# --- Registry ---


def build_registry_get_payload(session_mgr: SessionManager, settings: Settings) -> Dict[str, Any]:
    """Payload for GET /settings/registry."""
    rcfg = RegistryCfg.from_session(session_mgr, settings)
    initialized = False

    if rcfg.is_configured:
        try:
            svc = RegistryService.from_context(get_project(session_mgr), settings)
            initialized = svc.is_initialized()
        except Exception:
            logger.debug("Could not check registry marker")

    return {
        'success': True,
        **rcfg.as_dict(),
        'configured': initialized,
        'registry_locked': is_registry_locked(settings),
    }


def apply_registry_save(data: Dict[str, Any], session_mgr: SessionManager) -> Dict[str, Any]:
    """Persist registry catalog/schema/volume from request body."""
    project = get_project(session_mgr)
    reg = project.settings.setdefault('registry', {})
    if data.get('catalog'):
        reg['catalog'] = data['catalog']
    if data.get('schema'):
        reg['schema'] = data['schema']
    if data.get('volume'):
        reg['volume'] = data['volume']
    project.save()
    return {'success': True, 'message': 'Registry configuration saved'}


def initialize_registry_result(session_mgr: SessionManager, settings: Settings) -> Dict[str, Any]:
    try:
        project = get_project(session_mgr)
        svc = RegistryService.from_context(project, settings)
        if not svc.cfg.is_configured:
            return {'success': False, 'message': 'Registry catalog, schema, and volume must be configured first'}

        client = get_databricks_client(project, settings)
        if not client:
            return {'success': False, 'message': 'Databricks not configured'}

        ok, msg = svc.initialize(client)
        return {'success': ok, 'message': msg}
    except Exception as e:
        logger.exception("Initialize registry failed: %s", e)
        return {'success': False, 'message': str(e)}


def list_registry_projects_result(session_mgr: SessionManager, settings: Settings) -> Dict[str, Any]:
    try:
        project = get_project(session_mgr)
        svc = RegistryService.from_context(project, settings)
        if not svc.cfg.is_configured:
            return {'success': False, 'message': 'Registry not configured', 'projects': []}

        ok, result, msg = svc.list_project_details()
        if not ok:
            return {'success': False, 'message': msg, 'projects': []}
        return {'success': True, 'projects': result}
    except Exception as e:
        logger.exception("List registry projects failed: %s", e)
        return {'success': False, 'message': str(e), 'projects': []}


def delete_registry_project_result(
    project_name: str,
    session_mgr: SessionManager,
    settings: Settings,
) -> Dict[str, Any]:
    try:
        project = get_project(session_mgr)
        svc = RegistryService.from_context(project, settings)
        if not svc.cfg.is_configured:
            return {'success': False, 'message': 'Registry not configured'}

        errors = svc.delete_project(project_name)

        if errors:
            return {'success': False, 'message': f'Partially deleted. Errors: {"; ".join(errors)}'}

        return {'success': True, 'message': f'Project "{project_name}" deleted from registry'}
    except Exception as e:
        logger.exception("Delete registry project failed: %s", e)
        return {'success': False, 'message': str(e)}


def delete_registry_version_result(
    project_name: str,
    version: str,
    session_mgr: SessionManager,
    settings: Settings,
) -> Dict[str, Any]:
    try:
        project = get_project(session_mgr)
        svc = RegistryService.from_context(project, settings)
        if not svc.cfg.is_configured:
            return {'success': False, 'message': 'Registry not configured'}

        d_ok, d_msg = svc.delete_version(project_name, version)
        if not d_ok:
            return {'success': False, 'message': f'Failed to delete version: {d_msg}'}

        return {'success': True, 'message': f'Version {version} deleted from "{project_name}"'}
    except Exception as e:
        logger.exception("Delete registry version failed: %s", e)
        return {'success': False, 'message': str(e)}


# --- Emoji / base URI ---


def set_default_emoji_result(
    emoji: str,
    request: Request,
    session_mgr: SessionManager,
    settings: Settings,
) -> Dict[str, Any]:
    admin_err = require_admin_error(request, session_mgr, settings)
    if admin_err:
        return admin_err

    _, host, token, registry_cfg = _resolve_context(session_mgr, settings)
    ok, msg = global_config_service.set_default_emoji(host, token, registry_cfg, emoji)
    if not ok:
        return {'success': False, 'message': msg}
    return {'success': True, 'emoji': emoji}


def save_base_uri_result(
    base_uri: str,
    request: Request,
    session_mgr: SessionManager,
    settings: Settings,
) -> Dict[str, Any]:
    admin_err = require_admin_error(request, session_mgr, settings)
    if admin_err:
        return admin_err

    _, host, token, registry_cfg = _resolve_context(session_mgr, settings)
    ok, msg = global_config_service.set_default_base_uri(host, token, registry_cfg, base_uri)
    if not ok:
        return {'success': False, 'message': msg}
    return {'success': True, 'base_uri': base_uri}


# --- Permissions ---


def build_permissions_me(request: Request, session_mgr: SessionManager, settings: Settings) -> Dict[str, Any]:
    email = getattr(request.state, 'user_email', '') or request.headers.get('x-forwarded-email', '')
    display_name = request.headers.get('x-forwarded-preferred-username', email)

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
        _, host, token, registry_cfg = _resolve_context(session_mgr, settings)
        user_token = request.headers.get('x-forwarded-access-token', '')

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
        logger.error("permissions/me: error resolving role for %s: %s", email, e, exc_info=True)

    return {
        'email': email,
        'display_name': display_name,
        'role': role,
        'is_app_admin': is_app_admin,
        'is_app_mode': True,
    }


def build_permissions_diag(request: Request, settings: Settings) -> Dict[str, Any]:
    from databricks.sdk import WorkspaceClient
    import requests as _req

    email = request.headers.get("x-forwarded-email", "")
    user_token = request.headers.get("x-forwarded-access-token", "")
    app_name = settings.ontobricks_app_name
    diag: dict = {
        "email": email,
        "app_name": app_name,
        "is_app_mode": is_databricks_app(),
        "user_token_present": bool(user_token),
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


def list_permissions_result(session_mgr: SessionManager, settings: Settings) -> Dict[str, Any]:
    _, host, token, registry_cfg = _resolve_context(session_mgr, settings)
    entries = permission_service.list_entries(host, token, registry_cfg)
    return {'success': True, 'permissions': entries}


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
        return {'success': False, 'message': 'Principal (email or group name) is required'}
    if role not in ('viewer', 'editor'):
        return {'success': False, 'message': 'Role must be "viewer" or "editor"'}

    _, host, token, registry_cfg = _resolve_context(session_mgr, settings)
    if not registry_cfg.get('catalog') or not registry_cfg.get('schema'):
        return {'success': False, 'message': 'Registry not configured. Initialize the registry in Settings first.'}

    ok, msg = permission_service.add_or_update_entry(
        host, token, registry_cfg, principal, principal_type, display_name, role,
    )
    return {'success': ok, 'message': msg}


def delete_permission_result(
    principal: str,
    session_mgr: SessionManager,
    settings: Settings,
) -> Dict[str, Any]:
    _, host, token, registry_cfg = _resolve_context(session_mgr, settings)
    if not registry_cfg.get('catalog') or not registry_cfg.get('schema'):
        return {'success': False, 'message': 'Registry not configured. Initialize the registry in Settings first.'}

    ok, msg = permission_service.remove_entry(host, token, registry_cfg, principal)
    return {'success': ok, 'message': msg}


def list_principals_result(session_mgr: SessionManager, settings: Settings) -> Dict[str, Any]:
    _, host, token, _ = _resolve_context(session_mgr, settings)
    app_name = settings.ontobricks_app_name
    permission_service.clear_principals_cache()
    result = permission_service.list_app_principals(host, token, app_name)
    return {'success': True, 'users': result.get('users', []), 'groups': result.get('groups', [])}


def search_workspace_principals(
    query: str, principal_type: str, session_mgr: SessionManager, settings: Settings,
) -> Dict[str, Any]:
    """Search all workspace users or groups via SCIM, filtered by *query*."""
    _, host, token, _ = _resolve_context(session_mgr, settings)
    from back.core.databricks import DatabricksClient
    client = DatabricksClient(host=host, token=token)

    if principal_type == "group":
        groups = client.search_groups(query, max_results=50)
        return {"success": True, "results": groups}
    else:
        users = client.search_users(query, max_results=50)
        return {"success": True, "results": users}


# --- LadybugDB local files ---


def human_size(nbytes: int) -> str:
    """Return a human-readable file size string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f} {unit}" if unit != "B" else f"{nbytes} B"
        nbytes /= 1024  # type: ignore[assignment]
    return f"{nbytes:.1f} PB"


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
                "size_display": human_size(size),
                "modified_iso": mtime_dt.isoformat(),
                "modified_display": mtime_dt.strftime("%Y-%m-%d %H:%M:%S UTC"),
            })
    except OSError as exc:
        logger.warning("Failed to list LadybugDB directory: %s", exc)
        return {"success": False, "message": str(exc), "files": []}

    return {"success": True, "files": items, "base_dir": LADYBUG_BASE_DIR}


def delete_ladybugdb_file(filename: str) -> Dict[str, Any]:
    """Delete a file or directory under LadybugDB base dir (basename only)."""
    safe_name = os.path.basename(filename)
    if not safe_name or safe_name in (".", ".."):
        return {"success": False, "message": "Invalid filename"}

    target = os.path.join(LADYBUG_BASE_DIR, safe_name)
    if not os.path.exists(target):
        return {"success": False, "message": f"Not found: {safe_name}"}

    try:
        if os.path.isdir(target):
            shutil.rmtree(target)
        else:
            os.remove(target)
        logger.info("Deleted LadybugDB file: %s", target)
        return {"success": True, "message": f"Deleted {safe_name}"}
    except OSError as exc:
        logger.warning("Failed to delete LadybugDB file %s: %s", target, exc)
        return {"success": False, "message": str(exc)}


# --- Schedules ---


def list_schedules_result(session_mgr: SessionManager, settings: Settings) -> Dict[str, Any]:
    _, host, token, registry_cfg = _resolve_context(session_mgr, settings)
    scheduler = _get_scheduler()
    try:
        entries = scheduler.get_all_schedules(host, token, registry_cfg)
        return {"success": True, "schedules": entries}
    except Exception as e:
        logger.exception("list_schedules failed: %s", e)
        return {"success": False, "message": str(e), "schedules": []}


def save_schedule_result(
    data: Dict[str, Any],
    session_mgr: SessionManager,
    settings: Settings,
) -> Dict[str, Any]:
    try:
        project_name = (data.get("project_name") or "").strip()
        interval_minutes = int(data.get("interval_minutes", 60))
        drop_existing = bool(data.get("drop_existing", True))
        enabled = bool(data.get("enabled", True))
        version = (data.get("version") or "latest").strip()

        if not project_name:
            return {"success": False, "message": "project_name is required"}

        _, host, token, registry_cfg = _resolve_context(session_mgr, settings)

        scheduler = _get_scheduler()
        ok, msg = scheduler.save_schedule(
            host, token, registry_cfg, settings, project_name, interval_minutes,
            drop_existing, enabled, version=version,
        )
        return {"success": ok, "message": msg}
    except Exception as e:
        logger.exception("save_schedule failed: %s", e)
        return {"success": False, "message": str(e)}


def get_schedule_history_result(
    project_name: str,
    session_mgr: SessionManager,
    settings: Settings,
) -> Dict[str, Any]:
    _, host, token, registry_cfg = _resolve_context(session_mgr, settings)
    scheduler = _get_scheduler()
    try:
        entries = scheduler.get_schedule_history(host, token, registry_cfg, project_name)
        return {"success": True, "project_name": project_name, "history": entries}
    except Exception as e:
        logger.exception("get_schedule_history failed for '%s': %s", project_name, e)
        return {"success": False, "message": str(e), "history": []}


def scheduler_status_payload() -> Dict[str, Any]:
    scheduler = _get_scheduler()
    return {"success": True, **scheduler.status()}


def delete_schedule_result(
    project_name: str,
    session_mgr: SessionManager,
    settings: Settings,
) -> Dict[str, Any]:
    try:
        _, host, token, registry_cfg = _resolve_context(session_mgr, settings)

        scheduler = _get_scheduler()
        ok, msg = scheduler.remove_schedule(host, token, registry_cfg, project_name)
        return {"success": ok, "message": msg}
    except Exception as e:
        logger.exception("delete_schedule failed: %s", e)
        return {"success": False, "message": str(e)}
