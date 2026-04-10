"""
Internal API -- Settings / configuration JSON endpoints.

Moved from app/frontend/settings/routes.py during the front/back split.
"""
from fastapi import APIRouter, Request, Depends

from shared.config.settings import get_settings, Settings
from back.core.logging import get_logger
from back.objects.session import SessionManager, get_session_manager
from back.core.helpers import resolve_default_base_uri, resolve_default_emoji
from back.objects.session import get_project

from back.services import settings as config_service

logger = get_logger(__name__)

router = APIRouter(prefix="/settings", tags=["Settings"])


# ===========================================
# Main Configuration
# ===========================================

@router.get("/current")
async def get_current_config(session_mgr: SessionManager = Depends(get_session_manager), settings: Settings = Depends(get_settings)):
    """Get current Databricks configuration."""
    return config_service.build_current_config(session_mgr, settings)


@router.post("/save")
async def save_config(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Save Databricks configuration.

    Host/token are per-session.  Warehouse ID is instance-global (admin only).
    Catalog/schema are NOT stored -- they are selected dynamically when needed.
    """
    data = await request.json()
    return config_service.apply_config_save(data, request, session_mgr, settings)


@router.post("/test-connection")
async def test_connection_post(session_mgr: SessionManager = Depends(get_session_manager), settings: Settings = Depends(get_settings)):
    """Test Databricks connection (POST)."""
    return await config_service.test_connection(session_mgr, settings)


# ===========================================
# Warehouse Selection
# ===========================================

@router.get("/warehouses")
async def get_warehouses(session_mgr: SessionManager = Depends(get_session_manager), settings: Settings = Depends(get_settings)):
    """Get available SQL warehouses."""
    return await config_service.fetch_warehouses(session_mgr, settings)


@router.post("/select-warehouse")
async def select_warehouse(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Select a SQL warehouse.

    Tries to persist the choice globally (UC Volume) so all users
    share it.  When the registry is not configured yet (bootstrap
    scenario), falls back to storing in the session so the user
    can immediately browse catalogs and set up the registry.
    """
    data = await request.json()
    return config_service.select_warehouse(
        data.get('warehouse_id'), request, session_mgr, settings,
    )


# ===========================================
# Catalog/Schema/Volume Navigation
# ===========================================

@router.get("/catalogs")
async def get_catalogs(session_mgr: SessionManager = Depends(get_session_manager), settings: Settings = Depends(get_settings)):
    """Get available Unity Catalog catalogs."""
    return await config_service.fetch_catalogs(session_mgr, settings)


@router.get("/schemas")
async def get_schemas(catalog: str, session_mgr: SessionManager = Depends(get_session_manager), settings: Settings = Depends(get_settings)):
    """Get schemas in a catalog (query param version)."""
    return await config_service.fetch_schemas(catalog, session_mgr, settings)


@router.get("/schemas/{catalog}")
async def get_schemas_path(catalog: str, session_mgr: SessionManager = Depends(get_session_manager), settings: Settings = Depends(get_settings)):
    """Get schemas in a catalog (path param version)."""
    return await config_service.fetch_schemas(
        catalog, session_mgr, settings, log_label="Get schemas (path)",
    )


@router.get("/volumes")
async def get_volumes(catalog: str, schema: str, session_mgr: SessionManager = Depends(get_session_manager), settings: Settings = Depends(get_settings)):
    """Get volumes in a schema (query param version)."""
    return await config_service.fetch_volumes(catalog, schema, session_mgr, settings)


@router.get("/volumes/{catalog}/{schema}")
async def get_volumes_path(catalog: str, schema: str, session_mgr: SessionManager = Depends(get_session_manager), settings: Settings = Depends(get_settings)):
    """Get volumes in a schema (path param version)."""
    return await config_service.fetch_volumes(
        catalog, schema, session_mgr, settings, log_label="Get volumes (path)",
    )


# ===========================================
# Project Registry
# ===========================================


@router.get("/registry")
async def get_registry(session_mgr: SessionManager = Depends(get_session_manager),
                       settings: Settings = Depends(get_settings)):
    """Return current project-registry configuration and initialization status."""
    return config_service.build_registry_get_payload(session_mgr, settings)


@router.post("/registry")
async def save_registry(request: Request,
                        session_mgr: SessionManager = Depends(get_session_manager),
                        settings: Settings = Depends(get_settings)):
    """Persist registry catalog / schema / volume in settings.registry."""
    if config_service.is_registry_locked(settings):
        return {'success': False, 'message': 'Registry is configured via Databricks App resources and cannot be changed here.'}

    data = await request.json()
    return config_service.apply_registry_save(data, session_mgr)


@router.post("/registry/initialize")
async def initialize_registry(session_mgr: SessionManager = Depends(get_session_manager),
                              settings: Settings = Depends(get_settings)):
    """Create the registry Volume (and root marker) if they do not exist."""
    return config_service.initialize_registry_result(session_mgr, settings)


@router.get("/registry/projects")
async def list_registry_projects(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """List projects in the registry with name and description."""
    return config_service.list_registry_projects_result(session_mgr, settings)


@router.delete("/registry/projects/{project_name}")
async def delete_registry_project(
    project_name: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Delete a project folder and all its versions from the registry."""
    return config_service.delete_registry_project_result(project_name, session_mgr, settings)


@router.delete("/registry/projects/{project_name}/versions/{version}")
async def delete_registry_version(
    project_name: str,
    version: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Delete a single version file from a project in the registry."""
    return config_service.delete_registry_version_result(
        project_name, version, session_mgr, settings,
    )


# ===========================================
# Emoji & Base URI Settings
# ===========================================

@router.get("/get-default-emoji")
async def get_default_emoji(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Get default emoji setting (instance-global)."""
    project = get_project(session_mgr)
    return {'success': True, 'emoji': resolve_default_emoji(project, settings)}


@router.post("/set-default-emoji")
async def set_default_emoji(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Set default emoji (admin only, stored globally)."""
    data = await request.json()
    emoji = data.get('emoji', '📦')
    return config_service.set_default_emoji_result(emoji, request, session_mgr, settings)


@router.get("/get-base-uri")
async def get_base_uri(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Get default base URI domain (instance-global)."""
    project = get_project(session_mgr)
    return {'success': True, 'base_uri': resolve_default_base_uri(project, settings)}


@router.post("/save-base-uri")
async def save_base_uri(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Save default base URI domain (admin only, stored globally)."""
    data = await request.json()
    base_uri = data.get('base_uri', 'https://databricks-ontology.com')
    return config_service.save_base_uri_result(base_uri, request, session_mgr, settings)


# ===========================================
# Permissions Management
# ===========================================


@router.get("/permissions/me")
async def permissions_me(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return the current user's identity and resolved role."""
    return config_service.build_permissions_me(request, session_mgr, settings)


@router.get("/permissions/diag")
async def permissions_diag(
    request: Request,
    settings: Settings = Depends(get_settings),
):
    """Diagnostic: run the admin check in detail and return raw results."""
    return config_service.build_permissions_diag(request, settings)


@router.get("/permissions")
async def list_permissions(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """List all permission entries (admin only)."""
    return config_service.list_permissions_result(session_mgr, settings)


@router.post("/permissions")
async def add_permission(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Add or update a permission entry (admin only)."""
    data = await request.json()
    return config_service.add_permission_result(data, session_mgr, settings)


@router.delete("/permissions/{principal:path}")
async def delete_permission(
    principal: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Remove a permission entry (admin only)."""
    return config_service.delete_permission_result(principal, session_mgr, settings)


@router.get("/permissions/principals")
async def list_principals(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """List users and groups from the Databricks App permissions for the picker.

    Always fetches fresh data (bypasses cache) so newly added app users
    appear immediately in the dropdown.
    """
    return config_service.list_principals_result(session_mgr, settings)


@router.get("/permissions/search")
async def search_principals(
    q: str = "",
    type: str = "user",
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Search all workspace users or groups via SCIM.

    Query parameter ``q`` is the search term (min 2 chars).
    Query parameter ``type`` is ``user`` or ``group``.
    """
    if len(q.strip()) < 2:
        return {"success": True, "results": []}
    return config_service.search_workspace_principals(q.strip(), type, session_mgr, settings)


# ===========================================
# LadybugDB Local Files
# ===========================================


@router.get("/ladybugdb/files")
async def list_ladybugdb_files():
    """List files and directories stored in the LadybugDB local directory."""
    return config_service.list_ladybugdb_files()


@router.delete("/ladybugdb/files/{filename:path}")
async def delete_ladybugdb_file(filename: str):
    """Delete a file or directory from the LadybugDB local directory."""
    return config_service.delete_ladybugdb_file(filename)


# ===========================================
# Scheduled Builds
# ===========================================

@router.get("/schedules")
async def list_schedules(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return all per-project build schedules."""
    return config_service.list_schedules_result(session_mgr, settings)


@router.post("/schedules")
async def save_schedule(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Create or update a build schedule for a project."""
    data = await request.json()
    return config_service.save_schedule_result(data, session_mgr, settings)


@router.get("/schedules/{project_name}/history")
async def get_schedule_history(
    project_name: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return the run history for a single project schedule."""
    return config_service.get_schedule_history_result(project_name, session_mgr, settings)


@router.get("/schedules/status")
async def scheduler_status():
    """Diagnostic: return the APScheduler internal state (running, jobs, next-run times)."""
    return config_service.scheduler_status_payload()


@router.delete("/schedules/{project_name}")
async def delete_schedule(
    project_name: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Remove a build schedule for a project."""
    return config_service.delete_schedule_result(project_name, session_mgr, settings)
