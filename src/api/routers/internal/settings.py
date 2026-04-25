"""
Internal API -- Settings / configuration JSON endpoints.

Moved from app/frontend/settings/routes.py during the front/back split.
"""

from fastapi import APIRouter, Request, Depends

from shared.config.settings import get_settings, Settings
from shared.config.constants import DEFAULT_BASE_URI
from back.core.errors import ValidationError
from back.objects.session import SessionManager, get_session_manager
from back.core.helpers import resolve_default_base_uri, resolve_default_emoji
from back.objects.session import get_domain
from back.objects.registry import ROLE_ADMIN, require

from api.routers.internal._permissions import filter_visible_domains

from back.objects.domain.SettingsService import SettingsService as config_service

router = APIRouter(prefix="/settings", tags=["Settings"])


def _settings_request_identity(request: Request) -> tuple[str, str, str, str, str]:
    """Extract user identity primitives for :class:`SettingsService` (no FastAPI types in domain layer)."""
    email = getattr(request.state, "user_email", "") or request.headers.get(
        "x-forwarded-email", ""
    )
    display_name = request.headers.get("x-forwarded-preferred-username", email) or ""
    user_token = request.headers.get("x-forwarded-access-token", "") or ""
    user_role = getattr(request.state, "user_role", "") or ""
    user_domain_role = getattr(request.state, "user_domain_role", "") or ""
    return email, display_name, user_token, user_role, user_domain_role


# ===========================================
# Main Configuration
# ===========================================


@router.get("/current")
async def get_current_config(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
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
    email, _display_name, user_token, _user_role, _user_domain_role = (
        _settings_request_identity(request)
    )
    return config_service.apply_config_save(
        data, email, user_token, session_mgr, settings
    )


@router.post("/test-connection")
async def test_connection_post(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Test Databricks connection (POST)."""
    return await config_service.test_connection(session_mgr, settings)


# ===========================================
# Warehouse Selection
# ===========================================


@router.get("/warehouses")
async def get_warehouses(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
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
    email, _display_name, user_token, _user_role, _user_domain_role = (
        _settings_request_identity(request)
    )
    return config_service.select_warehouse(
        data.get("warehouse_id"),
        email,
        user_token,
        session_mgr,
        settings,
    )


# ===========================================
# Catalog/Schema/Volume Navigation
# ===========================================


@router.get("/catalogs")
async def get_catalogs(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Get available Unity Catalog catalogs."""
    return await config_service.fetch_catalogs(session_mgr, settings)


@router.get("/schemas")
async def get_schemas(
    catalog: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Get schemas in a catalog (query param version)."""
    return await config_service.fetch_schemas(catalog, session_mgr, settings)


@router.get("/schemas/{catalog}")
async def get_schemas_path(
    catalog: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Get schemas in a catalog (path param version)."""
    return await config_service.fetch_schemas(
        catalog,
        session_mgr,
        settings,
        log_label="Get schemas (path)",
    )


@router.get("/volumes")
async def get_volumes(
    catalog: str,
    schema: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Get volumes in a schema (query param version)."""
    return await config_service.fetch_volumes(catalog, schema, session_mgr, settings)


@router.get("/volumes/{catalog}/{schema}")
async def get_volumes_path(
    catalog: str,
    schema: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Get volumes in a schema (path param version)."""
    return await config_service.fetch_volumes(
        catalog,
        schema,
        session_mgr,
        settings,
        log_label="Get volumes (path)",
    )


# ===========================================
# Domain Registry
# ===========================================


@router.get("/registry")
async def get_registry(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return current domain-registry configuration and initialization status."""
    return config_service.build_registry_get_payload(session_mgr, settings)


@router.post("/registry")
async def save_registry(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Persist registry catalog / schema / volume in settings.registry."""
    if config_service.is_registry_locked(settings):
        raise ValidationError(
            "Registry is configured via Databricks App resources and cannot be changed here.",
        )

    data = await request.json()
    return config_service.apply_registry_save(data, session_mgr)


@router.post("/registry/initialize")
async def initialize_registry(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Create the registry Volume (and root marker) if they do not exist."""
    return config_service.initialize_registry_result(session_mgr, settings)


@router.get("/registry/domains")
async def list_registry_domains(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """List domains in the registry with name and description.

    Non-admin users only see domains they have a role on; admins see all.
    """
    result = config_service.list_registry_domains_result(session_mgr, settings)
    result["domains"] = filter_visible_domains(
        request, session_mgr, settings, result.get("domains", [])
    )
    return result


@router.get("/registry/bridges")
async def list_registry_bridges(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """List all bridges across every domain in the registry."""
    return config_service.list_registry_bridges_result(session_mgr, settings)


@router.delete("/registry/domains/{domain_name}")
async def delete_registry_domain(
    domain_name: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Delete a domain folder and all its versions from the registry."""
    return config_service.delete_registry_domain_result(
        domain_name, session_mgr, settings
    )


@router.delete("/registry/domains/{domain_name}/versions/{version}")
async def delete_registry_version(
    domain_name: str,
    version: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Delete a single version file from a domain in the registry."""
    return config_service.delete_registry_version_result(
        domain_name,
        version,
        session_mgr,
        settings,
    )


@router.post("/registry/domains/{domain_name}/versions/{version}/active")
async def set_registry_version_active(
    domain_name: str,
    version: str,
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Set or clear the Active flag on a version in the registry."""
    data = await request.json()
    enabled = bool(data.get("enabled", False))
    return config_service.set_registry_version_active_result(
        domain_name,
        version,
        enabled,
        session_mgr,
        settings,
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
    domain = get_domain(session_mgr)
    return {"success": True, "emoji": resolve_default_emoji(domain, settings)}


@router.post("/set-default-emoji")
async def set_default_emoji(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Set default emoji (admin only, stored globally)."""
    data = await request.json()
    emoji = data.get("emoji", "📦")
    email, _display_name, user_token, _user_role, _user_domain_role = (
        _settings_request_identity(request)
    )
    return config_service.set_default_emoji_result(
        emoji, email, user_token, session_mgr, settings
    )


@router.get("/get-base-uri")
async def get_base_uri(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Get default base URI domain (instance-global)."""
    domain = get_domain(session_mgr)
    return {"success": True, "base_uri": resolve_default_base_uri(domain, settings)}


@router.post("/save-base-uri")
async def save_base_uri(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Save default base URI domain (admin only, stored globally)."""
    data = await request.json()
    base_uri = data.get("base_uri", DEFAULT_BASE_URI.rstrip("/"))
    email, _display_name, user_token, _user_role, _user_domain_role = (
        _settings_request_identity(request)
    )
    return config_service.save_base_uri_result(
        base_uri, email, user_token, session_mgr, settings
    )


@router.get("/get-registry-cache-ttl")
async def get_registry_cache_ttl(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Get registry cache TTL in seconds (instance-global)."""
    return config_service.get_registry_cache_ttl_result(session_mgr, settings)


@router.post("/save-registry-cache-ttl")
async def save_registry_cache_ttl(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Save registry cache TTL in seconds (admin only, stored globally)."""
    data = await request.json()
    ttl = int(data.get("registry_cache_ttl", 300))
    email, _display_name, user_token, _user_role, _user_domain_role = (
        _settings_request_identity(request)
    )
    return config_service.save_registry_cache_ttl_result(
        ttl, email, user_token, session_mgr, settings
    )


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
    email, display_name, user_token, user_role, user_domain_role = (
        _settings_request_identity(request)
    )
    return config_service.build_permissions_me(
        email,
        display_name,
        user_token,
        user_role,
        user_domain_role,
        session_mgr,
        settings,
    )


@router.get("/permissions/diag")
async def permissions_diag(
    request: Request,
    settings: Settings = Depends(get_settings),
):
    """Diagnostic: run the admin check in detail and return raw results."""
    email = request.headers.get("x-forwarded-email", "")
    _, display_name, user_token, user_role, user_domain_role = (
        _settings_request_identity(request)
    )
    return config_service.build_permissions_diag(
        email,
        display_name,
        user_token,
        user_role,
        user_domain_role,
        settings,
    )


@router.get(
    "/permissions",
    dependencies=[Depends(require(ROLE_ADMIN))],
)
async def list_app_permissions(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return the Databricks App principals (users + groups).

    Read-only mirror of the App's ACL.  Used by Settings → Permissions
    and as the row source for Registry → Teams.
    """
    return config_service.list_app_principals_result(session_mgr, settings)


@router.get(
    "/permissions/principals",
    dependencies=[Depends(require(ROLE_ADMIN))],
)
async def list_principals(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """List users and groups from the Databricks App permissions for the picker.

    Always fetches fresh data (bypasses cache) so newly added app users
    appear immediately in the dropdown.
    """
    return config_service.list_principals_result(session_mgr, settings)


@router.get(
    "/permissions/search",
    dependencies=[Depends(require(ROLE_ADMIN))],
)
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
    return config_service.search_workspace_principals(
        q.strip(), type, session_mgr, settings
    )


# ===========================================
# Domain-Level Permissions
# ===========================================


@router.get(
    "/domain-permissions/{domain_name}",
    dependencies=[Depends(require(ROLE_ADMIN))],
)
async def list_domain_permissions(
    domain_name: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """List permission entries for a specific domain (admin only)."""
    return config_service.list_domain_permissions_result(
        domain_name, session_mgr, settings
    )


@router.post(
    "/domain-permissions/{domain_name}",
    dependencies=[Depends(require(ROLE_ADMIN))],
)
async def add_domain_permission(
    domain_name: str,
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Add or update a permission entry for a specific domain (admin only)."""
    data = await request.json()
    return config_service.add_domain_permission_result(
        domain_name, data, session_mgr, settings
    )


@router.delete(
    "/domain-permissions/{domain_name}/{principal:path}",
    dependencies=[Depends(require(ROLE_ADMIN))],
)
async def delete_domain_permission(
    domain_name: str,
    principal: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Remove a permission entry for a specific domain (admin only)."""
    return config_service.delete_domain_permission_result(
        domain_name, principal, session_mgr, settings
    )


# ===========================================
# Teams (Registry → Teams matrix)
# ===========================================


@router.get(
    "/teams",
    dependencies=[Depends(require(ROLE_ADMIN))],
)
async def teams_matrix(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return the Teams matrix payload: domains, principals, and assignments."""
    return config_service.build_teams_matrix_result(session_mgr, settings)


@router.post(
    "/teams",
    dependencies=[Depends(require(ROLE_ADMIN))],
)
async def teams_save_batch(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Persist a batch of team changes across multiple domains (admin only)."""
    data = await request.json()
    return config_service.save_teams_batch_result(data, session_mgr, settings)


# ===========================================
# Graph DB Engine
# ===========================================


@router.get("/graph-engine")
async def get_graph_engine(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return the currently configured graph DB engine."""
    return config_service.get_graph_engine_result(session_mgr, settings)


@router.post("/graph-engine")
async def set_graph_engine(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Set the graph DB engine (admin only, stored globally)."""
    data = await request.json()
    engine = data.get("graph_engine", "ladybug")
    email, _display_name, user_token, _user_role, _user_domain_role = (
        _settings_request_identity(request)
    )
    return config_service.set_graph_engine_result(
        engine, email, user_token, session_mgr, settings
    )


@router.get("/graph-engine-config")
async def get_graph_engine_config(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return the engine-specific JSON configuration."""
    return config_service.get_graph_engine_config_result(session_mgr, settings)


@router.post("/graph-engine-config")
async def set_graph_engine_config(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Set the engine-specific JSON configuration (admin only, stored globally)."""
    data = await request.json()
    config = data.get("graph_engine_config", {})
    email, _dn, user_token, _ur, _udr = _settings_request_identity(request)
    return config_service.set_graph_engine_config_result(
        config, email, user_token, session_mgr, settings
    )


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
    """Return all per-domain build schedules."""
    return config_service.list_schedules_result(session_mgr, settings)


@router.post("/schedules")
async def save_schedule(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Create or update a build schedule for a domain."""
    data = await request.json()
    return config_service.save_schedule_result(data, session_mgr, settings)


@router.get("/schedules/{domain_name}/history")
async def get_schedule_history(
    domain_name: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return the run history for a single domain schedule."""
    return config_service.get_schedule_history_result(
        domain_name, session_mgr, settings
    )


@router.get("/schedules/status")
async def scheduler_status():
    """Diagnostic: return the APScheduler internal state (running, jobs, next-run times)."""
    return config_service.scheduler_status_payload()


@router.delete("/schedules/{domain_name}")
async def delete_schedule(
    domain_name: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Remove a build schedule for a domain."""
    return config_service.delete_schedule_result(domain_name, session_mgr, settings)
