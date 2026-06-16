"""
Internal API -- Settings / configuration JSON endpoints.

Moved from app/frontend/settings/routes.py during the front/back split.
"""

import json

from typing import Optional

from fastapi import APIRouter, Request, Depends, Query

from shared.config.settings import get_settings, Settings
from shared.config.constants import DEFAULT_BASE_URI
from back.core.errors import ValidationError
from back.objects.session import SessionManager, get_session_manager
from back.core.helpers import resolve_default_base_uri, resolve_default_emoji, run_blocking
from back.objects.session import get_domain
from back.objects.registry import ROLE_ADMIN, require

from api.routers.internal._permissions import filter_visible_domains
from api.routers.internal._helpers import map_route_errors
from back.core.logging import get_logger

from back.objects.domain import SettingsService as config_service

router = APIRouter(prefix="/settings", tags=["Settings"])
logger = get_logger(__name__)


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
    return await run_blocking(config_service.build_registry_get_payload, session_mgr, settings)


@router.post("/registry/initialize")
async def initialize_registry(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Create the registry Volume (and root marker) if they do not exist."""
    return config_service.initialize_registry_result(session_mgr, settings)


@router.get(
    "/registry/lakebase-stats",
    dependencies=[Depends(require(ROLE_ADMIN))],
)
async def get_lakebase_stats(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return per-table row counts for the Lakebase registry schema.

    Powers the read-only inventory grid in the Registry Location
    panel. Raises :class:`~back.core.errors.ValidationError` or
    :class:`~back.core.errors.InfrastructureError` when the Lakebase
    resource is not bound, the backend is not installed, or the store
    cannot be queried.
    """
    with map_route_errors("registry lakebase stats", logger):
        return config_service.lakebase_stats_result(session_mgr, settings)


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


# ===========================================
# Registry OBX export / import
# ===========================================


@router.post("/registry/export")
async def export_registry_obx(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Export one or several registry domains as a `.obx` (JSON) file.

    Body shape::

        {
            "domains": [
                {"name": "claims", "mode": "all" | "active" | "latest" | "selected",
                 "versions": ["1", "2"]}
            ]
        }

    The response is a streamed JSON body with a ``Content-Disposition``
    attachment header so the browser saves it as ``ontobricks-YYYY-MM-DD.obx``.
    Domains the caller cannot see (per :func:`filter_visible_domains`) are
    silently dropped before the export runs.
    """
    from fastapi.responses import StreamingResponse
    import io

    spec = await request.json()
    requested = spec.get("domains") or []
    if requested:
        visible = filter_visible_domains(
            request, session_mgr, settings, requested
        )
        visible_names = {
            (e.get("name") if isinstance(e, dict) else str(e)) for e in visible
        }
        spec = {
            **spec,
            "domains": [d for d in requested if d.get("name") in visible_names],
        }

    email, _, _, _, _ = _settings_request_identity(request)
    result = config_service.export_registry_obx_result(
        spec, session_mgr, settings, exported_by=email
    )

    envelope = result["envelope"]
    body = json.dumps(envelope, indent=2).encode("utf-8")
    headers = {
        "Content-Disposition": f'attachment; filename="{result["filename"]}"',
        "X-OBX-Format-Version": str(envelope.get("format_version", "")),
        "X-OBX-Ontobricks-Version": envelope.get("ontobricks_version", ""),
        "X-OBX-Domain-Count": str(result.get("domain_count", 0)),
    }
    return StreamingResponse(
        io.BytesIO(body), media_type="application/json", headers=headers
    )


@router.post("/registry/import/preview")
async def preview_registry_obx_import(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Inspect an uploaded `.obx` file and report per-domain conflicts.

    Accepts multipart/form-data with a ``file`` field. Returns the envelope
    metadata (``format_version``, ``ontobricks_version``, …) plus a list of
    incoming domains annotated with ``exists``, ``conflicting_versions``,
    and a ``suggested_new_name`` for the rename action.
    """
    form = await request.form()
    upload = form.get("file")
    if upload is None:
        raise ValidationError("No file provided")
    file_bytes = await upload.read()
    return config_service.preview_obx_import_result(
        file_bytes, session_mgr, settings
    )


@router.post(
    "/registry/import",
    dependencies=[Depends(require(ROLE_ADMIN))],
)
async def import_registry_obx(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Import a `.obx` file into the registry (admin only).

    Multipart fields:

    * ``file`` -- the uploaded `.obx` JSON body.
    * ``decisions`` -- JSON string ``[{"name": <folder>,
      "action": "skip"|"overwrite"|"rename", "new_name": <str>}]``.
      Missing entries default to ``"skip"``.
    """
    form = await request.form()
    upload = form.get("file")
    if upload is None:
        raise ValidationError("No file provided")
    file_bytes = await upload.read()

    decisions_raw = form.get("decisions") or "[]"
    try:
        decisions = json.loads(decisions_raw)
    except json.JSONDecodeError as exc:
        raise ValidationError(
            f"Invalid 'decisions' field: not valid JSON ({exc})"
        ) from exc
    if not isinstance(decisions, list):
        raise ValidationError("'decisions' must be a JSON array")

    return config_service.import_registry_obx_result(
        file_bytes, decisions, session_mgr, settings
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



# ===========================================
# Branding (Navbar Logo)
# ===========================================


@router.get("/navbar-logo")
async def get_navbar_logo(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return the configured navbar logo (or the bundled default)."""
    return config_service.get_navbar_logo_result(session_mgr, settings)


@router.post("/navbar-logo")
async def upload_navbar_logo(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Upload a custom navbar logo (admin only, stored globally).

    Multipart form with a single field ``file``. The image is base64-
    encoded and stored as a ``data:`` URL inside the global config
    blob, so it works identically in local and Databricks App modes
    without touching Volumes or local disk. Recommended source size:
    64×64 px (square).
    """
    form = await request.form()
    upload = form.get("file")
    if upload is None or not hasattr(upload, "read"):
        raise ValidationError("Missing 'file' field in upload")
    content = await upload.read()
    content_type = getattr(upload, "content_type", "") or ""
    email, _display_name, user_token, _user_role, _user_domain_role = (
        _settings_request_identity(request)
    )
    return config_service.upload_navbar_logo_result(
        content, content_type, email, user_token, session_mgr, settings
    )


@router.delete("/navbar-logo")
async def reset_navbar_logo(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Reset the navbar logo to the bundled default (admin only)."""
    email, _display_name, user_token, _user_role, _user_domain_role = (
        _settings_request_identity(request)
    )
    return config_service.reset_navbar_logo_result(
        email, user_token, session_mgr, settings
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
    engine = data.get("graph_engine", "lakebase")
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


@router.get("/graph-engine/lakebase-health")
async def get_graph_engine_lakebase_health(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Probe Lakebase connectivity and graph schema (saved global config)."""
    with map_route_errors("graph engine Lakebase health", logger):
        return config_service.graph_engine_lakebase_health_result(session_mgr, settings)


@router.get("/graph-engine/uc-catalogs")
async def get_graph_engine_uc_catalogs(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Unity Catalog names for the Lakebase managed-sync UC catalog picker (read-only)."""
    with map_route_errors("graph engine UC catalogs", logger):
        return config_service.graph_engine_uc_catalogs_result(session_mgr, settings)


@router.get("/graph-engine/uc-schemas")
async def get_graph_engine_uc_schemas(
    catalog: str = "",
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Unity Catalog schemas within a catalog for the managed-sync UC schema picker."""
    with map_route_errors("graph engine UC schemas", logger):
        return config_service.graph_engine_uc_schemas_result(catalog, session_mgr, settings)


@router.get("/graph-engine/lakebase-projects")
async def get_graph_engine_lakebase_projects(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """List Lakebase Autoscaling projects visible in the workspace."""
    with map_route_errors("graph engine Lakebase projects", logger):
        return config_service.graph_engine_lakebase_projects_result(session_mgr, settings)


@router.get("/graph-engine/lakebase-branches")
async def get_graph_engine_lakebase_branches(
    project: str = "",
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """List branches for a Lakebase Autoscaling project."""
    with map_route_errors("graph engine Lakebase branches", logger):
        return config_service.graph_engine_lakebase_branches_result(
            project, session_mgr, settings
        )


@router.get("/graph-engine/lakebase-pg-databases")
async def get_graph_engine_lakebase_pg_databases(
    branch: str = "",
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """List Postgres databases on a Lakebase branch."""
    with map_route_errors("graph engine Lakebase PG databases", logger):
        return config_service.graph_engine_lakebase_pg_databases_result(
            branch, session_mgr, settings
        )


@router.get("/graph-engine/lakebase-pg-schemas")
async def get_graph_engine_lakebase_pg_schemas(
    database: str = "",
    branch_path: str = "",
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """List Postgres schemas in a Lakebase database."""
    with map_route_errors("graph engine Lakebase PG schemas", logger):
        return config_service.graph_engine_lakebase_pg_schemas_result(
            database, session_mgr, settings, branch_path=branch_path
        )


@router.get("/graph-engine/lakebase-objects")
async def get_graph_engine_lakebase_objects(
    database: str = "",
    branch_path: str = "",
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """List all user-owned schemas, tables and views in a Lakebase database (admin only).

    ``branch_path`` (full resource path, e.g. ``projects/…/branches/…``) is
    the form's current branch selection; when supplied the connection targets
    that branch directly rather than the saved/bound config.
    """
    with map_route_errors("graph engine Lakebase objects", logger):
        return config_service.graph_engine_lakebase_objects_result(
            database, branch_path, session_mgr, settings
        )


@router.get("/graph-engine/lakebase-sync-objects")
async def get_graph_engine_lakebase_sync_objects(
    database: str = "",
    branch_path: str = "",
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """List Unity Catalog / Lakeflow synced-table registrations for the Lakebase graph.

    Only meaningful when ``sync_mode == managed_synced``.  Probes each ``_sync``
    Postgres table against the Databricks synced-tables API in parallel and returns
    state + pipeline_id for each entry.
    """
    with map_route_errors("graph engine Lakebase sync objects", logger):
        return config_service.graph_engine_lakebase_sync_objects_result(
            database, branch_path, session_mgr, settings
        )


@router.post("/graph-engine/drop-uc-object")
async def post_graph_engine_drop_uc_object(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Drop a Unity Catalog table or Lakeflow synced-table registration.

    Body: ``{ "full_name": "catalog.schema.table", "is_sync": true|false }``
    """
    with map_route_errors("drop UC object", logger):
        data = await request.json()
        full_name = (data.get("full_name") or "").strip()
        is_sync = bool(data.get("is_sync", False))
        return config_service.graph_engine_drop_uc_object_result(
            full_name, is_sync, session_mgr, settings
        )


@router.post("/graph-engine/lakebase-drop-object")
async def post_graph_engine_lakebase_drop_object(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Drop a Postgres schema, table or view in the connected Lakebase database (admin only)."""
    data = await request.json()
    with map_route_errors("graph engine Lakebase drop object", logger):
        return config_service.graph_engine_lakebase_drop_object_result(
            kind=data.get("kind", ""),
            schema=data.get("schema", ""),
            name=data.get("name", ""),
            database=data.get("database", ""),
            branch_path=data.get("branch_path", ""),
            _session_mgr=session_mgr,
            _settings=settings,
        )


@router.get(
    "/graph-engine/lakebase-pg-roles",
    dependencies=[Depends(require(ROLE_ADMIN))],
)
async def get_graph_engine_lakebase_pg_roles(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """List Postgres roles on the graph Lakebase branch + overlay app-user status (admin only)."""
    with map_route_errors("graph engine Lakebase pg roles", logger):
        return config_service.graph_engine_lakebase_pg_roles_result(session_mgr, settings)


@router.post(
    "/graph-engine/lakebase-grant-superuser",
    dependencies=[Depends(require(ROLE_ADMIN))],
)
async def post_graph_engine_lakebase_grant_superuser(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Grant DATABRICKS_SUPERUSER to a user on the graph Lakebase branch (admin only).

    Body: ``{ "user_email": "user@example.com" }``
    """
    with map_route_errors("graph engine Lakebase grant superuser", logger):
        data = await request.json()
        user_email = (data.get("user_email") or "").strip()
        return config_service.graph_engine_lakebase_grant_superuser_result(
            user_email, session_mgr, settings
        )


@router.post(
    "/graph-engine/lakebase-provision",
    dependencies=[Depends(require(ROLE_ADMIN))],
)
async def post_graph_engine_lakebase_provision(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Provision a new Lakebase graph DB from scratch (admin only, async).

    Creates the Lakebase instance/project + Postgres database + graph schema
    and grants the app + MCP service principals. Returns a ``task_id`` the UI
    polls via ``GET /tasks/{id}``.

    Body: ``{ name, capacity, branch, database, schema, mcp_app_name,
    grant_uc_catalog }``.
    """
    data = await request.json()
    email, _dn, user_token, _ur, _udr = _settings_request_identity(request)
    with map_route_errors("provision Lakebase graph DB", logger):
        return config_service.graph_engine_lakebase_provision_result(
            data, email, user_token, session_mgr, settings
        )


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


@router.get("/build-runs/{domain_name}")
async def get_build_runs(
    domain_name: str,
    version: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return the build-run trace for a domain (newest-first, optional version)."""
    return config_service.get_build_runs_result(
        domain_name, session_mgr, settings, version=version, limit=limit
    )


@router.get("/build-analytics/{domain_name}")
async def get_build_analytics(
    domain_name: str,
    version: Optional[str] = Query(default=None),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return aggregate build statistics for a domain (optional version)."""
    return config_service.get_build_analytics_result(
        domain_name, session_mgr, settings, version=version
    )


@router.delete("/schedules/{domain_name}")
async def delete_schedule(
    domain_name: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Remove a build schedule for a domain."""
    return config_service.delete_schedule_result(domain_name, session_mgr, settings)


@router.post("/schedules/{domain_name}/run-now")
async def run_schedule_now(
    domain_name: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Fire the build schedule for *domain_name* immediately (one-shot)."""
    return config_service.trigger_schedule_now_result(
        domain_name, session_mgr, settings
    )


# ===========================================
# Scheduled Cohort Materialisations
# ===========================================


@router.get("/cohort-schedules")
async def list_cohort_schedules(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return all per-(domain, rule) cohort schedules."""
    return config_service.list_cohort_schedules_result(session_mgr, settings)


@router.get("/cohort-schedules/rules/{domain_name}")
async def list_cohort_rules_for_domain(
    domain_name: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """List saved cohort rules for *domain_name* (used by the schedule modal)."""
    return config_service.list_cohort_rules_for_domain_result(
        domain_name, session_mgr, settings
    )


@router.post("/cohort-schedules")
async def save_cohort_schedule(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Create or update a cohort materialisation schedule."""
    data = await request.json()
    return config_service.save_cohort_schedule_result(data, session_mgr, settings)


@router.get("/cohort-schedules/{domain_name}/{rule_id}/history")
async def get_cohort_schedule_history(
    domain_name: str,
    rule_id: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return the run history for a single cohort schedule."""
    return config_service.get_cohort_schedule_history_result(
        domain_name, rule_id, session_mgr, settings
    )


@router.delete("/cohort-schedules/{domain_name}/{rule_id}")
async def delete_cohort_schedule(
    domain_name: str,
    rule_id: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Remove a cohort schedule for *(domain_name, rule_id)*."""
    return config_service.delete_cohort_schedule_result(
        domain_name, rule_id, session_mgr, settings
    )


@router.post("/cohort-schedules/{domain_name}/{rule_id}/run-now")
async def run_cohort_schedule_now(
    domain_name: str,
    rule_id: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Fire the cohort schedule for *(domain_name, rule_id)* immediately."""
    return config_service.trigger_cohort_schedule_now_result(
        domain_name, rule_id, session_mgr, settings
    )
