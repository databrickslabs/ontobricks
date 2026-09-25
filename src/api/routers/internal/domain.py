"""
Internal API -- Domain management JSON endpoints.

Moved from app/frontend/project/routes.py during the front/back split.
"""

import json
import os
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Request, Depends, Query

from shared.config.settings import get_settings, Settings
from back.core.databricks import (
    DocumentExtractor,
    DocumentParseService,
    ParseStatus,
    is_databricks_app,
)
from back.core.errors import (
    InfrastructureError,
    NotFoundError,
    OntoBricksError,
    ValidationError,
)
from back.core.helpers import (
    get_databricks_client,
    resolve_warehouse_id,
)
from back.core.logging import get_logger
from back.core.task_manager import get_task_manager
from back.objects.session import (
    SessionManager,
    get_domain,
    get_session_manager,
    sanitize_domain_folder,
)
from back.objects.domain import Domain, SettingsService
from api.routers.internal._permissions import (
    assert_admin_can_create_domain,
    filter_visible_domains,
)

logger = get_logger(__name__)

router = APIRouter(prefix="/domain", tags=["Domain"])


# ===========================================
# Domain Info API
# ===========================================


@router.get("/info")
async def get_domain_info(session_mgr: SessionManager = Depends(get_session_manager)):
    """Get current domain information."""
    return Domain(get_domain(session_mgr)).get_domain_info()


@router.post("/info")
async def save_domain_info(
    request: Request, session_mgr: SessionManager = Depends(get_session_manager)
):
    """Save domain information."""
    data = await request.json()
    domain = get_domain(session_mgr)
    response_info = Domain(domain).save_domain_info(data)
    return {"success": True, "info": response_info, "message": "Domain info saved"}


# ===========================================
# Current User API
# ===========================================


@router.get("/current-user")
async def get_current_user(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return the current user's display name and email.

    In Databricks App mode the proxy headers carry the real user identity.
    Falls back to the SCIM /Me endpoint for local / PAT mode.
    """
    if is_databricks_app():
        name = request.headers.get("x-forwarded-preferred-username", "")
        email = request.headers.get("x-forwarded-email", "")
        if name or email:
            return {"success": True, "email": name or email}

    domain = get_domain(session_mgr)
    client = get_databricks_client(domain, settings)
    if not client:
        return {"success": True, "email": ""}
    email = client.get_current_user_email()
    return {"success": True, "email": email}


# ===========================================
# Domain Name Availability
# ===========================================


@router.get("/check-name")
async def check_domain_name(
    name: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Check whether a domain name is already taken in the registry."""
    folder = sanitize_domain_folder(name)
    domain = get_domain(session_mgr)
    try:
        svc = Domain(domain, settings).build_registry_service()
        if not svc.cfg.is_configured:
            return {"success": True, "available": True}
        already_ours = domain.domain_folder == folder
        exists = svc.domain_exists(folder)
        return {
            "success": True,
            "available": not exists or already_ours,
            "folder": folder,
        }
    except OntoBricksError:
        raise
    except Exception as exc:
        logger.exception("check_domain_name: registry lookup failed for '%s'", name)
        raise InfrastructureError(
            "Could not verify domain name availability",
        ) from exc


# ===========================================
# Domain Save/Export
# ===========================================


@router.post("/save")
async def save_domain(
    request: Request, session_mgr: SessionManager = Depends(get_session_manager)
):
    """Save domain to session (for export)."""
    data = await request.json()
    domain = get_domain(session_mgr)

    domain_name = data.get("name", "NewDomain")
    domain.info["name"] = domain_name
    domain.save()

    return {"success": True, "name": domain_name}


@router.get("/export")
async def export_domain(session_mgr: SessionManager = Depends(get_session_manager)):
    """Export complete domain as JSON.

    Note: Generated outputs (R2RML, OWL) are NOT exported - they are regenerated from source data.
    """
    domain = get_domain(session_mgr)

    export_data = domain.export_for_save()

    return {
        "success": True,
        "name": domain.info.get("name", "NewDomain"),
        "domain": export_data,
    }


# ===========================================
# Domain Import/Load
# ===========================================


@router.post("/import")
async def import_domain(
    request: Request, session_mgr: SessionManager = Depends(get_session_manager)
):
    """Import domain from JSON (supports both file upload and JSON body).

    For versioned domains, pass 'version' parameter to load a specific version.
    """
    content_type = request.headers.get("content-type", "")
    selected_version = None

    # Handle file upload (multipart/form-data)
    if "multipart/form-data" in content_type:
        form = await request.form()
        file = form.get("file")
        if file:
            content = await file.read()
            domain_data = json.loads(content.decode("utf-8"))
        else:
            raise ValidationError("No file provided")
    else:
        # Handle JSON body
        data = await request.json()
        raw = data.get("domain", data.get("project", data))
        domain_data = raw  # Support wrapped (domain|project) or unwrapped format
        selected_version = data.get("version")  # Optional: specific version to load

    domain = get_domain(session_mgr)
    return Domain(domain).import_domain(domain_data, selected_version)


# ===========================================
# Domain Reset/Clear
# ===========================================


@router.post("/reset")
async def reset_domain(session_mgr: SessionManager = Depends(get_session_manager)):
    """Reset entire domain to empty state."""
    domain = get_domain(session_mgr)
    domain.reset()
    domain.clear_uc_metadata()
    return {"success": True, "message": "Domain reset"}


@router.post("/clear")
async def clear_domain(session_mgr: SessionManager = Depends(get_session_manager)):
    """Clear current domain and start fresh (delegates to reset)."""
    return await reset_domain(session_mgr)


@router.post("/close")
async def close_domain(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Close the loaded domain: release the edit lock, then reset the session.

    Releasing the single-editor lock is what lets another user open the same
    DRAFT version in edit mode. The release must happen *before* the reset,
    since the lock target (folder, version) is read from the session. When
    the caller is only a viewer the release is a harmless no-op (the lock is
    keyed by holder e-mail).
    """
    from back.objects.registry.lockmgt import EditLockService

    EditLockService.release_for_session(request, session_mgr, settings)
    domain = get_domain(session_mgr)
    domain.reset()
    domain.clear_uc_metadata()
    return {"success": True, "message": "Domain closed"}


# ===========================================
# Session Debug
# ===========================================


@router.get("/session-debug")
async def get_session_debug(session_mgr: SessionManager = Depends(get_session_manager)):
    """Get full session data for debugging purposes.

    Returns the ``domain_data`` bucket (shaped by :class:`DomainSession`)
    plus every other top-level key present in the raw FastAPI session
    (e.g. ``graph_chat``) so callers can see non-domain buckets too.

    Only available when LOG_LEVEL is set to DEBUG.
    """
    payload = Domain(get_domain(session_mgr)).get_session_debug_response()
    extras = {
        k: v
        for k, v in (session_mgr.data or {}).items()
        if k not in ("domain_data", "project_data")
    }
    if extras:
        payload["extras"] = extras
    return payload


@router.get("/app-debug")
async def get_app_debug():
    """Expose global in-memory caches for debugging.

    Only available when LOG_LEVEL is set to DEBUG.
    """
    from shared.config.constants import DEFAULT_LOG_LEVEL

    if os.getenv("LOG_LEVEL", DEFAULT_LOG_LEVEL).upper() != "DEBUG":
        raise ValidationError("app-debug is only available when LOG_LEVEL=DEBUG")

    from back.objects.domain import get_version_status_cache_snapshot
    from back.objects.registry import get_registry_cache_snapshot

    return {
        "success": True,
        "caches": {
            "registry_domains": get_registry_cache_snapshot(),
            "version_status": get_version_status_cache_snapshot(),
        },
    }


# ===========================================
# Domain Configuration (Databricks)
# ===========================================


@router.get("/config")
async def get_domain_config(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Get domain-specific configuration.

    The warehouse_id is read-only here (set globally via Settings by admins).
    Catalog/schema are NOT stored -- they are selected dynamically when needed.
    """
    domain = get_domain(session_mgr)

    return {"success": True, "warehouse_id": resolve_warehouse_id(domain, settings)}


@router.post("/config")
async def save_domain_config(
    request: Request, session_mgr: SessionManager = Depends(get_session_manager)
):
    """Save domain-specific configuration.

    Note: warehouse_id is no longer stored per-project (it is instance-global).
    Catalog/schema are NOT stored -- they are selected dynamically when needed.
    """
    return {"success": True, "message": "Domain configuration saved"}


# ===========================================
# Design Views Management
# ===========================================


@router.get("/design-views")
async def get_design_views(session_mgr: SessionManager = Depends(get_session_manager)):
    """Get all design views and current view name."""
    return Domain(get_domain(session_mgr)).get_design_views()


@router.post("/design-views/create")
async def create_design_view(
    request: Request, session_mgr: SessionManager = Depends(get_session_manager)
):
    """Create a new design view."""
    data = await request.json()
    view_name = data.get("name", "").strip()
    copy_from = data.get("copy_from")
    domain = get_domain(session_mgr)
    return Domain(domain).create_design_view(view_name, copy_from)


@router.post("/design-views/rename")
async def rename_design_view(
    request: Request, session_mgr: SessionManager = Depends(get_session_manager)
):
    """Rename an existing design view."""
    data = await request.json()
    old_name = data.get("old_name", "").strip()
    new_name = data.get("new_name", "").strip()
    domain = get_domain(session_mgr)
    return Domain(domain).rename_design_view(old_name, new_name)


@router.post("/design-views/delete")
async def delete_design_view(
    request: Request, session_mgr: SessionManager = Depends(get_session_manager)
):
    """Delete a design view."""
    data = await request.json()
    view_name = data.get("name", "").strip()
    domain = get_domain(session_mgr)
    return Domain(domain).delete_design_view(view_name)


@router.post("/design-views/switch")
async def switch_design_view(
    request: Request, session_mgr: SessionManager = Depends(get_session_manager)
):
    """Switch to a different design view."""
    data = await request.json()
    view_name = data.get("name", "").strip()
    domain = get_domain(session_mgr)
    return Domain(domain).switch_design_view(view_name)


@router.get("/design-views/current")
async def get_current_design_view(
    session_mgr: SessionManager = Depends(get_session_manager),
):
    """Get the current design view layout."""
    return Domain(get_domain(session_mgr)).get_current_design_view()


@router.post("/design-views/save-current")
async def save_current_design_view(
    request: Request, session_mgr: SessionManager = Depends(get_session_manager)
):
    """Save layout data to the current view."""
    layout_data = await request.json()
    domain = get_domain(session_mgr)
    return Domain(domain).save_current_design_view(layout_data)


# ===========================================
# Map Layout Management
# ===========================================


@router.get("/map-layout")
async def get_map_layout(session_mgr: SessionManager = Depends(get_session_manager)):
    """Get the saved map layout (node positions)."""
    return Domain(get_domain(session_mgr)).get_map_layout()


@router.post("/map-layout")
async def save_map_layout(
    request: Request, session_mgr: SessionManager = Depends(get_session_manager)
):
    """Save the map layout (node positions)."""
    layout_data = await request.json()
    return Domain(get_domain(session_mgr)).save_map_layout(layout_data)


# ===========================================
# Unity Catalog Domain Management
# ===========================================


@router.get("/list-projects")
async def list_domains(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """List domain folders under /domains/ in the registry Volume.

    Non-admin users only see the domains they have a role on
    (viewer / editor / builder). Admins see everything.
    """
    domain = get_domain(session_mgr)
    svc = Domain(domain, settings).build_registry_service()
    result = Domain.list_domains_result(svc)
    result["domains"] = filter_visible_domains(
        request, session_mgr, settings, result.get("domains", [])
    )
    return result


@router.get("/list-versions")
async def list_domain_versions(
    domain_name: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """List available versions for a domain inside the registry."""
    domain = get_domain(session_mgr)
    svc = Domain(domain, settings).build_registry_service()
    return Domain.list_domain_versions_result(svc, domain_name)


@router.post("/save-to-uc")
async def save_domain_to_uc(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Save domain into the registry Volume under /domains/<name>/v{ver}.json."""
    domain = get_domain(session_mgr)
    assert_admin_can_create_domain(request, domain)
    p = Domain(domain, settings)
    actor_email = getattr(request.state, "user_email", "") or request.headers.get(
        "x-forwarded-email", ""
    )
    return p.save_domain_to_uc(
        p.build_registry_service(), actor_email=actor_email
    )


@router.post("/load-from-uc")
async def load_domain_from_uc(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Load domain from registry Volume.

    When opening a **different** domain than the one currently loaded, the
    previously-open domain is *closed first* — its single-editor lock is
    released **before** the new domain is loaded, so a user never holds two
    DRAFT locks at once. On a successful load of a DRAFT version this then
    auto-acquires the lock for the newly loaded version and returns a
    ``lock`` block describing whether this browser is the editor or a
    read-only viewer. (Same-domain version switches release the old version's
    lock after the load to avoid a needless release/re-acquire when reopening
    the same version.)
    """
    from back.objects.registry.lockmgt import EditLockService

    data = await request.json()
    domain_name = data.get("domain", data.get("project"))
    version = data.get("version")
    domain = get_domain(session_mgr)
    # Snapshot the previously loaded (folder, version) before the load mutates
    # the session, so the edit-lock orchestration can act on it.
    prev_folder = getattr(domain, "domain_folder", "") or ""
    prev_version = getattr(domain, "current_version", "") or ""

    # Opening a different domain closes the currently-open one first (releases
    # its edit-lock before the new load). Same-domain version switches defer to
    # on_domain_loaded below.
    switching = EditLockService.release_prev_on_switch(
        request,
        session_mgr,
        settings,
        prev_folder=prev_folder,
        prev_version=prev_version,
        new_domain=domain_name,
    )

    p = Domain(domain, settings)
    try:
        result = p.load_domain_from_uc(
            p.build_registry_service(), domain_name, version
        )
    except Exception:
        # Load raised after we freed the previous lock — restore it so a failed
        # switch does not orphan the user's lock on the domain they came from.
        if switching:
            EditLockService.reacquire(
                request, session_mgr, settings, prev_folder, prev_version
            )
        raise

    if isinstance(result, dict) and result.get("success"):
        result["lock"] = EditLockService.on_domain_loaded(
            request,
            session_mgr,
            settings,
            # Already released above when switching domains; only hand the
            # previous pair to on_domain_loaded for same-domain version swaps.
            prev_folder="" if switching else prev_folder,
            prev_version="" if switching else prev_version,
        )
    elif switching:
        # Load returned a failure envelope — restore the previous lock too.
        EditLockService.reacquire(
            request, session_mgr, settings, prev_folder, prev_version
        )
    return result


@router.post("/create-version")
async def create_new_version(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Create a new version of the domain and save to registry."""
    domain = get_domain(session_mgr)
    p = Domain(domain, settings)
    return p.create_new_domain_version(p.build_registry_service())


@router.get("/version-status")
async def get_version_status(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
    refresh: bool = False,
):
    """Get current version status and fetch available versions from registry.

    Results are cached server-side for a short TTL.
    Pass ``?refresh=true`` to force a fresh UC lookup.
    """
    return Domain(get_domain(session_mgr), settings).get_version_status(refresh=refresh)


@router.get("/versions-list")
async def list_version_details(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """List all versions with per-version description, mcp_enabled flag, and status."""
    domain = get_domain(session_mgr)
    p = Domain(domain, settings)
    return p.list_version_details(p.build_registry_service())


@router.delete("/versions/{version}")
async def delete_domain_version(
    version: str,
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    domain = get_domain(session_mgr)
    folder = domain.uc_domain_folder
    if not folder:
        raise ValidationError("Domain not saved to the registry")
    return SettingsService.delete_registry_version_result(
        folder,
        version,
        user_role=getattr(request.state, "user_role", "") or "",
        session_mgr=session_mgr,
        settings=settings,
    )


@router.get("/build-runs")
async def list_build_runs(
    version: Optional[str] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """List build runs recorded for the loaded domain (newest-first)."""
    domain = get_domain(session_mgr)
    p = Domain(domain, settings)
    return p.list_build_runs_result(p.build_registry_service(), version=version, limit=limit)


@router.get("/audit-trail")
async def audit_trail(
    limit: int = Query(default=500, ge=1, le=2000),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Unified audit trail (review decisions + build runs) for the loaded domain."""
    domain = get_domain(session_mgr)
    p = Domain(domain, settings)
    return p.audit_trail_result(p.build_registry_service(), limit=limit)


@router.post("/set-version-status")
async def set_version_status(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Transition a version's lifecycle status (DRAFT / IN-REVIEW / PUBLISHED).

    Body: ``{domain_name, version, status}``. ``domain_name`` is the target
    domain (may differ from the loaded session domain — e.g. from Registry
    Browse). Authorization is resolved against the *target* domain: the
    state machine and per-transition role tiers are enforced server-side
    by :meth:`SettingsService.set_registry_version_status_result`.
    """
    data = await request.json()
    domain_name = (data.get("domain_name") or "").strip()
    version = (data.get("version") or "").strip()
    new_status = (data.get("status") or "").strip()
    if not domain_name or not version or not new_status:
        raise ValidationError("domain_name, version and status are required")

    user_role = getattr(request.state, "user_role", "") or ""
    domain_role = SettingsService.resolve_domain_role(
        request, domain_name, settings, app_role=user_role
    )
    actor_email = getattr(request.state, "user_email", "") or request.headers.get(
        "x-forwarded-email", ""
    )
    result = SettingsService.set_registry_version_status_result(
        domain_name,
        version,
        new_status,
        user_role=user_role,
        user_domain_role=domain_role,
        actor_email=actor_email,
        session_mgr=session_mgr,
        settings=settings,
    )
    # A version leaving DRAFT becomes read-only for everyone; drop any
    # held edit lock so the next DRAFT re-open starts clean.
    if (
        isinstance(result, dict)
        and result.get("success")
        and new_status.upper() != "DRAFT"
    ):
        from back.objects.registry.lockmgt import EditLockService

        EditLockService.force_release(
            session_mgr, settings, domain_name, version
        )
    return result


# ===========================================
# Domain edit lock (single-editor concurrency control)
# ===========================================


@router.get("/edit-lock")
async def get_edit_lock(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Edit-lock status for the session's loaded DRAFT ``(folder, version)``.

    Returns ``{mode: "edit"|"view"|"none", holder_email, holder_name,
    acquired_at, is_self, is_admin, can_take_over}``. A non-forcing acquire
    is performed so the editor keeps the lock across page reloads.
    """
    from back.objects.registry.lockmgt import EditLockService

    return EditLockService.status(request, session_mgr, settings)


@router.post("/edit-lock/acquire")
async def acquire_edit_lock(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """(Re)acquire the lock. ``force`` (admin-only) is the "Take over" action."""
    from back.objects.registry.lockmgt import EditLockService

    try:
        data = await request.json()
    except Exception:  # noqa: BLE001
        data = {}
    force = bool(data.get("force"))
    return EditLockService.acquire(
        request, session_mgr, settings, force=force
    )


@router.post("/edit-lock/release")
async def release_edit_lock(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Release the current user's lock (the "Close" button)."""
    from back.objects.registry.lockmgt import EditLockService

    return EditLockService.release(request, session_mgr, settings)


@router.post("/edit-lock/renew")
async def renew_edit_lock(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Keep the holder's lease alive (periodic client ping while editing).

    Returns ``{success, renewed}``; ``renewed=false`` tells the client its
    editing session expired (the stale lease was reclaimed by another user).
    """
    from back.objects.registry.lockmgt import EditLockService

    return EditLockService.renew(request, session_mgr, settings)


# ===========================================
# Unity Catalog Metadata Management
# ===========================================


@router.get("/metadata")
async def get_metadata(session_mgr: SessionManager = Depends(get_session_manager)):
    """Get stored Unity Catalog metadata from session."""
    return Domain(get_domain(session_mgr)).get_metadata_response()


@router.post("/metadata/list-tables")
async def list_schema_tables(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """List all tables in a schema without loading full metadata.

    Returns table names only for selection before loading.
    """
    data = await request.json()
    catalog = data.get("catalog", "").strip()
    schema = data.get("schema", "").strip()
    domain = get_domain(session_mgr)
    return await Domain(domain, settings).list_schema_tables_result(catalog, schema)


@router.post("/metadata/initialize")
async def initialize_metadata(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Load Unity Catalog metadata by reading tables and columns from a schema.

    This merges new tables with existing metadata - existing tables are preserved,
    only new tables are added. Table and column comments are fetched from UC.

    If 'selected_tables' is provided, only those tables will be loaded.
    """
    data = await request.json()
    catalog = data.get("catalog", "").strip()
    schema = data.get("schema", "").strip()
    selected_tables = data.get("selected_tables", None)
    domain = get_domain(session_mgr)
    return Domain(domain, settings).initialize_metadata_result(
        catalog, schema, selected_tables
    )


@router.post("/metadata/save")
async def save_metadata(
    request: Request, session_mgr: SessionManager = Depends(get_session_manager)
):
    """Save selected tables to metadata.

    This allows users to filter which tables are kept in the metadata.
    Tables should have full_name field (catalog.schema.table).
    For backwards compatibility, if full_name is missing, it will be constructed
    from existing metadata's catalog/schema or legacy fields.
    """
    data = await request.json()
    tables = data.get("tables", [])
    return Domain(get_domain(session_mgr)).save_metadata_tables(tables)


@router.post("/metadata/removal-impact")
async def metadata_removal_impact(
    request: Request, session_mgr: SessionManager = Depends(get_session_manager)
):
    """Report which entity/relationship mappings reference the given tables.

    Read-only pre-flight for the data-source deletion guard: the UI calls
    this before confirming a removal so it can list what would break.
    ``table_names`` accepts ``catalog.schema.table`` or bare table names.
    """
    data = await request.json()
    table_names = data.get("table_names", [])
    return Domain(get_domain(session_mgr)).get_removal_impact(table_names)


@router.post("/metadata/clear")
async def clear_metadata(session_mgr: SessionManager = Depends(get_session_manager)):
    """Clear stored Unity Catalog metadata from session."""
    return Domain(get_domain(session_mgr)).clear_metadata()


@router.post("/metadata/update-table-location")
async def update_table_location(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
):
    """Update the data-source location (catalog.schema) for metadata tables.

    Rewrites the table's ``full_name`` to ``catalog.schema.table_name``.
    When ``apply_all`` is true every table in the metadata is updated.
    """
    data = await request.json()
    table_name = data.get("table_name", "").strip()
    catalog = data.get("catalog", "").strip()
    schema = data.get("schema", "").strip()
    apply_all = bool(data.get("apply_all", False))
    return Domain(get_domain(session_mgr)).update_table_data_source(
        table_name,
        catalog,
        schema,
        apply_all=apply_all,
    )


@router.post("/metadata/update-mappings")
async def update_mappings_from_metadata(
    session_mgr: SessionManager = Depends(get_session_manager),
):
    """Push catalog/schema from metadata tables into entity and relationship mappings.

    Reads each metadata table's ``full_name``, extracts catalog and schema,
    and updates matching entity mappings (catalog, schema fields) and
    relationship mappings (source_table, target_table fields).
    """
    return Domain(get_domain(session_mgr)).update_mappings_from_metadata()


@router.post("/metadata/initialize-async")
async def initialize_metadata_async(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Start async metadata loading and return task ID."""
    data = await request.json()
    catalog = data.get("catalog", "").strip()
    schema = data.get("schema", "").strip()
    selected_tables = data.get("selected_tables", None)
    domain = get_domain(session_mgr)
    return Domain(domain, settings).start_metadata_initialize_async(
        catalog, schema, selected_tables
    )


@router.post("/metadata/update-async")
async def update_metadata_async(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Start async metadata update and return task ID."""
    data = await request.json()
    table_names = data.get("table_names", None)
    domain = get_domain(session_mgr)
    return Domain(domain, settings).start_metadata_update_async(table_names)


@router.post("/metadata/update")
async def update_metadata(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Update metadata for already loaded tables by re-fetching from Unity Catalog.

    This refreshes column information for existing tables while preserving user-edited
    comments/descriptions. New columns are added, removed columns are deleted.

    If 'table_names' is provided, only those tables will be updated.
    """
    data = await request.json()
    table_names = data.get("table_names", None)
    domain = get_domain(session_mgr)
    return Domain(domain, settings).update_metadata_tables(table_names)


# ===========================================
# Domain Documents
# ===========================================


def _make_document_parse_service(
    domain: Any,
    settings: Settings,
    store: Any = None,
) -> DocumentParseService:
    """Build the Knowledge Store parse service (Lakebase store + extractor)."""
    if store is None:
        store = Domain(domain, settings).build_registry_service().store
    client = get_databricks_client(domain, settings)
    extractor = (
        DocumentExtractor(client=client) if client is not None else DocumentExtractor()
    )
    return DocumentParseService(store, extractor)


def _resolve_document_scope(domain: Any) -> Tuple[str, str]:
    """Resolve ``(folder, version)`` for the loaded domain's Knowledge Store."""
    folder = (
        getattr(domain, "uc_domain_folder", "")
        or getattr(domain, "domain_folder", "")
        or ""
    ).strip()
    version = str(getattr(domain, "current_version", "") or "1")
    return folder, version


def _run_document_parse_task(
    task: Any,
    service: DocumentParseService,
    folder: str,
    version: str,
    filename: str,
) -> None:
    """Background worker that keeps TaskManager and the parse row aligned."""
    manager = get_task_manager()
    manager.start_task(task.id, message=f"Parsing {filename}")
    try:
        row = service.parse_pending(folder, version, filename)
        if row.get("status") == ParseStatus.READY.value:
            manager.complete_task(
                task.id,
                result=row,
                message=f"Parsed {filename}",
            )
        else:
            manager.fail_task(
                task.id, row.get("error") or "Document parsing failed"
            )
    except Exception as exc:
        logger.exception("Document parse worker failed for %s: %s", filename, exc)
        manager.fail_task(task.id, "Document parsing failed")


def _schedule_document_parse(
    service: DocumentParseService,
    folder: str,
    version: str,
    filename: str,
) -> str:
    task = get_task_manager().run_background_task(
        name=f"Parse {filename}",
        task_type="document_parse",
        target=_run_document_parse_task,
        service=service,
        folder=folder,
        version=version,
        filename=filename,
        steps=[
            {
                "name": "Parse document",
                "description": f"Extracting text from {filename}",
            }
        ],
    )
    return task.id


@router.get("/documents/list")
async def list_documents(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """List the domain's Knowledge Store documents (metadata only, no text)."""
    try:
        domain = get_domain(session_mgr)
        folder, version = _resolve_document_scope(domain)
        if not folder:
            raise ValidationError("Domain not saved to the registry")

        parse_service = _make_document_parse_service(domain, settings)
        rows = parse_service.list_documents(folder, version)
        # Backward-compatible aliases (``name``/``size``) for the current UI.
        files = [
            {**row, "name": row.get("filename"), "size": row.get("size_bytes", 0)}
            for row in rows
        ]
        return {
            "success": True,
            "files": files,
            "message": f"{len(files)} file(s)",
        }
    except (ValidationError, InfrastructureError, NotFoundError):
        raise
    except Exception as e:
        logger.exception("List documents failed: %s", e)
        raise InfrastructureError("Failed to list documents", detail=str(e))


@router.post("/documents/upload")
async def upload_documents(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Upload one or more files to the domain's Knowledge Store.

    Accepts multipart/form-data with field name ``files``. Files are parsed
    Volume-free; each upload is capped at ``MAX_UPLOAD_BYTES`` (10 MB).
    """
    try:
        domain = get_domain(session_mgr)
        folder, version = _resolve_document_scope(domain)
        if not folder:
            raise ValidationError("Domain not saved to the registry")

        parse_service = _make_document_parse_service(domain, settings)

        form = await request.form()
        uploaded_files = form.getlist("files")
        if not uploaded_files:
            raise ValidationError("No files provided")

        max_bytes = DocumentParseService.MAX_UPLOAD_BYTES
        max_mb = max_bytes // (1024 * 1024)

        results: List[Dict[str, Any]] = []
        for upload in uploaded_files:
            raw_name = (upload.filename or "").strip() or "upload.bin"
            filename = os.path.basename(raw_name.replace("\\", "/"))
            if filename in ("", ".", ".."):
                results.append(
                    {
                        "filename": raw_name,
                        "success": False,
                        "message": "Invalid filename",
                    }
                )
                continue

            content = await upload.read()
            if len(content) > max_bytes:
                results.append(
                    {
                        "filename": filename,
                        "success": False,
                        "parse_status": ParseStatus.FAILED.value,
                        "message": f"File exceeds the {max_mb} MB upload limit",
                    }
                )
                continue

            try:
                submission = parse_service.prepare_upload(
                    folder, version, filename, content
                )
                item = {
                    "filename": filename,
                    "success": True,
                    "uploaded": submission.uploaded,
                    "no_op": submission.no_op,
                    "parse_status": submission.parse_status.value,
                }
                if submission.should_parse:
                    item["task_id"] = _schedule_document_parse(
                        parse_service, folder, version, filename
                    )
                    item["message"] = "Uploaded; parsing started"
                elif submission.no_op:
                    item["message"] = "Already uploaded and parsed"
                elif submission.parse_status is ParseStatus.READY:
                    item["message"] = "Uploaded and ready"
                else:
                    item["message"] = submission.error or "Document parsing failed"
                results.append(item)
            except Exception as exc:
                results.append(
                    {
                        "filename": filename,
                        "success": False,
                        "message": "Failed to upload file",
                        "detail": str(exc),
                    }
                )

        succeeded = sum(1 for r in results if r["success"])
        msg = f"{succeeded}/{len(results)} file(s) uploaded"
        if succeeded < len(results):
            first_err = next((r["message"] for r in results if not r["success"]), "")
            if first_err:
                msg = f"{msg}. {first_err}"
        return {
            "success": succeeded > 0,
            "message": msg,
            "results": results,
        }

    except (ValidationError, InfrastructureError, NotFoundError):
        raise
    except Exception as e:
        logger.exception("Upload documents failed: %s", e)
        raise InfrastructureError("Upload documents failed", detail=str(e))


@router.post("/documents/retry-parse")
async def retry_document_parse(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Retry parsing an existing failed or stale binary document."""
    try:
        data = await request.json()
        raw_filename = str(data.get("filename", "")).strip()
        filename = os.path.basename(raw_filename.replace("\\", "/"))
        if not filename or filename in (".", "..") or filename != raw_filename:
            raise ValidationError("Filename is required")

        domain = get_domain(session_mgr)
        folder, version = _resolve_document_scope(domain)
        if not folder:
            raise ValidationError("Domain not saved to the registry")
        parse_service = _make_document_parse_service(domain, settings)
        try:
            submission = parse_service.retry(folder, version, filename)
        except ValueError as exc:
            raise ValidationError(str(exc)) from exc

        result = {
            "success": True,
            "filename": filename,
            "parse_status": submission.parse_status.value,
            "message": (
                "Parsing already in progress"
                if submission.no_op
                else "Document parsing restarted"
            ),
        }
        if submission.should_parse:
            result["task_id"] = _schedule_document_parse(
                parse_service, folder, version, filename
            )
        return result
    except (ValidationError, InfrastructureError, NotFoundError):
        raise
    except Exception as e:
        logger.exception("Retry document parse failed: %s", e)
        raise InfrastructureError(
            "Retry document parse failed", detail=str(e)
        ) from e


@router.post("/documents/delete")
async def delete_document(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Purge one or several documents from the domain's Knowledge Store.

    Accepts ``{"filenames": [...]}`` (multi-purge) or the legacy
    ``{"filename": "..."}`` (single).
    """
    try:
        data = await request.json()
        filenames = data.get("filenames")
        if not isinstance(filenames, list):
            single = str(data.get("filename", "")).strip()
            filenames = [single] if single else []
        filenames = [str(f).strip() for f in filenames if str(f).strip()]
        if not filenames:
            raise ValidationError("At least one filename is required")

        domain = get_domain(session_mgr)
        folder, version = _resolve_document_scope(domain)
        if not folder:
            raise ValidationError("Domain not saved to the registry")

        parse_service = _make_document_parse_service(domain, settings)
        try:
            errors = parse_service.delete_documents(folder, version, filenames)
        except ValueError as exc:
            raise ValidationError(str(exc)) from exc

        if errors:
            logger.warning("Document purge reported errors: %s", errors)
            return {"success": False, "message": "; ".join(errors)}
        return {
            "success": True,
            "message": f"{len(filenames)} document(s) deleted",
        }

    except (ValidationError, InfrastructureError, NotFoundError):
        raise
    except Exception as e:
        logger.exception("Delete document failed: %s", e)
        raise InfrastructureError("Delete document failed", detail=str(e))


@router.get("/documents/preview/{filename:path}")
async def preview_document(
    filename: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return the parsed text of a Knowledge Store document as JSON.

    The original binary is never retained, so preview always serves the
    extracted corpus text (never a streamed binary).
    """
    try:
        domain = get_domain(session_mgr)
        folder, version = _resolve_document_scope(domain)
        if not folder:
            raise ValidationError("Domain not saved to the registry")

        parse_service = _make_document_parse_service(domain, settings)
        doc = parse_service.read_document(folder, version, filename)

        if doc.get("parse_status") != ParseStatus.READY.value:
            return {
                "success": False,
                "filename": filename,
                "parse_status": doc.get("parse_status"),
                "error": doc.get("error") or "Document is not ready",
            }
        return {
            "success": True,
            "filename": filename,
            "content": doc.get("content", ""),
            "parser": doc.get("parsed_with", ""),
            "parse_status": ParseStatus.READY.value,
        }

    except (ValidationError, InfrastructureError, NotFoundError):
        raise
    except Exception as e:
        logger.exception("Preview document failed: %s", e)
        raise InfrastructureError("Preview document failed", detail=str(e))
