"""
Internal API -- Domain management JSON endpoints.

Moved from app/frontend/project/routes.py during the front/back split.
"""

import io
import json
import os
from typing import Any, Dict, List

from fastapi import APIRouter, Request, Depends
from fastapi.responses import StreamingResponse

from shared.config.settings import get_settings, Settings
from back.core.databricks import is_databricks_app
from back.core.errors import (
    InfrastructureError,
    NotFoundError,
    OntoBricksError,
    ValidationError,
)
from back.core.helpers import (
    get_databricks_client,
    make_volume_file_service,
    resolve_warehouse_id,
)
from back.core.logging import get_logger
from back.objects.session import (
    SessionManager,
    get_domain,
    get_session_manager,
    sanitize_domain_folder,
)
from back.objects.domain import Domain

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
        return {"success": False, "email": ""}
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


# ===========================================
# Session Debug
# ===========================================


@router.get("/session-debug")
async def get_session_debug(session_mgr: SessionManager = Depends(get_session_manager)):
    """Get full session data for debugging purposes.

    Only available when LOG_LEVEL is set to DEBUG.
    """
    return Domain(get_domain(session_mgr)).get_session_debug_response()


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
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """List domain folders under /domains/ in the registry Volume."""
    domain = get_domain(session_mgr)
    svc = Domain(domain, settings).build_registry_service()
    return Domain.list_domains_result(svc)


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
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Save domain into the registry Volume under /domains/<name>/v{ver}.json."""
    domain = get_domain(session_mgr)
    p = Domain(domain, settings)
    return p.save_domain_to_uc(p.build_registry_service())


@router.post("/load-from-uc")
async def load_domain_from_uc(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Load domain from registry Volume."""
    data = await request.json()
    domain_name = data.get("domain", data.get("project"))
    version = data.get("version")
    domain = get_domain(session_mgr)
    p = Domain(domain, settings)
    return p.load_domain_from_uc(p.build_registry_service(), domain_name, version)


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


@router.post("/set-version-mcp")
async def set_version_mcp(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Toggle the API/MCP flag for a specific version (only one may be active)."""
    data = await request.json()
    version = data.get("version")
    enabled = bool(data.get("enabled", False))
    if not version:
        raise ValidationError("version is required")
    domain = get_domain(session_mgr)
    p = Domain(domain, settings)
    return p.set_version_mcp(p.build_registry_service(), version, enabled)


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


@router.get("/documents/list")
async def list_documents(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """List files in the domain volume's documents directory."""
    try:
        domain = get_domain(session_mgr)
        base_path = Domain(domain).get_documents_volume_path()
        if not base_path:
            raise ValidationError("Domain not saved to Unity Catalog")

        uc = make_volume_file_service(domain, settings)

        success, items, message = uc.list_directory(base_path)

        if not success and "not found" in message.lower():
            return {"success": True, "files": [], "message": "No documents yet"}

        if not success:
            logger.warning("List documents failed for %s: %s", base_path, message)
            raise InfrastructureError("Failed to list documents", detail=message)

        return {"success": True, "files": items, "message": f"{len(items)} file(s)"}
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
    """Upload one or more files to the domain volume's documents directory.

    Accepts multipart/form-data with field name ``files``.
    """
    try:
        domain = get_domain(session_mgr)
        base_path = Domain(domain).get_documents_volume_path()
        if not base_path:
            raise ValidationError("Domain not saved to Unity Catalog")

        uc = make_volume_file_service(domain, settings)
        if not uc.is_configured():
            raise ValidationError("Databricks authentication not configured")

        form = await request.form()
        uploaded_files = form.getlist("files")

        if not uploaded_files:
            raise ValidationError("No files provided")

        # Ensure …/domains/<folder>/documents exists (mkdir -p); required before first upload.
        ok_mk, mk_msg = uc.create_directory(base_path)
        if not ok_mk:
            logger.warning("Documents directory could not be created: %s", mk_msg)
            raise InfrastructureError(
                "Documents directory could not be created", detail=mk_msg
            )

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
            file_path = f"{base_path}/{filename}"

            try:
                ok, wmsg = uc.write_binary_file(file_path, content, overwrite=True)
                results.append(
                    {
                        "filename": filename,
                        "success": ok,
                        "message": "Uploaded" if ok else wmsg,
                    }
                )
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


@router.post("/documents/delete")
async def delete_document(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Delete a file from the domain volume's documents directory."""
    try:
        data = await request.json()
        filename = data.get("filename", "").strip()
        if not filename:
            raise ValidationError("Filename is required")

        domain = get_domain(session_mgr)
        base_path = Domain(domain).get_documents_volume_path()
        if not base_path:
            raise ValidationError("Domain not saved to Unity Catalog")

        uc = make_volume_file_service(domain, settings)

        file_path = f"{base_path}/{filename}"
        success, message = uc.delete_file(file_path)
        return {"success": success, "message": message}

    except (ValidationError, InfrastructureError, NotFoundError):
        raise
    except Exception as e:
        logger.exception("Delete document failed: %s", e)
        raise InfrastructureError("Delete document failed", detail=str(e))


_PREVIEW_CONTENT_TYPES = {
    "pdf": "application/pdf",
    "png": "image/png",
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "gif": "image/gif",
    "svg": "image/svg+xml",
}

_TEXT_EXTENSIONS = {
    "txt",
    "md",
    "json",
    "csv",
    "xml",
    "ttl",
    "owl",
    "rdf",
    "yaml",
    "yml",
    "toml",
    "ini",
    "cfg",
    "log",
    "sql",
    "py",
    "js",
    "ts",
    "html",
    "css",
}


@router.get("/documents/preview/{filename:path}")
async def preview_document(
    filename: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Stream a document from the domain volume for in-browser preview.

    Binary files (PDF, images) are streamed with the appropriate content-type.
    Text files are returned as JSON with a ``content`` field.
    """
    try:
        domain = get_domain(session_mgr)
        base_path = Domain(domain).get_documents_volume_path()
        if not base_path:
            raise ValidationError("Domain not saved to Unity Catalog")

        uc = make_volume_file_service(domain, settings)
        if not uc.is_configured():
            raise ValidationError("Databricks authentication not configured")

        file_path = f"{base_path}/{filename}"
        ext = (filename.rsplit(".", 1)[-1] if "." in filename else "").lower()

        content_type = _PREVIEW_CONTENT_TYPES.get(ext)
        if content_type:
            ok, data, pmsg = uc.read_binary_file(file_path)
            if not ok:
                if "not found" in pmsg.lower():
                    raise NotFoundError(f"File not found: {filename}")
                if "denied" in pmsg.lower():
                    raise InfrastructureError("Access denied", detail=pmsg)
                raise InfrastructureError(
                    "Failed to read file for preview", detail=pmsg
                )
            return StreamingResponse(
                io.BytesIO(data),
                media_type=content_type,
                headers={"Content-Disposition": f'inline; filename="{filename}"'},
            )

        if ext in _TEXT_EXTENSIONS:
            ok, text, pmsg = uc.read_file(file_path)
            if not ok:
                if "not found" in pmsg.lower():
                    raise NotFoundError(f"File not found: {filename}")
                if "denied" in pmsg.lower():
                    raise InfrastructureError("Access denied", detail=pmsg)
                raise InfrastructureError(
                    "Failed to read file for preview", detail=pmsg
                )
            return {"success": True, "content": text, "filename": filename, "ext": ext}

        raise ValidationError(f"Preview not supported for .{ext} files")

    except (ValidationError, InfrastructureError, NotFoundError):
        raise
    except Exception as e:
        logger.exception("Preview document failed: %s", e)
        raise InfrastructureError("Preview document failed", detail=str(e))
