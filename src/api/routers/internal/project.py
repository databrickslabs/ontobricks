"""
Internal API -- Project management JSON endpoints.

Moved from app/frontend/project/routes.py during the front/back split.
"""
import io
import json
import os
from typing import Any, Dict, List

from fastapi import APIRouter, Request, Depends
from fastapi.responses import StreamingResponse

from shared.config.settings import get_settings, Settings
from back.core.databricks import VolumeFileService, is_databricks_app
from back.core.helpers import get_databricks_client, get_databricks_host_and_token, resolve_warehouse_id
from back.core.logging import get_logger
from back.objects.session import SessionManager, get_project, get_session_manager
from back.objects.project import Project

logger = get_logger(__name__)

router = APIRouter(prefix="/project", tags=["Project"])


# ===========================================
# Project Info API
# ===========================================

@router.get("/info")
async def get_project_info(session_mgr: SessionManager = Depends(get_session_manager)):
    """Get current project information."""
    return Project(get_project(session_mgr)).get_project_info()


@router.post("/info")
async def save_project_info(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Save project information."""
    data = await request.json()
    project = get_project(session_mgr)
    response_info = Project(project).save_project_info(data)
    return {'success': True, 'info': response_info, 'message': 'Project info saved'}


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
            return {'success': True, 'email': name or email}

    project = get_project(session_mgr)
    client = get_databricks_client(project, settings)
    if not client:
        return {'success': False, 'email': ''}
    email = client.get_current_user_email()
    return {'success': True, 'email': email}


# ===========================================
# Project Save/Export
# ===========================================

@router.post("/save")
async def save_project(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Save project to session (for export)."""
    data = await request.json()
    project = get_project(session_mgr)
    
    project_name = data.get('name', 'NewProject')
    project.info['name'] = project_name
    project.save()
    
    return {'success': True, 'name': project_name}


@router.get("/export")
async def export_project(session_mgr: SessionManager = Depends(get_session_manager)):
    """Export complete project as JSON.
    
    Note: Generated outputs (R2RML, OWL) are NOT exported - they are regenerated from source data.
    """
    project = get_project(session_mgr)
    
    export_data = project.export_for_save()
    
    return {
        'success': True,
        'name': project.info.get('name', 'NewProject'),
        'project': export_data
    }



# ===========================================
# Project Import/Load
# ===========================================

@router.post("/import")
async def import_project(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Import project from JSON (supports both file upload and JSON body).
    
    For versioned projects, pass 'version' parameter to load a specific version.
    """
    content_type = request.headers.get('content-type', '')
    selected_version = None
    
    # Handle file upload (multipart/form-data)
    if 'multipart/form-data' in content_type:
        form = await request.form()
        file = form.get('file')
        if file:
            content = await file.read()
            project_data = json.loads(content.decode('utf-8'))
        else:
            return {'success': False, 'message': 'No file provided'}
    else:
        # Handle JSON body
        data = await request.json()
        project_data = data.get('project', data)  # Support both wrapped and unwrapped format
        selected_version = data.get('version')  # Optional: specific version to load
    
    project = get_project(session_mgr)
    return Project(project).import_project(project_data, selected_version)


# ===========================================
# Project Reset/Clear
# ===========================================

@router.post("/reset")
async def reset_project(session_mgr: SessionManager = Depends(get_session_manager)):
    """Reset entire project to empty state."""
    project = get_project(session_mgr)
    project.reset()
    project.clear_uc_metadata()
    return {'success': True, 'message': 'Project reset'}


@router.post("/clear")
async def clear_project(session_mgr: SessionManager = Depends(get_session_manager)):
    """Clear current project and start fresh (delegates to reset)."""
    return await reset_project(session_mgr)


# ===========================================
# Session Debug
# ===========================================

@router.get("/session-debug")
async def get_session_debug(session_mgr: SessionManager = Depends(get_session_manager)):
    """Get full session data for debugging purposes.

    Only available when LOG_LEVEL is set to DEBUG.
    """
    return Project(get_project(session_mgr)).get_session_debug_response()


# ===========================================
# Project Configuration (Databricks)
# ===========================================

@router.get("/config")
async def get_project_config(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Get project-specific configuration.
    
    The warehouse_id is read-only here (set globally via Settings by admins).
    Catalog/schema are NOT stored -- they are selected dynamically when needed.
    """
    project = get_project(session_mgr)
    
    return {
        'success': True,
        'warehouse_id': resolve_warehouse_id(project, settings)
    }


@router.post("/config")
async def save_project_config(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Save project-specific configuration.
    
    Note: warehouse_id is no longer stored per-project (it is instance-global).
    Catalog/schema are NOT stored -- they are selected dynamically when needed.
    """
    return {'success': True, 'message': 'Project configuration saved'}



# ===========================================
# Design Views Management
# ===========================================

@router.get("/design-views")
async def get_design_views(session_mgr: SessionManager = Depends(get_session_manager)):
    """Get all design views and current view name."""
    return Project(get_project(session_mgr)).get_design_views()


@router.post("/design-views/create")
async def create_design_view(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Create a new design view."""
    data = await request.json()
    view_name = data.get('name', '').strip()
    copy_from = data.get('copy_from')
    project = get_project(session_mgr)
    return Project(project).create_design_view(view_name, copy_from)


@router.post("/design-views/rename")
async def rename_design_view(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Rename an existing design view."""
    data = await request.json()
    old_name = data.get('old_name', '').strip()
    new_name = data.get('new_name', '').strip()
    project = get_project(session_mgr)
    return Project(project).rename_design_view(old_name, new_name)


@router.post("/design-views/delete")
async def delete_design_view(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Delete a design view."""
    data = await request.json()
    view_name = data.get('name', '').strip()
    project = get_project(session_mgr)
    return Project(project).delete_design_view(view_name)


@router.post("/design-views/switch")
async def switch_design_view(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Switch to a different design view."""
    data = await request.json()
    view_name = data.get('name', '').strip()
    project = get_project(session_mgr)
    return Project(project).switch_design_view(view_name)


@router.get("/design-views/current")
async def get_current_design_view(session_mgr: SessionManager = Depends(get_session_manager)):
    """Get the current design view layout."""
    return Project(get_project(session_mgr)).get_current_design_view()


@router.post("/design-views/save-current")
async def save_current_design_view(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Save layout data to the current view."""
    layout_data = await request.json()
    project = get_project(session_mgr)
    return Project(project).save_current_design_view(layout_data)


# ===========================================
# Map Layout Management
# ===========================================

@router.get("/map-layout")
async def get_map_layout(session_mgr: SessionManager = Depends(get_session_manager)):
    """Get the saved map layout (node positions)."""
    return Project(get_project(session_mgr)).get_map_layout()


@router.post("/map-layout")
async def save_map_layout(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Save the map layout (node positions)."""
    layout_data = await request.json()
    return Project(get_project(session_mgr)).save_map_layout(layout_data)


# ===========================================
# Unity Catalog Project Management
# ===========================================


@router.get("/list-projects")
async def list_projects(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """List project folders under /projects/ in the registry Volume."""
    project = get_project(session_mgr)
    svc = Project(project, settings).build_registry_service()
    return Project.list_projects_result(svc)


@router.get("/list-versions")
async def list_project_versions(
    project_name: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """List available versions for a project inside the registry."""
    project = get_project(session_mgr)
    svc = Project(project, settings).build_registry_service()
    return Project.list_project_versions_result(svc, project_name)


@router.post("/save-to-uc")
async def save_project_to_uc(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Save project into the registry Volume under /projects/<name>/v{ver}.json."""
    project = get_project(session_mgr)
    p = Project(project, settings)
    return p.save_project_to_uc(p.build_registry_service())


@router.post("/load-from-uc")
async def load_project_from_uc(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Load project from registry Volume."""
    data = await request.json()
    project_name = data.get('project')
    version = data.get('version')
    project = get_project(session_mgr)
    p = Project(project, settings)
    return p.load_project_from_uc(p.build_registry_service(), project_name, version)


@router.post("/create-version")
async def create_new_version(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Create a new version of the project and save to registry."""
    project = get_project(session_mgr)
    p = Project(project, settings)
    return p.create_new_project_version(p.build_registry_service())


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
    return Project(get_project(session_mgr), settings).get_version_status(refresh=refresh)


@router.get("/versions-list")
async def list_version_details(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """List all versions with per-version description, mcp_enabled flag, and status."""
    project = get_project(session_mgr)
    p = Project(project, settings)
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
        return {"success": False, "message": "version is required"}
    project = get_project(session_mgr)
    p = Project(project, settings)
    return p.set_version_mcp(p.build_registry_service(), version, enabled)


# ===========================================
# Unity Catalog Metadata Management
# =========================================== 

@router.get("/metadata")
async def get_metadata(session_mgr: SessionManager = Depends(get_session_manager)):
    """Get stored Unity Catalog metadata from session."""
    return Project(get_project(session_mgr)).get_metadata_response()


@router.post("/metadata/list-tables")
async def list_schema_tables(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """List all tables in a schema without loading full metadata.

    Returns table names only for selection before loading.
    """
    data = await request.json()
    catalog = data.get('catalog', '').strip()
    schema = data.get('schema', '').strip()
    project = get_project(session_mgr)
    return await Project(project, settings).list_schema_tables_result(catalog, schema)


@router.post("/metadata/initialize")
async def initialize_metadata(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Load Unity Catalog metadata by reading tables and columns from a schema.

    This merges new tables with existing metadata - existing tables are preserved,
    only new tables are added. Table and column comments are fetched from UC.

    If 'selected_tables' is provided, only those tables will be loaded.
    """
    data = await request.json()
    catalog = data.get('catalog', '').strip()
    schema = data.get('schema', '').strip()
    selected_tables = data.get('selected_tables', None)
    project = get_project(session_mgr)
    return Project(project, settings).initialize_metadata_result(catalog, schema, selected_tables)


@router.post("/metadata/save")
async def save_metadata(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager)
):
    """Save selected tables to metadata.

    This allows users to filter which tables are kept in the metadata.
    Tables should have full_name field (catalog.schema.table).
    For backwards compatibility, if full_name is missing, it will be constructed
    from existing metadata's catalog/schema or legacy fields.
    """
    data = await request.json()
    tables = data.get('tables', [])
    return Project(get_project(session_mgr)).save_metadata_tables(tables)


@router.post("/metadata/clear")
async def clear_metadata(session_mgr: SessionManager = Depends(get_session_manager)):
    """Clear stored Unity Catalog metadata from session."""
    return Project(get_project(session_mgr)).clear_metadata()


@router.post("/metadata/initialize-async")
async def initialize_metadata_async(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Start async metadata loading and return task ID."""
    data = await request.json()
    catalog = data.get('catalog', '').strip()
    schema = data.get('schema', '').strip()
    selected_tables = data.get('selected_tables', None)
    project = get_project(session_mgr)
    return Project(project, settings).start_metadata_initialize_async(catalog, schema, selected_tables)


@router.post("/metadata/update-async")
async def update_metadata_async(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Start async metadata update and return task ID."""
    data = await request.json()
    table_names = data.get('table_names', None)
    project = get_project(session_mgr)
    return Project(project, settings).start_metadata_update_async(table_names)


@router.post("/metadata/update")
async def update_metadata(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Update metadata for already loaded tables by re-fetching from Unity Catalog.

    This refreshes column information for existing tables while preserving user-edited
    comments/descriptions. New columns are added, removed columns are deleted.

    If 'table_names' is provided, only those tables will be updated.
    """
    data = await request.json()
    table_names = data.get('table_names', None)
    project = get_project(session_mgr)
    return Project(project, settings).update_metadata_tables(table_names)


# ===========================================
# Project Documents
# ===========================================


@router.get("/documents/list")
async def list_documents(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """List files in the project volume's documents directory."""
    try:
        project = get_project(session_mgr)
        base_path = Project(project).get_documents_volume_path()
        if not base_path:
            return {'success': False, 'message': 'Project not saved to Unity Catalog'}

        host, token = get_databricks_host_and_token(project, settings)
        uc = VolumeFileService(host=host, token=token)

        success, items, message = uc.list_directory(base_path)

        if not success and "not found" in message.lower():
            return {'success': True, 'files': [], 'message': 'No documents yet'}

        if not success:
            logger.warning("List documents failed for %s: %s", base_path, message)
            return {'success': False, 'files': [], 'message': message}

        return {'success': True, 'files': items, 'message': f'{len(items)} file(s)'}
    except Exception as e:
        logger.exception("List documents failed: %s", e)
        return {'success': False, 'message': str(e), 'files': []}


@router.post("/documents/upload")
async def upload_documents(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Upload one or more files to the project volume's documents directory.

    Accepts multipart/form-data with field name ``files``.
    """
    try:
        project = get_project(session_mgr)
        base_path = Project(project).get_documents_volume_path()
        if not base_path:
            return {'success': False, 'message': 'Project not saved to Unity Catalog'}

        host, token = get_databricks_host_and_token(project, settings)
        uc = VolumeFileService(host=host, token=token)
        if not uc.is_configured():
            return {'success': False, 'message': 'Databricks authentication not configured'}

        form = await request.form()
        uploaded_files = form.getlist('files')

        if not uploaded_files:
            return {'success': False, 'message': 'No files provided'}

        # Ensure …/projects/<folder>/documents exists (mkdir -p); required before first upload.
        ok_mk, mk_msg = uc.create_directory(base_path)
        if not ok_mk:
            logger.warning("Documents directory could not be created: %s", mk_msg)
            return {'success': False, 'message': mk_msg}

        results: List[Dict[str, Any]] = []
        for upload in uploaded_files:
            raw_name = (upload.filename or "").strip() or "upload.bin"
            filename = os.path.basename(raw_name.replace("\\", "/"))
            if filename in ("", ".", ".."):
                results.append({
                    'filename': raw_name,
                    'success': False,
                    'message': 'Invalid filename',
                })
                continue

            content = await upload.read()
            file_path = f"{base_path}/{filename}"

            try:
                ok, wmsg = uc.write_binary_file(file_path, content, overwrite=True)
                results.append({
                    'filename': filename,
                    'success': ok,
                    'message': 'Uploaded' if ok else wmsg,
                })
            except Exception as exc:
                results.append({'filename': filename, 'success': False, 'message': str(exc)})

        succeeded = sum(1 for r in results if r['success'])
        msg = f'{succeeded}/{len(results)} file(s) uploaded'
        if succeeded < len(results):
            first_err = next((r['message'] for r in results if not r['success']), '')
            if first_err:
                msg = f'{msg}. {first_err}'
        return {
            'success': succeeded > 0,
            'message': msg,
            'results': results,
        }

    except Exception as e:
        logger.exception("Upload documents failed: %s", e)
        return {'success': False, 'message': str(e)}


@router.post("/documents/delete")
async def delete_document(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Delete a file from the project volume's documents directory."""
    try:
        data = await request.json()
        filename = data.get('filename', '').strip()
        if not filename:
            return {'success': False, 'message': 'Filename is required'}

        project = get_project(session_mgr)
        base_path = Project(project).get_documents_volume_path()
        if not base_path:
            return {'success': False, 'message': 'Project not saved to Unity Catalog'}

        host, token = get_databricks_host_and_token(project, settings)
        uc = VolumeFileService(host=host, token=token)

        file_path = f"{base_path}/{filename}"
        success, message = uc.delete_file(file_path)
        return {'success': success, 'message': message}

    except Exception as e:
        logger.exception("Delete document failed: %s", e)
        return {'success': False, 'message': str(e)}


_PREVIEW_CONTENT_TYPES = {
    'pdf': 'application/pdf',
    'png': 'image/png',
    'jpg': 'image/jpeg',
    'jpeg': 'image/jpeg',
    'gif': 'image/gif',
    'svg': 'image/svg+xml',
}

_TEXT_EXTENSIONS = {
    'txt', 'md', 'json', 'csv', 'xml', 'ttl', 'owl', 'rdf',
    'yaml', 'yml', 'toml', 'ini', 'cfg', 'log', 'sql', 'py',
    'js', 'ts', 'html', 'css',
}


@router.get("/documents/preview/{filename:path}")
async def preview_document(
    filename: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Stream a document from the project volume for in-browser preview.

    Binary files (PDF, images) are streamed with the appropriate content-type.
    Text files are returned as JSON with a ``content`` field.
    """
    try:
        project = get_project(session_mgr)
        base_path = Project(project).get_documents_volume_path()
        if not base_path:
            return {'success': False, 'message': 'Project not saved to Unity Catalog'}

        host, token = get_databricks_host_and_token(project, settings)
        uc = VolumeFileService(host=host, token=token)
        if not uc.is_configured():
            return {'success': False, 'message': 'Databricks authentication not configured'}

        file_path = f"{base_path}/{filename}"
        ext = (filename.rsplit('.', 1)[-1] if '.' in filename else '').lower()

        content_type = _PREVIEW_CONTENT_TYPES.get(ext)
        if content_type:
            ok, data, pmsg = uc.read_binary_file(file_path)
            if not ok:
                if 'not found' in pmsg.lower():
                    return {'success': False, 'message': f'File not found: {filename}'}
                if 'denied' in pmsg.lower():
                    return {'success': False, 'message': 'Access denied'}
                return {'success': False, 'message': pmsg}
            return StreamingResponse(
                io.BytesIO(data),
                media_type=content_type,
                headers={'Content-Disposition': f'inline; filename="{filename}"'},
            )

        if ext in _TEXT_EXTENSIONS:
            ok, text, pmsg = uc.read_file(file_path)
            if not ok:
                if 'not found' in pmsg.lower():
                    return {'success': False, 'message': f'File not found: {filename}'}
                if 'denied' in pmsg.lower():
                    return {'success': False, 'message': 'Access denied'}
                return {'success': False, 'message': pmsg}
            return {'success': True, 'content': text, 'filename': filename, 'ext': ext}

        return {'success': False, 'message': f'Preview not supported for .{ext} files'}

    except Exception as e:
        logger.exception("Preview document failed: %s", e)
        return {'success': False, 'message': str(e)}
