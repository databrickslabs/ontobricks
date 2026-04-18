"""
Internal API -- Home / session / validation JSON endpoints.

Moved from app/frontend/home/routes.py during the front/back split.
"""

from fastapi import APIRouter, Request, Depends
from back.core.errors import ValidationError, InfrastructureError
from back.objects.session import SessionManager, get_session_manager

from shared.config.settings import get_settings, Settings
from back.core.databricks import DatabricksClient
from back.objects.session import get_domain
from back.core.helpers import get_databricks_client, resolve_warehouse_id
from back.objects.domain.HomeService import HomeService as home_service

router = APIRouter(tags=["Home"])


# ===========================================
# Session Status APIs
# ===========================================


@router.get("/navbar/state")
async def navbar_state(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Consolidated navbar state: domain info, validation, dtwin cache, warehouse.

    Returns everything the navbar needs in a single round-trip so the
    client avoids issuing four or five separate requests on every page load.
    """
    domain = get_domain(session_mgr)
    warehouse_id = resolve_warehouse_id(domain, settings)
    return await home_service.get_navbar_state(
        domain, settings, warehouse_id=warehouse_id
    )


@router.get("/session-status")
async def session_status(session_mgr: SessionManager = Depends(get_session_manager)):
    """Get current session status for navbar indicators."""
    domain = get_domain(session_mgr)
    return home_service.get_session_status(domain)


@router.post("/reset-session")
async def reset_session(session_mgr: SessionManager = Depends(get_session_manager)):
    """Reset entire session."""
    domain = get_domain(session_mgr)
    domain.reset()
    return {"success": True, "message": "Session reset"}


# ===========================================
# Validation APIs
# ===========================================


@router.get("/validate/ontology")
async def validate_ontology_endpoint(
    session_mgr: SessionManager = Depends(get_session_manager),
):
    """Validate current ontology."""
    domain = get_domain(session_mgr)
    return home_service.validate_ontology(domain)


@router.get("/validate/detailed")
async def validate_detailed(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Get detailed validation status including Digital Twin and warehouse."""
    domain = get_domain(session_mgr)
    warehouse_id = resolve_warehouse_id(domain, settings)
    return await home_service.get_detailed_validation(
        domain, settings, warehouse_id=warehouse_id
    )


# ===========================================
# Unity Catalog File Operations
# ===========================================


@router.post("/browse-volume")
async def browse_volume(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Browse files in a Unity Catalog volume."""
    data = await request.json()
    catalog = data.get("catalog")
    schema = data.get("schema")
    volume = data.get("volume")
    path = data.get("path", "")
    extensions = data.get("extensions", [])

    if not all([catalog, schema, volume]):
        raise ValidationError("Missing required parameters: catalog, schema, volume")

    client = _get_client(session_mgr, settings)
    if not client:
        raise ValidationError("Databricks not configured")

    volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
    if path:
        volume_path = f"{volume_path}/{path}"

    files = client.list_volume_files(volume_path)

    if extensions:
        files = [
            f
            for f in files
            if any(f.get("name", "").endswith(ext) for ext in extensions)
            or f.get("is_directory")
        ]

    return {"success": True, "files": files, "path": volume_path}


@router.post("/read-volume-file")
async def read_volume_file(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Read a file from Unity Catalog volume."""
    data = await request.json()
    # Accept both 'path' and 'file_path' for compatibility
    path = data.get("path") or data.get("file_path")

    if not path:
        raise ValidationError("File path is required")

    from back.core.databricks import VolumeFileService

    domain = get_domain(session_mgr)
    host = domain.databricks.get("host") or settings.databricks_host
    token = domain.databricks.get("token") or settings.databricks_token

    uc_service = VolumeFileService(host=host, token=token)
    success, content, message = uc_service.read_file(path)

    if not success:
        raise InfrastructureError(message or f"Could not read file: {path}")

    return {"success": True, "content": content}


@router.post("/write-volume-file")
async def write_volume_file(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Write a file to Unity Catalog volume."""
    data = await request.json()
    # Accept both 'path' and 'file_path' for compatibility
    path = data.get("path") or data.get("file_path")
    content = data.get("content")

    if not path or content is None:
        raise ValidationError("Path and content are required")

    from back.core.databricks import VolumeFileService

    domain = get_domain(session_mgr)
    host = domain.databricks.get("host") or settings.databricks_host
    token = domain.databricks.get("token") or settings.databricks_token

    uc_service = VolumeFileService(host=host, token=token)
    success, message = uc_service.write_file(path, content)

    if not success:
        raise InfrastructureError(message or f"Could not write file: {path}")

    return {"success": True, "message": message}


# ===========================================
# Ontology/Mapping Clear
# ===========================================


@router.post("/clear-ontology")
async def clear_ontology(session_mgr: SessionManager = Depends(get_session_manager)):
    """Clear ontology, associated mappings, and design layout from session."""
    domain = get_domain(session_mgr)
    domain.reset_ontology()
    return {"success": True, "message": "Ontology, mappings, and layout cleared"}


# ===========================================
# Helper Functions
# ===========================================


def _get_client(session_mgr: SessionManager, settings: Settings) -> DatabricksClient:
    """Get Databricks client using centralized helper."""
    domain = get_domain(session_mgr)
    return get_databricks_client(domain, settings)
