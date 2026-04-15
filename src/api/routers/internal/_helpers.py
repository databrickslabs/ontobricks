"""Shared route helpers for frontend modules.

Reusable request-handling utilities that are used by multiple
frontend route modules (ontology, mapping, domain, etc.).
"""
from fastapi import Request, Depends

from shared.config.settings import get_settings, Settings
from back.core.databricks import VolumeFileService
from back.core.helpers import get_databricks_host_and_token
from back.core.logging import get_logger
from back.objects.session import SessionManager, get_domain, get_session_manager

logger = get_logger(__name__)


async def save_content_to_uc(
    request: Request,
    session_mgr: SessionManager,
    settings: Settings,
    log_context: str = "content",
) -> dict:
    """Save text content to a Unity Catalog volume path.

    Extracts ``path`` and ``content`` from the JSON request body,
    writes via :class:`VolumeFileService`, and returns a standard
    ``{success, message}`` dict.
    """
    data = await request.json()
    path, content = data.get('path'), data.get('content')

    if not path or not content:
        return {'success': False, 'message': 'Path and content are required'}

    try:
        domain = get_domain(session_mgr)
        host, token = get_databricks_host_and_token(domain, settings)
        uc_service = VolumeFileService(host=host, token=token)
        success, message = uc_service.write_file(path, content)
        return {'success': success, 'message': message}
    except Exception as e:
        logger.exception("Save %s to UC failed: %s", log_context, e)
        return {'success': False, 'message': str(e)}
