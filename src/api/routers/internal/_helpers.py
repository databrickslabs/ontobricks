"""Shared route helpers for frontend modules.

Reusable request-handling utilities that are used by multiple
frontend route modules (ontology, mapping, domain, etc.).
"""
import logging
from contextlib import contextmanager

from fastapi import Request

from shared.config.settings import Settings
from back.core.errors import InfrastructureError, OntoBricksError, ValidationError
from back.core.helpers import make_volume_file_service
from back.core.logging import get_logger
from back.objects.session import SessionManager, get_domain

logger = get_logger(__name__)


@contextmanager
def map_route_errors(context: str, logger: logging.Logger):
    """Map unexpected exceptions to :class:`InfrastructureError` for API routes.

    Re-raises :class:`OntoBricksError` subclasses unchanged. Any other
    ``Exception`` is logged with ``logger.exception`` and wrapped in
    ``InfrastructureError(context, detail=str(exc))`` (works inside sync or
    async handlers because it only wraps a ``with`` block).
    """
    try:
        yield
    except OntoBricksError:
        raise
    except Exception as exc:
        logger.exception("%s: %s", context, exc)
        raise InfrastructureError(context, detail=str(exc)) from exc


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
        raise ValidationError("Path and content are required")

    try:
        domain = get_domain(session_mgr)
        uc_service = make_volume_file_service(domain, settings)
        success, message = uc_service.write_file(path, content)
        if not success:
            raise InfrastructureError(f"Failed to save {log_context} to UC", detail=message)
        return {'success': True, 'message': message}
    except (ValidationError, InfrastructureError):
        raise
    except Exception as e:
        logger.exception("Save %s to UC failed: %s", log_context, e)
        raise InfrastructureError(f"Failed to save {log_context}", detail=str(e))
