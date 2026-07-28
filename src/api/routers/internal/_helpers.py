"""Shared route helpers for frontend modules.

Reusable request-handling utilities that are used by multiple
frontend route modules (ontology, mapping, domain, etc.).
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING

from fastapi import Request

if TYPE_CHECKING:
    import logging

from shared.config.settings import Settings
from back.core.errors import InfrastructureError, OntoBricksError, ValidationError
from back.core.helpers import make_volume_file_service
from back.core.logging import get_logger
from back.objects.domain import SettingsService
from back.objects.session import SessionManager, get_domain

logger = get_logger(__name__)


def resolve_roles(request: Request, folder: str, settings: Settings):
    """Resolve ``(app_role, domain_role)`` for *folder* against the target domain.

    Shared by the comments and review routers, which both authorize against the
    *target* domain (which may differ from the loaded session domain).
    """
    user_role = getattr(request.state, "user_role", "") or ""
    domain_role = SettingsService.resolve_domain_role(
        request, folder, settings, app_role=user_role
    )
    return user_role, domain_role


async def read_json_body(request: Request) -> dict:
    """Return the parsed JSON request body, or ``{}`` when absent/invalid."""
    try:
        return await request.json()
    except Exception:  # noqa: BLE001
        return {}


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
    path, content = data.get("path"), data.get("content")

    if not path or not content:
        raise ValidationError("Path and content are required")

    try:
        domain = get_domain(session_mgr)
        uc_service = make_volume_file_service(domain, settings)
        success, message = uc_service.write_file(path, content)
        if not success:
            raise InfrastructureError(
                f"Failed to save {log_context} to UC", detail=message
            )
        return {"success": True, "message": message}
    except (ValidationError, InfrastructureError):
        raise
    except Exception as e:
        logger.exception("Save %s to UC failed: %s", log_context, e)
        raise InfrastructureError(f"Failed to save {log_context}", detail=str(e))
