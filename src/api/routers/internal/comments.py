"""
Internal API -- collaborative comments & tasks endpoints.

Backs the contextual thread panel (opened from the ontology, mapping and
graph surfaces, or the Validation workspace) and the assignee "My Tasks"
worklist. Comments are anchored to a canonical reference; a comment can be
turned into a task assigned to a teammate.

Authorization is resolved against the *target* domain (which may differ
from the loaded session domain) via
:meth:`SettingsService.resolve_domain_role`. The DRAFT/IN-REVIEW write
gate and the per-action role rules are enforced in
:class:`CommentService`.
"""

from fastapi import APIRouter, Request, Depends

from shared.config.settings import get_settings, Settings
from back.core.logging import get_logger
from back.objects.session import SessionManager, get_session_manager
from back.objects.domain import SettingsService
from back.objects.registry.CommentService import CommentService

logger = get_logger(__name__)

router = APIRouter(prefix="/comments", tags=["Comments"])


@router.get("/{folder}/{version}")
async def list_comments(
    folder: str,
    version: str,
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """All comments for the domain-wide ``(folder, version)`` thread."""
    user_role, domain_role = _roles(request, folder, settings)
    return CommentService.list_comments(
        request,
        session_mgr,
        settings,
        folder,
        version,
        user_role=user_role,
        user_domain_role=domain_role,
    )


@router.post("/{folder}/{version}")
async def add_comment(
    folder: str,
    version: str,
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Add a comment (or a reply via ``parent_id``) to the domain discussion."""
    data = await _body(request)
    user_role, domain_role = _roles(request, folder, settings)
    return CommentService.add_comment(
        request,
        session_mgr,
        settings,
        folder,
        version,
        body=(data.get("body") or ""),
        parent_id=(data.get("parent_id") or None),
        user_role=user_role,
        user_domain_role=domain_role,
    )


@router.post("/{folder}/{version}/{comment_id}/resolve")
async def resolve_comment(
    folder: str,
    version: str,
    comment_id: str,
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Resolve (or reopen) a comment thread."""
    data = await _body(request)
    resolved = data.get("resolved", True)
    user_role, domain_role = _roles(request, folder, settings)
    return CommentService.resolve_comment(
        request,
        session_mgr,
        settings,
        folder,
        version,
        comment_id,
        resolved=bool(resolved),
        user_role=user_role,
        user_domain_role=domain_role,
    )


@router.get("/{folder}/{version}/tasks")
async def list_tasks(
    folder: str,
    version: str,
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """All tasks for a version."""
    user_role, domain_role = _roles(request, folder, settings)
    return CommentService.list_tasks(
        request,
        session_mgr,
        settings,
        folder,
        version,
        user_role=user_role,
        user_domain_role=domain_role,
    )


@router.get("/{folder}/{version}/assignees")
async def list_assignees(
    folder: str,
    version: str,
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """People with a role on the domain — the task assignee picker."""
    del version  # assignees are per-domain, not per-version
    user_role, domain_role = _roles(request, folder, settings)
    return CommentService.list_assignees(
        request,
        session_mgr,
        settings,
        folder,
        user_role=user_role,
        user_domain_role=domain_role,
    )


@router.post("/{folder}/{version}/tasks")
async def create_task(
    folder: str,
    version: str,
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Create a task (optionally born from a comment)."""
    data = await _body(request)
    user_role, domain_role = _roles(request, folder, settings)
    return CommentService.create_task(
        request,
        session_mgr,
        settings,
        folder,
        version,
        assignee=(data.get("assignee") or ""),
        title=(data.get("title") or ""),
        description=(data.get("description") or ""),
        due_date=(data.get("due_date") or None),
        comment_id=(data.get("comment_id") or None),
        user_role=user_role,
        user_domain_role=domain_role,
    )


@router.post("/{folder}/{version}/tasks/{task_id}/status")
async def update_task_status(
    folder: str,
    version: str,
    task_id: str,
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Advance a task's status."""
    data = await _body(request)
    user_role, domain_role = _roles(request, folder, settings)
    return CommentService.update_task_status(
        request,
        session_mgr,
        settings,
        folder,
        version,
        task_id,
        status=(data.get("status") or ""),
        user_role=user_role,
        user_domain_role=domain_role,
    )


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _roles(request: Request, folder: str, settings: Settings):
    """Resolve (app_role, domain_role) for *folder* against the target domain."""
    user_role = getattr(request.state, "user_role", "") or ""
    domain_role = SettingsService.resolve_domain_role(
        request, folder, settings, app_role=user_role
    )
    return user_role, domain_role


async def _body(request: Request) -> dict:
    try:
        return await request.json()
    except Exception:  # noqa: BLE001
        return {}
