"""
Internal API -- Ontology review / validation workflow endpoints.

Backs the Registry "My Tasks" worklist and the Domain "Validation"
workspace. Orchestrates the review workflow (submit / sign-off /
publish / reopen) on top of the version lifecycle, persisting every
decision to the ``domain_review_events`` audit log.

Authorization is resolved against the *target* domain (which may differ
from the loaded session domain — e.g. acting on a task from the Registry
worklist) via :meth:`SettingsService.resolve_domain_role`. The workflow
role rules and quorum gate are enforced in :class:`ReviewService`.
"""

from fastapi import APIRouter, Request, Depends

from shared.config.settings import get_settings, Settings
from back.core.errors import ValidationError
from back.core.logging import get_logger
from back.objects.session import SessionManager, get_session_manager
from back.objects.domain import SettingsService
from back.objects.registry.ReviewService import ReviewService

logger = get_logger(__name__)

router = APIRouter(prefix="/review", tags=["Review"])


@router.get("/my-tasks")
async def my_tasks(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Cross-domain worklist of versions with a pending action for the caller."""
    return ReviewService.my_tasks(request, session_mgr, settings)


@router.get("/{folder}/{version}")
async def review_detail(
    folder: str,
    version: str,
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Review state, audit trail and allowed actions for a single version."""
    user_role, domain_role = _roles(request, folder, settings)
    return ReviewService.review_detail(
        request,
        session_mgr,
        settings,
        folder,
        version,
        user_role=user_role,
        user_domain_role=domain_role,
    )


@router.get("/{folder}/{version}/team")
async def review_team(
    folder: str,
    version: str,
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Domain access list (who can view / edit / build) for the domain.

    ``version`` is part of the path for URL symmetry with the other
    review endpoints; the access list is per-domain, not per-version.
    """
    del version
    return ReviewService.review_team(request, session_mgr, settings, folder)


@router.post("/{folder}/{version}/submit")
async def submit(
    folder: str,
    version: str,
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Submit a DRAFT version for review (DRAFT -> IN-REVIEW)."""
    comment = await _comment(request)
    user_role, domain_role = _roles(request, folder, settings)
    return ReviewService.submit(
        request,
        session_mgr,
        settings,
        folder,
        version,
        comment=comment,
        user_role=user_role,
        user_domain_role=domain_role,
    )


@router.post("/{folder}/{version}/signoff")
async def signoff(
    folder: str,
    version: str,
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Record a sign-off (``approve`` / ``request_changes``) on a version."""
    data = await _body(request)
    decision = (data.get("decision") or "").strip()
    comment = (data.get("comment") or "").strip()
    if not decision:
        raise ValidationError("decision is required")
    user_role, domain_role = _roles(request, folder, settings)
    return ReviewService.signoff(
        request,
        session_mgr,
        settings,
        folder,
        version,
        decision=decision,
        comment=comment,
        user_role=user_role,
        user_domain_role=domain_role,
    )


@router.post("/{folder}/{version}/publish")
async def publish(
    folder: str,
    version: str,
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Publish an IN-REVIEW version once the sign-off quorum is met."""
    comment = await _comment(request)
    user_role, domain_role = _roles(request, folder, settings)
    return ReviewService.publish(
        request,
        session_mgr,
        settings,
        folder,
        version,
        comment=comment,
        user_role=user_role,
        user_domain_role=domain_role,
    )


@router.post("/{folder}/{version}/reopen")
async def reopen(
    folder: str,
    version: str,
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Send an IN-REVIEW or PUBLISHED version back to Draft (admin only)."""
    comment = await _comment(request)
    user_role, domain_role = _roles(request, folder, settings)
    return ReviewService.reopen(
        request,
        session_mgr,
        settings,
        folder,
        version,
        comment=comment,
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


async def _comment(request: Request) -> str:
    data = await _body(request)
    return (data.get("comment") or "").strip()
