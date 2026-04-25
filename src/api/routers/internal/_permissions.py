"""Shared helpers for internal routers — request-aware permission utilities.

Single home for the small bits of FastAPI glue that wrap
:class:`back.objects.registry.PermissionService` so the per-router copies
in ``settings.py`` and ``domain.py`` stay in sync.
"""

from typing import Any, List

from fastapi import Request

from shared.config.settings import Settings
from back.objects.session import SessionManager, get_domain
from back.objects.registry import RegistryCfg, ROLE_ADMIN, permission_service


def filter_visible_domains(
    request: Request,
    session_mgr: SessionManager,
    settings: Settings,
    entries: List[Any],
) -> List[Any]:
    """Restrict *entries* to the domains the caller has a role on.

    ``entries`` may be a list of folder-name strings or dicts with a
    ``name`` key (the registry list endpoint returns dicts).  Admins —
    and any caller whose app role isn't resolvable, e.g. local-dev mode —
    get the full list back unchanged.
    """
    if not entries:
        return list(entries)

    user_role = getattr(request.state, "user_role", "") or ""
    if not user_role or user_role == ROLE_ADMIN:
        return list(entries)

    email = (
        getattr(request.state, "user_email", "")
        or request.headers.get("x-forwarded-email", "")
    )
    if not email:
        return list(entries)

    from back.core.helpers import get_databricks_host_and_token

    domain = get_domain(session_mgr)
    host, token = get_databricks_host_and_token(domain, settings)
    user_token = request.headers.get("x-forwarded-access-token", "") or ""
    registry_cfg = RegistryCfg.from_domain(domain, settings).as_dict()

    return permission_service.filter_accessible_domains(
        email,
        host,
        token,
        registry_cfg,
        settings.ontobricks_app_name,
        entries,
        user_token=user_token,
        app_role=user_role,
    )
