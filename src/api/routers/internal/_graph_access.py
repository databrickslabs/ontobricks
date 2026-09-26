"""Team gate for graph data access on all backends.

The middleware already blocks non-members from domain-scoped HTTP routes, but
Lakebase / Neo4j reads authenticate as the service principal (no UC identity),
so this explicit check is the sole authorization for those stores. It also
guards UC reads as defence in depth. No-op outside Databricks App mode.
"""

from __future__ import annotations

from fastapi import Request

from back.core.databricks import is_databricks_app
from back.core.errors import AuthorizationError
from back.objects.registry import ROLE_ADMIN, ROLE_VIEWER, role_level


def assert_domain_graph_read(request: Request) -> None:
    """Raise :class:`AuthorizationError` if the caller may not read the graph.

    App admins bypass the Team requirement (they are treated as domain admin).
    Everyone else must hold at least the Viewer role on the loaded domain.
    """
    if not is_databricks_app():
        return

    app_role = getattr(request.state, "user_role", "") or ""
    if app_role == ROLE_ADMIN:
        return

    domain_role = getattr(request.state, "user_domain_role", "") or ""
    if role_level(domain_role) < role_level(ROLE_VIEWER):
        raise AuthorizationError("You are not a member of this domain's team")


def assert_public_graph_read(request: Request, domain, settings) -> None:
    """Team gate for the **public** ``/api`` + ``/graphql`` graph-read surface.

    ``PermissionMiddleware`` bypasses the session role gate on those prefixes
    (so programmatic / MCP callers are not 302-redirected), which leaves
    ``request.state`` carrying empty roles. This resolves the **forwarded**
    user's role for the **target** (query-param / MCP-selected) *domain* and
    stamps it onto ``request.state`` before delegating to
    :func:`assert_domain_graph_read` — so MCP-driven reads get the same
    Viewer+ / admin-bypass / fail-closed rules as Explorer and Graph Chat.
    Unattended callers with no forwarded user resolve to ``none`` → 403.

    No-op outside Databricks App mode (local / PAT dev is unrestricted).
    """
    if not is_databricks_app():
        return

    from back.objects.domain import SettingsService

    folder = getattr(domain, "domain_folder", "") or ""
    domain_role = SettingsService.resolve_domain_role(request, folder, settings)
    request.state.user_domain_role = domain_role
    if role_level(domain_role) >= role_level(ROLE_ADMIN):
        request.state.user_role = ROLE_ADMIN
    assert_domain_graph_read(request)
