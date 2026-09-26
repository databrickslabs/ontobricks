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
