"""Frontend HTML route -- Entity URI resolution.

Accepts an ontology entity URI (path-based or query-param) and redirects
to the Knowledge Graph visualization with the entity focused.

When no explicit ``domain`` query-parameter is supplied the route
inspects the URI against all registry domains' base URIs and
automatically selects the owning domain so the Knowledge Graph page
can load the correct graph.

Cross-domain bridges are handled server-side: the target domain is
loaded into the session *before* the redirect, so the browser only
needs a single page load to display the graph.
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import RedirectResponse

from back.core.errors import ValidationError
from back.objects.domain.Domain import Domain
from back.objects.registry import RegistryService
from back.objects.session import SessionManager, get_session_manager, get_domain
from shared.config.settings import Settings, get_settings

router = APIRouter(tags=["Resolve"])


@router.get("/resolve", include_in_schema=False)
async def resolve_entity_query(
    request: Request,
    uri: str = Query(None, description="Full ontology entity URI"),
    domain: Optional[str] = Query(
        None, description="Target domain name for cross-domain bridges"
    ),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Resolve an entity URI passed as a query parameter."""
    if not uri:
        raise ValidationError("Missing required 'uri' query parameter")
    if not domain:
        domain = request.query_params.get("project")
    domain_op = Domain(get_domain(session_mgr), settings)
    url = await domain_op.resolve_entity_uri_redirect(uri, domain)
    return RedirectResponse(url=url, status_code=302)


@router.get("/resolve/{uri:path}", include_in_schema=False)
async def resolve_entity_path(
    request: Request,
    uri: str,
    domain: Optional[str] = Query(
        None, description="Target domain name for cross-domain bridges"
    ),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Resolve an entity URI embedded in the URL path."""
    if not uri:
        raise ValidationError("Missing entity URI in path")
    normalized = RegistryService.normalize_entity_uri(uri)
    if not domain:
        domain = request.query_params.get("project")
    domain_op = Domain(get_domain(session_mgr), settings)
    url = await domain_op.resolve_entity_uri_redirect(normalized, domain)
    return RedirectResponse(url=url, status_code=302)
