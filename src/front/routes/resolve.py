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
from urllib.parse import quote

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import RedirectResponse

from back.core.errors import ValidationError
from back.core.helpers import run_blocking
from back.core.logging import get_logger
from back.objects.domain.domain import Domain
from back.objects.registry import RegistryService
from back.objects.session import SessionManager, get_session_manager, get_domain
from shared.config.settings import Settings, get_settings

router = APIRouter(tags=["Resolve"])
logger = get_logger(__name__)


async def _bridge_domain_for_uri(
    entity_uri: str,
    session_mgr: SessionManager,
    settings: Settings,
) -> Optional[str]:
    """Wire session context into :meth:`RegistryService.resolve_uri_to_domain`."""
    domain = get_domain(session_mgr)
    svc = RegistryService.from_context(domain, settings)
    return await svc.resolve_uri_to_domain(
        entity_uri,
        (domain.info.get("name") or "").strip().lower(),
        (domain.domain_folder or "").strip().lower(),
        (domain.ontology or {}).get("base_uri", "").rstrip("/"),
    )


async def _switch_domain_if_needed(
    target_domain: str,
    session_mgr: SessionManager,
    settings: Settings,
) -> bool:
    """Load *target_domain* into the session if it differs from the current one.

    Returns True if the domain was switched (or was already current).
    """
    ds = get_domain(session_mgr)
    current_folder = (ds.domain_folder or "").strip().lower()
    if current_folder == target_domain.strip().lower():
        return True

    try:
        p = Domain(ds, settings)
        svc = p.build_registry_service()
        result = await run_blocking(p.load_domain_from_uc, svc, target_domain)
        if result.get("success"):
            logger.info("[Bridge] Server-side domain switch to '%s' v%s",
                        target_domain, result.get("version", "?"))
            return True
        logger.warning("[Bridge] Domain switch to '%s' failed: %s",
                       target_domain, result.get("message"))
    except Exception as e:
        logger.exception("[Bridge] Error switching to domain '%s': %s",
                         target_domain, e)
    return False


def _build_redirect(entity_uri: str, bridge_domain: Optional[str] = None) -> RedirectResponse:
    encoded = quote(entity_uri, safe="")
    target = f"/dtwin/?section=sigmagraph&focus={encoded}"
    if bridge_domain:
        target += f"&domain={quote(bridge_domain, safe='')}"
    logger.info("Resolving entity URI %s -> %s", entity_uri, target)
    return RedirectResponse(url=target, status_code=302)


async def _resolve_and_switch(
    entity_uri: str,
    domain_hint: Optional[str],
    session_mgr: SessionManager,
    settings: Settings,
) -> RedirectResponse:
    """Resolve the target domain, switch server-side, redirect without ``&domain=``."""
    target_domain = domain_hint
    if not target_domain:
        target_domain = await _bridge_domain_for_uri(entity_uri, session_mgr, settings)

    if target_domain:
        switched = await _switch_domain_if_needed(target_domain, session_mgr, settings)
        if switched:
            return _build_redirect(entity_uri)

    return _build_redirect(entity_uri, bridge_domain=target_domain)


@router.get("/resolve", include_in_schema=False)
async def resolve_entity_query(
    request: Request,
    uri: str = Query(None, description="Full ontology entity URI"),
    domain: Optional[str] = Query(None, description="Target domain name for cross-domain bridges"),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Resolve an entity URI passed as a query parameter."""
    if not uri:
        raise ValidationError("Missing required 'uri' query parameter")
    if not domain:
        domain = request.query_params.get("project")
    return await _resolve_and_switch(uri, domain, session_mgr, settings)


@router.get("/resolve/{uri:path}", include_in_schema=False)
async def resolve_entity_path(
    request: Request,
    uri: str,
    domain: Optional[str] = Query(None, description="Target domain name for cross-domain bridges"),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Resolve an entity URI embedded in the URL path."""
    if not uri:
        raise ValidationError("Missing entity URI in path")
    normalized = RegistryService.normalize_entity_uri(uri)
    if not domain:
        domain = request.query_params.get("project")
    return await _resolve_and_switch(normalized, domain, session_mgr, settings)
