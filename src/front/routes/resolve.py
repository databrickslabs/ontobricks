"""Frontend HTML route -- Entity URI resolution.

Accepts an ontology entity URI (path-based or query-param) and redirects
to the Knowledge Graph visualization with the entity focused.

When no explicit ``domain`` query-parameter is supplied the route
inspects the URI against all registry domains' base URIs and
automatically selects the owning domain so the Knowledge Graph page
can load the correct graph.
"""
from __future__ import annotations

import re
from typing import Optional
from urllib.parse import quote

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import RedirectResponse

from back.core.errors import ValidationError
from back.core.helpers import run_blocking
from back.core.logging import get_logger
from back.objects.session import SessionManager, get_session_manager, get_domain
from shared.config.settings import Settings, get_settings

router = APIRouter(tags=["Resolve"])
logger = get_logger(__name__)

_SCHEME_RE = re.compile(r"^https?:/[^/]")


def _normalize_uri(raw: str) -> str:
    """Restore double-slash after scheme when proxies collapse it."""
    if _SCHEME_RE.match(raw):
        return raw.replace(":/", "://", 1)
    return raw


async def _resolve_domain_for_uri(
    entity_uri: str,
    session_mgr: SessionManager,
    settings: Settings,
) -> Optional[str]:
    """Find the registry domain whose base URI matches *entity_uri*.

    Returns the domain folder name, or ``None`` when the URI already
    belongs to the currently loaded domain (no switch needed) or when
    no match is found.
    """
    try:
        from back.objects.registry import RegistryService

        domain = get_domain(session_mgr)
        svc = RegistryService.from_context(domain, settings)
        ok, details, msg = await run_blocking(svc.list_domain_details)
        if not ok:
            logger.warning("Could not list registry domains for URI resolution: %s", msg)
            return None

        current_name = (domain.info.get("name") or "").strip().lower()
        current_folder = (domain.domain_folder or "").strip().lower()
        current_base = (domain.ontology or {}).get("base_uri", "").rstrip("/")

        best_match: Optional[str] = None
        best_len = 0

        for p in details:
            base = (p.get("base_uri") or "").rstrip("/")
            if not base:
                continue
            if entity_uri.startswith(base) and len(base) > best_len:
                best_match = p["name"]
                best_len = len(base)

        if not best_match:
            logger.debug("No registry domain matches URI %s", entity_uri)
            return None

        if best_match.strip().lower() in (current_name, current_folder):
            logger.debug("URI %s belongs to the current domain; no switch needed", entity_uri)
            return None

        if current_base and entity_uri.startswith(current_base):
            logger.debug("URI %s matches current domain base URI; no switch needed", entity_uri)
            return None

        logger.info("URI %s resolved to domain '%s'", entity_uri, best_match)
        return best_match

    except Exception as exc:
        logger.warning("Error resolving domain for URI %s: %s", entity_uri, exc)
        return None


def _build_redirect(entity_uri: str, bridge_domain: Optional[str] = None) -> RedirectResponse:
    encoded = quote(entity_uri, safe="")
    target = f"/dtwin/?section=sigmagraph&focus={encoded}"
    if bridge_domain:
        target += f"&domain={quote(bridge_domain, safe='')}"
    logger.info("Resolving entity URI %s -> %s", entity_uri, target)
    return RedirectResponse(url=target, status_code=302)


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
    if not domain:
        domain = await _resolve_domain_for_uri(uri, session_mgr, settings)
    return _build_redirect(uri, bridge_domain=domain)


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
    normalized = _normalize_uri(uri)
    if not domain:
        domain = request.query_params.get("project")
    if not domain:
        domain = await _resolve_domain_for_uri(normalized, session_mgr, settings)
    return _build_redirect(normalized, bridge_domain=domain)
