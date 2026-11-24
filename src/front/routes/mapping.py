"""Frontend HTML route -- Mapping page (redirects into Graph → Management → Mapping)."""

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from back.core.graphdb.GraphDBFactory import is_graphless_backend
from back.objects.session import SessionManager, get_domain, get_session_manager
from front.fastapi.dependencies import home_redirect_if_unloaded

router = APIRouter(prefix="/mapping", tags=["Mapping"])


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def mapping_page(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
):
    """Legacy Mapping URL — redirect into Graph → Management → Mapping.

    Ontology-only ("No Backend") domains have no graph, so Mapping is
    disabled: fall back to the Domain → General page.
    """
    domain_session = get_domain(session_mgr)
    redirect = home_redirect_if_unloaded(domain_session)
    if redirect is not None:
        return redirect
    if is_graphless_backend((domain_session.info or {}).get("graph_backend")):
        return RedirectResponse("/domain/?section=home", status_code=303)

    query = request.url.query
    if query:
        target = f"/dtwin/?{query}"
    else:
        target = "/dtwin/?section=design&tab=informations"
    return RedirectResponse(target, status_code=303)
