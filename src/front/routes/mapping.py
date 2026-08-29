"""Frontend HTML route -- Mapping page."""

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from back.core.graphdb.GraphDBFactory import is_graphless_backend
from back.objects.session import SessionManager, get_domain, get_session_manager
from front.fastapi.dependencies import templates

router = APIRouter(prefix="/mapping", tags=["Mapping"])


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def mapping_page(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
):
    """Mapping management page.

    Ontology-only ("No Backend") domains have no graph, so Mapping is
    disabled: fall back to the Domain Information page.
    """
    domain_session = get_domain(session_mgr)
    if is_graphless_backend((domain_session.info or {}).get("graph_backend")):
        return RedirectResponse("/domain/?section=information", status_code=303)
    return templates.TemplateResponse(request, "mapping.html")
