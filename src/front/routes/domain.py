"""Frontend HTML route -- Domain page."""

from fastapi import APIRouter, Request, Depends
from fastapi.responses import HTMLResponse

from front.fastapi.dependencies import (
    home_redirect_if_unloaded,
    templates,
    triplestore_page_context,
)
from back.objects.session import SessionManager, get_session_manager, get_domain
from back.objects.domain import Domain
from shared.config.settings import Settings, get_settings

router = APIRouter(prefix="/domain", tags=["Domain"])


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def domain_page(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Domain management page."""
    domain_session = get_domain(session_mgr)
    redirect = home_redirect_if_unloaded(domain_session)
    if redirect is not None:
        return redirect
    domain_data = Domain(domain_session).get_domain_template_data()
    return templates.TemplateResponse(
        request,
        "domain.html",
        {
            "domain": domain_data,
            **triplestore_page_context(domain_session, settings),
        },
    )
