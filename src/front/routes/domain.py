"""Frontend HTML route -- Domain page."""

from fastapi import APIRouter, Request, Depends
from fastapi.responses import HTMLResponse

from front.fastapi.dependencies import templates, triplestore_page_context
from back.objects.session import SessionManager, get_session_manager, get_domain
from back.objects.domain import Domain

router = APIRouter(prefix="/domain", tags=["Domain"])


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def domain_page(
    request: Request, session_mgr: SessionManager = Depends(get_session_manager)
):
    """Domain management page."""
    domain_session = get_domain(session_mgr)
    domain_data = Domain(domain_session).get_domain_template_data()
    return templates.TemplateResponse(
        request,
        "domain.html",
        {
            "domain": domain_data,
            **triplestore_page_context(domain_session),
        },
    )
