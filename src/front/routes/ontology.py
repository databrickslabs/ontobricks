"""Frontend HTML route -- Ontology page."""

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse

from back.objects.session import SessionManager, get_session_manager, get_domain
from front.fastapi.dependencies import templates

router = APIRouter(prefix="/ontology", tags=["Ontology"])


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def ontology_page(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
):
    """Ontology management page."""
    domain_session = get_domain(session_mgr)
    ont = domain_session.ontology or {}

    reasoning_ctx = {
        "cohort_rules_count": len(ont.get("cohort_rules", [])),
    }

    return templates.TemplateResponse(
        request,
        "ontology.html",
        {
            "reasoning_ctx": reasoning_ctx,
            "domain_name": (domain_session.info or {}).get("name", "NewDomain"),
            "current_version": domain_session.current_version or "1",
        },
    )
