"""Frontend HTML route -- Ontology page."""

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from back.objects.session import SessionManager, get_session_manager, get_domain
from front.fastapi.dependencies import home_redirect_if_unloaded, templates

router = APIRouter(prefix="/ontology", tags=["Ontology"])


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def ontology_page(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
):
    """Ontology management page."""
    domain_session = get_domain(session_mgr)
    redirect = home_redirect_if_unloaded(domain_session)
    if redirect is not None:
        return redirect
    if request.query_params.get("section") == "cohorts":
        dest = "/simulation/?section=cohorts"
        rule = request.query_params.get("rule")
        if rule:
            dest += f"&rule={rule}"
        return RedirectResponse(dest, status_code=303)

    return templates.TemplateResponse(
        request,
        "ontology.html",
        {
            "domain_name": (domain_session.info or {}).get("name", "NewDomain"),
            "current_version": domain_session.current_version or "1",
        },
    )
