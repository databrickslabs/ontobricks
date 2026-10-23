"""Frontend HTML route -- Simulation (what-if sandbox) page."""

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from back.core.graphdb.GraphDBFactory import is_graphless_backend
from back.objects.digitaltwin import DigitalTwin
from back.objects.session import SessionManager, get_domain, get_session_manager
from back.objects.simulation import Simulation
from front.fastapi.dependencies import (
    home_redirect_if_unloaded,
    templates,
    triplestore_page_context,
)
from shared.config.settings import Settings, get_settings

router = APIRouter(prefix="/simulation", tags=["Simulation"])


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def simulation_page(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Simulation page.

    Ontology-only ("No Backend") domains have no graph to simulate on, so
    the page falls back to Domain → General like the Knowledge Graph page.
    """
    domain_session = get_domain(session_mgr)
    redirect = home_redirect_if_unloaded(domain_session)
    if redirect is not None:
        return redirect
    if is_graphless_backend((domain_session.info or {}).get("graph_backend")):
        return RedirectResponse("/domain/?section=information", status_code=303)

    ont = domain_session.ontology or {}
    simulation_ctx = {
        "supported": Simulation.is_backend_supported(domain_session),
        "backend_type": DigitalTwin(domain_session).effective_backend_label(),
        "owlrl_available": DigitalTwin.is_owlrl_available(),
        "swrl_rules_count": len(ont.get("swrl_rules", [])),
        "decision_tables_count": len(ont.get("decision_tables", [])),
        "sparql_rules_count": len(ont.get("sparql_rules", [])),
        "aggregate_rules_count": len(ont.get("aggregate_rules", [])),
        "cohort_rules_count": len(ont.get("cohort_rules", [])),
        "default_hops": Simulation.DEFAULT_HOPS,
        "class_cap": Simulation.DEFAULT_CLASS_CAP,
    }

    return templates.TemplateResponse(
        request,
        "simulation.html",
        {
            **triplestore_page_context(domain_session, settings),
            "simulation_ctx": simulation_ctx,
            "domain_name": (domain_session.info or {}).get("name", "NewDomain"),
            "current_version": domain_session.current_version or "1",
        },
    )
