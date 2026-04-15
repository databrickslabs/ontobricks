"""Frontend HTML route -- Digital Twin / Query page."""
from fastapi import APIRouter, Request, Depends
from fastapi.responses import HTMLResponse

from front.fastapi.dependencies import templates, triplestore_page_context
from back.objects.session import SessionManager, get_session_manager, get_domain
from back.core.helpers import effective_view_table
from back.objects.digitaltwin import DigitalTwin

router = APIRouter(prefix="/dtwin", tags=["Query"])


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def query_page(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
):
    """Query page."""
    domain_session = get_domain(session_mgr)

    ont = domain_session.ontology or {}
    props = ont.get("properties", [])
    view_table = effective_view_table(domain_session)
    reasoning_ctx = {
        "classes_count": len(ont.get("classes", [])),
        "properties_count": len(props),
        "swrl_rules_count": len(ont.get("swrl_rules", [])),
        "object_properties_count": sum(
            1 for p in props if p.get("type") == "ObjectProperty"
        ),
        "decision_tables_count": len(ont.get("decision_tables", [])),
        "sparql_rules_count": len(ont.get("sparql_rules", [])),
        "aggregate_rules_count": len(ont.get("aggregate_rules", [])),
        "owlrl_available": DigitalTwin.is_owlrl_available(),
        "backend_type": DigitalTwin(domain_session).effective_backend_label(),
        "materialize_table": f"{view_table}_inferred" if view_table else "",
    }

    return templates.TemplateResponse(request, "dtwin.html", {
        **triplestore_page_context(domain_session),
        "reasoning_ctx": reasoning_ctx,
        "domain_name": (domain_session.info or {}).get("name", "NewDomain"),
        "current_version": domain_session.current_version or "1",
    })
