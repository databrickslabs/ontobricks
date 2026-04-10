"""Frontend HTML route -- Digital Twin / Query page."""
from fastapi import APIRouter, Request, Depends
from fastapi.responses import HTMLResponse

from front.fastapi.dependencies import templates
from back.objects.session import SessionManager, get_session_manager, get_project
from back.core.helpers import effective_view_table, effective_graph_name
from back.objects.digitaltwin import DigitalTwin

router = APIRouter(prefix="/dtwin", tags=["Query"])


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def query_page(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
):
    """Query page."""
    project = get_project(session_mgr)
    ts_cache = (project.triplestore or {}).get('stats', {})

    ont = project.ontology or {}
    props = ont.get("properties", [])
    view_table = effective_view_table(project)
    materialize_table = f"{view_table}_inferred" if view_table else ""
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
        "backend_type": DigitalTwin(project).effective_backend_label(),
        "materialize_table": materialize_table,
    }

    return templates.TemplateResponse(request, "dtwin.html", {
        "view_table": view_table,
        "graph_name": effective_graph_name(project),
        "triplestore_cache": ts_cache,
        "reasoning_ctx": reasoning_ctx,
    })
