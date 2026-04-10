"""GraphQL API endpoint for OntoBricks.

Mounts a per-project GraphQL endpoint that auto-generates a typed
schema from the project's ontology and resolves queries against the
triple store.

Routes (mounted at ``/graphql`` on the main app and at the external prefix from ``api.constants.EXTERNAL_GRAPHQL_PUBLIC_PREFIX``):

    GET  …/graphql              — list projects with GraphQL available
    GET  …/graphql/{project}    — GraphiQL playground
    POST …/graphql/{project}    — execute a GraphQL query
"""

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional

from api.constants import EXTERNAL_GRAPHQL_PUBLIC_PREFIX
from shared.config.settings import get_settings, Settings
from back.objects.session import SessionManager, get_session_manager, get_project
from back.core.graphql import DEFAULT_DEPTH, MAX_DEPTH
from back.core.logging import get_logger
from back.core.errors import ValidationError, NotFoundError, InfrastructureError
from back.objects.registry import RegistryService
from back.core.triplestore import get_triplestore
from shared.config.constants import DEFAULT_BASE_URI
from back.core.helpers import effective_graph_name

logger = get_logger(__name__)

router = APIRouter()


# ------------------------------------------------------------------
# Request / response models
# ------------------------------------------------------------------


class GraphQLRequest(BaseModel):
    query: str = Field(..., description="GraphQL query string")
    variables: Optional[Dict[str, Any]] = Field(None, description="Query variables")
    operationName: Optional[str] = Field(None, description="Operation name")
    depth: Optional[int] = Field(None, description="Relationship traversal depth (1–5, default 2)")


class GraphQLProjectInfo(BaseModel):
    name: str
    description: str = ""


class GraphQLProjectsResponse(BaseModel):
    success: bool
    projects: List[GraphQLProjectInfo] = []
    message: Optional[str] = None


# ------------------------------------------------------------------
# Helpers (reuse patterns from digitaltwin API)
# ------------------------------------------------------------------


def _load_project_from_registry(project_name, session_mgr, settings):
    """Load a project by name from the registry and return a ProjectSession."""
    project = get_project(session_mgr)

    svc = RegistryService.from_context(project, settings)
    if not svc.cfg.is_configured:
        raise ValidationError("Registry not configured")

    # If the user already has this registry folder open at a chosen version,
    # keep it. Otherwise GraphQL would call load_mcp_project_data(), which
    # picks the newest version with mcp_enabled=True — often an older v3 while
    # the user is on v4 — and would open the wrong LadybugDB file (.lbug) for
    # subsequent Digital Twin / data-quality calls.
    session_folder = (getattr(project, "project_folder", None) or "").strip()
    session_ver = (getattr(project, "current_version", None) or "").strip()
    if session_folder == project_name and session_ver:
        ok_session, data_session, msg_session = svc.read_version(
            project_name, session_ver
        )
        if ok_session:
            project.clear_generated_content()
            project.import_from_file(data_session, version=session_ver)
            project.project_folder = project_name
            project.save()
            logger.info(
                "GraphQL: using session project '%s' at version %s",
                project_name,
                session_ver,
            )
            _ensure_ladybug_synced(project, svc.uc)
            return project
        logger.warning(
            "GraphQL: cannot read session version %s for '%s' (%s) — "
            "falling back to MCP-enabled / latest version",
            session_ver,
            project_name,
            msg_session,
        )

    ok, data, version, err = svc.load_mcp_project_data(project_name)
    if not ok:
        if "not found" in err.lower() or "no versions" in err.lower():
            raise NotFoundError(err)
        raise InfrastructureError(err)

    project.clear_generated_content()
    project.import_from_file(data, version=version)
    project.project_folder = project_name
    project.save()

    logger.info(
        "GraphQL: loaded project '%s' version %s from registry",
        project_name, version,
    )

    _ensure_ladybug_synced(project, svc.uc)

    return project


def _ensure_ladybug_synced(project, uc_service):
    """Pull LadybugDB data from UC Volume to local disk if not already present."""
    try:
        from back.core.triplestore.ladybugdb import sync_from_volume

        uc_path = project.uc_project_path
        if not uc_path:
            logger.warning("GraphQL LadybugDB: no UC project path — skipping sync")
            return
        db_name = effective_graph_name(project)
        ok, msg = sync_from_volume(uc_service, uc_path, db_name)
        if ok:
            logger.info("GraphQL LadybugDB: synced '%s' from volume", db_name)
        else:
            logger.warning("GraphQL LadybugDB: sync failed — %s", msg)
    except Exception as e:
        logger.warning("GraphQL LadybugDB: sync error — %s", e)


def _get_schema_and_context(project, settings):
    """Build (or retrieve cached) GraphQL schema and execution context."""
    from back.core.graphql import build_schema_for_project

    ontology = project.ontology or {}
    classes = ontology.get("classes", [])
    properties_list = ontology.get("properties", [])
    base_uri = ontology.get("base_uri", DEFAULT_BASE_URI)
    project_name = (project.info or {}).get("name", "")

    result = build_schema_for_project(classes, properties_list, base_uri, project_name)
    if not result:
        raise ValidationError("Could not generate GraphQL schema — ontology may be empty.")

    schema, metadata = result

    store = get_triplestore(project, settings, backend="graph")
    if not store:
        raise InfrastructureError("Graph backend (LadybugDB) not configured or unreachable.")

    table = effective_graph_name(project)

    logger.info(
        "GraphQL context: table=%s, store=%s, classes=%d",
        table, type(store).__name__, len(classes),
    )

    context = {
        "triplestore": store,
        "table_name": table,
        "base_uri": base_uri,
    }

    return schema, context


# ------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------


@router.get(
    "",
    response_model=GraphQLProjectsResponse,
    summary="List GraphQL-enabled projects",
    description="Returns projects that have both MCP/API enabled and a "
    "populated ontology — prerequisites for a GraphQL schema.",
)
async def graphql_list_projects(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    project = get_project(session_mgr)
    svc = RegistryService.from_context(project, settings)
    if not svc.cfg.is_configured:
        return GraphQLProjectsResponse(
            success=False, message="Registry not configured"
        )

    ok, items, msg = svc.list_mcp_projects(require_ontology=True)
    if not ok:
        return GraphQLProjectsResponse(success=False, message=msg)
    return GraphQLProjectsResponse(
        success=True,
        projects=[GraphQLProjectInfo(name=p["name"], description=p["description"]) for p in items],
    )


@router.get(
    "/settings/depth",
    summary="GraphQL depth settings",
    description="Returns the default and maximum relationship traversal depth.",
)
async def graphql_depth_settings():
    return JSONResponse(content={
        "default": DEFAULT_DEPTH,
        "max": MAX_DEPTH,
    })


@router.get(
    "/{project_name}",
    response_class=HTMLResponse,
    summary="GraphiQL playground",
    description="Interactive GraphQL playground for a project's knowledge graph.",
)
async def graphql_playground(
    request: Request,
    project_name: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    project = _load_project_from_registry(project_name, session_mgr, settings)
    _get_schema_and_context(project, settings)

    display_name = (project.info or {}).get("name", project_name)
    api_prefix = (
        EXTERNAL_GRAPHQL_PUBLIC_PREFIX
        if request.url.path.startswith(EXTERNAL_GRAPHQL_PUBLIC_PREFIX)
        else "/graphql"
    )
    return HTMLResponse(_graphiql_html(project_name, display_name, api_prefix))


@router.post(
    "/{project_name}",
    summary="Execute GraphQL query",
    description="Execute a GraphQL query against a project's knowledge graph. "
    "The schema is auto-generated from the project's ontology.",
)
async def graphql_execute(
    project_name: str,
    body: GraphQLRequest,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    project = _load_project_from_registry(project_name, session_mgr, settings)
    schema, context = _get_schema_and_context(project, settings)

    if body.depth is not None:
        context["depth"] = min(max(body.depth, 1), MAX_DEPTH)

    logger.debug(
        "GraphQL query for '%s' (depth=%s): %s",
        project_name, context.get("depth", DEFAULT_DEPTH), body.query[:200],
    )

    result = schema.execute_sync(
        body.query,
        variable_values=body.variables,
        operation_name=body.operationName,
        context_value=context,
    )

    response: Dict[str, Any] = {}
    if result.data is not None:
        response["data"] = result.data
    if result.errors:
        response["errors"] = [
            {"message": str(e), "path": getattr(e, "path", None)}
            for e in result.errors
        ]

    status = 200 if not result.errors else 400
    return JSONResponse(content=response, status_code=status)


@router.get(
    "/{project_name}/schema",
    summary="Introspect GraphQL schema (SDL)",
    description="Return the auto-generated GraphQL schema in SDL format.",
)
async def graphql_sdl(
    project_name: str,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    project = _load_project_from_registry(project_name, session_mgr, settings)
    schema, _ = _get_schema_and_context(project, settings)

    from strawberry.printer import print_schema

    sdl = print_schema(schema)
    return JSONResponse(content={"sdl": sdl})


@router.get(
    "/{project_name}/debug",
    summary="Debug predicate mapping",
    description="Compare triple-store predicates with GraphQL schema expectations. "
    "Also returns subject counts and optional search diagnostics.",
)
async def graphql_debug(
    project_name: str,
    type_name: str = Query(None, description="Type to inspect (e.g. Customer)"),
    search: str = Query(None, description="Test search string (e.g. Martinez)"),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    project = _load_project_from_registry(project_name, session_mgr, settings)
    schema, context = _get_schema_and_context(project, settings)

    from back.core.graphql import build_schema_for_project

    ontology = project.ontology or {}
    classes = ontology.get("classes", [])
    base_uri = ontology.get("base_uri", "")

    result = build_schema_for_project(
        classes, ontology.get("properties", []), base_uri,
        (project.info or {}).get("name", ""),
    )
    if not result:
        return JSONResponse({"error": "no schema"})

    _, metadata = result
    store = context["triplestore"]
    table = context["table_name"]

    debug_info: dict = {"_backend": "graph", "_table": table, "_store": type(store).__name__}

    for tname, tinfo in metadata.types.items():
        if type_name and tname.lower() != type_name.lower():
            continue
        try:
            store_preds = sorted(store.get_predicates_for_type(table, tinfo.cls_uri))
        except Exception as e:
            store_preds = [f"ERROR: {e}"]

        entry: dict = {
            "class_uri": tinfo.cls_uri,
            "schema_predicate_to_field": tinfo.predicate_to_field,
            "triplestore_predicates": store_preds,
        }

        try:
            all_uris = store.find_subjects_by_type(table, tinfo.cls_uri, limit=5, offset=0)
            entry["subject_count_sample"] = len(all_uris)
            entry["sample_subjects"] = all_uris[:3]
        except Exception as e:
            entry["subject_count_sample"] = f"ERROR: {e}"

        if search:
            try:
                found = store.find_subjects_by_type(table, tinfo.cls_uri, limit=5, offset=0, search=search)
                entry["search_results"] = found
            except Exception as e:
                entry["search_results"] = f"ERROR: {e}"

        debug_info[tname] = entry

    return JSONResponse(content=debug_info)


# ------------------------------------------------------------------
# GraphiQL HTML
# ------------------------------------------------------------------


def _graphiql_html(project_name: str, display_name: str, graphql_api_prefix: str) -> str:
    """*graphql_api_prefix* is ``/graphql`` or ``EXTERNAL_GRAPHQL_PUBLIC_PREFIX`` so GraphiQL POSTs to the same mount."""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>OntoBricks GraphQL — {display_name}</title>
  <link rel="icon" href="/static/global/img/favicon-32.png" />
  <link href="https://unpkg.com/graphiql@3/graphiql.min.css" rel="stylesheet" />
  <style>
    body {{ margin: 0; overflow: hidden; }}
    #graphiql {{ height: 100vh; }}
    .graphiql-container .topBar {{
      background: #1b3a4b;
    }}
    #depthBar {{
      position: fixed; top: 8px; right: 16px; z-index: 100;
      display: flex; align-items: center; gap: 6px;
      background: rgba(255,255,255,0.9); padding: 4px 10px;
      border-radius: 6px; font-size: 13px; box-shadow: 0 1px 4px rgba(0,0,0,0.15);
    }}
    #depthBar select {{ padding: 2px 6px; border-radius: 4px; border: 1px solid #ccc; }}
  </style>
</head>
<body>
  <div id="depthBar">
    <label for="depthSel">Depth</label>
    <select id="depthSel"></select>
  </div>
  <div id="graphiql"></div>
  <script crossorigin src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
  <script crossorigin src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>
  <script crossorigin src="https://unpkg.com/graphiql@3/graphiql.min.js"></script>
  <script>
    (async function() {{
      var dflt = {DEFAULT_DEPTH}, mx = {MAX_DEPTH};
      try {{
        var r = await fetch('{graphql_api_prefix}/settings/depth');
        if (r.ok) {{ var d = await r.json(); dflt = d.default || dflt; mx = d.max || mx; }}
      }} catch {{}}
      var sel = document.getElementById('depthSel');
      for (var i = 1; i <= mx; i++) {{
        var o = document.createElement('option');
        o.value = i; o.textContent = i;
        if (i === dflt) o.selected = true;
        sel.appendChild(o);
      }}
      function fetcher(params) {{
        var depth = parseInt(sel.value || dflt, 10);
        var body = Object.assign({{}}, params, {{ depth: depth }});
        return fetch('{graphql_api_prefix}/{project_name}', {{
          method: 'POST',
          headers: {{ 'Content-Type': 'application/json' }},
          credentials: 'same-origin',
          body: JSON.stringify(body),
        }}).then(function(r) {{ return r.json(); }});
      }}
      ReactDOM.createRoot(document.getElementById('graphiql')).render(
        React.createElement(GraphiQL, {{
          fetcher: fetcher,
          defaultEditorToolsVisibility: true,
        }})
      );
    }})();
  </script>
</body>
</html>"""
