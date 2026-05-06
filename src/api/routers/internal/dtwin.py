"""
Internal API -- Digital Twin / query JSON endpoints.

Moved from app/frontend/digitaltwin/routes.py during the front/back split.
"""

from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Request, Depends
from back.core.logging import get_logger
from back.core.errors import (
    InfrastructureError,
    NotFoundError,
    ValidationError,
)
from back.objects.registry import ROLE_BUILDER, require
from shared.config.constants import DEFAULT_BASE_URI, DEFAULT_GRAPH_NAME
from back.objects.session import SessionManager, get_session_manager, get_domain
from shared.config.settings import get_settings, Settings
from back.core.w3c import sparql, uri_local_name
from back.core.databricks import DatabricksClient, is_databricks_app
from back.core.triplestore import get_triplestore
from back.objects.digitaltwin import CohortService, DigitalTwin, DomainSnapshot
from back.core.helpers import (
    effective_graph_name,
    effective_view_table,
    get_databricks_client,
    get_databricks_credentials,
    make_volume_file_service,
    is_uri,
    run_blocking,
)

logger = get_logger(__name__)

router = APIRouter(prefix="/dtwin", tags=["Query"])


# ===========================================
# Query Execution
# ===========================================


@router.post("/execute")
async def execute_sparql(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Execute a SPARQL query via Spark SQL."""
    data = await request.json()
    query = data.get("query", "")
    limit = data.get("limit")

    if not query:
        raise ValidationError("No query provided")

    domain = get_domain(session_mgr)
    domain.ensure_generated_content()
    r2rml_content = domain.get_r2rml()

    if not r2rml_content:
        raise ValidationError(
            "No R2RML mapping available. Please configure ontology and mappings first."
        )

    return await DigitalTwin(domain).execute_spark_query(
        query, r2rml_content, limit, settings
    )


@router.post("/translate")
async def translate_sparql(
    request: Request, session_mgr: SessionManager = Depends(get_session_manager)
):
    """Translate a SPARQL query to SQL without executing."""
    data = await request.json()
    sparql_query = data.get("query", "")
    limit = data.get("limit")

    if not sparql_query:
        raise ValidationError("No SPARQL query provided")

    domain = get_domain(session_mgr)
    domain.ensure_generated_content()
    r2rml_content = domain.get_r2rml()

    if not r2rml_content:
        raise ValidationError(
            "No R2RML mapping available. Please configure mappings first."
        )

    entity_mappings, relationship_mappings = sparql.extract_r2rml_mappings(
        r2rml_content
    )
    base_uri = domain.ontology.get("base_uri", DEFAULT_BASE_URI)

    entity_mappings = DigitalTwin.augment_mappings_from_config(
        entity_mappings, domain.assignment, base_uri, domain.ontology
    )
    relationship_mappings = DigitalTwin.augment_relationships_from_config(
        relationship_mappings, domain.assignment, base_uri, domain.ontology
    )

    return sparql.translate_sparql_to_spark(
        sparql_query, entity_mappings, limit, relationship_mappings
    )


# ===========================================
# Groups (for graph expand/collapse)
# ===========================================


@router.get("/groups")
async def get_groups(session_mgr: SessionManager = Depends(get_session_manager)):
    """Return ontology entity groups for the Sigma graph expand/collapse feature.

    Each group contains the member class names so the frontend can build
    super-nodes for collapsed groups and restore member nodes on expand.
    """
    domain = get_domain(session_mgr)
    base_uri = domain.ontology.get("base_uri", DEFAULT_BASE_URI).rstrip("#") + "#"

    groups = []
    for g in domain.groups:
        members = g.get("members", [])
        member_uris = [
            m if m.startswith("http") else (base_uri + m) for m in members if m
        ]
        groups.append(
            {
                "name": g.get("name", ""),
                "label": g.get("label", g.get("name", "")),
                "color": g.get("color", ""),
                "icon": g.get("icon", ""),
                "members": members,
                "memberUris": member_uris,
            }
        )

    return {"success": True, "groups": groups}


# ===========================================
# Triple Store Sync
# ===========================================


@router.post(
    "/sync/start",
    dependencies=[Depends(require(ROLE_BUILDER, scope="domain"))],
)
async def start_triplestore_sync(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Start async dual digital twin build: CREATE VIEW (Triple-Store) then populate LadybugDB graph.

    Supports two modes controlled by the ``build_mode`` body parameter:

    * ``"incremental"`` (default) — version-gate check + server-side diff
      via a Delta snapshot table.  Falls back to full when no snapshot
      exists or when the diff exceeds a threshold.
    * ``"full"`` — drop and recreate the graph (legacy behaviour).
    """
    import threading
    from back.core.task_manager import get_task_manager

    data = await request.json()
    build_mode = data.get("build_mode", "incremental")
    force_full = build_mode == "full" or data.get("drop_existing", False)

    domain = get_domain(session_mgr)

    view_table = effective_view_table(domain)
    graph_name = effective_graph_name(domain)

    parts = view_table.split(".")
    if len(parts) != 3:
        raise ValidationError(
            "View location must be fully qualified: catalog.schema.view_name (configure in Domain / Triple Store tab)"
        )

    domain.ensure_generated_content()
    r2rml_content = domain.get_r2rml()

    if not r2rml_content:
        raise ValidationError(
            "No R2RML mapping available. Please ensure ontology and assignments are configured."
        )

    host, token, warehouse_id = get_databricks_credentials(domain, settings)
    if not host and not is_databricks_app():
        raise ValidationError("Databricks not configured")
    if not token and not is_databricks_app():
        raise ValidationError("Databricks not configured")
    if not warehouse_id:
        raise ValidationError("No SQL warehouse configured")

    config_changed = (
        (domain.last_update or "") > (domain.last_build or "")
        if domain.last_update
        else False
    )

    domain.triplestore.pop("stats", None)
    domain.triplestore.pop("_ts_cache_timestamp", None)
    if domain.last_update:
        domain.triplestore["build_last_update"] = domain.last_update

    from datetime import datetime, timezone as tz

    domain.last_build = datetime.now(tz.utc).isoformat()
    domain.save()

    base_uri = domain.ontology.get("base_uri", DEFAULT_BASE_URI)
    mapping_config = domain.assignment
    ontology_config = domain.ontology
    stored_source_versions = dict(domain.source_versions or {})
    delta_cfg = domain.delta or {}
    domain_snap = DomainSnapshot(domain)

    tm = get_task_manager()
    task = tm.create_task(
        name="Digital Twin Build",
        task_type="triplestore_sync",
        steps=[
            {
                "name": "prepare",
                "description": "Preparing mappings and generating queries",
            },
            {
                "name": "gate",
                "description": "Checking source tables for new data",
            },
            {
                "name": "view",
                "description": "Creating the Digital Twin view",
            },
            {
                "name": "diff",
                "description": "Detecting what changed since last build",
            },
            {
                "name": "graph",
                "description": "Updating the knowledge graph",
            },
            {
                "name": "snapshot",
                "description": "Saving a snapshot for next time",
            },
            {
                "name": "archive",
                "description": "Backing up the graph to the registry",
            },
        ],
    )

    def run_sync():
        DigitalTwin.run_build_task(
            tm,
            task.id,
            domain,
            settings,
            domain_snap,
            host,
            token,
            warehouse_id,
            view_table,
            graph_name,
            r2rml_content,
            base_uri,
            mapping_config,
            ontology_config,
            stored_source_versions,
            delta_cfg,
            force_full,
            config_changed=config_changed,
            snapshot_version=getattr(domain, "current_version", "1") or "1",
            build_kind="session",
        )

    thread = threading.Thread(target=run_sync, daemon=True)
    thread.start()

    return {"success": True, "task_id": task.id, "message": "Sync started"}


@router.post("/sync/load")
async def load_triplestore(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Load triples from the graph database and return them as query results."""
    try:
        domain = get_domain(session_mgr)
        graph_name = effective_graph_name(domain)

        store = get_triplestore(domain, settings, backend="graph")
        if not store:
            raise InfrastructureError("Graph backend is not configured")

        try:
            results = store.query_triples(graph_name)
        except (ValidationError, InfrastructureError, NotFoundError):
            raise
        except Exception as e:
            logger.exception("Load graph query failed: %s", e)
            error_msg = str(e)
            if "does not exist" in error_msg.lower():
                raise NotFoundError(
                    f"Graph {graph_name} does not exist. Run Build first.",
                    detail=error_msg,
                )
            raise InfrastructureError(
                "Error reading graph from the graph backend", detail=error_msg
            )

        return {
            "success": True,
            "results": results,
            "columns": ["subject", "predicate", "object"],
            "count": len(results),
        }

    except (ValidationError, InfrastructureError, NotFoundError):
        raise
    except Exception as e:
        logger.exception("Load graph failed: %s", e)
        raise InfrastructureError(
            "Error loading graph from the triple store", detail=str(e)
        )


# ===========================================
# Cluster Detection
# ===========================================


@router.post("/clusters/detect")
async def detect_clusters(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Run community detection on the full knowledge graph."""
    try:
        data = await request.json()
        algorithm = data.get("algorithm", "louvain")
        resolution = float(data.get("resolution", 1.0))
        predicate_filter = data.get("predicate_filter")
        class_filter = data.get("class_filter")
        max_triples = int(data.get("max_triples", 500_000))

        domain = get_domain(session_mgr)
        graph_name = effective_graph_name(domain)
        if not graph_name:
            raise ValidationError("Graph name is not configured")

        store = get_triplestore(domain, settings, backend="graph")
        if not store:
            raise InfrastructureError("Graph backend is not configured")

        dt = DigitalTwin(domain)
        result = await run_blocking(
            dt.detect_clusters,
            store,
            graph_name,
            algorithm=algorithm,
            resolution=resolution,
            predicate_filter=predicate_filter,
            class_filter=class_filter,
            max_triples=max_triples,
        )

        return {"success": True, **result}

    except (ValidationError, InfrastructureError, NotFoundError):
        raise
    except ValueError as e:
        logger.warning("Cluster detection rejected: %s", e)
        raise ValidationError("Cluster detection parameters are invalid", detail=str(e))
    except Exception as e:
        logger.exception("Cluster detection failed: %s", e)
        raise InfrastructureError("Cluster detection failed", detail=str(e))


# ===========================================
# Cohort Discovery
# ===========================================
#
# Routes resolve the cohort backend through a small Parameter Object
# (:class:`CohortEngineContext`) that bundles the saved-rule store,
# the graph backend, the resolved graph name, and a ready-to-use
# :class:`CohortService`. This keeps every engine route to a single
# call site and avoids 5 lines of boilerplate per handler.


@dataclass
class CohortEngineContext:
    """Pre-resolved dependencies for cohort engine routes.

    Carrying both ``service`` and the store/graph_name lets the route
    body remain a single :func:`run_blocking` call into the service
    method while preserving the original parameter shape (the engine
    is backend-agnostic and takes ``store`` + ``graph_name`` directly).
    """

    domain: Any
    settings: Settings
    store: Any
    graph_name: str
    service: CohortService


def cohort_engine_context(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
) -> CohortEngineContext:
    """Resolve the cohort engine context for the active domain.

    Raises :class:`ValidationError` when the graph name is not
    configured and :class:`InfrastructureError` when the graph
    backend cannot be instantiated.
    """
    domain = get_domain(session_mgr)
    graph_name = effective_graph_name(domain)
    if not graph_name:
        raise ValidationError("Graph name is not configured")
    store = get_triplestore(domain, settings, backend="graph")
    if not store:
        raise InfrastructureError("Graph backend is not configured")
    return CohortEngineContext(
        domain=domain,
        settings=settings,
        store=store,
        graph_name=graph_name,
        service=CohortService(domain),
    )


async def cohort_json_body(request: Request) -> dict:
    """Decode the request body as a JSON object.

    Centralises the ``"Body must be a JSON object"`` guard previously
    duplicated in every POST handler in this block.
    """
    data = await request.json()
    if not isinstance(data, dict):
        raise ValidationError("Body must be a JSON object")
    return data


@router.get("/cohorts/rules")
async def list_cohort_rules(
    session_mgr: SessionManager = Depends(get_session_manager),
):
    """Return all saved cohort rules for the active domain."""
    domain = get_domain(session_mgr)
    rules = CohortService(domain).list_rules()
    return {"success": True, "rules": rules, "count": len(rules)}


@router.post(
    "/cohorts/rules",
    dependencies=[Depends(require(ROLE_BUILDER, scope="domain"))],
)
async def upsert_cohort_rule(
    body: dict = Depends(cohort_json_body),
    session_mgr: SessionManager = Depends(get_session_manager),
):
    """Validate and upsert a cohort rule into the active domain."""
    domain = get_domain(session_mgr)
    rule = CohortService(domain).save_rule(body)
    return {"success": True, "rule": rule}


@router.delete(
    "/cohorts/rules/{rule_id}",
    dependencies=[Depends(require(ROLE_BUILDER, scope="domain"))],
)
async def delete_cohort_rule(
    rule_id: str,
    session_mgr: SessionManager = Depends(get_session_manager),
):
    """Delete a saved cohort rule by id."""
    domain = get_domain(session_mgr)
    deleted = CohortService(domain).delete_rule(rule_id)
    if not deleted:
        raise NotFoundError(f"Cohort rule '{rule_id}' was not found")
    return {"success": True, "rule_id": rule_id}


@router.post("/cohorts/dry-run")
async def cohort_dry_run(
    body: dict = Depends(cohort_json_body),
    ctx: CohortEngineContext = Depends(cohort_engine_context),
):
    """Run the cohort engine on a candidate rule without writing anything."""
    try:
        result = await run_blocking(
            ctx.service.dry_run, body, ctx.store, ctx.graph_name
        )
    except ValueError as exc:
        raise ValidationError("Cohort rule is invalid", detail=str(exc))
    return {"success": True, **result}


@router.post(
    "/cohorts/materialize",
    dependencies=[Depends(require(ROLE_BUILDER, scope="domain"))],
)
async def cohort_materialize(
    body: dict = Depends(cohort_json_body),
    ctx: CohortEngineContext = Depends(cohort_engine_context),
):
    """Re-run a saved rule and write outputs as configured (graph/UC table)."""
    rule_id = (body.get("rule_id") or "").strip()
    if not rule_id:
        raise ValidationError("Missing rule_id")
    client = get_databricks_client(ctx.domain, ctx.settings)
    domain_version = getattr(ctx.domain, "current_version", "1") or "1"

    def _label_resolver(uris):
        try:
            metadata = ctx.store.get_entity_metadata(ctx.graph_name, list(uris))
        except Exception:
            return {}
        return {row.get("uri", ""): row.get("label", "") for row in metadata or []}

    try:
        result = await run_blocking(
            ctx.service.materialize,
            rule_id,
            ctx.store,
            ctx.graph_name,
            client,
            domain_version,
            _label_resolver,
        )
    except NotFoundError:
        raise
    except ValueError as exc:
        raise ValidationError("Cohort rule is invalid", detail=str(exc))
    return {"success": True, **result}


@router.get("/cohorts/preview/class-stats")
async def cohort_class_stats(
    class_uri: str,
    ctx: CohortEngineContext = Depends(cohort_engine_context),
):
    """Live counter — instances of *class_uri* in the graph."""
    if not class_uri:
        raise ValidationError("Missing class_uri")
    out = await run_blocking(
        ctx.service.class_stats, class_uri, ctx.store, ctx.graph_name
    )
    return {"success": True, **out}


@router.post("/cohorts/preview/edge-count")
async def cohort_edge_count(
    body: dict = Depends(cohort_json_body),
    ctx: CohortEngineContext = Depends(cohort_engine_context),
):
    """Live counter — candidate edges produced by current ``links``."""
    out = await run_blocking(
        ctx.service.edge_count, body, ctx.store, ctx.graph_name
    )
    return {"success": True, **out}


@router.post("/cohorts/preview/node-count")
async def cohort_node_count(
    body: dict = Depends(cohort_json_body),
    ctx: CohortEngineContext = Depends(cohort_engine_context),
):
    """Live counter — surviving members after node-level compatibility."""
    out = await run_blocking(
        ctx.service.node_count, body, ctx.store, ctx.graph_name
    )
    return {"success": True, **out}


@router.post("/cohorts/preview/path-trace")
async def cohort_path_trace(
    body: dict = Depends(cohort_json_body),
    ctx: CohortEngineContext = Depends(cohort_engine_context),
):
    """Per-hop frontier diagnostic — see exactly which hop empties the
    walk for a multi-hop linkage rule.

    Body shape mirrors the rule's relevant slice::

        {"class_uri": "...", "links": [...], "compatibility": [...]}

    Returns the engine's trace (see :meth:`CohortBuilder.trace_paths`)
    used by the Preview tab's *Trace path* button.
    """
    out = await run_blocking(
        ctx.service.path_trace, body, ctx.store, ctx.graph_name
    )
    return {"success": True, **out}


@router.post("/cohorts/sample-values")
async def cohort_sample_values(
    body: dict = Depends(cohort_json_body),
    ctx: CohortEngineContext = Depends(cohort_engine_context),
):
    """Return up to N distinct values for a property/class pair (picker)."""
    class_uri = (body.get("class_uri") or "").strip()
    property_uri = (body.get("property") or "").strip()
    limit = int(body.get("limit", 20))
    if not class_uri or not property_uri:
        raise ValidationError("class_uri and property are required")
    out = await run_blocking(
        ctx.service.sample_values,
        class_uri,
        property_uri,
        ctx.store,
        ctx.graph_name,
        limit,
    )
    return {"success": True, **out}


@router.post("/cohorts/explain")
async def cohort_explain(
    body: dict = Depends(cohort_json_body),
    ctx: CohortEngineContext = Depends(cohort_engine_context),
):
    """Return a per-stage breakdown for a single member URI (Why? / Why not?)."""
    rule = body.get("rule", {})
    target = (body.get("target") or "").strip()
    if not target:
        raise ValidationError("Missing target URI")
    out = await run_blocking(
        ctx.service.explain, rule, target, ctx.store, ctx.graph_name
    )
    return {"success": True, **out}


@router.get("/cohorts/uc/suggest-target")
async def cohort_uc_suggest_target(
    rule_name: str = "",
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return suggested catalog/schema/table_name for the active domain.

    The optional ``rule_name`` query parameter scopes the suggested UC
    table name to the rule being configured -- the modal proposes
    ``cohorts_<snake_rule_name>`` so the table is self-describing.
    """
    domain = get_domain(session_mgr)
    out = CohortService(domain).suggest_uc_target(settings, rule_name)
    return {"success": True, **out}


@router.post("/cohorts/uc/probe-write")
async def cohort_uc_probe_write(
    body: dict = Depends(cohort_json_body),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Run a 3-step read-only permission probe for a UC Delta target."""
    domain = get_domain(session_mgr)
    client = get_databricks_client(domain, settings)
    if client is None:
        raise InfrastructureError("Databricks credentials not configured")
    out = await run_blocking(CohortService.probe_uc_write, body, client)
    return {"success": True, **out}


# ===========================================
# Cohort Discovery Agent (Stage 2 — NL → CohortRule)
# ===========================================


@router.post("/cohorts/agent")
async def cohorts_agent(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Translate a natural-language prompt into a validated CohortRule.

    Body::

        {
            "prompt":  "find consultants who can be staffed together",
            "history": [{"role": "user"|"assistant", "content": "..."}, ...]
        }

    Response::

        {
            "success": true,
            "rule":  { ... validated CohortRule JSON ...} | null,
            "reply": "...short markdown explanation...",
            "tools": [{"name": "list_classes", "duration_ms": 12}, ...],
            "iterations": 4,
            "usage": {"prompt_tokens": ..., "completion_tokens": ...}
        }

    The proposed rule is *not* persisted -- the user reviews and saves it
    via the existing ``POST /cohorts/rules`` route. If the agent could
    not assemble a valid rule, ``rule`` is ``null`` and ``reply`` carries
    the (likely clarifying) follow-up question.
    """
    import asyncio
    import os

    from agents.agent_cohort import run_agent as run_cohort_agent
    from api.routers.internal._helpers import map_route_errors
    from back.core.helpers import get_databricks_host_and_token

    data = await request.json()
    prompt = (data.get("prompt") or data.get("message") or "").strip()
    client_history = data.get("history") or []

    if not prompt:
        raise ValidationError("No prompt provided")

    domain = get_domain(session_mgr)

    host, token = get_databricks_host_and_token(domain, settings)
    if not host or not token:
        raise ValidationError("Databricks credentials not configured")

    llm_endpoint = (domain.info or {}).get("llm_endpoint", "") or ""
    if not llm_endpoint:
        llm_endpoint = _auto_discover_llm_endpoint(domain, settings)
        if llm_endpoint:
            logger.info(
                "CohortAgent: auto-selected LLM endpoint '%s' (no domain default)",
                llm_endpoint,
            )
    if not llm_endpoint:
        raise ValidationError(
            "No LLM serving endpoint available. Please set one in Domain Settings.",
        )

    reg = DigitalTwin.resolve_registry(session_mgr, settings)
    registry_params = {
        "registry_catalog": reg.get("catalog") or "",
        "registry_schema": reg.get("schema") or "",
        "registry_volume": reg.get("volume") or "",
    }

    app_port = os.environ.get("DATABRICKS_APP_PORT") or os.environ.get("PORT") or "8000"
    base_url = f"http://localhost:{app_port}"

    session_cookies = dict(request.cookies or {})
    _FORWARDED_HEADER_PREFIXES = ("x-forwarded-", "x-real-")
    _FORWARDED_EXTRA_HEADERS = {"x-csrf-token", "referer"}
    session_headers = {
        k: v
        for k, v in request.headers.items()
        if k.lower().startswith(_FORWARDED_HEADER_PREFIXES)
        or k.lower() in _FORWARDED_EXTRA_HEADERS
    }

    domain_name = _chat_resolve_domain_name(domain)

    logger.info(
        "CohortAgent: prompt=%s, domain=%s, endpoint=%s",
        prompt[:80],
        domain_name,
        llm_endpoint,
    )

    with map_route_errors("Cohort Discovery agent request failed", logger):
        agent_result = await asyncio.to_thread(
            run_cohort_agent,
            host=host,
            token=token,
            endpoint_name=llm_endpoint,
            base_url=base_url,
            domain_name=domain_name,
            registry_params=registry_params,
            session_cookies=session_cookies,
            session_headers=session_headers,
            user_message=prompt,
            conversation_history=client_history,
        )

    if not agent_result.success:
        raise InfrastructureError(
            "Cohort Discovery agent failed",
            detail=agent_result.error or None,
        )

    tool_calls = [
        {
            "name": step.tool_name,
            "duration_ms": step.duration_ms,
        }
        for step in agent_result.steps
        if step.step_type == "tool_result"
    ]

    return {
        "success": True,
        "rule": agent_result.proposed_rule,
        "reply": agent_result.reply,
        "tools": tool_calls,
        "iterations": agent_result.iterations,
        "usage": agent_result.usage,
    }


@router.post("/sync/filter")
async def filter_triplestore(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Query the triple store with filter criteria and return only matching triples.

    Supports two phases via the ``phase`` field:

    * ``"preview"`` (default) — run seed search only and return a flat list of
      matching entities with their type and label so the user can pick which
      ones to explore.
    * ``"expand"`` — accept ``selected_uris`` (list of subject URIs chosen by
      the user in the preview modal) and run the depth expansion + triple fetch.
    """
    try:
        data = await request.json()
        phase = data.get("phase", "preview")

        domain = get_domain(session_mgr)
        graph_name = effective_graph_name(domain)
        if not graph_name:
            raise ValidationError("Graph name is not configured")

        store = get_triplestore(domain, settings, backend="graph")
        if not store:
            raise InfrastructureError("Graph backend is not configured")

        # ── Phase 1: preview (seed search → flat list) ──────────────
        if phase == "preview":
            entity_type = (data.get("entity_type") or "").strip()
            field = data.get("field", "any")
            match_type = data.get("match_type", "contains")
            value = (data.get("value") or "").strip()

            if not entity_type and not value:
                raise ValidationError("Please specify an entity type or search value.")

            logger.info(
                "Filter preview – type=%s, field=%s, match=%s, value=%s",
                entity_type,
                field,
                match_type,
                value,
            )

            try:
                entity_set = store.find_seed_subjects(
                    graph_name,
                    entity_type=entity_type,
                    field=field,
                    match_type=match_type,
                    value=value,
                )
            except (ValidationError, InfrastructureError, NotFoundError):
                raise
            except Exception as e:
                logger.exception("Filter seed query failed: %s", e)
                msg = str(e)
                if "does not exist" in msg.lower():
                    raise NotFoundError(
                        f"Graph {graph_name} does not exist. Run Build first.",
                        detail=msg,
                    )
                raise InfrastructureError("Error querying graph", detail=msg)

            if not entity_set:
                return {
                    "success": True,
                    "phase": "preview",
                    "seeds": [],
                    "total": 0,
                    "capped": False,
                    "message": "No entities found matching the filter criteria.",
                }

            max_preview = 500
            capped = len(entity_set) > max_preview
            preview_uris = list(entity_set)[:max_preview]

            try:
                metadata = store.get_entity_metadata(graph_name, preview_uris)
            except (ValidationError, InfrastructureError, NotFoundError):
                raise
            except Exception as e:
                logger.exception("Entity metadata query failed: %s", e)
                raise InfrastructureError(
                    "Error fetching entity metadata", detail=str(e)
                )

            seeds = [
                {
                    "uri": m["uri"],
                    "type": uri_local_name(m["type"]) if m["type"] else "Unknown",
                    "type_uri": m["type"],
                    "label": m["label"] or uri_local_name(m["uri"]),
                }
                for m in metadata
            ]
            seeds.sort(key=lambda s: (s["type"], s["label"]))

            logger.info(
                "Filter preview – %d seeds returned (total=%d, capped=%s)",
                len(seeds),
                len(entity_set),
                capped,
            )

            return {
                "success": True,
                "phase": "preview",
                "seeds": seeds,
                "total": len(entity_set),
                "capped": capped,
            }

        # ── Phase 2: expand (selected URIs → full graph) ────────────
        selected_uris = data.get("selected_uris", [])
        if not selected_uris:
            raise ValidationError("No entities selected for expansion.")

        include_rels = data.get("include_rels", True)
        depth = min(int(data.get("depth", 3)), 5)
        client_max = int(data.get("max_entities", 5000))
        max_entities = max(100, min(client_max, 50_000))

        entity_set: set = set(selected_uris)
        initial_count = len(entity_set)
        capped = False

        logger.info(
            "Filter expand – %d selected URIs, depth=%d, max=%d",
            initial_count,
            depth,
            max_entities,
        )

        if include_rels and depth > 0:
            current_level = set(entity_set)
            for d in range(depth):
                if not current_level or len(entity_set) >= max_entities:
                    break
                logger.debug(
                    "Filter expand – level %d (%d entities so far)",
                    d + 1,
                    len(entity_set),
                )
                try:
                    neighbors = store.expand_entity_neighbors(graph_name, current_level)
                except Exception as e:
                    logger.warning("Expansion query at level %d failed: %s", d + 1, e)
                    break
                new_entities = neighbors - entity_set
                if not new_entities:
                    break
                remaining = max_entities - len(entity_set)
                if len(new_entities) > remaining:
                    new_entities = set(list(new_entities)[:remaining])
                    capped = True
                entity_set.update(new_entities)
                if capped:
                    break
                current_level = new_entities

        logger.info(
            "Filter expand – fetching triples for %d entities (%d seed + %d expanded, capped=%s)",
            len(entity_set),
            initial_count,
            len(entity_set) - initial_count,
            capped,
        )
        try:
            results = store.get_triples_for_subjects(graph_name, list(entity_set))
        except (ValidationError, InfrastructureError, NotFoundError):
            raise
        except Exception as e:
            logger.exception("Filter final query failed: %s", e)
            raise InfrastructureError(
                "Error fetching triples for the filter", detail=str(e)
            )

        max_triples = 100_000
        if len(results) > max_triples:
            results = results[:max_triples]
            capped = True

        return {
            "success": True,
            "phase": "expand",
            "results": results,
            "columns": ["subject", "predicate", "object"],
            "count": len(results),
            "initial_count": initial_count,
            "expanded_count": len(entity_set),
            "capped": capped,
        }

    except (ValidationError, InfrastructureError, NotFoundError):
        raise
    except Exception as e:
        logger.exception("Filter triplestore failed: %s", e)
        raise InfrastructureError("Error filtering the triple store", detail=str(e))


@router.get("/sync/changes")
async def triplestore_changes(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Check if ontology or assignments changed since the last build."""
    domain = get_domain(session_mgr)
    await run_blocking(DigitalTwin(domain).sync_last_build_from_schedule, settings)

    last_update = domain.last_update
    last_build = domain.last_build
    needs_rebuild = bool(last_update and last_build and last_update > last_build)
    return {"needs_rebuild": needs_rebuild}


@router.get("/sync/status")
async def triplestore_status(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
    refresh: bool = False,
):
    """Lightweight check: does the triple store table exist and contain data?

    Returns session-cached status when available; falls back to a live
    query and caches the result.  ``refresh`` is accepted for API
    compatibility and ignored.
    """
    _ = refresh  # query param kept for backward compatibility
    try:
        domain = get_domain(session_mgr)
        dt = DigitalTwin(domain)
        await run_blocking(dt.sync_last_build_from_schedule, settings)
        return await dt.get_or_fetch_graph_status(settings)
    except (ValidationError, InfrastructureError, NotFoundError):
        raise
    except Exception as e:
        logger.exception("Triplestore status failed: %s", e)
        raise InfrastructureError(
            "Could not retrieve triple store status", detail=str(e)
        )


# ===========================================
# Consolidated Information Endpoint
# ===========================================


@router.get("/sync/info")
async def sync_info(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return all data the Digital Twin Information page needs in one shot.

    Graph status and artefact existence are served from the session cache
    when available (populated after each successful build).  On a cache miss
    the values are fetched live from Databricks and then cached for the next
    request.
    """
    import asyncio
    import time as _t
    from back.objects.domain.HomeService import HomeService as home_service
    from back.objects.domain import Domain

    t0 = _t.monotonic()

    domain = get_domain(session_mgr)

    readiness = home_service.validate_status(domain)
    domain_info_data = Domain(domain).get_domain_info()

    last_update = domain.last_update
    last_build = domain.last_build
    needs_rebuild = bool(last_update and last_build and last_update > last_build)

    t_prep = _t.monotonic()

    dt = DigitalTwin(domain)

    async def _schedule_sync():
        t_s = _t.monotonic()
        await run_blocking(dt.sync_last_build_from_schedule, settings)
        logger.debug(
            "sync_info: _schedule_sync took %.0fms", (_t.monotonic() - t_s) * 1000
        )

    async def _graph_status():
        t_s = _t.monotonic()
        out = await dt.get_or_fetch_graph_status(settings)
        logger.debug(
            "sync_info: graph status took %.0fms", (_t.monotonic() - t_s) * 1000
        )
        return out

    async def _dt_exist():
        t_s = _t.monotonic()
        out = await dt.get_or_fetch_dt_existence(settings)
        logger.debug(
            "sync_info: dt existence took %.0fms", (_t.monotonic() - t_s) * 1000
        )
        return out

    _, ts_status, dt_exist = await asyncio.gather(
        _schedule_sync(),
        _graph_status(),
        _dt_exist(),
    )

    if domain.last_build and domain.last_build != last_build:
        last_build = domain.last_build
        needs_rebuild = (
            last_update > last_build if last_update and last_build else needs_rebuild
        )
        dt_exist["last_built"] = last_build

    logger.info(
        "sync_info: total=%.0fms (prep=%.0fms, parallel I/O=%.0fms)",
        (_t.monotonic() - t0) * 1000,
        (t_prep - t0) * 1000,
        (_t.monotonic() - t_prep) * 1000,
    )

    return {
        "readiness": readiness,
        "triplestore_status": ts_status,
        "domain_info": domain_info_data,
        "dt_existence": dt_exist,
        "changes": {"needs_rebuild": needs_rebuild},
    }


# ===========================================
# Digital Twin Existence Checks
# ===========================================


@router.get("/sync/dt-existence")
async def dt_existence(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Check existence of each Digital Twin artefact.

    Returns session-cached results when available; falls back to live
    Databricks checks and caches the result.
    """
    domain = get_domain(session_mgr)
    dt = DigitalTwin(domain)
    await run_blocking(dt.sync_last_build_from_schedule, settings)
    return await dt.get_or_fetch_dt_existence(settings)


@router.post(
    "/sync/reload-from-registry",
    dependencies=[Depends(require(ROLE_BUILDER, scope="domain"))],
)
async def reload_graph_from_registry(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Download the LadybugDB archive from the registry volume and extract it locally."""
    from back.objects.domain import Domain

    domain = get_domain(session_mgr)
    uc = make_volume_file_service(domain, settings)
    if not uc.is_configured():
        raise ValidationError("Databricks credentials are not configured")
    warning = Domain(domain).sync_ladybug_from_volume(uc)
    if warning:
        logger.warning("reload-from-registry failed: %s", warning)
        raise InfrastructureError(
            "Failed to reload graph from registry", detail=warning
        )

    logger.info("reload-from-registry succeeded for domain '%s'", domain.domain_folder)
    return {"success": True, "message": "Graph reloaded from registry"}


@router.post(
    "/sync/drop-snapshot",
    dependencies=[Depends(require(ROLE_BUILDER, scope="domain"))],
)
async def drop_snapshot(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Drop the incremental snapshot table to force a full rebuild on next build."""
    domain = get_domain(session_mgr)
    snapshot_table = domain.snapshot_table
    if not snapshot_table:
        raise ValidationError("No snapshot table is configured")

    host, token, warehouse_id = get_databricks_credentials(domain, settings)
    if not host or not warehouse_id:
        raise ValidationError("Databricks is not configured")

    from back.core.triplestore import IncrementalBuildService

    client = DatabricksClient(host=host, token=token, warehouse_id=warehouse_id)
    incr_svc = IncrementalBuildService(client)
    incr_svc.drop_snapshot(snapshot_table)

    domain.source_versions = {}
    domain.save()

    return {"success": True, "message": f"Snapshot {snapshot_table} dropped"}


# ===========================================
# Triple Store Insights
# ===========================================


@router.get("/sync/stats")
async def triplestore_stats(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
    refresh: bool = False,
):
    """Return content statistics about the triple store."""
    try:
        domain = get_domain(session_mgr)
        graph_name = effective_graph_name(domain)

        if not graph_name:
            raise ValidationError("Graph name is not configured")

        if not refresh:
            cached = DigitalTwin(domain).get_ts_cache("stats")
            if cached:
                preds = cached.get("top_predicates") or []
                has_kind = preds and "kind" in preds[0]
                if has_kind:
                    logger.debug("Returning cached graph stats")
                    return cached
                logger.debug("Stale stats cache (missing 'kind'); refreshing")

        store = get_triplestore(domain, settings, backend="graph")
        if not store:
            raise InfrastructureError("Graph backend is not configured")

        agg = store.get_aggregate_stats(graph_name)
        total_count = agg["total"]
        subject_count = agg["distinct_subjects"]
        predicate_count = agg["distinct_predicates"]
        label_count = agg["label_count"]

        entity_types = store.get_type_distribution(graph_name)
        top_predicates = store.get_predicate_distribution(graph_name)

        type_count = sum(int(r.get("cnt", 0)) for r in entity_types)
        relationship_count = total_count - type_count - label_count

        classified = DigitalTwin(domain).classify_predicates(top_predicates)

        result = {
            "success": True,
            "total_triples": total_count,
            "distinct_subjects": subject_count,
            "distinct_predicates": predicate_count,
            "entity_types": [
                {"uri": r["type_uri"], "count": int(r["cnt"])} for r in entity_types
            ],
            "top_predicates": classified,
            "label_count": label_count,
            "type_assertion_count": type_count,
            "relationship_count": max(relationship_count, 0),
        }
        DigitalTwin(domain).set_ts_cache("stats", result)
        return result
    except (ValidationError, InfrastructureError, NotFoundError):
        raise
    except Exception as e:
        logger.exception("Triplestore stats failed: %s", e)
        raise InfrastructureError(
            "Error retrieving triple store statistics", detail=str(e)
        )


# ===========================================
# Data Quality — SHACL-driven
# ===========================================


@router.post("/dataquality/execute")
async def execute_dataquality_check(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Execute a single SHACL shape check against the triple store."""
    try:
        data = await request.json()
        shape = data.get("shape", {})
        backend = data.get("backend", "view").strip()
        domain = get_domain(session_mgr)
        if backend == "graph":
            triplestore_table = effective_graph_name(domain).strip()
        else:
            triplestore_table = data.get("triplestore_table", "").strip()

        if not triplestore_table:
            raise ValidationError("Triple store table is not specified.")
        if not shape:
            raise ValidationError("No shape was provided.")

        from back.core.w3c import SHACLService

        store = get_triplestore(domain, settings, backend=backend)
        if not store:
            raise InfrastructureError(f"Could not initialize {backend} backend")

        if backend == "graph":
            graph_name = triplestore_table or effective_graph_name(domain)
            triples = await run_blocking(store.query_triples, graph_name)
            if not triples:
                raise ValidationError(f"Graph '{graph_name}' is empty. Build first.")
            violations = SHACLService.evaluate_shape_in_memory(shape, triples)
            return {
                "success": True,
                "violations": violations,
                "count": len(violations),
                "sql": "",
                "engine": "in-memory",
            }

        sql = SHACLService.shape_to_sql(shape, triplestore_table)
        if not sql:
            raise ValidationError(
                f"Cannot translate shape {shape.get('id', '?')} to SQL"
            )
        results = await run_blocking(store.execute_query, sql)
        return {
            "success": True,
            "violations": results or [],
            "count": len(results) if results else 0,
            "sql": sql,
        }
    except (ValidationError, InfrastructureError, NotFoundError):
        raise
    except Exception as e:
        logger.exception("SHACL quality check failed: %s", e)
        raise InfrastructureError("SHACL quality check failed", detail=str(e))


@router.post("/dataquality/start")
async def start_dataquality_checks(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Run all enabled SHACL shapes as an async quality-check task."""
    import threading
    from back.core.task_manager import get_task_manager

    data = await request.json()
    dimensions = data.get("dimensions") or []
    requested_backend = data.get("backend", "").strip() or "view"
    violation_limit = int(data.get("violation_limit", 10))
    if violation_limit <= 0:
        violation_limit = None

    domain = get_domain(session_mgr)
    if requested_backend == "graph":
        triplestore_table = effective_graph_name(domain).strip()
    else:
        triplestore_table = data.get("triplestore_table", "").strip()

    if not triplestore_table:
        raise ValidationError("Triple store table is not specified.")
    shapes = domain.shacl_shapes
    if dimensions:
        shapes = [s for s in shapes if s.get("category") in dimensions]
    shapes = [s for s in shapes if s.get("enabled", True)]

    swrl_rules = domain.swrl_rules or []
    ontology_dict = getattr(domain, "ontology", None)
    if not isinstance(ontology_dict, dict):
        ontology_dict = (
            domain._data.get("ontology", {}) if hasattr(domain, "_data") else {}
        )
    decision_tables = [
        dt for dt in ontology_dict.get("decision_tables", []) if dt.get("enabled", True)
    ]
    aggregate_rules = [
        r for r in ontology_dict.get("aggregate_rules", []) if r.get("enabled", True)
    ]

    if not shapes and not swrl_rules and not decision_tables and not aggregate_rules:
        raise ValidationError(
            "No enabled shapes, SWRL rules, decision tables or aggregate rules to check."
        )

    total = len(shapes) + len(swrl_rules) + len(decision_tables) + len(aggregate_rules)
    domain_snap = DomainSnapshot(domain)
    tm = get_task_manager()
    task = tm.create_task(
        name="Data Quality Checks",
        task_type="dataquality_checks",
        steps=[{"name": "running", "description": f"Running {total} quality checks"}],
    )

    def run_checks():
        DigitalTwin.run_data_quality_task(
            tm,
            task.id,
            settings,
            domain_snap,
            shapes,
            triplestore_table,
            requested_backend,
            total,
            swrl_rules=swrl_rules,
            ontology_dict=ontology_dict,
            decision_tables=decision_tables,
            aggregate_rules=aggregate_rules,
            violation_limit=violation_limit,
        )

    thread = threading.Thread(target=run_checks, daemon=True)
    thread.start()
    return {
        "success": True,
        "task_id": task.id,
        "message": f"Data quality checks started ({total} checks)",
    }


# ===========================================
# Legacy Quality Routes (kept for backward-compat)
# ===========================================


@router.post("/quality/execute", deprecated=True)
async def execute_quality_check(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Execute a quality check against the triple store table.

    .. deprecated::
        Use ``/dataquality/*`` endpoints instead (SHACL-driven data quality).
    """
    logger.warning(
        "Deprecated endpoint %s — use /dataquality/* instead", request.url.path
    )
    try:
        data = await request.json()
        check_type = data.get("check_type", "")
        triplestore_table = data.get("triplestore_table", "").strip()
        params = data.get("params", {})

        if not triplestore_table:
            raise ValidationError(
                "Triple store table is not specified. Configure it in Domain Settings."
            )
        if not check_type:
            raise ValidationError("No check type was specified")

        sql = DigitalTwin.build_quality_sql(check_type, triplestore_table, params)
        if not sql:
            raise ValidationError(f"Unsupported check type: {check_type}")

        domain = get_domain(session_mgr)
        store = get_triplestore(domain, settings, backend="view")
        if not store:
            raise InfrastructureError(
                "View backend is not configured (check Databricks connection)"
            )

        try:
            results = await run_blocking(store.execute_query, sql)
        except (ValidationError, InfrastructureError, NotFoundError):
            raise
        except Exception as e:
            logger.exception("Quality check query execution failed: %s", e)
            error_msg = str(e)
            if (
                "TABLE_OR_VIEW_NOT_FOUND" in error_msg
                or "does not exist" in error_msg.lower()
            ):
                raise NotFoundError(
                    f"View {triplestore_table} does not exist. Please build first.",
                    detail=error_msg,
                )
            raise InfrastructureError(
                "Quality check query execution failed", detail=error_msg
            )

        return {
            "success": True,
            "violations": results or [],
            "count": len(results) if results else 0,
            "sql": sql,
        }

    except (ValidationError, InfrastructureError, NotFoundError):
        raise
    except Exception as e:
        logger.exception("Execute quality check failed: %s", e)
        raise InfrastructureError("Error executing quality check", detail=str(e))


@router.post("/quality/start", deprecated=True)
async def start_quality_checks(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Start all quality checks as an asynchronous task.

    .. deprecated::
        Use ``/dataquality/*`` endpoints instead (SHACL-driven data quality).
    """
    logger.warning(
        "Deprecated endpoint %s — use /dataquality/* instead", request.url.path
    )
    import threading
    from back.core.task_manager import get_task_manager

    data = await request.json()
    triplestore_table = data.get("triplestore_table", "").strip()
    checks = data.get("checks", [])

    if not triplestore_table:
        raise ValidationError("Triple store table is not specified.")
    if not checks:
        raise ValidationError("No quality checks to run.")

    total_checks = len(checks)
    domain = get_domain(session_mgr)
    domain_snap = DomainSnapshot(domain)

    tm = get_task_manager()
    task = tm.create_task(
        name="Quality Checks",
        task_type="quality_checks",
        steps=[
            {"name": "running", "description": f"Running {total_checks} quality checks"}
        ],
    )

    def run_checks():
        import time

        start_time = time.time()

        try:
            tm.start_task(task.id, f"Running {total_checks} quality checks...")

            from back.core.triplestore import get_triplestore as _get_ts

            store = _get_ts(domain_snap, settings, backend="view")
            if not store:
                tm.fail_task(
                    task.id,
                    "Could not initialize view backend (check Databricks connection)",
                )
                return

            pop_cache = {}
            results = []
            for idx, check in enumerate(checks):
                check_type = check.get("check_type", "")
                params = check.get("params", {})
                name = check.get("name", f"Check {idx + 1}")
                category = check.get("category", "unknown")

                progress = int(((idx) / total_checks) * 100)
                tm.update_progress(
                    task.id, progress, f"Running check {idx + 1}/{total_checks}: {name}"
                )

                sql = DigitalTwin.build_quality_sql(
                    check_type, triplestore_table, params
                )
                if not sql:
                    results.append(
                        {
                            "name": name,
                            "category": category,
                            "status": "info",
                            "message": f"Unsupported check type: {check_type}",
                            "violations": [],
                            "sql": "",
                        }
                    )
                    continue

                class_uri = params.get("class_uri", "")

                try:
                    violations = store.execute_query(sql) or []
                    if len(violations) > 0:
                        result = {
                            "name": name,
                            "category": category,
                            "status": "error",
                            "message": check.get("error_message", "").replace(
                                "{count}", str(len(violations))
                            )
                            or f"{len(violations)} violations found",
                            "violations": violations,
                            "sql": sql,
                        }
                    else:
                        result = {
                            "name": name,
                            "category": category,
                            "status": "success",
                            "message": check.get(
                                "success_message", "No violations found"
                            ),
                            "violations": [],
                            "sql": sql,
                        }
                    pop = DigitalTwin._count_class_population_sql(
                        store, triplestore_table, class_uri, pop_cache
                    )
                    DigitalTwin._enrich_with_population(result, pop)
                    results.append(result)
                except Exception as e:
                    error_msg = str(e)
                    if (
                        "TABLE_OR_VIEW_NOT_FOUND" in error_msg
                        or "does not exist" in error_msg.lower()
                    ):
                        tm.fail_task(
                            task.id,
                            f"View {triplestore_table} does not exist. Please build first.",
                        )
                        return
                    results.append(
                        {
                            "name": name,
                            "category": category,
                            "status": "warning",
                            "message": "Could not validate: the data source returned an error.",
                            "violations": [],
                            "sql": sql,
                        }
                    )

            DigitalTwin.complete_dq_task(tm, task, results, time.time() - start_time)

        except Exception as e:
            logger.exception("Quality checks failed: %s", e)
            tm.fail_task(task.id, "Quality checks failed")

    thread = threading.Thread(target=run_checks, daemon=True)
    thread.start()

    return {
        "success": True,
        "task_id": task.id,
        "message": f"Quality checks started ({total_checks} checks)",
    }


# ===========================================
# Inference
# ===========================================


@router.post("/reasoning/start")
async def start_reasoning(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Start all inference phases as an asynchronous task."""
    import threading
    from back.core.task_manager import get_task_manager

    data = await request.json()
    options = {
        "tbox": data.get("tbox", True),
        "swrl": data.get("swrl", True),
        "graph": data.get("graph", True),
        "decision_tables": data.get("decision_tables", False),
        "sparql_rules": data.get("sparql_rules", False),
        "aggregate_rules": data.get("aggregate_rules", False),
    }

    domain = get_domain(session_mgr)
    domain.ensure_generated_content()
    domain_snap = DomainSnapshot(domain)

    tm = get_task_manager()
    task = tm.create_task(
        name="Inference",
        task_type="reasoning",
        steps=[{"name": "running", "description": "Running inference phases"}],
    )

    def run_reasoning():
        DigitalTwin.run_inference_task(
            tm,
            task.id,
            settings,
            domain_snap,
            options,
            build_kind="session",
        )

    thread = threading.Thread(target=run_reasoning, daemon=True)
    thread.start()

    return {"success": True, "task_id": task.id, "message": "Inference started"}


@router.post("/reasoning/materialize")
async def materialize_inferred(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Materialise previously inferred triples to Delta and/or LadybugDB."""
    from back.core.task_manager import get_task_manager
    from back.core.reasoning import InferredTriple, ReasoningResult, ReasoningService

    data = await request.json()
    task_id = data.get("task_id", "")
    do_delta = data.get("materialize_delta", False)
    do_graph = data.get("materialize_graph", False)
    mat_table = (data.get("materialize_table") or "").strip()

    if not task_id:
        raise ValidationError("Missing task_id")
    if not do_delta and not do_graph:
        raise ValidationError("Select at least one materialisation target")

    tm = get_task_manager()
    task = tm.get_task(task_id)
    if not task or not task.result:
        raise NotFoundError("Inference results were not found for this task")

    raw_triples = task.result.get("inferred_triples", [])
    if not raw_triples:
        raise ValidationError("There are no inferred triples to materialise")

    uri_triples = [
        t
        for t in raw_triples
        if is_uri(t.get("subject", ""))
        and is_uri(t.get("predicate", ""))
        and is_uri(t.get("object", ""))
    ]

    domain = get_domain(session_mgr)
    domain.ensure_generated_content()
    domain_snap = DomainSnapshot(domain)

    result = {}

    if do_delta and mat_table and len(mat_table.split(".")) == 3 and uri_triples:
        try:
            client = get_databricks_client(domain_snap, settings)
            if client is None:
                result["materialize_error"] = "Databricks credentials not configured"
            else:
                count = ReasoningService.materialize_to_delta(
                    client, mat_table, uri_triples
                )
                result["materialize_count"] = count
                result["materialize_table"] = mat_table
        except Exception as e:
            logger.exception("Materialise to Delta failed: %s", e)
            result["materialize_error"] = "Materialise to Delta failed"
            result["materialize_table"] = mat_table

    if do_graph and uri_triples:
        try:
            store = get_triplestore(domain_snap, settings, backend="graph")
            if store is None:
                result["materialize_graph_error"] = "Graph store not available"
            else:
                svc = ReasoningService(domain_snap, store)
                inferred = [
                    InferredTriple(
                        subject=t.get("subject", ""),
                        predicate=t.get("predicate", ""),
                        object=t.get("object", ""),
                        provenance=t.get("provenance", ""),
                    )
                    for t in uri_triples
                ]
                rr = ReasoningResult(inferred_triples=inferred)
                count = svc.materialize_inferred(rr)
                result["materialize_graph_count"] = count
        except Exception as e:
            logger.exception("Materialise to graph failed: %s", e)
            result["materialize_graph_error"] = "Materialise to graph failed"

    result["success"] = True
    return result


@router.get("/reasoning/inferred")
async def get_inferred_triples(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
):
    """Backward-compatible stub: reasoning results are not persisted in the session.

    Clients should use the completed task payload from ``/tasks/{task_id}``.
    """
    _ = get_domain(session_mgr)
    return {
        "success": True,
        "reasoning": {
            "last_run": None,
            "inferred_count": 0,
            "inferred_triples": [],
        },
    }


# ===========================================
# Graph Chat Assistant (LLM over the knowledge graph)
# ===========================================


# Session key for the Graph Chat cache (history + limit).
# Shape: {"limit": int, "history": {<domain_name>: [{"role", "content"}, ...]}}
_CHAT_SESSION_KEY = "graph_chat"
_CHAT_DEFAULT_LIMIT = 20         # number of user+assistant turns kept per domain
_CHAT_MIN_LIMIT = 5
_CHAT_MAX_LIMIT = 100


def _chat_cache(session_mgr: SessionManager) -> dict:
    """Return the Graph Chat session cache, creating an empty one if absent."""
    cache = session_mgr.get(_CHAT_SESSION_KEY)
    if not isinstance(cache, dict):
        cache = {"limit": _CHAT_DEFAULT_LIMIT, "history": {}}
    else:
        cache.setdefault("limit", _CHAT_DEFAULT_LIMIT)
        cache.setdefault("history", {})
    return cache


def _chat_save_cache(session_mgr: SessionManager, cache: dict) -> None:
    session_mgr.set(_CHAT_SESSION_KEY, cache)


def _chat_resolve_domain_name(domain) -> str:
    """Return the active domain's name, falling back through the
    common locations used by :class:`DomainSession` / session payloads.

    ``DomainSession`` does **not** expose a ``.name`` property; the name
    is stored under ``domain.info["name"]`` (and historically also under
    ``domain.domain["name"]`` / ``domain.domain_folder``).  Using a
    blind ``getattr(domain, "name", "")`` silently returns ``""`` and
    then the Graph Chat agent thinks no domain is selected.
    """
    if domain is None:
        return ""
    info = getattr(domain, "info", None) or {}
    name = (info.get("name") or "").strip() if isinstance(info, dict) else ""
    if name:
        return name
    d = getattr(domain, "domain", None) or {}
    if isinstance(d, dict):
        name = (d.get("name") or "").strip()
        if name:
            return name
    folder = getattr(domain, "domain_folder", "") or ""
    if isinstance(folder, str) and folder.strip():
        return folder.strip()
    return ""


def _chat_domain_key(domain) -> str:
    return _chat_resolve_domain_name(domain) or "__default__"


def _chat_clamp_limit(limit) -> int:
    try:
        value = int(limit)
    except (TypeError, ValueError):
        value = _CHAT_DEFAULT_LIMIT
    return max(_CHAT_MIN_LIMIT, min(_CHAT_MAX_LIMIT, value))


def _chat_trim(messages: list, limit: int) -> list:
    """Keep only the last ``limit`` turns (user + assistant messages).

    A turn is a user message optionally followed by an assistant reply,
    so we keep the last ``2 * limit`` items.
    """
    if limit <= 0 or not messages:
        return []
    keep = 2 * limit
    return messages[-keep:] if len(messages) > keep else list(messages)


def _auto_discover_llm_endpoint(domain, settings) -> str:
    """Best-effort auto-selection of a serving endpoint for Graph Chat.

    Walks the workspace's serving endpoints and returns the first one
    that looks ready to serve chat completions.  Preference order:

    1. Databricks hosted foundation models (pay-per-token), matched by
       the ``databricks-`` prefix (e.g. ``databricks-meta-llama-*``).
    2. Any other ``READY`` endpoint.

    Returns an empty string if nothing usable can be found.
    """
    try:
        from back.core.sqlwizard import SQLWizardService

        client = get_databricks_client(domain, settings)
        if not client:
            return ""
        endpoints = SQLWizardService(client).get_model_serving_endpoints() or []
    except Exception as exc:
        logger.debug("GraphChat: auto-discover LLM failed: %s", exc)
        return ""

    def _is_ready(ep: dict) -> bool:
        state = (ep.get("state") or "").upper()
        return state in ("READY", "TRUE", "UP")

    for ep in endpoints:
        name = ep.get("name") or ""
        if name.startswith("databricks-") and _is_ready(ep):
            return name
    for ep in endpoints:
        if _is_ready(ep) and ep.get("name"):
            return ep["name"]
    return ""


@router.post("/assistant/chat")
async def dtwin_assistant_chat(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Process a single chat turn with the Graph Chat agent.

    Expects JSON body::

        {
            "message": "List entity types",
            "history": [{"role": "user"|"assistant", "content": "..."}, ...]
        }

    Returns::

        {
            "success": true,
            "reply": "...markdown...",
            "tools": [{"name": "list_entity_types", "duration_ms": 123}, ...],
            "usage": {"prompt_tokens": ..., "completion_tokens": ..., ...}
        }
    """
    import asyncio
    import os

    from api.routers.internal._helpers import map_route_errors
    from back.core.helpers import get_databricks_host_and_token
    from agents.agent_dtwin_chat import run_agent as run_chat_agent

    data = await request.json()
    user_message = (data.get("message") or "").strip()
    client_history = data.get("history") or []

    if not user_message:
        raise ValidationError("No message provided")

    domain = get_domain(session_mgr)
    domain_key = _chat_domain_key(domain)
    chat_cache = _chat_cache(session_mgr)
    limit = _chat_clamp_limit(chat_cache.get("limit", _CHAT_DEFAULT_LIMIT))

    # Prefer the server-side persisted history (survives page navigation)
    # but fall back to whatever the client sent (legacy / cache miss).
    saved_history = chat_cache["history"].get(domain_key) or []
    history = saved_history if saved_history else client_history

    host, token = get_databricks_host_and_token(domain, settings)
    if not host or not token:
        raise ValidationError("Databricks credentials not configured")

    # Always use the domain-selected LLM when available. If the domain
    # hasn't pinned one yet, fall back to auto-discovering the first
    # READY serving endpoint in the workspace so Graph Chat never hard-
    # fails just because "llm_endpoint" wasn't saved in Domain Settings.
    llm_endpoint = (domain.info or {}).get("llm_endpoint", "") or ""
    if not llm_endpoint:
        llm_endpoint = _auto_discover_llm_endpoint(domain, settings)
        if llm_endpoint:
            logger.info(
                "GraphChat: auto-selected LLM endpoint '%s' (no domain default)",
                llm_endpoint,
            )
    if not llm_endpoint:
        raise ValidationError(
            "No LLM serving endpoint available. Please set one in Domain Settings.",
        )

    reg = DigitalTwin.resolve_registry(session_mgr, settings)
    registry_params = {
        "registry_catalog": reg.get("catalog") or "",
        "registry_schema": reg.get("schema") or "",
        "registry_volume": reg.get("volume") or "",
    }

    # Build the loopback base URL used by the agent's HTTPX client to
    # reach the external /api/v1/... and internal /dtwin/... routes
    # running in this same FastAPI process.  On Databricks Apps the port
    # is exposed as DATABRICKS_APP_PORT; locally it defaults to 8000.
    app_port = os.environ.get("DATABRICKS_APP_PORT") or os.environ.get("PORT") or "8000"
    base_url = f"http://localhost:{app_port}"

    # Forward the caller's session cookies so the loopback routes
    # resolve the same user session and active domain.
    session_cookies = dict(request.cookies or {})

    # Forward the Databricks-Apps identity + CSRF headers so the loopback
    # call passes PermissionMiddleware (which otherwise 302-redirects the
    # anonymous internal request to ``/access-denied``).
    _FORWARDED_HEADER_PREFIXES = ("x-forwarded-", "x-real-")
    _FORWARDED_EXTRA_HEADERS = {"x-csrf-token", "referer"}
    session_headers = {
        k: v
        for k, v in request.headers.items()
        if k.lower().startswith(_FORWARDED_HEADER_PREFIXES)
        or k.lower() in _FORWARDED_EXTRA_HEADERS
    }

    domain_name = _chat_resolve_domain_name(domain)

    logger.info(
        "GraphChat: user_message=%s, domain=%s, endpoint=%s",
        user_message[:80],
        domain_name,
        llm_endpoint,
    )

    with map_route_errors("Graph Chat agent request failed", logger):
        agent_result = await asyncio.to_thread(
            run_chat_agent,
            host=host,
            token=token,
            endpoint_name=llm_endpoint,
            base_url=base_url,
            domain_name=domain_name,
            registry_params=registry_params,
            session_cookies=session_cookies,
            session_headers=session_headers,
            user_message=user_message,
            conversation_history=history,
        )

    if not agent_result.success:
        raise InfrastructureError(
            "Graph Chat agent failed",
            detail=agent_result.error or None,
        )

    tool_calls = [
        {
            "name": step.tool_name,
            "duration_ms": step.duration_ms,
        }
        for step in agent_result.steps
        if step.step_type == "tool_result"
    ]

    # Persist the exchange in the session cache (per-domain, trimmed to
    # the configured limit) so the discussion survives page navigation.
    # ``history`` is expected to hold PRIOR turns only; drop a trailing
    # entry that accidentally echoes the current user_message so we
    # never double-record the same question (also self-heals any pre-
    # existing sessions that were written with the old contract).
    prior = list(history)
    if prior and prior[-1].get("role") == "user" and (
        prior[-1].get("content") or ""
    ).strip() == user_message.strip():
        prior = prior[:-1]
    prior.append({"role": "user", "content": user_message})
    prior.append({"role": "assistant", "content": agent_result.reply or ""})
    chat_cache["history"][domain_key] = _chat_trim(prior, limit)
    _chat_save_cache(session_mgr, chat_cache)

    return {
        "success": True,
        "reply": agent_result.reply,
        "tools": tool_calls,
        "iterations": agent_result.iterations,
        "usage": agent_result.usage,
    }


@router.get("/assistant/history")
async def dtwin_assistant_history_get(
    session_mgr: SessionManager = Depends(get_session_manager),
):
    """Return the persisted Graph Chat history for the active domain.

    Response shape::

        {
            "success": true,
            "domain": "<domain name>",
            "messages": [{"role": "user"|"assistant", "content": "..."}, ...],
            "limit": <int>,
            "min_limit": 5,
            "max_limit": 100
        }
    """
    domain = get_domain(session_mgr)
    domain_key = _chat_domain_key(domain)
    cache = _chat_cache(session_mgr)
    return {
        "success": True,
        "domain": getattr(domain, "name", "") or "",
        "messages": cache["history"].get(domain_key, []),
        "limit": _chat_clamp_limit(cache.get("limit", _CHAT_DEFAULT_LIMIT)),
        "min_limit": _CHAT_MIN_LIMIT,
        "max_limit": _CHAT_MAX_LIMIT,
        "default_limit": _CHAT_DEFAULT_LIMIT,
    }


@router.delete("/assistant/history")
async def dtwin_assistant_history_clear(
    session_mgr: SessionManager = Depends(get_session_manager),
):
    """Clear the persisted Graph Chat history for the active domain."""
    domain = get_domain(session_mgr)
    domain_key = _chat_domain_key(domain)
    cache = _chat_cache(session_mgr)
    if domain_key in cache["history"]:
        cache["history"].pop(domain_key, None)
        _chat_save_cache(session_mgr, cache)
    return {"success": True}


@router.get("/graphql/schema")
async def dtwin_graphql_schema(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return the GraphQL SDL built from the CURRENT session's domain.

    Unlike the public ``/graphql/{domain}/schema`` route, this endpoint
    does **not** require the domain to be published in the registry.
    It works purely from the in-session ontology so Graph Chat can
    introspect the schema even while the user is still building the
    domain.
    """
    from back.core.graphql import build_schema_for_domain
    from strawberry.printer import print_schema

    domain = get_domain(session_mgr)
    display_name = _chat_resolve_domain_name(domain)
    if not display_name:
        raise ValidationError("No domain selected in the current session.")

    ontology = domain.ontology or {}
    classes = ontology.get("classes", []) or []
    properties_list = ontology.get("properties", []) or []
    base_uri = ontology.get("base_uri", DEFAULT_BASE_URI)

    if not classes:
        raise ValidationError(
            "Ontology is empty — add at least one class to generate a GraphQL schema."
        )

    result = build_schema_for_domain(classes, properties_list, base_uri, display_name)
    if not result:
        raise ValidationError(
            "Could not generate GraphQL schema from the current ontology."
        )
    schema, _metadata = result
    return {
        "success": True,
        "domain": display_name,
        "sdl": print_schema(schema),
    }


@router.post("/graphql/execute")
async def dtwin_graphql_execute(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Execute a GraphQL query against the CURRENT session's domain.

    Session-aware counterpart of ``POST /graphql/{domain}`` used by the
    Graph Chat agent.  Requires a configured graph backend (LadybugDB)
    to resolve the query.
    """
    from back.core.graphql import build_schema_for_domain, DEFAULT_DEPTH, MAX_DEPTH
    from back.core.triplestore import get_triplestore
    from back.core.helpers import effective_graph_name

    domain = get_domain(session_mgr)
    display_name = _chat_resolve_domain_name(domain)
    if not display_name:
        raise ValidationError("No domain selected in the current session.")

    body = await request.json()
    query = (body.get("query") or "").strip()
    if not query:
        raise ValidationError("Missing 'query' in request body.")
    variables = body.get("variables") or None
    operation_name = body.get("operationName")
    depth = body.get("depth")

    ontology = domain.ontology or {}
    classes = ontology.get("classes", []) or []
    properties_list = ontology.get("properties", []) or []
    base_uri = ontology.get("base_uri", DEFAULT_BASE_URI)

    result = build_schema_for_domain(classes, properties_list, base_uri, display_name)
    if not result:
        raise ValidationError(
            "Could not generate GraphQL schema from the current ontology."
        )
    schema, _metadata = result

    store = get_triplestore(domain, settings, backend="graph")
    if not store:
        raise InfrastructureError(
            "Graph backend (LadybugDB) not configured or unreachable."
        )

    context = {
        "triplestore": store,
        "table_name": effective_graph_name(domain),
        "base_uri": base_uri,
    }
    if depth is not None:
        try:
            context["depth"] = min(max(int(depth), 1), MAX_DEPTH)
        except (TypeError, ValueError):
            context["depth"] = DEFAULT_DEPTH

    exec_result = schema.execute_sync(
        query,
        variable_values=variables,
        operation_name=operation_name,
        context_value=context,
    )

    response: dict = {"success": True, "domain": display_name}
    if exec_result.data is not None:
        response["data"] = exec_result.data
    if exec_result.errors:
        response["success"] = False
        response["errors"] = [
            {"message": str(e), "path": getattr(e, "path", None)}
            for e in exec_result.errors
        ]
    return response


@router.get("/triples/find")
async def dtwin_triples_find(
    entity_type: str | None = None,
    search: str | None = None,
    depth: int = 1,
    limit: int = 1000,
    offset: int = 0,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Session-aware search + BFS traversal over the in-session domain.

    Mirrors ``GET /api/v1/digitaltwin/triples/find`` but resolves the
    domain from the user's session instead of the registry, so the
    Graph Chat agent can introspect domains that have never been
    published as a version.
    """
    from back.core.helpers import sql_escape

    if not entity_type and not search:
        raise ValidationError("Provide at least entity_type or search")

    depth = max(1, min(int(depth or 1), 10))
    limit = max(1, min(int(limit or 1000), 10000))
    offset = max(0, int(offset or 0))

    domain = get_domain(session_mgr)
    table = effective_graph_name(domain)
    if not table:
        raise ValidationError("Graph name not configured")

    store = get_triplestore(domain, settings, backend="graph")
    if not store:
        raise ValidationError("Graph backend not configured")

    rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    rdfs_label = "http://www.w3.org/2000/01/rdf-schema#label"

    try:
        seed_conditions: list[str] = []
        if entity_type:
            esc = sql_escape(entity_type).lower()
            seed_conditions.append(
                f"subject IN (SELECT subject FROM {table} "
                f"WHERE predicate = '{rdf_type}' AND "
                f"(LOWER(object) LIKE '%#{esc}' OR LOWER(object) LIKE '%/{esc}'))"
            )
        if search:
            esc = sql_escape(search).lower()
            seed_conditions.append(
                f"(subject IN (SELECT subject FROM {table} "
                f"WHERE (predicate = '{rdfs_label}' "
                f"OR predicate LIKE '%#label' OR predicate LIKE '%/label' "
                f"OR predicate LIKE '%#name' OR predicate LIKE '%/name') "
                f"AND LOWER(object) LIKE '%{esc}%') "
                f"OR LOWER(subject) LIKE '%/{esc}%' "
                f"OR LOWER(subject) LIKE '%#{esc}%')"
            )

        seed_where = " WHERE " + " AND ".join(seed_conditions)

        bfs_rows = store.bfs_traversal(
            table,
            seed_where,
            depth,
            search=search or "",
            entity_type=entity_type or "",
        )

        if not bfs_rows:
            return {
                "success": True,
                "seed_count": 0,
                "depth": depth,
                "message": "No matching entities found",
                "triples": [],
                "count": 0,
                "total": 0,
                "limit": limit,
                "offset": offset,
                "entity_count": 0,
            }

        all_entities = {r["entity"] for r in bfs_rows}
        seed_count = sum(1 for r in bfs_rows if int(r.get("min_lvl", 0)) == 0)
        all_entities = DigitalTwin.expand_uri_aliases(store, table, all_entities)

        all_rows = store.get_triples_for_subjects(table, list(all_entities))
        seen: set = set()
        all_triples: list = []
        for r in all_rows:
            key = (r["subject"], r["predicate"], r["object"])
            if key not in seen:
                seen.add(key)
                all_triples.append(r)

        total = len(all_triples)
        page = all_triples[offset : offset + limit]

        return {
            "success": True,
            "seed_count": seed_count,
            "depth": depth,
            "triples": [
                {
                    "subject": r.get("subject", ""),
                    "predicate": r.get("predicate", ""),
                    "object": r.get("object", ""),
                }
                for r in page
            ],
            "count": len(page),
            "total": total,
            "limit": limit,
            "offset": offset,
            "entity_count": len(all_entities),
        }
    except (ValidationError, InfrastructureError, NotFoundError):
        raise
    except Exception as e:
        logger.exception("dtwin_triples_find failed: %s", e)
        raise InfrastructureError("Triple search failed", detail=str(e)) from e


@router.post("/assistant/history/limit")
async def dtwin_assistant_history_set_limit(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
):
    """Update the maximum number of turns kept per-domain (session-scoped).

    Body: ``{"limit": <int>}``.  Clamped to ``[5, 100]``.  When the new
    limit is smaller than the existing history, it is trimmed in place.
    """
    data = await request.json()
    new_limit = _chat_clamp_limit(data.get("limit"))
    cache = _chat_cache(session_mgr)
    cache["limit"] = new_limit
    cache["history"] = {
        dom: _chat_trim(msgs, new_limit) for dom, msgs in cache["history"].items()
    }
    _chat_save_cache(session_mgr, cache)
    return {"success": True, "limit": new_limit}
