"""
Internal API -- Knowledge Graph / query JSON endpoints.

Moved from app/frontend/digitaltwin/routes.py during the front/back split.
"""

import os
import secrets
import time
from typing import Any, Optional

from fastapi import APIRouter, Request, Depends, Query
from back.core.logging import get_logger
from back.core.errors import (
    InfrastructureError,
    NotFoundError,
    ValidationError,
)
from api.routers.internal._guards import require
from api.routers.internal._graph_access import assert_domain_graph_read
from back.objects.registry import ROLE_BUILDER
from shared.config.constants import DEFAULT_BASE_URI, DEFAULT_GRAPH_NAME
from back.objects.session import SessionManager, get_session_manager, get_domain
from shared.config.settings import get_settings, Settings
from back.core.w3c import sparql
from back.core.databricks import is_databricks_app
from back.core.graphdb import get_graphdb
from back.core.graph_analysis import (
    MODE_JOB,
    analytics_job_configured,
    analytics_job_status,
)
from back.objects.digitaltwin import (
    CohortEngineContext,
    CohortService,
    DigitalTwin,
    DomainSnapshot,
    GraphFilter,
    NodeBusinessRuleService,
    NodeContextService,
    TwinAssistantCache,
    TwinDataQualityRun,
    TwinGraphAccess,
    TwinGraphBuild,
    TwinGraphStats,
    TwinInferredMaterialize,
    TwinLakehouseBuild,
    TwinNeighborTriples,
    TwinOntologyGroups,
    TwinSparqlTranslate,
    VirtualAttributeService,
)
from back.objects.domain import HomeService, Domain
from api.routers.digitaltwin import NodeContextResponse
from back.core.helpers import (
    effective_databricks_table,
    effective_graph_name,
    effective_graph_query_table,
    effective_view_table,
    get_databricks_client,
    get_triplestore_sql_credentials,
    make_volume_file_service,
    require_domain_llm,
    run_blocking,
)

logger = get_logger(__name__)

router = APIRouter(prefix="/dtwin", tags=["Query"])


def _graph_query_table(
    domain,
    settings,
    store=None,
    *,
    include_inferred: bool = True,
) -> str:
    return TwinGraphAccess.graph_query_table(
        domain, settings, store, include_inferred=include_inferred
    )


def _dataquality_table(domain, settings) -> str:
    return TwinGraphAccess.dataquality_table(
        domain, settings, view_table_fn=effective_view_table
    )

# Canonical rdf:type predicate. Neighbour expansion must preserve type
# triples so the knowledge graph can group/colour expanded nodes by their
# declared entity type rather than their raw identifier (issue #52).
_RDF_TYPE_URI = TwinNeighborTriples.RDF_TYPE_URI


def _is_type_predicate(predicate: str) -> bool:
    return TwinNeighborTriples.is_type_predicate(predicate)


def _filter_neighbor_triples(
    rows: list[dict[str, str]],
    visited: set[str],
    limit: int,
) -> list[dict[str, str]]:
    return TwinNeighborTriples.filter_neighbor_triples(rows, visited, limit)


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
    assert_domain_graph_read(request)
    data = await request.json()
    query = sparql.require_read_only_sparql(data.get("query", ""))
    limit = data.get("limit")

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
    sparql_query = sparql.require_read_only_sparql(data.get("query", ""))
    limit = data.get("limit")

    domain = get_domain(session_mgr)
    return TwinSparqlTranslate.translate(domain, sparql_query, limit, DEFAULT_BASE_URI)


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
    return {
        "success": True,
        "groups": TwinOntologyGroups.list_groups(domain, DEFAULT_BASE_URI),
    }


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
    """Start async knowledge graph build: CREATE VIEW then populate the graph store.

    Always performs a full rebuild. When the graph engine is ``lakebase`` in
    ``managed_synced`` mode, the Lakeflow pipeline handles the data-plane
    refresh automatically.
    """
    import threading
    from back.core.task_manager import get_task_manager

    await request.json()  # consume body (drop_existing / build_mode kept for API compat)

    domain = get_domain(session_mgr)

    plan = TwinGraphBuild.prepare_session_sync(
        domain,
        settings,
        view_table_fn=effective_view_table,
        graph_name_fn=effective_graph_name,
        get_credentials=get_triplestore_sql_credentials,
        is_databricks_app_fn=is_databricks_app,
    )

    tm = get_task_manager()
    task = tm.create_task(
        name="Knowledge Graph Build",
        task_type="triplestore_sync",
        steps=[
            {"name": "prepare", "description": "Preparing mappings and generating queries"},
            {"name": "view", "description": "Creating the Knowledge Graph view"},
            *plan.graph_steps,
        ],
    )

    def run_sync():
        DigitalTwin.run_build_task(
            tm,
            task.id,
            domain,
            settings,
            plan.domain_snap,
            plan.host,
            plan.token,
            plan.warehouse_id,
            plan.view_table,
            plan.graph_name,
            plan.r2rml_content,
            plan.base_uri,
            plan.mapping_config,
            plan.ontology_config,
            plan.delta_cfg,
            build_kind="session",
        )

    thread = threading.Thread(target=run_sync, daemon=True)
    thread.start()

    return {"success": True, "task_id": task.id, "message": "Sync started"}


@router.post(
    "/adjacency/refresh",
    dependencies=[Depends(require(ROLE_BUILDER, scope="domain"))],
)
async def refresh_adjacency_only(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Start an adjacency-only refresh task for Lakebase/Lakehouse graphs."""
    import threading

    from back.core.graphdb.GraphDBFactory import GraphDBFactory
    from back.core.task_manager import get_task_manager

    domain = get_domain(session_mgr)
    backend = GraphDBFactory._resolve_graph_backend(domain)
    if backend in ("neo4j", "none"):
        raise ValidationError(
            "Adjacency refresh is only available for lakebase and databricks graph backends."
        )
    if backend not in ("lakebase", "databricks"):
        raise ValidationError(
            f"Adjacency refresh is not available for graph backend '{backend}'."
        )

    domain_snap = DomainSnapshot(domain)
    tm = get_task_manager()
    task = tm.create_task(
        name="Adjacency Refresh",
        task_type="adjacency_refresh",
        steps=[
            {"name": "open", "description": "Opening graph backend"},
            {"name": "adjacency", "description": "Rebuilding adjacency indexes"},
        ],
    )

    def run_refresh():
        DigitalTwin.run_adjacency_refresh_task(
            tm,
            task.id,
            settings,
            domain_snap,
            backend=backend,
        )

    thread = threading.Thread(target=run_refresh, daemon=True)
    thread.start()
    return {"success": True, "task_id": task.id, "message": "Adjacency refresh started"}


@router.post("/sync/load")
async def load_triplestore(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Load triples from the graph database and return them as query results."""
    try:
        try:
            body = await request.json()
        except Exception:
            body = {}
        include_inferred = body.get("include_inferred", True)

        domain = get_domain(session_mgr)
        store = _require_graph_store(domain, settings)
        query_table = _graph_query_table(
            domain, settings, store, include_inferred=include_inferred
        )

        try:
            # Blocking full-graph read — offload so it doesn't freeze the loop.
            results = await run_blocking(store.query_triples, query_table)
        except (ValidationError, InfrastructureError, NotFoundError):
            raise
        except Exception as e:
            logger.exception("Load graph query failed: %s", e)
            error_msg = str(e)
            if "does not exist" in error_msg.lower():
                raise NotFoundError(
                    f"Graph {query_table} does not exist. Run Build first.",
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
    assert_domain_graph_read(request)
    try:
        data = await request.json()
        algorithm = data.get("algorithm", "louvain")
        resolution = float(data.get("resolution", 1.0))
        predicate_filter = data.get("predicate_filter")
        class_filter = data.get("class_filter")
        max_triples = int(data.get("max_triples", settings.analytics_max_triples))

        domain = get_domain(session_mgr)
        store = _require_graph_store(domain, settings)
        graph_name = _graph_query_table(domain, settings, store)
        if not graph_name:
            raise ValidationError("Graph name is not configured")

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
# Graph Metrics
# ===========================================


def _load_stored_metrics(domain, settings) -> Optional[dict]:
    return TwinGraphAccess.load_stored_metrics(domain, settings)


@router.post("/metrics/compute")
async def compute_graph_metrics(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Start an asynchronous knowledge-graph metrics computation.

    The NetworkX analysis can take a while on large graphs, so it runs in
    a background :class:`TaskManager` thread. The result (only the LAST
    one) is persisted to the registry ``graph_analytics`` cache; clients
    poll ``/tasks/{task_id}`` and then read ``/dtwin/metrics/latest``.
    """
    import threading

    from back.core.task_manager import get_task_manager

    try:
        try:
            data = await request.json()
        except Exception:
            data = {}
        predicate_filter = data.get("predicate_filter")
        class_filter = data.get("class_filter")

        domain = get_domain(session_mgr)
        store = _require_graph_store(domain, settings)
        graph_name = _graph_query_table(domain, settings, store)
        if not graph_name:
            raise ValidationError("Graph name is not configured")

        # One compute path. When it cannot run, say why instead of quietly
        # returning a thinner metric set.
        job_available, blocked_reason = analytics_job_status(domain, settings)
        if not job_available:
            raise ValidationError(
                blocked_reason
                or (
                    "Graph analytics runs on Databricks, which is not enabled "
                    "for this workspace. Enable 'Compute large-graph metrics on "
                    "Databricks' in Settings."
                )
            )

        tm = get_task_manager()
        task = tm.create_task(
            name="Graph Analytics",
            task_type="graph_analytics",
            steps=[
                {"name": "compute", "description": "Computing graph metrics"},
                {"name": "store", "description": "Storing analytics result"},
            ],
        )

        def run_metrics():
            DigitalTwin.run_metrics_task(
                tm,
                task.id,
                domain,
                settings,
                graph_name,
                predicate_filter=predicate_filter,
                class_filter=class_filter,
                top_n=settings.analytics_top_n,
            )

        thread = threading.Thread(target=run_metrics, daemon=True)
        thread.start()

        return {
            "success": True,
            "task_id": task.id,
            "mode": MODE_JOB,
            "message": "Analysis started (running on Databricks)",
        }

    except (ValidationError, InfrastructureError, NotFoundError):
        raise
    except ValueError as e:
        logger.warning("Graph metrics rejected: %s", e)
        raise ValidationError("Graph metrics parameters are invalid", detail=str(e))
    except Exception as e:
        logger.exception("Graph metrics failed: %s", e)
        raise InfrastructureError("Graph metrics computation failed", detail=str(e))


@router.get("/metrics/latest")
async def get_latest_graph_metrics(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return the LAST persisted metrics result for the active domain/version.

    Reads the ``graph_analytics`` cache populated by the background
    compute task. ``{success, has_result: false}`` when no analysis has
    been run yet for this version.
    """
    try:
        domain = get_domain(session_mgr)
        stored = _load_stored_metrics(domain, settings)
        if not stored:
            return {"success": True, "has_result": False}

        # Rows stored before analytics became job-only carry mode="in_memory" or
        # "pushdown". Nothing branches on mode, so they still render.
        result = stored.get("result") or {}
        return {
            "success": True,
            "has_result": True,
            "computed_at": stored.get("computed_at", ""),
            "duration_ms": stored.get("duration_ms", 0),
            "class_filter": stored.get("class_filter") or [],
            "graph_name": stored.get("graph_name", ""),
            **result,
        }

    except (ValidationError, InfrastructureError, NotFoundError):
        raise
    except Exception as e:
        logger.exception("Loading latest graph metrics failed: %s", e)
        raise InfrastructureError("Loading latest graph metrics failed", detail=str(e))


@router.get("/metrics/series")
async def get_graph_metric_series(
    metric: str = Query(...),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return one exhaustive score series from the latest analytics run."""
    try:
        domain = get_domain(session_mgr)
        stored = _load_stored_metrics(domain, settings)
        if not stored:
            return {"success": True, "has_result": False}

        graph_name = stored.get("graph_name", "") or ""
        if not graph_name:
            return {"success": True, "has_result": False}

        page = await run_blocking(
            DigitalTwin(domain).load_graph_metric_series,
            graph_name,
            metric,
            settings,
        )
        return {
            "success": True,
            "has_result": True,
            "metric": metric,
            "computed_at": stored.get("computed_at", ""),
            **page,
        }

    except (ValidationError, InfrastructureError, NotFoundError):
        raise
    except Exception as e:
        logger.exception("Loading graph metric series failed: %s", e)
        raise InfrastructureError("Loading graph metric series failed", detail=str(e))


@router.get("/metrics/history")
async def get_graph_metrics_history(
    version: Optional[str] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return the analytics run history (newest-first) for this domain.

    Spans every version unless ``version`` scopes it. Backs the analytics
    table on Knowledge Graph → Management → Runs, which has no version
    filter. Guarding on a version here would report an empty history for a
    domain whose current version is blank, even with rows on file for
    earlier ones.
    """
    from back.objects.registry.RegistryService import RegistryService

    try:
        domain = get_domain(session_mgr)
        folder = getattr(domain, "uc_domain_folder", "") or ""
        if not folder:
            return {"success": True, "runs": []}

        svc = RegistryService.from_context(domain, settings)
        runs = svc.load_graph_analytics_runs(folder, version, limit=limit)
        return {"success": True, "runs": runs}

    except (ValidationError, InfrastructureError, NotFoundError):
        raise
    except Exception as e:
        logger.exception("Loading graph metrics history failed: %s", e)
        raise InfrastructureError("Loading graph metrics history failed", detail=str(e))


@router.get("/metrics/summary")
async def get_graph_metrics_summary(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return stored graph structure stats and top-PageRank nodes (cockpit card).

    Reads from the ``graph_analytics`` cache instead of recomputing, so
    opening the Domain Validation page no longer blocks on a full NetworkX
    run. ``{success, has_result: false}`` when no analysis has been run.
    """
    try:
        domain = get_domain(session_mgr)
        stored = _load_stored_metrics(domain, settings)
        if not stored:
            return {"success": True, "has_result": False}

        return {
            "success": True,
            "has_result": True,
            "stats": stored.get("stats") or {},
            "top_pagerank": stored.get("top_pagerank") or [],
            "computed_at": stored.get("computed_at", ""),
        }

    except (ValidationError, InfrastructureError, NotFoundError):
        raise
    except Exception as e:
        logger.exception("Graph metrics summary failed: %s", e)
        raise InfrastructureError("Graph metrics summary failed", detail=str(e))


@router.post("/metrics/interpret")
async def interpret_graph_metrics(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Run the graph-interpreter agent on the supplied metrics payload.

    Expects the JSON body produced by ``/dtwin/metrics/compute`` plus an
    optional ``class_filter`` list so the agent knows the entity type.
    The agent may call ``get_entity_details`` to look up specific entities
    before producing its structured insights.
    Returns ``{ success, sections: [{ title, body | items }] }``.
    """
    try:
        data = await request.json()
        domain = get_domain(session_mgr)

        host, token, llm_endpoint, _llm_endpoint_kind = require_domain_llm(
            domain, settings
        )

        # Build loopback base URL so the agent can call get_entity_details
        app_port = os.environ.get("DATABRICKS_APP_PORT") or os.environ.get("PORT") or "8000"
        base_url = f"http://localhost:{app_port}"
        session_cookies = dict(request.cookies or {})
        session_headers = {
            k: v
            for k, v in request.headers.items()
            if k.lower().startswith("x-forwarded-") or k.lower() == "x-csrf-token"
        }

        dt = DigitalTwin(domain)
        result = await run_blocking(
            dt.interpret_graph_metrics,
            data,
            host,
            token,
            llm_endpoint,
            base_url,
            session_cookies,
            session_headers,
        )
        return result

    except (ValidationError, InfrastructureError, NotFoundError):
        raise
    except Exception as e:
        logger.exception("Graph metrics interpretation failed: %s", e)
        raise InfrastructureError("Graph metrics interpretation failed", detail=str(e))


# ===========================================
# Cohort Discovery
# ===========================================
#
# Routes resolve the cohort backend through :class:`CohortEngineContext`.
# FastAPI ``Depends`` stays here; the Parameter Object lives in
# ``back.objects.digitaltwin``.


def cohort_engine_context(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
) -> CohortEngineContext:
    """Resolve the cohort engine context for the active domain."""
    return CohortEngineContext.from_domain(
        get_domain(session_mgr),
        settings,
        require_store=_require_graph_store,
        query_table=_graph_query_table,
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


def _require_graph_store(domain, settings):
    return TwinGraphAccess.require_graph_store(
        domain, settings, get_store=get_graphdb
    )


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
        except Exception as exc:
            logger.debug("Label resolver: entity metadata unavailable: %s", exc)
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
        include_inferred = data.get("include_inferred", True)

        domain = get_domain(session_mgr)
        store = _require_graph_store(domain, settings)
        from back.core.graphdb.search_cache import (
            apply_search_cache_to_store,
            parse_cache_param,
        )

        apply_search_cache_to_store(
            store, domain, parse_cache_param(data.get("cache"))
        )
        query_table = _graph_query_table(
            domain, settings, store, include_inferred=include_inferred
        )

        if phase == "preview":
            entity_type = (data.get("entity_type") or "").strip()
            field = data.get("field", "any")
            match_type = data.get("match_type", "contains")
            value = (data.get("value") or "").strip()
            if not entity_type and not value:
                raise ValidationError("Please specify an entity type or search value.")
            logger.info(
                "Filter preview – type=%s, field=%s, match=%s, value=%s",
                entity_type, field, match_type, value,
            )
            payload = await run_blocking(
                DigitalTwin.filter_preview,
                store, query_table, entity_type, field, match_type, value,
            )
        else:
            selected_uris = data.get("selected_uris", [])
            if not selected_uris:
                raise ValidationError("No entities selected for expansion.")
            include_rels = data.get("include_rels", True)
            max_depth_cap = 3 if is_databricks_app() else 5
            depth = min(int(data.get("depth", 3)), max_depth_cap)
            max_entities = GraphFilter.clamp_max_entities(data.get("max_entities"))
            batch_size = 250 if is_databricks_app() else 1000
            max_fetch_seconds = 40.0 if is_databricks_app() else 120.0
            payload = await run_blocking(
                DigitalTwin.filter_expand,
                store, query_table, selected_uris,
                include_rels, depth, max_entities,
                batch_size, 100_000, max_fetch_seconds,
            )

        return {"success": True, **payload}

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

    Returns session-cached status when available; falls back to a live query and
    caches the result. ``refresh=true`` bypasses the cache, which is the escape
    hatch the Build page's Refresh button needs when a stale entry disagrees with
    the live graph.
    """
    try:
        domain = get_domain(session_mgr)
        dt = DigitalTwin(domain)
        await run_blocking(dt.sync_last_build_from_schedule, settings)
        return await dt.get_or_fetch_graph_status(settings, force_refresh=refresh)
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
    """Return all data the Knowledge Graph Information page needs in one shot.

    Graph status and artefact existence are served from the session cache
    when available (populated after each successful build).  On a cache miss
    the values are fetched live from Databricks and then cached for the next
    request.
    """
    import asyncio
    import time as _t

    t0 = _t.monotonic()

    domain = get_domain(session_mgr)

    readiness = HomeService.validate_status(domain)
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

    # Graph status is served cache-first for the same reason as DT existence
    # below: a live probe is up to two full COUNT(*) scans over the union graph
    # view (~4s each on the serverless warehouse) and used to block this
    # endpoint — and therefore the whole KG page's first paint. On a cache miss
    # we now return a lightweight "pending" skeleton (no probe) and let the
    # frontend confirm the live count off the request path via
    # `/dtwin/sync/status` whenever `triplestore_status_pending` is set.
    cached_status = dt.get_ts_cache("status")
    triplestore_status_pending = cached_status is None

    def _pending_status() -> dict:
        return {
            "success": True,
            # Tri-state stays None (unknown) — the frontend renders a "checking"
            # spinner, not a "not built" badge, until the background probe lands.
            "has_data": None,
            "pending": True,
            "count": 0,
            "view_table": effective_view_table(domain),
            "graph_name": effective_graph_query_table(domain, settings),
            "reason": "Checking graph status…",
        }

    # DT existence is served cache-first so the Build page paints instantly.
    # The live probe is a cold SQL-warehouse / Lakebase wake-up that used to
    # block this endpoint for tens of seconds; it now runs off the request path.
    # The frontend confirms the live state with a non-blocking follow-up to
    # `/dtwin/sync/dt-existence` whenever `dt_existence_pending` is set.
    cached_existence = dt.get_ts_cache("dt_existence")
    dt_exist = cached_existence or dt.pending_dt_existence(settings)
    dt_existence_pending = True

    await _schedule_sync()
    ts_status = cached_status if cached_status is not None else _pending_status()

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
        "triplestore_status_pending": triplestore_status_pending,
        "domain_info": domain_info_data,
        "dt_existence": dt_exist,
        "dt_existence_pending": dt_existence_pending,
        "changes": {"needs_rebuild": needs_rebuild},
    }


# ===========================================
# Databricks Triple Store Build (Delta only)
# ===========================================


@router.get("/databricks-build/info")
async def databricks_build_info(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Readiness + ``…_data`` status for the Databricks triple-store build page.

    ``materialization`` tells the page whether that relation is a Delta table
    or a pass-through view, so it can label it and explain the triple count.
    """
    from back.core.graphdb.delta import _table_naming
    from back.core.graphdb.delta.health import probe_from_client
    from back.core.graphdb.delta.DeltaBase import create_databricks_client
    from back.core.graphdb.GraphDBFactory import GraphDBFactory

    domain = get_domain(session_mgr)
    backend = GraphDBFactory._resolve_triple_store_backend(domain, settings)
    readiness = HomeService.validate_status(domain)
    view_table = effective_view_table(domain)
    data_table = effective_databricks_table(domain, settings)
    client = create_databricks_client(domain, settings)
    data_status = probe_from_client(client, data_table) if data_table else {}
    return {
        "success": True,
        "triple_store_backend": backend,
        "materialization": GraphDBFactory.resolve_lakehouse_materialization(
            domain, settings
        ),
        "readiness": readiness,
        "view_table": view_table,
        "data_table": data_table,
        "inferred_table": _table_naming.inferred_table_fqn(domain, settings),
        "triplestore_status": data_status,
    }


@router.post(
    "/databricks-build/start",
    dependencies=[Depends(require(ROLE_BUILDER, scope="domain"))],
)
async def start_databricks_triplestore_build(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Materialize UC Delta triple store (VIEW → TABLE); no Lakebase sync."""
    import threading
    from back.core.task_manager import get_task_manager
    from back.core.graphdb.delta.DeltaTripleStoreBuildPipeline import (
        lakehouse_build_steps,
    )
    from back.objects.digitaltwin._databricks_triplestore_build import (
        run_databricks_triplestore_build,
    )

    await request.json()

    domain = get_domain(session_mgr)
    plan = TwinLakehouseBuild.prepare_session(
        domain,
        settings,
        view_table_fn=effective_view_table,
        data_table_fn=effective_databricks_table,
        get_credentials=get_triplestore_sql_credentials,
        is_databricks_app_fn=is_databricks_app,
    )

    tm = get_task_manager()
    task = tm.create_task(
        name="Databricks Triple Store Build",
        task_type="databricks_triplestore_build",
        steps=lakehouse_build_steps(plan.materialization),
    )

    def run_build():
        run_databricks_triplestore_build(
            tm,
            task.id,
            domain,
            settings,
            plan.domain_snap,
            plan.host,
            plan.token,
            plan.warehouse_id,
            plan.view_table,
            plan.data_table,
            plan.r2rml_content,
            plan.mapping_config,
            plan.ontology_config,
            plan.base_uri,
            build_kind="session",
        )

    threading.Thread(target=run_build, daemon=True).start()
    return {"success": True, "task_id": task.id}


# ===========================================
# Knowledge Graph Existence Checks
# ===========================================


@router.get("/sync/dt-existence")
async def dt_existence(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Check existence of each Knowledge Graph artefact.

    Always probes Databricks/Lakebase live so the result reflects the current
    state (the session cache can carry a stale ``False`` from a transient
    Postgres timeout).
    """
    domain = get_domain(session_mgr)
    dt = DigitalTwin(domain)
    await run_blocking(dt.sync_last_build_from_schedule, settings)
    return await dt.get_or_fetch_dt_existence(settings, force_refresh=True)


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
        store = _require_graph_store(domain, settings)
        graph_name = _graph_query_table(domain, settings, store)

        if not graph_name:
            raise ValidationError("Graph name is not configured")

        if not refresh:
            cached = DigitalTwin(domain).get_ts_cache("stats")
            if TwinGraphStats.cache_is_fresh(cached):
                logger.debug("Returning cached graph stats")
                return cached
            if cached:
                logger.debug("Stale stats cache; refreshing")

        store = _require_graph_store(domain, settings)

        # These are four independent blocking SQL round-trips to the graph
        # warehouse. Run them concurrently in the sized thread pool (rather than
        # inline and sequentially, which both froze the event loop and paid the
        # *sum* of the query times) so total latency is ~max(query) — a big win
        # on serverless Lakehouse/RT warehouses. CPU-only assembly (predicate
        # classification, the cheap analytics-job check) runs on the loop after.
        import asyncio

        agg, entity_types, top_predicates, inferred_count = await asyncio.gather(
            run_blocking(store.get_aggregate_stats, graph_name),
            run_blocking(store.get_type_distribution, graph_name),
            run_blocking(store.get_predicate_distribution, graph_name),
            run_blocking(store.get_inferred_triple_count, graph_name),
        )

        job_available, job_blocked_reason = analytics_job_configured(domain, settings)
        result = TwinGraphStats.assemble(
            domain,
            settings,
            agg=agg,
            entity_types=entity_types,
            top_predicates=top_predicates,
            inferred_count=inferred_count,
            job_available=job_available,
            job_blocked_reason=job_blocked_reason,
        )
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
    """Execute a single SHACL shape check against the triple-store VIEW."""
    assert_domain_graph_read(request)
    try:
        data = await request.json()
        shape = data.get("shape", {})
        domain = get_domain(session_mgr)
        triplestore_table = _dataquality_table(domain, settings)

        if not shape:
            raise ValidationError("No shape was provided.")

        from back.core.w3c import SHACLService

        store = get_graphdb(domain, settings, engine="view")
        if not store:
            raise InfrastructureError("Could not reach the SQL warehouse")

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
    domain = get_domain(session_mgr)
    run = TwinDataQualityRun.from_request(domain, data)
    triplestore_table = _dataquality_table(domain, settings)
    domain_snap = DomainSnapshot(domain)
    tm = get_task_manager()
    task = tm.create_task(
        name="Data Quality Checks",
        task_type="dataquality_checks",
        steps=[
            {
                "name": "running",
                "description": f"Running {run.total} quality checks",
            }
        ],
    )

    def run_checks():
        DigitalTwin.run_data_quality_task(
            tm,
            task.id,
            settings,
            domain_snap,
            run.shapes,
            triplestore_table,
            run.total,
            swrl_rules=run.swrl_rules,
            ontology_dict=run.ontology_dict,
            decision_tables=run.decision_tables,
            aggregate_rules=run.aggregate_rules,
            violation_limit=run.violation_limit,
        )

    thread = threading.Thread(target=run_checks, daemon=True)
    thread.start()
    return {
        "success": True,
        "task_id": task.id,
        "message": f"Data quality checks started ({run.total} checks)",
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
    # Per-rule name filters (optional; empty set = run all rules in that phase)
    for key in ("swrl_rule_names", "decision_table_names", "sparql_rule_names", "aggregate_rule_names"):
        names = data.get(key)
        if names:
            options[key] = set(names)

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
    """Materialise previously inferred triples to Delta and/or the active graph store."""
    from back.core.task_manager import get_task_manager

    data = await request.json()
    domain = get_domain(session_mgr)
    tm = get_task_manager()
    return TwinInferredMaterialize.run(
        domain,
        settings,
        task_id=data.get("task_id", ""),
        do_delta=data.get("materialize_delta", False),
        do_graph=data.get("materialize_graph", False),
        mat_table=(data.get("materialize_table") or "").strip(),
        get_task=tm.get_task,
        get_store=get_graphdb,
        get_client=get_databricks_client,
    )


@router.delete(
    "/reasoning/inferred",
    dependencies=[Depends(require(ROLE_BUILDER, scope="domain"))],
)
async def purge_materialized_inferences(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Purge generated graph triples without modifying mapped source data."""
    domain = get_domain(session_mgr)
    store = _require_graph_store(domain, settings)
    graph_name = effective_graph_name(domain)
    try:
        purged_count = await run_blocking(
            store.purge_materialized_triples,
            graph_name,
        )
    except NotImplementedError as exc:
        raise InfrastructureError(
            "The active graph backend cannot safely purge materialized inferences",
            detail=str(exc),
        ) from exc
    return {
        "success": True,
        "graph_name": graph_name,
        "purged_count": purged_count,
    }


@router.get("/reasoning/inferred")
async def get_inferred_triples(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return live materialized-inference status without listing triples."""
    assert_domain_graph_read(session_mgr.request)
    domain = get_domain(session_mgr)
    store = _require_graph_store(domain, settings)
    graph_name = effective_graph_name(domain)
    supported = bool(store.supports_materialized_inference_purge)
    inferred_count = (
        await run_blocking(store.get_inferred_triple_count, graph_name)
        if supported
        else None
    )
    return {
        "success": True,
        "graph_name": graph_name,
        "materialized_inference_count": inferred_count,
        "purge_supported": supported,
        "reasoning": {
            "last_run": None,
            "inferred_count": inferred_count,
            "inferred_triples": [],
        },
    }


# ===========================================
# Graph Chat Assistant (LLM over the knowledge graph)
# ===========================================


@router.get("/classes")
async def dtwin_classes(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return session-domain classes and Graph Chat action metadata."""
    domain = get_domain(session_mgr)
    swrl_rules = NodeBusinessRuleService.domain_swrl_rules(domain)
    return {
        "success": True,
        "domain_name": _chat_resolve_domain_name(domain),
        "classes": [
            {
                "name": cls.get("name", ""),
                "uri": cls.get("uri", ""),
                "dataset": cls.get("dataset") or None,
                "bridges": NodeContextService.enrich_bridge_targets(
                    NodeContextService.class_bridge_entries(cls),
                    session_mgr=session_mgr,
                    settings=settings,
                    drop_unavailable=False,
                ),
                "actions": NodeContextService.class_action_entries(cls),
                "business_rules": NodeBusinessRuleService.class_entries(cls, swrl_rules),
                "virtualAttributes": VirtualAttributeService.class_entries(cls),
            }
            for cls in (domain.get_classes() or [])
        ],
    }


@router.post("/nodes/action/request")
async def dtwin_nodes_action_request(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
):
    """Validate an entity + allow-listed action and mint a one-time pending token.

    Does **not** invoke the Unity Catalog function — this only checks that
    the entity resolves to an ontology class and that the action is declared
    on that class, then stores a pending entry in the session cache. Call
    ``POST /dtwin/nodes/action/confirm`` with the returned token to execute.
    """
    data = await request.json()
    entity_uri = (data.get("entity_uri") or "").strip()
    action_full_name = (data.get("action_full_name") or "").strip()
    if not entity_uri or not action_full_name:
        raise ValidationError("entity_uri and action_full_name are required")

    domain = get_domain(session_mgr)
    raw_classes = domain.get_classes() or []
    matched_cls = NodeContextService.match_ontology_class(entity_uri, raw_classes)
    if matched_cls is None:
        raise ValidationError("No ontology class matches this entity URI")

    class_name = matched_cls.get("name", "")
    action = next(
        (
            a
            for a in NodeContextService.class_action_entries(matched_cls)
            if a["fullName"] == action_full_name
        ),
        None,
    )
    if action is None:
        raise ValidationError(
            f"Action {action_full_name!r} is not configured on class {class_name!r}"
        )

    domain_key = _chat_domain_key(domain)
    cache = _chat_cache(session_mgr)
    _pending_actions_prune(cache)

    token = secrets.token_urlsafe(24)
    cache["pending_actions"][token] = {
        "domain": domain_key,
        "entity_uri": entity_uri,
        "action_full_name": action["fullName"],
        "expires_at": time.time() + _PENDING_ACTION_TTL_SEC,
        "used": False,
    }
    _chat_save_cache(session_mgr, cache)

    entity_label = DigitalTwin.extract_local_id(entity_uri)

    logger.info(
        "nodes/action/request: minted pending token for entity=%s action=%s domain=%s",
        entity_label,
        action["fullName"],
        domain_key,
    )

    return {
        "success": True,
        "pending_action": {
            "token": token,
            "entity_uri": entity_uri,
            "entity_label": entity_label,
            "action": action["fullName"],
            "description": action.get("description"),
            "expires_in_sec": _PENDING_ACTION_TTL_SEC,
        },
        "message": f"Confirm to run {action['fullName']} on {entity_label}.",
    }


@router.post("/nodes/action/confirm")
async def dtwin_nodes_action_confirm(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Consume a pending-action token and invoke the Unity Catalog function once.

    The token is marked ``used`` **before** invocation so a double-click or
    retried request cannot invoke the action twice. If the invocation itself
    fails, the token stays used and the caller must ``request`` a fresh one —
    tokens are not refunded on failure.
    """
    data = await request.json()
    token = (data.get("token") or "").strip()
    if not token:
        raise ValidationError("token is required")

    domain = get_domain(session_mgr)
    domain_key = _chat_domain_key(domain)
    cache = _chat_cache(session_mgr)
    _pending_actions_prune(cache)

    entry = cache["pending_actions"].get(token)
    if (
        not entry
        or entry.get("kind", "action") != "action"
        or entry.get("used")
        or entry.get("domain") != domain_key
        or entry.get("expires_at", 0) <= time.time()
    ):
        raise ValidationError("Action expired — request again")

    entry["used"] = True
    _chat_save_cache(session_mgr, cache)

    return await NodeContextService.invoke_action(
        domain,
        settings,
        entity_uri=entry["entity_uri"],
        action_full_name=entry["action_full_name"],
    )


@router.post("/nodes/action/cancel")
async def dtwin_nodes_action_cancel(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
):
    """Discard a pending-action token if present. Always returns success.

    Best-effort UI cleanup: the token also self-expires via TTL, so a missing
    or already-consumed token is not an error.
    """
    try:
        data = await request.json()
    except Exception:
        data = {}
    token = (data.get("token") or "").strip()
    if token:
        cache = _chat_cache(session_mgr)
        if cache["pending_actions"].pop(token, None) is not None:
            _chat_save_cache(session_mgr, cache)
    return {"success": True}


@router.post(
    "/nodes/business-rule/request",
    dependencies=[Depends(require(ROLE_BUILDER, scope="domain"))],
)
async def dtwin_nodes_business_rule_request(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
):
    """Validate an entity + class-declared business rule and mint a one-time token.

    Does **not** run the rule. Call ``POST /dtwin/nodes/business-rule/confirm``
    with the returned token to execute it and write the inferred triples.
    """
    data = await request.json()
    entity_uri = (data.get("entity_uri") or "").strip()
    rule_name = (data.get("rule") or "").strip()
    if not entity_uri or not rule_name:
        raise ValidationError("entity_uri and rule are required")

    domain = get_domain(session_mgr)
    resolved = NodeContextService.resolve_business_rule(
        domain, entity_uri=entity_uri, rule_name=rule_name
    )
    rule = resolved["rule"]

    domain_key = _chat_domain_key(domain)
    cache = _chat_cache(session_mgr)
    _pending_actions_prune(cache)
    token = secrets.token_urlsafe(24)
    cache["pending_actions"][token] = {
        "kind": "business_rule",
        "domain": domain_key,
        "entity_uri": entity_uri,
        "rule": rule["name"],
        "expires_at": time.time() + _PENDING_ACTION_TTL_SEC,
        "used": False,
    }
    _chat_save_cache(session_mgr, cache)

    entity_label = DigitalTwin.extract_local_id(entity_uri)
    logger.info(
        "nodes/business-rule/request: minted token for entity=%s rule=%s domain=%s",
        entity_label,
        rule["name"],
        domain_key,
    )
    return {
        "success": True,
        "pending_business_rule": {
            "token": token,
            "entity_uri": entity_uri,
            "entity_label": entity_label,
            "rule": rule["name"],
            "description": rule.get("description"),
            "antecedent": rule.get("antecedent", ""),
            "consequent": rule.get("consequent", ""),
            "expires_in_sec": _PENDING_ACTION_TTL_SEC,
        },
    }


@router.post(
    "/nodes/business-rule/confirm",
    dependencies=[Depends(require(ROLE_BUILDER, scope="domain"))],
)
async def dtwin_nodes_business_rule_confirm(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Consume a pending business-rule token and run the rule once.

    The token is marked ``used`` before execution so a double-click cannot
    run the rule twice; it is not refunded on failure.
    """
    data = await request.json()
    token = (data.get("token") or "").strip()
    if not token:
        raise ValidationError("token is required")

    domain = get_domain(session_mgr)
    domain_key = _chat_domain_key(domain)
    cache = _chat_cache(session_mgr)
    _pending_actions_prune(cache)

    entry = cache["pending_actions"].get(token)
    if (
        not entry
        or entry.get("kind") != "business_rule"
        or entry.get("used")
        or entry.get("domain") != domain_key
        or entry.get("expires_at", 0) <= time.time()
    ):
        raise ValidationError("Business rule request expired — request again")

    entry["used"] = True
    _chat_save_cache(session_mgr, cache)

    return await NodeContextService.run_business_rule(
        domain,
        settings,
        entity_uri=entry["entity_uri"],
        rule_name=entry["rule"],
    )


@router.post("/nodes/business-rule/cancel")
async def dtwin_nodes_business_rule_cancel(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
):
    """Discard a pending business-rule token if present. Always returns success."""
    return await dtwin_nodes_action_cancel(request, session_mgr)


@router.get("/nodes/context", response_model=NodeContextResponse, response_model_exclude_none=True)
async def dtwin_nodes_context(
    entity_uri: str,
    fetch_dataset_rows: bool = False,
    dataset_row_limit: int = 5,
    follow_bridges: bool = False,
    bridge_depth: int = 1,
    compute_virtual_attributes: bool = False,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Resolve node context against the active session domain."""
    assert_domain_graph_read(session_mgr.request)
    domain = get_domain(session_mgr)
    payload = await NodeContextService.resolve_context(
        domain,
        settings,
        entity_uri=entity_uri,
        session_mgr=session_mgr,
        fetch_dataset_rows=fetch_dataset_rows,
        dataset_row_limit=max(1, min(dataset_row_limit or 5, 20)),
        follow_bridges=follow_bridges,
        bridge_depth=max(1, min(bridge_depth or 1, 1)),
        compute_virtual_attributes=compute_virtual_attributes,
        registry_catalog=None,
        registry_schema=None,
        registry_volume=None,
    )
    return NodeContextResponse(**payload)


@router.get("/nodes/virtual-attributes")
async def dtwin_nodes_virtual_attributes(
    entity_uri: str,
    function: Optional[str] = None,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Compute a node's virtual attributes against the active session domain.

    Dedicated to the Graph Explorer's Compute button: going through
    ``/nodes/context`` would re-resolve the dataset and the bridges for
    nothing. Omit *function* to compute every group declared on the class.
    """
    domain = get_domain(session_mgr)
    return await NodeContextService.compute_virtual_attributes(
        domain,
        settings,
        entity_uri=entity_uri,
        function_full_name=function,
    )


# Session key for the Graph Chat cache (history + limit + pending actions).
# Shape: {
#   "limit": int,
#   "history": {<domain_name>: [{"role", "content"}, ...]},
#   "pending_actions": {
#       <token>: {"domain", "entity_uri", "action_full_name", "expires_at", "used"},
#   },
# }
_CHAT_SESSION_KEY = TwinAssistantCache.SESSION_KEY
_CHAT_DEFAULT_LIMIT = TwinAssistantCache.DEFAULT_LIMIT
_CHAT_MIN_LIMIT = TwinAssistantCache.MIN_LIMIT
_CHAT_MAX_LIMIT = TwinAssistantCache.MAX_LIMIT
_UPGRADE_INSTANCE_ADVICE = TwinAssistantCache.UPGRADE_INSTANCE_ADVICE
_PENDING_ACTION_TTL_SEC = TwinAssistantCache.PENDING_ACTION_TTL_SEC


def _resource_pressure_payload() -> dict:
    return TwinAssistantCache.resource_pressure_payload()


def _chat_cache(session_mgr: SessionManager) -> dict:
    return TwinAssistantCache.chat_cache(session_mgr)


def _pending_actions_prune(cache: dict) -> None:
    return TwinAssistantCache.pending_actions_prune(cache)


def _chat_save_cache(session_mgr: SessionManager, cache: dict) -> None:
    return TwinAssistantCache.save_cache(session_mgr, cache)


def _chat_resolve_domain_name(domain) -> str:
    return TwinAssistantCache.resolve_domain_name(domain)


def _chat_domain_key(domain) -> str:
    return TwinAssistantCache.domain_key(domain)


def _chat_clamp_limit(limit) -> int:
    return TwinAssistantCache.clamp_limit(limit)


def _chat_trim(messages: list, limit: int) -> list:
    return TwinAssistantCache.trim(messages, limit)


def _chat_response_payload(agent_result, event_type: str | None = None) -> dict:
    return TwinAssistantCache.chat_response_payload(agent_result, event_type)


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
    from agents.agent_dtwin_chat import run_agent as run_chat_agent

    data = await request.json()
    user_message = (data.get("message") or "").strip()
    client_history = data.get("history") or []
    describe_depth = max(1, min(int(data.get("depth") or 1), 5))

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

    host, token, llm_endpoint, _llm_endpoint_kind = require_domain_llm(
        domain, settings
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
            describe_depth=describe_depth,
        )

    if not agent_result.success:
        raise InfrastructureError(
            "Graph Chat agent failed",
            detail=agent_result.error or None,
        )

    # Persist the exchange in the session cache (per-domain, trimmed to
    # the configured limit) so the discussion survives page navigation.
    # ``history`` is expected to hold PRIOR turns only; drop a trailing
    # entry that accidentally echoes the current user_message so we
    # never double-record the same question (also self-heals any pre-
    # existing sessions that were written with the old contract).
    #
    # Re-read the cache instead of reusing the pre-agent ``chat_cache``
    # snapshot: the agent's tool calls loop back into this same process
    # (e.g. ``POST /dtwin/nodes/action/request``) and may have minted a
    # ``pending_actions`` token into the session while ``run_agent`` was
    # running. Saving the stale snapshot would clobber that token.
    prior = list(history)
    if prior and prior[-1].get("role") == "user" and (
        prior[-1].get("content") or ""
    ).strip() == user_message.strip():
        prior = prior[:-1]
    prior.append({"role": "user", "content": user_message})
    prior.append({"role": "assistant", "content": agent_result.reply or ""})
    chat_cache = _chat_cache(session_mgr)
    chat_cache["history"][domain_key] = _chat_trim(prior, limit)
    _chat_save_cache(session_mgr, chat_cache)

    return _chat_response_payload(agent_result)


@router.post("/assistant/chat/stream")
async def dtwin_assistant_chat_stream(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Stream a single Graph Chat turn using Server-Sent Events.

    Sends one SSE event per agent step as it happens, then a final
    ``done`` event with the complete reply.  The session cache is
    updated server-side after the stream closes, identical to the
    blocking ``POST /assistant/chat`` endpoint.

    Event shapes::

        data: {"type": "step",  "step_type": "tool_call",   "tool_name": "...", "content": "..."}
        data: {"type": "step",  "step_type": "tool_result", "tool_name": "...", "duration_ms": 123}
        data: {"type": "done",  "reply": "...",  "tools": [...], "usage": {...}, "iterations": N}
        data: {"type": "error", "message": "..."}
    """
    import asyncio
    import json as _json
    import os

    from fastapi.responses import StreamingResponse
    from api.routers.internal._helpers import map_route_errors
    from agents.agent_dtwin_chat import run_agent as run_chat_agent
    from agents.engine_base import AgentStep

    data = await request.json()
    user_message = (data.get("message") or "").strip()
    client_history = data.get("history") or []
    describe_depth = max(1, min(int(data.get("depth") or 1), 5))

    if not user_message:
        raise ValidationError("No message provided")

    domain = get_domain(session_mgr)
    domain_key = _chat_domain_key(domain)
    chat_cache = _chat_cache(session_mgr)
    limit = _chat_clamp_limit(chat_cache.get("limit", _CHAT_DEFAULT_LIMIT))

    saved_history = chat_cache["history"].get(domain_key) or []
    history = saved_history if saved_history else client_history

    host, token, llm_endpoint, _llm_endpoint_kind = require_domain_llm(
        domain, settings
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
        "GraphChat/stream: user_message=%s, domain=%s, endpoint=%s",
        user_message[:80],
        domain_name,
        llm_endpoint,
    )

    loop = asyncio.get_event_loop()
    event_queue: asyncio.Queue = asyncio.Queue()

    def _on_event(step: AgentStep) -> None:
        """Forward an AgentStep from the sync thread to the async generator."""
        asyncio.run_coroutine_threadsafe(event_queue.put(step), loop).result(timeout=10)

    async def _run_agent_task() -> None:
        try:
            with map_route_errors("Graph Chat stream agent failed", logger):
                result = await asyncio.to_thread(
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
                    describe_depth=describe_depth,
                    on_event=_on_event,
                )
            await event_queue.put(("done", result))
        except Exception as exc:
            await event_queue.put(("error", str(exc)))

    agent_task = asyncio.create_task(_run_agent_task())

    async def _generate():
        try:
            while True:
                item = await event_queue.get()

                if isinstance(item, tuple):
                    kind, payload = item
                    if kind == "done":
                        agent_result = payload
                        # Update session cache exactly like the blocking endpoint.
                        # Re-read the cache instead of reusing the pre-agent
                        # snapshot: the agent's tool calls loop back into this
                        # same process and may have minted a pending_actions
                        # token into the session while run_agent was running.
                        prior = list(history)
                        if prior and prior[-1].get("role") == "user" and (
                            prior[-1].get("content") or ""
                        ).strip() == user_message.strip():
                            prior = prior[:-1]
                        prior.append({"role": "user", "content": user_message})
                        prior.append({"role": "assistant", "content": agent_result.reply or ""})
                        fresh_cache = _chat_cache(session_mgr)
                        fresh_cache["history"][domain_key] = _chat_trim(prior, limit)
                        _chat_save_cache(session_mgr, fresh_cache)

                        yield "data: " + _json.dumps(
                            _chat_response_payload(agent_result, event_type="done")
                        ) + "\n\n"
                        break

                    else:  # error
                        yield "data: " + _json.dumps({
                            "type": "error",
                            "message": payload,
                        }) + "\n\n"
                        break

                elif isinstance(item, AgentStep):
                    yield "data: " + _json.dumps({
                        "type": "step",
                        "step_type": item.step_type,
                        "tool_name": item.tool_name,
                        "content": item.content,
                        "duration_ms": item.duration_ms,
                    }) + "\n\n"

        finally:
            if not agent_task.done():
                agent_task.cancel()

    return StreamingResponse(
        _generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


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
    from back.fastapi.graphql_routes import _diagnose_empty_ontology
    from strawberry.printer import print_schema

    domain = get_domain(session_mgr)
    display_name = _chat_resolve_domain_name(domain)
    if not display_name:
        raise ValidationError("No domain selected in the current session.")

    ontology = domain.ontology or {}
    classes = ontology.get("classes", []) or []
    properties_list = ontology.get("properties", []) or []
    base_uri = ontology.get("base_uri", DEFAULT_BASE_URI)

    # Friendly fallback: when the ontology is too thin to back a GraphQL
    # schema, return a 200 with ``sdl=null`` + a typed ``reason``. The UI
    # branches on ``ready`` to render an in-context hint instead of a
    # blunt "HTTP 400" toast.
    diag = _diagnose_empty_ontology(classes, properties_list)
    if diag is not None:
        reason, message = diag
        return {
            "success": True,
            "ready": False,
            "domain": display_name,
            "sdl": None,
            "reason": reason,
            "message": message,
            "stats": {
                "classes": len(classes),
                "properties": len(properties_list),
            },
        }

    result = build_schema_for_domain(classes, properties_list, base_uri, display_name)
    if not result:
        raise ValidationError(
            "Could not generate GraphQL schema from the current ontology."
        )
    schema, _metadata = result
    return {
        "success": True,
        "ready": True,
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
    Graph Chat agent.  Requires a configured graph backend
    to resolve the query.
    """
    from back.core.graphql import build_schema_for_domain, DEFAULT_DEPTH, MAX_DEPTH
    from back.core.helpers import effective_graph_name

    assert_domain_graph_read(request)
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

    store = _require_graph_store(domain, settings)

    context = {
        "triplestore": store,
        "table_name": _graph_query_table(domain, settings, store),
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
    entity_type: Optional[str] = None,
    search: Optional[str] = None,
    depth: int = 1,
    limit: int = 1000,
    offset: int = 0,
    cache: Optional[str] = None,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Session-aware search + BFS traversal over the in-session domain.

    Mirrors ``GET /api/v1/digitaltwin/triples/find`` but resolves the
    domain from the user's session instead of the registry, so the
    Graph Chat agent can introspect domains that have never been
    published as a version.
    """
    from back.core.query_limits import get_graph_chat_result_cap

    assert_domain_graph_read(session_mgr.request)
    if not entity_type and not search:
        raise ValidationError("Provide at least entity_type or search")

    depth = max(1, min(int(depth or 1), 10))
    limit = max(1, min(int(limit or 1000), get_graph_chat_result_cap()))
    offset = max(0, int(offset or 0))

    domain = get_domain(session_mgr)
    store = _require_graph_store(domain, settings)
    from back.core.graphdb.search_cache import (
        apply_search_cache_to_store,
        parse_cache_param,
        search_cache_usage,
    )

    apply_search_cache_to_store(store, domain, parse_cache_param(cache))
    table = _graph_query_table(domain, settings, store)
    if not table:
        raise ValidationError("Graph name not configured")

    try:
        # BFS traversal issues multiple blocking SQL round-trips — offload it
        # off the event loop so concurrent requests aren't stalled.
        result = await run_blocking(
            DigitalTwin.find_triples_bfs,
            store,
            table,
            entity_type=entity_type,
            search=search,
            depth=depth,
            limit=limit,
            offset=offset,
        )
        payload = {
            "success": True,
            "seed_count": result["seed_count"],
            "depth": depth,
            "triples": [
                {
                    "subject": r.get("subject", ""),
                    "predicate": r.get("predicate", ""),
                    "object": r.get("object", ""),
                }
                for r in result["triples"]
            ],
            "count": result["count"],
            "total": result["total"],
            "has_more": bool(result.get("has_more", False)),
            "limit": limit,
            "offset": offset,
            "entity_count": result["entity_count"],
        }
        if result.get("message"):
            payload["message"] = result["message"]
        mode, used = search_cache_usage(store, table)
        payload["cache_used"] = used
        payload["cache_mode"] = mode
        return payload
    except (ValidationError, InfrastructureError, NotFoundError):
        raise
    except Exception as e:
        logger.exception("dtwin_triples_find failed: %s", e)
        raise InfrastructureError("Triple search failed", detail=str(e)) from e


@router.get("/neighbors")
async def dtwin_neighbors(
    uri: str,
    depth: int = 2,
    limit: int = 2000,
    include_inferred: bool = True,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Expand *uri* by ``depth`` BFS hops and return the induced subgraph
    triples.

    Used by the knowledge graph's right-click "Expand neighbours" action to
    enrich the displayed graph with one or more hops of related entities.
    Only triples whose object is a literal *or* whose object is a URI also
    present in the visited set are returned, so the front-end can render
    proper edges without ghost endpoints.
    """
    assert_domain_graph_read(session_mgr.request)
    if not uri:
        raise ValidationError("Provide 'uri'")

    depth = max(1, min(int(depth or 2), 5))
    limit = max(1, min(int(limit or 2000), 20000))

    domain = get_domain(session_mgr)
    store = _require_graph_store(domain, settings)
    from back.core.graphdb.search_cache import apply_search_cache_to_store

    apply_search_cache_to_store(store, domain, None)
    table = _graph_query_table(domain, settings, store)
    if not table:
        raise ValidationError("Graph name not configured")

    query_table = table if include_inferred else store.synced_table_name(table)

    try:
        # The BFS expansion runs one blocking SQL query per hop plus a final
        # bulk fetch. Offload the whole sequence to the thread pool so the
        # right-click "Expand neighbours" action doesn't freeze the event loop.
        def _expand() -> tuple[set[str], list]:
            visited: set[str] = {uri}
            frontier: set[str] = {uri}
            for _ in range(depth):
                if not frontier:
                    break
                next_hop = (
                    store.expand_entity_neighbors(query_table, frontier) - visited
                )
                if not next_hop:
                    break
                visited |= next_hop
                frontier = next_hop

            rows = store.get_triples_for_subjects(query_table, list(visited))
            return visited, _filter_neighbor_triples(rows, visited, limit)

        visited, triples = await run_blocking(_expand)

        return {
            "success": True,
            "seed_uri": uri,
            "depth": depth,
            "entity_count": len(visited),
            "columns": ["subject", "predicate", "object"],
            "triples": triples,
            "count": len(triples),
        }
    except (ValidationError, InfrastructureError, NotFoundError):
        raise
    except Exception as e:
        logger.exception("dtwin_neighbors failed: %s", e)
        raise InfrastructureError("Neighbour expansion failed", detail=str(e)) from e


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
