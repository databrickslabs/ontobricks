"""
Internal API -- Knowledge Graph / query JSON endpoints.

Moved from app/frontend/digitaltwin/routes.py during the front/back split.
"""

from dataclasses import dataclass
import os
import time
from typing import Any, Optional

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
from back.core.w3c import sparql
from back.core.databricks import DatabricksClient, is_databricks_app
from back.core.triplestore import get_triplestore
from back.objects.digitaltwin import CohortService, DigitalTwin, DomainSnapshot
from back.objects.domain import HomeService, Domain
from back.core.helpers import (
    effective_graph_name,
    effective_view_table,
    get_databricks_client,
    get_databricks_credentials,
    get_databricks_host_and_token,
    make_volume_file_service,
    is_uri,
    run_blocking,
)

logger = get_logger(__name__)

router = APIRouter(prefix="/dtwin", tags=["Query"])

# Canonical rdf:type predicate. Neighbour expansion must preserve type
# triples so the knowledge graph can group/colour expanded nodes by their
# declared entity type rather than their raw identifier (issue #52).
_RDF_TYPE_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


def _is_type_predicate(predicate: str) -> bool:
    """Return True for ``rdf:type`` predicates (full URI or ``#type``/``/type``)."""
    if not predicate:
        return False
    return (
        predicate == _RDF_TYPE_URI
        or predicate.endswith("#type")
        or predicate.endswith("/type")
    )


def _filter_neighbor_triples(
    rows: list[dict[str, str]],
    visited: set[str],
    limit: int,
) -> list[dict[str, str]]:
    """Reduce raw store rows to the triples the knowledge graph can render.

    A triple is kept when its object is a literal, when its object URI is
    part of *visited* (so edges have both endpoints rendered), or when it is
    an ``rdf:type`` triple. Type triples are preserved even though the class
    URI is never in *visited*: the front-end groups and colours nodes by
    their declared type, so dropping them makes freshly expanded nodes fall
    back to identifier-based grouping with random colours (issue #52).
    """
    triples: list[dict[str, str]] = []
    seen: set = set()
    for r in rows:
        s = r.get("subject", "") or ""
        p = r.get("predicate", "") or ""
        o = r.get("object", "") or ""
        key = (s, p, o)
        if key in seen:
            continue
        is_uri_obj = o.startswith("http://") or o.startswith("https://")
        if is_uri_obj and o not in visited and not _is_type_predicate(p):
            continue
        seen.add(key)
        triples.append({"subject": s, "predicate": p, "object": o})
        if len(triples) >= limit:
            break
    return triples


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
    """Start async knowledge graph build: CREATE VIEW then populate the graph store.

    Always performs a full rebuild. When the graph engine is ``lakebase`` in
    ``managed_synced`` mode, the Lakeflow pipeline handles the data-plane
    refresh automatically.
    """
    import threading
    from back.core.task_manager import get_task_manager

    await request.json()  # consume body (drop_existing / build_mode kept for API compat)

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
    delta_cfg = domain.delta or {}
    domain_snap = DomainSnapshot(domain)

    # Detect managed-synced mode using the same authoritative path as
    # _build_pipeline._resolve_lakebase_mode so the task step-list always
    # matches what the pipeline will actually execute.
    #
    # Previously this read only the domain.settings["registry"] mirror which
    # is absent when GlobalConfigService is the sole persistence layer
    # (common in the deployed App where the mirror write never fires).
    try:
        from back.core.triplestore.TripleStoreFactory import TripleStoreFactory

        _engine = TripleStoreFactory._resolve_graph_engine(domain, settings, force=True) or ""
        _ecfg = TripleStoreFactory._resolve_graph_engine_config(domain, settings, force=True) or {}
    except Exception as _exc:  # noqa: BLE001
        logger.debug("Engine config resolution failed, defaulting to non-synced: %s", _exc)
        _engine = ""
        _ecfg = {}
    _is_synced_mode = _engine == "lakebase" and _ecfg.get("sync_mode") == "managed_synced"

    if _is_synced_mode:
        _graph_steps = [
            {"name": "uc_schema",       "description": "Ensuring Unity Catalog schema"},
            {"name": "sync_register",   "description": "Registering synced table in Unity Catalog"},
            {"name": "sync_companion",  "description": "Creating companion table"},
            {"name": "sync_data",       "description": "Syncing data from Delta (Lakeflow)"},
            {"name": "union_view",      "description": "Creating knowledge graph union view"},
            {"name": "finalize",        "description": "Finalizing knowledge graph"},
        ]
    else:
        _graph_steps = [
            {"name": "graph", "description": "Updating the knowledge graph"},
        ]

    tm = get_task_manager()
    task = tm.create_task(
        name="Knowledge Graph Build",
        task_type="triplestore_sync",
        steps=[
            {"name": "prepare", "description": "Preparing mappings and generating queries"},
            {"name": "view",    "description": "Creating the Knowledge Graph view"},
            *_graph_steps,
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
            delta_cfg,
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
        try:
            body = await request.json()
        except Exception:
            body = {}
        include_inferred = body.get("include_inferred", True)

        domain = get_domain(session_mgr)
        graph_name = effective_graph_name(domain)

        store = _require_graph_store(domain, settings)

        query_table = graph_name if include_inferred else store.synced_table_name(graph_name)

        try:
            results = store.query_triples(query_table)
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
        max_triples = int(data.get("max_triples", settings.analytics_max_triples))

        domain = get_domain(session_mgr)
        graph_name = effective_graph_name(domain)
        if not graph_name:
            raise ValidationError("Graph name is not configured")

        store = _require_graph_store(domain, settings)

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
    """Return the cached ``graph_analytics`` row for the active domain/version.

    Resolves ``(folder, version)`` from the domain session and reads the
    last persisted result via the registry. ``None`` when nothing is
    stored yet or the lookup is not possible. Never raises.
    """
    from back.objects.registry.RegistryService import RegistryService

    folder = getattr(domain, "uc_domain_folder", "") or ""
    version = str(getattr(domain, "current_version", "") or "")
    if not folder or not version:
        return None
    try:
        svc = RegistryService.from_context(domain, settings)
        return svc.load_graph_analytics(folder, version)
    except Exception as exc:  # noqa: BLE001
        logger.debug("load_graph_analytics failed: %s", exc)
        return None


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
        max_triples = int(data.get("max_triples", settings.analytics_max_triples))
        max_nodes_betweenness = int(data.get("max_nodes_betweenness", 2_000))

        domain = get_domain(session_mgr)
        graph_name = effective_graph_name(domain)
        if not graph_name:
            raise ValidationError("Graph name is not configured")

        store = _require_graph_store(domain, settings)

        # Fail fast on graphs too large for in-memory analysis — the triple
        # count is already known, so reject here instead of spawning a
        # background task that would only fail after loading everything.
        try:
            total_triples = int(store.get_aggregate_stats(graph_name).get("total", 0))
        except Exception:  # noqa: BLE001
            total_triples = 0
        if total_triples and total_triples > max_triples:
            raise ValidationError(
                f"This Knowledge Graph has {total_triples:,} triples, which "
                f"exceeds the analytics limit of {max_triples:,}. In-memory "
                f"centrality/structure analysis is disabled for graphs this "
                f"large — reduce the synced graph (exclude entity types in "
                f"KG → Sync) or raise ONTOBRICKS_ANALYTICS_MAX_TRIPLES."
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
                store,
                graph_name,
                predicate_filter=predicate_filter,
                class_filter=class_filter,
                max_triples=max_triples,
                max_nodes_betweenness=max_nodes_betweenness,
            )

        thread = threading.Thread(target=run_metrics, daemon=True)
        thread.start()

        return {
            "success": True,
            "task_id": task.id,
            "message": "Analysis started",
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


@router.get("/metrics/history")
async def get_graph_metrics_history(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return the analytics run history (newest-first) for this domain/version.

    Lightweight metadata per run from the ``graph_analytics_runs`` trace —
    backs the Analytics page "History" tab.
    """
    from back.objects.registry.RegistryService import RegistryService

    try:
        domain = get_domain(session_mgr)
        folder = getattr(domain, "uc_domain_folder", "") or ""
        version = str(getattr(domain, "current_version", "") or "")
        if not folder or not version:
            return {"success": True, "runs": []}

        svc = RegistryService.from_context(domain, settings)
        runs = svc.load_graph_analytics_runs(folder, version, limit=100)
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

        host, token = get_databricks_host_and_token(domain, settings)
        if not host or not token:
            raise ValidationError("Databricks credentials not configured")

        llm_endpoint = (domain.info or {}).get("llm_endpoint", "") or ""
        if not llm_endpoint:
            llm_endpoint = _auto_discover_llm_endpoint(domain, settings)
        if not llm_endpoint:
            raise ValidationError(
                "No LLM serving endpoint available. Please set one in Domain Settings."
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
    store = _require_graph_store(domain, settings)
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


def _require_graph_store(domain, settings):
    """Return the graph-backend triple store or raise :class:`InfrastructureError`.

    Centralises the five-line guard that every graph-facing route repeated::

        store = get_triplestore(domain, settings, backend="graph")
        if not store:
            raise InfrastructureError("Graph backend is not configured")
    """
    store = get_triplestore(domain, settings, backend="graph")
    if not store:
        raise InfrastructureError("Graph backend is not configured")
    return store


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
        graph_name = effective_graph_name(domain)
        if not graph_name:
            raise ValidationError("Graph name is not configured")

        store = _require_graph_store(domain, settings)

        query_table = graph_name if include_inferred else store.synced_table_name(graph_name)

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
            client_max = int(data.get("max_entities", 5000))
            server_entity_cap = 3_000 if is_databricks_app() else 50_000
            max_entities = max(100, min(client_max, server_entity_cap))
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

    async def _graph_status():
        t_s = _t.monotonic()
        out = await dt.get_or_fetch_graph_status(settings)
        logger.debug(
            "sync_info: graph status took %.0fms", (_t.monotonic() - t_s) * 1000
        )
        return out

    # DT existence is served cache-first so the Build page paints instantly.
    # The live probe is a cold SQL-warehouse / Lakebase wake-up that used to
    # block this endpoint for tens of seconds; it now runs off the request path.
    # The frontend confirms the live state with a non-blocking follow-up to
    # `/dtwin/sync/dt-existence` whenever `dt_existence_pending` is set.
    cached_existence = dt.get_ts_cache("dt_existence")
    dt_exist = cached_existence or dt.pending_dt_existence(settings)
    dt_existence_pending = True

    _, ts_status = await asyncio.gather(
        _schedule_sync(),
        _graph_status(),
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
        "dt_existence_pending": dt_existence_pending,
        "changes": {"needs_rebuild": needs_rebuild},
    }


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

        store = _require_graph_store(domain, settings)

        agg = store.get_aggregate_stats(graph_name)
        total_count = agg["total"]
        subject_count = agg["distinct_subjects"]
        predicate_count = agg["distinct_predicates"]
        label_count = agg["label_count"]

        entity_types = store.get_type_distribution(graph_name)
        top_predicates = store.get_predicate_distribution(graph_name)

        type_count = sum(int(r.get("cnt", 0)) for r in entity_types)
        relationship_count = total_count - type_count - label_count

        inferred_count = store.get_inferred_triple_count(graph_name)

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
            "inferred_triples": inferred_count,
            # Effective KG-analytics safety limit, so the Analytics page can
            # warn up-front when total_triples exceeds it (in-memory NetworkX
            # guard; see Settings.analytics_max_triples).
            "analytics_max_triples": settings.analytics_max_triples,
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
    shape_ids = data.get("shape_ids") or []
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
    if shape_ids:
        shape_ids_set = set(shape_ids)
        shapes = [s for s in shapes if s.get("id") in shape_ids_set]
    elif dimensions:
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
            describe_depth=describe_depth,
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
    from back.core.helpers import get_databricks_host_and_token
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

    host, token = get_databricks_host_and_token(domain, settings)
    if not host or not token:
        raise ValidationError("Databricks credentials not configured")

    llm_endpoint = (domain.info or {}).get("llm_endpoint", "") or ""
    if not llm_endpoint:
        llm_endpoint = _auto_discover_llm_endpoint(domain, settings)
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
                        # Update session cache exactly like the blocking endpoint
                        prior = list(history)
                        if prior and prior[-1].get("role") == "user" and (
                            prior[-1].get("content") or ""
                        ).strip() == user_message.strip():
                            prior = prior[:-1]
                        prior.append({"role": "user", "content": user_message})
                        prior.append({"role": "assistant", "content": agent_result.reply or ""})
                        chat_cache["history"][domain_key] = _chat_trim(prior, limit)
                        _chat_save_cache(session_mgr, chat_cache)

                        tool_calls = [
                            {"name": s.tool_name, "duration_ms": s.duration_ms}
                            for s in agent_result.steps
                            if s.step_type == "tool_result"
                        ]
                        yield "data: " + _json.dumps({
                            "type": "done",
                            "reply": agent_result.reply or "",
                            "tools": tool_calls,
                            "iterations": agent_result.iterations,
                            "usage": agent_result.usage,
                            "success": agent_result.success,
                        }) + "\n\n"
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
    Graph Chat agent.  Requires a configured graph backend
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

    store = _require_graph_store(domain, settings)

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
    entity_type: Optional[str] = None,
    search: Optional[str] = None,
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

    store = _require_graph_store(domain, settings)

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
    if not uri:
        raise ValidationError("Provide 'uri'")

    depth = max(1, min(int(depth or 2), 5))
    limit = max(1, min(int(limit or 2000), 20000))

    domain = get_domain(session_mgr)
    table = effective_graph_name(domain)
    if not table:
        raise ValidationError("Graph name not configured")

    store = _require_graph_store(domain, settings)

    query_table = table if include_inferred else store.synced_table_name(table)

    try:
        visited: set[str] = {uri}
        frontier: set[str] = {uri}
        for _ in range(depth):
            if not frontier:
                break
            next_hop = store.expand_entity_neighbors(query_table, frontier) - visited
            if not next_hop:
                break
            visited |= next_hop
            frontier = next_hop

        rows = store.get_triples_for_subjects(query_table, list(visited))

        triples = _filter_neighbor_triples(rows, visited, limit)

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
