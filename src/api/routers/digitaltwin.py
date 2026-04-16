"""
Digital Twin External REST API

Provides programmatic access to the triple store: status, insights,
build trigger, and triple retrieval.

Domain registry listing, versions, design status, and artifacts (OWL, R2RML, Spark SQL) live under
``/api/v1/domains`` and ``/api/v1/domain/...`` (see ``api.routers.domains``).

All endpoints accept an optional domain query parameter (``domain_name``,
with legacy alias ``project_name``). When supplied the API loads the named
domain from the registry instead of relying on the current browser session.
An optional version parameter (``domain_version``, legacy ``project_version``)
targets a specific version; when omitted, the latest version is used.

Use ``GET /api/v1/domain/versions?domain_name=...`` to discover available versions.
"""
from fastapi import APIRouter, Depends, Query
from pydantic import AliasChoices, BaseModel, Field
from typing import Any, Dict, List, Optional

from back.core.logging import get_logger
from back.core.errors import ValidationError, NotFoundError, InfrastructureError
from api.constants import DEFAULT_BASE_URI, DEFAULT_GRAPH_NAME
from back.objects.session import SessionManager, get_session_manager
from shared.config.settings import get_settings, Settings
from back.core.triplestore import get_triplestore
from back.core.helpers import get_databricks_credentials, sql_escape, effective_view_table, effective_graph_name, is_uri
from back.objects.digitaltwin import DigitalTwin
from back.objects.digitaltwin.models import DomainSnapshot

# Tests may patch ``api.routers.digitaltwin`` for registry resolution helpers.
_resolve_registry = DigitalTwin.resolve_registry
_extract_local_id = DigitalTwin.extract_local_id
_expand_uri_aliases = DigitalTwin.expand_uri_aliases

logger = get_logger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class StatusResponse(BaseModel):
    success: bool
    view_table: Optional[str] = None
    graph_name: Optional[str] = None
    has_data: bool = False
    count: int = 0
    last_modified: Optional[str] = None
    reason: Optional[str] = None
    message: Optional[str] = None


class EntityTypeStat(BaseModel):
    uri: str
    count: int


class PredicateStat(BaseModel):
    uri: str
    count: int


class StatsResponse(BaseModel):
    success: bool
    total_triples: int = 0
    distinct_subjects: int = 0
    distinct_predicates: int = 0
    entity_types: List[EntityTypeStat] = []
    top_predicates: List[PredicateStat] = []
    label_count: int = 0
    type_assertion_count: int = 0
    relationship_count: int = 0
    message: Optional[str] = None


class BuildRequest(BaseModel):
    build_mode: str = Field("incremental", description="'incremental' (detect changes, apply diff) or 'full' (drop and recreate)")
    drop_existing: bool = Field(False, description="Deprecated: use build_mode='full' instead")


class BuildStartedResponse(BaseModel):
    success: bool
    task_id: Optional[str] = None
    message: Optional[str] = None


class TaskProgressResponse(BaseModel):
    """Generic task-polling response used by all async task endpoints."""
    success: bool
    task_id: str
    status: str = Field(..., description="pending | running | completed | failed | cancelled")
    progress: int = Field(0, ge=0, le=100)
    message: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


BuildProgressResponse = TaskProgressResponse


def _poll_task(task_id: str) -> TaskProgressResponse:
    """Shared helper: look up a task and return its progress response."""
    from back.core.task_manager import get_task_manager
    task = get_task_manager().get_task(task_id)
    if not task:
        raise NotFoundError("Task not found")
    return TaskProgressResponse(
        success=True, task_id=task.id, status=task.status,
        progress=task.progress or 0,
        message=task.message or '',
        result=task.result, error=task.error,
    )


class TripleRow(BaseModel):
    subject: str
    predicate: str
    object: str


class TriplesResponse(BaseModel):
    success: bool
    triples: List[TripleRow] = []
    count: int = 0
    total: Optional[int] = None
    message: Optional[str] = None


class FindResponse(BaseModel):
    success: bool
    seed_count: int = Field(0, description="Number of entities matching the initial search")
    depth: int = Field(1, description="Traversal depth used")
    triples: List[TripleRow] = []
    count: int = Field(0, description="Triples returned in this page")
    total: int = Field(0, description="Total triples found across all pages")
    limit: int = Field(1000, description="Page size used")
    offset: int = Field(0, description="Offset used")
    entity_count: int = 0
    message: Optional[str] = None


class DataQualityRequest(BaseModel):
    category: Optional[str] = Field(None, description="Filter shapes by category (e.g. 'cardinality', 'value')")
    backend: str = Field("graph", description="Backend to run checks against: 'view' (SQL) or 'graph' (in-memory)")


class DataQualityStartedResponse(BaseModel):
    success: bool
    task_id: Optional[str] = None
    shape_count: int = 0
    message: Optional[str] = None


class InferenceRequest(BaseModel):
    tbox: bool = Field(True, description="Run T-Box (OWL 2 RL) inference")
    swrl: bool = Field(True, description="Run SWRL rule execution")
    graph: bool = Field(True, description="Run graph-structural reasoning")
    constraints: bool = Field(True, description="Run constraint checks")
    decision_tables: bool = Field(False, description="Run DMN-style decision table rules")
    sparql_rules: bool = Field(False, description="Run SPARQL CONSTRUCT inference rules")
    aggregate_rules: bool = Field(False, description="Run aggregate (GROUP BY/HAVING) rules")
    append_graph: bool = Field(False, description="Append inferred triples to the knowledge graph after inference completes")
    materialize: bool = Field(False, description="Write inferred triples to a Delta table")
    materialize_table: Optional[str] = Field(None, description="Fully-qualified table name for materialization (catalog.schema.table)")


class InferenceStartedResponse(BaseModel):
    success: bool
    task_id: Optional[str] = None
    message: Optional[str] = None


class InferenceResultResponse(BaseModel):
    success: bool
    inferred_count: int = 0
    violations_count: int = 0
    reasoning: Optional[Dict[str, Any]] = None
    message: Optional[str] = None


# ---------------------------------------------------------------------------
# GET /registry  — stateless registry discovery for external clients (MCP)
# ---------------------------------------------------------------------------

@router.get(
    "/registry",
    summary="Get registry configuration",
    description="Return the domain registry location (catalog.schema.volume). "
                "Reads from the current session if available, otherwise from "
                "environment variables (REGISTRY_CATALOG, REGISTRY_SCHEMA, REGISTRY_VOLUME).",
)
async def dt_registry(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    reg = DigitalTwin.resolve_registry(session_mgr, settings)
    catalog, schema, volume = reg['catalog'], reg['schema'], reg['volume']
    return {
        "catalog": catalog,
        "schema": schema,
        "volume": volume,
        "configured": bool(catalog and schema),
    }


# ---------------------------------------------------------------------------
# GET /status
# ---------------------------------------------------------------------------

@router.get(
    "/status",
    response_model=StatusResponse,
    summary="Triple store status",
    description="Check whether the triple store is configured, which backend is used, "
                "and how many triples it currently contains.",
)
async def dt_status(
    domain_name: Optional[str] = Query(
        None,
        validation_alias=AliasChoices("domain_name", "project_name"),
        description="Domain name in the registry (uses current session domain if omitted)",
    ),
    domain_version: Optional[str] = Query(
        None,
        validation_alias=AliasChoices("domain_version", "project_version"),
        description="Domain version to load (uses latest version if omitted)",
    ),
    registry_catalog: Optional[str] = Query(None, description="Override registry catalog"),
    registry_schema: Optional[str] = Query(None, description="Override registry schema"),
    registry_volume: Optional[str] = Query(None, description="Override registry volume"),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    domain = DigitalTwin.resolve_domain(domain_name, session_mgr, settings, registry_catalog, registry_schema, registry_volume, domain_version)
    view_table = effective_view_table(domain, settings).strip()
    graph_name = effective_graph_name(domain)

    graph_store = get_triplestore(domain, settings, backend="graph")
    if not graph_store:
        return StatusResponse(success=True, view_table=view_table, graph_name=graph_name,
                              reason='Graph backend not configured')

    try:
        if not graph_store.table_exists(graph_name):
            return StatusResponse(success=True, view_table=view_table, graph_name=graph_name,
                                  reason='Graph does not exist yet')
        status = graph_store.get_status(graph_name)
        count = status.get('count', 0)
        last_mod = status.get('last_modified')
        return StatusResponse(
            success=True, view_table=view_table, graph_name=graph_name,
            has_data=count > 0, count=count,
            last_modified=str(last_mod) if last_mod else None,
        )
    except Exception as e:
        logger.exception("dt_status failed: %s", e)
        raise InfrastructureError("Digital Twin status check failed", detail=str(e)) from e


# ---------------------------------------------------------------------------
# GET /stats
# ---------------------------------------------------------------------------

@router.get(
    "/stats",
    response_model=StatsResponse,
    summary="Triple store insights",
    description="Return content statistics: entity type breakdown, predicate counts, "
                "label/relationship totals.",
)
async def dt_stats(
    domain_name: Optional[str] = Query(
        None,
        validation_alias=AliasChoices("domain_name", "project_name"),
        description="Domain name in the registry (uses current session domain if omitted)",
    ),
    domain_version: Optional[str] = Query(
        None,
        validation_alias=AliasChoices("domain_version", "project_version"),
        description="Domain version to load (uses latest version if omitted)",
    ),
    registry_catalog: Optional[str] = Query(None, description="Override registry catalog"),
    registry_schema: Optional[str] = Query(None, description="Override registry schema"),
    registry_volume: Optional[str] = Query(None, description="Override registry volume"),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    domain = DigitalTwin.resolve_domain(domain_name, session_mgr, settings, registry_catalog, registry_schema, registry_volume, domain_version)
    graph_name = effective_graph_name(domain)

    if not graph_name:
        raise ValidationError("Graph name not configured")

    store = get_triplestore(domain, settings, backend="graph")
    if not store:
        raise ValidationError("Graph backend not configured")

    try:
        stats = store.get_aggregate_stats(graph_name)
        total = stats["total"]
        subj = stats["distinct_subjects"]
        pred = stats["distinct_predicates"]
        type_cnt = stats["type_assertion_count"]
        lbl = stats["label_count"]

        entity_rows = store.get_type_distribution(graph_name)
        pred_rows = store.get_predicate_distribution(graph_name)

        rel_cnt = max(total - type_cnt - lbl, 0)

        return StatsResponse(
            success=True,
            total_triples=total, distinct_subjects=subj, distinct_predicates=pred,
            entity_types=[EntityTypeStat(uri=r['type_uri'], count=int(r['cnt'])) for r in entity_rows],
            top_predicates=[PredicateStat(uri=r['predicate'], count=int(r['cnt'])) for r in pred_rows],
            label_count=lbl, type_assertion_count=type_cnt, relationship_count=rel_cnt,
        )
    except Exception as e:
        logger.exception("dt_stats failed: %s", e)
        raise InfrastructureError("Triple store stats retrieval failed", detail=str(e)) from e


# ---------------------------------------------------------------------------
# POST /build
# ---------------------------------------------------------------------------

@router.post(
    "/build",
    response_model=BuildStartedResponse,
    summary="Start a Digital Twin build",
    description="Generate all triples from the current ontology + mapping configuration "
                "and write them to the configured triple store backend. "
                "Returns a `task_id` that can be polled via `GET /build/{task_id}`.",
)
async def dt_build(
    body: BuildRequest = BuildRequest(),
    domain_name: Optional[str] = Query(
        None,
        validation_alias=AliasChoices("domain_name", "project_name"),
        description="Domain name in the registry (uses current session domain if omitted)",
    ),
    domain_version: Optional[str] = Query(
        None,
        validation_alias=AliasChoices("domain_version", "project_version"),
        description="Domain version to load (uses latest version if omitted)",
    ),
    registry_catalog: Optional[str] = Query(None, description="Override registry catalog"),
    registry_schema: Optional[str] = Query(None, description="Override registry schema"),
    registry_volume: Optional[str] = Query(None, description="Override registry volume"),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    import threading
    from back.core.task_manager import get_task_manager
    from back.core.w3c import sparql
    from back.core.databricks import DatabricksClient

    domain = DigitalTwin.resolve_domain(domain_name, session_mgr, settings, registry_catalog, registry_schema, registry_volume, domain_version)
    view_table = effective_view_table(domain, settings).strip()
    graph_name = effective_graph_name(domain)
    if not view_table:
        raise ValidationError("View location not configured")

    parts = view_table.split('.')
    if len(parts) != 3:
        raise ValidationError("View must be fully qualified: catalog.schema.view_name")

    domain.ensure_generated_content()
    r2rml = domain.get_r2rml()
    if not r2rml:
        raise ValidationError("No R2RML mapping available")

    host, token, warehouse_id = get_databricks_credentials(domain, settings)
    if not host or not token:
        raise ValidationError("Databricks not configured")
    if not warehouse_id:
        raise ValidationError("No SQL warehouse configured")

    base_uri = domain.ontology.get('base_uri', DEFAULT_BASE_URI)
    mapping_config = domain.assignment
    ontology_config = domain.ontology

    snap = DomainSnapshot(domain)
    force_full = body.build_mode == 'full' or body.drop_existing
    stored_source_versions = dict(domain.source_versions or {})
    delta_cfg = domain.delta or {}

    tm = get_task_manager()
    task = tm.create_task(name="Digital Twin Build (API)", task_type="triplestore_sync",
                          steps=[{'name': 'prepare', 'description': 'Preparing'},
                                 {'name': 'gate', 'description': 'Checking source tables'},
                                 {'name': 'view', 'description': 'Creating VIEW'},
                                 {'name': 'diff', 'description': 'Computing diff'},
                                 {'name': 'graph', 'description': 'Applying to graph'},
                                 {'name': 'snapshot', 'description': 'Refreshing snapshot'}])

    def _run():
        import time
        t0 = time.time()
        actual_mode = 'full' if force_full else 'incremental'
        try:
            tm.start_task(task.id, "Preparing mappings...")
            src = DatabricksClient(host=host, token=token, warehouse_id=warehouse_id)
            ent, rels = sparql.extract_r2rml_mappings(r2rml)
            ent = DigitalTwin.augment_mappings_from_config(ent, mapping_config, base_uri, ontology_config)
            rels = DigitalTwin.augment_relationships_from_config(rels, mapping_config, base_uri, ontology_config)
            if not ent and not rels:
                tm.fail_task(task.id, 'No valid mappings found'); return
            sparql_q = (f"PREFIX : <{base_uri}>\nPREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                        f"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n\n"
                        f"SELECT ?subject ?predicate ?object\nWHERE {{\n    ?subject ?predicate ?object .\n}}")
            res = sparql.translate_sparql_to_spark(sparql_q, ent, None, rels, dialect='spark')
            if not res['success']:
                tm.fail_task(task.id, res.get('message', 'Translation failed')); return
            tm.update_progress(task.id, 10, "SQL generated")

            from back.core.triplestore import IncrementalBuildService
            incr_svc = IncrementalBuildService(src)
            snapshot_table = incr_svc.snapshot_table_name(
                (domain.info or {}).get('name', DEFAULT_GRAPH_NAME), delta_cfg,
                version=snap.current_version,
            )

            new_source_versions = {}
            if actual_mode == 'incremental':
                tm.advance_step(task.id, "Checking source tables...")
                source_tables = incr_svc.extract_source_tables(mapping_config)
                changed, new_source_versions = incr_svc.check_source_versions(source_tables, stored_source_versions)
                if not changed:
                    tm.complete_task(task.id, result={
                        'triple_count': 0, 'view_table': view_table, 'graph_name': graph_name,
                        'build_mode': 'skipped', 'skipped_reason': 'No source table changes detected',
                        'duration_seconds': time.time() - t0,
                    }, message='No source data changes — build skipped'); return

            tm.advance_step(task.id, f"Creating VIEW {view_table}...")
            catalog, schema, vname = parts
            view_ok, view_msg = src.create_or_replace_view(catalog, schema, vname, res['sql'])
            if not view_ok:
                logger.error("API build: failed to create VIEW %s: %s", view_table, view_msg)
                tm.fail_task(task.id, f'Failed to create VIEW: {view_msg}'); return
            logger.info("API build: created VIEW %s", view_table)
            tm.update_progress(task.id, 25, "VIEW created")

            to_add = []
            to_remove = []
            total_cnt = 0

            if actual_mode == 'incremental' and incr_svc.snapshot_exists(snapshot_table):
                tm.advance_step(task.id, "Computing incremental diff...")
                try:
                    to_add, to_remove = incr_svc.compute_diff(view_table, snapshot_table)
                    total_cnt = incr_svc.count_view_triples(view_table)
                    if incr_svc.should_fallback_to_full(len(to_add), len(to_remove), total_cnt):
                        actual_mode = 'full'
                except Exception:
                    actual_mode = 'full'
            else:
                actual_mode = 'full'

            tm.advance_step(task.id, "Applying changes to graph...")
            from back.core.triplestore import get_triplestore as _ts
            store = _ts(snap, settings, backend="graph")
            if not store:
                tm.fail_task(task.id, 'Could not initialize LadybugDB backend'); return

            if actual_mode == 'full':
                triples = src.execute_query(f"SELECT * FROM {view_table}")
                cnt = len(triples)
                if cnt == 0:
                    tm.complete_task(task.id, result={
                        'triple_count': 0, 'view_table': view_table, 'graph_name': graph_name,
                        'build_mode': 'full', 'duration_seconds': time.time() - t0,
                    }, message='VIEW created but no triples generated'); return
                store.drop_table(graph_name)
                store.create_table(graph_name)
                def _prog(w, t_):
                    tm.update_progress(task.id, 50 + int(w / t_ * 40), f"Written {w}/{t_}...")
                store.insert_triples(graph_name, triples, batch_size=500, on_progress=_prog)
                store.optimize_table(graph_name)
                total_cnt = cnt
            else:
                cnt = len(to_add) + len(to_remove)
                if to_remove:
                    store.delete_triples(graph_name, to_remove, batch_size=500)
                if to_add:
                    store.insert_triples(graph_name, to_add, batch_size=500)
                if cnt > 0:
                    store.optimize_table(graph_name)

            tm.advance_step(task.id, "Refreshing snapshot...")
            try:
                incr_svc.refresh_snapshot(view_table, snapshot_table)
            except Exception as snap_e:
                logger.warning("API build: snapshot refresh failed: %s", snap_e)

            try:
                if new_source_versions:
                    domain.source_versions = new_source_versions
                    domain.save()
            except Exception:
                pass

            result_data = {
                'triple_count': total_cnt, 'view_table': view_table, 'graph_name': graph_name,
                'build_mode': actual_mode, 'snapshot_table': snapshot_table,
                'duration_seconds': time.time() - t0,
            }
            if actual_mode == 'incremental':
                result_data['diff'] = {'added': len(to_add), 'removed': len(to_remove)}
                msg = f"Incremental: +{len(to_add)} / -{len(to_remove)} in {time.time()-t0:.1f}s"
            else:
                msg = f"Full rebuild: {total_cnt} triples in {time.time()-t0:.1f}s"
            tm.complete_task(task.id, result=result_data, message=msg)
        except Exception as exc:
            logger.exception("API build failed: %s", exc)
            tm.fail_task(task.id, str(exc))

    threading.Thread(target=_run, daemon=True).start()
    return BuildStartedResponse(success=True, task_id=task.id, message='Build started')


# ---------------------------------------------------------------------------
# GET /build/{task_id}
# ---------------------------------------------------------------------------

@router.get(
    "/build/{task_id}",
    response_model=BuildProgressResponse,
    summary="Poll build progress",
    description="Check the progress of a previously started build. "
                "Returns status, progress percentage, and result when completed.",
)
async def dt_build_progress(task_id: str):
    return _poll_task(task_id)


# ---------------------------------------------------------------------------
# GET /triples/find
# ---------------------------------------------------------------------------

@router.get(
    "/triples/find",
    response_model=FindResponse,
    summary="Find entities and traverse relationships",
    description="Search for entities by type and/or label text, then traverse "
                "their relationships up to N levels deep (BFS graph walk). "
                "Returns all triples discovered during traversal.",
)
async def dt_triples_find(
    entity_type: Optional[str] = None,
    search: Optional[str] = None,
    depth: int = 1,
    limit: int = 1000,
    offset: int = 0,
    domain_name: Optional[str] = Query(
        None,
        validation_alias=AliasChoices("domain_name", "project_name"),
        description="Domain name in the registry (uses current session domain if omitted)",
    ),
    domain_version: Optional[str] = Query(
        None,
        validation_alias=AliasChoices("domain_version", "project_version"),
        description="Domain version to load (uses latest version if omitted)",
    ),
    registry_catalog: Optional[str] = Query(None, description="Override registry catalog"),
    registry_schema: Optional[str] = Query(None, description="Override registry schema"),
    registry_volume: Optional[str] = Query(None, description="Override registry volume"),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    if not entity_type and not search:
        raise ValidationError("Provide at least entity_type or search")
    depth = max(1, min(depth, 10))
    limit = max(1, min(limit, 10000))
    offset = max(0, offset)

    domain = DigitalTwin.resolve_domain(domain_name, session_mgr, settings, registry_catalog, registry_schema, registry_volume, domain_version)
    table = effective_graph_name(domain)
    if not table:
        raise ValidationError("Graph name not configured")

    store = get_triplestore(domain, settings, backend="graph")
    if not store:
        raise ValidationError("Graph backend not configured")

    rdf_type = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'

    try:
        rdfs_label = 'http://www.w3.org/2000/01/rdf-schema#label'

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

        seed_where = ' WHERE ' + ' AND '.join(seed_conditions)

        bfs_rows = store.bfs_traversal(
            table, seed_where, depth,
            search=search or "", entity_type=entity_type or "",
        )

        if not bfs_rows:
            return FindResponse(success=True, seed_count=0, depth=depth,
                                message='No matching entities found')

        all_entities = {r['entity'] for r in bfs_rows}
        seed_count = sum(1 for r in bfs_rows if int(r.get('min_lvl', 0)) == 0)

        all_entities = DigitalTwin.expand_uri_aliases(store, table, all_entities)

        all_rows = store.get_triples_for_subjects(table, list(all_entities))

        seen_triples: set = set()
        all_triples: list = []
        for r in all_rows:
            key = (r['subject'], r['predicate'], r['object'])
            if key not in seen_triples:
                seen_triples.add(key)
                all_triples.append(r)

        total = len(all_triples)
        page = all_triples[offset:offset + limit]

        return FindResponse(
            success=True,
            seed_count=seed_count,
            depth=depth,
            triples=[TripleRow(subject=r.get('subject', ''), predicate=r.get('predicate', ''),
                               object=r.get('object', '')) for r in page],
            count=len(page),
            total=total,
            limit=limit,
            offset=offset,
            entity_count=len(all_entities),
        )
    except Exception as e:
        logger.exception("dt_triples_find failed: %s", e)
        raise InfrastructureError("Triple search failed", detail=str(e)) from e


# ---------------------------------------------------------------------------
# GET /triples
# ---------------------------------------------------------------------------

@router.get(
    "/triples",
    response_model=TriplesResponse,
    summary="Retrieve triples",
    description="Query triples from the configured triple store with optional filters. "
                "Supports filtering by entity type, predicate, subject/object text search, "
                "and pagination via limit/offset.",
)
async def dt_triples(
    subject: Optional[str] = None,
    predicate: Optional[str] = None,
    object: Optional[str] = None,
    entity_type: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 1000,
    offset: int = 0,
    backend: Optional[str] = Query("graph", description="Backend: 'view' or 'graph' (default graph)"),
    domain_name: Optional[str] = Query(
        None,
        validation_alias=AliasChoices("domain_name", "project_name"),
        description="Domain name in the registry (uses current session domain if omitted)",
    ),
    domain_version: Optional[str] = Query(
        None,
        validation_alias=AliasChoices("domain_version", "project_version"),
        description="Domain version to load (uses latest version if omitted)",
    ),
    registry_catalog: Optional[str] = Query(None, description="Override registry catalog"),
    registry_schema: Optional[str] = Query(None, description="Override registry schema"),
    registry_volume: Optional[str] = Query(None, description="Override registry volume"),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    domain = DigitalTwin.resolve_domain(domain_name, session_mgr, settings, registry_catalog, registry_schema, registry_volume, domain_version)
    be = backend or "graph"
    table = (effective_view_table(domain, settings).strip() if be == "view"
             else effective_graph_name(domain))
    if not table:
        raise ValidationError("Triple store not configured")

    store = get_triplestore(domain, settings, backend=be)
    if not store:
        raise ValidationError("Backend not configured")

    try:
        conditions = []
        if subject:
            conditions.append(f"subject LIKE '%{sql_escape(subject)}%'")
        if predicate:
            conditions.append(f"predicate = '{sql_escape(predicate)}'")
        if object:
            conditions.append(f"object LIKE '%{sql_escape(object)}%'")
        if entity_type:
            rdf_type = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
            conditions.append(
                f"subject IN (SELECT subject FROM {table} "
                f"WHERE predicate = '{rdf_type}' AND object LIKE '%{sql_escape(entity_type)}%')"
            )
        if search:
            escaped = sql_escape(search)
            conditions.append(
                f"(subject LIKE '%{escaped}%' OR predicate LIKE '%{escaped}%' OR object LIKE '%{escaped}%')"
            )

        total = store.paginated_count(table, conditions)
        rows = store.paginated_triples(table, conditions, limit, offset)

        return TriplesResponse(
            success=True,
            triples=[TripleRow(subject=r.get('subject', ''), predicate=r.get('predicate', ''),
                               object=r.get('object', '')) for r in rows],
            count=len(rows),
            total=total,
        )
    except Exception as e:
        logger.exception("dt_triples failed: %s", e)
        error_msg = str(e)
        if 'TABLE_OR_VIEW_NOT_FOUND' in error_msg or 'does not exist' in error_msg.lower():
            raise NotFoundError(f"{table} does not exist. Run build first.") from e
        raise InfrastructureError("Triple retrieval failed", detail=error_msg) from e


# ---------------------------------------------------------------------------
# POST /dataquality/start
# ---------------------------------------------------------------------------

@router.post(
    "/dataquality/start",
    response_model=DataQualityStartedResponse,
    summary="Run data quality checks",
    description="Start SHACL-based data quality checks as an asynchronous task. "
                "Evaluates all enabled SHACL shapes (or a filtered category) against "
                "the triple store and returns a task_id to poll for progress.",
)
async def dt_dataquality_start(
    body: DataQualityRequest = DataQualityRequest(),
    domain_name: Optional[str] = Query(
        None,
        validation_alias=AliasChoices("domain_name", "project_name"),
        description="Domain name in the registry (uses current session domain if omitted)",
    ),
    domain_version: Optional[str] = Query(
        None,
        validation_alias=AliasChoices("domain_version", "project_version"),
        description="Domain version to load (uses latest version if omitted)",
    ),
    registry_catalog: Optional[str] = Query(None, description="Override registry catalog"),
    registry_schema: Optional[str] = Query(None, description="Override registry schema"),
    registry_volume: Optional[str] = Query(None, description="Override registry volume"),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    import threading
    from back.core.task_manager import get_task_manager
    domain = DigitalTwin.resolve_domain(
        domain_name, session_mgr, settings,
        registry_catalog, registry_schema, registry_volume, domain_version,
    )

    shapes = domain.shacl_shapes
    if body.category:
        shapes = [s for s in shapes if s.get("category") == body.category]
    shapes = [s for s in shapes if s.get("enabled", True)]

    swrl_rules = domain.swrl_rules or []
    ontology_dict = getattr(domain, 'ontology', None)
    if not isinstance(ontology_dict, dict):
        ontology_dict = domain._data.get('ontology', {}) if hasattr(domain, '_data') else {}

    if not shapes and not swrl_rules:
        return DataQualityStartedResponse(
            success=False,
            message="No enabled shapes or SWRL rules to check"
            + (f" (category={body.category})" if body.category else ""),
        )

    view_table = effective_view_table(domain, settings).strip()
    graph_name = effective_graph_name(domain)
    triplestore_table = graph_name if body.backend == "graph" else view_table
    if not triplestore_table:
        raise ValidationError("Triple store not configured")

    total = len(shapes) + len(swrl_rules)
    domain_snap = DigitalTwin.make_snapshot(domain)

    tm = get_task_manager()
    task = tm.create_task(
        name="Data Quality Checks (API)",
        task_type="dataquality_checks",
        steps=[{"name": "running", "description": f"Running {total} quality checks"}],
    )

    requested_backend = body.backend

    def _run():
        import time
        from back.core.triplestore import get_triplestore as _get_ts

        t0 = time.time()
        try:
            tm.start_task(task.id, f"Running {total} data quality checks ({requested_backend})...")

            store = _get_ts(domain_snap, settings, backend=requested_backend)
            if not store:
                tm.fail_task(task.id, f"Could not initialize {requested_backend} backend")
                return

            if requested_backend == "graph":
                DigitalTwin.run_graph_checks(
                    tm, task, shapes, store, triplestore_table, domain_snap, t0, total,
                    swrl_rules=swrl_rules, ontology=ontology_dict,
                )
            else:
                DigitalTwin.run_sql_checks(
                    tm, task, shapes, triplestore_table, store, t0, total,
                    swrl_rules=swrl_rules, ontology=ontology_dict,
                )
        except Exception as exc:
            logger.exception("API data quality checks failed: %s", exc)
            tm.fail_task(task.id, str(exc))

    threading.Thread(target=_run, daemon=True).start()

    return DataQualityStartedResponse(
        success=True,
        task_id=task.id,
        shape_count=total,
        message=f"Data quality checks started ({total} shapes, backend={body.backend})",
    )


# ---------------------------------------------------------------------------
# GET /dataquality/{task_id}
# ---------------------------------------------------------------------------

@router.get(
    "/dataquality/{task_id}",
    response_model=TaskProgressResponse,
    summary="Poll data quality progress",
    description="Check the progress of a previously started data quality check. "
                "Returns status, progress percentage, and results when completed.",
)
async def dt_dataquality_progress(task_id: str):
    return _poll_task(task_id)


# ---------------------------------------------------------------------------
# POST /inference/start
# ---------------------------------------------------------------------------

@router.post(
    "/inference/start",
    response_model=InferenceStartedResponse,
    summary="Run inference",
    description="Start OWL 2 RL inference, SWRL rule execution, graph reasoning, "
                "constraint checking, SHACL inference rules, decision tables, "
                "SPARQL CONSTRUCT rules, and aggregate rules as an asynchronous task. "
                "Each phase can be toggled on or off. Optionally append inferred "
                "triples to the knowledge graph via ``append_graph``. "
                "Returns a task_id to poll.",
)
async def dt_inference_start(
    body: InferenceRequest = InferenceRequest(),
    domain_name: Optional[str] = Query(
        None,
        validation_alias=AliasChoices("domain_name", "project_name"),
        description="Domain name in the registry (uses current session domain if omitted)",
    ),
    domain_version: Optional[str] = Query(
        None,
        validation_alias=AliasChoices("domain_version", "project_version"),
        description="Domain version to load (uses latest version if omitted)",
    ),
    registry_catalog: Optional[str] = Query(None, description="Override registry catalog"),
    registry_schema: Optional[str] = Query(None, description="Override registry schema"),
    registry_volume: Optional[str] = Query(None, description="Override registry volume"),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    import threading
    from back.core.task_manager import get_task_manager
    from back.core.helpers import get_databricks_client

    domain = DigitalTwin.resolve_domain(
        domain_name, session_mgr, settings,
        registry_catalog, registry_schema, registry_volume, domain_version,
    )
    domain.ensure_generated_content()
    domain_snap = DigitalTwin.make_snapshot(domain)

    options = {
        "tbox": body.tbox,
        "swrl": body.swrl,
        "graph": body.graph,
        "constraints": body.constraints,
        "decision_tables": body.decision_tables,
        "sparql_rules": body.sparql_rules,
        "aggregate_rules": body.aggregate_rules,
        "append_graph": body.append_graph,
        "materialize": body.materialize,
        "materialize_table": (body.materialize_table or "").strip(),
    }

    tm = get_task_manager()
    task = tm.create_task(
        name="Inference (API)",
        task_type="reasoning",
        steps=[{"name": "running", "description": "Running inference phases"}],
    )

    def _run():
        try:
            logger.info("API inference task %s: starting", task.id)
            tm.start_task(task.id)
            tm.update_progress(task.id, 10, "Initialising triple store")

            store = get_triplestore(domain_snap, settings, backend="graph")
            if store is None:
                logger.info("API inference task %s: graph store unavailable, falling back to view", task.id)
                store = get_triplestore(domain_snap, settings, backend="view")

            from back.core.reasoning import ReasoningService
            svc = ReasoningService(domain_snap, store)
            tm.update_progress(task.id, 30, "Running inference phases")

            logger.info(
                "API inference task %s: phases tbox=%s swrl=%s graph=%s constraints=%s "
                "decision_tables=%s sparql_rules=%s aggregate_rules=%s",
                task.id, options["tbox"], options["swrl"],
                options["graph"], options["constraints"],
                options["decision_tables"],
                options["sparql_rules"], options["aggregate_rules"],
            )

            def _swrl_progress(idx, total, rule_name):
                pct = 30 + int((idx / max(total, 1)) * 50)
                tm.update_progress(task.id, pct, f"SWRL {idx + 1}/{total}: {rule_name}")

            result = svc.run_full_reasoning(options, progress_callback=_swrl_progress)
            logger.info(
                "API inference task %s: done — %d inferred, %d violations",
                task.id, len(result.inferred_triples), len(result.violations),
            )

            tm.update_progress(task.id, 90, "Finalising")

            result_dict = result.to_dict()
            import datetime as _dt
            result_dict["last_run"] = _dt.datetime.utcnow().isoformat()
            result_dict["inferred_count"] = len(result.inferred_triples)
            result_dict["violations_count"] = len(result.violations)

            if options.get("append_graph") and result.inferred_triples:
                tm.update_progress(task.id, 92, "Appending inferred triples to graph...")
                try:
                    graph_store = get_triplestore(domain_snap, settings, backend="graph")
                    if graph_store is None:
                        logger.warning("API inference %s: cannot append to graph — store unavailable", task.id)
                        result_dict["append_graph_error"] = "Graph store not available"
                    else:
                        from back.core.reasoning.models import ReasoningResult as _RR
                        append_count = ReasoningService(domain_snap, graph_store).materialize_inferred(
                            _RR(inferred_triples=result.inferred_triples)
                        )
                        result_dict["append_graph_count"] = append_count
                        logger.info("API inference %s: appended %d triples to graph", task.id, append_count)
                except Exception as ag_err:
                    logger.exception("API inference %s: append to graph failed: %s", task.id, ag_err)
                    result_dict["append_graph_error"] = str(ag_err)

            mat_table = options.get("materialize_table", "")
            if options.get("materialize") and mat_table and len(mat_table.split(".")) == 3:
                tm.update_progress(task.id, 95, f"Materialising to {mat_table}...")

                triples = [
                    {"subject": t.subject, "predicate": t.predicate, "object": t.object}
                    for t in result.inferred_triples
                    if is_uri(t.subject) and is_uri(t.predicate) and is_uri(t.object)
                ]
                if triples:
                    try:
                        client = get_databricks_client(domain_snap, settings)
                        if client is None:
                            logger.warning("API inference %s: cannot materialise — no credentials", task.id)
                        else:
                            count = ReasoningService.materialize_to_delta(client, mat_table, triples)
                            result_dict["materialize_count"] = count
                            result_dict["materialize_table"] = mat_table
                            logger.info("API inference %s: materialised %d triples to %s", task.id, count, mat_table)
                    except Exception as mat_err:
                        logger.exception("API inference %s: materialisation failed: %s", task.id, mat_err)
                        result_dict["materialize_error"] = str(mat_err)

            tm.complete_task(
                task.id,
                result=result_dict,
                message=(
                    f"Inference complete: {len(result.inferred_triples)} inferred, "
                    f"{len(result.violations)} violations"
                ),
            )
            logger.info("API inference task %s: completed", task.id)
        except Exception as e:
            logger.exception("API inference task %s failed: %s", task.id, e)
            tm.fail_task(task.id, error=str(e))

    threading.Thread(target=_run, daemon=True).start()

    enabled = [k for k in ("tbox", "swrl", "graph", "constraints") if options.get(k)]
    return InferenceStartedResponse(
        success=True,
        task_id=task.id,
        message=f"Inference started (phases: {', '.join(enabled)})",
    )


# ---------------------------------------------------------------------------
# GET /inference/results  (registered before {task_id} to avoid path collision)
# ---------------------------------------------------------------------------

@router.get(
    "/inference/results",
    response_model=InferenceResultResponse,
    summary="Get inference results (stub)",
    description="Inference results are not persisted in the domain session. "
                "Poll ``GET /digitaltwin/inference/{task_id}`` for the completed run payload.",
)
async def dt_inference_results(
    domain_name: Optional[str] = Query(
        None,
        validation_alias=AliasChoices("domain_name", "project_name"),
        description="Domain name in the registry (uses current session domain if omitted)",
    ),
    domain_version: Optional[str] = Query(
        None,
        validation_alias=AliasChoices("domain_version", "project_version"),
        description="Domain version to load (uses latest version if omitted)",
    ),
    registry_catalog: Optional[str] = Query(None, description="Override registry catalog"),
    registry_schema: Optional[str] = Query(None, description="Override registry schema"),
    registry_volume: Optional[str] = Query(None, description="Override registry volume"),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    _ = DigitalTwin.resolve_domain(
        domain_name, session_mgr, settings,
        registry_catalog, registry_schema, registry_volume, domain_version,
    )
    return InferenceResultResponse(
        success=True,
        message="Inference results are not stored in the session. Use GET /digitaltwin/inference/{task_id} after the run completes.",
        inferred_count=0,
        violations_count=0,
        reasoning=None,
    )


# ---------------------------------------------------------------------------
# GET /inference/{task_id}
# ---------------------------------------------------------------------------

@router.get(
    "/inference/{task_id}",
    response_model=TaskProgressResponse,
    summary="Poll inference progress",
    description="Check the progress of a previously started inference task. "
                "Returns status, progress percentage, and results when completed.",
)
async def dt_inference_progress(task_id: str):
    return _poll_task(task_id)
