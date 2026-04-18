"""
Internal API -- Digital Twin / query JSON endpoints.

Moved from app/frontend/digitaltwin/routes.py during the front/back split.
"""
from fastapi import APIRouter, Request, Depends
from back.core.logging import get_logger
from back.core.errors import (
    AuthorizationError,
    InfrastructureError,
    NotFoundError,
    ValidationError,
)
from back.objects.registry import ROLE_BUILDER, role_level
from shared.config.constants import DEFAULT_BASE_URI, DEFAULT_GRAPH_NAME
from back.objects.session import SessionManager, get_session_manager, get_domain
from shared.config.settings import get_settings, Settings
from back.core.w3c import sparql, uri_local_name
from back.core.databricks import DatabricksClient, is_databricks_app
from back.core.triplestore import get_triplestore
from back.objects.digitaltwin import DigitalTwin, DomainSnapshot
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
    settings: Settings = Depends(get_settings)
):
    """Execute a SPARQL query via Spark SQL."""
    data = await request.json()
    query = data.get('query', '')
    limit = data.get('limit')

    if not query:
        raise ValidationError("No query provided")

    domain = get_domain(session_mgr)
    domain.ensure_generated_content()
    r2rml_content = domain.get_r2rml()

    if not r2rml_content:
        raise ValidationError("No R2RML mapping available. Please configure ontology and mappings first.")

    return await DigitalTwin(domain).execute_spark_query(query, r2rml_content, limit, settings)


@router.post("/translate")
async def translate_sparql(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Translate a SPARQL query to SQL without executing."""
    data = await request.json()
    sparql_query = data.get('query', '')
    limit = data.get('limit')

    if not sparql_query:
        raise ValidationError("No SPARQL query provided")

    domain = get_domain(session_mgr)
    domain.ensure_generated_content()
    r2rml_content = domain.get_r2rml()

    if not r2rml_content:
        raise ValidationError("No R2RML mapping available. Please configure mappings first.")

    entity_mappings, relationship_mappings = sparql.extract_r2rml_mappings(r2rml_content)
    base_uri = domain.ontology.get('base_uri', DEFAULT_BASE_URI)

    entity_mappings = DigitalTwin.augment_mappings_from_config(
        entity_mappings, domain.assignment, base_uri, domain.ontology
    )
    relationship_mappings = DigitalTwin.augment_relationships_from_config(
        relationship_mappings, domain.assignment, base_uri, domain.ontology
    )

    return sparql.translate_sparql_to_spark(sparql_query, entity_mappings, limit, relationship_mappings)


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
    base_uri = domain.ontology.get('base_uri', DEFAULT_BASE_URI).rstrip('#') + '#'

    groups = []
    for g in domain.groups:
        members = g.get('members', [])
        member_uris = [
            m if m.startswith('http') else (base_uri + m)
            for m in members if m
        ]
        groups.append({
            'name': g.get('name', ''),
            'label': g.get('label', g.get('name', '')),
            'color': g.get('color', ''),
            'icon': g.get('icon', ''),
            'members': members,
            'memberUris': member_uris,
        })

    return {'success': True, 'groups': groups}


# ===========================================
# Triple Store Sync
# ===========================================

@router.post("/sync/start")
async def start_triplestore_sync(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
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

    effective_role = getattr(request.state, 'user_domain_role', None) or getattr(request.state, 'user_role', '')
    if role_level(effective_role) < role_level(ROLE_BUILDER):
        raise AuthorizationError("Only builders and admins can build a digital twin")

    data = await request.json()
    build_mode = data.get('build_mode', 'incremental')
    force_full = build_mode == 'full' or data.get('drop_existing', False)

    domain = get_domain(session_mgr)

    view_table = effective_view_table(domain)
    graph_name = effective_graph_name(domain)

    parts = view_table.split('.')
    if len(parts) != 3:
        raise ValidationError("View location must be fully qualified: catalog.schema.view_name (configure in Domain / Triple Store tab)")

    domain.ensure_generated_content()
    r2rml_content = domain.get_r2rml()

    if not r2rml_content:
        raise ValidationError("No R2RML mapping available. Please ensure ontology and assignments are configured.")

    host, token, warehouse_id = get_databricks_credentials(domain, settings)
    if not host and not is_databricks_app():
        raise ValidationError("Databricks not configured")
    if not token and not is_databricks_app():
        raise ValidationError("Databricks not configured")
    if not warehouse_id:
        raise ValidationError("No SQL warehouse configured")

    config_changed = (domain.last_update or '') > (domain.last_build or '') if domain.last_update else False

    domain.triplestore.pop('stats', None)
    domain.triplestore.pop('_ts_cache_timestamp', None)
    if domain.last_update:
        domain.triplestore['build_last_update'] = domain.last_update

    from datetime import datetime, timezone as tz
    domain.last_build = datetime.now(tz.utc).isoformat()
    domain.save()

    base_uri = domain.ontology.get('base_uri', DEFAULT_BASE_URI)
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
            {'name': 'prepare', 'description': 'Preparing mappings and generating SQL'},
            {'name': 'gate', 'description': 'Checking source tables for changes'},
            {'name': 'view', 'description': 'Creating Triple-Store VIEW in Unity Catalog'},
            {'name': 'diff', 'description': 'Computing incremental diff'},
            {'name': 'graph', 'description': 'Applying changes to LadybugDB graph'},
            {'name': 'snapshot', 'description': 'Refreshing snapshot table'},
            {'name': 'archive', 'description': 'Archiving graph to registry'},
        ]
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

    return {
        'success': True,
        'task_id': task.id,
        'message': 'Sync started'
    }


@router.post("/sync/load")
async def load_triplestore(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
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
            if 'does not exist' in error_msg.lower():
                raise NotFoundError(
                    f"Graph {graph_name} does not exist. Run Build first.",
                    detail=error_msg,
                )
            raise InfrastructureError("Error reading graph from the graph backend", detail=error_msg)

        return {
            'success': True,
            'results': results,
            'columns': ['subject', 'predicate', 'object'],
            'count': len(results),
        }

    except (ValidationError, InfrastructureError, NotFoundError):
        raise
    except Exception as e:
        logger.exception("Load graph failed: %s", e)
        raise InfrastructureError("Error loading graph from the triple store", detail=str(e))


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
            store, graph_name,
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


@router.post("/sync/filter")
async def filter_triplestore(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
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
        phase = data.get('phase', 'preview')

        domain = get_domain(session_mgr)
        graph_name = effective_graph_name(domain)
        if not graph_name:
            raise ValidationError("Graph name is not configured")

        store = get_triplestore(domain, settings, backend="graph")
        if not store:
            raise InfrastructureError("Graph backend is not configured")

        # ── Phase 1: preview (seed search → flat list) ──────────────
        if phase == "preview":
            entity_type = (data.get('entity_type') or '').strip()
            field = data.get('field', 'any')
            match_type = data.get('match_type', 'contains')
            value = (data.get('value') or '').strip()

            if not entity_type and not value:
                raise ValidationError("Please specify an entity type or search value.")

            logger.info(
                "Filter preview – type=%s, field=%s, match=%s, value=%s",
                entity_type, field, match_type, value,
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
                if 'does not exist' in msg.lower():
                    raise NotFoundError(
                        f"Graph {graph_name} does not exist. Run Build first.",
                        detail=msg,
                    )
                raise InfrastructureError("Error querying graph", detail=msg)

            if not entity_set:
                return {
                    'success': True,
                    'phase': 'preview',
                    'seeds': [],
                    'total': 0,
                    'capped': False,
                    'message': 'No entities found matching the filter criteria.',
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
                raise InfrastructureError("Error fetching entity metadata", detail=str(e))

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

            logger.info("Filter preview – %d seeds returned (total=%d, capped=%s)",
                        len(seeds), len(entity_set), capped)

            return {
                'success': True,
                'phase': 'preview',
                'seeds': seeds,
                'total': len(entity_set),
                'capped': capped,
            }

        # ── Phase 2: expand (selected URIs → full graph) ────────────
        selected_uris = data.get('selected_uris', [])
        if not selected_uris:
            raise ValidationError("No entities selected for expansion.")

        include_rels = data.get('include_rels', True)
        depth = min(int(data.get('depth', 3)), 5)
        client_max = int(data.get('max_entities', 5000))
        max_entities = max(100, min(client_max, 50_000))

        entity_set: set = set(selected_uris)
        initial_count = len(entity_set)
        capped = False

        logger.info("Filter expand – %d selected URIs, depth=%d, max=%d",
                     initial_count, depth, max_entities)

        if include_rels and depth > 0:
            current_level = set(entity_set)
            for d in range(depth):
                if not current_level or len(entity_set) >= max_entities:
                    break
                logger.debug("Filter expand – level %d (%d entities so far)", d + 1, len(entity_set))
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
            len(entity_set), initial_count, len(entity_set) - initial_count, capped,
        )
        try:
            results = store.get_triples_for_subjects(graph_name, list(entity_set))
        except (ValidationError, InfrastructureError, NotFoundError):
            raise
        except Exception as e:
            logger.exception("Filter final query failed: %s", e)
            raise InfrastructureError("Error fetching triples for the filter", detail=str(e))

        max_triples = 100_000
        if len(results) > max_triples:
            results = results[:max_triples]
            capped = True

        return {
            'success': True,
            'phase': 'expand',
            'results': results,
            'columns': ['subject', 'predicate', 'object'],
            'count': len(results),
            'initial_count': initial_count,
            'expanded_count': len(entity_set),
            'capped': capped,
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
    return {'needs_rebuild': needs_rebuild}


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
        raise InfrastructureError("Could not retrieve triple store status", detail=str(e))


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
        logger.debug("sync_info: _schedule_sync took %.0fms", (_t.monotonic() - t_s) * 1000)

    async def _graph_status():
        t_s = _t.monotonic()
        out = await dt.get_or_fetch_graph_status(settings)
        logger.debug("sync_info: graph status took %.0fms", (_t.monotonic() - t_s) * 1000)
        return out

    async def _dt_exist():
        t_s = _t.monotonic()
        out = await dt.get_or_fetch_dt_existence(settings)
        logger.debug("sync_info: dt existence took %.0fms", (_t.monotonic() - t_s) * 1000)
        return out

    _, ts_status, dt_exist = await asyncio.gather(
        _schedule_sync(),
        _graph_status(),
        _dt_exist(),
    )

    if domain.last_build and domain.last_build != last_build:
        last_build = domain.last_build
        needs_rebuild = last_update > last_build if last_update and last_build else needs_rebuild
        dt_exist['last_built'] = last_build

    logger.info(
        "sync_info: total=%.0fms (prep=%.0fms, parallel I/O=%.0fms)",
        (_t.monotonic() - t0) * 1000,
        (t_prep - t0) * 1000,
        (_t.monotonic() - t_prep) * 1000,
    )

    return {
        'readiness': readiness,
        'triplestore_status': ts_status,
        'domain_info': domain_info_data,
        'dt_existence': dt_exist,
        'changes': {'needs_rebuild': needs_rebuild},
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


@router.post("/sync/reload-from-registry")
async def reload_graph_from_registry(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Download the LadybugDB archive from the registry volume and extract it locally."""
    from back.objects.domain import Domain
    from back.objects.registry import ROLE_BUILDER, role_level
    effective_role = getattr(request.state, 'user_domain_role', None) or getattr(request.state, 'user_role', '')
    if role_level(effective_role) < role_level(ROLE_BUILDER):
        raise AuthorizationError("Only builders and admins can reload the graph from registry")

    domain = get_domain(session_mgr)
    uc = make_volume_file_service(domain, settings)
    if not uc.is_configured():
        raise ValidationError("Databricks credentials are not configured")
    warning = Domain(domain).sync_ladybug_from_volume(uc)
    if warning:
        logger.warning("reload-from-registry failed: %s", warning)
        raise InfrastructureError("Failed to reload graph from registry", detail=warning)

    logger.info("reload-from-registry succeeded for domain '%s'", domain.domain_folder)
    return {'success': True, 'message': 'Graph reloaded from registry'}


@router.post("/sync/drop-snapshot")
async def drop_snapshot(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Drop the incremental snapshot table to force a full rebuild on next build."""
    from back.objects.registry import ROLE_BUILDER, role_level
    effective_role = getattr(request.state, 'user_domain_role', None) or getattr(request.state, 'user_role', '')
    if role_level(effective_role) < role_level(ROLE_BUILDER):
        raise AuthorizationError("Only builders and admins can drop the snapshot")

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

    return {'success': True, 'message': f'Snapshot {snapshot_table} dropped'}


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
            cached = DigitalTwin(domain).get_ts_cache('stats')
            if cached:
                preds = cached.get('top_predicates') or []
                has_kind = preds and 'kind' in preds[0]
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

        type_count = sum(int(r.get('cnt', 0)) for r in entity_types)
        relationship_count = total_count - type_count - label_count

        classified = DigitalTwin(domain).classify_predicates(top_predicates)

        result = {
            'success': True,
            'total_triples': total_count,
            'distinct_subjects': subject_count,
            'distinct_predicates': predicate_count,
            'entity_types': [{'uri': r['type_uri'], 'count': int(r['cnt'])} for r in entity_types],
            'top_predicates': classified,
            'label_count': label_count,
            'type_assertion_count': type_count,
            'relationship_count': max(relationship_count, 0),
        }
        DigitalTwin(domain).set_ts_cache('stats', result)
        return result
    except (ValidationError, InfrastructureError, NotFoundError):
        raise
    except Exception as e:
        logger.exception("Triplestore stats failed: %s", e)
        raise InfrastructureError("Error retrieving triple store statistics", detail=str(e))


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
            raise ValidationError(f"Cannot translate shape {shape.get('id', '?')} to SQL")
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
    ontology_dict = getattr(domain, 'ontology', None)
    if not isinstance(ontology_dict, dict):
        ontology_dict = domain._data.get('ontology', {}) if hasattr(domain, '_data') else {}
    decision_tables = [dt for dt in ontology_dict.get("decision_tables", []) if dt.get("enabled", True)]
    aggregate_rules = [r for r in ontology_dict.get("aggregate_rules", []) if r.get("enabled", True)]

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
    return {"success": True, "task_id": task.id, "message": f"Data quality checks started ({total} checks)"}


# ===========================================
# Legacy Quality Routes (kept for backward-compat)
# ===========================================

@router.post("/quality/execute", deprecated=True)
async def execute_quality_check(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Execute a quality check against the triple store table.

    .. deprecated::
        Use ``/dataquality/*`` endpoints instead (SHACL-driven data quality).
    """
    logger.warning("Deprecated endpoint %s — use /dataquality/* instead", request.url.path)
    try:
        data = await request.json()
        check_type = data.get('check_type', '')
        triplestore_table = data.get('triplestore_table', '').strip()
        params = data.get('params', {})

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
            if 'TABLE_OR_VIEW_NOT_FOUND' in error_msg or 'does not exist' in error_msg.lower():
                raise NotFoundError(
                    f"View {triplestore_table} does not exist. Please build first.",
                    detail=error_msg,
                )
            raise InfrastructureError("Quality check query execution failed", detail=error_msg)

        return {
            'success': True,
            'violations': results or [],
            'count': len(results) if results else 0,
            'sql': sql
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
    settings: Settings = Depends(get_settings)
):
    """Start all quality checks as an asynchronous task.

    .. deprecated::
        Use ``/dataquality/*`` endpoints instead (SHACL-driven data quality).
    """
    logger.warning("Deprecated endpoint %s — use /dataquality/* instead", request.url.path)
    import threading
    from back.core.task_manager import get_task_manager

    data = await request.json()
    triplestore_table = data.get('triplestore_table', '').strip()
    checks = data.get('checks', [])

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
        steps=[{'name': 'running', 'description': f'Running {total_checks} quality checks'}],
    )

    def run_checks():
        import time
        start_time = time.time()

        try:
            tm.start_task(task.id, f"Running {total_checks} quality checks...")

            from back.core.triplestore import get_triplestore as _get_ts
            store = _get_ts(domain_snap, settings, backend="view")
            if not store:
                tm.fail_task(task.id, 'Could not initialize view backend (check Databricks connection)')
                return

            pop_cache = {}
            results = []
            for idx, check in enumerate(checks):
                check_type = check.get('check_type', '')
                params = check.get('params', {})
                name = check.get('name', f'Check {idx + 1}')
                category = check.get('category', 'unknown')

                progress = int(((idx) / total_checks) * 100)
                tm.update_progress(task.id, progress, f"Running check {idx + 1}/{total_checks}: {name}")

                sql = DigitalTwin.build_quality_sql(check_type, triplestore_table, params)
                if not sql:
                    results.append({
                        'name': name, 'category': category, 'status': 'info',
                        'message': f'Unsupported check type: {check_type}',
                        'violations': [], 'sql': ''
                    })
                    continue

                class_uri = params.get('class_uri', '')

                try:
                    violations = store.execute_query(sql) or []
                    if len(violations) > 0:
                        result = {
                            'name': name, 'category': category, 'status': 'error',
                            'message': check.get('error_message', '').replace('{count}', str(len(violations))) or f'{len(violations)} violations found',
                            'violations': violations, 'sql': sql
                        }
                    else:
                        result = {
                            'name': name, 'category': category, 'status': 'success',
                            'message': check.get('success_message', 'No violations found'),
                            'violations': [], 'sql': sql
                        }
                    pop = DigitalTwin._count_class_population_sql(
                        store, triplestore_table, class_uri, pop_cache
                    )
                    DigitalTwin._enrich_with_population(result, pop)
                    results.append(result)
                except Exception as e:
                    error_msg = str(e)
                    if 'TABLE_OR_VIEW_NOT_FOUND' in error_msg or 'does not exist' in error_msg.lower():
                        tm.fail_task(task.id, f'View {triplestore_table} does not exist. Please build first.')
                        return
                    results.append({
                        'name': name, 'category': category, 'status': 'warning',
                        'message': 'Could not validate: the data source returned an error.',
                        'violations': [], 'sql': sql
                    })

            DigitalTwin.complete_dq_task(tm, task, results, time.time() - start_time)

        except Exception as e:
            logger.exception("Quality checks failed: %s", e)
            tm.fail_task(task.id, "Quality checks failed")

    thread = threading.Thread(target=run_checks, daemon=True)
    thread.start()

    return {
        'success': True,
        'task_id': task.id,
        'message': f'Quality checks started ({total_checks} checks)'
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
        t for t in raw_triples
        if is_uri(t.get("subject", "")) and is_uri(t.get("predicate", "")) and is_uri(t.get("object", ""))
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
                count = ReasoningService.materialize_to_delta(client, mat_table, uri_triples)
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
