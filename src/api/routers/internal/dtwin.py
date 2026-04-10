"""
Internal API -- Digital Twin / query JSON endpoints.

Moved from app/frontend/digitaltwin/routes.py during the front/back split.
"""
from fastapi import APIRouter, Request, Depends
from back.core.logging import get_logger
from back.core.errors import ValidationError
from shared.config.constants import DEFAULT_BASE_URI, DEFAULT_GRAPH_NAME
from back.objects.session import SessionManager, get_session_manager
from shared.config.settings import get_settings, Settings
from back.core.w3c import sparql
from back.core.databricks import is_databricks_app
from back.core.databricks import DatabricksClient
from back.core.triplestore import get_triplestore
from back.objects.digitaltwin import DigitalTwin
from back.objects.digitaltwin.models import ProjectSnapshot
from back.objects.session import get_project
from back.core.helpers import get_databricks_client, get_databricks_credentials, run_blocking, effective_view_table, effective_graph_name, is_uri

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

    project = get_project(session_mgr)
    project.ensure_generated_content()
    r2rml_content = project.get_r2rml()

    if not r2rml_content:
        raise ValidationError("No R2RML mapping available. Please configure ontology and mappings first.")

    return await DigitalTwin(project).execute_spark_query(query, r2rml_content, limit, settings)


@router.post("/translate")
async def translate_sparql(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Translate a SPARQL query to SQL without executing."""
    data = await request.json()
    sparql_query = data.get('query', '')
    limit = data.get('limit')

    if not sparql_query:
        raise ValidationError("No SPARQL query provided")

    project = get_project(session_mgr)
    project.ensure_generated_content()
    r2rml_content = project.get_r2rml()

    if not r2rml_content:
        raise ValidationError("No R2RML mapping available. Please configure mappings first.")

    entity_mappings, relationship_mappings = sparql.extract_r2rml_mappings(r2rml_content)
    base_uri = project.ontology.get('base_uri', DEFAULT_BASE_URI)

    entity_mappings = DigitalTwin.augment_mappings_from_config(
        entity_mappings, project.assignment, base_uri, project.ontology
    )
    relationship_mappings = DigitalTwin.augment_relationships_from_config(
        relationship_mappings, project.assignment, base_uri, project.ontology
    )

    return sparql.translate_sparql_to_spark(sparql_query, entity_mappings, limit, relationship_mappings)


# ===========================================
# Triple Store Sync
# ===========================================

@router.post("/sync/start")
async def start_triplestore_sync(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Start async dual digital twin build: CREATE VIEW (zero-copy) then populate LadybugDB graph.

    Supports two modes controlled by the ``build_mode`` body parameter:

    * ``"incremental"`` (default) — version-gate check + server-side diff
      via a Delta snapshot table.  Falls back to full when no snapshot
      exists or when the diff exceeds a threshold.
    * ``"full"`` — drop and recreate the graph (legacy behaviour).
    """
    import threading
    from back.core.task_manager import get_task_manager

    data = await request.json()
    build_mode = data.get('build_mode', 'incremental')
    force_full = build_mode == 'full' or data.get('drop_existing', False)

    project = get_project(session_mgr)

    view_table = effective_view_table(project)
    graph_name = effective_graph_name(project)

    parts = view_table.split('.')
    if len(parts) != 3:
        raise ValidationError("View location must be fully qualified: catalog.schema.view_name (configure in Project / Triple Store tab)")

    project.ensure_generated_content()
    r2rml_content = project.get_r2rml()

    if not r2rml_content:
        raise ValidationError("No R2RML mapping available. Please ensure ontology and assignments are configured.")

    host, token, warehouse_id = get_databricks_credentials(project, settings)
    if not host and not is_databricks_app():
        raise ValidationError("Databricks not configured")
    if not token and not is_databricks_app():
        raise ValidationError("Databricks not configured")
    if not warehouse_id:
        raise ValidationError("No SQL warehouse configured")

    config_changed = (project.last_update or '') > (project.last_build or '') if project.last_update else False

    project.triplestore.pop('stats', None)
    if project.last_update:
        project.triplestore['build_last_update'] = project.last_update

    from datetime import datetime, timezone as tz
    project.last_build = datetime.now(tz.utc).isoformat()
    project.save()

    base_uri = project.ontology.get('base_uri', DEFAULT_BASE_URI)
    mapping_config = project.assignment
    ontology_config = project.ontology
    stored_source_versions = dict(project.source_versions or {})
    delta_cfg = project.delta or {}
    proj_snap = ProjectSnapshot(project)

    tm = get_task_manager()
    task = tm.create_task(
        name="Digital Twin Build",
        task_type="triplestore_sync",
        steps=[
            {'name': 'prepare', 'description': 'Preparing mappings and generating SQL'},
            {'name': 'gate', 'description': 'Checking source tables for changes'},
            {'name': 'view', 'description': 'Creating zero-copy VIEW in Unity Catalog'},
            {'name': 'diff', 'description': 'Computing incremental diff'},
            {'name': 'graph', 'description': 'Applying changes to LadybugDB graph'},
            {'name': 'snapshot', 'description': 'Refreshing snapshot table'},
        ]
    )

    def run_sync():
        import time
        start_time = time.time()
        actual_mode = 'full' if force_full or config_changed else 'incremental'

        try:
            tm.start_task(task.id, "Preparing mappings...")
            source_client = DatabricksClient(host=host, token=token, warehouse_id=warehouse_id)

            entity_mappings, relationship_mappings = sparql.extract_r2rml_mappings(r2rml_content)
            entity_mappings = DigitalTwin.augment_mappings_from_config(
                entity_mappings, mapping_config, base_uri, ontology_config
            )
            relationship_mappings = DigitalTwin.augment_relationships_from_config(
                relationship_mappings, mapping_config, base_uri, ontology_config
            )

            if not entity_mappings and not relationship_mappings:
                tm.fail_task(task.id, 'No valid mappings found')
                return

            all_data_sparql = (
                f"PREFIX : <{base_uri}>\n"
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n\n"
                "SELECT ?subject ?predicate ?object\n"
                "WHERE {\n"
                "    ?subject ?predicate ?object .\n"
                "}"
            )

            result = sparql.translate_sparql_to_spark(
                all_data_sparql, entity_mappings, None, relationship_mappings, dialect='spark'
            )

            if not result['success']:
                tm.fail_task(task.id, result.get('message', 'Failed to translate SPARQL to SQL'))
                return

            spark_sql = result['sql']
            tm.update_progress(task.id, 10, "SQL generated")

            # --- Phase 1: Version gate (incremental only) ---
            from back.core.triplestore import IncrementalBuildService
            incr_svc = IncrementalBuildService(source_client)

            new_source_versions = {}
            if actual_mode == 'incremental':
                tm.advance_step(task.id, "Checking source tables for changes...")
                source_tables = incr_svc.extract_source_tables(mapping_config)
                logger.info("Incremental gate: checking %d source tables", len(source_tables))

                changed, new_source_versions = incr_svc.check_source_versions(
                    source_tables, stored_source_versions
                )
                if not changed:
                    duration = time.time() - start_time
                    logger.info("Incremental gate: no source changes detected — skipping build")
                    tm.complete_task(task.id, result={
                        'triple_count': 0,
                        'view_table': view_table,
                        'graph_name': graph_name,
                        'build_mode': 'skipped',
                        'skipped_reason': 'No source table changes detected',
                        'duration_seconds': duration,
                    }, message='No source data changes — build skipped')
                    return

                tm.update_progress(task.id, 15, "Source changes detected")

            # --- Phase 2: Refresh VIEW ---
            tm.advance_step(task.id, f"Creating VIEW {view_table}...")
            try:
                catalog, schema, vname = parts
                view_ok, view_msg = source_client.create_or_replace_view(catalog, schema, vname, spark_sql)
                if not view_ok:
                    logger.error("Failed to create VIEW %s: %s", view_table, view_msg)
                    tm.fail_task(task.id, f'Failed to create VIEW: {view_msg}')
                    return
                logger.info("Created VIEW %s", view_table)
            except Exception as e:
                logger.exception("Failed to create VIEW %s: %s", view_table, e)
                tm.fail_task(task.id, f'Failed to create VIEW: {str(e)}')
                return
            tm.update_progress(task.id, 25, f"VIEW {view_table} created")

            # --- Phase 3: Diff or full load ---
            snapshot_table = incr_svc.snapshot_table_name(
                (project.info or {}).get('name', DEFAULT_GRAPH_NAME), delta_cfg,
                version=project.current_version,
            )

            to_add = []
            to_remove = []
            total_triple_count = 0

            if actual_mode == 'incremental' and incr_svc.snapshot_exists(snapshot_table):
                tm.advance_step(task.id, "Computing incremental diff...")
                try:
                    to_add, to_remove = incr_svc.compute_diff(view_table, snapshot_table)
                    total_triple_count = incr_svc.count_view_triples(view_table)

                    if incr_svc.should_fallback_to_full(len(to_add), len(to_remove), total_triple_count):
                        logger.info("Diff too large — falling back to full rebuild")
                        actual_mode = 'full'
                    else:
                        tm.update_progress(
                            task.id, 40,
                            f"Diff: +{len(to_add)} / -{len(to_remove)} triples"
                        )
                except Exception as e:
                    logger.warning("Incremental diff failed, falling back to full: %s", e)
                    actual_mode = 'full'
            else:
                if actual_mode == 'incremental':
                    logger.info("No snapshot table — first build will be full + create snapshot")
                actual_mode = 'full'

            # --- Phase 4: Apply to graph ---
            tm.advance_step(task.id, "Applying changes to LadybugDB graph...")

            from back.core.triplestore import get_triplestore as _get_ts
            store = _get_ts(proj_snap, settings, backend="graph")
            if not store:
                tm.fail_task(task.id, 'Could not initialize LadybugDB backend')
                return

            if actual_mode == 'full':
                tm.update_progress(task.id, 40, "Reading all triples from VIEW...")
                try:
                    triples = source_client.execute_query(f"SELECT * FROM {view_table}")
                except Exception as e:
                    logger.exception("Failed to read from VIEW %s: %s", view_table, e)
                    tm.fail_task(task.id, f'Query execution on VIEW failed: {str(e)}')
                    return

                triple_count = len(triples)
                if triple_count == 0:
                    tm.complete_task(task.id, result={
                        'triple_count': 0,
                        'view_table': view_table,
                        'graph_name': graph_name,
                        'build_mode': 'full',
                        'duration_seconds': time.time() - start_time
                    }, message='VIEW created but no triples generated (check your mappings)')
                    return

                tm.update_progress(task.id, 50, f"Full rebuild: writing {triple_count} triples...")
                store.drop_table(graph_name)
                store.create_table(graph_name)

                def _on_progress_full(written, total):
                    progress = 50 + int(written / total * 40)
                    tm.update_progress(task.id, min(progress, 90), f"Written {written}/{total} triples...")

                store.insert_triples(graph_name, triples, batch_size=500, on_progress=_on_progress_full)
                store.optimize_table(graph_name)
                total_triple_count = triple_count

            else:
                triple_count = len(to_add) + len(to_remove)
                if triple_count == 0:
                    duration = time.time() - start_time
                    tm.complete_task(task.id, result={
                        'triple_count': total_triple_count,
                        'view_table': view_table,
                        'graph_name': graph_name,
                        'build_mode': 'incremental',
                        'diff': {'added': 0, 'removed': 0},
                        'duration_seconds': duration,
                    }, message=f'No changes to apply ({total_triple_count} triples unchanged)')
                    # Still update source versions
                    return

                progress_base = 45

                if to_remove:
                    tm.update_progress(task.id, progress_base, f"Removing {len(to_remove)} triples...")
                    def _on_del_progress(done, total):
                        p = progress_base + int(done / total * 20)
                        tm.update_progress(task.id, min(p, progress_base + 20), f"Removed {done}/{total} triples...")
                    store.delete_triples(graph_name, to_remove, batch_size=500, on_progress=_on_del_progress)

                if to_add:
                    add_base = progress_base + 25
                    tm.update_progress(task.id, add_base, f"Inserting {len(to_add)} triples...")
                    def _on_add_progress(done, total):
                        p = add_base + int(done / total * 20)
                        tm.update_progress(task.id, min(p, add_base + 20), f"Inserted {done}/{total} triples...")
                    store.insert_triples(graph_name, to_add, batch_size=500, on_progress=_on_add_progress)

                store.optimize_table(graph_name)

            # --- Phase 5: Refresh snapshot ---
            tm.advance_step(task.id, "Refreshing snapshot table...")
            try:
                incr_svc.refresh_snapshot(view_table, snapshot_table)
                logger.info("Snapshot table %s refreshed", snapshot_table)
            except Exception as e:
                logger.warning("Failed to refresh snapshot (non-fatal): %s", e)

            # Store new source versions in project session
            try:
                if new_source_versions:
                    project.source_versions = new_source_versions
                project.snapshot_table = snapshot_table
                project.save()
            except Exception as e:
                logger.warning("Could not persist source versions: %s", e)

            # Populate DT session cache so subsequent page loads avoid live I/O
            try:
                import os
                from back.core.triplestore.ladybugdb import local_db_path, graph_volume_path

                final_count = total_triple_count or triple_count
                build_stamp = project.triplestore.get('build_last_update')

                status_cache = {
                    'success': True,
                    'has_data': final_count > 0,
                    'count': final_count,
                    'view_table': view_table,
                    'graph_name': graph_name,
                }
                if build_stamp and final_count > 0:
                    status_cache['last_modified'] = build_stamp

                db_name = graph_name or DEFAULT_GRAPH_NAME
                lb_cfg = getattr(project, 'ladybug', None) or {}
                if not lb_cfg and hasattr(project, 'triplestore'):
                    lb_cfg = (project.triplestore or {}).get('ladybug', {})
                local_base = lb_cfg.get('db_path', '/tmp/ontobricks')
                local_path = local_db_path(db_name, local_base)
                uc_path = project.uc_project_path
                registry_lbug_path = graph_volume_path(uc_path, db_name) if uc_path else ''

                existence_cache = {
                    'view_exists': True,
                    'snapshot_exists': True,
                    'snapshot_table': snapshot_table,
                    'view_table': view_table,
                    'graph_name': graph_name,
                    'local_lbug_exists': os.path.exists(local_path),
                    'local_lbug_path': local_path,
                    'registry_lbug_exists': None,
                    'registry_lbug_path': registry_lbug_path,
                    'last_built': project.last_build,
                    'last_update': project.last_update,
                }

                dt = DigitalTwin(project)
                dt.set_ts_cache('status', status_cache)
                dt.set_ts_cache('dt_existence', existence_cache)
                logger.debug("Build cache populated: count=%d", final_count)
            except Exception as e:
                logger.warning("Could not populate DT session cache: %s", e)

            duration = time.time() - start_time

            result_data = {
                'triple_count': total_triple_count or triple_count,
                'view_table': view_table,
                'graph_name': graph_name,
                'build_mode': actual_mode,
                'snapshot_table': snapshot_table,
                'duration_seconds': duration,
            }
            if actual_mode == 'incremental':
                result_data['diff'] = {'added': len(to_add), 'removed': len(to_remove)}
                msg = f"Incremental: +{len(to_add)} / -{len(to_remove)} triples in {duration:.1f}s"
            else:
                msg = f"Full rebuild: {total_triple_count or triple_count} triples in {duration:.1f}s"

            tm.complete_task(task.id, result=result_data, message=msg)

        except Exception as e:
            logger.exception("Triple store sync failed: %s", e)
            tm.fail_task(task.id, str(e))

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
        project = get_project(session_mgr)
        graph_name = effective_graph_name(project)

        store = get_triplestore(project, settings, backend="graph")
        if not store:
            return {'success': False, 'message': 'Graph backend not configured'}

        try:
            results = store.query_triples(graph_name)
        except Exception as e:
            logger.exception("Load graph query failed: %s", e)
            error_msg = str(e)
            if 'does not exist' in error_msg.lower():
                return {'success': False, 'message': f'Graph {graph_name} does not exist. Run Build first.'}
            return {'success': False, 'message': f'Error reading graph: {error_msg}'}

        return {
            'success': True,
            'results': results,
            'columns': ['subject', 'predicate', 'object'],
            'count': len(results),
        }

    except Exception as e:
        logger.exception("Load graph failed: %s", e)
        return {'success': False, 'message': f'Error loading graph: {str(e)}'}


@router.post("/sync/filter")
async def filter_triplestore(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Query the triple store with filter criteria and return only matching triples."""
    try:
        data = await request.json()
        entity_type = (data.get('entity_type') or '').strip()
        field = data.get('field', 'any')
        match_type = data.get('match_type', 'contains')
        value = (data.get('value') or '').strip()
        include_rels = data.get('include_rels', True)
        depth = min(int(data.get('depth', 3)), 5)

        if not entity_type and not value:
            return {'success': False, 'message': 'Please specify an entity type or search value.'}

        project = get_project(session_mgr)
        graph_name = effective_graph_name(project)
        if not graph_name:
            return {'success': False, 'message': 'Graph name not configured'}

        store = get_triplestore(project, settings, backend="graph")
        if not store:
            return {'success': False, 'message': 'Graph backend not configured'}

        logger.info(
            "Filter graph – type=%s, field=%s, match=%s, value=%s",
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
        except Exception as e:
            logger.exception("Filter seed query failed: %s", e)
            msg = str(e)
            if 'does not exist' in msg.lower():
                return {'success': False, 'message': f'Graph {graph_name} does not exist. Run Build first.'}
            return {'success': False, 'message': f'Error querying graph: {msg}'}
        initial_count = len(entity_set)

        if not entity_set:
            return {
                'success': True,
                'results': [],
                'columns': ['subject', 'predicate', 'object'],
                'count': 0,
                'initial_count': 0,
                'expanded_count': 0,
                'message': 'No entities found matching the filter criteria.',
            }

        max_entities = 5000
        if include_rels and depth > 0:
            current_level = set(entity_set)
            for d in range(depth):
                if not current_level or len(entity_set) >= max_entities:
                    break
                logger.debug("Filter graph – expansion level %d (%d entities so far)", d + 1, len(entity_set))
                try:
                    neighbors = store.expand_entity_neighbors(graph_name, current_level)
                except Exception as e:
                    logger.warning("Expansion query at level %d failed: %s", d + 1, e)
                    break
                new_entities = neighbors - entity_set
                if not new_entities:
                    break
                entity_set.update(new_entities)
                current_level = new_entities

        logger.info(
            "Filter graph – fetching triples for %d entities (%d seed + %d expanded)",
            len(entity_set), initial_count, len(entity_set) - initial_count,
        )
        try:
            results = store.get_triples_for_subjects(graph_name, list(entity_set))
        except Exception as e:
            logger.exception("Filter final query failed: %s", e)
            return {'success': False, 'message': f'Error fetching triples: {str(e)}'}

        return {
            'success': True,
            'results': results,
            'columns': ['subject', 'predicate', 'object'],
            'count': len(results),
            'initial_count': initial_count,
            'expanded_count': len(entity_set),
        }

    except Exception as e:
        logger.exception("Filter triplestore failed: %s", e)
        return {'success': False, 'message': f'Error filtering triple store: {str(e)}'}


@router.get("/sync/changes")
async def triplestore_changes(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Check if ontology or assignments changed since the last build."""
    project = get_project(session_mgr)
    await run_blocking(DigitalTwin(project).sync_last_build_from_schedule, settings)

    last_update = project.last_update
    last_build = project.last_build
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
        project = get_project(session_mgr)
        dt = DigitalTwin(project)
        await run_blocking(dt.sync_last_build_from_schedule, settings)
        return await dt.get_or_fetch_graph_status(settings)
    except Exception as e:
        logger.exception("Triplestore status failed: %s", e)
        return {'success': False, 'has_data': False, 'count': 0,
                'message': str(e)}


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
    from back.services import home as home_service
    from back.objects.project import Project as ProjectDomain

    t0 = _t.monotonic()

    project = get_project(session_mgr)

    readiness = home_service.validate_status(project)
    project_info_data = ProjectDomain(project).get_project_info()

    last_update = project.last_update
    last_build = project.last_build
    needs_rebuild = bool(last_update and last_build and last_update > last_build)

    t_prep = _t.monotonic()

    dt = DigitalTwin(project)

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

    if project.last_build and project.last_build != last_build:
        last_build = project.last_build
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
        'project_info': project_info_data,
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
    project = get_project(session_mgr)
    dt = DigitalTwin(project)
    await run_blocking(dt.sync_last_build_from_schedule, settings)
    return await dt.get_or_fetch_dt_existence(settings)


@router.post("/sync/reload-from-registry")
async def reload_graph_from_registry(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Download the LadybugDB archive from the registry volume and extract it locally."""
    from back.core.triplestore.ladybugdb import sync_from_volume

    project = get_project(session_mgr)
    uc_path = project.uc_project_path
    if not uc_path:
        return {'success': False, 'message': 'Registry path not configured'}

    db_name = effective_graph_name(project) or DEFAULT_GRAPH_NAME

    host = project.databricks.get('host') or settings.databricks_host
    token = project.databricks.get('token') or settings.databricks_token
    if not host or not token:
        return {'success': False, 'message': 'Databricks credentials not configured'}

    from back.core.databricks import VolumeFileService
    uc = VolumeFileService(host=host, token=token)

    ok, msg = sync_from_volume(uc, uc_path, db_name)
    if not ok:
        logger.warning("reload-from-registry failed: %s", msg)
        return {'success': False, 'message': msg}

    logger.info("reload-from-registry succeeded: %s", msg)
    return {'success': True, 'message': msg}


@router.post("/sync/drop-snapshot")
async def drop_snapshot(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Drop the incremental snapshot table to force a full rebuild on next build."""
    project = get_project(session_mgr)
    snapshot_table = project.snapshot_table
    if not snapshot_table:
        return {'success': False, 'message': 'No snapshot table configured'}

    host, token, warehouse_id = get_databricks_credentials(project, settings)
    if not host or not warehouse_id:
        return {'success': False, 'message': 'Databricks not configured'}

    from back.core.triplestore import IncrementalBuildService
    client = DatabricksClient(host=host, token=token, warehouse_id=warehouse_id)
    incr_svc = IncrementalBuildService(client)
    incr_svc.drop_snapshot(snapshot_table)

    project.snapshot_table = ''
    project.source_versions = {}
    project.save()

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
        project = get_project(session_mgr)
        graph_name = effective_graph_name(project)

        if not graph_name:
            return {'success': False, 'message': 'Graph name not configured'}

        if not refresh:
            cached = DigitalTwin(project).get_ts_cache('stats')
            if cached:
                preds = cached.get('top_predicates') or []
                has_kind = preds and 'kind' in preds[0]
                if has_kind:
                    logger.debug("Returning cached graph stats")
                    return cached
                logger.debug("Stale stats cache (missing 'kind'); refreshing")

        store = get_triplestore(project, settings, backend="graph")
        if not store:
            return {'success': False, 'message': 'Graph backend not configured'}

        agg = store.get_aggregate_stats(graph_name)
        total_count = agg["total"]
        subject_count = agg["distinct_subjects"]
        predicate_count = agg["distinct_predicates"]
        label_count = agg["label_count"]

        entity_types = store.get_type_distribution(graph_name)
        top_predicates = store.get_predicate_distribution(graph_name)

        type_count = sum(int(r.get('cnt', 0)) for r in entity_types)
        relationship_count = total_count - type_count - label_count

        classified = DigitalTwin(project).classify_predicates(top_predicates)

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
        DigitalTwin(project).set_ts_cache('stats', result)
        return result
    except Exception as e:
        logger.exception("Triplestore stats failed: %s", e)
        return {'success': False, 'message': f'Error retrieving stats: {str(e)}'}


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
        project = get_project(session_mgr)
        if backend == "graph":
            triplestore_table = effective_graph_name(project).strip()
        else:
            triplestore_table = data.get("triplestore_table", "").strip()

        if not triplestore_table:
            return {"success": False, "message": "Triple store table not specified."}
        if not shape:
            return {"success": False, "message": "No shape provided."}

        from back.core.w3c import SHACLService

        store = get_triplestore(project, settings, backend=backend)
        if not store:
            return {"success": False, "message": f"Could not initialize {backend} backend"}

        if backend == "graph":
            graph_name = triplestore_table or effective_graph_name(project)
            triples = await run_blocking(store.query_triples, graph_name)
            if not triples:
                return {"success": False, "message": f"Graph '{graph_name}' is empty. Build first."}
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
            return {"success": False, "message": f"Cannot translate shape {shape.get('id', '?')} to SQL"}
        results = await run_blocking(store.execute_query, sql)
        return {
            "success": True,
            "violations": results or [],
            "count": len(results) if results else 0,
            "sql": sql,
        }
    except Exception as e:
        logger.exception("SHACL quality check failed: %s", e)
        return {"success": False, "message": str(e)}


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

    project = get_project(session_mgr)
    if requested_backend == "graph":
        triplestore_table = effective_graph_name(project).strip()
    else:
        triplestore_table = data.get("triplestore_table", "").strip()

    if not triplestore_table:
        return {"success": False, "message": "Triple store table not specified."}
    shapes = project.shacl_shapes
    if dimensions:
        shapes = [s for s in shapes if s.get("category") in dimensions]
    shapes = [s for s in shapes if s.get("enabled", True)]

    swrl_rules = project.swrl_rules or []
    ontology_dict = getattr(project, 'ontology', None)
    if not isinstance(ontology_dict, dict):
        ontology_dict = project._data.get('ontology', {}) if hasattr(project, '_data') else {}
    decision_tables = [dt for dt in ontology_dict.get("decision_tables", []) if dt.get("enabled", True)]
    aggregate_rules = [r for r in ontology_dict.get("aggregate_rules", []) if r.get("enabled", True)]

    if not shapes and not swrl_rules and not decision_tables and not aggregate_rules:
        return {"success": False, "message": "No enabled shapes, SWRL rules, decision tables or aggregate rules to check."}

    total = len(shapes) + len(swrl_rules) + len(decision_tables) + len(aggregate_rules)
    proj_snap = ProjectSnapshot(project)
    tm = get_task_manager()
    task = tm.create_task(
        name="Data Quality Checks",
        task_type="dataquality_checks",
        steps=[{"name": "running", "description": f"Running {total} quality checks"}],
    )

    def run_checks():
        import time
        from back.core.triplestore import get_triplestore as _get_ts

        t0 = time.time()
        try:
            backend = requested_backend or "view"
            tm.start_task(task.id, f"Running {total} data quality checks ({backend})...")

            store = _get_ts(proj_snap, settings, backend=backend)
            if not store:
                tm.fail_task(task.id, f"Could not initialize {backend} backend")
                return

            if backend == "graph":
                DigitalTwin.run_graph_checks(
                    tm, task, shapes, store, triplestore_table, proj_snap, t0, total,
                    swrl_rules=swrl_rules, ontology=ontology_dict,
                    decision_tables=decision_tables,
                    aggregate_rules=aggregate_rules,
                )
            else:
                DigitalTwin.run_sql_checks(
                    tm, task, shapes, triplestore_table, store, t0, total,
                    swrl_rules=swrl_rules, ontology=ontology_dict,
                    decision_tables=decision_tables,
                    aggregate_rules=aggregate_rules,
                )

        except Exception as exc:
            logger.exception("Data quality checks failed: %s", exc)
            tm.fail_task(task.id, str(exc))

    thread = threading.Thread(target=run_checks, daemon=True)
    thread.start()
    return {"success": True, "task_id": task.id, "message": f"Data quality checks started ({total} checks)"}


# ===========================================
# Legacy Quality Routes (kept for backward-compat)
# ===========================================

@router.post("/quality/execute")
async def execute_quality_check(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Execute a quality check against the triple store table."""
    try:
        data = await request.json()
        check_type = data.get('check_type', '')
        triplestore_table = data.get('triplestore_table', '').strip()
        params = data.get('params', {})

        if not triplestore_table:
            return {'success': False, 'message': 'Triple store table not specified. Configure it in Project Settings.'}
        if not check_type:
            return {'success': False, 'message': 'No check type specified'}

        sql = DigitalTwin.build_quality_sql(check_type, triplestore_table, params)
        if not sql:
            return {'success': False, 'message': f'Unsupported check type: {check_type}'}

        project = get_project(session_mgr)
        store = get_triplestore(project, settings, backend="view")
        if not store:
            return {'success': False, 'message': 'View backend not configured (check Databricks connection)'}

        try:
            results = await run_blocking(store.execute_query, sql)
        except Exception as e:
            logger.exception("Quality check query execution failed: %s", e)
            error_msg = str(e)
            if 'TABLE_OR_VIEW_NOT_FOUND' in error_msg or 'does not exist' in error_msg.lower():
                return {'success': False, 'message': f'View {triplestore_table} does not exist. Please build first.'}
            return {'success': False, 'message': f'Query execution error: {error_msg}'}

        return {
            'success': True,
            'violations': results or [],
            'count': len(results) if results else 0,
            'sql': sql
        }

    except Exception as e:
        logger.exception("Execute quality check failed: %s", e)
        return {'success': False, 'message': f'Error executing quality check: {str(e)}'}


@router.post("/quality/start")
async def start_quality_checks(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Start all quality checks as an asynchronous task."""
    import threading
    from back.core.task_manager import get_task_manager

    data = await request.json()
    triplestore_table = data.get('triplestore_table', '').strip()
    checks = data.get('checks', [])

    if not triplestore_table:
        return {'success': False, 'message': 'Triple store table not specified.'}
    if not checks:
        return {'success': False, 'message': 'No quality checks to run.'}

    total_checks = len(checks)
    project = get_project(session_mgr)
    proj_snap = ProjectSnapshot(project)

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
            store = _get_ts(proj_snap, settings, backend="view")
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
                        'message': f'Could not validate: {error_msg}',
                        'violations': [], 'sql': sql
                    })

            DigitalTwin.complete_dq_task(tm, task, results, time.time() - start_time)

        except Exception as e:
            logger.exception("Quality checks failed: %s", e)
            tm.fail_task(task.id, str(e))

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

    project = get_project(session_mgr)
    project.ensure_generated_content()
    proj_snap = ProjectSnapshot(project)

    tm = get_task_manager()
    task = tm.create_task(
        name="Inference",
        task_type="reasoning",
        steps=[{"name": "running", "description": "Running inference phases"}],
    )

    def run_reasoning():
        try:
            logger.info("Reasoning task %s: starting", task.id)
            tm.start_task(task.id)
            tm.update_progress(task.id, 10, "Initialising triple store")

            store = get_triplestore(proj_snap, settings, backend="graph")
            if store is None:
                logger.info("Reasoning task %s: graph store unavailable, falling back to view", task.id)
                store = get_triplestore(proj_snap, settings, backend="view")
            logger.info("Reasoning task %s: store=%s", task.id, type(store).__name__ if store else "None")

            from back.core.reasoning import ReasoningService
            svc = ReasoningService(proj_snap, store)
            tm.update_progress(task.id, 30, "Running inference phases")

            logger.info(
                "Reasoning task %s: running phases (tbox=%s, swrl=%s, graph=%s, "
                "decision_tables=%s, sparql_rules=%s, aggregate_rules=%s)",
                task.id, options.get("tbox"), options.get("swrl"),
                options.get("graph"),
                options.get("decision_tables"),
                options.get("sparql_rules"), options.get("aggregate_rules"),
            )
            result = svc.run_full_reasoning(options)
            logger.info("Reasoning task %s: phases done — %d inferred",
                        task.id, len(result.inferred_triples))

            tm.update_progress(task.id, 90, "Finalising")

            result_dict = result.to_dict()
            result_dict.pop("violations", None)
            import datetime as _dt
            result_dict["last_run"] = _dt.datetime.utcnow().isoformat()
            result_dict["inferred_count"] = len(result.inferred_triples)

            tm.complete_task(
                task.id,
                result=result_dict,
                message=f"Inference complete: {len(result.inferred_triples)} inferred",
            )
            logger.info("Reasoning task %s: completed successfully", task.id)
        except Exception as e:
            logger.exception("Reasoning task %s failed: %s", task.id, e)
            tm.fail_task(task.id, error=str(e))

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
    from back.core.reasoning import ReasoningService
    from back.core.reasoning.models import ReasoningResult, InferredTriple

    data = await request.json()
    task_id = data.get("task_id", "")
    do_delta = data.get("materialize_delta", False)
    do_graph = data.get("materialize_graph", False)
    mat_table = (data.get("materialize_table") or "").strip()

    if not task_id:
        return {"success": False, "message": "Missing task_id"}
    if not do_delta and not do_graph:
        return {"success": False, "message": "Select at least one target"}

    tm = get_task_manager()
    task = tm.get_task(task_id)
    if not task or not task.result:
        return {"success": False, "message": "Inference results not found for this task"}

    raw_triples = task.result.get("inferred_triples", [])
    if not raw_triples:
        return {"success": False, "message": "No inferred triples to materialise"}

    uri_triples = [
        t for t in raw_triples
        if is_uri(t.get("subject", "")) and is_uri(t.get("predicate", "")) and is_uri(t.get("object", ""))
    ]

    project = get_project(session_mgr)
    project.ensure_generated_content()
    proj_snap = ProjectSnapshot(project)

    result = {}

    if do_delta and mat_table and len(mat_table.split(".")) == 3 and uri_triples:
        try:
            client = get_databricks_client(proj_snap, settings)
            if client is None:
                result["materialize_error"] = "Databricks credentials not configured"
            else:
                count = ReasoningService.materialize_to_delta(client, mat_table, uri_triples)
                result["materialize_count"] = count
                result["materialize_table"] = mat_table
        except Exception as e:
            logger.exception("Materialise to Delta failed: %s", e)
            result["materialize_error"] = str(e)
            result["materialize_table"] = mat_table

    if do_graph and uri_triples:
        try:
            store = get_triplestore(proj_snap, settings, backend="graph")
            if store is None:
                result["materialize_graph_error"] = "Graph store not available"
            else:
                svc = ReasoningService(proj_snap, store)
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
            result["materialize_graph_error"] = str(e)

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
    _ = get_project(session_mgr)
    return {
        "success": True,
        "reasoning": {
            "last_run": None,
            "inferred_count": 0,
            "inferred_triples": [],
        },
    }
