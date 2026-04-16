"""
Internal API -- Mapping JSON endpoints.

Moved from app/frontend/mapping/routes.py during the front/back split.
"""
from fastapi import APIRouter, Request, Depends
from fastapi.responses import Response

from shared.config.settings import get_settings, Settings
from back.core.databricks import DatabricksClient
from back.core.helpers import (
    get_databricks_client,
    get_databricks_credentials,
    run_blocking,
)
from agents.serialization import serialize_agent_steps
from back.core.logging import get_logger
from back.objects.session import SessionManager, get_domain, get_session_manager
from back.core.task_manager import get_task_manager
from back.objects.mapping import Mapping
from back.core.errors import OntoBricksError, ValidationError, InfrastructureError, NotFoundError

logger = get_logger(__name__)

router = APIRouter(prefix="/mapping", tags=["Mapping"])


# ===========================================
# Mapping Configuration API Routes
# ===========================================

@router.get("/load")
async def load_mapping(session_mgr: SessionManager = Depends(get_session_manager)):
    """Load mapping configuration from session."""
    domain = get_domain(session_mgr)
    entity_mappings = domain.get_entity_mappings()
    relationship_mappings = domain.get_relationship_mappings()
    return {
        'success': True,
        'config': {
            'entities': entity_mappings,
            'relationships': relationship_mappings,
            'data_source_mappings': entity_mappings,  # backward compat
            'relationship_mappings': relationship_mappings,  # backward compat
            'r2rml_output': domain.get_r2rml()
        },
        'r2rml_output': domain.get_r2rml()
    }


@router.post("/save")
async def save_mapping(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Save mapping configuration to session."""
    data = await request.json()
    mapping_config = data.get('config', data)  # Support both wrapped and unwrapped
    
    domain = get_domain(session_mgr)
    stats = Mapping(domain).save_mapping_config(mapping_config)
    return {'success': True, 'message': 'Mapping saved', 'stats': stats}


@router.post("/reset")
async def reset_mapping_endpoint(session_mgr: SessionManager = Depends(get_session_manager)):
    """Reset mapping configuration."""
    domain = get_domain(session_mgr)
    Mapping(domain).reset_mapping()
    return {'success': True, 'message': 'Mapping reset'}


@router.post("/clear")
async def clear_mapping(session_mgr: SessionManager = Depends(get_session_manager)):
    """Clear all mapping data from session (delegates to reset)."""
    return await reset_mapping_endpoint(session_mgr)


# ===========================================
# Entity Mapping API
# ===========================================

@router.post("/entity/add")
async def add_entity_mapping(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Add an entity mapping."""
    data = await request.json()
    domain = get_domain(session_mgr)
    _, new_mapping = Mapping(domain).add_or_update_entity_mapping(data)
    return {'success': True, 'mapping': new_mapping}


@router.post("/entity/delete")
async def delete_entity_mapping(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Delete an entity mapping."""
    data = await request.json()
    domain = get_domain(session_mgr)
    ontology_class = data.get('ontology_class')
    
    if Mapping(domain).delete_entity_mapping(ontology_class):
        return {'success': True}
    raise NotFoundError("Mapping not found")


# ===========================================
# Relationship Mapping API
# ===========================================

@router.post("/relationship/add")
async def add_relationship_mapping(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Add a relationship mapping."""
    data = await request.json()
    domain = get_domain(session_mgr)
    _, new_mapping = Mapping(domain).add_or_update_relationship_mapping(data)
    return {'success': True, 'mapping': new_mapping}


@router.post("/relationship/delete")
async def delete_relationship_mapping(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Delete a relationship mapping."""
    data = await request.json()
    domain = get_domain(session_mgr)
    property_uri = data.get('property')
    
    if Mapping(domain).delete_relationship_mapping(property_uri):
        return {'success': True}
    raise NotFoundError("Mapping not found")


# ===========================================
# Exclude / Include Entities
# ===========================================

@router.post("/exclude")
async def toggle_exclude(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Toggle the excluded flag on individual mapping entries.

    The ``excluded`` boolean is stored directly on each entry in
    ``entities`` (keyed by ``ontology_class``) or
    ``relationships`` (keyed by ``property``).
    If no entry exists yet for an excluded item a minimal stub is created.

    Body JSON:
        uris: list[str]        – URIs to toggle
        excluded: bool         – True to exclude, False to include
        item_type: str         – 'entity' or 'relationship' (default 'entity')
    """
    data = await request.json()
    uris = data.get('uris', [])
    excluded = bool(data.get('excluded', True))
    item_type = data.get('item_type', 'entity')

    domain = get_domain(session_mgr)
    changed = Mapping(domain).toggle_exclude_items(uris, excluded, item_type)
    logger.info("Toggled excluded=%s for %d %s(s)", excluded, changed, item_type)
    return {'success': True, 'changed': changed}


# ===========================================
# R2RML Generation
# ===========================================

@router.post("/generate")
async def generate_r2rml(session_mgr: SessionManager = Depends(get_session_manager)):
    """Generate R2RML from current mapping configuration."""
    domain = get_domain(session_mgr)
    
    m = Mapping(domain)
    r2rml_content = m.generate_r2rml()
    return {'success': True, 'r2rml': r2rml_content, 'stats': m.get_mapping_stats()}


# ===========================================
# SQL Query Testing
# ===========================================

@router.post("/test-query")
async def test_sql_query(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Test a SQL query and return column information."""
    data = await request.json()
    sql_query = data.get('query') or data.get('sql_query', '')
    limit = data.get('limit', 100)
    
    if not sql_query:
        raise ValidationError("No SQL query provided")
    
    try:
        domain = get_domain(session_mgr)
        host, token, warehouse_id = get_databricks_credentials(domain, settings)
        
        if not warehouse_id:
            raise ValidationError("No SQL warehouse configured")
        
        client = DatabricksClient(host=host, token=token, warehouse_id=warehouse_id)
        result = await run_blocking(Mapping.test_sql_query, client, sql_query, limit=int(limit))
        
        return {'success': True, **result}
    except OntoBricksError:
        raise
    except Exception as e:
        logger.warning("test_sql_query failed: %s", e, exc_info=True)
        raise InfrastructureError("SQL query test failed", detail=str(e)) from e


# ===========================================
# Table & Column Discovery
# ===========================================

@router.post("/tables")
async def get_tables(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Get tables for selected catalog and schema."""
    data = await request.json()
    catalog, schema = data.get('catalog'), data.get('schema')
    
    if not catalog or not schema:
        raise ValidationError("Catalog and schema are required")
    
    try:
        domain = get_domain(session_mgr)
        client = get_databricks_client(domain, settings)
        if not client:
            raise ValidationError("Databricks not configured")
        return {'tables': await run_blocking(client.get_tables, catalog, schema)}
    except OntoBricksError:
        raise
    except Exception as e:
        logger.warning("get_tables failed for %s.%s: %s", catalog, schema, e, exc_info=True)
        raise InfrastructureError("Failed to list tables", detail=str(e)) from e


@router.post("/table-columns")
async def get_table_columns(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Get columns for a specific table."""
    data = await request.json()
    catalog, schema, table = data.get('catalog'), data.get('schema'), data.get('table')
    
    if not all([catalog, schema, table]):
        raise ValidationError("Catalog, schema, and table are required")
    
    try:
        domain = get_domain(session_mgr)
        client = get_databricks_client(domain, settings)
        if not client:
            raise ValidationError("Databricks not configured")
        return {'columns': await run_blocking(client.get_table_columns, catalog, schema, table)}
    except OntoBricksError:
        raise
    except Exception as e:
        logger.exception("Get table columns failed: %s", e)
        raise InfrastructureError("Failed to list table columns", detail=str(e)) from e


# ===========================================
# R2RML Import/Export
# ===========================================

@router.post("/parse-r2rml")
async def parse_r2rml(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Parse R2RML content and extract mappings."""
    data = await request.json()
    r2rml_content = data.get('content', '')
    
    if not r2rml_content:
        raise ValidationError("No R2RML content provided")
    
    try:
        return Mapping(get_domain(session_mgr)).parse_r2rml(r2rml_content)
    except OntoBricksError:
        raise
    except Exception as e:
        logger.exception("Parse R2RML failed: %s", e)
        raise ValidationError("Failed to parse R2RML content", detail=str(e)) from e


@router.get("/download")
async def download_r2rml(session_mgr: SessionManager = Depends(get_session_manager)):
    """Download generated R2RML as TTL file."""
    domain = get_domain(session_mgr)
    r2rml_content = domain.get_r2rml()
    
    if not r2rml_content:
        raise ValidationError("No R2RML mapping is available to download")
    
    return Response(
        content=r2rml_content,
        media_type="text/turtle",
        headers={"Content-Disposition": "attachment; filename=mapping.ttl"}
    )


@router.post("/save-to-uc")
async def save_mapping_to_uc(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Save R2RML mapping to Unity Catalog volume."""
    from api.routers.internal._helpers import save_content_to_uc
    return await save_content_to_uc(request, session_mgr, settings, log_context="mapping")


# ===========================================
# Diagnostics
# ===========================================

@router.get("/diagnostics")
async def run_diagnostics(session_mgr: SessionManager = Depends(get_session_manager)):
    """Run comprehensive validation on all entity and relationship mappings."""
    domain = get_domain(session_mgr)
    return Mapping(domain).run_diagnostics()


# ===========================================
# SQL Wizard API (Text-to-SQL)
# ===========================================

@router.get("/wizard/llm-endpoints")
async def get_llm_endpoints(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Get available model serving endpoints for SQL generation."""
    try:
        from back.core.sqlwizard import SQLWizardService
        
        domain = get_domain(session_mgr)
        client = get_databricks_client(domain, settings)
        
        if not client:
            raise ValidationError("Databricks not configured")
        
        wizard = SQLWizardService(client)
        endpoints = wizard.get_model_serving_endpoints()
        return {'success': True, 'endpoints': endpoints}
        
    except OntoBricksError:
        raise
    except Exception as e:
        logger.exception("Get LLM endpoints failed: %s", e)
        raise InfrastructureError("Failed to list model serving endpoints", detail=str(e)) from e


@router.post("/wizard/schema-context")
async def get_schema_context(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Get schema context (tables and columns) for SQL generation."""
    try:
        from back.core.sqlwizard import SQLWizardService
        
        data = await request.json()
        catalog = data.get('catalog')
        schema = data.get('schema')
        
        if not catalog or not schema:
            raise ValidationError("Catalog and schema are required")
        
        domain = get_domain(session_mgr)
        client = get_databricks_client(domain, settings)
        
        if not client:
            raise ValidationError("Databricks not configured")
        
        wizard = SQLWizardService(client)
        context = wizard.get_schema_context(catalog, schema)
        
        return {
            'success': True,
            'context': {
                'tables': context.tables,
                'rendered': context.to_yaml_like()
            }
        }
        
    except OntoBricksError:
        raise
    except Exception as e:
        logger.exception("Get schema context failed: %s", e)
        raise InfrastructureError("Failed to load schema context", detail=str(e)) from e


@router.post("/wizard/generate-sql")
async def generate_sql_from_prompt(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Generate SQL from natural language prompt using LLM.
    
    Request body:
        endpoint_name: Name of the model serving endpoint
        catalog: Unity Catalog catalog name
        schema: Schema name
        prompt: Natural language description of the query
        limit: Optional result limit (default: 100)
        validate_plan: Whether to run EXPLAIN validation (default: true)
        schema_context: Optional pre-built schema context with tables (from domain metadata)
        mapping_type: Type of mapping ('entity', 'relationship', or None for general)
    """
    try:
        from back.core.sqlwizard import SQLWizardService
        
        data = await request.json()
        
        endpoint_name = data.get('endpoint_name')
        catalog = data.get('catalog')  # deprecated - only used if no schema_context
        schema = data.get('schema')  # deprecated - only used if no schema_context
        prompt = data.get('prompt')
        limit = data.get('limit', 100)
        validate_plan = data.get('validate_plan', True)
        schema_context = data.get('schema_context')  # Pre-built context with tables (each has full_name)
        mapping_type = data.get('mapping_type')  # 'entity', 'relationship', or None
        
        if not endpoint_name or not prompt:
            raise ValidationError("Missing required fields: endpoint_name, prompt")
        
        # If no schema_context provided, we need catalog/schema to fetch from UC (deprecated path)
        if not schema_context or not schema_context.get('tables'):
            if not catalog or not schema:
                raise ValidationError(
                    "Missing required fields: schema_context with tables (or catalog/schema for legacy fetch)"
                )
        
        domain = get_domain(session_mgr)
        client = get_databricks_client(domain, settings)
        
        if not client:
            raise ValidationError("Databricks not configured")
        
        wizard = SQLWizardService(client)
        
        result = wizard.generate_sql(
            endpoint_name=endpoint_name,
            user_prompt=prompt,
            limit=limit,
            validate_plan=validate_plan,
            schema_context_data=schema_context,
            mapping_type=mapping_type,
            catalog=catalog,  # Only used if no schema_context
            schema=schema  # Only used if no schema_context
        )
        
        return result
        
    except OntoBricksError:
        raise
    except Exception as e:
        logger.exception("Generate SQL from prompt failed: %s", e)
        raise InfrastructureError("SQL generation from prompt failed", detail=str(e)) from e


@router.post("/wizard/validate-sql")
async def validate_sql(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Validate a SQL query (static + optional EXPLAIN).
    
    Request body:
        sql: SQL query to validate
        catalog: Unity Catalog catalog name
        schema: Schema name
        validate_plan: Whether to run EXPLAIN validation (default: false)
    """
    try:
        from back.core.sqlwizard import SQLWizardService

        data = await request.json()
        
        sql = data.get('sql')
        catalog = data.get('catalog')
        schema = data.get('schema')
        validate_plan = data.get('validate_plan', False)
        
        if not sql:
            raise ValidationError("SQL query is required")
        
        domain = get_domain(session_mgr)
        client = get_databricks_client(domain, settings)
        
        if not client:
            raise ValidationError("Databricks not configured")
        
        wizard = SQLWizardService(client)
        return Mapping.validate_mapping_sql(wizard, sql, catalog, schema, validate_plan)

    except OntoBricksError:
        raise
    except Exception as e:
        logger.exception("Validate SQL failed: %s", e)
        raise InfrastructureError("SQL validation failed", detail=str(e)) from e


# ===========================================
# Auto-Map Async
# ===========================================

def _chunk_pct(chunk_idx: int, num_chunks: int, inner_pct: int) -> int:
    """Map a per-chunk progress percentage to an overall 1-95 range."""
    chunk_span = 94.0 / max(num_chunks, 1)
    return min(1 + int(chunk_idx * chunk_span + (inner_pct / 100.0) * chunk_span), 95)


@router.post("/auto-assign/start")
async def start_auto_assign(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Start async auto-map process and return task ID."""
    import threading

    data = await request.json()
    entities = data.get('entities', [])
    relationships = data.get('relationships', [])
    
    total_items = len(entities) + len(relationships)
    logger.info(
        "===== AUTO-ASSIGN START ===== entities=%d, relationships=%d, total=%d",
        len(entities), len(relationships), total_items,
    )
    if total_items == 0:
        logger.info("Auto-assign: nothing to process — returning early")
        raise ValidationError("No items to process")
    
    # Validate configuration
    domain = get_domain(session_mgr)
    host, token, warehouse_id = get_databricks_credentials(domain, settings)
    
    schema_context, schema_err = Mapping(domain).resolve_auto_assign_schema_context(
        data.get("schema_context") or {}
    )
    if schema_err:
        logger.warning("Auto-assign: %s", schema_err)
        raise ValidationError("Schema context could not be resolved", detail=schema_err)
    logger.info(
        "Auto-assign: schema_context — %d table(s)",
        len(schema_context.get("tables", [])),
    )
    
    if not host or not token:
        logger.warning("Auto-assign: Databricks credentials missing")
        raise ValidationError("Databricks not configured")
    
    if not warehouse_id:
        logger.warning("Auto-assign: no SQL warehouse configured")
        raise ValidationError("No SQL warehouse configured")
    
    llm_endpoint = domain.info.get('llm_endpoint', '')
    if not llm_endpoint:
        logger.warning("Auto-assign: no LLM serving endpoint configured")
        raise ValidationError("No LLM serving endpoint configured")
    
    logger.info(
        "Auto-assign: config OK — host=%s, warehouse=%s, llm_endpoint=%s",
        host[:40] + "…" if len(host) > 40 else host, warehouse_id, llm_endpoint,
    )
    logger.debug(
        "Auto-assign: entity names=%s",
        [e.get("name", "?") for e in entities],
    )
    logger.debug(
        "Auto-assign: relationship names=%s",
        [r.get("name", "?") for r in relationships],
    )
    
    # Create the client
    client = DatabricksClient(host=host, token=token, warehouse_id=warehouse_id)
    if not client:
        logger.error("Auto-assign: failed to create DatabricksClient")
        raise InfrastructureError("Failed to create Databricks client")
    
    # Create task
    tm = get_task_manager()
    task = tm.create_task(
        name=f"Auto-Map ({len(entities)} entities, {len(relationships)} relationships)",
        task_type="auto_assign",
        steps=[
            {'name': 'init', 'description': 'Initializing auto-map'},
            {'name': 'entities', 'description': f'Processing {len(entities)} entities'},
            {'name': 'relationships', 'description': f'Processing {len(relationships)} relationships'},
            {'name': 'finalize', 'description': 'Finalizing mappings'}
        ]
    )
    logger.info("Auto-assign: task created — id=%s", task.id)

    # Get current mappings to update
    entity_mappings = list(domain.get_entity_mappings())
    relationship_mappings = list(domain.get_relationship_mappings())
    logger.info(
        "Auto-assign: existing mappings — %d entity, %d relationship",
        len(entity_mappings), len(relationship_mappings),
    )

    # Capture session file info for direct writes from the background thread
    session_id = getattr(request.state, 'session_id', None)
    session_ref = getattr(request.state, 'session', None)

    def run_auto_assign():
        import time as _time
        from shared.config.constants import AUTO_ASSIGN_CHUNK_SIZE, AUTO_ASSIGN_CHUNK_COOLDOWN

        try:
            tm.start_task(task.id, "Starting auto-mapping agent…")
            task.result = {
                "live_stats": True,
                "entities_assigned": 0,
                "entities_total": len(entities),
                "relationships_assigned": 0,
                "relationships_total": len(relationships),
            }
            logger.info("Auto-assign agent thread started — task=%s", task.id)

            # Fetch documents once at agent start
            documents = Mapping.fetch_documents_for_agent(domain, host, token)

            # Split items into chunks to avoid rate-limit exhaustion
            all_items = [("entity", e) for e in entities] + [("rel", r) for r in relationships]
            chunk_size = max(AUTO_ASSIGN_CHUNK_SIZE, 1)
            chunks = [all_items[i:i + chunk_size] for i in range(0, len(all_items), chunk_size)]
            num_chunks = len(chunks)

            logger.info(
                "Auto-assign: splitting %d items into %d chunk(s) of ≤%d",
                len(all_items), num_chunks, chunk_size,
            )

            # Accumulators across all chunks — keyed by URI to prevent duplicates
            entity_mapping_by_uri = {}
            rel_mapping_by_uri = {}
            # URIs that this chunk was asked to map (only accept results for these)
            all_steps = []
            total_iterations = 0
            total_usage = {"prompt_tokens": 0, "completion_tokens": 0}
            chunk_errors = []

            for chunk_idx, chunk in enumerate(chunks):
                chunk_num = chunk_idx + 1
                chunk_entities = [item for kind, item in chunk if kind == "entity"]
                chunk_rels = [item for kind, item in chunk if kind == "rel"]
                chunk_total = len(chunk_entities) + len(chunk_rels)

                # Track which URIs this chunk is responsible for
                chunk_entity_uris = {e.get("uri", "") for e in chunk_entities}
                chunk_rel_uris = {r.get("uri", "") for r in chunk_rels}

                logger.info(
                    "----- Chunk %d/%d: %d entities, %d relationships -----",
                    chunk_num, num_chunks, len(chunk_entities), len(chunk_rels),
                )

                # Cooldown between chunks (skip before the first)
                if chunk_idx > 0:
                    logger.info("Auto-assign: cooling down %ds before chunk %d/%d", AUTO_ASSIGN_CHUNK_COOLDOWN, chunk_num, num_chunks)
                    tm.update_progress(task.id, _chunk_pct(chunk_idx, num_chunks, 0),
                                       f"Cooling down before chunk {chunk_num}/{num_chunks}…")
                    _time.sleep(AUTO_ASSIGN_CHUNK_COOLDOWN)

                def on_step(msg: str, progress_pct: int = 0):
                    overall_pct = _chunk_pct(chunk_idx, num_chunks, progress_pct)
                    tm.update_progress(task.id, overall_pct, f"[{chunk_num}/{num_chunks}] {msg}")

                # Pass existing + already-accumulated as read-only context
                context_entity_mappings = entity_mappings + list(entity_mapping_by_uri.values())
                context_rel_mappings = relationship_mappings + list(rel_mapping_by_uri.values())

                try:
                    agent_result = Mapping(domain).auto_assign_with_agent(
                        host=host,
                        token=token,
                        endpoint_name=llm_endpoint,
                        client=client,
                        metadata=schema_context,
                        ontology={"entities": chunk_entities, "relationships": chunk_rels},
                        entity_mappings=context_entity_mappings,
                        relationship_mappings=context_rel_mappings,
                        documents=documents,
                        on_step=on_step,
                    )
                except Exception as chunk_exc:
                    logger.exception("Auto-assign chunk %d/%d crashed: %s", chunk_num, num_chunks, chunk_exc)
                    chunk_errors.append(f"Chunk {chunk_num}: {chunk_exc}")
                    continue

                if agent_result.error and not agent_result.success:
                    logger.warning("Auto-assign chunk %d/%d failed: %s", chunk_num, num_chunks, agent_result.error)
                    chunk_errors.append(f"Chunk {chunk_num}: {agent_result.error}")
                    continue

                # Only accept mappings for URIs this chunk was asked to process
                for em in agent_result.entity_mappings:
                    uri = em.get("ontology_class") or em.get("class_uri", "")
                    if uri and uri in chunk_entity_uris:
                        entity_mapping_by_uri[uri] = em
                for rm in agent_result.relationship_mappings:
                    uri = rm.get("property", "")
                    if uri and uri in chunk_rel_uris:
                        rel_mapping_by_uri[uri] = rm

                all_steps.extend(agent_result.steps)
                total_iterations += agent_result.iterations
                for k in total_usage:
                    total_usage[k] += agent_result.usage.get(k, 0)

                e_done = len(entity_mapping_by_uri)
                r_done = len(rel_mapping_by_uri)

                tm.update_progress(
                    task.id,
                    _chunk_pct(chunk_idx, num_chunks, 100),
                    f"[{chunk_num}/{num_chunks}] Entities: {e_done}/{len(entities)}, Relationships: {r_done}/{len(relationships)}",
                )
                task.result = {
                    "live_stats": True,
                    "entities_assigned": e_done,
                    "entities_total": len(entities),
                    "relationships_assigned": r_done,
                    "relationships_total": len(relationships),
                }

                logger.info(
                    "Chunk %d/%d done: +%d entities, +%d rels (cumulative: %d entities, %d rels)",
                    chunk_num, num_chunks,
                    agent_result.stats.get("entities", 0),
                    agent_result.stats.get("relationships", 0),
                    e_done, r_done,
                )

            # --- All chunks processed ---
            all_entity_mappings = list(entity_mapping_by_uri.values())
            all_relationship_mappings = list(rel_mapping_by_uri.values())
            e_count = len(all_entity_mappings)
            r_count = len(all_relationship_mappings)

            logger.info(
                "===== AUTO-ASSIGN AGENT DONE ===== entities=%d, relationships=%d, iterations=%d, chunks=%d, errors=%d",
                e_count, r_count, total_iterations, num_chunks, len(chunk_errors),
            )
            logger.info(
                "Auto-assign: usage — prompt_tokens=%d, completion_tokens=%d",
                total_usage.get("prompt_tokens", 0), total_usage.get("completion_tokens", 0),
            )
            for em in all_entity_mappings:
                logger.info(
                    "Auto-assign: entity mapping — class=%s, id=%s, label=%s, attrs=%d",
                    em.get("class_name", "?"), em.get("id_column", "?"),
                    em.get("label_column", "?"), len(em.get("attribute_mappings", {})),
                )
            for rm in all_relationship_mappings:
                logger.info(
                    "Auto-assign: relationship mapping — prop=%s, src=%s, tgt=%s",
                    rm.get("property_name", "?"), rm.get("source_id_column", "?"),
                    rm.get("target_id_column", "?"),
                )

            if e_count == 0 and r_count == 0:
                error_detail = "; ".join(chunk_errors) if chunk_errors else "No mappings produced"
                logger.error("Auto-assign: no mappings produced — %s", error_detail)
                tm.fail_task(task.id, error_detail)
                return

            per_item_results = Mapping.build_per_item_results(
                entities, relationships,
                all_entity_mappings,
                all_relationship_mappings,
            )

            Mapping.save_mappings_to_session(
                session_id, session_ref,
                all_entity_mappings,
                all_relationship_mappings,
                existing_entity_mappings=entity_mappings,
                existing_relationship_mappings=relationship_mappings,
            )

            message = f"Completed: {e_count} entities, {r_count} relationships mapped"
            if chunk_errors:
                message += f" ({len(chunk_errors)} chunk(s) had errors)"

            tm.complete_task(task.id, result={
                'results': per_item_results,
                'stats': {
                    'total': total_items,
                    'success': e_count + r_count,
                    'failed': total_items - e_count - r_count,
                },
                'entity_mappings': all_entity_mappings,
                'relationship_mappings': all_relationship_mappings,
                'agent_steps': serialize_agent_steps(all_steps),
                'agent_iterations': total_iterations,
                'agent_usage': total_usage,
            }, message=message)

        except Exception as e:
            logger.exception("===== AUTO-ASSIGN AGENT FAILED ===== %s", e)
            tm.fail_task(task.id, "Auto-assign failed unexpectedly")

    # Start thread
    thread = threading.Thread(target=run_auto_assign, daemon=True)
    thread.start()
    logger.info("Auto-assign: background thread started — task=%s", task.id)
    
    return {
        'success': True,
        'task_id': task.id,
        'message': 'Auto-map started'
    }


# ===========================================
# Single-item Auto-Map (async task)
# ===========================================

SINGLE_ITEM_MAX_ITERATIONS = 15

@router.post("/auto-assign/single")
async def single_auto_assign(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings)
):
    """Start the auto-mapping agent for a single entity or relationship.

    Returns a task_id immediately; the caller polls /tasks/{task_id} for the result.
    Uses the same agent as the batch auto-map but scoped to one item.
    """
    import threading

    data = await request.json()
    item_type = data.get("type")  # "entity" or "relationship"
    item = data.get("item")       # entity or relationship dict

    if item_type not in ("entity", "relationship") or not item:
        raise ValidationError("Provide type ('entity'|'relationship') and item.")

    domain = get_domain(session_mgr)
    host, token, warehouse_id = get_databricks_credentials(domain, settings)

    if not host or not token:
        raise ValidationError("Databricks not configured")
    if not warehouse_id:
        raise ValidationError("No SQL warehouse configured")

    llm_endpoint = domain.info.get("llm_endpoint", "")
    if not llm_endpoint:
        raise ValidationError("No LLM serving endpoint configured")

    schema_context, schema_err = Mapping(domain).resolve_auto_assign_schema_context(None)
    if schema_err:
        raise ValidationError("Schema context could not be resolved", detail=schema_err)

    client = DatabricksClient(host=host, token=token, warehouse_id=warehouse_id)

    item_name = item.get("name", "?")
    entities = [item] if item_type == "entity" else []
    relationships = [item] if item_type == "relationship" else []
    ontology_payload = {"entities": entities, "relationships": relationships}

    logger.info(
        "Single auto-assign: type=%s, name=%s, endpoint=%s",
        item_type, item_name, llm_endpoint,
    )

    tm = get_task_manager()
    task = tm.create_task(
        name=f"Auto-Map: {item_name}",
        task_type="auto_assign_single",
        steps=[
            {"name": "init", "description": "Starting agent"},
            {"name": "process", "description": f"Processing {item_type}: {item_name}"},
            {"name": "finalize", "description": "Finalizing mapping"},
        ],
    )
    logger.info("Single auto-assign: task created — id=%s", task.id)

    session_id = getattr(request.state, 'session_id', None)
    session_ref = getattr(request.state, 'session', None)
    existing_entity_mappings = list(domain.get_entity_mappings())
    existing_relationship_mappings = list(domain.get_relationship_mappings())

    def _run():
        try:
            tm.start_task(task.id, f"Auto-mapping {item_type}: {item_name}…")

            documents = Mapping.fetch_documents_for_agent(domain, host, token)

            def on_step(msg: str, progress_pct: int = 0):
                tm.update_progress(task.id, progress_pct, msg)

            agent_result = Mapping(domain).auto_assign_with_agent(
                host=host,
                token=token,
                endpoint_name=llm_endpoint,
                client=client,
                metadata=schema_context,
                ontology=ontology_payload,
                documents=documents,
                max_iterations=SINGLE_ITEM_MAX_ITERATIONS,
                on_step=on_step,
            )

            if not agent_result.success:
                logger.warning("Single auto-assign agent failed: %s", agent_result.error)
                tm.fail_task(task.id, agent_result.error or "Agent failed")
                return

            mapping = None
            if item_type == "entity" and agent_result.entity_mappings:
                mapping = agent_result.entity_mappings[0]
                logger.info(
                    "Single auto-assign entity result: class=%s, id=%s, label=%s, attrs=%d",
                    mapping.get("class_name", "?"),
                    mapping.get("id_column", "?"),
                    mapping.get("label_column", "?"),
                    len(mapping.get("attribute_mappings", {})),
                )
            elif item_type == "relationship" and agent_result.relationship_mappings:
                mapping = agent_result.relationship_mappings[0]
                logger.info(
                    "Single auto-assign rel result: prop=%s, src=%s, tgt=%s",
                    mapping.get("property_name", "?"),
                    mapping.get("source_id_column", "?"),
                    mapping.get("target_id_column", "?"),
                )

            if not mapping:
                tm.fail_task(task.id, "Agent completed but produced no mapping")
                return

            # Persist the single mapping result to the session
            if item_type == "entity":
                Mapping.save_mappings_to_session(
                    session_id, session_ref,
                    agent_result.entity_mappings,
                    None,
                    existing_entity_mappings=existing_entity_mappings,
                )
            else:
                Mapping.save_mappings_to_session(
                    session_id, session_ref,
                    None,
                    agent_result.relationship_mappings,
                    existing_relationship_mappings=existing_relationship_mappings,
                )

            tm.complete_task(task.id, result={
                "item_type": item_type,
                "mapping": mapping,
                "iterations": agent_result.iterations,
            }, message=f"Assigned {item_type}: {item_name}")

        except Exception as exc:
            logger.exception("Single auto-assign thread error: %s", exc)
            tm.fail_task(task.id, "Single auto-assign failed unexpectedly")

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()

    return {"success": True, "task_id": task.id}
