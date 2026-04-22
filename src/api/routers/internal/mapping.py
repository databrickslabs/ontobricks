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
from back.core.logging import get_logger
from back.objects.session import SessionManager, get_domain, get_session_manager
from back.core.task_manager import get_task_manager
from back.objects.mapping import Mapping
from back.core.errors import (
    OntoBricksError,
    ValidationError,
    InfrastructureError,
    NotFoundError,
)

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
        "success": True,
        "config": {
            "entities": entity_mappings,
            "relationships": relationship_mappings,
            "data_source_mappings": entity_mappings,  # backward compat
            "relationship_mappings": relationship_mappings,  # backward compat
            "r2rml_output": domain.get_r2rml(),
        },
        "r2rml_output": domain.get_r2rml(),
    }


@router.post("/save")
async def save_mapping(
    request: Request, session_mgr: SessionManager = Depends(get_session_manager)
):
    """Save mapping configuration to session."""
    data = await request.json()
    mapping_config = data.get("config", data)  # Support both wrapped and unwrapped

    domain = get_domain(session_mgr)
    stats = Mapping(domain).save_mapping_config(mapping_config)
    return {"success": True, "message": "Mapping saved", "stats": stats}


@router.post("/reset")
async def reset_mapping_endpoint(
    session_mgr: SessionManager = Depends(get_session_manager),
):
    """Reset mapping configuration."""
    domain = get_domain(session_mgr)
    Mapping(domain).reset_mapping()
    return {"success": True, "message": "Mapping reset"}


@router.post("/clear")
async def clear_mapping(session_mgr: SessionManager = Depends(get_session_manager)):
    """Clear all mapping data from session (delegates to reset)."""
    return await reset_mapping_endpoint(session_mgr)


# ===========================================
# Entity Mapping API
# ===========================================


@router.post("/entity/add")
async def add_entity_mapping(
    request: Request, session_mgr: SessionManager = Depends(get_session_manager)
):
    """Add an entity mapping."""
    data = await request.json()
    domain = get_domain(session_mgr)
    _, new_mapping = Mapping(domain).add_or_update_entity_mapping(data)
    return {"success": True, "mapping": new_mapping}


@router.post("/entity/delete")
async def delete_entity_mapping(
    request: Request, session_mgr: SessionManager = Depends(get_session_manager)
):
    """Delete an entity mapping."""
    data = await request.json()
    domain = get_domain(session_mgr)
    ontology_class = data.get("ontology_class")

    if Mapping(domain).delete_entity_mapping(ontology_class):
        return {"success": True}
    raise NotFoundError("Mapping not found")


# ===========================================
# Relationship Mapping API
# ===========================================


@router.post("/relationship/add")
async def add_relationship_mapping(
    request: Request, session_mgr: SessionManager = Depends(get_session_manager)
):
    """Add a relationship mapping."""
    data = await request.json()
    domain = get_domain(session_mgr)
    _, new_mapping = Mapping(domain).add_or_update_relationship_mapping(data)
    return {"success": True, "mapping": new_mapping}


@router.post("/relationship/delete")
async def delete_relationship_mapping(
    request: Request, session_mgr: SessionManager = Depends(get_session_manager)
):
    """Delete a relationship mapping."""
    data = await request.json()
    domain = get_domain(session_mgr)
    property_uri = data.get("property")

    if Mapping(domain).delete_relationship_mapping(property_uri):
        return {"success": True}
    raise NotFoundError("Mapping not found")


# ===========================================
# Exclude / Include Entities
# ===========================================


@router.post("/exclude")
async def toggle_exclude(
    request: Request, session_mgr: SessionManager = Depends(get_session_manager)
):
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
    uris = data.get("uris", [])
    excluded = bool(data.get("excluded", True))
    item_type = data.get("item_type", "entity")

    domain = get_domain(session_mgr)
    changed = Mapping(domain).toggle_exclude_items(uris, excluded, item_type)
    logger.info("Toggled excluded=%s for %d %s(s)", excluded, changed, item_type)
    return {"success": True, "changed": changed}


# ===========================================
# R2RML Generation
# ===========================================


@router.post("/generate")
async def generate_r2rml(session_mgr: SessionManager = Depends(get_session_manager)):
    """Generate R2RML from current mapping configuration."""
    domain = get_domain(session_mgr)

    m = Mapping(domain)
    r2rml_content = m.generate_r2rml()
    return {"success": True, "r2rml": r2rml_content, "stats": m.get_mapping_stats()}


# ===========================================
# SQL Query Testing
# ===========================================


@router.post("/test-query")
async def test_sql_query(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Test a SQL query and return column information."""
    data = await request.json()
    sql_query = data.get("query") or data.get("sql_query", "")
    limit = data.get("limit", 100)

    if not sql_query:
        raise ValidationError("No SQL query provided")

    try:
        domain = get_domain(session_mgr)
        host, token, warehouse_id = get_databricks_credentials(domain, settings)

        if not warehouse_id:
            raise ValidationError("No SQL warehouse configured")

        client = DatabricksClient(host=host, token=token, warehouse_id=warehouse_id)
        result = await run_blocking(
            Mapping.test_sql_query, client, sql_query, limit=int(limit)
        )

        return {"success": True, **result}
    except OntoBricksError:
        raise
    except Exception as e:
        sql_preview = sql_query.replace("\n", " ").strip()[:500]
        logger.warning(
            "test_sql_query failed (warehouse=%s, sql=%r): %s",
            warehouse_id if "warehouse_id" in locals() else "?",
            sql_preview,
            e,
            exc_info=True,
        )
        raise InfrastructureError("SQL query test failed", detail=str(e)) from e


# ===========================================
# Table & Column Discovery
# ===========================================


@router.post("/tables")
async def get_tables(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Get tables for selected catalog and schema."""
    data = await request.json()
    catalog, schema = data.get("catalog"), data.get("schema")

    if not catalog or not schema:
        raise ValidationError("Catalog and schema are required")

    try:
        domain = get_domain(session_mgr)
        client = get_databricks_client(domain, settings)
        if not client:
            raise ValidationError("Databricks not configured")
        return {"tables": await run_blocking(client.get_tables, catalog, schema)}
    except OntoBricksError:
        raise
    except Exception as e:
        logger.warning(
            "get_tables failed for %s.%s: %s", catalog, schema, e, exc_info=True
        )
        raise InfrastructureError("Failed to list tables", detail=str(e)) from e


@router.post("/table-columns")
async def get_table_columns(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Get columns for a specific table."""
    data = await request.json()
    catalog, schema, table = data.get("catalog"), data.get("schema"), data.get("table")

    if not all([catalog, schema, table]):
        raise ValidationError("Catalog, schema, and table are required")

    try:
        domain = get_domain(session_mgr)
        client = get_databricks_client(domain, settings)
        if not client:
            raise ValidationError("Databricks not configured")
        return {
            "columns": await run_blocking(
                client.get_table_columns, catalog, schema, table
            )
        }
    except OntoBricksError:
        raise
    except Exception as e:
        logger.exception("Get table columns failed: %s", e)
        raise InfrastructureError("Failed to list table columns", detail=str(e)) from e


# ===========================================
# R2RML Import/Export
# ===========================================


@router.post("/parse-r2rml")
async def parse_r2rml(
    request: Request, session_mgr: SessionManager = Depends(get_session_manager)
):
    """Parse R2RML content and extract mappings."""
    data = await request.json()
    r2rml_content = data.get("content", "")

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
        headers={"Content-Disposition": "attachment; filename=mapping.ttl"},
    )


@router.post("/save-to-uc")
async def save_mapping_to_uc(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Save R2RML mapping to Unity Catalog volume."""
    from api.routers.internal._helpers import save_content_to_uc

    return await save_content_to_uc(
        request, session_mgr, settings, log_context="mapping"
    )


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
    settings: Settings = Depends(get_settings),
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
        return {"success": True, "endpoints": endpoints}

    except OntoBricksError:
        raise
    except Exception as e:
        logger.exception("Get LLM endpoints failed: %s", e)
        raise InfrastructureError(
            "Failed to list model serving endpoints", detail=str(e)
        ) from e


@router.post("/wizard/schema-context")
async def get_schema_context(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Get schema context (tables and columns) for SQL generation."""
    try:
        from back.core.sqlwizard import SQLWizardService

        data = await request.json()
        catalog = data.get("catalog")
        schema = data.get("schema")

        if not catalog or not schema:
            raise ValidationError("Catalog and schema are required")

        domain = get_domain(session_mgr)
        client = get_databricks_client(domain, settings)

        if not client:
            raise ValidationError("Databricks not configured")

        wizard = SQLWizardService(client)
        context = wizard.get_schema_context(catalog, schema)

        return {
            "success": True,
            "context": {"tables": context.tables, "rendered": context.to_yaml_like()},
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
    settings: Settings = Depends(get_settings),
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

        endpoint_name = data.get("endpoint_name")
        catalog = data.get("catalog")  # deprecated - only used if no schema_context
        schema = data.get("schema")  # deprecated - only used if no schema_context
        prompt = data.get("prompt")
        limit = data.get("limit", 100)
        validate_plan = data.get("validate_plan", True)
        schema_context = data.get(
            "schema_context"
        )  # Pre-built context with tables (each has full_name)
        mapping_type = data.get("mapping_type")  # 'entity', 'relationship', or None

        if not endpoint_name or not prompt:
            raise ValidationError("Missing required fields: endpoint_name, prompt")

        # If no schema_context provided, we need catalog/schema to fetch from UC (deprecated path)
        if not schema_context or not schema_context.get("tables"):
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
            schema=schema,  # Only used if no schema_context
        )

        return result

    except OntoBricksError:
        raise
    except Exception as e:
        logger.exception("Generate SQL from prompt failed: %s", e)
        raise InfrastructureError(
            "SQL generation from prompt failed", detail=str(e)
        ) from e


@router.post("/wizard/validate-sql")
async def validate_sql(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
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

        sql = data.get("sql")
        catalog = data.get("catalog")
        schema = data.get("schema")
        validate_plan = data.get("validate_plan", False)

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


@router.post("/auto-assign/start")
async def start_auto_assign(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Start async auto-map process and return task ID."""
    import threading

    data = await request.json()
    entities = data.get("entities", [])
    relationships = data.get("relationships", [])

    total_items = len(entities) + len(relationships)
    logger.info(
        "===== AUTO-ASSIGN START ===== entities=%d, relationships=%d, total=%d",
        len(entities),
        len(relationships),
        total_items,
    )
    if total_items == 0:
        logger.info("Auto-assign: nothing to process — returning early")
        raise ValidationError("No items to process")

    # Validate configuration
    domain = get_domain(session_mgr)
    host, token, warehouse_id = get_databricks_credentials(domain, settings)

    try:
        schema_context = Mapping(domain).resolve_auto_assign_schema_context(
            data.get("schema_context") or {}
        )
    except ValidationError as exc:
        logger.warning("Auto-assign: %s", exc.message)
        raise ValidationError(
            "Schema context could not be resolved", detail=exc.message
        ) from exc
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

    llm_endpoint = domain.info.get("llm_endpoint", "")
    if not llm_endpoint:
        logger.warning("Auto-assign: no LLM serving endpoint configured")
        raise ValidationError("No LLM serving endpoint configured")

    logger.info(
        "Auto-assign: config OK — host=%s, warehouse=%s, llm_endpoint=%s",
        host[:40] + "…" if len(host) > 40 else host,
        warehouse_id,
        llm_endpoint,
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
            {"name": "init", "description": "Initializing auto-map"},
            {"name": "entities", "description": f"Processing {len(entities)} entities"},
            {
                "name": "relationships",
                "description": f"Processing {len(relationships)} relationships",
            },
            {"name": "finalize", "description": "Finalizing mappings"},
        ],
    )
    logger.info("Auto-assign: task created — id=%s", task.id)

    entity_mappings = list(domain.get_entity_mappings())
    relationship_mappings = list(domain.get_relationship_mappings())
    logger.info(
        "Auto-assign: existing mappings — %d entity, %d relationship",
        len(entity_mappings),
        len(relationship_mappings),
    )

    session_id = getattr(request.state, "session_id", None)
    session_ref = getattr(request.state, "session", None)
    mapping_svc = Mapping(domain)

    def run_auto_assign():
        mapping_svc.run_auto_assign_task(
            task,
            entities=entities,
            relationships=relationships,
            host=host,
            token=token,
            client=client,
            llm_endpoint=llm_endpoint,
            schema_context=schema_context,
            session_id=session_id,
            session_ref=session_ref,
            entity_mappings=entity_mappings,
            relationship_mappings=relationship_mappings,
        )

    thread = threading.Thread(target=run_auto_assign, daemon=True)
    thread.start()
    logger.info("Auto-assign: background thread started — task=%s", task.id)

    return {"success": True, "task_id": task.id, "message": "Auto-map started"}


# ===========================================
# Single-item Auto-Map (async task)
# ===========================================


@router.post("/auto-assign/single")
async def single_auto_assign(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Start the auto-mapping agent for a single entity or relationship.

    Returns a task_id immediately; the caller polls /tasks/{task_id} for the result.
    Uses the same agent as the batch auto-map but scoped to one item.
    """
    import threading

    data = await request.json()
    item_type = data.get("type")  # "entity" or "relationship"
    item = data.get("item")  # entity or relationship dict

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

    try:
        schema_context = Mapping(domain).resolve_auto_assign_schema_context(None)
    except ValidationError as exc:
        raise ValidationError(
            "Schema context could not be resolved", detail=exc.message
        ) from exc

    client = DatabricksClient(host=host, token=token, warehouse_id=warehouse_id)

    item_name = item.get("name", "?")

    logger.info(
        "Single auto-assign: type=%s, name=%s, endpoint=%s",
        item_type,
        item_name,
        llm_endpoint,
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

    session_id = getattr(request.state, "session_id", None)
    session_ref = getattr(request.state, "session", None)
    existing_entity_mappings = list(domain.get_entity_mappings())
    existing_relationship_mappings = list(domain.get_relationship_mappings())
    mapping_svc = Mapping(domain)

    def _run():
        mapping_svc.run_single_auto_assign_task(
            task,
            item_type=item_type,
            item=item,
            host=host,
            token=token,
            client=client,
            llm_endpoint=llm_endpoint,
            schema_context=schema_context,
            session_id=session_id,
            session_ref=session_ref,
            existing_entity_mappings=existing_entity_mappings,
            existing_relationship_mappings=existing_relationship_mappings,
        )

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()

    return {"success": True, "task_id": task.id}
