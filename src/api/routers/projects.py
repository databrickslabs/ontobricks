"""External project API: registry listing, versions, design readiness, and artifacts (OWL, R2RML, Spark SQL).

Mounted at ``/api/v1/projects`` and ``/api/v1/project/...`` (prefix ``/v1`` on the sub-app).
"""
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from api.constants import DEFAULT_BASE_URI
from shared.config.settings import Settings, get_settings
from back.core.errors import ValidationError, InfrastructureError, NotFoundError
from back.core.logging import get_logger
from back.objects.digitaltwin import DigitalTwin
from back.objects.registry import RegistryCfg, RegistryService
from back.objects.session import SessionManager, get_project, get_session_manager

logger = get_logger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class OWLResponse(BaseModel):
    success: bool
    format: str = "turtle"
    content: str = ""
    base_uri: Optional[str] = None
    class_count: int = 0
    property_count: int = 0
    message: Optional[str] = None


class R2RMLResponse(BaseModel):
    success: bool
    format: str = "turtle"
    content: str = ""
    base_uri: Optional[str] = None
    entity_count: int = 0
    relationship_count: int = 0
    message: Optional[str] = None


class SparkSQLResponse(BaseModel):
    success: bool
    sql: str = ""
    base_uri: Optional[str] = None
    dialect: str = "spark"
    message: Optional[str] = None


class ProjectInfo(BaseModel):
    name: str
    description: str = ""


class ProjectsResponse(BaseModel):
    success: bool
    projects: List[ProjectInfo] = Field(default_factory=list)
    message: Optional[str] = None


class OntologyStatus(BaseModel):
    ready: bool = False
    base_uri: Optional[str] = None
    class_count: int = 0
    property_count: int = 0
    constraint_count: int = 0
    has_owl: bool = False


class MetadataStatus(BaseModel):
    ready: bool = False
    table_count: int = 0


class AssignmentStatus(BaseModel):
    ready: bool = False
    entity_total: int = 0
    entity_mapped: int = 0
    relationship_total: int = 0
    relationship_mapped: int = 0
    attribute_total: int = 0
    attribute_mapped: int = 0
    progress_percent: int = 0
    status: str = "not_started"
    has_r2rml: bool = False


class DesignStatusResponse(BaseModel):
    success: bool
    project_name: Optional[str] = None
    ontology: Optional[OntologyStatus] = None
    metadata: Optional[MetadataStatus] = None
    assignment: Optional[AssignmentStatus] = None
    build_ready: bool = False
    message: Optional[str] = None


class VersionInfo(BaseModel):
    version: str
    is_latest: bool = False


class VersionsResponse(BaseModel):
    success: bool
    project_name: Optional[str] = None
    versions: List[VersionInfo] = Field(default_factory=list)
    latest_version: Optional[str] = None
    message: Optional[str] = None


# ---------------------------------------------------------------------------
# GET /projects
# ---------------------------------------------------------------------------


@router.get(
    "/projects",
    response_model=ProjectsResponse,
    summary="List registry projects",
    description="Return all projects stored in the registry with their name and description.",
)
async def list_registry_projects(
    registry_catalog: Optional[str] = Query(None, description="Override registry catalog"),
    registry_schema: Optional[str] = Query(None, description="Override registry schema"),
    registry_volume: Optional[str] = Query(None, description="Override registry volume"),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    project = get_project(session_mgr)
    reg = DigitalTwin.resolve_registry(session_mgr, settings, registry_catalog, registry_schema, registry_volume)
    cfg = RegistryCfg.from_dict(reg)
    if not cfg.is_configured:
        raise ValidationError("Registry not configured")

    svc = RegistryService(cfg, DigitalTwin.uc_from_project(project, settings))
    ok, items, msg = svc.list_mcp_projects()
    if not ok:
        raise InfrastructureError(msg or "Failed to list projects")
    return ProjectsResponse(
        success=True,
        projects=[ProjectInfo(name=p['name'], description=p['description']) for p in items],
    )


# ---------------------------------------------------------------------------
# GET /project/versions
# ---------------------------------------------------------------------------


@router.get(
    "/project/versions",
    response_model=VersionsResponse,
    summary="List project versions",
    description="Return all versions available for a given project in the registry, "
    "sorted from latest to oldest.",
)
async def list_project_versions(
    project_name: str = Query(..., description="Project name in the registry"),
    registry_catalog: Optional[str] = Query(None, description="Override registry catalog"),
    registry_schema: Optional[str] = Query(None, description="Override registry schema"),
    registry_volume: Optional[str] = Query(None, description="Override registry volume"),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    project = get_project(session_mgr)
    reg = DigitalTwin.resolve_registry(session_mgr, settings, registry_catalog, registry_schema, registry_volume)
    cfg = RegistryCfg.from_dict(reg)
    if not cfg.is_configured:
        raise ValidationError("Registry not configured")

    svc = RegistryService(cfg, DigitalTwin.uc_from_project(project, settings))

    versions_sorted = svc.list_versions_sorted(project_name, reverse=True)
    if not versions_sorted:
        raise NotFoundError(f"No versions found for project '{project_name}'")

    latest = versions_sorted[0]
    return VersionsResponse(
        success=True,
        project_name=project_name,
        versions=[
            VersionInfo(version=v, is_latest=(v == latest))
            for v in versions_sorted
        ],
        latest_version=latest,
    )


# ---------------------------------------------------------------------------
# GET /project/design-status
# ---------------------------------------------------------------------------


@router.get(
    "/project/design-status",
    response_model=DesignStatusResponse,
    summary="Get design status",
    description="Return the readiness status of the project's ontology, metadata, "
    "and assignment. Indicates whether the project is ready to build "
    "(sync triples to the triple store).",
)
async def get_project_design_status(
    project_name: Optional[str] = Query(None, description="Project name in the registry (uses current session project if omitted)"),
    project_version: Optional[str] = Query(None, description="Project version to load (uses latest version if omitted)"),
    registry_catalog: Optional[str] = Query(None, description="Override registry catalog"),
    registry_schema: Optional[str] = Query(None, description="Override registry schema"),
    registry_volume: Optional[str] = Query(None, description="Override registry volume"),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    project = DigitalTwin.resolve_project(
        project_name, session_mgr, settings,
        registry_catalog, registry_schema, registry_volume, project_version,
    )

    proj_name = project.project_folder or (project.info or {}).get('name', '')

    classes = project.get_classes() or []
    properties = project.ontology.get('properties', [])
    constraints = project.constraints or []
    base_uri = project.ontology.get('base_uri', '')
    ontology_valid = project.is_ontology_valid()

    project.ensure_generated_content()
    has_owl = bool(project.generated.get('owl'))

    ontology_status = OntologyStatus(
        ready=ontology_valid,
        base_uri=base_uri or None,
        class_count=len(classes),
        property_count=len(properties),
        constraint_count=len(constraints),
        has_owl=has_owl,
    )

    meta = project._data.get('project', {}).get('metadata', {})
    tables = meta.get('tables', [])
    metadata_status = MetadataStatus(
        ready=len(tables) > 0,
        table_count=len(tables),
    )

    all_entities = project.get_entity_mappings() or []
    all_relationships = project.get_relationship_mappings() or []

    excluded_class_uris = {
        m.get('ontology_class') for m in all_entities if m.get('excluded')
    }
    active_classes = [c for c in classes if c.get('uri') and c['uri'] not in excluded_class_uris]

    obj_properties = [
        p for p in properties
        if p.get('type') in ('ObjectProperty', 'owl:ObjectProperty')
        or (not p.get('type') and p.get('range') and not DigitalTwin.is_datatype_range(p.get('range', '')))
    ]
    excluded_class_names = {
        c.get('name') or c.get('localName', '')
        for c in classes if c.get('uri') in excluded_class_uris
    }
    active_properties = [
        p for p in obj_properties
        if not p.get('excluded')
        and p.get('domain', '') not in excluded_class_names
        and p.get('range', '') not in excluded_class_names
    ]

    entity_total = len(active_classes)
    relationship_total = len(active_properties)

    active_class_uris = {c['uri'] for c in active_classes}
    entity_mapped = 0
    mapping_by_class = {}
    for m in all_entities:
        uri = m.get('ontology_class') or m.get('class_uri', '')
        if uri in active_class_uris and (m.get('sql_query') or m.get('table_name')):
            entity_mapped += 1
            mapping_by_class[uri] = m

    active_prop_uris = {p.get('uri') for p in active_properties if p.get('uri')}
    relationship_mapped = sum(
        1 for r in all_relationships if r.get('property') in active_prop_uris
    )

    attribute_total = 0
    attribute_mapped = 0
    for cls in active_classes:
        data_props = cls.get('dataProperties', [])
        attribute_total += len(data_props)
        em = mapping_by_class.get(cls.get('uri', ''))
        if em:
            attr_map = em.get('attribute_mappings', {})
            for dp in data_props:
                attr_name = dp.get('name') or dp.get('localName', '')
                if attr_name and attr_map.get(attr_name):
                    attribute_mapped += 1

    total_items = entity_total + relationship_total + attribute_total
    mapped_items = entity_mapped + relationship_mapped + attribute_mapped
    progress = round((mapped_items / total_items) * 100) if total_items > 0 else 0

    if total_items == 0:
        status_label = 'not_started'
    elif mapped_items == 0:
        status_label = 'not_started'
    elif mapped_items >= total_items:
        status_label = 'complete'
    else:
        status_label = 'in_progress'

    has_r2rml = bool(project.get_r2rml())

    assignment_status = AssignmentStatus(
        ready=status_label == 'complete',
        entity_total=entity_total,
        entity_mapped=entity_mapped,
        relationship_total=relationship_total,
        relationship_mapped=relationship_mapped,
        attribute_total=attribute_total,
        attribute_mapped=attribute_mapped,
        progress_percent=progress,
        status=status_label,
        has_r2rml=has_r2rml,
    )

    build_ready = ontology_valid and status_label == 'complete' and has_r2rml

    logger.info(
        "API: design-status for '%s' — ontology=%s, metadata=%d tables, "
        "assignment=%s (%d%%), build_ready=%s",
        proj_name, ontology_valid, len(tables),
        status_label, progress, build_ready,
    )

    return DesignStatusResponse(
        success=True,
        project_name=proj_name,
        ontology=ontology_status,
        metadata=metadata_status,
        assignment=assignment_status,
        build_ready=build_ready,
    )


# ---------------------------------------------------------------------------
# GET /project/ontology, /project/r2rml, /project/sparksql
# ---------------------------------------------------------------------------


@router.get(
    "/project/ontology",
    response_model=OWLResponse,
    summary="Get project ontology (OWL/Turtle)",
    description="Return the project's ontology serialized as OWL in Turtle format. "
    "Includes the full OWL document, base URI, and class/property counts.",
)
async def get_project_ontology(
    project_name: Optional[str] = Query(None, description="Project name in the registry (uses current session project if omitted)"),
    project_version: Optional[str] = Query(None, description="Project version to load (uses latest version if omitted)"),
    registry_catalog: Optional[str] = Query(None, description="Override registry catalog"),
    registry_schema: Optional[str] = Query(None, description="Override registry schema"),
    registry_volume: Optional[str] = Query(None, description="Override registry volume"),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    project = DigitalTwin.resolve_project(
        project_name, session_mgr, settings,
        registry_catalog, registry_schema, registry_volume, project_version,
    )

    classes = project.get_classes()
    properties = project.ontology.get('properties', [])
    if not classes:
        raise ValidationError("Project has no ontology classes defined")

    project.ensure_generated_content()
    owl_content = project.generated.get('owl', '')

    if not owl_content:
        try:
            from back.objects.ontology import Ontology
            owl_content = Ontology.generate_owl(
                project.ontology,
                project.constraints,
                project.swrl_rules,
                project.axioms,
                project.expressions,
            )
            project.generated['owl'] = owl_content
        except Exception as e:
            logger.exception("OWL generation failed: %s", e)
            raise InfrastructureError(f"OWL generation failed: {e}") from e

    if not owl_content:
        raise InfrastructureError("Could not generate OWL content")

    base_uri = project.ontology.get('base_uri', DEFAULT_BASE_URI)
    logger.info(
        "API: returning OWL for project '%s' (%d classes, %d properties)",
        project.project_folder or '(session)', len(classes), len(properties),
    )

    return OWLResponse(
        success=True,
        format='turtle',
        content=owl_content,
        base_uri=base_uri,
        class_count=len(classes),
        property_count=len(properties),
    )


@router.get(
    "/project/r2rml",
    response_model=R2RMLResponse,
    summary="Get R2RML mapping",
    description="Return the project's R2RML mapping document in Turtle format. "
    "R2RML defines how relational tables map to RDF triples.",
)
async def get_project_r2rml(
    project_name: Optional[str] = Query(None, description="Project name in the registry (uses current session project if omitted)"),
    project_version: Optional[str] = Query(None, description="Project version to load (uses latest version if omitted)"),
    registry_catalog: Optional[str] = Query(None, description="Override registry catalog"),
    registry_schema: Optional[str] = Query(None, description="Override registry schema"),
    registry_volume: Optional[str] = Query(None, description="Override registry volume"),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    project = DigitalTwin.resolve_project(
        project_name, session_mgr, settings,
        registry_catalog, registry_schema, registry_volume, project_version,
    )

    entities = project.get_entity_mappings()
    relationships = project.get_relationship_mappings()
    if not entities:
        raise ValidationError("Project has no entity mappings defined")

    project.ensure_generated_content()
    r2rml_content = project.get_r2rml()

    if not r2rml_content:
        try:
            from back.core.w3c import R2RMLGenerator
            base_uri = project.ontology.get('base_uri', DEFAULT_BASE_URI)
            generator = R2RMLGenerator(base_uri)
            r2rml_content = generator.generate_mapping(project.assignment, project.ontology)
            project.set_r2rml(r2rml_content)
        except Exception as e:
            logger.exception("R2RML generation failed: %s", e)
            raise InfrastructureError(f"R2RML generation failed: {e}") from e

    if not r2rml_content:
        raise InfrastructureError("Could not generate R2RML content")

    base_uri = project.ontology.get('base_uri', DEFAULT_BASE_URI)
    logger.info(
        "API: returning R2RML for project '%s' (%d entities, %d relationships)",
        project.project_folder or '(session)', len(entities), len(relationships),
    )

    return R2RMLResponse(
        success=True,
        format='turtle',
        content=r2rml_content,
        base_uri=base_uri,
        entity_count=len(entities),
        relationship_count=len(relationships),
    )


@router.get(
    "/project/sparksql",
    response_model=SparkSQLResponse,
    summary="Get generated Spark SQL",
    description="Return the Spark SQL query generated from the project's R2RML mappings. "
    "This is the SQL that produces (subject, predicate, object) triples when "
    "executed against the source tables.",
)
async def get_project_sparksql(
    project_name: Optional[str] = Query(None, description="Project name in the registry (uses current session project if omitted)"),
    project_version: Optional[str] = Query(None, description="Project version to load (uses latest version if omitted)"),
    registry_catalog: Optional[str] = Query(None, description="Override registry catalog"),
    registry_schema: Optional[str] = Query(None, description="Override registry schema"),
    registry_volume: Optional[str] = Query(None, description="Override registry volume"),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    from back.core.w3c import sparql

    project = DigitalTwin.resolve_project(
        project_name, session_mgr, settings,
        registry_catalog, registry_schema, registry_volume, project_version,
    )
    project.ensure_generated_content()

    r2rml_content = project.get_r2rml()
    if not r2rml_content:
        raise ValidationError("No R2RML mapping available. Configure mappings first.")

    base_uri = project.ontology.get('base_uri', DEFAULT_BASE_URI)
    try:
        entity_mappings, relationship_mappings = sparql.extract_r2rml_mappings(r2rml_content)
        entity_mappings = DigitalTwin.augment_mappings_from_config(
            entity_mappings, project.assignment, base_uri, project.ontology
        )
        relationship_mappings = DigitalTwin.augment_relationships_from_config(
            relationship_mappings, project.assignment, base_uri, project.ontology
        )

        if not entity_mappings and not relationship_mappings:
            raise ValidationError("No valid mappings found in R2RML")

        sparql_query = (
            f"PREFIX : <{base_uri}>\n"
            "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
            "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n\n"
            "SELECT ?subject ?predicate ?object\n"
            "WHERE {\n"
            "    ?subject ?predicate ?object .\n"
            "}"
        )

        result = sparql.translate_sparql_to_spark(
            sparql_query, entity_mappings, None, relationship_mappings,
        )

        if not result.get('success'):
            raise InfrastructureError(result.get('message', 'SQL translation failed'))

        sql_content = result.get('sql', '')
        logger.info(
            "API: returning Spark SQL for project '%s' (%d chars)",
            project.project_folder or '(session)', len(sql_content),
        )

        return SparkSQLResponse(
            success=True,
            sql=sql_content,
            base_uri=base_uri,
        )
    except Exception as e:
        logger.exception("Spark SQL generation failed: %s", e)
        raise InfrastructureError(f"SQL generation failed: {e}") from e
