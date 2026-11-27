"""
External REST API Routes (v1)

Stateless API endpoints for external integrations.
All endpoints accept authentication via headers or request body.
"""

from fastapi import APIRouter, Header
from pydantic import AliasChoices, BaseModel, Field
from typing import Any, Literal, Optional

from back.core.databricks import is_databricks_app
from back.core.databricks.request_identity import get_request_identity
from back.core.errors import AuthorizationError, ValidationError, NotFoundError
from back.core.helpers import get_databricks_host_and_token
from shared.config.constants import APP_VERSION
from shared.config.settings import get_settings
from back.objects.domain import resolve_domain_slice
from back.objects.registry import ROLE_ADMIN, ROLE_APP_USER, permission_service
from api import service

router = APIRouter()


# ===========================================
# Pydantic Models for Request/Response
# ===========================================


class CredentialsModel(BaseModel):
    """Base model with optional Databricks credentials."""

    databricks_host: Optional[str] = Field(
        None,
        description=(
            "Databricks workspace URL (e.g., https://my-workspace.cloud.databricks.com). "
            "Local development only: ignored when OntoBricks runs as a Databricks App."
        ),
        examples=["https://my-workspace.cloud.databricks.com"],
    )
    databricks_token: Optional[str] = Field(
        None,
        description=(
            "Token for local development only. Ignored when OntoBricks runs as a "
            "Databricks App, which always uses its own service principal."
        ),
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "databricks_host": "https://my-workspace.cloud.databricks.com",
                    "databricks_token": "dapi...",
                }
            ]
        }
    }


class UCLocationModel(CredentialsModel):
    """Unity Catalog location model for accessing domain JSON files."""

    catalog: str = Field(..., description="Unity Catalog name", examples=["main"])
    schema_name: str = Field(
        ...,
        alias="schema",
        description="Schema name within the catalog",
        examples=["ontobricks"],
    )
    volume: str = Field(
        ..., description="Volume name for file storage", examples=["projects"]
    )

    model_config = {
        "populate_by_name": True,
        "json_schema_extra": {
            "examples": [
                {"catalog": "main", "schema": "ontobricks", "volume": "projects"}
            ]
        },
    }


class DomainPathModel(CredentialsModel):
    """Model with Unity Catalog domain JSON file path."""

    model_config = {"populate_by_name": True}

    domain_path: str = Field(
        ...,
        description="Full path to the context JSON file in Unity Catalog volume",
        validation_alias=AliasChoices("domain_path", "project_path"),
        examples=["/Volumes/main/ontobricks/domains/my_domain.json"],
    )


class QueryModel(DomainPathModel):
    """SPARQL query execution request model."""

    query: str = Field(
        ...,
        description="SPARQL query to execute",
        examples=["SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10"],
    )
    limit: int = Field(
        100, description="Maximum number of results to return", ge=1, le=10000
    )
    engine: str = Field(
        "local",
        description="Query execution engine: 'local' (RDFLib) or 'spark' (Databricks SQL)",
        examples=["local", "spark"],
    )
    output_format: str = Field(
        "rows",
        description=(
            "Result encoding: 'rows' (default tabular binding list) or "
            "'json-ld' (CONSTRUCT/DESCRIBE only). Note: limit truncates "
            "triples, not complete subgraphs."
        ),
        examples=["rows", "json-ld"],
    )


class ValidateQueryModel(BaseModel):
    """SPARQL query validation request model."""

    query: str = Field(
        ...,
        description="SPARQL query to validate",
        examples=["SELECT ?s ?p ?o WHERE { ?s ?p ?o }"],
    )


class SuccessResponse(BaseModel):
    """Standard API success response."""

    success: bool = Field(True, description="Indicates if the request was successful")
    data: Any = Field(..., description="Response payload")
    message: Optional[str] = Field(None, description="Optional message")


class DomainStatisticsResponse(BaseModel):
    """Counts and generated-artifact availability for a domain document."""

    classes: int = 0
    properties: int = 0
    entities: int = 0
    relationships: int = 0
    has_r2rml: bool = False


class DomainFileInfoResponse(BaseModel):
    """Normalized metadata returned for one domain JSON document."""

    name: str
    description: str = ""
    uri: str = ""
    version: str
    status: str = "DRAFT"
    author: str = ""
    graph_backend: Literal["none", "lakebase", "databricks", "neo4j"] = Field(
        default="lakebase",
        description="Normalized backend; 'none' denotes an ontology-only context.",
    )
    statistics: DomainStatisticsResponse


class DomainInfoSuccessResponse(BaseModel):
    """Typed success envelope for stateless domain metadata."""

    success: bool = True
    data: DomainFileInfoResponse
    message: Optional[str] = None


# ===========================================
# Helper Functions
# ===========================================


def _app_credentials() -> tuple[Optional[str], Optional[str]]:
    """Return the service principal's host/token, for a caller with access to the app.

    Raises:
        AuthorizationError: the forwarded identity is missing or has no access
            to this Databricks App (``ROLE_NONE``).
    """
    identity = get_request_identity()
    settings = get_settings()
    host, token = get_databricks_host_and_token(None, settings)
    role = (
        permission_service.get_user_role(
            identity.email,
            host,
            token,
            {},
            settings.ontobricks_app_name,
            user_token=identity.user_token,
        )
        if identity.email
        else ""
    )
    if role not in (ROLE_ADMIN, ROLE_APP_USER):
        raise AuthorizationError(
            "This endpoint runs with the app's service principal and requires "
            "a signed-in user with access to the OntoBricks app."
        )
    return host, token


def get_credentials(
    request_data: Optional[CredentialsModel],
    x_databricks_host: Optional[str] = None,
    x_databricks_token: Optional[str] = None,
) -> tuple[Optional[str], Optional[str]]:
    """Resolve the Databricks credentials a /api/v1 route runs with.

    In Databricks Apps mode caller-supplied host/token (headers or body) are
    ignored and the app's service principal is used: the app never accepts a
    personal access token (Marketplace SOP). Because the service principal can
    read more than the caller, it is only handed to a signed-in user who has
    access to the app (the ``/api/`` prefix skips the role middleware, so the
    check is made here). In local development the ``X-Databricks-*`` headers
    win over the ``databricks_*`` body fields.
    """
    if is_databricks_app():
        return _app_credentials()

    host = x_databricks_host
    token = x_databricks_token

    if not host and request_data:
        host = request_data.databricks_host
    if not token and request_data:
        token = request_data.databricks_token

    return host, token


# ===========================================
# Health Check
# ===========================================


@router.get("/health", summary="API Health Check", tags=["Health"])
async def api_health():
    """
    Check the health status of the API v1 endpoints.

    Returns:
        - **status**: Health status (healthy/unhealthy)
        - **version**: API version
        - **service**: Service name
        - **framework**: Web framework used
    """
    return {
        "status": "healthy",
        "version": APP_VERSION,
        "service": "OntoBricks API",
        "framework": "FastAPI",
    }


# ===========================================
# Domain Endpoints
# ===========================================


@router.post("/domains/list", response_model=SuccessResponse, summary="List Contexts")
async def list_domains(
    data: UCLocationModel,
    x_databricks_host: Optional[str] = Header(
        None, alias="X-Databricks-Host", description="Databricks workspace URL"
    ),
    x_databricks_token: Optional[str] = Header(
        None,
        alias="X-Databricks-Token",
        description="Token for local development only; ignored in Databricks Apps mode",
    ),
):
    """
    List all OntoBricks domain JSON files in a Unity Catalog volume.

    Returns a list of `.json` domain files found in the specified volume location.

    **Authentication:** as a Databricks App, OntoBricks always uses its own
    service principal and ignores any credentials below. For local development
    they can be provided via:
    - HTTP Headers: `X-Databricks-Host`, `X-Databricks-Token`
    - Request body: `databricks_host`, `databricks_token`

    **Returns:**
    - **domains**: List of domain file entries
    - **count**: Number of domains found
    """
    host, token = get_credentials(data, x_databricks_host, x_databricks_token)

    domains = service.list_domains_from_uc(
        data.catalog,
        data.schema_name,
        data.volume,
        host,
        token,
    )

    return SuccessResponse(
        data={"domains": domains, "count": len(domains)},
        message=f"Found {len(domains)} contexts",
    )


@router.post("/domain/info", response_model=DomainInfoSuccessResponse)
async def get_domain_info(
    data: DomainPathModel,
    x_databricks_host: Optional[str] = Header(None, alias="X-Databricks-Host"),
    x_databricks_token: Optional[str] = Header(None, alias="X-Databricks-Token"),
):
    """Get domain information and statistics."""
    host, token = get_credentials(data, x_databricks_host, x_databricks_token)

    domain_data = service.load_domain_from_uc(data.domain_path, host, token)
    info = service.get_domain_info(domain_data)
    return SuccessResponse(data=info)


@router.post("/domain/ontology", response_model=SuccessResponse)
async def get_ontology(
    data: DomainPathModel,
    x_databricks_host: Optional[str] = Header(None, alias="X-Databricks-Host"),
    x_databricks_token: Optional[str] = Header(None, alias="X-Databricks-Token"),
):
    """Get ontology details (classes and properties)."""
    host, token = get_credentials(data, x_databricks_host, x_databricks_token)

    domain_data = service.load_domain_from_uc(data.domain_path, host, token)
    ontology_info = service.get_ontology_info(domain_data)
    return SuccessResponse(data=ontology_info)


@router.post("/domain/ontology/classes", response_model=SuccessResponse)
async def get_ontology_classes(
    data: DomainPathModel,
    x_databricks_host: Optional[str] = Header(None, alias="X-Databricks-Host"),
    x_databricks_token: Optional[str] = Header(None, alias="X-Databricks-Token"),
):
    """Get list of ontology classes with their URIs."""
    host, token = get_credentials(data, x_databricks_host, x_databricks_token)

    domain_data = service.load_domain_from_uc(data.domain_path, host, token)
    classes = service.get_ontology_classes(domain_data)
    return SuccessResponse(data={"classes": classes, "count": len(classes)})


@router.post("/domain/ontology/properties", response_model=SuccessResponse)
async def get_ontology_properties(
    data: DomainPathModel,
    x_databricks_host: Optional[str] = Header(None, alias="X-Databricks-Host"),
    x_databricks_token: Optional[str] = Header(None, alias="X-Databricks-Token"),
):
    """Get list of ontology properties (relationships) with their URIs."""
    host, token = get_credentials(data, x_databricks_host, x_databricks_token)

    domain_data = service.load_domain_from_uc(data.domain_path, host, token)
    properties = service.get_ontology_properties(domain_data)
    return SuccessResponse(data={"properties": properties, "count": len(properties)})


@router.post("/domain/mappings", response_model=SuccessResponse)
async def get_mappings(
    data: DomainPathModel,
    x_databricks_host: Optional[str] = Header(None, alias="X-Databricks-Host"),
    x_databricks_token: Optional[str] = Header(None, alias="X-Databricks-Token"),
):
    """Get mapping details (entity and relationship mappings)."""
    host, token = get_credentials(data, x_databricks_host, x_databricks_token)

    domain_data = service.load_domain_from_uc(data.domain_path, host, token)
    mapping_info = service.get_mapping_info(domain_data)
    return SuccessResponse(data=mapping_info)


@router.post("/domain/r2rml", response_model=SuccessResponse)
async def get_r2rml(
    data: DomainPathModel,
    x_databricks_host: Optional[str] = Header(None, alias="X-Databricks-Host"),
    x_databricks_token: Optional[str] = Header(None, alias="X-Databricks-Token"),
):
    """Get the R2RML mapping content from a domain."""
    host, token = get_credentials(data, x_databricks_host, x_databricks_token)

    domain_data = service.load_domain_from_uc(data.domain_path, host, token)
    sl = resolve_domain_slice(domain_data)
    r2rml_content = sl["assignment"].get("r2rml_output", "")

    if not r2rml_content:
        raise NotFoundError("No R2RML mapping found in context. Generate R2RML first.")

    return SuccessResponse(data={"r2rml": r2rml_content, "format": "turtle"})


# ===========================================
# Query Endpoints
# ===========================================


@router.post("/query", response_model=SuccessResponse, summary="Execute SPARQL Query")
async def execute_query(
    data: QueryModel,
    x_databricks_host: Optional[str] = Header(
        None, alias="X-Databricks-Host", description="Databricks workspace URL"
    ),
    x_databricks_token: Optional[str] = Header(
        None,
        alias="X-Databricks-Token",
        description="Token for local development only; ignored in Databricks Apps mode",
    ),
):
    """
    Execute a SPARQL query against a domain's mapped data.

    The query is translated to SQL using R2RML mappings and executed against
    Databricks.

    Execution engines:

    - **local** — RDFLib for local SPARQL processing (default).
    - **spark** — Spark SQL on Databricks.

    Response fields include **results** (rows), **columns**, **count**, and
    **engine**.

    Example SPARQL (prefixes and graph pattern)::

        PREFIX ex: <http://example.org/>
        SELECT ?person ?name
        WHERE {
            ?person a ex:Person .
            ?person ex:name ?name .
        }
        LIMIT 10

    """
    is_valid, error_msg = service.validate_sparql_query(data.query)
    if not is_valid:
        raise ValidationError(f"Invalid query: {error_msg}")

    host, token = get_credentials(data, x_databricks_host, x_databricks_token)

    domain_data = service.load_domain_from_uc(data.domain_path, host, token)
    result = service.execute_sparql_query(
        domain_data,
        data.query,
        data.limit,
        data.engine,
        output_format=data.output_format,
    )

    payload = {
        "results": result.get("results", []),
        "columns": result.get("columns", []),
        "count": result.get("count", 0),
        "engine": result.get("engine", data.engine),
    }
    if "jsonld" in result:
        payload["jsonld"] = result["jsonld"]
        payload["format"] = result.get("format", "json-ld")
    return SuccessResponse(data=payload)


@router.post("/query/validate", response_model=SuccessResponse)
async def validate_query(data: ValidateQueryModel):
    """Validate a SPARQL query syntax."""
    is_valid, error_msg = service.validate_sparql_query(data.query)

    return SuccessResponse(
        data={"valid": is_valid, "error": error_msg if not is_valid else None}
    )


@router.post("/query/samples", response_model=SuccessResponse)
async def get_sample_queries(
    data: DomainPathModel,
    x_databricks_host: Optional[str] = Header(None, alias="X-Databricks-Host"),
    x_databricks_token: Optional[str] = Header(None, alias="X-Databricks-Token"),
):
    """Get sample SPARQL queries for a domain."""
    host, token = get_credentials(data, x_databricks_host, x_databricks_token)

    domain_data = service.load_domain_from_uc(data.domain_path, host, token)
    samples = service.generate_sample_queries(domain_data)
    return SuccessResponse(data={"queries": samples, "count": len(samples)})
