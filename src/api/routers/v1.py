"""
External REST API Routes (v1)

Stateless API endpoints for external integrations.
All endpoints accept authentication via headers or request body.
"""
from fastapi import APIRouter, Header
from pydantic import AliasChoices, BaseModel, Field
from typing import Optional, Any

from back.core.errors import ValidationError, NotFoundError
from shared.config.constants import APP_VERSION
from back.objects.domain.payload import resolve_domain_slice
from api import service

router = APIRouter()


# ===========================================
# Pydantic Models for Request/Response
# ===========================================

class CredentialsModel(BaseModel):
    """Base model with optional Databricks credentials."""
    databricks_host: Optional[str] = Field(
        None, 
        description="Databricks workspace URL (e.g., https://my-workspace.cloud.databricks.com)",
        examples=["https://my-workspace.cloud.databricks.com"]
    )
    databricks_token: Optional[str] = Field(
        None, 
        description="Personal Access Token or OAuth token for authentication"
    )
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "databricks_host": "https://my-workspace.cloud.databricks.com",
                    "databricks_token": "dapi..."
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
        examples=["ontobricks"]
    )
    volume: str = Field(..., description="Volume name for file storage", examples=["projects"])
    
    model_config = {
        "populate_by_name": True,
        "json_schema_extra": {
            "examples": [
                {
                    "catalog": "main",
                    "schema": "ontobricks",
                    "volume": "projects"
                }
            ]
        }
    }


class DomainPathModel(CredentialsModel):
    """Model with Unity Catalog domain JSON file path."""
    model_config = {"populate_by_name": True}

    domain_path: str = Field(
        ...,
        description="Full path to the domain JSON file in Unity Catalog volume",
        validation_alias=AliasChoices("domain_path", "project_path"),
        examples=["/Volumes/main/ontobricks/domains/my_domain.json"],
    )


class QueryModel(DomainPathModel):
    """SPARQL query execution request model."""
    query: str = Field(
        ..., 
        description="SPARQL query to execute",
        examples=["SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10"]
    )
    limit: int = Field(
        100, 
        description="Maximum number of results to return",
        ge=1,
        le=10000
    )
    engine: str = Field(
        "local", 
        description="Query execution engine: 'local' (RDFLib) or 'spark' (Databricks SQL)",
        examples=["local", "spark"]
    )


class ValidateQueryModel(BaseModel):
    """SPARQL query validation request model."""
    query: str = Field(
        ..., 
        description="SPARQL query to validate",
        examples=["SELECT ?s ?p ?o WHERE { ?s ?p ?o }"]
    )


class SuccessResponse(BaseModel):
    """Standard API success response."""
    success: bool = Field(True, description="Indicates if the request was successful")
    data: Any = Field(..., description="Response payload")
    message: Optional[str] = Field(None, description="Optional message")


# ===========================================
# Helper Functions
# ===========================================

def get_credentials(
    request_data: Optional[CredentialsModel],
    x_databricks_host: Optional[str] = None,
    x_databricks_token: Optional[str] = None
) -> tuple[Optional[str], Optional[str]]:
    """Extract Databricks credentials from headers or request body."""
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
        "framework": "FastAPI"
    }


# ===========================================
# Domain Endpoints
# ===========================================

@router.post("/domains/list", response_model=SuccessResponse, summary="List Domains")
async def list_domains(
    data: UCLocationModel,
    x_databricks_host: Optional[str] = Header(None, alias="X-Databricks-Host", description="Databricks workspace URL"),
    x_databricks_token: Optional[str] = Header(None, alias="X-Databricks-Token", description="Authentication token")
):
    """
    List all OntoBricks domain JSON files in a Unity Catalog volume.

    Returns a list of `.json` domain files found in the specified volume location.

    **Authentication** can be provided via:
    - HTTP Headers: `X-Databricks-Host`, `X-Databricks-Token`
    - Request body: `databricks_host`, `databricks_token`

    **Returns:**
    - **domains**: List of domain file entries
    - **count**: Number of domains found
    """
    host, token = get_credentials(data, x_databricks_host, x_databricks_token)

    domains = service.list_domains_from_uc(
        data.catalog, data.schema_name, data.volume, host, token,
    )

    return SuccessResponse(
        data={"domains": domains, "count": len(domains)},
        message=f"Found {len(domains)} domains",
    )


@router.post("/domain/info", response_model=SuccessResponse)
async def get_domain_info(
    data: DomainPathModel,
    x_databricks_host: Optional[str] = Header(None, alias="X-Databricks-Host"),
    x_databricks_token: Optional[str] = Header(None, alias="X-Databricks-Token")
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
    x_databricks_token: Optional[str] = Header(None, alias="X-Databricks-Token")
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
    x_databricks_token: Optional[str] = Header(None, alias="X-Databricks-Token")
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
    x_databricks_token: Optional[str] = Header(None, alias="X-Databricks-Token")
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
    x_databricks_token: Optional[str] = Header(None, alias="X-Databricks-Token")
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
    x_databricks_token: Optional[str] = Header(None, alias="X-Databricks-Token")
):
    """Get the R2RML mapping content from a domain."""
    host, token = get_credentials(data, x_databricks_host, x_databricks_token)

    domain_data = service.load_domain_from_uc(data.domain_path, host, token)
    sl = resolve_domain_slice(domain_data)
    r2rml_content = sl["assignment"].get("r2rml_output", "")

    if not r2rml_content:
        raise NotFoundError("No R2RML mapping found in domain. Generate R2RML first.")

    return SuccessResponse(data={"r2rml": r2rml_content, "format": "turtle"})


# ===========================================
# Query Endpoints
# ===========================================

@router.post("/query", response_model=SuccessResponse, summary="Execute SPARQL Query")
async def execute_query(
    data: QueryModel,
    x_databricks_host: Optional[str] = Header(None, alias="X-Databricks-Host", description="Databricks workspace URL"),
    x_databricks_token: Optional[str] = Header(None, alias="X-Databricks-Token", description="Authentication token")
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
    result = service.execute_sparql_query(domain_data, data.query, data.limit, data.engine)

    return SuccessResponse(data={
        "results": result.get('results', []),
        "columns": result.get('columns', []),
        "count": result.get('count', 0),
        "engine": result.get('engine', data.engine),
    })


@router.post("/query/validate", response_model=SuccessResponse)
async def validate_query(data: ValidateQueryModel):
    """Validate a SPARQL query syntax."""
    is_valid, error_msg = service.validate_sparql_query(data.query)
    
    return SuccessResponse(data={
        "valid": is_valid,
        "error": error_msg if not is_valid else None
    })


@router.post("/query/samples", response_model=SuccessResponse)
async def get_sample_queries(
    data: DomainPathModel,
    x_databricks_host: Optional[str] = Header(None, alias="X-Databricks-Host"),
    x_databricks_token: Optional[str] = Header(None, alias="X-Databricks-Token")
):
    """Get sample SPARQL queries for a domain."""
    host, token = get_credentials(data, x_databricks_host, x_databricks_token)

    domain_data = service.load_domain_from_uc(data.domain_path, host, token)
    samples = service.generate_sample_queries(domain_data)
    return SuccessResponse(data={"queries": samples, "count": len(samples)})
