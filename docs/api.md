# OntoBricks External REST API

The OntoBricks REST API provides stateless endpoints for external applications to query ontologies and retrieve domain metadata.

## Base URL

```
http://localhost:8000/api/v1
```

## Authentication

All endpoints that access Unity Catalog require Databricks authentication. Credentials can be provided via:

### HTTP Headers (Recommended)
```
X-Databricks-Host: https://your-workspace.cloud.databricks.com
X-Databricks-Token: dapi...your-token
```

### Request Body
```json
{
    "databricks_host": "https://your-workspace.cloud.databricks.com",
    "databricks_token": "dapi...your-token"
}
```

## Response Format

All endpoints return JSON responses with a standard format:

### Success Response
```json
{
    "success": true,
    "data": { ... },
    "message": "Optional message"
}
```

### Error Response
```json
{
    "success": false,
    "error": "Error description"
}
```

---

## Endpoints

### Health Check

#### `GET /api/v1/health`

Check if the API is running.

**Response:**
```json
{
    "status": "healthy",
    "version": "<APP_VERSION>",
    "service": "OntoBricks API"
}
```

---

### Domain endpoints

URLs use `/api/v1/domains` and `/api/v1/domain/…` for **domain** operations (saved ontology + mappings).

#### `POST /api/v1/domains/list`

List available domains in a Unity Catalog volume.

**Request:**
```json
{
    "catalog": "my_catalog",
    "schema": "my_schema",
    "volume": "my_volume"
}
```

**Response:**
```json
{
    "success": true,
    "data": {
        "domains": [
            {
                "name": "my_domain.json",
                "path": "/Volumes/my_catalog/my_schema/my_volume/my_domain.json",
                "size": 15234
            }
        ],
        "count": 1
    }
}
```

---

#### `POST /api/v1/domain/info`

Get domain information and statistics.

**Request:**
```json
{
    "project_path": "/Volumes/catalog/schema/volume/domain.json"
}
```

**Response:**
```json
{
    "success": true,
    "data": {
        "name": "My Ontology Domain",
        "description": "Domain description",
        "author": "John Doe",
        "version": "1.0.0",
        "project_version": "1.0",
        "created_at": "2024-01-15T10:30:00",
        "statistics": {
            "entities": 5,
            "relationships": 3,
            "entity_mappings": 4,
            "relationship_mappings": 2
        }
    }
}
```

---

#### `POST /api/v1/domain/ontology`

Get full ontology details including classes and properties.

**Request:**
```json
{
    "project_path": "/Volumes/catalog/schema/volume/domain.json"
}
```

**Response:**
```json
{
    "success": true,
    "data": {
        "base_uri": "http://example.org/ontology/",
        "prefix": "ont",
        "title": "My Ontology",
        "description": "Ontology description",
        "classes": [...],
        "properties": [...],
        "class_count": 5,
        "property_count": 3
    }
}
```

---

#### `POST /api/v1/domain/ontology/classes`

Get list of ontology classes with their URIs.

**Request:**
```json
{
    "project_path": "/Volumes/catalog/schema/volume/domain.json"
}
```

**Response:**
```json
{
    "success": true,
    "data": {
        "classes": [
            {
                "name": "Person",
                "uri": "http://example.org/ontology/Person",
                "attributes": [
                    {"name": "firstName", "type": "string"},
                    {"name": "lastName", "type": "string"}
                ],
                "description": "A person entity"
            }
        ],
        "count": 1
    }
}
```

---

#### `POST /api/v1/domain/ontology/properties`

Get list of ontology properties (relationships) with their URIs.

**Request:**
```json
{
    "project_path": "/Volumes/catalog/schema/volume/domain.json"
}
```

**Response:**
```json
{
    "success": true,
    "data": {
        "properties": [
            {
                "name": "worksFor",
                "uri": "http://example.org/ontology/worksFor",
                "domain": "Person",
                "range": "Company",
                "attributes": [],
                "description": "Employment relationship"
            }
        ],
        "count": 1
    }
}
```

---

#### `POST /api/v1/domain/mappings`

Get mapping details (entity and relationship mappings).

**Request:**
```json
{
    "project_path": "/Volumes/catalog/schema/volume/domain.json"
}
```

**Response:**
```json
{
    "success": true,
    "data": {
        "data_source_mappings": [...],
        "relationship_mappings": [...],
        "has_r2rml": true,
        "entity_mapping_count": 4,
        "relationship_mapping_count": 2
    }
}
```

---

#### `POST /api/v1/domain/r2rml`

Get the R2RML mapping content from a domain.

**Request:**
```json
{
    "project_path": "/Volumes/catalog/schema/volume/domain.json"
}
```

**Response:**
```json
{
    "success": true,
    "data": {
        "r2rml": "@prefix rr: <http://www.w3.org/ns/r2rml#> ...",
        "format": "turtle"
    }
}
```

---

### Query Endpoints

#### `POST /api/v1/query`

Execute a SPARQL query against a domain's ontology.

**Request:**
```json
{
    "project_path": "/Volumes/catalog/schema/volume/domain.json",
    "query": "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10",
    "limit": 100,
    "engine": "local"
}
```

**Parameters:**
- `project_path` (required): Path to the domain JSON file in Unity Catalog
- `query` (required): SPARQL query string
- `limit` (optional): Maximum number of results (default: 100)
- `engine` (optional): Query engine - `local` (RDFLib) or `spark` (default: `local`)

**Response:**
```json
{
    "success": true,
    "data": {
        "results": [
            {"s": "http://example.org/entity1", "p": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "o": "http://example.org/Person"}
        ],
        "columns": ["s", "p", "o"],
        "count": 1,
        "engine": "local"
    }
}
```

---

#### `POST /api/v1/query/validate`

Validate SPARQL query syntax.

**Request:**
```json
{
    "query": "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10"
}
```

**Response:**
```json
{
    "success": true,
    "data": {
        "valid": true,
        "error": null
    }
}
```

---

#### `POST /api/v1/query/samples`

Get sample SPARQL queries generated for a domain.

**Request:**
```json
{
    "project_path": "/Volumes/catalog/schema/volume/domain.json"
}
```

**Response:**
```json
{
    "success": true,
    "data": {
        "queries": [
            {
                "name": "List all entity types",
                "description": "Returns all distinct entity types (classes) in the ontology",
                "query": "PREFIX ont: <http://example.org/> ..."
            }
        ],
        "count": 4
    }
}
```

---

## GraphQL API

OntoBricks auto-generates a typed GraphQL schema from each domain's ontology. Ontology classes become GraphQL types, data properties become scalar fields, and object properties become typed relationship fields with nested traversal.

**URL choice:** The same router is mounted twice on the main app: **in-app / browser** paths below use ``/graphql/...``. For the **mounted external API** (OpenAPI at ``/api/docs``), use ``/api/v1/graphql/...`` instead — same handlers and payloads, different prefix (see ``api.constants.EXTERNAL_GRAPHQL_PUBLIC_PREFIX``).

### Base URL (main UI server)

```
http://localhost:8000/graphql
```

For programmatic access via the external sub-application:

```
http://localhost:8000/api/v1/graphql
```

---

### List GraphQL-enabled domains

#### `GET /graphql`

Returns all domains in the configured registry that have a materialized triple store.

**Response:**
```json
{
    "success": true,
    "domains": [
        {
            "name": "my_domain",
            "description": ""
        }
    ],
    "message": null
}
```

---

### GraphiQL Playground

#### `GET /graphql/{project_name}`

Opens the interactive GraphiQL IDE for a specific domain. The playground provides auto-complete, documentation explorer, and query history.

**Parameters:**
- `project_name` (path, required): Name of the domain in the registry

---

### Execute GraphQL Query

#### `POST /graphql/{project_name}`

Execute a GraphQL query against the domain's auto-generated schema.

**Request:**
```json
{
    "query": "{ allCustomer(limit: 5) { id label hasInteraction { label } } }",
    "variables": {},
    "operationName": null,
    "depth": 2
}
```

**Parameters:**
- `project_name` (path, required): Name of the domain in the registry
- `query` (body, required): GraphQL query string
- `variables` (body, optional): Query variables
- `operationName` (body, optional): Operation name for multi-operation documents

**Response:**
```json
{
    "data": {
        "allCustomer": [
            {
                "id": "Customer/C001",
                "label": "Alice Smith",
                "hasInteraction": [
                    { "label": "Call 2024-01-15" }
                ]
            }
        ]
    }
}
```

---

### Schema Introspection (SDL)

#### `GET /graphql/{project_name}/schema`

Returns the full GraphQL Schema Definition Language (SDL) for the domain.

**Parameters:**
- `project_name` (path, required): Name of the domain in the registry

**Response:**
```graphql
type Customer {
  id: String!
  label: String
  hasInteraction: [Interaction]
}

type Interaction {
  id: String!
  label: String
  date: String
}

type Query {
  allCustomer(limit: Int = 50, offset: Int = 0, search: String): [Customer!]!
  customer(id: String!): Customer
  allInteraction(limit: Int = 50, offset: Int = 0, search: String): [Interaction!]!
  interaction(id: String!): Interaction
}
```

---

### Notes on GraphQL API

1. **Schema is auto-generated**: The schema is built dynamically from the ontology. Each ontology class becomes a GraphQL type; data properties become `String` fields; object properties become typed relationship fields.
2. **Per-domain schemas**: Different domains may have completely different schemas, reflecting their ontology.
3. **Caching**: Schemas are cached per domain and invalidated on ontology changes.
4. **Relationship depth**: Nested relationships are resolved to a configurable depth (default 2, max 5). The depth can be set via the `depth` field in the request body or the depth selector in the GraphiQL playground.
5. **Triple store required**: The domain must have a materialized triple store (synced via Digital Twin) for GraphQL queries to return data.

---

## Digital Twin API

The Digital Twin API provides stateless, programmatic access to the knowledge graph — triple store status, entity search, ontology artifacts, and build triggers. All endpoints accept an optional `project_name` query parameter to load a domain from the registry instead of the browser session.

### Base URL

```
http://localhost:8000/api/v1/digitaltwin
```

---

### Registry

#### `GET /api/v1/digitaltwin/registry`

Returns the domain registry location (catalog, schema, volume).

---

### List domains

#### `GET /api/v1/domains`

List all MCP-enabled domains in the registry.

---

### Versions

#### `GET /api/v1/domain/versions`

Returns all versions for a domain in the registry, latest first.

**Parameters:**
- `project_name` (query, required): Domain name in the registry

---

### Design Status

#### `GET /api/v1/domain/design-status`

Returns a comprehensive readiness status including ontology, metadata, and mapping completeness.

**Parameters:**
- `project_name` (query, optional): Domain name in the registry
- `project_version` (query, optional): Specific version to load (latest if omitted)

**Response:**
```json
{
    "success": true,
    "ontology": {
        "ready": true,
        "class_count": 10,
        "property_count": 9,
        "base_uri": "https://ontobricks.com/ontology#"
    },
    "metadata": {
        "ready": true,
        "table_count": 5
    },
    "assignment": {
        "ready": true,
        "entity_total": 10,
        "entity_mapped": 10,
        "relationship_total": 9,
        "relationship_mapped": 9,
        "completion_pct": 100
    },
    "build_ready": true
}
```

---

### Triple Store Status

#### `GET /api/v1/digitaltwin/status`

Check backend type, table name, data availability, and triple count.

**Parameters:**
- `project_name` (query, optional): Domain name in the registry

---

### Ontology (OWL)

#### `GET /api/v1/domain/ontology`

Return the domain's OWL ontology in Turtle format.

**Parameters:**
- `project_name` (query, optional): Domain name in the registry

---

### R2RML Mapping

#### `GET /api/v1/domain/r2rml`

Return the domain's R2RML mapping document in Turtle format.

**Parameters:**
- `project_name` (query, optional): Domain name in the registry

---

### Generated Spark SQL

#### `GET /api/v1/domain/sparksql`

Return the Spark SQL that produces triples from the source tables.

**Parameters:**
- `project_name` (query, optional): Domain name in the registry

---

### Statistics

#### `GET /api/v1/digitaltwin/stats`

Aggregated statistics: total triples, entity types, predicates, labels.

**Parameters:**
- `project_name` (query, optional): Domain name in the registry

---

### Build (Sync)

#### `POST /api/v1/digitaltwin/build`

Trigger a triple store build (sync). Returns a task_id for progress polling.

---

### Entity Search (BFS Traversal)

#### `GET /api/v1/digitaltwin/triples/find`

BFS-based entity search with depth control.

**Parameters:**
- `project_name` (query, optional): Domain name in the registry
- `search` (query): Search text
- `entity_type` (query, optional): Filter by type
- `depth` (query, optional): BFS depth (default: 2)

---

## Example Usage

### Python

```python
import requests

# Configuration
API_BASE = "http://localhost:8000/api/v1"
HEADERS = {
    "Content-Type": "application/json",
    "X-Databricks-Host": "https://your-workspace.cloud.databricks.com",
    "X-Databricks-Token": "dapi..."
}

# List domains
response = requests.post(
    f"{API_BASE}/domains/list",
    headers=HEADERS,
    json={
        "catalog": "main",
        "schema": "default",
        "volume": "ontologies"
    }
)
payload = response.json()

# Execute SPARQL query
response = requests.post(
    f"{API_BASE}/query",
    headers=HEADERS,
    json={
        "project_path": "/Volumes/main/default/ontologies/my_domain.json",
        "query": "SELECT ?type (COUNT(?s) as ?count) WHERE { ?s a ?type } GROUP BY ?type",
        "limit": 50
    }
)
results = response.json()
print(results['data']['results'])
```

### cURL

```bash
# Health check
curl http://localhost:8000/api/v1/health

# List domains
curl -X POST http://localhost:8000/api/v1/domains/list \
  -H "Content-Type: application/json" \
  -H "X-Databricks-Host: https://your-workspace.cloud.databricks.com" \
  -H "X-Databricks-Token: dapi..." \
  -d '{"catalog": "main", "schema": "default", "volume": "ontologies"}'

# Execute query
curl -X POST http://localhost:8000/api/v1/query \
  -H "Content-Type: application/json" \
  -H "X-Databricks-Host: https://your-workspace.cloud.databricks.com" \
  -H "X-Databricks-Token: dapi..." \
  -d '{
    "project_path": "/Volumes/main/default/ontologies/my_domain.json",
    "query": "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10"
  }'
```

---

## Error Codes

| HTTP Code | Description |
|-----------|-------------|
| 200 | Success |
| 400 | Bad Request - Missing or invalid parameters |
| 401 | Unauthorized - Invalid or missing credentials |
| 404 | Not Found - Domain or resource not found |
| 500 | Internal Server Error |

---

## Notes

1. **Stateless**: The API is stateless - each request loads the domain fresh from Unity Catalog.
2. **Engine**: Currently, only the `local` engine (RDFLib) is fully supported. The `spark` engine requires additional setup.
3. **R2RML Required**: SPARQL queries require the domain to have an R2RML mapping generated. Use the web interface to generate mappings first.
4. **Security**: Never share your Databricks token. Consider using environment variables or secure credential management.



---

## Internal REST API reference (merged)

## OntoBricks API Reference

This document describes the REST API endpoints available in OntoBricks.

### Base URL

- **Local Development**: `http://localhost:8000`
- **Databricks Apps**: `https://<workspace>.databricks.com/apps/<app-id>/`

---

### API Overview by Module

| Module | Base Path | Purpose |
|--------|-----------|---------|
| Domain API | `/api/v1/domains`, `/api/v1/domain` | Registry list, versions, design status, OWL/R2RML/SQL artifacts |
| Digital Twin API | `/api/v1/digitaltwin` | Stateless access to triple store, builds, triple search, quality, reasoning |
| Core/Navbar | `/` | Session status, file browsing |
| Settings | `/settings` | Databricks connection, settings |
| Scheduled Builds | `/settings/schedules` | Automated triple store build scheduling |
| Ontology | `/ontology` | Ontology design, OWL operations |
| SWRL Rules | `/ontology/swrl` | SWRL rule management |
| Constraints | `/ontology/constraints` | Property constraints |
| Axioms | `/ontology/axioms` | OWL expressions & axioms |
| SHACL Data Quality | `/ontology/dataquality` | SHACL shape CRUD, Turtle import/export |
| Mapping | `/mapping` | Entity/relationship mapping, R2RML |
| SQL Wizard | `/mapping/wizard` | LLM-assisted SQL generation for mappings |
| Digital Twin | `/dtwin` | Sync, knowledge graph, quality checks, internal query execution |
| Data Quality Execution | `/dtwin/dataquality` | Run SHACL checks against triple store |
| Reasoning | `/dtwin/reasoning` | OWL 2 RL + SWRL inference, inferred triples |
| GraphQL | `/graphql` (UI); `/api/v1/graphql` (external API mount) | Auto-generated typed GraphQL schema from ontology |
| Domain | `/domain` | Domain save/load operations (UI route) |

---

### Core Endpoints

These endpoints provide shared functionality used across the application.

#### Get Session Status

Get current session statistics for the home page dashboard.

```http
GET /session-status
```

**Response:**
```json
{
  "has_config": true,
  "has_taxonomy": true,
  "taxonomy_name": "MyOrganization",
  "class_count": 3,
  "has_mappings": true,
  "entity_mappings": 3,
  "relationship_mappings": 2,
  "has_r2rml": true,
  "has_rdf": false,
  "triple_count": 0
}
```

#### Get Ontology Status

Get the current ontology load status (for navbar indicators).

```http
GET /ontology-status
```

**Response:**
```json
{
  "loaded": true,
  "has_r2rml": true,
  "has_taxonomy": true,
  "name": "MyOrganization",
  "taxonomy_classes": 3,
  "taxonomy_properties": 5
}
```

#### Reset Session

Clear all session data (ontology, mappings, R2RML).

```http
POST /reset-session
```

**Response:**
```json
{
  "success": true,
  "message": "Session reset successfully"
}
```

#### Browse Volume Files

List files in a Unity Catalog volume.

```http
POST /browse-volume
```

**Request Body:**
```json
{
  "catalog": "main",
  "schema": "default",
  "volume": "ontologies",
  "path": ""
}
```

**Response:**
```json
{
  "success": true,
  "files": [
    {
      "name": "taxonomy.ttl",
      "path": "/Volumes/main/default/ontologies/taxonomy.ttl",
      "size": 2048,
      "is_directory": false
    }
  ],
  "path": "/Volumes/main/default/ontologies"
}
```

#### Read Volume File

Read a file from a Unity Catalog volume.

```http
POST /read-volume-file
```

**Request Body:**
```json
{
  "file_path": "/Volumes/main/default/ontologies/taxonomy.ttl"
}
```

**Response:**
```json
{
  "success": true,
  "content": "@prefix owl: <http://www.w3.org/2002/07/owl#> ...",
  "filename": "taxonomy.ttl",
  "path": "/Volumes/main/default/ontologies/taxonomy.ttl"
}
```

---

### Configuration Endpoints

#### Get Current Configuration

```http
GET /settings/current
```

**Response:**
```json
{
  "host": "https://your-workspace.databricks.com",
  "token": "********",
  "warehouse_id": "abc123",
  "catalog": "main",
  "schema": "default",
  "volume_path": "/Volumes/system/ontobricks/mappings",
  "from_env": true,
  "has_config": true
}
```

#### Test Connection

```http
POST /settings/test-connection
```

**Request Body:**
```json
{
  "host": "https://your-workspace.databricks.com",
  "token": "dapi...",
  "warehouse_id": "abc123"
}
```

**Response:**
```json
{
  "success": true,
  "message": "Connection successful"
}
```

#### Get Warehouses

```http
GET /settings/warehouses
```

**Response:**
```json
{
  "warehouses": [
    {"id": "abc123", "name": "Starter Warehouse"},
    {"id": "def456", "name": "Production Warehouse"}
  ]
}
```

#### Get Catalogs

```http
GET /settings/catalogs
```

**Response:**
```json
{
  "catalogs": ["main", "samples", "system"]
}
```

#### Get Schemas

```http
GET /settings/schemas/<catalog>
```

**Response:**
```json
{
  "schemas": ["default", "information_schema"]
}
```

#### Get Volumes

```http
GET /settings/volumes/<catalog>/<schema>
```

**Response:**
```json
{
  "volumes": ["data", "ontologies", "mappings"]
}
```

#### Save Configuration

```http
POST /settings/save
```

**Request Body:**
```json
{
  "warehouse_id": "abc123",
  "catalog": "main",
  "schema": "default"
}
```

#### Get/Set Default Emoji

```http
GET /settings/get-default-emoji
POST /settings/set-default-emoji
```

#### Get/Save Base URI

```http
GET /settings/get-base-uri
POST /settings/save-base-uri
```

---

### Ontology Endpoints

#### Get Ontology Page

```http
GET /ontology/
```

Returns the ontology designer HTML page.

#### Save Ontology to Session

```http
POST /ontology/save
```

**Request Body:**
```json
{
  "name": "MyOrganization",
  "base_uri": "https://databricks-ontology.com/MyOrganization#",
  "classes": [
    {
      "name": "Person",
      "label": "Person",
      "emoji": "👤",
      "description": "Represents a person",
      "dataProperties": [
        {"name": "email", "type": "string"}
      ]
    }
  ],
  "properties": [
    {
      "name": "name",
      "type": "DatatypeProperty",
      "domain": "Person",
      "range": "xsd:string"
    },
    {
      "name": "worksIn",
      "type": "ObjectProperty",
      "domain": "Person",
      "range": "Department",
      "direction": "forward",
      "properties": [
        {"id": "attr1", "name": "startDate", "type": "date"}
      ]
    }
  ]
}
```

**Response:**
```json
{
  "success": true,
  "message": "Ontology configuration saved to session"
}
```

#### Load Ontology from Session

```http
GET /ontology/load
```

**Response:**
```json
{
  "success": true,
  "config": {
    "name": "MyOrganization",
    "base_uri": "https://databricks-ontology.com/MyOrganization#",
    "classes": [...],
    "properties": [...]
  }
}
```

#### Generate OWL

```http
POST /ontology/generate-owl
```

**Request Body:**
```json
{
  "name": "MyOrganization",
  "base_uri": "https://databricks-ontology.com/MyOrganization#",
  "classes": [...],
  "properties": [...]
}
```

**Response:**
```json
{
  "success": true,
  "owl": "@prefix owl: <http://www.w3.org/2002/07/owl#> ..."
}
```

#### Parse OWL

```http
POST /ontology/parse-owl
```

**Request Body:**
```json
{
  "content": "@prefix owl: <http://www.w3.org/2002/07/owl#> ..."
}
```

**Response:**
```json
{
  "success": true,
  "message": "Parsed successfully: 3 classes, 5 properties",
  "ontology": {
    "info": {...},
    "classes": [...],
    "properties": [...]
  },
  "stats": {
    "classes": 3,
    "properties": 5
  }
}
```

#### Reset Ontology

```http
POST /ontology/reset
```

**Response:**
```json
{
  "success": true,
  "message": "Ontology reset successfully"
}
```

#### Get Loaded Ontology

```http
GET /ontology/get-loaded-ontology
```

#### Save to Unity Catalog

```http
POST /ontology/save-to-uc
```

**Request Body:**
```json
{
  "content": "@prefix owl: ...",
  "path": "/Volumes/main/default/ontologies/taxonomy.ttl"
}
```

---

### SWRL Rules Endpoints

SWRL (Semantic Web Rule Language) rules for automatic inference.

#### List SWRL Rules

```http
GET /ontology/swrl/list
```

**Response:**
```json
{
  "success": true,
  "rules": [
    {
      "name": "InferGrandparent",
      "description": "Infers grandparent relationship",
      "antecedent": "Person(?x) ∧ hasParent(?x, ?y) ∧ hasParent(?y, ?z)",
      "consequent": "hasGrandparent(?x, ?z)"
    }
  ]
}
```

#### Save SWRL Rule

```http
POST /ontology/swrl/save
```

**Request Body:**
```json
{
  "rule": {
    "name": "InferGrandparent",
    "description": "Infers grandparent relationship",
    "antecedent": "Person(?x) ∧ hasParent(?x, ?y) ∧ hasParent(?y, ?z)",
    "consequent": "hasGrandparent(?x, ?z)"
  },
  "index": -1
}
```

**Note:** Set `index` to -1 for new rules, or the rule index to update existing.

#### Delete SWRL Rule

```http
POST /ontology/swrl/delete
```

**Request Body:**
```json
{
  "index": 0
}
```

#### Validate SWRL Rule

```http
POST /ontology/swrl/validate
```

**Request Body:**
```json
{
  "rule": {
    "antecedent": "Person(?x) ∧ hasParent(?x, ?y)",
    "consequent": "hasGrandparent(?x, ?z)"
  }
}
```

**Response:**
```json
{
  "success": false,
  "valid": false,
  "errors": ["Undefined variables in consequent: ?z"]
}
```

---

### Property Constraints Endpoints

Manage cardinality constraints, value restrictions, and property characteristics.

#### List Constraints

```http
GET /ontology/constraints/list
```

**Response:**
```json
{
  "success": true,
  "constraints": [
    {
      "type": "exactCardinality",
      "className": "Employee",
      "property": "hasManager",
      "cardinalityValue": 1
    },
    {
      "type": "functional",
      "property": "hasBirthDate"
    }
  ]
}
```

#### Save Constraint

```http
POST /ontology/constraints/save
```

**Request Body:**
```json
{
  "constraint": {
    "type": "maxCardinality",
    "className": "Person",
    "property": "hasPhone",
    "cardinalityValue": 3
  },
  "index": -1
}
```

**Constraint Types:**

| Category | Types |
|----------|-------|
| Cardinality | `minCardinality`, `maxCardinality`, `exactCardinality` |
| Value Restrictions | `allValuesFrom`, `someValuesFrom`, `hasValue` |
| Property Characteristics | `functional`, `inverseFunctional`, `transitive`, `symmetric`, `asymmetric`, `reflexive`, `irreflexive` |

#### Delete Constraint

```http
POST /ontology/constraints/delete
```

**Request Body:**
```json
{
  "index": 0
}
```

#### Get Constraints by Property

```http
GET /ontology/constraints/get-by-property/<property_uri>
```

#### Get Constraints by Class

```http
GET /ontology/constraints/get-by-class/<class_uri>
```

---

### SHACL Data Quality Endpoints

Manage SHACL shapes for data quality validation. Shapes define constraints (cardinality, datatype, pattern, custom SPARQL) that are checked against the triple store.

#### List Shapes

```http
GET /ontology/dataquality/list
```

**Query Parameters:** `category` (optional) — filter by category (completeness, conformance, cardinality, structural, uniqueness)

**Response:**
```json
{
  "success": true,
  "shapes": [
    {
      "id": "shape_1",
      "name": "Customer.email must exist",
      "target_class": "Customer",
      "property": "email",
      "constraint_type": "sh:minCount",
      "constraint_value": "1",
      "category": "completeness",
      "severity": "Violation"
    }
  ]
}
```

#### Save Shape

```http
POST /ontology/dataquality/save
```

**Request Body:**
```json
{
  "shape": {
    "id": "shape_1",
    "name": "Customer.email must exist",
    "target_class": "Customer",
    "property": "email",
    "constraint_type": "sh:minCount",
    "constraint_value": "1",
    "category": "completeness",
    "severity": "Violation"
  }
}
```

#### Delete Shape

```http
POST /ontology/dataquality/delete
```

**Request Body:**
```json
{
  "id": "shape_1"
}
```

#### Export Shapes as Turtle

```http
GET /ontology/dataquality/export
```

Returns all SHACL shapes as a Turtle (.ttl) file download.

#### Import Shapes from Turtle

```http
POST /ontology/dataquality/import
```

**Request Body:**
```json
{
  "content": "@prefix sh: <http://www.w3.org/ns/shacl#> ..."
}
```

#### Migrate Legacy Constraints to SHACL

```http
POST /ontology/dataquality/migrate
```

Converts legacy ontology constraints to SHACL shapes.

---

### OWL Axioms Endpoints

Manage OWL class expressions and axioms.

#### List Axioms

```http
GET /ontology/axioms/list
```

**Response:**
```json
{
  "success": true,
  "axioms": [
    {
      "type": "equivalentClass",
      "subject": "Employee",
      "objects": ["Person"],
      "description": "Employee is equivalent to Person with a job"
    },
    {
      "type": "disjointWith",
      "subject": "Person",
      "objects": ["Organization"]
    },
    {
      "type": "propertyChain",
      "subject": "hasGrandparent",
      "chain": ["hasParent", "hasParent"]
    }
  ]
}
```

#### Save Axiom

```http
POST /ontology/axioms/save
```

**Request Body (Equivalent Class):**
```json
{
  "axiom": {
    "type": "equivalentClass",
    "subject": "Employee",
    "objects": ["Person"],
    "description": "Employee equals Person with job"
  },
  "index": -1
}
```

**Request Body (Property Chain):**
```json
{
  "axiom": {
    "type": "propertyChain",
    "subject": "hasGrandparent",
    "chain": ["hasParent", "hasParent"]
  },
  "index": -1
}
```

**Request Body (OneOf Enumeration):**
```json
{
  "axiom": {
    "type": "oneOf",
    "subject": "TrafficLight",
    "individuals": "Red, Yellow, Green"
  },
  "index": -1
}
```

**Axiom Types:**

| Category | Types |
|----------|-------|
| Class Relationships | `equivalentClass`, `disjointWith`, `disjointUnion` |
| Class Expressions | `unionOf`, `intersectionOf`, `complementOf`, `oneOf` |
| Property Relationships | `equivalentProperty`, `inverseOf`, `propertyChain`, `disjointProperties` |

#### Delete Axiom

```http
POST /ontology/axioms/delete
```

**Request Body:**
```json
{
  "index": 0
}
```

#### Get Axioms by Class

```http
GET /ontology/axioms/get-by-class/<class_uri>
```

#### Get Axioms by Type

```http
GET /ontology/axioms/get-by-type/<axiom_type>
```

---

### Mapping Endpoints

#### Get Mapping Page

```http
GET /mapping/
```

Returns the mapping configuration HTML page.

#### Get Tables

```http
POST /mapping/tables
```

**Request Body:**
```json
{
  "catalog": "main",
  "schema": "default"
}
```

**Response:**
```json
{
  "tables": ["person", "department", "project"]
}
```

#### Get Table Columns

```http
POST /mapping/table-columns
```

**Request Body:**
```json
{
  "catalog": "main",
  "schema": "default",
  "table": "person"
}
```

**Response:**
```json
{
  "columns": [
    {"name": "person_id", "type": "STRING"},
    {"name": "name", "type": "STRING"},
    {"name": "email", "type": "STRING"}
  ]
}
```

#### Test SQL Query

```http
POST /mapping/test-query
```

**Request Body:**
```json
{
  "query": "SELECT person_id, dept_id FROM person_department"
}
```

**Response:**
```json
{
  "success": true,
  "columns": ["person_id", "dept_id"],
  "rows": [
    {"person_id": "P001", "dept_id": "D001"}
  ],
  "row_count": 1
}
```

#### Save Mapping

```http
POST /mapping/save
```

**Request Body:**
```json
{
  "data_source_mappings": [
    {
      "ontology_class": "https://example.org/ontology#Person",
      "ontology_class_label": "Person",
      "sql_query": "SELECT person_id, name, email FROM main.default.person",
      "id_column": "person_id",
      "label_column": "name",
      "attribute_mappings": {
        "email": "email"
      }
    }
  ],
  "relationship_mappings": [
    {
      "property": "https://example.org/ontology#worksIn",
      "property_label": "worksIn",
      "source_entity": "Person",
      "target_entity": "Department",
      "sql_query": "SELECT person_id, dept_id FROM person_department",
      "source_column": "person_id",
      "target_column": "dept_id",
      "direction": "forward",
      "attribute_mappings": {
        "startDate": "start_date"
      }
    }
  ]
}
```

#### Load Mapping

```http
GET /mapping/load
```

#### Generate R2RML

```http
POST /mapping/generate
```

**Response:**
```json
{
  "success": true,
  "r2rml": "@prefix rr: <http://www.w3.org/ns/r2rml#> ...",
  "stats": {
    "entity_mappings": 3,
    "relationship_mappings": 2
  }
}
```

#### Parse R2RML

```http
POST /mapping/parse-r2rml
```

**Request Body:**
```json
{
  "content": "@prefix rr: <http://www.w3.org/ns/r2rml#> ..."
}
```

**Response:**
```json
{
  "success": true,
  "message": "R2RML parsed successfully",
  "entity_mappings": [...],
  "relationship_mappings": [...],
  "stats": {
    "entity_count": 3,
    "relationship_count": 2
  }
}
```

#### Reset Mappings

```http
POST /mapping/reset
```

#### Download R2RML

```http
GET /mapping/download
```

Returns `mapping.ttl` file download.

#### Save to Unity Catalog

```http
POST /mapping/save-to-uc
```

---

### Digital Twin Endpoints

#### Get Digital Twin Page

```http
GET /dtwin
```

Returns the Digital Twin HTML page (sync, quality, triples, knowledge graph).

#### Execute Query (Internal)

Used internally by the sync and knowledge graph features to generate and execute SQL from the ontology mappings.

```http
POST /dtwin/execute
```

**Request Body:**
```json
{
  "query": "PREFIX ont: <https://example.org/ontology#>\nSELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 100",
  "engine": "sansa",
  "limit": 100
}
```

**Response:**
```json
{
  "success": true,
  "columns": ["subject", "predicate", "object"],
  "results": [
    {
      "subject": "https://example.org/Person/P001",
      "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
      "object": "https://example.org/ontology#Person"
    },
    {
      "subject": "https://example.org/Person/P001",
      "predicate": "http://www.w3.org/2000/01/rdf-schema#label",
      "object": "John Doe"
    },
    {
      "subject": "https://example.org/Person/P001",
      "predicate": "https://example.org/ontology#worksIn",
      "object": "https://example.org/Department/D001"
    }
  ],
  "count": 3,
  "engine": "spark",
  "generated_sql": "SELECT DISTINCT subject, predicate, object FROM (...) LIMIT 100",
  "tables_queried": ["person", "department"]
}
```

**Engine Options:**
- `sansa` - Execute via Spark SQL on Databricks (translates SPARQL to SQL)
- `local` - Execute locally using RDFLib (for small datasets or testing)

#### Get Triple Store Status

```http
GET /dtwin/sync/status
```

**Response:**
```json
{
  "success": true,
  "has_data": true,
  "count": 1500,
  "last_modified": "2026-02-15 14:32:10"
}
```

The `last_modified` field is retrieved from the Unity Catalog Delta table metadata (`DESCRIBE DETAIL`) and indicates the last time the triple store table was updated.

#### Auto-Map Entity Icons (LLM)

Use the domain's configured LLM serving endpoint to suggest emoji icons for entity names.

```http
POST /dtwin/auto-assign-icons
```

**Request Body:**
```json
{
  "entity_names": ["Customer", "Order", "Product", "Invoice"]
}
```

**Response:**
```json
{
  "success": true,
  "icons": {
    "Customer": "🧑",
    "Order": "📋",
    "Product": "📦",
    "Invoice": "🧾"
  }
}
```

> **Note**: Requires a valid LLM serving endpoint configured in Domain Settings (`llm_endpoint`).

---

### Data Quality Execution Endpoints

Execute SHACL data quality checks against the triple store (Delta or LadybugDB).

#### Execute Quality Checks (Synchronous)

```http
POST /dtwin/dataquality/execute
```

**Request Body:**
```json
{
  "backend": "delta",
  "table_name": "catalog.schema.triples"
}
```

**Response:**
```json
{
  "success": true,
  "results": [
    {
      "shape_name": "Customer.email must exist",
      "category": "completeness",
      "severity": "Violation",
      "total_entities": 150,
      "violations": 3,
      "pass_rate": 98.0,
      "details": "3 violations found — 98.0% pass on 150 entities"
    }
  ],
  "summary": {
    "total_checks": 12,
    "passed": 10,
    "failed": 2
  }
}
```

**Backend Options:**
- `delta` — Execute checks as SQL against the Delta triple store table
- `graph` — Execute checks in-memory against the LadybugDB graph

#### Start Quality Checks (Async)

```http
POST /dtwin/dataquality/start
```

Returns a `task_id` for progress polling via `GET /tasks/{task_id}/status`.

---

### Reasoning Endpoints

Run OWL 2 RL inference and SWRL rule execution against the triple store.

#### Start Reasoning (Async)

```http
POST /dtwin/reasoning/start
```

**Request Body:**
```json
{
  "phases": ["tbox", "swrl", "structural"],
  "materialize": true,
  "target_table": "catalog.schema.triples_inferred"
}
```

Returns a `task_id`. Reasoning runs OWL 2 RL T-Box closure, SWRL rules, and optional structural reasoning (transitivity, symmetry).

#### Get Inferred Triples

```http
GET /dtwin/reasoning/inferred
```

**Response:**
```json
{
  "success": true,
  "inferred_count": 42,
  "triples": [
    {
      "subject": "https://example.org/Person/P001",
      "predicate": "https://example.org/ontology#hasGrandparent",
      "object": "https://example.org/Person/P003",
      "rule": "InferGrandparent",
      "phase": "swrl"
    }
  ]
}
```

---

### GraphQL Endpoints

OntoBricks auto-generates a typed GraphQL schema from the ontology. Each class becomes a GraphQL type, data properties become scalar fields, and object properties become typed relationship fields.

#### List GraphQL-enabled domains

```http
GET /graphql
```

Returns all domains in the configured registry that have a materialized triple store and can be queried via GraphQL.

**Response:**
```json
{
  "success": true,
  "domains": [
    {
      "name": "my_domain",
      "description": ""
    }
  ],
  "message": null
}
```

#### GraphiQL Playground

```http
GET /graphql/{project_name}
```

Opens the interactive GraphiQL IDE for the domain. Provides auto-complete, documentation explorer, and query history.

#### Get GraphQL Depth Settings

```http
GET /graphql/settings/depth
```

**Response:**
```json
{
  "default": 2,
  "max": 5
}
```

#### Execute GraphQL Query

```http
POST /graphql/{project_name}
```

**Request Body:**
```json
{
  "query": "{ allCustomer(limit: 5) { id label hasInteraction { label } } }",
  "variables": {},
  "operationName": null,
  "depth": 2
}
```

**Response:**
```json
{
  "data": {
    "allCustomer": [
      {
        "id": "Customer/C001",
        "label": "Alice Smith",
        "hasInteraction": [
          { "label": "Call 2024-01-15" }
        ]
      }
    ]
  }
}
```

#### Schema Introspection (SDL)

```http
GET /graphql/{project_name}/schema
```

Returns the full GraphQL Schema Definition Language (SDL) for the domain.

**Response (text/plain):**
```graphql
type Customer {
  id: String!
  label: String
  hasInteraction: [Interaction]
}

type Query {
  allCustomer(limit: Int = 50, offset: Int = 0, search: String): [Customer!]!
  customer(id: String!): Customer
}
```

> **Note**: The GraphQL schema is auto-generated at runtime from the domain's ontology. Each domain has its own schema, cached and invalidated on ontology changes.

---

### External REST API (`/api/v1`)

**Domain** routes (`/api/v1/domains`, `/api/v1/domain/...`) and **Digital Twin** routes (`/api/v1/digitaltwin/...`). Most accept an optional `project_name` (and often `project_version`) to load a domain from the registry instead of the browser session.

**Digital Twin base URL:** `http://localhost:8000/api/v1/digitaltwin`

#### Registry

```http
GET /api/v1/digitaltwin/registry
```

Returns the domain registry location (catalog, schema, volume).

#### List domains

```http
GET /api/v1/domains
```

List all MCP-enabled domains in the registry.

#### Versions

```http
GET /api/v1/domain/versions
```

**Query Parameters:** `project_name` (required)

Returns all versions for the domain, latest first.

#### Design Status

```http
GET /api/v1/domain/design-status
```

**Query Parameters:** `project_name` (optional), `project_version` (optional)

Returns a comprehensive readiness status including ontology, metadata, and mapping completeness.

**Response:**
```json
{
  "success": true,
  "ontology": {
    "ready": true,
    "class_count": 10,
    "property_count": 9,
    "base_uri": "https://ontobricks.com/ontology#"
  },
  "metadata": {
    "ready": true,
    "table_count": 5
  },
  "assignment": {
    "ready": true,
    "entity_total": 10,
    "entity_mapped": 10,
    "relationship_total": 9,
    "relationship_mapped": 9,
    "completion_pct": 100
  },
  "build_ready": true
}
```

#### Triple Store Status

```http
GET /api/v1/digitaltwin/status
```

**Query Parameters:** `project_name` (optional)

Check backend type, table name, data availability, and triple count.

#### Ontology (OWL)

```http
GET /api/v1/domain/ontology
```

**Query Parameters:** `project_name` (optional)

Return the domain's OWL ontology in Turtle format.

#### R2RML Mapping

```http
GET /api/v1/domain/r2rml
```

**Query Parameters:** `project_name` (optional)

Return the domain's R2RML mapping document in Turtle format.

#### Generated Spark SQL

```http
GET /api/v1/domain/sparksql
```

**Query Parameters:** `project_name` (optional)

Return the Spark SQL that produces triples from the source tables.

#### Statistics

```http
GET /api/v1/digitaltwin/stats
```

**Query Parameters:** `project_name` (optional)

Aggregated statistics: total triples, entity types, predicates, labels.

#### Build (Sync)

```http
POST /api/v1/digitaltwin/build
```

Trigger a triple store build (sync). Returns a task_id for progress polling.

#### Entity Search (BFS Traversal)

```http
GET /api/v1/digitaltwin/triples/find
```

**Query Parameters:**
- `project_name` (optional): Domain name in the registry
- `search` (required): Search text
- `entity_type` (optional): Filter by type
- `depth` (optional): BFS depth (default: 2)

BFS-based entity search with depth control.

---

### Scheduled Builds Endpoints

Manage scheduled triple store builds (requires APScheduler).

#### List Schedules

```http
GET /settings/schedules
```

#### Create/Update Schedule

```http
POST /settings/schedules
```

**Request Body:**
```json
{
  "project_name": "my_domain",
  "cron": "0 2 * * *",
  "enabled": true
}
```

#### Delete Schedule

```http
DELETE /settings/schedules/{schedule_id}
```

---

### Mapping SQL Wizard Endpoints

LLM-assisted SQL generation for mapping queries.

#### Get Schema Context

```http
GET /mapping/wizard/schema-context
```

Returns table/column metadata for LLM context.

#### Generate SQL

```http
POST /mapping/wizard/generate-sql
```

**Request Body:**
```json
{
  "entity_name": "Customer",
  "attributes": ["email", "name", "phone"],
  "schema_context": {...}
}
```

**Response:**
```json
{
  "success": true,
  "sql": "SELECT customer_id, email, name, phone FROM main.default.customers",
  "explanation": "Maps Customer entity to the customers table..."
}
```

#### Validate SQL

```http
POST /mapping/wizard/validate-sql
```

**Request Body:**
```json
{
  "sql": "SELECT customer_id FROM main.default.customers"
}
```

---

### Data Structures

#### Entity (Class) Object

```json
{
  "name": "Person",
  "localName": "Person",
  "label": "Person",
  "emoji": "👤",
  "description": "Represents a person in the organization",
  "dataProperties": [
    {"name": "email", "type": "string"},
    {"name": "salary", "type": "decimal"}
  ]
}
```

#### Relationship (Object Property) Object

```json
{
  "name": "worksIn",
  "localName": "worksIn",
  "type": "ObjectProperty",
  "domain": "Person",
  "range": "Department",
  "direction": "forward",
  "properties": [
    {"id": "attr1", "name": "startDate", "type": "date"},
    {"id": "attr2", "name": "role", "type": "string"}
  ]
}
```

#### Direction Values

| Value | Description |
|-------|-------------|
| `forward` | Relationship goes from domain to range (→) |
| `reverse` | Relationship goes from range to domain (←) |
| `bidirectional` | Relationship goes both ways (↔) |

---

### Error Responses

All endpoints return errors in a consistent format:

```json
{
  "success": false,
  "message": "Error description",
  "error": "Detailed error (optional)"
}
```

**HTTP Status Codes:**

| Code | Meaning |
|------|---------|
| 200 | Success |
| 400 | Bad request (missing/invalid parameters) |
| 401 | Unauthorized (invalid token) |
| 404 | Resource not found |
| 500 | Internal server error |

---

### Authentication

OntoBricks uses Databricks authentication for all data operations:

1. Credentials loaded from environment variables (`.env`)
2. Stored in session during use
3. Never persisted to disk in plain text

**Required Environment Variables:**
- `DATABRICKS_HOST`: Workspace URL (with `https://`)
- `DATABRICKS_TOKEN`: Personal access token or service principal token
- `DATABRICKS_SQL_WAREHOUSE_ID`: SQL Warehouse identifier

---

### Rate Limiting

OntoBricks does not implement rate limiting directly. Limits are determined by:
- Databricks API rate limits
- SQL Warehouse query concurrency

**Best Practices:**
- Use `LIMIT` clauses in queries
- Avoid very large result sets
- Monitor SQL Warehouse utilization

---

### Service Layer

HTML routes are thin; they call **domain objects** under `back/objects/*` and **core** libraries under `back/core/*` directly. A few modules (`home`, `settings`) retain a thin service module under `back/services/` for page-level helpers.

| Route module | Primary domain/core | Purpose |
|--------------|---------------------|---------|
| `front/routes/home.py`, `api/routers/internal/home.py` | `back/services/home.py`, `back/objects/session/DomainSession.py` | Home / session overview |
| `front/routes/home.py` (settings page served from home), `api/routers/internal/settings.py` | `back/services/settings.py`, `shared/config/settings.py`, `shared/config/constants.py` | Settings & environment UI |
| `front/routes/ontology.py`, `api/routers/internal/ontology.py` | `back/objects/ontology/ontology.py`, `back/core/w3c/*` | Ontology design & import |
| `front/routes/mapping.py`, `api/routers/internal/mapping.py` | `back/objects/mapping/mapping.py`, `back/core/w3c/r2rml/*` | Table mapping & R2RML |
| `front/routes/dtwin.py`, `api/routers/internal/dtwin.py` | `back/objects/digitaltwin/digitaltwin.py`, `back/core/w3c/sparql/SparqlTranslator.py` | Digital Twin, SPARQL, query UI |
| `front/routes/domain.py`, `api/routers/internal/domain.py` | `back/objects/domain/domain.py`, `back/objects/session/DomainSession.py` | Domain save/load & registry UX |
| `api/routers/internal/tasks.py` | (handlers in routes; registry scheduler via `back/objects/registry`) | Task status / triggers |
| `api/routers/v1.py`, `api/routers/domains.py`, `api/routers/digitaltwin.py` | `api/service.py` | External stateless REST |
| `back/fastapi/graphql_routes.py` | `back/core/graphql/GraphQLSchemaBuilder.py`, `back/core/graphql/ResolverFactory.py` | GraphQL (also mounted under `/api/v1/graphql`) |

This separation ensures:
- Routes are thin HTTP handlers only
- Business logic lives in domain objects (`back/objects/`) and is testable and reusable
- Clear separation of concerns
- Simplified session management with single-key pattern per module

---

### Session management pattern

Session state lives on ``request.state.session`` (see ``back/objects/session``). The **ontology** and **mapping** HTML routes read and write structured payloads (often keyed as ``ontology_config`` / mapping data) via ``SessionManager`` and the domain classes:

- ``back/objects/ontology/ontology.py`` — ``Ontology`` class for the ontology UI.
- ``back/objects/mapping/mapping.py`` — ``Mapping`` class for mapping UI and session merge helpers.

Prefer inspecting those domain modules and the corresponding ``front/routes/`` and ``api/routers/internal/`` modules for the exact keys and JSON shapes; they are the supported extension points for new UI flows.
