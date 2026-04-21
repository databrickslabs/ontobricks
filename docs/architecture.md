# OntoBricks Architecture

## Overview

OntoBricks is a web-based **Knowledge Graph Builder** that runs natively on Databricks. The core workflow is:

1. **Design** an ontology visually (or import one from OWL / industry standards).
2. **Map** ontology entities to Unity Catalog tables using R2RML.
3. **Materialize** a triple store (Delta table or embedded LadybugDB graph).
4. **Explore** the resulting knowledge graph — visual navigation, SPARQL, GraphQL, data-quality checks, and reasoning.

Under the hood, SPARQL translates ontology mappings into Spark SQL — users never need to write SPARQL themselves.

### High-Level Architecture

![Architecture Overview](images/architecture-overview.svg)

| Layer | What it does |
|-------|-------------|
| **User Interface** | Bootstrap 5.3 + OntoViz visual editor + Sigma.js / D3.js graph views |
| **MCP Server** | Separate Databricks App (`mcp-ontobricks`) exposing knowledge-graph tools to LLM clients (Cursor, Claude Desktop, Playground) |
| **FastAPI Application** | Routes → Domain Objects → Core layered architecture with GlobalConfigService, PermissionService, and BuildScheduler |
| **LLM Agents** | MLflow-traced agentic loops for ontology generation, auto-mapping, icon mapping, and conversational assistance |
| **Reasoning Engine** | OWL 2 RL deductive closure, SWRL rules (compiled to SQL / Cypher), graph reasoning, and constraint validation |
| **Triple Store Backends** | Pluggable Delta and LadybugDB backends via a factory pattern, with BFS, shortest path, and transitive closure built in |
| **Databricks Platform** | Unity Catalog (metadata & governance), SQL Warehouse (query execution), UC Volumes (shared storage) |

---

## Semantic Web Standards Stack

OntoBricks leverages multiple W3C semantic web standards to bridge relational data and knowledge graphs:

![Semantic Web Stack](images/semantic-web-stack.svg)

The stack shows how each layer builds upon the previous:

| Layer | Standard | Role in OntoBricks |
|-------|----------|-------------------|
| **Query** | SPARQL | Semantic query language (used internally for SQL generation) |
| **Validation** | SHACL | Shapes Constraint Language for data quality validation |
| **Mapping** | R2RML | Maps tables to RDF triples |
| **Rules** | SWRL | Horn-clause rules for inference and violation detection |
| **Ontology** | OWL/RDFS | Defines classes and properties |
| **Data** | RDF | Triple data model (S, P, O) |
| **Storage** | SQL / Cypher | Delta table (Spark SQL) or LadybugDB (Cypher) |

---

## Key Standards Explained

### 1. RDF (Resource Description Framework)

**What it is**: The foundational data model for the semantic web. All data is expressed as triples: `(Subject, Predicate, Object)`.

**How OntoBricks uses it**:
- Entities become RDF resources with URIs (e.g., `https://example.org/Person/P001`)
- Relationships become RDF predicates (e.g., `:worksIn`)
- Generated using RDFLib library in Python

**Example Triple**:
```turtle
<https://example.org/Person/P001> <https://example.org/worksIn> <https://example.org/Department/D001> .
```

### 2. OWL (Web Ontology Language)

**What it is**: A knowledge representation language for creating ontologies. Extends RDFS with richer semantics.

**How OntoBricks uses it**:
- **Visual Designer (OntoViz)** creates ontologies visually
- **Form-based interface** for detailed class/property definition
- Classes defined as `owl:Class`
- Properties defined as `owl:ObjectProperty` (relationships) or `owl:DatatypeProperty` (attributes)
- Stored in Turtle (.ttl) format

**Generated OWL Example**:
```turtle
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix : <https://example.org/ontology#> .

:Person a owl:Class ;
    rdfs:label "Person" .

:worksIn a owl:ObjectProperty ;
    rdfs:domain :Person ;
    rdfs:range :Department .
```

**OntoBricks Components**:
- `OntologyGenerator` (`src/back/core/w3c/owl/OntologyGenerator.py`) - Generates OWL from UI configuration
- `OntologyParser` (`src/back/core/w3c/owl/OntologyParser.py`) - Parses existing OWL files
- `OntoViz` (`src/front/static/global/ontoviz/ontoviz.js`) - Visual ontology designer

### 3. R2RML (RDB to RDF Mapping Language)

**What it is**: A W3C standard for expressing mappings from relational databases to RDF.

**How OntoBricks uses it**:
- **Mapping Page** generates R2RML automatically
- Maps Databricks tables to RDF classes
- Maps columns to RDF properties
- Maps SQL queries to relationships between entities
- Supports relationship direction (forward, reverse, bidirectional)

**R2RML Structure**:
```turtle
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix : <https://example.org/ontology#> .

# Entity Mapping (TriplesMap)
<#PersonMapping> a rr:TriplesMap ;
    rr:logicalTable [ rr:tableName "main.default.person" ] ;
    rr:subjectMap [
        rr:template "https://example.org/Person/{person_id}" ;
        rr:class :Person
    ] ;
    rr:predicateObjectMap [
        rr:predicate rdfs:label ;
        rr:objectMap [ rr:column "name" ]
    ] .

# Relationship Mapping (with SQL Query)
<#WorksInMapping> a rr:TriplesMap ;
    rr:logicalTable [ rr:sqlQuery """
        SELECT person_id, dept_id 
        FROM main.default.person_department
    """ ] ;
    rr:subjectMap [
        rr:template "https://example.org/Person/{person_id}"
    ] ;
    rr:predicateObjectMap [
        rr:predicate :worksIn ;
        rr:objectMap [
            rr:template "https://example.org/Department/{dept_id}"
        ]
    ] .
```

**OntoBricks Components**:
- `R2RMLGenerator` (`src/back/core/w3c/r2rml/R2RMLGenerator.py`) - Generates R2RML from mapping config
- `R2RMLParser` (`src/back/core/w3c/r2rml/R2RMLParser.py`) - Parses existing R2RML files

### 4. SPARQL (SPARQL Protocol and RDF Query Language)

**What it is**: The standard query language for RDF data, similar to SQL for relational databases.

**How OntoBricks uses it**:
- SPARQL is used internally to generate Spark SQL from R2RML mappings
- Users do not write SPARQL directly — the system handles translation automatically
- The external REST API (`/api/v1/query`) also accepts SPARQL for programmatic access

**SPARQL Query Example**:
```sparql
PREFIX ont: <https://example.org/ontology#>
SELECT ?person ?personName ?department
WHERE {
    ?person a ont:Person .
    ?person rdfs:label ?personName .
    ?person ont:worksIn ?department .
}
LIMIT 100
```

**OntoBricks SPARQL-to-SQL Translation**:
- `SparqlTranslator` (`src/back/core/w3c/sparql/SparqlTranslator.py`) implements the translator
- Parses SPARQL patterns (subject, predicate, object)
- Maps patterns to table columns using R2RML mappings
- Generates Spark SQL with JOINs, UNION ALL, and STACK functions

### 5. Query Processing Pipeline (inspired by SANSA)

**What it is**: OntoBricks translates ontology mappings into SQL queries to extract triples from Databricks tables and materialize them into a triple store (Delta table or LadybugDB embedded graph).

**How OntoBricks implements this** (inspired by the [SANSA Stack](https://github.com/SANSA-Stack)):

![OBDA Query Flow](images/obda-query-flow.svg)

**Processing Steps:**

1. **SPARQL Query** - Generated internally from ontology and mappings
2. **SPARQL Parser** - Extract SELECT variables, parse WHERE patterns
3. **Triple Patterns** - Identify (subject, predicate, object) patterns
4. **R2RML Analyzer** - Load TriplesMap definitions, build mappings
5. **SPARQL→SQL Translator** - Match patterns to mappings, generate SQL
6. **Generated Spark SQL** - Query with JOINs and UNION ALL
7. **Triple Store Backend Dispatch** - Factory routes to the configured backend:
   - **Delta**: SQL Warehouse executes Spark SQL
   - **LadybugDB** (7b): Bypasses SQL translation entirely; uses native Cypher against the ontology-derived graph schema
8. **RDF-style Results** - Uniform (subject, predicate, object) triples from all backends
9. **Knowledge Graph** - Sigma.js WebGL-powered graph with entity details panel, search, filtering, and data cluster detection (Louvain/Label Propagation/Greedy Modularity)

**Generated Spark SQL Example** (for generic triple query):
```sql
SELECT DISTINCT subject, predicate, object FROM (
    -- Entity triples (rdf:type and properties)
    SELECT 
        CONCAT('https://example.org/Person/', CAST(person_id AS STRING)) AS subject,
        stack(2, 
            'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'https://example.org/ontology#Person',
            'http://www.w3.org/2000/01/rdf-schema#label', CAST(name AS STRING)
        ) AS (predicate, object)
    FROM main.default.person

    UNION ALL

    -- Relationship triples
    SELECT 
        CONCAT('https://example.org/Person/', CAST(person_id AS STRING)) AS subject,
        'https://example.org/ontology#worksIn' AS predicate,
        CONCAT('https://example.org/Department/', CAST(dept_id AS STRING)) AS object
    FROM (SELECT person_id, dept_id FROM main.default.person_department) AS rel_subquery
) AS triples
WHERE object IS NOT NULL
LIMIT 100
```

---

## Application Architecture

The application follows a clean **Routes → Domain objects → Core** layered architecture. The FastAPI surface is split across **`shared`**, **`front`**, and **`back`** packages (see **FastAPI application split** below). The stateless **external** REST API lives under `src/api/` and is mounted by the shared app factory.

### Layered Design

| Layer | Components | Responsibility |
|-------|------------|----------------|
| **Routes** | HTML: `src/front/routes/*.py`; session-aware JSON: `src/api/routers/internal/*.py` | Thin HTTP handlers (request/response only) |
| **Domain Objects** | Classes in `src/back/objects/` | Business logic, validation, transformation (routes call domain classes directly) |
| **Core** | `back/core/helpers/`, `DatabricksClient`, `VolumeFileService`, OWL/R2RML generators, triple-store factory, `GlobalConfigService`, `PermissionService`, `BuildScheduler` | Shared utilities, Databricks connectivity, backend abstraction |
| **Services** | `src/back/services/` (e.g. `home.py`) | Page-level orchestration where routes delegate beyond raw domain objects |
| **Frontend** | `OntoViz`, `Sigma.js`, `Graphology`, `D3.js`, Bootstrap; `src/front/templates/`, `src/front/static/` | Visual design, graph rendering, UI |

### FastAPI application split

The ASGI app is built in **`src/shared/fastapi/main.py`** (`create_app()`), which wires middleware, static files, and routers. Responsibilities are divided as follows:

| Package | Location | Role |
|---------|----------|------|
| **shared** | `shared/fastapi/main.py`, `shared/fastapi/health.py`, `shared/fastapi/csrf.py`, `shared/fastapi/timing.py` | Application factory, CORS / session / permission / CSRF / request-timing middleware, `/static` mount, health and root endpoints; includes routers from `front.routes`, `api.routers.internal`, and mounts the external API |
| **front** | `front/fastapi/dependencies.py` | Jinja2 templates and shared FastAPI/Starlette dependencies for HTML routes |
| **back** | `back/fastapi/graphql_routes.py` | GraphQL router (per-domain auto-generated schema) |

**Uvicorn entry point:** `shared.fastapi.main:app` (see `run.py`, which imports `create_app()` from `shared.fastapi.main`).

### Configuration Split

OntoBricks separates configuration into several layers:

| File | Purpose | Examples |
|------|---------|---------|
| `src/shared/config/settings.py` | **Environment-specific settings** loaded from `.env` / env vars via Pydantic `BaseSettings` | Databricks host, token, session directory |
| `src/shared/config/constants.py` | **Static constants and defaults** that rarely change and are shared across modules | App name/version, OWL namespaces, LLM defaults, wizard quick-templates |
| `src/front/config/menu_config.json` | **Sidebar navigation** for the Jinja2 UI | Page labels, menu structure |
| `src/back/objects/session/global_config.py` | **Instance-level `GlobalConfigService`** — admin-editable settings stored in a `.global_config.json` file on the UC Volume root, shared across all user sessions | SQL Warehouse ID, default base URI, default class icon (emoji) |
| `src/back/objects/registry/permissions.py` | **`PermissionService`** — stores per-user permission levels in `.permissions.json` (app-wide) and per-domain overrides in `.domain_permissions.json` (per domain folder). Defines the role hierarchy: `admin > builder > editor > viewer > none`. | `CAN MANAGE` admin flag |

**`src/shared/config/constants.py`** key contents:

| Constant | Description |
|----------|-------------|
| `APP_NAME`, `APP_VERSION` | Application identity |
| `ONTOBRICKS_NS` | RDFLib `Namespace` for the OntoBricks schema (`http://ontobricks.com/schema#`) |
| `DEFAULT_BASE_URI` | Default ontology base URI |
| `LLM_DEFAULT_MAX_TOKENS`, `LLM_DEFAULT_TEMPERATURE` | LLM generation defaults |
| `WIZARD_TEMPLATES` | Ontology generation quick-templates (served to the frontend via `GET /ontology/wizard/templates`) |
| `AUTO_ASSIGN_CHUNK_SIZE` | Max entities + relationships per auto-map agent run (default: 5) |
| `AUTO_ASSIGN_CHUNK_COOLDOWN` | Seconds to wait between agent chunks to avoid LLM rate limits (default: 15) |

To add a new generation template, add an entry to `WIZARD_TEMPLATES` in `src/shared/config/constants.py` — the UI button is rendered dynamically.

**`src/back/objects/session/global_config.py`** (`GlobalConfigService` singleton):

| Setting | Description | Modified By |
|---------|-------------|-------------|
| `warehouse_id` | SQL Warehouse ID used by all backends and API calls | Admin only |
| `default_base_uri` | Default ontology base URI domain | Admin only |
| `default_emoji` | Default class icon emoji (e.g. `📦`) | Admin only |

The service caches the JSON file in memory with a TTL to minimize UC Volume reads. Settings are resolved via `resolve_warehouse_id()`, `resolve_default_base_uri()`, and `resolve_default_emoji()` helper functions in `src/back/core/helpers/`. For version-scoped UC paths and LadybugDB local layout, `DatabricksHelpers.py` also provides `effective_uc_version_path` and `resolve_ladybug_local_path`.

**UC Volume file layout** (root of the configured registry volume):

```
/Volumes/{catalog}/{schema}/{volume}/
├── .registry                  # Marker file (presence = initialized)
├── .global_config.json        # Instance-level admin settings (includes build schedules)
├── .permissions.json          # App-wide per-user permission levels (viewer / editor / builder)
└── domains/                   # Domain version files (legacy registries may use `projects/`)
    └── {domain_name}/
        ├── .domain_permissions.json  # Optional per-domain role overrides
        ├── V1/
        │   ├── V1.json                                    # Domain version payload
        │   ├── documents/                                 # Version-scoped documents
        │   └── ontobricks_{name}_V1.lbug.tar.gz           # LadybugDB archive
        ├── V2/
        │   ├── V2.json
        │   ├── documents/
        │   └── ontobricks_{name}_V2.lbug.tar.gz
        └── ...
```

### Key Design Principles

1. **Separation of Concerns**: Routes handle HTTP, services handle logic
2. **Session State**: Custom file-based session stores ontology, mappings, R2RML between requests
3. **Modular UI**: Sidebar layout with reusable partials per page
4. **No Manual Query Writing**: SPARQL is used internally — translated to SQL automatically for triple materialization
5. **Visual-First Design**: OntoViz enables drag-and-drop ontology creation
6. **Agentic Automation**: LLM-powered agents with MCP-style tools handle complex tasks autonomously (see [Agentic Architecture](architecture.md#agentic-architecture))
7. **Observability**: MLflow tracing captures every agent → LLM → tool span for debugging, cost tracking, and evaluation
8. **MCP Integration**: The MCP server exposes knowledge-graph tools (entity search, GraphQL queries, domain selection) to LLM clients via the Model Context Protocol (see [MCP Server](mcp.md))

---

## Module Structure

The application follows a strict code organization pattern (see `src/.coding_rules.md`):
- **Routes** — HTML handlers live in `src/front/routes/`; session-aware JSON endpoints live in `src/api/routers/internal/`. Keep routing thin (no business rules or data access in route functions).
- **Domain objects** (`back.objects`) hold business logic; routes call them directly unless they use a small **service** in `src/back/services/`.
- **Core** (`back.core`) holds reusable infrastructure, Databricks connectivity, and W3C standards.
- **Front** — Jinja2 templates are consolidated under `src/front/templates/`; static assets under `src/front/static/`; menu config under `src/front/config/`.
- **Shared** — App factory, health routes, and cross-cutting settings/constants (`shared.config`).
- All Python files in `core/` and `objects/` use **PascalCase** naming (e.g. `DatabricksClient.py`).

```
src/
├── shared/                             # Shared app shell & configuration
│   ├── fastapi/
│   │   ├── main.py                     # Application factory (create_app), middleware, router registration
│   │   ├── health.py                   # Health check & root endpoints
│   │   ├── csrf.py                     # CSRF double-submit cookie middleware
│   │   └── timing.py                   # Request duration logging middleware
│   └── config/
│       ├── settings.py                 # Pydantic BaseSettings (env vars, .env)
│       └── constants.py                # Static constants & defaults (namespaces, LLM params, wizard templates)
│
├── front/                              # HTML UI: routes, templates, static, menu
│   ├── fastapi/
│   │   └── dependencies.py             # Jinja2 templates & shared dependencies for HTML routes
│   ├── config/
│   │   └── menu_config.json            # Sidebar navigation structure
│   ├── routes/                         # HTML page routers (home, ontology, mapping, digital twin, project)
│   ├── templates/                      # Consolidated Jinja2 templates (partials per feature area)
│   └── static/                         # Static assets (css/, js/, img/, ontoviz/, per-area folders)
│
├── back/                               # Backend: domain, core infra, GraphQL
│   ├── fastapi/
│   │   └── graphql_routes.py           # GraphQL API (auto-generated schema per domain)
│   ├── services/                       # Optional page-level services (e.g. home.py)
│   ├── core/                           # Core infrastructure
│   │   ├── helpers/                    # Centralized helper functions
│   │   │   ├── DatabricksHelpers.py
│   │   │   ├── SQLHelpers.py
│   │   │   └── URIHelpers.py
│   │   ├── logging/                    # Centralized logging (dictConfig via LogManager)
│   │   │   └── LogManager.py
│   │   ├── task_manager/               # In-memory async task runner (threading-based)
│   │   │   └── TaskManager.py
│   │   ├── errors/                     # Centralized error hierarchy
│   │   │   ├── OntoBricksError.py      # Base error class
│   │   │   ├── ValidationError.py
│   │   │   ├── NotFoundError.py
│   │   │   ├── AuthorizationError.py
│   │   │   ├── ConflictError.py
│   │   │   ├── InfrastructureError.py
│   │   │   └── ErrorResponse.py
│   │   │
│   │   ├── databricks/                 # Databricks connectivity
│   │   │   ├── DatabricksAuth.py       # Authentication & utility functions
│   │   │   ├── DatabricksClient.py     # Thin facade
│   │   │   ├── SQLWarehouse.py         # Query execution, DDL (connection-pooled)
│   │   │   ├── UnityCatalog.py         # Catalogs, schemas, tables, volumes
│   │   │   ├── UCDomainIO.py           # Domain I/O on UC Volumes
│   │   │   ├── VolumeFileService.py    # File I/O on UC Volumes
│   │   │   ├── WorkspaceService.py     # SCIM users/groups
│   │   │   ├── DashboardService.py     # Lakeview + legacy dashboards
│   │   │   └── MetadataService.py      # Table metadata
│   │   │
│   │   ├── w3c/                        # W3C semantic web standards
│   │   │   ├── owl/                    # OntologyGenerator, OntologyParser
│   │   │   ├── r2rml/                  # R2RMLGenerator, R2RMLParser
│   │   │   ├── rdfs/                   # RDFSParser
│   │   │   ├── sparql/                 # SparqlTranslator, SparqlQueryRunner, DomainQueryService
│   │   │   └── shacl/                  # SHACLGenerator, SHACLParser, SHACLService
│   │   │
│   │   ├── graphql/                    # GraphQLSchemaBuilder, ResolverFactory, SchemaMetadata
│   │   ├── triplestore/                # Triple store backend abstraction
│   │   │   ├── TripleStoreBackend.py   # Abstract interface
│   │   │   ├── TripleStoreFactory.py   # Backend factory ("view" → Delta, "graph" → LadybugDB)
│   │   │   ├── IncrementalBuildService.py
│   │   │   ├── delta/                  # DeltaTripleStore (SQL Warehouse backend)
│   │   │   └── ladybugdb/              # LadybugGraphStore, LadybugFlatStore, GraphSchema, etc.
│   │   │
│   │   ├── reasoning/                  # Reasoning engine (OWL 2 RL + SWRL + SPARQL rules + Decision tables)
│   │   │   ├── ReasoningService.py     # Multi-phase orchestrator
│   │   │   ├── OWLRLReasoner.py        # OWL 2 RL deductive closure via owlrl
│   │   │   ├── SWRLEngine.py           # SWRL rule execution, backend dispatch
│   │   │   ├── SWRLParser.py           # SWRL rule parsing
│   │   │   ├── SWRLSQLTranslator.py    # SWRL → Spark SQL
│   │   │   ├── SWRLCypherTranslator.py # SWRL → Cypher
│   │   │   ├── SWRLFlatCypherTranslator.py
│   │   │   ├── SWRLBuiltinRegistry.py  # SWRL built-in function registry
│   │   │   ├── SPARQLRuleEngine.py     # SPARQL-based rule execution
│   │   │   ├── AggregateRuleEngine.py  # Aggregate rule support
│   │   │   ├── DecisionTableEngine.py  # Decision table evaluation
│   │   │   └── models.py               # InferredTriple, RuleViolation, ReasoningResult
│   │   │
│   │   ├── industry/                   # Industry-standard ontology importers
│   │   │   ├── fibo/                   # FiboImportService
│   │   │   ├── cdisc/                  # CdiscImportService
│   │   │   └── iof/                    # IofImportService
│   │   │
│   │   ├── graph_analysis/             # Community detection & clustering
│   │   │   ├── CommunityDetector.py    # CommunityDetector (NetworkX backend)
│   │   │   └── models.py              # ClusterRequest, ClusterResult, DetectionResult, DetectionStats
│   │   │
│   │   └── sqlwizard/                  # SQL generation helpers for entity mapping
│   │       └── SQLWizardService.py
│   │
│   └── objects/                        # Domain objects (business logic layer)
│       ├── ontology/                   # Ontology domain (ontology.py, json_views.py)
│       ├── mapping/                    # Mapping domain (mapping.py, json_views.py)
│       ├── domain/                     # Saved domain / UC I/O (domain.py, payload.py, version_status.py)
│       ├── digitaltwin/                # Digital Twin domain (DigitalTwin.py, models.py)
│       ├── session/                    # Session management
│       │   ├── middleware.py           # File-based session middleware (cookie + ASGI)
│       │   ├── SessionManager.py       # Request-scoped session get/set/delete wrapper
│       │   ├── DomainSession.py       # DomainSession (current OntoBricks domain payload)
│       │   └── global_config.py        # Instance-level GlobalConfigService
│       └── registry/                   # Registry, permissions & scheduled builds
│           ├── service.py              # RegistryService + RegistryCfg
│           ├── permissions.py          # PermissionService (ADMIN / BUILDER / EDITOR / VIEWER / NONE + domain-level overrides)
│           └── scheduler.py            # BuildScheduler (APScheduler-based)
│
├── api/                                # REST API layer (mounted into the main app)
│   ├── external_app.py                 # External stateless API app factory (/api/v1/…)
│   ├── service.py                      # API business logic
│   └── routers/
│       ├── v1.py                       # /api/v1/* endpoints
│       ├── domains.py                  # Domain registry & artifact endpoints (`/api/v1/domains`, `/api/v1/domain/...`)
│       ├── digitaltwin.py              # Digital Twin REST API
│       └── internal/                   # Session-aware JSON routes for the web UI
│           ├── home.py, settings.py, domain.py, ontology.py, mapping.py, dtwin.py, tasks.py, …
│
├── agents/                             # LLM Agents
│   ├── llm_utils.py                    # Shared LLM call with retry (429/503 backoff)
│   ├── serialization.py                # Agent serialization utilities
│   ├── tracing.py                      # MLflow tracing setup & decorators
│   ├── tools/                          # Shared agent tools (ontology, mapping, metadata, SQL, etc.)
│   ├── agent_owl_generator/            # OWL ontology generation agent
│   ├── agent_auto_assignment/          # Entity/relationship → SQL mapping agent
│   ├── agent_auto_icon_assign/         # Emoji icon mapping agent
│   └── agent_ontology_assistant/       # Conversational assistant + ResponsesAgent wrapper
│
└── mcp-server/                         # MCP Server (separate Databricks App)
    ├── app.yaml                        # Databricks App config
    ├── deploy-mcp-server.sh            # Deployment script
    ├── pyproject.toml                  # Python dependencies
    └── server/
        ├── app.py                      # MCP tools, domain selection, text formatting
        └── main.py                     # Entry point

tests/                                  # Test suite (at project root)
├── conftest.py
├── e2e/                                # End-to-end tests
│   └── test_e2e_flows.py
├── test_ladybug.py
├── test_ladybug_schema.py
├── test_ladybug_sync.py
├── test_reasoning.py
├── test_reasoning_service.py
├── test_permissions.py
├── test_registry.py
├── test_triplestore_factory.py
├── test_graphql.py
├── test_sparql_service.py
├── test_owl_generator.py
├── test_owl_parser.py
├── test_r2rml_generator.py
├── test_r2rml_parser.py
├── ... (50+ test modules)
```

---

## OntoViz Component Architecture

OntoViz is a custom JavaScript library for visual entity-relationship diagram editing. It is designed to be **reusable** and can be integrated into other projects.

For complete documentation, see [OntoViz Documentation](architecture.md#ontoviz---visual-ontology-editor).

### Key Features

| Feature | Description |
|---------|-------------|
| **Entities** | OWL Classes with icons, names, and data properties |
| **Relationships** | OWL Object Properties with direction and attributes |
| **Inheritances** | rdfs:subClassOf links with property inheritance |
| **Canvas Controls** | Zoom, pan, auto-layout, minimap |
| **Serialization** | JSON export/import for persistence |

### Core Classes

```javascript
class Entity {
    id, name, icon, description, properties, x, y
}

class Relationship {
    id, name, sourceEntityId, targetEntityId, direction, properties
}

class Inheritance {
    id, sourceEntityId, targetEntityId
}

class OntoViz {
    // Entity, Relationship, Inheritance management
    // Layout & navigation
    // JSON serialization
}
```

### Inheritance Feature

Inheritance links represent `rdfs:subClassOf` relationships in OWL:

- **Visual representation**: Dotted line with hollow arrow from parent to child
- **Drag-and-drop creation**: Click △ toolbar button, then drag from parent connector to child entity
- **Property inheritance**: Child entities automatically inherit and display parent's properties (read-only)
- **OWL generation**: Produces `rdfs:subClassOf` triples in the generated OWL
- **Cascade updates**: When parent properties change, child entities are automatically updated

---

## User Interface Architecture

The UI uses a consistent **sidebar layout** across all main pages:

![UI Layout](images/ui-layout.svg)

### UI Components

| Component | File | Purpose |
|-----------|------|---------|
| **Base Template** | `base.html` | Navbar, global CSS/JS |
| **Sidebar Layout** | `sidebar-layout.css` | Reusable sidebar + content structure |
| **Sidebar Nav** | `sidebar-nav.js` | Tab-like navigation controller |
| **Partials** | `partials/_*.html` | Section content (information, entities, etc.) |
| **Core JS** | `ontology-core.js`, `mapping-core.js` | Shared state and functions |
| **OntoViz** | `ontoviz.js`, `ontoviz.css` | Visual ontology designer |

### Page Structure

Each main page (Ontology, Mapping, Digital Twin) follows this pattern:

```
page.html
├── sidebar-layout (container)
│   ├── sidebar-nav (left menu)
│   │   ├── Section group 1
│   │   │   └── Nav links (data-section="...")
│   │   └── Section group 2
│   │       └── Nav links
│   └── sidebar-content (right area)
│       ├── section#design-section
│       │   └── {% include "partials/_ontology_design.html" %}
│       ├── section#entities-section
│       │   └── {% include "partials/_ontology_entities.html" %}
│       └── ... more sections
└── page-specific scripts
```

---

## Data Flow

![Data Flow](images/data-flow.svg)

### Flow Summary

| Flow | Steps | Key Components |
|------|-------|----------------|
| **Design** | Visual design → Auto-save → Session storage | `OntoViz`, Session middleware |
| **Ontology** | Design/Configure → Save → Generate OWL | `OntologyGenerator`, Session middleware |
| **Mapping** | Load ontology → Map/Auto-Map → Validate attributes → Generate R2RML | `R2RMLGenerator`, `SQLWizardService`, `TaskManager` |
| **Digital Twin** | Build → Quality Check (async) → Auto-load Triples → Explore Knowledge Graph | `sparql_service`, `TaskManager`, Sigma.js graph |
| **API/MCP** | REST → resolve domain → triple store query → formatted response | Digital Twin API, MCP Server, GraphQL |

### Asynchronous Task Processing

Long-running operations use the **TaskManager** pattern (`src/back/core/task_manager/TaskManager.py`), an in-memory singleton that manages background tasks:

| Task Type | Triggered By | Description |
|-----------|-------------|-------------|
| `triplestore_sync` | Digital Twin → Build | Generates and writes triples to the configured backend (Delta or LadybugDB) |
| `quality_checks` | Digital Twin → Quality | Runs all quality checks sequentially with per-check progress |
| `auto_assign` | Mapping → Auto-Map | Batch-maps entities and relationships via LLM; splits large jobs into chunks of `AUTO_ASSIGN_CHUNK_SIZE` with cooldown between chunks to avoid rate limits |

**How it works:**
1. Frontend sends a `POST` to start the task; backend creates a `TaskManager` task and spawns a `threading.Thread`
2. Frontend stores the `task_id` in `sessionStorage` and polls `/tasks/{task_id}` for progress
3. Backend thread updates progress (percentage, current step message) via `TaskManager`
4. On completion, the task result is returned to the frontend, which saves mappings and updates the UI
5. If the user navigates away and returns, the frontend resumes monitoring from `sessionStorage`

### Scheduled Builds (BuildScheduler)

The **BuildScheduler** (`src/back/objects/registry/scheduler.py`) provides per-domain scheduled triple store builds using APScheduler's `BackgroundScheduler`. Schedule definitions are persisted in `.global_config.json` on the UC Volume alongside other instance-level settings.

Each schedule entry contains:

| Field | Description |
|-------|-------------|
| `interval_minutes` | How often to run (2, 5, 10, 30, 60, 360, 720, 1440) |
| `drop_existing` | Whether to replace data on each build |
| `enabled` | Whether the schedule is active |
| `last_run` | ISO timestamp of the last execution |
| `last_status` | `"success"` / `"error"` / `null` |
| `last_message` | Human-readable outcome of the last run |

Jobs are restored at startup from environment-variable credentials when available. If registry config is session-only, jobs are lazily registered when a user opens the Schedule tab.

### Session State

The session middleware maintains state using a **unified domain session** pattern (implemented by `DomainSession`):

| Key | Contents | Set By |
|-----|----------|--------|
| `project_data` | Complete domain: info, ontology, mapping, design_layout, databricks, preferences, generated | DomainSession |

**Unified Session Structure:**

```python
{
    "info": { "name", "description", "author" },
    "current_version": "1.0",
    "ontology": { 
        "name", "base_uri", "classes", "properties",
        "constraints", "swrl_rules", "axioms" 
    },
    "assignment": { 
        "entities", "relationships", 
        "r2rml_output"  # Runtime only, not exported
    },
    "design_layout": { "entities", "relationships", "inheritances", "positions" },
    "databricks": { "host", "token" },  # Not exported; warehouse_id is in GlobalConfigService
    "preferences": { },                 # Not exported; emoji/base URI are in GlobalConfigService
    "generated": { "owl", "sql" }       # Not exported
}
```

**Session Service Pattern:**

The `DomainSession` class (`src/back/objects/session/DomainSession.py`) provides:
- Unified access to all domain data
- Automatic migration from legacy session keys
- Export/import for file persistence
- Version management support

---

## Unity Catalog Versioning

OntoBricks uses Unity Catalog Volumes for domain storage with built-in version control.

### Storage Structure

```
Unity Catalog
└── catalog (e.g., main)
    └── schema (e.g., ontobricks)
        └── volume (e.g., OntoBricksRegistry)
            └── domains/
                └── {domain_name}/
                    ├── V1/
                    │   ├── V1.json                                # Version 1 payload
                    │   ├── documents/                             # Version-scoped documents
                    │   └── ontobricks_{name}_V1.lbug.tar.gz       # LadybugDB archive
                    ├── V2/
                    │   ├── V2.json                                # Version 2 payload
                    │   ├── documents/
                    │   └── ontobricks_{name}_V2.lbug.tar.gz
                    └── V3/
                        ├── V3.json                                # Version 3 payload
                        ├── documents/
                        └── ontobricks_{name}_V3.lbug.tar.gz
```

### Export Format (Versioned)

Domains are exported in a versioned JSON format:

```json
{
    "info": { 
        "name": "My Domain", 
        "description": "...", 
        "author": "..." 
    },
    "versions": {
        "1": {
            "ontology": { /* classes, properties, constraints, rules, axioms */ },
            "assignment": { /* entity and relationship mappings */ },
            "design_layout": { /* OntoViz visual state */ }
        }
    }
}
```

### What is NOT Saved

For security and regeneration reasons, these are excluded from exports:
- **Databricks credentials** (host, token)
- **Instance-level settings** (warehouse_id, base URI, emoji — stored in `GlobalConfigService`)
- **R2RML output** (regenerated from mappings on load)
- **OWL output** (regenerated from ontology on load)
- **Query results** (ephemeral data)
- **User preferences** (runtime only)

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/domain/list-versions` | GET | List all versions of a domain |
| `/domain/save-to-uc` | POST | Save domain to Unity Catalog |
| `/domain/load-from-uc` | POST | Load specific version from UC |
| `/domain/create-version` | POST | Create new version (increment) |
| `/domain/version-status` | GET | Get current version status |

### Version Workflow

1. **First Save**: Creates `domains/{name}/V1/V1.json`
2. **Update Save**: Overwrites `domains/{name}/V{ver}/V{ver}.json`
3. **Create Version**: Increments the version number, copies documents from the previous version directory
4. **Load Version**: Loads the requested version from `V{ver}/V{ver}.json`, regenerates R2RML and OWL

### Auto-Save Flow (Design View)

```
User Action → OntoViz Event → Debounce → syncDesignToOntology → /ontology/save → Session Update
```

---

## Triple Store Backends

OntoBricks supports two interchangeable triple store backends, selectable per domain in the **Domain → Information → Triple Store** tab:

| Backend | Key | Storage | Query Language | Use Case |
|---------|-----|---------|----------------|----------|
| **Delta** | `view` | Databricks Delta table via SQL Warehouse | Spark SQL | Production workloads on Databricks with full Unity Catalog governance |
| **LadybugDB** | `graph` | Embedded graph database on local disk (`/tmp`) | Cypher | Lightweight, zero-infrastructure local development and demos |

### Backend Abstraction

All backends implement the `TripleStoreBackend` abstract base class (`src/back/core/triplestore/TripleStoreBackend.py`), which defines a common interface for:
- Table lifecycle: `create_table`, `drop_table`, `table_exists`
- Triple operations: `insert_triples`, `query_triples`, `count_triples`
- Named queries: `get_aggregate_stats`, `find_subjects_by_type`, `bfs_traversal`, etc.

The factory (`src/back/core/triplestore/TripleStoreFactory.py`) reads `domain.triplestore.backend` and returns the appropriate implementation.

### LadybugDB Architecture

LadybugDB (`real_ladybug`) is an embedded graph database with Cypher query support. It differs from Delta in several ways:

**Storage Model:**
- Data lives on local disk at `/tmp/ontobricks_<project_name>.lbug`
- Supports two internal models:
  - **Flat model**: A single `Triple` node table with `(id, subject, predicate, object)` columns — used when no ontology is provided
  - **Graph model**: Ontology classes become node tables, object properties become relationship tables — used when the domain has a loaded ontology

**Graph Schema Generation** (`src/back/core/triplestore/ladybugdb/GraphSchema.py`, `GraphSchemaBuilder.py`):
- Maps each OWL class to a LadybugDB node table (`CREATE NODE TABLE`)
- Maps each OWL ObjectProperty to a relationship table (`CREATE REL TABLE`)
- A `Resource` fallback table captures entities whose type is not in the ontology
- DDL is generated as Cypher statements and executed at table creation time

**UC Volume Sync** (`src/back/core/triplestore/ladybugdb/GraphSyncService.py`):
- **After the Digital Twin build**: the local `.lbug` directory is archived into a `tar.gz` and uploaded to the UC Volume at `domains/{folder}/V{version}/ontobricks_{name}_V{version}.lbug.tar.gz`
- On **domain load**: the archive is downloaded and extracted to `/tmp`, restoring the graph
- Archive path: `/Volumes/{cat}/{sch}/{vol}/domains/{folder}/V{version}/ontobricks_{name}_V{version}.lbug.tar.gz`

**Query Translation:**
- All named query methods (stats, type distribution, BFS traversal, etc.) are implemented natively in Cypher instead of SQL
- A `_translate_conditions` helper converts simple SQL conditions (equality, LIKE patterns) to Cypher equivalents
- `execute_query()` raises `NotImplementedError` since raw SQL is not supported

---

## Reasoning Engine

OntoBricks includes a **multi-phase reasoning engine** (`src/back/core/reasoning/`) that brings formal ontology reasoning, rule evaluation, graph-structural inference, and constraint checking to the Databricks Lakehouse — without requiring an external reasoner or graph database.

### Architecture

```
                    ReasoningService.run_full_reasoning()
                                  │
         ┌────────────────────────┼────────────────────────┐
         │                        │                        │
         ▼                        ▼                        ▼
   ┌───────────┐           ┌───────────┐           ┌───────────┐
   │  Phase 1  │           │  Phase 2  │           │  Phase 3  │
   │  T-Box    │           │   SWRL    │           │   Graph   │
   │ (OWL 2 RL)│           │  Rules    │           │ Reasoning │
   └─────┬─────┘           └─────┬─────┘           └─────┬─────┘
         │                       │                       │
   owlrl library           SWRLEngine              TripleStore-
   DeductiveClosure        ├─ SQL translator       Backend methods
   (OWLRL_Semantics)       └─ Cypher translator    ├─ transitive_closure
         │                       │                 ├─ symmetric_expand
         ▼                       ▼                 └─ shortest_path
   InferredTriple[]        RuleViolation[]               │
                           InferredTriple[]               ▼
                                                   InferredTriple[]
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 ▼
                        ┌───────────────┐        ┌──────────────┐
                        │ Phase 4       │        │ Materialize  │
                        │ Constraints   │        │ (optional)   │
                        │ (Cypher/SQL)  │        │ Write back   │
                        └───────────────┘        │ to store     │
                                                 └──────────────┘
```

### Phase 1: T-Box Reasoning — OWL 2 RL Profile

**OWL 2 RL** (Rule Language) is one of three profiles defined by the [W3C OWL 2 specification](https://www.w3.org/TR/owl2-profiles/#OWL_2_RL). It is specifically designed for **forward-chaining rule-based implementations** — making it ideal for materialization workflows where inferred triples are written back to a triple store.

**Implementation** (`src/back/core/reasoning/OWLRLReasoner.py`):

The `OWLRLReasoner` class uses the [`owlrl`](https://owl-rl.readthedocs.io/) Python library to perform deductive closure:

1. Parses the domain's generated OWL Turtle into an RDFLib `Graph`
2. Snapshots the original triple set
3. Runs `DeductiveClosure(OWLRL_Semantics).expand(graph)` — applying all OWL 2 RL entailment rules via forward chaining
4. Computes the delta (new triples minus original triples)
5. Filters noise: blank-node triples, axiomatic vocabulary (RDF/RDFS/OWL/XSD namespaces), reflexive tautologies (`X sameAs X`, `X subClassOf X`), and redundant type declarations

**What OWL 2 RL infers:**
- Subclass/subproperty entailments (`rdfs:subClassOf`, `rdfs:subPropertyOf`)
- Domain and range typing (if `P rdfs:domain C` and `x P y`, then `x rdf:type C`)
- Property characteristics (functional, inverse functional, transitive, symmetric)
- Class axioms (equivalent classes, disjoint classes)
- Property chain axioms
- `sameAs` / `differentFrom` reasoning

**Two modes:**
- **T-Box only** (default): Closure over the ontology schema — fast, suitable for all dataset sizes
- **T-Box + A-Box**: Closure over ontology + instance data — suitable for small datasets (< ~50,000 triples)

### Phase 2: SWRL Rule Evaluation

**SWRL** (Semantic Web Rule Language) extends OWL with Horn-clause rules. OntoBricks implements a custom SWRL engine that compiles rules into the query language of the active triple store backend.

**Rule format:**
```
Antecedent → Consequent
```
Where atoms are class assertions (`Person(?x)`) or property assertions (`worksIn(?x, ?y)`).

**Implementation** (`src/back/core/reasoning/SWRLEngine.py`, `SWRLSQLTranslator.py`, `SWRLCypherTranslator.py`):

| Component | File | Role |
|-----------|------|------|
| `SWRLEngine` | `SWRLEngine.py` | Orchestrator — selects translator, executes rules, collects results |
| `SWRLSQLTranslator` | `SWRLSQLTranslator.py` | Compiles SWRL atoms into SQL with multi-join + `NOT EXISTS` for violation detection |
| `SWRLFlatCypherTranslator` | `SWRLFlatCypherTranslator.py` | Compiles to Cypher for LadybugDB flat store (single `Triple` node table) |
| `SWRLCypherTranslator` | `SWRLCypherTranslator.py` | Compiles to Cypher using the ontology-derived graph schema (typed node/rel tables) |

**Execution modes:**
- **Violation detection**: Finds instances where the antecedent holds but the consequent does not (generates `NOT EXISTS` subqueries)
- **Materialization**: Inserts inferred consequent triples into the store (generates `INSERT` statements)

**Backend dispatch**: The engine inspects the store instance type (`DeltaTripleStore` → SQL, `LadybugFlatStore` → flat Cypher, `LadybugGraphStore` → graph-schema Cypher) and selects the appropriate translator.

**URI resolution**: The engine builds a lowercase-name → URI map from the ontology, normalizing property URIs to the data namespace used by R2RML so that SWRL atom names match the predicates stored in the triple store.

### Phase 3: Graph Reasoning

Graph reasoning leverages **OWL property characteristics** to perform structural inference on the triple store:

**Transitive closure** (`owl:TransitiveProperty`):
- For properties like `partOf`, `subRegionOf`, or `reportsTo`
- Discovers indirect relationships not explicitly asserted (if A partOf B and B partOf C, infers A partOf C)
- **Delta**: SQL recursive CTE with depth limit
- **LadybugDB**: Cypher variable-length path patterns `[:REL*2..N acyclic]`

**Symmetric expansion** (`owl:SymmetricProperty`):
- For properties like `adjacentTo`, `siblingOf`, or `marriedTo`
- For every `(a, P, b)` where `(b, P, a)` is missing, adds the inverse edge
- **Delta**: SQL `NOT EXISTS` anti-join
- **LadybugDB**: Cypher `NOT EXISTS` subquery

**Shortest path** (LadybugDB only):
- Uses Cypher's native `SHORTEST` algorithm
- Not available on Delta (too expensive in SQL)

### Phase 4: Constraint Checking

Validates instance data in the triple store against formal ontology constraints:

| Constraint Type | OWL Construct | Validation |
|----------------|--------------|-----------|
| Min cardinality | `owl:minCardinality` | Subject has at least N values for property |
| Max cardinality | `owl:maxCardinality` | Subject has at most N values for property |
| Exact cardinality | `owl:cardinality` | Subject has exactly N values for property |
| Functional | `owl:FunctionalProperty` | At most one distinct object per subject |
| Inverse functional | `owl:InverseFunctionalProperty` | At most one distinct subject per object |
| Value constraints | OntoBricks extension | `notNull`, `startsWith`, `endsWith`, `contains`, `equals`, `matches` (regex) |
| No orphans | Global rule | Every subject has an `rdf:type` assertion |
| Require labels | Global rule | Every typed entity has an `rdfs:label` |

**Execution**:
- On **LadybugDB**: Constraint checks run as Cypher queries via `ReasoningService`
- On **Delta**: Quality checks run as SQL queries via the Digital Twin quality pipeline

### Reasoning Data Model

All reasoning phases produce standardized output via three dataclasses (`src/back/core/reasoning/models.py`):

| Model | Purpose |
|-------|---------|
| `InferredTriple` | A new triple with `subject`, `predicate`, `object`, `provenance` (e.g., `"owlrl"`, `"swrl:RuleName"`, `"graph:transitive"`) |
| `RuleViolation` | A constraint failure with `rule_name`, `subject`, `message`, `check_type` |
| `ReasoningResult` | Aggregated output: `inferred_triples[]`, `violations[]`, `stats{}` — mergeable across phases |

### Materialization

Inferred triples from any phase can be **materialized** (written back) to the triple store:
- `ReasoningService.materialize_inferred()` inserts into the active backend (Delta or LadybugDB)
- `ReasoningService.materialize_to_delta()` provides a static method for Delta-specific materialization with table creation and data replacement

### Key Files

| File | Purpose |
|------|---------|
| `src/back/core/reasoning/ReasoningService.py` | `ReasoningService` — orchestrates all phases |
| `src/back/core/reasoning/OWLRLReasoner.py` | `OWLRLReasoner` — OWL 2 RL deductive closure |
| `src/back/core/reasoning/SWRLEngine.py` | `SWRLEngine` — SWRL rule orchestration and backend dispatch |
| `src/back/core/reasoning/SWRLSQLTranslator.py` | `SWRLSQLTranslator` — SWRL → Spark SQL compilation |
| `src/back/core/reasoning/SWRLCypherTranslator.py` | `SWRLCypherTranslator` / `SWRLFlatCypherTranslator` — SWRL → Cypher compilation |
| `src/back/core/reasoning/models.py` | `InferredTriple`, `RuleViolation`, `ReasoningResult` dataclasses |
| `src/back/core/triplestore/TripleStoreBackend.py` | Graph reasoning primitives: `transitive_closure()`, `symmetric_expand()`, `shortest_path()` |

---

## Graph Analysis — Community Detection

OntoBricks provides **data cluster detection** on the knowledge graph, allowing users to discover communities of densely connected entities. Detection is available at two levels:

### Client-Side (Graphology)

The frontend uses the `graphology-communities-louvain` algorithm (bundled with `graphology-library`) to run Louvain community detection directly in the browser on the currently displayed subgraph. This is instant and requires no backend call.

### Server-Side (NetworkX)

For full-graph analysis, the backend `CommunityDetector` service (`src/back/core/graph_analysis/CommunityDetector.py`) loads all triples from the triple store, builds an undirected NetworkX graph (filtering out RDF type/label predicates), and runs one of three algorithms:

| Algorithm | NetworkX Function | Description |
|-----------|-------------------|-------------|
| **Louvain** | `community.louvain_communities()` | Modularity-maximizing hierarchical clustering (default) |
| **Label Propagation** | `community.label_propagation_communities()` | Fast, near-linear-time detection |
| **Greedy Modularity** | `community.greedy_modularity_communities()` | Greedy agglomerative approach |

The result includes cluster membership, modularity score, and per-cluster member lists. It is returned to the frontend via `POST /dtwin/clusters/detect`.

### Visualization

Detected clusters can be visualized in several ways:

- **Color by cluster** — nodes are recolored by community assignment instead of entity type
- **Resolution slider** — controls Louvain granularity (higher resolution = more clusters)
- **Collapse/expand** — clusters can be collapsed into super-nodes showing size and member count; clicking a super-node shows its members in the detail panel

### File Reference

| File | Purpose |
|------|---------|
| `src/back/core/graph_analysis/CommunityDetector.py` | `CommunityDetector` — loads triples, builds NetworkX graph, runs algorithm |
| `src/back/core/graph_analysis/models.py` | `ClusterRequest`, `ClusterResult`, `DetectionResult`, `DetectionStats` dataclasses |
| `src/api/routers/internal/dtwin.py` | `POST /dtwin/clusters/detect` endpoint |
| `src/back/objects/digitaltwin/digitaltwin.py` | `DigitalTwin.detect_clusters()` method |
| `src/front/static/query/js/query-sigmagraph.js` | Client-side Louvain detection, cluster UI logic, super-node rendering |
| `src/front/templates/partials/dtwin/_query_sigmagraph.html` | Data Clusters sidebar panel |
| `src/front/static/query/css/query-sigmagraph.css` | Cluster panel and chip styles |

---

## SHACL Data Quality

OntoBricks includes a **SHACL (Shapes Constraint Language)** module (`src/back/core/w3c/shacl/`) that provides W3C-standard data quality validation for the knowledge graph.

### Architecture

```
                    SHACLService
                        │
         ┌──────────────┼──────────────┐
         ▼              ▼              ▼
   ┌───────────┐  ┌───────────┐  ┌───────────┐
   │  Shapes   │  │  Turtle   │  │ Validate  │
   │   CRUD    │  │ Round-Trip│  │ & Execute │
   └───────────┘  └───────────┘  └───────────┘
   create/update  SHACLGenerator  PySHACL (in-memory)
   delete/list    SHACLParser     shape_to_sql (SQL)
```

### Quality Categories

Shapes are organized into six data quality categories:

| Category | SHACL Constraints | Example |
|----------|-------------------|---------|
| **Completeness** | `sh:minCount` | Every entity must have a label |
| **Cardinality** | `sh:minCount`, `sh:maxCount` | At most 3 phone numbers per person |
| **Uniqueness** | `sh:hasValue`, `sh:in` | Entity IDs must be unique |
| **Consistency** | `sh:class`, `sh:node` | Relationship targets must be of the correct type |
| **Conformance** | `sh:pattern`, `sh:datatype` | Email must match regex, dates must be xsd:date |
| **Structural** | `sh:closed`, `sh:sparql` | No unexpected properties, custom SPARQL checks |

### Key Components

| File | Class | Purpose |
|------|-------|---------|
| `src/back/core/w3c/shacl/SHACLService.py` | `SHACLService` | Shape CRUD, legacy constraint migration, SQL compilation, PySHACL validation |
| `src/back/core/w3c/shacl/SHACLGenerator.py` | `SHACLGenerator` | Builds RDFLib graph of `sh:NodeShape` / `sh:PropertyShape` and serializes to Turtle |
| `src/back/core/w3c/shacl/SHACLParser.py` | `SHACLParser` | Parses Turtle (or other RDF formats) into internal shape dictionaries |

### Execution Modes

- **PySHACL validation** (`validate_graph`): Validates an RDF graph against shapes in-memory using the `pyshacl` library — returns conformance status, violation list, and report text
- **SQL compilation** (`shape_to_sql`): Compiles individual shapes into Spark SQL queries against the flat `(subject, predicate, object)` triple table — supports `sh:minCount`, `sh:maxCount`, `sh:pattern`, `sh:hasValue`, `sh:class`
- **In-memory evaluation** (`evaluate_shape_in_memory`): Lightweight evaluation for simple shapes without SQL execution
- **Legacy migration** (`migrate_legacy_constraints`): Converts existing OntoBricks constraint definitions to SHACL shape dictionaries

### UI Integration

- **Ontology → Data Quality** sidebar section: Define, edit, and manage SHACL shapes visually with category-based organization
- **Digital Twin → Data Quality** sidebar section: Run shapes against the triple store with violation reporting

---

## Technology Stack

### Backend

| Technology | Version | Purpose |
|------------|---------|---------|
| Python | 3.10+ | Core language |
| FastAPI | 0.109+ | Web framework |
| Uvicorn | latest | ASGI server |
| RDFLib | 7.0+ | RDF/OWL operations |
| owlrl | 7.0+ | OWL 2 RL forward-chaining reasoner (deductive closure on RDFLib graphs) |
| PySHACL | 0.26+ | W3C SHACL validator for RDFLib graphs (data quality shapes validation) |
| real_ladybug | 0.1+ | LadybugDB embedded graph database (Cypher-based triple store backend) |
| Databricks SQL Connector | 3.0+ | Database connectivity |
| MLflow | 2.19+ | Agent tracing, evaluation, and Databricks Agent Framework |
| FastMCP | 2.3+ | MCP server SDK for LLM tool integration |
| Strawberry GraphQL | 0.220+ | Auto-generated typed GraphQL schema from ontology |

### Frontend

| Technology | Version | Purpose |
|------------|---------|---------|
| Bootstrap | 5.3 | UI framework |
| Bootstrap Icons | 1.11 | Icon library |
| Sigma.js | 3.0.2 | Knowledge Graph visualization (WebGL) |
| Graphology | 0.26.0 | Graph data model and algorithms |
| D3.js | 7.x | Data-driven DOM manipulation |
| Grid.js | latest | Advanced data tables |
| OntoViz | 1.0 | Visual entity-relationship designer |
| Vanilla JavaScript | ES6+ | Client-side logic |

### External Standards

| Standard | Version | Purpose |
|----------|---------|---------|
| RDF | 1.1 | Data model |
| OWL | 2 | Ontology language |
| R2RML | W3C Rec | Mapping language |
| SPARQL | 1.1 | Query language |
| SWRL | W3C Sub | Rule language |
| SHACL | W3C Rec | Data quality validation |
| Turtle | 1.1 | RDF serialization |

---

## Logging

OntoBricks uses Python's standard `logging` module with `logging.config.dictConfig` for a structured, log4J-style configuration. All configuration lives under **`src/back/core/logging/`** (`LogManager.py` implements setup and logger naming).

### Architecture

| Component | Description |
|-----------|-------------|
| **`src/back/core/logging/`** | Package — `LogManager` builds and applies a `dictConfig`; `setup_logging` / `get_logger` are re-exported from `__init__.py` |
| **`setup_logging()`** | Called once at startup (in `run.py`) before the app is created |
| **`get_logger(name)`** | Helper to obtain a child logger under the `ontobricks` namespace |

### Loggers

| Logger | Purpose |
|--------|---------|
| `ontobricks` | Application logger — `back.*`, `front.*`, `shared.*`, and `api.*` modules use child loggers |
| `uvicorn` / `uvicorn.access` / `uvicorn.error` | HTTP server logs |
| `fastapi` | Framework logs |
| Root | Catch-all at WARNING level |

### Handlers

| Handler | Type | Target |
|---------|------|--------|
| `console` | `StreamHandler` | `stdout` — visible in Databricks App logs |
| `file` | `RotatingFileHandler` | Rotating log file (10 MB, 5 backups) |

The file handler writes to a path determined by (in priority order):
1. `LOG_DIR` environment variable
2. `/local_disk0/logs` when running inside Databricks Apps
3. `./logs` for local development

### Format

All log lines follow a detailed ISO-8601 format:

```
2026-02-17T14:30:05+0000 | INFO     | ontobricks.shared.fastapi.main | main.lifespan:95 | OntoBricks FastAPI starting
```

### Configuration

Logging is controlled via environment variables or Pydantic `Settings`:

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |
| `LOG_DIR` | *(auto)* | Directory for the rotating log file |
| `LOG_FILE` | `ontobricks.log` | Filename inside `LOG_DIR` |
| `LOG_FORMAT` | *(text)* | Set to `json` to emit structured JSON lines (one JSON object per log entry with `ts`, `level`, `logger`, `module`, `func`, `line`, `msg` fields) |

### Structured JSON Logging

Set `LOG_FORMAT=json` to switch both console and file handlers to JSON-line output. Each entry is a single JSON object:

```json
{"ts": "2026-04-19T10:30:05+00:00", "level": "INFO", "logger": "ontobricks.shared.fastapi.main", "module": "main", "func": "lifespan", "line": 95, "msg": "OntoBricks FastAPI starting"}
```

This mode is recommended for production deployments where logs are aggregated by external tools (e.g. Databricks log console, ELK, Datadog).

### Request Timing

The `RequestTimingMiddleware` (`shared/fastapi/timing.py`) logs `method`, `path`, `status_code`, and `duration_ms` for every non-static request. Combined with JSON logging, this provides per-endpoint latency visibility without external APM tooling.

### Usage in Modules

```python
from back.core.logging import get_logger

logger = get_logger(__name__)
logger.info("Processing %d triples", count)
```

---

## MLflow Observability

OntoBricks integrates [MLflow](https://mlflow.org/) for agent observability, evaluation, and compatibility with the Databricks Agent Framework.

### Tracing Architecture

```
┌─────────────────────────────────────────────────┐
│                 FastAPI Startup                   │
│   setup_tracing() → mlflow.set_experiment(...)   │
│                  mlflow.tracing.enable()          │
└──────────────────────┬──────────────────────────┘
                       │
      ┌────────────────┼────────────────┐
      ▼                ▼                ▼
 @trace_agent     @trace_llm      @trace_tool
 (run_agent)      (_call_llm)     (_execute_tool)
      │                │                │
      └────────────────┼────────────────┘
                       ▼
              MLflow Tracking Server
         (local mlflow.db or Databricks)
```

Every agent invocation produces a nested span tree:

| Span Type | Decorator | Captures |
|-----------|-----------|----------|
| **AGENT** | `@trace_agent` | Full run — inputs (secrets excluded), result status, iterations, token usage |
| **LLM** | `@trace_llm` | Each LLM call — endpoint, message count, finish reason, prompt/completion tokens |
| **TOOL** | `@trace_tool` | Each tool dispatch — tool name, arguments, result length |

### Tracking Destination

| Environment | Tracking URI | Experiment Path | Storage |
|-------------|-------------|-----------------|---------|
| **Local dev** (default) | *(not set)* | `ontobricks-agents` | `mlflow.db` + `mlruns/` on disk |
| **Local dev** (persistent) | `MLFLOW_TRACKING_URI=databricks` | `/Shared/ontobricks-agents` | Databricks workspace |
| **Databricks App** | `MLFLOW_TRACKING_URI=databricks` (set in `app.yaml`) | `/Shared/ontobricks-agents` | Databricks workspace |

When the tracking URI is `databricks`, experiment names are automatically resolved to absolute workspace paths (`/Shared/<name>`) so that traces are accessible from the workspace Experiments UI.

### Viewing Traces

In the Databricks workspace:
1. Navigate to **Machine Learning > Experiments**
2. Open the **`/Shared/ontobricks-agents`** experiment
3. Click any run, then the **Traces** tab to see the full span tree

### Databricks Agent Framework

The Ontology Assistant agent has a `ResponsesAgent` wrapper (`src/agents/agent_ontology_assistant/responses_agent.py`) that implements the MLflow [`ResponsesAgent`](https://mlflow.org/docs/latest/genai/serving/responses-agent) interface. This enables:

- **AI Playground** — interactive testing
- **Agent Evaluation** — quality measurement with LLM judges
- **Model Serving** — deployment as a managed endpoint
- **MLflow Model Logging** — versioning and tracking via `log_model.py`

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `MLFLOW_TRACKING_URI` | *(none)* | Set to `databricks` for persistent traces |
| `ONTOBRICKS_MLFLOW_EXPERIMENT` | `ontobricks-agents` | Experiment name (auto-prefixed with `/Shared/` on Databricks) |

Tracing degrades gracefully: if MLflow is not configured or the tracking server is unreachable, agents run normally without traces.

### Key Files

| File | Purpose |
|------|---------|
| `src/agents/tracing.py` | Setup, decorators (`trace_agent`, `trace_llm`, `trace_tool`), secret filtering |
| `src/agents/agent_ontology_assistant/responses_agent.py` | `ResponsesAgent` wrapper for the Databricks Agent Framework |
| `src/agents/agent_ontology_assistant/log_model.py` | Script to log the agent model to MLflow |
| `src/shared/fastapi/main.py` | Calls `setup_tracing()` at application startup |

---

## Performance Infrastructure

### SQL Connection Pooling

`SQLWarehouse` maintains a `queue.Queue`-based pool of reusable database connections (`src/back/core/databricks/SQLWarehouse.py`). Instead of opening a fresh `databricks.sql.connect()` per query (costly due to TLS handshakes), connections are borrowed from the pool and returned after use. Stale connections (idle > 300 s) are discarded automatically.

| Parameter | Default | Notes |
|-----------|---------|-------|
| Pool size | 8 | Max concurrent connections per warehouse |
| Max idle | 300 s | Connections older than this are replaced |

### Dedicated Thread Pool

All blocking Databricks I/O runs through `run_blocking()` in `DatabricksHelpers.py`, which dispatches to a dedicated `ThreadPoolExecutor` instead of the default asyncio pool. This prevents SQL latency from starving the event loop.

| Variable | Default | Description |
|----------|---------|-------------|
| `ONTOBRICKS_THREAD_POOL_SIZE` | `20` | Max workers for blocking I/O |

---

## Security Considerations

### Authentication
- Personal Access Token (development)
- Service Principal (production/Databricks Apps)
- Tokens stored in environment variables

### CSRF Protection
- Double-submit cookie pattern via `CSRFMiddleware` (`shared/fastapi/csrf.py`)
- A `csrf_token` cookie is set on first visit; state-changing requests (POST, PUT, PATCH, DELETE) must include the same value in an `X-CSRF-Token` header
- The browser-side `fetch()` wrapper in `utils.js` attaches the header automatically
- Bypass paths: `/static/`, `/health`, `/api/`, `/graphql/`, docs endpoints
- Disabled via `CSRF_DISABLED=1` for automated test suites

### Data Protection
- Session cookies use `secure=True` and `samesite=lax` when running as a Databricks App (`DATABRICKS_APP_PORT` set), ensuring cookies are only sent over HTTPS
- No credentials persisted to disk
- HTTPS enforced in production

### SQL Injection Prevention
- Parameterized queries via Databricks SQL Connector
- Read-only query validation for test queries

---

## External REST API

OntoBricks provides a stateless REST API at `/api/v1/` for external applications to:

### Available Endpoints

**Domain API** (`/api/v1/domains`, `/api/v1/domain/...`):

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/domains` | GET | List registry domains (MCP-enabled only) |
| `/api/v1/domain/versions` | GET | List versions for a named domain |
| `/api/v1/domain/design-status` | GET | Design status (ontology, metadata, mapping readiness) |
| `/api/v1/domain/ontology` | GET | Get domain OWL ontology (Turtle) |
| `/api/v1/domain/r2rml` | GET | Get R2RML mapping (Turtle) |
| `/api/v1/domain/sparksql` | GET | Get generated Spark SQL |

**Digital Twin API** (`/api/v1/digitaltwin/`):

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/digitaltwin/registry` | GET | Get registry configuration |
| `/api/v1/digitaltwin/status` | GET | Triple store status |
| `/api/v1/digitaltwin/stats` | GET | Triple store statistics |
| `/api/v1/digitaltwin/build` | POST | Trigger triple store build |
| `/api/v1/digitaltwin/triples/find` | GET | BFS entity search/traversal |

**Legacy v1 API**:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/health` | GET | Health check |
| `/api/v1/domains/list` | POST | List domains in Unity Catalog |
| `/api/v1/domain/info` | POST | Get domain metadata |
| `/api/v1/domain/ontology` | POST | Get ontology details |
| `/api/v1/domain/ontology/classes` | POST | Get ontology classes |
| `/api/v1/domain/ontology/properties` | POST | Get ontology properties |
| `/api/v1/domain/mappings` | POST | Get mapping details |
| `/api/v1/domain/r2rml` | POST | Get R2RML content |
| `/api/v1/query` | POST | Execute SPARQL query |
| `/api/v1/query/validate` | POST | Validate SPARQL syntax |
| `/api/v1/query/samples` | POST | Get sample queries |

**GraphQL API**:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/graphql` | GET | List GraphQL-enabled domains |
| `/graphql/settings/depth` | GET | GraphQL depth settings |
| `/graphql/{project_name}` | GET | GraphiQL playground |
| `/graphql/{project_name}` | POST | Execute GraphQL query |
| `/graphql/{project_name}/schema` | GET | SDL schema |

### Authentication

API endpoints accept Databricks credentials via:
- **Headers**: `X-Databricks-Host`, `X-Databricks-Token`
- **Request Body**: `databricks_host`, `databricks_token`

### Example Usage

```python
import requests

response = requests.post(
    "http://localhost:8000/api/v1/query",
    headers={
        "Content-Type": "application/json",
        "X-Databricks-Host": "https://workspace.databricks.com",
        "X-Databricks-Token": "dapi..."
    },
    json={
        "project_path": "/Volumes/catalog/schema/volume/project.json",
        "query": "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10"
    }
)
results = response.json()
```

See [API Documentation](api.md) for complete endpoint reference.

---

## Extension Points

1. **Additional Data Sources**: Implement new client classes in `src/back/core/databricks/`
2. **Custom R2RML Patterns**: Extend `R2RMLGenerator` in `src/back/core/w3c/r2rml/R2RMLGenerator.py`
3. **New Output Formats**: Add serializers in mapping or ontology modules
4. **Additional SPARQL Features**: Extend `SparqlTranslator` in `src/back/core/w3c/sparql/SparqlTranslator.py`
5. **Custom Visualizations**: Extend Sigma.js knowledge graph in query template
6. **Authentication Providers**: Add new auth methods in `DatabricksClient`
7. **OntoViz Extensions**: Add new entity/relationship types, custom rendering
8. **Triple Store Backends**: Implement `TripleStoreBackend` in `src/back/core/triplestore/` — two backends are already available (Delta, LadybugDB)
9. **Theming**: Modify OntoViz CSS variables for custom themes
10. **SWRL Built-ins**: Extend the SWRL engine (`src/back/core/reasoning/SWRLEngine.py`) with additional built-in atoms beyond class and property assertions (e.g., math, string, comparison built-ins)
11. **Reasoning Profiles**: Add new reasoning profiles beyond OWL 2 RL (e.g., OWL 2 EL) by implementing alternative reasoner classes in `src/back/core/reasoning/`
12. **Custom Constraint Types**: Add domain-specific constraint validators to the constraint checking phase in `ReasoningService`
13. **New LLM Agents**: Add agents under `src/agents/` using the shared tool framework (see [Agentic Architecture](architecture.md#agentic-architecture))
14. **New Agent Tools**: Add reusable tools in `src/agents/tools/` for agents to compose
15. **GraphQL Customization**: Extend the auto-generated schema in `src/back/core/graphql/` — add custom resolvers, DataLoader batching, or subscription support
16. **MCP Server Tools**: Add new MCP tools in `src/mcp-server/server/app.py` for additional knowledge-graph operations
17. **Industry Ontology Importers**: Extend FIBO, CDISC, IOF services in `src/back/core/industry/` for domain-specific ontology support

---

## References

### W3C Standards
- **RDF 1.1 Primer**: https://www.w3.org/TR/rdf11-primer/
- **OWL 2 Web Ontology Language**: https://www.w3.org/TR/owl2-overview/
- **OWL 2 Profiles (RL, EL, QL)**: https://www.w3.org/TR/owl2-profiles/
- **SWRL**: https://www.w3.org/submissions/SWRL/
- **SHACL**: https://www.w3.org/TR/shacl/
- **R2RML: RDB to RDF Mapping Language**: https://www.w3.org/TR/r2rml/
- **SPARQL 1.1 Query Language**: https://www.w3.org/TR/sparql11-query/

### Libraries & Tools
- **RDFLib Documentation**: https://rdflib.readthedocs.io/
- **owlrl (OWL 2 RL Reasoner)**: https://owl-rl.readthedocs.io/
- **PySHACL**: https://github.com/RDFLib/pySHACL
- **LadybugDB (real_ladybug)**: Embedded graph database with Cypher support — https://pypi.org/project/real-ladybug/
- **SANSA Stack (inspiration)**: https://github.com/SANSA-Stack
- **Databricks SQL Connector**: https://docs.databricks.com/dev-tools/python-sql-connector.html


---

## Component guides (merged)

The following sections were previously separate documents.
## Agentic Architecture

### Overview

OntoBricks uses **LLM-powered agents** to automate complex, multi-step tasks that would otherwise require significant manual effort. Each agent follows an MCP-style (Model Context Protocol) pattern: an autonomous loop where the LLM reasons about the task, calls tools to gather context or perform actions, and iterates until the goal is achieved.

All agents run against the **Databricks Foundation Model API** (or any OpenAI-compatible chat/completions endpoint) and are designed to degrade gracefully if the endpoint does not support function calling.

In addition to the UI-driven agents, OntoBricks provides an **MCP server** (`mcp-ontobricks`) that exposes knowledge-graph tools to LLM clients (Databricks Playground, Cursor, Claude Desktop) via the Model Context Protocol. The MCP server is a separate Databricks App that calls the main app's REST and GraphQL APIs. See [MCP Server](mcp.md) for details.

```
┌─────────────────────────────────────────────────────────┐
│                    FastAPI Route                         │
│  (creates task, spawns background thread)                │
├─────────────────────────────────────────────────────────┤
│                                                         │
│   ┌───────────────────────────────────────────────┐     │
│   │              Agent Engine                      │     │
│   │                                                │     │
│   │   System Prompt                                │     │
│   │       ↓                                        │     │
│   │   ┌──────────┐    tool_calls    ┌──────────┐  │     │
│   │   │   LLM    │ ───────────────→ │  Tools   │  │     │
│   │   │ (iterate)│ ←─────────────── │ (execute)│  │     │
│   │   └──────────┘   tool_results   └──────────┘  │     │
│   │       ↓                                        │     │
│   │   AgentResult                                  │     │
│   └───────────────────────────────────────────────┘     │
│                                                         │
│   TaskManager.complete_task(result)                      │
│   Frontend polls /tasks/{id} → applies result            │
└─────────────────────────────────────────────────────────┘
```

---

### Agents

#### 1. OWL Generator Agent (`agent_owl_generator`)

**Purpose**: Autonomously generate a complete OWL ontology (Turtle format) from domain metadata and uploaded documents.

| Parameter | Value |
|-----------|-------|
| Max iterations | 10 |
| LLM timeout | 180s |
| Max tokens | 4096 |
| Temperature | 0.1 |

**Workflow**:
1. Receives a user prompt describing the desired ontology
2. Calls `get_metadata` and `get_table_detail` to understand the data schema
3. Calls `list_documents` and `read_document` to ingest uploaded reference material
4. Generates OWL/Turtle output based on gathered context

**Tools used**: `get_metadata`, `get_table_detail`, `list_documents`, `read_document`

**Invoked by**: `POST /ontology/generate` → background thread → TaskManager

---

#### 2. Auto-Mapping Agent (`agent_auto_assignment`)

**Purpose**: Autonomously map ontology entities and relationships to SQL queries against the domain's Databricks tables. The agent writes SQL, validates it by executing queries, and submits the finalized mappings.

| Parameter | Value |
|-----------|-------|
| Max iterations | 60 (batch) / 15 (single-item) |
| LLM timeout | 180s |
| Max tokens | 2048 |
| Temperature | 0.1 |
| Iteration delay | 3s between LLM calls |
| Chunk size | 5 items per agent run (`AUTO_ASSIGN_CHUNK_SIZE`) |
| Chunk cooldown | 15s between chunks (`AUTO_ASSIGN_CHUNK_COOLDOWN`) |

**Workflow**:
1. Calls `get_ontology` to see entities, relationships, and their attributes
2. Calls `get_metadata` to understand available tables and columns
3. For each entity/relationship:
   - Writes a SQL query using `execute_sql` to validate it
   - Iterates on SQL errors until the query succeeds
   - Calls `submit_entity_mapping` or `submit_relationship_mapping` to finalize
4. Repeats until all items are mapped or iteration limit is reached

**Tools used**: `get_ontology`, `get_metadata`, `execute_sql`, `submit_entity_mapping`, `submit_relationship_mapping`

**Invoked by**:
- **Batch**: `POST /mapping/auto-assign/start` → background thread → TaskManager. Large jobs are split into chunks of `AUTO_ASSIGN_CHUNK_SIZE` items; each chunk runs its own agent loop with a `AUTO_ASSIGN_CHUNK_COOLDOWN` pause between chunks to avoid LLM rate limits (429 errors). Partial results accumulate across chunks.
- **Single-item**: `POST /mapping/auto-assign/single` → background thread → TaskManager (processes one entity or relationship)

**Single-item mode**: The same agent engine is used with `max_iterations=15`. The ontology payload is scoped to the single target item. The frontend fires the request, polls `/tasks/{id}`, and saves the result directly to `MappingState.config` by URI — enabling concurrent auto-maps on different items.

---

#### 3. Auto Icon Assign Agent (`agent_auto_icon_assign`)

**Purpose**: Choose visually representative emoji icons for each ontology entity by analyzing entity names, attributes, and data context.

| Parameter | Value |
|-----------|-------|
| Max iterations | 8 |
| LLM timeout | 120s |
| Max tokens | 2048 |
| Temperature | 0.3 |

**Workflow**:
1. Calls `get_ontology` to see all entities and their properties
2. Optionally calls `get_metadata` to understand what each entity represents
3. Selects an emoji for each entity and calls `assign_icons` with the full mapping

**Tools used**: `get_ontology`, `get_metadata`, `assign_icons`

**Invoked by**: `POST /ontology/auto-assign-icons` (synchronous, wrapped in `asyncio.to_thread`)

---

#### 4. Ontology Assistant (`agent_ontology_assistant`)

**Purpose**: Interactive conversational agent that can modify the domain ontology based on natural language instructions — add/remove entities, adjust relationships, clean orphans, and answer questions about the ontology.

| Parameter | Value |
|-----------|-------|
| Max iterations | 15 |
| LLM timeout | 180s |
| Max tokens | 4096 |
| Temperature | 0.1 |

**Workflow**:
1. Receives a user message (e.g., "Add an entity called Vehicle with attributes: plate, color")
2. Calls `get_ontology` to understand the current ontology state
3. Modifies the ontology (adds/removes entities, relationships, properties)
4. Returns the updated ontology for the frontend to apply

**Tools used**: `get_ontology`, plus ontology mutation functions built into the engine

**Invoked by**: `POST /ontology/assistant/invoke` (synchronous, wrapped in `asyncio.to_thread`)

**Databricks Agent Framework**: This agent has a `ResponsesAgent` wrapper (`responses_agent.py`) that implements the MLflow `ResponsesAgent` interface, enabling AI Playground testing, Agent Evaluation, Model Serving deployment, and MLflow model logging (see the MLflow section in [Architecture](architecture.md)).

---

### Shared Tools

All tools live in `src/agents/tools/` and follow a consistent pattern:

- **Definition**: An OpenAI function-calling JSON schema (`TOOL_DEFINITIONS` list)
- **Handler**: A Python function that receives `ToolContext` as its first argument (`TOOL_HANDLERS` dict)
- **Composability**: Each agent's `tools.py` assembles only the tools it needs

#### Tool Catalog

| Tool | Module | Description | Used By |
|------|--------|-------------|---------|
| `get_metadata` | `metadata.py` | Returns domain table schemas (names, columns, types) | All agents |
| `get_table_detail` | `metadata.py` | Returns detailed schema for a specific table | OWL Generator |
| `list_documents` | `documents.py` | Lists uploaded domain documents from Unity Catalog | OWL Generator |
| `read_document` | `documents.py` | Reads content of a specific document | OWL Generator |
| `get_ontology` | `ontology.py` | Returns current ontology (entities, relationships, attributes) | Auto-Mapping, Icon Mapping, Ontology Assistant |
| `execute_sql` | `sql.py` | Executes a SQL query via Databricks SQL Warehouse | Auto-Mapping |
| `submit_entity_mapping` | `mapping.py` | Saves a validated entity → SQL mapping | Auto-Mapping |
| `submit_relationship_mapping` | `mapping.py` | Saves a validated relationship → SQL mapping | Auto-Mapping |
| `assign_icons` | `icons.py` | Saves entity → emoji icon mapping | Icon Mapping |

#### ToolContext

The `ToolContext` dataclass (`tools/context.py`) provides shared runtime state to all tools:

```python
@dataclass
class ToolContext:
    # Common (all agents)
    host: str                # Databricks workspace URL
    token: str               # Databricks access token
    metadata: dict           # Domain table metadata

    # OWL Generator
    uc_location: dict        # Unity Catalog file location

    # Auto-Mapping
    client: Any              # DatabricksClient for SQL execution
    ontology: dict           # Current ontology data
    entity_mappings: list    # Accumulated entity mapping results
    relationship_mappings: list  # Accumulated relationship mapping results

    # Icon Assign
    icon_results: dict       # Accumulated icon assignments
```

Each agent populates only the fields it needs; unused fields remain at their defaults.

---

### Agent Engine Pattern

All three agents share the same engine structure (defined independently in each `engine.py`):

#### Core Loop

```
1. Build messages = [system_prompt, user_prompt]
2. For iteration in 1..MAX_ITERATIONS:
   a. Call LLM with messages + tool_definitions
   b. If response contains tool_calls:
      - Execute each tool via TOOL_HANDLERS
      - Append tool results to messages
      - Continue loop
   c. If response is plain text (no tool_calls):
      - Extract final output
      - Break
3. Build AgentResult from accumulated state
```

#### Key Functions

| Function | Description |
|----------|-------------|
| `run_agent(...)` | Public entry point — sets up context, runs the loop, returns `AgentResult` |
| `_call_llm(...)` | HTTP POST to Databricks Foundation Model API |
| `_execute_tool(...)` | Dispatches a tool call to the appropriate handler |
| `_extract_content(...)` | Extracts text from LLM response (handles different response formats) |

#### Fallback Mode

If the LLM endpoint returns HTTP 400/422 (indicating it doesn't support the `tools` parameter), the OWL Generator and Icon Assign agents automatically retry without tools, falling back to single-shot generation. The Auto-Mapping agent does not fall back because its workflow fundamentally requires tool calls (SQL execution, mapping submission).

#### Task Integration

Agents are invoked from FastAPI routes via background threads:

```python
## Route handler
tm = get_task_manager()
task = tm.create_task(name="...", task_type="...", steps=[...])

def _run():
    tm.start_task(task.id)
    result = run_agent(...)
    tm.complete_task(task.id, result=result)

thread = threading.Thread(target=_run, daemon=True)
thread.start()
return {"success": True, "task_id": task.id}
```

The frontend polls `GET /tasks/{task_id}` until the task completes, then applies the result.

---

### Adding a New Agent

1. **Create the agent directory**: `src/agents/agent_<name>/`
2. **Define tools** in `src/agents/tools/` (reuse existing tools where possible)
3. **Create `tools.py`**: Assemble `TOOL_DEFINITIONS` and `TOOL_HANDLERS` from shared tools
4. **Create `engine.py`**: Implement `run_agent()` with a system prompt and the agentic loop
5. **Create `__init__.py`**: Export `run_agent` and `AgentResult`
6. **Wire the route**: Create a FastAPI endpoint that spawns the agent in a background thread via TaskManager
7. **Wire the frontend**: Call the endpoint, poll `/tasks/{id}`, and apply the result

#### Tool Authoring Convention

Each tool module exports:

```python
## Handler function
def tool_<name>(ctx: ToolContext, **kwargs) -> str:
    """Must return a JSON string."""
    ...

## OpenAI function-calling definition
<NAME>_TOOL_DEFINITIONS: List[dict] = [{ "type": "function", "function": {...} }]

## Name → handler mapping
<NAME>_TOOL_HANDLERS: Dict[str, Callable] = { "<name>": tool_<name> }
```

---

### Architecture Decisions

| Decision | Rationale |
|----------|-----------|
| Agents run in background threads | Keeps the FastAPI event loop responsive; users can continue working |
| Fire-and-forget with polling | Enables concurrent auto-maps on different entities |
| URI-keyed saves | Results are saved by target URI, not by current panel state — avoids race conditions when the user navigates during processing |
| Shared ToolContext | Avoids passing many arguments; each agent uses only the fields it needs |
| Tools return JSON strings | Consistent interface for the LLM to parse; easy to log and debug |
| Per-agent tool assembly | Each agent composes only the tools it needs — keeps prompt token usage minimal |
| Graceful fallback | Agents degrade to single-shot when tool calling is unavailable |

---

### MLflow Tracing

All agents are instrumented with [MLflow tracing](https://mlflow.org/docs/latest/genai/tracing/) for observability, evaluation, and monitoring. Tracing is initialised at application startup in `src/shared/fastapi/main.py` and creates spans for:

| Span type | Decorator | Captures |
|-----------|-----------|----------|
| **AGENT** | `@trace_agent` | Full agent run — inputs (excluding secrets), result status, iterations, usage |
| **LLM** | `@trace_llm` | Each LLM call — endpoint, message count, finish reason, token usage |
| **TOOL** | `@trace_tool` | Each tool dispatch — tool name, arguments, result length |

#### Configuration

| Env Variable | Default | Description |
|---|---|---|
| `MLFLOW_TRACKING_URI` | *(none)* | Set to `databricks` to persist traces to the workspace tracking server |
| `ONTOBRICKS_MLFLOW_EXPERIMENT` | `ontobricks-agents` | MLflow experiment name for traces |

When `MLFLOW_TRACKING_URI=databricks`, relative experiment names are automatically resolved to `/Shared/<name>` so they appear under **Machine Learning > Experiments** in the Databricks workspace. This is configured in `app.yaml` for production deployments.

Tracing degrades gracefully: if MLflow is not configured or the tracking server is unreachable, agents run normally without traces.

#### Tracing module

The shared tracing utilities live in `src/agents/tracing.py`:

- `setup_tracing(experiment_name)` — call once at startup
- `trace_agent(name)`, `trace_llm(name)`, `trace_tool(name)` — decorators
- Secrets (`token`, `host`, `client`) are excluded from span inputs automatically

---

### Databricks Agent Framework Integration

#### Ontology Assistant — ResponsesAgent

The Ontology Assistant has a wrapper that implements the MLflow
[`ResponsesAgent`](https://mlflow.org/docs/latest/genai/serving/responses-agent)
interface, making it compatible with:

- **AI Playground** — test the agent interactively
- **Agent Evaluation** — measure quality with LLM judges
- **Model Serving** — deploy as a managed endpoint
- **MLflow logging** — version and track agent models

##### Files

| File | Purpose |
|------|---------|
| `src/agents/agent_ontology_assistant/responses_agent.py` | `OntologyAssistantResponsesAgent` class |
| `src/agents/agent_ontology_assistant/log_model.py` | Script to log the agent to MLflow |

##### Usage — In-process (current FastAPI route)

```
POST /ontology/assistant/invoke
Content-Type: application/json

{
    "input": [
        {"role": "user", "content": "Add an entity called Vehicle"}
    ]
}
```

The route automatically fills `custom_inputs` (host, token, endpoint, ontology) from the active session. If the ontology is modified, the domain is saved.

##### Usage — Log to MLflow

```bash
python -m agents.agent_ontology_assistant.log_model
```

This creates an MLflow run with the agent model, which can then be registered in Unity Catalog and served via Databricks Model Serving.

##### Custom inputs / outputs

| Field | Direction | Contents |
|-------|-----------|----------|
| `custom_inputs.host` | in | Databricks workspace URL |
| `custom_inputs.token` | in | Databricks access token |
| `custom_inputs.endpoint_name` | in | Foundation Model API serving endpoint |
| `custom_inputs.classes` | in | Current ontology classes (list of dicts) |
| `custom_inputs.properties` | in | Current ontology properties (list of dicts) |
| `custom_inputs.base_uri` | in | Ontology base URI |
| `custom_outputs.success` | out | Whether the agent completed successfully |
| `custom_outputs.ontology_changed` | out | Whether any mutations were applied |
| `custom_outputs.classes` | out | Mutated classes (when changed) |
| `custom_outputs.properties` | out | Mutated properties (when changed) |## OntoViz - Visual Ontology Editor

OntoViz is a custom JavaScript library for visual entity-relationship diagram editing, integrated into OntoBricks for ontology design. It is **reusable** and can be integrated into other projects.

### Overview

OntoViz provides a visual canvas for creating and managing ontology structures with:
- **Entities** (OWL Classes) - Represent concepts in your domain
- **Relationships** (OWL Object Properties) - Connect entities with directed links
- **Inheritances** (rdfs:subClassOf) - Define class hierarchies with property inheritance

![OntoViz Editor](screenshots/Ontology%20design.png)

---

### Features

#### Entity Management

Entities represent ontology classes (`owl:Class`) and are displayed as interactive boxes on the canvas.

| Feature | Description |
|---------|-------------|
| **Name** | Editable entity name (click to edit) |
| **Icon** | Customizable emoji icon for visual identification |
| **Description** | Optional text description |
| **Attributes** | Data properties with name and type |
| **4 Anchors** | Connection points (top, bottom, left, right) for relationships |

##### Creating Entities

1. Click the **+ Add Entity** button in the toolbar
2. A new entity appears on the canvas
3. Click on the entity name to rename it
4. Use the **+** button on the entity to add attributes
5. Use the **🎨** button to select an icon
6. Use the **📝** button to add a description

##### Entity Properties

Each entity can have multiple data properties (attributes):

```json
{
  "id": "entity_123",
  "name": "Person",
  "icon": "👤",
  "description": "Represents a person",
  "properties": [
    { "id": "prop_1", "name": "email", "type": "string" },
    { "id": "prop_2", "name": "age", "type": "integer" }
  ],
  "x": 100,
  "y": 150
}
```

---

#### Relationship Management

Relationships represent object properties (`owl:ObjectProperty`) that connect entities.

| Feature | Description |
|---------|-------------|
| **Name** | Editable relationship name (click on label) |
| **Direction** | Forward (→), Reverse (←), or Bidirectional (↔) |
| **Attributes** | Optional relationship properties |
| **Visual** | Solid line with arrow indicating direction |

##### Creating Relationships

1. Click and drag from one entity's anchor (○) to another entity
2. A relationship line is created with a label box in the middle
3. Click the label to rename the relationship
4. Click the direction button (→/←/↔) to change direction
5. Use the **+** button on the relationship box to add attributes

##### Relationship Direction

Each relationship has a direction that controls:
- **Forward (→)**: Domain → Range (e.g., Person → Department)
- **Reverse (←)**: Range → Domain (e.g., Department ← Person)
- **Bidirectional (↔)**: Both directions

Click the direction indicator on the relationship box to cycle through options.

##### Self-Loop Relationships

OntoViz supports recursive relationships (entity linked to itself):
- Displayed as curved quarter-circle arcs
- Relationship box can be positioned around the entity
- Anchors adjust automatically based on box position

---

#### Inheritance Links

Inheritance links represent class hierarchies using `rdfs:subClassOf`.

| Feature | Description |
|---------|-------------|
| **Visual Style** | Dotted line with hollow triangle arrow |
| **Direction** | From parent class to child class |
| **Property Inheritance** | Child automatically inherits parent's attributes |
| **Read-Only Inherited** | Inherited properties shown as non-editable in child |

##### Creating Inheritance

1. Click the **△ Inheritance** button in the toolbar to enter inheritance mode
2. Drag from the parent entity's connector to the child entity
3. A dotted line with hollow arrow appears
4. Click the arrow to reverse direction if needed

##### Property Inheritance

When an inheritance link is created:
- Child entities automatically display parent's properties
- Inherited properties are shown with a visual indicator (read-only)
- Changes to parent properties cascade to all children
- Children can have additional properties beyond inherited ones

**Example:**
```
Person (parent)
├── name: string
└── email: string

Employee (child, inherits from Person)
├── name: string (inherited, read-only)
├── email: string (inherited, read-only)
├── employeeId: string (own property)
└── salary: decimal (own property)
```

---

### Canvas Controls

#### Navigation

| Control | Action |
|---------|--------|
| **Scroll Wheel** | Zoom in/out |
| **Click + Drag (background)** | Pan the canvas |
| **Click + Drag (entity)** | Move entity |

#### Toolbar

| Button | Function |
|--------|----------|
| **+ Add Entity** | Create a new entity |
| **△ Inheritance** | Toggle inheritance creation mode |
| **Grid Layout** | Auto-arrange entities in a grid |
| **Center** | Fit all entities in view |
| **Minimap** | Toggle navigation minimap |

#### Layout Features

##### Auto-Layout

Click the **Grid Layout** button to automatically organize entities:
- Uses force-directed algorithm to minimize overlaps
- Places connected entities near each other
- Maintains relationship visibility

##### Center View

Click **Center** to:
- Fit all entities within the visible canvas
- Auto-zoom to show the complete diagram
- Useful after loading a large diagram

##### Minimap

Toggle the minimap for:
- Overview of the entire diagram
- Quick navigation to different areas
- Visual indicator of current viewport

---

### Data Serialization

OntoViz supports JSON import/export for persistence.

#### Export Format

```json
{
  "entities": [
    {
      "id": "entity_1",
      "name": "Person",
      "icon": "👤",
      "description": "A human being",
      "x": 100,
      "y": 100,
      "properties": [
        { "id": "prop_1", "name": "name", "type": "string" },
        { "id": "prop_2", "name": "email", "type": "string" }
      ]
    },
    {
      "id": "entity_2",
      "name": "Department",
      "icon": "🏢",
      "x": 400,
      "y": 100,
      "properties": [
        { "id": "prop_3", "name": "departmentName", "type": "string" }
      ]
    }
  ],
  "relationships": [
    {
      "id": "rel_1",
      "name": "worksIn",
      "sourceEntityId": "entity_1",
      "targetEntityId": "entity_2",
      "direction": "forward",
      "properties": [
        { "id": "attr_1", "name": "startDate", "type": "date" }
      ]
    }
  ],
  "inheritances": [
    {
      "id": "inh_1",
      "sourceEntityId": "parent_entity_id",
      "targetEntityId": "child_entity_id"
    }
  ],
  "positions": {}
}
```

---

### Standalone Usage

OntoViz can be used independently of OntoBricks. See the demo at `src/front/static/global/ontoviz/index.html`.

#### Basic Integration

```html
<!-- Include OntoViz CSS -->
<link rel="stylesheet" href="ontoviz/css/ontoviz.css">

<!-- Container for the editor -->
<div id="ontoviz-container" style="width: 100%; height: 600px;"></div>

<!-- Include OntoViz JS -->
<script src="ontoviz/ontoviz.js"></script>

<script>
// Initialize OntoViz
const canvas = new OntoViz(document.getElementById('ontoviz-container'), {
    showToolbar: true,
    showMinimap: true,
    snapToGrid: true,
    gridSize: 20
});

// Add entities
const person = canvas.addEntity({ 
    name: 'Person', 
    x: 100, 
    y: 100, 
    icon: '👤',
    properties: [{ name: 'name', type: 'string' }] 
});

const dept = canvas.addEntity({ 
    name: 'Department', 
    x: 400, 
    y: 100, 
    icon: '🏢' 
});

// Add relationship
canvas.addRelationship({ 
    name: 'worksIn',
    sourceEntityId: person.id, 
    targetEntityId: dept.id,
    direction: 'forward'
});

// Export data
const data = canvas.toJSON();
console.log(JSON.stringify(data, null, 2));
</script>
```

#### Configuration Options

```javascript
new OntoViz(container, {
    // Display
    showToolbar: true,      // Show the toolbar
    showMinimap: true,      // Show navigation minimap
    
    // Grid
    snapToGrid: true,       // Snap entities to grid
    gridSize: 20,           // Grid cell size in pixels
    
    // Behavior
    autoLayout: false,      // Auto-layout on load
    readOnly: false,        // Disable editing
    
    // Styling
    defaultEntityIcon: '📦' // Default icon for new entities
});
```

#### Event Callbacks

OntoViz provides callbacks for integration with external systems:

```javascript
new OntoViz(container, {
    // Entity events
    onEntityCreate: (entity) => {
        console.log('Entity created:', entity.name);
    },
    onEntityUpdate: (entity) => {
        console.log('Entity updated:', entity.name);
    },
    onEntityDelete: (entity) => {
        console.log('Entity deleted:', entity.name);
    },
    
    // Relationship events
    onRelationshipCreate: (relationship) => {
        console.log('Relationship created:', relationship.name);
    },
    onRelationshipUpdate: (relationship) => {
        console.log('Relationship updated:', relationship.name);
    },
    onRelationshipDelete: (relationship) => {
        console.log('Relationship deleted:', relationship.name);
    },
    
    // Inheritance events
    onInheritanceCreate: (inheritance) => {
        console.log('Inheritance created');
    },
    onInheritanceDelete: (inheritance) => {
        console.log('Inheritance deleted');
    },
    
    // Selection events
    onSelectionChange: (selection) => {
        console.log('Selection changed:', selection);
    }
});
```

---

### API Reference

#### OntoViz Class

##### Constructor

```javascript
const canvas = new OntoViz(container, options);
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `container` | HTMLElement | DOM element to render into |
| `options` | Object | Configuration options |

##### Entity Methods

| Method | Description |
|--------|-------------|
| `addEntity(options)` | Create and add a new entity |
| `updateEntity(id, updates)` | Update entity properties |
| `removeEntity(id)` | Delete an entity (and its relationships) |
| `getEntity(id)` | Get entity by ID |
| `getEntities()` | Get all entities |

##### Relationship Methods

| Method | Description |
|--------|-------------|
| `addRelationship(options)` | Create a relationship between entities |
| `updateRelationship(id, updates)` | Update relationship properties |
| `removeRelationship(id)` | Delete a relationship |
| `getRelationship(id)` | Get relationship by ID |
| `getRelationships()` | Get all relationships |

##### Inheritance Methods

| Method | Description |
|--------|-------------|
| `addInheritance(options)` | Create inheritance link |
| `updateInheritance(id, updates)` | Update inheritance |
| `removeInheritance(id)` | Delete inheritance link |
| `getInheritedProperties(entityId)` | Get inherited properties for an entity |

##### Layout Methods

| Method | Description |
|--------|-------------|
| `autoLayoutGrid(options)` | Arrange entities in a grid |
| `centerDiagram(options)` | Center and fit diagram in view |
| `zoomToFit()` | Zoom to show all content |

##### Serialization Methods

| Method | Description |
|--------|-------------|
| `toJSON()` | Export diagram as JSON |
| `fromJSON(data)` | Import diagram from JSON |
| `clear()` | Clear all content |

---

### OWL Generation

When used with OntoBricks, OntoViz generates W3C-compliant OWL:

#### Entity → owl:Class

```turtle
:Person a owl:Class ;
    rdfs:label "Person" ;
    rdfs:comment "A human being" .

:name a owl:DatatypeProperty ;
    rdfs:domain :Person ;
    rdfs:range xsd:string .
```

#### Relationship → owl:ObjectProperty

```turtle
:worksIn a owl:ObjectProperty ;
    rdfs:domain :Person ;
    rdfs:range :Department .
```

#### Inheritance → rdfs:subClassOf

```turtle
:Employee a owl:Class ;
    rdfs:subClassOf :Person ;
    rdfs:label "Employee" .
```

---

### Naming Conventions

Entity, relationship, and property names must follow these rules:

| Rule | Description |
|------|-------------|
| **Characters** | Letters, numbers, underscores (`_`), hyphens (`-`) |
| **No Spaces** | Use underscores or CamelCase instead |
| **No Symbols** | Special characters are not allowed |
| **Case Sensitive** | `Person` and `person` are different |

**Recommended Conventions:**
- **Entities**: PascalCase (e.g., `Person`, `CustomerOrder`)
- **Relationships**: camelCase (e.g., `worksIn`, `hasOrder`)
- **Properties**: camelCase (e.g., `firstName`, `orderDate`)

---

### Files Structure

```
src/front/static/global/ontoviz/
├── ontoviz.js              # Main library code
├── index.html              # Standalone demo page
├── ontoviz_instructions.txt # Feature specifications
└── css/
    ├── ontoviz.css         # Main entry (imports all)
    ├── ontoviz-variables.css # Design tokens / CSS variables
    ├── ontoviz-entity.css  # Entity styling
    ├── ontoviz-relationship.css # Relationship & inheritance styling
    └── ontoviz-ui.css      # UI components (toolbar, minimap)
```

---

### Theming

OntoViz uses CSS variables for theming. Customize by overriding these variables:

```css
:root {
    /* Entity colors */
    --ontoviz-entity-bg: #ffffff;
    --ontoviz-entity-border: #dee2e6;
    --ontoviz-entity-header-bg: #f8f9fa;
    
    /* Relationship colors */
    --ontoviz-relationship-line: #6c757d;
    --ontoviz-relationship-arrow: #6c757d;
    
    /* Inheritance colors */
    --ontoviz-inheritance-line: #adb5bd;
    
    /* Canvas */
    --ontoviz-canvas-bg: #f5f5f5;
    --ontoviz-grid-color: #e0e0e0;
}
```

---

### Browser Support

OntoViz works in all modern browsers:
- Chrome 80+
- Firefox 75+
- Safari 13+
- Edge 80+

No external dependencies required (uses vanilla JavaScript and CSS).

---

### License

MIT License - OntoViz is open source and can be used in commercial projects.

---

### See Also

- [User Guide](user-guide.md) — consolidated usage guide
- [Getting Started](get-started.md) — installation and configuration

---

### Incremental LadybugDB Sync — Design Spec

**Date:** 2026-04-02
**Scope:** Source data changes only (ontology/mapping changes still trigger full rebuilds)

### Problem

The Digital Twin build always performs a full rebuild: drops the entire LadybugDB
graph, re-reads all triples from the Unity Catalog VIEW, and re-inserts everything.
For large knowledge graphs this is expensive in SQL Warehouse compute, wall-clock
time, and graph-DB write I/O — even when only a handful of source rows changed.

### Solution Overview

A two-phase incremental sync that:

1. **Version Gate** — cheaply detects whether any source Delta table has changed
   since the last build. If nothing changed, the build is skipped entirely.
2. **Server-Side Diff via Delta Snapshot Table** — when changes are detected,
   computes the diff (additions + removals) server-side on the SQL Warehouse
   using `EXCEPT` queries against a persisted snapshot, then applies only the
   delta to LadybugDB.

#### Flow

```
Sync triggered (manual or scheduled)
        │
        ▼
Phase 0: Mode Check
   ontology/mapping changed since last build?
   ──yes──▶ FULL rebuild (existing behavior)
        │ no
        ▼
Phase 1: Version Gate
   DESCRIBE HISTORY on each source table
   compare versions with stored versions
   ──all match──▶ SKIP (nothing changed)
        │ at least one changed
        ▼
Phase 2: Refresh VIEW
   CREATE OR REPLACE VIEW (same as today)
        │
        ▼
Phase 3: Server-Side Diff via SQL
   Snapshot table exists?
   ──no──▶ first build → full load + create snapshot
   ──yes──▶ compute diff via EXCEPT
        │ diff computed
        ▼
Phase 4: Apply Delta to LadybugDB
   Insert new triples
   Delete removed triples
        │
        ▼
Phase 5: Update State
   Overwrite snapshot with current VIEW
   Store new source table versions
   Stamp last_build timestamp
```

### Detailed Design

#### 1. Snapshot Table

A managed Delta table in the same catalog/schema as the VIEW — both of which
are always the registry `catalog.schema` for the active domain:

- **Name:** `_ob_snapshot_{safe_domain}_v{version}` (derived; never stored)
- **Schema:** `(subject STRING, predicate STRING, object STRING)`
- **Lifecycle:** Created on first incremental build; dropped on domain delete
  or forced full rebuild.

Diff queries (executed server-side on SQL Warehouse):

```sql
-- Triples to ADD
SELECT subject, predicate, object
FROM {view_table}
EXCEPT
SELECT subject, predicate, object
FROM {snapshot_table}

-- Triples to REMOVE
SELECT subject, predicate, object
FROM {snapshot_table}
EXCEPT
SELECT subject, predicate, object
FROM {view_table}
```

Snapshot refresh after successful apply:

```sql
CREATE OR REPLACE TABLE {snapshot_table}
AS SELECT subject, predicate, object FROM {view_table}
```

#### 2. Source Table Version Tracking

Extract the list of source tables from R2RML entity mappings (`sql_query` fields).
For each source table:

```sql
DESCRIBE HISTORY {table} LIMIT 1
```

Returns the latest `version` number (monotonically increasing). Stored as:

```json
"domain.triplestore.source_versions": {
  "catalog.schema.table_a": 42,
  "catalog.schema.table_b": 17
}
```

The snapshot table name is not stored; it is computed on the fly from the
registry catalog/schema, the domain name and the current version
(`{registry.catalog}.{registry.schema}._ob_snapshot_<domain>_v<version>`).

#### 3. New Components

##### `src/back/core/triplestore/IncrementalBuildService.py`

```python
class IncrementalBuildService:
    """Manages incremental sync: version gate, diff, and apply."""

    def __init__(self, client, settings)

    def extract_source_tables(self, entity_mappings, relationship_mappings) -> list[str]
    def check_source_versions(self, source_tables, stored_versions) -> (changed, new_versions)
    def snapshot_table_name(self, project_name, delta_cfg) -> str
    def snapshot_exists(self, snapshot_table) -> bool
    def compute_diff(self, view_table, snapshot_table) -> (to_add, to_remove)
    def refresh_snapshot(self, view_table, snapshot_table) -> None
    def create_snapshot(self, view_table, snapshot_table) -> None
    def drop_snapshot(self, snapshot_table) -> None
```

##### New abstract method on `TripleStoreBackend`

```python
def delete_triples(self, table_name, triples, batch_size=500, on_progress=None) -> int
```

**Flat model:** `MATCH (t:Node {subject: ..., predicate: ..., object: ...}) DELETE t`
**Graph model:** Classify triples (same as insert), then:
- Node removals: `MATCH (n:Table {uri: '...'}) DETACH DELETE n`
- Relationship removals: `MATCH (a)-[r:RelTable]->(b) WHERE a.uri = ... AND b.uri = ... DELETE r`
- Attribute removals: `MATCH (n:Table {uri: '...'}) SET n.col = NULL`

#### 4. Modified Files

- **`src/back/objects/session/DomainSession.py`** — add `source_versions` to `domain.triplestore`
  in the empty domain template and expose `snapshot_table`/`delta` as computed read-only
  properties derived from the registry.
- **`TripleStoreBackend.py`** — add abstract `delete_triples` method.
- **`LadybugFlatStore.py`** — implement `delete_triples` for flat model.
- **`LadybugGraphStore.py`** — implement `delete_triples` for graph model.
- **`src/api/routers/internal/dtwin.py`** — `start_triplestore_sync` gains `incremental` mode (delegating to `IncrementalBuildService` when conditions are met); extend `/sync/status`, `/sync/changes`, and `/sync/dt-existence` with incremental metadata.
- **`src/back/objects/registry/scheduler.py`** — `_run_scheduled_build` uses incremental path.
- **`src/front/templates/partials/dtwin/_query_sync.html`** — add snapshot info in Triple-Store Digital Twin card.
- **`src/front/static/query/js/query-sync.js`** — render snapshot existence/status.

#### 5. Sync Options (UI)

The existing "Replace existing data" checkbox is replaced by a mode selector:

- **Incremental** (default) — version gate + diff
- **Full rebuild** — drop + recreate (existing behavior, equivalent to
  `drop_existing=True`)

#### 6. Fallback Rules

Incremental sync gracefully falls back to full rebuild when:

- Ontology or mappings changed since last build
- Snapshot table doesn't exist (first build)
- User explicitly requests full rebuild
- Diff exceeds a threshold (>80% of total triples changed)
- Any error during incremental path

#### 7. Digital Twin Info Page

The Triple-Store Digital Twin card gains a new line below the VIEW name:

```
Unity Catalog VIEW
  catalog.schema.view_name           ✓ Exists

Incremental Snapshot
  catalog.schema._ob_snapshot_xxx    ✓ Exists  |  [Drop]
```

The `[Drop]` button forces the next build to be a full rebuild.

#### 8. Build Result Enrichment

The sync result includes incremental metadata:

```json
{
  "triple_count": 50000,
  "build_mode": "incremental",
  "diff": { "added": 120, "removed": 15 },
  "skipped_reason": null,
  "duration_seconds": 4.2
}
```

Or when skipped:

```json
{
  "triple_count": 0,
  "build_mode": "skipped",
  "skipped_reason": "No source table changes detected",
  "duration_seconds": 0.8
}
```
