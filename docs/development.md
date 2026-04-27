# Development

Notes for contributors: dependencies, testing, and permission / SDK notes.

## OntoBricks Dependencies

This document describes all external dependencies used by OntoBricks, including Python packages, UI frameworks, and JavaScript libraries.

---

### Python Dependencies

#### Runtime Dependencies

| Package | Version | Description | License | Link |
|---------|---------|-------------|---------|------|
| **FastAPI** | ≥0.109.0 | Modern, fast web framework for building APIs with Python | MIT | [fastapi.tiangolo.com](https://fastapi.tiangolo.com/) |
| **Uvicorn** | ≥0.27.0 | Lightning-fast ASGI server implementation (with `standard` extras) | BSD-3-Clause | [uvicorn.org](https://uvicorn.org/) |
| **Starlette** | ≥0.35.0 | Lightweight ASGI toolkit (FastAPI dependency, pinned for middleware features) | BSD-3-Clause | [starlette.io](https://www.starlette.io/) |
| **Jinja2** | ≥3.1.0 | Template engine for Python | BSD-3-Clause | [jinja.palletsprojects.com](https://jinja.palletsprojects.com/) |
| **pydantic** | ≥2.5.0 | Data validation using Python type hints | MIT | [docs.pydantic.dev](https://docs.pydantic.dev/) |
| **pydantic-settings** | ≥2.1.0 | Settings management with environment variable loading (BaseSettings) | MIT | [docs.pydantic.dev/latest/concepts/pydantic_settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) |
| **python-multipart** | ≥0.0.6 | Multipart form data parsing for file uploads in FastAPI | Apache-2.0 | [pypi.org/project/python-multipart](https://pypi.org/project/python-multipart/) |
| **itsdangerous** | ≥2.1.0 | Cryptographic signing utilities (available for session security) | BSD-3-Clause | [palletsprojects.com/p/itsdangerous](https://palletsprojects.com/p/itsdangerous/) |
| **aiofiles** | ≥23.0.0 | Async file I/O for Starlette static file serving | Apache-2.0 | [pypi.org/project/aiofiles](https://pypi.org/project/aiofiles/) |
| **databricks-sql-connector** | ≥3.0.0 | Python SQL connector for Databricks SQL Warehouses | Apache-2.0 | [pypi.org/project/databricks-sql-connector](https://pypi.org/project/databricks-sql-connector/) |
| **databricks-sdk** | ≥0.20.0 | Databricks SDK for workspace API, OAuth authentication, and auto-auth in App mode | Apache-2.0 | [pypi.org/project/databricks-sdk](https://pypi.org/project/databricks-sdk/) |
| **pyarrow** | ≥14.0.0 | Columnar in-memory data format (required by databricks-sql-connector) | Apache-2.0 | [arrow.apache.org/docs/python](https://arrow.apache.org/docs/python/) |
| **RDFLib** | ≥7.6.0 | Python library for working with RDF (Resource Description Framework) | BSD-3-Clause | [rdflib.readthedocs.io](https://rdflib.readthedocs.io/) |
| **python-dotenv** | ≥1.0.0 | Load environment variables from `.env` files | BSD-3-Clause | [pypi.org/project/python-dotenv](https://pypi.org/project/python-dotenv/) |
| **requests** | ≥2.31.0 | HTTP library for Python | Apache-2.0 | [requests.readthedocs.io](https://requests.readthedocs.io/) |
| **real_ladybug** | ≥0.1.0 | LadybugDB embedded graph database engine (Cypher-based, used by LadybugDB triplestore backend) | MIT | [pypi.org/project/real-ladybug](https://pypi.org/project/real-ladybug/) |
| **APScheduler** | ≥3.10.0 | Advanced Python Scheduler for background jobs (used by BuildScheduler for scheduled triple store builds) | MIT | [pypi.org/project/APScheduler](https://pypi.org/project/APScheduler/) |
| **owlrl** | ≥7.0.0 | OWL 2 RL forward-chaining reasoner — performs deductive closure on RDFLib graphs for ontology-level inference | W3C | [owl-rl.readthedocs.io](https://owl-rl.readthedocs.io/) |
| **pyshacl** | ≥0.26.0 | W3C SHACL validator for RDFLib graphs — validates RDF data against SHACL shapes for data quality checks | Apache-2.0 | [github.com/RDFLib/pySHACL](https://github.com/RDFLib/pySHACL) |
| **NetworkX** | ≥3.0 | Graph analysis library — server-side community detection (Louvain, Label Propagation, Greedy Modularity) on the full knowledge graph | BSD-3-Clause | [networkx.org](https://networkx.org/) |
| **strawberry-graphql[fastapi]** | ≥0.220.0 | GraphQL library for Python — auto-generates typed schema from ontology and integrates with FastAPI | MIT | [strawberry.rocks](https://strawberry.rocks/) |
| **MLflow** | ≥2.19.0 | ML lifecycle platform — used for agent tracing, evaluation, and the Databricks Agent Framework (ResponsesAgent) | Apache-2.0 | [mlflow.org](https://mlflow.org/) |

##### MCP Server Additional Dependencies

These packages are used by the MCP server (`src/mcp-server/`) which runs as a separate Databricks App:

| Package | Version | Description | License | Link |
|---------|---------|-------------|---------|------|
| **fastmcp** | ≥2.3.1 | Model Context Protocol (MCP) server SDK for exposing tools to LLMs | Apache-2.0 | [gofastmcp.com](https://gofastmcp.com/) |
| **httpx** | ≥0.25.0 | Async HTTP client (used by MCP server to call the main app REST API) | BSD-3-Clause | [www.python-httpx.org](https://www.python-httpx.org/) |

#### Development Dependencies

| Package | Version | Description | License | Link |
|---------|---------|-------------|---------|------|
| **pytest** | ≥7.4.0 | Testing framework for Python | MIT | [docs.pytest.org](https://docs.pytest.org/) |
| **pytest-asyncio** | ≥0.21.0 | Async test support for pytest | MIT | [pypi.org/project/pytest-asyncio](https://pypi.org/project/pytest-asyncio/) |
| **pytest-cov** | ≥4.1.0 | Code coverage plugin for pytest | MIT | [pypi.org/project/pytest-cov](https://pypi.org/project/pytest-cov/) |
| **httpx** | ≥0.25.0 | HTTP client for testing FastAPI applications | BSD-3-Clause | [www.python-httpx.org](https://www.python-httpx.org/) |
| **responses** | ≥0.24.0 | Mock HTTP request/response library for unit tests | Apache-2.0 | [pypi.org/project/responses](https://pypi.org/project/responses/) |
| **playwright** | ≥1.40.0 | Browser automation for end-to-end tests | Apache-2.0 | [playwright.dev/python](https://playwright.dev/python/) |
| **black** | ≥23.0.0 | Python code formatter | MIT | [black.readthedocs.io](https://black.readthedocs.io/) |
| **flake8** | ≥6.0.0 | Python style guide enforcer | MIT | [flake8.pycqa.org](https://flake8.pycqa.org/) |
| **Sphinx** | ≥7.0.0 | Documentation generator from docstrings (API reference) | BSD-2-Clause | [sphinx-doc.org](https://www.sphinx-doc.org/) |
| **myst-parser** | ≥3.0.0 | Markdown support for Sphinx (MyST flavour) | MIT | [myst-parser.readthedocs.io](https://myst-parser.readthedocs.io/) |

---

### Frontend Dependencies (CDN)

#### CSS Frameworks

| Library | Version | Description | License | CDN |
|---------|---------|-------------|---------|-----|
| **Bootstrap** | 5.3.2 | Front-end CSS framework for responsive UI components | MIT | [cdn.jsdelivr.net/npm/bootstrap@5.3.2](https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css) |
| **Bootstrap Icons** | 1.11.2 | Official open-source SVG icon library for Bootstrap | MIT | [cdn.jsdelivr.net/npm/bootstrap-icons@1.11.2](https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.2/font/bootstrap-icons.css) |
| **Grid.js Theme** | latest | Mermaid theme for Grid.js data table | MIT | [unpkg.com/gridjs](https://unpkg.com/gridjs/dist/theme/mermaid.min.css) |

#### JavaScript Libraries

| Library | Version | Description | License | CDN |
|---------|---------|-------------|---------|-----|
| **Bootstrap Bundle** | 5.3.2 | JavaScript plugins for Bootstrap (includes Popper.js) | MIT | [cdn.jsdelivr.net/npm/bootstrap@5.3.2](https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js) |
| **D3.js** | 7.x | Data-driven document manipulation and graph data processing | ISC | [d3js.org/d3.v7.min.js](https://d3js.org/d3.v7.min.js) |
| **Sigma.js** | 3.0.2 | WebGL-powered graph visualization library for large-scale networks | MIT | [cdnjs.com/libraries/sigma.js](https://cdnjs.com/libraries/sigma.js/) |
| **Graphology** | 0.26.0 | Robust graph data model and algorithm library (used with Sigma.js) | MIT | [graphology.github.io](https://graphology.github.io/) |
| **Graphology Library** | 0.8.0 | Standard library bundle for Graphology (includes ForceAtlas2 layout and Louvain community detection) | MIT | [cdn.jsdelivr.net/npm/graphology-library](https://cdn.jsdelivr.net/npm/graphology-library@0.8.0/) |
| **Grid.js** | latest | Advanced table plugin with sorting, searching, and pagination | MIT | [unpkg.com/gridjs](https://unpkg.com/gridjs/dist/gridjs.umd.js) |
| **Chart.js** | 4.x | Simple yet flexible charting library (doughnut gauges on Cockpit) | MIT | [cdn.jsdelivr.net/npm/chart.js@4](https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js) |

---

### Local/Bundled Libraries

#### OntoViz

| Property | Value |
|----------|-------|
| **Name** | OntoViz |
| **Version** | 1.0.0 |
| **Location** | `src/front/static/global/ontoviz/ontoviz.js`, `src/front/static/global/ontoviz/css/` |
| **Description** | Custom entity-relationship visual editor for ontology design |
| **License** | MIT |
| **Author** | OntoBricks Team |

OntoViz is a lightweight, self-contained library for creating and managing ER diagrams. It provides:
- Drag-and-drop entity creation
- Visual relationship connections with direction control (forward, reverse, bidirectional)
- **Inheritance links** for class hierarchies (rdfs:subClassOf)
- Entity attributes management with inherited properties display
- Auto-layout algorithms (grid and force-directed)
- Zoom and pan controls
- Minimap navigation
- JSON serialization for persistence

---

### Dependency Details

#### FastAPI

FastAPI is the core web framework powering OntoBricks. It provides:
- Modern async route handling with automatic OpenAPI documentation
- Custom file-based session management
- Jinja2 templating for HTML rendering
- Static file serving via Starlette

**Example usage:**
```python
from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates

app = FastAPI()
templates = Jinja2Templates(directory="src/front/templates")

@app.get('/ontology/')
async def ontology_page(request: Request):
    return templates.TemplateResponse("ontology.html", {"request": request})
```

#### databricks-sql-connector

Enables direct connectivity to Databricks SQL Warehouses for executing queries.

**Example usage:**
```python
from databricks import sql

with sql.connect(
    server_hostname=host,
    http_path=warehouse_path,
    access_token=token
) as connection:
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM catalog.schema.table LIMIT 10")
        result = cursor.fetchall()
```

#### RDFLib

Powers all RDF/OWL operations including:
- Parsing OWL ontologies (Turtle, RDF/XML formats)
- Generating OWL content from ontology configuration
- SPARQL query execution (local mode)
- R2RML mapping generation

**Example usage:**
```python
from rdflib import Graph, Namespace, URIRef, Literal

g = Graph()
g.parse("ontology.ttl", format="turtle")

## Query the graph
for s, p, o in g:
    print(f"{s} {p} {o}")
```

#### Bootstrap

Provides the visual foundation for OntoBricks UI including:
- Responsive grid layout
- Navigation components (navbar, dropdowns)
- Form controls and validation
- Modal dialogs
- Cards and panels
- Buttons and badges

**Example usage:**
```html
<div class="card">
    <div class="card-header">
        <i class="bi bi-diagram-3"></i> Entity Details
    </div>
    <div class="card-body">
        <button class="btn btn-primary">Save</button>
    </div>
</div>
```

#### D3.js

Used for data-driven DOM manipulation and graph data processing in the Digital Twin page:
- Data array processing for graph node and link construction
- SVG element management
- Force simulation utilities

**Example usage:**
```javascript
const svg = d3.select("#graphContainer")
    .append("svg")
    .attr("width", width)
    .attr("height", height);

const simulation = d3.forceSimulation(nodes)
    .force("link", d3.forceLink(links).id(d => d.id))
    .force("charge", d3.forceManyBody().strength(-300))
    .force("center", d3.forceCenter(width / 2, height / 2));
```

#### Sigma.js + Graphology

Powers the WebGL-accelerated graph visualization in the Digital Twin "Knowledge Graph" view:
- High-performance rendering of large graphs via WebGL
- Interactive pan, zoom, hover highlighting, and click selection
- ForceAtlas2 layout algorithm (via graphology-library)
- Node/edge reducers for real-time filtering and search highlighting

**Example usage:**
```javascript
const graph = new graphology.Graph();
graph.addNode("1", { label: "Alice", x: 0, y: 0, size: 10, color: "#FF3621" });
graph.addNode("2", { label: "Bob", x: 1, y: 1, size: 10, color: "#6366F1" });
graph.addEdge("1", "2", { label: "knows", size: 2, color: "#bbb" });

const renderer = new Sigma(graph, document.getElementById("container"));
```

#### Grid.js

Provides advanced data table functionality for query results:
- Sortable columns
- Search/filter capabilities
- Pagination
- Resizable columns
- Custom cell rendering

**Example usage:**
```javascript
new gridjs.Grid({
    columns: ["Name", "Email", "Department"],
    data: [
        ["John", "john@example.com", "Engineering"],
        ["Jane", "jane@example.com", "Marketing"]
    ],
    search: true,
    sort: true,
    pagination: { limit: 100 }
}).render(document.getElementById("resultsContainer"));
```

---

### License Summary

| License | Packages |
|---------|----------|
| **MIT** | FastAPI, pydantic, pydantic-settings, Bootstrap, Bootstrap Icons, Sigma.js, Graphology, Grid.js, Chart.js, OntoViz, strawberry-graphql, real_ladybug, pytest, pytest-asyncio, pytest-cov, black, flake8 |
| **BSD-3-Clause** | Uvicorn, Starlette, Jinja2, itsdangerous, RDFLib, python-dotenv, httpx, NetworkX |
| **Apache-2.0** | databricks-sql-connector, databricks-sdk, pyarrow, python-multipart, aiofiles, requests, fastmcp, MLflow, pyshacl, responses, playwright |
| **BSD-2-Clause** | Sphinx |
| **ISC** | D3.js |

All dependencies use permissive open-source licenses compatible with commercial use.

---

### Version Compatibility

- **Python**: ≥3.10
- **Node.js**: Not required (all frontend assets via CDN)
- **Browser**: Modern browsers (Chrome, Firefox, Safari, Edge)

---

### Updating Dependencies

#### Python Dependencies

```bash
## Update all dependencies
uv sync --upgrade

## Update a specific package
uv lock --upgrade-package fastapi && uv sync
```

#### Frontend Dependencies

Frontend libraries are loaded via CDN with pinned versions. To update:

1. Edit `src/front/templates/base.html` for Bootstrap
2. Edit `src/front/templates/dtwin.html` for D3.js and Grid.js
3. Test thoroughly after updates

---

### Security Considerations

- All CDN resources use HTTPS
- Python packages are installed from PyPI
- Regular security audits recommended via `pip-audit`
- Keep dependencies updated for security patches
- **CSRF protection** is active for all state-changing requests (POST, PUT, PATCH, DELETE). The middleware (`shared/fastapi/csrf.py`) validates a `X-CSRF-Token` header against the `csrf_token` cookie. Tests bypass this via `CSRF_DISABLED=1` (set in `tests/conftest.py`).
- **Static asset versioning**: All `<script>` and `<link>` tags must use `?v={{ asset_version }}` for deterministic cache busting. Do not use `{{ range(...) | random }}`.



---

## OntoBricks Test Campaign

This document describes the OntoBricks automated test suite: what is tested, how to run it, the test infrastructure, and conventions for adding new tests.

---

### Quick Start

```bash
## Install dev dependencies (if not already done)
uv sync --dev

## Run the full test suite
.venv/bin/python -m pytest tests/

## Run with coverage report
.venv/bin/python -m pytest tests/ --cov=back --cov=front --cov=shared --cov=api --cov=agents --cov-report=term-missing

## Run a single test file
.venv/bin/python -m pytest tests/test_owl_parser.py

## Run a single test class or method
.venv/bin/python -m pytest tests/test_owl_parser.py::TestGetClasses::test_returns_defined_classes

## Run only unit tests (by marker)
.venv/bin/python -m pytest -m unit

## Run only integration tests (by marker)
.venv/bin/python -m pytest -m integration
```

---

### Test Architecture

#### Directory Structure

```
tests/
  conftest.py                       # Shared fixtures and test configuration
  fixtures/
    sample_ontology.owl             # Sample OWL/Turtle content for parsing tests
    sample_domain.json              # Sample domain export for import/export tests
  test_owl_parser.py                # OWL parser unit tests
  test_owl_generator.py             # OWL generator unit tests
  test_r2rml_parser.py              # R2RML parser unit tests
  test_r2rml_generator.py           # R2RML generator unit tests
  test_sparql_service.py            # SPARQL translation and local execution
  test_ontology_service.py          # Ontology domain helpers (Ontology class)
  test_home_service.py              # Home/dashboard service layer
  test_dtwin_service.py             # Digital Twin domain class (back.objects.digitaltwin)
  test_mapping_service.py           # Mapping domain class (back.objects.mapping.Mapping)
  test_domain_service.py            # Domain class (back.objects.domain.Domain)
  test_domain_session.py            # DomainSession state management
  test_databricks_client.py         # Databricks client (mocked)
  test_llm_utils.py                 # LLM retry logic
  test_triplestore_factory.py       # Triplestore factory
  test_ladybug.py                   # LadybugDB triple store backend (flat model)
  test_ladybug_schema.py            # LadybugDB graph schema generation
  test_ladybug_sync.py              # LadybugDB UC Volume sync
  test_routes.py                    # HTTP route tests (all endpoints)
  test_sql_wizard.py                # SQL Wizard service (pre-existing)
  test_ui_rendering.py              # UI Layer 1: HTML DOM structure tests (stdlib html.parser)
  test_workflow_owl_roundtrip.py    # Integration: OWL generate -> parse cycle
  test_workflow_mapping.py          # Integration: ontology -> R2RML pipeline
  test_workflow_sparql.py           # Integration: SPARQL-to-SQL translation pipeline
  test_workflow_domain.py           # Integration: domain export/import cycle
  test_registry.py                  # Registry service and cache tests
  test_permissions.py               # Permission service tests
  test_reasoning.py                 # Reasoning engine tests
  test_reasoning_service.py         # Reasoning service orchestration
  test_swrl_engine.py               # SWRL rule compilation and execution
  test_business_rules.py            # Business rules engine tests
  test_dataquality.py               # SHACL data quality shapes and SQL generation
  test_errors.py                    # Centralized error hierarchy
  test_config.py                    # Configuration / settings tests
  test_middleware_session.py         # Session middleware tests
  test_helpers.py                   # Helper function unit tests
  test_rdfs_parser.py               # RDFS parser tests
  test_ladybug_reasoning.py         # LadybugDB reasoning integration
  e2e/
    conftest.py                     # Uvicorn server + Playwright browser fixtures
    test_e2e_flows.py               # UI Layer 2: end-to-end browser tests (Playwright)
```

#### Configuration

Test configuration lives in two files:

- **`pytest.ini`** -- Test discovery, markers, and default options (`-v`, `--tb=short`, `--strict-markers`).
- **`pyproject.toml`** (`[tool.uv].dev-dependencies`) -- Dev dependencies: `pytest`, `pytest-asyncio`, `httpx`, `pytest-cov`, `responses`, `black`, `flake8`, `playwright`.

#### Shared Fixtures (`conftest.py`)

The `conftest.py` file provides reusable fixtures available to all test files:

| Fixture | Scope | Description |
|---------|-------|-------------|
| `setup_test_env` | session | Sets `TESTING=1`, removes Databricks env vars to isolate tests from production |
| `client` | function | Starlette `TestClient` wrapping the FastAPI app |
| `mock_session_mgr` | function | Mocked `SessionManager` returning a fresh `DomainSession` |
| `domain_session` | function | A clean `DomainSession` with a mock session manager |
| `sample_ontology_config` | function | Dict with base URI, classes (Person, Organization), properties, constraints |
| `sample_mapping_config` | function | Dict with entity and relationship mapping definitions |
| `sample_owl_content` | function | Raw OWL/Turtle string loaded from `tests/fixtures/sample_ontology.owl` |
| `mock_databricks_client` | function | Mocked `DatabricksClient` with stubbed catalog/schema/table methods |

---

### What Is Tested

#### P0 -- Core Module Unit Tests (62 tests)

These test the pure-logic modules that implement OntoBricks' semantic web capabilities. No HTTP, no mocking of external services.

| Test File | Tests | Module Under Test | What Is Verified |
|-----------|-------|-------------------|------------------|
| `test_owl_parser.py` | 24 | `back.core.w3c.owl.OntologyParser` | Initialization, `_to_camel_case`, `get_classes`, `get_properties`, `get_ontology_info`, `get_constraints`, `get_swrl_rules`, `get_axioms` |
| `test_owl_generator.py` | 18 | `back.core.w3c.owl.OntologyGenerator` | Basic generation, classes with hierarchy, datatype/object properties, property constraints, SWRL rules, axioms, generate-then-parse roundtrip |
| `test_r2rml_generator.py` | 11 | `back.core.w3c.r2rml.R2RMLGenerator` | Initialization, entity mapping (SQL query, label, attributes, exclusion flag), relationship mapping, `generate_r2rml_from_config` convenience function |
| `test_r2rml_parser.py` | 9 | `back.core.w3c.r2rml.R2RMLParser` | Valid/invalid R2RML parsing, entity extraction, `_parse_table_name` helper, `parse_r2rml_content` convenience, generate-then-parse roundtrip |

#### P0 -- Service Layer Unit Tests (48 tests)

Test the business-logic service functions that sit between routes and core modules.

| Test File | Tests | Module Under Test | What Is Verified |
|-----------|-------|-------------------|------------------|
| `test_sparql_service.py` | 9 | `back.core.w3c.sparql` | SQL dialect helpers, `extract_r2rml_mappings`, `execute_local_query` (SELECT, LIMIT), `translate_sparql_to_spark` (non-SELECT rejection, basic SELECT) |
| `test_ontology_service.py` | 50 | `back.objects.ontology.Ontology` | `ensure_uris`, `get_ontology_stats`, `normalize_property_domain_range`, `prune_mappings_to_ontology_uris`, `save_ontology_config_from_editor`, `delete_class_by_uri`, `delete_property_by_uri`, `add_class`, `update_class`, `add_property`, `update_property`, `ingest_owl`, `apply_parsed_rdfs_to_project`, `rename_relationship_references`, `apply_agent_ontology_changes`, `validate_swrl_rule`, `merge_icon_suggestions`, `postprocess_generated_owl`, `build_class_from_data`, `build_property_from_data`, `validate_constraint`, `generate_owl`, `parse_owl` |
| `test_home_service.py` | 13 | `back.services.home` | `get_session_status`, `get_ontology_status`, `validate_ontology`, `validate_mapping`, `validate_status`, `get_detailed_validation` |
| `test_dtwin_service.py` | 13 | `back.objects.digitaltwin` | `_normalize_base_uri`, `_safe_class_label`, `augment_mappings_from_config`, `augment_relationships_from_config` |

#### P1 -- Mocked External Dependencies (72 tests)

These use `unittest.mock` to isolate modules that depend on external systems (Databricks, LLMs, file system).

| Test File | Tests | Module Under Test | What Is Verified |
|-----------|-------|-------------------|------------------|
| `test_databricks_client.py` | 20 | `back.core.databricks.DatabricksClient` | `is_databricks_app`, `normalize_host`, `get_workspace_host`, `DatabricksClient` init, `has_valid_auth`, `test_connection`, `get_catalogs`, `get_schemas`, `get_tables`, `get_table_columns` |
| `test_domain_session.py` | 21 | `back.objects.session.DomainSession` | `get_empty_domain`, `DomainSession` properties (info, version, ontology, assignment, generated, R2RML), save/reset, export/import, legacy migration |
| `test_mapping_service.py` | 14 | `back.objects.mapping.Mapping` | `build_entity_mapping`, `build_relationship_mapping`, add/update/delete entities and relationships, `get_mapping_stats`, `save_mapping_config`, `reset_mapping` |
| `test_domain_service.py` | 8 | `back.objects.domain.Domain` | `get_domain_info`, `get_domain_stats`, `save_domain_info`, `get_domain_template_data` |
| `test_llm_utils.py` | 5 | `agents.llm_utils` | `call_llm_with_retry` -- success, retry on 429/503 HTTP errors, retry on timeout, retry exhaustion |
| `test_triplestore_factory.py` | 4 | `back.core.triplestore.TripleStoreFactory` | Unknown backend handling, missing Delta config, successful Delta instantiation (mocked) |

#### P1b -- LadybugDB Tests (82 tests)

These test the LadybugDB embedded graph database backend end-to-end using a real `real_ladybug` database (on-disk in `/tmp`, cleaned up after each test). Skipped automatically if `real_ladybug` is not installed.

| Test File | Tests | Module Under Test | What Is Verified |
|-----------|-------|-------------------|------------------|
| `test_ladybug.py` | 45 | `back.core.triplestore.ladybugdb` | `LadybugFlatStore` flat model: lifecycle (create/drop/close), insert/query, count, status, aggregate stats, type/predicate distributions, find subjects (by type, search, pagination), resolve by ID, triples for subjects, seed subjects (all match modes), paginated queries, BFS traversal, pattern matching, neighbor expansion, SQL-to-Cypher condition translation |
| `test_ladybug_schema.py` | 22 | `back.core.triplestore.ladybugdb` | `GraphSchema`, `NodeTableDef`, `RelTableDef`, identifier sanitization, local name extraction, schema generation from ontology classes + properties, URI-to-table mappings, fallback handling, DDL generation, triple classification (node inserts, relationships, attributes) |
| `test_ladybug_sync.py` | 15 | `back.core.triplestore.ladybugdb` | Path helpers, `sync_to_volume` (success, missing local, upload failure), `sync_from_volume` (success, archive not found, download error, replace existing local) — UC service calls are mocked |

#### P2 -- HTTP Route Tests (59 tests)

End-to-end HTTP tests using Starlette's `TestClient`. These verify that routes are reachable, return correct status codes, and produce expected response shapes.

| Test Class | Endpoints Covered |
|------------|-------------------|
| `TestHealthRoutes` | `GET /health`, `GET /health/detailed` |
| `TestMainPages` | `GET /`, `GET /about`, `GET /session-status`, `GET /ontology-status`, `GET /validate/*` |
| `TestSettingsRoutes` | `GET /settings`, `GET /settings/current`, `POST /settings/save`, `GET /settings/get-default-emoji`, `GET /settings/get-base-uri` |
| `TestOntologyRoutes` | Page loads, load/save config, class/property CRUD, generate/import OWL, constraints list/save, SWRL list/save, axioms list |
| `TestMappingRoutes` | Page loads, load/save config, entity/relationship CRUD, generate R2RML |
| `TestProjectRoutes` | Page loads, info/save, export/import, config, session debug, design/map layout, design views |
| `TestQueryRoutes` | `GET /dtwin/` |
| `TestDigitalTwinAPI` | `GET /api/v1/digitaltwin/status`, `/ontology`, `/r2rml`, `/sparksql`, `/design-status` |
| `TestTasksRoutes` | `GET /tasks/`, `GET /tasks/active` |
| `TestAPIv1Routes` | `GET /api/v1/health`, `POST /api/v1/query/validate`, domain endpoints (validation and auth-required behavior) |

#### P3 -- Workflow / Integration Tests (12 tests)

These exercise multi-module pipelines to verify that components work together correctly.

| Test File | Tests | Pipeline | What Is Verified |
|-----------|-------|----------|------------------|
| `test_workflow_owl_roundtrip.py` | 4 | Parse OWL -> Regenerate -> Re-parse | Classes, properties, constraints, SWRL rules, axioms survive a full roundtrip |
| `test_workflow_mapping.py` | 3 | Ontology config -> Build mappings -> Generate R2RML -> Parse R2RML | Entity exclusion, relationship direction, and overall pipeline integrity |
| `test_workflow_sparql.py` | 2 | Ontology + Mapping -> R2RML -> Extraction -> SPARQL-to-SQL | End-to-end translation of SPARQL queries to Spark SQL via R2RML mappings |
| `test_workflow_domain.py` | 3 | Create domain -> Set data -> Export -> Import -> Verify | Full domain roundtrip data integrity, secret exclusion from exports |

#### Pre-Existing Tests

| Test File | Tests | Notes |
|-----------|-------|-------|
| `test_sql_wizard.py` | 42 | Tests for the SQL Wizard service. Some failures are pre-existing and not related to the current test campaign. Run separately: `.venv/bin/python -m pytest tests/test_sql_wizard.py` |
| `test_dataquality.py` | 12+ | Tests for the SHACL data quality service — SQL generation (cardinality, datatype, sparql), URI normalization, in-memory evaluation |
| `test_reasoning_service.py` | — | Tests for the reasoning service (if present) |

---

### Coverage

Generate a coverage report with:

```bash
.venv/bin/python -m pytest tests/ --cov=back --cov=front --cov=shared --cov=api --cov=agents --cov-report=term-missing
```

Generate an HTML coverage report:

```bash
.venv/bin/python -m pytest tests/ --cov=back --cov=front --cov=shared --cov=api --cov=agents --cov-report=html
open htmlcov/index.html
```

#### Coverage by Module (as of Feb 2026)

| Module | Coverage | Notes |
|--------|----------|-------|
| `back/core/w3c/r2rml/R2RMLParser.py` | 99% | Near-complete |
| `back/core/w3c/r2rml/R2RMLGenerator.py` | 80% | Good coverage |
| `back/objects/mapping/mapping.py` | 78% | Good coverage |
| `back/services/home.py` | 76% | Good coverage |
| `api/routers/v1.py` | 70% | Auth-required paths lower coverage |
| `back/objects/digitaltwin/DigitalTwin.py` | 67% | Good coverage |
| `back/objects/session/DomainSession.py` | 66% | Legacy migration paths less covered |
| `back/core/w3c/owl/OntologyParser.py` | 63% | Complex helper methods partially covered |
| `back/core/w3c/owl/OntologyGenerator.py` | 50% | Advanced OWL features (annotations, complex axioms) less covered |
| `back/core/w3c/sparql/SparqlTranslator.py` | 41% | Large module; advanced translation paths need Databricks |
| Routes (`front/routes/`, `api/routers/internal/`) | 11-40% | Route handlers require full app context for deeper testing |
| `back/core/triplestore/ladybugdb/` | ~80% | Good coverage via real LadybugDB tests (flat model); graph model paths partially covered |
| `back/core/triplestore/ladybugdb/GraphSchema.py` | ~90% | Schema generation, DDL, triple classification well covered |
| `back/core/triplestore/ladybugdb/GraphSyncService.py` | ~95% | Sync to/from UC Volume fully tested with mocked UC service |

Modules with 0% coverage (`back/core/w3c/rdfs`, `back/core/sqlwizard`, `back/core/databricks/MetadataService.py`) are either not yet tested or depend entirely on external services.

---

### Test Conventions

#### Naming

- Test files: `test_<module_name>.py`
- Test classes: `Test<FunctionalArea>` (e.g., `TestGetClasses`, `TestEntityMapping`)
- Test functions: `test_<what_is_being_tested>` (e.g., `test_returns_defined_classes`, `test_missing_name_uses_default`)

#### Markers

Defined in `pytest.ini`:

```ini
markers =
    unit: Unit tests
    integration: Integration tests
    slow: Slow running tests
```

Use them to selectively run subsets:

```bash
.venv/bin/python -m pytest -m unit
.venv/bin/python -m pytest -m integration
.venv/bin/python -m pytest -m "not slow"
```

#### Writing New Tests

1. **Place the file** in `tests/` with the `test_` prefix.
2. **Use fixtures** from `conftest.py` when possible (`client`, `domain_session`, `sample_ontology_config`, etc.).
3. **Mock external calls** -- never call real Databricks APIs, LLMs, or network services in tests. Use `unittest.mock.patch` or the `responses` library for HTTP mocking.
4. **Keep tests fast** -- the full suite runs in under 3 seconds. Avoid `time.sleep` or expensive setup.
5. **Test one thing per test function** -- each test should verify a single behavior or edge case.
6. **Add fixtures to `conftest.py`** when they are reused across multiple test files.

#### Example: Adding a Unit Test

```python
import pytest
from back.core.w3c.owl import OntologyParser

class TestNewFeature:
    def test_parses_annotation_properties(self, sample_owl_content):
        parser = OntologyParser(sample_owl_content)
        props = parser.get_properties()
        annotations = [p for p in props if p.get('type') == 'annotation']
        assert len(annotations) >= 0  # adjust to your expectation

    def test_handles_empty_input(self):
        parser = OntologyParser("")
        assert parser.get_classes() == []
```

#### Example: Adding a Route Test

```python
class TestNewRoute:
    def test_my_endpoint_returns_200(self, client):
        response = client.get('/my-new-endpoint')
        assert response.status_code == 200

    def test_my_endpoint_post_saves_data(self, client):
        response = client.post('/my-new-endpoint/save', json={'key': 'value'})
        assert response.status_code == 200
        data = response.json()
        assert data.get('success') is True
```

---

### Continuous Integration

To integrate the test suite in a CI pipeline, use:

```bash
## Install dependencies
uv sync --dev

## Run tests with JUnit XML output (for CI reporting)
.venv/bin/python -m pytest tests/ --junitxml=test-results.xml --cov=back --cov=front --cov=shared --cov=api --cov=agents --cov-report=xml

## Fail if coverage drops below a threshold
.venv/bin/python -m pytest tests/ --cov=back --cov=front --cov=shared --cov=api --cov=agents --cov-fail-under=25
```

---

### UI Tests

UI testing uses two complementary layers:

#### Layer 1: HTML Rendering Tests (pytest + stdlib `html.parser`)

Fast tests that fetch pages via the Starlette `TestClient` and verify DOM structure with a small `HTMLParser` collector (stdlib only). No browser required.

**File:** `tests/test_ui_rendering.py` (~99 tests)

| Page | What Is Verified |
|------|-----------------|
| All pages | Navbar present, brand link, notification container, Bootstrap/utils.js scripts, nav dropdowns (Registry, Domain, Digital Twin), Ontology/Mapping links under Domain dropdown, Settings link, warehouse status, task tracker |
| Home `/` | Hero section, domain panel, stat items (Entities, Relationships, Mappings), quick links (Settings, About) |
| Settings `/settings` | Connection form, host/token/warehouse displays, Test Connection button, base URI field, Save button |
| Registry `/registry/` | Registry domains section, schedules table, API endpoint cards |
| Ontology `/ontology` | Sidebar section groups: Ontology Editor (Information, Import, Generate, Model, Business Views, Entities, Relationships), Advanced (Data Quality, Business Rules, Expr. & Axioms), W3C Standards (OWL); section divs, OntoViz script |
| Mapping `/mapping` | Sidebar with 6 section links (Information, Designer, Manual, Auto-Map, R2RML, Spark SQL), mapping-core.js |
| Domain `/domain` | Sidebar with 6 section links (Information, Metadata, Documents, Validation, OWL, R2RML), section divs |
| Digital Twin `/dtwin/` | Sidebar section groups: Navigation (Knowledge Graph, GraphQL), Advanced (Data Quality, Reasoning); Sigma.js script |
| About `/about` | Page renders, contains "OntoBricks" |

**Run:**

```bash
.venv/bin/python -m pytest tests/test_ui_rendering.py -v
```

#### Layer 2: End-to-End Browser Tests (Playwright)

Browser-based tests using Playwright against a live Uvicorn server. Verifies navigation, sidebar switching, interactive elements.

**Files:**
- `tests/e2e/conftest.py` -- Uvicorn server fixture (port 18765) and Playwright browser/page fixtures
- `tests/e2e/test_e2e_flows.py` (~45 tests)

| Test Class | What Is Verified |
|------------|-----------------|
| `TestNavigation` | All pages load with correct titles, navbar brand navigates home, Settings link works |
| `TestHomePage` | Hero visible, domain panel visible, 3 workflow cards, stat items present |
| `TestSettingsPage` | Connection form visible, host display, base URI field populated, Save button enabled |
| `TestOntologySidebar` | All 11 sidebar items switch to correct section, wizard select-all checkbox exists |
| `TestMappingSidebar` | All 6 sidebar items switch to correct section |
| `TestProjectSidebar` | All 6 sidebar items switch to correct section |
| `TestDigitalTwinSidebar` | Knowledge Graph section visible by default, sidebar navigation links present |
| `TestAboutPage` | Page content and R2RML reference present |

**Prerequisites:**

```bash
## Install Chromium for Playwright (one-time)
.venv/bin/python -m playwright install chromium
```

**Run:**

```bash
## Layer 2 only
.venv/bin/python -m pytest tests/e2e/ -v

## Both UI layers
.venv/bin/python -m pytest tests/test_ui_rendering.py tests/e2e/ -v
```

#### Dependencies

| Package | Layer | Purpose |
|---------|-------|---------|
| *(stdlib `html.parser`)* | Layer 1 | HTML parsing for DOM assertions |
| `playwright>=1.40.0` | Layer 2 | Browser automation for E2E tests |

---

### Known Limitations

- **MCP server tests** are not included in the automated test suite — the MCP server is a separate project with its own dependencies.
- **Databricks-connected features** (Unity Catalog operations, SQL Warehouse queries, triplestore sync) cannot be tested without a live workspace. These are tested at the mock/stub level only.
- **LLM agent features** (AI Assistant, auto-map icons) rely on external API calls that are mocked with `responses` or `unittest.mock.patch`.
- **E2E tests** validate navigation and DOM visibility but cannot fully test features that require a real Databricks connection (e.g., loading metadata, running queries).
- **`test_sql_wizard.py`** has pre-existing failures unrelated to this test campaign and should be investigated separately.

---

## OntoBricks — Permission & Rights Management

This document describes how access control works in OntoBricks, covering
both the **Databricks App–level permissions** and the **in-app permission
list**. It is intended for administrators who deploy and manage OntoBricks.

---

### Table of Contents

1. [Overview](#overview)
2. [Roles](#roles)
3. [How Permission Resolution Works](#how-permission-resolution-works)
4. [Databricks App Permissions (External)](#databricks-app-permissions-external)
5. [In-App Permission List (Internal)](#in-app-permission-list-internal)
6. [Managing Permissions — Step by Step](#managing-permissions--step-by-step)
7. [Service Principal Setup](#service-principal-setup)
8. [Local Development Mode](#local-development-mode)
9. [Settings Page Access](#settings-page-access)
10. [Access Denied Page](#access-denied-page)
11. [Configuration Reference](#configuration-reference)
12. [Caching Behaviour](#caching-behaviour)
13. [Diagnostic Endpoint](#diagnostic-endpoint)
14. [Troubleshooting](#troubleshooting)

---

### Overview

OntoBricks uses a **two-layer** access-control model:

| Layer | Where it lives | What it controls |
|-------|----------------|------------------|
| **Databricks App permissions** | Databricks workspace UI → Apps → *ontobricks* → Permissions | Who can reach the app at all, and who is an **Admin** (`CAN_MANAGE`). |
| **In-app permission list** | `.permissions.json` stored in the Unity Catalog Registry Volume | Fine-grained roles (**Viewer** / **Editor** / **Builder**) for individual users and groups. |
| **Domain-level overrides** | `.domain_permissions.json` inside each domain folder | Per-domain role overrides that can restrict (but not elevate) the app-level role. |

A user's effective role is determined by combining all three layers.

---

### Roles

| Role | Source | Capabilities |
|------|--------|--------------|
| **Admin** | Databricks App `CAN_MANAGE` permission | Full access. Can view, edit, build, and manage the Settings page including the permission list. |
| **Builder** | In-app permission list | Can view, edit, and **build digital twins**. Cannot access Settings. |
| **Editor** | In-app permission list | Can view all pages, create and modify domains, ontologies, and mappings. **Cannot build digital twins.** Cannot access Settings. |
| **Viewer** | In-app permission list | Read-only access. Can browse domains, ontologies, and query results. All write operations (POST, PUT, PATCH, DELETE) are blocked. Cannot access Settings. |
| **None** | Default when not matched | Completely blocked. Redirected to the Access Denied page. |

Role hierarchy: `admin > builder > editor > viewer > none`.

---

### How Permission Resolution Works

Every incoming HTTP request passes through the `PermissionMiddleware`. The
resolution follows this priority order:

```
Request arrives
  │
  ├─ Running locally (no DATABRICKS_APP_PORT)?
  │    └─ Yes → role = admin, full access
  │
  ├─ Path is a bypass path (/static, /health, /docs, /api, etc.)?
  │    └─ Yes → pass through, no role enforcement
  │
  └─ Resolve app-level role via get_user_role():
       │
       ├─ 1. is_admin(email)?
       │    Check if the user has CAN_MANAGE on the Databricks App.
       │    └─ Yes → app_role = admin
       │
       ├─ 2. User explicitly listed in .permissions.json?
       │    Match email (case-insensitive).
       │    └─ Yes → app_role from entry (viewer, editor, or builder)
       │
       ├─ 3. User belongs to a group listed in .permissions.json?
       │    Check group membership via Databricks SCIM API.
       │    └─ Yes → app_role from group entry (viewer, editor, or builder)
       │
       └─ 4. No match
            └─ app_role = none → Access Denied
       │
       └─ Resolve domain-level role (when a domain is loaded):
            │
            ├─ Admin → always admin (domain overrides ignored)
            ├─ User/group listed in .domain_permissions.json?
            │    └─ Yes → effective_role = min(app_role, domain_role)
            └─ Not listed → effective_role = app_role (inherit)
```

#### Enforcement Rules

| Condition | Action |
|-----------|--------|
| `role = none` | JSON 403 (for fetch/XHR) or redirect to `/access-denied` (for page navigation). |
| `role = viewer` + write method (POST/PUT/PATCH/DELETE) | JSON 403: "Viewer role does not allow write operations". |
| `role != admin` + path starts with `/settings` | JSON 403 or redirect to `/`. Only admins can access the Settings page. |
| Digital twin build + effective domain role < `builder` | JSON 403: "Only builders and admins can build a digital twin". |
| Otherwise | Request proceeds normally. |

---

### Databricks App Permissions (External)

These are managed in the Databricks workspace UI:

1. Navigate to **Compute → Apps → ontobricks → Permissions**.
2. Add users or groups with one of two permission levels:

| Permission Level | Effect in OntoBricks |
|------------------|----------------------|
| **CAN_MANAGE** | The user is an OntoBricks **Admin**. Full access including the Settings page and permission management. |
| **CAN_USE** | The user can reach the app, but their actual role depends on the in-app permission list. If they are not in the list, they are blocked. |

#### Who Needs CAN_MANAGE?

- At minimum, the person responsible for setting up OntoBricks.
- The app's own **Service Principal** (required for the admin check to
  work — see [Service Principal Setup](#service-principal-setup)).

#### Who Needs CAN_USE?

- All users who should be able to access OntoBricks.
- Groups like `users` or `account users` can be added for broad access,
  with roles then controlled by the in-app permission list.

---

### In-App Permission List (Internal)

The in-app permission list is stored as a JSON file in the configured
Unity Catalog Registry Volume:

```
/Volumes/{catalog}/{schema}/{volume}/.permissions.json
```

#### File Format

```json
{
  "version": 1,
  "permissions": [
    {
      "principal": "alice@example.com",
      "principal_type": "user",
      "display_name": "Alice Smith",
      "role": "editor"
    },
    {
      "principal": "data-team",
      "principal_type": "group",
      "display_name": "data-team",
      "role": "viewer"
    }
  ]
}
```

#### Entry Fields

| Field | Description |
|-------|-------------|
| `principal` | Email address (for users) or group name (for groups). |
| `principal_type` | `"user"` or `"group"`. |
| `display_name` | Human-readable name shown in the UI. |
| `role` | `"viewer"`, `"editor"`, or `"builder"`. Admins are never listed here — they are determined by `CAN_MANAGE`. |

#### Empty Permission List Behaviour

When `.permissions.json` does not exist or has an empty `permissions`
array, **only users with `CAN_MANAGE` have access**. All other
authenticated users are blocked. This is by design — it prevents
accidental open access before the admin configures the permission list.

---

### Domain-Level Permissions

Domain-level permissions allow admins to override a user's app-level role
on a per-domain basis. These overrides can only **restrict** (never
elevate) the app-level role.

#### Storage

Each domain has an optional `.domain_permissions.json` file inside its
domain folder on the registry volume:

```
/Volumes/{catalog}/{schema}/{volume}/domains/{domain_name}/.domain_permissions.json
```

#### File Format

```json
{
  "version": 1,
  "permissions": [
    { "principal": "alice@example.com", "principal_type": "user", "role": "viewer" },
    { "principal": "data-team", "principal_type": "group", "role": "builder" }
  ]
}
```

#### Resolution Logic

- **Admin** users always keep admin access regardless of domain overrides.
- If a user has no entry in the domain permission file, they inherit their
  app-level role unchanged (backward compatible).
- If a user has a domain-level entry, their effective role is
  `min(app_role, domain_role)`. For example, an app-level `builder` with
  domain-level `viewer` is effectively a `viewer` on that domain.

#### API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/settings/domain-permissions/{domain_name}` | List domain entries |
| POST | `/settings/domain-permissions/{domain_name}` | Add/update entry |
| DELETE | `/settings/domain-permissions/{domain_name}/{principal}` | Remove entry |

These endpoints are admin-only.

---

### Managing Permissions — Step by Step

#### Prerequisites

1. You have `CAN_MANAGE` on the Databricks App (`ontobricks`).
2. The app's Service Principal also has `CAN_MANAGE` on itself (see
   [Service Principal Setup](#service-principal-setup)).
3. A Registry is configured in OntoBricks (Settings → Registry tab).

#### Adding a User or Group

1. Open OntoBricks and click the **gear icon** (⚙) in the navbar to go
   to Settings.
2. Select the **Permissions** tab.
3. Click the **Add** button.
4. In the modal:
   - Select **User** or **Group**.
   - Search and select from the dropdown. The list shows users and groups
     that have permissions on the Databricks App (CAN_USE or CAN_MANAGE).
   - Choose a role: **Viewer**, **Editor**, or **Builder**.
5. Click **Add**. The entry is immediately saved to the Registry.

#### Changing a Role

In the Permissions tab, use the **role dropdown** next to any entry to
switch between Viewer and Editor. Changes are saved automatically.

#### Removing a User or Group

Click the **trash icon** next to the entry and confirm the deletion.

#### Important Notes

- The permission list only controls users who are **not** admins.
  Users with `CAN_MANAGE` always have full admin access regardless of
  whether they appear in the list.
- The user/group picker only shows principals that already have
  permissions on the Databricks App (CAN_USE or CAN_MANAGE). To add
  someone who doesn't appear in the list, first grant them `CAN_USE`
  on the Databricks App.

---

### Service Principal Setup

The OntoBricks Databricks App runs under a **Service Principal** (SP).
This SP needs `CAN_MANAGE` on the app itself so it can read the app's
access control list to determine which users are admins.

#### Granting CAN_MANAGE to the SP

1. Find the SP's application ID. It appears in the app details (e.g.,
   `84eff7c5-778b-4c0b-a24d-36126bcc8cca`).

2. Grant the permission using the Databricks CLI:

   ```bash
   databricks permissions update apps ontobricks \
     --json '{
       "access_control_list": [
         {
           "service_principal_name": "84eff7c5-778b-4c0b-a24d-36126bcc8cca",
           "permission_level": "CAN_MANAGE"
         }
       ]
     }'
   ```

3. Verify:

   ```bash
   databricks permissions get apps ontobricks
   ```

   The SP should appear with `CAN_MANAGE` (non-inherited).

#### What Happens Without SP CAN_MANAGE?

The admin check (`is_admin`) will fail because the SDK cannot read the
app's ACL. This causes **all users to be denied access**, including
those with `CAN_MANAGE` set in the Databricks UI.

---

### Local Development Mode

When running locally (i.e., `DATABRICKS_APP_PORT` is not set), the
permission system is **completely disabled**:

- Every user gets `role = admin`.
- All pages and features are accessible.
- The Settings page and Permissions tab work normally.
- No Databricks App permissions or `.permissions.json` are checked.

This allows local development and testing without any permission setup.

---

### Settings Page Access

The entire Settings page (`/settings` and all sub-routes) is restricted
to **admin users only**. This includes:

- The General tab (Databricks connection, SQL Warehouse).
- The Registry tab (Unity Catalog Volume configuration).
- The Permissions tab (user/group role management).
- The About tab.

Non-admin users:
- Do not see the gear icon (⚙) or the SQL Warehouse status icon in the
  navbar.
- Do not see the "Settings" quick link on the home page.
- Are redirected to `/` if they navigate directly to `/settings`.

---

### Access Denied Page

Users who are blocked (role = none) see a standalone page with:

- A shield icon and "Access Denied" heading.
- Their email address (from `X-Forwarded-Email`).
- Instructions to contact an OntoBricks administrator.

The page has **no navbar or navigation** to prevent confusion, since the
user has no access to any part of the application.

---

### Configuration Reference

#### app.yaml Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `ONTOBRICKS_APP_NAME` | No | Optional override of the deployed Databricks App name used to query the app's permission ACL. If unset, the runtime auto-detects it from the Databricks-Apps-injected `DATABRICKS_APP_NAME` (e.g. `ontobricks` for prod, `ontobricks-dev` for the sandbox). Useful in `.env` for local development. |
| `REGISTRY_CATALOG` | Yes | Unity Catalog catalog for the Registry Volume where `.permissions.json` is stored. |
| `REGISTRY_SCHEMA` | Yes | Unity Catalog schema for the Registry Volume. |
| `REGISTRY_VOLUME` | No | Volume name (default: `"OntoBricksRegistry"`). |

#### Permission File Location

```
/Volumes/{REGISTRY_CATALOG}/{REGISTRY_SCHEMA}/{REGISTRY_VOLUME}/.permissions.json
```

#### API Endpoints

| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/settings/permissions/me` | GET | Bypassed | Returns the current user's email, role, and `is_app_admin` flag. Used by the UI to show/hide admin elements. |
| `/settings/permissions/diag` | GET | Bypassed | Diagnostic: returns SDK auth details, CAN_MANAGE principals, cache state. |
| `/settings/permissions` | GET | Admin | Lists all permission entries. |
| `/settings/permissions` | POST | Admin | Adds or updates a permission entry. Body: `{ principal, principal_type, display_name, role }`. |
| `/settings/permissions/{principal}` | DELETE | Admin | Removes a permission entry. |
| `/settings/permissions/principals` | GET | Admin | Lists users and groups from the Databricks App permissions for the add-user picker. |

---

### Caching Behaviour

To reduce API calls, the permission system caches several results:

| Cache | TTL | What is cached |
|-------|-----|----------------|
| Admin check | 60 seconds | Per-email `is_admin` result. Cleared on every page load and `/permissions/me` call. |
| Permission file | 5 minutes | Contents of `.permissions.json`. Forced refresh on add/update/delete operations. |
| App principals | 10 minutes | Users/groups from the Databricks App permissions API. Cleared before loading the add-user picker. |

Permission changes made through the Settings UI take effect immediately
because the relevant caches are invalidated. Changes made externally
(e.g., granting `CAN_MANAGE` in the Databricks UI) are picked up on the
next page load since the admin cache is always cleared before checking.

---

### Diagnostic Endpoint

If admin access is not working as expected, visit:

```
https://<your-app-url>/settings/permissions/diag
```

This endpoint bypasses the permission middleware and returns:

```json
{
  "email": "user@example.com",
  "app_name": "ontobricks",
  "is_app_mode": true,
  "sdk_host": "https://your-workspace.cloud.databricks.com",
  "sdk_auth_type": "azure-client-secret",
  "can_manage_principals": [
    "user@example.com",
    "84eff7c5-778b-4c0b-a24d-36126bcc8cca"
  ],
  "email_is_manager": true,
  "sdk_error": null,
  "admin_cache": {
    "user@example.com": { "result": true, "age_s": 12.3 }
  }
}
```

Key fields to check:
- **`sdk_error`**: If not null, the SDK cannot read app permissions. Usually means the SP is missing `CAN_MANAGE`.
- **`can_manage_principals`**: List of all principals with `CAN_MANAGE`. Your email should appear here.
- **`email_is_manager`**: Whether the current user's email matches a `CAN_MANAGE` principal.

---

### Troubleshooting

#### "Access Denied" despite having CAN_MANAGE

1. Visit `/settings/permissions/diag` to check the SDK status.
2. Verify the SP has `CAN_MANAGE`:
   ```bash
   databricks permissions get apps ontobricks
   ```
3. If the SP is missing, grant it (see [Service Principal Setup](#service-principal-setup)).
4. Hard refresh the app (Cmd+Shift+R / Ctrl+Shift+R).

#### Settings gear icon not visible

The gear icon only appears for admins. Verify:
1. The user has `CAN_MANAGE` on the Databricks App.
2. The SP has `CAN_MANAGE` (so the admin check can succeed).
3. Hard refresh the page.

#### User added to in-app permission list but still blocked

1. The user must also have at least `CAN_USE` on the Databricks App.
   Without it, they cannot reach the app at all.
2. Check that the principal email matches exactly (case-insensitive).

#### User not appearing in the "Add" picker dropdown

The picker shows users/groups from the **Databricks App permissions**,
not all workspace users. The person must first be granted `CAN_USE` (or
`CAN_MANAGE`) on the app in the Databricks UI.

#### Permission changes not taking effect

Changes through the Settings UI are immediate. For changes made
externally (Databricks UI), hard refresh the OntoBricks app. The admin
cache is cleared on every page load.

#### Console errors: "Unexpected token '<'"

This means API calls are returning HTML instead of JSON, usually because
the permission middleware is redirecting to the access-denied page. Check
that the admin check is succeeding (see diagnostic endpoint above).
