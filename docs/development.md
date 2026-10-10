# Development

Notes for contributors: dependencies, testing, and permission / SDK notes.

<!-- toc -->
**Contents**

- [OntoBricks Dependencies](#ontobricks-dependencies)
  - [Python Dependencies](#python-dependencies)
  - [Frontend Dependencies (CDN)](#frontend-dependencies-cdn)
  - [Local/Bundled Libraries](#localbundled-libraries)
  - [Dependency Details](#dependency-details)
  - [License Summary](#license-summary)
  - [Version Compatibility](#version-compatibility)
  - [Updating Dependencies](#updating-dependencies)
  - [Security Considerations](#security-considerations)
- [OntoBricks Test Campaign](#ontobricks-test-campaign)
  - [Quick Start](#quick-start)
  - [Test Architecture](#test-architecture)
  - [What Is Tested](#what-is-tested)
  - [Coverage](#coverage)
  - [Test Conventions](#test-conventions)
  - [Continuous Integration](#continuous-integration)
  - [UI Tests](#ui-tests)
  - [Known Limitations](#known-limitations)
- [OntoBricks — Permission & Rights Management](#ontobricks--permission--rights-management)
  - [Overview](#overview)
  - [Roles](#roles)
  - [How Permission Resolution Works](#how-permission-resolution-works)
  - [Databricks App Permissions (External)](#databricks-app-permissions-external)
  - [In-App Permission List (Internal)](#in-app-permission-list-internal)
  - [Domain-Level Permissions](#domain-level-permissions)
  - [Managing Permissions — Step by Step](#managing-permissions--step-by-step)
  - [Service Principal Setup](#service-principal-setup)
  - [Local Development Mode](#local-development-mode)
  - [Settings Page Access](#settings-page-access)
  - [Access Denied Page](#access-denied-page)
  - [Configuration Reference](#configuration-reference)
  - [Caching Behaviour](#caching-behaviour)
  - [Diagnostic Endpoint](#diagnostic-endpoint)
  - [Troubleshooting](#troubleshooting)
- [Code map](#code-map)
  - [1. Architecture and Key Principles](#1-architecture-and-key-principles)
  - [2. Project Structure](#2-project-structure)
  - [3. UI (HTML pages and client navigation)](#3-ui-html-pages-and-client-navigation)
  - [4. API (REST v1, Knowledge Graph, GraphQL)](#4-api-rest-v1-knowledge-graph-graphql)
  - [5. Agents (src/agents)](#5-agents-srcagents)
  - [6. MCP server (src/mcp-server)](#6-mcp-server-srcmcp-server)
  - [7. Code Style and Patterns](#7-code-style-and-patterns)
  - [8. Error Handling](#8-error-handling)
  - [9. Logging](#9-logging)
  - [10. Performance Optimization](#10-performance-optimization)
  - [11. Testing and Deployment](#11-testing-and-deployment)
  - [12. Quick file index](#12-quick-file-index)
<!-- /toc -->

## OntoBricks Dependencies

This document describes all external dependencies used by OntoBricks, including Python packages, UI frameworks, and JavaScript libraries.

---

### Python Dependencies

#### Runtime Dependencies

| Package | Version | Description | License | Link |
|---------|---------|-------------|---------|------|
| **FastAPI** | ≥0.129.0 | Modern, fast web framework for building APIs with Python | MIT | [fastapi.tiangolo.com](https://fastapi.tiangolo.com/) |
| **Uvicorn** | ≥0.27.0 | Lightning-fast ASGI server implementation (with `standard` extras) | BSD-3-Clause | [uvicorn.org](https://uvicorn.org/) |
| **Starlette** | ≥1.3.1 | Lightweight ASGI toolkit (FastAPI dependency, pinned for middleware and security fixes) | BSD-3-Clause | [starlette.io](https://www.starlette.io/) |
| **Jinja2** | ≥3.1.0 | Template engine for Python | BSD-3-Clause | [jinja.palletsprojects.com](https://jinja.palletsprojects.com/) |
| **pydantic** | ≥2.5.0 | Data validation using Python type hints | MIT | [docs.pydantic.dev](https://docs.pydantic.dev/) |
| **pydantic-settings** | ≥2.14.2 | Settings management with environment variable loading (BaseSettings) | MIT | [docs.pydantic.dev/latest/concepts/pydantic_settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) |
| **python-multipart** | ≥0.0.30 | Multipart form data parsing for file uploads in FastAPI | Apache-2.0 | [pypi.org/project/python-multipart](https://pypi.org/project/python-multipart/) |
| **itsdangerous** | ≥2.1.0 | Cryptographic signing utilities (available for session security) | BSD-3-Clause | [palletsprojects.com/p/itsdangerous](https://palletsprojects.com/p/itsdangerous/) |
| **aiofiles** | ≥23.0.0 | Async file I/O for Starlette static file serving | Apache-2.0 | [pypi.org/project/aiofiles](https://pypi.org/project/aiofiles/) |
| **databricks-sql-connector[kernel]** | ≥4.4.0 | Python SQL connector with the native Kernel backend used for Lakehouse/RT Statement Execution API queries | Apache-2.0 | [pypi.org/project/databricks-sql-connector](https://pypi.org/project/databricks-sql-connector/) |
| **databricks-sql-kernel** | ≥1.0.0,<2.0.0 | Native Rust SQL client core installed by the connector's `kernel` extra | Apache-2.0 | [pypi.org/project/databricks-sql-kernel](https://pypi.org/project/databricks-sql-kernel/) |
| **databricks-sdk** | ≥0.20.0 | Databricks SDK for workspace API, OAuth authentication, and auto-auth in App mode | Apache-2.0 | [pypi.org/project/databricks-sdk](https://pypi.org/project/databricks-sdk/) |
| **pyarrow** | ≥23.0.1 | Columnar in-memory data format (required by databricks-sql-connector) | Apache-2.0 | [arrow.apache.org/docs/python](https://arrow.apache.org/docs/python/) |
| **RDFLib** | ≥7.6.0 | Python library for working with RDF (Resource Description Framework) | BSD-3-Clause | [rdflib.readthedocs.io](https://rdflib.readthedocs.io/) |
| **python-dotenv** | ≥1.2.2 | Load environment variables from `.env` files | BSD-3-Clause | [pypi.org/project/python-dotenv](https://pypi.org/project/python-dotenv/) |
| **requests** | ≥2.33.0 | HTTP library for Python | Apache-2.0 | [requests.readthedocs.io](https://requests.readthedocs.io/) |
| **psycopg[binary,pool]** | ≥3.2.0 | Postgres driver + connection pool used by the Lakebase Graph DB engine and the Lakebase registry backend | LGPL-3.0 | [psycopg.org/psycopg3](https://www.psycopg.org/psycopg3/) |
| **APScheduler** | ≥3.10.0 | Advanced Python Scheduler for background jobs (used by BuildScheduler for scheduled triple store builds) | MIT | [pypi.org/project/APScheduler](https://pypi.org/project/APScheduler/) |
| **owlrl** | ≥7.0.0 | OWL 2 RL forward-chaining reasoner — performs deductive closure on RDFLib graphs for ontology-level inference | W3C | [owl-rl.readthedocs.io](https://owl-rl.readthedocs.io/) |
| **pyshacl** | ≥0.26.0 | W3C SHACL validator for RDFLib graphs — validates RDF data against SHACL shapes for data quality checks | Apache-2.0 | [github.com/RDFLib/pySHACL](https://github.com/RDFLib/pySHACL) |
| **NetworkX** | ≥3.0 | Graph analysis library — server-side community detection (Louvain, Label Propagation, Greedy Modularity) on the full graph viewer | BSD-3-Clause | [networkx.org](https://networkx.org/) |
| **strawberry-graphql[fastapi]** | ≥0.315.7 | GraphQL library for Python — auto-generates typed schema from ontology and integrates with FastAPI | MIT | [strawberry.rocks](https://strawberry.rocks/) |
| **MLflow** | ≥3.16.1 | ML lifecycle platform — used for agent tracing, evaluation, and the Databricks Agent Framework (ResponsesAgent) | Apache-2.0 | [mlflow.org](https://mlflow.org/) |

##### MCP Server Additional Dependencies

These packages are used by the MCP server (`src/mcp-server/`) which runs as a separate Databricks App:

| Package | Version | Description | License | Link |
|---------|---------|-------------|---------|------|
| **fastmcp** | ≥2.3.1 | Model Context Protocol (MCP) server SDK for exposing tools to LLMs | Apache-2.0 | [gofastmcp.com](https://gofastmcp.com/) |
| **httpx** | ≥0.25.0 | Async HTTP client (used by MCP server to call the main app REST API) | BSD-3-Clause | [www.python-httpx.org](https://www.python-httpx.org/) |
| **cryptography** | ≥50.0.0 | MCP transitive security floor (PKCS#7 oracle fix) | Apache-2.0 / BSD-3-Clause | [cryptography.io](https://cryptography.io/) |

#### Development Dependencies

| Package | Version | Description | License | Link |
|---------|---------|-------------|---------|------|
| **pytest** | ≥9.0.3 | Testing framework for Python | MIT | [docs.pytest.org](https://docs.pytest.org/) |
| **pytest-asyncio** | ≥0.21.0 | Async test support for pytest | MIT | [pypi.org/project/pytest-asyncio](https://pypi.org/project/pytest-asyncio/) |
| **pytest-cov** | ≥4.1.0 | Code coverage plugin for pytest | MIT | [pypi.org/project/pytest-cov](https://pypi.org/project/pytest-cov/) |
| **httpx** | ≥0.25.0 | HTTP client for testing FastAPI applications | BSD-3-Clause | [www.python-httpx.org](https://www.python-httpx.org/) |
| **responses** | ≥0.24.0 | Mock HTTP request/response library for unit tests | Apache-2.0 | [pypi.org/project/responses](https://pypi.org/project/responses/) |
| **playwright** | ≥1.40.0 | Browser automation for end-to-end tests | Apache-2.0 | [playwright.dev/python](https://playwright.dev/python/) |
| **black** | ≥26.3.1 | Python code formatter | MIT | [black.readthedocs.io](https://black.readthedocs.io/) |
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
| **License** | [Databricks License](../LICENSE.txt) |
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

Used for data-driven DOM manipulation and graph data processing in the Knowledge Graph page:
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

Powers the WebGL-accelerated graph visualization in the Knowledge Graph "Graph Viewer" view:
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
| **Databricks License** | OntoViz (first-party OntoBricks component) |
| **MIT** | FastAPI, pydantic, pydantic-settings, Bootstrap, Bootstrap Icons, Sigma.js, Graphology, Grid.js, Chart.js, strawberry-graphql, pytest, pytest-asyncio, pytest-cov, black, flake8 |
| **BSD-3-Clause** | Uvicorn, Starlette, Jinja2, itsdangerous, RDFLib, python-dotenv, httpx, NetworkX |
| **Apache-2.0** | databricks-sql-connector, databricks-sdk, pyarrow, python-multipart, aiofiles, requests, fastmcp, MLflow, pyshacl, responses, playwright |
| **LGPL-3.0** | psycopg (binary + pool) |
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
# Update all dependencies from the public index, then install the new lock.
UV_INDEX_URL=https://pypi.org/simple PIP_INDEX_URL=https://pypi.org/simple \
  uv lock --upgrade
uv sync --frozen --extra lakebase

# Update a specific package.
UV_INDEX_URL=https://pypi.org/simple PIP_INDEX_URL=https://pypi.org/simple \
  uv lock --upgrade-package fastapi
uv sync --frozen --extra lakebase
```

Both `uv.lock` files are deployment artifacts. `registry =
"https://pypi.org/simple"` identifies the package index used for resolution;
the pinned `url = "https://files.pythonhosted.org/..."` entries are the wheel
and source downloads used by `uv run --frozen` in Databricks Apps. Never commit
`pypi-proxy.dev.databricks.com` URLs. For routine install, run, and test
commands, always pass `--frozen` so uv cannot rewrite the lock.

Current explicit security floors include
`databricks-sql-connector>=4.4.0`, `mlflow>=3.16.1`, `nltk>=3.10.3`,
`sentence-transformers>=5.6.0`, `pip>=26.2.0`, `transformers>=5.10.0`,
`gitpython>=3.1.62`, `pyjwt>=2.15.0`, `urllib3>=2.8.0`, `anyio>=4.14.2`,
and `cryptography>=50.0.0`. `oauthlib>=4` is blocked by
`databricks-sql-connector` (`oauthlib<4`). `pyproject.toml` remains
authoritative.

#### Frontend Dependencies

Frontend libraries are loaded via CDN with pinned versions. To update:

1. Edit `src/front/templates/base.html` for Bootstrap
2. Edit `src/front/templates/dtwin.html` for D3.js and Grid.js
3. Test thoroughly after updates

---

### Security Considerations

- All CDN resources use HTTPS
- Python packages resolve through `pypi.org/simple`; frozen installs download
  pinned artifacts from `files.pythonhosted.org`, never the internal proxy
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
uv sync --frozen --extra lakebase

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
  test_dtwin_service.py             # Knowledge Graph domain class (back.objects.digitaltwin)
  test_mapping_service.py           # Mapping domain class (back.objects.mapping.Mapping)
  test_domain_service.py            # Domain class (back.objects.domain.Domain)
  test_domain_session.py            # DomainSession state management
  test_databricks_client.py         # Databricks client (mocked)
  test_llm_utils.py                 # LLM retry logic
  test_triplestore_factory.py       # Graph DB factory auto-resolve + view path
  test_graphdb_factory.py           # GraphDB engine factory
  test_lakebase_flat_store.py       # Lakebase Postgres flat-store backend
  test_synced_table_manager.py      # Lakeflow synced-table orchestration (managed_synced)
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
| `test_triplestore_factory.py` | 6 | `back.core.graphdb.GraphDBFactory` | Unknown engine handling, auto-resolve to lakebase/delta, resolved engine_config passthrough, view store host/success paths (mocked) |

#### P1b -- Lakebase Graph DB Tests

These test the Lakebase Postgres graph engine. Heavy paths (`COPY FROM
STDIN`, Lakeflow synced-table orchestration) are mocked at the
`psycopg`/SDK boundary so the suite runs without a live Postgres or
Databricks workspace.

| Test File | Module Under Test | What Is Verified |
|-----------|-------------------|------------------|
| `test_lakebase_flat_store.py` | `back.core.graphdb.lakebase.LakebaseFlatStore` | Table lifecycle, `insert_triples` (small + bulk paths via `bulk_insert_iter`), `query_triples`, `count_triples`, `delete_triples`, `bulk_delete_iter`, named queries (BFS, transitive closure, symmetric expansion), capability flags |
| `test_synced_table_manager.py` | `back.core.graphdb.lakebase.SyncedTableManager` | Idempotent `ensure`, `_build_synced_table_payload`, refresh trigger, polling, race-handling on `ALREADY_EXISTS`, deletion |
| `test_graphdb_factory.py` | `back.core.graphdb.GraphDBFactory` | Default engine resolution (`lakebase`), `engine_config` propagation, unknown-engine handling |

#### P2 -- HTTP Route Tests (59 tests)

End-to-end HTTP tests using Starlette's `TestClient`. These verify that routes are reachable, return correct status codes, and produce expected response shapes.

| Test Class | Endpoints Covered |
|------------|-------------------|
| `TestHealthRoutes` | `GET /health` (readiness probe — verifies the response shape and that `/health/detailed` has been retired) |
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
| `back/core/graphdb/lakebase/LakebaseFlatStore.py` | ~80% | Triple CRUD, bulk paths and named queries covered with mocked psycopg |
| `back/core/graphdb/lakebase/SyncedTableManager.py` | ~85% | `ensure` idempotency, refresh trigger, polling and delete fully tested with mocked Databricks SDK |

Modules with 0% coverage (`back/core/w3c/rdfs`, `back/core/sqlwizard`, `back/core/databricks/unity_catalog/MetadataService.py`) are either not yet tested or depend entirely on external services.

---

### Test Conventions

#### Naming

- Test files: `test_<module_name>.py`
- Test classes: `Test<FunctionalArea>` (e.g., `TestGetClasses`, `TestEntityMapping`)
- Test functions: `test_<what_is_being_tested>` (e.g., `test_returns_defined_classes`, `test_missing_name_uses_default`)

#### Markers

Defined in `pytest.ini` (see the file for the authoritative list):

```ini
markers =
    unit: Unit tests (pure in-process, no I/O — default)
    integration: Multi-module, in-proc backends
    contract: Schema/contract verification
    e2e: Playwright browser flows (CI nightly only)
    scenario: Live end-to-end journey suites under tests/e2e/scenarios/ (opt-in)
    dependency: Cross-scenario ordering (pytest-dependency; campaign only)
    db / spark / external / live_integration / property / slow / eval / mcp: see pytest.ini
```

Use them to selectively run subsets:

```bash
# Routine / agent runs — everything except the opt-in live scenarios:
uv run --frozen pytest -q -m "not scenario"

.venv/bin/python -m pytest -m unit
.venv/bin/python -m pytest -m integration
.venv/bin/python -m pytest -m "not slow"
```

#### Live scenario campaign

The suites under `tests/e2e/scenarios/` (`test_scenario_01_generate_live.py` →
`02` → `03` → `test_scenario_validation.py`) are a **single, ordered, billable
journey** (warehouse + LLM + a durable registry write) against a **running**
app. They are `scenario`-marked, excluded from routine runs, and gated behind
`ONTOBRICKS_SCENARIO_LIVE=1`. Run the whole campaign with:

```bash
make scenario-campaign                                   # local dev server
make scenario-campaign ONTOBRICKS_LIVE_BASE=https://<app-url>
```

This sets `ONTOBRICKS_SCENARIO_LIVE=1` + `ONTOBRICKS_SCENARIO_CHAIN=1`
(pytest-dependency chaining: an upstream failure skips the downstream suites),
runs them in filename order via `uv run --frozen` with `VIRTUAL_ENV` cleared
(so a foreign worktree venv cannot hijack pytest), and writes reports to
`artifacts/scenarios/` (`campaign.xml`, `campaign.html`, `campaign_report.md`).
Scenario 1 drives the staged Generate wizard (Detect Entities → review →
Complete), not the 0.8 one-shot Generate click. The canonical guide
for environment variables, chaining, registry isolation, and adding scenarios
is `tests/e2e/scenarios/README.md`.

Scenario 4 additionally needs a Databricks profile/token and SQL warehouse ID
exported in the pytest shell because it creates and drops scratch UC tables.
The deterministic full-lifecycle browser journey can be run independently
with `ONTOBRICKS_E2E_FAKE_CREDS=1`; see the canonical scenario guide.

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
uv sync --frozen --extra lakebase

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
| All pages | Navbar present, brand link, notification container, Bootstrap/utils.js scripts, nav dropdowns (Registry, Domain, Knowledge Graph), Ontology/Mapping links under Domain dropdown, Settings link, warehouse status, task tracker |
| Home `/` | Hero section, domain panel, stat items (Entities, Relationships, Mappings), quick links (Settings, About) |
| Settings `/settings` | Connection form, host/token/warehouse displays, Test Connection button, base URI field, Save button |
| Registry modal (`/?open=registry`) | Navbar boxes icon (`bi-boxes`) → Browse + Bridges modal |
| Settings `/settings` | Databricks/Global/backends config; Admin → Teams; Automation (Scheduler, Runs — admin only, all domains); Developer → API |
| Ontology `/ontology` | Sidebar section groups: Ontology Editor (Information, Import, Generate, Studio, Business Views, Entities, Relationships), Advanced (Data Quality, Business Rules, Expr. & Axioms); Studio **Export** (OWL); section divs, OntoViz script |
| Mapping `/mapping` | Sidebar with section links (Information, Studio, Manual, Auto-Map, Spark SQL), Studio **Export** (R2RML), mapping-core.js |
| Domain `/domain` | Sidebar with 6 section links (Information, Metadata, Documents, Validation, OWL, R2RML), section divs |
| Knowledge Graph `/dtwin/` | Sidebar section groups: Navigation (Graph Viewer, GraphQL), Advanced (Data Quality, Reasoning); Sigma.js script |
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
| `TestDigitalTwinSidebar` | Graph Viewer section visible by default, sidebar navigation links present |
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
| **Admin** | Databricks App `CAN_MANAGE` permission | Full access. Can create domains in the registry, view, edit, build, and manage the Settings page including the permission list. |
| **Builder** | In-app permission list | Can view, edit, **import and manage data sources**, and **build graph viewers**. Cannot change shared Settings (SQL Warehouse, Teams, branding). |
| **Editor** | In-app permission list | Can view all pages and modify assigned domains, ontologies, mappings, and **data sources**. Cannot create a domain (Admin only). **Cannot build graph viewers.** Cannot change shared Settings. |
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
| `role != admin` + path starts with `/settings` | JSON 403 or redirect to `/`, except an explicit GET allow-list (registry/warehouse status **and** Unity Catalog catalog/schema browse used by **Add Data Source**). Settings writes and warehouse selection stay admin-only. |
| Digital twin build + effective domain role < `builder` | JSON 403: "Only builders and admins can build a graph viewer". |
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

#### Adding a User or Group (domain roles)

1. Open OntoBricks and click the **gear icon** (⚙) in the navbar to go
   to Settings.
2. Open **Admin → Teams**.
3. Assign **Viewer**, **Editor**, or **Builder** roles per domain in the
   matrix. Principals listed there come from Databricks App permissions
   (`CAN_USE` / `CAN_MANAGE`).
4. Click **Save**. Changes are written to the Registry.

#### Changing or clearing a role

In the Teams matrix, use the role dropdown for a principal/domain cell.
Clear a role to revoke domain access. Save when done.

#### Important Notes

- App-level access is managed only in **Databricks → Apps → Permissions**.
  OntoBricks no longer has a separate Settings → Permissions page.
- Users with `CAN_MANAGE` always have full admin access regardless of
  domain-role assignments.
- The Teams matrix only shows principals that already have permissions
  on the Databricks App. To add someone who doesn't appear, first grant
  them `CAN_USE` on the Databricks App.

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
- The Settings page and Admin → Teams matrix work normally.
- No Databricks App permissions or `.permissions.json` are checked.

This allows local development and testing without any permission setup.

---

### Settings Page Access

The entire Settings page (`/settings` and all sub-routes) is restricted
to **admin users only**. This includes:

- The General tab (Databricks connection, SQL Warehouse).
- The Registry tab (Unity Catalog Volume configuration).
- The Admin → Teams matrix (per-domain role management).
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
| `/settings/permissions` | GET | Admin | Lists Databricks App principals (users + groups). Used by Teams. |
| `/settings/permissions/principals` | GET | Admin | Lists users and groups from the Databricks App permissions for the add-user picker. |

---

### Caching Behaviour

To reduce API calls, the permission system caches several results:

| Cache | TTL | What is cached |
|-------|-----|----------------|
| Admin check | 60 seconds | Per-email `is_admin` result. Cleared on every page load and `/permissions/me` call. |
| Permission file | 5 minutes | Contents of `.permissions.json`. Forced refresh on add/update/delete operations. |
| App principals | 10 minutes | Users/groups from the Databricks App permissions API. Cleared before loading the add-user picker. |

Permission changes made through Settings → Admin → Teams take effect immediately
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

---

## Code map

This document maps how the **main OntoBricks application** is wired: the browser UI, programmatic REST/GraphQL APIs, LLM **agents**, and the separate **MCP** server. It also covers **coding conventions**, **error handling**, **logging**, **project structure**, and **contributor workflows**.

The FastAPI entrypoint is `src/shared/fastapi/main.py` (`create_app`, `app = create_app()`). Application source is split across five top-level packages under `src/`: **`back`** (domain, core infra, GraphQL, services), **`front`** (HTML routes, Jinja2, menu config, templates, static assets), **`shared`** (app factory, middleware, health, settings/constants), **`api`** (external REST and internal JSON routers), and **`agents`** (LLM engines).

---

### 1. Architecture and Key Principles

#### 1.1 Layered architecture

All code follows a strict three-layer stack: **routes (HTTP)** → **domain classes** (`back/objects`) → **core infrastructure** (`back/core`).

- **Domain classes own all business logic.** Routes are thin: extract parameters, call a domain method, return response.
- **`service.py` files are deprecated.** The preferred pattern is routes → domain class directly. Existing service files are legacy and should be migrated.
- Core infrastructure (`back/core`) has **no dependency on HTTP/FastAPI types**.
- Domain objects (`back/objects`) have **no `Request`/`Response` inside the package**.

#### 1.2 Naming conventions

- Python files containing a class: **PascalCase** matching the class name (e.g., `OWLRLReasoner.py`, `SWRLSQLTranslator.py`).
- Python files without a primary class: **lowercase with underscores** (e.g., `constants.py`, `models.py`).
- CSS/JS assets: lowercase with dashes, organised by feature area under `front/static/`.

#### 1.3 Class-first policy

| Guideline | Detail |
|-----------|--------|
| Default to a service class | When adding new functionality, create a class that owns the state and methods. |
| Constructor receives dependencies | Pass configuration, credentials, or collaborators via `__init__` for testability. |
| Methods over functions | Reserve module-level functions only for thin factory/accessor helpers or pure stateless utilities. |
| One public class per file | Name the file after the class in PascalCase. Small helper dataclasses in the same file are fine. Shared constants go in `constants.py`, shared DTOs in `models.py`. |

#### 1.4 Package organisation

- Every subpackage `__init__.py` must re-export public symbols. Consumers import from the package, never from internal modules.
- Use `__all__` when the public surface is well-defined.
- Keep `__init__.py` files short — only imports, `__all__`, and thin backward-compatible wrappers.
- When refactoring functions into class methods, provide backward-compatible wrappers in `__init__.py`.

#### 1.5 Domain security

- A domain must **never** include tokens, passwords, or other secrets.
- A domain must **never** include query results or transient data.
- If you see secrets in domain structures, move them to configuration, env vars, or temporary storage.

---

### 2. Project Structure

#### 2.1 Top-level packages

| Package | Purpose |
|---------|---------|
| `src/back/` | Backend: domain classes (`objects/`), core infra (`core/`), GraphQL routes (`fastapi/`), services (`services/`). |
| `src/front/` | Frontend: HTML-only routes (`routes/`), Jinja2 config (`fastapi/`), menu config (`config/`), templates (`templates/`), static assets (`static/`). |
| `src/shared/` | Shared infrastructure: app factory & middleware (`fastapi/`), settings & constants (`config/`). |
| `src/api/` | API layer: external REST API (`routers/v1.py` etc.), internal JSON API (`routers/internal/`). |
| `src/agents/` | LLM agent engines (OWL generator, auto-assign, icon assign, assistant). |

#### 2.2 Three-layer FastAPI split

| Package | Contents | Depends on |
|---------|----------|------------|
| `shared/fastapi/` | Application factory (`main.py`), permission middleware, router registration, health routes (`health.py`). | `front`, `back`, `api` |
| `front/fastapi/` | Jinja2 template config, custom filters, menu globals (`dependencies.py`). | `front/config/` |
| `back/fastapi/` | GraphQL API (`graphql_routes.py`). | `back/core/`, `back/objects/` |

Entry point: `run.py` imports `create_app` from `shared.fastapi.main`.

#### 2.3 Frontend structure

```
src/front/
├── fastapi/          <- Jinja2 rendering configuration (dependencies.py)
├── config/           <- Frontend-only configuration (menu_config.json)
├── routes/           <- One file per page, TemplateResponse only
├── templates/        <- Consolidated Jinja2 templates
│   ├── base.html, home.html, ontology.html, mapping.html, ...
│   └── partials/     <- Grouped by topic (layout/, ontology/, mapping/, dtwin/, domain/)
└── static/           <- CSS, JS, images (global/, ontology/, mapping/, domain/, …)
```

#### 2.4 Backend domain objects (`src/back/objects/`)

```
back/objects/
├── registry/         <- UC domain registry, permissions, scheduled builds
├── session/          <- HTTP session storage, domain state, instance config
├── domain/           <- Session-scoped UC/metadata/layout operations
├── ontology/         <- Ontology domain class
├── mapping/          <- Mapping domain class
└── digitaltwin/      <- Knowledge Graph domain class
```

#### 2.5 Backend core infrastructure (`src/back/core/`)

```
back/core/
├── agents/           <- AgentClient (LLM gateway)
├── errors/           <- OntoBricksError hierarchy
├── helpers/          <- Cross-cutting utilities (DatabricksHelpers, SQLHelpers, URIHelpers)
├── logging/          <- LogManager, setup_logging(), get_logger()
├── task_manager/     <- Async in-memory task tracking
├── graphdb/          <- Single triple store / graph DB abstraction (Lakebase Postgres, Delta UC, view)
├── databricks/       <- Typed facades for Databricks API surfaces
│   └── lakebase/     <- Shared Lakebase (Postgres) technical access: auth,
│                        JWT mint, connection pool, psycopg gate, grants.
│                        Consumed by both the registry store and the graph
│                        triple store (two independent databases).
├── graphql/          <- Strawberry GraphQL schema builder
├── w3c/              <- W3C standard parsers/generators (OWL, RDFS, R2RML, SPARQL, SHACL)
├── industry/         <- Standard industry ontology importers (CDISC, FIBO, IOF)
├── graph_analysis/   <- Community detection (NetworkX backend)
├── reasoning/        <- OWL 2 RL, SWRL, decision tables, SPARQL rules, aggregates
├── sqlwizard/        <- LLM-assisted SQL generation
├── mcp_tools.py      <- Catalog of the configurable MCP surface + policy coercion
└── query_limits.py   <- Shared query/row caps
```

The two flat modules are deliberately not packages: they hold constants and
pure functions with no state and no subcomponents.

**`mcp_tools.py`** owns the single definition of what a domain can configure
over MCP — the registry-level tools that are always exposed, the domain-scoped
tools that are not, the three ontology attachments, and their modes — plus
`coerce_mcp_policy()`, which normalizes any persisted or user-submitted blob
(dropping unknown or registry-level entries, never raising). The Domain
Information MCP tab, the domain service and the external REST contract all
resolve the surface here so they cannot drift. The MCP server process
deliberately does **not** import it: it only ever applies a policy it received
over REST, and the two packages cannot import each other.

#### 2.6 API layer (`src/api/`)

```
api/
├── routers/
│   ├── v1.py, domains.py, digitaltwin.py   <- External API (/api/v1/*)
│   └── internal/                           <- Internal API (session-aware JSON)
│       ├── home.py, settings.py, ontology.py, mapping.py, dtwin.py, domain.py, tasks.py
│       └── _helpers.py
└── external_app.py
```

#### 2.7 Where to put new code

| Location | Purpose |
|----------|---------|
| `back/core/<pkg>/` | Shared infrastructure (Databricks, graphdb, W3C, reasoning, …). No HTTP dependency. |
| `back/objects/<pkg>/` | Application domain classes (Ontology, Mapping, DigitalTwin, Domain, registry, session). No `Request`/`Response`. |
| `front/routes/<area>.py` | HTML routing only — parameter extraction, domain method call, `TemplateResponse`. |
| `api/routers/internal/` | Internal JSON API — session-aware, delegates to domain classes. |
| `api/routers/v1.py` etc. | External REST API — stateless. |

**Rule of thumb:** If the logic is about **this app's domain/registry/session model**, put it in `back/objects`. If it could be reused by a different app talking to Databricks or RDF standards, put it in `back/core`. If it is purely about HTML or HTTP wiring, keep it in `front/` routes.

---

### 3. UI (HTML pages and client navigation)

#### 3.1 How requests reach the UI

1. **Middleware stack** (order matters; Starlette runs *last added* first on the way *in*):  
   - **CORS** — allows credentials for local dev.  
   - **PermissionMiddleware** — when running as a Databricks App, resolves the user role from registry permissions and blocks viewers from mutating HTTP methods; bypasses `/static/`, `/api/`, `/graphql/`, OpenAPI, health, etc. Local dev behaves as admin.  
   - **FileSessionMiddleware** — cookie-backed **file sessions** (JSON on disk under `settings.session_dir`); skips static, docs, health, and `/tasks/*` so task polling does not churn session I/O. The cookie must be 32 lowercase hex characters, since it is used as a filename; anything else mints a fresh ID. Files are written only once a session is actually modified, their mtime records last use (refreshed at most hourly), and `reap_expired_sessions()` clears ones older than `settings.session_max_age` at startup.

2. **Static files** are mounted at `/static` from `src/front/static/` (with a fallback path for unusual layouts).

3. **HTML routers** are plain FastAPI `APIRouter` modules under `src/front/routes/` (e.g. `home.py`, `ontology.py`). Each module declares a **path prefix** (except home) and returns `templates.TemplateResponse(...)` for full pages or JSON for XHR endpoints used by the SPA-like panels. Session-aware **JSON** endpoints used by the UI (settings, tasks, navbar state, etc.) live under **`src/api/routers/internal/`** and are registered alongside the HTML routers from `shared/fastapi/main.py`.

#### 3.2 Template loading (Jinja2)

`src/front/fastapi/dependencies.py` builds a single `Jinja2Templates` instance whose **search path** is centered on:

- **`src/front/templates/`** — top-level page templates (e.g. `ontology.html`, `base.html`).
- **`src/front/templates/partials/`** — shared fragments, with feature subfolders: **`layout/`**, **`ontology/`**, **`mapping/`**, **`dtwin/`**, **`domain/`** (each added to the Jinja search path so includes resolve cleanly).

Templates are named by file (e.g. `ontology.html`) as long as the name is unique across the search path.

Custom Jinja helpers include **`url_for`** (static + `request.url_for`), a **`range`** filter, and **`triplestore_page_context`** (shared template context builder for `view_table`, `graph_name`, and `triplestore_cache` used by dtwin and domain pages).

#### 3.3 UI route map (high level)

| Prefix / path | Module | Typical templates | Role |
|---------------|--------|-------------------|------|
| `/`, `/about`, `/settings` (HTML), `/access-denied`, status endpoints for navbar | `front/routes/home.py` | `home.html`, `about.html`, `settings.html`, `access_denied.html` | Landing, about, settings **page shell**, consolidated `/navbar/state`, session/ontology status |
| `/settings/*` (API) | `api/routers/internal/settings.py` | (mostly JSON) | Load/save Databricks config, test connection, permissions helpers |
| `/ontology/*` | `front/routes/ontology.py` | `ontology.html`, fragments | Ontology editor, SHACL, industry catalogs, **agent**-backed flows (chat, OWL generation, icons) |
| `/mapping/*` | `front/routes/mapping.py` | mapping templates | R2RML / table–ontology mapping UI |
| `/dtwin/*` | `front/routes/dtwin.py` | dtwin templates | SPARQL, graph exploration, triple-store–backed "graph viewer" UI |
| `/domain/*` | `front/routes/domain.py` | domain templates | Domain JSON in UC volumes, versioning, metadata, documents |
| `/registry/` | `front/routes/registry.py` | redirects to `/?open=registry` | Opens Registry modal (Browse + Bridges) on Home |
| `/resolve` | `front/routes/resolve.py` | — (redirect) | Entity URI resolution — finds the owning domain and redirects to the graph viewer |
| `/tasks/*` | `api/routers/internal/tasks.py` | — | Task list/detail JSON for long-running work |

**Note:** The **settings HTML page** is served at **`GET /settings`** from **home** routes; **internal settings** routes use the same **`/settings`** prefix for **JSON APIs** (`/settings/current`, `/settings/save`, etc.).

**XHR / JSON:** Besides **`tasks`**, the ontology, mapping, graph viewer, and domain UIs call session-aware JSON handlers in **`api/routers/internal/ontology.py`**, **`mapping.py`**, **`dtwin.py`**, and **`domain.py`** (and **`home.py`** for shared navbar/session helpers). Paths align with the same feature areas as the HTML routers above.

#### 3.4 Menu and client-side navigation

- **Declarative menu:** `src/front/config/menu_config.json` defines sections (Domain, Ontology, Mapping, Knowledge Graph, …), **routes** (e.g. `/domain/`), top-level Bootstrap Icons, groups, items, and `navbar_actions` (e.g. `domainSave`). The server can expose this to the client or the client can load it; either way it is the **source of truth** for section IDs, icons, and default tabs. When changing a top-level `icon`, also update `breadcrumb.js` `_ROUTE_MAP` (and any hardcoded labels such as the help walkthrough); `tests/units/front/test_knowledge_graph_icon.py` guards Domain ≠ Knowledge Graph and menu ≡ breadcrumb for `/dtwin/`.
- **JavaScript:** under `src/front/static/global/js/` (sidebar, navbar, domain actions). Pages load shared layout partials from templates and drive **in-page tabs** via menu item IDs rather than full page loads for every interaction.

#### 3.5 Core UI concerns (conceptual)

- **SessionManager / `get_domain`:** Most handlers depend on `SessionManager` to read/write the **current domain blob** (ontology, mappings, registry pointers, triple-store options) in the file session.
- **Databricks integration:** UI services call helpers such as `get_databricks_client`, `get_databricks_host_and_token`, `resolve_warehouse_id`, and `VolumeFileService` to run SQL, read/write Unity Catalog volumes, and align with app vs. PAT auth.
- **TaskManager:** Long operations (builds, agent loops) register tasks under `/tasks/{id}` so the UI can poll progress without holding a request open.

#### 3.6 Frontend code guidelines

- HTML templates use Jinja2 with `base.html` and partial templates for modularity.
- Templates must **NOT** contain inline CSS or JavaScript.
- Treat the OntoViz JS/CSS as a reusable library across pages.
- Put shared, reusable front-end code (CSS, JS, images) under `front/static/`.

**JavaScript extraction procedure** (when moving inline JS out of templates):
1. If a JS file with the same area name does not exist, create it in `src/front/static/<area>/js/` (e.g., `mapping`, `domain`, `ontology`).
2. Move the JavaScript code into that file.
3. Remove the moved JavaScript from the HTML template and add a `<script src="...">` reference to the new JS file.

Apply the same procedure for inline CSS → `src/front/static/<area>/css/`.

---

### 4. API (REST v1, Knowledge Graph, GraphQL)

#### 4.1 Registration

In `src/shared/fastapi/main.py`, `_register_routers` mounts:

- **Health** — `shared/fastapi/health.py` (app health; may overlap conceptually with v1 `/health`).
- **External REST v1** — `src/api/routers/v1.py` at **`/api/v1`** (stateless; credentials in body or headers), exposed via the mounted external API app (see `api.external_app` and `EXTERNAL_API_MOUNT_PREFIX`).
- **Knowledge Graph API** — `src/api/routers/digitaltwin.py` at **`/api/v1/digitaltwin`** (registry, domain artifacts, build, triples, quality, reasoning).
- **GraphQL** — `src/back/fastapi/graphql_routes.py` at **`/graphql`** and on the external app (see `api.external_app`; per-domain GraphQL execution).

The **OpenAPI** document is at `/openapi.json`; interactive docs at `/docs` and `/redoc`.

#### 4.2 `/api/v1` (stateless integration API)

Defined in `api/routers/v1.py`. Representative endpoints:

- **`POST /api/v1/domains/list`**, **`POST /api/v1/domain/info`**, ontology/classes/properties, mappings, R2RML extraction.
- **`POST /api/v1/query`** — SPARQL execution with `engine` choice (e.g. local vs Spark).
- **`POST /api/v1/query/validate`**, **`POST /api/v1/query/samples`**.

Pydantic models encode UC location (`catalog` / `schema` / `volume`) and optional `databricks_host` / `databricks_token`. Business logic for the external REST layer is delegated to `api.service` where appropriate.

#### 4.3 `/api/v1/domains` and `/api/v1/domain` (registry list & artifacts)

Defined in `api/routers/domains.py`. Representative paths:

| Method | Path | Purpose (summary) |
|--------|------|-------------------|
| GET | `/api/v1/domains` | List MCP-exposed registry domains with `mcp_policy`, `graph_backend`, and `has_graph` |
| GET | `/api/v1/domain/classes` | Per-class dataset / bridge / action metadata, filtered by the domain's MCP context policy |
| GET | `/api/v1/domain/versions`, `/domain/design-status` | Versions and design readiness |
| GET | `/api/v1/domain/ontology`, `/api/v1/domain/r2rml`, `/api/v1/domain/sparksql` | Serialized design artifacts |

#### 4.4 `/api/v1/digitaltwin` (Knowledge Graph)

Defined in `api/routers/digitaltwin.py`. Representative paths:

| Method | Path | Purpose (summary) |
|--------|------|-------------------|
| GET | `/registry` | Registry location (catalog, schema, volume) |
| GET | `/status`, `/stats` | Triple store status and statistics |
| POST | `/build` + GET `/build/{task_id}` | Trigger and poll materialization |
| GET | `/triples`, `/triples/find` | Raw or navigated triple access |
| POST | `/dataquality/start` + GET `/dataquality/{task_id}` | Async data quality |
| POST | `/reasoning/start` + GET `/reasoning/{task_id}`, GET `/reasoning/results` | Reasoning jobs and results |

These endpoints combine **registry configuration** (Unity Catalog volume for the domain index) with **per-domain** parameters and often use the same Databricks credential resolution patterns as the UI.

#### 4.5 GraphQL

`back/fastapi/graphql_routes.py` wires `back.core.graphql` to expose an **ontology-derived schema** for a named domain (`GET /graphql`, `GET/POST /graphql/{domain}`, schema sub-routes). It complements SPARQL with a typed graph API.

#### 4.6 Core API concerns (conceptual)

- **No browser session required** for v1/digitaltwin when callers pass credentials and UC paths; the **UI** relies on **FileSessionMiddleware** instead.
- **PermissionMiddleware** bypasses `/api/` and `/graphql/` prefixes at enforcement time today; authorization for external callers is primarily **token + UC ACLs**.
- **Async vs sync:** Some services use `run_blocking` to call blocking Databricks or RDF libraries from async routes; routes must **await** those helpers to avoid coroutine bugs.

---

### 5. Agents (`src/agents`)

#### 5.1 Purpose

Agents are **LLM loops with tools**: they are invoked from **ontology** routes (and similar) for assisted editing, generation, or batch suggestions. They are **not** separate HTTP servers; they run **in-process** inside the main app worker.

#### 5.2 Agent packages

Each agent typically has:

- **`engine.py`** — orchestration: system prompt, iteration budget, `AgentResult` / step tracing.
- **`tools.py`** — tool definitions and handlers that mutate or read **in-memory ontology state** / context.
- **`__init__.py`** or **`run_agent`** — entry used by FastAPI handlers.

Examples under `src/agents/`:

- **`agent_ontology_assistant`** — conversational edits to the loaded ontology (exposed via e.g. `ontology_assistant_chat` in `front/routes/ontology.py`).
- **`agent_owl_generator`** — generates OWL from natural language (async task wrapper in ontology routes).
- **`agent_auto_icon_assign`** — suggests emoji icons for entities.
- **`agent_auto_assignment`** — automated mapping or assignment workflows (see package for details).

Shared utilities include **`agents.engine_base`** (shared `AgentStep` data class, `call_serving_endpoint` for LLM calls, `dispatch_tool` for tool execution, `extract_message_content` for response parsing, and `accumulate_usage` for token tracking), **`agents.llm_utils`** (retry/backoff logic), **`agents.tools.context`** (`ToolContext` for domain/session-aware tool execution), and **`agents.tracing`** (initialized from app `lifespan` in `src/shared/fastapi/main.py` via `setup_tracing()`).

#### 5.3 Integration pattern

1. HTTP handler validates inputs and loads **domain** state from session (or task payload).  
2. Handler calls **`run_agent(...)`** with context (ontology snapshot, preferences).  
3. Agent returns structured output; handler persists changes to session or completes a **TaskManager** task with progress updates.  
4. UI polls **`/tasks/{task_id}`** or receives immediate JSON for short runs.

---

### 6. MCP server (`src/mcp-server`)

#### 6.1 Role

The **MCP** package exposes OntoBricks capabilities (domains, status, entities, GraphQL) as **MCP tools and resources** for hosts such as Databricks Genie, Claude Desktop, or custom MCP clients. It does **not** duplicate the full rule engine server-side: it **`httpx`** calls the **main OntoBricks HTTP API** (`ONTOBRICKS_URL`).

#### 6.2 Layout

- **`src/mcp-server/server/app.py`** — `FastMCP` server factory (`create_mcp_server`), tool/resource definitions, HTTP client helpers, response formatting; **`create_databricks_app`** builds a **combined** FastAPI app mounting MCP HTTP routes for Databricks App deployment.
- **`src/mcp-server/server/main.py`** — CLI entry (`uv run --frozen mcp-ontobricks`) running **`combined_app`** with Uvicorn.
- **`app.yaml`** / deploy scripts (see `docs/deployment.md`) wire env vars: `ONTOBRICKS_URL`, registry volume or catalog/schema/volume, warehouse ID, etc.

#### 6.3 Operating modes (from module docstring)

- **`databricks`** — combined FastAPI + FastMCP app; uses **service principal** / app identity to reach the main app.  
- **`standalone`** — separate process (stdio/SSE HTTP) pointing at `ONTOBRICKS_URL` (e.g. localhost).  
- **`mounted`** — optional embedding in the main OntoBricks process (loopback to the same host/port).

#### 6.4 Navigation / protocol

MCP has **no Jinja routes**; "navigation" is **tool choice** (e.g. `list_domains` → `select_domain` → `describe_entity`). Resources such as `ontobricks://domains` map to REST paths like **`/api/v1/domains`**. GraphQL-related tools POST to **`/graphql/<domain>`** with the same registry parameters the REST API expects.

#### 6.5 Core MCP concerns (conceptual)

- **Auth alignment:** In Databricks mode, MCP identity may differ from an end-user browser session; registry and domain visibility follow **OAuth M2M / SP** permissions on UC objects.  
- **Configuration:** Registry location must match the main app's bound volume expectations to avoid 404s or stale paths (see registry helpers in the main codebase).  
- **Health:** `combined_app` exposes a small **`GET /`** JSON health object (service name, `ontobricks_url`, warehouse, registry display).
- **Per-domain policy:** each domain's `mcp_policy` arrives on `GET /api/v1/domains` and is cached per domain name. `select_domain` calls `ctx.reset_visibility()` then `ctx.disable_components()`, so the *visibility rules* are session-scoped and no restart is needed. Because the proxy holds no authority, every domain-scoped tool also re-checks the policy on entry, and the real withholding happens in the main app's endpoints.
- **Process-global selection (limitation):** `create_mcp_server()` is called once per process, so `_selected_domain`, `_domain_policy` and `_class_actions` are closures shared by every session. Concurrent clients on different domains therefore share one selection — pre-existing behaviour that affects query routing as much as the policy check. Making the selection session-scoped is the prerequisite for true multi-client isolation.

---

### 7. Code Style and Patterns

#### 7.1 Backend (Python/FastAPI)

- Use `def` for pure functions and `async def` for asynchronous operations (FastAPI route handlers, SDK calls).
- **Type Hints**: Use Python type hints extensively.
- **RORO Pattern**: API handlers follow "Receive an Object, Return an Object".
- Guard clauses and early returns are preferred.

**Route patterns:**

- HTML route files (`front/routes/`) serve `TemplateResponse` only — no business logic, no JSON.
- Internal API handlers (`api/routers/internal/`) return JSON and delegate to domain classes.
- External API endpoints (`api/routers/v1.py`, etc.) are stateless REST.

**Internal API handler pattern** (preferred):

```python
# api/routers/internal/mapping.py — JSON only
from back.objects.mapping import Mapping

@router.post("/entity/add")
async def add_entity_mapping(request: Request, session_mgr = Depends(get_session_manager)):
    data = await request.json()
    domain = get_domain(session_mgr)
    _, new_mapping = Mapping(domain).add_or_update_entity_mapping(data)
    return {'success': True, 'mapping': new_mapping}
```

**Frontend HTML handler pattern:**

```python
# front/routes/mapping.py — HTML only
@router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def mapping_page(request: Request):
    return templates.TemplateResponse(request, "mapping.html")
```

**Dependency Injection:**

- Use FastAPI `Depends` for wiring (sessions, services, auth), not core business logic.
- Session management: simplify and centralize; minimize session variables.

#### 7.2 Refactoring vocabulary

When refactoring, use Martin Fowler's named refactorings: Extract Function, Extract Class, Introduce Parameter Object, Move Function, etc. Work in small, reversible steps. Keep code runnable and testable after every step.

**Code smells and preferred refactorings:**

- **Long route handlers** → Extract Function/Method to move business logic into the domain class in `back/objects/`. Introduce Explaining Variable for complex expressions.
- **Large "god" services or utility modules** → Extract Class or Extract Module by domain. Move Function closer to the data it operates on.
- **Repeated request/response boilerplate** → Extract Function for common response building. Replace Magic Number/String with named constants.
- **Data clumps / long parameter lists** → Introduce Parameter Object (Pydantic model or dataclass).
- **Tight coupling between routes and ORM models** → Introduce Pydantic schemas for input/output. Encapsulate Field / Encapsulate Collection instead of exposing ORM internals.

---

### 8. Error Handling

All application errors go through the centralized hierarchy in `back/core/errors/`. A global exception handler in `shared/fastapi/main.create_app()` translates them into a uniform `ErrorResponse` JSON body.

**Rules:**

- Always raise from the `OntoBricksError` hierarchy, never return `{'success': False, ...}` or bare `HTTPException`.
- Use the right subclass: `ValidationError` (400), `NotFoundError` (404), `AuthorizationError` (403), `ConflictError` (409), `InfrastructureError` (502).
- Never expose `str(e)` to clients. Put safe descriptions in `message`; internal details in `detail`.
- Services must not catch-and-swallow. Let errors propagate to the global handler.

**Error classes:**

| Class | HTTP | When |
|-------|------|------|
| `ValidationError` | 400 | Bad client input, missing params, invalid query syntax |
| `NotFoundError` | 404 | Resource, domain, or task does not exist |
| `AuthorizationError` | 403 | Caller lacks permission |
| `ConflictError` | 409 | Duplicate entity, version conflict |
| `InfrastructureError` | 502 | External system failure (Databricks, network, file I/O) |
| `OntoBricksError` | 500 | Catch-all for unexpected domain errors |

**Error response shape:**

```json
{
  "error": "not_found",
  "message": "Domain not found",
  "detail": "...",
  "request_id": "abc-123"
}
```

`detail` is omitted in production. `request_id` comes from the `X-Request-Id` header or is auto-generated.

**Logging inside error paths:**

- The global handler already logs every error — `WARNING` for known `OntoBricksError`, `ERROR` with traceback for unexpected exceptions.
- If you need to add extra context before re-raising, use `logger.warning(...)` — never `logger.exception(...)` followed by a `return`, which swallows the error.
- Only catch exceptions when you need to *translate* them to a more specific `OntoBricksError` subclass.

---

### 9. Logging

All logging goes through `back/core/logging/LogManager`.

- **Never** use `print()` — use a logger.
- **Never** call `logging.basicConfig()` — `setup_logging()` in `run.py` handles it once.
- Get a logger: `from back.core.logging import get_logger; logger = get_logger(__name__)`.
- **Never** use f-strings or `.format()` in logging calls with dynamic variables. Use %-style formatting: `logger.error("Error for user %s", user_email, exc_info=True)`.
- Log all errors and warnings to help debugging.
- When running async processes, add logs at every step.
- Level / directory / filename controlled via `LOG_LEVEL`, `LOG_DIR`, `LOG_FILE` env vars.
- Always use the standard `logging` library with `logging.config.dictConfig` for configuration.
- All logging configuration lives in `back/core/logging/` (the `LogManager` class in `LogManager.py`) and is reused across the app.

---

### 10. Performance Optimization

**Backend:**

- **Asynchronous Operations**: Use `async/await` for Databricks SDK calls and I/O-bound operations.
- **Logging**: Use %-style formatting (not f-strings) in logging calls for both security and performance.
- **Session Data**: Minimize the number of session variables. Prefer computed values over stored ones when the cost is low.
- **Caching**: Use TTL caches for frequently accessed, rarely changing data (e.g., version status in `version_status.py`).
- **Triple Store / Graph DB**: Be mindful of query complexity. The Lakebase Graph DB engine streams large builds via `bulk_insert_iter` (`COPY FROM STDIN`) instead of materializing the full triple set in the FastAPI process; the Delta layer relies on Liquid Clustering and `OPTIMIZE` post-build.
- **Background Tasks**: Offload heavy operations to `TaskManager` or Databricks Workflows. Log progress at each async step.

**Frontend:**

- **Template Reuse**: Use base templates and partials to keep the UI modular and DRY.
- **Static Assets**: Minimise inline scripts/styles. Reference external CSS/JS files for cacheability.
- **Lazy Loading**: Load heavy visualisation libraries (OntoViz) only when needed.

---

### 11. Testing and Deployment

- **Test framework**: `pytest` for the backend. Run with `uv run --frozen pytest -q -m "not scenario"`.
- **Package management**: This project uses **uv** for Python package management and virtual environment management. Dependencies are declared in `pyproject.toml`. Run routine commands via `uv run --frozen <command>`.
- **Deployment**: Deployed as a Databricks App via `databricks bundle deploy` using `app.yaml` (Databricks Asset Bundle format).
- **Development Server**: Run via `run.py` which imports `create_app` from `shared.fastapi.main`. Uvicorn with auto-reload for local development.
- **Changelog**: After any code change, update `/changelogs/YYYY-MM-DD.log` with a section including title, context, numbered changes with file paths, modified files list, and test results.

#### 11.1 Documentation rules

- Review `/docs` and update it when refactoring changes structure or behaviour relevant to users.
- For each feature, ensure there is at least one concrete example in the docs.
- Update `README.md` to cover all features and capabilities.
- **Dependencies**: Maintain the **Dependencies** section in `docs/development.md` describing all external dependencies (Python libs, JS frameworks, CSS libs) with short description, link, and license type.
- When adding or removing dependencies, update `docs/development.md` accordingly.

#### 11.2 Sphinx API documentation

- Source files in `/docs/sphinx` (config in `conf.py`, module `.rst` files in `api/`).
- After any change that adds, removes, or renames a Python module, class, or public function:
  1. Update or create the corresponding `.rst` file under `/docs/sphinx/api/`.
  2. Re-build by running `scripts/build_docs.sh` from the project root.
  3. Verify no new Sphinx warnings about missing or renamed symbols.
- When adding a **new subpackage** to `back/core`, `back/objects`, or `agents`, add a new `.rst` file and reference it from the parent `.rst` toctree.
- All public classes and functions must have **docstrings** (Google or NumPy style) so `autodoc` renders them properly.

#### 11.3 Adding a new subpackage checklist

1. **Choose the parent:** infrastructure → `back/core`; registry/session/domain → `back/objects`.
2. **Create the directory** with an `__init__.py`.
3. **Define the service class** in a PascalCase file matching the class name. Constructor receives config/dependencies. Use `from back.core.logging import get_logger`.
4. **Add supporting files** if needed (dataclasses, constants, secondary services).
5. **Re-export** all public symbols from `__init__.py`.
6. **Update `back/core/__init__.py`** only if the new package is widely-used infrastructure.
7. **Wire into routes** via FastAPI dependencies or direct imports from the package.
8. **Update Sphinx** under `docs/sphinx/api/`.

---

### 12. Quick file index

| Area | Key paths |
|------|-----------|
| App factory | `src/shared/fastapi/main.py` |
| Health | `src/shared/fastapi/health.py` |
| Settings, constants & templates | `src/shared/config/settings.py`, `src/shared/config/constants.py`, `src/front/fastapi/dependencies.py` (includes `triplestore_page_context` shared helper) |
| Sessions | `src/back/objects/session/` (`middleware.py`, `SessionManager.py`, `DomainSession.py`) |
| Databricks / volumes | `src/back/core/databricks/`, `src/back/core/helpers/` |
| Registry / permissions | `src/back/objects/registry/` |
| Saved domain / UC layout (`Domain` class) | `src/back/objects/domain/` (`Domain` class); feature services in `src/back/services/` (e.g. `home.py`, `settings.py`) |
| Tasks | `src/back/core/task_manager/`, `src/api/routers/internal/tasks.py` |
| UI HTML routes | `src/front/routes/*.py` |
| Internal JSON API (session-aware) | `src/api/routers/internal/*.py` |
| REST v1 | `src/api/routers/v1.py`, `src/api/service.py` |
| Domain list & artifacts | `src/api/routers/domains.py` |
| Knowledge Graph REST | `src/api/routers/digitaltwin.py` |
| Graph analysis / clustering | `src/back/core/graph_analysis/` (`CommunityDetector`, models) |
| GraphQL | `src/back/fastapi/graphql_routes.py` |
| Agents | `src/agents/**` |
| MCP | `src/mcp-server/server/app.py`, `src/mcp-server/server/main.py` |

---

*For day-to-day contributor workflows (tests, deps, rights), see [`development.md`](development.md). For product-level architecture, see [`architecture.md`](architecture.md). Detailed coding and refactoring rules live in `src/.coding_rules.md`.*
