# OntoBricks code organization (for contributors)

This document maps how the **main OntoBricks application** is wired: the browser UI, programmatic REST/GraphQL APIs, LLM **agents**, and the separate **MCP** server. It also covers **coding conventions**, **error handling**, **logging**, **project structure**, and **contributor workflows**.

The FastAPI entrypoint is `src/shared/fastapi/main.py` (`create_app`, `app = create_app()`). Application source is split across five top-level packages under `src/`: **`back`** (domain, core infra, GraphQL, services), **`front`** (HTML routes, Jinja2, menu config, templates, static assets), **`shared`** (app factory, middleware, health, settings/constants), **`api`** (external REST and internal JSON routers), and **`agents`** (LLM engines).

---

## 1. Architecture and Key Principles

### 1.1 Layered architecture

All code follows a strict three-layer stack: **routes (HTTP)** → **domain classes** (`back/objects`) → **core infrastructure** (`back/core`).

- **Domain classes own all business logic.** Routes are thin: extract parameters, call a domain method, return response.
- **`service.py` files are deprecated.** The preferred pattern is routes → domain class directly. Existing service files are legacy and should be migrated.
- Core infrastructure (`back/core`) has **no dependency on HTTP/FastAPI types**.
- Domain objects (`back/objects`) have **no `Request`/`Response` inside the package**.

### 1.2 Naming conventions

- Python files containing a class: **PascalCase** matching the class name (e.g., `OWLRLReasoner.py`, `SWRLSQLTranslator.py`).
- Python files without a primary class: **lowercase with underscores** (e.g., `constants.py`, `models.py`).
- CSS/JS assets: lowercase with dashes, organised by feature area under `front/static/`.

### 1.3 Class-first policy

| Guideline | Detail |
|-----------|--------|
| Default to a service class | When adding new functionality, create a class that owns the state and methods. |
| Constructor receives dependencies | Pass configuration, credentials, or collaborators via `__init__` for testability. |
| Methods over functions | Reserve module-level functions only for thin factory/accessor helpers or pure stateless utilities. |
| One public class per file | Name the file after the class in PascalCase. Small helper dataclasses in the same file are fine. Shared constants go in `constants.py`, shared DTOs in `models.py`. |

### 1.4 Package organisation

- Every subpackage `__init__.py` must re-export public symbols. Consumers import from the package, never from internal modules.
- Use `__all__` when the public surface is well-defined.
- Keep `__init__.py` files short — only imports, `__all__`, and thin backward-compatible wrappers.
- When refactoring functions into class methods, provide backward-compatible wrappers in `__init__.py`.

### 1.5 Domain security

- A domain must **never** include tokens, passwords, or other secrets.
- A domain must **never** include query results or transient data.
- If you see secrets in domain structures, move them to configuration, env vars, or temporary storage.

---

## 2. Project Structure

### 2.1 Top-level packages

| Package | Purpose |
|---------|---------|
| `src/back/` | Backend: domain classes (`objects/`), core infra (`core/`), GraphQL routes (`fastapi/`), services (`services/`). |
| `src/front/` | Frontend: HTML-only routes (`routes/`), Jinja2 config (`fastapi/`), menu config (`config/`), templates (`templates/`), static assets (`static/`). |
| `src/shared/` | Shared infrastructure: app factory & middleware (`fastapi/`), settings & constants (`config/`). |
| `src/api/` | API layer: external REST API (`routers/v1.py` etc.), internal JSON API (`routers/internal/`). |
| `src/agents/` | LLM agent engines (OWL generator, auto-assign, icon assign, assistant). |

### 2.2 Three-layer FastAPI split

| Package | Contents | Depends on |
|---------|----------|------------|
| `shared/fastapi/` | Application factory (`main.py`), permission middleware, router registration, health routes (`health.py`). | `front`, `back`, `api` |
| `front/fastapi/` | Jinja2 template config, custom filters, menu globals (`dependencies.py`). | `front/config/` |
| `back/fastapi/` | GraphQL API (`graphql_routes.py`). | `back/core/`, `back/objects/` |

Entry point: `run.py` imports `create_app` from `shared.fastapi.main`.

### 2.3 Frontend structure

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

### 2.4 Backend domain objects (`src/back/objects/`)

```
back/objects/
├── registry/         <- UC domain registry, permissions, scheduled builds
├── session/          <- HTTP session storage, domain state, instance config
├── domain/           <- Session-scoped UC/metadata/layout operations
├── ontology/         <- Ontology domain class
├── mapping/          <- Mapping domain class
└── digitaltwin/      <- Digital Twin domain class
```

### 2.5 Backend core infrastructure (`src/back/core/`)

```
back/core/
├── agents/           <- AgentClient (LLM gateway)
├── errors/           <- OntoBricksError hierarchy
├── helpers/          <- Cross-cutting utilities (DatabricksHelpers, SQLHelpers, URIHelpers)
├── logging/          <- LogManager, setup_logging(), get_logger()
├── task_manager/     <- Async in-memory task tracking
├── triplestore/      <- Triple store backends (Delta views in UC)
├── graphdb/          <- Pluggable graph DB engine abstraction (LadybugDB, …)
├── databricks/       <- Typed facades for Databricks API surfaces
├── graphql/          <- Strawberry GraphQL schema builder
├── w3c/              <- W3C standard parsers/generators (OWL, RDFS, R2RML, SPARQL, SHACL)
├── industry/         <- Standard industry ontology importers (CDISC, FIBO, IOF)
├── graph_analysis/   <- Community detection (NetworkX backend)
├── reasoning/        <- OWL 2 RL, SWRL, decision tables, SPARQL rules, aggregates
└── sqlwizard/        <- LLM-assisted SQL generation
```

### 2.6 API layer (`src/api/`)

```
api/
├── routers/
│   ├── v1.py, domains.py, digitaltwin.py   <- External API (/api/v1/*)
│   └── internal/                           <- Internal API (session-aware JSON)
│       ├── home.py, settings.py, ontology.py, mapping.py, dtwin.py, domain.py, tasks.py
│       └── _helpers.py
└── external_app.py
```

### 2.7 Where to put new code

| Location | Purpose |
|----------|---------|
| `back/core/<pkg>/` | Shared infrastructure (Databricks, triplestore, W3C, reasoning, …). No HTTP dependency. |
| `back/objects/<pkg>/` | Application domain classes (Ontology, Mapping, DigitalTwin, Domain, registry, session). No `Request`/`Response`. |
| `front/routes/<area>.py` | HTML routing only — parameter extraction, domain method call, `TemplateResponse`. |
| `api/routers/internal/` | Internal JSON API — session-aware, delegates to domain classes. |
| `api/routers/v1.py` etc. | External REST API — stateless. |

**Rule of thumb:** If the logic is about **this app's domain/registry/session model**, put it in `back/objects`. If it could be reused by a different app talking to Databricks or RDF standards, put it in `back/core`. If it is purely about HTML or HTTP wiring, keep it in `front/` routes.

---

## 3. UI (HTML pages and client navigation)

### 3.1 How requests reach the UI

1. **Middleware stack** (order matters; Starlette runs *last added* first on the way *in*):  
   - **CORS** — allows credentials for local dev.  
   - **PermissionMiddleware** — when running as a Databricks App, resolves the user role from registry permissions and blocks viewers from mutating HTTP methods; bypasses `/static/`, `/api/`, `/graphql/`, OpenAPI, health, etc. Local dev behaves as admin.  
   - **FileSessionMiddleware** — cookie-backed **file sessions** (JSON on disk under `settings.session_dir`); skips static, docs, health, and `/tasks/*` so task polling does not churn session I/O.

2. **Static files** are mounted at `/static` from `src/front/static/` (with a fallback path for unusual layouts).

3. **HTML routers** are plain FastAPI `APIRouter` modules under `src/front/routes/` (e.g. `home.py`, `ontology.py`). Each module declares a **path prefix** (except home) and returns `templates.TemplateResponse(...)` for full pages or JSON for XHR endpoints used by the SPA-like panels. Session-aware **JSON** endpoints used by the UI (settings, tasks, navbar state, etc.) live under **`src/api/routers/internal/`** and are registered alongside the HTML routers from `shared/fastapi/main.py`.

### 3.2 Template loading (Jinja2)

`src/front/fastapi/dependencies.py` builds a single `Jinja2Templates` instance whose **search path** is centered on:

- **`src/front/templates/`** — top-level page templates (e.g. `ontology.html`, `base.html`).
- **`src/front/templates/partials/`** — shared fragments, with feature subfolders: **`layout/`**, **`ontology/`**, **`mapping/`**, **`dtwin/`**, **`domain/`** (each added to the Jinja search path so includes resolve cleanly).

Templates are named by file (e.g. `ontology.html`) as long as the name is unique across the search path.

Custom Jinja helpers include **`url_for`** (static + `request.url_for`), a **`range`** filter, and **`triplestore_page_context`** (shared template context builder for `view_table`, `graph_name`, and `triplestore_cache` used by dtwin and domain pages).

### 3.3 UI route map (high level)

| Prefix / path | Module | Typical templates | Role |
|---------------|--------|-------------------|------|
| `/`, `/about`, `/settings` (HTML), `/access-denied`, status endpoints for navbar | `front/routes/home.py` | `home.html`, `about.html`, `settings.html`, `access_denied.html` | Landing, about, settings **page shell**, consolidated `/navbar/state`, session/ontology status |
| `/settings/*` (API) | `api/routers/internal/settings.py` | (mostly JSON) | Load/save Databricks config, test connection, permissions helpers |
| `/ontology/*` | `front/routes/ontology.py` | `ontology.html`, fragments | Ontology editor, SHACL, industry catalogs, **agent**-backed flows (chat, OWL generation, icons) |
| `/mapping/*` | `front/routes/mapping.py` | mapping templates | R2RML / table–ontology mapping UI |
| `/dtwin/*` | `front/routes/dtwin.py` | dtwin templates | SPARQL, graph exploration, triple-store–backed "digital twin" UI |
| `/domain/*` | `front/routes/domain.py` | domain templates | Domain JSON in UC volumes, versioning, metadata, documents |
| `/registry/*` | `front/routes/registry.py` | `registry.html`, fragments | Multi-domain registry browser, schedules, API endpoint reference |
| `/resolve` | `front/routes/resolve.py` | — (redirect) | Entity URI resolution — finds the owning domain and redirects to the knowledge graph |
| `/tasks/*` | `api/routers/internal/tasks.py` | — | Task list/detail JSON for long-running work |

**Note:** The **settings HTML page** is served at **`GET /settings`** from **home** routes; **internal settings** routes use the same **`/settings`** prefix for **JSON APIs** (`/settings/current`, `/settings/save`, etc.).

**XHR / JSON:** Besides **`tasks`**, the ontology, mapping, digital twin, and domain UIs call session-aware JSON handlers in **`api/routers/internal/ontology.py`**, **`mapping.py`**, **`dtwin.py`**, and **`domain.py`** (and **`home.py`** for shared navbar/session helpers). Paths align with the same feature areas as the HTML routers above.

### 3.4 Menu and client-side navigation

- **Declarative menu:** `src/front/config/menu_config.json` defines sections (Domain, Ontology, Mapping, …), **routes** (e.g. `/domain/`), groups, items, and `navbar_actions` (e.g. `projectSave`). The server can expose this to the client or the client can load it; either way it is the **source of truth** for section IDs and default tabs.
- **JavaScript:** under `src/front/static/global/js/` (sidebar, navbar, domain actions). Pages load shared layout partials from templates and drive **in-page tabs** via menu item IDs rather than full page loads for every interaction.

### 3.5 Core UI concerns (conceptual)

- **SessionManager / `get_domain`:** Most handlers depend on `SessionManager` to read/write the **current domain blob** (ontology, mappings, registry pointers, triple-store options) in the file session.
- **Databricks integration:** UI services call helpers such as `get_databricks_client`, `get_databricks_host_and_token`, `resolve_warehouse_id`, and `VolumeFileService` to run SQL, read/write Unity Catalog volumes, and align with app vs. PAT auth.
- **TaskManager:** Long operations (builds, agent loops) register tasks under `/tasks/{id}` so the UI can poll progress without holding a request open.

### 3.6 Frontend code guidelines

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

## 4. API (REST v1, Digital Twin, GraphQL)

### 4.1 Registration

In `src/shared/fastapi/main.py`, `_register_routers` mounts:

- **Health** — `shared/fastapi/health.py` (app health; may overlap conceptually with v1 `/health`).
- **External REST v1** — `src/api/routers/v1.py` at **`/api/v1`** (stateless; credentials in body or headers), exposed via the mounted external API app (see `api.external_app` and `EXTERNAL_API_MOUNT_PREFIX`).
- **Digital Twin API** — `src/api/routers/digitaltwin.py` at **`/api/v1/digitaltwin`** (registry, domain artifacts, build, triples, quality, reasoning).
- **GraphQL** — `src/back/fastapi/graphql_routes.py` at **`/graphql`** and on the external app (see `api.external_app`; per-domain GraphQL execution).

The **OpenAPI** document is at `/openapi.json`; interactive docs at `/docs` and `/redoc`.

### 4.2 `/api/v1` (stateless integration API)

Defined in `api/routers/v1.py`. Representative endpoints:

- **`POST /api/v1/domains/list`**, **`POST /api/v1/domain/info`**, ontology/classes/properties, mappings, R2RML extraction.
- **`POST /api/v1/query`** — SPARQL execution with `engine` choice (e.g. local vs Spark).
- **`POST /api/v1/query/validate`**, **`POST /api/v1/query/samples`**.

Pydantic models encode UC location (`catalog` / `schema` / `volume`) and optional `databricks_host` / `databricks_token`. Business logic for the external REST layer is delegated to `api.service` where appropriate.

### 4.3 `/api/v1/domains` and `/api/v1/domain` (registry list & artifacts)

Defined in `api/routers/domains.py`. Representative paths:

| Method | Path | Purpose (summary) |
|--------|------|-------------------|
| GET | `/api/v1/domains` | List MCP-exposed registry domains |
| GET | `/api/v1/domain/versions`, `/domain/design-status` | Versions and design readiness |
| GET | `/api/v1/domain/ontology`, `/api/v1/domain/r2rml`, `/api/v1/domain/sparksql` | Serialized design artifacts |

### 4.4 `/api/v1/digitaltwin` (Digital Twin)

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

### 4.5 GraphQL

`back/fastapi/graphql_routes.py` wires `back.core.graphql` to expose an **ontology-derived schema** for a named domain (`GET /graphql`, `GET/POST /graphql/{domain}`, schema sub-routes). It complements SPARQL with a typed graph API.

### 4.6 Core API concerns (conceptual)

- **No browser session required** for v1/digitaltwin when callers pass credentials and UC paths; the **UI** relies on **FileSessionMiddleware** instead.
- **PermissionMiddleware** bypasses `/api/` and `/graphql/` prefixes at enforcement time today; authorization for external callers is primarily **token + UC ACLs**.
- **Async vs sync:** Some services use `run_blocking` to call blocking Databricks or RDF libraries from async routes; routes must **await** those helpers to avoid coroutine bugs.

---

## 5. Agents (`src/agents`)

### 5.1 Purpose

Agents are **LLM loops with tools**: they are invoked from **ontology** routes (and similar) for assisted editing, generation, or batch suggestions. They are **not** separate HTTP servers; they run **in-process** inside the main app worker.

### 5.2 Agent packages

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

### 5.3 Integration pattern

1. HTTP handler validates inputs and loads **domain** state from session (or task payload).  
2. Handler calls **`run_agent(...)`** with context (ontology snapshot, preferences).  
3. Agent returns structured output; handler persists changes to session or completes a **TaskManager** task with progress updates.  
4. UI polls **`/tasks/{task_id}`** or receives immediate JSON for short runs.

---

## 6. MCP server (`src/mcp-server`)

### 6.1 Role

The **MCP** package exposes OntoBricks capabilities (domains, status, entities, GraphQL) as **MCP tools and resources** for hosts such as Databricks Genie, Claude Desktop, or custom MCP clients. It does **not** duplicate the full rule engine server-side: it **`httpx`** calls the **main OntoBricks HTTP API** (`ONTOBRICKS_URL`).

### 6.2 Layout

- **`src/mcp-server/server/app.py`** — `FastMCP` server factory (`create_mcp_server`), tool/resource definitions, HTTP client helpers, response formatting; **`create_databricks_app`** builds a **combined** FastAPI app mounting MCP HTTP routes for Databricks App deployment.
- **`src/mcp-server/server/main.py`** — CLI entry (`uv run mcp-ontobricks`) running **`combined_app`** with Uvicorn.
- **`app.yaml`** / deploy scripts (see `docs/deployment.md`) wire env vars: `ONTOBRICKS_URL`, registry volume or catalog/schema/volume, warehouse ID, etc.

### 6.3 Operating modes (from module docstring)

- **`databricks`** — combined FastAPI + FastMCP app; uses **service principal** / app identity to reach the main app.  
- **`standalone`** — separate process (stdio/SSE HTTP) pointing at `ONTOBRICKS_URL` (e.g. localhost).  
- **`mounted`** — optional embedding in the main OntoBricks process (loopback to the same host/port).

### 6.4 Navigation / protocol

MCP has **no Jinja routes**; "navigation" is **tool choice** (e.g. `list_domains` → `select_domain` → `describe_entity`). Resources such as `ontobricks://domains` map to REST paths like **`/api/v1/domains`**. GraphQL-related tools POST to **`/graphql/<domain>`** with the same registry parameters the REST API expects.

### 6.5 Core MCP concerns (conceptual)

- **Auth alignment:** In Databricks mode, MCP identity may differ from an end-user browser session; registry and domain visibility follow **OAuth M2M / SP** permissions on UC objects.  
- **Configuration:** Registry location must match the main app's bound volume expectations to avoid 404s or stale paths (see registry helpers in the main codebase).  
- **Health:** `combined_app` exposes a small **`GET /`** JSON health object (service name, `ontobricks_url`, warehouse, registry display).

---

## 7. Code Style and Patterns

### 7.1 Backend (Python/FastAPI)

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

### 7.2 Refactoring vocabulary

When refactoring, use Martin Fowler's named refactorings: Extract Function, Extract Class, Introduce Parameter Object, Move Function, etc. Work in small, reversible steps. Keep code runnable and testable after every step.

**Code smells and preferred refactorings:**

- **Long route handlers** → Extract Function/Method to move business logic into the domain class in `back/objects/`. Introduce Explaining Variable for complex expressions.
- **Large "god" services or utility modules** → Extract Class or Extract Module by domain. Move Function closer to the data it operates on.
- **Repeated request/response boilerplate** → Extract Function for common response building. Replace Magic Number/String with named constants.
- **Data clumps / long parameter lists** → Introduce Parameter Object (Pydantic model or dataclass).
- **Tight coupling between routes and ORM models** → Introduce Pydantic schemas for input/output. Encapsulate Field / Encapsulate Collection instead of exposing ORM internals.

---

## 8. Error Handling

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

## 9. Logging

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

## 10. Performance Optimization

**Backend:**

- **Asynchronous Operations**: Use `async/await` for Databricks SDK calls and I/O-bound operations.
- **Logging**: Use %-style formatting (not f-strings) in logging calls for both security and performance.
- **Session Data**: Minimize the number of session variables. Prefer computed values over stored ones when the cost is low.
- **Caching**: Use TTL caches for frequently accessed, rarely changing data (e.g., version status in `version_status.py`).
- **Triple Store / Graph DB**: Be mindful of query complexity; use incremental builds (`IncrementalBuildService`) when possible rather than full rebuilds.
- **Background Tasks**: Offload heavy operations to `TaskManager` or Databricks Workflows. Log progress at each async step.

**Frontend:**

- **Template Reuse**: Use base templates and partials to keep the UI modular and DRY.
- **Static Assets**: Minimise inline scripts/styles. Reference external CSS/JS files for cacheability.
- **Lazy Loading**: Load heavy visualisation libraries (OntoViz) only when needed.

---

## 11. Testing and Deployment

- **Test framework**: `pytest` for the backend. Run with `uv run pytest -q`.
- **Package management**: This project uses **uv** for Python package management and virtual environment management. Dependencies are declared in `pyproject.toml`. Run commands via `uv run <command>`.
- **Deployment**: Deployed as a Databricks App via `databricks bundle deploy` using `app.yaml` (Databricks Asset Bundle format).
- **Development Server**: Run via `run.py` which imports `create_app` from `shared.fastapi.main`. Uvicorn with auto-reload for local development.
- **Changelog**: After any code change, update `/changelogs/YYYY-MM-DD.log` with a section including title, context, numbered changes with file paths, modified files list, and test results.

### 11.1 Documentation rules

- Review `/docs` and update it when refactoring changes structure or behaviour relevant to users.
- For each feature, ensure there is at least one concrete example in the docs.
- Update `README.md` to cover all features and capabilities.
- **Dependencies**: Maintain the **Dependencies** section in `docs/development.md` describing all external dependencies (Python libs, JS frameworks, CSS libs) with short description, link, and license type.
- When adding or removing dependencies, update `docs/development.md` accordingly.

### 11.2 Sphinx API documentation

- Source files in `/docs/sphinx` (config in `conf.py`, module `.rst` files in `api/`).
- After any change that adds, removes, or renames a Python module, class, or public function:
  1. Update or create the corresponding `.rst` file under `/docs/sphinx/api/`.
  2. Re-build by running `scripts/build_docs.sh` from the project root.
  3. Verify no new Sphinx warnings about missing or renamed symbols.
- When adding a **new subpackage** to `back/core`, `back/objects`, or `agents`, add a new `.rst` file and reference it from the parent `.rst` toctree.
- All public classes and functions must have **docstrings** (Google or NumPy style) so `autodoc` renders them properly.

### 11.3 Adding a new subpackage checklist

1. **Choose the parent:** infrastructure → `back/core`; registry/session/domain → `back/objects`.
2. **Create the directory** with an `__init__.py`.
3. **Define the service class** in a PascalCase file matching the class name. Constructor receives config/dependencies. Use `from back.core.logging import get_logger`.
4. **Add supporting files** if needed (dataclasses, constants, secondary services).
5. **Re-export** all public symbols from `__init__.py`.
6. **Update `back/core/__init__.py`** only if the new package is widely-used infrastructure.
7. **Wire into routes** via FastAPI dependencies or direct imports from the package.
8. **Update Sphinx** under `docs/sphinx/api/`.

---

## 12. Quick file index

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
| Digital Twin REST | `src/api/routers/digitaltwin.py` |
| Graph analysis / clustering | `src/back/core/graph_analysis/` (`CommunityDetector`, models) |
| GraphQL | `src/back/fastapi/graphql_routes.py` |
| Agents | `src/agents/**` |
| MCP | `src/mcp-server/server/app.py`, `src/mcp-server/server/main.py` |

---

*For day-to-day contributor workflows (tests, deps, rights), see [`development.md`](development.md). For product-level architecture, see [`architecture.md`](architecture.md). For detailed coding rules and refactoring guidelines, see [`src/.coding_rules.md`](../src/.coding_rules.md).*
