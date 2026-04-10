# OntoBricks code organization (for contributors)

This document maps how the **main OntoBricks application** is wired: the browser UI, programmatic REST/GraphQL APIs, LLM **agents**, and the separate **MCP** server. It focuses on **navigation** (URLs, templates, static assets) and **core responsibilities** (sessions, Databricks, registry, tasks).

The FastAPI entrypoint is `src/shared/fastapi/main.py` (`create_app`, `app = create_app()`). Application source is split across five top-level packages under `src/`: **`back`** (domain, core infra, GraphQL, services), **`front`** (HTML routes, Jinja2, menu config, templates, static assets), **`shared`** (app factory, middleware, health, settings/constants), **`api`** (external REST and internal JSON routers), and **`agents`** (LLM engines).

---

## 1. UI (HTML pages and client navigation)

### 1.1 How requests reach the UI

1. **Middleware stack** (order matters; Starlette runs *last added* first on the way *in*):  
   - **CORS** — allows credentials for local dev.  
   - **PermissionMiddleware** — when running as a Databricks App, resolves the user role from registry permissions and blocks viewers from mutating HTTP methods; bypasses `/static/`, `/api/`, `/graphql/`, OpenAPI, health, etc. Local dev behaves as admin.  
   - **FileSessionMiddleware** — cookie-backed **file sessions** (JSON on disk under `settings.session_dir`); skips static, docs, health, and `/tasks/*` so task polling does not churn session I/O.

2. **Static files** are mounted at `/static` from `src/front/static/` (with a fallback path for unusual layouts).

3. **HTML routers** are plain FastAPI `APIRouter` modules under `src/front/routes/` (e.g. `home.py`, `ontology.py`). Each module declares a **path prefix** (except home) and returns `templates.TemplateResponse(...)` for full pages or JSON for XHR endpoints used by the SPA-like panels. Session-aware **JSON** endpoints used by the UI (settings, tasks, navbar state, etc.) live under **`src/api/routers/internal/`** and are registered alongside the HTML routers from `shared/fastapi/main.py`.

### 1.2 Template loading (Jinja2)

`src/front/fastapi/dependencies.py` builds a single `Jinja2Templates` instance whose **search path** is centered on:

- **`src/front/templates/`** — top-level page templates (e.g. `ontology.html`, `base.html`).
- **`src/front/templates/partials/`** — shared fragments, with feature subfolders: **`layout/`**, **`ontology/`**, **`mapping/`**, **`dtwin/`**, **`project/`** (each added to the Jinja search path so includes resolve cleanly).

Templates are named by file (e.g. `ontology.html`) as long as the name is unique across the search path.

Custom Jinja helpers include **`url_for`** (static + `request.url_for`) and a **`range`** filter.

### 1.3 UI route map (high level)

| Prefix / path | Module | Typical templates | Role |
|---------------|--------|-------------------|------|
| `/`, `/about`, `/settings` (HTML), `/access-denied`, status endpoints for navbar | `front/routes/home.py` | `home.html`, `about.html`, `settings.html`, `access_denied.html` | Landing, about, settings **page shell**, consolidated `/navbar/state`, session/ontology status |
| `/settings/*` (API) | `api/routers/internal/settings.py` | (mostly JSON) | Load/save Databricks config, test connection, permissions helpers |
| `/ontology/*` | `front/routes/ontology.py` | `ontology.html`, fragments | Ontology editor, SHACL, industry catalogs, **agent**-backed flows (chat, OWL generation, icons) |
| `/mapping/*` | `front/routes/mapping.py` | mapping templates | R2RML / table–ontology mapping UI |
| `/dtwin/*` | `front/routes/dtwin.py` | dtwin templates | SPARQL, graph exploration, triple-store–backed “digital twin” UI |
| `/project/*` | `front/routes/project.py` | project templates | Project JSON in UC volumes, versioning, metadata, documents |
| `/tasks/*` | `api/routers/internal/tasks.py` | — | Task list/detail JSON for long-running work |

**Note:** The **settings HTML page** is served at **`GET /settings`** from **home** routes; **internal settings** routes use the same **`/settings`** prefix for **JSON APIs** (`/settings/current`, `/settings/save`, etc.).

**XHR / JSON:** Besides **`tasks`**, the ontology, mapping, digital twin, and project UIs call session-aware JSON handlers in **`api/routers/internal/ontology.py`**, **`mapping.py`**, **`dtwin.py`**, and **`project.py`** (and **`home.py`** for shared navbar/session helpers). Paths align with the same feature areas as the HTML routers above.

### 1.4 Menu and client-side navigation

- **Declarative menu:** `src/front/config/menu_config.json` defines sections (Project, Ontology, Mapping, …), **routes** (e.g. `/project/`), groups, items, and `navbar_actions` (e.g. `projectSave`). The server can expose this to the client or the client can load it; either way it is the **source of truth** for section IDs and default tabs.
- **JavaScript:** under `src/front/static/global/js/` (sidebar, navbar, project actions). Pages load shared layout partials from templates and drive **in-page tabs** via menu item IDs rather than full page loads for every interaction.

### 1.5 Core UI concerns (conceptual)

- **SessionManager / `get_project`:** Most handlers depend on `SessionManager` to read/write the **current project blob** (ontology, mappings, registry pointers, triple-store options) in the file session.
- **Databricks integration:** UI services call helpers such as `get_databricks_client`, `get_databricks_host_and_token`, `resolve_warehouse_id`, and `VolumeFileService` to run SQL, read/write Unity Catalog volumes, and align with app vs. PAT auth.
- **TaskManager:** Long operations (builds, agent loops) register tasks under `/tasks/{id}` so the UI can poll progress without holding a request open.

---

## 2. API (REST v1, Digital Twin, GraphQL)

### 2.1 Registration

In `src/shared/fastapi/main.py`, `_register_routers` mounts:

- **Health** — `shared/fastapi/health.py` (app health; may overlap conceptually with v1 `/health`).
- **External REST v1** — `src/api/routers/v1.py` at **`/api/v1`** (stateless; credentials in body or headers), exposed via the mounted external API app (see `api.external_app` and `EXTERNAL_API_MOUNT_PREFIX`).
- **Digital Twin API** — `src/api/routers/digitaltwin.py` at **`/api/v1/digitaltwin`** (registry, project assets, build, triples, quality, reasoning).
- **GraphQL** — `src/back/fastapi/graphql_routes.py` at **`/graphql`** and on the external app (see `api.external_app`; per-project GraphQL execution).

The **OpenAPI** document is at `/openapi.json`; interactive docs at `/docs` and `/redoc`.

### 2.2 `/api/v1` (stateless integration API)

Defined in `api/routers/v1.py`. Representative endpoints:

- **`POST /api/v1/projects/list`**, **`POST /api/v1/project/info`**, ontology/classes/properties, mappings, R2RML extraction.
- **`POST /api/v1/query`** — SPARQL execution with `engine` choice (e.g. local vs Spark).
- **`POST /api/v1/query/validate`**, **`POST /api/v1/query/samples`**.

Pydantic models encode UC location (`catalog` / `schema` / `volume`) and optional `databricks_host` / `databricks_token`. Business logic for the external REST layer is delegated to `api.service` where appropriate.

### 2.3 `/api/v1/projects` and `/api/v1/project` (registry list & artifacts)

Defined in `api/routers/projects.py`. Representative paths:

| Method | Path | Purpose (summary) |
|--------|------|-------------------|
| GET | `/api/v1/projects` | List MCP-exposed registry projects |
| GET | `/api/v1/project/versions`, `/project/design-status` | Versions and design readiness |
| GET | `/api/v1/project/ontology`, `/r2rml`, `/sparksql` | Serialized design artifacts |

### 2.4 `/api/v1/digitaltwin` (Digital Twin)

Defined in `api/routers/digitaltwin.py`. Representative paths:

| Method | Path | Purpose (summary) |
|--------|------|-------------------|
| GET | `/registry` | Registry location (catalog, schema, volume) |
| GET | `/status`, `/stats` | Triple store status and statistics |
| POST | `/build` + GET `/build/{task_id}` | Trigger and poll materialization |
| GET | `/triples`, `/triples/find` | Raw or navigated triple access |
| POST | `/dataquality/start` + GET `/dataquality/{task_id}` | Async data quality |
| POST | `/reasoning/start` + GET `/reasoning/{task_id}`, GET `/reasoning/results` | Reasoning jobs and results |

These endpoints combine **registry configuration** (Unity Catalog volume for the project index) with **per-project** parameters and often use the same Databricks credential resolution patterns as the UI.

### 2.5 GraphQL

`back/fastapi/graphql_routes.py` wires `back.core.graphql` to expose an **ontology-derived schema** for a named project (`GET /graphql`, `GET/POST /graphql/{project}`, schema sub-routes). It complements SPARQL with a typed graph API.

### 2.6 Core API concerns (conceptual)

- **No browser session required** for v1/digitaltwin when callers pass credentials and UC paths; the **UI** relies on **FileSessionMiddleware** instead.
- **PermissionMiddleware** bypasses `/api/` and `/graphql/` prefixes at enforcement time today; authorization for external callers is primarily **token + UC ACLs**.
- **Async vs sync:** Some services use `run_blocking` to call blocking Databricks or RDF libraries from async routes; routes must **await** those helpers to avoid coroutine bugs.

---

## 3. Agents (`src/agents`)

### 3.1 Purpose

Agents are **LLM loops with tools**: they are invoked from **ontology** routes (and similar) for assisted editing, generation, or batch suggestions. They are **not** separate HTTP servers; they run **in-process** inside the main app worker.

### 3.2 Agent packages

Each agent typically has:

- **`engine.py`** — orchestration: system prompt, iteration budget, `AgentResult` / step tracing.
- **`tools.py`** — tool definitions and handlers that mutate or read **in-memory ontology state** / context.
- **`__init__.py`** or **`run_agent`** — entry used by FastAPI handlers.

Examples under `src/agents/`:

- **`agent_ontology_assistant`** — conversational edits to the loaded ontology (exposed via e.g. `ontology_assistant_chat` in `front/routes/ontology.py`).
- **`agent_owl_generator`** — generates OWL from natural language (async task wrapper in ontology routes).
- **`agent_auto_icon_assign`** — suggests emoji icons for entities.
- **`agent_auto_assignment`** — automated mapping or assignment workflows (see package for details).

Shared utilities include **`agents.llm_utils`** (LLM calls), **`agents.tools.context`** (`ToolContext` for project/session-aware tool execution), and **`agents.tracing`** (initialized from app `lifespan` in `src/shared/fastapi/main.py` via `setup_tracing()`).

### 3.3 Integration pattern

1. HTTP handler validates inputs and loads **project** from session (or task payload).  
2. Handler calls **`run_agent(...)`** with context (ontology snapshot, preferences).  
3. Agent returns structured output; handler persists changes to session/project or completes a **TaskManager** task with progress updates.  
4. UI polls **`/tasks/{task_id}`** or receives immediate JSON for short runs.

---

## 4. MCP server (`src/mcp-server`)

### 4.1 Role

The **MCP** package exposes OntoBricks capabilities (projects, status, entities, GraphQL) as **MCP tools and resources** for hosts such as Databricks Genie, Claude Desktop, or custom MCP clients. It does **not** duplicate the full rule engine server-side: it **`httpx`** calls the **main OntoBricks HTTP API** (`ONTOBRICKS_URL`).

### 4.2 Layout

- **`src/mcp-server/server/app.py`** — `FastMCP` server factory (`create_mcp_server`), tool/resource definitions, HTTP client helpers, response formatting; **`create_databricks_app`** builds a **combined** FastAPI app mounting MCP HTTP routes for Databricks App deployment.
- **`src/mcp-server/server/main.py`** — CLI entry (`uv run mcp-ontobricks`) running **`combined_app`** with Uvicorn.
- **`app.yaml`** / deploy scripts (see `docs/deployment.md`) wire env vars: `ONTOBRICKS_URL`, registry volume or catalog/schema/volume, warehouse ID, etc.

### 4.3 Operating modes (from module docstring)

- **`databricks`** — combined FastAPI + FastMCP app; uses **service principal** / app identity to reach the main app.  
- **`standalone`** — separate process (stdio/SSE HTTP) pointing at `ONTOBRICKS_URL` (e.g. localhost).  
- **`mounted`** — optional embedding in the main OntoBricks process (loopback to the same host/port).

### 4.4 Navigation / protocol

MCP has **no Jinja routes**; “navigation” is **tool choice** (e.g. `list_projects` → `select_project` → `describe_entity`). Resources such as `ontobricks://projects` map to REST paths like **`/api/v1/projects`**. GraphQL-related tools POST to **`/graphql/<project>`** with the same registry parameters the REST API expects.

### 4.5 Core MCP concerns (conceptual)

- **Auth alignment:** In Databricks mode, MCP identity may differ from an end-user browser session; registry and project visibility follow **OAuth M2M / SP** permissions on UC objects.  
- **Configuration:** Registry location must match the main app’s bound volume expectations to avoid 404s or stale paths (see registry helpers in the main codebase).  
- **Health:** `combined_app` exposes a small **`GET /`** JSON health object (service name, `ontobricks_url`, warehouse, registry display).

---

## 5. Quick file index

| Area | Key paths |
|------|-----------|
| App factory | `src/shared/fastapi/main.py` |
| Health | `src/shared/fastapi/health.py` |
| Settings, constants & templates | `src/shared/config/settings.py`, `src/shared/config/constants.py`, `src/front/fastapi/dependencies.py` |
| Sessions | `src/back/objects/session/` (`middleware.py`, `manager.py`, `project_session.py`) |
| Databricks / volumes | `src/back/core/databricks/`, `src/back/core/helpers/` |
| Registry / permissions | `src/back/objects/registry/` |
| Project domain (UC, metadata, layout) | `src/back/objects/project/` (`Project` class); feature services in `src/back/services/` (e.g. `home.py`, `settings.py`) |
| Tasks | `src/back/core/task_manager/`, `src/api/routers/internal/tasks.py` |
| UI HTML routes | `src/front/routes/*.py` |
| Internal JSON API (session-aware) | `src/api/routers/internal/*.py` |
| REST v1 | `src/api/routers/v1.py`, `src/api/service.py` |
| Project list & artifacts | `src/api/routers/projects.py` |
| Digital Twin REST | `src/api/routers/digitaltwin.py` |
| GraphQL | `src/back/fastapi/graphql_routes.py` |
| Agents | `src/agents/**` |
| MCP | `src/mcp-server/server/app.py`, `src/mcp-server/server/main.py` |

---

*For day-to-day contributor workflows (tests, deps, rights), see [`development.md`](development.md). For product-level architecture, see [`architecture.md`](architecture.md).*
