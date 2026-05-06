<p align="center">
  <img src="src/front/static/global/img/ontobricks-icon.svg" alt="OntoBricks Logo" width="120" height="120">
</p>

<h1 align="center">OntoBricks 0.2.1</h1>

<p align="center">
  <strong>Digital Twin Builder for Databricks</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/python-3.10+-blue.svg" alt="Python">
  <img src="https://img.shields.io/badge/fastapi-0.109+-green.svg" alt="FastAPI">
</p>

## Project Description

OntoBricks is a web application that transforms Databricks tables into a materialized knowledge graph. It lets you design ontologies (OWL), map them to Unity Catalog tables via R2RML, materialize triples into a Delta or LadybugDB triple store, reason over the graph (OWL 2 RL, SWRL, SHACL), and query it through an auto-generated GraphQL API. The entire pipeline — from metadata import to a queryable knowledge graph — can run in four clicks using LLM-powered automation.

## Project Support

Please note that all projects in the /databrickslabs github account are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support.

## Building the Project

OntoBricks uses [uv](https://docs.astral.sh/uv/) for dependency management. All dependencies are declared in `pyproject.toml`.

```bash
# Clone the repository
git clone <repository-url>
cd OntoBricks

# Install dependencies (uv resolves them from pyproject.toml)
uv sync

# Or use the setup script
scripts/setup.sh
```

### Prerequisites

- Python 3.10 or higher
- Databricks workspace access with a Personal Access Token
- A SQL Warehouse ID
- A Unity Catalog Volume for the domain registry
- *(Optional)* A Databricks Lakebase Postgres database — required only
  when the admin switches the registry storage backend from
  **Volume** (default) to **Lakebase** in Settings → Registry.
  OntoBricks targets **Lakebase Autoscaling** exclusively (Provisioned
  instances are not supported). Install the optional driver with
  `uv sync --extra lakebase`.

## Deploying / Installing the Project

### Local Development

```bash
# Configure credentials
cp .env.example .env
# Edit .env with your Databricks host, token, and warehouse ID

# Start the application
scripts/start.sh
# Open http://localhost:8000
```

### Deploy to Databricks Apps

```bash
# Install and configure the Databricks CLI
pip install databricks-cli
databricks configure --token

# Deploy
make deploy
# Or: scripts/deploy.sh
```

After deployment, bind the **sql-warehouse** and **volume** resources in the Databricks Apps UI (**Compute > Apps > ontobricks > Resources**). If the registry volume is empty, open the app and click **Settings > Registry > Initialize**.

> **Lakebase backend (optional).** To deploy with the Lakebase Postgres
> backend instead of (in addition to) the Volume, deploy to the
> `dev-lakebase` target (`databricks bundle deploy -t dev-lakebase`) and
> tune the bundle variables `lakebase_project`, `lakebase_branch`,
> `lakebase_database_resource_segment` (the `db-…` id from
> `databricks postgres list-databases "projects/<id>/branches/<branch>" -o json`,
> **not** the Postgres database name shown in the SQL UI), and
> `lakebase_registry_schema` (mirror in `app.yaml` as `LAKEBASE_SCHEMA`).
> The DAB composes the full Apps `postgres.database` path. The DAB binds a `database` Apps resource so the
> runtime auto-injects `PGHOST`/`PGPORT`/`PGDATABASE`/`PGUSER`; the app
> mints the OAuth token automatically (no user secret required). The
> default `dev`/`prod` targets stay Volume-only and keep working as
> before.

> **First deploy only:** `make deploy` runs `scripts/bootstrap-app-permissions.sh` automatically, which grants each app's service principal `CAN_MANAGE` on itself. Without that grant the middleware cannot read the app's own ACL and every first-time visitor — including the deploying `CAN_MANAGE` user — lands on the access-denied page. If you deploy via `databricks bundle deploy` directly, run `make bootstrap-perms` once afterwards (it is idempotent).

See [Deployment Guide](docs/deployment.md) for the full checklist including resource configuration and permissions.

## Releasing the Project

1. Ensure all tests pass: `make test`
2. Update the version in `pyproject.toml`
3. Commit, tag, and push:

```bash
git add -A && git commit -m "Release vX.Y.Z"
git tag vX.Y.Z
git push origin main --tags
```

4. Deploy the new version: `make deploy`

## Using the Project

### Automated Pipeline (4 clicks)

| Step | Action | What Happens |
|------|--------|--------------|
| **1** | **Import Metadata** (Domain > Metadata) | Fetches table and column metadata from Unity Catalog |
| **2** | **Generate Ontology** (Ontology > Wizard) | LLM designs entities, relationships, and attributes from your metadata |
| **3** | **Auto-Map** (Mapping > Auto-Map) | LLM generates SQL mappings for every entity and relationship |
| **4** | **Synchronize** (Digital Twin > Status) | Executes mappings and populates the triple store |

### Domain & registry (0.1.2 UX)

- **Ontology Designer** — the main ontology graph view lives under **Ontology → Designer** (visual canvas + AI Assistant).
- **Domain Cockpit (Validation)** — **Active Version** shows which registry version is exposed via **API / MCP**; it can differ from the version you have loaded in the editor.
- **Registry → Browse** — only place to **set the Active (API/MCP) version** for a domain; **Domain → Versions** shows that status as a read-only badge.
- **New domain** — after **New Domain**, a full-page loading overlay runs until Domain Information finishes its first load.
- **Domain Information** — triple-store / snapshot / local graph paths update when you **commit** the domain name (blur or change) or change version (aligned with naming rules before save).
- **Duplicate names** — **Save to Unity Catalog** is blocked if the sanitized domain name already exists in the registry (inline check + confirmation before POST).
- **Navbar** — domain name and version in the top bar refresh after load, save, clear, import, and version switches (browser cache invalidated on those actions).

### Manual Workflow

1. **Design** an ontology visually using the OntoViz canvas, or import OWL/RDFS/industry standards (FIBO, CDISC, IOF)
2. **Map** ontology entities to Databricks tables with column-level precision
3. **Build** the Digital Twin — materializes triples into the triple store (incremental by default)
4. **Query** through the GraphQL playground or explore the interactive knowledge graph
5. **Reason** over the graph — run OWL 2 RL inference, SWRL rules, SHACL validation, and constraint checks

### Knowledge Graph Features

- **Two-phase search** — preview matching entities in a flat list, then select specific ones to expand into the full graph with relationships and neighbors
- **Configurable search depth** — control the maximum traversal depth and entity cap for graph expansion
- **Bridge navigation** — follow cross-domain bridges to automatically switch domains and focus on the target entity in the knowledge graph
- **Data cluster detection** — detect communities in the knowledge graph using Louvain, Label Propagation, or Greedy Modularity algorithms; available client-side (Graphology) for the visible subgraph and server-side (NetworkX) for the full graph; cluster results can be visualized with color-by-cluster mode and collapsed into super-nodes
- **Data quality violation limits** — cap the number of violations displayed per rule (configurable via dropdown, default 10) for faster quality checks
- **Per-rule progress tracking** — SWRL inference and data quality checks report progress for each individual rule

### AI Assistant

The **Ontology Designer** view (**Ontology → Designer**) includes a floating AI Assistant (bottom-right of the canvas) that lets you modify your ontology through natural language commands — add entities, remove orphans, list relationships, and more. Conversation history is maintained within the session.

### Navigation & Performance

- **Deep-linked sidebar sections** — shareable URLs, browser Back/Forward support
- **Breadcrumb navigation** — always see your position (Registry > Domain > Ontology > Section)
- **Keyboard shortcuts** — `Cmd/Ctrl+S` save, `Cmd/Ctrl+K` search, `?` help overlay
- **SQL connection pooling** — reusable database connections, no per-query TLS handshake
- **CSRF protection** — double-submit cookie for all state-changing requests
- **Structured JSON logging** — set `LOG_FORMAT=json` for production-grade observability

### MCP Integration

OntoBricks exposes the knowledge graph to LLM agents via the [Model Context Protocol](https://modelcontextprotocol.io/). Deploy the companion `mcp-ontobricks` app and connect from Cursor, Claude Desktop, or the Databricks Playground.

### Registry Import / Export (CLI)

Promote domains between Databricks environments with the
`scripts/registry_transfer.sh` command-line tool — export a curated subset
of domains/versions from a source registry into a `.zip`, then preview and
commit it into the target registry. No UI, no HTTP endpoint. See
[Registry Import / Export (CLI)](docs/import-export.md) for the full
reference and examples.

### Documentation

Full documentation is available in [`docs/`](docs/README.md). For a comprehensive feature list and architecture details, see [INFO.md](docs/INFO.md).
