<p align="center">
  <img src="src/front/static/global/img/ontobricks-icon.svg" alt="OntoBricks Logo" width="120" height="120">
</p>

<h1 align="center">OntoBricks 0.5.0</h1>

<p align="center">
  <strong>Digital Twin Builder for Databricks</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/python-3.10+-blue.svg" alt="Python">
  <img src="https://img.shields.io/badge/fastapi-0.109+-green.svg" alt="FastAPI">
</p>

## Project Description

OntoBricks is a web application that transforms Databricks tables into a materialized knowledge graph. It lets you design ontologies (OWL), map them to Unity Catalog tables via R2RML, materialize triples into a Delta-backed triple store and a Lakebase Postgres graph engine, reason over the graph (OWL 2 RL, SWRL, SHACL), and query it through an auto-generated GraphQL API. The entire pipeline — from metadata import to a queryable knowledge graph — can run in four clicks using LLM-powered automation.

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
- Databricks workspace access (Databricks Apps must be enabled). Local
  development uses a Personal Access Token; production uses the App's
  service principal.
- A SQL Warehouse (you'll need its ID for local dev).
- **Databricks Lakebase Autoscaling** project + branch + Postgres
  database — **required since v0.4.0** for the domain registry
  (domains, versions, permissions, schedules, global config) and the
  Graph DB triple store. Provisioned Lakebase instances are **not**
  supported. The Postgres driver (`psycopg[binary]` + `psycopg-pool`)
  is declared as an optional dependency so volume-only forks can opt
  out — install with `uv sync --extra lakebase` for any normal
  deployment.
- **Unity Catalog Volume** in the catalog/schema that hosts the
  triplestore VIEWs (`triplestore_<domain>_v<n>`). The volume is
  reserved for binary artefacts (`documents/` uploads — domain-scoped
  attachments imported by the ontology designer).
- `psql` (libpq client) on `PATH` for the Lakebase permission
  bootstrap scripts (`brew install libpq && brew link --force libpq`
  on macOS).

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
# Install and authenticate the Databricks CLI (>= 0.250.0)
brew install databricks            # or curl -fsSL https://databricks.com/install.sh | sh
databricks auth login --host https://<workspace>

# Edit scripts/deploy.config.sh (warehouse, registry catalog/schema,
# Lakebase project/branch/database — see the file header) and then:
make deploy
# Or directly: scripts/deploy.sh
```

`scripts/deploy.sh` generates `app.yaml` from `app.yaml.template` +
`scripts/deploy.config.sh`, validates and deploys the DAB bundle on
target `dev-lakebase`, runs `scripts/bootstrap-app-permissions.sh`
(app SP `CAN_MANAGE` on itself), then runs
`scripts/bootstrap-lakebase-perms.sh` on the registry / graph / sync
schemas. All steps are idempotent.

After the first deploy, bind the **sql-warehouse**, **volume**, and
**postgres** (Lakebase) resources in the Databricks Apps UI
(**Compute > Apps > <your-app> > Resources**) if the DAB bind did
not take. Open the app and click **Settings > Registry > Initialize**
to create the Lakebase schema; re-run `make bootstrap-lakebase` once
afterwards so the freshly created schema picks up `USAGE/DML`.

> **Lakebase deploy targets.** Pick a Databricks Lakebase Autoscaling
> project + branch and a Postgres database, then set the
> `LAKEBASE_PROJECT`, `LAKEBASE_BRANCH`,
> `LAKEBASE_DATABASE_RESOURCE_SEGMENT` (the `db-…` id from
> `databricks postgres list-databases "projects/<id>/branches/<branch>" -o json`,
> **not** the Postgres database name shown in the SQL UI), and
> `LAKEBASE_REGISTRY_SCHEMA` defaults in `scripts/deploy.config.sh`.
> The DAB composes the full Apps `postgres.database` path and binds a
> `postgres` Apps resource so the runtime auto-injects
> `PGHOST` / `PGPORT` / `PGDATABASE` / `PGUSER`; the app mints the
> Lakebase JWT automatically (no user secret required).

> **Upgrading from a pre-v0.4.0 deployment.** Pre-v0.4.0 stored the
> entire registry as JSON on the Unity Catalog Volume. Run
> `scripts/migrate-registry-to-lakebase.sh` once before upgrading to
> v0.4.0+ to copy every JSON-shaped artefact (domains, versions,
> permissions, schedules, global config) into Lakebase. Binary
> artefacts on the Volume are left untouched.

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

### Graph DB engine (Settings → Graph DB)

The **graph** triple-store backend is pluggable; the abstraction (`GraphDBFactory` / `GraphDBBackend`) is preserved so additional engines can be added in the future. Today only one engine ships:

- **Lakebase (Postgres)** — default; **three Postgres objects per domain version** (`*_sync` bulk-data table, `*__app` companion for reasoning/cohort writes, `g_<dom>_v<n>` UNION view for reads) inside a configurable Postgres schema on the **App-bound** Lakebase database (same connection as the optional Lakebase registry backend). Requires the `lakebase` extra (`uv sync --extra lakebase`) so `psycopg` is installed.

Engine-specific options are stored as global JSON (`graph_engine_config`). For Lakebase the supported keys are **`database`** (optional override of `PGDATABASE`), **`schema`** (optional, default `ontobricks_graph`), **`sync_mode`** (`app_managed` default, or `managed_synced` to delegate bulk ingest to a Databricks Lakeflow snapshot pipeline), **`sync_table_mode`** (`snapshot` / `triggered` / `continuous` — `snapshot` is the recommended mode), **`sync_timeout_s`** (default 600), **`sync_uc_catalog`** (UC catalog the synced table is registered in; defaults to the snapshot Delta catalog when unset), and **`sync_uc_schema`** (UC schema segment for the synced-table FQN; defaults to the registry UC schema so the Lakeflow object lands in the same UC namespace as other registry artefacts). See `docs/lakebase-graphdb.md` for the full reference.

> **Lakebase permission grants (three schemas).** The app service principal needs `USAGE + DML` on up to three Postgres schemas — each covered by one run of `scripts/bootstrap-lakebase-perms.sh`:
>
> | Schema | When to run | Deploy config var |
> |---|---|---|
> | Registry schema (e.g. `ontobricks_registry`) | After `Settings → Registry → Initialize` | `LAKEBASE_BOOTSTRAP_SCHEMA` |
> | Graph schema (e.g. `ontobricks_graph`) | After first Digital Twin `Build` | `LAKEBASE_GRAPH_SCHEMA` |
> | Sync schema (e.g. `ontobricks`) | After first Lakeflow snapshot (`managed_synced` only) | `LAKEBASE_SYNC_SCHEMA` |
>
> `scripts/deploy.sh` calls the bootstrap for all three automatically. If the Graph DB is on a **separate Lakebase instance** from the registry, set `LAKEBASE_GRAPH_PROJECT`, `LAKEBASE_GRAPH_BRANCH`, and `LAKEBASE_GRAPH_DATABASE` in `scripts/deploy.config.sh` so the second and third grants target the correct instance.

> **Lakebase build performance.** When the active engine is Lakebase, the Digital Twin build streams warehouse rows in `fetchmany` batches (`SQLWarehouse.iter_rows`) and ingests them via `COPY FROM STDIN` into a per-batch temp table followed by `INSERT … ON CONFLICT DO NOTHING` (and the symmetrical `DELETE … USING` for incremental removes). The FastAPI process never holds the full graph or the full diff: snapshot CTAS and `EXCEPT` execution stay warehouse-side, the app pipes one batch at a time. There is no Volume archive thread — Postgres is the system of record for the graph.

> **Lakebase managed-synced mode.** When `graph_engine_config.sync_mode = "managed_synced"`, the bulk R2RML data movement is moved entirely off the app: a Databricks Lakeflow snapshot pipeline keeps a Postgres synced table in lock-step with the R2RML view, and the FastAPI process only orchestrates (`SyncedTableManager.ensure` + `trigger_and_wait`). Reasoning + cohort writes stay on the direct PG path through a writable companion table; readers see both via a UNION view (back-compat name). PG layout per graph version: `g_<dom>_v<n>_sync` (Lakeflow), `g_<dom>_v<n>__app` (app), `g_<dom>_v<n>` (UNION view). See `docs/graphdb-integration.md §9` for the full architecture.

### Manual Workflow

1. **Design** an ontology visually using the OntoViz canvas, or import OWL/RDFS/industry standards (FIBO, CDISC, IOF, HL7 FHIR R4/R4B/R5)
2. **Map** ontology entities to Databricks tables with column-level precision
3. **Build** the Digital Twin — materializes triples into the triple store (incremental by default)
4. **Query** through the GraphQL playground or explore the interactive knowledge graph
5. **Reason** over the graph — run OWL 2 RL inference, SWRL rules, SHACL validation, and constraint checks

### Knowledge Graph Features

- **Two-phase search** — preview matching entities in a flat list, then select specific ones to expand into the full graph with relationships and neighbors
- **Configurable search depth** — control the maximum traversal depth and entity cap for graph expansion
- **Right-click "Expand neighbours"** — enrich the current graph in place with N-hop neighbours of any selected node (depth follows the right-pane Depth slider, default 2); newly added entities are highlighted and the camera zooms to frame them, with a non-blocking spinner in the canvas top-right while the request runs
- **Bridge navigation** — follow cross-domain bridges to automatically switch domains and focus on the target entity in the knowledge graph
- **Data cluster detection** — detect communities in the knowledge graph using Louvain, Label Propagation, or Greedy Modularity algorithms; available client-side (Graphology) for the visible subgraph and server-side (NetworkX) for the full graph; cluster results can be visualized with color-by-cluster mode and collapsed into super-nodes
- **Cohort discovery** — group entities that travel together using rule-based linkage (shared resources via predicates) and compatibility constraints (same-value, value-equals, value-in, value-range); deterministic, explainable cohorts with live counters, why/why-not explainers, and idempotent materialisation as graph triples (`:inCohort`) or Unity Catalog Delta tables. See [`docs/cohort_discovery.md`](docs/cohort_discovery.md).
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

### Registry OBX Export / Import (UI)

Export one or more domains directly from **Registry → Browse** to a portable
`.obx` file with per-domain version-mode selection (Latest / Active / All /
Choose). Import with per-domain conflict resolution (Skip / Overwrite / Rename).
No command line required — ideal for ad-hoc transfers and cross-tenant sharing.

### Registry Import / Export (CLI)

For automated promotion pipelines use the
`scripts/registry_transfer.sh` command-line tool — export a curated subset
of domains/versions from a source registry into a `.zip`, then preview and
commit it into the target registry. See
[Registry Import / Export](docs/import-export.md) for the full reference,
examples, and a comparison of the OBX UI vs CLI approaches.

### Ontology Pitfalls Detector

Detect 19 structural, logical, and semantic pitfalls (P1.1–P4.7) in your
ontology from the **Ontology → Pitfalls** sidebar panel. Fast graph-only
checks run immediately; ML-heavy checks (semantic similarity, NLP naming)
require installing the optional extra:

```bash
uv sync --extra pitfalls
```

### Documentation

Full documentation is available in [`docs/`](docs/README.md). For a comprehensive feature list and architecture details, see [INFO.md](docs/INFO.md).
