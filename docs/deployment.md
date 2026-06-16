# OntoBricks Deployment Guide

## Overview

This guide covers deploying OntoBricks both locally for development and to Databricks Apps for production use, including the optional MCP server for Databricks Playground integration and instructions for deploying to a new workspace.

Deployment uses **Databricks Asset Bundles (DAB)** — a declarative, repeatable approach that deploys both apps in a single command from `databricks.yml` at the project root.

**Architecture at a glance:**

```
┌──────────────────────┐      ┌──────────────────────┐
│   ontobricks-XXX     │      │   mcp-ontobricks     │
│   (Databricks App)   │◄─────│   (Databricks App)   │
│                      │ REST │                      │
│   Web UI + REST API  │ API  │   MCP Server for     │
│   Knowledge Graph    │      │   Databricks         │
│                      │      │   Playground          │
└──────────┬───────────┘      └──────────────────────┘
           │
           ├──────────────────┐
           ▼                  ▼
   ┌───────────────────────────────────┐
   │  SQL Warehouse (Delta view)       │
   │  + Lakebase Postgres (Graph DB)   │
   └───────────────────────────────────┘
           │
           ▼
   ┌───────────────┐
   │  MLflow        │
   │  Tracking      │
   │  (Experiments) │
   └───────────────┘
```

**Key files:**

| File | Purpose |
|------|---------|
| `databricks.yml` | DAB bundle definition — apps, permissions, targets |
| `app.yaml` | Main app runtime config — command, env vars, resource declarations |
| `src/mcp-server/app.yaml` | MCP server runtime config |
| `.databricksignore` | Excludes non-runtime files from the bundle sync |
| `scripts/deploy.sh` | Convenience wrapper around DAB commands |

---

## Architecture

This section provides detailed diagrams covering the OntoBricks component topology, Lakebase Postgres schema layout, data-flow through the Digital Twin build pipeline, network ports, and all permission layers required for a production deployment.

### A. Component Architecture

```
                    ┌──────────────────────────────────────────────────────────────────────────┐
                    │                         Databricks Workspace                               │
                    │                                                                             │
  Browser users ──► │  ┌───────────────────────────────────────────────────────────────────┐    │
                    │  │           ontobricks-XXX   (Databricks App)                        │    │
                    │  │           FastAPI + Uvicorn · port: $DATABRICKS_APP_PORT           │    │
                    │  │                                                                     │    │
                    │  │   ┌──────────────────┐  ┌──────────────────┐  ┌─────────────────┐ │    │
                    │  │   │    Web UI         │  │   REST API       │  │   GraphQL       │ │    │
                    │  │   │  (Jinja2 + JS)    │  │   /api/v1/*      │  │   /graphql      │ │    │
                    │  │   └──────────────────┘  └──────────────────┘  └─────────────────┘ │    │
                    │  │                                                                     │    │
                    │  │   LLM Agents  (Databricks Model Serving / Foundation Models):      │    │
                    │  │   OWL Generator · Auto-Assignment · Ontology Assistant             │    │
                    │  │   Digital-Twin Chat · Cohort Generator · Auto-Icon Assign          │    │
                    │  └────────────────────────┬────────────────────────────────────────── ┘    │
                    │                           │  REST /api/v1/*  (OAuth M2M)                   │
  Playground ──────►│  ┌────────────────────────▼──────────────────────────────────────┐         │
  Cursor / Claude   │  │       mcp-ontobricks  (Databricks App)                        │         │
  (MCP protocol)    │  │       FastMCP · Uvicorn · port: $DATABRICKS_APP_PORT           │         │
                    │  └───────────────────────────────────────────────────────────────┘         │
                    │                                                                             │
                    │  ═══════════════════════ Bound Resources ═══════════════════════════════   │
                    │                                                                             │
                    │  ┌──────────────────┐  ┌─────────────────────────┐  ┌─────────────────┐  │
                    │  │  SQL Warehouse    │  │       UC Volume          │  │ Lakebase        │  │
                    │  │  CAN_USE          │  │  WRITE_VOLUME            │  │ Postgres        │  │
                    │  │                   │  │  /Volumes/<c>/<s>/<vol>  │  │ CAN_CONNECT     │  │
                    │  │ · SPARQL → SQL    │  │                          │  │ _AND_CREATE     │  │
                    │  │ · Delta VIEWs     │  │ · OWL / R2RML artefacts  │  │ (dev-lakebase)  │  │
                    │  │ · UC metadata     │  │ · Domain files           │  │                 │  │
                    │  │ · Lakeflow sync   │  │ · History / audit logs   │  │ · Registry      │  │
                    │  └──────────────────┘  └─────────────────────────┘  │   tables        │  │
                    │                                                       │ · Graph triple  │  │
                    │  ┌────────────────────────┐  ┌──────────────────┐   │   store         │  │
                    │  │  Model Serving / FMs    │  │ MLflow Tracking  │   └─────────────────┘  │
                    │  │  (LLM endpoints, SQL    │  │ /Shared/onto…    │                        │
                    │  │   Wizard)               │  │ (agent traces)   │                        │
                    │  └────────────────────────┘  └──────────────────┘                        │
                    └──────────────────────────────────────────────────────────────────────────┘
```

### B. Lakebase as Graph DB — Schema Layout

OntoBricks uses Lakebase Postgres (Autoscaling) as **both** its registry store (structured metadata) and its Graph DB (triple store). Three schemas live inside the same Postgres database; each is bootstrapped at a different stage of the deployment lifecycle.

```
╔══════════════════════════════════════════════════════════════════════════════════════════╗
║          Lakebase Postgres — Autoscaling project, bound via app.yaml resource           ║
║          Database (datname): ontobricks_registry  (or databricks_postgres in legacy)    ║
╠═══════════════════════════════════════╦════════════════════════════════════════════════╣
║  Schema: ontobricks_registry          ║  Schema: ontobricks_graph                      ║
║  Created by: Settings → Initialize    ║  Created by: first Digital Twin Build          ║
║  Granted by: make bootstrap-lakebase  ║  Granted by: make bootstrap-lakebase           ║
╠═══════════════════════════════════════╣════════════════════════════════════════════════╣
║  registries                           ║  Per-domain, per-version triple tables:        ║
║   └ one row per OntoBricks instance   ║                                                ║
║  global_config  (JSONB settings)      ║  g_<domain>_v<n>_sync                          ║
║  domains        (folder tree)         ║   ← triples bulk-loaded by:                   ║
║  domain_versions (full documents)     ║     · app_managed: COPY FROM STDIN             ║
║  domain_permissions (roles / ACL)     ║     · managed_synced: Lakeflow snapshot        ║
║  schedules      (build definitions)   ║                                                ║
║  schedule_runs  (build run history)   ║  g_<domain>_v<n>__app                          ║
║                                       ║   ← reasoning results / cohort writes          ║
║                                       ║                                                ║
║                                       ║  g_<domain>_v<n>  (UNION VIEW — read only)    ║
║                                       ║   ← target of SPARQL queries                  ║
║                                       ║   SELECT … FROM _sync UNION ALL __app         ║
║                                       ║                                                ║
║                                       ║  Columns: subject · predicate · object        ║
║                                       ║           datatype · lang                     ║
╠═══════════════════════════════════════╩════════════════════════════════════════════════╣
║  Schema: ontobricks   (optional — managed_synced sync mode only)                       ║
║  Lakeflow synced tables — Lakebase mirror of the UC Delta triplestore VIEW              ║
║  Created by: first Lakeflow snapshot on a managed_synced domain                        ║
║  Granted by: manual bootstrap-lakebase-perms.sh -s <sync_schema> (managed_synced)      ║
╚══════════════════════════════════════════════════════════════════════════════════════════╝
```

### C. Digital Twin Build — Data Flow

```
  UC Source Tables         SQL Warehouse               Lakebase Postgres
  ─────────────────────    ──────────────────────────  ──────────────────────────────────

  catalog.schema.A ──┐                                 ┌─ app_managed (default) ─────────┐
  catalog.schema.B ──┼──► R2RML engine                 │  fetchmany() batches             │
  catalog.schema.N ──┘    (rr:sqlQuery + joins)         │  COPY FROM STDIN                 │
                          converts rows → triples        │  INSERT ON CONFLICT DO NOTHING   │
                                   │                    └────────────────────────────────┬─┘
                                   │ triple stream ─────────────────────────────────────►
                                   │                                                      g_<dom>_v<n>_sync
                                   ▼                    ┌─ managed_synced (optional) ─────┐
                        CREATE OR REPLACE VIEW           │  Lakeflow orchestrates           │
                        triplestore_<domain>_Vn          │  Postgres Synced Table API       │
                        (persisted in Unity Catalog)     └────────────────────────────────┬─┘
                                                                                           │
                                                         reasoning / cohort writes ────────►  g_<dom>_v<n>__app
                                                                                           │
                                                         ┌──────────────────────────────────────────────────┐
                                                         │  g_<domain>_v<n>  (UNION VIEW — reader-facing)   │
                                                         │  SELECT * FROM g_<dom>_v<n>_sync                 │
                                                         │  UNION ALL                                        │
                                                         │  SELECT * FROM g_<dom>_v<n>__app                 │
                                                         └─────────────────────┬────────────────────────────┘
                                                                               │
                    SPARQL query ──► SPARQL→SQL translator ──► SQL Warehouse ──┘  (or direct Lakebase path)
```

### D. Network Ports

**Local development** (`scripts/start.sh` / `uv run python run.py`):

```
┌──────────────────────────────────────────────────────────────────────────┐
│  Service                   Port    Transport   Notes                       │
│  ─────────────────────     ──────  ──────────  ──────────────────────────  │
│  OntoBricks (FastAPI)       8000   HTTP        http://localhost:8000        │
│  MCP server (HTTP mode)     9100   HTTP        --http flag, dev/test only   │
│  MCP server (stdio)           —    stdio       Cursor / Claude Desktop      │
│  Lakebase Postgres           5432   TLS         PGHOST/PGPORT from .env     │
│  SQL Warehouse                443   HTTPS       Databricks connector / SDK  │
└──────────────────────────────────────────────────────────────────────────┘
```

**Deployed on Databricks Apps** (all outbound — no inbound ports need to be opened):

```
┌────────────────────────────────────────────────────────────────────────────────────────────┐
│  Service              Port                   Exposure                  Auth                  │
│  ─────────────────    ─────────────────────  ────────────────────────  ──────────────────── │
│  ontobricks-XXX       $DATABRICKS_APP_PORT   https://<ws>/apps/…       Databricks SSO        │
│  mcp-ontobricks       $DATABRICKS_APP_PORT   https://<ws>/apps/…       Databricks SSO        │
│  Lakebase Postgres    5432 (TLS)             PGHOST injected by Apps   OAuth JWT (no passwd)  │
│  SQL Warehouse         443 (HTTPS)           internal via SDK           OAuth (SP token)      │
│  Model Serving / FMs   443 (HTTPS)           internal via SDK           OAuth (SP token)      │
│  MLflow Tracking        443 (HTTPS)           internal via SDK           OAuth (SP token)      │
└────────────────────────────────────────────────────────────────────────────────────────────┘
```

> Lakebase password authentication is **not used** — `LakebaseAuth` mints a short-lived JWT via `POST /api/2.0/postgres/credentials` on every connection. No `PGPASSWORD` is required or stored.

### E. Permission Layers

All four layers must be satisfied before the application is fully functional. They are independent and must each be configured separately.

```
╔══════════════ Layer 1 — Databricks App-Level (CAN_MANAGE / CAN_USE) ══════════════════╗
║                                                                                         ║
║  Principal            App                 Level          Set by                         ║
║  ─────────────────── ─────────────────── ────────────── ──────────────────────────── ║
║  Deploying user       ontobricks-XXX      CAN_MANAGE     databricks.yml > permissions   ║
║  group: users         ontobricks-XXX      CAN_USE        databricks.yml > permissions   ║
║  Main app SP          ontobricks-XXX      CAN_MANAGE     bootstrap-app-permissions.sh   ║
║  MCP app SP           mcp-ontobricks      CAN_MANAGE     bootstrap-app-permissions.sh   ║
║  MCP app SP           ontobricks-XXX      CAN_USE        bootstrap-app-permissions.sh   ║
║                                                                                         ║
║  ► make bootstrap-perms  (idempotent; auto-called by make deploy)                       ║
╚═════════════════════════════════════════════════════════════════════════════════════════╝

╔══════════════ Layer 2 — Unity Catalog Data-Plane Grants ═══════════════════════════════╗
║                                                                                         ║
║  Object                              Privilege                        Required for       ║
║  ─────────────────────────────────── ──────────────────────────────── ────────────────  ║
║  CATALOG  <registry_catalog>         USE CATALOG                      all operations     ║
║  SCHEMA   <registry_catalog>.<sch>   USE SCHEMA                       all operations     ║
║                                      CREATE TABLE                     triplestore        ║
║                                      CREATE VIEW                      Digital Twin Sync  ║
║  VOLUME   <cat>.<sch>.<vol>           READ VOLUME + WRITE VOLUME       artefact storage   ║
║  CATALOG  <source_catalog>            USE CATALOG                      Data Source picker ║
║  SCHEMA   <src_cat>.<src_sch>         USE SCHEMA                       R2RML builds       ║
║  TABLE    <src_cat>.<src_sch>.<tbl>   SELECT                           per mapping entry  ║
║                                                                                         ║
║  ► Grant once as UC admin (see §3 for full SQL snippets)                                ║
╚═════════════════════════════════════════════════════════════════════════════════════════╝

╔══════════════ Layer 3 — Lakebase Postgres (control + data plane) ══════════════════════╗
║                                                                                         ║
║  Resource                             Grant              Timing                          ║
║  ─────────────────────────────────── ────────────────── ───────────────────────────── ║
║  Lakebase project (control-plane)     CAN_USE            before first deploy             ║
║  Schema: ontobricks_registry          USAGE + CREATE +   after Settings → Initialize     ║
║                                       DML on tables      (schema created by the app)     ║
║  Schema: ontobricks_graph             USAGE + CREATE +   after first Digital Twin Build  ║
║                                       DML on tables                                      ║
║  Schema: ontobricks  (sync, optional) USAGE + CREATE +   after first Lakeflow snapshot   ║
║                                       DML on tables                                      ║
║                                                                                         ║
║  DML = SELECT + INSERT + UPDATE + DELETE on tables                                      ║
║      + USAGE + SELECT + UPDATE on sequences (bigserial PKs)                             ║
║      + ALTER DEFAULT PRIVILEGES  ← covers all future tables automatically               ║
║                                                                                         ║
║  ► make bootstrap-lakebase  (idempotent; auto-called by make deploy for dev-lakebase)   ║
╚═════════════════════════════════════════════════════════════════════════════════════════╝

╔══════════════ Layer 4 — In-App OntoBricks Roles ═══════════════════════════════════════╗
║                                                                                         ║
║  Role     Source                          Access                                         ║
║  ──────── ─────────────────────────────── ──────────────────────────────────────────── ║
║  Admin    CAN_MANAGE on Databricks App    Full access + manage permission list           ║
║  Editor   domain_permissions table        Full read + write on all features              ║
║  Viewer   domain_permissions table        Read-only (no create / edit / delete)          ║
║  (none)   not in list                     Access Denied — redirected to error page       ║
║                                                                                         ║
║  ► Settings → Permissions (only visible to Admins)                                      ║
╚═════════════════════════════════════════════════════════════════════════════════════════╝
```

> **Deployment order matters.** Layer 1 bootstrap must run before the first user logs in. Layer 2 grants must be in place before any build or sync. Layer 3 grants for `ontobricks_registry` must be applied after the registry schema is initialized (step 14 in the Full Deployment Checklist). Layer 3 grants for `ontobricks_graph` must be applied after the first Digital Twin build creates that schema.

---

## 1. Local Development Setup

### Prerequisites

- Python 3.10 or higher
- `uv` package manager (recommended) or `pip`
- Git
- Access to a Databricks workspace (Databricks Apps must be enabled)
- Databricks CLI installed (`>= 0.250.0`) and authenticated
- **Lakebase project** — must be created via the **old instances API**
  (`POST /api/2.0/database/instances`), not the Databricks UI "New project"
  button. The UI uses `POST /api/2.0/postgres/projects`, which produces an
  autoscaling-only project that is **incompatible** with the Synced Tables
  API (`POST /api/2.0/database/synced_tables`) used by the Digital Twin
  build. Use `scripts/setup-lakebase.sh` to create the project correctly
  (see §2 prerequisites below).
- `psql` (libpq client) on `PATH` for `scripts/bootstrap-lakebase-perms.sh`
  (`brew install libpq && brew link --force libpq` on macOS).

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd OntoBricks

# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install dependencies
# `--extra lakebase` is required since v0.4.0 — the registry runs on Lakebase Postgres.
uv venv
source .venv/bin/activate
uv sync --extra lakebase
```

### Environment Variables

Create a `.env` file from the example:

```bash
cp .env.example .env
```

Edit `.env` with your values:

```bash
# Required
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-personal-access-token
DATABRICKS_SQL_WAREHOUSE_ID=your-warehouse-id

# Registry / triplestore — UC namespace (volume must already exist)
REGISTRY_CATALOG=<your-catalog>
REGISTRY_SCHEMA=<your-schema>
REGISTRY_VOLUME=OntoBricksRegistry

# Lakebase (required since v0.4.0 — semantic local-dev coordinates)
LAKEBASE_PROJECT=ontobricks-app          # Autoscaling project name
LAKEBASE_BRANCH=develop                  # Branch to connect to
LAKEBASE_DATABASE=ontobricks_registry    # Postgres database (datname)
LAKEBASE_SCHEMA=ontobricks_registry      # Postgres schema for the registry
PGUSER=you@example.com                   # Your Databricks email locally; SP id in Apps
# PGHOST / PGPORT / PGDATABASE are auto-injected by the Apps platform — do not set here

# MLflow — persist agent traces to your workspace (recommended)
MLFLOW_TRACKING_URI=databricks
```

> `PGPASSWORD` is intentionally **not** in this list. `LakebaseAuth`
> mints a short-lived JWT via `POST /api/2.0/postgres/credentials`
> on every connection.

### Run Locally

```bash
scripts/start.sh
# Or directly: uv run python run.py
# Open http://localhost:8000
```

### Running Tests

```bash
uv run pytest                                      # all tests
uv run pytest --cov=back --cov=front --cov=shared --cov=api --cov=agents --cov-report=html   # with coverage
uv run pytest tests/e2e/ -v                        # end-to-end tests
```

---

## 2. Databricks Apps Deployment (DAB)

Deployment uses **Databricks Asset Bundles** to deploy both the main app and the MCP server declaratively from a single `databricks.yml` at the project root.

**Naming in this guide:** **`ontobricks-XXX`** stands for whatever main app **`name`** you set under `resources.apps.ontobricks_dev_app` in `databricks.yml` (replace it in every command and UI path). The MCP app name in this bundle is typically **`mcp-ontobricks`**.

### Prerequisites

| Requirement | Details |
|---|---|
| Databricks CLI | `>= 0.250.0` — check with `databricks -v` |
| Authenticated CLI | `databricks auth login --host https://<workspace>` |
| SQL Warehouse | A running SQL Warehouse in the workspace |
| Apps feature | Databricks Apps must be enabled on the workspace |
| Unity Catalog | A catalog, schema, and volume for the project registry |
| **Lakebase project** | Must be provisioned via `scripts/setup-lakebase.sh` — **do not** use the Databricks UI "New project" button (calls wrong API, incompatible with Synced Tables). The script uses `POST /api/2.0/database/instances` and prints the `db-…` resource id to put in `scripts/deploy.config.sh > DEFAULT_LAKEBASE_DATABASE_RESOURCE_SEGMENT`. See §2a below. |
| `psql` on PATH | Required by `scripts/bootstrap-lakebase-perms.sh` (`brew install libpq && brew link --force libpq` on macOS). |
| UC grants for the app SP | The app runs as a service principal. See [§3 Unity Catalog Permissions for the Service Principal](#3-unity-catalog-permissions-for-the-service-principal) for the exact grants required on the registry catalog/schema, the registry volume, and your source tables. |
| Lakebase grants for the app SP | `CAN_USE` on the Lakebase instance + `USAGE/DML` on the registry / graph / sync schemas. Bootstrap with `scripts/bootstrap-lakebase-perms.sh` (`make bootstrap-lakebase`) — `scripts/deploy.sh` runs it automatically on the `dev-lakebase` target. |

### Step 0 — Create the Lakebase project (first-time only)

> **Critical:** The Databricks UI "New project" button calls
> `POST /api/2.0/postgres/projects` which creates an autoscaling-only project.
> That API is **incompatible** with the Synced Tables API
> (`POST /api/2.0/database/synced_tables`) used by the Digital Twin build.
> You must use `scripts/setup-lakebase.sh` instead.

```bash
# Create the project (once per workspace):
./scripts/setup-lakebase.sh --name ontobricks-demo --capacity CU_2

# The script prints the db-… resource id at the end — copy it into
# deploy.config.sh > DEFAULT_LAKEBASE_DATABASE_RESOURCE_SEGMENT.
```

The script:
1. Creates the instance via `POST /api/2.0/database/instances` (synced-tables-compatible).
2. Waits for `AVAILABLE`.
3. Creates the Postgres database (`ontobricks_demo` by default).
4. Prints the `db-…` segment needed for `deploy.config.sh`.

**Options:**

| Flag | Default | Description |
|------|---------|-------------|
| `--name` | `ontobricks-demo` | Project name |
| `--capacity` | `CU_2` | Compute: `CU_1`, `CU_2`, `CU_4` |
| `--branch` | `production` | Initial branch |
| `--database` | `ontobricks_demo` | Postgres database name |
| `--profile` | `DEFAULT` | Databricks CLI profile |
| `--dry-run` | — | Print plan without executing |

---

### Step 1 — Authenticate

```bash
databricks auth login --host https://<workspace>.cloud.databricks.com

# Verify
databricks current-user me
```

### Step 2 — Customize `scripts/deploy.config.sh` (single source of truth)

**Important.** `app.yaml` is **generated** at deploy time from
`app.yaml.template` + `scripts/deploy.config.sh` by
`scripts/_render-app-yaml.py` (called from `scripts/deploy.sh`). The
generated file is `.gitignored`. **Do not edit `app.yaml` by hand** —
edit `scripts/deploy.config.sh` instead.

Edit the workspace-specific defaults in `scripts/deploy.config.sh`:

| Variable | Maps to | Description |
|----------|---------|-------------|
| `DEFAULT_APP_NAME` | `databricks.yml > var.app_name` and `DATABRICKS_APP_NAME` at runtime | Deployed name of the FastAPI app (e.g. `ontobricks-030`). |
| `DEFAULT_MCP_APP_NAME` | `databricks.yml > var.mcp_app_name` | Deployed name of the MCP companion (must start with `mcp-`). |
| `DEFAULT_DAB_TARGET` | `databricks bundle deploy -t <target>` | `dev-lakebase` (default) or `dev` (volume-only fallback). |
| `DEFAULT_WAREHOUSE_ID` | `app.yaml > DATABRICKS_SQL_WAREHOUSE_ID_DEFAULT` + the `sql-warehouse` bundle resource | **SQL Warehouses** → your warehouse → **Connection details**. |
| `DEFAULT_REGISTRY_CATALOG` / `_SCHEMA` / `_VOLUME` | Bundle `volume` resource (`uc_securable: <cat>.<schema>.<volume>`) | UC namespace that hosts the binary-artefact volume + the triplestore VIEW. |
| `DEFAULT_LAKEBASE_PROJECT` | `databricks.yml > var.lakebase_project` | Autoscaling **project id** (final segment of `projects/<id>`). |
| `DEFAULT_LAKEBASE_BRANCH` | `databricks.yml > var.lakebase_branch` | Branch id (e.g. `production`). |
| `DEFAULT_LAKEBASE_DATABASE_RESOURCE_SEGMENT` | `databricks.yml > var.lakebase_database_resource_segment` | **`db-…` resource id** from the Postgres API `name` field — **not** `datname` / `status.postgres_database`. Resolve with `databricks postgres list-databases "projects/<project>/branches/<branch>" -o json`. |
| `DEFAULT_LAKEBASE_REGISTRY_SCHEMA` | `databricks.yml > var.lakebase_registry_schema` + `LAKEBASE_SCHEMA` at runtime | Postgres schema for the registry (e.g. `ontobricks_registry`). |
| `DEFAULT_APP_TRIPLESTORE_TABLE` | `app.yaml > DATABRICKS_TRIPLESTORE_TABLE` | Fully-qualified `catalog.schema.table` fallback for MCP/session-less paths. |
| `DEFAULT_APP_MLFLOW_TRACKING_URI` | `app.yaml > MLFLOW_TRACKING_URI` | `databricks` (persists traces to the workspace) or empty for local-only. |

Each `DEFAULT_*` is consumed via `export FOO="${FOO:-$DEFAULT_FOO}"`,
so you can override any value for a single run without editing the
file:

```bash
WAREHOUSE_ID=abc123def456 make deploy
LAKEBASE_PROJECT=other-project LAKEBASE_BRANCH=staging make deploy
```

`scripts/_render-app-yaml.py` substitutes `${APP_*}` placeholders in
`app.yaml.template` into the generated `app.yaml`.

### Step 3 — `databricks.yml` (bundle variables)

`databricks.yml` declares the bundle resources and the `variables:`
contract, but the **values** come from `scripts/deploy.config.sh` —
`scripts/deploy.sh` passes each as `--var=key=value` so the YAML
defaults are only used by `databricks bundle validate` outside the
deploy script. The variables are:

| Variable | Purpose |
|----------|---------|
| `app_name` / `mcp_app_name` | Deployed app names (final URL segments). |
| `warehouse_id` | SQL Warehouse ID for the `sql-warehouse` resource. |
| `registry_catalog` / `registry_schema` / `registry_volume` | UC triple for the `uc_securable` volume resource. |
| `lakebase_project` / `lakebase_branch` | Lakebase Autoscaling project + branch (path segments). |
| `lakebase_database_resource_segment` | **`db-…` resource id** for the Apps `postgres.database` path (see Step 2). |
| `lakebase_registry_schema` | Postgres schema mirrored into `LAKEBASE_SCHEMA` in `app.yaml`. |

> **Do not** set `lakebase_database_resource_segment` to the schema
> name `ontobricks_registry` unless you intentionally bind a dedicated
> Postgres database whose **resource** id is that string (rare). The
> default `dev-lakebase` setup binds a dedicated Postgres database
> whose `datname` is `ontobricks_registry`, with registry tables in
> the schema `ontobricks_registry`. Older single-DB layouts used
> `databricks_postgres` as `datname` with the same schema name inside
> it — use `list-databases` to see which `db-…` row matches your bind.

Update the `permissions` section in `databricks.yml` to grant `CAN_MANAGE`
to the deploying user:

```yaml
permissions:
  - level: CAN_MANAGE
    user_name: <your-email>
  - level: CAN_USE
    group_name: users
```

> **MCP `ONTOBRICKS_URL`.** `src/mcp-server/app.yaml` still holds the
> main app URL the MCP companion calls back into (`ONTOBRICKS_URL`).
> This is the only `app.yaml` you edit by hand. Update it after the
> first deploy with `databricks apps get <main-app> -o json | python3
> -c "import sys,json; print(json.load(sys.stdin)['url'])"`.

> **Note** — the deployed app name no longer needs an explicit
> `ONTOBRICKS_APP_NAME` env var. The runtime auto-detects it from the
> Databricks-Apps-injected `DATABRICKS_APP_NAME`. Set
> `ONTOBRICKS_APP_NAME` only as an explicit override (e.g. in `.env`
> for local development).

The `REGISTRY_CATALOG`, `REGISTRY_SCHEMA`, and `REGISTRY_VOLUME`
static variables in `app.yaml` are **only used for local development /
MCP fallback**. In a deployed app the `volume` resource binding
injects `REGISTRY_VOLUME_PATH` automatically.

### Step 4 — Validate the bundle

```bash
# Volume-only target (no Lakebase postgres resource in the bundle)
databricks bundle validate -t dev

# Default sandbox with Lakebase Autoscaling postgres binding
databricks bundle validate -t dev-lakebase
```

This checks the bundle configuration without deploying. Fix any errors before proceeding.

### Step 5 — Deploy

This repository’s **`scripts/deploy.sh`** defaults to **`dev-lakebase`** (Volume + Lakebase postgres binding). Use **`-t dev`** for Volume-only (no postgres resource).

```bash
# Deploy both apps (main + MCP) with Lakebase binding — recommended
databricks bundle deploy -t dev-lakebase

# Or use the convenience script (validate + deploy + start main app)
scripts/deploy.sh

# Volume-only registry backend (no Lakebase bind)
scripts/deploy.sh -t dev
```

If the apps already exist in the workspace (e.g., from a previous manual deploy), bind bundle resources to them **once** (resource keys and app **names** must match `databricks.yml`):

```bash
databricks bundle deployment bind ontobricks_dev_app ontobricks-XXX -t dev-lakebase --auto-approve
databricks bundle deployment bind mcp_ontobricks_app mcp-ontobricks -t dev-lakebase --auto-approve
databricks bundle deploy -t dev-lakebase
```

Or run `scripts/deploy.sh --bind -t dev-lakebase` to bind the **main** app only (see script header for MCP bind if you extend the script).

### Step 5b — Lakebase schema grants (target `dev-lakebase` only)

Databricks does **not** auto-grant the app service principal anything
on Lakebase Postgres objects, even when the `postgres` resource
binding is wired correctly. `scripts/deploy.sh` calls
`scripts/bootstrap-lakebase-perms.sh` automatically on the
`dev-lakebase` target (you can re-run it manually any time — it is
idempotent).

The deploy script is **registry-scoped**: it bootstraps only the
**registry** schema. The graph DB is configured in-app
(`Settings → Graph DB`) and may live in a **different** Lakebase
project, so its grant is handled by the in-app "Create graph DB" flow
or a manual `bootstrap-lakebase-perms.sh` run.

| Schema | When to bootstrap | Who runs it |
|--------|-------------------|-------------|
| Registry (`ontobricks_registry`) | After **Settings → Registry → Initialize** has created the schema | `deploy.sh` automatically (`LAKEBASE_REGISTRY_SCHEMA` in `deploy.config.sh`) |
| Graph DB (`ontobricks_graph`) | After the **first Digital Twin Build** has created the schema | In-app "Create graph DB" flow, or manual run with the graph project/branch/database |

The script grants:

1. `CAN_USE` on the Lakebase database instance (control-plane).
2. `USAGE + CREATE` on the Postgres schema.
3. `SELECT / INSERT / UPDATE / DELETE` on every existing table.
4. `USAGE / SELECT / UPDATE` on every existing sequence (bigserial PKs).
5. The same set via `ALTER DEFAULT PRIVILEGES` so future tables inherit.

Manual invocation:

```bash
# Registry schema
scripts/bootstrap-lakebase-perms.sh \
  -i "<lakebase_project>" \
  -b "<lakebase_branch>" \
  -d ontobricks_registry \
  -s ontobricks_registry \
  -a ontobricks-XXX \
  -a mcp-ontobricks

# Graph schema (run after first Build — use the graph DB's own
# project/branch/database, which MAY differ from the registry)
scripts/bootstrap-lakebase-perms.sh \
  -i "<graph_project>" -b "<graph_branch>" \
  -d "<graph_database>" -s ontobricks_graph \
  -a ontobricks-XXX -a mcp-ontobricks
```

The registry grant uses `LAKEBASE_PROJECT` / `LAKEBASE_BRANCH` /
`LAKEBASE_REGISTRY_DATABASE` from `deploy.config.sh`; only the `-s`
schema name changes if you grant additional schemas in the same database.

If your Lakebase instance still uses the shared default database
**`databricks_postgres`** (older single-DB layouts) with the registry
schema **`ontobricks_registry`** inside it, pass **`-d databricks_postgres`**
and set `LAKEBASE_REGISTRY_DATABASE=databricks_postgres` in
`scripts/deploy.config.sh`.

### Step 6 — Start the apps

```bash
# Start the main UI app (resource key ontobricks_dev_app)
databricks bundle run ontobricks_dev_app -t dev-lakebase

# Start the MCP server (resource key mcp_ontobricks_app)
databricks bundle run mcp_ontobricks_app -t dev-lakebase
```

`scripts/deploy.sh` (without `--no-run`) starts **only** the main app (`ontobricks-XXX`). Start the MCP app with `databricks bundle run mcp_ontobricks_app -t dev-lakebase`, or use **`make deploy`** then run the MCP command separately.

### Step 7 — Bind resources (first deploy only)

After the first deployment, bind the app resources in the Databricks workspace UI:

1. Go to **Compute > Apps** and open your main app (placeholder **`ontobricks-XXX`** — same string as `resources.apps.ontobricks_dev_app.name` in `databricks.yml`).
2. Click **Resources**
3. Bind `sql-warehouse` to a running SQL Warehouse
4. Bind `volume` to the Unity Catalog Volume for the project registry (e.g., `your_catalog.your_schema.registry` — match `registry_*` variables in `databricks.yml`)
5. Repeat for **`mcp-ontobricks`** (same warehouse and volume)
6. For **`dev-lakebase`**, confirm the **postgres** resource is bound to the Lakebase database you configured (no extra UI step if deploy succeeded).
7. Verify both apps show status **Running**

> **Note:** Resource bindings persist across redeployments — you only need to do this once per workspace. Once the `sql-warehouse` and `volume` resources are bound, the corresponding controls in the Settings page are **locked**. To change them, update the resource bindings in the Apps UI and restart the app.

### Step 8 — Initialize the registry (first deploy only)

If the volume is empty (first deployment):

1. Open the app URL
2. Go to **Settings > Registry**
3. Click **Initialize** to bootstrap the registry

### Step 9 — Verify

```bash
# Check app status (names must match databricks.yml resources.apps.*.name)
databricks apps get ontobricks-XXX
databricks apps get mcp-ontobricks

# Or via bundle
databricks bundle summary -t dev-lakebase
```

### `scripts/deploy.sh` Reference

The convenience script wraps **`databricks bundle validate`**, **`databricks bundle deploy`**, optional **`bundle deployment bind`**, **`bundle run`** (main app only), **`bootstrap-app-permissions.sh`**, and **`bootstrap-lakebase-perms.sh`** (when target is `dev-lakebase`):

```bash
scripts/deploy.sh                    # default: -t dev-lakebase — validate + deploy + run ontobricks-XXX
scripts/deploy.sh -t dev             # Volume-only target (no Lakebase postgres bind)
scripts/deploy.sh -t dev-lakebase    # explicit Lakebase target (same as default)
scripts/deploy.sh --no-run           # deploy artifacts without starting the app
scripts/deploy.sh --bind             # bind main app resource key → existing app name, then deploy
```

### `app.yaml` Configuration — Full Reference

> **Generated file — do not edit `app.yaml` directly.** It is rendered
> at deploy time from `app.yaml.template` + `scripts/deploy.config.sh`
> by `scripts/_render-app-yaml.py`. Edit the config, then run
> `make deploy` (or `make render-app-yaml` to only re-render).

The rendered `app.yaml` controls the Databricks App runtime. Here is
every variable explained:

```yaml
# Command to start the app — uv resolves dependencies from pyproject.toml.
# `--extra lakebase` installs psycopg[binary] + psycopg-pool so the
# Lakebase backend works on every target (Volume-only deploys carry a
# ~10MB unused wheel but the Lakebase code paths stay gated by
# LakebaseAuth.is_available).
command:
  - "uv"
  - "run"
  - "--extra"
  - "lakebase"
  - "python"
  - "run.py"

env:
  # ── SQL Warehouse ──────────────────────────────────────────────
  # Injected from the sql-warehouse resource binding (databricks.yml).
  - name: DATABRICKS_SQL_WAREHOUSE_ID
    valueFrom: sql-warehouse

  # Static fallback warehouse ID for MCP / session-less API calls.
  - name: DATABRICKS_SQL_WAREHOUSE_ID_DEFAULT
    value: "${APP_SQL_WAREHOUSE_FALLBACK}"

  # ── Triple store fallback (Delta) ──────────────────────────────
  - name: DATABRICKS_TRIPLESTORE_TABLE
    value: "${APP_TRIPLESTORE_TABLE}"

  # ── Project Registry ───────────────────────────────────────────
  # Injected from the volume resource binding (uc_securable in
  # databricks.yml). The path looks like /Volumes/<cat>/<schema>/<vol>.
  - name: REGISTRY_VOLUME_PATH
    valueFrom: volume
  # Static fallbacks — used by MCP when no volume resource is bound.
  - name: REGISTRY_CATALOG
    value: "${APP_REGISTRY_CATALOG}"
  - name: REGISTRY_SCHEMA
    value: "${APP_REGISTRY_SCHEMA}"
  - name: REGISTRY_VOLUME
    value: "${APP_REGISTRY_VOLUME}"

  # ── Lakebase ───────────────────────────────────────────────────
  # On dev-lakebase the DAB binds a `database` resource. The Apps
  # runtime then auto-injects PGHOST / PGPORT / PGDATABASE / PGUSER /
  # PGAPPNAME / PGSSLMODE — no explicit valueFrom mapping needed.
  # The Postgres password is minted at runtime by LakebaseAuth via
  # POST /api/2.0/postgres/credentials.
  # LAKEBASE_* vars are informational in the deployed app (used to
  # display connection details in Settings → Registry) and as fallback
  # when PGHOST is absent (local dev without a bound database resource).
  - name: LAKEBASE_SCHEMA
    value: "${APP_LAKEBASE_SCHEMA}"
  - name: LAKEBASE_PROJECT
    value: "${APP_LAKEBASE_PROJECT}"
  - name: LAKEBASE_DATABASE
    value: "${APP_LAKEBASE_DATABASE}"
  - name: LAKEBASE_BRANCH
    value: "${APP_LAKEBASE_BRANCH}"

  # ── MLflow ─────────────────────────────────────────────────────
  - name: MLFLOW_TRACKING_URI
    value: "${APP_MLFLOW_TRACKING_URI}"

# ── Resources ──────────────────────────────────────────────────
# Declared here for local validation, but the BOUND resources at
# runtime come from databricks.yml (which uses uc_securable for the
# volume so the bundle owns the UC ACL).
resources:
  - name: sql-warehouse
    description: "SQL Warehouse for executing queries and metadata operations"
    sql_warehouse:
      permission: CAN_USE
  - name: volume
    description: "Unity Catalog Volume for the OntoBricks domain registry"
    volume:
      permission: CAN_READ_WRITE
```

### Resource-Locked Settings

When OntoBricks detects that it is running as a Databricks App with resource bindings, the Settings page automatically locks the affected controls:

| Resource | What is locked | How to change |
|----------|---------------|---------------|
| `sql-warehouse` | SQL Warehouse dropdown + refresh button | Rebind the `sql-warehouse` resource in **Compute > Apps > Resources** |
| `volume` | Registry Change button | Rebind the `volume` resource in **Compute > Apps > Resources** |

The **Initialize** button remains available when the volume resource is bound but the registry has not been initialized yet (no `.registry` marker). This allows first-time setup without unlocking the UI.

In **local development mode** (no Databricks App resources), all Settings controls remain fully editable.

---

## 3. Unity Catalog Permissions for the Service Principal

Databricks Apps run as a **service principal** (SP) created automatically when the app is deployed. The app's workspace bindings (`sql-warehouse`, `volume`) only cover the compute and the registry volume — they do **not** grant Unity Catalog privileges on the catalogs, schemas, or tables OntoBricks reads from and writes to. These grants are your responsibility and must be done once per workspace.

You can look up the app's SP ID with:

```bash
databricks apps get ontobricks-XXX -o json \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('service_principal_client_id'))"
```

Throughout this guide, replace **`ontobricks-XXX`** with your workspace’s actual main app name from `databricks.yml`.

Throughout this section that identifier is written as `<app-sp>` (it is a UUID / client-id, not an email).

> **Privilege mental model.** In Unity Catalog, to reference any object you need `USE CATALOG` on its catalog and `USE SCHEMA` on its schema. To read table/view data you need `SELECT`. To create new tables or views in a schema you need schema-level `CREATE TABLE` / `CREATE VIEW`. To modify or replace an object that **already exists** (including `CREATE OR REPLACE` over an existing object) you must own it or hold `MANAGE` on it. Volumes are separate: files under `/Volumes/...` need `READ VOLUME` and `WRITE VOLUME`. `ALL PRIVILEGES` is a convenient shorthand during onboarding but you should narrow it down before production.

### 3.1 — What OntoBricks actually does in UC

The main app's service principal performs the following operations at runtime. Every one of them needs a UC grant.

| # | Operation | Target | UC privilege required |
|---|-----------|--------|-----------------------|
| 1 | `SHOW CATALOGS`, `SHOW SCHEMAS`, `SHOW TABLES`, `DESCRIBE`, `SHOW VOLUMES`, `information_schema.tables` lookups (Data Source picker) | Source catalogs + registry catalog | `USE CATALOG` + `USE SCHEMA` + `SELECT` on browsed tables |
| 2 | `SELECT` on source tables referenced by R2RML `sql_query` entries (VIEW creation + build) | Each source table/view | `SELECT` |
| 3 | `CREATE OR REPLACE VIEW <registry_catalog>.<registry_schema>.triplestore_<domain>_V<n>` (Digital Twin Sync) | Registry schema | Schema `CREATE VIEW`. If an object with the same name already exists from a previous build, additionally `MANAGE` on it or SP ownership. |
| 4 | `SELECT subject, predicate, object FROM <triplestore VIEW>` (SPARQL + Lakebase Graph DB population) | The triplestore VIEW | `SELECT` (inherited by the SP as owner once it created the VIEW in step 3). |
| 5 | `CREATE TABLE IF NOT EXISTS <table>(subject STRING, predicate STRING, object STRING) USING DELTA`, `DELETE FROM`, `INSERT INTO` on the optional `DATABRICKS_TRIPLESTORE_TABLE` fallback (reasoning materialisation, MCP session-less calls) | Fallback triple-store table | Schema `CREATE TABLE`. If the table pre-exists, `MODIFY` to `DELETE`/`INSERT` + `SELECT`. |
| 6 | File I/O under `/Volumes/<registry_catalog>/<registry_schema>/<registry_volume>/` (projects, domains, history log, registry artefacts) | Registry volume | `READ VOLUME` + `WRITE VOLUME`. |
| 7 | `POST /api/2.1/unity-catalog/volumes` — only triggered from **Settings → Registry → Initialize** when the volume does not yet exist | Registry schema | Schema `CREATE VOLUME` (skip if you create the volume manually up front). |
| 8 | `CREATE SCHEMA IF NOT EXISTS`, `CREATE TABLE`, `INSERT … COPY FROM STDIN`, `SELECT`, `DELETE` on the App-bound Lakebase Postgres database (Graph DB engine + optionally registry hybrid backend) | Lakebase database | Lakebase user role with privileges on the configured schema (default `ontobricks_graph`). Authentication uses the App-injected OAuth token. |

All the above run through the **SQL Warehouse** bound to the app (`sql-warehouse` resource) on behalf of the app SP. The `CAN_USE` grant on the warehouse covers compute access; data access is controlled by UC.

### 3.2 — Registry catalog/schema grants (minimum viable set)

Run these SQL statements **once** as a workspace admin (or anyone with `MANAGE` on the catalog/schema), substituting your bundle values from `databricks.yml`:

```sql
-- The catalog the registry lives in
GRANT USE CATALOG   ON CATALOG `<registry_catalog>`               TO `<app-sp>`;

-- The schema that holds the triplestore VIEW, snapshot tables, and the registry Volume
GRANT USE SCHEMA    ON SCHEMA  `<registry_catalog>`.`<registry_schema>` TO `<app-sp>`;
GRANT CREATE TABLE  ON SCHEMA  `<registry_catalog>`.`<registry_schema>` TO `<app-sp>`;
GRANT CREATE VIEW   ON SCHEMA  `<registry_catalog>`.`<registry_schema>` TO `<app-sp>`;

-- Registry Volume (files: projects, domains, history, registry artefacts).
-- The `volume` resource binding only grants compute reach-through; UC ACLs still apply.
GRANT READ VOLUME   ON VOLUME  `<registry_catalog>`.`<registry_schema>`.`<registry_volume>` TO `<app-sp>`;
GRANT WRITE VOLUME  ON VOLUME  `<registry_catalog>`.`<registry_schema>`.`<registry_volume>` TO `<app-sp>`;

-- Optional — only if you want the Settings UI to be able to create the
-- registry volume itself on first run (otherwise create it once as admin).
GRANT CREATE VOLUME ON SCHEMA  `<registry_catalog>`.`<registry_schema>` TO `<app-sp>`;
```

Alternatively — shorter, broader, and fine for a dev workspace:

```sql
GRANT USE CATALOG    ON CATALOG `<registry_catalog>`                   TO `<app-sp>`;
GRANT ALL PRIVILEGES ON SCHEMA  `<registry_catalog>`.`<registry_schema>` TO `<app-sp>`;
```

Prefer the explicit list above for a production workspace because `ALL PRIVILEGES` also implies `MANAGE`, which is broader than what the app needs day to day.

### 3.3 — Source data grants (customer tables/views)

For every table or view referenced in an R2RML mapping (i.e. anything that appears in the **Data Sources** tab of a domain), the app SP needs to read the data:

```sql
GRANT USE CATALOG ON CATALOG `<source_catalog>`                               TO `<app-sp>`;
GRANT USE SCHEMA  ON SCHEMA  `<source_catalog>`.`<source_schema>`             TO `<app-sp>`;

-- Per-object is tightest:
GRANT SELECT      ON TABLE   `<source_catalog>`.`<source_schema>`.`<table>`   TO `<app-sp>`;

-- Or grant schema-wide if you trust the SP with the whole schema:
-- GRANT SELECT   ON SCHEMA  `<source_catalog>`.`<source_schema>`             TO `<app-sp>`;
```

If any mapping `sql_query` joins tables from multiple schemas or catalogs, repeat the `USE CATALOG` / `USE SCHEMA` / `SELECT` chain for each. The VIEW creation in step 3 will fail with `TABLE_OR_VIEW_NOT_FOUND` or `PERMISSION_DENIED` if one is missing.

### 3.4 — What you do **not** need to grant

- `MODIFY` on source tables — the app only reads them.
- `MANAGE` on the registry schema — `CREATE TABLE` + `CREATE VIEW` are sufficient because the SP automatically owns the objects it creates.
- `MANAGE` on the registry catalog — not required unless you want the SP to `DROP CATALOG` / `ALTER CATALOG`, which OntoBricks never does.

### 3.5 — Known pitfall: stale object blocking `CREATE OR REPLACE VIEW`

If a build previously materialised the triplestore as a **TABLE** (legacy code path or a manual CTAS in SQL Editor), the next Sync will try to run `CREATE OR REPLACE VIEW` over the same name. Databricks refuses this because:

1. `CREATE OR REPLACE VIEW` cannot convert an existing TABLE to a VIEW, even for the owner.
2. Even if it could, the SP would need `MANAGE` on that pre-existing object.

The user-facing error surfaces as:

```
Sync failed: Failed to create VIEW: PERMISSION_DENIED: User does not have MANAGE on Table '<cat>.<schema>.triplestore_<domain>_v<n>'.
```

**Fix (once):** drop the stale object yourself, then re-run Sync:

```sql
DROP TABLE IF EXISTS `<registry_catalog>`.`<registry_schema>`.`triplestore_<domain>_v<n>`;
```

Once dropped, the app SP recreates the object as a VIEW and owns it from then on.

### 3.6 — Granting when the SP ID is unknown yet

If `databricks apps get` reports the SP client-id as `None` (app not fully provisioned), bind it by email-alias instead. Apps expose a dedicated SP user with the form `<app-name>@<account-id>.iam.databricks.com`, visible under **Admin → Service principals** after the first deploy. You can grant against that principal with the same SQL, substituting its identifier in the `TO` clause.

### 3.7 — Quick verification

Once grants are in place, you can verify from any SQL editor or notebook authenticated as the app SP (or by running a build in the app and watching the logs):

```sql
-- Should succeed (list registry schema objects)
SHOW TABLES  IN `<registry_catalog>`.`<registry_schema>`;
SHOW VOLUMES IN `<registry_catalog>`.`<registry_schema>`;

-- Should return the current SP's privileges on the schema
SHOW GRANTS `<app-sp>` ON SCHEMA `<registry_catalog>`.`<registry_schema>`;
```

If a user reports a sync failure, the app now surfaces the underlying Databricks error in the task status (`PERMISSION_DENIED`, `TABLE_OR_VIEW_NOT_FOUND`, unresolved column, etc.). The full traceback is always in the app logs.

---

## 4. Permission Management

OntoBricks includes a built-in permission system that controls who can access the app and what they can do. Permissions are managed in **Settings > Permissions** and are only active when running as a Databricks App (local development has no restrictions).

### How It Works

| Role | Access |
|------|--------|
| **Admin** | Full access + can manage the permission list. Determined by **CAN_MANAGE** on the Databricks App. |
| **Editor** | Full access to all features (read + write). |
| **Viewer** | Read-only access (cannot create, edit, or delete). |
| **No role** | Blocked entirely (redirect to Access Denied page). |

When no permissions are configured yet, only users with **CAN_MANAGE** on the Databricks App have access. Everyone else is blocked until an admin adds them via the Permissions tab.

### How Admin Detection Works

At runtime, the app checks whether the logged-in user has `CAN_MANAGE` on the Databricks App by calling the Permissions API. The check uses the **user's own OAuth token** (forwarded by the Databricks Apps proxy via `x-forwarded-access-token`). This means:

- Admin detection uses the user's OAuth token when the Apps runtime forwards one.
- If the user's forwarded token is not available or lacks scope for `/api/2.0/permissions/apps/*`, the app falls back to the SDK (service principal) and then a REST call with the SP token.
- Those fallbacks require the app's service principal to have at least `CAN_VIEW_PERMISSIONS` on its **own app**, which is **not granted automatically** when the app is created.

### First-Deploy Bootstrap (required once per workspace)

Databricks Apps do not grant the freshly-created service principal any permission on the app it runs. Until that is fixed, `list_app_principals` returns `403`, the middleware cannot resolve any user (including `CAN_MANAGE` deployers), and the access-denied page appears on the very first request.

`make deploy` (via `scripts/deploy.sh`) runs the fix automatically. For manual deploys:

```bash
make bootstrap-perms
# equivalent to: scripts/bootstrap-app-permissions.sh ontobricks-XXX mcp-ontobricks
```

The script is idempotent. It discovers each app's service principal via `databricks apps get` and grants it `CAN_MANAGE` on itself. If the first post-deploy page load shows a **"First-deploy bootstrap required"** banner, that is exactly the situation — run the command and reload.

### Managing Permissions

1. Ensure you have **CAN_MANAGE** on the sandbox app **`ontobricks-XXX`** in the Databricks UI (**Compute > Apps > ontobricks-XXX > Permissions**), or on your production app name if different
2. Open the app and go to **Settings > Permissions**
3. Click **Add** to grant access to workspace users or groups
4. Assign each principal a **Viewer** or **Editor** role
5. Users not in the list are blocked from accessing the app

### Diagnostics

If a user reports "Access Denied", hit the diagnostic endpoint (accessible even when blocked):

```
https://<app-url>/settings/permissions/diag
```

This returns:
- The user's email and forwarded token status
- SDK (SP token) check result
- User-token check result
- Which principals have `CAN_MANAGE`
- The cached admin decision and its age

---

## 5. Deploying to a New Workspace

When moving OntoBricks to a different Databricks workspace, follow these steps.

### 5.1 — Authenticate to the new workspace

```bash
# Option A: Set as default
databricks auth login --host https://<new-workspace>.cloud.databricks.com

# Option B: Use a named profile
databricks auth login --host https://<new-workspace>.cloud.databricks.com --profile new-ws
```

Verify:

```bash
databricks current-user me
# Or with a profile: databricks current-user me --profile new-ws
```

### 5.1b — Create the Lakebase project (new workspace, first time)

```bash
# Create the project via the correct API (synced-tables-compatible):
./scripts/setup-lakebase.sh --name ontobricks-demo --capacity CU_2

# Copy the printed db-… segment into deploy.config.sh (DEFAULT_LAKEBASE_DATABASE_RESOURCE_SEGMENT)
```

### 5.2 — Prepare Unity Catalog resources

The new workspace needs a catalog/schema where OntoBricks can store projects and triple stores:

```bash
# Create a schema for OntoBricks (adjust catalog name)
databricks sql query "CREATE SCHEMA IF NOT EXISTS main.ontobricks"

# Create the registry volume
databricks sql query "CREATE VOLUME IF NOT EXISTS main.ontobricks.OntoBricksRegistry"
```

### 5.3 — Update configuration files

**`scripts/deploy.config.sh`** — this is the single source of truth.
Update the `DEFAULT_*` literals (or override via env) for the new
workspace:

```bash
DEFAULT_APP_NAME="ontobricks-XXX"             # your main app name
DEFAULT_MCP_APP_NAME="mcp-ontobricks"
DEFAULT_DAB_TARGET="dev-lakebase"

DEFAULT_WAREHOUSE_ID="<new-warehouse-id>"
DEFAULT_REGISTRY_CATALOG="<new-catalog>"
DEFAULT_REGISTRY_SCHEMA="<new-schema>"
DEFAULT_REGISTRY_VOLUME="registry"

DEFAULT_LAKEBASE_PROJECT="<lakebase-project-id>"
DEFAULT_LAKEBASE_BRANCH="production"
DEFAULT_LAKEBASE_DATABASE_RESOURCE_SEGMENT="<db-… from list-databases>"
DEFAULT_LAKEBASE_REGISTRY_SCHEMA="ontobricks_registry"

DEFAULT_APP_TRIPLESTORE_TABLE="<catalog>.<schema>.<triplestore_table>"
```

`scripts/deploy.sh` will pass every `DEFAULT_*` value to
`databricks bundle deploy` as a `--var=` override and render
`app.yaml` from the template.

**`databricks.yml`** — only the structural bits change here.
Update the `permissions:` section with the deploying user's email
(the `variables:` defaults are overridden by `deploy.config.sh`).

**`src/mcp-server/app.yaml`** — update the main app URL after the
first deploy:

```yaml
- name: ONTOBRICKS_URL
  value: "https://<new-ontobricks-app-url>"
```

### 5.4 — Deploy

```bash
# Validate first (use the same target you will deploy)
databricks bundle validate -t dev-lakebase

# Deploy and start main app (Lakebase target); start MCP separately if needed
scripts/deploy.sh -t dev-lakebase
databricks bundle run mcp_ontobricks_app -t dev-lakebase
```

### 5.5 — Bind resources

1. Go to **Compute > Apps > ontobricks-XXX > Resources** (or your main app name)
2. Bind `sql-warehouse` to a running SQL Warehouse
3. Bind `volume` to the registry UC Volume
4. Repeat for `mcp-ontobricks`

### 5.6 — Initialize and verify

1. Open the app URL
2. Go to **Settings > Registry > Initialize** (if the volume is empty)
3. Verify both apps are **Running**: `databricks apps get ontobricks-XXX` and `databricks apps get mcp-ontobricks`

### 5.7 — Update MCP server URL

After the main app is deployed and running:

```bash
# Get the main app URL
databricks apps get ontobricks-XXX -o json | python3 -c "import sys,json; print(json.load(sys.stdin)['url'])"
```

Update `ONTOBRICKS_URL` in `src/mcp-server/app.yaml` with this URL, then redeploy the bundle and restart the MCP app:

```bash
databricks bundle deploy -t dev-lakebase
databricks bundle run mcp_ontobricks_app -t dev-lakebase
```

### New Workspace Checklist

```
[ ] 1.  databricks auth login --host https://<new-workspace>
[ ] 2.  Verify: databricks current-user me
[ ] 3.  Create Unity Catalog resources (catalog, schema, volume)
[ ] 4.  Create Lakebase project via `scripts/setup-lakebase.sh` (run BEFORE deploy — copy the `db-…` id into `deploy.config.sh`)
        Resolve the db-… resource id with:
          databricks postgres list-databases \
            "projects/<project>/branches/<branch>" -o json
[ ] 5.  Edit scripts/deploy.config.sh:
        - DEFAULT_APP_NAME / DEFAULT_MCP_APP_NAME
        - DEFAULT_WAREHOUSE_ID
        - DEFAULT_REGISTRY_CATALOG / _SCHEMA / _VOLUME
        - DEFAULT_LAKEBASE_PROJECT / _BRANCH / _DATABASE_RESOURCE_SEGMENT / _REGISTRY_SCHEMA
        - DEFAULT_APP_TRIPLESTORE_TABLE
[ ] 6.  Update databricks.yml permissions (your email with CAN_MANAGE)
[ ] 7.  make bundle-validate
[ ] 8.  make deploy                # scripts/deploy.sh -t dev-lakebase
[ ] 9.  Bind sql-warehouse, volume, postgres resources in the Apps UI
        if the bundle bind didn't take (both apps: APP_NAME, MCP_APP_NAME)
[ ] 10. Grant UC privileges to each app's service principal (see §3):
        registry USE CATALOG / USE SCHEMA / CREATE TABLE / CREATE VIEW +
        volume READ/WRITE + source-table SELECT
[ ] 11. Open app → Settings → Registry → Initialize
[ ] 12. Re-run make deploy (or scripts/bootstrap-lakebase-perms.sh) so the
        registry / graph / sync schema GRANTs apply against the just-
        created schemas
[ ] 13. Verify both apps are RUNNING
[ ] 14. Update ONTOBRICKS_URL in src/mcp-server/app.yaml with the main app URL
[ ] 15. databricks bundle deploy -t dev-lakebase && databricks bundle run mcp_ontobricks_app -t dev-lakebase
[ ] 16. Verify MCP appears in Databricks Playground
```

---

## 6. Triple Store & Graph DB Backend Configuration

OntoBricks always materializes both a Delta view (Unity Catalog) and a Graph DB engine (Lakebase Postgres). Both layers are pluggable through their respective factories — see `docs/graphdb-integration.md` for adding a new Graph DB engine.

### Delta view (`view`) — No Extra Setup Required

The Delta view is created by R2RML on a Databricks SQL Warehouse and persists triples for governance and lineage. On Databricks Apps, the app's service principal authenticates via OAuth automatically — the only requirement is the SQL Warehouse resource declared in `app.yaml` (already configured).

### Lakebase Postgres (`graph`, engine `lakebase`) — Bound by `databricks.yml`

The Graph DB layer runs on the App-bound Lakebase Postgres instance. The Apps runtime injects `PGHOST` / `PGPORT` / `PGDATABASE` / `PGUSER` and OntoBricks mints a short-lived OAuth token via `WorkspaceClient().config.authenticate()`. Two write modes are available:

- `app_managed` (default): the FastAPI app streams R2RML rows in `fetchmany` batches and ingests via `COPY FROM STDIN` + `INSERT … ON CONFLICT DO NOTHING`.
- `managed_synced`: Databricks Lakeflow keeps a Postgres synced table in lock-step with the Delta view; OntoBricks orchestrates `SyncedTableManager.ensure` + `trigger_and_wait`. A writable companion table absorbs reasoning / cohort writes; readers see both via a UNION view.

The `scripts/bootstrap-lakebase-perms.sh` script grants the app SP the required Lakebase / Postgres privileges (`CREATE` on the schema, `INSERT/SELECT/DELETE` on the per-domain tables). Run it once after the bundle is deployed.

---

## 7. MCP Server Deployment (Databricks Playground)

The MCP server (`mcp-ontobricks`) is a **separate** Databricks App that exposes OntoBricks knowledge-graph tools to the Databricks Playground. It must have a name starting with `mcp-` to be discoverable.

### Prerequisites

| Requirement | Details |
|---|---|
| Main app deployed | Your main app (**`ontobricks-XXX`** in examples — the `name` under `ontobricks_dev_app` in `databricks.yml`) must be deployed and running first |
| Databricks CLI | Authenticated to the same workspace |
| Playground access | Databricks Playground must be enabled |

### Deploy with DAB

The MCP server is deployed alongside the main app by the same `databricks.yml` bundle:

```bash
# Deploy both app definitions (single bundle deploy)
databricks bundle deploy -t dev-lakebase

# Or the convenience script (starts main app only — start MCP separately)
scripts/deploy.sh -t dev-lakebase

# After the main app is up, start the MCP app if needed
databricks bundle run mcp_ontobricks_app -t dev-lakebase
```

### MCP `app.yaml` Configuration

```yaml
command:
  - "uv"
  - "run"
  - "mcp-ontobricks"

env:
  - name: ONTOBRICKS_URL
    value: "https://<your-ontobricks-app-url>"

  - name: DATABRICKS_SQL_WAREHOUSE_ID
    valueFrom: sql-warehouse

  - name: REGISTRY_VOLUME_PATH
    valueFrom: volume

resources:
  - name: sql-warehouse
    sql_warehouse:
      permission: CAN_USE
  - name: volume
    volume:
      permission: CAN_READ_WRITE
```

> **Important**: Update `ONTOBRICKS_URL` to match your main app's URL before deploying. Find it with:
> ```bash
> databricks apps get ontobricks-XXX -o json | python3 -c "import sys,json; print(json.load(sys.stdin)['url'])"
> ```

### Post-Deployment Resource Binding

After the first deployment, bind the MCP server's resources:

1. Go to **Compute > Apps > mcp-ontobricks > Resources**
2. Bind `sql-warehouse` to the same SQL Warehouse used by the main app
3. Bind `volume` to the same registry UC Volume

### Grant the MCP App Access to the Main App

The MCP server calls the main app's REST API using its service principal's OAuth token. The `users` group should already have `CAN_USE` on the main app (set in `databricks.yml`). If it doesn't:

```bash
databricks apps update-permissions ontobricks-XXX --json '{
  "access_control_list": [
    { "group_name": "users", "permission_level": "CAN_USE" }
  ]
}'
```

Use your real main app name everywhere **`ontobricks-XXX`** appears (from `databricks.yml`).

### Using in Databricks Playground

1. Go to your Databricks workspace
2. Navigate to **Playground**
3. `mcp-ontobricks` appears in the **MCP Servers** list (apps starting with `mcp-` are shown automatically)
4. Select it to use OntoBricks knowledge-graph tools in conversations

### Available MCP Tools

| Tool | Description |
|---|---|
| `list_projects` | List all projects (knowledge graphs) in the registry with names and descriptions |
| `select_project` | Activate a project by name — subsequent queries operate on its triple store |
| `list_entity_types` | Human-readable overview of the selected project's knowledge graph (entity types, counts, predicates) |
| `describe_entity` | Search by name/type and get a full-text description with attributes, relationships, and BFS traversal |
| `get_status` | Compact diagnostic: project, backend, table, data availability, triple count |

### Standalone / Local MCP Usage

For LLM clients like Cursor or Claude Desktop (stdio transport):

```bash
cd mcp-server
uv run python -c \
  "from server.app import create_mcp_server; create_mcp_server('standalone').run(transport='stdio')"
```

Or via the convenience wrapper at the project root:

```bash
python src/mcp-server/mcp_server.py              # stdio (default)
python src/mcp-server/mcp_server.py --http       # streamable-http on port 9100
```

Override the target URL:

```bash
ONTOBRICKS_URL=http://your-host:8000 python src/mcp-server/mcp_server.py
```

### Client Configuration Examples

**Cursor** (`.cursor/mcp.json`):

```json
{
  "mcpServers": {
    "ontobricks": {
      "command": "uv",
      "args": ["run", "python", "-c",
        "from server.app import create_mcp_server; create_mcp_server('standalone').run(transport='stdio')"
      ],
      "cwd": "/path/to/OntoBricks/mcp-server"
    }
  }
}
```

**Claude Desktop** (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "ontobricks": {
      "command": "uv",
      "args": ["run", "python", "-c",
        "from server.app import create_mcp_server; create_mcp_server('standalone').run(transport='stdio')"
      ],
      "cwd": "/path/to/OntoBricks/mcp-server",
      "env": { "ONTOBRICKS_URL": "http://localhost:8000" }
    }
  }
}
```

---

## 8. MLflow Agent Observability

OntoBricks agents are instrumented with MLflow tracing. When deployed to Databricks, traces are persisted to the workspace tracking server.

### How It Works

- `MLFLOW_TRACKING_URI=databricks` is set in `app.yaml`
- Application startup in `src/shared/fastapi/main.py` calls `setup_tracing()`, which creates the `/Shared/ontobricks-agents` experiment
- Every agent call (OWL Generator, Auto-Mapping, Auto Icon Assign, Ontology Assistant) produces a span tree:

```
AGENT (run_agent)
├── LLM (_call_llm)        — endpoint, tokens, latency
├── TOOL (tool:get_metadata) — arguments, result
├── LLM (_call_llm)        — next iteration
├── TOOL (tool:execute_sql)  — SQL query, result
└── ...
```

### Viewing Traces

1. In your Databricks workspace, go to **Machine Learning > Experiments**
2. Open **`/Shared/ontobricks-agents`**
3. Click any run, then the **Traces** tab

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `MLFLOW_TRACKING_URI` | *(none)* | Set to `databricks` for persistent traces |
| `ONTOBRICKS_MLFLOW_EXPERIMENT` | `ontobricks-agents` | Experiment name (auto-prefixed with `/Shared/` on Databricks) |

Tracing degrades gracefully: if MLflow is not configured, agents run normally without traces.

---

## 9. DAB Reference

### Bundle Structure

```
OntoBricks/
├── databricks.yml          # Bundle definition (apps, permissions, targets)
├── .databricksignore       # Excludes non-runtime files from sync
├── app.yaml                # Main app runtime config
├── src/mcp-server/
│   └── app.yaml            # MCP server runtime config
└── docs/dab-reference.md
    └── README.md           # DAB-specific documentation
```

### Targets

| Target | Mode | Description |
|--------|------|-------------|
| `dev` | development | Volume-only registry backend — **no** Apps `postgres` resource. |
| `dev-lakebase` | development | Same apps plus **Lakebase Autoscaling** `postgres` binding (default for `make deploy` / `scripts/deploy.sh`). |

```bash
databricks bundle deploy -t dev-lakebase
databricks bundle run ontobricks_dev_app -t dev-lakebase
databricks bundle run mcp_ontobricks_app -t dev-lakebase
```

### Variables

Override defaults with `--var` flags or in a target-specific `variables:` block. **Authoritative defaults** live in `databricks.yml`; the table below summarizes intent:

| Variable | Description |
|----------|-------------|
| `warehouse_id` | SQL Warehouse ID for the `sql-warehouse` resource. |
| `registry_catalog` / `registry_schema` / `registry_volume` | UC triple for the `volume` resource (`catalog.schema.volume` name). |
| `lakebase_project` / `lakebase_branch` / `lakebase_database_resource_segment` | Lakebase Autoscaling binding (final segment must be `db-…` from `list-databases`). |
| `lakebase_registry_schema` | Postgres schema for the registry (mirror `LAKEBASE_SCHEMA` in `app.yaml`). |

```bash
databricks bundle deploy -t dev-lakebase --var warehouse_id=abc123def456
```

### Makefile targets (excerpt)

```bash
make deploy              # scripts/deploy.sh -t dev-lakebase
make deploy-volume       # Volume-only: scripts/deploy.sh -t dev
make deploy-no-run       # Deploy without bundle run
make bundle-validate     # databricks bundle validate -t dev-lakebase
make bundle-summary      # databricks bundle summary -t dev-lakebase
make bootstrap-perms     # app SP CAN_MANAGE on self (see scripts/bootstrap-app-permissions.sh)
make bootstrap-lakebase  # Lakebase schema grants (see scripts/bootstrap-lakebase-perms.sh)
```

### File Sync

The `.databricksignore` at the project root excludes non-runtime files (tests, data, IDE config, Sphinx HTML build output, etc.) from the main app sync, but **includes** `docs/` so the in-app Help Center can serve `/api/help/docs/*` in production. The MCP server has its own `source_code_path` pointing directly to `src/mcp-server/`.

### Binding existing apps

If the apps already exist in the workspace from a previous deployment:

```bash
databricks bundle deployment bind ontobricks_dev_app ontobricks-XXX -t dev-lakebase --auto-approve
databricks bundle deployment bind mcp_ontobricks_app mcp-ontobricks -t dev-lakebase --auto-approve
databricks bundle deploy -t dev-lakebase
```

---

## 10. Full Deployment Checklist

Use this checklist when deploying OntoBricks from scratch on any workspace:

```
[ ] 1.  Databricks CLI installed (>= 0.250.0) and authenticated
          databricks auth login --host https://<workspace>
          databricks current-user me
[ ] 2.  SQL Warehouse created and running
[ ] 3.  Unity Catalog resources available:
        [ ] A catalog you can use (e.g., main or your personal catalog)
        [ ] A schema within that catalog (e.g., ontobricks)
        [ ] A Volume for the project registry (e.g., OntoBricksRegistry)
[ ] 4.  Lakebase project created via `scripts/setup-lakebase.sh` (`db-…` id copied into `deploy.config.sh`)
        (required since v0.4.0 — Provisioned tier is not supported).
        Resolve the db-… resource id:
          databricks postgres list-databases \
            "projects/<project>/branches/<branch>" -o json
[ ] 5.  psql available (brew install libpq && brew link --force libpq)
[ ] 6.  Edit scripts/deploy.config.sh:
        [ ] DEFAULT_APP_NAME / DEFAULT_MCP_APP_NAME
        [ ] DEFAULT_WAREHOUSE_ID
        [ ] DEFAULT_REGISTRY_CATALOG / _SCHEMA / _VOLUME
        [ ] DEFAULT_LAKEBASE_PROJECT / _BRANCH / _DATABASE_RESOURCE_SEGMENT
        [ ] DEFAULT_LAKEBASE_REGISTRY_SCHEMA (mirrored as LAKEBASE_SCHEMA in app.yaml)
        [ ] DEFAULT_APP_TRIPLESTORE_TABLE
[ ] 7.  Update databricks.yml permissions (your email with CAN_MANAGE)
[ ] 8.  Validate:  make bundle-validate
[ ] 9.  Deploy:    make deploy                  # runs scripts/deploy.sh -t dev-lakebase
[ ] 10. Verify bundle bound sql-warehouse / volume / postgres on both apps
        (UI: Compute > Apps > <app> > Resources). Re-bind manually if not.
[ ] 11. Grant Unity Catalog privileges to each app service principal (see §3):
        [ ] Registry catalog: USE CATALOG
        [ ] Registry schema : USE SCHEMA + CREATE TABLE + CREATE VIEW
        [ ] Registry volume : READ VOLUME + WRITE VOLUME
        [ ] Each source catalog/schema referenced in R2RML mappings:
            USE CATALOG + USE SCHEMA + SELECT (per table or schema-wide)
[ ] 12. App self-permissions:
        make bootstrap-perms
        (already invoked by `make deploy`, but safe to re-run anytime.)
[ ] 13. Open app URL → Settings → Registry → Initialize (creates the Postgres schema)
[ ] 14. Re-run Lakebase GRANT bootstrap so the now-existing schemas pick up USAGE/DML:
        make bootstrap-lakebase
        (also runs as part of `make deploy`; idempotent.)
[ ] 15. Verify main app is RUNNING:
          databricks apps get <main-app>
[ ] 16. (If using MCP) Update ONTOBRICKS_URL in src/mcp-server/app.yaml:
          databricks apps get <main-app> -o json | python3 -c "import sys,json; print(json.load(sys.stdin)['url'])"
[ ] 17. (If using MCP) Redeploy and start MCP:
          databricks bundle deploy -t dev-lakebase && databricks bundle run mcp_ontobricks_app -t dev-lakebase
[ ] 18. (If using MCP) Verify in Databricks Playground
```

---

## 11. Troubleshooting

### App Won't Start

```bash
# Check app status (use your bundle’s app names)
databricks apps get ontobricks-XXX
databricks apps get mcp-ontobricks

# Check recent deployment status
databricks apps list-deployments ontobricks-XXX
```

### "Access Denied" Despite Having CAN_MANAGE

1. Hit the diagnostic endpoint: `https://<app-url>/settings/permissions/diag`
2. Check `user_token_present` — should be `true` in Databricks App mode
3. Check `user_token_can_manage` — should list your email
4. If `email_is_manager` is `false`, verify your email matches the CAN_MANAGE grant exactly
5. Check `admin_cache` — if stale, restart the app to clear it

### "Databricks credentials not configured"

The agents need OAuth credentials to call the Foundation Model API. In a Databricks App, these are resolved automatically via the service principal. If you see this error:

1. Verify the app is running as a Databricks App (not locally)
2. Check that the LLM endpoint is configured in the project settings
3. Review app logs for OAuth token resolution errors

### Connection Errors

- Verify the SQL Warehouse is running and the resource binding is correct
- Check the SP has correct permissions on catalogs/schemas — see [§3 Unity Catalog Permissions for the Service Principal](#3-unity-catalog-permissions-for-the-service-principal)
- Review app logs in the Databricks Apps UI

### Digital Twin Sync: `PERMISSION_DENIED` / `Failed to create VIEW`

If **Sync** fails with a message like

```
Sync failed: Failed to create VIEW: PERMISSION_DENIED:
User does not have MANAGE on Table '<cat>.<schema>.triplestore_<domain>_v<n>'.
```

then an object with the same name already exists in UC as a **TABLE** — typically left behind by a legacy materialised build — and `CREATE OR REPLACE VIEW` cannot overwrite it. Drop the stale object once as an admin and retry:

```sql
DROP TABLE IF EXISTS `<cat>`.`<schema>`.`triplestore_<domain>_v<n>`;
```

If the failure is a plain `PERMISSION_DENIED` without a pre-existing object, the app SP is missing a UC grant — see [§3.2](#32--registry-catalogschema-grants-minimum-viable-set) for the registry schema and [§3.3](#33--source-data-grants-customer-tablesviews) for source tables.

### Digital Twin Sync: `TABLE_OR_VIEW_NOT_FOUND`

The app SP lacks `USE CATALOG` / `USE SCHEMA` / `SELECT` on one of the source tables referenced in an R2RML mapping. Identify which source from the error message and grant per [§3.3](#33--source-data-grants-customer-tablesviews).

### `localhost` Redirects When Deployed

All internal navigation links must include a trailing slash (e.g., `/dtwin/` not `/dtwin`). FastAPI's `redirect_slashes` generates `localhost:8000` redirects behind the Databricks proxy.

### Module Import Errors After Redeployment

Stale `__pycache__` or old directories in the workspace can cause import conflicts:

```bash
# Remove stale workspace files if needed
databricks workspace delete /Users/<you>/ontobricks/<stale-dir> --recursive
```

### MCP Server Returns 401 Unauthorized

The MCP app's SP needs `CAN_USE` permission on the main app. The `users` group should already have this via `databricks.yml`.

### Agent Traces Not Appearing

1. Verify `MLFLOW_TRACKING_URI=databricks` is set in `app.yaml`
2. Check the app logs for `MLflow tracing enabled — experiment='/Shared/ontobricks-agents'`
3. Run an agent call (e.g., generate an ontology) to create the first trace
4. The experiment is created on first use, not at startup

---

## 12. Production Considerations

### Security

- Never commit `.env` files or secrets to Git
- Use Databricks Secrets for sensitive data (passwords, tokens)
- Use service principals for production (automatically handled by Databricks Apps)
### Performance

- Use an appropriately sized SQL Warehouse (enable auto-stop to save costs)

### Monitoring

- **App logs**: Available in the Databricks Apps console
- **Log level**: Configure via `LOG_LEVEL` environment variable (`DEBUG`, `INFO`, `WARNING`, `ERROR`)
- **Structured JSON logs**: Set `LOG_FORMAT=json` to emit one JSON object per log line — ideal for log aggregation and search
- **Request timing**: Every non-static request is logged with method, path, status code, and duration in milliseconds
- **Thread pool**: Tune concurrent blocking work via `ONTOBRICKS_THREAD_POOL_SIZE` (default `20`)
- **Health checks**: `GET /health` runs a comprehensive readiness probe (`/tmp` + session/log dirs, Databricks auth, SQL warehouse, registry UC volume read+write, registry catalog/schema view DDL, Lakebase USAGE) and returns `{status, summary:{ok,warnings,errors}, checks:[…]}`. The endpoint always returns HTTP 200 — external probes should look at the top-level `status` and `summary.errors` fields rather than the HTTP code, so a single flickering dependency does not pull the app out of rotation.
- **Agent traces**: View under **Machine Learning > Experiments > `/Shared/ontobricks-agents`** — each agent call shows a span tree with inputs, outputs, latency, and token usage

### Updating

```bash
# Pull latest code and redeploy
git pull origin main
scripts/deploy.sh -t dev-lakebase
databricks bundle run mcp_ontobricks_app -t dev-lakebase
```
