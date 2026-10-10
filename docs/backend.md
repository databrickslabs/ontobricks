# Graph backends

Where the knowledge graph is stored and how it is read. Each domain picks
an engine under **Domain → Information → Knowledge Graph**: Lakebase
(Postgres), Lakehouse (Delta triples), or Neo4j. This guide covers the
Lakebase and Neo4j engines, then the engine contract, Unity Catalog layout,
and the query optimizations shared by all engines.

<!-- toc -->
**Contents**

- [Lakebase graph store](#lakebase-graph-store)
  - [1. Architecture overview](#1-architecture-overview)
  - [2. Prerequisites](#2-prerequisites)
  - [3. Provisioning the Lakebase project](#3-provisioning-the-lakebase-project)
  - [4. Configuring the engine in the app](#4-configuring-the-engine-in-the-app)
  - [5. Write modes](#5-write-modes)
  - [6. Postgres schema layout](#6-postgres-schema-layout)
  - [7. Scripts reference](#7-scripts-reference)
  - [8. Permissions bootstrap](#8-permissions-bootstrap)
  - [9. Knowledge Graph build — step by step](#9-knowledge-graph-build--step-by-step)
  - [10. Troubleshooting](#10-troubleshooting)
  - [Quick-start checklist (Lakebase Graph DB)](#quick-start-checklist-lakebase-graph-db)
- [Neo4j backend](#neo4j-backend)
  - [1. What the typed model stores](#1-what-the-typed-model-stores)
  - [2. Server requirements](#2-server-requirements)
  - [3. Neo4j flavor compatibility](#3-neo4j-flavor-compatibility)
  - [4. Domain binding — named connection (required)](#4-domain-binding--named-connection-required)
  - [5. OntoBricks assumes it owns the connected database](#5-ontobricks-assumes-it-owns-the-connected-database)
  - [6. Migration note (pre-release flat graphs)](#6-migration-note-pre-release-flat-graphs)
  - [7. Operational notes](#7-operational-notes)
- [Engine integration](#engine-integration)
  - [1. Architecture Overview](#1-architecture-overview-1)
  - [2. The Contract](#2-the-contract)
  - [3. Step-by-Step Integration](#3-step-by-step-integration)
  - [4. Reference: Lakebase Engine Structure](#4-reference-lakebase-engine-structure)
  - [5. Starter Kit](#5-starter-kit)
  - [6. Checklist](#6-checklist)
  - [7. FAQ](#7-faq)
  - [8. Lakebase build performance](#8-lakebase-build-performance)
  - [9. Lakebase managed-synced mode (data plane only)](#9-lakebase-managed-synced-mode-data-plane-only)
  - [Graph query optimizations](#graph-query-optimizations)
<!-- /toc -->

---

## Lakebase graph store


OntoBricks ships with **one built-in graph database engine: Lakebase Postgres**.
This document covers everything you need to provision, configure, and operate it —
from the API gotchas that trip up fresh installs to the Postgres schema layout and
troubleshooting runbook.

For the developer guide on *adding a new engine*, see `docs/graphdb-integration.md`.

---

### Table of Contents

1. [Architecture overview](#1-architecture-overview)
2. [Prerequisites](#2-prerequisites)
3. [Provisioning the Lakebase project](#3-provisioning-the-lakebase-project)
4. [Configuring the engine in the app](#4-configuring-the-engine-in-the-app)
5. [Write modes](#5-write-modes)
6. [Postgres schema layout](#6-postgres-schema-layout)
7. [Scripts reference](#7-scripts-reference)
8. [Permissions bootstrap](#8-permissions-bootstrap)
9. [Knowledge Graph build — step by step](#9-knowledge-graph-build--step-by-step)
10. [Troubleshooting](#10-troubleshooting)

---

### 1. Architecture overview

```
┌─────────────────────────────────────────────────┐
│  OntoBricks (FastAPI)                           │
│                                                 │
│  GraphDBFactory.create(engine="lakebase")       │
│    └─ LakebaseFlatStore                          │
│              │                                  │
│              │  COPY FROM STDIN / INSERT         │
│              ▼                                  │
│  ┌─────────────────────────────────────────┐   │
│  │  Lakebase Postgres (App-bound)          │   │
│  │  schema: ontobricks_graph               │   │
│  │    g_<domain>_v<n>           (app_managed)  │
│  │    g_<domain>_v<n>_sync      (synced table) │
│  │    g_<domain>_v<n>__app      (companion)    │
│  │    g_<domain>_v<n>           (UNION VIEW)   │
│  └─────────────────────────────────────────┘   │
│              │                                  │
│              │  Lakeflow snapshot pipeline       │
│              ▼                                  │
│  ┌─────────────────────────────────────────┐   │
│  │  Unity Catalog                          │   │
│  │  <catalog>.<schema>.<domain>_vN_sync    │   │
│  │  (registered synced table)              │   │
│  └─────────────────────────────────────────┘   │
└─────────────────────────────────────────────────┘
```

OntoBricks maintains **two complementary storage layers**:

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Triple Store** | Delta views in Unity Catalog (SQL Warehouse) | Governance, lineage, SPARQL source-of-truth |
| **Graph DB** | Lakebase Postgres (flat triple table or UNION view) | Fast in-process graph traversal, reasoning, cohort writes |

The Graph DB engine is selected **per domain** under **Domain → Information → Knowledge Graph** (defaults to `lakebase`). Engine *connection* settings remain workspace-global under **Settings → Back end**.

---

### 2. Prerequisites

#### 2.1 — Databricks workspace requirements

| Requirement | Notes |
|-------------|-------|
| Databricks Apps enabled | Needed to run OntoBricks as a platform app |
| SQL Warehouse | Standard or Serverless; bound as the `sql-warehouse` resource |
| Unity Catalog | A catalog + schema for the registry and triplestore views |
| Lakebase feature | Must be enabled on the workspace (contact workspace admin if absent) |
| `psql` on PATH | Required by `scripts/bootstrap/lakebase-perms.sh` (`brew install libpq && brew link --force libpq` on macOS) |

#### 2.2 — Python dependencies

Lakebase support is an **optional extra** since v0.4.0:

```bash
# Local development
uv sync --frozen --extra lakebase

# Or pip
pip install ".[lakebase]"
```

This installs `psycopg[binary]>=3.2.0` and `psycopg-pool>=3.2.0`.
The deployed `app.yaml` already includes `--extra lakebase` in the startup command.

#### 2.3 — Critical API distinction

Lakebase has **two project-creation APIs** with different capabilities:

| API | Endpoint | Synced Tables compatible? |
|-----|----------|--------------------------|
| New (Autoscaling only) | `POST /api/2.0/postgres/projects` | **NO** |
| Old (Autoscaling + Provisioned) | `POST /api/2.0/database/instances` | **YES** |

The **Databricks UI "New project" button** calls the new API and produces a project
that is **incompatible** with `POST /api/2.0/database/synced_tables` (used by the
Knowledge Graph `managed_synced` build mode). Always use `scripts/bootstrap/setup-lakebase.sh`
to provision the project.

---

### 3. Provisioning the Lakebase project

#### 3.1 — Create with `setup-lakebase.sh`

Run once per workspace before the first deploy:

```bash
./scripts/bootstrap/setup-lakebase.sh --name ontobricks-demo --capacity CU_2
```

The script:
1. Checks whether the instance name already exists via `GET /api/2.0/database/instances`.
2. Creates it via `POST /api/2.0/database/instances` (synced-tables-compatible).
3. Polls until the instance reaches `AVAILABLE`.
4. Resolves the branch endpoint and creates the Postgres database.
5. Prints the `db-…` resource id — copy this into `scripts/deploy.config.sh`.

**All options:**

| Flag | Default | Description |
|------|---------|-------------|
| `--name NAME` | `ontobricks-demo` | Lakebase instance name |
| `--capacity CU_N` | `CU_2` | Compute tier: `CU_1`, `CU_2`, `CU_4` |
| `--branch BRANCH` | `production` | Initial branch name |
| `--database DBNAME` | `ontobricks_demo` | Postgres `datname` to create |
| `--profile PROFILE` | `DEFAULT` | Databricks CLI profile |
| `--wait N` | `120` | Seconds to wait for `AVAILABLE` |
| `--dry-run` | — | Print plan without executing |

#### 3.1b — One-click provisioning from Settings (in-app alternative)

Admins can provision a graph DB end-to-end from the UI instead of running the
two scripts by hand. In **Settings → Lakebase → Connection** tab there is a
**"Create graph DB from scratch"** card: fill in the instance/project name,
compute capacity, branch, Postgres database, graph schema, and the MCP app
name, then click **Create graph DB**. The action runs as an async job (a
progress bar + per-step log update live, polling `GET /tasks/{id}` like a
Knowledge Graph build) and performs the same flow as
`scripts/bootstrap/setup-lakebase.sh` + `scripts/bootstrap/lakebase-perms.sh`:

1. Create the Lakebase instance (via the synced-tables-compatible
   `/api/2.0/database/instances` API) and wait for `AVAILABLE`.
2. Create the Postgres database and the graph schema.
3. Grant `CAN_USE` on the project and `USAGE/CREATE/DML` on the schema to the
   app **and** MCP service principals; optionally grant `ALL_PRIVILEGES` on the
   configured UC catalog (managed-sync only).

On success the chosen project/branch/database/schema are written into
`graph_engine_config`, so the Connection pickers reflect the new target.

> **Permission model (unchanged — only automated).** The button runs as the
> app's **own service principal**, not a human. It therefore needs the SP to be
> allowed to create Lakebase instances; if it is not, the job fails on the first
> step with a clear message. Schema grants to the MCP SP are best-effort and
> surfaced as warnings when the MCP Postgres role does not exist yet. In those
> cases the shell scripts (`POST /api/2.0/database/instances` as a human owner)
> remain the documented fallback — re-run `scripts/bootstrap/lakebase-perms.sh`
> after the apps have connected once.

#### 3.2 — After the script

Set the project + Postgres datname in `scripts/deploy.config.sh`
(`deploy.sh` resolves the `db-…` segment automatically):

```bash
DEFAULT_LAKEBASE_PROJECT="<project-name>"
DEFAULT_LAKEBASE_DATABASE="<postgres-datname>"
# Optional override:
# LAKEBASE_DATABASE_RESOURCE_SEGMENT="db-xxxx-xxxxxxxxxx"
```

You can also look it up at any time:

```bash
databricks postgres list-databases \
  "projects/<project-name>/branches/production" -o json \
  | python3 -c "import sys,json; [print(d['name']) for d in json.load(sys.stdin)]"
```

#### 3.3 — Name reservation gotcha

If you delete a Lakebase project from the new `/postgres/projects` API, the name
may remain **ghost-reserved** in the `/database/instances` namespace for an extended
period (15 minutes to hours). If `setup-lakebase.sh` fails with
`Instance name is not unique`, choose a different name and update `deploy.config.sh`.

---

### 4. Configuring the engine in the app

#### 4.1 — UI configuration

Go to **Settings → Graph DB → Engine Configuration** and enter a JSON object:

```jsonc
{
  "schema": "ontobricks_graph",    // Postgres schema for graph tables (default)
  "database": "ontobricks_demo",   // Postgres database (overrides PGDATABASE)
  "sync_mode": "app_managed"       // or "managed_synced" — see §5
}
```

The engine selector should show `lakebase`. If it shows empty, reload the page.

#### 4.2 — All `graph_engine_config.lakebase` keys

``graph_engine_config`` is nested per backend::

    {
      "lakebase":  { ... keys below ... },
      "neo4j":     { "uri": "...", "database": "neo4j", ... },
      "lakehouse": { "warehouse_id": "..." }
    }

Flat legacy blobs are still accepted and rewritten to this shape on Save.
Lakehouse SQL calls resolve ``lakehouse.warehouse_id`` (then fall back to the
global warehouse).

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `schema` | string | `ontobricks_graph` | Postgres schema for graph triple tables. Overridden by the Registry Volume schema when Settings → Registry resolves a non-empty triplet. |
| `database` | string | injected `PGDATABASE` | Postgres database name — overrides the Apps-injected value. |
| `sync_mode` | string | omitted → `app_managed` at runtime; Settings proposes `managed_synced` | Write mode: `managed_synced` (Lakeflow, Settings default) or `app_managed` (direct COPY). Workspaces that never save the key keep `app_managed`. |
| `sync_table_mode` | string | `snapshot` | Lakeflow schedule. Only `snapshot` is valid: the source is an R2RML view (no CDF). Legacy `triggered` / `continuous` values are coerced to `snapshot`. |
| `sync_timeout_s` | int | `600` | Max seconds to wait for a Lakeflow sync run to complete. |
| `sync_uc_catalog` | string | *(auto-detected)* | UC catalog for synced table registration. Auto-detected from Registry settings / `ONTOBRICKS_SYNC_UC_CATALOG` / `domain.delta.catalog`. |

#### 4.3 — Environment variables (deployed app)

The Apps runtime auto-injects these when the `postgres` resource is bound in `databricks.yml`:

| Variable | Source | Description |
|----------|--------|-------------|
| `PGHOST` | Apps runtime | Lakebase endpoint hostname |
| `PGPORT` | Apps runtime | Postgres port (5432) |
| `PGDATABASE` | Apps runtime | Postgres database name |
| `PGUSER` | Apps runtime | Postgres username (SP client ID in Apps) |
| `PGSSLMODE` | Apps runtime | SSL mode (`require`) |

The Postgres **password** is never stored. `LakebaseAuth` mints a short-lived JWT
via `POST /api/2.0/postgres/credentials` on every connection open.

For local development (no Apps resource injection), set these in `.env`:

```bash
LAKEBASE_PROJECT=ontobricks-demo2   # Autoscaling project name
LAKEBASE_BRANCH=production          # Branch to connect to
LAKEBASE_DATABASE=ontobricks_demo   # Postgres datname
LAKEBASE_SCHEMA=ontobricks_registry # Registry schema
PGUSER=you@example.com              # Your Databricks login email
```

#### 4.4 — `deploy.config.sh` variables

These drive the DAB deployment (edit before `make deploy`):

| Variable | Description |
|----------|-------------|
| `DEFAULT_LAKEBASE_PROJECT` | Lakebase project name (final segment of `projects/<id>`) |
| `DEFAULT_LAKEBASE_BRANCH` | Branch (e.g. `production`) |
| `DEFAULT_LAKEBASE_DATABASE` | Postgres datname the registry schema lives in |
| `LAKEBASE_DATABASE_RESOURCE_SEGMENT` | Optional env override for the `db-…` id; when unset, `deploy.sh` resolves it from `DEFAULT_LAKEBASE_DATABASE` |
| `DEFAULT_LAKEBASE_SCHEMA` | Postgres schema for the registry (mirrors `LAKEBASE_SCHEMA` in `app.yaml`) |

> `deploy.config.sh` is **registry-scoped**. The graph DB schema/database
> are configured in-app (`Settings → Graph DB` → `graph_engine_config`)
> and may live in a different Lakebase project — they are not deploy vars.

---

### 5. Write modes

#### 5.1 — `app_managed` (runtime fallback when `sync_mode` is omitted)

The FastAPI process streams R2RML rows from the SQL Warehouse and writes them
directly into the `*_sync` table via `COPY FROM STDIN`. The same 3-object
layout used by `managed_synced` is created: a `*_sync` bulk-data table, a
`*__app` companion for reasoning/cohort writes, and a union view for readers.

```
R2RML view (SQL Warehouse)
    │  iter_rows batches
    ▼
LakebaseFlatStore.bulk_load_into_sync
    │  COPY FROM STDIN → g_<domain>_v<n>_sync  (app-owned)
    ▼
g_<domain>_v<n>  (UNION VIEW over _sync + __app)
```

- Simple setup — no Lakeflow pipelines required.
- App process is on the hot path for large graphs.
- Reasoning / cohort writes always go to `*__app` (consistent with `managed_synced`).
- Suitable for most use cases.

#### 5.2 — `managed_synced` (Lakeflow; Settings proposal)

A **Databricks Lakeflow snapshot pipeline** keeps a Postgres **synced table** in
lock-step with the R2RML Delta view. The app only orchestrates; bulk movement
happens entirely on the Databricks side.

```
R2RML view (Unity Catalog)
    │  Lakeflow snapshot pipeline
    ▼
g_<domain>_v<n>_sync  (Postgres, Lakeflow-owned, read-only)

App writes (reasoning, cohort):
g_<domain>_v<n>__app  (Postgres, app-owned, writable)

Readers:
g_<domain>_v<n>  (UNION VIEW over _sync + __app)
```

Enable it with:
```json
{ "sync_mode": "managed_synced" }
```

**When to use `managed_synced`:**
- Source tables are very large (millions of triples).
- You want Databricks lineage on the synced table.
- The app process should not be the bottleneck during builds.

**Additional requirements for `managed_synced`:**
- The Lakebase project must be provisioned via `scripts/bootstrap/setup-lakebase.sh`
  (provisioned instance, not autoscaling-only) — see §2.3.
- The app SP needs `CAN_USE` on the Lakebase database instance — applied by
  `scripts/bootstrap/lakebase-perms.sh`.
- The UC schema for the synced table must exist before the first build
  (`CREATE SCHEMA IF NOT EXISTS` is run automatically by the build pipeline).

---

### 6. Postgres schema layout

#### 6.1 — Schemas

OntoBricks uses up to three Postgres schemas in the same Lakebase project:

| Schema | Default name | Created by | Purpose |
|--------|-------------|-----------|---------|
| Registry | `ontobricks_registry` | `Settings → Registry → Initialize` | Project metadata, domain configs, schedule runs |
| Graph DB | `ontobricks_graph` | First Knowledge Graph Build | Per-domain triple tables (and views in `managed_synced`) |
| Sync | *(UC registry schema segment)* | First Lakeflow snapshot | Auto-created by Lakeflow; mirrors the UC `<schema>` segment |

#### 6.2 — Objects per graph version

Both `app_managed` and `managed_synced` use the same **3-object triple layout**
per domain version, plus **four Explorer graph-index tables** rebuilt by
`rebuild_adjacency`. The difference between write modes is who writes to the
`*_sync` table.

| Object | Owner | Naming | Description |
|--------|-------|--------|-------------|
| Sync table | App (`app_managed`) / Lakeflow (`managed_synced`) | `g_<domain>_v<n>_sync` | Bulk warehouse data; `(subject, predicate, object, datatype, lang)`. App writes via `COPY FROM STDIN`; Lakeflow writes via snapshot pipeline. |
| Companion table | App (read/write) | `g_<domain>_v<n>__app` | Reasoning / cohort / materialise triples; `(subject, predicate, object, datatype, lang)` |
| UNION view | App DDL | `g_<domain>_v<n>` | `SELECT … FROM _sync UNION ALL SELECT … FROM __app`; exposes the back-compat 5-column shape |
| Adjacency out | App | `g_<domain>_v<n>_adj_out` | Typed entity–entity outgoing edges; btree on `(src, predicate)` |
| Adjacency in | App | `g_<domain>_v<n>_adj_in` | Typed incoming edges; btree on `(dst, predicate)` |
| Entity search | App | `g_<domain>_v<n>_entity_search` | Preview with Inferred on; prefix + optional `pg_trgm` GIN |
| Entity search (asserted) | App | `g_<domain>_v<n>_entity_search_asserted` | Preview with Inferred off; rebuilt from `_sync` |
| Property companion | App | `g_<domain>_v<n>_props` | Outgoing triples of typed subjects; btree on `subject` |

SPARQL still targets the union view `g_<domain>_v<n>`. Explorer hops use the
adjacency tables; Preview uses `_entity_search` or `_entity_search_asserted`;
expansion payload fetch uses `_props` (SPO fallback if that table is missing).
Indexes are snapshots — **Refresh cache** or a full Build refreshes them
together.

#### 6.3 — Drop cascade (both modes)

`LakebaseFlatStore.drop_table(name)` removes all three objects in order:
1. `DROP VIEW IF EXISTS g_<domain>_v<n>` (UNION view)
2. `DROP TABLE IF EXISTS g_<domain>_v<n>__app` (companion)
3. `app_managed`: `DROP TABLE IF EXISTS g_<domain>_v<n>_sync` (sync table)
   `managed_synced`: `SyncedTableManager.delete(uc_name, purge_data=True)` — removes the UC synced-table registration and the underlying Postgres `_sync` table

#### 6.4 — Long literal objects (`object_hash`)

Postgres btree indexes (including the composite primary key) cannot index TEXT
values longer than ~2704 bytes. OntoBricks therefore stores the full `object`
literal for reads/deletes but keys uniqueness on a generated `object_hash`
column (`digest(object, 'sha256')` via the `pgcrypto` extension).

**`pgcrypto` must live in `public`.** Pooled connections run
`SET search_path TO "<graph_schema>", public`, and a bare `CREATE EXTENSION`
installs into the *first* search_path entry — i.e. the graph schema. Since
`IF NOT EXISTS` is then a permanent no-op, changing the graph schema strands
`digest()` out of reach and every Build fails with
`function digest(text, unknown) does not exist`.

Two places keep this correct, both idempotent:

- `ensure_pgcrypto` (app) installs into `public` and **relocates** a stranded
  extension via `ALTER EXTENSION pgcrypto SET SCHEMA public`. The app owns any
  extension it created, so it can self-heal.
- `make bootstrap-lakebase` **Step 1b** does the same as the admin, and warns
  (without aborting) when the extension is app-owned and cannot be relocated.

The extension is **per database** — the graph database is often a different
Lakebase project/database from the registry (`graph_engine_config.database`),
so fixing one does not fix the other.

- **`app_managed`**: `*_sync` and `*__app` tables are created with
  `PRIMARY KEY (subject, predicate, object_hash)`.
- **`managed_synced`**: the Delta warehouse VIEW exposes `object_hash`
  (`sha2(object, 256)`) and Lakeflow uses
  `primary_key_columns = [subject, predicate, object_hash]`.

Graphs created before this layout need a **full Knowledge Graph rebuild** (drop
+ recreate) to pick up the new schema.

---

### 7. Scripts reference

#### `scripts/bootstrap/setup-lakebase.sh`

Provisions a Lakebase project via `POST /api/2.0/database/instances` (synced-tables-compatible).

```bash
# Basic usage
./scripts/bootstrap/setup-lakebase.sh --name my-project --capacity CU_2

# Dry-run to preview
./scripts/bootstrap/setup-lakebase.sh --name my-project --dry-run

# Custom profile
./scripts/bootstrap/setup-lakebase.sh --name my-project --profile prod-workspace
```

**Outputs:** prints the `db-…` resource id (informational). Set `DEFAULT_LAKEBASE_PROJECT` / `DEFAULT_LAKEBASE_DATABASE` in `deploy.config.sh`; optional override `LAKEBASE_DATABASE_RESOURCE_SEGMENT`.

#### `scripts/bootstrap/lakebase-perms.sh`

Grants the app service principals the Postgres and control-plane permissions
they need to operate. **Idempotent — safe to run repeatedly.**

```bash
# Registry schema
scripts/bootstrap/lakebase-perms.sh \
  -i ontobricks-demo2 -b production \
  -d ontobricks_demo -s ontobricks_registry \
  -a ontobricks-030 -a mcp-ontobricks

# Graph DB schema (run after first Build)
scripts/bootstrap/lakebase-perms.sh \
  -i ontobricks-demo2 -b production \
  -d ontobricks_demo -s ontobricks_graph \
  -a ontobricks-030 -a mcp-ontobricks

# Sync schema (managed_synced only — run after first Lakeflow snapshot)
scripts/bootstrap/lakebase-perms.sh \
  -i ontobricks-demo2 -b production \
  -d ontobricks_demo -s ontobricks \
  -a ontobricks-030 -a mcp-ontobricks
```

`make deploy` (via `scripts/deploy.sh`) grants the **registry** schema
automatically. The graph and sync schemas are granted by the in-app
"Create graph DB" flow or by running the commands above manually — they
are not deploy vars, since the graph DB may live in a different Lakebase
project.

**What each run grants:**

| Grant | Level | Purpose |
|-------|-------|---------|
| `CREATE EXTENSION pgcrypto WITH SCHEMA public` | Database | Installs / relocates `digest()` for companion `object_hash` (Step 1b) |
| `CAN_USE` (control-plane) | Lakebase instance | Allows the SP to call Lakebase APIs (e.g. `synced_tables`) |
| `CAN_USE` (autoscaling API) | Lakebase project | Belt-and-suspenders for autoscaling path |
| `USAGE` + `CREATE` | Postgres schema | Let the SP create tables/views in the schema |
| `SELECT/INSERT/UPDATE/DELETE` | All existing tables | DML on current objects |
| `USAGE/SELECT/UPDATE` | All existing sequences | Required for `bigserial` PKs |
| `ALTER DEFAULT PRIVILEGES` | Schema | Future tables/sequences inherit the same grants |
| `ALL PRIVILEGES` (UC catalog) | Unity Catalog catalog | Read back synced tables; only granted when `-c` flag passed |

#### `scripts/deploy.sh`

Full deploy pipeline (called by `make deploy`). On the `dev-lakebase` target it:

1. Renders `app.yaml` from `app.yaml.template` + `deploy.config.sh`.
2. Validates the DAB bundle.
3. Deploys both apps.
4. Starts the main app.
5. Bootstraps app self-permissions (`bootstrap-app-permissions.sh`).
6. Runs `bootstrap-lakebase-perms.sh` for all configured schemas.

```bash
make deploy                    # dev-lakebase target (default)
make bootstrap-lakebase        # run only the Lakebase grants
make deploy-volume             # dev target (volume-only, no Lakebase binding)
```

---

### 8. Permissions bootstrap

#### 8.1 — Order of operations

Lakebase schemas are created lazily (by app actions), so grants must follow creation:

```
Step 1:  make deploy
         → CAN_USE on instance applied immediately (before schema exists)
Step 2:  Open app → Settings → Registry → Initialize
         → Creates 'ontobricks_registry' schema in Postgres
Step 3:  make bootstrap-lakebase  (or make deploy again — idempotent)
         → Applies USAGE + DML on 'ontobricks_registry' schema
Step 4:  Build a Knowledge Graph (Settings → Knowledge Graph → Build)
         → Creates 'ontobricks_graph' schema (first build)
Step 5:  make bootstrap-lakebase  (or make deploy again)
         → Applies USAGE + DML on 'ontobricks_graph' schema
Step 6:  (managed_synced only) First Lakeflow snapshot completes
         → Creates the sync schema automatically
Step 7:  make bootstrap-lakebase  (or make deploy again)
         → Applies USAGE + DML on sync schema
```

`make deploy` is always safe to re-run — it skips schemas that don't exist yet
and prints an informational message instead of failing.

#### 8.2 — Verify permissions

Check what the SP has been granted from a SQL editor or psql:

```sql
-- From psql (connected as human admin)
\dn+                                     -- list schemas + ACLs
\dp ontobricks_graph.*                   -- table-level ACLs
SELECT * FROM information_schema.role_table_grants WHERE grantee = '<sp-client-id>';
```

Check `CAN_USE` on the instance (Databricks CLI):

```bash
databricks permissions get database-instances/<instance-id> -o json
```

---

### 9. Knowledge Graph build — step by step

This section describes what the Lakebase engine does during a **Build** for the
`managed_synced` mode. For `app_managed`, steps 3–6 are replaced by direct
`COPY FROM STDIN` ingestion.

| Step | What happens | What can fail |
|------|-------------|---------------|
| 1 | Resolve the synced UC FQN: `<catalog>.<schema>.<domain>_vN_sync` | Wrong catalog resolved — check `sync_uc_catalog` / `ONTOBRICKS_SYNC_UC_CATALOG` |
| 2 | `CREATE SCHEMA IF NOT EXISTS` in Unity Catalog (SQL Warehouse DDL) | SP missing `CREATE SCHEMA` on UC — grant via SQL |
| 3 | `SyncedTableManager.ensure(...)` — idempotent `create_synced_database_table` call | `Database instance is not found` — project created via wrong API; `Not authorized` — SP missing `CAN_USE` on instance |
| 4 | Create companion table `g_<domain>_vN__app` in Postgres | SP missing `CREATE` on graph schema |
| 5 | `SyncedTableManager.trigger_and_wait(...)` — fires Lakeflow snapshot, waits for completion | Timeout — increase `sync_timeout_s`; pipeline stuck — check Lakeflow pipeline status in Databricks UI |
| 6 | `LakebaseFlatStore.ensure_synced_union_view(name)` — creates the UNION view | `"<view>" is not a view` — an old table with the same name exists (auto-dropped since v0.4.1); `_sync table not found` — Lakeflow didn't materialise the table yet |
| 7 | `TRUNCATE` companion (full rebuild only) | |

Build logs are streamed to the UI and to the application log. Look for lines tagged
`[DT-BUILD <id>]` for per-step context.

---

### 10. Troubleshooting

#### `Database instance is not found`

```
Failed to create synced table …: Database instance is not found.
```

**Cause:** The Lakebase project was created via the new autoscaling API
(`/postgres/projects`) which is **not** listed in `/database/instances`.
The Synced Tables API only accepts provisioned instance names.

**Fix:**
1. Delete the old project from the UI.
2. Re-create it with `scripts/bootstrap/setup-lakebase.sh`.
3. Update `DEFAULT_LAKEBASE_PROJECT` / `DEFAULT_LAKEBASE_DATABASE` in `deploy.config.sh` (optional: `LAKEBASE_DATABASE_RESOURCE_SEGMENT`).
4. `make deploy`.

If the name is reserved (`Instance name is not unique`), choose a different name —
deleted names can stay ghost-reserved for hours.

---

#### `The user is not authorized … assign 'Can Use' or 'Can Manage'`

```
Failed to create synced table …:
The user is not authorized … assign cf06ae08… 'Can Use' or 'Can Manage'
for Database instance 6b981581-…
```

**Cause:** The app service principal lacks `CAN_USE` on the Lakebase instance.

**Fix:**

```bash
make bootstrap-lakebase
# or manually:
databricks permissions update database-instances/<instance-id> \
  --json '{"access_control_list":[{"service_principal_name":"<sp-id>","permission_level":"CAN_USE"}]}'
```

---

#### `"<view>" is not a view`

```
Failed: Could not create Lakebase union view after sync: "<name>" is not a view.
```

**Cause:** An object with the union view's name already exists as a **TABLE**
(e.g. left over from an `app_managed` build on the same version).
`CREATE OR REPLACE VIEW` cannot replace an existing table in PostgreSQL.

**Fix (automatic since v0.4.1):** The `ensure_union_view` function now
auto-detects and drops the conflicting table before creating the view.
If you see this error on an older deployment, drop the table manually:

```sql
-- From psql connected to the Lakebase database
DROP TABLE IF EXISTS ontobricks_graph."g_<domain>_vN" CASCADE;
```

Then retry the build.

---

#### `_sync table '…_b' not found in Postgres`

```
Failed: Could not create Lakebase union view after sync:
_sync table 'cust360auto_v4_sync_b' not found in Postgres.
```

**Cause:** The Lakeflow snapshot pipeline ran against the wrong branch
(e.g. `production` instead of `demo`). The `_sync` table therefore landed in
a different Lakebase branch schema.

**Fix:**
1. In **Settings → Graph DB**, verify `database` matches the Postgres `datname`
   for your branch.
2. In `deploy.config.sh`, verify `DEFAULT_LAKEBASE_BRANCH` points to the
   branch configured in the app settings.
3. `make deploy` and rebuild.

---

#### `Lakebase connection failed: database "ontobricks_registry" does not exist`

**Cause:** The registry database or schema has not been created yet.
This is expected on a fresh deployment.

**Fix (in order):**
1. Open the deployed app.
2. Go to **Settings → Registry → Initialize**.
3. After initialisation, run `make bootstrap-lakebase` (or `make deploy`).

---

#### `logicalDatabaseName must be defined when creating synced table in a standard catalog`

**Cause:** The `graph_engine_config.database` key is missing or the
`PGDATABASE` env var was not injected by the Apps runtime (postgres resource
not bound).

**Fix:**
1. Confirm the `postgres` resource binding in the Databricks Apps UI.
2. Add `"database": "<datname>"` to the `graph_engine_config` JSON.
3. Restart the app.

---

#### `Must specify either database instance name or both database project and branch`

**Cause:** The synced table registration call is missing both the instance name
and the project+branch pair. This means `lakebase_project` or
`lakebase_database_resource_segment` is empty in `deploy.config.sh`.

**Fix:**
1. Confirm `DEFAULT_LAKEBASE_PROJECT` / `DEFAULT_LAKEBASE_DATABASE` in `deploy.config.sh`.
2. If auto-resolve fails, set `LAKEBASE_DATABASE_RESOURCE_SEGMENT=db-xxxx-xxxxxxxxxx`
   (from `databricks postgres list-databases "projects/<project>/branches/<branch>" -o json`).
3. `make deploy`.

---

#### Build uses `app_managed` even though `managed_synced` is configured in Settings

**Symptom:** The build log shows `sync_mode=app_managed` and iterates triples
through the app process. The Settings → Graph DB page correctly displays the
saved config with `"sync_mode": "managed_synced"`.

**Cause (fixed in v0.6.0):** On cold start, if the Lakebase store was briefly
unavailable (typical the first few seconds after the app container started), the
`GlobalConfigService` in-memory cache was populated with the empty template
(`_empty()`). Subsequent reads within the 5-minute cache TTL returned the empty
config, so the build resolved `sync_mode` as missing and silently fell back to
`app_managed`. The Settings page was not affected because it always forces a
fresh read.

**Fix:** Upgrade to v0.6.0 — `_resolve_lakebase_mode` now bypasses the cache
via `force=True`.  No configuration change needed; simply redeploy.

---

#### Settings → Graph DB: catalog list is empty

The UC catalog dropdown uses the configured SQL Warehouse. If the warehouse is not
yet saved to the global config, the dropdown falls back to `DATABRICKS_SQL_WAREHOUSE_ID`
from the environment. If all fallbacks fail, the "Configure a SQL warehouse first"
message appears.

**Fix:**
1. Go to **Settings → Databricks** and save the SQL Warehouse.
2. If it still fails, verify `DATABRICKS_SQL_WAREHOUSE_ID` / `DATABRICKS_SQL_WAREHOUSE_ID_DEFAULT`
   are set in the Apps resource binding or `.env`.

---

#### Delete asset button does nothing (Settings → Graph DB)

**Cause:** `window.confirm()` is suppressed inside the Databricks Apps iframe.

**Fix (applied since v0.4.1):** The delete flow uses a Bootstrap modal
instead of `window.confirm()`. If you see this on an older deployment,
`make deploy` to pick up the fix.

---

### Quick-start checklist (Lakebase Graph DB)

```
[ ] 1. Create Lakebase project:
        ./scripts/bootstrap/setup-lakebase.sh --name <name> --capacity CU_2
[ ] 2. Set DEFAULT_LAKEBASE_PROJECT / DEFAULT_LAKEBASE_DATABASE in deploy.config.sh
        (optional override: LAKEBASE_DATABASE_RESOURCE_SEGMENT="db-xxxx-xxxxxxxxxx")
[ ] 3. Set DEFAULT_LAKEBASE_BRANCH (and DEFAULT_LAKEBASE_SCHEMA if needed) in deploy.config.sh
[ ] 4. make deploy
[ ] 5. Bind resources in Databricks Apps UI (sql-warehouse, volume, postgres)
[ ] 6. Open app → Settings → Registry → Initialize
[ ] 7. make bootstrap-lakebase  (or make deploy — idempotent)
[ ] 8. Open Settings → Graph DB
        - Engine: lakebase
        - Config: { "sync_mode": "app_managed" }  (or "managed_synced")
[ ] 9. Build your first Knowledge Graph
[ ] 10. make bootstrap-lakebase again (grants on ontobricks_graph schema)
```

---

## Neo4j backend


OntoBricks can store a domain's Knowledge Graph in **Neo4j** as a **typed
property graph** (as opposed to the flat triple tables used by the Lakebase and
Delta backends). This document states what a Neo4j server must provide, which
Neo4j *flavors* are supported for the first release, and the limitations users
should know before pointing a production domain at Neo4j.

For the connection/setup walk-through see `docs/pr47-neo4j-demo/`. For the
graph model internals see the module docstrings in
`src/back/core/graphdb/neo4j/`.

---

### 1. What the typed model stores

When a domain's graph backend is **Neo4j** (Domain → Information → Knowledge
Graph → *Neo4j*), the build pipeline writes a property graph, not flat triples:

| RDF construct | Neo4j representation |
|---|---|
| subject / object URI | a **node**, MERGE-keyed on its full URI (`uri` property) |
| `rdf:type` | a Neo4j **label** on the node |
| `rdfs:label` | the node's `name` property |
| predicate with a **literal** object | a **property** on the subject node |
| predicate with a **URI** object | a **relationship** `(s)-[:reltype]->(o)` |

Every node also carries a per-graph **marker label** (the sanitised
`<Domain>_V<version>` name) and is covered by a `uri` uniqueness constraint, so
one domain graph is a single `MATCH (n:\`<marker>\`)` away for counting,
isolation, and dropping.

Reads reconstruct the exact original `{subject, predicate, object}` triples from
the graph (using a small per-graph reverse-map stored on a `:__GraphSchema`
node), so the Knowledge-Graph view, GraphQL, and reasoning layers behave
identically to the SQL backends — while traversal and reasoning run as **native
Cypher** relationship patterns.

---

### 2. Server requirements

**A Neo4j server running version 5.x is required.** This is a *server* version
requirement, not just a driver requirement. The backend uses 5.x-only syntax:

- `CREATE CONSTRAINT … FOR (n:Label) REQUIRE n.uri IS UNIQUE`
  (Neo4j 4.x used the older `ASSERT` form)
- `SHOW CONSTRAINTS YIELD name, labelsOrTypes`
- `SHOW DATABASES YIELD name`

Neo4j **4.x will not work** and is not supported.

**APOC is not required.** The write path deliberately inlines sanitised labels
and groups rows with `UNWIND` rather than calling `apoc.create.*`, so the
backend runs on Aura Free and Community with no plugins installed.

**Named connection profiles.** Settings → Neo4j stores one or more **named
connections** (`graph_engine_config.neo4j.connections[]`), each with its own
Bolt URI, database, username, encryption flag, and Databricks secret
scope/key. There is no auto-migration from the older flat single-profile keys —
admins re-enter connections in the master–detail UI.

**Auth.** Every connection must use a Databricks secret (`auth_method:
databricks_secret`). Clear-text passwords are stripped on save. In a deployed
Databricks App the runtime still accepts a bound `NEO4J_PASSWORD` env var as a
legacy fallback for older configs, but the Settings UI only exposes the
secret-scope path. Self-hosted servers may use `bolt://` / `neo4j://`
(unencrypted) or `neo4j+s://` / `bolt+s://` (TLS embedded).

---

### 3. Neo4j flavor compatibility

OntoBricks is developed and tested against **Neo4j Aura** (managed, v5). Other
flavors work with the caveats below.

| Flavor | Typed graph write/read | Multi-database (per connection) | Notes |
|---|---|---|---|
| **Aura** (managed) | ✅ | ✅ (paid tiers) | Primary dev/test target. Aura Free is single-DB. |
| **Enterprise** (self-hosted) | ✅ | ✅ | Full support; put the target DB name on the connection profile. |
| **Community** (self-hosted) | ✅ | ❌ single `neo4j` DB only | Typed graph works; leave the connection's database at the Community default. |
| **AuraDS / Neo4j 4.x** | ❌ (4.x) / ⚠️ (AuraDS = v5, works) | — | 4.x unsupported; AuraDS follows the v5 rules above. |

---

### 4. Domain binding — named connection (required)

When a domain's Graph Backend is **Neo4j**, Domain → Information → Knowledge
Graph requires a **Neo4j connection** dropdown value (`domain.info.neo4j_connection`).
That name resolves to one Settings profile at build / query time. There is no
blank “use default” and no per-domain database override — the database lives on
the connection profile.

> **If the profile points at a database that does not exist on the server**,
> queries fail when the driver opens a session. Prefer **Test connection** on
> the selected profile in Settings → Neo4j before binding domains to it.

Deleting or renaming a connection that domains still reference is rejected with
the list of affected domains.

---

### 5. OntoBricks assumes it owns the connected database

The admin **Objects** tab (Settings → Neo4j → Objects) lists **every OntoBricks
graph** in the **selected** connection's Neo4j database — that is, every node
label backed by a `node_<label>_uri` uniqueness constraint — with node and
relationship counts, and lets an admin **drop** any of them. Pick the
connection in the Objects picker first.

Because this operates on the whole connected database:

- **Give each OntoBricks deployment its own Neo4j database** (Enterprise / Aura)
  **or its own instance.** Two deployments pointed at the same database will see
  and can drop each other's graphs.
- Non-OntoBricks data in the same database is *not* listed (it has no
  `node_*_uri` marker constraint) and is not touched by graph drops, but you are
  still sharing an instance — plan capacity and access accordingly.

---

### 6. Migration note (pre-release flat graphs)

Earlier v0.7 pre-release builds wrote Neo4j as a **flat triple store**
(`(:Label {subject, predicate, object})` nodes with no relationships). The typed
model is a **breaking storage change**: old flat graphs are **not** migrated
automatically and must be **rebuilt**. Old flat nodes and new typed nodes cannot
coexist meaningfully under the same marker label — drop the old graph (Objects
tab) and rebuild the domain.

Domains that still have a flat workspace Neo4j config (no `connections[]`) must
be reconfigured: add named connection(s) in Settings → Neo4j, then set each
Neo4j domain's **Neo4j connection** on Domain → Information → Knowledge Graph.

---

### 7. Operational notes

- **Bulk-build memory.** On heap-constrained instances (e.g. Aura Free), very
  large ontologies may need batch tuning; the default insert batch size is
  2000 triples.
- **No raw Cypher entry point.** All writes go through the build pipeline after
  ontology validation (the C2 safeguard); there is no user-facing Cypher console.
- **Test connection.** Settings → Neo4j → **Test connection** (per selected
  profile) runs a Bolt handshake plus a trivial `RETURN 1` so you can confirm
  reachability without running a build. The former dedicated Health tab was
  removed; Test connection covers the same probe.

---

## Engine integration


This guide walks a developer through adding support for a new graph database
engine to OntoBricks.  It covers the architecture, the abstract contracts,
registration in the factory and global config, and a ready-to-use starter kit.

---

OntoBricks ships three **runtime** graph engines. The backend is chosen **per domain** under
**Domain → Information → Knowledge Graph** (mandatory; defaults to ``lakebase``) — the selection is
stored in ``DomainSession.info['graph_backend']``. Engine *connection* config stays workspace-global
under **Settings → Back end**.

| Engine | Storage | Notes |
|--------|---------|--------|
| ``lakebase`` (default) | Flat triple tables on **Lakebase Postgres** | Uses the App-bound Postgres instance (``PGHOST`` / ``PGDATABASE``…). Configure ``graph_engine_config.lakebase`` with optional ``database`` (Postgres DB name on that instance) and ``schema`` (default ``ontobricks_graph``), and ``sync_mode`` (``app_managed`` or ``managed_synced``). SQL-only (no Cypher); reasoning uses the existing SQL translators. |
| ``databricks`` (Delta) | Unity Catalog Delta triple tables | Configure ``graph_engine_config.lakehouse.warehouse_id``. |
| ``neo4j`` | Native graph over Bolt | Neo4j Aura or self-hosted; connection config in ``graph_engine_config.neo4j`` (``uri``, ``database``, credentials). |

Each backend's connection settings are stored in **separate** buckets under
``graph_engine_config`` (``lakebase`` / ``neo4j`` / ``lakehouse``) so an admin can
configure all backends independently without shared keys. Flat legacy blobs are
migrated on read and rewritten nested on the next Save.

``GraphDBFactory.create(engine=...)`` is the single decision point: only the selected engine is instantiated. The capability flags on ``GraphDBBackend`` (``supports_cypher``, ``is_cypher_backend``, ``query_dialect``) are kept as architectural seams so a future Cypher / Gremlin engine can be added without rewiring reasoning.

---

### 1. Architecture Overview

OntoBricks stores all graph viewer data through a single abstraction:

| Layer | Package | Purpose |
|-------|---------|---------|
| **Graph DB** | `back.core.graphdb` | The single triple store / graph DB layer. Ships Lakebase Postgres and Unity Catalog Delta engines; pluggable for embedded/Cypher engines for traversal, reasoning, and analytics. |

The single `GraphDBFactory` reads the **per-domain** backend choice from
`DomainSession.info['graph_backend']` and constructs the matching backend. Calling
`get_graphdb(domain, settings)` with no engine auto-resolves; passing
`engine="view"` returns a raw read-only Delta store for health probes.

```
get_graphdb(domain, settings)          # engine=None → auto-resolve
    │
    └─ GraphDBFactory.create(engine=None)
           │
           ├─ _resolve_graph_backend()         →  domain.info["graph_backend"]
           │                                        "lakebase" | "databricks" | "neo4j"
           ├─ _resolve_triple_store_backend()  →  "lakebase" | "databricks"
           ├─ _resolve_graph_engine()          →  "lakebase" | "neo4j"
           └─ GraphDBFactory.create(domain, settings, engine="neo4j")
                  │
                  └─ _create_neo4j(domain, settings)  →  Neo4jStore(...)
```

#### Key files

| File | Role |
|------|------|
| `src/back/core/graphdb/GraphDBBackend.py` | The single abstract base — triple CRUD, named query + reasoning methods (SQL defaults), capability flags, connection management, sync. |
| `src/back/core/graphdb/GraphDBFactory.py` | Factory — engine resolution + maps engine names to constructor methods. |
| `src/back/core/graphdb/constants.py` | Shared RDF constants (`RDF_TYPE`, `RDFS_LABEL`). |
| `src/back/core/graphdb/delta/DeltaFlatStore.py` | Unity Catalog Delta engine (also the raw `view` store). |
| `src/back/core/graphdb/delta/_table_naming.py` | FQN helpers for the R2RML VIEW, `_data`, `_inferred`, `_graph`. |
| `src/back/core/graphdb/delta/materialize.py` | CTAS / companion / union-VIEW SQL used by Build. |
| `src/back/core/graphdb/__init__.py` | Package exports (`get_graphdb`, `GRAPHDB_AVAILABLE`). |
| `src/back/objects/session/GlobalConfigService.py` | Persists the engine *connection* config (`graph_engine_config`) in `.global_config.json`. The backend *selection* lives per-domain in `DomainSession.info['graph_backend']`. |

#### Lakehouse (Delta) Unity Catalog objects

Regardless of which Graph DB engine the domain uses, **Knowledge Graph → Build** always materialises a small family of Unity Catalog objects in the registry `catalog.schema`. They share the base name `triplestore_<safe_domain>_V<version>`:

| Suffix / name | Kind | Created by | Consumed by |
|---------------|------|------------|-------------|
| *(no suffix)* `triplestore_<domain>_V<n>` | VIEW | Build — `CREATE OR REPLACE VIEW` from the R2RML SQL | Source for `_data`; governance / lineage of the mapping |
| `_data` | Delta TABLE *or* VIEW | **table** mode: `CREATE OR REPLACE TABLE … AS SELECT … FROM <view>`, `CLUSTER BY (predicate, subject)`. **view** mode: pass-through `CREATE OR REPLACE VIEW` (no row copy). Lakebase and Neo4j domains always get the TABLE. | Graph Analytics (Lakeflow job); bulk half of `_graph`; Lakehouse engine reads |
| `_inferred` | Delta TABLE | Build — `CREATE TABLE IF NOT EXISTS` (same SPO shape); truncated on full rebuild | Reasoning / cohort / app writes |
| `_graph` | VIEW | Build — `_data UNION ALL _inferred` | Explorer, filters, stats, GraphQL when inferred triples are included |

Which objects are copies vs views, and how Lakebase / Neo4j add a second
store, is tabulated in
[Architecture → Backend capability and object lifecycle](architecture.md#backend-capability-and-object-lifecycle).

```text
source tables
    → R2RML VIEW          (live mapping)
    → _data               (TABLE: mapped snapshot, or VIEW: no copy)
         ↘
           _graph VIEW    (interactive graph reads)
         ↗
      _inferred TABLE     (app-written triples)
```

The mapped snapshot (`_data`) is what makes KPIs identical across backends: Lakebase and Neo4j mirror those triples into their own stores, but analytics always scores `_data`, never the engine-local copy and never `_inferred`. If `_data` is missing (typical for domains last built before the materialise step was unconditional), analytics refuses the run and tells the user to rebuild — it must not be reported as a warehouse connectivity failure.

##### Adjacency index objects

After the `_graph` VIEW is created, **Build** also materialises four graph
indexes: two adjacency tables for hop queries, one compact entity-search
table for Explorer Preview, and one typed-subject property table for the
final Explorer payload fetch. These are separate companions, not part of the
four-object triple-store family above:

| Object | Kind | Clustering / index | Created when | Used by |
|--------|------|--------------------|--------------|---------|
| `triplestore_<domain>_V<n>_adj_out` | Delta TABLE | `CLUSTER BY (src, predicate)` | End of Build (after `_data` CTAS in table mode; from `_graph` view in view mode) | Explorer hops, `expand_entity_neighbors` — outgoing direction |
| `triplestore_<domain>_V<n>_adj_in`  | Delta TABLE | `CLUSTER BY (dst, predicate)` | same | reverse hops |
| `triplestore_<domain>_V<n>_entity_search` | Delta TABLE | `CLUSTER BY (type_uri, label_lc)` + Bloom on `label_lc,uri_lc` | same | Explorer Preview (Inferred on) |
| `triplestore_<domain>_V<n>_entity_search_asserted` | Delta TABLE | same | same, from `_data` | Explorer Preview (Inferred off) |
| `triplestore_<domain>_V<n>_props` | Delta TABLE | `CLUSTER BY (subject)` | same | Explorer expansion payload — all outgoing triples of typed subjects |

**Lakebase graph-index tables** use `_adj_out`, `_adj_in`,
`_entity_search`, `_entity_search_asserted`, and `_props` suffixes. The entity
index stores normalized URI and label columns alongside type metadata;
`_props` has a btree index on `subject`. Both `app_managed` and
`managed_synced` modes use the same Postgres graph layout per version
(`_sync` bulk-data table, `__app` writable companion, and the reader-facing
union view `g_<dom>_v<n>`).  `rebuild_adjacency` reads hops/`_props` from the
union view and asserted Preview from `_sync`.

**Neo4j** has none of these companion tables. Native Bolt graph traversal and
search remain unchanged; no adjacency refresh action is available.

##### Graph-cache refresh

Beyond the full **Build**, a **Refresh cache** action (builder / admin
only) rebuilds `_adj_out`, `_adj_in`, `_entity_search`, and `_props` without
rematerializing `_data` or touching inferred triples. The entity-search
snapshot powers Preview when **Inferred** is enabled; asserted-only searches
fall back to the asserted SPO relation. Adjacency-driven expansion reads its
payload from `_props`; if that table is missing, it retries against the
reader-facing SPO relation. Lakehouse rebuild DDL always runs on
the configured **Build SQL Warehouse**, never on the Lakehouse/RT query
warehouse, because Lakehouse/RT does not support `CREATE OR REPLACE TABLE`.
The semantics differ by mode:

| Mode | What "Refresh cache" does | When source data appears in traversal |
|------|-------------------------------|---------------------------------------|
| **Lakehouse — `view` materialization** | Reruns the adjacency CTAS from the live `_graph` VIEW (which itself re-executes the R2RML SQL). Source-table changes propagate immediately because `_data` is a pass-through view. | After the next adjacency refresh (source rows are live via `_data`; adjacency tables are a snapshot of `_graph` at refresh time) |
| **Lakehouse — `table` materialization** | Reindexes from the *existing* `_graph` snapshot — `_data` is **not** rebuilt. New source rows are not visible in traversal until a full **Build** runs. | After the next full **Build** only |
| **Lakebase** | Reindexes `_adj_out`, `_adj_in`, `_entity_search`, and `_props` from the current reader-facing union view in one data-load transaction. | After the next Refresh cache or Build |
| **Neo4j** | *(not available)* | N/A — Neo4j uses native traversal, no adjacency tables exist |

> **Delta consistency note:** Lakehouse graph-index rebuild replaces
> `_adj_out`, `_adj_in`, `_entity_search`, and `_props` sequentially, not as
> a cross-table atomic swap. If a point-in-time read must see both directions
> from the same snapshot, avoid running reads concurrently with Build/Refresh
> and read after the task completes.
>
> **Important:** On a Lakehouse domain in `table` materialization mode, the
> Explorer's entity expansion (which uses adjacency tables) and a raw SPARQL
> scan of `_graph` can disagree between builds.  Expansion reflects the
> adjacency snapshot; SPARQL scans `_graph` which always includes `_inferred`.
> A full **Build** is required to ingest source-table changes into `_data` and
> therefore into the adjacency snapshot.

Canonical prose also lives in [`architecture.md` § Lakehouse Unity Catalog objects](architecture.md#lakehouse-unity-catalog-objects).

---

### 2. The Contract

A new engine implements the single `GraphDBBackend` abstraction:

#### 2.1 `GraphDBBackend` (core CRUD)

These abstract methods **must** be implemented:

| Method | Signature | Description |
|--------|-----------|-------------|
| `create_table` | `(table_name: str) -> None` | Create the `(subject, predicate, object)` storage. |
| `drop_table` | `(table_name: str) -> None` | Drop the table if it exists. |
| `insert_triples` | `(table_name, triples, batch_size, on_progress) -> int` | Batch insert triples. Return count inserted. |
| `query_triples` | `(table_name: str) -> List[Dict[str, str]]` | Return all triples as `{subject, predicate, object}` dicts. |
| `count_triples` | `(table_name: str) -> int` | Return the number of triples. |
| `table_exists` | `(table_name: str) -> bool` | Check if the triple table exists. |
| `get_status` | `(table_name: str) -> Dict[str, Any]` | Return `{count, last_modified, path, format}`. |
| `execute_query` | `(query: str) -> List[Dict[str, Any]]` | Execute a raw query (SQL or native). Raise `NotImplementedError` if not applicable. |

These methods have **SQL default implementations** that you should **override**
if your engine does not speak SQL:

- `get_aggregate_stats`
- `get_type_distribution` / `get_predicate_distribution`
- `find_subjects_by_type` / `resolve_subject_by_id`
- `get_entity_metadata` / `get_triples_for_subjects`
- `get_predicates_for_type`
- `paginated_triples` / `paginated_count`
- `bfs_traversal`
- `find_seed_subjects` / `find_subjects_by_patterns`
- `transitive_closure` / `symmetric_expand` / `shortest_path`
- `expand_entity_neighbors`
- `delete_triples` (raises `NotImplementedError` by default)
- `optimize_table` (no-op by default)

#### 2.2 `GraphDBBackend` (graph-specific methods)

**Constructor parameter** — every engine receives `engine_config: Dict[str, Any]`
(default `{}`) from the factory.  This is a free-form JSON dict set by the
admin in **Settings > Graph DB > Engine Configuration**.  Each engine defines
its own keys.  For Lakebase, recognised keys include ``database``, ``schema``,
and ``mode`` (``app_managed`` or ``managed_synced``).

These abstract methods **must** be implemented:

| Method | Signature | Description |
|--------|-----------|-------------|
| `get_connection` | `() -> Any` | Return (and lazily open) the native database connection. |
| `close` | `() -> None` | Release the connection and any related resources. |

These have sensible **defaults** that you should **override** as needed:

| Method | Default | Override when... |
|--------|---------|-----------------|
| `supports_cypher` | `False` | Your engine speaks Cypher. |
| `supports_graph_model` | `False` | Your engine uses typed node/relationship tables. |
| `query_dialect` | `"sql"` | Your engine uses a different dialect (e.g. `"cypher"`, `"gremlin"`). |
| `get_node_table(name)` | Returns `name` unchanged | Your engine has naming constraints (e.g. identifier sanitisation). |
| `get_graph_schema()` | `None` | Your engine builds a graph schema from the ontology. |
| `sync_to_remote(uc_path, volume_service)` | No-op | Your engine stores files that should be synced to UC Volumes. |
| `sync_from_remote(uc_path, volume_service)` | No-op | Same, for restore on cold start. |
| `local_path()` | `None` | Your engine stores data locally. |
| `remote_archive_path(uc_domain_path)` | `None` | Your engine has a remote archive naming convention. |
| `get_query_translator(table_name)` | `SWRLSQLTranslator()` | Your engine needs a custom SWRL/rule translator for reasoning. |

---

### 3. Step-by-Step Integration

#### Step 1 — Create the engine subpackage

```
src/back/core/graphdb/
├── __init__.py
├── GraphDBBackend.py
├── GraphDBFactory.py
├── lakebase/           ← existing (Postgres flat-store reference impl)
├── _starter_kit/       ← copy-paste template (ExampleStore.py)
└── kuzu/               ← NEW
    ├── __init__.py
    └── KuzuStore.py
```

Per coding rules: **one public class per file**, file named after the class
in PascalCase.

#### Step 2 — Implement the store class

Create `src/back/core/graphdb/kuzu/KuzuStore.py`.  Copy it from the starter
kit at `src/back/core/graphdb/_starter_kit/ExampleStore.py` and rename.
See [Section 5](#5-starter-kit) for details.

Key decisions:

1. **Query dialect**: If your engine speaks Cypher, set `supports_cypher = True`
   and `query_dialect = "cypher"`. Override the named query methods with native
   Cypher implementations and ship a matching `SWRLCypherTranslator` (the SQL
   translator stays the default for SQL engines).

2. **Graph model**: If your engine uses typed node/relationship tables, set
   `supports_graph_model = True` and implement `get_graph_schema()`. If it uses
   a flat triple table (like the shipped `LakebaseFlatStore`), leave it `False`.

3. **Reasoning translator**: Return the appropriate `SWRL*Translator` from
   `get_query_translator()`. For SQL engines, the default `SWRLSQLTranslator`
   works.

4. **Sync**: If your engine stores data as local files, implement
   `sync_to_remote()` and `sync_from_remote()` to archive/restore via
   `VolumeFileService`. Lakebase does not need this — the data lives in Postgres.

#### Step 3 — Create the package `__init__.py`

```python
# src/back/core/graphdb/kuzu/__init__.py
"""KuzuDB graph database backend."""
from back.core.graphdb.kuzu.KuzuStore import KuzuStore  # noqa: F401

__all__ = ["KuzuStore"]
```

#### Step 4 — Register the engine in `GraphDBFactory`

Edit `src/back/core/graphdb/GraphDBFactory.py`:

```python
def create(self, domain, settings=None, engine=None, engine_config=None):
    if engine is None:
        engine = "lakebase"
    if engine_config is None:
        engine_config = {}

    if engine == "lakebase":
        return self._create_lakebase(domain, settings, engine_config=engine_config)

    if engine == "kuzu":                      # ← NEW
        return self._create_kuzu(domain, settings, engine_config=engine_config)

    logger.warning("Unknown graph DB engine: %s", engine)
    return None

def _create_kuzu(self, domain, settings=None, *, engine_config=None):   # ← NEW
    """Instantiate a KuzuDB store."""
    try:
        from back.core.graphdb.kuzu.KuzuStore import KuzuStore
        base_name = (domain.info or {}).get("name", DEFAULT_GRAPH_NAME)
        version = getattr(domain, 'current_version', '1') or '1'
        db_name = f"{base_name}_V{version}"
        return KuzuStore(db_name=db_name, engine_config=engine_config)
    except ImportError as e:
        logger.warning("KuzuDB requires kuzu: %s", e)
        return None
    except Exception as e:
        logger.exception("Failed to create KuzuStore: %s", e)
        return None
```

> **`engine_config`** is a free-form JSON dict set by the admin in
> **Settings > Graph DB > Engine Configuration**. The factory reads it
> from `GlobalConfigService` and passes it to every engine constructor.
> Each engine defines its own keys (e.g. `host`, `port`, `credentials_path`).
> For Lakebase, recognised keys are `database`, `schema`, and `mode`.

Then update the availability check at the bottom of the file:

```python
try:
    from back.core.graphdb.kuzu.KuzuStore import KuzuStore  # noqa: F401
    GraphDBFactory.KUZU_AVAILABLE = True
except ImportError:
    GraphDBFactory.KUZU_AVAILABLE = False
```

#### Step 5 — Register the engine in the per-domain backend vocabulary

The backend *selection* is per-domain. Add your engine to the unified
vocabulary + mapping in `src/back/core/graphdb/GraphDBFactory.py`:

```python
GRAPH_BACKENDS = ("lakebase", "databricks", "neo4j", "kuzu")  # ← add here
```

Then map it inside `_resolve_triple_store_backend` / `_resolve_graph_engine`
so `graph_backend == "kuzu"` resolves to your engine.

#### Step 6 — Add the option to the per-domain dropdown

Edit `src/front/templates/partials/domain/_domain_information.html` — add an
`<option>` to the `#domainGraphBackend` select in the Knowledge Graph tab:

```html
<select class="form-select domain-editable" id="domainGraphBackend" ...>
    <option value="lakebase">Lakebase (Postgres)</option>
    <option value="databricks">Lakehouse</option>
    <option value="neo4j">Neo4j</option>
    <option value="kuzu">KuzuDB</option>        <!-- NEW -->
</select>
```

If the engine needs global *connection* config, add its section to
`src/front/templates/settings.html` (Settings → Back end) and persist it via
`graph_engine_config`.

#### Step 7 — Add the dependency

Add the engine's Python package to `pyproject.toml` as an optional dependency:

```toml
[project.optional-dependencies]
kuzu = ["kuzu>=0.4"]
```

Update `docs/development.md` with the new dependency (name, link, license).

#### Step 8 — Add tests

Create `tests/test_kuzu_store.py` following the patterns in
`tests/test_lakebase_flat_store.py`. At minimum, test:

- Store instantiation (with and without the library installed)
- `create_table` / `drop_table`
- `insert_triples` / `query_triples` / `count_triples`
- `table_exists` / `get_status`
- Capability flags (`supports_cypher`, `query_dialect`)

#### Step 9 — Update documentation

- Update this file if the architecture changes.
- Add an entry to `docs/development.md` in the Dependencies section.
- Add a Sphinx `.rst` file under `docs/sphinx/api/` for the new subpackage.
- Update the changelog.

---

### 4. Reference: Lakebase Engine Structure

The built-in Lakebase Postgres engine is the reference implementation:

```
graphdb/lakebase/
├── __init__.py           ← re-exports
├── LakebaseBase.py       ← GraphDBBackend subclass (connection pool, capabilities)
├── LakebaseFlatStore.py  ← Flat triple table (subject, predicate, object) on Postgres
├── SyncedTableManager.py ← Lakeflow synced-table orchestration (managed_synced mode)
└── models.py             ← Internal dataclasses
```

The flat store keeps the contract simple: a single Postgres table per
`(domain, version)` with a primary key on `(subject, predicate, object)` and
two write modes (`app_managed` via `COPY FROM STDIN`, `managed_synced` via
Lakeflow). A simpler engine can use a single store class and skip
`SyncedTableManager`.

---

### 5. Starter Kit

A ready-to-use starter kit lives at:

```
src/back/core/graphdb/_starter_kit/
├── README.md          ← usage instructions
├── __init__.py        ← package re-exports (template)
└── ExampleStore.py    ← full store class with every method stubbed
```

#### How to use

1. **Copy** the `_starter_kit/` directory into a new subpackage:

   ```bash
   cp -r src/back/core/graphdb/_starter_kit src/back/core/graphdb/kuzu
   ```

2. **Rename** `ExampleStore.py` to `KuzuStore.py` (matching your engine class).

3. **Find and replace** these placeholders throughout the copied files:

   | Placeholder | Replace with | Example |
   |-------------|-------------|---------|
   | `ExampleStore` | Your class name | `KuzuStore` |
   | `example_store` | Your module name (snake_case) | `kuzu_store` |
   | `example` | Your engine identifier (lowercase) | `kuzu` |
   | `Example` | Your engine display name | `Kuzu` |
   | `example_library` | The Python package to import | `kuzu` |

4. **Fill in** every `TODO` marker with your engine's native API calls.

5. **Continue from [Step 3](#step-3--create-the-package-__init__py)** above
   to register the engine in the factory, global config, and UI.

The `ExampleStore.py` template contains the full method contract with
detailed docstrings, grouped into sections:
- Capability flags (`supports_cypher`, `query_dialect`, …)
- Connection management (`get_connection`, `close`)
- Schema helpers (`get_node_table`, `get_graph_schema`)
- Sync to/from UC Volume (`sync_to_remote`, `sync_from_remote`)
- Reasoning support (`get_query_translator`)
- Core CRUD (`create_table`, `insert_triples`, `query_triples`, …)
- Named query overrides (commented stubs for non-SQL engines)

---

### 6. Checklist

Use this checklist to track your progress:

- [ ] Create `src/back/core/graphdb/<engine>/` package with `__init__.py`
- [ ] Implement `<EngineName>Store(GraphDBBackend)` with all abstract methods
- [ ] Override named query methods if your engine is non-SQL
- [ ] Register engine in `GraphDBFactory.create()` + add `_create_<engine>()` method
- [ ] Add engine name to `GRAPH_BACKENDS` in `GraphDBFactory.py` + map it in the resolvers
- [ ] Add `<option>` to `#domainGraphBackend` in `_domain_information.html`
- [ ] Add optional dependency to `pyproject.toml`
- [ ] Add tests in `tests/test_<engine>_store.py`
- [ ] Update `docs/development.md` (dependency table)
- [ ] Add Sphinx `.rst` under `docs/sphinx/api/`
- [ ] Update changelog

---

### 7. FAQ

**Q: Can I support both flat and graph models?**
Yes. Create a base class extending `GraphDBBackend`, then two subclasses
(flat and graph). Register the graph variant in the factory and have it
fall back to flat when the ontology is not available.

**Q: What if my engine is remote (e.g. Neo4j Aura)?**
The architecture supports it.  `get_connection()` can return a driver
connected to a remote endpoint.  `sync_to_remote` / `sync_from_remote` may
be no-ops if data is already remote.  `local_path()` should return `None`.

**Q: What about the reasoning engines?**
Reasoning engines use `GraphDBBackend.is_cypher_backend(store)` and the
capability flags to decide which translator to use.  If your engine speaks
Cypher, set the flag and return the appropriate translator from
`get_query_translator()`.  If SQL, the defaults work.

**Q: Which factory do I edit?**
Only `GraphDBFactory`.  It reads the engine from `GlobalConfigService` and
dispatches to the matching `_create_<engine>` constructor.

---

### 8. Lakebase build performance

When the active engine is **Lakebase**, the Knowledge Graph build keeps heavy
data on the Databricks side and never holds the full triple set inside the
FastAPI process.

#### Read side (Databricks SQL → app)

`SQLWarehouse.iter_rows(query, batch_size=5000)` opens a cursor on the
warehouse and yields dict rows in `fetchmany` batches. The build pipeline
uses it for the full rebuild (`SELECT subject, predicate, object FROM view`)
without ever materializing the full triple set inside the FastAPI process.

#### Write side (app → Lakebase Postgres)

`LakebaseFlatStore` exposes two streaming bulk paths used by the pipeline:

- `bulk_insert_iter(table, triple_iter, batch_size=5000)` — per batch:
  `CREATE TEMP TABLE _ob_copy_stage … ON COMMIT DROP`, `COPY FROM STDIN`
  (binary), then `INSERT INTO {phy} … SELECT FROM _ob_copy_stage ON CONFLICT
  DO NOTHING`. The temp table lives only inside the per-batch transaction
  (`conn.transaction()` is needed because the pool runs `autocommit=True`).
- `bulk_delete_iter(table, triple_iter, batch_size=5000)` — symmetrical
  `COPY` into `_ob_del_stage` followed by `DELETE FROM {phy} USING
  _ob_del_stage d WHERE …`.

`insert_triples` / `delete_triples` keep their public signatures and
delegate to the bulk iterator paths once the payload crosses
`_BULK_INSERT_THRESHOLD` / `_BULK_DELETE_THRESHOLD` (50 rows).

#### Pipeline gating

`_BuildPipeline._stream_triples_into_store` and
`_stream_triples_out_of_store` call `bulk_insert_iter` /
`bulk_delete_iter` when the store exposes them (Lakebase) and fall back to
materializing the iterator into a list for backends without a streaming
write path. `_start_background_archive` is a no-op for SQL-backed engines:
the Delta view + Postgres tables are the system of record, no archive is
pushed to the Volume.

---

### 9. Lakebase managed-synced mode (data plane only)

The factory still treats omitted ``sync_mode`` as ``app_managed`` (COPY
through the FastAPI process via ``iter_rows`` + ``COPY FROM STDIN``) so
workspaces that never saved Settings keep that path.

**Settings → Lakebase** proposes ``managed_synced``: bulk movement leaves
the app entirely. A Databricks **Lakeflow snapshot pipeline** keeps a
Postgres **synced table** in lock-step with the R2RML view, and the app
only orchestrates. Reasoning + cohort writes (small volumes) keep their
direct PG path through a writable **companion table**; readers see both
via a **UNION view** with the legacy table name, so SPARQL / KG search
code is unchanged. Triggered and Continuous Lakeflow schedules are not
offered: the source is a view and has no Change Data Feed.

#### Postgres layout per graph version

| Object | Owner | Purpose |
|--------|-------|---------|
| `g_<dom>_v<n>_sync` | Lakeflow (read-only) | Mirrors the source view via snapshot. |
| `g_<dom>_v<n>__app`  | App (read/write)     | Reasoning + cohort triples (datatype/lang aware). |
| `g_<dom>_v<n>`       | App DDL (`CREATE OR REPLACE VIEW`) | UNION view readers query (back-compat name). |

The synced side is restricted to `(subject, predicate, object)` — the union
view NULL-pads `datatype` / `lang` for those rows so the view exposes a
uniform 5-column shape.

#### Configuration

`graph_engine_config` accepts the following extra keys (all optional):

```jsonc
{
  "schema": "ontobricks_graph",         // fallback PG schema only when Registry has no Volume schema
  "database": "appdb",                   // PG database (overrides PGDATABASE)
  "sync_mode": "managed_synced",         // Settings proposes this; omitted still means app_managed at runtime
  "sync_table_mode": "snapshot",         // snapshot only (views have no CDF; triggered/continuous are coerced)
  "sync_timeout_s": 600,                  // wait deadline for a sync run
  "sync_uc_catalog": "main"              // UC catalog for synced table registration (optional override)
}
```

Sync UC naming is `<sync_uc_catalog or fallback>.<schema>.<table>` where **schema**
is resolved by ``resolve_lakebase_graph_schema``: **Registry Volume schema**
(``RegistryCfg.schema``) **always wins** when Settings → Registry resolves to a
non-empty triplet; otherwise ``graph_engine_config.schema`` (default
``ontobricks_graph``). Together with catalog fallback from the same Registry,
managed-synced tables register under the **same ``catalog.schema`` as the Volume**.

#### Unity Catalog Explorer — graph triples + synced table

Open **Catalog Explorer** at ``<catalog>.<registry_volume_schema>``: graph triple
tables, companion, union view, and the UC synced-table registration share that
schema segment once the store is constructed (see build log
``Managed-sync registers UC synced table at …``).

`validate_engine_config_keys` enforces the type and value constraints.

#### Build pipeline branch

`_BuildPipeline._apply_via_synced_pipeline(full=...)` replaces the row-level
ingest in synced mode:

1. Resolve the synced UC FQN as `<catalog>.<schema>.<base>_sync` where
   *catalog* is: ``graph_engine_config.sync_uc_catalog`` if set; otherwise
   ``resolve_sync_uc_fallback_catalog`` — optional deployment env
   ``ONTOBRICKS_SYNC_UC_CATALOG`` (legacy ``ONTBRICKS_*`` still honoured),
   then **Settings → Registry** UC catalog,
   then ``domain.delta.catalog`` (per-domain Delta catalog). This avoids
   registering the synced table under a personal/home UC catalog when the
   registry triplet points at the team catalog.
2. ``CREATE SCHEMA IF NOT EXISTS`` for that **Unity Catalog** ``catalog.schema``
   (SQL warehouse DDL). The synced-table API requires this metastore object;
   Postgres schema alone on Lakebase is not enough.
   See ``_sync_uc_schema.ensure_uc_schema_for_synced_table_fqn``.
3. `SyncedTableManager.ensure(...)` -- idempotent
   `WorkspaceClient.database.create_synced_database_table` call.
4. `LakebaseFlatStore.ensure_synced_companion(name)` — companion table only
   (must run before Lakeflow materializes the ``_sync`` table).
5. `SyncedTableManager.trigger_and_wait(...)` — calls `trigger_refresh`
   (`pipelines.start_update` with ``full_refresh=True``), then waits on the
   returned **update id** via ``pipelines.get_update`` until that Lakeflow run
   finishes (so we do not mistake a stale ``ONLINE`` synced-table status for the
   new build). If ``start_update`` was skipped because another update was already
   active, it falls back to ``wait_get_pipeline_idle`` plus synced-table polling.
   The update wait also polls the synced-table status and fails immediately on
   terminal states such as ``OFFLINE_FAILED``. On the next build, ``ensure``
   attempts to delete the broken registration; when the Lakebase control plane
   rejects deletion, it registers the replacement under the first available
   ``_b`` / ``_c`` / ``_d`` fallback suffix and returns that actual name to the
   remaining build steps.
6. `LakebaseFlatStore.ensure_synced_union_view(name)` — union view after the ``_sync`` table
   exists in Postgres (``CREATE OR REPLACE VIEW`` references the synced table).
7. On full rebuild, `TRUNCATE` the companion so reasoning + cohort start
   from a clean slate.

`_compute_diff_or_fall_through` short-circuits to `actual_mode = "full"` in
synced mode -- snapshot pipelines always rewrite the table, so a row-level
diff is wasted work. `_refresh_snapshot` is also skipped (Lakeflow is the
truth).

The scheduler mirrors this logic via `_apply_synced_pipeline` in
`back/objects/registry/scheduler.py`.

#### Read paths

`LakebaseFlatStore` separates the resolvers:

- `_writable_table_id(name)` -- companion in synced mode, legacy phy in
  app-managed mode (used by `insert_triples`, COPY insert, COPY delete,
  `delete_triples`).
- `_readable_table_id(name)` -- union view in synced mode (same identifier
  as the legacy phy in app-managed mode), used by `query_triples`,
  `iter_triples`, `count_triples`, `table_exists`, `get_status`.
- `optimize_table` vacuums only the writable companion in synced mode (the
  synced side is Lakeflow-managed).

#### Lifecycle

`LakebaseFlatStore.drop_table(name)` cascades in synced mode:
1. `DROP VIEW IF EXISTS` for the union view.
2. `DROP TABLE IF EXISTS` for the companion.
3. `SyncedTableManager.delete(uc_name, purge_data=True)` to remove the
   synced table from UC and its underlying PG table.

If the SDK or UC catalog is unavailable, the cascade still drops the PG
view + companion and logs a warning rather than aborting.

---

### Graph query optimizations

This guide describes the performance techniques currently used by OntoBricks
for interactive graph reads. It covers Explorer Preview, bounded graph
expansion, payload retrieval, backend-specific physical layouts, query
transport, safety limits, and operational refresh behavior.

The implementation supports three execution models:

- **Lakehouse** — SQL over Delta tables and views.
- **Lakebase** — SQL over Postgres tables and a reader-facing union view.
- **Neo4j** — native Bolt/Cypher traversal; the SQL companion tables described
  below do not apply.

#### 1. Read path overview

An Explorer search is split into three database/display phases:

1. **Preview** finds up to 500 typed seed entities.
2. **Expansion** performs a bounded breadth-first search (BFS) from the selected
   seeds.
3. **Payload fetch** retrieves the outgoing triples needed to render the
   discovered entities.

Lakehouse and Lakebase avoid repeatedly scanning the full
subject-predicate-object (SPO) relation by materializing graph-index
companions:

| Companion | Shape | Purpose |
|-----------|-------|---------|
| `_adj_out` | `(src, predicate, dst)` | Outgoing typed entity-to-entity hops |
| `_adj_in` | `(dst, predicate, src)` | Incoming typed entity-to-entity hops |
| `_entity_search` | `(uri, type_uri, label, uri_lc, label_lc)` | Preview (Inferred on) |
| `_entity_search_asserted` | same | Preview (Inferred off) |
| `_props` | `(subject, predicate, object)` | Expansion payload for typed subjects |

The companions are snapshots of the reader-facing graph at the moment of the
rebuild — this is a full snapshot replacement, **not** an incremental refresh.
Build and **Refresh cache** both trigger the same `rebuild_adjacency` backend
operation; Refresh cache is a lightweight alternative when the underlying source
data has not changed and only the indexes need to be brought up to date.
**Domain → Information → Backend → Search cache** skips that rebuild when
off; turning the switch back on leaves the domain in `pending_refresh` until
the next successful rebuild (live SPO until then). `cache=true` on a read
requires the companions and returns 400 if they are missing.

For Lakehouse (Delta) graphs, four or five companion tables are rebuilt
concurrently using a `ThreadPoolExecutor(max_workers=min(5, N))` per rebuild
invocation, where N is the number of companions scheduled. The fifth companion,
`entity_search_asserted`, is conditional: it is added only when both the
asserted-SPO table and the companion FQN resolve to non-empty strings. Each
worker submits its DDL statement through the Databricks SQL connector, which
maintains a thread-safe connection pool; each worker borrows a separate pooled
connection so the CTAS statements run on the warehouse in true parallel without
contention on the Python-side connection. (The Statement Execution API is used
only for real-time reads — not for the DDL/write rebuild path.) Per-companion elapsed time and total wall time are
logged at `INFO` level on completion.

Lakebase graphs rebuild adjacency in three sequential steps that remain serial:
DDL (table creation / schema changes) runs outside a transaction, then all
companion data is replaced inside a single Postgres transaction, then ANALYZE
runs in a separate non-transactional step. The transactional data-rebuild phase
keeps the reader-facing union view consistent and avoids partial reads during
the window; DDL and ANALYZE are intentionally excluded from the transaction.

#### 2. Materialized Delta read layer

In Lakehouse `table` materialization mode, the mapping result is copied into a
Delta `_data` table:

```sql
CREATE OR REPLACE TABLE <graph>_data
USING DELTA
CLUSTER BY (predicate, subject)
AS SELECT subject, predicate, object FROM <mapping_view>
```

The layout co-locates common predicate/subject filters. `OPTIMIZE` compacts
files and applies Liquid Clustering after builds. App-written inferred triples
remain in `_inferred`; the `_graph` view combines `_data` and `_inferred`.

In `view` materialization mode, `_data` remains a pass-through view. This avoids
copying source data but makes raw SPO queries re-run mapping SQL. The
physical graph-index companions are therefore especially important in this
mode.

Implementation:

- `src/back/core/graphdb/delta/materialize.py`
- `src/back/core/graphdb/delta/DeltaFlatStore.py`

#### 3. Entity Preview index

`_entity_search` reduces Preview from repeated SPO joins/scans to one bounded
lookup. It stores one deterministic row per typed entity:

- the subject URI;
- one type (`MIN(rdf:type)`);
- one label (`MIN(rdfs:label)`, or an empty string);
- normalized lowercase URI and label columns.

Preview filters directly on `type_uri`, `uri_lc`, and `label_lc`, then probes
with `LIMIT 501` (no warehouse `ORDER BY`). The extra row indicates that the
displayed 500-result list was capped without running a separate count query.
Rows are sorted by type then label in the app. Explorer Search defaults to
**Starts with**; **Contains** remains available, including programmatic
local-name lookup.

##### Lakehouse layout

The Delta table uses `CLUSTER BY (type_uri, label_lc)` plus a best-effort
Bloom filter on `label_lc` and `uri_lc`. Type-restricted prefix searches
benefit most; Bloom helps equality more than leading-wildcard `contains`.

##### Lakebase indexes

Lakebase creates:

- a primary key on `uri`;
- a btree on `type_uri`;
- `text_pattern_ops` btrees on `label_lc` and `uri_lc`;
- `pg_trgm` GIN indexes on `label_lc` and `uri_lc` when the extension can be
  created (skipped if the role cannot `CREATE EXTENSION`).

The pattern indexes accelerate equality and prefix (`starts with`) lookups.
GIN accelerates `contains` (`LIKE '%value%'`) when the planner selects it.

##### Asserted-only Preview

The optimized union table is used when Explorer **Inferred** is enabled.
When Inferred is off, Preview uses `_entity_search_asserted`, rebuilt from
`_data` (Lakehouse) or `_sync` (Lakebase). A missing companion falls back to
the asserted SPO relation; unrelated SQL errors are not swallowed.

Implementation:

- `src/back/core/graphdb/entity_search.py`
- `GraphDBBackend.find_preview_seeds`

#### 4. Query-shape and round-trip reductions

Several smaller optimizations avoid unnecessary warehouse work:

- Entity metadata fetch requests `rdf:type` and `rdfs:label` in one
  predicate-`IN` query, then splits the rows in Python. This replaces two
  scans/round trips through the graph union view.
- Top-node analytics applies `LIMIT` before joining labels and types, so
  metadata joins touch only the returned nodes.
- Delta relation existence uses `SELECT 1 ... LIMIT 1`, not `COUNT(*)`.
  Missing-relation errors map to false; authorization and transport failures
  still propagate.
- Lakebase relation existence queries `information_schema.tables` and
  `information_schema.views` with `UNION ALL ... LIMIT 1`.
- Lakebase URI-alias expansion groups `%/<local-id>` patterns by suffix length
  and emits `RIGHT(subject, length) = ANY(ARRAY[...])`. Query size therefore
  scales with the few distinct suffix lengths rather than hundreds or
  thousands of `OR subject LIKE ...` clauses.

Implementations:

- `GraphDBBackend.get_entity_metadata`
- `GraphDBBackend.get_top_nodes_by_degree`
- `DeltaFlatStore.table_exists`
- `LakebaseFlatStore.table_exists`
- `LakebaseFlatStore.find_subjects_by_patterns`

#### 5. Bidirectional adjacency

The adjacency tables contain only typed entity-to-entity relationships.
`rdf:type`, `rdfs:label`, literal-valued properties, and links to untyped
objects are excluded from hop discovery.

Two physical directions avoid an `OR` over subject and object for every BFS
step:

```sql
SELECT dst FROM <graph>_adj_out WHERE src IN (...)
UNION ALL
SELECT src FROM <graph>_adj_in WHERE dst IN (...)
```

Physical layout:

| Backend | `_adj_out` | `_adj_in` |
|---------|------------|-----------|
| Lakehouse | `CLUSTER BY (src, predicate)` | `CLUSTER BY (dst, predicate)` |
| Lakebase | primary key `(src, predicate, dst)` + btree `(src, predicate)` | primary key `(dst, predicate, src)` + btree `(dst, predicate)` |

This makes both outgoing and incoming exploration key-based rather than a scan
of the SPO union.

#### 6. Single-statement bounded BFS

Lakehouse and Lakebase perform multi-level expansion and payload retrieval in
one SQL statement. Each BFS level reads the adjacency tables, removes entities
already visited, and applies the configured entity bound.

The dialect-specific visited-set operation is:

- Spark: `LEFT ANTI JOIN`;
- Postgres: `WHERE NOT EXISTS`.

Seed URIs are deduplicated before SQL generation. Candidate levels use
`UNION ALL`; deduplication happens once at the level boundary rather than on
every branch.

The query probes `max_entities + 1` and `max_triples + 1`. This detects
truncation without unbounded counting or transferring an oversized graph.

Compatibility fallbacks retain the same bounds:

1. If adjacency tables are unavailable, Delta generates fixed-depth SPO CTEs;
   Lakebase derives temporary `adj_out`/`adj_in` CTEs from the SPO relation.
2. A backend without single-statement expansion uses the legacy Python loop,
   querying one frontier at a time and fetching subject triples in batches.
   That path also has a 120-second payload-fetch budget.

Implementation: `src/back/core/graphdb/adjacency.py`.

#### 7. Property companion for payload fetch

Adjacency makes hop discovery fast, but it intentionally omits labels, types,
literals, and some IRI-valued properties. `_props` closes the remaining
performance gap by materializing every outgoing triple whose subject is a
typed instance.

After BFS, the final query joins discovered entities to `_props`:

```sql
SELECT p.subject, p.predicate, p.object
FROM <graph>_props p
JOIN discovered_entities e ON e.entity = p.subject
LIMIT <max_triples + 1>
```

Physical layout:

- Lakehouse: `CLUSTER BY (subject)` plus best-effort `OPTIMIZE`.
- Lakebase: btree on `subject`, followed by `ANALYZE`.

If `_props` is absent on a graph built before this optimization, the backend
retries the same adjacency expansion with the reader-facing SPO relation for
the final payload. Only a missing-table error triggers this compatibility
fallback. The process remembers that negative result so later expansions skip
the guaranteed-failing companion query. A successful Build or **Refresh cache**
clears the negative after rebuilding `_props`.

Depth-zero expansion bypasses BFS CTEs and filters the payload relation directly
with `WHERE subject IN (...)`. Spark BFS payload joins broadcast the bounded
entity set instead of shuffling it with the graph relation.

Implementation: `src/back/core/graphdb/props.py`.

#### 8. Aggregate and analysis pushdown

The graph overview combines total triples, distinct subjects/predicates, type
assertions, and labels in one aggregate query.

Delta overrides exact `COUNT(DISTINCT ...)` with Spark
`approx_count_distinct(...)` for the overview. This avoids a full distinct
shuffle on multi-million-row triple tables; the approximate value is
appropriate for at-a-glance statistics. Lakebase keeps exact Postgres
distinct counts.

Filtered graph analytics pushes class and predicate scope into SQL through
`query_triples_for_analysis`. For class filters, it loads selected instances
plus their direct neighbors and metadata instead of transferring the complete
triple store and filtering only in Python. Non-SQL engines retain the
in-process fallback.

Implementation:

- `GraphDBBackend.get_aggregate_stats`
- `DeltaFlatStore._distinct_count_expr`
- `GraphDBBackend.query_triples_for_analysis`

#### 9. Snapshot rebuild and statistics

All four companions share one refresh clock:

- full Knowledge Graph Build;
- manual **Refresh cache**;
- reasoning materialization when triples change;
- cohort writes that change graph edges/properties.

Graph Cache Refresh can also run as a standalone Scheduler task for a selected
domain and version. It invokes the same full companion-index rebuild as the
interactive **Refresh cache** action. The task is available for Lakehouse and
Lakebase graphs; Neo4j does not use these companions.

Lakebase performs `TRUNCATE` and `INSERT` for all companions in one
transaction, then runs `ANALYZE` so the Postgres planner sees current
statistics.

Lakehouse uses sequential `CREATE OR REPLACE TABLE` operations followed by
best-effort `OPTIMIZE`. The replacements are not a cross-table atomic swap;
avoid reading during a Build/Refresh when a same-instant view of all companions
is required.

Freshness differs by mode:

- **Lakehouse table mode:** source-table changes require a full Build because
  `_data` is a snapshot.
- **Lakehouse view mode:** Refresh cache re-runs the live mapping view and
  captures current source rows in the companions.
- **Lakebase:** Refresh cache captures the current reader-facing union view.

Post-write maintenance also preserves read performance:

- Delta applies best-effort `OPTIMIZE` to the inferred and graph-index tables.
- Lakebase runs `VACUUM ANALYZE` on app-managed bulk data and writable
  companions (only the writable companion in managed-synced mode).
- Large Lakebase mutations use `COPY FROM STDIN` staging plus set-based
  `INSERT ... ON CONFLICT DO NOTHING` or `DELETE ... USING`; small mutations
  avoid staging overhead.

#### 10. GraphQL and MCP find read the same companions

`query_graphql`'s typed list resolver (`SchemaMetadata._query_subjects` →
`GraphDBBackend.find_subjects_by_type`) and its nested-field triple loader
(`SchemaMetadata._load_triples` → `GraphDBBackend.get_triples_for_subjects`)
read `_entity_search` and `_props` when ready, instead of the reader-facing
SPO relation. `search` on the GraphQL list resolver then matches only
`rdfs:label`/URI (`_entity_search`'s `label_lc`/`uri_lc`) — the same fields
Explorer Preview searches — rather than every literal predicate.

MCP's `describe_entity` and the shared `/triples/find` route
(`DigitalTwin.find_triples_bfs` → `GraphDBBackend.bfs_traversal`) seed from
`_entity_search` and walk `_adj_out`/`_adj_in` instead of a `WITH RECURSIVE`
scan of the SPO relation, when both companions are ready. Hop endpoints then
require both sides of an edge to have an `rdf:type` assertion, matching
Explorer's own adjacency restriction; a companion-missing graph (pre-build,
pre-refresh) or a missing-table error mid-query falls back to the exact
`WITH RECURSIVE` SQL used before this change.

`/triples/find` response pagination is deterministic on `(subject, predicate,
object)` and reports exact `total` plus additive `has_more` for downstream
formatters/clients; truncation hints should follow `has_more` rather than
deriving from `total > page_size`.

`entity_type` keeps two matching modes that already existed before this
change and are preserved exactly: GraphQL/Preview take a full class URI
(`type_uri` equality); MCP's `describe_entity`/`/triples/find` take a bare
local name (`type_uri` suffix match, `#name` / `/name`).

Neo4j is unaffected — its `find_subjects_by_type`, `get_triples_for_subjects`,
and `bfs_traversal` overrides use native Cypher and never reach these
companion branches.

Implementation:

- `back/core/graphdb/entity_search.py`: `entity_search_uri_search_sql`,
  `entity_search_seed_sql`, `_entity_search_text_clause`,
  `is_missing_relation_error`.
- `back/core/graphdb/adjacency.py`: `seeded_bfs_sql`.
- `back/core/graphdb/GraphDBBackend.py`: `find_subjects_by_type`,
  `get_triples_for_subjects`, `bfs_traversal`.

#### 11. Query bounds and cancellation

Graph reads are protected independently from long-running build operations:

- default graph statement timeout: 60 seconds;
- configurable range: 5–900 seconds;
- default graph-chat result cap: 10,000 triples;
- configurable result-cap range: 100–100,000 triples;
- Explorer Preview, entity, triple, and depth bounds are applied before result
  serialization.

Lakebase scopes `SET statement_timeout` to the borrowed pooled connection and
resets it in `finally`, preventing the timeout from leaking into later writes.

Lakehouse passes the same graph timeout to the SQL service. Thrift connections
use warehouse `STATEMENT_TIMEOUT`; Statement Execution API requests use a
client deadline and cancel timed-out statements.

Implementation: `src/back/core/query_limits.py`.

#### 12. Lakehouse/RT query transport

Build and graph reads can use different warehouses:

- Build and graph-index DDL always use the Build SQL Warehouse over Thrift.
- Interactive reads may use a dedicated Lakehouse/RT query warehouse.

Inside Databricks Apps, RT reads use the Statement Execution API with
`JSON_ARRAY` and `INLINE` results. This avoids Kernel CloudFetch downloads from
external object storage, which are unavailable from the App runtime. Internal
chunk links are followed only on the workspace host; external links and
truncated responses are rejected.

Local development keeps the SQL connector/Kernel path when configured.

Implementation:

- `src/back/core/databricks/StatementExecutionWarehouse.py`
- `src/back/core/databricks/DatabricksClient.py`

The regular SQL Warehouse service also retries transient transport failures
up to three times with bounded backoff and a fresh connection. Retry
classification covers cold-start/auto-scaling 5xx responses and stale pooled
connections; SQL errors and unsupported-protocol configuration errors fail
immediately.

#### 13. Neo4j native indexes and traversal

Neo4j does not mirror the SQL graph-index companions. Each graph gets a marker
label and a uniqueness constraint on node `uri`; Neo4j uses the constraint's
backing index for identity lookup and `MERGE`.

Entity types are represented as node labels and graph relationships are real
Neo4j relationships. Search can therefore match a graph marker plus a class
label directly, while expansion uses native variable-length, bidirectional
Cypher paths rather than SQL BFS over SPO rows. Seed lists and search values
are passed as parameters, and result limits are pushed into Cypher.

Implementation:

- `src/back/core/graphdb/neo4j/Neo4jWriteOps.py`
- `src/back/core/graphdb/neo4j/Neo4jReadOps.py`

#### 14. Connection and transfer efficiency

Lakebase uses a connection pool rather than opening a Postgres connection for
each graph query. Expansion is returned in one result set instead of one
request per BFS level.

The optimized Preview and expansion paths avoid preliminary table-existence
round trips. They optimistically query companion tables and fall back only
when the database reports a missing relation. This reduces steady-state
latency while preserving compatibility with graphs built by older versions.

Repeated graph-status and artifact-existence probes use the Digital Twin
session cache. Only successful, known results are cached; transient failures
are not stored as false negatives. This cache reduces page-level metadata
queries but does not cache Explorer neighborhoods or query result sets.

#### 15. Browser-observed timings

Explorer records the latest completed search timing as:

- Preview request;
- Expansion request;
- Display/render;
- Total active search time.

The total excludes user dwell in the seed-selection dialog. This split helps
distinguish database search cost, traversal/payload cost, and browser rendering
cost before choosing the next optimization.

On 2026-09-16, a BIGCustomers Lakehouse graph built before `_props` measured:

- Preview: about 870 ms for both Starts with and Contains on one selective term;
- Expansion depth 0 / 1 / 2 / 3: 10.85 s / 5.84 s / 2.90 s / 3.71 s;
- depth 3 returned 14,594 triples, while request transfer added only 2–3 ms.

The missing `_props` error and SPO retry occurred on every expansion. These
measurements prioritize companion refresh and expansion SQL over more Preview
work. Re-run the same comparison after `_props` is present before drawing
conclusions about rendering or payload changes.

After **Refresh cache** rebuilt `_props`, the same three-run benchmark measured
1.48 s / 1.83 s / 1.69 s for depths 0 / 1 / 2. That is approximately 86%,
69%, and 42% faster than the stale-companion path, respectively. No missing
`_props` errors appeared. The first request at each shape remained slower,
consistent with warehouse/cache warm-up.

Implementation: `src/front/static/query/js/query-sigmagraph.js`.

#### 16. Concrete operating example

After a Build or after reasoning/cohort writes:

1. Open **Knowledge Graph → Build**.
2. Run **Refresh cache** if a full Build is not required.
3. Confirm an old graph no longer logs a missing `_props` companion during
   expansion before tuning Preview.
4. In Explorer Search, select an entity type when possible.
5. Prefer **Exact** or **Starts with** over **Contains** for selective indexed
   Preview searches.
6. Keep depth and entity/triple caps as low as the use case allows.
7. Open the search timing details. Optimize Preview if Preview dominates;
   optimize hop/payload paths only if Expansion dominates.

For a Lakehouse table-mode domain, use a full Build when source tables changed;
Refresh cache alone only reindexes the existing `_data` snapshot.

#### 17. Backend summary

| Technique | Lakehouse | Lakebase | Neo4j |
|-----------|-----------|----------|-------|
| Materialized/clustered SPO | Delta table mode | Native Postgres storage | Native graph |
| `_entity_search` | Delta, clustered by type | Btree/pattern indexes | Native query |
| `_adj_out` / `_adj_in` | Delta, clustered by endpoint | Endpoint btrees | Native traversal |
| `_props` | Delta, clustered by subject | Subject btree | Native properties |
| Single-statement bounded BFS | Spark SQL | Postgres SQL | Native Cypher path |
| Statement timeout | Warehouse/SEA | Postgres | Driver/query behavior |
| Dedicated RT read transport | Optional | N/A | N/A |

#### 18. Planned, not yet implemented

Application-side Preview sorting, Starts-with default, asserted search,
Lakebase trigram indexes, and Lakehouse search clustering/Bloom are shipped.

Do not implement type columns on adjacency, N-hop materialization, integer ID
interning, CSR arrays, visualization-only payloads, or process-local
neighborhood caches without new timings after `_props` is present. Predicate
layout changes are useful only for a measured predicate-filtered traversal.

See:

- `docs/superpowers/plans/2026-09-15-search-transversal-next.md`
- `docs/superpowers/plans/2026-09-16-expansion-fallback-latency.md`

#### 19. Parallel companion rebuild — measured performance

On 2026-09-17, a **Refresh cache** on the BIGCustomers Lakehouse domain
(~1.35 M entity-search rows, ~1.70 M adjacency rows, ~7.64 M props rows) was
timed before and after the parallel implementation shipped in `e5e6ced7`.
The serial baseline was recorded on 2026-09-16; the parallel run on 2026-09-17.
Exact data equality between the two runs was not recorded — the speedup is
**indicative rather than a controlled benchmark** but represents the same named
domain under comparable load.

| Run | Wall time | Method |
|-----|-----------|--------|
| Prior serial baseline | 128.62 s | Sequential rebuild |
| Parallel (N=5 workers) | **45.0 s** | `ThreadPoolExecutor(max_workers=min(5, 5))` |

N=5 because `entity_search_asserted` resolved (asserted-SPO table present).
**Indicative speedup: ~2.86× / ~65 % wall-time reduction.**

Per-companion log lines from the parallel run:

| Companion | Elapsed |
|-----------|---------|
| `adj_in` | 37.3 s |
| `adj_out` | 38.6 s |
| `entity_search_asserted` | 44.4 s |
| `props` | 44.5 s |
| `entity_search` | 45.0 s |

The adjacency tables complete first (37–38 s); the three heavier companions
finish together at the 44–45 s mark. The critical path is `entity_search` at
45 s — exactly what the full wall time measures. No warehouse admission queuing
was observed; all five CTAS statements ran in parallel because `entity_search_asserted`
was present (N=5).

Post-rebuild smoke checks confirmed `adjacency_ready = True`, correct row
counts in all five companions, successful entity search (label lookup), and
successful expansion (depth 1, 6 entities, 30 triples from one seed).

The `DROP_COMMAND_TYPE_MISMATCH` warnings that appear when companions already
exist as tables (rather than legacy views) are benign and handled by the
existing `materialize.drop_relation` guard. The Bloom filter warning on
`entity_search` is a warehouse-tier limitation and does not affect search
correctness.

Implementation: `src/back/core/graphdb/delta/DeltaFlatStore.py`
(`rebuild_adjacency`, `_rebuild_adjacency_table`, `_rebuild_props_table`,
`_rebuild_entity_search_table`).

#### Related documentation

- [Graph DB integration](#engine-integration)
- [Architecture](architecture.md)
- [Lakebase graph DB](#lakebase-graph-store)
- [User guide](user-guide.md)
