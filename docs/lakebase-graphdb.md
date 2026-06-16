# Lakebase as Graph DB — OntoBricks Reference

OntoBricks ships with **one built-in graph database engine: Lakebase Postgres**.
This document covers everything you need to provision, configure, and operate it —
from the API gotchas that trip up fresh installs to the Postgres schema layout and
troubleshooting runbook.

For the developer guide on *adding a new engine*, see `docs/graphdb-integration.md`.

---

## Table of Contents

1. [Architecture overview](#1-architecture-overview)
2. [Prerequisites](#2-prerequisites)
3. [Provisioning the Lakebase project](#3-provisioning-the-lakebase-project)
4. [Configuring the engine in the app](#4-configuring-the-engine-in-the-app)
5. [Write modes](#5-write-modes)
6. [Postgres schema layout](#6-postgres-schema-layout)
7. [Scripts reference](#7-scripts-reference)
8. [Permissions bootstrap](#8-permissions-bootstrap)
9. [Digital Twin build — step by step](#9-digital-twin-build--step-by-step)
10. [Troubleshooting](#10-troubleshooting)

---

## 1. Architecture overview

```
┌─────────────────────────────────────────────────┐
│  OntoBricks (FastAPI)                           │
│                                                 │
│  TripleStoreFactory                             │
│    └─ GraphDBFactory.create(engine="lakebase")  │
│         └─ LakebaseFlatStore                    │
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

The Graph DB engine is selected under **Settings → Graph DB** and is always `lakebase` in the default bundle.

---

## 2. Prerequisites

### 2.1 — Databricks workspace requirements

| Requirement | Notes |
|-------------|-------|
| Databricks Apps enabled | Needed to run OntoBricks as a platform app |
| SQL Warehouse | Standard or Serverless; bound as the `sql-warehouse` resource |
| Unity Catalog | A catalog + schema for the registry and triplestore views |
| Lakebase feature | Must be enabled on the workspace (contact workspace admin if absent) |
| `psql` on PATH | Required by `scripts/bootstrap-lakebase-perms.sh` (`brew install libpq && brew link --force libpq` on macOS) |

### 2.2 — Python dependencies

Lakebase support is an **optional extra** since v0.4.0:

```bash
# Local development
uv sync --extra lakebase

# Or pip
pip install ".[lakebase]"
```

This installs `psycopg[binary]>=3.2.0` and `psycopg-pool>=3.2.0`.
The deployed `app.yaml` already includes `--extra lakebase` in the startup command.

### 2.3 — Critical API distinction

Lakebase has **two project-creation APIs** with different capabilities:

| API | Endpoint | Synced Tables compatible? |
|-----|----------|--------------------------|
| New (Autoscaling only) | `POST /api/2.0/postgres/projects` | **NO** |
| Old (Autoscaling + Provisioned) | `POST /api/2.0/database/instances` | **YES** |

The **Databricks UI "New project" button** calls the new API and produces a project
that is **incompatible** with `POST /api/2.0/database/synced_tables` (used by the
Digital Twin `managed_synced` build mode). Always use `scripts/setup-lakebase.sh`
to provision the project.

---

## 3. Provisioning the Lakebase project

### 3.1 — Create with `setup-lakebase.sh`

Run once per workspace before the first deploy:

```bash
./scripts/setup-lakebase.sh --name ontobricks-demo --capacity CU_2
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

### 3.1b — One-click provisioning from Settings (in-app alternative)

Admins can provision a graph DB end-to-end from the UI instead of running the
two scripts by hand. In **Settings → Lakebase → Connection** tab there is a
**"Create graph DB from scratch"** card: fill in the instance/project name,
compute capacity, branch, Postgres database, graph schema, and the MCP app
name, then click **Create graph DB**. The action runs as an async job (a
progress bar + per-step log update live, polling `GET /tasks/{id}` like a
Digital Twin build) and performs the same flow as
`scripts/setup-lakebase.sh` + `scripts/bootstrap-lakebase-perms.sh`:

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
> remain the documented fallback — re-run `scripts/bootstrap-lakebase-perms.sh`
> after the apps have connected once.

### 3.2 — After the script

Copy the printed `db-…` segment into `scripts/deploy.config.sh`:

```bash
DEFAULT_LAKEBASE_DATABASE_RESOURCE_SEGMENT="db-xxxx-xxxxxxxxxx"
```

You can also look it up at any time:

```bash
databricks postgres list-databases \
  "projects/<project-name>/branches/production" -o json \
  | python3 -c "import sys,json; [print(d['name']) for d in json.load(sys.stdin)]"
```

### 3.3 — Name reservation gotcha

If you delete a Lakebase project from the new `/postgres/projects` API, the name
may remain **ghost-reserved** in the `/database/instances` namespace for an extended
period (15 minutes to hours). If `setup-lakebase.sh` fails with
`Instance name is not unique`, choose a different name and update `deploy.config.sh`.

---

## 4. Configuring the engine in the app

### 4.1 — UI configuration

Go to **Settings → Graph DB → Engine Configuration** and enter a JSON object:

```jsonc
{
  "schema": "ontobricks_graph",    // Postgres schema for graph tables (default)
  "database": "ontobricks_demo",   // Postgres database (overrides PGDATABASE)
  "sync_mode": "app_managed"       // or "managed_synced" — see §5
}
```

The engine selector should show `lakebase`. If it shows empty, reload the page.

### 4.2 — All `graph_engine_config` keys

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `schema` | string | `ontobricks_graph` | Postgres schema for graph triple tables. Overridden by the Registry Volume schema when Settings → Registry resolves a non-empty triplet. |
| `database` | string | injected `PGDATABASE` | Postgres database name — overrides the Apps-injected value. |
| `sync_mode` | string | `app_managed` | Write mode: `app_managed` (direct COPY) or `managed_synced` (Lakeflow pipeline). |
| `sync_table_mode` | string | `snapshot` | Lakeflow pipeline type: `snapshot`, `triggered`, or `continuous`. Only used in `managed_synced`. |
| `sync_timeout_s` | int | `600` | Max seconds to wait for a Lakeflow sync run to complete. |
| `sync_uc_catalog` | string | *(auto-detected)* | UC catalog for synced table registration. Auto-detected from Registry settings / `ONTOBRICKS_SYNC_UC_CATALOG` / `domain.delta.catalog`. |

### 4.3 — Environment variables (deployed app)

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

### 4.4 — `deploy.config.sh` variables

These drive the DAB deployment (edit before `make deploy`):

| Variable | Description |
|----------|-------------|
| `DEFAULT_LAKEBASE_PROJECT` | Lakebase project name (final segment of `projects/<id>`) |
| `DEFAULT_LAKEBASE_BRANCH` | Branch (e.g. `production`) |
| `DEFAULT_LAKEBASE_DATABASE_RESOURCE_SEGMENT` | `db-…` resource id from `list-databases` |
| `DEFAULT_LAKEBASE_REGISTRY_DATABASE` | Postgres datname the registry schema lives in |
| `DEFAULT_LAKEBASE_REGISTRY_SCHEMA` | Postgres schema for the registry (mirrors `LAKEBASE_SCHEMA` in `app.yaml`) |

> `deploy.config.sh` is **registry-scoped**. The graph DB schema/database
> are configured in-app (`Settings → Graph DB` → `graph_engine_config`)
> and may live in a different Lakebase project — they are not deploy vars.

---

## 5. Write modes

### 5.1 — `app_managed` (default)

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

### 5.2 — `managed_synced` (Lakeflow)

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
- The Lakebase project must be provisioned via `scripts/setup-lakebase.sh`
  (provisioned instance, not autoscaling-only) — see §2.3.
- The app SP needs `CAN_USE` on the Lakebase database instance — applied by
  `scripts/bootstrap-lakebase-perms.sh`.
- The UC schema for the synced table must exist before the first build
  (`CREATE SCHEMA IF NOT EXISTS` is run automatically by the build pipeline).

---

## 6. Postgres schema layout

### 6.1 — Schemas

OntoBricks uses up to three Postgres schemas in the same Lakebase project:

| Schema | Default name | Created by | Purpose |
|--------|-------------|-----------|---------|
| Registry | `ontobricks_registry` | `Settings → Registry → Initialize` | Project metadata, domain configs, schedule runs |
| Graph DB | `ontobricks_graph` | First Digital Twin Build | Per-domain triple tables (and views in `managed_synced`) |
| Sync | *(UC registry schema segment)* | First Lakeflow snapshot | Auto-created by Lakeflow; mirrors the UC `<schema>` segment |

### 6.2 — Objects per graph version

Both `app_managed` and `managed_synced` use the same **3-object layout** per
domain version. The difference is who writes to the `*_sync` table.

| Object | Owner | Naming | Description |
|--------|-------|--------|-------------|
| Sync table | App (`app_managed`) / Lakeflow (`managed_synced`) | `g_<domain>_v<n>_sync` | Bulk warehouse data; `(subject, predicate, object, datatype, lang)`. App writes via `COPY FROM STDIN`; Lakeflow writes via snapshot pipeline. |
| Companion table | App (read/write) | `g_<domain>_v<n>__app` | Reasoning / cohort / materialise triples; `(subject, predicate, object, datatype, lang)` |
| UNION view | App DDL | `g_<domain>_v<n>` | `SELECT … FROM _sync UNION ALL SELECT … FROM __app`; exposes the back-compat 5-column shape |

All SPARQL queries and graph traversal operations target the back-compat name
`g_<domain>_v<n>` — no downstream code is aware of which mode is active.

### 6.3 — Drop cascade (both modes)

`LakebaseFlatStore.drop_table(name)` removes all three objects in order:
1. `DROP VIEW IF EXISTS g_<domain>_v<n>` (UNION view)
2. `DROP TABLE IF EXISTS g_<domain>_v<n>__app` (companion)
3. `app_managed`: `DROP TABLE IF EXISTS g_<domain>_v<n>_sync` (sync table)
   `managed_synced`: `SyncedTableManager.delete(uc_name, purge_data=True)` — removes the UC synced-table registration and the underlying Postgres `_sync` table

---

## 7. Scripts reference

### `scripts/setup-lakebase.sh`

Provisions a Lakebase project via `POST /api/2.0/database/instances` (synced-tables-compatible).

```bash
# Basic usage
./scripts/setup-lakebase.sh --name my-project --capacity CU_2

# Dry-run to preview
./scripts/setup-lakebase.sh --name my-project --dry-run

# Custom profile
./scripts/setup-lakebase.sh --name my-project --profile prod-workspace
```

**Outputs:** prints the `db-…` resource id that goes into `deploy.config.sh > DEFAULT_LAKEBASE_DATABASE_RESOURCE_SEGMENT`.

### `scripts/bootstrap-lakebase-perms.sh`

Grants the app service principals the Postgres and control-plane permissions
they need to operate. **Idempotent — safe to run repeatedly.**

```bash
# Registry schema
scripts/bootstrap-lakebase-perms.sh \
  -i ontobricks-demo2 -b production \
  -d ontobricks_demo -s ontobricks_registry \
  -a ontobricks-030 -a mcp-ontobricks

# Graph DB schema (run after first Build)
scripts/bootstrap-lakebase-perms.sh \
  -i ontobricks-demo2 -b production \
  -d ontobricks_demo -s ontobricks_graph \
  -a ontobricks-030 -a mcp-ontobricks

# Sync schema (managed_synced only — run after first Lakeflow snapshot)
scripts/bootstrap-lakebase-perms.sh \
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
| `CAN_USE` (control-plane) | Lakebase instance | Allows the SP to call Lakebase APIs (e.g. `synced_tables`) |
| `CAN_USE` (autoscaling API) | Lakebase project | Belt-and-suspenders for autoscaling path |
| `USAGE` + `CREATE` | Postgres schema | Let the SP create tables/views in the schema |
| `SELECT/INSERT/UPDATE/DELETE` | All existing tables | DML on current objects |
| `USAGE/SELECT/UPDATE` | All existing sequences | Required for `bigserial` PKs |
| `ALTER DEFAULT PRIVILEGES` | Schema | Future tables/sequences inherit the same grants |
| `ALL PRIVILEGES` (UC catalog) | Unity Catalog catalog | Read back synced tables; only granted when `-c` flag passed |

### `scripts/deploy.sh`

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

## 8. Permissions bootstrap

### 8.1 — Order of operations

Lakebase schemas are created lazily (by app actions), so grants must follow creation:

```
Step 1:  make deploy
         → CAN_USE on instance applied immediately (before schema exists)
Step 2:  Open app → Settings → Registry → Initialize
         → Creates 'ontobricks_registry' schema in Postgres
Step 3:  make bootstrap-lakebase  (or make deploy again — idempotent)
         → Applies USAGE + DML on 'ontobricks_registry' schema
Step 4:  Build a Digital Twin (Settings → Digital Twin → Build)
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

### 8.2 — Verify permissions

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

## 9. Digital Twin build — step by step

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

## 10. Troubleshooting

### `Database instance is not found`

```
Failed to create synced table …: Database instance is not found.
```

**Cause:** The Lakebase project was created via the new autoscaling API
(`/postgres/projects`) which is **not** listed in `/database/instances`.
The Synced Tables API only accepts provisioned instance names.

**Fix:**
1. Delete the old project from the UI.
2. Re-create it with `scripts/setup-lakebase.sh`.
3. Update `DEFAULT_LAKEBASE_DATABASE_RESOURCE_SEGMENT` in `deploy.config.sh`.
4. `make deploy`.

If the name is reserved (`Instance name is not unique`), choose a different name —
deleted names can stay ghost-reserved for hours.

---

### `The user is not authorized … assign 'Can Use' or 'Can Manage'`

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

### `"<view>" is not a view`

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

### `_sync table '…_b' not found in Postgres`

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

### `Lakebase connection failed: database "ontobricks_registry" does not exist`

**Cause:** The registry database or schema has not been created yet.
This is expected on a fresh deployment.

**Fix (in order):**
1. Open the deployed app.
2. Go to **Settings → Registry → Initialize**.
3. After initialisation, run `make bootstrap-lakebase` (or `make deploy`).

---

### `logicalDatabaseName must be defined when creating synced table in a standard catalog`

**Cause:** The `graph_engine_config.database` key is missing or the
`PGDATABASE` env var was not injected by the Apps runtime (postgres resource
not bound).

**Fix:**
1. Confirm the `postgres` resource binding in the Databricks Apps UI.
2. Add `"database": "<datname>"` to the `graph_engine_config` JSON.
3. Restart the app.

---

### `Must specify either database instance name or both database project and branch`

**Cause:** The synced table registration call is missing both the instance name
and the project+branch pair. This means `lakebase_project` or
`lakebase_database_resource_segment` is empty in `deploy.config.sh`.

**Fix:**
1. Run `databricks postgres list-databases "projects/<project>/branches/<branch>" -o json`.
2. Copy the `name` field (looks like `db-xxxx-xxxxxxxxxx`) into
   `DEFAULT_LAKEBASE_DATABASE_RESOURCE_SEGMENT` in `deploy.config.sh`.
3. `make deploy`.

---

### Settings → Graph DB: catalog list is empty

The UC catalog dropdown uses the configured SQL Warehouse. If the warehouse is not
yet saved to the global config, the dropdown falls back to `DATABRICKS_SQL_WAREHOUSE_ID`
from the environment. If all fallbacks fail, the "Configure a SQL warehouse first"
message appears.

**Fix:**
1. Go to **Settings → Databricks** and save the SQL Warehouse.
2. If it still fails, verify `DATABRICKS_SQL_WAREHOUSE_ID` / `DATABRICKS_SQL_WAREHOUSE_ID_DEFAULT`
   are set in the Apps resource binding or `.env`.

---

### Delete asset button does nothing (Settings → Graph DB)

**Cause:** `window.confirm()` is suppressed inside the Databricks Apps iframe.

**Fix (applied since v0.4.1):** The delete flow uses a Bootstrap modal
instead of `window.confirm()`. If you see this on an older deployment,
`make deploy` to pick up the fix.

---

## Quick-start checklist (Lakebase Graph DB)

```
[ ] 1. Create Lakebase project:
        ./scripts/setup-lakebase.sh --name <name> --capacity CU_2
[ ] 2. Copy the printed db-… id into deploy.config.sh:
        DEFAULT_LAKEBASE_DATABASE_RESOURCE_SEGMENT="db-xxxx-xxxxxxxxxx"
[ ] 3. Set DEFAULT_LAKEBASE_PROJECT and DEFAULT_LAKEBASE_BRANCH in deploy.config.sh
[ ] 4. make deploy
[ ] 5. Bind resources in Databricks Apps UI (sql-warehouse, volume, postgres)
[ ] 6. Open app → Settings → Registry → Initialize
[ ] 7. make bootstrap-lakebase  (or make deploy — idempotent)
[ ] 8. Open Settings → Graph DB
        - Engine: lakebase
        - Config: { "sync_mode": "app_managed" }  (or "managed_synced")
[ ] 9. Build your first Digital Twin
[ ] 10. make bootstrap-lakebase again (grants on ontobricks_graph schema)
```
