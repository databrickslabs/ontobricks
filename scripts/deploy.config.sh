#!/usr/bin/env bash
# ── OntoBricks deployment configuration ─────────────────────────────
#
# SINGLE SOURCE OF TRUTH for everything `make deploy` needs.
# Sourced by `scripts/deploy.sh`. Every variable is env-overridable
# (`FOO=bar make deploy`) so CI can drive deployments without
# committing per-environment changes.
#
# ┌─────────────────────────────────────────────────────────────────┐
# │  TO DEPLOY A NEW INSTANCE: change DEFAULT_APP_NAME only.        │
# │  Everything else derives from it automatically:                  │
# │    MCP app name     → "mcp-<DEFAULT_APP_NAME>"                   │
# │    Registry schema  → "<DEFAULT_APP_NAME>" (hyphens→underscores) │
# │    Lakebase schema  → same as registry schema                    │
# │    Lakebase datname → same as registry schema                    │
# │    Workspace folder → .bundle/<DEFAULT_APP_NAME>/<target>        │
# │                                                                   │
# │  Per-workspace constants (section 0b) stay shared across all     │
# │  instances — set them once for your workspace.                   │
# └─────────────────────────────────────────────────────────────────┘
#
# Workflow:
#
#   1. Change DEFAULT_APP_NAME to a free name (`databricks apps list`).
#   2. Override section 0b constants if deploying to a different workspace.
#   3. Run `make deploy` (or `scripts/deploy.sh` directly).
#   4. For a one-off override: `WAREHOUSE_ID=abc make deploy`.
#
# Sections:
#
#   0a. Instance identity  — THE ONE THING to change per deployment.
#   0b. Workspace constants — per-workspace, shared across instances.
#   0c. Derived defaults   — auto-computed from 0a; do not edit.
#   1.  Apps               — exports for the app names
#   2.  DAB target         — which `databricks.yml` target to deploy
#   3.  DAB vars           — overrides for `databricks.yml > variables:`
#   4.  Runtime fallbacks  — values rendered into `app.yaml` at deploy time

# ── 0a. Instance identity ────────────────────────────────────────────
# THE ONLY LINE YOU NEED TO CHANGE to create a new deployment.
# App names are workspace-global — pick one not already in `databricks apps list`.
# The workspace sync folder is automatically isolated per app name.
DEFAULT_APP_NAME="ontobricks-050"

# ── 0b. Workspace constants ──────────────────────────────────────────
# Set once for your workspace. Shared across all instances deployed here.

# SQL Warehouse
DEFAULT_WAREHOUSE_ID="d2096aa075ad44a3"

# Unity Catalog — catalog that holds per-instance schemas
DEFAULT_REGISTRY_CATALOG="benoit_cayla"
DEFAULT_REGISTRY_VOLUME="registry"

# Lakebase Autoscaling project + branch (shared across instances —
# each instance gets its own schema inside the same database).
DEFAULT_LAKEBASE_PROJECT="ontobricks-demo2"
DEFAULT_LAKEBASE_BRANCH="production"
# db-… resource id from `databricks postgres list-databases
#   "projects/<project>/branches/<branch>" -o json`
DEFAULT_LAKEBASE_DATABASE_RESOURCE_SEGMENT="db-v6vc-8ibz5oeigo"

# ── 0b2. Schema name override (optional) ─────────────────────────────
# Leave empty ("") to auto-derive from DEFAULT_APP_NAME (recommended for
# new deployments — hyphens replaced by underscores).
# Set explicitly only when pointing at a pre-existing schema whose name
# does not follow the DEFAULT_APP_NAME convention, or when sharing a
# schema between app instances.
#
#   New deployment  → leave empty, schema derives automatically:
#                     "ontobricks-foo" → "ontobricks_foo"
#   Existing schema → set the exact name here:
DEFAULT_SCHEMA_OVERRIDE="ontobricks_demo"   # ← set "" for new deployments

# ── 0c. Derived defaults (auto-computed — do NOT edit) ────────────────
# MCP companion app name: "mcp-<app-name>"
DEFAULT_MCP_APP_NAME="mcp-${DEFAULT_APP_NAME}"

# Schema slug: hyphens → underscores  (e.g. "ontobricks-foo" → "ontobricks_foo")
_APP_SLUG="${DEFAULT_APP_NAME//-/_}"

# UC registry schema (override wins; falls back to slug)
DEFAULT_REGISTRY_SCHEMA="${DEFAULT_SCHEMA_OVERRIDE:-${_APP_SLUG}}"

# Lakebase schema + datname — mirror the UC schema name for consistency
DEFAULT_LAKEBASE_REGISTRY_SCHEMA="${DEFAULT_SCHEMA_OVERRIDE:-${_APP_SLUG}}"
DEFAULT_LAKEBASE_REGISTRY_DATABASE="${DEFAULT_SCHEMA_OVERRIDE:-${_APP_SLUG}}"

# DAB resource keys (static — identifiers in databricks.yml, not app names)
DEFAULT_APP_RESOURCE_KEY="ontobricks_dev_app"
DEFAULT_MCP_APP_RESOURCE_KEY="mcp_ontobricks_app"

# ── 0d. app.yaml runtime fallback literals ───────────────────────────
# Only literal values that have no section-3 counterpart live here.
# Registry triplet + warehouse derive from section 3 (see section 4).
DEFAULT_APP_TRIPLESTORE_TABLE_NAME="default_triplestore"
DEFAULT_APP_MLFLOW_TRACKING_URI="databricks"

# ── DAB target ───────────────────────────────────────────────────────
DEFAULT_DAB_TARGET="dev-lakebase"

# ── 1. Apps ─────────────────────────────────────────────────────────
# The FastAPI UI app and its MCP companion server.
# APP_NAME / MCP_APP_NAME are passed to databricks.yml as
# --var=app_name / --var=mcp_app_name, so this file is the single
# source of truth for both the DAB resource name and the CLI lookups.
export APP_NAME="${APP_NAME:-$DEFAULT_APP_NAME}"
export MCP_APP_NAME="${MCP_APP_NAME:-$DEFAULT_MCP_APP_NAME}"

# DAB resource keys (rarely change — they're identifiers in
# `databricks.yml > resources.apps`, not the deployed app names).
export APP_RESOURCE_KEY="${APP_RESOURCE_KEY:-$DEFAULT_APP_RESOURCE_KEY}"
export MCP_APP_RESOURCE_KEY="${MCP_APP_RESOURCE_KEY:-$DEFAULT_MCP_APP_RESOURCE_KEY}"

# ── 2. DAB target ───────────────────────────────────────────────────
# `dev`           : Volume-only registry backend.
# `dev-lakebase`  : Volume + Lakebase Autoscaling Postgres binding
#                   (default — required for the Postgres registry).
export DAB_TARGET="${DAB_TARGET:-$DEFAULT_DAB_TARGET}"

# ── 3. DAB variable overrides (databricks.yml > variables:) ─────────
# Passed to `databricks bundle deploy` as `--var=key=value`. Override
# the defaults declared in `databricks.yml` so that file stays a
# pure declaration of structure, not configuration.
export WAREHOUSE_ID="${WAREHOUSE_ID:-$DEFAULT_WAREHOUSE_ID}"

# Unity Catalog Volume securable: the deployed app gets WRITE_VOLUME
# on `<REGISTRY_CATALOG>.<REGISTRY_SCHEMA>.<REGISTRY_VOLUME>`.
export REGISTRY_CATALOG="${REGISTRY_CATALOG:-$DEFAULT_REGISTRY_CATALOG}"
export REGISTRY_SCHEMA="${REGISTRY_SCHEMA:-$DEFAULT_REGISTRY_SCHEMA}"
export REGISTRY_VOLUME="${REGISTRY_VOLUME:-$DEFAULT_REGISTRY_VOLUME}"

# Lakebase Autoscaling project / branch / database. The bundle
# composes the full Apps `postgres:` resource paths from these.
# `LAKEBASE_DATABASE_RESOURCE_SEGMENT` is the `db-…` id from
# `databricks postgres list-databases` — NOT the `datname` /
# `status.postgres_database`. See `databricks.yml` lines 110-156
# for the full caveat.
export LAKEBASE_PROJECT="${LAKEBASE_PROJECT:-$DEFAULT_LAKEBASE_PROJECT}"
export LAKEBASE_BRANCH="${LAKEBASE_BRANCH:-$DEFAULT_LAKEBASE_BRANCH}"
export LAKEBASE_DATABASE_RESOURCE_SEGMENT="${LAKEBASE_DATABASE_RESOURCE_SEGMENT:-$DEFAULT_LAKEBASE_DATABASE_RESOURCE_SEGMENT}"

# Registry Postgres datname (psql grants + app.yaml runtime). NOT the db-… segment.
export LAKEBASE_REGISTRY_DATABASE="${LAKEBASE_REGISTRY_DATABASE:-$DEFAULT_LAKEBASE_REGISTRY_DATABASE}"

# Lakebase Postgres schema OntoBricks writes the registry into.
# Mirrored into `app.yaml > LAKEBASE_SCHEMA` (section 4) so the
# runtime knows where to look.
export LAKEBASE_REGISTRY_SCHEMA="${LAKEBASE_REGISTRY_SCHEMA:-$DEFAULT_LAKEBASE_REGISTRY_SCHEMA}"

# ── 4. app.yaml runtime fallbacks ───────────────────────────────────
# Templated into `app.yaml` (and `src/mcp-server/app.yaml`) from
# their respective *.template files at deploy time.
# Only consulted when the matching DAB resource is unbound.

# MCP companion: URL of the main OntoBricks app.
# Format: https://<app-name>-<workspace-id>.<region>.databricksapps.com
# Retrieve after first deploy with: databricks apps get <APP_NAME> | jq -r .url
# Leave empty ("") to use localhost:8000 (local dev only).
export APP_ONTOBRICKS_URL="${APP_ONTOBRICKS_URL:-}"

# DBSQL warehouse fallback for MCP / session-less API calls.
# Tracks the section-3 bound warehouse (WAREHOUSE_ID) unless overridden.
export APP_SQL_WAREHOUSE_FALLBACK="${APP_SQL_WAREHOUSE_FALLBACK:-$WAREHOUSE_ID}"

# Default fully-qualified Delta triplestore table (catalog.schema.table)
# used when no domain session is active. Catalog.schema track the section-3
# registry values; only the table name is a literal.
export APP_TRIPLESTORE_TABLE="${APP_TRIPLESTORE_TABLE:-${REGISTRY_CATALOG}.${REGISTRY_SCHEMA}.${DEFAULT_APP_TRIPLESTORE_TABLE_NAME}}"

# Registry runtime fallbacks — used when the Volume resource is not
# bound (typically local dev / MCP). Track the section-3 bound Volume
# (REGISTRY_*) so the fallback matches what you deploy. Override any of
# them only if you intentionally want the unbound path on a different
# volume.
export APP_REGISTRY_CATALOG="${APP_REGISTRY_CATALOG:-$REGISTRY_CATALOG}"
export APP_REGISTRY_SCHEMA="${APP_REGISTRY_SCHEMA:-$REGISTRY_SCHEMA}"
export APP_REGISTRY_VOLUME="${APP_REGISTRY_VOLUME:-$REGISTRY_VOLUME}"

# Lakebase Postgres schema (tracks LAKEBASE_REGISTRY_SCHEMA from
# section 3 by default — must match the schema actually GRANTed by
# `bootstrap-lakebase-perms.sh`).
export APP_LAKEBASE_SCHEMA="${APP_LAKEBASE_SCHEMA:-$LAKEBASE_REGISTRY_SCHEMA}"

# Lakebase Postgres database name — must match the actual ``postgres_database``
# of the bound ``db-…`` resource (i.e. the datname, not the schema name).
# Tracks LAKEBASE_REGISTRY_DATABASE (section 3), the real datname.
export APP_LAKEBASE_DATABASE="${APP_LAKEBASE_DATABASE:-$LAKEBASE_REGISTRY_DATABASE}"

# Lakebase project (autoscaling instance name) — informational in deployed app.
export APP_LAKEBASE_PROJECT="${APP_LAKEBASE_PROJECT:-$LAKEBASE_PROJECT}"

# Lakebase branch deployed to (used by LakebaseAuth host-resolution
# fallback when PGHOST is not injected — e.g. local dev without binding).
export APP_LAKEBASE_BRANCH="${APP_LAKEBASE_BRANCH:-$LAKEBASE_BRANCH}"

# Lakebase managed-synced: UC catalog for the Lakeflow synced-table registration.
# Leave empty to let OntoBricks auto-resolve the catalog from the registry
# Volume config (recommended — the SP receives ALL_PRIVILEGES on the registry
# catalog at deploy time via scripts/bootstrap-app-permissions.sh).
# Override with a shared catalog name only if your registry catalog is not
# accessible to the app service principal in production.
# NOTE: intentionally NOT using ${APP_SYNC_UC_CATALOG:-} here so that a
# stale shell export from a previous deploy never bleeds through.
export APP_SYNC_UC_CATALOG=""

# MLflow tracking URI (`databricks` = workspace tracking server).
export APP_MLFLOW_TRACKING_URI="${APP_MLFLOW_TRACKING_URI:-$DEFAULT_APP_MLFLOW_TRACKING_URI}"
