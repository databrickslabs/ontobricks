#!/usr/bin/env bash
# ── OntoBricks deployment configuration ─────────────────────────────
#
# SINGLE SOURCE OF TRUTH for everything `make deploy` needs.
# Sourced by `scripts/deploy.sh`. Every variable is env-overridable
# (`FOO=bar make deploy`) so CI can drive deployments without
# committing per-environment changes.
#
# ┌─────────────────────────────────────────────────────────────────┐
# │  TO DEPLOY A NEW INSTANCE:                                      │
# │    1. Set DEFAULT_APP_NAME.                                     │
# │    2. Set DEFAULT_LAKEBASE_DATABASE (existing Postgres datname).│
# │    3. Optionally set DEFAULT_DATABRICKS_PROFILE if you use a    │
# │       non-default Databricks CLI profile.                       │
# │    4. Optionally set DEFAULT_LAKEBASE_SCHEMA if you want a      │
# │       specific schema name (defaults to app-name slug).         │
# │    5. Optionally set DEFAULT_REGISTRY_SCHEMA if the UC schema   │
# │       differs from the app-name slug.                           │
# │                                                                  │
# │  The Lakebase db-… resource segment is resolved automatically   │
# │  from DEFAULT_LAKEBASE_DATABASE — you never need to set it.     │
# └─────────────────────────────────────────────────────────────────┘
#
# Sections:
#   0a. Instance identity  — change these per deployment.
#   0b. Workspace constants — set once per workspace.
#   0c. Derived defaults   — auto-computed; do not edit.
#   1.  Apps               — exports for the app names
#   2.  DAB target         — which `databricks.yml` target to deploy
#   3.  DAB vars           — values for `databricks.yml > variables:`
#   4.  Runtime fallbacks  — values rendered into `app.yaml` at deploy time

# ── 0a. Instance identity ────────────────────────────────────────────
# THE ONLY LINE YOU NEED TO CHANGE to create a new deployment.
DEFAULT_APP_NAME="ontobricks-060"

# ── 0b. Workspace constants ──────────────────────────────────────────
# Set once for your workspace. Shared across all instances deployed here.

# Databricks CLI profile (`databricks auth profiles`). Leave empty to use
# the CLI default profile. Exported as DATABRICKS_CONFIG_PROFILE for all
# `databricks` invocations when this file is sourced (make deploy, bootstrap, …).
DEFAULT_DATABRICKS_PROFILE="fe-vm-bcayla-demos"

# SQL Warehouse
DEFAULT_WAREHOUSE_ID="d2096aa075ad44a3"

# Unity Catalog
DEFAULT_REGISTRY_CATALOG="benoit_cayla"
# UC schema for the Volume registry
DEFAULT_REGISTRY_SCHEMA="ontobricks_demo"
DEFAULT_REGISTRY_VOLUME="registry"

# Lakebase Autoscaling project + branch
DEFAULT_LAKEBASE_PROJECT="ontobricks-demo2"
DEFAULT_LAKEBASE_BRANCH="production"
# Postgres database (datname) on the shared Lakebase instance — the value of
# status.postgres_database from list-databases (underscores OK). Do NOT copy the
# hyphenated database_id from the resource path name; those differ when the
# datname contains underscores (API uses hyphens only in database_id / RFC-1123).
# Each app gets its own SCHEMA inside this database.
DEFAULT_LAKEBASE_DATABASE="ontobricks_demo"
# Postgres schema inside the Lakebase database, Each instance should have its own schema for isolation.
DEFAULT_LAKEBASE_SCHEMA="ontobricks_demo"
# Example — reuse existing schema: DEFAULT_LAKEBASE_SCHEMA="ontobricks_demo"

# ── 0c. Derived defaults (auto-computed — do NOT edit) ────────────────
DEFAULT_MCP_APP_NAME="mcp-${DEFAULT_APP_NAME}"

# DAB resource keys (static identifiers in databricks.yml)
DEFAULT_APP_RESOURCE_KEY="ontobricks_dev_app"
DEFAULT_MCP_APP_RESOURCE_KEY="mcp_ontobricks_app"

# ── 0d. app.yaml runtime fallback literals ───────────────────────────
DEFAULT_APP_TRIPLESTORE_TABLE_NAME="default_triplestore"
DEFAULT_APP_MLFLOW_TRACKING_URI="databricks"

# ── DAB target ───────────────────────────────────────────────────────
DEFAULT_DAB_TARGET="dev-lakebase"

# ── 0e. Databricks CLI profile ───────────────────────────────────────
# Override for one-off runs: DATABRICKS_CONFIG_PROFILE=my-profile make deploy
if [[ -n "${DATABRICKS_CONFIG_PROFILE:-$DEFAULT_DATABRICKS_PROFILE}" ]]; then
    export DATABRICKS_CONFIG_PROFILE="${DATABRICKS_CONFIG_PROFILE:-$DEFAULT_DATABRICKS_PROFILE}"
fi

# ── 1. Apps ─────────────────────────────────────────────────────────
export APP_NAME="${APP_NAME:-$DEFAULT_APP_NAME}"
export MCP_APP_NAME="${MCP_APP_NAME:-$DEFAULT_MCP_APP_NAME}"
export APP_RESOURCE_KEY="${APP_RESOURCE_KEY:-$DEFAULT_APP_RESOURCE_KEY}"
export MCP_APP_RESOURCE_KEY="${MCP_APP_RESOURCE_KEY:-$DEFAULT_MCP_APP_RESOURCE_KEY}"

# ── 2. DAB target ───────────────────────────────────────────────────
# `dev`           : Volume-only registry backend.
# `dev-lakebase`  : Volume + Lakebase Autoscaling Postgres binding (default).
export DAB_TARGET="${DAB_TARGET:-$DEFAULT_DAB_TARGET}"

# ── 3. DAB variable overrides (databricks.yml > variables:) ─────────
export WAREHOUSE_ID="${WAREHOUSE_ID:-$DEFAULT_WAREHOUSE_ID}"

# Unity Catalog Volume securable.
export REGISTRY_CATALOG="${REGISTRY_CATALOG:-$DEFAULT_REGISTRY_CATALOG}"
# Env-overridable so `scripts/update-deployed-app.sh` can pin the schema it
# read back from a live app. `make deploy` `unset`s REGISTRY_SCHEMA first, so
# the routine deploy still always uses DEFAULT_REGISTRY_SCHEMA.
export REGISTRY_SCHEMA="${REGISTRY_SCHEMA:-$DEFAULT_REGISTRY_SCHEMA}"
export REGISTRY_VOLUME="${REGISTRY_VOLUME:-$DEFAULT_REGISTRY_VOLUME}"

# Lakebase project / branch.
export LAKEBASE_PROJECT="${LAKEBASE_PROJECT:-$DEFAULT_LAKEBASE_PROJECT}"
export LAKEBASE_BRANCH="${LAKEBASE_BRANCH:-$DEFAULT_LAKEBASE_BRANCH}"

# Lakebase Postgres datname and schema — always taken from the file (not
# from the shell environment) so stale exports never bleed through.
export LAKEBASE_DATABASE="${DEFAULT_LAKEBASE_DATABASE}"
export LAKEBASE_SCHEMA="${DEFAULT_LAKEBASE_SCHEMA}"

# db-… resource segment — resolved by deploy.sh from LAKEBASE_DATABASE
# via the Databricks API. Exposed here so it can be overridden in CI
# without re-running the API lookup (e.g. LAKEBASE_DATABASE_RESOURCE_SEGMENT=db-xxx make deploy).
export LAKEBASE_DATABASE_RESOURCE_SEGMENT="${LAKEBASE_DATABASE_RESOURCE_SEGMENT:-}"

# ── 4. app.yaml runtime fallbacks ───────────────────────────────────

# MCP companion URL (leave empty for local dev).
export APP_ONTOBRICKS_URL="${APP_ONTOBRICKS_URL:-}"

# DBSQL warehouse fallback.
export APP_SQL_WAREHOUSE_FALLBACK="${APP_SQL_WAREHOUSE_FALLBACK:-$WAREHOUSE_ID}"

# Default triplestore table.
export APP_TRIPLESTORE_TABLE="${APP_TRIPLESTORE_TABLE:-${REGISTRY_CATALOG}.${REGISTRY_SCHEMA}.${DEFAULT_APP_TRIPLESTORE_TABLE_NAME}}"

# Registry Volume runtime fallbacks (local dev / MCP without bound resource).
export APP_REGISTRY_CATALOG="${APP_REGISTRY_CATALOG:-$REGISTRY_CATALOG}"
export APP_REGISTRY_SCHEMA="${APP_REGISTRY_SCHEMA:-$REGISTRY_SCHEMA}"
export APP_REGISTRY_VOLUME="${APP_REGISTRY_VOLUME:-$REGISTRY_VOLUME}"

# Lakebase runtime values rendered into app.yaml — always from file.
export APP_LAKEBASE_SCHEMA="${LAKEBASE_SCHEMA}"
export APP_LAKEBASE_DATABASE="${LAKEBASE_DATABASE}"
export APP_LAKEBASE_PROJECT="${APP_LAKEBASE_PROJECT:-$LAKEBASE_PROJECT}"
export APP_LAKEBASE_BRANCH="${APP_LAKEBASE_BRANCH:-$LAKEBASE_BRANCH}"

# Lakebase managed-synced UC catalog (leave empty to auto-resolve).
# Intentionally NOT using :- so stale exports never bleed through.
export APP_SYNC_UC_CATALOG=""

# MLflow tracking URI.
export APP_MLFLOW_TRACKING_URI="${APP_MLFLOW_TRACKING_URI:-$DEFAULT_APP_MLFLOW_TRACKING_URI}"
