#!/usr/bin/env bash
# ── OntoBricks deployment configuration ─────────────────────────────
#
# SINGLE SOURCE OF TRUTH for everything `make deploy` needs.
# Sourced by `scripts/deploy.sh`. Every variable is env-overridable
# (`FOO=bar make deploy`) so CI can drive deployments without
# committing per-environment changes.
#
# Workflow:
#
#   1. To change a default permanently, edit the matching
#      `DEFAULT_*` literal in section 0 below.
#   2. To override for one run, prefix the command:
#      `WAREHOUSE_ID=abc make deploy`.
#   3. Then run `make deploy` (or `scripts/deploy.sh` directly).
#
# Sections:
#
#   0. Defaults         — literal default values for every variable
#                         (`DEFAULT_*`). Edit here to change a default
#                         permanently. Not exported (config-internal).
#   1. Apps             — names of the FastAPI app + its MCP companion
#   2. DAB target       — which `databricks.yml` target to deploy
#   3. DAB vars         — overrides for `databricks.yml > variables:`
#                         (warehouse, registry catalog/schema/volume,
#                         Lakebase project / branch / database / schema)
#   4. Runtime fallbacks — values rendered into `app.yaml` from
#                          `app.yaml.template` at deploy time. These
#                          are used when the corresponding DAB
#                          resource isn't bound (local dev, MCP).
#
# Notes:
# - Sections 1-4 each follow the same pattern:
#     `export FOO="${FOO:-$DEFAULT_FOO}"`
#   This means: use the caller's `FOO` if it's already set/non-empty,
#   otherwise fall back to `DEFAULT_FOO` from section 0.
# - Section 3 (DAB vars) drive the BOUND resources Databricks Apps
#   actually wires up at runtime — `${var.warehouse_id}`, the volume
#   securable path, the Lakebase Postgres database path.
# - Section 4 are the static fallbacks baked into `app.yaml`. The DAB
#   binding always wins when present; the fallback is only consulted
#   when the resource is unbound (e.g. MCP API calls, local
#   `python run.py`). They can intentionally diverge from section 3
#   if you keep separate registries for prod-bound vs. local.
# - To add a new variable:
#     a. Add `DEFAULT_NAME="…"` to section 0 below.
#     b. Add `export NAME="${NAME:-$DEFAULT_NAME}"` to the matching
#        section (1-4).
#     c. If it maps to `databricks.yml`, add a `--var=` line to
#        `scripts/deploy.sh > _dab_var_overrides`.
#     d. If it maps to `app.yaml`, add `${NAME}` in
#        `app.yaml.template` and let the renderer handle it.

# ── 0. Defaults ─────────────────────────────────────────────────────
# Literal default values. Edit a value here to change the project-wide
# default permanently. These are plain assignments (no export, no
# `:-` fallback) — they're config-internal, not meant to leak to
# subprocesses or to be env-overridden themselves. Override the
# matching `FOO` from the environment instead (see header).

# 1. Apps
DEFAULT_APP_NAME="ontobricks-030"
DEFAULT_MCP_APP_NAME="mcp-ontobricks"
DEFAULT_APP_RESOURCE_KEY="ontobricks_dev_app"
DEFAULT_MCP_APP_RESOURCE_KEY="mcp_ontobricks_app"

# 2. DAB target
DEFAULT_DAB_TARGET="dev-lakebase"

# 3. DAB variable overrides
DEFAULT_WAREHOUSE_ID="d2096aa075ad44a3"
DEFAULT_REGISTRY_CATALOG="benoit_cayla"
DEFAULT_REGISTRY_SCHEMA="ontobricks_030"
DEFAULT_REGISTRY_VOLUME="registry"
DEFAULT_LAKEBASE_PROJECT="ontobricks-app"
DEFAULT_LAKEBASE_BRANCH="production"
DEFAULT_LAKEBASE_DATABASE_RESOURCE_SEGMENT="db-8u4l-0na72ub5sp"
DEFAULT_LAKEBASE_REGISTRY_SCHEMA="ontobricks_registry"

# 3b. Lakebase GRANT bootstrap (only the database needs a literal
# default — the instance and schema track LAKEBASE_PROJECT and
# LAKEBASE_REGISTRY_SCHEMA respectively, see section 3b below).
DEFAULT_LAKEBASE_BOOTSTRAP_DATABASE="ontobricks_registry"

# 4. app.yaml runtime fallbacks
DEFAULT_APP_SQL_WAREHOUSE_FALLBACK="66e8366e84d57752"
DEFAULT_APP_DATABRICKS_CATALOG="main"
DEFAULT_APP_DATABRICKS_SCHEMA="default"
DEFAULT_APP_TRIPLESTORE_TABLE="benoit_cayla.ontobricks.default_triplestore"
DEFAULT_APP_REGISTRY_CATALOG="benoit_cayla"
DEFAULT_APP_REGISTRY_SCHEMA="ontobricks"
DEFAULT_APP_REGISTRY_VOLUME="OntoBricksRegistry"
DEFAULT_APP_MLFLOW_TRACKING_URI="databricks"

# ── 1. Apps ─────────────────────────────────────────────────────────
# The FastAPI UI app and its MCP companion server. APP_NAME must
# match `databricks.yml > resources.apps.ontobricks_dev_app.name`
# (the deploy script verifies this on every run).
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

# Lakebase Postgres schema OntoBricks writes the registry into.
# Mirrored into `app.yaml > LAKEBASE_SCHEMA` (section 4) so the
# runtime knows where to look.
export LAKEBASE_REGISTRY_SCHEMA="${LAKEBASE_REGISTRY_SCHEMA:-$DEFAULT_LAKEBASE_REGISTRY_SCHEMA}"

# ── 3b. Lakebase GRANT bootstrap (psql) ─────────────────────────────
# Inputs to `scripts/bootstrap-lakebase-perms.sh`, called by
# `deploy.sh` after the bundle deploy on `dev-lakebase`. Instance
# and schema intentionally track LAKEBASE_PROJECT /
# LAKEBASE_REGISTRY_SCHEMA from section 3 — the bootstrap targets
# the same bound resource by default. Only the database has its own
# literal (DEFAULT_LAKEBASE_BOOTSTRAP_DATABASE) because it diverges
# from the bound resource path on some setups.
export LAKEBASE_BOOTSTRAP_INSTANCE="${LAKEBASE_BOOTSTRAP_INSTANCE:-$LAKEBASE_PROJECT}"
export LAKEBASE_BOOTSTRAP_DATABASE="${LAKEBASE_BOOTSTRAP_DATABASE:-$DEFAULT_LAKEBASE_BOOTSTRAP_DATABASE}"
export LAKEBASE_BOOTSTRAP_SCHEMA="${LAKEBASE_BOOTSTRAP_SCHEMA:-$LAKEBASE_REGISTRY_SCHEMA}"

# ── 4. app.yaml runtime fallbacks ───────────────────────────────────
# Templated into `app.yaml` from `app.yaml.template` at deploy time.
# Only consulted when the matching DAB resource is unbound.

# DBSQL warehouse fallback for MCP / session-less API calls.
export APP_SQL_WAREHOUSE_FALLBACK="${APP_SQL_WAREHOUSE_FALLBACK:-$DEFAULT_APP_SQL_WAREHOUSE_FALLBACK}"

# Default Unity Catalog catalog/schema for ad-hoc operations.
export APP_DATABRICKS_CATALOG="${APP_DATABRICKS_CATALOG:-$DEFAULT_APP_DATABRICKS_CATALOG}"
export APP_DATABRICKS_SCHEMA="${APP_DATABRICKS_SCHEMA:-$DEFAULT_APP_DATABRICKS_SCHEMA}"

# Default fully-qualified Delta triplestore table (catalog.schema.table)
# used when no domain session is active.
export APP_TRIPLESTORE_TABLE="${APP_TRIPLESTORE_TABLE:-$DEFAULT_APP_TRIPLESTORE_TABLE}"

# Registry runtime fallbacks — used when the Volume resource is not
# bound (typically local dev). These can intentionally point at a
# different volume than the DAB-bound one in section 3.
export APP_REGISTRY_CATALOG="${APP_REGISTRY_CATALOG:-$DEFAULT_APP_REGISTRY_CATALOG}"
export APP_REGISTRY_SCHEMA="${APP_REGISTRY_SCHEMA:-$DEFAULT_APP_REGISTRY_SCHEMA}"
export APP_REGISTRY_VOLUME="${APP_REGISTRY_VOLUME:-$DEFAULT_APP_REGISTRY_VOLUME}"

# Lakebase Postgres schema (tracks LAKEBASE_REGISTRY_SCHEMA from
# section 3 by default — must match the schema actually GRANTed by
# `bootstrap-lakebase-perms.sh`).
export APP_LAKEBASE_SCHEMA="${APP_LAKEBASE_SCHEMA:-$LAKEBASE_REGISTRY_SCHEMA}"

# MLflow tracking URI (`databricks` = workspace tracking server).
export APP_MLFLOW_TRACKING_URI="${APP_MLFLOW_TRACKING_URI:-$DEFAULT_APP_MLFLOW_TRACKING_URI}"
