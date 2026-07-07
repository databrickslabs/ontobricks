#!/usr/bin/env bash
# Shared Lakebase connection troubleshooting hints (stderr).
# Source from deploy/bootstrap scripts after Lakebase vars are known.
#
# Usage:
#   _lakebase_print_diag_hints "<reason>" <project> <branch> [datname] [db_segment] [config_file]

_lakebase_print_diag_hints() {
    local reason="$1"
    local project="$2"
    local branch="$3"
    local database="${4:-}"
    local db_segment="${5:-}"
    local config_file="${6:-scripts/deploy.config.sh}"

    local branch_path="projects/${project}/branches/${branch}"
    local profile_prefix=""
    [[ -n "${DATABRICKS_CONFIG_PROFILE:-}" ]] && profile_prefix=" --profile ${DATABRICKS_CONFIG_PROFILE}"

    echo "" >&2
    echo "  Lakebase diagnostics (${reason}):" >&2
    echo "    project  : ${project}" >&2
    echo "    branch   : ${branch}" >&2
    [[ -n "$database" ]] && echo "    datname  : ${database} (DEFAULT_LAKEBASE_DATABASE — use status.postgres_database)" >&2
    [[ -n "$db_segment" ]] && echo "    db id    : ${db_segment} (resource path segment / database_id, often db-… or hyphenated)" >&2
    echo "" >&2
    echo "  Two names per database (easy to confuse):" >&2
    echo "    • status.postgres_database → PGDATABASE / DEFAULT_LAKEBASE_DATABASE (underscores OK)" >&2
    echo "    • name trailing segment    → Apps postgres binding / db-… id (hyphens only, RFC-1123)" >&2
    echo "    If your datname is ontobricks_demo, the resource id may show ontobricks-demo." >&2
    echo "" >&2
    echo "  Common causes:" >&2
    echo "    • Wrong LAKEBASE_PROJECT or LAKEBASE_BRANCH in ${config_file}" >&2
    echo "    • Postgres database not created yet — run: scripts/setup-lakebase.sh" >&2
    echo "    • DEFAULT_LAKEBASE_DATABASE set to the hyphenated resource id instead of postgres_database" >&2
    echo "    • LAKEBASE_DATABASE datname ≠ status.postgres_database from the API" >&2
    echo "    • LAKEBASE_DATABASE_RESOURCE_SEGMENT unset or stale (must be the db-… id, not the datname)" >&2
    echo "    • Project created via the Databricks UI (use setup-lakebase.sh for Synced Tables compatibility)" >&2
    echo "    • CLI profile/workspace mismatch (check DEFAULT_DATABRICKS_PROFILE in ${config_file})" >&2
    echo "" >&2
    echo "  Inspect the live project:" >&2
    echo "    databricks${profile_prefix} postgres list-databases \"${branch_path}\" -o json" >&2
    echo "    databricks${profile_prefix} api get /api/2.0/postgres/${branch_path}/endpoints" >&2
    echo "" >&2
    echo "  In list-databases JSON, set DEFAULT_LAKEBASE_DATABASE from status.postgres_database;" >&2
    echo "  set LAKEBASE_DATABASE_RESOURCE_SEGMENT from the trailing name segment (db-… or slug)." >&2
}
