#!/usr/bin/env bash
set -euo pipefail

# ── OntoBricks — deployment prerequisite checker ────────────────────
# Read-only validation before install / deploy / bootstrap. Run this
# (or `make deploy-check`) before your first deploy to catch missing
# tooling, auth problems, unreachable Lakebase endpoints, and registry
# schema migration blockers early.
#
# Usage:
#   scripts/check-deploy-prerequisites.sh              # full deploy preflight (default target)
#   scripts/check-deploy-prerequisites.sh --local      # local dev install only (setup.sh)
#   scripts/check-deploy-prerequisites.sh --volume     # dev target (no Lakebase checks)
#   scripts/check-deploy-prerequisites.sh --provision  # setup-lakebase.sh prerequisites
#   scripts/check-deploy-prerequisites.sh --lakebase   # Lakebase bootstrap checks only
#
# Exit 0 when no blocking issues; non-zero otherwise. Warnings are printed
# but do not fail the script unless they represent a hard error inside
# the Lakebase preflight module.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/_deploy-preflight.sh"

MODE="deploy"
TARGET=""
CONFIG_FILE="scripts/deploy.config.sh"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --local)     MODE="local"; shift ;;
        --volume)    MODE="volume"; shift ;;
        --provision) MODE="provision"; shift ;;
        --lakebase)  MODE="lakebase"; shift ;;
        -t|--target) TARGET="${2:-}"; [[ -n "$TARGET" ]] || { echo "ERROR: -t/--target requires a value" >&2; exit 2; }; shift 2 ;;
        -h|--help)
            sed -n '4,18p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) echo "Unknown option: $1 (run with --help)" >&2; exit 2 ;;
    esac
done

echo "=== OntoBricks deployment prerequisite check ==="

case "$MODE" in
    local)
        _preflight_check_local_dev
        _preflight_summary
        exit $?
        ;;
    provision)
        _preflight_begin "Lakebase provisioning prerequisites"
        _preflight_require_cmd databricks "install Databricks CLI ≥ 0.250.0"
        _preflight_require_cmd python3
        _preflight_require_cmd curl
        _preflight_check_databricks_auth || true
        _preflight_summary
        exit $((_PREFLIGHT_FAILED > 0 ? 1 : 0))
        ;;
esac

# Deploy / volume / lakebase modes need deploy.config.sh
_preflight_require_file "$CONFIG_FILE"
# shellcheck disable=SC1090
. "$CONFIG_FILE"

IS_LAKEBASE=false
if [[ "$MODE" == "lakebase" ]]; then
    IS_LAKEBASE=true
elif [[ "$MODE" == "volume" ]]; then
    IS_LAKEBASE=false
else
    TARGET="${TARGET:-$DAB_TARGET}"
    [[ "$TARGET" == *lakebase* ]] && IS_LAKEBASE=true
fi

_preflight_check_local_dev
_preflight_check_deploy_files "$IS_LAKEBASE"

_preflight_begin "Deploy tooling"
_preflight_require_cmd databricks "install Databricks CLI ≥ 0.250.0 — https://docs.databricks.com/dev-tools/cli/"
_preflight_require_cmd python3
_cli_ver="$(databricks version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n1 || true)"
_preflight_ok "Databricks CLI${_cli_ver:+ v${_cli_ver}}"

_preflight_check_databricks_auth || { _preflight_summary; exit 1; }

_preflight_begin "Deploy configuration"
require_var() { _preflight_require_var "$1"; }
require_var APP_NAME
require_var MCP_APP_NAME
require_var WAREHOUSE_ID
require_var REGISTRY_CATALOG
require_var REGISTRY_SCHEMA
require_var REGISTRY_VOLUME
if $IS_LAKEBASE; then
    require_var LAKEBASE_PROJECT
    require_var LAKEBASE_BRANCH
    require_var LAKEBASE_DATABASE
    require_var LAKEBASE_SCHEMA
    _preflight_require_cmd psql "required for bootstrap-lakebase-perms.sh — brew install libpq && brew link --force libpq"
fi
_preflight_ok "deploy.config.sh values present"

_preflight_check_bootstrap_app_perms "$APP_NAME" "$MCP_APP_NAME"

if $IS_LAKEBASE; then
    # Resolve db-… segment when possible (same logic as deploy.sh, read-only).
    if [[ -z "${LAKEBASE_DATABASE_RESOURCE_SEGMENT:-}" ]]; then
        _branch_path="projects/${LAKEBASE_PROJECT}/branches/${LAKEBASE_BRANCH}"
        _resolve_out="$(databricks postgres list-databases "$_branch_path" -o json 2>/dev/null || true)"
        if [[ -n "$_resolve_out" ]]; then
            _resolve_hit="$(printf '%s' "$_resolve_out" \
                | python3 -c "
import sys, json
raw = sys.stdin.read()
try:
    data = json.loads(raw)
    dbs = data if isinstance(data, list) else data.get('databases', [])
    for db in dbs:
        if db.get('status', {}).get('postgres_database') == '${LAKEBASE_DATABASE}':
            print(db.get('name','').split('/')[-1])
            break
except Exception:
    pass
" 2>/dev/null || true)"
            if [[ -n "$_resolve_hit" ]]; then
                LAKEBASE_DATABASE_RESOURCE_SEGMENT="$_resolve_hit"
                export LAKEBASE_DATABASE_RESOURCE_SEGMENT
                _preflight_ok "Resolved Lakebase db segment: ${LAKEBASE_DATABASE_RESOURCE_SEGMENT}"
            fi
        fi
        if [[ -z "${LAKEBASE_DATABASE_RESOURCE_SEGMENT:-}" ]]; then
            _preflight_warn "Could not resolve db-… segment for datname '${LAKEBASE_DATABASE}' — deploy.sh will retry; verify LAKEBASE_PROJECT/LAKEBASE_BRANCH/DATABASE"
        fi
    else
        _preflight_ok "Lakebase db segment: ${LAKEBASE_DATABASE_RESOURCE_SEGMENT}"
    fi

    _preflight_begin "Databricks resources (read-only)"
    if databricks warehouses get "$WAREHOUSE_ID" >/dev/null 2>&1; then
        _preflight_ok "SQL warehouse ${WAREHOUSE_ID}"
    else
        _preflight_fail "SQL warehouse '${WAREHOUSE_ID}' not found or not accessible"
    fi
    _vol_fqn="${REGISTRY_CATALOG}.${REGISTRY_SCHEMA}.${REGISTRY_VOLUME}"
    if databricks volumes read "$_vol_fqn" >/dev/null 2>&1; then
        _preflight_ok "Volume ${_vol_fqn}"
    else
        _preflight_fail "Volume '${_vol_fqn}' not found or not accessible"
    fi
    _branch_path="projects/${LAKEBASE_PROJECT}/branches/${LAKEBASE_BRANCH}"
    if _pg_dbs="$(databricks postgres list-databases "$_branch_path" -o json 2>/dev/null)"; then
        if [[ -n "${LAKEBASE_DATABASE_RESOURCE_SEGMENT:-}" ]] \
            && printf '%s' "$_pg_dbs" | grep -q "${LAKEBASE_DATABASE_RESOURCE_SEGMENT}"; then
            _preflight_ok "Lakebase database segment present on ${_branch_path}"
        else
            _preflight_warn "Lakebase database not verified under ${_branch_path} — run scripts/setup-lakebase.sh if missing"
        fi
    else
        _preflight_fail "Could not list Lakebase databases for ${_branch_path}"
    fi

    _preflight_check_lakebase_bootstrap \
        "$LAKEBASE_PROJECT" \
        "$LAKEBASE_BRANCH" \
        "$LAKEBASE_DATABASE" \
        "$LAKEBASE_SCHEMA" \
        "$APP_NAME" \
        "$MCP_APP_NAME" || true
fi

_preflight_summary
exit $((_PREFLIGHT_FAILED > 0 ? 1 : 0))
