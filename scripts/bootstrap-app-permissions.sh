#!/usr/bin/env bash
set -euo pipefail

# ── OntoBricks — First-Deploy App Self-Permission Bootstrap ─────────
# Databricks Apps do not auto-grant their own service principal any
# permission on the app they run. Without this grant the middleware
# cannot read the app's own ACL (GET /api/2.0/permissions/apps/{name})
# and every user — including the CAN_MANAGE deployer — is shown the
# "access denied" page on the very first request.
#
# This script looks up each app's service principal and grants it
# CAN_MANAGE on its own app. It is idempotent and safe to re-run.
#
# Usage:
#   scripts/bootstrap-app-permissions.sh                        # bootstrap default sandbox apps
#   scripts/bootstrap-app-permissions.sh ontobricks-030         # explicit (positional)
#   scripts/bootstrap-app-permissions.sh a b c                  # bootstrap several apps
#   APP_NAME=ontobricks-040 scripts/bootstrap-app-permissions.sh   # override default sandbox app
#   MCP_APP_NAME=mcp-foo  scripts/bootstrap-app-permissions.sh    # override default sandbox MCP
#
# Prerequisites:
#   - Databricks CLI authenticated (databricks auth login ...)
#   - The apps already exist (run `make deploy` first)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."

# Default sandbox app pair. Override via env (e.g.
# `APP_NAME=ontobricks-040 scripts/bootstrap-app-permissions.sh`) to
# target a parallel sandbox without editing this file. Positional
# args, when provided, take precedence over both the env vars and
# these defaults. The names below match the Makefile's $(APP_NAME)
# variable to keep `make bootstrap-perms` and direct invocation in
# sync.
#
# This bundle only manages the dev sandbox apps. The production
# ``ontobricks`` and ``mcp-ontobricks`` apps were carved out on
# 2026-04-27 and live in a different repo/bundle.
APP_NAME="${APP_NAME:-ontobricks-030}"
MCP_APP_NAME="${MCP_APP_NAME:-mcp-ontobricks}"
DEFAULT_APPS=("$APP_NAME" "$MCP_APP_NAME")

if [[ $# -gt 0 ]]; then
    APPS=("$@")
else
    APPS=("${DEFAULT_APPS[@]}")
fi

if ! command -v databricks >/dev/null 2>&1; then
    echo "ERROR: Databricks CLI not installed." >&2
    exit 1
fi

if ! databricks current-user me >/dev/null 2>&1; then
    echo "ERROR: Not authenticated. Run: databricks auth login --host https://<workspace>" >&2
    exit 1
fi

echo "=== OntoBricks — App Self-Permission Bootstrap ==="
echo "Apps: ${APPS[*]}"
echo

get_service_principal_id() {
    local app="$1"
    databricks apps get "$app" -o json 2>/dev/null | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(2)
print(d.get('service_principal_client_id') or '')
" 2>/dev/null || true
}

grant_app_permission() {
    local app="$1"
    local target_sp="$2"
    local level="$3"
    if databricks apps update-permissions "$app" --json "{
        \"access_control_list\": [{
            \"service_principal_name\": \"$target_sp\",
            \"permission_level\": \"$level\"
        }]
    }" >/dev/null 2>&1; then
        return 0
    fi
    return 1
}

grant_self_permission() {
    local app="$1"
    LAST_SP_ID=""

    local sp_id
    sp_id="$(get_service_principal_id "$app")"

    if [[ -z "$sp_id" || "$sp_id" == "None" ]]; then
        echo "  [$app] SKIP — could not resolve service principal (app may not exist yet)"
        return 1
    fi

    echo "  [$app] service principal: $sp_id"

    # Idempotent: `update-permissions` merges/overwrites ACL entries for the
    # listed principals without touching others. Re-running has no effect.
    if grant_app_permission "$app" "$sp_id" "CAN_MANAGE"; then
        echo "  [$app] ✓ granted CAN_MANAGE to own service principal"
        LAST_SP_ID="$sp_id"
        return 0
    else
        echo "  [$app] ✗ failed to grant CAN_MANAGE — you need CAN_MANAGE on the app to run this"
        return 1
    fi
}

FAILED=0
APP_SP_ID=""
MCP_SP_ID=""
FIRST_APP=""
SECOND_APP=""
FIRST_SP_ID=""
SECOND_SP_ID=""
APP_FOR_CAN_USE="${APP_NAME}"
MCP_FOR_CAN_USE="${MCP_APP_NAME}"
idx=0
for app in "${APPS[@]}"; do
    if grant_self_permission "$app"; then
        if [[ $idx -eq 0 ]]; then
            FIRST_APP="$app"
            FIRST_SP_ID="$LAST_SP_ID"
        elif [[ $idx -eq 1 ]]; then
            SECOND_APP="$app"
            SECOND_SP_ID="$LAST_SP_ID"
        fi
        if [[ "$app" == "$APP_NAME" ]]; then
            APP_SP_ID="$LAST_SP_ID"
        fi
        if [[ "$app" == "$MCP_APP_NAME" ]]; then
            MCP_SP_ID="$LAST_SP_ID"
        fi
    else
        FAILED=$((FAILED + 1))
    fi
    idx=$((idx + 1))
done

# The MCP companion calls the main app REST API with its own Databricks
# App service-principal token, so it needs CAN_USE on the main app.
if [[ -z "$APP_SP_ID" && -n "$FIRST_SP_ID" ]]; then
    APP_SP_ID="$FIRST_SP_ID"
    APP_FOR_CAN_USE="$FIRST_APP"
fi
if [[ -z "$MCP_SP_ID" && -n "$SECOND_SP_ID" ]]; then
    MCP_SP_ID="$SECOND_SP_ID"
    MCP_FOR_CAN_USE="$SECOND_APP"
fi

if [[ -n "$APP_SP_ID" && -n "$MCP_SP_ID" ]]; then
    if grant_app_permission "$APP_FOR_CAN_USE" "$MCP_SP_ID" "CAN_USE"; then
        echo "  [$APP_FOR_CAN_USE] ✓ granted CAN_USE to MCP service principal ($MCP_FOR_CAN_USE)"
    else
        echo "  [$APP_FOR_CAN_USE] ✗ failed to grant CAN_USE to MCP service principal ($MCP_FOR_CAN_USE)"
        FAILED=$((FAILED + 1))
    fi
else
    echo "  [cross-app] SKIP — could not resolve both app service principals"
fi

# ── UC schema ALL_PRIVILEGES for both SPs ────────────────────────────────────
# The SP must be able to CREATE OR REPLACE views/tables in the registry schema
# even if those objects were previously created by a different principal (e.g.
# the deploying user in a notebook run). Without ALL_PRIVILEGES on the schema,
# CREATE OR REPLACE VIEW on an object owned by another principal fails with
# "Permission denied … MANAGE on Table/View".
# Source the deploy config for catalog + schema values; skip silently if absent.
_UC_CATALOG=""
_UC_SCHEMA=""
if [[ -f scripts/deploy.config.sh ]]; then
    # shellcheck disable=SC1091
    _UC_CATALOG="$(source scripts/deploy.config.sh 2>/dev/null; echo "${REGISTRY_CATALOG:-}")"
    _UC_SCHEMA="$(source scripts/deploy.config.sh 2>/dev/null; echo "${REGISTRY_SCHEMA:-}")"
fi
# Allow env overrides
_UC_CATALOG="${UC_CATALOG:-$_UC_CATALOG}"
_UC_SCHEMA="${UC_SCHEMA:-$_UC_SCHEMA}"

if [[ -n "$_UC_CATALOG" && -n "$_UC_SCHEMA" ]]; then
    echo
    echo "=== UC schema ALL_PRIVILEGES: ${_UC_CATALOG}.${_UC_SCHEMA} ==="
    for sp in "${APP_SP_ID:-}" "${MCP_SP_ID:-}"; do
        [[ -z "$sp" ]] && continue
        if databricks grants update SCHEMA "${_UC_CATALOG}.${_UC_SCHEMA}" \
            --json "{\"changes\":[{\"principal\":\"${sp}\",\"add\":[\"ALL_PRIVILEGES\"]}]}" \
            >/dev/null 2>&1; then
            echo "  ✓ ALL_PRIVILEGES on ${_UC_CATALOG}.${_UC_SCHEMA} → $sp"
        else
            echo "  ⚠ UC schema grant failed for $sp — run manually:"
            echo "    databricks grants update SCHEMA ${_UC_CATALOG}.${_UC_SCHEMA} \\"
            echo "      --json '{\"changes\":[{\"principal\":\"${sp}\",\"add\":[\"ALL_PRIVILEGES\"]}]}'"
        fi
    done
else
    echo "  [UC schema] SKIP — REGISTRY_CATALOG/REGISTRY_SCHEMA not resolved (source deploy.config.sh or set UC_CATALOG/UC_SCHEMA env vars)"
fi

echo
if [[ $FAILED -eq 0 ]]; then
    echo "=== Done — all apps bootstrapped ==="
    exit 0
else
    echo "=== Done with $FAILED failure(s) — see messages above ==="
    exit 1
fi
