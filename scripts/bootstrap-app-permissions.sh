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
#   scripts/bootstrap-app-permissions.sh ontobricks-020       # explicit
#   scripts/bootstrap-app-permissions.sh a b c           # bootstrap several apps
#
# Prerequisites:
#   - Databricks CLI authenticated (databricks auth login ...)
#   - The apps already exist (run `make deploy` first)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."

# This bundle only manages the dev sandbox app. The production
# ``ontobricks`` and ``mcp-ontobricks`` apps were carved out on
# 2026-04-27 and live in a different repo/bundle.
DEFAULT_APPS=("ontobricks-020" "mcp-ontobricks")

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

grant_self_permission() {
    local app="$1"

    local sp_id
    sp_id=$(databricks apps get "$app" -o json 2>/dev/null | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(2)
print(d.get('service_principal_client_id') or '')
" 2>/dev/null || true)

    if [[ -z "$sp_id" || "$sp_id" == "None" ]]; then
        echo "  [$app] SKIP — could not resolve service principal (app may not exist yet)"
        return 1
    fi

    echo "  [$app] service principal: $sp_id"

    # Idempotent: `update-permissions` merges/overwrites ACL entries for the
    # listed principals without touching others. Re-running has no effect.
    if databricks apps update-permissions "$app" --json "{
        \"access_control_list\": [{
            \"service_principal_name\": \"$sp_id\",
            \"permission_level\": \"CAN_MANAGE\"
        }]
    }" >/dev/null 2>&1; then
        echo "  [$app] ✓ granted CAN_MANAGE to own service principal"
        return 0
    else
        echo "  [$app] ✗ failed to grant CAN_MANAGE — you need CAN_MANAGE on the app to run this"
        return 1
    fi
}

FAILED=0
for app in "${APPS[@]}"; do
    if ! grant_self_permission "$app"; then
        FAILED=$((FAILED + 1))
    fi
done

echo
if [[ $FAILED -eq 0 ]]; then
    echo "=== Done — all apps bootstrapped ==="
    exit 0
else
    echo "=== Done with $FAILED failure(s) — see messages above ==="
    exit 1
fi
