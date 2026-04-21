#!/usr/bin/env bash
set -euo pipefail

# ── OntoBricks Deployment Script ────────────────────────────────────
# Uses Databricks Asset Bundles (DAB) to deploy both the main app and
# the MCP companion server in a single command.
#
# Usage:
#   scripts/deploy.sh              # deploy + run main app (dev target)
#   scripts/deploy.sh --all        # deploy + run both apps
#   scripts/deploy.sh --mcp-only   # deploy + run MCP server only
#   scripts/deploy.sh -t prod      # deploy to production target
#   scripts/deploy.sh --no-run     # deploy without starting apps
#   scripts/deploy.sh --bind       # also bind resources post-deploy
#
# Prerequisites:
#   - Databricks CLI >= 0.250.0
#   - Authenticated profile (databricks auth login --host ...)
#   - databricks.yml at the project root

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."
TARGET="dev"
RUN_MAIN=true
RUN_MCP=false
NO_RUN=false
DO_BIND=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        -t|--target) TARGET="$2"; shift 2 ;;
        --all)       RUN_MCP=true; shift ;;
        --mcp-only)  RUN_MAIN=false; RUN_MCP=true; shift ;;
        --no-run)    NO_RUN=true; shift ;;
        --bind)      DO_BIND=true; shift ;;
        -h|--help)
            sed -n '3,12p' "$0" | sed 's/^# //'
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

echo "=== OntoBricks Deployment (DAB) ==="
echo "Target: $TARGET"

# ── 1. Verify CLI auth ──────────────────────────────────────────────
if ! databricks current-user me &>/dev/null; then
    echo "ERROR: Not authenticated. Run: databricks auth login --host https://<workspace>"
    exit 1
fi
DATABRICKS_USERNAME=$(databricks current-user me -o json | python3 -c "import sys,json; print(json.load(sys.stdin)['userName'])")
echo "User:   $DATABRICKS_USERNAME"

# ── 2. Validate ─────────────────────────────────────────────────────
echo ""
echo "--- Validating bundle ---"
databricks bundle validate -t "$TARGET"

# ── 3. Deploy ────────────────────────────────────────────────────────
echo ""
echo "--- Deploying (target: $TARGET) ---"
databricks bundle deploy -t "$TARGET"

# ── 4. Bind resources (first-time only) ────────────────────────────
if $DO_BIND; then
    echo ""
    echo "--- Binding existing apps ---"
    databricks bundle deployment bind ontobricks_app ontobricks -t "$TARGET" --auto-approve 2>/dev/null \
        && echo "Bound ontobricks_app" \
        || echo "ontobricks_app: new app or already bound"
    databricks bundle deployment bind mcp_ontobricks_app mcp-ontobricks -t "$TARGET" --auto-approve 2>/dev/null \
        && echo "Bound mcp_ontobricks_app" \
        || echo "mcp_ontobricks_app: new app or already bound"
fi

# ── 5. Run ───────────────────────────────────────────────────────────
if ! $NO_RUN; then
    if $RUN_MAIN; then
        echo ""
        echo "--- Starting ontobricks ---"
        databricks bundle run ontobricks_app -t "$TARGET"
    fi
    if $RUN_MCP; then
        echo ""
        echo "--- Starting mcp-ontobricks ---"
        databricks bundle run mcp_ontobricks_app -t "$TARGET"
    fi
fi

# ── 6. Verify ────────────────────────────────────────────────────────
echo ""
echo "--- Verification ---"
for APP in ontobricks mcp-ontobricks; do
    STATUS=$(databricks apps get "$APP" -o json 2>/dev/null | python3 -c "
import sys, json
d = json.load(sys.stdin)
state = d.get('app_status',{}).get('state','UNKNOWN')
url   = d.get('url','')
print(f'{state}  {url}')
" 2>/dev/null || echo "NOT DEPLOYED")
    printf "  %-20s %s\n" "$APP" "$STATUS"
done

## ── 7. App self-permissions (first-deploy bootstrap) ───────────────
# Each app's service principal needs CAN_MANAGE on its OWN app so the
# middleware can read the ACL to resolve admin/app-user roles.  Without
# this, the first request hits /access-denied even for CAN_MANAGE users.
# Safe to re-run — it's idempotent.
echo ""
echo "--- App self-permissions ---"
chmod +x scripts/bootstrap-app-permissions.sh
if $RUN_MAIN && $RUN_MCP; then
    scripts/bootstrap-app-permissions.sh ontobricks mcp-ontobricks || true
elif $RUN_MAIN; then
    scripts/bootstrap-app-permissions.sh ontobricks || true
elif $RUN_MCP; then
    scripts/bootstrap-app-permissions.sh mcp-ontobricks || true
fi

## ── 8. Cross-app permissions (MCP → main app) ──────────────────────
# The MCP server's service principal needs CAN_USE on the main app to
# call its REST API.  Attempt to discover and grant automatically.
if $RUN_MCP || [[ "$RUN_MAIN" == "true" && "$RUN_MCP" == "false" ]]; then
    echo ""
    echo "--- Cross-app permissions (MCP → ontobricks) ---"
    MCP_SP_NAME=$(databricks apps get mcp-ontobricks -o json 2>/dev/null | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d.get('service_principal_client_id', ''))
" 2>/dev/null || true)

    if [ -n "$MCP_SP_NAME" ] && [ "$MCP_SP_NAME" != "None" ]; then
        echo "  MCP service principal: $MCP_SP_NAME"
        databricks apps update-permissions ontobricks --json "{
            \"access_control_list\": [{
                \"service_principal_name\": \"$MCP_SP_NAME\",
                \"permission_level\": \"CAN_USE\"
            }]
        }" 2>/dev/null \
            && echo "  ✓ Granted CAN_USE on ontobricks for MCP server SP" \
            || echo "  ⚠ Could not auto-grant — see post-deployment reminders below"
    else
        echo "  ⚠ Could not determine MCP SP — grant manually (see reminders below)"
    fi
fi

echo ""
echo "=== Done ==="
echo ""
echo "Post-deployment reminders:"
echo "  1. Bind resources in the Databricks Apps UI (sql-warehouse + volume)"
echo "  2. Initialize the registry if this is the first deploy (Settings > Registry > Initialize)"
echo "  3. Resource bindings carry over between deploys — only needed once"
echo "  4. MCP cross-app auth: the MCP app's service principal needs CAN_USE"
echo "     on the 'ontobricks' app.  If auto-grant above failed, run:"
echo "       databricks apps get mcp-ontobricks -o json  # find service_principal_id"
echo "       databricks apps update-permissions ontobricks --json '{...}'"
