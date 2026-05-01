#!/usr/bin/env bash
set -euo pipefail

# ── OntoBricks Deployment Script ────────────────────────────────────
# Wraps ``databricks bundle`` for the **dev sandbox bundle**
# (``databricks.yml``), which exposes the dev sandbox app ``ontobricks-020``
# (resource key ``ontobricks_dev_app``).
#
# The production ``ontobricks`` app and the ``mcp-ontobricks`` MCP
# server are no longer managed by this bundle — they were carved out
# on 2026-04-27. Restore the previous bundle from git history if you
# need to deploy them.
#
# Usage:
#   scripts/deploy.sh                     # deploy + run (target: dev-lakebase)
#   scripts/deploy.sh -t dev              # Volume-only sandbox (no Lakebase binding)
#   scripts/deploy.sh --no-run            # deploy artifacts without starting the app
#   scripts/deploy.sh --bind              # also (re)bind the existing app to this bundle
#
# Targets (see ``databricks.yml``):
#   - ``dev``           Volume-only registry backend
#   - ``dev-lakebase``  Volume + Lakebase Autoscaling Postgres binding (default)
#
# Prerequisites:
#   - Databricks CLI >= 0.250.0
#   - Authenticated profile (``databricks auth login --host ...``)
#   - ``databricks.yml`` at the project root

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."

TARGET="dev-lakebase"
NO_RUN=false
DO_BIND=false

APP_RESOURCE_KEY="ontobricks_dev_app"
# Must match ``resources.apps.ontobricks_dev_app.name`` in ``databricks.yml``
# (used for ``databricks apps get`` / Lakebase bootstrap SP lookup).
APP_NAME="ontobricks-020"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -t|--target) TARGET="$2"; shift 2 ;;
        --no-run)    NO_RUN=true; shift ;;
        --bind)      DO_BIND=true; shift ;;
        -h|--help)
            sed -n '3,28p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

echo "=== OntoBricks Deployment (DAB) ==="
echo "Target: $TARGET"
echo "App:    $APP_NAME ($APP_RESOURCE_KEY)"

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
    echo "--- Binding existing app ---"
    databricks bundle deployment bind "$APP_RESOURCE_KEY" "$APP_NAME" -t "$TARGET" --auto-approve 2>/dev/null \
        && echo "Bound $APP_RESOURCE_KEY → $APP_NAME" \
        || echo "$APP_RESOURCE_KEY: new app or already bound"
fi

# ── 5. Run ───────────────────────────────────────────────────────────
if ! $NO_RUN; then
    echo ""
    echo "--- Starting $APP_NAME ---"
    databricks bundle run "$APP_RESOURCE_KEY" -t "$TARGET"
fi

# ── 6. Verify ────────────────────────────────────────────────────────
echo ""
echo "--- Verification ---"
STATUS=$(databricks apps get "$APP_NAME" -o json 2>/dev/null | python3 -c "
import sys, json
d = json.load(sys.stdin)
state = d.get('app_status',{}).get('state','UNKNOWN')
url   = d.get('url','')
print(f'{state}  {url}')
" 2>/dev/null || echo "NOT DEPLOYED")
printf "  %-20s %s\n" "$APP_NAME" "$STATUS"

# ── 7. App self-permissions (first-deploy bootstrap) ───────────────
# The app's service principal needs CAN_MANAGE on its OWN app so the
# middleware can read the ACL to resolve admin/app-user roles.
# Idempotent — safe to re-run.
echo ""
echo "--- App self-permissions ---"
chmod +x scripts/bootstrap-app-permissions.sh
scripts/bootstrap-app-permissions.sh "$APP_NAME" || true

# ── 8. Lakebase schema permissions (dev-lakebase only) ─────────────
# When the postgres resource binding is unbound/rebound — which happens
# every time we redeploy with a different target — Lakebase loses the
# schema-level GRANTs the app SP needs (USAGE on the schema, DML on
# tables, USAGE/SELECT/UPDATE on sequences). The runtime then fails
# with "Role '<sp-id>' lacks USAGE on schema 'ontobricks_registry'".
#
# Re-running the bootstrap is idempotent, so we do it on every
# Lakebase-target deploy. Failures are tolerated (e.g. first deploy
# before the schema is initialised, or psql not installed) — the
# script prints actionable guidance in that case.
if [[ "$TARGET" == "dev-lakebase" ]]; then
    echo ""
    echo "--- Lakebase schema permissions ---"
    chmod +x scripts/bootstrap-lakebase-perms.sh
    if ! scripts/bootstrap-lakebase-perms.sh -a "$APP_NAME"; then
        echo ""
        echo "  ⚠ Lakebase permission bootstrap did not complete cleanly."
        echo "    If the registry schema does not exist yet, initialise it"
        echo "    from Settings > Registry > Initialize and re-run:"
        echo "      scripts/bootstrap-lakebase-perms.sh -a $APP_NAME"
    fi
fi

echo ""
echo "=== Done ==="
echo ""
echo "Post-deployment reminders:"
echo "  1. Bind resources in the Databricks Apps UI if this is a fresh app:"
echo "       sql-warehouse + volume (always), postgres (only on dev-lakebase)"
echo "  2. Initialize the registry on first deploy: Settings > Registry > Initialize"
echo "  3. Resource bindings carry over between deploys — only needed once"
echo "  4. To switch backends, redeploy with the matching target:"
echo "       scripts/deploy.sh -t dev           # Volume-only"
echo "       scripts/deploy.sh -t dev-lakebase  # Lakebase Postgres"
