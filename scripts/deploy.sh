#!/usr/bin/env bash
set -euo pipefail

# ── OntoBricks Deployment Script ────────────────────────────────────
# Single-entry orchestrator for everything `make deploy` does:
#
#   1. Source `scripts/deploy.config.sh` — the single source of truth
#      for app names, DAB target, DAB variable overrides, and the
#      runtime fallbacks rendered into `app.yaml`. Edit that file (or
#      override any variable via env) to change deployment values.
#   2. Render `app.yaml` from `app.yaml.template` so the runtime env
#      block matches the config.
#   3. Validate + deploy the bundle, passing every DAB variable as
#      `--var=key=value` so `databricks.yml` stays a pure structural
#      declaration.
#   4. (Optional) bind the existing Apps resource to this bundle.
#   5. Start the app (unless `--no-run`).
#   6. Bootstrap the app SP self-permissions.
#   7. (Lakebase target only) bootstrap the Postgres schema GRANTs.
#
# The bundle (databricks.yml) only manages the dev sandbox apps:
# `${APP_NAME}` (FastAPI UI) and `${MCP_APP_NAME}` (MCP companion).
# The production `ontobricks` and `mcp-ontobricks` apps were carved
# out on 2026-04-27 and live in a different repo/bundle.
#
# Usage:
#   scripts/deploy.sh                     # deploy + run (uses DAB_TARGET from config)
#   scripts/deploy.sh -t dev              # override target on the fly
#   scripts/deploy.sh --no-run            # deploy artifacts without starting the app
#   scripts/deploy.sh --bind              # also (re)bind the existing app to this bundle
#   scripts/deploy.sh --no-bootstrap      # skip steps 6 + 7 (perm/Lakebase bootstrap)
#   scripts/deploy.sh --skip-app-yaml     # skip step 2 (use the existing app.yaml as-is)
#
# Targets (declared in `databricks.yml`):
#   - `dev`           Volume-only registry backend
#   - `dev-lakebase`  Volume + Lakebase Autoscaling Postgres binding (default)
#
# Prerequisites:
#   - Databricks CLI >= 0.250.0
#   - Authenticated profile (`databricks auth login --host ...`)
#   - `databricks.yml` + `app.yaml.template` + `scripts/deploy.config.sh` at the project root

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."

# ── 0. Load configuration ───────────────────────────────────────────
CONFIG_FILE="scripts/deploy.config.sh"
if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "ERROR: $CONFIG_FILE not found." >&2
    exit 1
fi
# shellcheck disable=SC1090
. "$CONFIG_FILE"

# Local CLI flags — override (don't pollute) what the config exported.
TARGET="$DAB_TARGET"
NO_RUN=false
DO_BIND=false
DO_BOOTSTRAP=true
RENDER_APP_YAML=true

while [[ $# -gt 0 ]]; do
    case "$1" in
        -t|--target)        TARGET="$2"; shift 2 ;;
        --no-run)           NO_RUN=true; shift ;;
        --bind)             DO_BIND=true; shift ;;
        --no-bootstrap)     DO_BOOTSTRAP=false; shift ;;
        --skip-app-yaml)    RENDER_APP_YAML=false; shift ;;
        -h|--help)
            sed -n '4,42p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

# ── DAB variable overrides ──────────────────────────────────────────
# Composed from the env exported by `deploy.config.sh`. If you add a
# variable to `databricks.yml > variables:`, also surface it in
# `deploy.config.sh` and add a `--var=` line here.
_dab_var_overrides=(
    "--var=warehouse_id=${WAREHOUSE_ID}"
    "--var=registry_catalog=${REGISTRY_CATALOG}"
    "--var=registry_schema=${REGISTRY_SCHEMA}"
    "--var=registry_volume=${REGISTRY_VOLUME}"
    "--var=lakebase_project=${LAKEBASE_PROJECT}"
    "--var=lakebase_branch=${LAKEBASE_BRANCH}"
    "--var=lakebase_database_resource_segment=${LAKEBASE_DATABASE_RESOURCE_SEGMENT}"
    "--var=lakebase_registry_schema=${LAKEBASE_REGISTRY_SCHEMA}"
)

echo "=== OntoBricks Deployment (DAB) ==="
echo "Config  : $CONFIG_FILE"
echo "Target  : $TARGET"
echo "App     : $APP_NAME ($APP_RESOURCE_KEY)"
echo "MCP app : $MCP_APP_NAME ($MCP_APP_RESOURCE_KEY)"
echo "Registry: ${REGISTRY_CATALOG}.${REGISTRY_SCHEMA}.${REGISTRY_VOLUME}"
if [[ "$TARGET" == *lakebase* ]]; then
    echo "Lakebase: projects/${LAKEBASE_PROJECT}/branches/${LAKEBASE_BRANCH}/databases/${LAKEBASE_DATABASE_RESOURCE_SEGMENT}"
fi

# ── 1. Verify CLI auth ──────────────────────────────────────────────
if ! databricks current-user me &>/dev/null; then
    echo "ERROR: Not authenticated. Run: databricks auth login --host https://<workspace>" >&2
    exit 1
fi
DATABRICKS_USERNAME=$(databricks current-user me -o json \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['userName'])")
echo "User    : $DATABRICKS_USERNAME"

# ── 2. Render app.yaml from template ───────────────────────────────
if $RENDER_APP_YAML; then
    echo ""
    echo "--- Rendering app.yaml from app.yaml.template ---"
    python3 scripts/_render-app-yaml.py
else
    echo ""
    echo "--- (skipping app.yaml render — using existing file) ---"
fi

# ── 3. Validate ─────────────────────────────────────────────────────
echo ""
echo "--- Validating bundle ---"
databricks bundle validate -t "$TARGET" "${_dab_var_overrides[@]}"

# ── 4. Deploy ────────────────────────────────────────────────────────
echo ""
echo "--- Deploying (target: $TARGET) ---"
databricks bundle deploy -t "$TARGET" "${_dab_var_overrides[@]}"

# ── 5. Bind resources (first-time only) ────────────────────────────
if $DO_BIND; then
    echo ""
    echo "--- Binding existing app ---"
    databricks bundle deployment bind "$APP_RESOURCE_KEY" "$APP_NAME" \
        -t "$TARGET" --auto-approve 2>/dev/null \
        && echo "Bound $APP_RESOURCE_KEY → $APP_NAME" \
        || echo "$APP_RESOURCE_KEY: new app or already bound"
fi

# ── 6. Run ───────────────────────────────────────────────────────────
if ! $NO_RUN; then
    echo ""
    echo "--- Starting $APP_NAME ---"
    databricks bundle run "$APP_RESOURCE_KEY" -t "$TARGET" "${_dab_var_overrides[@]}"
fi

# ── 7. Verify ────────────────────────────────────────────────────────
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

if ! $DO_BOOTSTRAP; then
    echo ""
    echo "(skipping bootstrap steps per --no-bootstrap)"
    echo ""
    echo "=== Done ==="
    exit 0
fi

# ── 8. App self-permissions (first-deploy bootstrap) ───────────────
# The app's service principal needs CAN_MANAGE on its OWN app so the
# middleware can read the ACL to resolve admin/app-user roles.
# Idempotent — safe to re-run. The bootstrap script reads APP_NAME /
# MCP_APP_NAME from the env we exported via deploy.config.sh.
echo ""
echo "--- App self-permissions ---"
chmod +x scripts/bootstrap-app-permissions.sh
scripts/bootstrap-app-permissions.sh "$APP_NAME" "$MCP_APP_NAME" || true

# ── 9. Lakebase schema permissions (dev-lakebase only) ─────────────
# When the postgres resource binding is unbound/rebound — which happens
# every time we redeploy with a different target — Lakebase loses the
# schema-level GRANTs the app SP needs (USAGE on the schema, DML on
# tables, USAGE/SELECT/UPDATE on sequences). The runtime then fails
# with "Role '<sp-id>' lacks USAGE on schema '${LAKEBASE_BOOTSTRAP_SCHEMA}'".
#
# Re-running the bootstrap is idempotent, so we do it on every
# Lakebase-target deploy. Failures are tolerated (e.g. first deploy
# before the schema is initialised, or psql not installed) — the
# script prints actionable guidance in that case.
if [[ "$TARGET" == *lakebase* ]]; then
    echo ""
    echo "--- Lakebase schema permissions ---"
    chmod +x scripts/bootstrap-lakebase-perms.sh
    if ! scripts/bootstrap-lakebase-perms.sh \
            -i "$LAKEBASE_BOOTSTRAP_INSTANCE" \
            -d "$LAKEBASE_BOOTSTRAP_DATABASE" \
            -s "$LAKEBASE_BOOTSTRAP_SCHEMA" \
            -a "$APP_NAME" \
            -a "$MCP_APP_NAME"; then
        echo ""
        echo "  ⚠ Lakebase permission bootstrap did not complete cleanly."
        echo "    If the registry schema does not exist yet, initialise it"
        echo "    from Settings > Registry > Initialize and re-run:"
        echo "      scripts/bootstrap-lakebase-perms.sh \\"
        echo "        -i $LAKEBASE_BOOTSTRAP_INSTANCE \\"
        echo "        -d $LAKEBASE_BOOTSTRAP_DATABASE \\"
        echo "        -s $LAKEBASE_BOOTSTRAP_SCHEMA \\"
        echo "        -a $APP_NAME -a $MCP_APP_NAME"
    fi
fi

echo ""
echo "=== Done ==="
echo ""
echo "Post-deployment reminders:"
echo "  1. To change ANY deployment value, edit scripts/deploy.config.sh"
echo "     and re-run \`make deploy\` — never edit app.yaml directly"
echo "     (it is generated from app.yaml.template + the config)."
echo "  2. Bind resources in the Databricks Apps UI if this is a fresh app:"
echo "       sql-warehouse + volume (always), postgres (only on dev-lakebase)"
echo "  3. Initialize the registry on first deploy: Settings > Registry > Initialize"
echo "  4. Resource bindings carry over between deploys — only needed once"
echo "  5. To switch backends, redeploy with the matching target:"
echo "       scripts/deploy.sh -t dev           # Volume-only"
echo "       scripts/deploy.sh -t dev-lakebase  # Lakebase Postgres"
