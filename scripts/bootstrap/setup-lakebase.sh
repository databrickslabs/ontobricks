#!/usr/bin/env bash
# ── OntoBricks: Lakebase project setup ──────────────────────────────────────
#
# Creates a Lakebase project that is compatible with the Synced Tables API
# (POST /api/2.0/database/synced_tables), which requires the project to be
# registered as a database instance (POST /api/2.0/database/instances).
#
# Background:
#   Lakebase has two project-creation APIs:
#     • NEW: POST /api/2.0/postgres/projects     → autoscaling-only project.
#            NOT compatible with the Synced Tables API.
#     • OLD: POST /api/2.0/database/instances    → autoscaling project that
#            also registers in /api/2.0/database/instances, making it
#            compatible with the Synced Tables API.
#
#   OntoBricks uses the Synced Tables API in "managed_synced" Digital Twin
#   build mode.  Use this script (not the Databricks UI "New project" button,
#   which calls the NEW API) to provision your Lakebase project.
#
# Usage:
#   ./scripts/bootstrap/setup-lakebase.sh [OPTIONS]
#
# Options:
#   --name      NAME       Project name (default: ontobricks-demo)
#   --capacity  CU_N       Compute capacity: CU_1, CU_2, CU_4 (default: CU_2)
#   --branch    BRANCH     Initial branch to create (default: production)
#   --database  DBNAME     Postgres database to create inside the project
#                          (default: ontobricks_demo)
#   --profile   PROFILE    Databricks CLI profile (default: DEFAULT)
#   --wait      N          Max seconds to wait for AVAILABLE state (default: 120)
#   --dry-run              Print what would be done without doing it
#
# After this script succeeds:
#   1. Update DEFAULT_LAKEBASE_PROJECT / DEFAULT_LAKEBASE_DATABASE in
#      scripts/deploy.config.sh (datname). deploy.sh resolves the db-…
#      segment automatically; optional override: LAKEBASE_DATABASE_RESOURCE_SEGMENT.
#   2. Rebind the postgres resource in the Databricks Apps UI if needed.
#   3. Run: make deploy
#   4. Run: make bootstrap-lakebase
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# scripts/bootstrap/ → scripts/ → repo root (one level was wrong after the reorg — #133).
cd "$SCRIPT_DIR/../.."

# Preflight CLI auth + tooling before any API calls.
chmod +x scripts/_internal/check-deploy-prerequisites.sh
scripts/_internal/check-deploy-prerequisites.sh --provision

# ── Defaults ────────────────────────────────────────────────────────────────
DEFAULT_NAME="ontobricks-demo"
DEFAULT_CAPACITY="CU_2"
DEFAULT_BRANCH="production"
DEFAULT_DATABASE="ontobricks_demo"
DEFAULT_PROFILE="DEFAULT"
DEFAULT_WAIT=120
DRY_RUN=false

# ── Argument parsing ─────────────────────────────────────────────────────────
NAME="$DEFAULT_NAME"
CAPACITY="$DEFAULT_CAPACITY"
BRANCH="$DEFAULT_BRANCH"
DATABASE="$DEFAULT_DATABASE"
PROFILE="$DEFAULT_PROFILE"
WAIT="$DEFAULT_WAIT"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --name)      NAME="$2";     shift 2 ;;
    --capacity)  CAPACITY="$2"; shift 2 ;;
    --branch)    BRANCH="$2";   shift 2 ;;
    --database)  DATABASE="$2"; shift 2 ;;
    --profile)   PROFILE="$2";  shift 2 ;;
    --wait)      WAIT="$2";     shift 2 ;;
    --dry-run)   DRY_RUN=true;  shift   ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

# ── Helpers ──────────────────────────────────────────────────────────────────
_log() { echo "[setup-lakebase] $*"; }
_ok()  { echo "[setup-lakebase] OK  $*"; }
_err() { echo "[setup-lakebase] ERR $*" >&2; exit 1; }
_dry() { echo "[setup-lakebase] DRY $*"; }

# ── Resolve Databricks host + token from CLI config ──────────────────────────
# `databricks auth env` prints JSON on current CLIs and `export KEY="…"`
# lines on older ones — handle both. Always exits 0 so the empty-value
# guard below is reached (a bare grep would trip `set -o pipefail` and
# abort the script before it can print a diagnostic).
_cfg() {
  databricks auth env --profile "$PROFILE" 2>/dev/null | python3 -c "
import sys, json, re
raw = sys.stdin.read()
try:
    print(json.loads(raw).get('env', {}).get(sys.argv[1], ''))
except ValueError:
    m = re.search(r'^export %s=\"(.*)\"$' % re.escape(sys.argv[1]), raw, re.M)
    print(m.group(1) if m else '')
" "$1"
}

HOST=$(_cfg DATABRICKS_HOST)
TOKEN=$(_cfg DATABRICKS_TOKEN)

# OAuth profiles (auth_type=databricks-cli) expose no DATABRICKS_TOKEN in
# `auth env` — mint a short-lived OAuth access token instead.
if [[ -z "$TOKEN" ]]; then
  TOKEN=$(databricks auth token --profile "$PROFILE" 2>/dev/null | python3 -c "
import sys, json
print(json.load(sys.stdin).get('access_token', ''))
" 2>/dev/null || true)
fi

[[ -z "$HOST" || -z "$TOKEN" ]] && \
  _err "Could not resolve DATABRICKS_HOST / DATABRICKS_TOKEN from profile '$PROFILE'."

HOST="${HOST%/}"  # strip trailing slash

# ── Dry-run guard ─────────────────────────────────────────────────────────────
if $DRY_RUN; then
  _dry "Would create Lakebase instance:"
  _dry "  name=$NAME  capacity=$CAPACITY  pg_native_login=true"
  _dry "  then create database '$DATABASE' inside branch '$BRANCH'"
  _dry "  host=$HOST"
  exit 0
fi

# ── Step 1: Check name availability ──────────────────────────────────────────
_log "Checking if '$NAME' already exists in database/instances …"
EXISTING=$(curl -sf "$HOST/api/2.0/database/instances/$NAME" \
  -H "Authorization: Bearer $TOKEN" 2>/dev/null || true)

if echo "$EXISTING" | grep -q '"state"'; then
  STATE=$(echo "$EXISTING" | python3 -c "import sys,json; print(json.load(sys.stdin).get('state','?'))")
  HOST_DNS=$(echo "$EXISTING" | python3 -c "import sys,json; print(json.load(sys.stdin).get('read_write_dns','?'))")
  _ok "Instance '$NAME' already exists (state=$STATE, host=$HOST_DNS)"
  _log "Skipping creation — proceeding to database step."
else
  # ── Step 2: Create via old instances API ─────────────────────────────────
  _log "Creating Lakebase instance '$NAME' (capacity=$CAPACITY) via /api/2.0/database/instances …"
  _log "  NOTE: The Databricks UI 'New project' button uses a different API that is"
  _log "        NOT compatible with the Synced Tables API. This script uses the correct one."
  PAYLOAD=$(printf '{"name":"%s","capacity":"%s","enable_pg_native_login":true}' "$NAME" "$CAPACITY")
  RESULT=$(curl -sf -X POST "$HOST/api/2.0/database/instances" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "$PAYLOAD" 2>&1) || {
      ERR_MSG=$(echo "$RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('message','unknown'))" 2>/dev/null || echo "$RESULT")
      _err "Failed to create instance: $ERR_MSG"
    }

  _ok "Instance creation request accepted."

  # ── Step 3: Wait for AVAILABLE ─────────────────────────────────────────────
  _log "Waiting for '$NAME' to become AVAILABLE (max ${WAIT}s) …"
  ELAPSED=0
  while true; do
    sleep 5
    ELAPSED=$((ELAPSED + 5))
    STATUS_JSON=$(curl -sf "$HOST/api/2.0/database/instances/$NAME" \
      -H "Authorization: Bearer $TOKEN" 2>/dev/null || true)
    STATE=$(echo "$STATUS_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('state','?'))" 2>/dev/null || echo "?")
    _log "  state=$STATE (${ELAPSED}s elapsed)"
    if [[ "$STATE" == "AVAILABLE" ]]; then
      HOST_DNS=$(echo "$STATUS_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('read_write_dns','?'))")
      _ok "Instance '$NAME' is AVAILABLE at $HOST_DNS"
      break
    fi
    if [[ $ELAPSED -ge $WAIT ]]; then
      _err "Timed out after ${WAIT}s waiting for AVAILABLE (state=$STATE). Check the Databricks UI."
    fi
  done
fi

# ── Step 4: Get production branch endpoint ────────────────────────────────────
_log "Resolving endpoint for branch '$BRANCH' …"
EP_JSON=$(curl -sf "$HOST/api/2.0/postgres/projects/$NAME/branches/$BRANCH/endpoints" \
  -H "Authorization: Bearer $TOKEN" 2>/dev/null || true)
EP_HOST=$(echo "$EP_JSON" | python3 -c "
import sys, json
data = json.load(sys.stdin)
eps = data if isinstance(data, list) else (data or {}).get('endpoints', [])
for ep in eps:
    st = ep.get('status', {})
    if st.get('state') in ('EP_STATE_ACTIVE', 'AVAILABLE', None):
        print(st.get('host', st.get('dns', '')))
        break
" 2>/dev/null || true)

if [[ -z "$EP_HOST" ]]; then
  _log "  (endpoint not yet active or API returned empty — this is normal for new projects)"
else
  _ok "Branch endpoint: $EP_HOST"
fi

# ── Step 5: Create / confirm the Postgres database ───────────────────────────
_log "Listing databases in '$NAME/$BRANCH' …"
DB_LIST=$(databricks postgres list-databases "projects/$NAME/branches/$BRANCH" \
  --profile "$PROFILE" -o json 2>/dev/null || echo '{"databases":[]}')

DB_SEGMENT=$(echo "$DB_LIST" | python3 -c "
import sys, json
data = json.load(sys.stdin)
# Current CLIs print a bare JSON array; older ones wrap it in {'databases': […]}.
dbs = data if isinstance(data, list) else (data or {}).get('databases', [])
for db in dbs:
    status = db.get('status', {})
    pg_db  = status.get('postgres_database', '')
    seg    = db.get('name','').split('/')[-1]
    if pg_db == sys.argv[1] or seg == sys.argv[1]:
        print(seg)
        break
" "$DATABASE" 2>/dev/null || true)

if [[ -n "$DB_SEGMENT" ]]; then
  _ok "Database '$DATABASE' already exists: segment='$DB_SEGMENT'"
else
  _log "Creating Postgres database '$DATABASE' inside '$NAME/$BRANCH' …"
  # The databases API requires spec.postgres_database plus spec.role in
  # full resource-path form; resolve the branch's first role (the
  # creator's) rather than hardcoding one.
  ROLE_PATH=$(curl -sf "$HOST/api/2.0/postgres/projects/$NAME/branches/$BRANCH/roles" \
    -H "Authorization: Bearer $TOKEN" 2>/dev/null | python3 -c "
import sys, json
data = json.load(sys.stdin)
roles = data if isinstance(data, list) else (data or {}).get('roles', [])
print(roles[0]['name'] if roles else '')
" 2>/dev/null || true)
  [[ -z "$ROLE_PATH" ]] && \
    _err "Could not resolve a Postgres role on '$NAME/$BRANCH' to own '$DATABASE'."

  DB_API_OUT=$(curl -sf -X POST \
    "$HOST/api/2.0/postgres/projects/$NAME/branches/$BRANCH/databases" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"spec\":{\"postgres_database\":\"$DATABASE\",\"role\":\"$ROLE_PATH\"}}" 2>&1) || {
      _log "  API output: $DB_API_OUT"
      _err "Failed to create database '$DATABASE' on '$NAME/$BRANCH'."
    }

  # Re-list to get the db-* segment
  sleep 3
  DB_LIST=$(databricks postgres list-databases "projects/$NAME/branches/$BRANCH" \
    --profile "$PROFILE" -o json 2>/dev/null || echo '{"databases":[]}')
  DB_SEGMENT=$(echo "$DB_LIST" | python3 -c "
import sys, json
data = json.load(sys.stdin)
dbs = data if isinstance(data, list) else (data or {}).get('databases', [])
for db in dbs:
    pg_db = db.get('status', {}).get('postgres_database', '')
    seg   = db.get('name','').split('/')[-1]
    if pg_db == sys.argv[1] or seg == sys.argv[1]:
        print(seg)
        break
" "$DATABASE" 2>/dev/null || true)
fi

# ── Step 6: Print summary ─────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  Lakebase project setup complete                             ║"
echo "╠══════════════════════════════════════════════════════════════╣"
printf "║  Instance name  : %-43s║\n" "$NAME"
printf "║  Branch         : %-43s║\n" "$BRANCH"
printf "║  Database       : %-43s║\n" "$DATABASE"
printf "║  DB segment     : %-43s║\n" "${DB_SEGMENT:-<not resolved — see list-databases>}"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  NEXT STEPS:                                                 ║"
echo "║                                                              ║"
echo "║  1. Update scripts/deploy.config.sh:                        ║"
echo "║     DEFAULT_LAKEBASE_PROJECT=\"$NAME\""
echo "║     DEFAULT_LAKEBASE_DATABASE=\"$DATABASE\""
echo "║     # optional: LAKEBASE_DATABASE_RESOURCE_SEGMENT=\"${DB_SEGMENT:-db-…}\""
echo "║                                                              ║"
echo "║  2. In Databricks Apps UI: rebind the postgres resource      ║"
echo "║     to the new project endpoint (if the host changed).       ║"
echo "║                                                              ║"
echo "║  3. Run: make deploy                                         ║"
echo "║  4. Run: make bootstrap-lakebase                             ║"
echo "║  5. In the app UI: Settings → Registry → Initialize          ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

if [[ -n "$DB_SEGMENT" ]]; then
  echo "Copy-paste for deploy.config.sh (datname + optional segment override):"
  echo "  DEFAULT_LAKEBASE_PROJECT=\"$NAME\""
  echo "  DEFAULT_LAKEBASE_DATABASE=\"$DATABASE\""
  echo "  # LAKEBASE_DATABASE_RESOURCE_SEGMENT=\"$DB_SEGMENT\"  # optional; deploy.sh auto-resolves"
  echo ""
fi
