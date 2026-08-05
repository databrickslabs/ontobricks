#!/usr/bin/env bash
set -euo pipefail

# ── OntoBricks — Update an already-deployed app (code + SQL) ─────────
# Push the latest local code to an EXISTING OntoBricks deployment and
# apply any pending Lakebase registry-schema migrations — WITHOUT
# editing scripts/deploy.config.sh and without knowing the instance's
# coordinates by heart. Everything is read back from the live apps:
#
#   * SQL warehouse id, registry Volume (catalog.schema.volume), and the
#     Lakebase Autoscaling postgres binding (project / branch / db-…
#     segment) come from `databricks apps get <app> -o json`.
#   * The deployed `app.yaml` (UI + MCP) is fetched verbatim and re-synced
#     so the runtime env (LAKEBASE_SCHEMA / LAKEBASE_DATABASE, triplestore
#     table, MLflow URI, sync catalog, …) is preserved exactly as-is.
#
# It then reuses the canonical tooling rather than reimplementing it:
#   1. scripts/deploy.sh  — validate + bundle deploy (with the introspected
#      resource bindings) + sync-snapshot self-heal + restart both apps.
#      Run with --skip-app-yaml (keep the fetched app.yaml) and
#      --no-bootstrap (we bootstrap below with the introspected schema, not
#      the deploy.config.sh default).
#   2. scripts/bootstrap-lakebase-perms.sh — re-grant the app SPs on the
#      registry schema (a redeploy rebinds postgres and can drop grants)
#      and apply the idempotent registry-schema migrations (status,
#      review_quorum, build_runs, domain_review_events, domain_comments,
#      domain_tasks). This is the "SQL if necessary" step; it is a no-op
#      when everything is already present.
#
# The ONLY arguments are the two app names.
#
# Usage:
#   scripts/update-deployed-app.sh <UI_APP_NAME> <MCP_APP_NAME>
#
# Example:
#   scripts/update-deployed-app.sh ontobricks-060 mcp-ontobricks-060
#
# Prerequisites:
#   - Databricks CLI authenticated against the workspace hosting the apps
#   - python3 on PATH
#   - psql on PATH (only when the app is Lakebase-backed — for the SQL step)
#   - You own the registry schema (or have GRANT OPTION) for the SQL step

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."

# ── Output + error helpers (same style as scripts/deploy.sh) ────────
if [[ -t 1 ]]; then
    _C_RED=$'\033[31m'; _C_GRN=$'\033[32m'; _C_YEL=$'\033[33m'
    _C_BLU=$'\033[36m'; _C_RST=$'\033[0m'
else
    _C_RED=""; _C_GRN=""; _C_YEL=""; _C_BLU=""; _C_RST=""
fi

CURRENT_STEP="startup"
begin_step() { CURRENT_STEP="$1"; echo ""; echo "${_C_BLU}── $1 ──${_C_RST}"; }
info() { echo "  $*"; }
ok()   { echo "  ${_C_GRN}✓${_C_RST} $*"; }
warn() { echo "  ${_C_YEL}⚠${_C_RST}  $*" >&2; }
die()  { echo "" >&2; echo "${_C_RED}✗ ERROR:${_C_RST} $*" >&2; exit 1; }

_on_error() {
    local rc=$1 line=$2
    echo "" >&2
    echo "${_C_RED}✗ Update aborted${_C_RST} (exit ${rc})" >&2
    echo "  step    : ${CURRENT_STEP}" >&2
    echo "  command : ${BASH_COMMAND}" >&2
    echo "  line    : scripts/update-deployed-app.sh:${line}" >&2
    exit "$rc"
}
trap '_on_error $? $LINENO' ERR

require_cmd() { command -v "$1" >/dev/null 2>&1 || die "Required command not found on PATH: '$1'${2:+ — $2}"; }

usage() {
    sed -n '4,46p' "$0" | sed 's/^# \{0,1\}//'
}

# ── Parse arguments — exactly two app names ─────────────────────────
case "${1:-}" in
    -h|--help) usage; exit 0 ;;
esac
if [[ $# -ne 2 ]]; then
    echo "${_C_RED}✗ ERROR:${_C_RST} expected exactly 2 arguments (UI app name, MCP app name), got $#." >&2
    echo "" >&2
    echo "Usage: scripts/update-deployed-app.sh <UI_APP_NAME> <MCP_APP_NAME>" >&2
    exit 2
fi
UI_APP="$1"
MCP_APP="$2"

echo "${_C_BLU}=== OntoBricks — Update Deployed App (code + SQL) ===${_C_RST}"
echo "UI app  : $UI_APP"
echo "MCP app : $MCP_APP"

# ── 1. Preflight ────────────────────────────────────────────────────
begin_step "Preflight"
require_cmd databricks "install the Databricks CLI ≥ 0.250.0"
require_cmd python3 "needed to parse CLI JSON output"
require_file_msg() { [[ -f "$1" ]] || die "Required file missing: $1${2:+ — $2}"; }
require_file_msg "databricks.yml" "the DAB bundle definition"
require_file_msg "scripts/deploy.sh" "the deploy orchestrator this script delegates to"
if ! databricks current-user me >/dev/null 2>&1; then
    die "Not authenticated to Databricks. Run: databricks auth login --host https://<workspace>"
fi
DATABRICKS_USERNAME="$(databricks current-user me -o json \
    | python3 -c 'import sys,json; print(json.load(sys.stdin).get("userName","<unknown>"))' 2>/dev/null || echo "<unknown>")"
ok "authenticated as ${DATABRICKS_USERNAME}"

# ── 2. Introspect the deployed apps ─────────────────────────────────
begin_step "Read existing configuration from '$UI_APP'"

UI_JSON="$(databricks apps get "$UI_APP" -o json 2>/dev/null || true)"
[[ -n "$UI_JSON" ]] || die "Could not fetch app '$UI_APP'. Check the name and that you can access it: databricks apps list"

MCP_JSON="$(databricks apps get "$MCP_APP" -o json 2>/dev/null || true)"
[[ -n "$MCP_JSON" ]] || die "Could not fetch app '$MCP_APP'. Check the name: databricks apps list"

# Parse the UI app's resources + source path into KEY=VALUE lines.
_parse_app() {
    APP_JSON="$1" python3 - <<'PY'
import json, os
d = json.loads(os.environ["APP_JSON"])
res = {r.get("name", ""): r for r in d.get("resources", [])}
wh = ((res.get("sql-warehouse") or {}).get("sql_warehouse") or {}).get("id", "")
vol = ((res.get("volume") or {}).get("uc_securable") or {}).get("securable_full_name", "")
pg = (res.get("postgres") or {}).get("postgres") or {}
src = ((d.get("active_deployment") or {}).get("deployment_artifacts") or {}).get("source_code_path", "")
print("WAREHOUSE_ID\t" + wh)
print("VOLUME_FQN\t" + vol)
print("PG_BRANCH\t" + (pg.get("branch", "") or ""))
print("PG_DATABASE_PATH\t" + (pg.get("database", "") or ""))
print("SOURCE_PATH\t" + (src or ""))
PY
}

WAREHOUSE_ID=""; VOLUME_FQN=""; PG_BRANCH=""; PG_DATABASE_PATH=""; UI_SOURCE_PATH=""
while IFS=$'\t' read -r _k _v; do
    case "$_k" in
        WAREHOUSE_ID)     WAREHOUSE_ID="$_v" ;;
        VOLUME_FQN)       VOLUME_FQN="$_v" ;;
        PG_BRANCH)        PG_BRANCH="$_v" ;;
        PG_DATABASE_PATH) PG_DATABASE_PATH="$_v" ;;
        SOURCE_PATH)      UI_SOURCE_PATH="$_v" ;;
    esac
done < <(_parse_app "$UI_JSON")

MCP_SOURCE_PATH=""
while IFS=$'\t' read -r _k _v; do
    [[ "$_k" == "SOURCE_PATH" ]] && MCP_SOURCE_PATH="$_v"
done < <(_parse_app "$MCP_JSON")

[[ -n "$WAREHOUSE_ID" ]] || die "App '$UI_APP' has no 'sql-warehouse' resource binding — is it an OntoBricks app?"
[[ -n "$VOLUME_FQN" ]]   || die "App '$UI_APP' has no 'volume' resource binding — is it an OntoBricks app?"

# Volume FQN  →  catalog.schema.volume
REGISTRY_CATALOG="${VOLUME_FQN%%.*}"
_vol_rest="${VOLUME_FQN#*.}"
REGISTRY_SCHEMA="${_vol_rest%%.*}"
REGISTRY_VOLUME="${_vol_rest##*.}"

# Lakebase binding (present only on Lakebase-backed apps).
IS_LAKEBASE=false
LAKEBASE_PROJECT=""; LAKEBASE_BRANCH=""; LAKEBASE_DATABASE_RESOURCE_SEGMENT=""
if [[ -n "$PG_BRANCH" ]]; then
    IS_LAKEBASE=true
    # PG_BRANCH = projects/<project>/branches/<branch>
    _pb="${PG_BRANCH#projects/}"
    LAKEBASE_PROJECT="${_pb%%/*}"
    LAKEBASE_BRANCH="${PG_BRANCH##*/}"
    # PG_DATABASE_PATH = .../databases/<db-… segment>
    LAKEBASE_DATABASE_RESOURCE_SEGMENT="${PG_DATABASE_PATH##*/}"
fi

TARGET="dev"
$IS_LAKEBASE && TARGET="dev-lakebase"

ok "warehouse : ${WAREHOUSE_ID}"
ok "volume    : ${VOLUME_FQN}  (catalog=${REGISTRY_CATALOG} schema=${REGISTRY_SCHEMA} volume=${REGISTRY_VOLUME})"
if $IS_LAKEBASE; then
    ok "lakebase  : projects/${LAKEBASE_PROJECT}/branches/${LAKEBASE_BRANCH}/databases/${LAKEBASE_DATABASE_RESOURCE_SEGMENT}"
else
    ok "backend   : Volume-only (no postgres binding) — SQL step will be skipped"
fi
ok "DAB target: ${TARGET}"

# ── 3. Fetch the deployed app.yaml(s) verbatim ──────────────────────
# Re-syncing the live env block is what makes this an in-place CODE
# update that reuses the existing configuration.
begin_step "Fetch deployed app.yaml (preserve runtime env)"

# Fall back to the bundle's conventional sync root if the API did not
# report a source path (e.g. app never deployed via this bundle).
[[ -n "$UI_SOURCE_PATH"  ]] || UI_SOURCE_PATH="/Workspace/Users/${DATABRICKS_USERNAME}/.bundle/${UI_APP}/${TARGET}/files"
[[ -n "$MCP_SOURCE_PATH" ]] || MCP_SOURCE_PATH="/Workspace/Users/${DATABRICKS_USERNAME}/.bundle/${UI_APP}/${TARGET}/files/src/mcp-server"

fetch_app_yaml() {
    local remote="$1" local_path="$2" label="$3"
    rm -f "$local_path"
    if databricks workspace export "$remote" --format AUTO --file "$local_path" >/dev/null 2>&1 \
        && [[ -s "$local_path" ]]; then
        ok "fetched ${label} app.yaml ← ${remote}"
        return 0
    fi
    return 1
}

if ! fetch_app_yaml "${UI_SOURCE_PATH%/}/app.yaml" "app.yaml" "UI"; then
    die "Could not export the deployed UI app.yaml from '${UI_SOURCE_PATH%/}/app.yaml'. \
The app must have a synced app.yaml to reuse its configuration. Inspect: databricks workspace list \"${UI_SOURCE_PATH}\""
fi
if ! fetch_app_yaml "${MCP_SOURCE_PATH%/}/app.yaml" "src/mcp-server/app.yaml" "MCP"; then
    die "Could not export the deployed MCP app.yaml from '${MCP_SOURCE_PATH%/}/app.yaml'. \
Inspect: databricks workspace list \"${MCP_SOURCE_PATH}\""
fi

# Pull LAKEBASE_SCHEMA / LAKEBASE_DATABASE out of the fetched UI app.yaml —
# these are runtime env, not resource bindings, so they only live in the file.
read_app_yaml_env() {
    APP_YAML="app.yaml" python3 - <<'PY'
import os, re
text = open(os.environ["APP_YAML"], encoding="utf-8").read()
# Match a `- name: KEY` followed by `value: "…"` in the env: list.
def find(key):
    m = re.search(
        r'-\s*name:\s*%s\s*\n\s*value:\s*"?([^"\n]*)"?' % re.escape(key), text)
    return (m.group(1).strip() if m else "")
print("LAKEBASE_SCHEMA\t" + find("LAKEBASE_SCHEMA"))
print("LAKEBASE_DATABASE\t" + find("LAKEBASE_DATABASE"))
PY
}

LAKEBASE_SCHEMA=""; LAKEBASE_DATABASE=""
if $IS_LAKEBASE; then
    while IFS=$'\t' read -r _k _v; do
        case "$_k" in
            LAKEBASE_SCHEMA)   LAKEBASE_SCHEMA="$_v" ;;
            LAKEBASE_DATABASE) LAKEBASE_DATABASE="$_v" ;;
        esac
    done < <(read_app_yaml_env)

    [[ -n "$LAKEBASE_SCHEMA" ]] || die "Deployed UI app.yaml has no LAKEBASE_SCHEMA — cannot target the registry schema for the SQL step."

    # The Postgres datname is needed for the psql connection in the SQL step.
    # Prefer the app.yaml value; otherwise resolve it from the db-… segment.
    if [[ -z "$LAKEBASE_DATABASE" ]]; then
        info "LAKEBASE_DATABASE not in app.yaml — resolving datname from the db-… segment…"
        LAKEBASE_DATABASE="$(databricks postgres list-databases \
            "projects/${LAKEBASE_PROJECT}/branches/${LAKEBASE_BRANCH}" -o json 2>/dev/null \
            | SEG="$LAKEBASE_DATABASE_RESOURCE_SEGMENT" python3 -c "
import sys, json, os
seg = os.environ['SEG']
raw = sys.stdin.read()
try:
    data = json.loads(raw)
    dbs = data if isinstance(data, list) else data.get('databases', [])
    for db in dbs:
        if db.get('name','').split('/')[-1] == seg:
            print(db.get('status',{}).get('postgres_database',''))
            break
except Exception:
    pass
" 2>/dev/null || true)"
    fi
    [[ -n "$LAKEBASE_DATABASE" ]] || die "Could not determine the Lakebase Postgres datname for the SQL step."
    ok "registry  : schema=${LAKEBASE_SCHEMA} database=${LAKEBASE_DATABASE}"
fi

# ── 4. Push the code (delegate to the canonical deploy orchestrator) ─
# Export the introspected values so deploy.config.sh's env-overridable
# vars pick them up; --skip-app-yaml keeps the fetched app.yaml; we run
# the bootstrap ourselves in step 5 with the introspected schema.
begin_step "Update code (databricks bundle deploy via scripts/deploy.sh)"
chmod +x scripts/deploy.sh

APP_NAME="$UI_APP" \
MCP_APP_NAME="$MCP_APP" \
WAREHOUSE_ID="$WAREHOUSE_ID" \
REGISTRY_CATALOG="$REGISTRY_CATALOG" \
REGISTRY_SCHEMA="$REGISTRY_SCHEMA" \
REGISTRY_VOLUME="$REGISTRY_VOLUME" \
LAKEBASE_PROJECT="${LAKEBASE_PROJECT:-}" \
LAKEBASE_BRANCH="${LAKEBASE_BRANCH:-}" \
LAKEBASE_DATABASE_RESOURCE_SEGMENT="${LAKEBASE_DATABASE_RESOURCE_SEGMENT:-}" \
    scripts/deploy.sh -t "$TARGET" --skip-app-yaml --no-bootstrap \
    || die "code update failed — see the deploy output above."
ok "code synced + apps restarted"

# ── 5. Apply SQL migrations + re-grant (Lakebase only) ──────────────
if $IS_LAKEBASE; then
    begin_step "Update SQL (registry schema migrations + SP grants)"
    require_cmd psql "the libpq client is needed for the SQL step — brew install libpq && brew link --force libpq"
    chmod +x scripts/bootstrap-lakebase-perms.sh

    _UC_CATALOG_ARG=()
    [[ -n "$REGISTRY_CATALOG" ]] && _UC_CATALOG_ARG=(-c "$REGISTRY_CATALOG")

    if scripts/bootstrap-lakebase-perms.sh \
            -i "$LAKEBASE_PROJECT" \
            -b "$LAKEBASE_BRANCH" \
            -d "$LAKEBASE_DATABASE" \
            -s "$LAKEBASE_SCHEMA" \
            "${_UC_CATALOG_ARG[@]}" \
            -a "$UI_APP" \
            -a "$MCP_APP"; then
        ok "registry schema migrations applied + SP grants refreshed"
    else
        warn "SQL/grant bootstrap returned non-zero — if the registry schema is not initialised yet,"
        warn "open Settings → Registry → Initialize, then re-run this script (idempotent)."
    fi
else
    begin_step "Update SQL (skipped)"
    info "Volume-only backend — no Lakebase registry schema to migrate."
fi

echo ""
echo "${_C_GRN}=== Done — '${UI_APP}' updated ===${_C_RST}"
echo ""
echo "Reused configuration (read back from the live app):"
echo "  warehouse : ${WAREHOUSE_ID}"
echo "  volume    : ${VOLUME_FQN}"
if $IS_LAKEBASE; then
    echo "  lakebase  : ${LAKEBASE_PROJECT}/${LAKEBASE_BRANCH}  db=${LAKEBASE_DATABASE}  schema=${LAKEBASE_SCHEMA}"
fi
