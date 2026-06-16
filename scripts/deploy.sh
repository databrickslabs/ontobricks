#!/usr/bin/env bash
set -euo pipefail

# ── OntoBricks Deployment Script ────────────────────────────────────
# Single-entry orchestrator for everything `make deploy` does. Every
# step is checked; on failure the script prints the step, the failing
# command, the line, and a targeted hint, then aborts (set -e + ERR trap):
#
#   1. Preflight — assert the required tooling (databricks, python3),
#      files (databricks.yml, app.yaml.template, bootstrap scripts), and
#      non-empty config values are all present before touching the API.
#   2. Verify Databricks authentication (fails fast with a login hint).
#   3. Render `app.yaml` from `app.yaml.template` so the runtime env
#      block matches `scripts/deploy.config.sh` (the single source of truth).
#   4. Validate the bundle (every DAB variable passed as `--var=key=value`
#      so `databricks.yml` stays a pure structural declaration).
#   5. Resource existence checks — confirm the SQL warehouse, registry
#      Volume, and (Lakebase target) Postgres database the bundle binds
#      to actually exist. Read-only; `--dry-run` stops here.
#   6. Deploy the bundle.
#  6b. Verify the synced source actually landed on the workspace —
#      auto-heals a stale DAB sync-snapshot (the local cache that tracks
#      "what's already uploaded"): if a critical file (run.py, pyproject.toml)
#      is missing remotely, it wipes the snapshot and re-deploys once, so the
#      app can never boot with a missing entrypoint / dependencies file.
#   7. (Optional, --bind) bind the existing Apps resource to this bundle.
#   8. Start the app (unless `--no-run`).
#   9. Verify the deployed app status + resource bindings.
#  10. Bootstrap the app SP self-permissions and (Lakebase target only)
#      the Postgres schema GRANTs.
#
# The bundle (databricks.yml) only manages the dev sandbox apps:
# `${APP_NAME}` (FastAPI UI) and `${MCP_APP_NAME}` (MCP companion).
# The production `ontobricks` and `mcp-ontobricks` apps were carved
# out on 2026-04-27 and live in a different repo/bundle.
#
# Usage:
#   scripts/deploy.sh                     # deploy + run (uses DAB_TARGET from config)
#   scripts/deploy.sh --dry-run           # run ALL checks (preflight→validate→resources), make NO changes
#   scripts/deploy.sh -t dev              # override target on the fly
#   scripts/deploy.sh --no-run            # deploy artifacts without starting the app
#   scripts/deploy.sh --bind              # also (re)bind the existing app to this bundle
#   scripts/deploy.sh --no-bootstrap      # skip the perm/Lakebase bootstrap steps
#   scripts/deploy.sh --skip-app-yaml     # skip the app.yaml render (use the existing file as-is)
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

# ── Output + error helpers ──────────────────────────────────────────
# Colours only when stdout is a TTY (CI logs stay clean).
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

# Fired by the ERR trap on any uncaught failure (set -e). Reports which
# step failed, the exact command + line, and a targeted hint.
_on_error() {
    local rc=$1 line=$2
    echo "" >&2
    echo "${_C_RED}✗ Deployment aborted${_C_RST} (exit ${rc})" >&2
    echo "  step    : ${CURRENT_STEP}" >&2
    echo "  command : ${BASH_COMMAND}" >&2
    echo "  line    : scripts/deploy.sh:${line}" >&2
    case "$CURRENT_STEP" in
        *Validate*|*Deploy*|*Start*)
            echo "  hint    : inspect the error above; re-run \`databricks bundle validate -t ${TARGET:-?}\`" >&2
            echo "            and double-check the --var values in ${CONFIG_FILE:-scripts/deploy.config.sh}." >&2 ;;
        *auth*|*Authentication*)
            echo "  hint    : run \`databricks auth login --host https://<workspace>\` then retry." >&2 ;;
        *Render*)
            echo "  hint    : check scripts/_render-app-yaml.py and the APP_* values in ${CONFIG_FILE:-scripts/deploy.config.sh}." >&2 ;;
    esac
    exit "$rc"
}
trap '_on_error $? $LINENO' ERR

# ── Preflight assertion helpers ─────────────────────────────────────
require_cmd()  { command -v "$1" >/dev/null 2>&1 || die "Required command not found on PATH: '$1'${2:+ — $2}"; }
require_file() { [[ -f "$1" ]] || die "Required file missing: $1${2:+ — $2}"; }
require_var()  { [[ -n "${!1:-}" ]] || die "Required config variable '$1' is empty — set it in ${CONFIG_FILE} (or pass $1=… on the command line)."; }

# ── 0. Load configuration ───────────────────────────────────────────
CONFIG_FILE="scripts/deploy.config.sh"
require_file "$CONFIG_FILE" "the deploy configuration (single source of truth)"
# shellcheck disable=SC1090
. "$CONFIG_FILE"

# Local CLI flags — override (don't pollute) what the config exported.
TARGET="$DAB_TARGET"
NO_RUN=false
DO_BIND=false
DO_BOOTSTRAP=true
RENDER_APP_YAML=true
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        -t|--target)        TARGET="${2:-}"; [[ -n "$TARGET" ]] || die "-t/--target requires a value"; shift 2 ;;
        --no-run)           NO_RUN=true; shift ;;
        --bind)             DO_BIND=true; shift ;;
        --no-bootstrap)     DO_BOOTSTRAP=false; shift ;;
        --skip-app-yaml)    RENDER_APP_YAML=false; shift ;;
        --dry-run|--check)  DRY_RUN=true; shift ;;
        -h|--help)
            sed -n '4,48p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) die "Unknown option: $1 (run with --help for usage)" ;;
    esac
done

IS_LAKEBASE=false
[[ "$TARGET" == *lakebase* ]] && IS_LAKEBASE=true

# ── DAB variable overrides ──────────────────────────────────────────
# Composed from the env exported by `deploy.config.sh`. If you add a
# variable to `databricks.yml > variables:`, also surface it in
# `deploy.config.sh` and add a `--var=` line here.
_dab_var_overrides=(
    "--var=app_name=${APP_NAME}"
    "--var=mcp_app_name=${MCP_APP_NAME}"
    "--var=warehouse_id=${WAREHOUSE_ID}"
    "--var=registry_catalog=${REGISTRY_CATALOG}"
    "--var=registry_schema=${REGISTRY_SCHEMA}"
    "--var=registry_volume=${REGISTRY_VOLUME}"
    "--var=lakebase_project=${LAKEBASE_PROJECT}"
    "--var=lakebase_branch=${LAKEBASE_BRANCH}"
    "--var=lakebase_database_resource_segment=${LAKEBASE_DATABASE_RESOURCE_SEGMENT}"
    "--var=lakebase_registry_schema=${LAKEBASE_REGISTRY_SCHEMA}"
)

EXPECTED_VOLUME_FQN="${REGISTRY_CATALOG}.${REGISTRY_SCHEMA}.${REGISTRY_VOLUME}"
EXPECTED_PG_BRANCH_PATH="projects/${LAKEBASE_PROJECT}/branches/${LAKEBASE_BRANCH}"
EXPECTED_PG_DATABASE_PATH="${EXPECTED_PG_BRANCH_PATH}/databases/${LAKEBASE_DATABASE_RESOURCE_SEGMENT}"

echo "${_C_BLU}=== OntoBricks Deployment (DAB) ===${_C_RST}"
echo "Config  : $CONFIG_FILE"
echo "Target  : $TARGET"
echo "App     : $APP_NAME ($APP_RESOURCE_KEY)"
echo "MCP app : $MCP_APP_NAME ($MCP_APP_RESOURCE_KEY)"
echo "Registry: ${REGISTRY_CATALOG}.${REGISTRY_SCHEMA}.${REGISTRY_VOLUME}"
if $IS_LAKEBASE; then
    echo "Lakebase: projects/${LAKEBASE_PROJECT}/branches/${LAKEBASE_BRANCH}/databases/${LAKEBASE_DATABASE_RESOURCE_SEGMENT}"
fi

# ── 1. Preflight checks ─────────────────────────────────────────────
begin_step "Preflight checks"

# 1a. Required tooling.
require_cmd databricks "install the Databricks CLI ≥ 0.250.0 — https://docs.databricks.com/dev-tools/cli/"
require_cmd python3 "needed to render app.yaml and parse CLI JSON output"
_cli_ver="$(databricks version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n1 || true)"
ok "tooling present (databricks${_cli_ver:+ v$_cli_ver}, python3)"

# 1b. Required files (only those the chosen flags will actually use).
require_file "databricks.yml" "the DAB bundle definition"
if $RENDER_APP_YAML; then
    require_file "app.yaml.template" "the app.yaml source template"
    require_file "src/mcp-server/app.yaml.template" "the MCP app.yaml source template"
    require_file "scripts/_render-app-yaml.py" "the app.yaml renderer"
fi
if $DO_BOOTSTRAP; then
    require_file "scripts/bootstrap-app-permissions.sh"
    $IS_LAKEBASE && require_file "scripts/bootstrap-lakebase-perms.sh"
fi
ok "required files present"

# 1c. Required config values (fail fast with a precise name).
require_var APP_NAME; require_var MCP_APP_NAME
require_var APP_RESOURCE_KEY; require_var MCP_APP_RESOURCE_KEY
require_var TARGET
require_var WAREHOUSE_ID
require_var REGISTRY_CATALOG; require_var REGISTRY_SCHEMA; require_var REGISTRY_VOLUME
if $IS_LAKEBASE; then
    require_var LAKEBASE_PROJECT; require_var LAKEBASE_BRANCH
    require_var LAKEBASE_DATABASE_RESOURCE_SEGMENT
    require_var LAKEBASE_REGISTRY_SCHEMA; require_var LAKEBASE_REGISTRY_DATABASE
    # Common mistake: putting the datname / schema in the resource segment.
    case "$LAKEBASE_DATABASE_RESOURCE_SEGMENT" in
        db-*) : ;;
        *) warn "LAKEBASE_DATABASE_RESOURCE_SEGMENT='${LAKEBASE_DATABASE_RESOURCE_SEGMENT}' does not look like a 'db-…' resource id (see databricks.yml). Did you use the datname/schema by mistake?" ;;
    esac
fi
ok "deploy.config values present"

# 1d. Target sanity (soft — bundle validate is the source of truth).
case "$TARGET" in
    dev|dev-lakebase) : ;;
    *) warn "target '${TARGET}' is not one of the documented targets (dev, dev-lakebase) — continuing; bundle validate will confirm it exists." ;;
esac

# ── 2. Verify Databricks authentication ─────────────────────────────
begin_step "Verify Databricks authentication"
if ! databricks current-user me &>/dev/null; then
    die "Not authenticated to Databricks. Run: databricks auth login --host https://<workspace> (or export DATABRICKS_HOST/DATABRICKS_TOKEN)."
fi
DATABRICKS_USERNAME="$(databricks current-user me -o json \
    | python3 -c 'import sys,json; print(json.load(sys.stdin).get("userName","<unknown>"))' 2>/dev/null || echo "<unknown>")"
ok "authenticated as ${DATABRICKS_USERNAME}"

# ── 2b. Resolve ONTOBRICKS_URL for the MCP app.yaml ─────────────────
# Fetch the main app's URL from the platform so the MCP companion yaml
# always points at the correct deployed instance.
# On the very first deploy the app does not exist yet → URL is empty →
# the renderer strips the optional ONTOBRICKS_URL entry → the MCP falls
# back to localhost:8000 until you redeploy (second run auto-fixes it).
if [[ -z "${APP_ONTOBRICKS_URL:-}" ]]; then
    _fetched_url="$(databricks apps get "$APP_NAME" -o json 2>/dev/null \
        | python3 -c "import sys,json; print(json.load(sys.stdin).get('url',''))" \
        2>/dev/null || true)"
    if [[ -n "$_fetched_url" ]]; then
        export APP_ONTOBRICKS_URL="$_fetched_url"
        info "resolved ONTOBRICKS_URL → ${APP_ONTOBRICKS_URL}"
    else
        info "ONTOBRICKS_URL not yet known (first deploy?) — MCP will use localhost fallback until next deploy"
    fi
fi

# ── 3. Render app.yaml from template ────────────────────────────────
begin_step "Render app.yaml"
if $RENDER_APP_YAML; then
    python3 scripts/_render-app-yaml.py \
        || die "app.yaml render failed — check scripts/_render-app-yaml.py and the APP_* values in ${CONFIG_FILE}."
    require_file "app.yaml" "the renderer was expected to produce app.yaml"
    require_file "src/mcp-server/app.yaml" "the renderer was expected to produce src/mcp-server/app.yaml"
    ok "app.yaml and src/mcp-server/app.yaml rendered from templates"
else
    require_file "app.yaml" "needed when --skip-app-yaml is used (nothing to render onto)"
    info "skipped (--skip-app-yaml) — using the existing app.yaml as-is"
fi

# ── 4. Validate bundle ──────────────────────────────────────────────
begin_step "Validate bundle"
databricks bundle validate -t "$TARGET" "${_dab_var_overrides[@]}" \
    || die "bundle validation failed for target '${TARGET}'. Fix the errors above (commonly a bad --var or a target not declared in databricks.yml)."
ok "bundle valid"

# ── 5. Resource existence checks ────────────────────────────────────
# Confirm the upstream Databricks resources the bundle binds to actually
# exist *before* we mutate anything. These are read-only API calls; a
# failure is a warning (CLI subcommands / permissions vary) but is
# upgraded to a hard error under --dry-run so a check run is decisive.
begin_step "Resource existence checks"
CHECK_FAILED=0

# A soft check: run the command; on failure warn (or die under --dry-run).
check_resource() {
    local label="$1"; shift
    if "$@" >/dev/null 2>&1; then
        ok "$label exists"
    else
        CHECK_FAILED=$((CHECK_FAILED + 1))
        if $DRY_RUN; then
            warn "$label NOT found or not accessible — \`$*\`"
        else
            warn "$label could not be verified (\`$*\`) — continuing; the deploy will fail if it is truly missing."
        fi
    fi
}

check_resource "SQL warehouse '${WAREHOUSE_ID}'" \
    databricks warehouses get "$WAREHOUSE_ID"

check_resource "Volume '${EXPECTED_VOLUME_FQN}'" \
    databricks volumes read "$EXPECTED_VOLUME_FQN"

if $IS_LAKEBASE; then
    # The Lakebase Postgres database must already exist (created by
    # scripts/setup-lakebase.sh). Verify the db-… segment is listed under
    # the configured project/branch.
    if _pg_dbs="$(databricks postgres list-databases "$EXPECTED_PG_BRANCH_PATH" -o json 2>/dev/null)"; then
        if printf '%s' "$_pg_dbs" | grep -q "$LAKEBASE_DATABASE_RESOURCE_SEGMENT"; then
            ok "Lakebase database '${LAKEBASE_DATABASE_RESOURCE_SEGMENT}' present on ${EXPECTED_PG_BRANCH_PATH}"
        else
            CHECK_FAILED=$((CHECK_FAILED + 1))
            warn "Lakebase database '${LAKEBASE_DATABASE_RESOURCE_SEGMENT}' not found under ${EXPECTED_PG_BRANCH_PATH}. List ids with: databricks postgres list-databases \"${EXPECTED_PG_BRANCH_PATH}\" -o json"
        fi
    else
        CHECK_FAILED=$((CHECK_FAILED + 1))
        warn "could not list Lakebase databases for ${EXPECTED_PG_BRANCH_PATH} — verify LAKEBASE_PROJECT/LAKEBASE_BRANCH and that the project was created via scripts/setup-lakebase.sh."
    fi
fi

if [[ $CHECK_FAILED -eq 0 ]]; then
    ok "all referenced resources verified"
fi

# ── Dry run stops here — everything above is read-only ──────────────
if $DRY_RUN; then
    echo ""
    if [[ $CHECK_FAILED -gt 0 ]]; then
        die "DRY RUN found ${CHECK_FAILED} resource problem(s) above. Fix them in ${CONFIG_FILE} before deploying."
    fi
    echo "${_C_GRN}=== Dry run OK ===${_C_RST}  preflight + auth + render + validate + resource checks all passed."
    info "no Databricks changes were made (app.yaml was rendered locally, as it always is)."
    info "re-run without --dry-run to deploy."
    exit 0
fi

# ── 6. Deploy bundle ────────────────────────────────────────────────
begin_step "Deploy bundle (target: $TARGET)"
databricks bundle deploy -t "$TARGET" "${_dab_var_overrides[@]}" \
    || die "bundle deploy failed. Common causes: insufficient workspace permissions, an app name already owned by another bundle, or a transient API error (safe to retry)."
ok "bundle deployed"

# ── 6b. Verify synced source integrity (auto-heal snapshot drift) ───
# DAB syncs files INCREMENTALLY against a local snapshot under
# `.databricks/bundle/<target>/sync-snapshots/`. If that snapshot drifts
# from the remote `files/` dir — e.g. the remote dir was emptied, or the
# app_name (and thus root_path) was changed and changed back — `bundle
# deploy` trusts the snapshot and uploads ONLY the changed files. The app
# then boots from a near-empty source and crashes with
# "can't open file '…/run.py'" / "No dependencies file found".
#
# We catch that here: confirm the critical boot files exist on the
# workspace; if not, wipe the local snapshot (forcing a full re-upload)
# and re-deploy once, then re-verify.
begin_step "Verify synced source"

# Critical files the apps need to boot. Paths are relative to the synced
# bundle root (source_code_path "." for the UI, "src/mcp-server" for MCP).
_CRITICAL_SYNCED_FILES=(run.py pyproject.toml src/mcp-server/pyproject.toml src/mcp-server/app.yaml src/mcp-server/app.yaml.template)

# Authoritative remote files dir from the resolved bundle; fall back to the
# databricks.yml root_path convention if `bundle summary` is unavailable.
REMOTE_FILES_PATH="$(databricks bundle summary -t "$TARGET" "${_dab_var_overrides[@]}" -o json 2>/dev/null \
    | python3 -c 'import sys,json; print((json.load(sys.stdin).get("workspace") or {}).get("file_path",""))' 2>/dev/null || true)"
if [[ -z "$REMOTE_FILES_PATH" ]]; then
    REMOTE_FILES_PATH="/Workspace/Users/${DATABRICKS_USERNAME}/.bundle/${APP_NAME}/${TARGET}/files"
    info "bundle summary unavailable — assuming synced files at ${REMOTE_FILES_PATH}"
fi

# Echoes the missing files (one per line) and returns non-zero if any are absent.
_missing_synced_files() {
    local f missing=()
    for f in "${_CRITICAL_SYNCED_FILES[@]}"; do
        databricks workspace get-status "${REMOTE_FILES_PATH}/${f}" >/dev/null 2>&1 || missing+=("$f")
    done
    [[ ${#missing[@]} -eq 0 ]] && return 0
    printf '%s ' "${missing[@]}"; return 1
}

if _missing="$(_missing_synced_files)"; then
    ok "synced source verified on the workspace"
else
    warn "synced source is INCOMPLETE — missing on the workspace: ${_missing}"
    warn "cause: a stale DAB sync snapshot (local upload cache out of sync with the remote files/ dir)."
    info "auto-healing: clearing the local sync snapshot for '${TARGET}' and re-deploying once…"
    rm -f ".databricks/bundle/${TARGET}/sync-snapshots/"*.json 2>/dev/null || true
    databricks bundle deploy -t "$TARGET" "${_dab_var_overrides[@]}" \
        || die "re-deploy after snapshot reset failed. Inspect: databricks workspace list \"${REMOTE_FILES_PATH}\""
    if _missing="$(_missing_synced_files)"; then
        ok "synced source verified after a clean re-sync"
    else
        die "synced source STILL missing after a full re-sync: ${_missing}. The app would crash on boot. Inspect: databricks workspace list \"${REMOTE_FILES_PATH}\""
    fi
fi

# ── 7. Bind resources (first-time only) ────────────────────────────
if $DO_BIND; then
    begin_step "Bind existing app"
    if databricks bundle deployment bind "$APP_RESOURCE_KEY" "$APP_NAME" \
            -t "$TARGET" --auto-approve 2>/dev/null; then
        ok "bound $APP_RESOURCE_KEY → $APP_NAME"
    else
        info "$APP_RESOURCE_KEY: new app or already bound — nothing to do"
    fi
fi

# ── 8. Run ──────────────────────────────────────────────────────────
if ! $NO_RUN; then
    begin_step "Start $APP_NAME"
    databricks bundle run "$APP_RESOURCE_KEY" -t "$TARGET" "${_dab_var_overrides[@]}" \
        || die "failed to start app '${APP_NAME}'. Inspect the logs: databricks apps logs ${APP_NAME}"
    ok "app start requested"

    begin_step "Start $MCP_APP_NAME"
    databricks bundle run "$MCP_APP_RESOURCE_KEY" -t "$TARGET" "${_dab_var_overrides[@]}" \
        || die "failed to start app '${MCP_APP_NAME}'. Inspect the logs: databricks apps logs ${MCP_APP_NAME}"
    ok "MCP app start requested"
else
    begin_step "Start apps (skipped)"
    info "skipped per --no-run"
fi

# ── 9. Verify deployment ────────────────────────────────────────────
begin_step "Verify deployment"
STATUS=$(databricks apps get "$APP_NAME" -o json 2>/dev/null | python3 -c "
import sys, json
d = json.load(sys.stdin)
state = d.get('app_status',{}).get('state','UNKNOWN')
url   = d.get('url','')
print(f'{state}  {url}')
" 2>/dev/null || echo "NOT DEPLOYED")
printf "  %-20s %s\n" "$APP_NAME" "$STATUS"

MCP_STATUS=$(databricks apps get "$MCP_APP_NAME" -o json 2>/dev/null | python3 -c "
import sys, json
d = json.load(sys.stdin)
state = d.get('app_status',{}).get('state','UNKNOWN')
url   = d.get('url','')
print(f'{state}  {url}')
" 2>/dev/null || echo "NOT DEPLOYED")
printf "  %-20s %s\n" "$MCP_APP_NAME" "$MCP_STATUS"

verify_app_resources() {
    local app_name="$1"
    local expect_lakebase="$2"
    local app_json
    app_json="$(databricks apps get "$app_name" -o json 2>/dev/null || true)"
    if [[ -z "$app_json" ]]; then
        echo "  [$app_name] ✗ could not fetch app details to verify resources"
        return 1
    fi

    if APP_JSON="$app_json" \
        EXPECT_WAREHOUSE="$WAREHOUSE_ID" \
        EXPECT_VOLUME="$EXPECTED_VOLUME_FQN" \
        EXPECT_PG_BRANCH="$EXPECTED_PG_BRANCH_PATH" \
        EXPECT_PG_DATABASE="$EXPECTED_PG_DATABASE_PATH" \
        EXPECT_LAKEBASE="$expect_lakebase" \
        python3 - <<'PY'
import json, os, sys

data = json.loads(os.environ["APP_JSON"])
resources = {r.get("name", ""): r for r in data.get("resources", [])}
errors: list[str] = []

sqlw = ((resources.get("sql-warehouse") or {}).get("sql_warehouse") or {}).get("id")
if sqlw != os.environ["EXPECT_WAREHOUSE"]:
    errors.append(
        f"sql-warehouse.id mismatch: got={sqlw!r} expected={os.environ['EXPECT_WAREHOUSE']!r}"
    )

volume = (
    (resources.get("volume") or {})
    .get("uc_securable", {})
    .get("securable_full_name")
)
if volume != os.environ["EXPECT_VOLUME"]:
    errors.append(
        f"volume.securable_full_name mismatch: got={volume!r} expected={os.environ['EXPECT_VOLUME']!r}"
    )

expect_lakebase = os.environ["EXPECT_LAKEBASE"] == "true"
postgres = (resources.get("postgres") or {}).get("postgres") or {}
if expect_lakebase:
    if postgres.get("branch") != os.environ["EXPECT_PG_BRANCH"]:
        errors.append(
            f"postgres.branch mismatch: got={postgres.get('branch')!r} expected={os.environ['EXPECT_PG_BRANCH']!r}"
        )
    if postgres.get("database") != os.environ["EXPECT_PG_DATABASE"]:
        errors.append(
            f"postgres.database mismatch: got={postgres.get('database')!r} expected={os.environ['EXPECT_PG_DATABASE']!r}"
        )

if errors:
    for e in errors:
        print(e)
    sys.exit(1)
print("ok")
PY
    then
        echo "  [$app_name] ✓ resources match configured values"
        return 0
    fi

    echo "  [$app_name] ✗ resource verification failed"
    return 1
}

echo ""
info "Resource binding check:"
VERIFY_FAILED=0
if ! verify_app_resources "$APP_NAME" "$IS_LAKEBASE"; then
    VERIFY_FAILED=$((VERIFY_FAILED + 1))
fi
if ! verify_app_resources "$MCP_APP_NAME" "$IS_LAKEBASE"; then
    VERIFY_FAILED=$((VERIFY_FAILED + 1))
fi
if [[ $VERIFY_FAILED -gt 0 ]]; then
    warn "$VERIFY_FAILED app(s) have resources that do not match deploy.config values — bind them in the Databricks Apps UI (see reminders below)."
fi

if ! $DO_BOOTSTRAP; then
    begin_step "Bootstrap (skipped)"
    info "skipped permission/Lakebase bootstrap per --no-bootstrap"
    echo ""
    echo "${_C_GRN}=== Done ===${_C_RST}"
    exit 0
fi

# ── 10. App self-permissions (first-deploy bootstrap) ──────────────
# The app's service principal needs CAN_MANAGE on its OWN app so the
# middleware can read the ACL to resolve admin/app-user roles.
# The MCP app service principal also needs CAN_USE on the main app so
# MCP tool calls can reach /api/v1/* without 401.
# Idempotent — safe to re-run. The bootstrap script reads APP_NAME /
# MCP_APP_NAME from the env we exported via deploy.config.sh.
begin_step "App self-permissions"
chmod +x scripts/bootstrap-app-permissions.sh
if scripts/bootstrap-app-permissions.sh "$APP_NAME" "$MCP_APP_NAME"; then
    ok "app self-permissions applied"
else
    warn "app self-permission bootstrap returned non-zero — the app may not yet be reachable; re-run \`make bootstrap-perms\` once it is RUNNING."
fi

# ── 11. Registry Lakebase schema permissions (dev-lakebase only) ───
# When the postgres resource binding is unbound/rebound — which happens
# every time we redeploy with a different target — Lakebase loses the
# schema-level GRANTs the app SP needs (USAGE on the schema, DML on
# tables, USAGE/SELECT/UPDATE on sequences). The runtime then fails
# with "Role '<sp-id>' lacks USAGE on schema '${LAKEBASE_REGISTRY_SCHEMA}'".
#
# This script is registry-scoped: it only grants on the REGISTRY schema.
# The graph DB is configured in-app (Settings → Graph DB) and may live in
# a different Lakebase project, so its grants are handled by the in-app
# "Create graph DB" flow (or a manual bootstrap-lakebase-perms.sh run
# with the explicit graph project/database/schema).
#
# Re-running the bootstrap is idempotent, so we do it on every
# Lakebase-target deploy. Failures are tolerated (e.g. first deploy
# before the schema is initialised, or psql not installed) — the
# script prints actionable guidance in that case.
if $IS_LAKEBASE; then
    begin_step "Lakebase schema permissions"
    chmod +x scripts/bootstrap-lakebase-perms.sh
    # Pass the registry catalog so bootstrap also grants UC ALL_PRIVILEGES —
    # required for the SP to read back synced tables via the Lakebase API.
    _UC_CATALOG_ARG=()
    if [[ -n "${REGISTRY_CATALOG:-}" ]]; then
        _UC_CATALOG_ARG=(-c "$REGISTRY_CATALOG")
    fi

    if ! scripts/bootstrap-lakebase-perms.sh \
            -i "$LAKEBASE_PROJECT" \
            -b "$LAKEBASE_BRANCH" \
            -d "$LAKEBASE_REGISTRY_DATABASE" \
            -s "$LAKEBASE_REGISTRY_SCHEMA" \
            "${_UC_CATALOG_ARG[@]}" \
            -a "$APP_NAME" \
            -a "$MCP_APP_NAME"; then
        echo ""
        echo "  ⚠ Lakebase permission bootstrap did not complete cleanly."
        echo "    If the registry schema does not exist yet, initialise it"
        echo "    from Settings > Registry > Initialize and re-run:"
        echo "      scripts/bootstrap-lakebase-perms.sh \\"
        echo "        -i $LAKEBASE_PROJECT \\"
        echo "        -b $LAKEBASE_BRANCH \\"
        echo "        -d $LAKEBASE_REGISTRY_DATABASE \\"
        echo "        -s $LAKEBASE_REGISTRY_SCHEMA \\"
        echo "        ${_UC_CATALOG_ARG[*]:+-c $REGISTRY_CATALOG \\}"
        echo "        -a $APP_NAME -a $MCP_APP_NAME"
    fi
fi

echo ""
echo "${_C_GRN}=== Done ===${_C_RST}"
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
