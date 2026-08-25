#!/usr/bin/env bash
# Shared deploy / install preflight helpers.
# Source from deploy.sh, setup.sh, setup-lakebase.sh — do not execute directly.
#
# Public functions (after sourcing deploy.config.sh when needed):
#   _preflight_begin / _preflight_ok / _preflight_warn / _preflight_fail
#   _preflight_require_cmd / _preflight_require_file / _preflight_require_var
#   _preflight_check_databricks_auth
#   _preflight_check_local_dev
#   _preflight_check_deploy_files
#   _preflight_check_lakebase_bootstrap
#   _preflight_check_bootstrap_app_perms

set -euo pipefail

# Resolved from BASH_SOURCE, not from the caller's SCRIPT_DIR: this file is
# sourced by scripts/deploy.sh, where SCRIPT_DIR already points at scripts/
# rather than scripts/_internal/, so a `${SCRIPT_DIR:-...}` default never
# applies and the sibling lookup below would miss.
_PREFLIGHT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck disable=SC1091
source "${_PREFLIGHT_DIR}/_lakebase-diag.sh"

if [[ -t 1 ]]; then
    _PF_C_GRN=$'\033[32m'; _PF_C_YEL=$'\033[33m'; _PF_C_RED=$'\033[31m'; _PF_C_RST=$'\033[0m'
else
    _PF_C_GRN=""; _PF_C_YEL=""; _PF_C_RED=""; _PF_C_RST=""
fi

_PREFLIGHT_FAILED=0
_PREFLIGHT_WARNINGS=0

_preflight_begin() { echo ""; echo "── $1 ──"; }
_preflight_ok()    { echo "  ${_PF_C_GRN}✓${_PF_C_RST} $*"; }
_preflight_warn()  { _PREFLIGHT_WARNINGS=$((_PREFLIGHT_WARNINGS + 1)); echo "  ${_PF_C_YEL}⚠${_PF_C_RST}  $*" >&2; }
_preflight_fail()  { _PREFLIGHT_FAILED=$((_PREFLIGHT_FAILED + 1)); echo "  ${_PF_C_RED}✗${_PF_C_RST} $*" >&2; }

_preflight_require_cmd() {
    command -v "$1" >/dev/null 2>&1 || {
        _preflight_fail "Required command not found on PATH: '$1'${2:+ — $2}"
        return 1
    }
    _preflight_ok "$1 present"
}

_preflight_require_file() {
    [[ -f "$1" ]] || {
        _preflight_fail "Required file missing: $1${2:+ — $2}"
        return 1
    }
}

_preflight_require_var() {
    [[ -n "${!1:-}" ]] || {
        _preflight_fail "Required config variable '$1' is empty — set it in ${CONFIG_FILE:-scripts/deploy.config.sh}"
        return 1
    }
}

_preflight_check_databricks_auth() {
    if ! databricks current-user me >/dev/null 2>&1; then
        _preflight_fail "Databricks CLI not authenticated — run: databricks auth login --host https://<workspace>${DATABRICKS_CONFIG_PROFILE:+ --profile $DATABRICKS_CONFIG_PROFILE}"
        return 1
    fi
    local user
    user="$(databricks current-user me -o json 2>/dev/null \
        | python3 -c 'import sys,json; print(json.load(sys.stdin).get("userName","<unknown>"))' 2>/dev/null || echo "<unknown>")"
    _preflight_ok "Databricks CLI authenticated as ${user}"
}

_preflight_check_local_dev() {
    _preflight_begin "Local development prerequisites"
    _preflight_require_cmd python3 "Python 3.10+ required"
    local py_minor
    py_minor="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
    if python3 -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)'; then
        _preflight_ok "Python ${py_minor} (>= 3.10)"
    else
        _preflight_fail "Python ${py_minor} is too old — need >= 3.10"
    fi
    if command -v uv >/dev/null 2>&1; then
        _preflight_ok "uv present ($(uv --version 2>/dev/null || echo installed))"
    else
        _preflight_warn "uv not on PATH — setup.sh will install it"
    fi
}

_preflight_check_deploy_files() {
    local include_lakebase="${1:-false}"
    _preflight_begin "Deploy file prerequisites"
    _preflight_require_file "databricks.yml"
    _preflight_require_file "app.yaml.template"
    _preflight_require_file "src/mcp-server/app.yaml.template"
    _preflight_require_file "scripts/_internal/_render-app-yaml.py"
    _preflight_require_file "scripts/deploy.config.sh"
    _preflight_require_file "scripts/bootstrap/app-permissions.sh"
    _preflight_require_file "run.py"
    _preflight_require_file "pyproject.toml"
    if [[ "$include_lakebase" == "true" ]]; then
        _preflight_require_file "scripts/bootstrap/lakebase-perms.sh"
        _preflight_require_file "scripts/_internal/_lakebase_preflight.py"
        _preflight_require_file "scripts/migrations/upgrade_0.4_to_0.5.sql"
        _preflight_require_file "scripts/migrations/upgrade_0.5_to_0.6.sql"
        _preflight_require_file "scripts/migrations/upgrade_0.6_to_0.7.sql"
        _preflight_require_file "scripts/migrations/upgrade_0.7_to_0.8.sql"
    fi
    _preflight_ok "required project files present"
}

# Read-only checks mirroring bootstrap-lakebase-perms.sh + registry SQL upgrades.
# Sets _PREFLIGHT_FAILED on blocking issues. Warnings are non-fatal (e.g. schema
# not yet initialised on a first deploy).
_preflight_check_lakebase_bootstrap() {
    local project="$1"
    local branch="$2"
    local database="$3"
    local schema="$4"
    shift 4
    local apps=("$@")

    _preflight_begin "Lakebase bootstrap + migration preflight"
    _preflight_require_cmd databricks "install Databricks CLI ≥ 0.250.0"
    _preflight_require_cmd psql "brew install libpq && brew link --force libpq"
    _preflight_require_cmd python3

    if ! _preflight_check_databricks_auth; then
        return 1
    fi

    local -a app_args=()
    local app
    for app in "${apps[@]}"; do
        [[ -n "$app" ]] && app_args+=(--app "$app")
    done

    local rc=0
    if ! python3 scripts/_internal/_lakebase_preflight.py \
            --project "$project" \
            --branch "$branch" \
            --database "$database" \
            --schema "$schema" \
            "${app_args[@]}"; then
        rc=1
    fi

    if [[ $rc -ne 0 ]]; then
        _preflight_fail "Lakebase bootstrap preflight failed — see checks above"
        _lakebase_print_diag_hints \
            "bootstrap-lakebase-perms.sh preflight failed" \
            "$project" "$branch" "$database" "${LAKEBASE_DATABASE_RESOURCE_SEGMENT:-}" \
            "${CONFIG_FILE:-scripts/deploy.config.sh}"
        return 1
    fi

    _preflight_ok "Lakebase bootstrap + migration preflight passed"
}

_preflight_check_bootstrap_app_perms() {
    local -a apps=("$@")
    _preflight_begin "App permission bootstrap preflight"
    _preflight_require_cmd databricks
    _preflight_check_databricks_auth || return 1

    local app sp_id
    for app in "${apps[@]}"; do
        [[ -z "$app" ]] && continue
        sp_id="$(databricks apps get "$app" -o json 2>/dev/null \
            | python3 -c 'import sys,json
try:
    d=json.load(sys.stdin)
except Exception:
    sys.exit(0)
print(d.get("service_principal_client_id") or "")' 2>/dev/null || true)"
        if [[ -n "$sp_id" && "$sp_id" != "None" ]]; then
            _preflight_ok "app '${app}' exists (SP ${sp_id})"
        else
            _preflight_warn "app '${app}' not found yet — bootstrap-app-permissions.sh will run after first deploy"
        fi
    done
}

# A Databricks app name is immutable, so changing it renames nothing: Terraform
# destroys the app and creates a new one. The DAB resource key is static while
# the name comes from DEFAULT_APP_NAME, so editing that one line silently means
# "delete the running app". If the create then fails — a missing secret scope is
# enough — you are left with no app at all, which is exactly how ontobricks-060
# disappeared on 2026-07-30.
#
# Args: <dab_target> <app_resource_key> <desired_app_name>
_preflight_check_app_rename() {
    local target="$1" resource_key="$2" desired="$3"
    _preflight_begin "App rename safety"

    local state=".databricks/bundle/${target}/terraform/terraform.tfstate"
    if [[ ! -f "$state" ]]; then
        _preflight_ok "no local Terraform state for '${target}' — nothing to replace"
        return 0
    fi

    local current
    current="$(python3 - "$state" "$resource_key" <<'PY' 2>/dev/null || true
import json, sys
path, key = sys.argv[1], sys.argv[2]
try:
    with open(path) as fh:
        state = json.load(fh)
except Exception:
    sys.exit(0)
for res in state.get("resources") or []:
    if res.get("type") == "databricks_app" and res.get("name") == key:
        for inst in res.get("instances") or []:
            name = (inst.get("attributes") or {}).get("name")
            if name:
                print(name)
                sys.exit(0)
PY
)"

    if [[ -z "$current" ]]; then
        _preflight_ok "no app recorded under '${resource_key}' — this deploy creates one"
        return 0
    fi
    if [[ "$current" == "$desired" ]]; then
        _preflight_ok "app name unchanged ('${current}')"
        return 0
    fi

    echo "" >&2
    echo "  ${_PF_C_RED}This deploy will DESTROY the running app '${current}'${_PF_C_RST}" >&2
    echo "  and create '${desired}' in its place, because a Databricks app name" >&2
    echo "  cannot be changed in place. The old app, its URL and its compute are" >&2
    echo "  gone for good; if the create then fails you are left with neither." >&2
    echo "" >&2
    echo "  Your registry Volume and Lakebase schema are separate resources and" >&2
    echo "  are not touched." >&2
    echo "" >&2
    echo "  To keep '${current}', revert DEFAULT_INSTANCE_ID in ${CONFIG_FILE:-scripts/deploy.config.sh}" >&2
    echo "  (or deploy the new ID under its own target — the default when you only" >&2
    echo "  change INSTANCE_ID). Same-target renames destroy the old app." >&2
    echo "" >&2

    if [[ "${ALLOW_APP_RENAME:-}" == "1" ]]; then
        _preflight_warn "ALLOW_APP_RENAME=1 — proceeding to replace '${current}' with '${desired}'"
        return 0
    fi
    if [[ -t 0 ]]; then
        local reply=""
        read -r -p "  Type the name of the app to destroy ('${current}') to continue: " reply
        if [[ "$reply" == "$current" ]]; then
            _preflight_warn "confirmed — replacing '${current}' with '${desired}'"
            return 0
        fi
        _preflight_fail "app rename not confirmed — aborting before anything is destroyed"
        return 1
    fi
    _preflight_fail "app rename needs confirmation — re-run with ALLOW_APP_RENAME=1 to accept destroying '${current}'"
    return 1
}

# A secret scope that does not exist fails `terraform apply` on the app resource.
# Args: <scope> [key ...]
_preflight_check_secret_scope() {
    local scope="$1"; shift
    _preflight_begin "App secret bindings"
    if [[ -z "$scope" ]]; then
        _preflight_warn "no secret scope configured — skipping"
        return 0
    fi
    if ! databricks secrets list-secrets "$scope" >/dev/null 2>&1; then
        _preflight_fail "secret scope '${scope}' not found or unreadable — the app resource binds a secret from it and \`terraform apply\` will fail. Check \`databricks secrets list-scopes\`, then set NEO4J_SECRET_SCOPE in ${CONFIG_FILE:-scripts/deploy.config.sh}"
        return 1
    fi
    _preflight_ok "secret scope '${scope}' readable"

    local key present
    for key in "$@"; do
        [[ -z "$key" ]] && continue
        present="$(databricks secrets list-secrets "$scope" -o json 2>/dev/null \
            | python3 -c 'import sys,json
try:
    rows=json.load(sys.stdin)
except Exception:
    rows=[]
print("yes" if any(r.get("key")==sys.argv[1] for r in rows) else "")' "$key" 2>/dev/null || true)"
        if [[ -n "$present" ]]; then
            _preflight_ok "secret '${scope}/${key}' present"
        else
            _preflight_fail "secret '${scope}/${key}' missing — create it: databricks secrets put-secret ${scope} ${key} --string-value '<value>'"
        fi
    done
}

_preflight_summary() {
    echo ""
    if [[ $_PREFLIGHT_FAILED -gt 0 ]]; then
        echo "${_PF_C_RED}Preflight FAILED${_PF_C_RST}: ${_PREFLIGHT_FAILED} blocking issue(s), ${_PREFLIGHT_WARNINGS} warning(s)"
        echo "See documentation/DEPLOY_CHECKLIST.md for the full deployment requirements."
        return 1
    fi
    if [[ $_PREFLIGHT_WARNINGS -gt 0 ]]; then
        echo "${_PF_C_YEL}Preflight passed with ${_PREFLIGHT_WARNINGS} warning(s)${_PF_C_RST} — review messages above."
    else
        echo "${_PF_C_GRN}Preflight OK${_PF_C_RST} — all checks passed."
    fi
    return 0
}
