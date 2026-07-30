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

# Resolve this file's own directory. Do NOT inherit a caller-set SCRIPT_DIR:
# deploy.sh sets SCRIPT_DIR to scripts/, but this helper and its siblings
# (e.g. _lakebase-diag.sh) live in scripts/_internal/, so we must compute our
# own location or the source below resolves to the wrong directory.
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

_preflight_summary() {
    echo ""
    if [[ $_PREFLIGHT_FAILED -gt 0 ]]; then
        echo "${_PF_C_RED}Preflight FAILED${_PF_C_RST}: ${_PREFLIGHT_FAILED} blocking issue(s), ${_PREFLIGHT_WARNINGS} warning(s)"
        echo "See docs/DEPLOY_CHECKLIST.md for the full deployment requirements."
        return 1
    fi
    if [[ $_PREFLIGHT_WARNINGS -gt 0 ]]; then
        echo "${_PF_C_YEL}Preflight passed with ${_PREFLIGHT_WARNINGS} warning(s)${_PF_C_RST} — review messages above."
    else
        echo "${_PF_C_GRN}Preflight OK${_PF_C_RST} — all checks passed."
    fi
    return 0
}
