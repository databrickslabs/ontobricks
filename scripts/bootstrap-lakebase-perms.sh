#!/usr/bin/env bash
set -euo pipefail

# ── OntoBricks — Lakebase Schema Permission Bootstrap ───────────────
# A Databricks Apps service principal is created without any privileges
# on Lakebase Postgres objects, even when the app's ``postgres:`` resource
# binding is wired correctly. The first time the app tries to read the
# ``ontobricks_registry`` schema (or whatever ``--schema`` you pass), it
# silently sees an empty information_schema and the admin Registry
# Location panel reports "not initialized — 0 rows everywhere", which is
# the canonical false negative for this missing-USAGE state.
#
# This script connects to the Lakebase instance with your *human*
# credentials (as the schema owner), looks up each app's service
# principal client id, and grants it the privileges OntoBricks needs:
#
#   - USAGE + CREATE on the schema
#   - SELECT/INSERT/UPDATE/DELETE on every existing table
#   - USAGE/SELECT/UPDATE on every existing sequence (bigserial PKs)
#   - The same set as ALTER DEFAULT PRIVILEGES so future tables inherit
#
# Idempotent — re-running is a no-op for objects that already carry the
# privileges.
#
# Defaults match the **dev sandbox** described in ``databricks.yml``:
# Lakebase Autoscaling project ``ontobricks-app``, Postgres database
# ``ontobricks_registry`` (the dedicated ``datname`` bound by the Apps
# ``postgres`` resource when the bundle targets that DB — see
# ``lakebase_database_resource_segment`` in ``databricks.yml``), schema
# ``ontobricks_registry``, and grantees ``ontobricks-030`` +
# ``mcp-ontobricks``. If your instance still uses the shared default DB
# ``databricks_postgres`` with a registry **schema** named
# ``ontobricks_registry`` inside it, pass ``-d databricks_postgres``.
# Pass ``-i`` / ``-d`` / ``-s`` / ``-a`` to retarget any of those when
# you migrate to a different project, database, schema, or app.
# Defaults can also be set via env (``INSTANCE`` / ``DATABASE`` /
# ``SCHEMA``) — ``scripts/deploy.sh`` does this from
# ``scripts/deploy.config.sh``.
#
# Usage:
#   scripts/bootstrap-lakebase-perms.sh                # default: dev sandbox
#                                                       (ontobricks-app → ontobricks_registry DB → ontobricks_registry schema → apps)
#   scripts/bootstrap-lakebase-perms.sh -a ontobricks  # production app on the same instance
#   scripts/bootstrap-lakebase-perms.sh -i ontobricks-app -d databricks_postgres -s ontobricks_registry \
#                                       -a ontobricks-030   # shared default Postgres database
#   scripts/bootstrap-lakebase-perms.sh -i ontobricks-app -d ontobricks_registry -s ontobricks_registry \
#                                       -a ontobricks -a ontobricks-dev  # legacy multi-grantee layout
#
# Prerequisites:
#   - Databricks CLI authenticated against the same workspace as the apps
#   - ``psql`` on PATH (libpq client; ``brew install libpq && brew link --force libpq``)
#   - You own the schema (or otherwise have GRANT OPTION on it).

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."

INSTANCE="${INSTANCE:-ontobricks-app}"
DATABASE="${DATABASE:-ontobricks_registry}"
SCHEMA="${SCHEMA:-ontobricks_registry}"
APPS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        -i|--instance) INSTANCE="$2"; shift 2 ;;
        -d|--database) DATABASE="$2"; shift 2 ;;
        -s|--schema)   SCHEMA="$2"; shift 2 ;;
        -a|--app)      APPS+=("$2"); shift 2 ;;
        -h|--help)
            sed -n '2,32p' "$0"
            exit 0 ;;
        *)
            echo "Unknown argument: $1" >&2
            echo "Run with --help for usage." >&2
            exit 2 ;;
    esac
done

if [[ ${#APPS[@]} -eq 0 ]]; then
    # Defaults to the Lakebase-backed dev app only. The production
    # ``ontobricks`` app currently runs on the Volume backend and
    # would not benefit from these grants — pass ``-a ontobricks``
    # explicitly when you migrate it. ``APP_NAME`` / ``MCP_APP_NAME``
    # come from ``scripts/deploy.config.sh`` when invoked via
    # ``scripts/deploy.sh``.
    APPS=("${APP_NAME:-ontobricks-030}" "${MCP_APP_NAME:-mcp-ontobricks}")
fi

for cmd in databricks psql python3; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "ERROR: '$cmd' not on PATH" >&2
        exit 1
    fi
done

if ! databricks current-user me >/dev/null 2>&1; then
    echo "ERROR: Databricks CLI not authenticated." >&2
    echo "       Run: databricks auth login --host https://<workspace>" >&2
    exit 1
fi

PGUSER="$(databricks current-user me 2>/dev/null \
    | python3 -c 'import sys,json; print(json.load(sys.stdin).get("userName",""))')"
if [[ -z "$PGUSER" ]]; then
    echo "ERROR: Could not resolve your Databricks userName for PGUSER." >&2
    exit 1
fi

# Resolve the project's primary endpoint via the Postgres API.
# OntoBricks targets Lakebase Autoscaling exclusively — the legacy
# ``/api/2.0/database/instances/<name>`` endpoint 404s on Autoscaling-
# only projects, so we walk projects → branches → endpoints and pick
# the first endpoint whose ``status.hosts.host`` is populated.
INSTANCE_NAME="$INSTANCE"
ENDPOINT_INFO="$(INSTANCE_NAME="$INSTANCE_NAME" python3 - <<'PY'
import json, os, subprocess, sys

instance = os.environ["INSTANCE_NAME"]


def api_get(path):
    out = subprocess.run(
        ["databricks", "api", "get", path],
        capture_output=True,
        text=True,
    )
    if out.returncode != 0 or not out.stdout.strip():
        return None
    try:
        return json.loads(out.stdout)
    except json.JSONDecodeError:
        return None


branches = (
    api_get(f"/api/2.0/postgres/projects/{instance}/branches") or {}
).get("branches") or []
for branch in branches:
    branch_path = branch.get("name") or ""
    if not branch_path:
        continue
    endpoints = (
        api_get(f"/api/2.0/postgres/{branch_path}/endpoints") or {}
    ).get("endpoints") or []
    for ep in endpoints:
        hosts = (ep.get("status") or {}).get("hosts") or {}
        host = (hosts.get("host") or "").strip()
        endpoint_path = ep.get("name") or ""
        if host and endpoint_path:
            print(host)
            print(endpoint_path)
            sys.exit(0)
sys.exit(1)
PY
)"
if [[ -z "$ENDPOINT_INFO" ]]; then
    echo "ERROR: Could not resolve a primary endpoint for Lakebase Autoscaling project '${INSTANCE}'." >&2
    echo "       Check 'databricks api get /api/2.0/postgres/projects/${INSTANCE}/branches'" >&2
    echo "       and confirm the project name (it must be the Autoscaling project_id)." >&2
    exit 1
fi
PGHOST="$(printf '%s\n' "$ENDPOINT_INFO" | sed -n 1p)"
ENDPOINT_PATH="$(printf '%s\n' "$ENDPOINT_INFO" | sed -n 2p)"

echo "=== OntoBricks — Lakebase Schema Permission Bootstrap ==="
echo "Project  : ${INSTANCE} (${PGHOST})"
echo "Endpoint : ${ENDPOINT_PATH}"
echo "Database : ${DATABASE}"
echo "Schema   : ${SCHEMA}"
echo "Acting as: ${PGUSER}"
echo "Apps     : ${APPS[*]}"
echo

# Mint a Lakebase JWT via the Autoscaling Postgres API. The legacy
# ``/api/2.0/database/credentials`` mint cannot scope tokens to
# Autoscaling-only project endpoints.
PGPASSWORD="$(databricks api post /api/2.0/postgres/credentials \
    --json "{\"endpoint\":\"${ENDPOINT_PATH}\"}" \
    | python3 -c 'import sys,json; print(json.load(sys.stdin).get("token",""))')"
if [[ -z "$PGPASSWORD" ]]; then
    echo "ERROR: Failed to mint a Lakebase JWT for instance '${INSTANCE}'." >&2
    exit 1
fi
export PGPASSWORD

PGCONN="host=${PGHOST} port=5432 user=${PGUSER} dbname=${DATABASE} sslmode=require"

# Ensure the target schema actually exists. If not, the operator
# probably ran the script before deploying / initialising the app.
if ! psql "$PGCONN" -tAc "SELECT 1 FROM information_schema.schemata WHERE schema_name='${SCHEMA}'" \
        | grep -q 1; then
    echo "ERROR: Schema '${SCHEMA}' does not exist in database '${DATABASE}'." >&2
    echo "       Initialise the registry from the OntoBricks Settings UI first." >&2
    exit 1
fi

FAILED=0
for app in "${APPS[@]}"; do
    sp_id="$(databricks apps get "$app" -o json 2>/dev/null \
        | python3 -c 'import sys,json
try:
    d=json.load(sys.stdin)
except Exception:
    sys.exit(2)
print(d.get("service_principal_client_id") or "")' 2>/dev/null || true)"

    if [[ -z "$sp_id" || "$sp_id" == "None" ]]; then
        echo "  [$app] SKIP — could not resolve service principal (app may not exist yet)"
        FAILED=$((FAILED+1))
        continue
    fi

    echo "  [$app] service principal: $sp_id"

    if ! psql "$PGCONN" -v ON_ERROR_STOP=1 -q <<SQL
GRANT USAGE, CREATE ON SCHEMA "${SCHEMA}" TO "${sp_id}";
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA "${SCHEMA}" TO "${sp_id}";
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA "${SCHEMA}" TO "${sp_id}";
ALTER DEFAULT PRIVILEGES IN SCHEMA "${SCHEMA}"
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "${sp_id}";
ALTER DEFAULT PRIVILEGES IN SCHEMA "${SCHEMA}"
    GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO "${sp_id}";
SQL
    then
        echo "  [$app] ✗ GRANT failed (need ownership of schema '${SCHEMA}' or GRANT OPTION)"
        FAILED=$((FAILED+1))
        continue
    fi

    has_usage="$(psql "$PGCONN" -tAc \
        "SELECT has_schema_privilege('${sp_id}', '${SCHEMA}', 'USAGE')" \
        | tr -d '[:space:]')"
    has_select="$(psql "$PGCONN" -tAc \
        "SELECT has_table_privilege('${sp_id}', '${SCHEMA}.registries', 'SELECT')" \
        | tr -d '[:space:]')"
    if [[ "$has_usage" == "t" && "$has_select" == "t" ]]; then
        echo "  [$app] ✓ granted USAGE + DML on schema '${SCHEMA}'"
    else
        echo "  [$app] ✗ verify failed (USAGE=$has_usage, SELECT registries=$has_select)"
        FAILED=$((FAILED+1))
    fi
done

echo
if [[ $FAILED -eq 0 ]]; then
    echo "=== Done — Lakebase schema bootstrap complete ==="
    exit 0
else
    echo "=== Done with $FAILED failure(s) — see messages above ==="
    exit 1
fi
