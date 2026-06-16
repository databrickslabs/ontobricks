#!/usr/bin/env bash
set -euo pipefail

# ── OntoBricks — Lakebase Schema Permission Bootstrap ───────────────
# A Databricks Apps service principal is created without any privileges
# on Lakebase Postgres objects, even when the app's ``postgres:`` resource
# binding is wired correctly. The first time the app tries to read a schema
# it silently sees an empty information_schema — the canonical false negative
# for a missing USAGE grant.
#
# This script connects to the Lakebase instance with your *human*
# credentials (as the schema owner), looks up each app's service
# principal client id, and grants it the privileges OntoBricks needs:
#
#   - CAN_USE on the Lakebase project (control-plane, both API endpoints)
#   - USAGE + CREATE on the Postgres schema (data-plane)
#   - SELECT/INSERT/UPDATE/DELETE on every existing table
#   - USAGE/SELECT/UPDATE on every existing sequence (bigserial PKs)
#   - The same set as ALTER DEFAULT PRIVILEGES so future tables inherit
#   - (managed_synced only) ALL PRIVILEGES on the UC catalog so the SP
#     can read back synced tables it created or that were pre-created by
#     another principal.  Pass -c/--catalog to enable this grant.
#
# Idempotent — re-running is a no-op for objects that already carry the
# privileges.
#
# ── Generic per-schema grant tool ──────────────────────────────────────────
#
# This is a single-schema grant tool: -i/-b/-d locate the Lakebase
# project / branch / database and -s names the schema to grant on.
#
# OntoBricks has two distinct Lakebase schemas, which may live in the
# SAME or in DIFFERENT Lakebase projects:
#
#   1. Registry schema  (e.g. ontobricks_registry)
#      Coords : deploy.config.sh → LAKEBASE_PROJECT / LAKEBASE_BRANCH /
#               LAKEBASE_REGISTRY_DATABASE / LAKEBASE_REGISTRY_SCHEMA
#      → ``scripts/deploy.sh`` grants this one automatically on every
#        dev-lakebase deploy (re-run after "Settings > Registry > Initialize"
#        if the schema did not exist yet at deploy time).
#
#   2. Graph schema  (e.g. ontobricks_graph)
#      Configured IN-APP (Settings → Graph DB) and may live in a
#      DIFFERENT Lakebase project. ``deploy.sh`` does NOT touch it — the
#      in-app "Create graph DB" flow runs this grant, or run it manually
#      with the graph DB's own project/branch/database below.
#
# Manual runs:
#
#     # Registry
#     scripts/bootstrap-lakebase-perms.sh \
#       -i ontobricks-app -b production -d ontobricks_registry \
#       -s ontobricks_registry -a ontobricks-030 -a mcp-ontobricks
#
#     # Graph DB (use the graph project/branch/database — may differ)
#     scripts/bootstrap-lakebase-perms.sh \
#       -i <graph-project> -b <graph-branch> -d <graph-database> \
#       -s ontobricks_graph -a ontobricks-030 -a mcp-ontobricks
#
# Prerequisites:
#   - Databricks CLI authenticated against the same workspace as the apps
#   - ``psql`` on PATH (libpq client; ``brew install libpq && brew link --force libpq``)
#   - You own the schema (or otherwise have GRANT OPTION on it).

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."

INSTANCE="${INSTANCE:-ontobricks-app}"
BRANCH="${BRANCH:-${LAKEBASE_BRANCH:-production}}"
DATABASE="${DATABASE:-ontobricks_registry}"
SCHEMA="${SCHEMA:-ontobricks_registry}"
# Unity Catalog catalog name — when set the SP receives ALL PRIVILEGES on
# the catalog so it can read back synced tables regardless of who created them.
# Required for managed_synced mode.  Pass -c/--catalog or set the env var.
UC_CATALOG="${UC_CATALOG:-}"
APPS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        -i|--instance) INSTANCE="$2"; shift 2 ;;
        -b|--branch)   BRANCH="$2"; shift 2 ;;
        -d|--database) DATABASE="$2"; shift 2 ;;
        -s|--schema)   SCHEMA="$2"; shift 2 ;;
        -c|--catalog)  UC_CATALOG="$2"; shift 2 ;;
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
# only projects, so we resolve the endpoint from the configured
# project+branch pair.
INSTANCE_NAME="$INSTANCE"
BRANCH_NAME="$BRANCH"
ENDPOINT_INFO="$(INSTANCE_NAME="$INSTANCE_NAME" BRANCH_NAME="$BRANCH_NAME" python3 - <<'PY'
import json, os, subprocess, sys

instance = os.environ["INSTANCE_NAME"]
branch = os.environ["BRANCH_NAME"]


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


branch_path = f"projects/{instance}/branches/{branch}"
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
    echo "ERROR: Could not resolve a primary endpoint for Lakebase Autoscaling project '${INSTANCE}' on branch '${BRANCH}'." >&2
    echo "       Check 'databricks api get /api/2.0/postgres/projects/${INSTANCE}/branches/${BRANCH}/endpoints'" >&2
    echo "       and confirm project/branch values match the app postgres resource binding." >&2
    exit 1
fi
PGHOST="$(printf '%s\n' "$ENDPOINT_INFO" | sed -n 1p)"
ENDPOINT_PATH="$(printf '%s\n' "$ENDPOINT_INFO" | sed -n 2p)"

echo "=== OntoBricks — Lakebase Schema Permission Bootstrap ==="
echo "Project  : ${INSTANCE} (${PGHOST})"
echo "Branch   : ${BRANCH}"
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

# ── Step 1: Instance-level CAN_USE (runs even on a fresh DB before init) ────
# Must happen BEFORE the schema guard so the SP can call the synced-tables
# API immediately after the first "Build" — even if the registry schema
# doesn't exist yet (e.g. between deploy and Settings → Registry → Initialize).
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
    echo "  [$app] granting CAN_USE on Lakebase project '${INSTANCE}'..."
    _can_use_ok=false
    if databricks api patch "/api/2.0/permissions/database-projects/${INSTANCE}" \
        --json "{\"access_control_list\": [{\"service_principal_name\": \"${sp_id}\", \"permission_level\": \"CAN_USE\"}]}" \
        >/dev/null 2>&1; then
        echo "  [$app] ✓ CAN_USE granted via database-projects (Autoscaling)"
        _can_use_ok=true
    fi
    if databricks api patch "/api/2.0/permissions/database-instances/${INSTANCE}" \
        --json "{\"access_control_list\": [{\"service_principal_name\": \"${sp_id}\", \"permission_level\": \"CAN_USE\"}]}" \
        >/dev/null 2>&1; then
        echo "  [$app] ✓ CAN_USE granted via database-instances (Provisioned / fallback)"
        _can_use_ok=true
    fi
    if ! $_can_use_ok; then
        echo "  [$app] ✗ Both CAN_USE grant attempts failed."
        FAILED=$((FAILED+1))
    fi
done

# ── Step 2: Postgres schema grants (requires the schema to exist) ────────────
# Ensure the target schema actually exists. If not, the operator
# probably ran the script before initialising the registry.
if ! psql "$PGCONN" -tAc "SELECT 1 FROM information_schema.schemata WHERE schema_name='${SCHEMA}'" \
        | grep -q 1; then
    echo "ERROR: Schema '${SCHEMA}' does not exist in database '${DATABASE}'." >&2
    echo "       Initialise the registry from the OntoBricks Settings UI first." >&2
    echo "       CAN_USE grants above were applied — re-run after initialisation" >&2
    echo "       to apply the Postgres schema grants." >&2
    exit 1
fi

# ── Step 2b: Registry schema migrations (idempotent — run as schema owner) ────
# Apply DDL columns/indexes added after the initial Initialize.
# Only runs when the registry table `domain_versions` actually exists in this
# schema — skipped silently for the graph schema which has a different layout.
_HAS_DOMAIN_VERSIONS="$(psql "$PGCONN" -tAc \
    "SELECT 1 FROM information_schema.tables WHERE table_schema='${SCHEMA}' AND table_name='domain_versions'" \
    | tr -d '[:space:]')"
if [[ "$_HAS_DOMAIN_VERSIONS" == "1" ]]; then
    echo "  Applying registry schema migrations..."
    if psql "$PGCONN" -v ON_ERROR_STOP=1 -q <<SQL
-- domain_versions.status (lifecycle column added after initial release)
ALTER TABLE "${SCHEMA}".domain_versions
    ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'DRAFT';
CREATE INDEX IF NOT EXISTS idx_domain_versions_status
    ON "${SCHEMA}".domain_versions(domain_id, status);

-- domains.review_quorum (per-domain sign-off quorum added after initial release)
ALTER TABLE "${SCHEMA}".domains
    ADD COLUMN IF NOT EXISTS review_quorum integer NOT NULL DEFAULT 1;

-- build_runs (build history table added after initial release)
CREATE TABLE IF NOT EXISTS "${SCHEMA}".build_runs (
    id                  bigserial PRIMARY KEY,
    domain_id           uuid NOT NULL
                        REFERENCES "${SCHEMA}".domains(id) ON DELETE CASCADE,
    version             text NOT NULL,
    build_kind          text NOT NULL DEFAULT 'session',
    status              text NOT NULL,
    message             text NOT NULL DEFAULT '',
    error               text NOT NULL DEFAULT '',
    started_at          timestamptz NOT NULL DEFAULT now(),
    finished_at         timestamptz,
    duration_s          double precision NOT NULL DEFAULT 0,
    triple_count        bigint NOT NULL DEFAULT 0,
    entity_count        integer NOT NULL DEFAULT 0,
    relationship_count  integer NOT NULL DEFAULT 0,
    sql_chars           integer NOT NULL DEFAULT 0,
    graph_engine        text NOT NULL DEFAULT '',
    sync_mode           text NOT NULL DEFAULT '',
    view_table          text NOT NULL DEFAULT '',
    graph_name          text NOT NULL DEFAULT '',
    task_id             text NOT NULL DEFAULT '',
    phase_times         jsonb NOT NULL DEFAULT '{}'::jsonb,
    stats               jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_build_runs_domain_version
    ON "${SCHEMA}".build_runs(domain_id, version, started_at DESC);

-- domain_review_events (validation/review audit log added after initial release)
CREATE TABLE IF NOT EXISTS "${SCHEMA}".domain_review_events (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    domain_id       uuid NOT NULL
                    REFERENCES "${SCHEMA}".domains(id) ON DELETE CASCADE,
    version         text NOT NULL,
    actor           text NOT NULL,
    action          text NOT NULL,
    from_status     text NOT NULL DEFAULT '',
    to_status       text NOT NULL DEFAULT '',
    comment         text NOT NULL DEFAULT '',
    meta            jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_review_events_domain_version
    ON "${SCHEMA}".domain_review_events(domain_id, version, created_at);
SQL
    then
        echo "  ✓ schema migrations applied (domain_versions.status, domains.review_quorum, build_runs, domain_review_events)"
    else
        echo "  ⚠ schema migration failed — continuing (SP grants below may partially succeed)"
    fi
fi

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

    # ── Postgres schema: USAGE + DML ─────────────────────────────────────────
    # CAN_USE (instance-level) was already granted in Step 1 above.
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
        echo "  [$app] ✗ Postgres GRANT failed (need ownership of schema '${SCHEMA}' or GRANT OPTION)"
        FAILED=$((FAILED+1))
        continue
    fi

    has_usage="$(psql "$PGCONN" -tAc \
        "SELECT has_schema_privilege('${sp_id}', '${SCHEMA}', 'USAGE')" \
        | tr -d '[:space:]')"
    # Check table-level SELECT on the first available table in the schema
    # (the `registries` table only exists in the registry schema, not the graph schema).
    first_table="$(psql "$PGCONN" -tAc \
        "SELECT tablename FROM pg_tables WHERE schemaname='${SCHEMA}' LIMIT 1" \
        | tr -d '[:space:]')"
    if [[ -n "$first_table" ]]; then
        has_select="$(psql "$PGCONN" -tAc \
            "SELECT has_table_privilege('${sp_id}', '${SCHEMA}.${first_table}', 'SELECT')" \
            | tr -d '[:space:]')"
    else
        # No tables yet (schema exists but is empty) — USAGE is enough to verify.
        has_select="t"
    fi
    if [[ "$has_usage" == "t" && "$has_select" == "t" ]]; then
        echo "  [$app] ✓ granted USAGE + DML on schema '${SCHEMA}'"
    else
        echo "  [$app] ✗ verify failed (USAGE=$has_usage, SELECT=${first_table:-<no tables>}=$has_select)"
        FAILED=$((FAILED+1))
    fi

    # ── 3. Unity Catalog: ALL PRIVILEGES on catalog (managed_synced only) ───
    # The Lakebase synced-table API (GET /api/2.0/database/synced_tables/{fqn})
    # returns 404 when the caller lacks SELECT on the resulting UC table — even
    # when it created the table.  This happens when:
    #   a) The table was created by a different principal (local dev run) and
    #      the SP is not the owner.
    #   b) The SP has UC CAN_USE but the catalog-level SELECT has not been
    #      explicitly applied.
    # Granting ALL PRIVILEGES on the catalog covers both cases and is idempotent.
    # Only done when -c/--catalog (or UC_CATALOG env var) is provided.
    if [[ -n "${UC_CATALOG:-}" ]]; then
        echo "  [$app] granting ALL PRIVILEGES on UC catalog '${UC_CATALOG}'..."
        if databricks grants update CATALOG "${UC_CATALOG}" \
            --json "{\"changes\": [{\"principal\": \"${sp_id}\", \"add\": [\"ALL_PRIVILEGES\"]}]}" \
            >/dev/null 2>&1; then
            echo "  [$app] ✓ UC ALL_PRIVILEGES granted on catalog '${UC_CATALOG}'"
        else
            echo "  [$app] ⚠ UC catalog grant failed (you may lack MANAGE on catalog '${UC_CATALOG}')"
            echo "          Run manually: databricks grants update CATALOG ${UC_CATALOG} \\"
            echo "            --json '{\"changes\":[{\"principal\":\"${sp_id}\",\"add\":[\"ALL_PRIVILEGES\"]}]}'"
        fi
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
