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
#   - (when -c/--catalog) ALL PRIVILEGES on the UC catalog (control-plane;
#     applied before the schema exists so a first deploy still grants it —
#     Initialize cannot self-serve this because the app SP lacks MANAGE)
#   - USAGE + CREATE on the Postgres schema (data-plane)
#   - SELECT/INSERT/UPDATE/DELETE on every existing table
#   - USAGE/SELECT/UPDATE on every existing sequence (bigserial PKs)
#   - The same set as ALTER DEFAULT PRIVILEGES so future tables inherit
#
# Pass -c/--catalog (or UC_CATALOG) to enable the UC catalog grant — required
# for managed_synced mode so the SP can read back synced tables.
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
#               LAKEBASE_DATABASE / LAKEBASE_SCHEMA
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
#     scripts/bootstrap/lakebase-perms.sh \
#       -i ontobricks-app -b production -d ontobricks_registry \
#       -s ontobricks_registry -a ontobricks-030 -a mcp-ontobricks
#
#     # Graph DB (use the graph project/branch/database — may differ)
#     scripts/bootstrap/lakebase-perms.sh \
#       -i <graph-project> -b <graph-branch> -d <graph-database> \
#       -s ontobricks_graph -a ontobricks-030 -a mcp-ontobricks
#
# Prerequisites:
#   - Databricks CLI authenticated against the same workspace as the apps
#   - ``psql`` on PATH (libpq client; ``brew install libpq && brew link --force libpq``)
#   - You own the schema (or otherwise have GRANT OPTION on it).

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# scripts/bootstrap/ → scripts/ → repo root (#133).
cd "$SCRIPT_DIR/../.."

# _lakebase-diag.sh lives in scripts/_internal/ (moved there by the scripts/
# reorg, commit 2c6ef76); this script is in scripts/bootstrap/, so reference
# the sibling _internal dir rather than SCRIPT_DIR itself.
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/../_internal/_lakebase-diag.sh"

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
    echo "       Run: databricks auth login --host https://<workspace>${DATABRICKS_CONFIG_PROFILE:+ --profile $DATABRICKS_CONFIG_PROFILE}" >&2
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
    _lakebase_print_diag_hints \
        "no postgres endpoint for project/branch" \
        "${INSTANCE}" "${BRANCH}" "${DATABASE}"
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

# Mint a Lakebase JWT via `databricks postgres generate-database-credential`
# (verified on Databricks CLI 1.14.1+). --output json is required: the CLI's
# default output format is text/table unless the user's config or
# DATABRICKS_OUTPUT_FORMAT already forces JSON, and without it the parser
# below dies silently under `set -euo pipefail`.
CRED_ERR_FILE="$(mktemp)"
if ! CRED_JSON="$(databricks postgres generate-database-credential "${ENDPOINT_PATH}" --output json 2>"$CRED_ERR_FILE")"; then
    CRED_ERROR="$(cat "$CRED_ERR_FILE")"
    rm -f "$CRED_ERR_FILE"
    echo "ERROR: Failed to mint a Lakebase JWT for project '${INSTANCE}' on branch '${BRANCH}' (CLI exited non-zero)." >&2
    [[ -n "$CRED_ERROR" ]] && printf '%s\n' "$CRED_ERROR" >&2
    _lakebase_print_diag_hints \
        "postgres generate-database-credential failed (endpoint: ${ENDPOINT_PATH})" \
        "${INSTANCE}" "${BRANCH}" "${DATABASE}"
    exit 1
fi
CRED_WARNING="$(cat "$CRED_ERR_FILE")"
rm -f "$CRED_ERR_FILE"
PGPASSWORD="$(printf '%s' "$CRED_JSON" | python3 -c '
import sys, json
try:
    data = json.load(sys.stdin)
except json.JSONDecodeError:
    data = {}
token = data.get("token") if isinstance(data, dict) else ""
print(token or "", end="")
')"
if [[ -z "$PGPASSWORD" ]]; then
    echo "ERROR: Failed to mint a Lakebase JWT for project '${INSTANCE}' on branch '${BRANCH}' — CLI succeeded but response was not valid JSON or had no token field." >&2
    [[ -n "$CRED_WARNING" ]] && printf '%s\n' "$CRED_WARNING" >&2
    _lakebase_print_diag_hints \
        "postgres generate-database-credential returned unparseable/empty credential (endpoint: ${ENDPOINT_PATH})" \
        "${INSTANCE}" "${BRANCH}" "${DATABASE}"
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

    # ── UC catalog ALL_PRIVILEGES (control-plane — no schema required) ──
    # Must run BEFORE the schema guard. On a first deploy the registry
    # schema does not exist yet, so the script used to exit early and
    # never grant UC privileges; Initialize then tried (and failed) as
    # the app SP which lacks MANAGE on the catalog (#137).
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

# ── Step 1b: pgcrypto (DB-level; required for companion object_hash) ──────────
# Graph companion / sync tables use a generated ``object_hash`` column via
# ``digest(..., 'sha256')``.
#
# The extension MUST live in ``public``: app connections run
# ``SET search_path TO "<graph_schema>", public``, and a bare
# ``CREATE EXTENSION`` installs into the *first* search_path entry — i.e. a
# graph schema. ``IF NOT EXISTS`` then becomes a permanent no-op, so renaming
# the graph schema strands digest() out of reach. Pin it to public and
# relocate a stranded install.
if ! psql "$PGCONN" -tAc "SELECT 1" >/dev/null 2>&1; then
    echo "ERROR: Cannot connect to Lakebase Postgres (host=${PGHOST}, dbname=${DATABASE})." >&2
    _lakebase_print_diag_hints \
        "psql connection failed — wrong datname or endpoint" \
        "${INSTANCE}" "${BRANCH}" "${DATABASE}"
    exit 1
fi
echo "  Ensuring pgcrypto extension in public (digest for companion object_hash)..."
# Relocation of an app-owned extension fails for a non-owner admin; the app
# self-heals in that case (it owns the extension), so warn rather than abort.
psql "$PGCONN" -q <<'SQL' 2>&1 | grep -vE '^(NOTICE|CREATE EXTENSION|DO)' || true
CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;
DO $$
DECLARE ext_schema text;
BEGIN
    SELECT n.nspname INTO ext_schema
    FROM pg_extension e
    JOIN pg_namespace n ON n.oid = e.extnamespace
    WHERE e.extname = 'pgcrypto';
    IF ext_schema IS NOT NULL AND ext_schema <> 'public' THEN
        RAISE NOTICE 'Relocating pgcrypto from % to public', ext_schema;
        BEGIN
            EXECUTE 'ALTER EXTENSION pgcrypto SET SCHEMA public';
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Could not relocate pgcrypto to public: %', SQLERRM;
        END;
    END IF;
END $$;
SQL

_PGCRYPTO_SCHEMA="$(psql "$PGCONN" -tAc \
    "SELECT n.nspname FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.extname='pgcrypto'" \
    2>/dev/null | tr -d '[:space:]')"
if [[ "$_PGCRYPTO_SCHEMA" == "public" ]]; then
    echo "  ✓ pgcrypto ready in public (digest available)"
elif [[ -n "$_PGCRYPTO_SCHEMA" ]]; then
    echo "  ⚠ pgcrypto lives in schema '${_PGCRYPTO_SCHEMA}', not public." >&2
    echo "    It is only visible to connections whose search_path includes that" >&2
    echo "    schema. The app relocates it automatically on the next Build (it owns" >&2
    echo "    the extension). To fix manually, run as its owner:" >&2
    echo "      ALTER EXTENSION pgcrypto SET SCHEMA public;" >&2
else
    echo "  ⚠ pgcrypto is not installed on '${DATABASE}' — the app installs it on" >&2
    echo "    first Build; re-run this script as an admin if that fails." >&2
fi

# ── Step 2: Postgres schema grants (requires the schema to exist) ────────────
# Ensure the target schema actually exists. If not, the operator
# probably ran the script before initialising the registry.
if ! psql "$PGCONN" -tAc "SELECT 1 FROM information_schema.schemata WHERE schema_name='${SCHEMA}'" \
        2>/dev/null | grep -q 1; then
    echo "ERROR: Schema '${SCHEMA}' does not exist in database '${DATABASE}'." >&2
    echo "       Initialise the registry from the OntoBricks Settings UI first." >&2
    echo "       CAN_USE / UC catalog / pgcrypto above were applied — re-run" >&2
    echo "       after initialisation to apply the Postgres schema grants." >&2
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
    # PostgreSQL checks table ownership before evaluating IF NOT EXISTS.
    # Registries initialized by the app have app-owned tables even when the
    # schema itself is human-owned, so replaying already-current no-op DDL
    # fails with "must be owner of table". Reuse the read-only preflight's
    # canonical migration expectations and only invoke DDL when work remains.
    _MIGRATIONS_CURRENT="0"
    if _MIGRATIONS_CURRENT="$(PGCONN="$PGCONN" python3 - "$SCHEMA" <<'PY'
import os
import sys

from scripts._internal._lakebase_preflight import inspect_migrations

pending, stale, errors = inspect_migrations(os.environ.copy(), sys.argv[1])
if errors:
    print(
        "Could not inspect registry migration state: " + "; ".join(errors),
        file=sys.stderr,
    )
    raise SystemExit(1)
print("1" if not pending and not stale else "0")
PY
)"; then
        :
    else
        echo "  ⚠ migration state inspection failed; falling back to idempotent DDL" >&2
        _MIGRATIONS_CURRENT="0"
    fi

    if [[ "$_MIGRATIONS_CURRENT" == "1" ]]; then
        echo "  ✓ Registry schema migrations already current; skipping DDL."
    else
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

-- domains.mcp_policy (per-domain MCP tool + context policy added in v0.8)
ALTER TABLE "${SCHEMA}".domains
    ADD COLUMN IF NOT EXISTS mcp_policy jsonb NOT NULL DEFAULT '{}'::jsonb;

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

-- graph_analytics (async KG analytics cache added after initial release —
-- one row per (domain_id, version), replaced on every successful recompute)
CREATE TABLE IF NOT EXISTS "${SCHEMA}".graph_analytics (
    domain_id    uuid NOT NULL
                 REFERENCES "${SCHEMA}".domains(id) ON DELETE CASCADE,
    version      text NOT NULL,
    status       text NOT NULL DEFAULT 'completed',
    graph_name   text NOT NULL DEFAULT '',
    class_filter jsonb NOT NULL DEFAULT '[]'::jsonb,
    stats        jsonb NOT NULL DEFAULT '{}'::jsonb,
    top_pagerank jsonb NOT NULL DEFAULT '[]'::jsonb,
    result       jsonb NOT NULL DEFAULT '{}'::jsonb,
    error        text NOT NULL DEFAULT '',
    task_id      text NOT NULL DEFAULT '',
    duration_ms  bigint NOT NULL DEFAULT 0,
    computed_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (domain_id, version)
);

-- graph_analytics_runs (append-only analysis run history)
CREATE TABLE IF NOT EXISTS "${SCHEMA}".graph_analytics_runs (
    id                  bigserial PRIMARY KEY,
    domain_id           uuid NOT NULL
                        REFERENCES "${SCHEMA}".domains(id) ON DELETE CASCADE,
    version             text NOT NULL,
    status              text NOT NULL DEFAULT 'completed',
    class_filter        jsonb NOT NULL DEFAULT '[]'::jsonb,
    node_count          bigint NOT NULL DEFAULT 0,
    edge_count          bigint NOT NULL DEFAULT 0,
    connected_components integer NOT NULL DEFAULT 0,
    avg_degree          double precision NOT NULL DEFAULT 0,
    density             double precision NOT NULL DEFAULT 0,
    duration_ms         bigint NOT NULL DEFAULT 0,
    task_id             text NOT NULL DEFAULT '',
    error               text NOT NULL DEFAULT '',
    computed_at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_graph_analytics_runs_domain_version
    ON "${SCHEMA}".graph_analytics_runs(domain_id, version, computed_at DESC);

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

-- domain_comments + domain_tasks (collaborative Discussions, v0.6 — final,
-- domain-wide shape; mirrors schema.sql). Created here so every deploy /
-- in-place update provisions them as the schema owner instead of relying on
-- the app's lazy self-heal. Converge any pre-existing table created with the
-- early per-anchor columns onto the final shape (drop anchor_type/anchor_ref).
CREATE TABLE IF NOT EXISTS "${SCHEMA}".domain_comments (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    domain_id   uuid NOT NULL
                REFERENCES "${SCHEMA}".domains(id) ON DELETE CASCADE,
    version     text NOT NULL,
    parent_id   uuid REFERENCES "${SCHEMA}".domain_comments(id) ON DELETE CASCADE,
    author      text NOT NULL,
    body        text NOT NULL DEFAULT '',
    resolved    boolean NOT NULL DEFAULT false,
    created_at  timestamptz NOT NULL DEFAULT now()
);
DROP INDEX IF EXISTS "${SCHEMA}".idx_domain_comments_anchor;
ALTER TABLE IF EXISTS "${SCHEMA}".domain_comments DROP COLUMN IF EXISTS anchor_type;
ALTER TABLE IF EXISTS "${SCHEMA}".domain_comments DROP COLUMN IF EXISTS anchor_ref;
CREATE INDEX IF NOT EXISTS idx_domain_comments_lookup
    ON "${SCHEMA}".domain_comments(domain_id, version, created_at);

CREATE TABLE IF NOT EXISTS "${SCHEMA}".domain_tasks (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    domain_id   uuid NOT NULL
                REFERENCES "${SCHEMA}".domains(id) ON DELETE CASCADE,
    version     text NOT NULL,
    assignee    text NOT NULL,
    created_by  text NOT NULL,
    title       text NOT NULL,
    description text NOT NULL DEFAULT '',
    status      text NOT NULL DEFAULT 'open'
                CHECK (status IN ('open', 'in_progress', 'done', 'cancelled')),
    due_date    date,
    comment_id  uuid REFERENCES "${SCHEMA}".domain_comments(id) ON DELETE SET NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_domain_tasks_assignee
    ON "${SCHEMA}".domain_tasks(lower(assignee), status);
CREATE INDEX IF NOT EXISTS idx_domain_tasks_domain
    ON "${SCHEMA}".domain_tasks(domain_id, version);

-- domain_edit_locks (single-editor DRAFT lock, v0.6 — mirrors schema.sql).
-- Created here as the schema owner because the app's lazy self-heal cannot:
-- the FK REFERENCES domains(id) needs a privilege the app service principal
-- lacks, so without this migration the table never exists on an in-place
-- update and every opener is falsely shown as a read-only viewer.
CREATE TABLE IF NOT EXISTS "${SCHEMA}".domain_edit_locks (
    domain_id      uuid NOT NULL
                   REFERENCES "${SCHEMA}".domains(id) ON DELETE CASCADE,
    version        text NOT NULL,
    holder_email   text NOT NULL,
    holder_name    text NOT NULL DEFAULT '',
    holder_session text NOT NULL DEFAULT '',
    acquired_at    timestamptz NOT NULL DEFAULT now(),
    heartbeat_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (domain_id, version)
);

-- domain_change_events (ontology/mapping audit trail, v0.6 — mirrors
-- schema.sql). Same owner-provisioning rationale as domain_edit_locks: the
-- app's lazy self-heal cannot create the FK to domains, so without this the
-- audit trail is silently empty on an in-place update.
CREATE TABLE IF NOT EXISTS "${SCHEMA}".domain_change_events (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    domain_id       uuid NOT NULL
                    REFERENCES "${SCHEMA}".domains(id) ON DELETE CASCADE,
    version         text NOT NULL,
    actor           text NOT NULL DEFAULT '',
    source          text NOT NULL DEFAULT 'user'
                    CHECK (source IN ('user', 'agent')),
    action          text NOT NULL,
    entity_type     text NOT NULL DEFAULT '',
    entity_ref      text NOT NULL DEFAULT '',
    summary         text NOT NULL DEFAULT '',
    meta            jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at     timestamptz NOT NULL DEFAULT now(),
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_change_events_domain_version
    ON "${SCHEMA}".domain_change_events(domain_id, version, occurred_at);

-- schedules / schedule_runs generic task registry (v0.7 — mirrors
-- schema.sql + LakebaseRegistryStore._ensure_schedule_task_columns).
-- Widens the build-only scheduler to (task_type, domain, target_key) so
-- Analytics / Inference / Cohort share the same tables. Applied here as
-- the schema owner because the unique-constraint swap needs ownership;
-- without this, make deploy alone leaves an in-place 0.6→0.7 registry
-- on the legacy UNIQUE (registry_id, domain_name) shape.
ALTER TABLE "${SCHEMA}".schedules
    ADD COLUMN IF NOT EXISTS task_type text NOT NULL DEFAULT 'build',
    ADD COLUMN IF NOT EXISTS target_key text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS config jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS last_count bigint NOT NULL DEFAULT 0;
ALTER TABLE "${SCHEMA}".schedule_runs
    ADD COLUMN IF NOT EXISTS task_type text NOT NULL DEFAULT 'build',
    ADD COLUMN IF NOT EXISTS target_key text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS detail jsonb NOT NULL DEFAULT '{}'::jsonb;
-- Fold the legacy build-only column into config (idempotent: only rows
-- still holding the empty default).
UPDATE "${SCHEMA}".schedules
SET config = jsonb_build_object('drop_existing', COALESCE(drop_existing, true))
WHERE config = '{}'::jsonb AND task_type = 'build';
DO \$sched\$
DECLARE
    cname text;
BEGIN
    FOR cname IN
        SELECT con.conname
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        JOIN pg_namespace ns ON ns.oid = rel.relnamespace
        WHERE ns.nspname = '${SCHEMA}'
          AND rel.relname = 'schedules'
          AND con.contype = 'u'
          AND pg_get_constraintdef(con.oid)
              = 'UNIQUE (registry_id, domain_name)'
    LOOP
        EXECUTE format(
            'ALTER TABLE %I.schedules DROP CONSTRAINT IF EXISTS %I',
            '${SCHEMA}', cname
        );
    END LOOP;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        JOIN pg_namespace ns ON ns.oid = rel.relnamespace
        WHERE ns.nspname = '${SCHEMA}'
          AND rel.relname = 'schedules'
          AND con.conname = 'schedules_type_domain_target_key'
    ) THEN
        EXECUTE format(
            'ALTER TABLE %I.schedules
                ADD CONSTRAINT schedules_type_domain_target_key
                UNIQUE (registry_id, task_type, domain_name, target_key)',
            '${SCHEMA}'
        );
    END IF;
END
\$sched\$;
DROP INDEX IF EXISTS "${SCHEMA}".idx_schedule_runs_domain;
CREATE INDEX IF NOT EXISTS idx_schedule_runs_domain
    ON "${SCHEMA}".schedule_runs(
        registry_id, task_type, domain_name, target_key, run_ts DESC
    );
SQL
        then
            echo "  ✓ schema migrations applied (domain_versions.status, domains.review_quorum, domains.mcp_policy, build_runs, graph_analytics, graph_analytics_runs, domain_review_events, domain_comments, domain_tasks, domain_edit_locks, domain_change_events, schedules/schedule_runs generic tasks)"
        else
            echo "  ⚠ schema migration failed — continuing (SP grants below may partially succeed)"
        fi
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
done

echo
if [[ $FAILED -eq 0 ]]; then
    echo "=== Done — Lakebase schema bootstrap complete ==="
    exit 0
else
    echo "=== Done with $FAILED failure(s) — see messages above ==="
    exit 1
fi
