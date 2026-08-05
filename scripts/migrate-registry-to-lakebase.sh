#!/usr/bin/env bash
set -euo pipefail

# ── OntoBricks — Registry Volume → Lakebase migration ──────────────
# Pre-v0.4.0 deployments stored the *entire* registry (domains,
# versions, permissions, schedules, global config) as JSON files on a
# Unity Catalog Volume. v0.4.0 moves that structured data to Lakebase
# (Postgres) and keeps the Volume strictly for domain-scoped binary
# artefacts (the ``documents/`` uploads imported by the ontology
# designer).
#
# This script is a one-shot migration: it reads the on-Volume JSON
# tree with the Databricks SDK (Volume Files API) and replays it
# through a live :class:`LakebaseRegistryStore`. The Volume is left
# untouched — binary files keep working as-is; only the JSON-shaped
# config is duplicated into Lakebase.
#
# Idempotent: re-running upserts the same rows and skips already-
# migrated versions.
#
# Prerequisites:
#   - Databricks CLI authenticated against the workspace (same
#     identity that owns the Volume).
#   - ``uv`` available (the script invokes ``uv run`` so psycopg
#     and databricks-sdk are pulled from the project's lakebase extra).
#   - The Lakebase ``database`` resource bound to the app has been
#     deployed so the target schema already exists. Run the schema
#     bootstrap first if needed::
#
#         scripts/bootstrap-lakebase-perms.sh
#
# Usage:
#   scripts/migrate-registry-to-lakebase.sh \\
#       --volume-path /Volumes/<catalog>/<schema>/<volume> \\
#       --lakebase-schema ontobricks_registry \\
#       [--lakebase-database ontobricks_registry] \\
#       [--dry-run]
#
# Defaults are pulled from scripts/deploy.config.sh when present.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."

# Source deploy.config.sh for sensible defaults (project root, app
# name, registry triplet, Lakebase coordinates).
if [[ -f scripts/deploy.config.sh ]]; then
    # shellcheck disable=SC1091
    source scripts/deploy.config.sh
fi

VOLUME_PATH="${VOLUME_PATH:-}"
LAKEBASE_SCHEMA_ARG="${LAKEBASE_SCHEMA:-ontobricks_registry}"
LAKEBASE_DATABASE_ARG="${LAKEBASE_DATABASE:-ontobricks_registry}"
LAKEBASE_INSTANCE_ARG="${LAKEBASE_PROJECT:-ontobricks-app}"
LAKEBASE_BRANCH_ARG="${LAKEBASE_BRANCH:-production}"
DRY_RUN=0

if [[ -n "${REGISTRY_CATALOG:-}" && -n "${REGISTRY_SCHEMA:-}" && -n "${REGISTRY_VOLUME:-}" ]]; then
    VOLUME_PATH="${VOLUME_PATH:-/Volumes/${REGISTRY_CATALOG}/${REGISTRY_SCHEMA}/${REGISTRY_VOLUME}}"
fi

while [[ $# -gt 0 ]]; do
    case "$1" in
        --volume-path)        VOLUME_PATH="$2"; shift 2 ;;
        --lakebase-schema)    LAKEBASE_SCHEMA_ARG="$2"; shift 2 ;;
        --lakebase-database)  LAKEBASE_DATABASE_ARG="$2"; shift 2 ;;
        --lakebase-instance)  LAKEBASE_INSTANCE_ARG="$2"; shift 2 ;;
        --lakebase-branch)    LAKEBASE_BRANCH_ARG="$2"; shift 2 ;;
        --dry-run)            DRY_RUN=1; shift ;;
        -h|--help)
            sed -n '2,40p' "$0"
            exit 0 ;;
        *)
            echo "Unknown argument: $1" >&2
            echo "Run with --help for usage." >&2
            exit 2 ;;
    esac
done

if [[ -z "$VOLUME_PATH" ]]; then
    echo "ERROR: --volume-path is required (or set REGISTRY_CATALOG/SCHEMA/VOLUME in scripts/deploy.config.sh)." >&2
    exit 2
fi

for cmd in databricks uv python3; do
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

# Resolve PG* from the Lakebase Autoscaling endpoint so the embedded
# Python step can connect with a freshly-minted JWT.
ENDPOINT_INFO="$(INSTANCE_NAME="$LAKEBASE_INSTANCE_ARG" BRANCH_NAME="$LAKEBASE_BRANCH_ARG" python3 - <<'PY'
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
endpoints = (api_get(f"/api/2.0/postgres/{branch_path}/endpoints") or {}).get("endpoints") or []
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
    echo "ERROR: Could not resolve a Lakebase endpoint for project '${LAKEBASE_INSTANCE_ARG}' / branch '${LAKEBASE_BRANCH_ARG}'." >&2
    exit 1
fi
PGHOST="$(printf '%s\n' "$ENDPOINT_INFO" | sed -n 1p)"
ENDPOINT_PATH="$(printf '%s\n' "$ENDPOINT_INFO" | sed -n 2p)"

PGUSER="$(databricks current-user me 2>/dev/null \
    | python3 -c 'import sys,json; print(json.load(sys.stdin).get("userName",""))')"
if [[ -z "$PGUSER" ]]; then
    echo "ERROR: Could not resolve your Databricks userName for PGUSER." >&2
    exit 1
fi

PGPASSWORD="$(databricks api post /api/2.0/postgres/credentials \
    --json "{\"endpoint\":\"${ENDPOINT_PATH}\"}" \
    | python3 -c 'import sys,json; print(json.load(sys.stdin).get("token",""))')"
if [[ -z "$PGPASSWORD" ]]; then
    echo "ERROR: Failed to mint a Lakebase JWT for instance '${LAKEBASE_INSTANCE_ARG}'." >&2
    exit 1
fi

export PGHOST PGPORT="5432" PGUSER PGPASSWORD
export PGDATABASE="$LAKEBASE_DATABASE_ARG"

# Parse the volume path so the embedded Python step can re-use the
# triplet to set RegistryCfg.catalog/schema/volume.
read -r CATALOG SCHEMA VOLUME <<<"$(python3 -c '
import sys
p = sys.argv[1].strip("/").split("/")
if len(p) >= 4 and p[0].lower() == "volumes":
    print(p[1], p[2], p[3])
else:
    sys.exit(f"Cannot parse Volume path {sys.argv[1]!r}; expected /Volumes/<c>/<s>/<v>")
' "$VOLUME_PATH")"

echo "=== OntoBricks — Registry Volume → Lakebase migration ==="
echo "Volume path   : $VOLUME_PATH"
echo "Lakebase host : $PGHOST"
echo "Lakebase db   : $PGDATABASE"
echo "Lakebase user : $PGUSER"
echo "Schema        : $LAKEBASE_SCHEMA_ARG"
[[ $DRY_RUN -eq 1 ]] && echo "(dry-run — no writes)"
echo

export REGISTRY_CATALOG="$CATALOG"
export REGISTRY_SCHEMA="$SCHEMA"
export REGISTRY_VOLUME="$VOLUME"
export LAKEBASE_SCHEMA="$LAKEBASE_SCHEMA_ARG"
export LAKEBASE_DATABASE="$LAKEBASE_DATABASE_ARG"
export DRY_RUN

# The Python step adds ``src/`` to ``sys.path`` so it can import the
# OntoBricks code without installing the project as a package. ``uv
# run --extra lakebase`` ensures psycopg/databricks-sdk are present.
uv run --extra lakebase --with databricks-sdk python3 - <<'PY'
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
SRC = ROOT / "src" if (ROOT / "src").exists() else Path.cwd() / "src"
sys.path.insert(0, str(SRC))

DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"

# ------------------------------------------------------------------
# 1. Read everything from the Unity Catalog Volume via the SDK.
# ------------------------------------------------------------------

from databricks.sdk import WorkspaceClient  # noqa: E402

ws = WorkspaceClient()


def vol_list(path):
    try:
        return list(ws.files.list_directory_contents(path))
    except Exception:
        return []


def vol_read_json(path):
    try:
        resp = ws.files.download(path)
    except Exception:
        return None
    raw = resp.contents.read() if hasattr(resp, "contents") else resp.read()
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    if not raw.strip():
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        print(f"  WARN: invalid JSON at {path}: {exc}", file=sys.stderr)
        return None


vol_path = os.environ["REGISTRY_CATALOG"], os.environ["REGISTRY_SCHEMA"], os.environ["REGISTRY_VOLUME"]
VOLUME_ROOT = f"/Volumes/{vol_path[0]}/{vol_path[1]}/{vol_path[2]}"

global_config = vol_read_json(f"{VOLUME_ROOT}/.global_config.json") or {}
print(f"global_config keys: {sorted(global_config.keys())}")

domains_root = None
for candidate in ("domains", "projects"):
    if vol_list(f"{VOLUME_ROOT}/{candidate}"):
        domains_root = f"{VOLUME_ROOT}/{candidate}"
        break

if domains_root is None:
    print("No domains/ or projects/ folder found — only global config will be copied.")
    domain_folders = []
else:
    domain_folders = [
        f.name for f in vol_list(domains_root)
        if getattr(f, "is_directory", False) and not f.name.startswith(".")
    ]
print(f"domain folders ({len(domain_folders)}): {domain_folders}")


def load_domain(folder):
    base = f"{domains_root}/{folder}"
    versions = {}
    for entry in vol_list(base):
        name = entry.name
        if getattr(entry, "is_directory", False) and name.startswith("V"):
            ver = name[1:]
            if ver.isdigit():
                doc = vol_read_json(f"{base}/{name}/{name}.json")
                if doc is not None:
                    versions[ver] = doc
    perms = vol_read_json(f"{base}/.permissions.json")
    history = vol_read_json(f"{base}/.schedule_history.json")
    return versions, perms, history


# ------------------------------------------------------------------
# 2. Replay everything through a real LakebaseRegistryStore.
# ------------------------------------------------------------------

from back.objects.registry.RegistryService import RegistryCfg  # noqa: E402
from back.objects.registry.store import RegistryFactory  # noqa: E402

cfg = RegistryCfg(
    catalog=vol_path[0],
    schema=vol_path[1],
    volume=vol_path[2],
    lakebase_schema=os.environ["LAKEBASE_SCHEMA"],
    lakebase_database=os.environ["LAKEBASE_DATABASE"],
)

if DRY_RUN:
    print("\n[dry-run] would write the following to Lakebase:")
else:
    store = RegistryFactory.from_cfg(cfg)
    ok, msg = store.initialize()
    if not ok:
        print(f"ERROR: Lakebase initialize failed: {msg}", file=sys.stderr)
        sys.exit(1)
    print(f"Lakebase init: {msg}")

stats = {"versions": 0, "perms": 0, "schedules": 0, "history": 0, "global": 0}

if global_config:
    if DRY_RUN:
        print(f"  global_config -> save_global_config({len(global_config)} keys)")
    else:
        ok, m = store.save_global_config(global_config)
        if not ok:
            print(f"  ERROR saving global_config: {m}", file=sys.stderr)
        else:
            stats["global"] = 1

# Schedules live in the global config blob — they are persisted via
# save_schedules separately if the legacy file kept them split.
legacy_schedules = global_config.get("schedules") if isinstance(global_config, dict) else None
if legacy_schedules:
    if DRY_RUN:
        print(f"  schedules -> save_schedules({len(legacy_schedules)} entries)")
    else:
        ok, m = store.save_schedules(legacy_schedules)
        if not ok:
            print(f"  ERROR saving schedules: {m}", file=sys.stderr)
        else:
            stats["schedules"] = len(legacy_schedules)

for folder in domain_folders:
    versions, perms, history = load_domain(folder)
    for ver, doc in sorted(versions.items(), key=lambda kv: int(kv[0])):
        if DRY_RUN:
            print(f"  {folder}/V{ver} -> write_version")
        else:
            ok, m = store.write_version(folder, ver, doc)
            if not ok:
                print(f"  ERROR write_version({folder}, {ver}): {m}", file=sys.stderr)
            else:
                stats["versions"] += 1
    if perms is not None:
        if DRY_RUN:
            print(f"  {folder}/.permissions.json -> save_domain_permissions")
        else:
            ok, m = store.save_domain_permissions(folder, perms)
            if not ok:
                print(f"  ERROR save_domain_permissions({folder}): {m}", file=sys.stderr)
            else:
                stats["perms"] += 1
    if isinstance(history, list):
        for entry in history:
            if DRY_RUN:
                print(f"  {folder}/.schedule_history.json -> append_schedule_history")
            else:
                store.append_schedule_history(folder, entry, max_entries=200)
                stats["history"] += 1

print("\n=== Migration summary ===")
for k, v in stats.items():
    print(f"  {k}: {v}")
if DRY_RUN:
    print("(dry-run — nothing was written)")
PY

echo
echo "=== Done ==="
