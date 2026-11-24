#!/usr/bin/env python3
"""Install the assembled Marketplace folder into a workspace, like Marketplace would.

A Marketplace install takes the ``ontobricks/`` subfolder verbatim, binds the
resources named in ``manifest.yaml``, grants the manifest's ``user_api_scopes``
and runs the fixed ``app.yaml``. It does NOT run any of the ``make deploy``
bootstrap steps (``app-permissions.sh``, ``lakebase-perms.sh``, the generated
``app.yaml``). The manifest can't declare a Lakebase database (PLAN.md Q1), so
the installer binds one right after install; this helper does that in the same
step when ``--lakebase-branch`` is given. Steps:

1. assemble ``ontobricks/`` (``scripts/marketplace/assemble.py``) into a temp dir
2. upload it to ``/Workspace/Users/<you>/ontobricks-marketplace-test/<app>``
   (replacing any previous upload, so deleted files don't linger)
3. create the app, or replace its resources if it already exists
4. deploy from the uploaded folder

Usage::

    python3 scripts/marketplace/emulate_install.py \\
        --profile <profile> --warehouse-id <id> \\
        --lakebase-branch projects/<project>/branches/production \\
        [--lakebase-database databricks_postgres] [--app-name ontobricks-mp-test] \\
        [--grant-self] [--dry-run]

``--profile`` is required on purpose: never deploy to a default profile by
accident. ``--grant-self`` additionally grants the app's service principal
CAN_MANAGE on its own app (what ``make deploy`` does, and what Marketplace may
not); leave it off to answer Q4.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
TEST_FOLDER = "ontobricks-marketplace-test"
DEFAULT_APP_NAME = "ontobricks-mp-test"
#: Description stamped on apps this helper creates; it is the only thing that
#: lets a re-run replace an existing app's resources (see ``may_replace``).
TEST_APP_DESCRIPTION = "OntoBricks: Marketplace install test"

_APP_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,28}[a-z0-9]$")
_BRANCH_RE = re.compile(r"^projects/[a-z0-9-]+/branches/[a-z0-9-]+$")


def validate_app_name(name: str) -> str:
    """Return *name* if it is a valid Databricks App name (2-30 chars, a-z 0-9 -)."""
    if not _APP_NAME_RE.match(name or ""):
        raise ValueError(
            f"invalid app name {name!r}: use 2-30 lowercase letters, digits or "
            "hyphens, starting and ending with a letter or digit"
        )
    return name


def build_resources(
    warehouse_id: str,
    branch: str | None,
    database_segment: str | None,
) -> list[dict]:
    """Return the app ``resources`` array.

    The install-time resources are named as in ``marketplace/manifest.yaml``;
    ``postgres`` is the post-install Lakebase binding (added when *branch* is given).
    """
    resources: list[dict] = [
        {
            "name": "sql-warehouse",
            "description": "SQL Warehouse for executing queries and metadata operations",
            "sql_warehouse": {"id": warehouse_id, "permission": "CAN_USE"},
        },
    ]
    if branch is None:
        return resources
    if not _BRANCH_RE.match(branch):
        raise ValueError(
            f"--lakebase-branch must be projects/<project>/branches/<branch>, got {branch!r}"
        )
    if not database_segment:
        raise ValueError("a Lakebase branch was given but no database resolved")
    resources.append(
        {
            "name": "postgres",
            "description": "Lakebase Autoscaling Postgres database for the OntoBricks registry",
            "postgres": {
                "branch": branch,
                "database": f"{branch}/databases/{database_segment}",
                "permission": "CAN_CONNECT_AND_CREATE",
            },
        }
    )
    return resources


def manifest_user_scopes(manifest_path: Path) -> list[str]:
    """``user_api_scopes`` a Marketplace install would grant (from manifest.yaml)."""
    import yaml

    return list((yaml.safe_load(manifest_path.read_text()) or {}).get("user_api_scopes") or [])


def workspace_source_path(user: str, app_name: str) -> str:
    """Workspace folder the assembled app is uploaded to (always under a test folder)."""
    return f"/Workspace/Users/{user}/{TEST_FOLDER}/{app_name}"


def may_replace(app: dict) -> bool:
    """True only for an app this helper created (so a typo can't redeploy prod)."""
    return (app or {}).get("description") == TEST_APP_DESCRIPTION


def install_commands(
    profile: str,
    app_name: str,
    source_path: str,
    *,
    exists: bool,
    resources: list[dict],
    user_scopes: list[str] | None = None,
) -> list[list[str]]:
    """CLI argv list: create the app (or replace its resources + scopes), then deploy it."""
    scopes = list(user_scopes or [])
    if exists:
        bind = [
            "databricks", "apps", "create-update", app_name,
            "--json", json.dumps({
                "update_mask": "resources,user_api_scopes",
                "app": {"resources": resources, "user_api_scopes": scopes},
            }),
        ]
    else:
        bind = [
            "databricks", "apps", "create",
            "--json", json.dumps({
                "name": app_name,
                "description": TEST_APP_DESCRIPTION,
                "resources": resources,
                "user_api_scopes": scopes,
            }),
        ]
    deploy = ["databricks", "apps", "deploy", app_name, "--source-code-path", source_path]
    return [bind + ["--profile", profile], deploy + ["--profile", profile]]


# ---------------------------------------------------------------------------
# I/O (not unit-tested; exercised by running the helper)
# ---------------------------------------------------------------------------


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _cli(argv: list[str], *, dry_run: bool = False, check: bool = True) -> subprocess.CompletedProcess:
    shown = " ".join(a if len(a) < 120 else a[:117] + "..." for a in argv)
    print(f"$ {shown}", flush=True)
    if dry_run:
        return subprocess.CompletedProcess(argv, 0, "", "")
    # cwd=temp dir: `apps deploy` switches to its project pipeline when run
    # next to a databricks.yml (the repo root has one).
    return subprocess.run(argv, check=check, text=True, capture_output=True, cwd=tempfile.gettempdir())


def _json(argv: list[str]) -> dict:
    return json.loads(_cli(argv).stdout or "{}")


def _resolve_database(profile: str, branch: str, requested: str) -> str:
    resolver = _load("lakebase_resolve_db", REPO_ROOT / "scripts" / "_internal" / "_lakebase-resolve-db.py")
    raw = _cli(["databricks", "postgres", "list-databases", branch, "-o", "json", "--profile", profile]).stdout
    hit = resolver.resolve(requested, raw)
    if hit is None:
        raise SystemExit(f"ERROR: no database {requested!r} on {branch}. Check: databricks postgres list-databases {branch}")
    return hit[0]


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Emulate a Marketplace install of the assembled app.")
    p.add_argument("--profile", required=True, help="Databricks CLI profile (required, never defaulted)")
    p.add_argument("--app-name", default=DEFAULT_APP_NAME)
    p.add_argument("--warehouse-id", required=True)
    p.add_argument("--lakebase-branch", help="projects/<project>/branches/<branch>; omit to test with no Lakebase bound")
    p.add_argument("--lakebase-database", default="databricks_postgres", help="Postgres datname or db-… id")
    p.add_argument("--grant-self", action="store_true", help="also grant the app SP CAN_MANAGE on its own app")
    p.add_argument("--dry-run", action="store_true", help="print the CLI calls, change nothing")
    args = p.parse_args(argv)

    try:
        app_name = validate_app_name(args.app_name)
        segment = None
        if args.lakebase_branch:
            segment = "db-DRYRUN" if args.dry_run else _resolve_database(args.profile, args.lakebase_branch, args.lakebase_database)
        resources = build_resources(args.warehouse_id, args.lakebase_branch, segment)
        user_scopes = manifest_user_scopes(REPO_ROOT / "marketplace" / "manifest.yaml")
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    prof = ["--profile", args.profile]

    # Never touch an app this helper didn't create: replacing resources and
    # redeploying from the test folder would break a real deployment.
    existing = None
    if not args.dry_run:
        got = _cli(["databricks", "apps", "get", app_name, "-o", "json", *prof], check=False)
        if got.returncode == 0:
            existing = json.loads(got.stdout or "{}")
    if existing is not None and not may_replace(existing):
        print(
            f"ERROR: app {app_name!r} already exists and was not created by this helper; "
            "refusing to replace its resources and redeploy it. Choose another --app-name.",
            file=sys.stderr,
        )
        return 1
    exists = existing is not None

    assemble = _load("marketplace_assemble", REPO_ROOT / "scripts" / "marketplace" / "assemble.py")
    folder = assemble.assemble(REPO_ROOT, Path(tempfile.mkdtemp(prefix="ontobricks-mp-")))
    print(f"Assembled {folder}")

    user = "<you>" if args.dry_run else _json(["databricks", "current-user", "me", "-o", "json", *prof])["userName"]
    src = workspace_source_path(user, app_name)
    _cli(["databricks", "workspace", "delete", src, "--recursive", *prof], dry_run=args.dry_run, check=False)
    _cli(["databricks", "workspace", "mkdirs", src, *prof], dry_run=args.dry_run)
    _cli(["databricks", "workspace", "import-dir", str(folder), src, "--overwrite", *prof], dry_run=args.dry_run)

    for cmd in install_commands(
        args.profile, app_name, src, exists=exists, resources=resources, user_scopes=user_scopes
    ):
        result = _cli(cmd, dry_run=args.dry_run, check=False)
        if result.returncode != 0:
            print(result.stdout + result.stderr, file=sys.stderr)
            return result.returncode

    if args.grant_self and not args.dry_run:
        sp = _json(["databricks", "apps", "get", app_name, "-o", "json", *prof]).get("service_principal_client_id")
        acl = {"access_control_list": [{"service_principal_name": sp, "permission_level": "CAN_MANAGE"}]}
        _cli(["databricks", "apps", "update-permissions", app_name, "--json", json.dumps(acl), *prof])

    url = "" if args.dry_run else _json(["databricks", "apps", "get", app_name, "-o", "json", *prof]).get("url", "")
    print(f"\nDeployed {app_name}: {url or '(see the Apps page)'}")
    print("Test checklist (PLAN.md, Task 6 Step 1):")
    print("  1. App starts; /health is not status=error")
    print("  2. Settings > Registry > Initialize succeeds (runs as the app SP)")
    print("  3. A second, non-admin user with CAN_USE can open it" + ("" if args.grant_self else "  <- answers Q4"))
    print("  4. POST /settings/save with {\"token\": \"x\"} returns 400")
    return 0


if __name__ == "__main__":
    sys.exit(main())
