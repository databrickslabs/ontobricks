#!/usr/bin/env python3
"""Read-only Lakebase preflight checks for deploy / bootstrap.

Used by ``scripts/check-deploy-prerequisites.sh`` and ``scripts/deploy.sh``
before mutating Databricks resources or running bootstrap-lakebase-perms.sh.

Exit codes:
  0 — all blocking checks passed (warnings may be present on stderr)
  1 — one or more blocking checks failed
  2 — usage / internal error
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from typing import Any

# Registry objects provisioned by bootstrap-lakebase-perms.sh Step 2b and the
# upgrade_lakebase_0.4_To_0.5.sql / upgrade_lakebase_0.5_To_0.6.sql scripts.
EXPECTED_COLUMNS: tuple[tuple[str, str], ...] = (
    ("domain_versions", "status"),
    ("domains", "review_quorum"),
)
EXPECTED_TABLES: tuple[str, ...] = (
    "build_runs",
    "graph_analytics",
    "graph_analytics_runs",
    "domain_review_events",
    "domain_comments",
    "domain_tasks",
    "domain_edit_locks",
    "domain_change_events",
)
STALE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("domain_comments", "anchor_type"),
    ("domain_comments", "anchor_ref"),
)


@dataclass
class CheckResult:
    name: str
    status: str  # ok | warn | fail | skip
    detail: str = ""


@dataclass
class PreflightReport:
    project: str
    branch: str
    database: str
    schema: str
    checks: list[CheckResult] = field(default_factory=list)
    migration_pending: list[str] = field(default_factory=list)
    migration_stale: list[str] = field(default_factory=list)

    @property
    def blocking_failures(self) -> list[CheckResult]:
        return [c for c in self.checks if c.status == "fail"]

    @property
    def warnings(self) -> list[CheckResult]:
        return [c for c in self.checks if c.status == "warn"]


def _run(cmd: list[str], *, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )


def _api_get(path: str) -> dict[str, Any] | None:
    out = _run(["databricks", "api", "get", path])
    if out.returncode != 0 or not out.stdout.strip():
        return None
    try:
        data = json.loads(out.stdout)
        return data if isinstance(data, dict) else None
    except json.JSONDecodeError:
        return None


def resolve_endpoint(project: str, branch: str) -> tuple[str, str] | None:
    branch_path = f"projects/{project}/branches/{branch}"
    payload = _api_get(f"/api/2.0/postgres/{branch_path}/endpoints") or {}
    for ep in payload.get("endpoints") or []:
        hosts = (ep.get("status") or {}).get("hosts") or {}
        host = (hosts.get("host") or "").strip()
        endpoint_path = (ep.get("name") or "").strip()
        if host and endpoint_path:
            return host, endpoint_path
    return None


def mint_token(endpoint_path: str) -> str:
    out = _run(
        [
            "databricks",
            "api",
            "post",
            "/api/2.0/postgres/credentials",
            "--json",
            json.dumps({"endpoint": endpoint_path}),
        ]
    )
    if out.returncode != 0 or not out.stdout.strip():
        return ""
    try:
        return str(json.loads(out.stdout).get("token") or "")
    except json.JSONDecodeError:
        return ""


def resolve_pguser() -> str:
    out = _run(["databricks", "current-user", "me"])
    if out.returncode != 0 or not out.stdout.strip():
        return ""
    try:
        return str(json.loads(out.stdout).get("userName") or "")
    except json.JSONDecodeError:
        return ""


def psql_query(conn_env: dict[str, str], sql: str) -> str:
    out = _run(
        ["psql", conn_env["PGCONN"], "-tAc", sql],
        env=conn_env,
    )
    if out.returncode != 0:
        raise RuntimeError(out.stderr.strip() or out.stdout.strip() or "psql failed")
    return (out.stdout or "").strip()


def inspect_migrations(conn_env: dict[str, str], schema: str) -> tuple[list[str], list[str], list[str]]:
    """Return (pending, stale, errors)."""
    pending: list[str] = []
    stale: list[str] = []
    errors: list[str] = []

    try:
        has_domain_versions = psql_query(
            conn_env,
            "SELECT 1 FROM information_schema.tables "
            f"WHERE table_schema='{schema}' AND table_name='domain_versions'",
        )
    except RuntimeError as exc:
        errors.append(str(exc))
        return pending, stale, errors

    if has_domain_versions != "1":
        return pending, stale, errors

    for table, column in EXPECTED_COLUMNS:
        try:
            present = psql_query(
                conn_env,
                "SELECT 1 FROM information_schema.columns "
                f"WHERE table_schema='{schema}' AND table_name='{table}' AND column_name='{column}'",
            )
        except RuntimeError as exc:
            errors.append(str(exc))
            continue
        if present != "1":
            pending.append(f"column {schema}.{table}.{column}")

    for table in EXPECTED_TABLES:
        try:
            present = psql_query(
                conn_env,
                "SELECT 1 FROM information_schema.tables "
                f"WHERE table_schema='{schema}' AND table_name='{table}'",
            )
        except RuntimeError as exc:
            errors.append(str(exc))
            continue
        if present != "1":
            pending.append(f"table {schema}.{table}")

    for table, column in STALE_COLUMNS:
        try:
            present = psql_query(
                conn_env,
                "SELECT 1 FROM information_schema.columns "
                f"WHERE table_schema='{schema}' AND table_name='{table}' AND column_name='{column}'",
            )
        except RuntimeError as exc:
            errors.append(str(exc))
            continue
        if present == "1":
            stale.append(f"column {schema}.{table}.{column}")

    try:
        has_check = psql_query(
            conn_env,
            "SELECT 1 FROM pg_constraint c "
            "JOIN pg_namespace n ON n.oid = c.connamespace "
            f"JOIN pg_class r ON r.oid = c.conrelid "
            f"WHERE n.nspname = '{schema}' AND r.relname = 'domain_versions' "
            "AND c.conname = 'domain_versions_status_check'",
        )
        if has_check != "1":
            pending.append(f"constraint {schema}.domain_versions_status_check")
    except RuntimeError as exc:
        errors.append(str(exc))

    return pending, stale, errors


def schema_owner_can_migrate(conn_env: dict[str, str], schema: str) -> tuple[bool, str]:
    try:
        owner = psql_query(
            conn_env,
            f"SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = '{schema}'",
        )
        current = psql_query(conn_env, "SELECT current_user")
        can_create = psql_query(
            conn_env,
            f"SELECT has_schema_privilege(current_user, '{schema}', 'CREATE')",
        )
    except RuntimeError as exc:
        return False, str(exc)

    if owner == current or can_create == "t":
        return True, f"owner={owner or '?'}, current={current or '?'}"
    return False, (
        f"current user '{current}' is not owner of schema '{schema}' (owner={owner}) "
        "and lacks CREATE — bootstrap migrations and GRANTs will fail"
    )


def run_preflight(
    *,
    project: str,
    branch: str,
    database: str,
    schema: str,
    apps: list[str],
    check_migrations: bool = True,
    check_apps: bool = True,
) -> PreflightReport:
    report = PreflightReport(
        project=project,
        branch=branch,
        database=database,
        schema=schema,
    )

    import shutil

    for cmd in ("databricks", "psql", "python3"):
        if shutil.which(cmd) is None:
            report.checks.append(
                CheckResult(cmd, "fail", f"'{cmd}' not found on PATH")
            )

    pguser = resolve_pguser()
    if not pguser:
        report.checks.append(
            CheckResult("pguser", "fail", "Could not resolve Databricks userName for PGUSER")
        )
        return report
    report.checks.append(CheckResult("pguser", "ok", pguser))

    endpoint = resolve_endpoint(project, branch)
    if not endpoint:
        report.checks.append(
            CheckResult(
                "endpoint",
                "fail",
                f"No active Postgres endpoint for projects/{project}/branches/{branch}",
            )
        )
        return report
    pghost, endpoint_path = endpoint
    report.checks.append(CheckResult("endpoint", "ok", f"{pghost} ({endpoint_path})"))

    token = mint_token(endpoint_path)
    if not token:
        report.checks.append(
            CheckResult("credentials", "fail", "Could not mint Lakebase JWT via postgres/credentials API")
        )
        return report
    report.checks.append(CheckResult("credentials", "ok", "JWT minted"))

    conn_env = os.environ.copy()
    conn_env.update(
        {
            "PGHOST": pghost,
            "PGPORT": "5432",
            "PGUSER": pguser,
            "PGPASSWORD": token,
            "PGDATABASE": database,
            "PGSSLMODE": "require",
            "PGCONN": (
                f"host={pghost} port=5432 user={pguser} dbname={database} sslmode=require"
            ),
        }
    )

    try:
        psql_query(conn_env, "SELECT 1")
        report.checks.append(CheckResult("psql_connect", "ok", f"connected to dbname={database}"))
    except RuntimeError as exc:
        report.checks.append(CheckResult("psql_connect", "fail", str(exc)))
        return report

    try:
        schema_exists = psql_query(
            conn_env,
            "SELECT 1 FROM information_schema.schemata "
            f"WHERE schema_name='{schema}'",
        )
    except RuntimeError as exc:
        report.checks.append(CheckResult("schema_exists", "fail", str(exc)))
        return report

    if schema_exists != "1":
        report.checks.append(
            CheckResult(
                "schema_exists",
                "warn",
                f"Schema '{schema}' does not exist yet — initialise via Settings → Registry → Initialize, "
                "then re-run bootstrap-lakebase",
            )
        )
        if check_migrations:
            report.checks.append(
                CheckResult("migrations", "skip", "registry schema not initialised")
            )
        if check_apps:
            _check_apps(report, apps)
        return report

    report.checks.append(CheckResult("schema_exists", "ok", schema))

    can_migrate, migrate_detail = schema_owner_can_migrate(conn_env, schema)
    if can_migrate:
        report.checks.append(CheckResult("schema_owner", "ok", migrate_detail))
    else:
        report.checks.append(CheckResult("schema_owner", "fail", migrate_detail))

    if check_migrations:
        pending, stale, errors = inspect_migrations(conn_env, schema)
        report.migration_pending = pending
        report.migration_stale = stale
        for err in errors:
            report.checks.append(CheckResult("migrations", "fail", err))
        if errors:
            pass
        elif stale:
            report.checks.append(
                CheckResult(
                    "migrations",
                    "warn",
                    "Stale draft columns present — run make bootstrap-lakebase or "
                    "scripts/upgrade_lakebase_0.5_To_0.6.sql: "
                    + ", ".join(stale),
                )
            )
        elif pending:
            if can_migrate:
                report.checks.append(
                    CheckResult(
                        "migrations",
                        "warn",
                        "Pending registry migrations will be applied by bootstrap-lakebase-perms.sh: "
                        + ", ".join(pending),
                    )
                )
            else:
                report.checks.append(
                    CheckResult(
                        "migrations",
                        "fail",
                        "Pending migrations but current user cannot apply DDL: "
                        + ", ".join(pending),
                    )
                )
        else:
            report.checks.append(CheckResult("migrations", "ok", "registry schema at current version"))

    if check_apps:
        _check_apps(report, apps)

    return report


def _check_apps(report: PreflightReport, apps: list[str]) -> None:
    for app in apps:
        out = _run(["databricks", "apps", "get", app, "-o", "json"])
        if out.returncode != 0 or not out.stdout.strip():
            report.checks.append(
                CheckResult(
                    f"app:{app}",
                    "warn",
                    "App not found yet — CAN_USE / schema GRANT steps will be skipped until after first deploy",
                )
            )
            continue
        try:
            sp_id = str(json.loads(out.stdout).get("service_principal_client_id") or "")
        except json.JSONDecodeError:
            sp_id = ""
        if sp_id:
            report.checks.append(CheckResult(f"app:{app}", "ok", f"service principal {sp_id}"))
        else:
            report.checks.append(
                CheckResult(f"app:{app}", "warn", "App exists but service principal id is empty")
            )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Lakebase deploy preflight checks")
    parser.add_argument("--project", required=True)
    parser.add_argument("--branch", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--app", action="append", default=[], dest="apps")
    parser.add_argument("--no-migrations", action="store_true")
    parser.add_argument("--no-apps", action="store_true")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable report on stdout")
    args = parser.parse_args(argv)

    report = run_preflight(
        project=args.project,
        branch=args.branch,
        database=args.database,
        schema=args.schema,
        apps=args.apps,
        check_migrations=not args.no_migrations,
        check_apps=not args.no_apps,
    )

    if args.json:
        payload = asdict(report)
        print(json.dumps(payload, indent=2))
    else:
        for check in report.checks:
            prefix = {"ok": "✓", "warn": "⚠", "fail": "✗", "skip": "·"}[check.status]
            line = f"  {prefix} {check.name}"
            if check.detail:
                line += f": {check.detail}"
            stream = sys.stderr if check.status in {"warn", "fail"} else sys.stdout
            print(line, file=stream)

    return 1 if report.blocking_failures else 0


if __name__ == "__main__":
    sys.exit(main())
