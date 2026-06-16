"""Readiness probe for OntoBricks — ``GET /health``.

This endpoint replaces the previous static dummy with a real
end-to-end check of every external dependency the application needs
to operate correctly:

* local filesystem — ``/tmp``, the session directory and the log
  directory must be writable, with enough free disk space;
* Databricks authentication — OAuth client-credentials in Apps mode
  or PAT in local development;
* SQL warehouse — TCP/SQL reachability via ``SELECT 1``;
* CloudFetch capability — connector prerequisites and lightweight
  runtime probe for ``use_cloud_fetch=True``;
* registry **UC volume** (binaries only) — Files-API read + write probe
  (a tiny sentinel file is written then deleted);
* registry **catalog/schema** — DDL probe via
  ``CREATE OR REPLACE VIEW <fqn> AS SELECT 1`` then ``DROP VIEW`` so
  view materialisation will succeed during Digital-Twin builds;
* **Lakebase** — connectivity/init checks plus explicit schema/table/
  sequence permission probes. When ``PG*`` env vars are unset the
  registry is unavailable (Lakebase is the sole structured-data
  backend since v0.4.0), so the probes report a warning.

Each probe returns ``{name, label, status, detail, duration_ms}``;
the top-level ``status`` is the worst severity across all probes.
``GET /health/detailed`` was removed — its information is now part of
``GET /health``.

The endpoint stays anonymous: ``/health`` is in the bypass list of
:class:`PermissionMiddleware`, :class:`CSRFMiddleware` and
:class:`RequestTimingMiddleware`, so external uptime probes (load
balancer, k8s liveness/readiness, Datadog) can call it without a
session cookie.
"""

from __future__ import annotations

import os
import shutil
import sys
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends

from shared.config.constants import APP_VERSION, HTTP_USER_AGENT
from shared.config.settings import Settings, get_settings

from back.core.helpers import run_blocking
from back.core.logging import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["Health"])

_OK = "ok"
_WARNING = "warning"
_ERROR = "error"
_SEVERITY_RANK = {_OK: 0, _WARNING: 1, _ERROR: 2}


# ---------------------------------------------------------------------------
# Probe runner
# ---------------------------------------------------------------------------


def _safely_run(name: str, label: str, fn: Callable[[], Tuple[str, str]]) -> Dict[str, Any]:
    """Run *fn* and convert it to a stable check dict.

    *fn* is expected to return ``(status, detail)``. Any exception is
    caught and surfaced as ``error`` so a single broken probe never
    fails the whole readiness response.
    """
    started = time.monotonic()
    try:
        status, detail = fn()
    except Exception as exc:  # noqa: BLE001 — catch-all is the point
        logger.exception("Health check %s raised: %s", name, exc)
        status, detail = _ERROR, f"Probe raised: {exc}"
    return {
        "name": name,
        "label": label,
        "status": status,
        "detail": detail,
        "duration_ms": int((time.monotonic() - started) * 1000),
    }


# ---------------------------------------------------------------------------
# Filesystem probes
# ---------------------------------------------------------------------------


def _format_gb(num_bytes: int) -> str:
    return f"{num_bytes / (1024 ** 3):.2f} GB"


def _check_directory_writable(path: str, *, low_warn_gb: float = 1.0, low_err_gb: float = 0.1) -> Tuple[str, str]:
    """Generic "this directory is usable" probe.

    Verifies the directory exists (creating it if missing), is
    writable, and has enough free space. ``low_warn_gb`` / ``low_err_gb``
    define the warning / error thresholds in GiB.
    """
    try:
        os.makedirs(path, exist_ok=True)
    except OSError as exc:
        return _ERROR, f"Cannot create {path}: {exc}"

    if not os.access(path, os.W_OK):
        return _ERROR, f"{path} is not writable by the app process"

    sentinel = os.path.join(path, f".health_{uuid.uuid4().hex[:8]}")
    try:
        with open(sentinel, "w", encoding="utf-8") as fh:
            fh.write("ok")
        os.remove(sentinel)
    except OSError as exc:
        return _ERROR, f"Write probe failed at {path}: {exc}"

    usage = shutil.disk_usage(path)
    free_gb = usage.free / (1024 ** 3)
    base_msg = (
        f"Writable; {_format_gb(usage.free)} free of {_format_gb(usage.total)}"
    )
    if free_gb < low_err_gb:
        return _ERROR, f"Critically low disk space — {base_msg}"
    if free_gb < low_warn_gb:
        return _WARNING, f"Low disk space — {base_msg}"
    return _OK, base_msg


def _check_tmp() -> Tuple[str, str]:
    return _check_directory_writable("/tmp", low_warn_gb=1.0, low_err_gb=0.1)


def _check_session_dir(settings: Settings) -> Tuple[str, str]:
    return _check_directory_writable(
        settings.session_dir, low_warn_gb=0.5, low_err_gb=0.05
    )


def _check_log_dir() -> Tuple[str, str]:
    """Resolve the live log directory and verify it is writable."""
    from back.core.logging.LogManager import LogManager

    mgr = LogManager.instance()
    log_path = mgr.log_path
    if not log_path:
        # Logging may not be configured yet (e.g. running under tests
        # that imported this module before ``LogManager.setup``). Treat
        # as advisory rather than failing the probe.
        return _WARNING, "Log manager has not been initialised yet"
    log_dir = os.path.dirname(log_path)
    return _check_directory_writable(log_dir, low_warn_gb=0.5, low_err_gb=0.05)


# ---------------------------------------------------------------------------
# Databricks probes
# ---------------------------------------------------------------------------


def _check_databricks_auth() -> Tuple[str, str]:
    """Verify the app has usable Databricks credentials.

    In Apps mode we additionally exercise the M2M OAuth path so a
    misconfigured ``DATABRICKS_CLIENT_ID`` / ``CLIENT_SECRET`` fails
    here rather than at the first warehouse call.
    """
    from back.core.databricks.DatabricksAuth import DatabricksAuth

    auth = DatabricksAuth()
    if not auth.has_valid_auth():
        if auth.is_app_mode:
            return (
                _ERROR,
                "App mode but DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET are missing",
            )
        return (
            _ERROR,
            "Local mode but DATABRICKS_TOKEN is not set",
        )
    if auth.is_app_mode:
        try:
            auth.get_oauth_token()
        except Exception as exc:  # noqa: BLE001 — vendor surface
            return _ERROR, f"OAuth token request failed: {exc}"
        return _OK, f"App mode OAuth credentials valid (host={auth.host})"
    return _OK, f"Personal Access Token configured (host={auth.host})"


def _build_health_client(settings: Optional[Settings] = None):
    """Instantiate a ``DatabricksClient`` with no domain/session.

    ``get_databricks_client`` already supports a ``None`` domain via
    ``RegistryCfg.from_domain(None, settings)``-style fallbacks, so the
    readiness route does not need a SessionManager.
    """
    from back.core.helpers import get_databricks_client

    return get_databricks_client(None, settings or get_settings())


def _check_warehouse(settings: Optional[Settings] = None) -> Tuple[str, str]:
    client = _build_health_client(settings)
    if client is None:
        return _WARNING, "No Databricks credentials available — warehouse not probed"
    if not getattr(client, "warehouse_id", ""):
        return _WARNING, "DATABRICKS_SQL_WAREHOUSE_ID is not configured"
    ok, msg = client.test_connection()
    return (_OK if ok else _ERROR), msg


def _check_cloud_fetch(settings: Optional[Settings] = None) -> Tuple[str, str]:
    """Report CloudFetch capability via the real runtime probe.

    Always calls :meth:`DatabricksAuth.probe_cloud_fetch_capability`,
    which issues a tiny ``SELECT 1`` with ``use_cloud_fetch=True`` and
    surfaces the actual outcome. Result is cached on the auth instance
    so SQL connections share the same verdict.
    """
    client = _build_health_client(settings)
    if client is None:
        return _WARNING, "Databricks credentials unavailable — CloudFetch not probed"
    if not getattr(client, "warehouse_id", ""):
        return _WARNING, "SQL warehouse not configured — CloudFetch not probed"

    capable, reason = client.auth.probe_cloud_fetch_capability()
    if capable:
        return _OK, f"CloudFetch enabled — {reason}"
    return _WARNING, f"CloudFetch unavailable — {reason}"


# ---------------------------------------------------------------------------
# Registry probes
# ---------------------------------------------------------------------------


def _resolve_registry_cfg(settings: Settings):
    from back.objects.registry import RegistryCfg

    return RegistryCfg.from_domain(None, settings)


def _check_registry_cfg(settings: Settings) -> Tuple[str, str]:
    cfg = _resolve_registry_cfg(settings)
    if not (cfg.catalog and cfg.schema and cfg.volume):
        return (
            _WARNING,
            "Registry catalog/schema/volume not fully resolved — set REGISTRY_VOLUME_PATH "
            "or bind a Volume resource to the Databricks App",
        )
    return (
        _OK,
        f"catalog={cfg.catalog} schema={cfg.schema} volume={cfg.volume} "
        f"lakebase_schema={cfg.lakebase_schema}",
    )


def _check_registry_volume_read(settings: Settings) -> Tuple[str, str]:
    cfg = _resolve_registry_cfg(settings)
    if not (cfg.catalog and cfg.schema and cfg.volume):
        return _WARNING, "Registry volume not configured — skipped"

    from back.core.databricks.DatabricksAuth import DatabricksAuth
    from back.core.databricks.VolumeFileService import VolumeFileService

    svc = VolumeFileService(auth=DatabricksAuth())
    if not svc.is_configured():
        return _ERROR, "Databricks credentials not available for Files API"
    vol_path = f"/Volumes/{cfg.catalog}/{cfg.schema}/{cfg.volume}"
    ok, items, msg = svc.list_directory(vol_path)
    if ok:
        return _OK, f"Listed {vol_path} — {len(items)} entries"
    return _ERROR, f"Cannot list {vol_path}: {msg}"


def _check_registry_volume_write(settings: Settings) -> Tuple[str, str]:
    """End-to-end write probe — write a tiny sentinel and delete it.

    Far stronger than ``SHOW GRANTS`` because it actually exercises the
    same Files API code path that the registry uses to persist
    ``.global_config.json`` and binary archives.
    """
    cfg = _resolve_registry_cfg(settings)
    if not (cfg.catalog and cfg.schema and cfg.volume):
        return _WARNING, "Registry volume not configured — skipped"

    from back.core.databricks.DatabricksAuth import DatabricksAuth
    from back.core.databricks.VolumeFileService import VolumeFileService

    svc = VolumeFileService(auth=DatabricksAuth())
    if not svc.is_configured():
        return _ERROR, "Databricks credentials not available for Files API"

    sentinel = (
        f"/Volumes/{cfg.catalog}/{cfg.schema}/{cfg.volume}"
        f"/.health_check_{uuid.uuid4().hex[:8]}.txt"
    )
    ok, msg = svc.write_file(sentinel, "ok")
    if not ok:
        return _ERROR, f"Volume write failed ({sentinel}): {msg}"
    # Best-effort cleanup; a leftover file is harmless but noisy.
    deleted, _del_msg = svc.delete_file(sentinel)
    if deleted:
        return _OK, f"Wrote+deleted sentinel at {sentinel}"
    return (
        _WARNING,
        f"Wrote sentinel but cleanup failed (please remove manually): {sentinel}",
    )


def _check_registry_uc_schema_ddl() -> Tuple[str, str]:
    """Probe ``CREATE OR REPLACE VIEW`` in the registry schema.

    The Digital-Twin build creates views in the registry catalog/schema.
    Failing this probe at startup catches missing
    ``CREATE`` / ``USE_SCHEMA`` grants long before the build job
    surfaces an opaque ``PERMISSION_DENIED`` deep in a SQL stack.
    """
    settings = get_settings()
    cfg = _resolve_registry_cfg(settings)
    if not (cfg.catalog and cfg.schema):
        return _WARNING, "Registry catalog/schema not configured — skipped"

    client = _build_health_client()
    if client is None:
        return _WARNING, "No Databricks credentials — DDL probe skipped"
    if not getattr(client, "warehouse_id", ""):
        return _WARNING, "No SQL warehouse configured — DDL probe skipped"

    name = f"_ontobricks_health_{uuid.uuid4().hex[:8]}"
    fqn = f"`{cfg.catalog}`.`{cfg.schema}`.`{name}`"
    try:
        client.execute_statement(f"CREATE OR REPLACE VIEW {fqn} AS SELECT 1 AS ok")
    except Exception as exc:  # noqa: BLE001
        return _ERROR, f"Cannot create view in {cfg.catalog}.{cfg.schema}: {exc}"
    try:
        client.execute_statement(f"DROP VIEW IF EXISTS {fqn}")
    except Exception as exc:  # noqa: BLE001
        # Created but couldn't clean up — admins will see the stray view.
        return _WARNING, f"View created but DROP failed for {fqn}: {exc}"
    return _OK, f"CREATE/DROP VIEW succeeded in {cfg.catalog}.{cfg.schema}"


# ---------------------------------------------------------------------------
# Graph DB (Lakebase graph schema) probe
# ---------------------------------------------------------------------------


def _check_graphdb_lakebase(settings: Settings) -> Tuple[str, str]:
    """Probe the configured Graph DB Lakebase database and graph schema.

    Uses the same auth selection as :class:`GraphDBFactory._create_lakebase`:
    ``BranchLakebaseAuth`` when ``graph_engine_config.lakebase_branch`` is set,
    otherwise the bound Lakebase auth.  This ensures the health probe always
    targets the same host as the actual build engine — the graph DB may be
    on a completely different Lakebase project than the registry.
    """
    from back.core.databricks.LakebaseAuth import BranchLakebaseAuth, get_lakebase_auth

    cfg = _resolve_registry_cfg(settings)
    try:
        from back.objects.registry.store import RegistryFactory

        store = RegistryFactory.from_cfg(cfg)
        global_cfg = store.load_global_config()
        engine_cfg = global_cfg.get("graph_engine_config") or {}
    except Exception as exc:  # noqa: BLE001
        return _WARNING, f"Could not load graph engine config: {exc}"

    database = (engine_cfg.get("database") or "").strip()
    schema = (engine_cfg.get("schema") or engine_cfg.get("graph_schema") or "ontobricks_graph").strip()
    branch_path = (engine_cfg.get("lakebase_branch") or "").strip()

    if not schema:
        return _WARNING, "Graph DB schema not configured — set it in Settings → Graph DB"

    # Select auth: explicit branch → BranchLakebaseAuth; else bound auth.
    if branch_path:
        auth = BranchLakebaseAuth(branch_path, database)
    else:
        auth = get_lakebase_auth()

    if not auth.is_available:
        return (
            _WARNING,
            "Lakebase not bound (PG* env vars unset) — Graph DB not probed",
        )

    try:
        from back.core.graphdb.lakebase.pool import _require_psycopg

        psycopg, _ = _require_psycopg()
        kwargs = auth.kwargs(application_name="ontobricks-graphdb-health")
        if database:
            kwargs["dbname"] = database

        with psycopg.connect(**kwargs) as conn, conn.cursor() as cur:
            cur.execute("SELECT current_database(), current_user")
            row = cur.fetchone() or ("?", "?")
            cur_db, cur_user = row[0], row[1]
            cur.execute(
                "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = %s)",
                (schema,),
            )
            schema_exists = bool((cur.fetchone() or [False])[0])
        if schema_exists:
            return _OK, f"Graph DB reachable — db={cur_db} schema={schema} user={cur_user}"
        return (
            _WARNING,
            f"Graph DB connected (db={cur_db}) but schema '{schema}' does not exist yet — "
            "run a Digital Twin build to create it",
        )
    except Exception as exc:  # noqa: BLE001
        return _ERROR, f"Graph DB probe failed (database={database or 'default'}, schema={schema}): {exc}"


# ---------------------------------------------------------------------------
# Lakebase probe
# ---------------------------------------------------------------------------


def _check_lakebase(settings: Settings) -> Tuple[str, str]:
    from back.core.databricks.LakebaseAuth import get_lakebase_auth

    auth = get_lakebase_auth()
    if not auth.is_available:
        return (
            _WARNING,
            "Lakebase not bound (PG* env vars unset) — registry is unavailable; "
            "set LAKEBASE_PROJECT + LAKEBASE_BRANCH + PGUSER in .env (local) or bind a database "
            "resource in app.yaml (deployed)",
        )

    cfg = _resolve_registry_cfg(settings)
    from back.objects.registry.store.lakebase.store import LakebaseRegistryStore

    store = LakebaseRegistryStore(
        registry_cfg=cfg,
        schema=cfg.lakebase_schema or "ontobricks_registry",
        database=cfg.lakebase_database or "",
    )
    status_dict = store.init_status()
    reason = status_dict.get("reason", "unknown")
    err = status_dict.get("error") or status_dict.get("reason")
    if status_dict.get("initialized"):
        return _OK, f"Lakebase ready — schema={store.schema} ({reason})"
    if reason in ("no_registries_table", "no_registry_row"):
        # Schema reachable, just not bootstrapped — admins can run
        # *Initialize* from Settings → Registry. Treat as warning.
        return _WARNING, str(err)
    # ``no_usage`` / ``connect_failed`` / unknown — these block the app.
    return _ERROR, str(err)


def _check_lakebase_permissions(settings: Settings) -> Tuple[str, str]:
    """Verify Lakebase registry privileges expected by OntoBricks runtime."""
    from back.core.databricks.LakebaseAuth import get_lakebase_auth

    auth = get_lakebase_auth()
    if not auth.is_available:
        return (
            _WARNING,
            "Lakebase not bound (PG* env vars unset) — permission checks skipped",
        )

    cfg = _resolve_registry_cfg(settings)
    from back.objects.registry.store.lakebase.store import LakebaseRegistryStore

    store = LakebaseRegistryStore(
        registry_cfg=cfg,
        schema=cfg.lakebase_schema or "ontobricks_registry",
        database=cfg.lakebase_database or "",
    )
    status_dict = store.init_status()
    reason = status_dict.get("reason", "unknown")
    err = status_dict.get("error") or status_dict.get("reason")
    if reason == "no_usage":
        return _ERROR, str(err)
    if reason in ("no_registries_table", "no_registry_row"):
        return _WARNING, f"Lakebase not initialized ({reason}) — permission probe partial: {err}"
    if reason != "ok":
        return _ERROR, f"Lakebase probe unavailable ({reason}): {err}"

    try:
        with store._connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT current_database(), current_user, "
                "       has_schema_privilege(current_user, %s, 'USAGE'), "
                "       has_schema_privilege(current_user, %s, 'CREATE')",
                (store.schema, store.schema),
            )
            row = cur.fetchone() or ("?", "?", False, False)
            cur_db, cur_user, has_usage, has_create = row

            cur.execute(
                "SELECT "
                "COALESCE(bool_and(has_table_privilege("
                "current_user, format('%%I.%%I', table_schema, table_name), 'SELECT')), true), "
                "COALESCE(bool_and(has_table_privilege("
                "current_user, format('%%I.%%I', table_schema, table_name), 'INSERT')), true), "
                "COALESCE(bool_and(has_table_privilege("
                "current_user, format('%%I.%%I', table_schema, table_name), 'UPDATE')), true), "
                "COALESCE(bool_and(has_table_privilege("
                "current_user, format('%%I.%%I', table_schema, table_name), 'DELETE')), true), "
                "COUNT(*) "
                "FROM information_schema.tables "
                "WHERE table_schema = %s AND table_type='BASE TABLE'",
                (store.schema,),
            )
            tbl_sel, tbl_ins, tbl_upd, tbl_del, tbl_count = cur.fetchone() or (
                True,
                True,
                True,
                True,
                0,
            )

            cur.execute(
                "SELECT "
                "COALESCE(bool_and(has_sequence_privilege("
                "current_user, format('%%I.%%I', sequence_schema, sequence_name), 'USAGE')), true), "
                "COALESCE(bool_and(has_sequence_privilege("
                "current_user, format('%%I.%%I', sequence_schema, sequence_name), 'SELECT')), true), "
                "COALESCE(bool_and(has_sequence_privilege("
                "current_user, format('%%I.%%I', sequence_schema, sequence_name), 'UPDATE')), true), "
                "COUNT(*) "
                "FROM information_schema.sequences "
                "WHERE sequence_schema = %s",
                (store.schema,),
            )
            seq_use, seq_sel, seq_upd, seq_count = cur.fetchone() or (True, True, True, 0)
    except Exception as exc:  # noqa: BLE001
        return _ERROR, f"Lakebase permission probe failed: {exc}"

    missing: List[str] = []
    if not has_usage:
        missing.append("schema USAGE")
    if not has_create:
        missing.append("schema CREATE")
    if not tbl_sel:
        missing.append("table SELECT")
    if not tbl_ins:
        missing.append("table INSERT")
    if not tbl_upd:
        missing.append("table UPDATE")
    if not tbl_del:
        missing.append("table DELETE")
    if not seq_use:
        missing.append("sequence USAGE")
    if not seq_sel:
        missing.append("sequence SELECT")
    if not seq_upd:
        missing.append("sequence UPDATE")

    if missing:
        return (
            _ERROR,
            "Missing Lakebase grants for role "
            f"'{cur_user}' on {cur_db}.{store.schema}: {', '.join(missing)}. "
            "Run scripts/bootstrap-lakebase-perms.sh.",
        )

    return (
        _OK,
        f"Lakebase permissions OK ({cur_db}.{store.schema}; "
        f"tables={int(tbl_count)}, sequences={int(seq_count)})",
    )


# ---------------------------------------------------------------------------
# Lakebase Accelerated Sync probe
# ---------------------------------------------------------------------------


def _check_lakebase_accelerated_sync(settings: Optional[Settings] = None) -> Tuple[str, str]:
    """Probe whether Lakebase Accelerated Sync (Database Synced Tables) is available.

    Sends a lightweight ``GET /api/2.0/database/synced_tables?limit=1`` request
    using the workspace bearer token.  Three outcomes are distinguished:

    * **ok** — the endpoint returned HTTP 200; the feature is enabled and
      accessible in this workspace.
    * **warning** — the workspace is reachable but the endpoint returned 403 or
      404, indicating the feature is not yet activated (e.g. workspace preview
      not enabled, region not supported, or entitlement missing).
    * **error** — credentials are unavailable, the host is not configured, or
      an unexpected HTTP / network error was encountered.
    """
    from back.core.databricks.DatabricksAuth import DatabricksAuth

    auth = DatabricksAuth()
    if not auth.has_valid_auth():
        return _WARNING, "Databricks credentials unavailable — Accelerated Sync not probed"
    if not auth.host:
        return _WARNING, "DATABRICKS_HOST not configured — Accelerated Sync not probed"

    import requests as _requests

    url = f"{auth.host}/api/2.0/database/synced_tables"
    headers = {
        "Authorization": f"Bearer {auth.get_bearer_token()}",
        "Accept": "application/json",
        "User-Agent": HTTP_USER_AGENT,
    }
    try:
        resp = _requests.get(url, headers=headers, params={"limit": "1"}, timeout=10)
    except Exception as exc:  # noqa: BLE001
        return _ERROR, f"Accelerated Sync probe request failed: {exc}"

    if resp.status_code == 200:
        data = resp.json() if resp.content else {}
        count = len(data.get("synced_database_tables") or data.get("tables") or [])
        return _OK, f"Lakebase Accelerated Sync enabled — {count} synced table(s) found"

    if resp.status_code in (403, 404):
        try:
            msg = (resp.json() or {}).get("message") or resp.text or ""
        except Exception:  # noqa: BLE001
            msg = resp.text or ""
        return (
            _WARNING,
            f"Lakebase Accelerated Sync not available in this workspace "
            f"(HTTP {resp.status_code}"
            + (f": {msg[:200]}" if msg else "")
            + ") — enable the preview from workspace Previews settings",
        )

    try:
        err_msg = (resp.json() or {}).get("message") or resp.text or ""
    except Exception:  # noqa: BLE001
        err_msg = resp.text or ""
    return (
        _ERROR,
        f"Accelerated Sync probe returned unexpected HTTP {resp.status_code}"
        + (f": {err_msg[:200]}" if err_msg else ""),
    )


# ---------------------------------------------------------------------------
# Aggregator
# ---------------------------------------------------------------------------


def run_readiness_checks(settings: Optional[Settings] = None) -> Dict[str, Any]:
    """Execute every probe sequentially and roll up the worst severity.

    The function is synchronous so individual probes can use blocking
    SDK calls without ``await``. Wrap the whole thing in
    :func:`run_blocking` from an ``async`` route to keep the event
    loop free.
    """
    settings = settings or get_settings()

    checks: List[Dict[str, Any]] = []
    checks.append(
        _safely_run(
            "runtime",
            "Application runtime",
            lambda: (
                _OK,
                f"Python {sys.version.split()[0]} — OntoBricks {APP_VERSION}",
            ),
        )
    )
    checks.append(_safely_run("filesystem.tmp", "/tmp writable + free space", _check_tmp))
    checks.append(
        _safely_run(
            "filesystem.session_dir",
            "Session directory writable",
            lambda: _check_session_dir(settings),
        )
    )
    checks.append(
        _safely_run("filesystem.log_dir", "Log directory writable", _check_log_dir)
    )
    checks.append(
        _safely_run("databricks.auth", "Databricks authentication", _check_databricks_auth)
    )
    checks.append(
        _safely_run(
            "databricks.warehouse",
            "SQL warehouse reachable",
            lambda: _check_warehouse(settings),
        )
    )
    checks.append(
        _safely_run(
            "databricks.cloudfetch",
            "CloudFetch capability",
            lambda: _check_cloud_fetch(settings),
        )
    )
    checks.append(
        _safely_run(
            "registry.cfg",
            "Registry configuration resolved",
            lambda: _check_registry_cfg(settings),
        )
    )
    checks.append(
        _safely_run(
            "registry.volume_read",
            "Registry UC volume — list",
            lambda: _check_registry_volume_read(settings),
        )
    )
    checks.append(
        _safely_run(
            "registry.volume_write",
            "Registry UC volume — write",
            lambda: _check_registry_volume_write(settings),
        )
    )
    checks.append(
        _safely_run(
            "registry.uc_schema_ddl",
            "Registry catalog/schema — view DDL",
            _check_registry_uc_schema_ddl,
        )
    )
    checks.append(
        _safely_run(
            "lakebase",
            "Lakebase — Registry Postgres",
            lambda: _check_lakebase(settings),
        )
    )
    checks.append(
        _safely_run(
            "lakebase.permissions",
            "Lakebase — Registry permissions",
            lambda: _check_lakebase_permissions(settings),
        )
    )
    checks.append(
        _safely_run(
            "graphdb.lakebase",
            "Lakebase — Graph DB (separate database)",
            lambda: _check_graphdb_lakebase(settings),
        )
    )
    checks.append(
        _safely_run(
            "lakebase.accelerated_sync",
            "Lakebase Accelerated Sync",
            lambda: _check_lakebase_accelerated_sync(settings),
        )
    )

    summary = {
        "total": len(checks),
        "ok": sum(1 for c in checks if c["status"] == _OK),
        "warnings": sum(1 for c in checks if c["status"] == _WARNING),
        "errors": sum(1 for c in checks if c["status"] == _ERROR),
    }
    overall = max(
        (c["status"] for c in checks),
        key=lambda s: _SEVERITY_RANK.get(s, 0),
        default=_OK,
    )
    return {
        "status": overall,
        "version": APP_VERSION,
        "service": "OntoBricks",
        "framework": "FastAPI",
        "summary": summary,
        "checks": checks,
    }


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------


@router.get("/health")
async def health_check(settings: Settings = Depends(get_settings)):
    """Readiness probe — returns ``200`` even when individual checks fail.

    External probes / load balancers should look at the top-level
    ``status`` field (``ok`` / ``warning`` / ``error``) and the
    ``summary.errors`` count. Returning a non-200 HTTP status would
    take the app out of rotation as soon as a *single* dependency
    flickered, which is rarely what you want for an analytical app.
    """
    return await run_blocking(run_readiness_checks, settings)
