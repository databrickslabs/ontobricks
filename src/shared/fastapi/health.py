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

    Exercises the active auth path eagerly (M2M OAuth for App mode, the
    Databricks SDK ``Config.authenticate`` call for CLI mode) so a
    misconfigured workspace fails here rather than at the first warehouse
    call.
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
            "Local mode but DATABRICKS_TOKEN is not set and no Databricks CLI "
            "profile was found in ~/.databrickscfg "
            "(run `databricks auth login` to configure one)",
        )
    if auth.auth_mode == "app":
        try:
            auth.get_oauth_token()
        except Exception as exc:  # noqa: BLE001 — vendor surface
            return _ERROR, f"OAuth token request failed: {exc}"
        return _OK, f"App mode OAuth credentials valid (host={auth.host})"
    if auth.auth_mode == "cli":
        try:
            auth.get_bearer_token()
        except Exception as exc:  # noqa: BLE001 — vendor surface
            return _ERROR, f"Databricks CLI profile authentication failed: {exc}"
        return (
            _OK,
            f"Databricks CLI profile '{auth.cli_profile_name}' configured "
            f"(host={auth.host})",
        )
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
    from back.core.databricks.uc import VolumeFileService

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
    from back.core.databricks.uc import VolumeFileService

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
    from back.core.databricks.lakebase import BranchLakebaseAuth, get_lakebase_auth

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
            "run a Knowledge Graph build to create it",
        )
    except Exception as exc:  # noqa: BLE001
        return _ERROR, f"Graph DB probe failed (database={database or 'default'}, schema={schema}): {exc}"


# ---------------------------------------------------------------------------
# Lakebase probe
# ---------------------------------------------------------------------------


def _check_lakebase(settings: Settings) -> Tuple[str, str]:
    from back.core.databricks.lakebase import get_lakebase_auth

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
    from back.core.databricks.lakebase import get_lakebase_auth

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
# Diagnostics probes (admin-only, grouped by subsystem)
# ---------------------------------------------------------------------------


def _check_uc_catalog_privileges(settings: Settings) -> Tuple[str, str]:
    """Verify the app identity can USE the registry UC catalog (list its schemas)."""
    cfg = _resolve_registry_cfg(settings)
    if not cfg.catalog:
        return (
            _WARNING,
            "Registry catalog not configured — set REGISTRY_VOLUME_PATH or bind a UC Volume "
            "resource in app.yaml",
        )
    client = _build_health_client(settings)
    if client is None:
        return _WARNING, "No Databricks credentials — skipped"
    try:
        schemas = client.get_schemas(cfg.catalog)
        if isinstance(schemas, list):
            return (
                _OK,
                f"USE CATALOG granted on '{cfg.catalog}' — {len(schemas)} schema(s) visible. "
                "The app service principal can list and access schemas within this catalog.",
            )
        return _WARNING, "Could not determine catalog access — unexpected response from UC API"
    except Exception as exc:
        err = str(exc)
        if any(k in err.upper() for k in ("PERMISSION_DENIED", "CATALOG_NOT_FOUND", "UNAUTHORIZED")):
            return (
                _ERROR,
                f"Cannot USE catalog '{cfg.catalog}': {err}. "
                "Grant USE CATALOG on this catalog to the app service principal via: "
                f"GRANT USE CATALOG ON CATALOG `{cfg.catalog}` TO `<app-sp>`",
            )
        return _ERROR, f"Catalog privilege check failed: {exc}"


def _check_uc_schema_privileges(settings: Settings) -> Tuple[str, str]:
    """Verify USE SCHEMA on the registry schema (list its objects)."""
    cfg = _resolve_registry_cfg(settings)
    if not (cfg.catalog and cfg.schema):
        return _WARNING, "Registry catalog/schema not configured"
    client = _build_health_client(settings)
    if client is None:
        return _WARNING, "No Databricks credentials — skipped"
    try:
        objects = client.list_tables_and_views(cfg.catalog, cfg.schema)
        if isinstance(objects, list):
            return (
                _OK,
                f"USE SCHEMA granted on '{cfg.catalog}.{cfg.schema}' — "
                f"{len(objects)} object(s) visible. "
                "The app can list tables and views in this schema.",
            )
        return _WARNING, "Could not determine schema access — unexpected response from UC API"
    except Exception as exc:
        err = str(exc)
        if any(k in err.upper() for k in ("PERMISSION_DENIED", "SCHEMA_NOT_FOUND", "UNAUTHORIZED")):
            return (
                _ERROR,
                f"Cannot USE schema '{cfg.schema}': {err}. "
                f"Grant: USE SCHEMA ON SCHEMA `{cfg.catalog}`.`{cfg.schema}` TO `<app-sp>`",
            )
        return _ERROR, f"Schema privilege check failed: {exc}"


def _check_uc_create_table_privilege(settings: Settings) -> Tuple[str, str]:
    """Probe CREATE TABLE + DROP TABLE in the registry schema.

    The Delta triple-store backend creates Delta TABLEs (not just VIEWs) in the
    registry schema during Knowledge Graph builds.  The existing ``_check_registry_uc_schema_ddl``
    probe only tests CREATE VIEW — a principal may have CREATE on views but not tables.
    This probe catches that gap before the first build attempts it.
    """
    settings = settings or get_settings()
    cfg = _resolve_registry_cfg(settings)
    if not (cfg.catalog and cfg.schema):
        return _WARNING, "Registry catalog/schema not configured — skipped"

    client = _build_health_client(settings)
    if client is None:
        return _WARNING, "No Databricks credentials — skipped"
    if not getattr(client, "warehouse_id", ""):
        return _WARNING, "No SQL warehouse configured — CREATE TABLE probe skipped"

    name = f"_ontobricks_health_{uuid.uuid4().hex[:8]}"
    fqn = f"`{cfg.catalog}`.`{cfg.schema}`.`{name}`"
    try:
        client.execute_statement(
            f"CREATE TABLE IF NOT EXISTS {fqn} (id BIGINT) USING DELTA"
        )
    except Exception as exc:
        return (
            _ERROR,
            f"Cannot CREATE TABLE in {cfg.catalog}.{cfg.schema}: {exc}. "
            "Required for Delta triple-store builds. "
            f"Grant: CREATE ON SCHEMA `{cfg.catalog}`.`{cfg.schema}` TO `<app-sp>`",
        )
    try:
        client.execute_statement(f"DROP TABLE IF EXISTS {fqn}")
    except Exception as exc:
        return (
            _WARNING,
            f"Table created but DROP failed for {fqn}: {exc}. "
            "Please drop it manually.",
        )
    return _OK, f"CREATE TABLE / DROP TABLE succeeded in {cfg.catalog}.{cfg.schema}"


def _check_lakebase_env_vars() -> Tuple[str, str]:
    """Verify the Lakebase PG* env vars are present (must be injected by Databricks Apps).

    OntoBricks requires ``PGHOST``, ``PGDATABASE``, and ``PGUSER`` to be set.
    ``PGPORT`` defaults to 5432 when absent.  The Postgres password is not an
    env var — it is a short-lived JWT minted by :class:`LakebaseAuth` on demand.

    In a Databricks App these are injected automatically when a ``database``
    resource is bound in ``app.yaml``. In local development they must be set
    in ``.env`` (or ``LAKEBASE_PROJECT`` + ``LAKEBASE_BRANCH`` + ``LAKEBASE_DATABASE``
    can be used instead of raw ``PG*`` values — see ``LakebaseAuth`` docs).
    """
    missing: List[str] = []
    present: List[str] = []

    for var in ("PGHOST", "PGDATABASE", "PGUSER"):
        if os.environ.get(var, "").strip():
            present.append(var)
        else:
            missing.append(var)

    port = os.environ.get("PGPORT", "5432")

    # Check alternative local-dev vars if PG* are absent
    alt_project  = os.environ.get("LAKEBASE_PROJECT", "").strip()
    alt_branch   = os.environ.get("LAKEBASE_BRANCH", "").strip()
    alt_database = os.environ.get("LAKEBASE_DATABASE", "").strip()
    alt_present  = [v for v, val in (
        ("LAKEBASE_PROJECT", alt_project),
        ("LAKEBASE_BRANCH", alt_branch),
        ("LAKEBASE_DATABASE", alt_database),
    ) if val]

    if missing:
        if alt_project and alt_branch:
            return (
                _WARNING,
                f"PG* vars not set ({', '.join(missing)} missing) — using alternative "
                f"local-dev vars: {', '.join(alt_present)}. "
                "In a deployed Databricks App, add a ``database`` resource binding in app.yaml.",
            )
        pghost_present = "PGHOST" in present
        return (
            _ERROR,
            f"Required Lakebase env vars missing: {', '.join(missing)}. "
            f"Present: {', '.join(present) or 'none'}. "
            "In a deployed Databricks App these are injected automatically from the "
            "``database`` resource binding in app.yaml. "
            "In local development: set PGHOST, PGDATABASE, PGUSER in .env, or "
            "set LAKEBASE_PROJECT + LAKEBASE_BRANCH + LAKEBASE_DATABASE instead.",
        )
    pghost = os.environ.get("PGHOST", "")
    pgdb   = os.environ.get("PGDATABASE", "")
    pguser = os.environ.get("PGUSER", "")
    return (
        _OK,
        f"PGHOST={pghost} PGDATABASE={pgdb} PGUSER={pguser} PGPORT={port}. "
        "All required Lakebase env vars are present.",
    )


def _check_lakebase_psycopg() -> Tuple[str, str]:
    """Verify the ``psycopg`` (v3) driver is installed and importable.

    ``psycopg`` is the only Postgres client used by OntoBricks; it is listed in
    ``pyproject.toml`` under ``[project.dependencies]`` as ``psycopg[binary]``.
    A missing or broken install would produce an opaque ``ImportError`` buried
    inside a connection attempt rather than a clear error.
    """
    try:
        import psycopg as _psycopg  # noqa: F401

        ver = getattr(_psycopg, "__version__", "?")
        return _OK, f"psycopg {ver} is installed and importable."
    except ImportError as exc:
        return (
            _ERROR,
            f"psycopg is not importable: {exc}. "
            "Install it with: pip install 'psycopg[binary]'  "
            "(or 'psycopg[c]' for the C extension build). "
            "Check pyproject.toml [project.dependencies].",
        )


def _check_lakebase_registry_initialized(settings: Settings) -> Tuple[str, str]:
    """Verify the registry schema has been initialized (registries row exists).

    The app cannot function without at least one row in the ``registries`` table
    that points to the correct catalog/schema/volume. This row is created by
    Settings → Registry → Initialize (or the ``initialize_registry`` API call).
    """
    from back.core.databricks.lakebase import get_lakebase_auth

    auth = get_lakebase_auth()
    if not auth.is_available:
        return _WARNING, "Lakebase not bound — skipped"

    cfg = _resolve_registry_cfg(settings)
    from back.objects.registry.store.lakebase.store import LakebaseRegistryStore

    store = LakebaseRegistryStore(
        registry_cfg=cfg,
        schema=cfg.lakebase_schema or "ontobricks_registry",
        database=cfg.lakebase_database or "",
    )
    status_dict = store.init_status()
    reason = status_dict.get("reason", "unknown")
    err = status_dict.get("error", "")

    if reason == "ok":
        try:
            with store._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f'SELECT catalog, schema, volume FROM "{store.schema}".registries '
                    "ORDER BY created_at ASC LIMIT 1"
                )
                row = cur.fetchone()
            if row:
                return (
                    _OK,
                    f"Registry row found — catalog={row[0]} schema={row[1]} volume={row[2]}. "
                    "The registry is initialized and the catalog/schema/volume triplet "
                    "is correctly stored in Lakebase.",
                )
            return (
                _WARNING,
                f"Registry schema '{store.schema}' exists but the registries table is empty. "
                "Run Settings → Registry → Initialize to create the registry row.",
            )
        except Exception as exc:
            return _WARNING, f"Could not read registry row: {exc}"

    if reason == "no_registry_row":
        return (
            _ERROR,
            f"The registries table exists in schema '{store.schema}' but has no row. "
            "Run Settings → Registry → Initialize to create the registry entry. "
            "Without this, the app cannot resolve domain paths or load global config.",
        )
    if reason == "no_registries_table":
        return (
            _ERROR,
            f"The 'registries' table is missing from schema '{store.schema}'. "
            "Run Settings → Registry → Initialize to create all registry tables.",
        )
    if reason == "no_usage":
        return (
            _ERROR,
            err or f"Role lacks USAGE on schema '{store.schema}'.",
        )
    return _ERROR, f"Registry not initialized ({reason}): {err}"


def _check_lakebase_registry_tables(settings: Settings) -> Tuple[str, str]:
    """Verify all expected Lakebase registry tables exist in the registry schema.

    Tables are split into three tiers:

    * **core** — blocking: the app cannot operate without these.
    * **optional** — created on first use; warning if missing.
    * **lazy** — created by the first relevant operation (e.g.
      ``domain_change_events`` is created by the first domain save that
      flushes the change-audit buffer).  These are expected to be absent
      on a fresh install and are reported informatively only.
    """
    from back.core.databricks.lakebase import get_lakebase_auth

    auth = get_lakebase_auth()
    if not auth.is_available:
        return _WARNING, "Lakebase not bound (PG* env vars unset) — skipped"

    cfg = _resolve_registry_cfg(settings)
    from back.objects.registry.store.lakebase.store import (
        LakebaseRegistryStore,
        _KNOWN_TABLES,
    )

    store = LakebaseRegistryStore(
        registry_cfg=cfg,
        schema=cfg.lakebase_schema or "ontobricks_registry",
        database=cfg.lakebase_database or "",
    )
    _CORE_TABLES = frozenset(
        {
            "registries",
            "global_config",
            "domains",
            "domain_versions",
            "domain_permissions",
            "schedules",
            "schedule_runs",
            "build_runs",
        }
    )
    # domain_change_events is created lazily on the first domain save that
    # flushes the change-audit buffer; it is intentionally absent from
    # _KNOWN_TABLES so row-count probes skip it.  We note it separately.
    _LAZY_TABLES = frozenset({"domain_change_events"})
    # Expected sequences: one per bigserial primary key
    _EXPECTED_SEQUENCES = frozenset(
        {"schedule_runs_id_seq", "build_runs_id_seq", "graph_analytics_runs_id_seq"}
    )

    try:
        with store._connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s AND table_type = 'BASE TABLE'",
                (store.schema,),
            )
            existing_tables = {row[0] for row in cur.fetchall()}

            cur.execute(
                "SELECT sequence_name FROM information_schema.sequences "
                "WHERE sequence_schema = %s",
                (store.schema,),
            )
            existing_seqs = {row[0] for row in cur.fetchall()}

        missing_core  = _CORE_TABLES - existing_tables
        missing_opt   = _KNOWN_TABLES - existing_tables - _CORE_TABLES
        lazy_present  = _LAZY_TABLES & existing_tables
        missing_seqs  = _EXPECTED_SEQUENCES - existing_seqs

        if missing_core:
            return (
                _ERROR,
                f"Missing core registry tables in schema '{store.schema}': "
                f"{', '.join(sorted(missing_core))}. "
                "Run Settings → Registry → Initialize to bootstrap the schema.",
            )

        present   = existing_tables & _KNOWN_TABLES
        seq_note  = (
            f"; sequences: {len(existing_seqs - missing_seqs)}/{len(_EXPECTED_SEQUENCES)} OK"
            if missing_seqs
            else f"; {len(existing_seqs)} sequence(s) present"
        )
        lazy_note = f"; lazy table 'domain_change_events' present" if lazy_present else ""

        base_msg = (
            f"{len(present)}/{len(_KNOWN_TABLES)} schema-DDL tables present in "
            f"'{store.schema}'{seq_note}{lazy_note}"
        )

        if missing_seqs:
            return (
                _WARNING,
                base_msg
                + f". Missing sequences: {', '.join(sorted(missing_seqs))} — "
                "these are created by schema initialization; run "
                "Settings → Registry → Initialize.",
            )
        if missing_opt:
            return (
                _WARNING,
                base_msg
                + f". Optional tables absent (created on first use): "
                + ", ".join(sorted(missing_opt)),
            )
        return _OK, base_msg
    except Exception as exc:
        return _ERROR, f"Registry table existence check failed: {exc}"


def _check_graphdb_tables(settings: Settings) -> Tuple[str, str]:
    """Report how many tables / views are in the configured graph DB schema."""
    from back.core.databricks.lakebase import BranchLakebaseAuth, get_lakebase_auth

    cfg = _resolve_registry_cfg(settings)
    try:
        from back.objects.registry.store import RegistryFactory

        store = RegistryFactory.from_cfg(cfg)
        global_cfg = store.load_global_config()
        engine_cfg = global_cfg.get("graph_engine_config") or {}
    except Exception as exc:
        return _WARNING, f"Could not load graph engine config: {exc}"

    database = (engine_cfg.get("database") or "").strip()
    schema = (
        engine_cfg.get("schema") or engine_cfg.get("graph_schema") or "ontobricks_graph"
    ).strip()
    branch_path = (engine_cfg.get("lakebase_branch") or "").strip()

    auth = BranchLakebaseAuth(branch_path, database) if branch_path else get_lakebase_auth()
    if not auth.is_available:
        return _WARNING, "Lakebase not bound — Graph DB not probed"

    try:
        from back.core.graphdb.lakebase.pool import _require_psycopg

        psycopg, _ = _require_psycopg()
        kwargs = auth.kwargs(application_name="ontobricks-graphdb-diag")
        if database:
            kwargs["dbname"] = database

        with psycopg.connect(**kwargs) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = %s",
                (schema,),
            )
            table_count = int((cur.fetchone() or [0])[0])
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.views WHERE table_schema = %s",
                (schema,),
            )
            view_count = int((cur.fetchone() or [0])[0])

        if table_count == 0 and view_count == 0:
            return (
                _WARNING,
                f"Graph schema '{schema}' is empty — no tables or views yet. "
                "Run Knowledge Graph → Build to populate it (one table + one VIEW per "
                "domain version, e.g. <domain>_<version> + <domain>_<version>_data).",
            )
        return (
            _OK,
            f"Graph schema '{schema}': {table_count} table(s), {view_count} view(s). "
            "Each Knowledge Graph build creates a raw triple table and a materialized _data table.",
        )
    except Exception as exc:
        return _ERROR, f"Graph DB table check failed (schema={schema}): {exc}"


def _check_graphdb_permissions(settings: Settings) -> Tuple[str, str]:
    """Verify SELECT / INSERT / UPDATE / DELETE on the graph DB schema tables."""
    from back.core.databricks.lakebase import BranchLakebaseAuth, get_lakebase_auth

    cfg = _resolve_registry_cfg(settings)
    try:
        from back.objects.registry.store import RegistryFactory

        store = RegistryFactory.from_cfg(cfg)
        global_cfg = store.load_global_config()
        engine_cfg = global_cfg.get("graph_engine_config") or {}
    except Exception as exc:
        return _WARNING, f"Could not load graph engine config: {exc}"

    database = (engine_cfg.get("database") or "").strip()
    schema = (
        engine_cfg.get("schema") or engine_cfg.get("graph_schema") or "ontobricks_graph"
    ).strip()
    branch_path = (engine_cfg.get("lakebase_branch") or "").strip()

    auth = BranchLakebaseAuth(branch_path, database) if branch_path else get_lakebase_auth()
    if not auth.is_available:
        return _WARNING, "Lakebase not bound — Graph DB permissions not probed"

    try:
        from back.core.graphdb.lakebase.pool import _require_psycopg

        psycopg, _ = _require_psycopg()
        kwargs = auth.kwargs(application_name="ontobricks-graphdb-diag")
        if database:
            kwargs["dbname"] = database

        with psycopg.connect(**kwargs) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT has_schema_privilege(current_user, %s, 'USAGE'), "
                "       has_schema_privilege(current_user, %s, 'CREATE'), "
                "       current_user",
                (schema, schema),
            )
            row = cur.fetchone() or (False, False, "?")
            has_usage, has_create, cur_user = row

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
                "WHERE table_schema = %s AND table_type = 'BASE TABLE'",
                (schema,),
            )
            tbl_sel, tbl_ins, tbl_upd, tbl_del, tbl_count = cur.fetchone() or (
                True,
                True,
                True,
                True,
                0,
            )

        missing: List[str] = []
        if not has_usage:
            missing.append("schema USAGE")
        if not has_create:
            missing.append("schema CREATE (required to create triple tables per KG build)")
        if not tbl_sel:
            missing.append("table SELECT")
        if not tbl_ins:
            missing.append("table INSERT")
        if not tbl_upd:
            missing.append("table UPDATE")
        if not tbl_del:
            missing.append("table DELETE")

        if missing:
            return (
                _ERROR,
                f"Missing Postgres permissions on graph schema '{schema}' for role '{cur_user}': "
                f"{', '.join(missing)}. "
                "Run Settings → Lakebase → Permissions to grant superuser, or use "
                "scripts/bootstrap-lakebase-perms.sh.",
            )
        return (
            _OK,
            f"Graph DB permissions OK — role '{cur_user}' on schema '{schema}' "
            f"({int(tbl_count)} table(s)); USAGE + CREATE + SELECT/INSERT/UPDATE/DELETE all granted.",
        )
    except Exception as exc:
        return _ERROR, f"Graph DB permission check failed (schema={schema}): {exc}"


def _check_graphdb_uc_catalog(settings: Settings) -> Tuple[str, str]:
    """Verify UC ALL_PRIVILEGES on sync_uc_catalog when sync_mode == managed_synced.

    In managed_synced mode the app's service principal must hold ALL_PRIVILEGES on
    the Unity Catalog catalog so Lakeflow can create/refresh the synced Delta table.
    This check is informational (warning) when sync_mode is not managed_synced.
    """
    cfg = _resolve_registry_cfg(settings)
    try:
        from back.objects.registry.store import RegistryFactory

        store = RegistryFactory.from_cfg(cfg)
        global_cfg = store.load_global_config()
        engine_cfg = global_cfg.get("graph_engine_config") or {}
    except Exception as exc:
        return _WARNING, f"Could not load graph engine config: {exc}"

    sync_mode = (engine_cfg.get("sync_mode") or "app_managed").strip()
    if sync_mode != "managed_synced":
        return (
            _OK,
            f"sync_mode={sync_mode!r} — UC catalog grant not required (only needed for managed_synced)",
        )

    uc_catalog = (engine_cfg.get("sync_uc_catalog") or "").strip()
    if not uc_catalog:
        # Fall back to registry catalog when sync_uc_catalog is not explicit.
        uc_catalog = (getattr(cfg, "catalog", None) or "").strip()

    if not uc_catalog:
        return (
            _ERROR,
            "sync_mode=managed_synced but sync_uc_catalog is not configured. "
            "Set graph_engine_config.sync_uc_catalog to the UC catalog where Lakeflow "
            "will register the synced Delta table.",
        )

    try:
        from back.core.databricks.DatabricksClient import DatabricksClient

        client = DatabricksClient()
        grants = (
            client.api_client.do(
                "GET",
                f"/api/2.1/unity-catalog/permissions/catalog/{uc_catalog}",
            )
            or {}
        )
        privilege_assignments = grants.get("privilege_assignments") or []
        # Check that at least one principal holds ALL_PRIVILEGES or a broad superset.
        _all_priv_markers = {"ALL_PRIVILEGES", "CREATE_TABLE", "CREATE_SCHEMA"}
        for assignment in privilege_assignments:
            privs = {p.upper() for p in (assignment.get("privileges") or [])}
            if "ALL_PRIVILEGES" in privs or _all_priv_markers.issubset(privs):
                principal = assignment.get("principal") or "?"
                return (
                    _OK,
                    f"UC catalog '{uc_catalog}': ALL_PRIVILEGES confirmed for '{principal}'.",
                )
        return (
            _WARNING,
            f"UC catalog '{uc_catalog}': ALL_PRIVILEGES not found in grant list. "
            "Run Settings → Lakebase → Create graph DB (with 'Grant UC catalog' enabled) "
            "or: GRANT ALL_PRIVILEGES ON CATALOG <catalog> TO <sp_client_id>.",
        )
    except Exception as exc:
        return (
            _WARNING,
            f"Could not read UC catalog grants for '{uc_catalog}': {exc}. "
            "Ensure the service principal has permission to read UC permissions.",
        )


def _check_delta_warehouse(settings: Settings) -> Tuple[str, str]:
    """Check whether the Delta triple-store warehouse is configured and reachable."""
    cfg = _resolve_registry_cfg(settings)
    try:
        from back.objects.registry.store import RegistryFactory

        store = RegistryFactory.from_cfg(cfg)
        global_cfg = store.load_global_config()
        backend = global_cfg.get("triple_store_backend", "lakebase")
        delta_warehouse_id = (global_cfg.get("delta_warehouse_id") or "").strip()
    except Exception as exc:
        return _WARNING, f"Could not read triple-store backend config: {exc}"

    if backend != "databricks":
        return (
            _OK,
            f"Delta backend not selected (current backend: {backend}) — skipped. "
            "Switch to Delta in Settings → Triple store → Back End to enable.",
        )
    if not delta_warehouse_id:
        return (
            _WARNING,
            "Delta backend is selected but no dedicated Delta warehouse is configured. "
            "Set one in Settings → Delta → SQL Warehouse (falls back to the global warehouse).",
        )

    client = _build_health_client(settings)
    if client is None:
        return _WARNING, "No Databricks credentials — warehouse probe skipped"
    ok, msg = client.sql.test_connection()
    return (_OK if ok else _ERROR), f"Delta warehouse {delta_warehouse_id}: {msg}"


def _check_delta_objects_exist(settings: Settings) -> Tuple[str, str]:
    """Check whether Delta triple-store objects exist in the registry UC schema."""
    cfg = _resolve_registry_cfg(settings)
    if not (cfg.catalog and cfg.schema):
        return _WARNING, "Registry catalog/schema not configured — skipped"

    try:
        from back.objects.registry.store import RegistryFactory

        store = RegistryFactory.from_cfg(cfg)
        global_cfg = store.load_global_config()
        backend = global_cfg.get("triple_store_backend", "lakebase")
    except Exception as exc:
        return _WARNING, f"Could not read triple-store config: {exc}"

    if backend != "databricks":
        return (
            _OK,
            f"Delta backend not selected (current: {backend}) — skipped. "
            "Delta objects only exist after a Knowledge Graph build with the Delta backend.",
        )

    client = _build_health_client(settings)
    if client is None:
        return _WARNING, "No Databricks credentials — skipped"
    if not getattr(client, "warehouse_id", ""):
        return _WARNING, "No SQL warehouse configured — Delta objects check skipped"

    try:
        objects = client.list_tables_and_views(cfg.catalog, cfg.schema) or []
        if not objects:
            return (
                _WARNING,
                f"No objects found in {cfg.catalog}.{cfg.schema}. "
                "Run Knowledge Graph → Build to create the R2RML VIEW and _data Delta table "
                "for each domain version.",
            )
        data_tables = [o for o in objects if isinstance(o, dict) and o.get("name", "").endswith("_data")]
        views = [o for o in objects if isinstance(o, dict) and o.get("table_type") == "VIEW"]
        return (
            _OK,
            f"{len(objects)} object(s) in {cfg.catalog}.{cfg.schema}: "
            f"{len(views)} VIEW(s), {len(data_tables)} _data table(s).",
        )
    except Exception as exc:
        return _ERROR, f"Delta objects check failed: {exc}"


# ---------------------------------------------------------------------------
# Diagnostics aggregator (grouped by subsystem)
# ---------------------------------------------------------------------------


def run_diagnostics_checks(settings: Optional[Settings] = None) -> Dict[str, Any]:
    """Run comprehensive diagnostics, grouped by subsystem.

    Returns four groups:

    * **Unity Catalog — Registry** — catalog/schema/volume access + DDL privileges
    * **Lakebase — Registry** — Postgres connection, schema tables, permissions
    * **Lakebase — Graph DB** — graph schema connectivity, tables, permissions
    * **Delta Triple Store** — Delta warehouse, UC objects, Accelerated Sync

    The function is synchronous; wrap in :func:`run_blocking` from an ``async`` route.
    """
    settings = settings or get_settings()
    groups = []
    all_checks: List[Dict[str, Any]] = []

    # ── Group 1: Unity Catalog — Registry ──────────────────────────────────
    uc_checks = [
        _safely_run(
            "uc.config",
            "Registry configuration resolved",
            lambda: _check_registry_cfg(settings),
        ),
        _safely_run(
            "uc.catalog",
            "USE CATALOG privilege",
            lambda: _check_uc_catalog_privileges(settings),
        ),
        _safely_run(
            "uc.schema",
            "USE SCHEMA privilege",
            lambda: _check_uc_schema_privileges(settings),
        ),
        _safely_run(
            "uc.view_ddl",
            "CREATE VIEW in schema (DDL probe)",
            _check_registry_uc_schema_ddl,
        ),
        _safely_run(
            "uc.table_ddl",
            "CREATE TABLE in schema (Delta build probe)",
            lambda: _check_uc_create_table_privilege(settings),
        ),
        _safely_run(
            "uc.volume_read",
            "UC Volume — list (READ VOLUME)",
            lambda: _check_registry_volume_read(settings),
        ),
        _safely_run(
            "uc.volume_write",
            "UC Volume — write sentinel (WRITE VOLUME)",
            lambda: _check_registry_volume_write(settings),
        ),
    ]
    groups.append(
        {
            "id": "uc_registry",
            "title": "Unity Catalog — Registry",
            "description": (
                "Verifies the privileges the app service principal needs on the Unity Catalog "
                "registry catalog and schema. "
                "Required grants: USE CATALOG (to navigate the catalog), "
                "USE SCHEMA (to list objects in the registry schema), "
                "CREATE (to materialise R2RML VIEWs and Delta tables during Knowledge Graph builds — "
                "tested separately for VIEWs and TABLEs since a grant may allow one but not the other), "
                "READ VOLUME + WRITE VOLUME (to store .obx exports, document uploads, "
                "and the global config blob on the UC Volume). "
                "Missing any of these grants will cause builds or registry saves to fail."
            ),
            "checks": uc_checks,
        }
    )
    all_checks.extend(uc_checks)

    # ── Group 2: Lakebase — Registry (Postgres) ────────────────────────────
    lb_checks = [
        _safely_run(
            "lakebase.psycopg",
            "psycopg driver installed",
            _check_lakebase_psycopg,
        ),
        _safely_run(
            "lakebase.env_vars",
            "Lakebase PG* env vars present",
            _check_lakebase_env_vars,
        ),
        _safely_run(
            "lakebase.connection",
            "Registry Postgres — connection + USAGE check",
            lambda: _check_lakebase(settings),
        ),
        _safely_run(
            "lakebase.initialized",
            "Registry row exists (initialized)",
            lambda: _check_lakebase_registry_initialized(settings),
        ),
        _safely_run(
            "lakebase.tables",
            "Registry tables + sequences — existence",
            lambda: _check_lakebase_registry_tables(settings),
        ),
        _safely_run(
            "lakebase.permissions",
            "Registry schema — Postgres DML privileges",
            lambda: _check_lakebase_permissions(settings),
        ),
    ]
    groups.append(
        {
            "id": "lakebase_registry",
            "title": "Lakebase — Registry (Postgres)",
            "description": (
                "Checks the Lakebase Postgres instance used as the OntoBricks registry back-end. "
                "Pre-requisites in order: "
                "(1) psycopg v3 driver installed; "
                "(2) PGHOST + PGDATABASE + PGUSER env vars injected by Databricks Apps "
                "(or set manually in .env); "
                "(3) Postgres connection succeeds and the role has USAGE on the registry schema; "
                "(4) A registry row exists in the 'registries' table (created by Initialize); "
                "(5) All 14 schema-DDL tables + 3 sequences are present — "
                "core tables: registries, global_config, domains, domain_versions, "
                "domain_permissions, schedules, schedule_runs, build_runs; "
                "optional tables: graph_analytics, graph_analytics_runs, domain_review_events, "
                "domain_comments, domain_tasks, domain_edit_locks; "
                "lazy table: domain_change_events (created on first domain save); "
                "(6) The role has USAGE/CREATE on the schema + SELECT/INSERT/UPDATE/DELETE "
                "on all tables + USAGE/SELECT/UPDATE on all sequences. "
                "Run Settings → Registry → Initialize or scripts/bootstrap-lakebase-perms.sh "
                "to fix permission and initialization issues."
            ),
            "checks": lb_checks,
        }
    )
    all_checks.extend(lb_checks)

    # ── Group 3: Lakebase — Graph DB ───────────────────────────────────────
    gdb_checks = [
        _safely_run(
            "graphdb.connection",
            "Graph DB — connection + schema exists",
            lambda: _check_graphdb_lakebase(settings),
        ),
        _safely_run(
            "graphdb.tables",
            "Graph DB — schema tables & views",
            lambda: _check_graphdb_tables(settings),
        ),
        _safely_run(
            "graphdb.permissions",
            "Graph DB — Postgres USAGE + CREATE + DML on schema",
            lambda: _check_graphdb_permissions(settings),
        ),
        _safely_run(
            "graphdb.uc_catalog",
            "Graph DB — UC catalog ALL_PRIVILEGES (managed_synced only)",
            lambda: _check_graphdb_uc_catalog(settings),
        ),
    ]
    groups.append(
        {
            "id": "graphdb",
            "title": "Lakebase — Graph DB",
            "description": (
                "Checks the Lakebase Postgres database used to store Knowledge Graph triples. "
                "This is a separate database from the registry (configured in "
                "Settings → Lakebase → Connection). "
                "Triple tables are created per domain+version during a Knowledge Graph build: "
                "a raw triple table (<domain>_<version>) and a materialized copy "
                "(<domain>_<version>_data). "
                "When Managed Sync mode is active, a _sync foreign table is also created. "
                "Required Postgres grants: (1) USAGE on the graph schema (to connect); "
                "(2) CREATE on the graph schema (to create new triple tables per KG build — "
                "one pair of tables is created each time a domain is rebuilt); "
                "(3) SELECT / INSERT / UPDATE / DELETE on all triple tables. "
                "In managed_synced mode only: the app's service principal also needs "
                "ALL_PRIVILEGES on the Unity Catalog catalog (sync_uc_catalog) so Lakeflow "
                "can create and refresh the synced Delta table inside UC. "
                "Use Settings → Lakebase → Permissions to grant superuser to users who need "
                "direct Postgres access. "
                "Use Settings → Lakebase → Create graph DB (with 'Grant UC catalog' enabled) "
                "to provision a new instance from scratch."
            ),
            "checks": gdb_checks,
        }
    )
    all_checks.extend(gdb_checks)

    # ── Group 4: Delta Triple Store ────────────────────────────────────────
    delta_checks = [
        _safely_run(
            "delta.warehouse",
            "Delta warehouse — configured + reachable",
            lambda: _check_delta_warehouse(settings),
        ),
        _safely_run(
            "delta.objects",
            "Delta triple-store objects in UC schema",
            lambda: _check_delta_objects_exist(settings),
        ),
        _safely_run(
            "delta.accelerated_sync",
            "Lakebase Accelerated Sync (optional)",
            lambda: _check_lakebase_accelerated_sync(settings),
        ),
    ]
    groups.append(
        {
            "id": "delta",
            "title": "Delta Triple Store",
            "description": (
                "Checks for the Delta (Unity Catalog) triple-store backend. "
                "When Delta is selected in Settings → Triple store → Back End, OntoBricks "
                "stores triples as VIEW + Delta TABLE pairs inside the registry UC schema. "
                "A dedicated SQL warehouse can be configured in Settings → Delta → SQL Warehouse "
                "for Delta graph queries (falls back to the global warehouse if unset). "
                "Lakebase Accelerated Sync (synced_tables API) is an optional feature that "
                "enables Managed Sync mode: Lakeflow keeps a Postgres mirror of the Delta VIEW "
                "up to date automatically, replacing the app-managed COPY loop. "
                "It requires the workspace preview to be enabled."
            ),
            "checks": delta_checks,
        }
    )
    all_checks.extend(delta_checks)

    summary = {
        "total": len(all_checks),
        "ok": sum(1 for c in all_checks if c["status"] == _OK),
        "warnings": sum(1 for c in all_checks if c["status"] == _WARNING),
        "errors": sum(1 for c in all_checks if c["status"] == _ERROR),
    }
    overall = max(
        (c["status"] for c in all_checks),
        key=lambda s: _SEVERITY_RANK.get(s, 0),
        default=_OK,
    )
    return {
        "status": overall,
        "version": APP_VERSION,
        "summary": summary,
        "groups": groups,
    }


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
