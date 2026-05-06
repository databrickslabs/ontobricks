"""Readiness probe for OntoBricks — ``GET /health``.

This endpoint replaces the previous static dummy with a real
end-to-end check of every external dependency the application needs
to operate correctly:

* local filesystem — ``/tmp``, the session directory and the log
  directory must be writable, with enough free disk space;
* Databricks authentication — OAuth client-credentials in Apps mode
  or PAT in local development;
* SQL warehouse — TCP/SQL reachability via ``SELECT 1``;
* registry **UC volume** — Files-API read + write probe (a tiny
  sentinel file is written then deleted);
* registry **catalog/schema** — DDL probe via
  ``CREATE OR REPLACE VIEW <fqn> AS SELECT 1`` then ``DROP VIEW`` so
  view materialisation will succeed during Digital-Twin builds;
* **Lakebase** — when ``PG*`` env vars are bound, ``USAGE`` on the
  registry schema plus the presence of the ``registries`` table.

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

from shared.config.constants import APP_VERSION
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


def _build_health_client():
    """Instantiate a ``DatabricksClient`` with no domain/session.

    ``get_databricks_client`` already supports a ``None`` domain via
    ``RegistryCfg.from_domain(None, settings)``-style fallbacks, so the
    readiness route does not need a SessionManager.
    """
    from back.core.helpers import get_databricks_client

    return get_databricks_client(None, get_settings())


def _check_warehouse() -> Tuple[str, str]:
    client = _build_health_client()
    if client is None:
        return _WARNING, "No Databricks credentials available — warehouse not probed"
    if not getattr(client, "warehouse_id", ""):
        return _WARNING, "DATABRICKS_SQL_WAREHOUSE_ID is not configured"
    ok, msg = client.test_connection()
    return (_OK if ok else _ERROR), msg


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
        f"Backend={cfg.backend} catalog={cfg.catalog} schema={cfg.schema} volume={cfg.volume}",
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
# Lakebase probe
# ---------------------------------------------------------------------------


def _check_lakebase(settings: Settings) -> Tuple[str, str]:
    from back.core.databricks.LakebaseAuth import get_lakebase_auth

    auth = get_lakebase_auth()
    if not auth.is_available:
        return (
            _OK,
            "Lakebase not bound (PG* env vars unset) — registry uses Volume backend",
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
        _safely_run("databricks.warehouse", "SQL warehouse reachable", _check_warehouse)
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
            "Lakebase Postgres",
            lambda: _check_lakebase(settings),
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
