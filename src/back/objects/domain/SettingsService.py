"""Databricks settings, registry, permissions, LadybugDB files, and schedule orchestration."""

from __future__ import annotations

import json
import os
import re
import shutil
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from back.core.errors import (
    AuthorizationError,
    InfrastructureError,
    NotFoundError,
    OntoBricksError,
    ValidationError,
)
from shared.config.settings import Settings
from back.core.databricks import is_databricks_app
from back.core.helpers import (
    get_databricks_client,
    get_databricks_host_and_token,
    resolve_warehouse_id,
    run_blocking,
)
from back.core.logging import get_logger
from back.objects.registry import (
    ASSIGNABLE_ROLES,
    RegistryCfg,
    RegistryService,
    permission_service,
    invalidate_registry_cache,
)
from back.objects.session import SessionManager, get_domain, global_config_service

logger = get_logger(__name__)

LADYBUG_BASE_DIR = "/tmp/ontobricks"

# Postgres database identifier — letters, digits, '_' or '-', must
# start with a letter or '_'. Conservatively narrower than the spec
# (no quoted identifiers) so we never have to worry about quoting it
# in dynamic SQL or in libpq conninfo strings.
_SAFE_DBNAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_-]*$")


def _enum_short(value: Any) -> str:
    """Return the short name of an SDK enum (or pass strings through).

    The Databricks SDK serialises ``DatabaseInstanceState`` and friends
    as ``ClassName.MEMBER``. The admin UI only needs ``MEMBER``.
    """
    if value is None or value == "":
        return ""
    s = str(value)
    return s.split(".", 1)[1] if "." in s else s


def _pg_version_short(value: Any) -> str:
    """Reduce ``PG_VERSION_16`` (SDK enum string) to ``"16"``."""
    s = _enum_short(value)
    if s.upper().startswith("PG_VERSION_"):
        return s[len("PG_VERSION_"):]
    return s


class SettingsService:
    """Configuration, registry, permissions, local LadybugDB files, and build schedules."""

    @staticmethod
    def _get_scheduler():
        """Defer APScheduler import until schedule endpoints run."""
        from back.objects.registry import get_scheduler as _gs

        return _gs()

    @staticmethod
    def is_warehouse_locked(settings: Settings) -> bool:
        """True when the SQL Warehouse is supplied by a Databricks App resource."""
        return is_databricks_app() and bool(settings.sql_warehouse_id)

    @staticmethod
    def is_registry_locked(settings: Settings) -> bool:
        """True when the registry is supplied by a Databricks App Volume resource."""
        return is_databricks_app() and bool(
            getattr(settings, "registry_volume_path", "")
        )

    @staticmethod
    def _resolve_context(session_mgr: SessionManager, settings: Settings):
        """Return the (domain, host, token, registry_cfg_dict) tuple used by most endpoints."""
        domain = get_domain(session_mgr)
        host, token = get_databricks_host_and_token(domain, settings)
        registry_cfg = RegistryCfg.from_domain(domain, settings).as_dict()
        return domain, host, token, registry_cfg

    @staticmethod
    def require_admin_error(
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> None:
        """Raise :class:`AuthorizationError` if the caller is not an admin in Databricks App mode."""
        if not is_databricks_app():
            return

        _, host, token, _ = SettingsService._resolve_context(session_mgr, settings)
        if not permission_service.is_admin(
            email,
            host,
            token,
            settings.ontobricks_app_name,
            user_token=user_token,
        ):
            raise AuthorizationError(
                "Only admins (CAN MANAGE) can change the SQL Warehouse"
            )

    @staticmethod
    def build_current_config(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """Build the payload for GET /settings/current."""
        domain = get_domain(session_mgr)

        host = domain.databricks.get("host") or settings.databricks_host
        token = domain.databricks.get("token") or settings.databricks_token
        warehouse_id = resolve_warehouse_id(domain, settings)

        has_config = bool(host and (token or settings.databricks_token))
        is_app_mode = bool(settings.databricks_host)

        auth_mode = "none"
        auth_display = "Not configured"
        if token:
            auth_mode = "token"
            auth_display = "Personal Access Token"
        elif is_app_mode:
            auth_mode = "app"
            auth_display = "Databricks App"

        warehouse_locked = SettingsService.is_warehouse_locked(settings)

        return {
            "host": host,
            "token": "***" if token else None,
            "warehouse_id": warehouse_id,
            "from_env": is_app_mode,
            "is_app_mode": is_app_mode,
            "auth_mode": auth_mode,
            "auth_display": auth_display,
            "has_config": has_config,
            "warehouse_locked": warehouse_locked,
        }

    @staticmethod
    def apply_config_save(
        data: Dict[str, Any],
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Apply POST /settings/save body to session and optional global warehouse."""
        domain = get_domain(session_mgr)

        if data.get("host"):
            domain.databricks["host"] = data["host"]
        if data.get("token"):
            domain.databricks["token"] = data["token"]

        if data.get("warehouse_id"):
            if SettingsService.is_warehouse_locked(settings):
                raise ValidationError(
                    "SQL Warehouse is configured via Databricks App resources and cannot be changed here.",
                )

            SettingsService.require_admin_error(
                email, user_token, session_mgr, settings
            )
            domain.databricks["warehouse_id"] = data["warehouse_id"]

            _, host, token, registry_cfg = SettingsService._resolve_context(
                session_mgr, settings
            )
            ok, msg = global_config_service.set_warehouse_id(
                host,
                token,
                registry_cfg,
                data["warehouse_id"],
            )
            if not ok:
                logger.info("Warehouse saved in session only (global: %s)", msg)

        domain.save()
        return {"success": True, "message": "Configuration saved"}

    @staticmethod
    async def test_connection(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """Test Databricks connectivity; returns success/message dict."""
        try:
            client = get_databricks_client(get_domain(session_mgr), settings)

            if not client:
                raise ValidationError(
                    "Databricks not configured. Please set DATABRICKS_HOST and DATABRICKS_TOKEN.",
                )

            warehouses = await run_blocking(client.get_warehouses)
            return {
                "success": True,
                "message": f"Connection successful. Found {len(warehouses)} warehouses.",
            }
        except OntoBricksError:
            raise
        except AttributeError as e:
            logger.exception("Test connection AttributeError: %s", e)
            error_msg = str(e)
            if "NoneType" in error_msg and "request" in error_msg:
                raise ValidationError(
                    "Databricks SDK not properly initialized. Check your authentication configuration.",
                ) from e
            raise InfrastructureError("Test connection failed", detail=error_msg) from e
        except Exception as e:
            logger.exception("Test connection failed: %s", e)
            raise InfrastructureError("Test connection failed", detail=str(e)) from e

    @staticmethod
    async def fetch_warehouses(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """List warehouses from Databricks (``warehouses`` key on success)."""
        try:
            client = get_databricks_client(get_domain(session_mgr), settings)
            if not client:
                raise ValidationError("Databricks not configured")
            return {"warehouses": await run_blocking(client.get_warehouses)}
        except OntoBricksError:
            raise
        except AttributeError as e:
            error_msg = str(e)
            if "NoneType" in error_msg and "request" in error_msg:
                logger.warning("Warehouses HTTP client error: %s", e)
                raise ValidationError(
                    "Databricks SDK not properly initialized. Check your authentication configuration.",
                ) from e
            logger.exception("Get warehouses AttributeError: %s", e)
            raise InfrastructureError(
                "Failed to list SQL warehouses", detail=error_msg
            ) from e
        except Exception as e:
            logger.exception("Get warehouses failed: %s", e)
            raise InfrastructureError(
                "Failed to list SQL warehouses", detail=str(e)
            ) from e

    @staticmethod
    def select_warehouse(
        warehouse_id: Optional[str],
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Persist warehouse selection in session and attempt global registry update."""
        if SettingsService.is_warehouse_locked(settings):
            raise ValidationError(
                "SQL Warehouse is configured via Databricks App resources and cannot be changed here.",
            )

        if not warehouse_id:
            raise ValidationError("No warehouse ID provided")

        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        domain, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        domain.databricks["warehouse_id"] = warehouse_id
        domain.save()

        ok, msg = global_config_service.set_warehouse_id(
            host,
            token,
            registry_cfg,
            warehouse_id,
        )
        if not ok:
            logger.info(
                "Warehouse stored in session only (global save failed: %s)",
                msg,
            )
            return {
                "success": True,
                "message": "Warehouse selected (stored in session — will persist globally once the registry is configured)",
            }
        return {"success": True, "message": "Warehouse selected"}

    @staticmethod
    async def fetch_catalogs(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        try:
            client = get_databricks_client(get_domain(session_mgr), settings)
            if not client:
                raise ValidationError("Databricks not configured")
            return {"catalogs": await run_blocking(client.get_catalogs)}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("Get catalogs failed: %s", e)
            raise InfrastructureError(
                "Failed to list Unity Catalog catalogs", detail=str(e)
            ) from e

    @staticmethod
    async def fetch_schemas(
        catalog: str,
        session_mgr: SessionManager,
        settings: Settings,
        *,
        log_label: str = "Get schemas",
    ) -> Dict[str, Any]:
        try:
            client = get_databricks_client(get_domain(session_mgr), settings)
            if not client:
                raise ValidationError("Databricks not configured")
            return {"schemas": await run_blocking(client.get_schemas, catalog)}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("%s failed: %s", log_label, e)
            raise InfrastructureError(f"{log_label} failed", detail=str(e)) from e

    @staticmethod
    async def fetch_volumes(
        catalog: str,
        schema: str,
        session_mgr: SessionManager,
        settings: Settings,
        log_label: str = "Get volumes",
    ) -> Dict[str, Any]:
        try:
            client = get_databricks_client(get_domain(session_mgr), settings)
            if not client:
                raise ValidationError("Databricks not configured")
            return {"volumes": await run_blocking(client.get_volumes, catalog, schema)}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("%s failed: %s", log_label, e)
            raise InfrastructureError(f"{log_label} failed", detail=str(e)) from e

    @staticmethod
    def build_registry_get_payload(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """Payload for GET /settings/registry.

        Includes the registry triplet (catalog/schema/volume), the
        active ``backend`` (``volume`` or ``lakebase``), the configured
        ``lakebase_schema``, and a read-only ``lakebase`` block that
        surfaces the runtime-injected Postgres connection parameters
        (``PGHOST``/``PGPORT``/``PGDATABASE``/``PGUSER``) plus
        availability/health for the admin UI.
        """
        rcfg = RegistryCfg.from_session(session_mgr, settings)
        initialized = False

        if rcfg.is_configured:
            try:
                svc = RegistryService.from_context(get_domain(session_mgr), settings)
                initialized = svc.is_initialized()
            except Exception:
                logger.debug("Could not check registry marker")

        return {
            "success": True,
            **rcfg.as_dict(),
            "configured": initialized,
            "registry_locked": SettingsService.is_registry_locked(settings),
            "available_backends": SettingsService._available_backends(),
            "lakebase": SettingsService._lakebase_runtime_info(rcfg),
        }

    @staticmethod
    def _available_backends() -> Dict[str, Any]:
        """Return which backends are usable in the current process.

        ``volume`` is always available. ``lakebase`` requires the
        optional :mod:`psycopg` extra and the four ``PG*`` environment
        variables that the Databricks App resource binding injects at
        runtime.
        """
        import os

        try:
            import psycopg  # noqa: F401  -- detect optional dep

            has_psycopg = True
        except ImportError:
            has_psycopg = False

        env_ready = all(
            os.environ.get(k)
            for k in ("PGHOST", "PGDATABASE", "PGUSER")
        )
        return {
            "volume": {"available": True, "reason": ""},
            "lakebase": {
                "available": has_psycopg and env_ready,
                "reason": (
                    ""
                    if has_psycopg and env_ready
                    else (
                        "psycopg extra not installed"
                        if not has_psycopg
                        else "Lakebase resource not bound (PGHOST/PGDATABASE/PGUSER missing)"
                    )
                ),
                "psycopg_installed": has_psycopg,
                "env_ready": env_ready,
            },
        }

    @staticmethod
    def _lakebase_runtime_info(rcfg: RegistryCfg) -> Dict[str, Any]:
        """Surface the read-only PG* connection params for the UI.

        Returns an empty block when the Lakebase resource is not bound.
        Never raises and never includes the OAuth token.

        When PG* env vars are present, also tries to enrich the payload
        with Databricks metadata about the bound instance (name, tier,
        state, pg_version, node_count). The lookup is best-effort and
        degrades silently on failure.

        ``database`` is the bound ``PGDATABASE``. ``database_override``
        is the (optional) admin-selected override stored in the
        registry config. ``effective_database`` is whichever of the
        two the store actually connects to — the override wins when
        set, otherwise the bound database is used.
        """
        import os

        host = os.environ.get("PGHOST", "")
        bound_db = os.environ.get("PGDATABASE", "")
        override_db = getattr(rcfg, "lakebase_database", "") or ""
        effective_db = override_db or bound_db
        if not host:
            return {
                "host": "",
                "port": "",
                "database": "",
                "database_override": override_db,
                "effective_database": effective_db,
                "user": "",
                "schema": rcfg.lakebase_schema,
                "bound": False,
                "initialized": False,
                "populated": False,
                "instance": None,
            }
        # Single probe: returns ``{initialized, populated}``. ``populated``
        # is true when the schema has the registry tables AND any of the
        # canonical data tables (domains, permission_sets, scheduled_*)
        # has at least one row. Used by the admin UI to:
        #   - hide *Migrate to Lakebase* when the admin is already on
        #     Lakebase and the tables hold data (the button doesn't make
        #     sense — it would silently overwrite live rows),
        #   - keep the button visible on Volume but downgrade it to a
        #     red *Re-sync* with a hard warning popup when Lakebase
        #     already holds data from a previous migration.
        status = SettingsService._lakebase_schema_status(rcfg)
        return {
            "host": host,
            "port": os.environ.get("PGPORT", "5432"),
            "database": bound_db,
            "database_override": override_db,
            "effective_database": effective_db,
            "user": os.environ.get("PGUSER", ""),
            "schema": rcfg.lakebase_schema,
            "bound": True,
            "initialized": status["initialized"],
            "populated": status["populated"],
            "instance": SettingsService._lakebase_instance_metadata(
                host, bound_db
            ),
        }

    @staticmethod
    def _lakebase_schema_initialized(rcfg: RegistryCfg) -> bool:
        """Best-effort probe of ``store.is_initialized()``. Never raises.

        Kept for callers that only need the boolean — internally
        :meth:`_lakebase_schema_status` is the canonical entry point
        because it returns both ``initialized`` and ``populated`` from
        a single store instance.
        """
        return SettingsService._lakebase_schema_status(rcfg)["initialized"]

    @staticmethod
    def _lakebase_schema_status(rcfg: RegistryCfg) -> Dict[str, bool]:
        """Probe ``initialized`` + ``populated`` for the Lakebase schema.

        ``initialized`` mirrors :meth:`RegistryStore.is_initialized` —
        true when the registry tables exist and a registry row matches
        this schema. ``populated`` is true when at least one of the
        canonical data tables (``domains``, ``domain_versions``,
        ``domain_permissions``, ``schedules``, ``schedule_runs``)
        carries one or more rows. Both default to ``False`` when
        psycopg is missing, the Lakebase resource is unbound, or any
        error occurs — this is purely informational UI plumbing.
        """
        result = {"initialized": False, "populated": False}
        try:
            import psycopg  # noqa: F401  -- gate on optional extra
        except ImportError:
            return result
        try:
            from back.objects.registry.store import RegistryFactory

            store = RegistryFactory.lakebase(
                registry_cfg=rcfg,
                schema=rcfg.lakebase_schema,
                database=rcfg.lakebase_database,
            )
            result["initialized"] = bool(store.is_initialized())
        except Exception as exc:  # noqa: BLE001 -- purely informational
            logger.debug("Lakebase schema init probe failed: %s", exc)
            return result
        if not result["initialized"]:
            return result
        # Cheap row-count probe across the canonical tables. The store
        # already short-circuits unknown table names so this is safe
        # even for partial schemas.
        try:
            counts = store.table_row_counts(
                (
                    "domains",
                    "domain_versions",
                    "domain_permissions",
                    "schedules",
                    "schedule_runs",
                )
            )
            result["populated"] = any((counts.get(t) or 0) > 0 for t in counts)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Lakebase populated probe failed: %s", exc)
        return result

    @staticmethod
    def _lakebase_instance_metadata(
        host: str, bound_database: str = ""
    ) -> Optional[Dict[str, Any]]:
        """Look up Databricks metadata for the Lakebase instance at ``host``.

        Matches by ``read_write_dns`` (and falls back to ``read_only_dns``)
        and returns a small JSON-friendly dict suitable for the admin UI.
        Returns ``None`` on any failure — the caller will still show the
        raw connection params (PGHOST/PGPORT/PGDATABASE/PGUSER), so this
        is purely additive.

        OntoBricks targets **Lakebase Autoscaling** exclusively: every
        instance is expected to expose the
        ``/api/2.0/postgres/projects/<name>`` resource. The payload
        therefore enriches the base instance fields with the active
        branch (the one hosting the bound ``PGDATABASE``) plus the
        project's autoscaling CU min/max range. Legacy Provisioned
        instances are not supported — see ``databricks.yml`` for the
        deployment configuration.
        """
        if not host:
            return None
        try:
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            target = host.strip().lower()
            for inst in w.database.list_database_instances():
                rw = (getattr(inst, "read_write_dns", "") or "").strip().lower()
                ro = (getattr(inst, "read_only_dns", "") or "").strip().lower()
                if target in (rw, ro):
                    capacity = getattr(inst, "effective_capacity", "") or getattr(
                        inst, "capacity", ""
                    ) or ""
                    state = getattr(inst, "state", "")
                    pg_version = getattr(inst, "pg_version", "") or ""
                    name = getattr(inst, "name", "") or ""
                    payload = {
                        "name": name,
                        "uid": getattr(inst, "uid", "") or "",
                        # SDK enums stringify as e.g. ``DatabaseInstanceState.AVAILABLE``;
                        # strip the prefix so the UI shows just ``AVAILABLE``.
                        "state": _enum_short(state),
                        "stopped": bool(
                            getattr(inst, "effective_stopped", False)
                            or getattr(inst, "stopped", False)
                        ),
                        "capacity": _enum_short(capacity),
                        "pg_version": _pg_version_short(pg_version),
                        "node_count": getattr(inst, "effective_node_count", None)
                        or getattr(inst, "node_count", None),
                        "creator": getattr(inst, "creator", "") or "",
                        "creation_time": getattr(inst, "creation_time", "") or "",
                        "endpoint": rw or ro,
                        "read_only_endpoint": ro,
                        "branch": "",
                        "branch_resource": "",
                        "autoscaling_min_cu": None,
                        "autoscaling_max_cu": None,
                    }
                    payload.update(
                        SettingsService._lakebase_branch_info(
                            w, name, bound_database
                        )
                    )
                    return payload
            logger.debug(
                "No Databricks database instance matched PGHOST=%s "
                "(searched read_write_dns / read_only_dns)",
                host,
            )
            return None
        except Exception as exc:  # noqa: BLE001 -- best-effort enrichment
            logger.debug("Lakebase instance lookup failed: %s", exc)
            return None

    @staticmethod
    def _lakebase_branch_info(
        w: Any, instance_name: str, bound_database: str
    ) -> Dict[str, Any]:
        """Look up the active branch + autoscaling CU range for an instance.

        Hits ``GET /api/2.0/postgres/projects/<name>`` to read the
        Autoscaling project metadata: default branch path and the
        project's autoscaling CU min/max settings. Then walks
        ``/projects/<name>/branches/*/databases`` to identify which
        branch actually hosts the bound ``PGDATABASE``; falls back to
        the project's ``default_branch`` if the database name is empty
        or unmatched.

        Always returns a dict; on any failure the dict is empty so
        callers can merge it on top of the base payload without losing
        the default values. OntoBricks expects every instance to be
        Autoscaling — Provisioned instances are not supported.
        """
        if not instance_name:
            return {}
        try:
            project = w.api_client.do(
                "GET", f"/api/2.0/postgres/projects/{instance_name}"
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Lakebase project lookup for %r failed (Autoscaling "
                "metadata unavailable): %s",
                instance_name,
                exc,
            )
            return {}
        if not isinstance(project, dict):
            return {}
        status = project.get("status") or {}
        default_branch_path = status.get("default_branch") or ""
        default_branch = default_branch_path.rsplit("/", 1)[-1] if default_branch_path else ""
        endpoint_settings = status.get("default_endpoint_settings") or {}
        out: Dict[str, Any] = {
            "branch": default_branch,
            "branch_resource": default_branch_path,
            "autoscaling_min_cu": endpoint_settings.get("autoscaling_limit_min_cu"),
            "autoscaling_max_cu": endpoint_settings.get("autoscaling_limit_max_cu"),
        }
        active_branch = SettingsService._lakebase_active_branch(
            w, instance_name, bound_database, default_branch_path
        )
        if active_branch:
            out["branch"] = active_branch.rsplit("/", 1)[-1]
            out["branch_resource"] = active_branch
        return out

    @staticmethod
    def _lakebase_active_branch(
        w: Any,
        instance_name: str,
        bound_database: str,
        default_branch_path: str,
    ) -> str:
        """Find the branch that hosts ``bound_database`` (best-effort).

        Returns the full resource path (``projects/<name>/branches/<b>``)
        or ``""`` on any miss. Walks every branch's database listing
        and matches on ``status.postgres_database``.
        """
        if not bound_database:
            return default_branch_path
        try:
            listing = w.api_client.do(
                "GET", f"/api/2.0/postgres/projects/{instance_name}/branches"
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "Lakebase branch listing for %r failed: %s", instance_name, exc
            )
            return default_branch_path
        branches = (listing or {}).get("branches") or []
        for branch in branches:
            branch_path = branch.get("name") or ""
            if not branch_path:
                continue
            try:
                dbs = w.api_client.do(
                    "GET", f"/api/2.0/postgres/{branch_path}/databases"
                )
            except Exception as exc:  # noqa: BLE001
                logger.debug(
                    "Lakebase database listing for %r failed: %s",
                    branch_path,
                    exc,
                )
                continue
            for db in (dbs or {}).get("databases", []) or []:
                pg_name = ((db.get("status") or {}).get("postgres_database")) or ""
                if pg_name == bound_database:
                    return branch_path
        return default_branch_path

    @staticmethod
    def list_lakebase_databases_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """List Postgres databases on the bound Lakebase instance.

        Used by the admin Registry Location modal so the admin can pick
        a database different from the one auto-injected via the Apps
        ``database`` resource binding (the bound ``PGDATABASE``). The
        Lakebase JWT scope is per-instance, so the cached token can
        connect to every database the SP is allowed on.

        Returns a JSON-friendly payload::

            {
              "success": true,
              "bound_database": "ontobricks_registry",  # PGDATABASE
              "selected_database": "ontobricks_other",  # registry config override
              "databases": [
                {"name": "ontobricks_registry", "connectable": true,
                 "is_bound": true, "is_selected": false},
                {"name": "ontobricks_other",    "connectable": true,
                 "is_bound": false, "is_selected": true},
                ...
              ]
            }

        ``connectable`` reflects ``has_database_privilege(current_user,
        datname, 'CONNECT')`` so the UI can disable databases the SP
        is not authorised on. Template / system databases are filtered
        out. The query runs against the *bound* database so it works
        even if the configured override has been revoked.
        """
        from back.core.databricks import get_lakebase_auth

        auth = get_lakebase_auth()
        if not auth.is_available:
            return {
                "success": False,
                "message": "Lakebase resource not bound (PGHOST/PGUSER missing)",
                "bound_database": "",
                "selected_database": "",
                "databases": [],
            }

        try:
            cfg = RegistryCfg.from_session(session_mgr, settings)
        except Exception as exc:  # noqa: BLE001
            return {
                "success": False,
                "message": f"Could not resolve registry context: {exc}",
                "bound_database": "",
                "selected_database": "",
                "databases": [],
            }

        try:
            import psycopg  # noqa: F401  -- ensure extra is installed
        except ImportError:
            return {
                "success": False,
                "message": "psycopg not installed",
                "bound_database": "",
                "selected_database": cfg.lakebase_database,
                "databases": [],
            }

        bound_db = auth.database
        selected_db = cfg.lakebase_database or ""

        # Connect to the *bound* database to enumerate. The bound DB
        # is the one we know the SP is authorised on.
        try:
            import psycopg

            with psycopg.connect(
                autocommit=True,
                **auth.kwargs(application_name="ontobricks-registry"),
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT datname,
                               has_database_privilege(
                                   current_user, datname, 'CONNECT'
                               )
                        FROM pg_database
                        WHERE datistemplate = false
                          AND datname NOT IN ('postgres', 'rdsadmin')
                        ORDER BY datname
                        """
                    )
                    rows = cur.fetchall()
        except Exception as exc:  # noqa: BLE001
            logger.exception("Lakebase list-databases failed")
            return {
                "success": False,
                "message": f"Could not list databases: {exc}",
                "bound_database": bound_db,
                "selected_database": selected_db,
                "databases": [],
            }

        databases = [
            {
                "name": name,
                "connectable": bool(connectable),
                "is_bound": name == bound_db,
                "is_selected": name == selected_db,
            }
            for (name, connectable) in rows
        ]
        return {
            "success": True,
            "bound_database": bound_db,
            "selected_database": selected_db,
            "databases": databases,
        }

    @staticmethod
    def lakebase_stats_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """Return per-table row counts for the Lakebase registry schema.

        Used by the admin Registry Location panel to give a quick at-a-
        glance inventory of what currently lives in Lakebase. Never
        raises — surfaces failures via ``success=False`` + ``message``.
        """
        from back.core.databricks import get_lakebase_auth

        auth = get_lakebase_auth()
        if not auth.is_available:
            return {
                "success": False,
                "message": "Lakebase resource not bound (PGHOST/PGUSER missing)",
                "tables": [],
            }

        try:
            domain = get_domain(session_mgr)
            cfg = RegistryCfg.from_domain(domain, settings)
            host, token = get_databricks_host_and_token(domain, settings)
        except Exception as exc:  # noqa: BLE001
            return {
                "success": False,
                "message": f"Could not resolve registry context: {exc}",
                "tables": [],
            }

        try:
            from back.objects.registry.store import RegistryFactory

            lakebase_cfg = RegistryCfg(
                catalog=cfg.catalog,
                schema=cfg.schema,
                volume=cfg.volume,
                backend="lakebase",
                lakebase_schema=cfg.lakebase_schema,
                lakebase_database=cfg.lakebase_database,
            )
            store = RegistryFactory.lakebase(
                registry_cfg=lakebase_cfg,
                schema=cfg.lakebase_schema,
                database=cfg.lakebase_database,
            )
        except ImportError:
            return {
                "success": False,
                "message": "Lakebase backend not installed (missing psycopg)",
                "tables": [],
            }
        except Exception as exc:  # noqa: BLE001
            return {
                "success": False,
                "message": f"Could not build Lakebase store: {exc}",
                "tables": [],
            }

        tables = (
            "registries",
            "global_config",
            "domains",
            "domain_versions",
            "domain_permissions",
            "schedules",
            "schedule_runs",
        )
        try:
            counts = store.table_row_counts(tables)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Lakebase table_row_counts failed")
            # Surface the *kind* of failure so the admin can
            # distinguish "schema not initialised yet" from "service
            # principal lacks USAGE" or "instance unreachable" — the
            # 0-rows-everywhere alternative used to mask real
            # deployment problems.
            return {
                "success": False,
                "schema": cfg.lakebase_schema,
                "initialized": False,
                "reason": "table_count_failed",
                "message": f"Could not query Lakebase: {exc}",
                "tables": [{"name": t, "rows": 0} for t in tables],
            }
        # Use the detailed probe so the UI can distinguish "missing
        # USAGE on the schema" (silent before — looked like an empty
        # registry) from genuine first-run states. Falls back to the
        # plain bool for stores that haven't grown ``init_status``.
        if hasattr(store, "init_status"):
            status = store.init_status()
            initialized = bool(status.get("initialized"))
            reason = status.get("reason") or ("ok" if initialized else "unknown")
            error = status.get("error")
        else:
            initialized = bool(store.is_initialized())
            reason = "ok" if initialized else "unknown"
            error = None
        payload: Dict[str, Any] = {
            "success": True,
            "schema": cfg.lakebase_schema,
            "initialized": initialized,
            "reason": reason,
            "tables": [{"name": t, "rows": counts.get(t, 0)} for t in tables],
        }
        if error:
            payload["message"] = error
        return payload

    @staticmethod
    def apply_registry_save(
        data: Dict[str, Any], session_mgr: SessionManager
    ) -> Dict[str, Any]:
        """Persist registry triplet, backend choice, and lakebase schema.

        Expected payload (all optional, partial updates allowed):

        - ``catalog`` / ``schema`` / ``volume``: Unity Catalog triplet.
        - ``backend``: ``"volume"`` or ``"lakebase"``.
        - ``lakebase_schema``: Postgres schema for registry tables.
        - ``lakebase_database``: optional Postgres database override.
          Empty string clears the override (falls back to the bound
          ``PGDATABASE``); the key must be present in the payload to
          opt into the change so a partial save never wipes it out.
        """
        domain = get_domain(session_mgr)
        reg = domain.settings.setdefault("registry", {})
        if data.get("catalog"):
            reg["catalog"] = data["catalog"]
        if data.get("schema"):
            reg["schema"] = data["schema"]
        if data.get("volume"):
            reg["volume"] = data["volume"]
        if "backend" in data:
            backend = (data.get("backend") or "volume").lower()
            if backend not in ("volume", "lakebase"):
                raise ValidationError(
                    f"Unknown registry backend '{backend}' — expected volume or lakebase"
                )
            reg["backend"] = backend
        if data.get("lakebase_schema"):
            reg["lakebase_schema"] = data["lakebase_schema"]
        if "lakebase_database" in data:
            override = (data.get("lakebase_database") or "").strip()
            if override:
                if not _SAFE_DBNAME_RE.match(override):
                    raise ValidationError(
                        f"Invalid Lakebase database name {override!r}. "
                        f"Use letters, digits, '_' or '-' (start with a letter or '_')."
                    )
                reg["lakebase_database"] = override
            else:
                reg.pop("lakebase_database", None)
        domain.save()
        return {"success": True, "message": "Registry configuration saved"}

    @staticmethod
    def migrate_to_lakebase_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """Copy Volume registry data into Lakebase tables.

        Builds a :class:`VolumeRegistryStore` from the active session
        configuration and a :class:`LakebaseRegistryStore` using the
        injected ``PG*`` env vars and the configured Lakebase schema.
        Binary artefacts (``documents/``, ``*.lbug.tar.gz``) are not
        moved — they always live on the Volume.
        """
        try:
            domain = get_domain(session_mgr)
            host, token = get_databricks_host_and_token(domain, settings)
            cfg = RegistryCfg.from_domain(domain, settings)
            if not cfg.catalog or not cfg.schema:
                raise ValidationError(
                    "Registry catalog and schema must be configured first"
                )

            from back.objects.registry.store import (
                RegistryFactory,
                migrate_volume_to_lakebase,
                summarize_migration,
            )

            volume_cfg = RegistryCfg(
                catalog=cfg.catalog,
                schema=cfg.schema,
                volume=cfg.volume,
                backend="volume",
                lakebase_schema=cfg.lakebase_schema,
                lakebase_database=cfg.lakebase_database,
            )
            lakebase_cfg = RegistryCfg(
                catalog=cfg.catalog,
                schema=cfg.schema,
                volume=cfg.volume,
                backend="lakebase",
                lakebase_schema=cfg.lakebase_schema,
                lakebase_database=cfg.lakebase_database,
            )

            src_store = RegistryFactory.volume(
                registry_cfg=volume_cfg, host=host, token=token
            )
            try:
                dst_store = RegistryFactory.lakebase(
                    registry_cfg=lakebase_cfg,
                    schema=cfg.lakebase_schema,
                    database=cfg.lakebase_database,
                )
            except ImportError as exc:
                raise InfrastructureError(
                    "Lakebase backend not installed",
                    detail=str(exc),
                ) from exc

            report = migrate_volume_to_lakebase(src_store, dst_store)
            ok, summary = summarize_migration(report)
            return {
                "success": ok,
                "message": (
                    "Migration complete — " if ok else "Migration completed with errors — "
                )
                + summary,
                "report": report.as_dict(),
            }
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("Migrate to Lakebase failed: %s", e)
            raise InfrastructureError(
                "Migrate to Lakebase failed", detail=str(e)
            ) from e

    @staticmethod
    def initialize_registry_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        try:
            domain = get_domain(session_mgr)
            # ``prefer_volume_binding=True`` so the Initialize flow
            # pins the registry triplet to the *current* Volume binding
            # (not the cached Lakebase ``registries`` row). Without
            # this, re-binding the Volume resource and re-clicking
            # Initialize would silently no-op the row update — the row
            # is the source of truth for read paths, so callers would
            # keep seeing the stale catalog/schema/volume.
            svc = RegistryService.from_context(
                domain, settings, prefer_volume_binding=True
            )
            if not svc.cfg.is_configured:
                raise ValidationError(
                    "Registry catalog, schema, and volume must be configured first"
                )

            client = get_databricks_client(domain, settings)
            if not client:
                raise ValidationError("Databricks not configured")

            ok, msg = svc.initialize(client)
            if not ok:
                raise InfrastructureError("Registry initialization failed", detail=msg)
            # Drop the process-local Lakebase triplet cache so the next
            # ``RegistryCfg.from_domain`` reads the freshly-upserted
            # ``registries`` row instead of returning the stale triplet
            # captured before this Initialize.
            try:
                from back.objects.registry.store.lakebase.store import (
                    reset_lakebase_triplet_cache,
                )

                reset_lakebase_triplet_cache()
            except Exception:  # noqa: BLE001
                logger.debug(
                    "reset_lakebase_triplet_cache unavailable; skipping",
                    exc_info=True,
                )
            return {"success": ok, "message": msg}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("Initialize registry failed: %s", e)
            raise InfrastructureError(
                "Initialize registry failed", detail=str(e)
            ) from e

    @staticmethod
    def list_registry_domains_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        try:
            domain = get_domain(session_mgr)
            svc = RegistryService.from_context(domain, settings)
            if not svc.cfg.is_configured:
                raise ValidationError("Registry not configured")

            ok, result, msg = svc.list_domain_details_cached()
            if not ok:
                raise InfrastructureError("Failed to list registry domains", detail=msg)
            return {"success": True, "domains": result}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("List registry domains failed: %s", e)
            raise InfrastructureError(
                "Failed to list registry domains", detail=str(e)
            ) from e

    @staticmethod
    def list_registry_bridges_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """Return all bridges across every domain in the registry."""
        try:
            domain = get_domain(session_mgr)
            svc = RegistryService.from_context(domain, settings)
            if not svc.cfg.is_configured:
                raise ValidationError("Registry not configured")

            ok, result, msg = svc.list_all_bridges()
            if not ok:
                raise InfrastructureError("Failed to list registry bridges", detail=msg)
            return {"success": True, "domains": result}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("List registry bridges failed: %s", e)
            raise InfrastructureError(
                "Failed to list registry bridges", detail=str(e)
            ) from e

    @staticmethod
    def delete_registry_domain_result(
        domain_name: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        try:
            domain = get_domain(session_mgr)
            svc = RegistryService.from_context(domain, settings)
            if not svc.cfg.is_configured:
                raise ValidationError("Registry not configured")

            errors = svc.delete_domain(domain_name)

            if errors:
                joined = "; ".join(errors)
                raise InfrastructureError(
                    "Registry domain was only partially deleted",
                    detail=joined,
                )

            return {
                "success": True,
                "message": f'Domain "{domain_name}" deleted from registry',
            }
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("Delete registry domain failed: %s", e)
            raise InfrastructureError(
                "Delete registry domain failed", detail=str(e)
            ) from e

    @staticmethod
    def delete_registry_version_result(
        domain_name: str,
        version: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        try:
            domain = get_domain(session_mgr)
            svc = RegistryService.from_context(domain, settings)
            if not svc.cfg.is_configured:
                raise ValidationError("Registry not configured")

            d_ok, d_msg = svc.delete_version(domain_name, version)
            if not d_ok:
                raise InfrastructureError(
                    "Failed to delete registry version", detail=d_msg
                )

            return {
                "success": True,
                "message": f'Version {version} deleted from "{domain_name}"',
            }
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("Delete registry version failed: %s", e)
            raise InfrastructureError(
                "Delete registry version failed", detail=str(e)
            ) from e

    @staticmethod
    def set_registry_version_active_result(
        domain_name: str,
        version: str,
        enabled: bool,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Toggle the *active* (``mcp_enabled``) flag for a specific version.

        Works on any domain in the registry — the domain does not need to be
        loaded in the current session.  Only one version per domain may be
        active; enabling one automatically disables the others.
        """
        try:
            domain = get_domain(session_mgr)
            svc = RegistryService.from_context(domain, settings)
            if not svc.cfg.is_configured:
                raise ValidationError("Registry not configured")

            sorted_versions = svc.list_versions_sorted(domain_name)
            if version not in sorted_versions:
                raise NotFoundError(f'Version {version} not found in "{domain_name}"')

            if enabled:
                for ver in sorted_versions:
                    if ver == version:
                        continue
                    ok, data, _ = svc.read_version(domain_name, ver)
                    if not ok:
                        continue
                    if data.get("info", {}).get("mcp_enabled"):
                        data["info"]["mcp_enabled"] = False
                        svc.write_version(domain_name, ver, json.dumps(data))

            ok, data, msg = svc.read_version(domain_name, version)
            if not ok:
                raise InfrastructureError("Failed to read registry version", detail=msg)

            data.setdefault("info", {})["mcp_enabled"] = enabled
            svc.write_version(domain_name, version, json.dumps(data))

            invalidate_registry_cache()

            if (
                domain.domain_folder == domain_name
                and domain.current_version == version
            ):
                domain.info["mcp_enabled"] = enabled

            return {"success": True, "version": version, "active": enabled}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("Set registry version active failed: %s", e)
            raise InfrastructureError(
                "Set registry version active failed", detail=str(e)
            ) from e

    @staticmethod
    def set_default_emoji_result(
        emoji: str,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        ok, msg = global_config_service.set_default_emoji(
            host, token, registry_cfg, emoji
        )
        if not ok:
            raise InfrastructureError("Failed to save default emoji", detail=msg)
        return {"success": True, "emoji": emoji}

    @staticmethod
    def save_base_uri_result(
        base_uri: str,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        ok, msg = global_config_service.set_default_base_uri(
            host, token, registry_cfg, base_uri
        )
        if not ok:
            raise InfrastructureError("Failed to save default base URI", detail=msg)
        return {"success": True, "base_uri": base_uri}

    @staticmethod
    def get_registry_cache_ttl_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        ttl = global_config_service.get_registry_cache_ttl(host, token, registry_cfg)
        return {"success": True, "registry_cache_ttl": ttl}

    @staticmethod
    def save_registry_cache_ttl_result(
        ttl: int,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        ok, msg = global_config_service.set_registry_cache_ttl(
            host, token, registry_cfg, ttl
        )
        if not ok:
            raise InfrastructureError("Failed to save registry cache TTL", detail=msg)
        return {"success": True, "registry_cache_ttl": max(10, int(ttl))}

    # ------------------------------------------------------------------
    #  Graph DB Engine
    # ------------------------------------------------------------------

    @staticmethod
    def get_graph_engine_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        engine = global_config_service.get_graph_engine(host, token, registry_cfg)
        allowed = list(global_config_service.ALLOWED_GRAPH_ENGINES)
        return {"success": True, "graph_engine": engine, "allowed_engines": allowed}

    @staticmethod
    def set_graph_engine_result(
        engine: str,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        ok, msg = global_config_service.set_graph_engine(
            host, token, registry_cfg, engine
        )
        if not ok:
            raise ValidationError(msg)
        return {"success": True, "graph_engine": engine}

    @staticmethod
    def get_graph_engine_config_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Return the engine-specific JSON configuration."""
        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        cfg = global_config_service.get_graph_engine_config(host, token, registry_cfg)
        return {"success": True, "graph_engine_config": cfg}

    @staticmethod
    def set_graph_engine_config_result(
        config: Dict[str, Any],
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Persist the engine-specific JSON configuration (admin only)."""
        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        ok, msg = global_config_service.set_graph_engine_config(
            host, token, registry_cfg, config
        )
        if not ok:
            raise ValidationError(msg)
        return {"success": True, "graph_engine_config": config}

    @staticmethod
    def build_permissions_me(
        email: str,
        display_name: str,
        user_token: str,
        user_role: str,
        user_domain_role: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        if not is_databricks_app():
            return {
                "email": email or "local-user",
                "display_name": display_name or "Local User",
                "role": "admin",
                "is_app_mode": False,
            }

        role = "none"
        is_app_admin = False
        domain_role = user_domain_role or ""
        domain_folder = ""
        try:
            domain, host, token, registry_cfg = SettingsService._resolve_context(
                session_mgr, settings
            )
            domain_folder = getattr(domain, "domain_folder", "") or ""

            permission_service.clear_admin_cache(email)
            is_app_admin = permission_service.is_admin(
                email,
                host,
                token,
                settings.ontobricks_app_name,
                user_token=user_token,
            )
            role = permission_service.get_user_role(
                email,
                host,
                token,
                registry_cfg,
                settings.ontobricks_app_name,
                user_token=user_token,
            )
            # Re-resolve domain role fresh so it matches what the
            # middleware sees on the next request (useful for debugging
            # why a viewer can/can't write).
            domain_role = permission_service.get_domain_role(
                email,
                host,
                token,
                registry_cfg,
                settings.ontobricks_app_name,
                domain_folder,
                user_token=user_token,
                app_role=role,
            )
        except Exception as e:
            logger.error(
                "permissions/me: error resolving role for %s (middleware app/domain role=%r/%r): %s",
                email,
                user_role,
                user_domain_role,
                e,
                exc_info=True,
            )

        return {
            "email": email,
            "display_name": display_name,
            "role": role,
            "is_app_admin": is_app_admin,
            "is_app_mode": True,
            "domain_folder": domain_folder,
            "domain_role": domain_role,
        }

    @staticmethod
    def build_permissions_diag(
        email: str,
        display_name: str,
        user_token: str,
        user_role: str,
        user_domain_role: str,
        settings: Settings,
    ) -> Dict[str, Any]:
        from databricks.sdk import WorkspaceClient
        import requests as _req

        app_name = settings.ontobricks_app_name
        diag: dict = {
            "email": email,
            "app_name": app_name,
            "is_app_mode": is_databricks_app(),
            "user_token_present": bool(user_token),
            "display_name": display_name,
            "state_user_role": user_role,
            "state_user_domain_role": user_domain_role,
        }

        # ── SDK path (SP token) ──
        try:
            w = WorkspaceClient()
            diag["sdk_host"] = str(getattr(w.config, "host", ""))
            diag["sdk_auth_type"] = str(getattr(w.config, "auth_type", ""))
            raw = w.api_client.do("GET", f"/api/2.0/permissions/apps/{app_name}")
            acl_list = raw.get("access_control_list", [])
            managers = []
            for acl in acl_list:
                principal = (
                    acl.get("user_name")
                    or acl.get("group_name")
                    or acl.get("service_principal_name")
                    or ""
                )
                for p in acl.get("all_permissions", []):
                    if p.get("permission_level") == "CAN_MANAGE":
                        managers.append(principal)
            diag["sdk_can_manage"] = managers
            diag["sdk_error"] = None
        except Exception as e:
            diag["sdk_error"] = f"{type(e).__name__}: {e}"
            diag["sdk_can_manage"] = []

        # ── User-token path (preferred at runtime) ──
        if user_token:
            try:
                host = diag.get("sdk_host", "").rstrip("/")
                resp = _req.get(
                    f"{host}/api/2.0/permissions/apps/{app_name}",
                    headers={"Authorization": f"Bearer {user_token}"},
                    timeout=5,
                )
                resp.raise_for_status()
                acl_list = resp.json().get("access_control_list", [])
                managers = []
                for acl in acl_list:
                    principal = (
                        acl.get("user_name")
                        or acl.get("group_name")
                        or acl.get("service_principal_name")
                        or ""
                    )
                    for p in acl.get("all_permissions", []):
                        if p.get("permission_level") == "CAN_MANAGE":
                            managers.append(principal)
                diag["user_token_can_manage"] = managers
                diag["email_is_manager"] = email.lower() in [
                    m.lower() for m in managers
                ]
                diag["user_token_error"] = None
            except Exception as e:
                diag["user_token_error"] = f"{type(e).__name__}: {e}"
                diag["user_token_can_manage"] = []
                diag["email_is_manager"] = False
        else:
            diag["email_is_manager"] = email.lower() in [
                m.lower() for m in diag.get("sdk_can_manage", [])
            ]

        diag["admin_cache"] = {
            k: {"result": v[0], "age_s": round(time.time() - v[1], 1)}
            for k, v in permission_service._admin_cache.items()
        }

        return diag

    @staticmethod
    def list_app_principals_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """Return the Databricks App principals (users + groups).

        Used by Settings → Permissions (read-only view) and as the row
        source for the Registry → Teams matrix picker.
        """
        _, host, token, _ = SettingsService._resolve_context(session_mgr, settings)
        app_name = settings.ontobricks_app_name
        permission_service.clear_principals_cache()
        result = permission_service.list_app_principals(host, token, app_name)
        return {
            "success": True,
            "users": result.get("users", []),
            "groups": result.get("groups", []),
        }

    @staticmethod
    def list_principals_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """Alias kept for the Teams picker dropdown."""
        return SettingsService.list_app_principals_result(session_mgr, settings)

    @staticmethod
    def search_workspace_principals(
        query: str,
        principal_type: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Search users or groups that have access to the Databricks App.

        Fetches the full app-permission principal list (cached by
        ``PermissionService``) and applies a case-insensitive *contains*
        filter on the client side.  This avoids SCIM calls that the app
        service-principal typically cannot perform and ensures only
        app-visible principals are returned.
        """
        _, host, token, _ = SettingsService._resolve_context(session_mgr, settings)
        app_name = settings.ontobricks_app_name
        all_principals = permission_service.list_app_principals(host, token, app_name)

        q = query.lower()

        if principal_type == "group":
            groups = [
                g
                for g in all_principals.get("groups", [])
                if q in (g.get("display_name") or "").lower()
            ]
            return {"success": True, "results": groups}

        users = [
            u
            for u in all_principals.get("users", [])
            if q in (u.get("email") or "").lower()
            or q in (u.get("display_name") or "").lower()
        ]
        return {"success": True, "results": users}

    @staticmethod
    def list_domain_permissions_result(
        domain_name: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        entries = permission_service.list_domain_entries(
            host, token, registry_cfg, domain_name
        )
        return {"success": True, "domain": domain_name, "permissions": entries}

    @staticmethod
    def add_domain_permission_result(
        domain_name: str,
        data: Dict[str, Any],
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        principal = data.get("principal", "").strip()
        principal_type = data.get("principal_type", "user")
        display_name = data.get("display_name", principal)
        role = data.get("role", "viewer")

        if not principal:
            raise ValidationError("Principal (email or group name) is required")
        if role not in ASSIGNABLE_ROLES:
            raise ValidationError('Role must be "viewer", "editor", or "builder"')
        if not domain_name:
            raise ValidationError("Domain name is required")

        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        if not registry_cfg.get("catalog") or not registry_cfg.get("schema"):
            raise ValidationError("Registry not configured")

        ok, msg = permission_service.add_or_update_domain_entry(
            host,
            token,
            registry_cfg,
            domain_name,
            principal,
            principal_type,
            display_name,
            role,
        )
        if not ok:
            raise InfrastructureError(
                "Failed to add or update domain permission", detail=msg
            )
        return {"success": ok, "message": msg}

    @staticmethod
    def delete_domain_permission_result(
        domain_name: str,
        principal: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        if not registry_cfg.get("catalog") or not registry_cfg.get("schema"):
            raise ValidationError("Registry not configured")

        ok, msg = permission_service.remove_domain_entry(
            host,
            token,
            registry_cfg,
            domain_name,
            principal,
        )
        if not ok:
            raise InfrastructureError("Failed to remove domain permission", detail=msg)
        return {"success": ok, "message": msg}

    # ------------------------------------------------------------------
    # Teams matrix (Registry → Teams)
    # ------------------------------------------------------------------

    @staticmethod
    def build_teams_matrix_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        """Return the Teams matrix payload: domains, principals, assignments.

        Payload shape::

            {
              "success": true,
              "domains": ["acme", "beta", ...],
              "principals": [
                {"principal": "alice@acme", "principal_type": "user",
                 "display_name": "Alice"},
                {"principal": "data-eng", "principal_type": "group",
                 "display_name": "data-eng"}
              ],
              "assignments": {
                "acme": {"alice@acme": "editor"},
                "beta": {"data-eng": "viewer"}
              }
            }
        """
        domain_obj, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        app_name = settings.ontobricks_app_name

        # Domains
        domains: List[str] = []
        try:
            svc = RegistryService.from_context(domain_obj, settings)
            ok, names, _msg = svc.list_domains_cached()
            if ok:
                domains = sorted(names)
        except Exception as exc:
            logger.warning("Teams matrix: failed to list domains: %s", exc)

        # Principals from Databricks App ACL
        permission_service.clear_principals_cache()
        app_principals = permission_service.list_app_principals(host, token, app_name)

        principals: List[Dict[str, Any]] = []
        for u in app_principals.get("users", []):
            email = u.get("email") or ""
            if not email:
                continue
            principals.append(
                {
                    "principal": email,
                    "principal_type": "user",
                    "display_name": u.get("display_name") or email,
                }
            )
        for g in app_principals.get("groups", []):
            name = g.get("display_name") or g.get("id") or ""
            if not name:
                continue
            principals.append(
                {
                    "principal": name,
                    "principal_type": "group",
                    "display_name": name,
                }
            )

        # Assignments per domain (key: domain -> {principal: role})
        assignments: Dict[str, Dict[str, str]] = {}
        for domain_name in domains:
            try:
                entries = permission_service.list_domain_entries(
                    host, token, registry_cfg, domain_name
                )
                row: Dict[str, str] = {}
                for e in entries:
                    principal = e.get("principal", "")
                    role = e.get("role", "")
                    if principal and role:
                        row[principal] = role
                if row:
                    assignments[domain_name] = row
            except Exception as exc:
                logger.warning(
                    "Teams matrix: failed to read team for %s: %s", domain_name, exc
                )

        return {
            "success": True,
            "domains": domains,
            "principals": principals,
            "assignments": assignments,
        }

    @staticmethod
    def save_teams_batch_result(
        data: Dict[str, Any],
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Persist a batch of team changes across multiple domains.

        Body shape::

            {
              "changes": [
                {"domain_folder": "acme",
                 "principal": "alice@acme",
                 "principal_type": "user",
                 "display_name": "Alice",
                 "role": "editor"},
                {"domain_folder": "beta",
                 "principal": "bob@acme",
                 "principal_type": "user",
                 "display_name": "Bob",
                 "role": null}           # null = remove
              ]
            }
        """
        changes = data.get("changes") or []
        if not isinstance(changes, list):
            raise ValidationError("Body must include a 'changes' array")

        validated: List[Dict[str, Any]] = []
        for idx, ch in enumerate(changes):
            if not isinstance(ch, dict):
                raise ValidationError(f"Change #{idx} is not an object")
            domain_folder = (ch.get("domain_folder") or "").strip()
            principal = (ch.get("principal") or "").strip()
            principal_type = ch.get("principal_type") or "user"
            display_name = ch.get("display_name") or principal
            role = ch.get("role")

            if not domain_folder:
                raise ValidationError(
                    f"Change #{idx}: 'domain_folder' is required"
                )
            if not principal:
                raise ValidationError(f"Change #{idx}: 'principal' is required")
            if principal_type not in ("user", "group"):
                raise ValidationError(
                    f"Change #{idx}: 'principal_type' must be 'user' or 'group'"
                )
            if role is not None and role not in ASSIGNABLE_ROLES:
                raise ValidationError(
                    f"Change #{idx}: 'role' must be one of "
                    f"{list(ASSIGNABLE_ROLES)} or null"
                )

            validated.append(
                {
                    "domain_folder": domain_folder,
                    "principal": principal,
                    "principal_type": principal_type,
                    "display_name": display_name,
                    "role": role,
                }
            )

        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        if not registry_cfg.get("catalog") or not registry_cfg.get("schema"):
            raise ValidationError("Registry not configured")

        saved, failed = permission_service.save_domain_permissions_batch(
            host, token, registry_cfg, validated
        )

        return {
            "success": len(failed) == 0,
            "saved": saved,
            "failed": failed,
            "total_changes": len(validated),
        }

    @staticmethod
    def human_size(nbytes: int) -> str:
        """Return a human-readable file size string."""
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if abs(nbytes) < 1024:
                return f"{nbytes:.1f} {unit}" if unit != "B" else f"{nbytes} B"
            nbytes /= 1024  # type: ignore[assignment]
        return f"{nbytes:.1f} PB"

    @staticmethod
    def list_ladybugdb_files() -> Dict[str, Any]:
        """List files under the LadybugDB local directory."""
        if not os.path.isdir(LADYBUG_BASE_DIR):
            return {"success": True, "files": [], "base_dir": LADYBUG_BASE_DIR}

        items: List[Dict[str, Any]] = []
        try:
            for entry in sorted(os.scandir(LADYBUG_BASE_DIR), key=lambda e: e.name):
                if not entry.name.endswith(".lbug"):
                    continue
                try:
                    stat = entry.stat(follow_symlinks=False)
                except OSError:
                    continue

                if entry.is_dir(follow_symlinks=False):
                    total_size = 0
                    for dirpath, _dirnames, filenames in os.walk(entry.path):
                        for fname in filenames:
                            try:
                                total_size += os.path.getsize(
                                    os.path.join(dirpath, fname)
                                )
                            except OSError:
                                pass
                    size = total_size
                else:
                    size = stat.st_size

                mtime_dt = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
                items.append(
                    {
                        "name": entry.name,
                        "is_dir": entry.is_dir(follow_symlinks=False),
                        "size_bytes": size,
                        "size_display": SettingsService.human_size(size),
                        "modified_iso": mtime_dt.isoformat(),
                        "modified_display": mtime_dt.strftime("%Y-%m-%d %H:%M:%S UTC"),
                    }
                )
        except OSError as exc:
            logger.warning("Failed to list LadybugDB directory: %s", exc)
            raise InfrastructureError(
                "Failed to list LadybugDB files", detail=str(exc)
            ) from exc

        return {"success": True, "files": items, "base_dir": LADYBUG_BASE_DIR}

    @staticmethod
    def delete_ladybugdb_file(filename: str) -> Dict[str, Any]:
        """Delete a file or directory under LadybugDB base dir (basename only)."""
        safe_name = os.path.basename(filename)
        if not safe_name or safe_name in (".", ".."):
            raise ValidationError("Invalid filename")

        target = os.path.join(LADYBUG_BASE_DIR, safe_name)
        if not os.path.exists(target):
            raise NotFoundError(f"Not found: {safe_name}")

        try:
            if os.path.isdir(target):
                shutil.rmtree(target)
            else:
                os.remove(target)
            logger.info("Deleted LadybugDB file: %s", target)
            return {"success": True, "message": f"Deleted {safe_name}"}
        except OSError as exc:
            logger.warning("Failed to delete LadybugDB file %s: %s", target, exc)
            raise InfrastructureError(
                "Failed to delete LadybugDB file", detail=str(exc)
            ) from exc

    @staticmethod
    def list_schedules_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        scheduler = SettingsService._get_scheduler()
        try:
            entries = scheduler.get_all_schedules(host, token, registry_cfg)
            return {"success": True, "schedules": entries}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("list_schedules failed: %s", e)
            raise InfrastructureError("Failed to list schedules", detail=str(e)) from e

    @staticmethod
    def save_schedule_result(
        data: Dict[str, Any],
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        try:
            domain_name = (
                data.get("domain_name") or data.get("project_name") or ""
            ).strip()
            interval_minutes = int(data.get("interval_minutes", 60))
            drop_existing = bool(data.get("drop_existing", True))
            enabled = bool(data.get("enabled", True))
            version = (data.get("version") or "latest").strip()

            if not domain_name:
                raise ValidationError("Domain name is required")

            _, host, token, registry_cfg = SettingsService._resolve_context(
                session_mgr, settings
            )

            scheduler = SettingsService._get_scheduler()
            ok, msg = scheduler.save_schedule(
                host,
                token,
                registry_cfg,
                settings,
                domain_name,
                interval_minutes,
                drop_existing,
                enabled,
                version=version,
            )
            if not ok:
                raise InfrastructureError("Failed to save schedule", detail=msg)
            return {"success": ok, "message": msg}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("save_schedule failed: %s", e)
            raise InfrastructureError("Failed to save schedule", detail=str(e)) from e

    @staticmethod
    def get_schedule_history_result(
        domain_name: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        scheduler = SettingsService._get_scheduler()
        try:
            entries = scheduler.get_schedule_history(
                host, token, registry_cfg, domain_name
            )
            return {"success": True, "domain_name": domain_name, "history": entries}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("get_schedule_history failed for '%s': %s", domain_name, e)
            raise InfrastructureError(
                "Failed to load schedule history", detail=str(e)
            ) from e

    @staticmethod
    def scheduler_status_payload() -> Dict[str, Any]:
        scheduler = SettingsService._get_scheduler()
        return {"success": True, **scheduler.status()}

    @staticmethod
    def delete_schedule_result(
        domain_name: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        try:
            _, host, token, registry_cfg = SettingsService._resolve_context(
                session_mgr, settings
            )

            scheduler = SettingsService._get_scheduler()
            ok, msg = scheduler.remove_schedule(host, token, registry_cfg, domain_name)
            if not ok:
                raise InfrastructureError("Failed to remove schedule", detail=msg)
            return {"success": ok, "message": msg}
        except OntoBricksError:
            raise
        except Exception as e:
            logger.exception("delete_schedule failed: %s", e)
            raise InfrastructureError("Failed to remove schedule", detail=str(e)) from e
