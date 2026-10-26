"""Lakebase graph-engine Settings: health, provision, objects, and grants.

Extracted from :class:`GraphEngineSettings` (Fowler Extract Class).
``SettingsService`` / ``GraphEngineSettings`` keep delegators for callers.
"""

from __future__ import annotations

import importlib
import os
from typing import Any, Dict, List

from back.core.errors import InfrastructureError, OntoBricksError, ValidationError
from shared.config.settings import Settings
from back.core.logging import get_logger
from back.objects.session import SessionManager
from back.objects.domain.settings.SettingsService import SettingsService

# Package ``__init__`` binds ``SettingsService`` as the class, which shadows
# the submodule. Tests patch that module's globals.
_ss = importlib.import_module("back.objects.domain.settings.SettingsService")

logger = get_logger(__name__)


class GraphEngineLakebaseSettings:
    """Lakebase connection probes, listing, provision, and object admin."""

    @staticmethod
    def graph_engine_lakebase_health_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Probe Lakebase Postgres for the configured graph schema (read-only).

        Uses ``graph_engine_config.lakebase.database`` (optional) and ``schema``
        from registry global config.
        """
        import os

        from back.core.databricks import get_lakebase_auth
        from back.core.databricks.lakebase import BranchLakebaseAuth
        from back.core.graphdb.engine_config import lakebase_section
        from back.core.graphdb.lakebase.LakebaseBase import (
            default_schema,
            resolve_postgres_database_override,
            validate_graph_schema,
        )

        # Resolve graph engine config first so we can pick the right auth.
        try:
            _, host, token, registry_cfg = SettingsService._resolve_context(
                session_mgr, settings
            )
            _ss.global_config_service.load(host, token, registry_cfg, force=True)
            gcfg = lakebase_section(
                _ss.global_config_service.get_graph_engine_config(
                    host, token, registry_cfg
                )
            )
        except Exception as exc:
            logger.warning("graph_engine_lakebase_health context failed: %s", exc)
            raise InfrastructureError(
                "Could not load graph engine config", detail=str(exc)
            ) from exc

        db_override = ""
        schema_raw = ""
        branch_path = ""
        if isinstance(gcfg, dict):
            db_override = resolve_postgres_database_override(gcfg)
            schema_raw = (gcfg.get("schema") or "").strip()
            branch_path = (gcfg.get("lakebase_branch") or "").strip()

        # Use the same auth selection as GraphDBFactory: BranchLakebaseAuth
        # when lakebase_branch is configured, else the bound auth.
        if branch_path:
            auth = BranchLakebaseAuth(branch_path, db_override)
        else:
            auth = get_lakebase_auth()

        port = int(os.environ.get("PGPORT", "5432") or "5432")
        bound_db = os.environ.get("PGDATABASE", "").strip()
        try:
            host_display = auth.host
        except Exception:  # noqa: BLE001
            host_display = os.environ.get("PGHOST", "") or os.environ.get("LAKEBASE_PROJECT", "")

        if not auth.is_available:
            raise ValidationError(
                "Lakebase not available — set LAKEBASE_PROJECT + LAKEBASE_BRANCH + PGUSER "
                "in .env (local), or bind a Databricks App postgres resource (deployed)."
            )

        try:
            schema = validate_graph_schema(schema_raw or default_schema())
        except ValueError as exc:
            raise ValidationError(str(exc)) from exc

        registry_db = bound_db or get_lakebase_auth().database  # PGDATABASE → registry store
        graph_db = db_override or registry_db                    # graph_engine_config.database

        try:
            from back.core.graphdb.lakebase.pool import _require_psycopg

            psycopg, _ = _require_psycopg()
        except ImportError as exc:
            raise InfrastructureError(
                "Lakebase backend not installed (missing psycopg)",
                detail=str(exc),
            ) from exc

        kwargs = auth.kwargs(application_name="ontobricks-graph-health")
        kwargs["dbname"] = graph_db

        schema_exists = False
        table_count = 0
        try:
            with psycopg.connect(**kwargs) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_namespace
                            WHERE nspname = %s
                        )
                        """,
                        (schema,),
                    )
                    row = cur.fetchone()
                    schema_exists = bool(row[0]) if row else False
                    if schema_exists:
                        cur.execute(
                            """
                            SELECT COUNT(*)
                            FROM pg_catalog.pg_class c
                            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                            WHERE n.nspname = %s AND c.relkind = 'r'
                            """,
                            (schema,),
                        )
                        row2 = cur.fetchone()
                        table_count = int(row2[0]) if row2 else 0
        except Exception as exc:
            # A failed connection / missing database is a configuration
            # condition, not a server error — return a graceful result the UI
            # renders as a warning instead of surfacing a scary 502.
            logger.warning("graph_engine_lakebase_health probe failed: %s", exc)
            return {
                "success": False,
                "reason": "probe_failed",
                "message": f"Lakebase health probe failed: {exc}",
                "host": host_display,
                "port": port,
                "registry_database": registry_db,
                "graph_database": graph_db,
                "graph_schema": schema,
                "schema_exists": False,
                "tables_in_schema": 0,
            }

        out: Dict[str, Any] = {
            "success": True,
            "reason": "ok",
            "host": host_display,
            "port": port,
            "registry_database": registry_db,
            "graph_database": graph_db,
            "graph_schema": schema,
            "schema_exists": schema_exists,
            "tables_in_schema": table_count,
        }
        if schema_exists:
            out["message"] = (
                f"Graph DB ready: database={graph_db!r}, schema={schema!r} "
                f"({table_count} table(s)). Registry database: {registry_db!r}."
            )
        else:
            out["message"] = (
                f"Connected to graph database {graph_db!r}, but schema {schema!r} "
                "does not exist yet — run a Knowledge Graph build or create the schema. "
                f"Registry database: {registry_db!r}."
            )
        return out

    @staticmethod
    def graph_engine_lakebase_projects_result(
        _session_mgr: SessionManager,
        _settings: Settings,
    ) -> Dict[str, Any]:
        """List all Lakebase Autoscaling projects visible in the workspace."""
        try:
            from databricks.sdk import WorkspaceClient

            from back.core.databricks.lakebase.LakebaseProjectService import (
                LakebaseProjectService,
            )

            w = WorkspaceClient()
            api = getattr(w, "api_client", None)
            if api is None or not hasattr(api, "do"):
                raise InfrastructureError("Databricks SDK api_client unavailable")
            raw = LakebaseProjectService.list_projects(api)
            projects = []
            for p in raw:
                name = p.get("name") or ""
                if not name:
                    continue
                short = name.rsplit("/", 1)[-1]
                status = p.get("status") or {}
                projects.append({
                    "name": name,
                    "short_name": short,
                    "state": status.get("state") or "",
                })
            return {"success": True, "projects": projects}
        except OntoBricksError:
            raise
        except Exception as exc:
            logger.warning("graph_engine_lakebase_projects failed: %s", exc)
            raise InfrastructureError(
                "list Lakebase projects failed", detail=str(exc)
            ) from exc

    @staticmethod
    def graph_engine_lakebase_branches_result(
        project_path: str,
        _session_mgr: SessionManager,
        _settings: Settings,
    ) -> Dict[str, Any]:
        """List branches for a Lakebase Autoscaling project."""
        if not project_path:
            raise ValidationError("project_path is required")
        try:
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            api = getattr(w, "api_client", None)
            if api is None or not hasattr(api, "do"):
                raise InfrastructureError("Databricks SDK api_client unavailable")
            # Normalise: accept both short name and full resource path
            if not project_path.startswith("projects/"):
                project_path = f"projects/{project_path}"
            raw = (
                api.do("GET", f"/api/2.0/postgres/{project_path}/branches") or {}
            ).get("branches") or []
            branches = []
            for b in raw:
                name = b.get("name") or ""
                if not name:
                    continue
                short = name.rsplit("/", 1)[-1]
                status = b.get("status") or {}
                branches.append({
                    "name": name,
                    "short_name": short,
                    "state": status.get("state") or "",
                })
            return {"success": True, "branches": branches}
        except OntoBricksError:
            raise
        except Exception as exc:
            logger.warning("graph_engine_lakebase_branches failed: %s", exc)
            raise InfrastructureError(
                "list Lakebase branches failed", detail=str(exc)
            ) from exc

    @staticmethod
    def graph_engine_lakebase_pg_databases_result(
        branch_path: str,
        _session_mgr: SessionManager,
        _settings: Settings,
    ) -> Dict[str, Any]:
        """List Postgres databases on a Lakebase branch endpoint."""
        if not branch_path:
            raise ValidationError("branch_path is required")
        try:
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            api = getattr(w, "api_client", None)
            if api is None or not hasattr(api, "do"):
                raise InfrastructureError("Databricks SDK api_client unavailable")
            raw = (
                api.do("GET", f"/api/2.0/postgres/{branch_path}/databases") or {}
            ).get("databases") or []
            databases = []
            for db in raw:
                status = db.get("status") or {}
                pg_name = status.get("postgres_database") or ""
                if pg_name:
                    databases.append(pg_name)
            return {"success": True, "databases": sorted(databases)}
        except OntoBricksError:
            raise
        except Exception as exc:
            logger.warning("graph_engine_lakebase_pg_databases failed: %s", exc)
            raise InfrastructureError(
                "list Lakebase Postgres databases failed", detail=str(exc)
            ) from exc

    @staticmethod
    def graph_engine_lakebase_pg_schemas_result(
        database: str,
        _session_mgr: SessionManager,
        _settings: Settings,
        branch_path: str = "",
    ) -> Dict[str, Any]:
        """List Postgres schemas in the graph Lakebase database.

        Uses :meth:`_graph_engine_auth` so it always connects to the correct
        graph project (BranchLakebaseAuth when configured, bound auth otherwise).
        ``branch_path`` / ``database`` from the form take priority over saved config.
        """
        try:
            from back.core.graphdb.lakebase.pool import _require_psycopg

            auth, effective_db = SettingsService._graph_engine_auth(
                _session_mgr, _settings,
                form_branch_path=branch_path,
                form_database=database,
            )
            if not auth.is_available:
                raise ValidationError(
                    "Lakebase resource not bound (LAKEBASE_PROJECT/LAKEBASE_BRANCH/PGUSER missing)"
                )
            psycopg, _ = _require_psycopg()
            kwargs = auth.kwargs(application_name="ontobricks-schema-list")
            if effective_db:
                kwargs["dbname"] = effective_db
            with psycopg.connect(**kwargs) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT nspname FROM pg_catalog.pg_namespace
                        WHERE nspname NOT LIKE 'pg_%'
                          AND nspname NOT IN ('information_schema')
                        ORDER BY nspname
                        """
                    )
                    schemas = [row[0] for row in cur.fetchall()]
            return {"success": True, "schemas": schemas}
        except OntoBricksError:
            raise
        except ImportError as exc:
            raise InfrastructureError(
                "Lakebase backend not installed (missing psycopg)",
                detail=str(exc),
            ) from exc
        except Exception as exc:
            logger.warning("graph_engine_lakebase_pg_schemas failed: %s", exc)
            raise InfrastructureError(
                "list Lakebase Postgres schemas failed", detail=str(exc)
            ) from exc

    @staticmethod
    def graph_engine_lakebase_provision_result(
        params: Dict[str, Any],
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Provision a brand-new Lakebase graph DB end-to-end (admin only).

        Creates the Lakebase instance/project + Postgres database + graph
        schema and grants ``CAN_USE`` on the project plus schema privileges
        to the app + MCP service principals — the in-app equivalent of
        ``scripts/setup-lakebase.sh`` + ``scripts/bootstrap-lakebase-perms.sh``.

        Runs in a worker thread tracked by the shared :class:`TaskManager`;
        the route returns a ``task_id`` the UI polls via ``GET /tasks/{id}``.
        """
        import threading

        from back.core.graphdb.lakebase.LakebaseBase import (
            default_schema,
            validate_graph_schema,
        )
        from back.core.graphdb.lakebase.LakebaseGraphProvisioner import (
            DEFAULT_BRANCH,
            DEFAULT_CAPACITY,
            LakebaseGraphProvisioner,
            provision_steps,
        )
        from back.core.task_manager import get_task_manager

        SettingsService.require_admin_error(email, user_token, session_mgr, settings)

        name = (params.get("name") or "").strip()
        if not name:
            raise ValidationError("A Lakebase instance/project name is required.")
        database = (params.get("database") or "").strip()
        if not database:
            raise ValidationError("A Postgres database name is required.")
        capacity = (params.get("capacity") or DEFAULT_CAPACITY).strip()
        branch = (params.get("branch") or DEFAULT_BRANCH).strip()
        try:
            schema = validate_graph_schema(
                (params.get("schema") or "").strip() or default_schema()
            )
        except ValueError as exc:
            raise ValidationError(str(exc)) from exc

        pg_user = os.environ.get("PGUSER", "").strip()
        if not pg_user:
            raise ValidationError(
                "PGUSER is not set — the provisioning button only works when the "
                "app is bound to Lakebase (Databricks App mode)."
            )

        # App service-principal grants only apply inside Databricks Apps.
        # Local development authenticates as PGUSER (the developer identity)
        # and must not infer or grant permissions to deployed app principals.
        app_mode = _ss.is_databricks_app()
        app_names: List[str] = []
        if app_mode:
            app_name = (settings.ontobricks_app_name or "").strip()
            mcp_app_name = _ss.resolve_mcp_app_name(
                app_name, explicit=(params.get("mcp_app_name") or "").strip()
            )
            for candidate in (app_name, mcp_app_name):
                if candidate and candidate not in app_names:
                    app_names.append(candidate)
            if not app_names:
                raise ValidationError(
                    "Could not determine the app name to grant — "
                    "set ONTOBRICKS_APP_NAME."
                )

        # Resolve sync mode + UC catalog from the saved engine config so the
        # managed_synced UC grant targets the right catalog.
        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        _ss.global_config_service.load(host, token, registry_cfg, force=True)
        saved_cfg = _ss.global_config_service.get_graph_engine_config(
            host, token, registry_cfg
        )
        from back.core.graphdb.engine_config import lakebase_section

        saved_cfg = lakebase_section(saved_cfg)
        sync_mode = (saved_cfg.get("sync_mode") or "app_managed").strip()
        uc_catalog = ""
        if (
            app_mode
            and bool(params.get("grant_uc_catalog"))
            and sync_mode == "managed_synced"
        ):
            uc_catalog = (saved_cfg.get("sync_uc_catalog") or "").strip()

        tm = get_task_manager()
        task = tm.create_task(
            name="Lakebase Graph DB Provision",
            task_type="lakebase_provision",
            steps=provision_steps(
                grant_uc=bool(uc_catalog),
                grant_app_permissions=app_mode,
            ),
        )

        def run_provision() -> None:
            LakebaseGraphProvisioner(
                tm=tm,
                task_id=task.id,
                name=name,
                capacity=capacity,
                branch=branch,
                database=database,
                schema=schema,
                app_names=app_names,
                sync_mode=sync_mode,
                uc_catalog=uc_catalog,
                pg_user=pg_user,
                operator_email=email,
            ).run()

        thread = threading.Thread(target=run_provision, daemon=True)
        thread.start()

        return {
            "success": True,
            "task_id": task.id,
            "message": "Lakebase graph DB provisioning started",
        }

    @staticmethod
    def _graph_engine_database(
        session_mgr: SessionManager,
        settings: Any,
    ) -> str:
        """Return the Lakebase ``database`` field from the saved graph engine config.

        Returns ``""`` on any failure so callers fall back gracefully.
        """
        try:
            from back.core.graphdb.engine_config import lakebase_section

            domain = _ss.get_domain(session_mgr)
            host, token = _ss.get_databricks_host_and_token(domain, settings)
            registry_cfg = _ss.RegistryCfg.from_domain(domain, settings).as_dict()
            ge = lakebase_section(
                _ss.global_config_service.get_graph_engine_config(host, token, registry_cfg)
            )
            return (ge.get("database") or "").strip()
        except Exception:  # noqa: BLE001
            return ""

    @staticmethod
    def _graph_engine_auth(
        session_mgr: SessionManager,
        settings: Any,
        form_branch_path: str = "",
        form_database: str = "",
    ):
        """Return the correct Lakebase auth for graph DB operations.

        Mirrors the auth selection in :class:`GraphDBFactory._create_lakebase`:

        * ``form_branch_path`` — explicit branch path from the request (e.g. from a
          Connection-tab form field).  Takes priority when non-empty.
        * Saved ``graph_engine_config.lakebase_branch`` — used when the form did not
          supply a branch path.
        * Bound auth (PGHOST) — fallback when no branch is configured anywhere.

        Also returns the effective database name (form_database → saved config → "").
        Returns ``(auth, database)``; raises on irrecoverable failures.
        """
        from back.core.databricks import get_lakebase_auth
        from back.core.databricks.lakebase import BranchLakebaseAuth

        branch_path = form_branch_path.strip()
        database = form_database.strip()

        # Load saved config to fill gaps not supplied by the form.
        try:
            from back.core.graphdb.engine_config import lakebase_section

            domain = _ss.get_domain(session_mgr)
            host, token = _ss.get_databricks_host_and_token(domain, settings)
            registry_cfg = _ss.RegistryCfg.from_domain(domain, settings).as_dict()
            ge = lakebase_section(
                _ss.global_config_service.get_graph_engine_config(host, token, registry_cfg)
            )
            if not branch_path:
                branch_path = (ge.get("lakebase_branch") or "").strip()
            if not database:
                database = (ge.get("database") or "").strip()
        except Exception:  # noqa: BLE001
            pass  # fall through to bound auth

        if branch_path:
            return BranchLakebaseAuth(branch_path, database), database
        return get_lakebase_auth(), database

    @staticmethod
    def _lakebase_kwargs_for_branch(
        branch_path: str,
        database: str,
        application_name: str,
    ) -> Dict[str, Any]:
        """Resolve psycopg connect kwargs directly from a Lakebase branch resource path.

        Uses the Databricks API to find the primary endpoint for ``branch_path``
        (format ``projects/<proj>/branches/<branch>``), mints a fresh JWT, and
        returns kwargs ready to pass to ``psycopg.connect()``.
        Raises on any resolution failure so the caller can return a clean error.
        """
        import os

        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        api = getattr(w, "api_client", None)
        if api is None or not hasattr(api, "do"):
            raise RuntimeError("Databricks SDK api_client unavailable")

        endpoints = (
            api.do("GET", f"/api/2.0/postgres/{branch_path}/endpoints") or {}
        ).get("endpoints") or []

        host = ""
        endpoint_resource = ""
        for ep in endpoints:
            h = ((ep.get("status") or {}).get("hosts") or {}).get("host", "").strip()
            if h:
                host = h
                endpoint_resource = ep.get("name") or ""
                break

        if not host:
            raise RuntimeError(
                f"No active endpoint found for branch path {branch_path!r}"
            )

        token_resp = api.do(
            "POST",
            "/api/2.0/postgres/credentials",
            body={"endpoint": endpoint_resource},
        ) or {}
        jwt = token_resp.get("token", "")
        if not jwt:
            raise RuntimeError(
                f"Failed to mint Lakebase JWT for endpoint {endpoint_resource!r}"
            )

        pguser = os.environ.get("PGUSER", "").strip()
        if not pguser:
            raise RuntimeError(
                "PGUSER is not set — required for Lakebase psycopg connections"
            )

        kwargs: Dict[str, Any] = {
            "host": host,
            "port": int(os.environ.get("PGPORT", "5432")),
            "user": pguser,
            "password": jwt,
            "dbname": database or "postgres",
            "sslmode": "require",
            "connect_timeout": 10,
            "application_name": application_name,
        }
        return kwargs

    @staticmethod
    def graph_engine_lakebase_objects_result(
        database: str,
        branch_path: str,
        _session_mgr: SessionManager,
        _settings: Settings,
    ) -> Dict[str, Any]:
        """List all user schemas, tables and views in the graph Lakebase database.

        Uses :meth:`_graph_engine_auth` to resolve the correct Lakebase host:
        saved ``graph_engine_config.lakebase_branch`` (BranchLakebaseAuth) when
        configured, otherwise the bound Lakebase (registry host).
        The ``branch_path`` / ``database`` form params take priority over saved
        config when provided.
        """
        try:
            from back.core.graphdb.lakebase.pool import _require_psycopg

            psycopg, _ = _require_psycopg()

            auth, effective_db = SettingsService._graph_engine_auth(
                _session_mgr, _settings,
                form_branch_path=branch_path,
                form_database=database,
            )
            if not auth.is_available:
                raise ValidationError(
                    "Lakebase not available — configure graph_engine_config.lakebase_branch "
                    "or bind a Lakebase resource."
                )
            kwargs = auth.kwargs(application_name="ontobricks-obj-list")
            if effective_db:
                kwargs["dbname"] = effective_db
            with psycopg.connect(**kwargs) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT current_user")
                    current_user = (cur.fetchone() or ("",))[0]

                    cur.execute(
                        """
                        SELECT nspname,
                               pg_catalog.pg_get_userbyid(nspowner) AS owner
                        FROM pg_catalog.pg_namespace
                        WHERE nspname NOT LIKE 'pg_%%'
                          AND SUBSTRING(nspname, 1, 2) != '__'
                          AND nspname NOT IN ('information_schema', 'public')
                          AND (
                              pg_catalog.pg_get_userbyid(nspowner) = current_user
                              OR has_schema_privilege(current_user, nspname, 'USAGE')
                          )
                        ORDER BY nspname
                        """
                    )
                    schemas = [{"name": r[0], "owner": r[1]} for r in cur.fetchall()]

                    # Include all schemas where the SP has USAGE (covers schemas
                    # created by the human deployer via bootstrap, where _sync and
                    # __app tables land during builds).
                    owned_schema_names = tuple(s["name"] for s in schemas)
                    if owned_schema_names:
                        cur.execute(
                            """
                            SELECT t.schemaname,
                                   t.tablename,
                                   pg_catalog.pg_get_userbyid(c.relowner) AS owner
                            FROM pg_catalog.pg_tables t
                            JOIN pg_catalog.pg_class c
                                 ON c.relname = t.tablename
                            JOIN pg_catalog.pg_namespace n
                                 ON n.oid = c.relnamespace
                                AND n.nspname = t.schemaname
                            WHERE t.schemaname = ANY(%s)
                            ORDER BY t.schemaname, t.tablename
                            """,
                            (list(owned_schema_names),),
                        )
                    else:
                        cur.execute("SELECT NULL, NULL, NULL WHERE FALSE")
                    tables = [
                        {"schema": r[0], "name": r[1], "owner": r[2]}
                        for r in cur.fetchall()
                    ]

                    if owned_schema_names:
                        cur.execute(
                            """
                            SELECT v.schemaname,
                                   v.viewname,
                                   pg_catalog.pg_get_userbyid(c.relowner) AS owner
                            FROM pg_catalog.pg_views v
                            JOIN pg_catalog.pg_class c
                                 ON c.relname = v.viewname
                            JOIN pg_catalog.pg_namespace n
                                 ON n.oid = c.relnamespace
                                AND n.nspname = v.schemaname
                            WHERE v.schemaname = ANY(%s)
                            ORDER BY v.schemaname, v.viewname
                            """,
                            (list(owned_schema_names),),
                        )
                    else:
                        cur.execute("SELECT NULL, NULL, NULL WHERE FALSE")
                    views = [
                        {"schema": r[0], "name": r[1], "owner": r[2]}
                        for r in cur.fetchall()
                    ]

            rcfg = _ss.RegistryCfg.from_session(_session_mgr, _settings)
            return {
                "success": True,
                "current_user": current_user,
                "registry_schema": rcfg.lakebase_schema or "ontobricks_registry",
                "schemas": schemas,
                "tables": tables,
                "views": views,
            }
        except OntoBricksError:
            raise
        except ImportError as exc:
            raise InfrastructureError(
                "Lakebase backend not installed (missing psycopg)",
                detail=str(exc),
            ) from exc
        except Exception as exc:
            logger.warning("graph_engine_lakebase_objects failed: %s", exc)
            raise InfrastructureError(
                "list Lakebase database objects failed", detail=str(exc)
            ) from exc

    @staticmethod
    def graph_engine_lakebase_sync_objects_result(
        database: str,
        branch_path: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """List UC Delta tables in the configured graph schema, plus Lakeflow state.

        Approach:

        1. Resolve ``sync_uc_catalog`` and ``uc_schema`` from engine config,
           falling back to the registry catalog when the former is unset.
        2. Call the UC REST API (``/api/2.1/unity-catalog/tables``) to enumerate
           every table/view in that schema — works regardless of sync_mode and
           requires no SQL warehouse.
        3. When ``sync_mode == managed_synced``, probe every ``_sync`` table via
           the Lakebase synced-tables API (parallel, max 4 workers) and attach
           ``state``, ``pipeline_id``, and ``source_table`` to the result.
        """
        import concurrent.futures

        try:
            from back.core.graphdb.engine_config import lakebase_section

            _, host, token, registry_cfg = SettingsService._resolve_context(
                session_mgr, settings
            )
            gcfg = lakebase_section(
                _ss.global_config_service.get_graph_engine_config(host, token, registry_cfg)
            )
            sync_mode = gcfg.get("sync_mode", "app_managed")

            # ── Resolve UC catalog / schema ───────────────────────────────
            sync_uc_catalog = (gcfg.get("sync_uc_catalog") or "").strip()
            sync_uc_schema_override = (gcfg.get("sync_uc_schema") or "").strip()
            graph_schema = (gcfg.get("schema") or "").strip()

            # Fall back to registry catalog when sync_uc_catalog is not set
            if not sync_uc_catalog:
                rcfg = _ss.RegistryCfg.from_session(session_mgr, settings)
                sync_uc_catalog = (rcfg.catalog or "").strip()

            uc_schema = sync_uc_schema_override or graph_schema or ""

            if not sync_uc_catalog or not uc_schema:
                return {
                    "success": True,
                    "sync_mode": sync_mode,
                    "uc_tables": [],
                    "message": (
                        "UC catalog or schema not configured "
                        "(set graph_engine_config.sync_uc_catalog and schema)"
                    ),
                }

            # ── List UC tables via REST API (no warehouse required) ───────
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            api = getattr(w, "api_client", None)
            if api is None or not hasattr(api, "do"):
                raise InfrastructureError("Databricks SDK api_client unavailable")

            raw = api.do(
                "GET",
                "/api/2.1/unity-catalog/tables",
                query={"catalog_name": sync_uc_catalog, "schema_name": uc_schema},
            ) or {}
            uc_raw_tables = raw.get("tables", []) or []

            # ── For managed_synced: probe Lakeflow state per _sync table ──
            lk_states: Dict[str, Any] = {}
            if sync_mode == "managed_synced" and uc_raw_tables:
                from back.core.graphdb.lakebase.SyncedTableManager import (
                    SyncedTableManager,
                    _to_dict,
                )

                mgr = SyncedTableManager()

                def _extract_source_table(synced: Any) -> str:
                    spec = getattr(synced, "spec", None)
                    if spec is not None:
                        val = getattr(spec, "source_table_full_name", "") or ""
                        if val:
                            return str(val)
                    d = _to_dict(synced)
                    return str(d.get("spec", {}).get("source_table_full_name", "") or "")

                def _probe_lk(tbl_raw: Dict[str, Any]) -> None:
                    name = tbl_raw.get("name", "")
                    if not name.endswith("_sync"):
                        return
                    full_name = (
                        tbl_raw.get("full_name")
                        or f"{sync_uc_catalog}.{uc_schema}.{name}"
                    )
                    try:
                        synced = mgr.get(full_name)
                        if synced is None:
                            lk_states[full_name] = {
                                "state": "NOT_FOUND",
                                "pipeline_id": "",
                                "source_table": "",
                            }
                        else:
                            spec = getattr(synced, "spec", None)
                            lk_states[full_name] = {
                                "state": SyncedTableManager._extract_state(synced) or "UNKNOWN",
                                "pipeline_id": SyncedTableManager._extract_pipeline_id(synced),
                                "source_table": _extract_source_table(synced),
                            }
                    except Exception as probe_exc:  # noqa: BLE001
                        lk_states[full_name] = {
                            "state": "ERROR",
                            "pipeline_id": "",
                            "source_table": "",
                            "error": str(probe_exc)[:300],
                        }

                with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                    list(executor.map(_probe_lk, uc_raw_tables))

            # ── Build response ────────────────────────────────────────────
            uc_tables = []
            for t in uc_raw_tables:
                name = t.get("name", "")
                full_name = t.get("full_name") or f"{sync_uc_catalog}.{uc_schema}.{name}"
                table_type = str(t.get("table_type", "") or "")
                lk = lk_states.get(full_name, {})
                uc_tables.append({
                    "name": name,
                    "full_name": full_name,
                    "table_type": table_type,
                    "is_sync": name.endswith("_sync"),
                    "state": lk.get("state", ""),
                    "pipeline_id": lk.get("pipeline_id", ""),
                    "source_table": lk.get("source_table", ""),
                    "error": lk.get("error", ""),
                })

            return {
                "success": True,
                "sync_mode": sync_mode,
                "uc_catalog": sync_uc_catalog,
                "uc_schema": uc_schema,
                "uc_tables": uc_tables,
            }
        except OntoBricksError:
            raise
        except Exception as exc:
            logger.warning("graph_engine_lakebase_sync_objects failed: %s", exc)
            raise InfrastructureError(
                "list Lakebase sync objects failed", detail=str(exc)
            ) from exc

    @staticmethod
    def graph_engine_lakebase_drop_object_result(
        kind: str,
        schema: str,
        name: str,
        database: str,
        branch_path: str,
        _session_mgr: SessionManager,
        _settings: Settings,
    ) -> Dict[str, Any]:
        """Drop a Postgres schema, table or view in the connected Lakebase database.

        ``kind`` must be one of ``schema``, ``table``, ``view``.
        Schemas are dropped with CASCADE.  Uses ``branch_path`` when provided
        so the drop targets the form's current connection, not the saved config.
        """
        allowed_kinds = {"schema", "table", "view"}
        if kind not in allowed_kinds:
            raise ValidationError(
                f"kind must be one of {allowed_kinds}, got: {kind!r}"
            )

        def _q(ident: str) -> str:
            return '"' + ident.replace('"', '""') + '"'

        if kind == "schema":
            ddl = f"DROP SCHEMA IF EXISTS {_q(name)} CASCADE"
        elif kind == "table":
            if not schema:
                raise ValidationError("schema is required for kind=table")
            ddl = f"DROP TABLE IF EXISTS {_q(schema)}.{_q(name)} CASCADE"
        else:
            if not schema:
                raise ValidationError("schema is required for kind=view")
            ddl = f"DROP VIEW IF EXISTS {_q(schema)}.{_q(name)} CASCADE"

        try:
            from back.core.graphdb.lakebase.pool import _require_psycopg

            psycopg, _ = _require_psycopg()

            auth, effective_db = SettingsService._graph_engine_auth(
                _session_mgr, _settings,
                form_branch_path=branch_path,
                form_database=database,
            )
            if not auth.is_available:
                raise ValidationError(
                    "Lakebase not available — configure graph_engine_config.lakebase_branch "
                    "or bind a Lakebase resource."
                )
            kwargs = auth.kwargs(application_name="ontobricks-obj-drop")
            if effective_db:
                kwargs["dbname"] = effective_db

            with psycopg.connect(**kwargs) as conn:
                with conn.cursor() as cur:
                    cur.execute(ddl)
            return {"success": True, "message": f"Dropped {kind}: {ddl}"}
        except OntoBricksError:
            raise
        except ImportError as exc:
            raise InfrastructureError(
                "Lakebase backend not installed (missing psycopg)",
                detail=str(exc),
            ) from exc
        except Exception as exc:
            logger.warning("graph_engine_lakebase_drop_object failed: %s", exc)
            raise InfrastructureError(
                "Lakebase drop object failed", detail=str(exc)
            ) from exc

    @staticmethod
    def graph_engine_lakebase_pg_roles_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """List Postgres roles on the graph Lakebase branch and overlay app-user status.

        Returns::

            {
                "success": True,
                "branch_path": "projects/.../branches/...",
                "roles": [
                    {"email": "user@example.com", "role_id": "...",
                     "has_superuser": True/False},
                    ...
                ],
                "app_users": [
                    {"email": "user@example.com", "display_name": "..."},
                    ...
                ],
            }
        """
        from databricks.sdk import WorkspaceClient

        auth, _ = SettingsService._graph_engine_auth(session_mgr, settings)
        if not auth.is_available:
            raise ValidationError(
                "Lakebase not available — configure graph_engine_config.lakebase_branch "
                "or bind a Lakebase resource."
            )
        branch_path = auth.branch_path
        if not branch_path:
            raise ValidationError("Could not resolve Lakebase branch path for Postgres roles API")

        w = WorkspaceClient()
        api = w.api_client
        raw = (api.do("GET", f"/api/2.0/postgres/{branch_path}/roles") or {})
        existing = raw.get("roles") or []

        roles = []
        for r in existing:
            status = r.get("status") or {}
            pg_role = str(status.get("postgres_role") or "").lower()
            if not pg_role:
                continue
            role_id = (r.get("name") or "").rsplit("/", 1)[-1]
            has_superuser = "DATABRICKS_SUPERUSER" in (status.get("membership_roles") or [])
            roles.append({"email": pg_role, "role_id": role_id, "has_superuser": has_superuser})

        # App users (best-effort — may be empty when SP has no ACL read access)
        app_users: List[Dict[str, Any]] = []
        try:
            _, host, token, _ = SettingsService._resolve_context(session_mgr, settings)
            app_name = settings.ontobricks_app_name
            principals = _ss.permission_service.list_app_principals(host, token, app_name)
            for u in principals.get("users", []):
                email = (u.get("email") or "").strip()
                if email:
                    app_users.append({
                        "email": email,
                        "display_name": u.get("display_name") or email,
                    })
        except Exception:  # noqa: BLE001
            pass

        return {
            "success": True,
            "branch_path": branch_path,
            "roles": roles,
            "app_users": app_users,
        }

    @staticmethod
    def graph_engine_lakebase_grant_superuser_result(
        user_email: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Ensure *user_email* has a Postgres OAuth role and DATABRICKS_SUPERUSER membership.

        Mirrors the logic of ``LakebaseGraphProvisioner._ensure_superuser_role``
        but operates on the currently configured graph Lakebase branch.
        Idempotent — re-granting an existing superuser is a no-op.
        """
        import time
        from databricks.sdk import WorkspaceClient

        user_email = (user_email or "").strip()
        if not user_email:
            raise ValidationError("user_email is required")

        auth, _ = SettingsService._graph_engine_auth(session_mgr, settings)
        if not auth.is_available:
            raise ValidationError(
                "Lakebase not available — configure graph_engine_config.lakebase_branch "
                "or bind a Lakebase resource."
            )
        branch_path = auth.branch_path
        if not branch_path:
            raise ValidationError("Could not resolve Lakebase branch path for Postgres roles API")

        w = WorkspaceClient()
        api = w.api_client

        existing = (api.do("GET", f"/api/2.0/postgres/{branch_path}/roles") or {}).get("roles") or []
        role_map: Dict[str, Dict[str, Any]] = {}
        for r in existing:
            status = r.get("status") or {}
            pg_role = str(status.get("postgres_role") or "").lower()
            if not pg_role:
                continue
            role_map[pg_role] = {
                "role_id": (r.get("name") or "").rsplit("/", 1)[-1],
                "has_superuser": "DATABRICKS_SUPERUSER" in (status.get("membership_roles") or []),
            }

        email_lower = user_email.lower()
        existing_role = role_map.get(email_lower)

        if existing_role and existing_role.get("has_superuser"):
            return {"success": True, "message": f"{user_email} already has DATABRICKS_SUPERUSER"}

        role_id: str = (existing_role or {}).get("role_id", "")

        if not role_id:
            op = (
                api.do(
                    "POST",
                    f"/api/2.0/postgres/{branch_path}/roles",
                    body={
                        "spec": {
                            "identity_type": "USER",
                            "postgres_role": user_email,
                            "auth_method": "LAKEBASE_OAUTH_V1",
                        }
                    },
                )
                or {}
            )
            op_name = op.get("name") or ""
            parts = op_name.split("/")
            if "roles" in parts:
                idx = parts.index("roles")
                if idx + 1 < len(parts) and parts[idx + 1] != "operations":
                    role_id = parts[idx + 1]
            if not role_id:
                raise InfrastructureError(
                    f"Could not create Postgres role for {user_email}",
                    detail=f"LRO name: {op_name!r}",
                )
            time.sleep(3.0)

        api.do(
            "PATCH",
            f"/api/2.0/postgres/{branch_path}/roles/{role_id}?update_mask=spec.membership_roles",
            body={"spec": {"membership_roles": ["DATABRICKS_SUPERUSER"]}},
        )
        logger.info("Granted DATABRICKS_SUPERUSER to %s on %s", user_email, branch_path)
        return {"success": True, "message": f"DATABRICKS_SUPERUSER granted to {user_email}"}
