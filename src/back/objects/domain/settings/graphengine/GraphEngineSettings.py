"""Graph engine Settings: Lakehouse health/objects and config get/set.

Lakebase lives in :class:`GraphEngineLakebaseSettings`; Neo4j in
:class:`GraphEngineNeo4jSettings`. Callers keep ``SettingsService.*``.
"""

from __future__ import annotations

import importlib
from typing import Any, Dict, List, Optional, Tuple

from back.core.errors import (
    InfrastructureError,
    OntoBricksError,
    ValidationError,
)
from shared.config.settings import Settings
from back.core.logging import get_logger
from back.objects.session import (
    SessionManager,
)
from back.objects.domain.settings.SettingsService import SettingsService

# Package ``__init__`` binds ``SettingsService`` as the class, which shadows
# the submodule for ``import ...SettingsService as _ss``. Tests patch this
# module's globals (``global_config_service``, ``resolve_app_registry_context``).
_ss = importlib.import_module("back.objects.domain.settings.SettingsService")

logger = get_logger(__name__)


class GraphEngineSettings:
    """Connection-tab graph engine config, health, and object listing."""

    @staticmethod
    def _mirror_graph_engine_to_domain_registry(
        session_mgr: SessionManager,
        *,
        config: Optional[Dict[str, Any]] = None,
        delta_warehouse_id: Optional[str] = None,
    ) -> None:
        """Copy graph DB *connection* settings into ``domain.settings['registry']``.

        Authoritative persistence is :class:`GlobalConfigService` via
        :meth:`RegistryStore.save_global_config` (Volume ``.global_config.json``
        or Lakebase ``global_config`` JSONB). Mirroring keeps the domain JSON
        export aligned with the catalog/schema/volume block for operators.

        The backend *selection* is no longer mirrored — it now lives per-domain
        in ``DomainSession.info['graph_backend']``. Lakehouse warehouse lives in
        ``graph_engine_config.lakehouse.warehouse_id`` only.
        """
        if config is None and delta_warehouse_id is None:
            return
        try:
            from back.core.graphdb.engine_config import normalize_graph_engine_config

            domain = _ss.get_domain(session_mgr)
            reg = domain.settings.setdefault("registry", {})
            if config is not None:
                reg["graph_engine_config"] = normalize_graph_engine_config(config)
            if delta_warehouse_id is not None:
                gec = normalize_graph_engine_config(
                    reg.get("graph_engine_config")
                    if isinstance(reg.get("graph_engine_config"), dict)
                    else {}
                )
                lh = dict(gec.get("lakehouse") or {})
                lh["warehouse_id"] = (delta_warehouse_id or "").strip()
                gec["lakehouse"] = lh
                reg["graph_engine_config"] = gec
            reg.pop("delta_warehouse_id", None)
            domain.save()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Could not mirror graph engine fields to domain.settings.registry: %s",
                exc,
            )

    @staticmethod
    def triple_store_databricks_health_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.core.graphdb.delta.health import schema_permission_summary

        host, token, registry_cfg = _ss.resolve_app_registry_context(settings)
        reg = registry_cfg if isinstance(registry_cfg, dict) else {}
        catalog = (reg.get("catalog") or "").strip()
        schema = (reg.get("schema") or "").strip()
        storage_location = f"{catalog}.{schema}" if catalog and schema else ""

        if not storage_location:
            return {
                "success": True,
                "registry_configured": False,
                "registry_catalog": catalog,
                "registry_schema": schema,
                "storage_location": "",
                "principal": "",
                "accessible": False,
                "operational": False,
                "permissions": [],
                "error": "Registry catalog/schema is not configured (Settings -> Registry)",
            }

        try:
            client = _ss.DatabricksClient(host=host, token=token)
            principal = (
                client.auth.client_id or client.workspace.get_current_user_email() or ""
            ).strip()
            if not principal:
                return {
                    "success": True,
                    "registry_configured": True,
                    "registry_catalog": catalog,
                    "registry_schema": schema,
                    "storage_location": storage_location,
                    "principal": "",
                    "accessible": False,
                    "operational": False,
                    "permissions": [],
                    "error": (
                        "Principal could not be resolved; effective-permissions "
                        "check was skipped."
                    ),
                }

            effective = client.catalog.get_effective_schema_permissions(
                catalog, schema, principal
            )
            accessible = bool(effective.get("accessible", False))
            assignments = effective.get("assignments", [])
            raw_error = effective.get("error")
            normalized_error = None if raw_error is None else str(raw_error)
            summary = schema_permission_summary(catalog, schema, principal, assignments)
            return {
                "success": True,
                "registry_configured": True,
                "registry_catalog": catalog,
                "registry_schema": schema,
                "storage_location": storage_location,
                "principal": principal,
                "accessible": accessible,
                "operational": bool(summary.get("operational", False)) and accessible,
                "permissions": summary.get("permissions", []),
                "error": normalized_error,
            }
        except Exception as exc:
            logger.warning("triple_store_databricks_health failed: %s", exc)
            raise InfrastructureError(
                "inspect effective schema permissions failed",
                detail=str(exc),
            ) from exc

    @staticmethod
    def triple_store_databricks_objects_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """List Lakehouse-owned UC objects, grouped by domain version."""
        from back.core.graphdb.delta.objects import (
            domain_match_key,
            fetch_uc_schema_tables,
            group_triplestore_objects,
        )

        domain_obj, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        reg = registry_cfg if isinstance(registry_cfg, dict) else {}
        catalog = (reg.get("catalog") or "").strip()
        schema = (reg.get("schema") or "").strip()
        storage_location = f"{catalog}.{schema}" if catalog and schema else ""

        if not storage_location:
            return {
                "success": True,
                "registry_configured": False,
                "storage_location": "",
                "registry_catalog": catalog,
                "registry_schema": schema,
                "domains": [],
                "analytics": [],
                "orphans": [],
                "analytics_location": "",
                "analytics_message": "",
                "message": (
                    "Registry catalog/schema is not configured "
                    "(Settings → Registry)"
                ),
            }

        try:
            lakehouse_keys = SettingsService._lakehouse_domain_version_keys(
                domain_obj, settings
            )
            raw_tables = fetch_uc_schema_tables(catalog, schema)
            groups = group_triplestore_objects(raw_tables, catalog, schema)
            domains = [
                {
                    "base": grp["base"],
                    "key": domain_match_key(grp["base"]),
                    "items": [
                        {
                            "kind": item["kind"],
                            "name": item["name"],
                            "full_name": item["full_name"],
                        }
                        for item in grp["sorted_items"]
                    ],
                }
                for grp in sorted(groups.values(), key=lambda g: g["base"])
                if domain_match_key(grp["base"]) in lakehouse_keys
            ]
            analytics_location, analytics, analytics_message = (
                SettingsService._analytics_objects(
                    settings, catalog, schema, raw_tables
                )
            )
            analytics = [
                group for group in analytics if group["key"] in lakehouse_keys
            ]
            return {
                "success": True,
                "registry_configured": True,
                "storage_location": storage_location,
                "registry_catalog": catalog,
                "registry_schema": schema,
                "domains": domains,
                "analytics": analytics,
                "orphans": [],
                "analytics_location": analytics_location,
                "analytics_message": analytics_message,
            }
        except Exception as exc:
            logger.warning("triple_store_databricks_objects failed: %s", exc)
            raise InfrastructureError(
                "list Delta triple-store objects failed", detail=str(exc)
            ) from exc

    @staticmethod
    def _lakehouse_domain_version_keys(
        domain_obj: Any,
        settings: Settings,
    ) -> set[str]:
        """Return physical object keys for versions using the Lakehouse backend."""
        svc = _ss.RegistryService.from_context(domain_obj, settings)
        ok, details, message = svc.list_domain_details_cached()
        if not ok:
            raise InfrastructureError(
                "Failed to list registry domains", detail=message
            )

        keys: set[str] = set()
        for domain in details or []:
            if not isinstance(domain, dict):
                continue
            folder = _ss.sanitize_domain_folder(str(domain.get("name") or ""))
            for version in domain.get("versions") or []:
                if not isinstance(version, dict):
                    continue
                backend = str(version.get("graph_backend") or "").strip().lower()
                version_id = str(version.get("version") or "").strip()
                if backend == "databricks" and version_id:
                    keys.add(f"{folder}_{version_id}".lower())
        return keys

    @staticmethod
    def _analytics_objects(
        settings: Settings,
        registry_catalog: str,
        registry_schema: str,
        registry_tables: List[Dict[str, Any]],
    ) -> Tuple[str, List[Dict[str, Any]], str]:
        """Group the analytics job's UC output tables, best-effort.

        The job writes to ``analytics_job_output_schema`` when set and to the
        registry schema otherwise, so the common case reuses the enumeration the
        caller already performed. A scan that fails returns its reason instead of
        raising — the triple-store listing must still render.
        """
        from back.core.graphdb.delta.objects import (
            fetch_uc_schema_tables,
            group_analytics_objects,
        )

        configured = (
            getattr(settings, "analytics_job_output_schema", "") or ""
        ).strip()
        location = configured or f"{registry_catalog}.{registry_schema}"
        if location.count(".") != 1:
            return (
                location,
                [],
                f"Analytics output schema '{location}' is not a catalog.schema pair",
            )

        catalog, schema = location.split(".", 1)
        try:
            if (catalog, schema) == (registry_catalog, registry_schema):
                raw_tables = registry_tables
            else:
                raw_tables = fetch_uc_schema_tables(catalog, schema)
        except Exception as exc:
            logger.warning("analytics object listing failed for %s: %s", location, exc)
            return (location, [], f"Could not list analytics tables in {location}")

        groups = group_analytics_objects(raw_tables, catalog, schema)
        analytics = [
            {
                "key": grp["key"],
                "base": grp["base"],
                "items": [
                    {
                        "kind": item["kind"],
                        "name": item["name"],
                        "full_name": item["full_name"],
                    }
                    for item in grp["sorted_items"]
                ],
            }
            for grp in sorted(groups.values(), key=lambda g: g["base"])
        ]
        return (location, analytics, "")

    @staticmethod
    def get_graph_engine_config_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Return the engine-specific JSON configuration.

        Empty ``lakebase_project``, ``lakebase_branch``, and ``database``
        fields are overlaid with env-var fallbacks so the Connection tab
        always reflects the current platform binding, even when the user
        has not yet explicitly saved those fields through the UI.
        """
        import os as _os

        from back.core.graphdb.engine_config import normalize_graph_engine_config

        _, host, token, registry_cfg = SettingsService._resolve_context(
            session_mgr, settings
        )
        _ss.global_config_service.load(host, token, registry_cfg, force=True)
        cfg = normalize_graph_engine_config(
            _ss.global_config_service.get_graph_engine_config(host, token, registry_cfg)
        )
        lb = dict(cfg.get("lakebase") or {})

        _env_project = _os.environ.get("LAKEBASE_PROJECT", "")
        _env_branch = _os.environ.get("LAKEBASE_BRANCH", "")
        _env_db = _os.environ.get("PGDATABASE", "") or _os.environ.get("LAKEBASE_DATABASE", "")
        if not lb.get("lakebase_project") and _env_project:
            lb["lakebase_project"] = _env_project
        if not lb.get("lakebase_branch") and _env_branch:
            lb["lakebase_branch"] = _env_branch
        if not lb.get("database") and _env_db:
            lb["database"] = _env_db
        cfg["lakebase"] = lb

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
        from back.core.graphdb.engine_config import normalize_graph_engine_config

        if not isinstance(config, dict):
            raise ValidationError("graph_engine_config must be a JSON object")
        config = normalize_graph_engine_config(config)
        neo = dict(config.get("neo4j") or {})

        # Strip clear-text passwords from every named connection and from any
        # leftover flat profile keys.
        previous = _ss.global_config_service.get_graph_engine_config(
            host, token, registry_cfg
        )
        SettingsService._assert_neo4j_connection_refs_safe(
            previous, config, session_mgr, settings
        )

        conns = list(neo.get("connections") or [])
        cleaned_conns = []
        seen_names: set[str] = set()
        for entry in conns:
            name = str(entry.get("name") or "").strip()
            if not name:
                continue
            if name in seen_names:
                raise ValidationError(
                    f"Duplicate Neo4j connection name {name!r} — names must be unique."
                )
            seen_names.add(name)
            uri = str(entry.get("uri") or "").strip()
            user = str(entry.get("username") or "").strip()
            scope = str(entry.get("secret_scope") or "").strip()
            key = str(entry.get("secret_key") or "").strip()
            if not uri:
                raise ValidationError(
                    f"Neo4j connection {name!r} is missing a Bolt URI."
                )
            if not user:
                raise ValidationError(
                    f"Neo4j connection {name!r} is missing a username."
                )
            if not scope or not key:
                raise ValidationError(
                    f"Neo4j connection {name!r} must set secret scope and secret name."
                )
            profile = dict(entry)
            profile["name"] = name
            profile["uri"] = uri
            profile["username"] = user
            profile["secret_scope"] = scope
            profile["secret_key"] = key
            profile["auth_method"] = (
                str(profile.get("auth_method") or "databricks_secret").strip()
                or "databricks_secret"
            )
            if (
                profile.get("password")
                and (
                    profile.get("auth_method") == "databricks_secret"
                    or _ss.is_neo4j_password_from_secret()
                )
            ):
                profile.pop("password", None)
            cleaned_conns.append(profile)

        neo = {"connections": cleaned_conns}
        config = {**config, "neo4j": neo}

        ok, msg = _ss.global_config_service.set_graph_engine_config(
            host, token, registry_cfg, config
        )
        if not ok:
            raise ValidationError(msg)
        persisted_cfg = _ss.global_config_service.get_graph_engine_config(
            host, token, registry_cfg
        )
        SettingsService._mirror_graph_engine_to_domain_registry(
            session_mgr, config=persisted_cfg
        )
        return {"success": True, "graph_engine_config": persisted_cfg}

    @staticmethod
    def _assert_neo4j_connection_refs_safe(
        previous: Dict[str, Any],
        new_config: Dict[str, Any],
        session_mgr: SessionManager,
        settings: Settings,
    ) -> None:
        """Reject deletes/renames of Neo4j connections still referenced by domains."""
        from back.objects.domain.settings.graphengine.GraphEngineNeo4jSettings import (
            GraphEngineNeo4jSettings,
        )

        return GraphEngineNeo4jSettings._assert_neo4j_connection_refs_safe(previous, new_config, session_mgr, settings)


    @staticmethod
    def _domains_referencing_neo4j_connections(
        session_mgr: SessionManager,
        settings: Settings,
        connection_names: List[str],
    ) -> Dict[str, List[str]]:
        """Map connection name → domain folders that reference it."""
        from back.objects.domain.settings.graphengine.GraphEngineNeo4jSettings import (
            GraphEngineNeo4jSettings,
        )

        return GraphEngineNeo4jSettings._domains_referencing_neo4j_connections(session_mgr, settings, connection_names)


    @staticmethod
    def graph_engine_neo4j_connections_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """List named Neo4j connection profiles (no passwords)."""
        from back.objects.domain.settings.graphengine.GraphEngineNeo4jSettings import (
            GraphEngineNeo4jSettings,
        )

        return GraphEngineNeo4jSettings.graph_engine_neo4j_connections_result(session_mgr, settings)


    @staticmethod
    def graph_engine_lakebase_health_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Probe Lakebase Postgres for the configured graph schema (read-only)."""
        from back.objects.domain.settings.graphengine.GraphEngineLakebaseSettings import (
            GraphEngineLakebaseSettings,
        )

        return GraphEngineLakebaseSettings.graph_engine_lakebase_health_result(session_mgr, settings)


    @staticmethod
    def graph_engine_neo4j_test_result(
        session_mgr: SessionManager,
        settings: Settings,
        *,
        connection_name: str = "",
        draft: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Probe Neo4j Bolt connectivity for a named connection (or draft fields)."""
        from back.objects.domain.settings.graphengine.GraphEngineNeo4jSettings import (
            GraphEngineNeo4jSettings,
        )

        return GraphEngineNeo4jSettings.graph_engine_neo4j_test_result(session_mgr, settings, connection_name=connection_name, draft=draft)


    @staticmethod
    def _neo4j_credentials_source(gcfg: Dict[str, Any]) -> str:
        """Human-readable description of where the Neo4j password came from."""
        from back.objects.domain.settings.graphengine.GraphEngineNeo4jSettings import (
            GraphEngineNeo4jSettings,
        )

        return GraphEngineNeo4jSettings._neo4j_credentials_source(gcfg)


    @staticmethod
    def graph_engine_neo4j_secret_scopes_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """List Databricks secret scopes for the Neo4j "Databricks secret" dropdown."""
        from back.objects.domain.settings.graphengine.GraphEngineNeo4jSettings import (
            GraphEngineNeo4jSettings,
        )

        return GraphEngineNeo4jSettings.graph_engine_neo4j_secret_scopes_result(session_mgr, settings)


    @staticmethod
    def graph_engine_neo4j_secret_keys_result(
        scope: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """List secret keys within *scope* for the Neo4j "Secret key" dropdown."""
        from back.objects.domain.settings.graphengine.GraphEngineNeo4jSettings import (
            GraphEngineNeo4jSettings,
        )

        return GraphEngineNeo4jSettings.graph_engine_neo4j_secret_keys_result(scope, session_mgr, settings)


    @staticmethod
    def _neo4j_connection_from_config(
        session_mgr,
        settings,
        *,
        connection_name: str = "",
    ):
        """Build a :class:`Neo4jConnection` from a named Settings profile."""
        from back.objects.domain.settings.graphengine.GraphEngineNeo4jSettings import (
            GraphEngineNeo4jSettings,
        )

        return GraphEngineNeo4jSettings._neo4j_connection_from_config(session_mgr, settings, connection_name=connection_name)


    @staticmethod
    def graph_engine_neo4j_databases_result(
        session_mgr: SessionManager,
        settings: Settings,
        *,
        connection_name: str = "",
    ) -> Dict[str, Any]:
        """List Neo4j databases on the server for a named connection (admin)."""
        from back.objects.domain.settings.graphengine.GraphEngineNeo4jSettings import (
            GraphEngineNeo4jSettings,
        )

        return GraphEngineNeo4jSettings.graph_engine_neo4j_databases_result(session_mgr, settings, connection_name=connection_name)


    @staticmethod
    def graph_engine_neo4j_labels_result(
        session_mgr: SessionManager,
        settings: Settings,
        *,
        connection_name: str = "",
    ) -> Dict[str, Any]:
        """List materialised Neo4j graphs (marker labels) + counts for the admin Objects tab."""
        from back.objects.domain.settings.graphengine.GraphEngineNeo4jSettings import (
            GraphEngineNeo4jSettings,
        )

        return GraphEngineNeo4jSettings.graph_engine_neo4j_labels_result(session_mgr, settings, connection_name=connection_name)


    @staticmethod
    def graph_engine_neo4j_health_result(
        session_mgr: SessionManager,
        settings: Settings,
        *,
        connection_name: str = "",
    ) -> Dict[str, Any]:
        """Bolt health probe for the Neo4j admin Health tab."""
        from back.objects.domain.settings.graphengine.GraphEngineNeo4jSettings import (
            GraphEngineNeo4jSettings,
        )

        return GraphEngineNeo4jSettings.graph_engine_neo4j_health_result(session_mgr, settings, connection_name=connection_name)


    @staticmethod
    def graph_engine_neo4j_drop_label_result(
        label: str,
        session_mgr: SessionManager,
        settings: Settings,
        *,
        connection_name: str = "",
    ) -> Dict[str, Any]:
        """Drop one Neo4j graph (marker label): its nodes, rels, constraint, schema map."""
        from back.objects.domain.settings.graphengine.GraphEngineNeo4jSettings import (
            GraphEngineNeo4jSettings,
        )

        return GraphEngineNeo4jSettings.graph_engine_neo4j_drop_label_result(label, session_mgr, settings, connection_name=connection_name)


    @staticmethod
    def graph_engine_uc_catalogs_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """List Unity Catalog names (``SHOW CATALOGS``) for the Lakebase UC picker.

        Read-only; uses the configured SQL warehouse.
        """
        try:
            domain, host, token, registry_cfg = SettingsService._resolve_context(
                session_mgr, settings
            )
            _ss.global_config_service.load(host, token, registry_cfg, force=True)
            warehouse_id = _ss.global_config_service.get_warehouse_id(
                host, token, registry_cfg
            )
            if not warehouse_id:
                warehouse_id = (
                    (domain.databricks or {}).get("warehouse_id") or ""
                )
            if not warehouse_id:
                warehouse_id = settings.sql_warehouse_id or ""
            if not warehouse_id:
                raise ValidationError(
                    "Configure a SQL warehouse under Settings → Databricks first."
                )
            from back.core.databricks.DatabricksAuth import DatabricksAuth
            from back.core.databricks.uc import UnityCatalog

            auth = DatabricksAuth(host=host, token=token, warehouse_id=warehouse_id)
            uc = UnityCatalog(auth)
            catalogs = uc.get_catalogs()
            return {
                "success": True,
                "catalogs": sorted(catalogs) if catalogs else [],
            }
        except OntoBricksError:
            raise
        except Exception as exc:
            logger.warning("graph_engine_uc_catalogs failed: %s", exc)
            raise InfrastructureError(
                "list Unity Catalog catalogs failed", detail=str(exc)
            ) from exc

    @staticmethod
    def graph_engine_lakebase_projects_result(
        _session_mgr: SessionManager,
        _settings: Settings,
    ) -> Dict[str, Any]:
        """List all Lakebase Autoscaling projects visible in the workspace."""
        from back.objects.domain.settings.graphengine.GraphEngineLakebaseSettings import (
            GraphEngineLakebaseSettings,
        )

        return GraphEngineLakebaseSettings.graph_engine_lakebase_projects_result(_session_mgr, _settings)


    @staticmethod
    def graph_engine_lakebase_branches_result(
        project_path: str,
        _session_mgr: SessionManager,
        _settings: Settings,
    ) -> Dict[str, Any]:
        """List branches for a Lakebase Autoscaling project."""
        from back.objects.domain.settings.graphengine.GraphEngineLakebaseSettings import (
            GraphEngineLakebaseSettings,
        )

        return GraphEngineLakebaseSettings.graph_engine_lakebase_branches_result(project_path, _session_mgr, _settings)


    @staticmethod
    def graph_engine_lakebase_pg_databases_result(
        branch_path: str,
        _session_mgr: SessionManager,
        _settings: Settings,
    ) -> Dict[str, Any]:
        """List Postgres databases on a Lakebase branch endpoint."""
        from back.objects.domain.settings.graphengine.GraphEngineLakebaseSettings import (
            GraphEngineLakebaseSettings,
        )

        return GraphEngineLakebaseSettings.graph_engine_lakebase_pg_databases_result(branch_path, _session_mgr, _settings)


    @staticmethod
    def graph_engine_lakebase_pg_schemas_result(
        database: str,
        _session_mgr: SessionManager,
        _settings: Settings,
        branch_path: str = "",
    ) -> Dict[str, Any]:
        """List Postgres schemas in the graph Lakebase database."""
        from back.objects.domain.settings.graphengine.GraphEngineLakebaseSettings import (
            GraphEngineLakebaseSettings,
        )

        return GraphEngineLakebaseSettings.graph_engine_lakebase_pg_schemas_result(database, _session_mgr, _settings, branch_path)


    @staticmethod
    def graph_engine_lakebase_provision_result(
        params: Dict[str, Any],
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Provision a brand-new Lakebase graph DB end-to-end (admin only)."""
        from back.objects.domain.settings.graphengine.GraphEngineLakebaseSettings import (
            GraphEngineLakebaseSettings,
        )

        return GraphEngineLakebaseSettings.graph_engine_lakebase_provision_result(params, email, user_token, session_mgr, settings)


    @staticmethod
    def _graph_engine_database(
        session_mgr: SessionManager,
        settings: Any,
    ) -> str:
        """Return the Lakebase ``database`` field from the saved graph engine config."""
        from back.objects.domain.settings.graphengine.GraphEngineLakebaseSettings import (
            GraphEngineLakebaseSettings,
        )

        return GraphEngineLakebaseSettings._graph_engine_database(session_mgr, settings)


    @staticmethod
    def _graph_engine_auth(
        session_mgr: SessionManager,
        settings: Any,
        form_branch_path: str = "",
        form_database: str = "",
    ):
        """Return the correct Lakebase auth for graph DB operations."""
        from back.objects.domain.settings.graphengine.GraphEngineLakebaseSettings import (
            GraphEngineLakebaseSettings,
        )

        return GraphEngineLakebaseSettings._graph_engine_auth(session_mgr, settings, form_branch_path, form_database)


    @staticmethod
    def _lakebase_kwargs_for_branch(
        branch_path: str,
        database: str,
        application_name: str,
    ) -> Dict[str, Any]:
        """Resolve psycopg connect kwargs directly from a Lakebase branch resource path."""
        from back.objects.domain.settings.graphengine.GraphEngineLakebaseSettings import (
            GraphEngineLakebaseSettings,
        )

        return GraphEngineLakebaseSettings._lakebase_kwargs_for_branch(branch_path, database, application_name)


    @staticmethod
    def graph_engine_lakebase_objects_result(
        database: str,
        branch_path: str,
        _session_mgr: SessionManager,
        _settings: Settings,
    ) -> Dict[str, Any]:
        """List all user schemas, tables and views in the graph Lakebase database."""
        from back.objects.domain.settings.graphengine.GraphEngineLakebaseSettings import (
            GraphEngineLakebaseSettings,
        )

        return GraphEngineLakebaseSettings.graph_engine_lakebase_objects_result(database, branch_path, _session_mgr, _settings)


    @staticmethod
    def graph_engine_lakebase_sync_objects_result(
        database: str,
        branch_path: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """List UC Delta tables in the configured graph schema, plus Lakeflow state."""
        from back.objects.domain.settings.graphengine.GraphEngineLakebaseSettings import (
            GraphEngineLakebaseSettings,
        )

        return GraphEngineLakebaseSettings.graph_engine_lakebase_sync_objects_result(database, branch_path, session_mgr, settings)


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
        """Drop a Postgres schema, table or view in the connected Lakebase database."""
        from back.objects.domain.settings.graphengine.GraphEngineLakebaseSettings import (
            GraphEngineLakebaseSettings,
        )

        return GraphEngineLakebaseSettings.graph_engine_lakebase_drop_object_result(kind, schema, name, database, branch_path, _session_mgr, _settings)


    @staticmethod
    def graph_engine_lakebase_pg_roles_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """List Postgres roles on the graph Lakebase branch and overlay app-user status."""
        from back.objects.domain.settings.graphengine.GraphEngineLakebaseSettings import (
            GraphEngineLakebaseSettings,
        )

        return GraphEngineLakebaseSettings.graph_engine_lakebase_pg_roles_result(session_mgr, settings)


    @staticmethod
    def graph_engine_lakebase_grant_superuser_result(
        user_email: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Ensure *user_email* has a Postgres OAuth role and DATABRICKS_SUPERUSER membership."""
        from back.objects.domain.settings.graphengine.GraphEngineLakebaseSettings import (
            GraphEngineLakebaseSettings,
        )

        return GraphEngineLakebaseSettings.graph_engine_lakebase_grant_superuser_result(user_email, session_mgr, settings)


    @staticmethod
    def graph_engine_drop_uc_object_result(
        full_name: str,
        is_sync: bool,
        _session_mgr: SessionManager,
        _settings: Settings,
    ) -> Dict[str, Any]:
        """Drop a Unity Catalog table or Lakeflow synced-table registration.

        When ``is_sync=True`` the entry is a Lakeflow-managed synced table:
        ``SyncedTableManager.delete()`` is used so both the Lakebase control-plane
        reservation and the UC registration are cleaned up.

        When ``is_sync=False`` a plain UC Delta table / view is deleted via the
        Unity Catalog REST API.
        """
        if not full_name or full_name.count(".") < 2:
            raise ValidationError(
                "full_name must be a 3-part Unity Catalog FQN (catalog.schema.table)"
            )

        try:
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            api = getattr(w, "api_client", None)
            if api is None or not hasattr(api, "do"):
                raise InfrastructureError("Databricks SDK api_client unavailable")

            if is_sync:
                from back.core.graphdb.lakebase.SyncedTableManager import SyncedTableManager

                mgr = SyncedTableManager()
                mgr.delete(full_name, purge_data=True)
            else:
                api.do("DELETE", f"/api/2.1/unity-catalog/tables/{full_name}")

            return {"success": True, "message": f"Dropped {full_name}"}
        except OntoBricksError:
            raise
        except Exception as exc:
            logger.warning("graph_engine_drop_uc_object failed: %s", exc)
            raise InfrastructureError(
                "Drop UC object failed", detail=str(exc)
            ) from exc

    @staticmethod
    def graph_engine_uc_schemas_result(
        catalog: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """List Unity Catalog schemas in a given catalog."""
        if not catalog:
            raise ValidationError("catalog is required")
        try:
            domain, host, token, registry_cfg = SettingsService._resolve_context(
                session_mgr, settings
            )
            _ss.global_config_service.load(host, token, registry_cfg, force=True)
            warehouse_id = _ss.global_config_service.get_warehouse_id(
                host, token, registry_cfg
            )
            if not warehouse_id:
                warehouse_id = (
                    (domain.databricks or {}).get("warehouse_id") or ""
                )
            if not warehouse_id:
                warehouse_id = settings.sql_warehouse_id or ""
            if not warehouse_id:
                raise ValidationError(
                    "Configure a SQL warehouse under Settings → Databricks first."
                )
            from back.core.databricks.DatabricksAuth import DatabricksAuth
            from back.core.databricks.uc import UnityCatalog

            auth = DatabricksAuth(host=host, token=token, warehouse_id=warehouse_id)
            uc = UnityCatalog(auth)
            schemas = uc.get_schemas(catalog)
            return {
                "success": True,
                "schemas": sorted(schemas) if schemas else [],
            }
        except OntoBricksError:
            raise
        except Exception as exc:
            logger.warning("graph_engine_uc_schemas failed: %s", exc)
            raise InfrastructureError(
                "list Unity Catalog schemas failed", detail=str(exc)
            ) from exc
