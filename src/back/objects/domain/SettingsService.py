"""Databricks settings, registry, permissions, and schedule orchestration."""

from __future__ import annotations

import copy
import json
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from back.core.errors import (
    AuthorizationError,
    InfrastructureError,
    NotFoundError,
    OntoBricksError,
    ValidationError,
)
from shared.config.constants import HTTP_USER_AGENT
from shared.config.settings import Settings
from back.core.databricks import is_databricks_app
from back.core.databricks.constants import PERMISSIONS_APPS_PATH
from back.core.databricks.lakebase.grants import resolve_mcp_app_name
from back.core.helpers import (
    DEFAULT_LOGO_PATH,
    build_auto_base_uri,
    get_databricks_client,
    get_databricks_host_and_token,
    normalize_ui_branding,
    resolve_app_registry_context,
    resolve_default_base_uri,
    resolve_delta_warehouse_id,
    resolve_use_cloud_fetch,
    resolve_warehouse_id,
    run_blocking,
    validate_optional_hex_color,
)
from back.core.logging import get_logger
from back.objects.registry import (
    ASSIGNABLE_ROLES,
    RegistryCfg,
    RegistryService,
    permission_service,
    invalidate_registry_cache,
    obx_format,
)
from back.objects.registry.version_lifecycle import (
    check_version_deletion,
    check_status_transition,
    STATUS_DRAFT,
    STATUS_IN_REVIEW,
    STATUS_PUBLISHED,
    version_deletion_capability,
)
from back.objects.domain.version_status import clear_version_status_cache
from back.objects.session import (
    SessionManager,
    get_domain,
    global_config_service,
    sanitize_domain_folder,
)

logger = get_logger(__name__)


class SettingsService:
    """Configuration, registry, permissions, and build schedules."""

    @staticmethod
    def _get_scheduler():
        """Defer APScheduler import until schedule endpoints run."""
        from back.objects.registry import get_scheduler as _gs

        return _gs()

    @staticmethod
    def is_warehouse_locked(settings: Settings) -> bool:
        from back.objects.domain.WarehouseSettings import WarehouseSettings

        return WarehouseSettings.is_warehouse_locked(settings)


    @staticmethod
    def is_registry_locked(settings: Settings) -> bool:
        """True when registry params are injected by Apps (not editable via .env).

        Lakebase backend: Apps injects PGHOST from the database resource.
        """
        if not is_databricks_app():
            return False
        import os
        return bool(os.environ.get("PGHOST", ""))

    @staticmethod
    def _resolve_context(session_mgr: SessionManager, settings: Settings):
        """Return the (domain, host, token, registry_cfg_dict) tuple used by most endpoints."""
        domain = get_domain(session_mgr)
        host, token = get_databricks_host_and_token(domain, settings)
        registry_cfg = RegistryCfg.from_domain(domain, settings).as_dict()
        return domain, host, token, registry_cfg

    @staticmethod
    def _mirror_graph_engine_to_domain_registry(
        session_mgr: SessionManager,
        *,
        config: Optional[Dict[str, Any]] = None,
        delta_warehouse_id: Optional[str] = None,
    ) -> None:
        """Copy graph DB *connection* settings into ``domain.settings['registry']``."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings._mirror_graph_engine_to_domain_registry(session_mgr, config=config, delta_warehouse_id=delta_warehouse_id)


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
        from back.objects.domain.WarehouseSettings import WarehouseSettings

        return WarehouseSettings.build_current_config(session_mgr, settings)


    @staticmethod
    def apply_config_save(
        data: Dict[str, Any],
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.WarehouseSettings import WarehouseSettings

        return WarehouseSettings.apply_config_save(data, email, user_token, session_mgr, settings)


    @staticmethod
    async def test_connection(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        from back.objects.domain.WarehouseSettings import WarehouseSettings

        return await WarehouseSettings.test_connection(session_mgr, settings)


    @staticmethod
    async def fetch_warehouses(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        from back.objects.domain.WarehouseSettings import WarehouseSettings

        return await WarehouseSettings.fetch_warehouses(session_mgr, settings)


    @staticmethod
    def select_warehouse(
        warehouse_id: Optional[str],
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.WarehouseSettings import WarehouseSettings

        return WarehouseSettings.select_warehouse(warehouse_id, email, user_token, session_mgr, settings)


    @staticmethod
    def select_build_warehouse(
        warehouse_id: Optional[str],
        warehouse_type: Optional[str],
        use_sea: bool,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.WarehouseSettings import WarehouseSettings

        return WarehouseSettings.select_build_warehouse(warehouse_id, warehouse_type, use_sea, email, user_token, session_mgr, settings)


    @staticmethod
    def select_delta_warehouse(
        warehouse_id: Optional[str],
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
        *,
        use_sea: bool = False,
    ) -> Dict[str, Any]:
        from back.objects.domain.WarehouseSettings import WarehouseSettings

        return WarehouseSettings.select_delta_warehouse(warehouse_id, email, user_token, session_mgr, settings, use_sea=use_sea)


    @staticmethod
    async def fetch_catalogs(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        from back.objects.domain.RegistrySettings import RegistrySettings

        return await RegistrySettings.fetch_catalogs(session_mgr, settings)


    @staticmethod
    async def fetch_schemas(
        catalog: str,
        session_mgr: SessionManager,
        settings: Settings,
        *,
        log_label: str = "Get schemas",
    ) -> Dict[str, Any]:
        from back.objects.domain.RegistrySettings import RegistrySettings

        return await RegistrySettings.fetch_schemas(catalog, session_mgr, settings, log_label=log_label)


    @staticmethod
    async def fetch_volumes(
        catalog: str,
        schema: str,
        session_mgr: SessionManager,
        settings: Settings,
        log_label: str = "Get volumes",
    ) -> Dict[str, Any]:
        from back.objects.domain.RegistrySettings import RegistrySettings

        return await RegistrySettings.fetch_volumes(catalog, schema, session_mgr, settings, log_label)


    @staticmethod
    async def fetch_uc_assets(
        catalog: str,
        schema: str,
        session_mgr: SessionManager,
        settings: Settings,
        log_label: str = "Get UC assets",
    ) -> Dict[str, Any]:
        from back.objects.domain.RegistrySettings import RegistrySettings

        return await RegistrySettings.fetch_uc_assets(catalog, schema, session_mgr, settings, log_label)


    @staticmethod
    async def fetch_uc_functions(
        catalog: str,
        schema: str,
        session_mgr: SessionManager,
        settings: Settings,
        log_label: str = "Get UC functions",
    ) -> Dict[str, Any]:
        from back.objects.domain.RegistrySettings import RegistrySettings

        return await RegistrySettings.fetch_uc_functions(catalog, schema, session_mgr, settings, log_label)


    @staticmethod
    async def check_lakebase_permissions(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        from back.objects.domain.RegistrySettings import RegistrySettings

        return await RegistrySettings.check_lakebase_permissions(session_mgr, settings)


    @staticmethod
    async def check_registry_access(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        from back.objects.domain.RegistrySettings import RegistrySettings

        return await RegistrySettings.check_registry_access(session_mgr, settings)


    @staticmethod
    def build_registry_get_payload(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        from back.objects.domain.RegistrySettings import RegistrySettings

        return RegistrySettings.build_registry_get_payload(session_mgr, settings)


    @staticmethod
    def _lakebase_runtime_info(rcfg: RegistryCfg) -> Dict[str, Any]:
        from back.objects.domain.RegistrySettings import RegistrySettings

        return RegistrySettings._lakebase_runtime_info(rcfg)


    @staticmethod
    def _lakebase_schema_initialized(rcfg: RegistryCfg) -> bool:
        from back.objects.domain.RegistrySettings import RegistrySettings

        return RegistrySettings._lakebase_schema_initialized(rcfg)


    @staticmethod
    def _lakebase_schema_status(rcfg: RegistryCfg) -> Dict[str, bool]:
        from back.objects.domain.RegistrySettings import RegistrySettings

        return RegistrySettings._lakebase_schema_status(rcfg)



    @staticmethod
    def initialize_registry_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        from back.objects.domain.RegistrySettings import RegistrySettings

        return RegistrySettings.initialize_registry_result(session_mgr, settings)


    @staticmethod
    def _registry_grant_app_names(settings: Settings) -> List[str]:
        from back.objects.domain.RegistrySettings import RegistrySettings

        return RegistrySettings._registry_grant_app_names(settings)


    @staticmethod
    def _grant_registry_permissions(
        session_mgr: SessionManager, settings: Settings
    ) -> Optional[Dict[str, Any]]:
        from back.objects.domain.RegistrySettings import RegistrySettings

        return RegistrySettings._grant_registry_permissions(session_mgr, settings)


    @staticmethod
    async def grant_registry_permissions_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        from back.objects.domain.RegistrySettings import RegistrySettings

        return await RegistrySettings.grant_registry_permissions_result(session_mgr, settings)


    @staticmethod
    def list_registry_domains_result(
        session_mgr: SessionManager,
        settings: Settings,
        *,
        user_role: str = "",
    ) -> Dict[str, Any]:
        from back.objects.domain.RegistryDomainSettings import RegistryDomainSettings

        return RegistryDomainSettings.list_registry_domains_result(session_mgr, settings, user_role=user_role)


    @staticmethod
    def list_registry_bridges_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        from back.objects.domain.RegistryDomainSettings import RegistryDomainSettings

        return RegistryDomainSettings.list_registry_bridges_result(session_mgr, settings)


    @staticmethod
    def delete_registry_domain_result(
        domain_name: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.RegistryDomainSettings import RegistryDomainSettings

        return RegistryDomainSettings.delete_registry_domain_result(domain_name, session_mgr, settings)


    @staticmethod
    def delete_registry_version_result(
        domain_name: str,
        version: str,
        *,
        user_role: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.RegistryDomainSettings import RegistryDomainSettings

        return RegistryDomainSettings.delete_registry_version_result(domain_name, version, user_role=user_role, session_mgr=session_mgr, settings=settings)


    @staticmethod
    def resolve_domain_role(
        request,
        domain_folder: str,
        settings: Settings,
        *,
        app_role: str = "",
    ) -> str:
        from back.objects.domain.RegistryDomainSettings import RegistryDomainSettings

        return RegistryDomainSettings.resolve_domain_role(request, domain_folder, settings, app_role=app_role)


    @staticmethod
    def set_registry_version_status_result(
        domain_name: str,
        version: str,
        new_status: str,
        *,
        user_role: str,
        user_domain_role: str,
        actor_email: str = "",
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.RegistryDomainSettings import RegistryDomainSettings

        return RegistryDomainSettings.set_registry_version_status_result(domain_name, version, new_status, user_role=user_role, user_domain_role=user_domain_role, actor_email=actor_email, session_mgr=session_mgr, settings=settings)


    @staticmethod
    def set_default_emoji_result(
        emoji: str,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.WorkspaceUiSettings import WorkspaceUiSettings

        return WorkspaceUiSettings.set_default_emoji_result(emoji, email, user_token, session_mgr, settings)


    @staticmethod
    def save_base_uri_result(
        base_uri: str,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.WorkspaceUiSettings import WorkspaceUiSettings

        return WorkspaceUiSettings.save_base_uri_result(base_uri, email, user_token, session_mgr, settings)


    @staticmethod
    def _validate_and_encode_logo(content: bytes, content_type: str) -> tuple[str, str]:
        from back.objects.domain.WorkspaceUiSettings import WorkspaceUiSettings

        return WorkspaceUiSettings._validate_and_encode_logo(content, content_type)


    @staticmethod
    def get_ui_branding_result(
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.WorkspaceUiSettings import WorkspaceUiSettings

        return WorkspaceUiSettings.get_ui_branding_result(email, user_token, session_mgr, settings)


    @staticmethod
    def save_ui_branding_result(
        app_title: str,
        primary_color: str,
        aurora_color: str,
        logo_content: Optional[bytes],
        logo_mime: Optional[str],
        reset_logo: bool,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.WorkspaceUiSettings import WorkspaceUiSettings

        return WorkspaceUiSettings.save_ui_branding_result(app_title, primary_color, aurora_color, logo_content, logo_mime, reset_logo, email, user_token, session_mgr, settings)


    @staticmethod
    def get_navbar_logo_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.WorkspaceUiSettings import WorkspaceUiSettings

        return WorkspaceUiSettings.get_navbar_logo_result(session_mgr, settings)


    @staticmethod
    def upload_navbar_logo_result(
        content: bytes,
        content_type: str,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.WorkspaceUiSettings import WorkspaceUiSettings

        return WorkspaceUiSettings.upload_navbar_logo_result(content, content_type, email, user_token, session_mgr, settings)


    @staticmethod
    def reset_navbar_logo_result(
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.WorkspaceUiSettings import WorkspaceUiSettings

        return WorkspaceUiSettings.reset_navbar_logo_result(email, user_token, session_mgr, settings)


    @staticmethod
    def get_registry_cache_ttl_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.WorkspaceUiSettings import WorkspaceUiSettings

        return WorkspaceUiSettings.get_registry_cache_ttl_result(session_mgr, settings)


    @staticmethod
    def save_registry_cache_ttl_result(
        ttl: int,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.WorkspaceUiSettings import WorkspaceUiSettings

        return WorkspaceUiSettings.save_registry_cache_ttl_result(ttl, email, user_token, session_mgr, settings)


    @staticmethod
    def get_graph_limits_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.WorkspaceUiSettings import WorkspaceUiSettings

        return WorkspaceUiSettings.get_graph_limits_result(session_mgr, settings)


    @staticmethod
    def save_graph_limits_result(
        graph_query_timeout_s: Optional[int],
        graph_chat_result_cap: Optional[int],
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.WorkspaceUiSettings import WorkspaceUiSettings

        return WorkspaceUiSettings.save_graph_limits_result(graph_query_timeout_s, graph_chat_result_cap, email, user_token, session_mgr, settings)


    @staticmethod
    def get_data_assets_import_limit_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.WorkspaceUiSettings import WorkspaceUiSettings

        return WorkspaceUiSettings.get_data_assets_import_limit_result(
            session_mgr, settings
        )


    @staticmethod
    def save_data_assets_import_limit_result(
        limit: int,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.WorkspaceUiSettings import WorkspaceUiSettings

        return WorkspaceUiSettings.save_data_assets_import_limit_result(
            limit, email, user_token, session_mgr, settings
        )


    @staticmethod
    def get_edit_lock_ttl_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.WorkspaceUiSettings import WorkspaceUiSettings

        return WorkspaceUiSettings.get_edit_lock_ttl_result(session_mgr, settings)


    @staticmethod
    def save_edit_lock_ttl_result(
        ttl_s: int,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.WorkspaceUiSettings import WorkspaceUiSettings

        return WorkspaceUiSettings.save_edit_lock_ttl_result(ttl_s, email, user_token, session_mgr, settings)


    @staticmethod
    def get_analytics_job_enabled_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.WorkspaceUiSettings import WorkspaceUiSettings

        return WorkspaceUiSettings.get_analytics_job_enabled_result(session_mgr, settings)


    @staticmethod
    def save_analytics_job_enabled_result(
        enabled: bool,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.WorkspaceUiSettings import WorkspaceUiSettings

        return WorkspaceUiSettings.save_analytics_job_enabled_result(enabled, email, user_token, session_mgr, settings)


    @staticmethod
    def save_use_cloud_fetch_result(
        enabled: bool,
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.WorkspaceUiSettings import WorkspaceUiSettings

        return WorkspaceUiSettings.save_use_cloud_fetch_result(enabled, email, user_token, session_mgr, settings)


    # ------------------------------------------------------------------
    #  Graph DB Engine
    # ------------------------------------------------------------------

    @staticmethod
    def get_delta_warehouse_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.WarehouseSettings import WarehouseSettings

        return WarehouseSettings.get_delta_warehouse_result(session_mgr, settings)


    @staticmethod
    def triple_store_databricks_health_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings.triple_store_databricks_health_result(session_mgr, settings)


    @staticmethod
    def triple_store_databricks_objects_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """List Lakehouse-owned UC objects, grouped by domain version."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings.triple_store_databricks_objects_result(session_mgr, settings)


    @staticmethod
    def _lakehouse_domain_version_keys(
        domain_obj: Any,
        settings: Settings,
    ) -> set[str]:
        """Return physical object keys for versions using the Lakehouse backend."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings._lakehouse_domain_version_keys(domain_obj, settings)


    @staticmethod
    def _analytics_objects(
        settings: Settings,
        registry_catalog: str,
        registry_schema: str,
        registry_tables: List[Dict[str, Any]],
    ) -> Tuple[str, List[Dict[str, Any]], str]:
        """Group the analytics job's UC output tables, best-effort."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings._analytics_objects(settings, registry_catalog, registry_schema, registry_tables)


    @staticmethod
    def get_graph_engine_config_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Return the engine-specific JSON configuration."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings.get_graph_engine_config_result(session_mgr, settings)


    @staticmethod
    def set_graph_engine_config_result(
        config: Dict[str, Any],
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Persist the engine-specific JSON configuration (admin only)."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings.set_graph_engine_config_result(config, email, user_token, session_mgr, settings)


    @staticmethod
    def _assert_neo4j_connection_refs_safe(
        previous: Dict[str, Any],
        new_config: Dict[str, Any],
        session_mgr: SessionManager,
        settings: Settings,
    ) -> None:
        """Reject deletes/renames of Neo4j connections still referenced by domains."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings._assert_neo4j_connection_refs_safe(previous, new_config, session_mgr, settings)


    @staticmethod
    def _domains_referencing_neo4j_connections(
        session_mgr: SessionManager,
        settings: Settings,
        connection_names: List[str],
    ) -> Dict[str, List[str]]:
        """Map connection name → domain folders that reference it."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings._domains_referencing_neo4j_connections(session_mgr, settings, connection_names)


    @staticmethod
    def graph_engine_neo4j_connections_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """List named Neo4j connection profiles (no passwords)."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings.graph_engine_neo4j_connections_result(session_mgr, settings)


    @staticmethod
    def graph_engine_lakebase_health_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Probe Lakebase Postgres for the configured graph schema (read-only)."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings.graph_engine_lakebase_health_result(session_mgr, settings)


    @staticmethod
    def graph_engine_neo4j_test_result(
        session_mgr: SessionManager,
        settings: Settings,
        *,
        connection_name: str = "",
        draft: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Probe Neo4j Bolt connectivity for a named connection (or draft fields)."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings.graph_engine_neo4j_test_result(session_mgr, settings, connection_name=connection_name, draft=draft)


    @staticmethod
    def _neo4j_credentials_source(gcfg: Dict[str, Any]) -> str:
        """Human-readable description of where the Neo4j password came from."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings._neo4j_credentials_source(gcfg)


    @staticmethod
    def graph_engine_neo4j_secret_scopes_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """List Databricks secret scopes for the Neo4j "Databricks secret" dropdown."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings.graph_engine_neo4j_secret_scopes_result(session_mgr, settings)


    @staticmethod
    def graph_engine_neo4j_secret_keys_result(
        scope: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """List secret keys within *scope* for the Neo4j "Secret key" dropdown."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings.graph_engine_neo4j_secret_keys_result(scope, session_mgr, settings)


    @staticmethod
    def _neo4j_connection_from_config(
        session_mgr,
        settings,
        *,
        connection_name: str = "",
    ):
        """Build a :class:`Neo4jConnection` from a named Settings profile."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings._neo4j_connection_from_config(session_mgr, settings, connection_name=connection_name)


    @staticmethod
    def graph_engine_neo4j_databases_result(
        session_mgr: SessionManager,
        settings: Settings,
        *,
        connection_name: str = "",
    ) -> Dict[str, Any]:
        """List Neo4j databases on the server for a named connection (admin)."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings.graph_engine_neo4j_databases_result(session_mgr, settings, connection_name=connection_name)


    @staticmethod
    def graph_engine_neo4j_labels_result(
        session_mgr: SessionManager,
        settings: Settings,
        *,
        connection_name: str = "",
    ) -> Dict[str, Any]:
        """List materialised Neo4j graphs (marker labels) + counts for the admin Objects tab."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings.graph_engine_neo4j_labels_result(session_mgr, settings, connection_name=connection_name)


    @staticmethod
    def graph_engine_neo4j_health_result(
        session_mgr: SessionManager,
        settings: Settings,
        *,
        connection_name: str = "",
    ) -> Dict[str, Any]:
        """Bolt health probe for the Neo4j admin Health tab."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings.graph_engine_neo4j_health_result(session_mgr, settings, connection_name=connection_name)


    @staticmethod
    def graph_engine_neo4j_drop_label_result(
        label: str,
        session_mgr: SessionManager,
        settings: Settings,
        *,
        connection_name: str = "",
    ) -> Dict[str, Any]:
        """Drop one Neo4j graph (marker label): its nodes, rels, constraint, schema map."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings.graph_engine_neo4j_drop_label_result(label, session_mgr, settings, connection_name=connection_name)


    @staticmethod
    def graph_engine_uc_catalogs_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """List Unity Catalog names (``SHOW CATALOGS``) for the Lakebase UC picker."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings.graph_engine_uc_catalogs_result(session_mgr, settings)


    @staticmethod
    def graph_engine_lakebase_projects_result(
        _session_mgr: SessionManager,
        _settings: Settings,
    ) -> Dict[str, Any]:
        """List all Lakebase Autoscaling projects visible in the workspace."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings.graph_engine_lakebase_projects_result(_session_mgr, _settings)


    @staticmethod
    def graph_engine_lakebase_branches_result(
        project_path: str,
        _session_mgr: SessionManager,
        _settings: Settings,
    ) -> Dict[str, Any]:
        """List branches for a Lakebase Autoscaling project."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings.graph_engine_lakebase_branches_result(project_path, _session_mgr, _settings)


    @staticmethod
    def graph_engine_lakebase_pg_databases_result(
        branch_path: str,
        _session_mgr: SessionManager,
        _settings: Settings,
    ) -> Dict[str, Any]:
        """List Postgres databases on a Lakebase branch endpoint."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings.graph_engine_lakebase_pg_databases_result(branch_path, _session_mgr, _settings)


    @staticmethod
    def graph_engine_lakebase_pg_schemas_result(
        database: str,
        _session_mgr: SessionManager,
        _settings: Settings,
        branch_path: str = "",
    ) -> Dict[str, Any]:
        """List Postgres schemas in the graph Lakebase database."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings.graph_engine_lakebase_pg_schemas_result(database, _session_mgr, _settings, branch_path)


    @staticmethod
    def graph_engine_lakebase_provision_result(
        params: Dict[str, Any],
        email: str,
        user_token: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Provision a brand-new Lakebase graph DB end-to-end (admin only)."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings.graph_engine_lakebase_provision_result(params, email, user_token, session_mgr, settings)


    @staticmethod
    def _graph_engine_database(
        session_mgr: SessionManager,
        settings: Any,
    ) -> str:
        """Return the Lakebase ``database`` field from the saved graph engine config."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings._graph_engine_database(session_mgr, settings)


    @staticmethod
    def _graph_engine_auth(
        session_mgr: SessionManager,
        settings: Any,
        form_branch_path: str = "",
        form_database: str = "",
    ):
        """Return the correct Lakebase auth for graph DB operations."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings._graph_engine_auth(session_mgr, settings, form_branch_path, form_database)


    @staticmethod
    def _lakebase_kwargs_for_branch(
        branch_path: str,
        database: str,
        application_name: str,
    ) -> Dict[str, Any]:
        """Resolve psycopg connect kwargs directly from a Lakebase branch resource path."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings._lakebase_kwargs_for_branch(branch_path, database, application_name)


    @staticmethod
    def graph_engine_lakebase_objects_result(
        database: str,
        branch_path: str,
        _session_mgr: SessionManager,
        _settings: Settings,
    ) -> Dict[str, Any]:
        """List all user schemas, tables and views in the graph Lakebase database."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings.graph_engine_lakebase_objects_result(database, branch_path, _session_mgr, _settings)


    @staticmethod
    def graph_engine_lakebase_sync_objects_result(
        database: str,
        branch_path: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """List UC Delta tables in the configured graph schema, plus Lakeflow state."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings.graph_engine_lakebase_sync_objects_result(database, branch_path, session_mgr, settings)


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
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings.graph_engine_lakebase_drop_object_result(kind, schema, name, database, branch_path, _session_mgr, _settings)


    @staticmethod
    def graph_engine_lakebase_pg_roles_result(
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """List Postgres roles on the graph Lakebase branch and overlay app-user status."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings.graph_engine_lakebase_pg_roles_result(session_mgr, settings)


    @staticmethod
    def graph_engine_lakebase_grant_superuser_result(
        user_email: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """Ensure *user_email* has a Postgres OAuth role and DATABRICKS_SUPERUSER membership."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings.graph_engine_lakebase_grant_superuser_result(user_email, session_mgr, settings)


    @staticmethod
    def graph_engine_drop_uc_object_result(
        full_name: str,
        is_sync: bool,
        _session_mgr: SessionManager,
        _settings: Settings,
    ) -> Dict[str, Any]:
        """Drop a Unity Catalog table or Lakeflow synced-table registration."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings.graph_engine_drop_uc_object_result(full_name, is_sync, _session_mgr, _settings)


    @staticmethod
    def graph_engine_uc_schemas_result(
        catalog: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        """List Unity Catalog schemas in a given catalog."""
        from back.objects.domain.GraphEngineSettings import GraphEngineSettings

        return GraphEngineSettings.graph_engine_uc_schemas_result(catalog, session_mgr, settings)


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
        from back.objects.domain.PermissionSettings import PermissionSettings

        return PermissionSettings.build_permissions_me(email, display_name, user_token, user_role, user_domain_role, session_mgr, settings)


    @staticmethod
    def build_permissions_diag(
        email: str,
        display_name: str,
        user_token: str,
        user_role: str,
        user_domain_role: str,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.PermissionSettings import PermissionSettings

        return PermissionSettings.build_permissions_diag(email, display_name, user_token, user_role, user_domain_role, settings)


    @staticmethod
    def list_app_principals_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        from back.objects.domain.PermissionSettings import PermissionSettings

        return PermissionSettings.list_app_principals_result(session_mgr, settings)


    @staticmethod
    def list_principals_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        from back.objects.domain.PermissionSettings import PermissionSettings

        return PermissionSettings.list_principals_result(session_mgr, settings)


    @staticmethod
    def search_workspace_principals(
        query: str,
        principal_type: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.PermissionSettings import PermissionSettings

        return PermissionSettings.search_workspace_principals(query, principal_type, session_mgr, settings)


    @staticmethod
    def list_domain_permissions_result(
        domain_name: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.PermissionSettings import PermissionSettings

        return PermissionSettings.list_domain_permissions_result(domain_name, session_mgr, settings)


    @staticmethod
    def add_domain_permission_result(
        domain_name: str,
        data: Dict[str, Any],
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.PermissionSettings import PermissionSettings

        return PermissionSettings.add_domain_permission_result(domain_name, data, session_mgr, settings)


    @staticmethod
    def delete_domain_permission_result(
        domain_name: str,
        principal: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.PermissionSettings import PermissionSettings

        return PermissionSettings.delete_domain_permission_result(domain_name, principal, session_mgr, settings)


    @staticmethod
    def build_teams_matrix_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        from back.objects.domain.PermissionSettings import PermissionSettings

        return PermissionSettings.build_teams_matrix_result(session_mgr, settings)


    @staticmethod
    def save_teams_batch_result(
        data: Dict[str, Any],
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.PermissionSettings import PermissionSettings

        return PermissionSettings.save_teams_batch_result(data, session_mgr, settings)


    @staticmethod
    def human_size(nbytes: int) -> str:
        from back.objects.domain.ScheduleSettings import ScheduleSettings

        return ScheduleSettings.human_size(nbytes)


    @staticmethod
    def list_schedules_result(
        session_mgr: SessionManager, settings: Settings
    ) -> Dict[str, Any]:
        from back.objects.domain.ScheduleSettings import ScheduleSettings

        return ScheduleSettings.list_schedules_result(session_mgr, settings)


    @staticmethod
    def save_schedule_result(
        data: Dict[str, Any],
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.ScheduleSettings import ScheduleSettings

        return ScheduleSettings.save_schedule_result(data, session_mgr, settings)


    @staticmethod
    def get_schedule_history_result(
        task_type: str,
        domain_name: str,
        session_mgr: SessionManager,
        settings: Settings,
        *,
        target_key: str = "",
    ) -> Dict[str, Any]:
        from back.objects.domain.ScheduleSettings import ScheduleSettings

        return ScheduleSettings.get_schedule_history_result(task_type, domain_name, session_mgr, settings, target_key=target_key)


    @staticmethod
    def get_build_runs_result(
        domain_name: str,
        session_mgr: SessionManager,
        settings: Settings,
        *,
        version: Optional[str] = None,
        limit: int = 100,
    ) -> Dict[str, Any]:
        from back.objects.domain.ScheduleSettings import ScheduleSettings

        return ScheduleSettings.get_build_runs_result(domain_name, session_mgr, settings, version=version, limit=limit)


    @staticmethod
    def _all_runs_result(
        kind: str,
        session_mgr: SessionManager,
        settings: Settings,
        *,
        folder: Optional[str],
        limit: int,
        offset: int,
    ) -> Dict[str, Any]:
        from back.objects.domain.ScheduleSettings import ScheduleSettings

        return ScheduleSettings._all_runs_result(kind, session_mgr, settings, folder=folder, limit=limit, offset=offset)


    @staticmethod
    def get_all_build_runs_result(
        session_mgr: SessionManager,
        settings: Settings,
        *,
        folder: Optional[str] = None,
        limit: int = 25,
        offset: int = 0,
    ) -> Dict[str, Any]:
        from back.objects.domain.ScheduleSettings import ScheduleSettings

        return ScheduleSettings.get_all_build_runs_result(session_mgr, settings, folder=folder, limit=limit, offset=offset)


    @staticmethod
    def get_all_analytics_runs_result(
        session_mgr: SessionManager,
        settings: Settings,
        *,
        folder: Optional[str] = None,
        limit: int = 25,
        offset: int = 0,
    ) -> Dict[str, Any]:
        from back.objects.domain.ScheduleSettings import ScheduleSettings

        return ScheduleSettings.get_all_analytics_runs_result(session_mgr, settings, folder=folder, limit=limit, offset=offset)


    @staticmethod
    def get_build_analytics_result(
        domain_name: str,
        session_mgr: SessionManager,
        settings: Settings,
        *,
        version: Optional[str] = None,
    ) -> Dict[str, Any]:
        from back.objects.domain.ScheduleSettings import ScheduleSettings

        return ScheduleSettings.get_build_analytics_result(domain_name, session_mgr, settings, version=version)


    @staticmethod
    def scheduler_status_payload() -> Dict[str, Any]:
        from back.objects.domain.ScheduleSettings import ScheduleSettings

        return ScheduleSettings.scheduler_status_payload()


    @staticmethod
    def delete_schedule_result(
        task_type: str,
        domain_name: str,
        session_mgr: SessionManager,
        settings: Settings,
        *,
        target_key: str = "",
    ) -> Dict[str, Any]:
        from back.objects.domain.ScheduleSettings import ScheduleSettings

        return ScheduleSettings.delete_schedule_result(task_type, domain_name, session_mgr, settings, target_key=target_key)


    @staticmethod
    def trigger_schedule_now_result(
        task_type: str,
        domain_name: str,
        session_mgr: SessionManager,
        settings: Settings,
        *,
        target_key: str = "",
    ) -> Dict[str, Any]:
        from back.objects.domain.ScheduleSettings import ScheduleSettings

        return ScheduleSettings.trigger_schedule_now_result(task_type, domain_name, session_mgr, settings, target_key=target_key)


    @staticmethod
    def list_cohort_rules_for_domain_result(
        domain_name: str,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.ScheduleSettings import ScheduleSettings

        return ScheduleSettings.list_cohort_rules_for_domain_result(domain_name, session_mgr, settings)


    # Public alias so callers/tests keep ``SettingsService.OBX_MAX_BYTES``.
    OBX_MAX_BYTES = 50 * 1024 * 1024

    @staticmethod
    def _resolve_versions_for_export(
        svc: RegistryService,
        folder: str,
        mode: str,
        explicit: Optional[List[str]],
    ) -> List[str]:
        from back.objects.domain.ObxSettings import ObxSettings

        return ObxSettings._resolve_versions_for_export(svc, folder, mode, explicit)


    @staticmethod
    def export_registry_obx_result(
        spec: Dict[str, Any],
        session_mgr: SessionManager,
        settings: Settings,
        exported_by: str = "",
    ) -> Dict[str, Any]:
        from back.objects.domain.ObxSettings import ObxSettings

        return ObxSettings.export_registry_obx_result(spec, session_mgr, settings, exported_by)


    @staticmethod
    def _decode_obx_payload(file_bytes: bytes) -> Dict[str, Any]:
        from back.objects.domain.ObxSettings import ObxSettings

        return ObxSettings._decode_obx_payload(file_bytes)


    @staticmethod
    def _camelcase_import_name(value: str) -> str:
        from back.objects.domain.ObxSettings import ObxSettings

        return ObxSettings._camelcase_import_name(value)


    @staticmethod
    def _suggest_import_name(svc: RegistryService, display_name: str) -> str:
        from back.objects.domain.ObxSettings import ObxSettings

        return ObxSettings._suggest_import_name(svc, display_name)


    @staticmethod
    def _prepare_renamed_version_doc(
        doc: Dict[str, Any],
        version: str,
        display_name: str,
        base_uri: str,
    ) -> Dict[str, Any]:
        from back.objects.domain.ObxSettings import ObxSettings

        return ObxSettings._prepare_renamed_version_doc(doc, version, display_name, base_uri)


    @staticmethod
    def preview_obx_import_result(
        file_bytes: bytes,
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.ObxSettings import ObxSettings

        return ObxSettings.preview_obx_import_result(file_bytes, session_mgr, settings)


    @staticmethod
    def import_registry_obx_result(
        file_bytes: bytes,
        decisions: List[Dict[str, Any]],
        session_mgr: SessionManager,
        settings: Settings,
    ) -> Dict[str, Any]:
        from back.objects.domain.ObxSettings import ObxSettings

        return ObxSettings.import_registry_obx_result(file_bytes, decisions, session_mgr, settings)

