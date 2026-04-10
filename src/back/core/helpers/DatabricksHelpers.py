import asyncio
import os
from functools import partial
from typing import Any, Callable, Dict, Optional, Tuple

import back.core.databricks as _databricks
from back.core.logging import get_logger

logger = get_logger(__name__)


class DatabricksHelpers:
    @staticmethod
    async def run_blocking(func: Callable, *args: Any, **kwargs: Any) -> Any:
        """Run a blocking function in a thread pool so it doesn't freeze the event loop.

        Usage in an ``async def`` route handler::

            result = await run_blocking(client.execute_query, sql)
        """
        loop = asyncio.get_running_loop()
        call = partial(func, *args, **kwargs) if kwargs else partial(func, *args)
        return await loop.run_in_executor(None, call)

    @staticmethod
    def _resolve_registry_cfg(project, settings) -> Dict[str, str]:
        """Build registry config dict from project session and env-var defaults.

        Legacy wrapper — new code should use ``RegistryCfg.from_project`` directly.
        """
        from back.objects.registry import RegistryCfg

        return RegistryCfg.from_project(project, settings).as_dict()

    @staticmethod
    def resolve_warehouse_id(project, settings) -> str:
        """Resolve the SQL Warehouse ID using a layered fallback strategy.

        Resolution order:

        1. **Global config** (``.global_config.json`` in the registry UC Volume)
           -- set by admins via the Settings page, shared across all users.
        2. **Session** (``project.databricks['warehouse_id']``) -- stored when
           the user selects a warehouse before the registry is configured.
        3. **Pydantic Settings** (``settings.sql_warehouse_id``) -- loaded from
           the ``DATABRICKS_SQL_WAREHOUSE_ID`` env var / ``app.yaml``.
        4. **Default env var** (``DATABRICKS_SQL_WAREHOUSE_ID_DEFAULT``) --
           static fallback defined in ``app.yaml`` for MCP / session-less calls.

        Args:
            project: ProjectSession instance
            settings: Settings instance from FastAPI

        Returns:
            The warehouse ID string (empty if none of the sources provide one).
        """
        from back.objects.session import global_config_service

        host, token = DatabricksHelpers.get_databricks_host_and_token(project, settings)
        registry_cfg = DatabricksHelpers._resolve_registry_cfg(project, settings)

        if host and registry_cfg.get('catalog') and registry_cfg.get('schema'):
            try:
                wid = global_config_service.get_warehouse_id(host, token, registry_cfg)
                if wid:
                    return wid
            except Exception as exc:
                logger.debug("Could not read global warehouse config: %s", exc)

        session_wid = project.databricks.get('warehouse_id', '')
        if session_wid:
            return session_wid

        if getattr(settings, 'sql_warehouse_id', ''):
            return settings.sql_warehouse_id

        return os.getenv("DATABRICKS_SQL_WAREHOUSE_ID_DEFAULT", "")

    @staticmethod
    def _resolve_global_setting(project, settings, getter_name: str) -> str:
        """Read a single value from the global config (UC Volume), returning '' on failure."""
        from back.objects.session import global_config_service

        host, token = DatabricksHelpers.get_databricks_host_and_token(project, settings)
        registry_cfg = DatabricksHelpers._resolve_registry_cfg(project, settings)

        if host and registry_cfg.get('catalog') and registry_cfg.get('schema'):
            try:
                getter = getattr(global_config_service, getter_name)
                val = getter(host, token, registry_cfg)
                if val:
                    return val
            except Exception as exc:
                logger.debug("Could not read global config (%s): %s", getter_name, exc)
        return ""

    @staticmethod
    def resolve_default_base_uri(project, settings) -> str:
        """Resolve the default ontology base URI domain from global config.

        Falls back to the hard-coded default ``https://databricks-ontology.com``.
        """
        return (
            DatabricksHelpers._resolve_global_setting(project, settings, "get_default_base_uri")
            or "https://databricks-ontology.com"
        )

    @staticmethod
    def resolve_default_emoji(project, settings) -> str:
        """Resolve the default class icon from global config.

        Falls back to the hard-coded default ``📦``.
        """
        return (
            DatabricksHelpers._resolve_global_setting(project, settings, "get_default_emoji")
            or "📦"
        )

    @staticmethod
    def get_databricks_client(project, settings):
        """Get Databricks client from project session or settings.

        In Databricks Apps mode, the SDK handles authentication automatically,
        so we don't need explicit host/token.

        Args:
            project: ProjectSession instance
            settings: Settings instance from FastAPI

        Returns:
            DatabricksClient instance or None if not configured
        """
        host = project.databricks.get('host') or settings.databricks_host
        token = project.databricks.get('token') or settings.databricks_token
        warehouse_id = DatabricksHelpers.resolve_warehouse_id(project, settings)

        # In Databricks Apps mode, always create a client (SDK handles auth)
        if _databricks.is_databricks_app():
            return _databricks.DatabricksClient(host=host, token=token, warehouse_id=warehouse_id)

        if host and token:
            return _databricks.DatabricksClient(host=host, token=token, warehouse_id=warehouse_id)

        return None

    @staticmethod
    def get_databricks_credentials(project, settings) -> Tuple[str, str, str]:
        """Get Databricks credentials from project session or settings.

        Falls back to OAuth token resolution in Databricks App mode.

        Args:
            project: ProjectSession instance
            settings: Settings instance from FastAPI

        Returns:
            Tuple of (host, token, warehouse_id)
        """
        host, token = DatabricksHelpers.get_databricks_host_and_token(project, settings)
        warehouse_id = DatabricksHelpers.resolve_warehouse_id(project, settings)
        return host, token, warehouse_id

    @staticmethod
    def get_databricks_host_and_token(project, settings) -> Tuple[str, str]:
        """Get only host and token from project session or settings.

        In Databricks App mode, auto-resolves the host via the SDK and
        obtains a short-lived OAuth token when explicit credentials are
        not stored in the project session or environment.

        Args:
            project: ProjectSession instance
            settings: Settings instance from FastAPI

        Returns:
            Tuple of (host, token)
        """
        host = project.databricks.get('host') or settings.databricks_host
        token = project.databricks.get('token') or settings.databricks_token

        if host and token:
            return _databricks.normalize_host(host), token

        if _databricks.is_databricks_app():
            if not host:
                host = _databricks.get_workspace_host()
            if not token and host:
                try:
                    client = _databricks.DatabricksClient(host=host)
                    token = client._get_oauth_token()
                    logger.debug("Obtained OAuth token for agent call (host=%s)", host)
                except Exception as exc:
                    logger.warning("Could not obtain OAuth token in app mode: %s", exc)

        return _databricks.normalize_host(host), token

    @staticmethod
    def require_serving_llm(
        project, settings,
    ) -> Tuple[Optional[Dict[str, Any]], Optional[Tuple[str, str, str]]]:
        """Validate host, token, and project LLM serving endpoint.

        Returns ``(error_response, None)`` or ``(None, (host, token, endpoint_name))``.
        ``error_response`` uses ``success`` + ``message`` for JSON routes.
        """
        host, token = DatabricksHelpers.get_databricks_host_and_token(project, settings)
        if not host or not token:
            return ({"success": False, "message": "Databricks credentials not configured"}, None)
        endpoint = (project.info or {}).get("llm_endpoint", "") or ""
        if not endpoint:
            return (
                {
                    "success": False,
                    "message": "No LLM serving endpoint configured. Please set it in Project Settings.",
                },
                None,
            )
        return (None, (host, token, endpoint))
