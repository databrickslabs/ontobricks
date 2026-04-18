import asyncio
import os
from functools import partial
from typing import Any, Callable, Dict, Tuple

import back.core.databricks as _databricks
from back.core.errors import ValidationError
from back.core.logging import get_logger
from shared.config.constants import DEFAULT_BASE_URI

logger = get_logger(__name__)


def make_volume_file_service(domain, settings=None):
    """Return :class:`VolumeFileService` using host/token from ``get_databricks_host_and_token``."""
    from back.core.databricks import VolumeFileService
    from shared.config.settings import get_settings as _get_settings

    resolved = settings if settings is not None else _get_settings()
    host, token = DatabricksHelpers.get_databricks_host_and_token(domain, resolved)
    return VolumeFileService(host=host, token=token)


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
    def _resolve_registry_cfg(domain, settings) -> Dict[str, str]:
        """Build registry config dict from domain session and env-var defaults.

        Legacy wrapper — new code should use ``RegistryCfg.from_domain`` directly.
        """
        from back.objects.registry import RegistryCfg

        return RegistryCfg.from_domain(domain, settings).as_dict()

    @staticmethod
    def resolve_warehouse_id(domain, settings) -> str:
        """Resolve the SQL Warehouse ID using a layered fallback strategy.

        Resolution order:

        1. **Global config** (``.global_config.json`` in the registry UC Volume)
           -- set by admins via the Settings page, shared across all users.
        2. **Session** (``domain.databricks['warehouse_id']``) -- stored when
           the user selects a warehouse before the registry is configured.
        3. **Pydantic Settings** (``settings.sql_warehouse_id``) -- loaded from
           the ``DATABRICKS_SQL_WAREHOUSE_ID`` env var / ``app.yaml``.
        4. **Default env var** (``DATABRICKS_SQL_WAREHOUSE_ID_DEFAULT``) --
           static fallback defined in ``app.yaml`` for MCP / session-less calls.

        Args:
            domain: DomainSession instance
            settings: Settings instance from FastAPI

        Returns:
            The warehouse ID string (empty if none of the sources provide one).
        """
        from back.objects.session import global_config_service

        host, token = DatabricksHelpers.get_databricks_host_and_token(domain, settings)
        registry_cfg = DatabricksHelpers._resolve_registry_cfg(domain, settings)

        if host and registry_cfg.get("catalog") and registry_cfg.get("schema"):
            try:
                wid = global_config_service.get_warehouse_id(host, token, registry_cfg)
                if wid:
                    return wid
            except Exception as exc:
                logger.debug("Could not read global warehouse config: %s", exc)

        session_wid = domain.databricks.get("warehouse_id", "")
        if session_wid:
            return session_wid

        if getattr(settings, "sql_warehouse_id", ""):
            return settings.sql_warehouse_id

        return os.getenv("DATABRICKS_SQL_WAREHOUSE_ID_DEFAULT", "")

    @staticmethod
    def _resolve_global_setting(domain, settings, getter_name: str) -> str:
        """Read a single value from the global config (UC Volume), returning '' on failure."""
        from back.objects.session import global_config_service

        host, token = DatabricksHelpers.get_databricks_host_and_token(domain, settings)
        registry_cfg = DatabricksHelpers._resolve_registry_cfg(domain, settings)

        if host and registry_cfg.get("catalog") and registry_cfg.get("schema"):
            try:
                getter = getattr(global_config_service, getter_name)
                val = getter(host, token, registry_cfg)
                if val:
                    return val
            except Exception as exc:
                logger.debug("Could not read global config (%s): %s", getter_name, exc)
        return ""

    @staticmethod
    def resolve_default_base_uri(domain, settings) -> str:
        """Resolve the default ontology base URI domain from global config.

        Falls back to :data:`shared.config.constants.DEFAULT_BASE_URI` (no trailing slash).
        """
        return DatabricksHelpers._resolve_global_setting(
            domain, settings, "get_default_base_uri"
        ) or DEFAULT_BASE_URI.rstrip("/")

    @staticmethod
    def resolve_default_emoji(domain, settings) -> str:
        """Resolve the default class icon from global config.

        Falls back to the hard-coded default ``📦``.
        """
        return (
            DatabricksHelpers._resolve_global_setting(
                domain, settings, "get_default_emoji"
            )
            or "📦"
        )

    @staticmethod
    def get_databricks_client(domain, settings):
        """Get Databricks client from domain session or settings.

        In Databricks Apps mode, the SDK handles authentication automatically,
        so we don't need explicit host/token.

        Args:
            domain: DomainSession instance
            settings: Settings instance from FastAPI

        Returns:
            DatabricksClient instance or None if not configured
        """
        host = domain.databricks.get("host") or settings.databricks_host
        token = domain.databricks.get("token") or settings.databricks_token
        warehouse_id = DatabricksHelpers.resolve_warehouse_id(domain, settings)

        # In Databricks Apps mode, always create a client (SDK handles auth)
        if _databricks.is_databricks_app():
            return _databricks.DatabricksClient(
                host=host, token=token, warehouse_id=warehouse_id
            )

        if host and token:
            return _databricks.DatabricksClient(
                host=host, token=token, warehouse_id=warehouse_id
            )

        return None

    @staticmethod
    def get_databricks_credentials(domain, settings) -> Tuple[str, str, str]:
        """Get Databricks credentials from domain session or settings.

        Falls back to OAuth token resolution in Databricks App mode.

        Args:
            domain: DomainSession instance
            settings: Settings instance from FastAPI

        Returns:
            Tuple of (host, token, warehouse_id)
        """
        host, token = DatabricksHelpers.get_databricks_host_and_token(domain, settings)
        warehouse_id = DatabricksHelpers.resolve_warehouse_id(domain, settings)
        return host, token, warehouse_id

    @staticmethod
    def get_databricks_host_and_token(domain, settings) -> Tuple[str, str]:
        """Get only host and token from domain session or settings.

        In Databricks App mode, auto-resolves the host via the SDK and
        obtains a short-lived OAuth token when explicit credentials are
        not stored in the domain session or environment.

        Args:
            domain: DomainSession instance
            settings: Settings instance from FastAPI

        Returns:
            Tuple of (host, token)
        """
        host = domain.databricks.get("host") or settings.databricks_host
        token = domain.databricks.get("token") or settings.databricks_token

        if host and token:
            return _databricks.normalize_host(host), token

        if _databricks.is_databricks_app():
            if not host:
                host = _databricks.get_workspace_host()
            if not token and host:
                try:
                    client = _databricks.DatabricksClient(host=host)
                    token = client.get_oauth_token()
                    logger.debug("Obtained OAuth token for agent call (host=%s)", host)
                except Exception as exc:
                    logger.warning("Could not obtain OAuth token in app mode: %s", exc)

        return _databricks.normalize_host(host), token

    @staticmethod
    def require_serving_llm(
        domain,
        settings,
    ) -> Tuple[str, str, str]:
        """Validate host, token, and domain LLM serving endpoint.

        Returns ``(host, token, endpoint_name)`` or raises :class:`ValidationError`.
        """
        host, token = DatabricksHelpers.get_databricks_host_and_token(domain, settings)
        if not host or not token:
            raise ValidationError("Databricks credentials not configured")
        endpoint = (domain.info or {}).get("llm_endpoint", "") or ""
        if not endpoint:
            raise ValidationError(
                "No LLM serving endpoint configured. Please set it in Domain Settings.",
            )
        return host, token, endpoint


def resolve_ladybug_local_path(domain, db_name: str) -> str:
    """Return the local file-system path for a LadybugDB archive."""
    from back.core.graphdb.ladybugdb import local_db_path
    from shared.config.constants import DEFAULT_LADYBUG_PATH

    return local_db_path(db_name, DEFAULT_LADYBUG_PATH)


def effective_uc_version_path(domain) -> str:
    """Return the version-scoped UC Volume path, with domain-level fallback.

    Prefers ``uc_version_path`` (``/Volumes/.../V{N}``) but falls back to
    ``uc_domain_path`` (``/Volumes/.../domains/{name}``) for legacy layouts,
    and returns an empty string when neither is available.
    """
    return (
        getattr(domain, "uc_version_path", "")
        or getattr(domain, "uc_domain_path", "")
        or ""
    )
