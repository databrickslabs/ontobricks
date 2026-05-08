"""Databricks authentication and host resolution.

Centralises OAuth (Databricks Apps) and PAT (local dev) authentication
so that every service class in this package can share a single
``DatabricksAuth`` instance instead of duplicating credential logic.
"""

import os
import time
from typing import Dict, Optional, Tuple

from back.core.logging import get_logger
from back.core.errors import ValidationError

from .constants import (
    _OAUTH_TOKEN_TTL,
    _SQL_SOCKET_TIMEOUT,
)

logger = get_logger(__name__)
_CLOUD_FETCH_PROBE_TTL_SECONDS = 300
_CLOUD_FETCH_PROBE_TIMEOUT_SECONDS = 8


class DatabricksAuth:
    """Shared authentication context for all Databricks service classes.

    Supports two modes:

    1. **Databricks Apps** — M2M OAuth via ``DATABRICKS_CLIENT_ID`` /
       ``DATABRICKS_CLIENT_SECRET``.
    2. **Local development** — Personal Access Token (``DATABRICKS_TOKEN``).
    """

    # Class-level cache: { (host, warehouse_id): (capable, reason, ts) }
    _cloud_fetch_cache: Dict[Tuple[str, str], Tuple[bool, str, float]] = {}

    @staticmethod
    def is_databricks_app() -> bool:
        """Return *True* when running inside a Databricks App.

        The platform sets ``DATABRICKS_APP_PORT`` automatically.
        """
        return os.getenv("DATABRICKS_APP_PORT") is not None

    @staticmethod
    def normalize_host(host: str) -> str:
        """Ensure *host* has an ``https://`` scheme and no trailing slash."""
        if not host:
            return ""
        host = host.strip()
        if not host.startswith("http://") and not host.startswith("https://"):
            host = f"https://{host}"
        return host.rstrip("/")

    @staticmethod
    def _resolve_global_cloud_fetch_default(host: str, token: str) -> bool:
        """Best-effort load of global CloudFetch setting (default: enabled)."""
        try:
            from shared.config.settings import get_settings
            from back.objects.registry import RegistryCfg
            from back.objects.session import global_config_service

            settings = get_settings()
            registry_cfg = RegistryCfg.from_domain(None, settings).as_dict()
            if not host or not registry_cfg.get("catalog") or not registry_cfg.get(
                "schema"
            ):
                return True
            return bool(global_config_service.get_use_cloud_fetch(host, token, registry_cfg))
        except Exception as exc:  # noqa: BLE001 - best-effort default resolution
            logger.debug(
                "Could not resolve global CloudFetch setting, defaulting to enabled: %s",
                exc,
            )
            return True

    @staticmethod
    def get_workspace_host() -> str:
        """Resolve the Databricks workspace host URL.

        Checks ``DATABRICKS_HOST`` first, then falls back to the Databricks
        SDK auto-detection (works inside Databricks Apps).
        """
        host = os.getenv("DATABRICKS_HOST", "")
        if host:
            return DatabricksAuth.normalize_host(host)

        try:
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            if w and w.config and w.config.host:
                return DatabricksAuth.normalize_host(w.config.host)
            return ""
        except AttributeError as exc:
            logger.debug("SDK HTTP client error during host detection: %s", exc)
            return ""
        except Exception as exc:
            logger.debug("Could not auto-detect host: %s", exc)
            return ""

    def __init__(
        self,
        host: Optional[str] = None,
        token: Optional[str] = None,
        warehouse_id: Optional[str] = None,
        use_cloud_fetch: Optional[bool] = None,
    ) -> None:
        self.token = token or os.getenv("DATABRICKS_TOKEN", "")
        self.warehouse_id = (
            warehouse_id
            or os.getenv("DATABRICKS_SQL_WAREHOUSE_ID", "")
            or os.getenv("DATABRICKS_SQL_WAREHOUSE_ID_DEFAULT", "")
        )
        self._oauth_token: Optional[str] = None
        self._oauth_token_ts: float = 0.0

        self.client_id = os.getenv("DATABRICKS_CLIENT_ID", "")
        self.client_secret = os.getenv("DATABRICKS_CLIENT_SECRET", "")
        self.is_app_mode = self.is_databricks_app()

        self.host = (
            DatabricksAuth.normalize_host(host) if host else self.get_workspace_host()
        )
        if use_cloud_fetch is None:
            self.use_cloud_fetch = self._resolve_global_cloud_fetch_default(
                self.host, self.token
            )
        else:
            self.use_cloud_fetch = bool(use_cloud_fetch)

        logger.info(
            "DatabricksAuth init — host=%s, app_mode=%s, warehouse=%s",
            self.host,
            self.is_app_mode,
            self.warehouse_id,
        )

    def get_oauth_token(self) -> str:
        """Obtain (or return cached) M2M OAuth access token.

        The token is cached for ``_OAUTH_TOKEN_TTL`` seconds.
        """
        now = time.time()
        if self._oauth_token and (now - self._oauth_token_ts) < _OAUTH_TOKEN_TTL:
            return self._oauth_token

        import requests

        if not self.host:
            raise ValidationError("DATABRICKS_HOST is not configured")

        host = DatabricksAuth.normalize_host(self.host)
        token_url = f"{host}/oidc/v1/token"
        logger.info("Requesting OAuth token from: %s", token_url)

        try:
            response = requests.post(
                token_url,
                data={"grant_type": "client_credentials", "scope": "all-apis"},
                auth=(self.client_id, self.client_secret),
                timeout=5,
            )
            response.raise_for_status()
            token_data = response.json()
            self._oauth_token = token_data["access_token"]
            self._oauth_token_ts = time.time()
            logger.info("OAuth token obtained and cached")
            return self._oauth_token
        except requests.exceptions.RequestException as exc:
            logger.error("Error getting token: %s", exc)
            if hasattr(exc, "response") and exc.response is not None:
                logger.error("Response: %s", exc.response.text)
            raise

    def get_auth_headers(self) -> dict:
        """Return ``Authorization`` + ``Content-Type`` headers for REST calls."""
        if self.is_app_mode and self.client_id and self.client_secret:
            token = self.get_oauth_token()
            return {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
        if self.token:
            return {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            }
        return {}

    def get_sql_connection_params(self) -> dict:
        """Return kwargs suitable for ``databricks.sql.connect()``.

        ``use_cloud_fetch`` reflects the latest cached capability probe
        (see :meth:`probe_cloud_fetch_capability`). When no probe has run
        yet, it follows global settings (enabled by default) and
        prerequisite checks.
        """
        server_hostname = self.host.replace("https://", "").replace("http://", "")
        params: dict = {
            "server_hostname": server_hostname,
            "http_path": f"/sql/1.0/warehouses/{self.warehouse_id}",
            "_socket_timeout": _SQL_SOCKET_TIMEOUT,
        }
        params["use_cloud_fetch"] = self.can_use_cloud_fetch()
        if self.is_app_mode and self.client_id and self.client_secret:
            params["access_token"] = self.get_oauth_token()
        elif self.token:
            params["access_token"] = self.token
        return params

    @staticmethod
    def _env_true(name: str) -> bool:
        return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}

    def _cloud_fetch_prerequisites(self) -> Tuple[bool, str]:
        if not self.host:
            return False, "host is missing"
        if not self.warehouse_id:
            return False, "warehouse_id is missing"
        if not self.has_valid_auth():
            return False, "credentials are missing"
        try:
            import pyarrow  # noqa: F401
        except Exception as exc:  # noqa: BLE001 - optional dependency probe
            return False, f"pyarrow unavailable: {exc}"
        return True, "prerequisites ok"

    def cloud_fetch_status(self, force: bool = False) -> Tuple[bool, str]:
        """Return ``(capable, reason)`` for CloudFetch in the current runtime.

        Cached per ``(host, warehouse_id)`` for
        ``_CLOUD_FETCH_PROBE_TTL_SECONDS``. Pass ``force=True`` to bypass
        the cache (used by ``/health``).
        """
        if self._env_true("DATABRICKS_DISABLE_CLOUD_FETCH"):
            return False, "Disabled by DATABRICKS_DISABLE_CLOUD_FETCH"
        if self._env_true("DATABRICKS_FORCE_CLOUD_FETCH"):
            return True, "Forced by DATABRICKS_FORCE_CLOUD_FETCH"
        if not self.use_cloud_fetch:
            return False, "Disabled by global settings"

        ok, msg = self._cloud_fetch_prerequisites()
        if not ok:
            return False, msg

        key = (self.host, self.warehouse_id)
        now = time.time()
        cached = DatabricksAuth._cloud_fetch_cache.get(key)
        if not force and cached and (now - cached[2]) < _CLOUD_FETCH_PROBE_TTL_SECONDS:
            return cached[0], cached[1]

        return self.probe_cloud_fetch_capability()

    def can_use_cloud_fetch(self) -> bool:
        """Return whether CloudFetch should be enabled for SQL params.

        Reads the cached probe outcome if any, otherwise falls back to a
        settings-driven default (enabled unless explicitly disabled)
        without triggering a probe — so building connection params stays a
        cheap, side-effect-free operation.
        """
        if self._env_true("DATABRICKS_DISABLE_CLOUD_FETCH"):
            return False
        if self._env_true("DATABRICKS_FORCE_CLOUD_FETCH"):
            return True
        if not self.use_cloud_fetch:
            return False

        ok, _ = self._cloud_fetch_prerequisites()
        if not ok:
            return False

        key = (self.host, self.warehouse_id)
        cached = DatabricksAuth._cloud_fetch_cache.get(key)
        if cached and (time.time() - cached[2]) < _CLOUD_FETCH_PROBE_TTL_SECONDS:
            return cached[0]

        return True

    def probe_cloud_fetch_capability(self) -> Tuple[bool, str]:
        """Issue a tiny ``SELECT 1`` with ``use_cloud_fetch=True`` and cache the outcome.

        Returns ``(capable, reason)``. The result is cached at the class
        level for ``_CLOUD_FETCH_PROBE_TTL_SECONDS`` so subsequent SQL
        connections can read the verdict cheaply.
        """
        if self._env_true("DATABRICKS_DISABLE_CLOUD_FETCH"):
            return False, "Disabled by DATABRICKS_DISABLE_CLOUD_FETCH"
        if self._env_true("DATABRICKS_FORCE_CLOUD_FETCH"):
            return True, "Forced by DATABRICKS_FORCE_CLOUD_FETCH"
        if not self.use_cloud_fetch:
            return False, "Disabled by global settings"

        prereq_ok, prereq_msg = self._cloud_fetch_prerequisites()
        if not prereq_ok:
            self._record_cloud_fetch(False, prereq_msg)
            return False, prereq_msg

        try:
            from databricks import sql

            probe_params = {
                "server_hostname": self.host.replace("https://", "").replace(
                    "http://", ""
                ),
                "http_path": f"/sql/1.0/warehouses/{self.warehouse_id}",
                "_socket_timeout": _CLOUD_FETCH_PROBE_TIMEOUT_SECONDS,
                "use_cloud_fetch": True,
            }
            if self.is_app_mode and self.client_id and self.client_secret:
                probe_params["access_token"] = self.get_oauth_token()
            elif self.token:
                probe_params["access_token"] = self.token

            with sql.connect(**probe_params) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchall()
            msg = "Probe SELECT 1 succeeded with use_cloud_fetch=True"
            self._record_cloud_fetch(True, msg)
            logger.info("CloudFetch probe: capable (%s)", msg)
            return True, msg
        except Exception as exc:  # noqa: BLE001 - vendor/network surface
            msg = f"Probe SELECT 1 failed with use_cloud_fetch=True: {exc}"
            self._record_cloud_fetch(False, msg)
            logger.info("CloudFetch probe: not capable (%s)", msg)
            return False, msg

    def _record_cloud_fetch(self, capable: bool, reason: str) -> None:
        DatabricksAuth._cloud_fetch_cache[(self.host, self.warehouse_id)] = (
            capable,
            reason,
            time.time(),
        )

    def has_valid_auth(self) -> bool:
        """Return *True* when usable credentials are available."""
        if self.is_app_mode:
            return bool(self.client_id and self.client_secret)
        return bool(self.token)

    def get_bearer_token(self) -> str:
        """Return the current bearer token (PAT or OAuth)."""
        if self.token:
            return self.token
        pat = os.getenv("DATABRICKS_TOKEN", "")
        if pat:
            return pat
        if self.is_app_mode:
            return self.get_oauth_token()
        return ""
