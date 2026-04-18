"""Databricks authentication and host resolution.

Centralises OAuth (Databricks Apps) and PAT (local dev) authentication
so that every service class in this package can share a single
``DatabricksAuth`` instance instead of duplicating credential logic.
"""

import os
import time
from typing import Optional

from back.core.logging import get_logger
from back.core.errors import ValidationError

from .constants import (
    _OAUTH_TOKEN_TTL,
    _SQL_SOCKET_TIMEOUT,
)

logger = get_logger(__name__)


class DatabricksAuth:
    """Shared authentication context for all Databricks service classes.

    Supports two modes:

    1. **Databricks Apps** — M2M OAuth via ``DATABRICKS_CLIENT_ID`` /
       ``DATABRICKS_CLIENT_SECRET``.
    2. **Local development** — Personal Access Token (``DATABRICKS_TOKEN``).
    """

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
        """Return kwargs suitable for ``databricks.sql.connect()``."""
        server_hostname = self.host.replace("https://", "").replace("http://", "")
        params: dict = {
            "server_hostname": server_hostname,
            "http_path": f"/sql/1.0/warehouses/{self.warehouse_id}",
            "_socket_timeout": _SQL_SOCKET_TIMEOUT,
        }
        if self.is_app_mode and self.client_id and self.client_secret:
            params["access_token"] = self.get_oauth_token()
        elif self.token:
            params["access_token"] = self.token
        return params

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
