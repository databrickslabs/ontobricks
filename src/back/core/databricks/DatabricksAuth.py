"""Databricks authentication and host resolution.

Centralises OAuth (Databricks Apps), PAT (local dev with tokens) and
Databricks CLI profile authentication (local dev where token generation
is blocked) so that every service class in this package can share a
single ``DatabricksAuth`` instance instead of duplicating credential logic.
"""

import os
import threading
import time
from typing import Any, Dict, Optional, Tuple

from back.core.logging import get_logger
from back.core.errors import ValidationError

from .constants import (
    _OAUTH_TOKEN_TTL,
    _SQL_SOCKET_TIMEOUT,
)
from shared.config.constants import HTTP_USER_AGENT

logger = get_logger(__name__)
_CLOUD_FETCH_PROBE_TTL_SECONDS = 300
_CLOUD_FETCH_PROBE_TIMEOUT_SECONDS = 8


class DatabricksAuth:
    """Shared authentication context for all Databricks service classes.

    Supports three modes, resolved in priority order:

    1. **Databricks Apps** — M2M OAuth via ``DATABRICKS_CLIENT_ID`` /
       ``DATABRICKS_CLIENT_SECRET`` (auto-injected by the platform).
    2. **Local Personal Access Token** — ``DATABRICKS_TOKEN``.
    3. **Local Databricks CLI** — a profile in ``~/.databrickscfg`` populated
       by ``databricks auth login``. Selected explicitly via
       ``DATABRICKS_CONFIG_PROFILE`` or implicitly via the default profile.
    """

    # Class-level cache: { (host, warehouse_id): (capable, reason, ts) }
    _cloud_fetch_cache: Dict[Tuple[str, str], Tuple[bool, str, float]] = {}
    _cloud_fetch_resolve_lock = threading.RLock()
    _resolving_cloud_fetch: bool = False

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
        with DatabricksAuth._cloud_fetch_resolve_lock:
            if DatabricksAuth._resolving_cloud_fetch:
                return True
            DatabricksAuth._resolving_cloud_fetch = True
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
                return bool(
                    global_config_service.get_use_cloud_fetch(host, token, registry_cfg)
                )
            except Exception as exc:  # noqa: BLE001 - best-effort default resolution
                logger.debug(
                    "Could not resolve global CloudFetch setting, defaulting to enabled: %s",
                    exc,
                )
                return True
            finally:
                DatabricksAuth._resolving_cloud_fetch = False

    @staticmethod
    def get_workspace_host() -> str:
        """Resolve the Databricks workspace host URL.

        Checks ``DATABRICKS_HOST`` first, then falls back to the Databricks
        SDK auto-detection (works inside Databricks Apps and for users who
        have run ``databricks auth login``).
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

    @staticmethod
    def _resolve_cli_config(profile: str, host: str) -> Optional[Any]:
        """Build an SDK ``Config`` for Databricks CLI profile auth.

        Returns ``None`` when no CLI profile is usable (e.g. ``~/.databrickscfg``
        is missing or the named profile does not exist). When *profile* was
        explicitly requested via ``DATABRICKS_CONFIG_PROFILE`` and resolution
        fails, the failure is logged at WARNING so a typo surfaces — the
        implicit default-profile path stays at DEBUG so users who never ran
        ``databricks auth login`` aren't spammed with noise.
        """
        try:
            from databricks.sdk.core import Config

            kwargs: Dict[str, Any] = {}
            if profile:
                kwargs["profile"] = profile
            if host:
                kwargs["host"] = host
            cfg = Config(**kwargs)
            if not cfg.host:
                return None
            return cfg
        except Exception as exc:  # noqa: BLE001 - vendor surface, fall through
            if profile:
                logger.warning(
                    "DATABRICKS_CONFIG_PROFILE=%r did not resolve: %s",
                    profile,
                    exc,
                )
            else:
                logger.debug("Could not resolve default CLI profile: %s", exc)
            return None

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
        self.config_profile = os.getenv("DATABRICKS_CONFIG_PROFILE", "").strip()

        explicit_host = DatabricksAuth.normalize_host(host) if host else ""

        self._cli_config: Optional[Any] = None
        if not self.is_app_mode and not self.token:
            self._cli_config = self._resolve_cli_config(
                self.config_profile, explicit_host
            )

        if explicit_host:
            self.host = explicit_host
        elif self._cli_config is not None and self._cli_config.host:
            self.host = DatabricksAuth.normalize_host(self._cli_config.host)
        else:
            self.host = self.get_workspace_host()

        if use_cloud_fetch is None:
            self.use_cloud_fetch = self._resolve_global_cloud_fetch_default(
                self.host, self.token
            )
        else:
            self.use_cloud_fetch = bool(use_cloud_fetch)

        if self.auth_mode == "cli":
            logger.info(
                "DatabricksAuth init — host=%s, mode=cli, profile=%s, warehouse=%s",
                self.host,
                self.cli_profile_name,
                self.warehouse_id,
            )
        else:
            logger.info(
                "DatabricksAuth init — host=%s, mode=%s, warehouse=%s",
                self.host,
                self.auth_mode,
                self.warehouse_id,
            )

    @property
    def auth_mode(self) -> str:
        """Resolved auth mode: ``"app"``, ``"pat"``, ``"cli"``, or ``"none"``."""
        if self.is_app_mode and self.client_id and self.client_secret:
            return "app"
        if self.token:
            return "pat"
        if self._cli_config is not None:
            return "cli"
        return "none"

    @property
    def cli_profile_name(self) -> str:
        """Resolved CLI profile name (``"default"`` when unspecified)."""
        return self.config_profile or "default"

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
                headers={"User-Agent": HTTP_USER_AGENT},
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
                "User-Agent": HTTP_USER_AGENT,
            }
        if self.token:
            return {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "User-Agent": HTTP_USER_AGENT,
            }
        if self._cli_config is not None:
            sdk_headers = self._cli_config.authenticate()
            headers = {
                "Content-Type": "application/json",
                "User-Agent": HTTP_USER_AGENT,
            }
            headers.update(sdk_headers)
            return headers
        return {"User-Agent": HTTP_USER_AGENT}

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
        elif self._cli_config is not None:
            params["credentials_provider"] = lambda: self._cli_config.authenticate
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
            elif self._cli_config is not None:
                probe_params["credentials_provider"] = (
                    lambda: self._cli_config.authenticate
                )

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
        if self.token:
            return True
        return self._cli_config is not None

    def get_bearer_token(self) -> str:
        """Return the current bearer token (PAT, OAuth, or CLI profile)."""
        if self.token:
            return self.token
        pat = os.getenv("DATABRICKS_TOKEN", "")
        if pat:
            return pat
        if self.is_app_mode:
            return self.get_oauth_token()
        if self._cli_config is not None:
            headers = self._cli_config.authenticate()
            authz = headers.get("Authorization", "")
            if authz.startswith("Bearer "):
                return authz[len("Bearer ") :]
        return ""
