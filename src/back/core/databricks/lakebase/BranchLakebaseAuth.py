"""Lakebase credentials scoped to one project branch."""

from __future__ import annotations

import os
import time

from back.core.databricks.lakebase.constants import TOKEN_TTL_S as _TOKEN_TTL_S
from back.core.errors import ValidationError
from back.core.logging import get_logger

logger = get_logger(__name__)


class BranchLakebaseAuth:
    """Lakebase auth for an explicit branch resource path.

    Identical interface to :class:`LakebaseAuth` but resolves the Postgres
    host and mints JWTs for a specific branch
    (``projects/<proj>/branches/<branch>``) rather than reading PGHOST from
    the environment.  Use this when the graph engine should target a
    different Lakebase project than the bound registry instance.

    Token caching follows the same ~55-minute TTL as :class:`LakebaseAuth`.
    The SP identity (``PGUSER``) and port (``PGPORT``) are still read from
    the environment — they belong to the app regardless of which project it
    connects to.
    """

    def __init__(self, branch_path: str, database: str = "") -> None:
        # branch_path: full resource path, e.g. "projects/xxx/branches/yyy"
        self._branch_path = branch_path.strip()
        self._database_override = (database or "").strip()
        self._w = None
        self._token: str = ""
        self._token_ts: float = 0.0
        self._host: str = ""
        self._endpoint_resource: str = ""

    # ------------------------------------------------------------------
    # Availability
    # ------------------------------------------------------------------

    @property
    def is_available(self) -> bool:
        if not self._branch_path:
            return False
        try:
            return bool(self._resolved_host())
        except Exception:  # noqa: BLE001
            return False

    # ------------------------------------------------------------------
    # Connection parameters
    # ------------------------------------------------------------------

    @property
    def host(self) -> str:
        h = self._resolved_host()
        if not h:
            raise ValidationError(
                f"Could not resolve Lakebase endpoint host for branch {self._branch_path!r}"
            )
        return h

    @property
    def port(self) -> int:
        return int(os.environ.get("PGPORT", "5432"))

    @property
    def database(self) -> str:
        return self._database_override or os.environ.get("PGDATABASE", "") or "postgres"

    @property
    def user(self) -> str:
        u = os.environ.get("PGUSER", "").strip()
        if not u:
            raise ValidationError(
                "PGUSER is not set — required for Lakebase psycopg connections"
            )
        return u

    @property
    def instance_name(self) -> str:
        """Return the project name (segment after 'projects/' in the resource path)."""
        parts = self._branch_path.split("/")
        # "projects/<proj>/branches/<branch>" → parts[1] = project name
        return parts[1] if len(parts) >= 2 else self._branch_path

    @property
    def branch_name(self) -> str:
        """Return the branch name (last segment of the resource path)."""
        parts = self._branch_path.split("/")
        # "projects/<proj>/branches/<branch>" → parts[-1] = branch name
        return parts[-1] if parts else ""

    @property
    def branch_path(self) -> str:
        """Return the full branch resource path (``projects/<proj>/branches/<branch>``)."""
        return self._branch_path

    # ------------------------------------------------------------------
    # Token minting
    # ------------------------------------------------------------------

    def password(self) -> str:
        now = time.time()
        if self._token and (now - self._token_ts) < _TOKEN_TTL_S:
            return self._token
        _ = self._resolved_host()
        if not self._endpoint_resource:
            raise ValidationError(
                f"No endpoint resolved for branch {self._branch_path!r}; cannot mint JWT"
            )
        self._ensure_workspace()
        api = getattr(self._w, "api_client", None)
        if api is None or not hasattr(api, "do"):
            raise ValidationError("WorkspaceClient.api_client unavailable")
        resp = (
            api.do(
                "POST",
                "/api/2.0/postgres/credentials",
                body={"endpoint": self._endpoint_resource},
            )
            or {}
        )
        token = resp.get("token") or ""
        if not token:
            raise ValidationError(
                f"Failed to mint Lakebase JWT for endpoint {self._endpoint_resource!r}"
            )
        self._token = token
        self._token_ts = now
        return self._token

    def invalidate(self) -> None:
        self._token = ""
        self._token_ts = 0.0

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def kwargs(
        self,
        *,
        application_name: str = "ontobricks",
        connect_timeout: int = 10,
    ) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.user,
            "password": self.password(),
            "sslmode": "require",
            "connect_timeout": connect_timeout,
            "application_name": application_name,
            "keepalives": 1,
            "keepalives_idle": 10,
            "keepalives_interval": 5,
            "keepalives_count": 3,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_workspace(self) -> None:
        if self._w is None:
            from databricks.sdk import WorkspaceClient

            self._w = WorkspaceClient()

    def _resolved_host(self) -> str:
        if self._host:
            return self._host
        if not self._branch_path:
            return ""
        try:
            self._ensure_workspace()
            api = getattr(self._w, "api_client", None)
            if api is None or not hasattr(api, "do"):
                return ""
            endpoints = (
                api.do("GET", f"/api/2.0/postgres/{self._branch_path}/endpoints") or {}
            ).get("endpoints") or []
            for ep in endpoints:
                h = ((ep.get("status") or {}).get("hosts") or {}).get("host", "").strip()
                if h:
                    self._host = h
                    self._endpoint_resource = ep.get("name") or ""
                    logger.info(
                        "BranchLakebaseAuth resolved host %r for branch %r",
                        h,
                        self._branch_path,
                    )
                    return h
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "BranchLakebaseAuth endpoint resolution failed (branch=%r): %s",
                self._branch_path,
                exc,
            )
        return ""
