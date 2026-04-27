"""Authentication helper for Databricks Lakebase (Postgres) connections.

When OntoBricks runs inside a Databricks App with a ``database``
resource bound to a Lakebase instance, the platform injects the
following environment variables into the app process:

- ``PGHOST``      — Lakebase endpoint hostname
- ``PGPORT``      — Postgres port (typically ``5432``)
- ``PGDATABASE``  — database name (canonical: ``ontobricks_registry``)
- ``PGUSER``      — Postgres role (the app's service principal)

The Postgres password is *not* injected: instead, the app mints a
short-lived **Lakebase-scoped JWT** via
``WorkspaceClient().database.generate_database_credential(
instance_names=[<name>])`` and uses it as the password. The plain
workspace bearer token returned by ``config.authenticate()`` is
**not** accepted by Lakebase — it's not a JWT.

The Lakebase instance name is sourced from (in order):

1. ``DATABASE_INSTANCE_NAME`` env var (if you set it explicitly).
2. A one-time SDK lookup that matches ``PGHOST`` against
   ``read_write_dns`` / ``read_only_dns`` of the workspace's
   database instances. Result is cached for the process lifetime.

``PGAPPNAME`` is **not** consulted — it's libpq's
``application_name`` (a free-form connection tracing label) and
the Databricks Apps runtime populates it with the *app's* name
(e.g. ``ontobricks-dev``), which has nothing to do with the
Lakebase instance.

Tokens are valid for ~1 hour, so :class:`LakebaseAuth` refreshes
them ~5 minutes before expiry.

OntoBricks targets **Lakebase Autoscaling** exclusively (the default
tier for every instance created after 2026-03-12). Provisioned
instances are not supported — the deployment YAML and the runtime
auth code both assume an Autoscaling project resource.
"""

from __future__ import annotations

import os
import time
from typing import Optional

from back.core.errors import ValidationError
from back.core.logging import get_logger

logger = get_logger(__name__)

_TOKEN_TTL_S = 3300  # refresh ~5 min before the 1h expiry


class LakebaseAuth:
    """Source ``PG*`` env vars and mint refreshing OAuth tokens.

    The class is safe to construct outside of Databricks Apps for
    testing — it only reads environment variables on demand. The
    workspace client is created lazily on the first ``password()``
    call so that volume-only environments never need the Databricks
    SDK to be importable.
    """

    def __init__(self) -> None:
        self._w = None  # WorkspaceClient, lazily constructed
        self._token: str = ""
        self._token_ts: float = 0.0
        self._instance_name: Optional[str] = None  # cached lookup

    # ------------------------------------------------------------------
    # Connection parameters (read directly from environment)
    # ------------------------------------------------------------------

    @property
    def host(self) -> str:
        host = os.environ.get("PGHOST", "")
        if not host:
            raise ValidationError(
                "PGHOST is not set — bind a Lakebase 'database' resource "
                "to the Databricks App or set REGISTRY_BACKEND=volume."
            )
        return host

    @property
    def port(self) -> int:
        return int(os.environ.get("PGPORT", "5432"))

    @property
    def database(self) -> str:
        return os.environ.get("PGDATABASE", "") or "ontobricks_registry"

    @property
    def user(self) -> str:
        user = os.environ.get("PGUSER", "")
        if not user:
            raise ValidationError(
                "PGUSER is not set — bind a Lakebase 'database' resource "
                "or set REGISTRY_BACKEND=volume."
            )
        return user

    @property
    def is_available(self) -> bool:
        """Return True when PG* env vars are populated.

        Used by the settings UI to display whether Lakebase can be
        selected on this deployment.
        """
        return bool(os.environ.get("PGHOST") and os.environ.get("PGUSER"))

    # ------------------------------------------------------------------
    # Token (Postgres password)
    # ------------------------------------------------------------------

    @property
    def instance_name(self) -> str:
        """Resolve the Lakebase instance name (cached).

        Order:
        1. ``DATABASE_INSTANCE_NAME`` env var (explicit override).
        2. SDK lookup: match ``PGHOST`` against
           ``read_write_dns`` / ``read_only_dns`` of the workspace's
           Lakebase instances.

        ``PGAPPNAME`` is intentionally **not** consulted — Databricks
        Apps sets it to the app name (e.g. ``ontobricks-dev``) which
        is unrelated to the Lakebase instance and would cause
        ``generate_database_credential`` to fail with
        ``Database instance '<app-name>' not found``.
        """
        if self._instance_name:
            return self._instance_name

        explicit = os.environ.get("DATABASE_INSTANCE_NAME")
        if explicit:
            self._instance_name = explicit
            return self._instance_name

        host = self.host.strip().lower()
        try:
            self._ensure_workspace()
            for inst in self._w.database.list_database_instances():
                rw = (getattr(inst, "read_write_dns", "") or "").strip().lower()
                ro = (getattr(inst, "read_only_dns", "") or "").strip().lower()
                if host in (rw, ro):
                    self._instance_name = inst.name
                    logger.info(
                        "Resolved Lakebase instance name %r from PGHOST=%s",
                        inst.name,
                        host,
                    )
                    return self._instance_name
        except Exception as exc:  # noqa: BLE001
            raise ValidationError(
                f"Could not resolve Lakebase instance name from PGHOST={host!r}: {exc}. "
                f"Set DATABASE_INSTANCE_NAME explicitly."
            ) from exc

        raise ValidationError(
            f"No Lakebase instance matched PGHOST={host!r}. "
            f"Set DATABASE_INSTANCE_NAME to the instance name."
        )

    def _ensure_workspace(self) -> None:
        """Lazily build the workspace client."""
        if self._w is not None:
            return
        try:
            from databricks.sdk import WorkspaceClient

            self._w = WorkspaceClient()
        except Exception as exc:  # noqa: BLE001
            raise ValidationError(
                f"Cannot initialise Databricks WorkspaceClient for Lakebase "
                f"authentication: {exc}"
            ) from exc

    def password(self) -> str:
        """Return a (cached) Lakebase JWT suitable as PG password.

        Calls ``WorkspaceClient.database.generate_database_credential``
        scoped to :attr:`instance_name`. The resulting token is a
        Lakebase-issued JWT (valid ~1h) — distinct from the plain
        workspace bearer token, which Lakebase rejects with
        ``Provided authentication token is not a valid JWT encoding``.
        """
        now = time.time()
        if self._token and (now - self._token_ts) < _TOKEN_TTL_S:
            return self._token

        self._ensure_workspace()
        try:
            cred = self._w.database.generate_database_credential(
                instance_names=[self.instance_name],
                request_id="ontobricks-registry",
            )
            self._token = (cred.token or "")
        except Exception as exc:  # noqa: BLE001
            raise ValidationError(
                f"Failed to mint Lakebase JWT for instance "
                f"{self._instance_name or '?'!r}: {exc}"
            ) from exc

        if not self._token:
            raise ValidationError("Lakebase JWT was empty")

        self._token_ts = now
        logger.debug(
            "Minted fresh Lakebase JWT for instance %s", self._instance_name
        )
        return self._token

    def invalidate(self) -> None:
        """Drop the cached token so the next call re-authenticates."""
        self._token = ""
        self._token_ts = 0.0

    # ------------------------------------------------------------------
    # Convenience: assemble psycopg connection kwargs
    # ------------------------------------------------------------------

    def conninfo(
        self, *, application_name: str = "ontobricks", connect_timeout: int = 10
    ) -> str:
        """Return a libpq conninfo string with a freshly-minted token."""
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password()} "
            f"sslmode=require connect_timeout={connect_timeout} "
            f"application_name={application_name}"
        )

    def kwargs(
        self,
        *,
        application_name: str = "ontobricks",
        connect_timeout: int = 10,
    ) -> dict:
        """Return psycopg-style keyword arguments for ``connect()``."""
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.user,
            "password": self.password(),
            "sslmode": "require",
            "connect_timeout": connect_timeout,
            "application_name": application_name,
        }


_default: Optional[LakebaseAuth] = None


def get_lakebase_auth() -> LakebaseAuth:
    """Return a process-wide :class:`LakebaseAuth` singleton."""
    global _default
    if _default is None:
        _default = LakebaseAuth()
    return _default
