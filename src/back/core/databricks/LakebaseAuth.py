"""Authentication helper for Databricks Lakebase (Postgres) connections.

OntoBricks targets **Lakebase Autoscaling exclusively**. The legacy
Provisioned tier and the Database Instance API
(``WorkspaceClient.database.list_database_instances`` /
``generate_database_credential``) are deliberately not used here —
Autoscaling-only projects are invisible to that API and any code path
that touches it is a foot-gun on a sandbox bound to such a project.

When OntoBricks runs inside a Databricks App with a ``database``
resource bound to a Lakebase Autoscaling project, the platform injects:

- ``PGHOST``      — Lakebase endpoint hostname
  (``ep-<id>.database.<region>.cloud.databricks.com``)
- ``PGPORT``      — Postgres port (typically ``5432``)
- ``PGDATABASE``  — the bound database name (e.g. ``ontobricks_registry`` or ``databricks_postgres``)
- ``PGUSER``      — Postgres role (the app's service principal)

The Postgres password is *not* injected. Instead, the app mints a
short-lived Lakebase JWT via the Postgres API
(``POST /api/2.0/postgres/credentials`` with the matching
``endpoint`` resource) and uses it as the password. The plain
workspace bearer token returned by ``config.authenticate()`` is
**not** accepted by Lakebase — it's not a JWT.

Endpoint resolution walks ``GET /api/2.0/postgres/projects`` →
branches → endpoints and matches ``status.hosts.host`` /
``status.hosts.read_only_host`` against ``PGHOST``. The matched
endpoint's full resource path
(``projects/<project_id>/branches/<branch_id>/endpoints/<endpoint_id>``)
is cached on the instance and used as the ``endpoint`` body of the
credential mint.

``PGAPPNAME`` is **not** consulted — it's libpq's
``application_name`` (a free-form connection tracing label) and the
Databricks Apps runtime populates it with the *app's* name (e.g.
``ontobricks-dev``), which has nothing to do with the Lakebase
project.

Tokens are valid for ~1 h, so :class:`LakebaseAuth` refreshes them
~5 minutes before expiry.
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
        # Project_id (final segment of ``projects/<id>``) — populated
        # by the Postgres API endpoint walk.
        self._instance_name: Optional[str] = None
        # Full endpoint resource path
        # (``projects/<project_id>/branches/<branch_id>/endpoints/<endpoint_id>``)
        # — populated alongside ``_instance_name`` and required by
        # :meth:`password` to mint a JWT scoped to that endpoint.
        self._endpoint_resource: Optional[str] = None

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
        """Resolve the Lakebase Autoscaling project_id (cached).

        Walks ``GET /api/2.0/postgres/projects`` → branches →
        endpoints and matches ``status.hosts.host`` /
        ``status.hosts.read_only_host`` against ``PGHOST``. On a hit
        we cache both the project_id and the matched endpoint
        resource path; the latter is consumed by :meth:`password`.

        Raises :class:`ValidationError` if no endpoint matches —
        that's the canonical "the bundle is binding the wrong
        project" failure and must surface loudly rather than
        silently fall back to a different code path.

        ``PGAPPNAME`` is intentionally **not** consulted: Databricks
        Apps populates it with the app's name (e.g. ``ontobricks-dev``)
        which is unrelated to the Lakebase project.
        """
        if self._instance_name:
            return self._instance_name

        host = self.host.strip().lower()
        try:
            self._ensure_workspace()
            project_id = self._lookup_via_postgres_api(host)
        except Exception as exc:  # noqa: BLE001
            raise ValidationError(
                f"Could not resolve Lakebase Autoscaling project from "
                f"PGHOST={host!r}: {exc}"
            ) from exc

        if not project_id:
            raise ValidationError(
                f"No Lakebase Autoscaling endpoint matched PGHOST={host!r}. "
                f"Confirm the Apps ``postgres`` resource binding points "
                f"at an Autoscaling project (legacy Provisioned "
                f"instances are not supported)."
            )

        self._instance_name = project_id
        logger.info(
            "Resolved Lakebase Autoscaling project %r from PGHOST=%s",
            project_id,
            host,
        )
        return self._instance_name

    def _lookup_via_postgres_api(self, host: str) -> Optional[str]:
        """Match ``host`` against Lakebase Autoscaling endpoints.

        Walks ``/api/2.0/postgres/projects`` → branches → endpoints
        and compares ``status.hosts.host`` / ``status.hosts.read_only_host``
        against ``host``. Returns the project_id (final segment of
        the resource name ``projects/<id>``) on a hit, ``None`` otherwise.

        On a hit, also caches the matched endpoint's full resource
        path on ``self._endpoint_resource`` so :meth:`password` can
        mint via ``POST /api/2.0/postgres/credentials``.

        Uses ``WorkspaceClient.api_client.do`` directly so we work
        across SDK versions that may or may not have ``w.postgres``
        bound on the public surface.
        """
        api = getattr(self._w, "api_client", None)
        if api is None or not hasattr(api, "do"):
            return None
        projects = (api.do("GET", "/api/2.0/postgres/projects") or {}).get(
            "projects"
        ) or []
        for project in projects:
            project_path = project.get("name") or ""
            if not project_path:
                continue
            branches = (
                api.do("GET", f"/api/2.0/postgres/{project_path}/branches") or {}
            ).get("branches") or []
            for branch in branches:
                branch_path = branch.get("name") or ""
                if not branch_path:
                    continue
                endpoints = (
                    api.do("GET", f"/api/2.0/postgres/{branch_path}/endpoints")
                    or {}
                ).get("endpoints") or []
                for endpoint in endpoints:
                    hosts = (endpoint.get("status") or {}).get("hosts") or {}
                    h = (hosts.get("host") or "").strip().lower()
                    ro = (hosts.get("read_only_host") or "").strip().lower()
                    if host in (h, ro):
                        endpoint_path = endpoint.get("name") or ""
                        if endpoint_path:
                            self._endpoint_resource = endpoint_path
                        return project_path.rsplit("/", 1)[-1]
        return None

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

        Mints via ``POST /api/2.0/postgres/credentials`` with the
        endpoint resource discovered by :attr:`instance_name`. This is
        the single supported path — the legacy Database Instance API
        (``WorkspaceClient.database.generate_database_credential``)
        cannot mint credentials for Autoscaling-only projects and is
        deliberately not called.

        The resulting token is a Lakebase-issued JWT (valid ~1 h) —
        distinct from the plain workspace bearer token, which Lakebase
        rejects with ``Provided authentication token is not a valid
        JWT encoding``.
        """
        now = time.time()
        if self._token and (now - self._token_ts) < _TOKEN_TTL_S:
            return self._token

        self._ensure_workspace()
        # Force project resolution so the endpoint resource path is
        # populated. ``instance_name`` caches; this is cheap on
        # subsequent calls.
        _ = self.instance_name
        if not self._endpoint_resource:
            # Defensive: ``instance_name`` either populates the
            # endpoint or raises. Reaching here means a future
            # refactor broke that invariant.
            raise ValidationError(
                "Lakebase endpoint resource was not populated during "
                "project resolution; cannot mint JWT."
            )

        try:
            token = self._mint_via_postgres_api(self._endpoint_resource)
        except Exception as exc:  # noqa: BLE001
            raise ValidationError(
                f"Failed to mint Lakebase JWT for project "
                f"{self._instance_name!r}: {exc}"
            ) from exc

        if not token:
            raise ValidationError("Lakebase JWT was empty")

        self._token = token
        self._token_ts = now
        logger.debug(
            "Minted fresh Lakebase JWT for project %s (endpoint=%s)",
            self._instance_name,
            self._endpoint_resource,
        )
        return self._token

    def _mint_via_postgres_api(self, endpoint_resource: str) -> str:
        """Mint a JWT via ``POST /api/2.0/postgres/credentials``.

        ``endpoint_resource`` is the full Postgres-API endpoint
        resource path, e.g.
        ``projects/<project_id>/branches/<branch_id>/endpoints/<endpoint_id>``.
        """
        api = getattr(self._w, "api_client", None)
        if api is None or not hasattr(api, "do"):
            raise ValidationError(
                "WorkspaceClient.api_client unavailable; cannot mint "
                "Lakebase JWT via Postgres API."
            )
        resp = api.do(
            "POST",
            "/api/2.0/postgres/credentials",
            body={"endpoint": endpoint_resource},
        ) or {}
        return resp.get("token") or ""

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
