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
   database instances (legacy Database Instance API — works for
   projects created via the Lakehouse/Provisioned UI or DABs).
3. **Lakebase Autoscaling fallback.** When step 2 returns no match
   (typical for projects created via the Autoscaling UI / Postgres
   API, whose regional ``ep-<id>.database.<region>.cloud.databricks.com``
   hostnames are not exposed on the legacy ``DatabaseInstance``),
   walk ``/api/2.0/postgres/projects`` → branches → endpoints and
   match ``status.hosts.host`` / ``status.hosts.read_only_host``
   against ``PGHOST``. The project_id is then used as the instance
   name (the two namespaces are aliased — ``generate_database_credential``
   accepts the project_id as ``instance_names=[<project_id>]``).
   Result is cached for the process lifetime.

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


def _looks_like_instance_not_found(exc: BaseException) -> bool:
    """Return True for legacy-API errors meaning "project not in this API".

    Heuristic match against the message — the SDK does not surface a
    typed ``ResourceDoesNotExist`` for this case consistently across
    versions, but the wire error always contains the literal
    ``Database instance '<name>' not found``.
    """
    msg = str(exc).lower()
    return "not found" in msg and "database instance" in msg


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
        # Full endpoint resource path
        # (``projects/<project_id>/branches/<branch_id>/endpoints/<endpoint_id>``)
        # populated when :meth:`_lookup_via_postgres_api` resolves PGHOST.
        # Required by :meth:`password` to mint via the Postgres API for
        # Autoscaling-only projects, which the legacy Database Instance
        # API does not see.
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
        """Resolve the Lakebase instance name (cached).

        Order:
        1. ``DATABASE_INSTANCE_NAME`` env var (explicit override).
        2. Database Instance API: match ``PGHOST`` against
           ``read_write_dns`` / ``read_only_dns`` of the workspace's
           Lakebase instances. Covers Provisioned and Autoscaling
           projects whose endpoints are exposed on the legacy API.
        3. Postgres API fallback: walk projects → branches →
           endpoints and match ``status.hosts.host`` /
           ``status.hosts.read_only_host``. Required for Autoscaling
           projects whose regional ``ep-<id>...`` hostnames are not
           surfaced on the Database Instance API.

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
            name = self._lookup_via_database_instances(host)
            if name:
                self._instance_name = name
                logger.info(
                    "Resolved Lakebase instance name %r from PGHOST=%s "
                    "via Database Instance API",
                    name,
                    host,
                )
                return self._instance_name

            name = self._lookup_via_postgres_api(host)
            if name:
                self._instance_name = name
                logger.info(
                    "Resolved Lakebase project_id %r from PGHOST=%s "
                    "via Postgres API endpoint walk",
                    name,
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

    def _lookup_via_database_instances(self, host: str) -> Optional[str]:
        """Match ``host`` against the legacy Database Instance API.

        Returns the instance name on a hit, ``None`` on no match.
        Raises if the SDK call itself fails — the caller wraps it.
        """
        for inst in self._w.database.list_database_instances():
            rw = (getattr(inst, "read_write_dns", "") or "").strip().lower()
            ro = (getattr(inst, "read_only_dns", "") or "").strip().lower()
            if host in (rw, ro):
                return getattr(inst, "name", None)
        return None

    def _lookup_via_postgres_api(self, host: str) -> Optional[str]:
        """Match ``host`` against the Autoscaling Postgres API.

        Walks ``/api/2.0/postgres/projects`` → branches → endpoints
        and compares ``status.hosts.host`` / ``status.hosts.read_only_host``
        against ``PGHOST``. Returns the project_id (final segment of
        the resource name ``projects/<id>``) on a hit, ``None`` otherwise.

        On a hit, also caches the matched endpoint's full resource path
        on ``self._endpoint_resource`` so :meth:`password` can mint via
        ``POST /api/2.0/postgres/credentials`` — the legacy Database
        Instance API does not see Autoscaling-only projects, so a JWT
        scoped to ``instance_names=[<project_id>]`` would fail with
        ``Database instance '<project_id>' not found``.

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

        Two minting paths, picked based on how :attr:`instance_name`
        was resolved:

        1. **Postgres API** (``POST /api/2.0/postgres/credentials``) —
           used when ``self._endpoint_resource`` is populated by the
           Postgres-API endpoint walk. This is the only path that
           works for Autoscaling-only projects (created via the
           Autoscaling UI / Postgres API), whose endpoints are
           invisible to the legacy Database Instance API.

        2. **Database Instance API** (legacy
           ``WorkspaceClient.database.generate_database_credential``)
           — used when the project was resolved via the legacy
           ``list_database_instances`` API or via an explicit
           ``DATABASE_INSTANCE_NAME`` override. Covers Provisioned
           instances and Autoscaling projects also exposed on the
           legacy API.

        If the legacy mint fails with a ``not found`` error (typical
        signal that the project is Autoscaling-only), we fall through
        to a Postgres-API endpoint walk and retry the mint there.

        The resulting token is a Lakebase-issued JWT (valid ~1h) —
        distinct from the plain workspace bearer token, which Lakebase
        rejects with ``Provided authentication token is not a valid
        JWT encoding``.
        """
        now = time.time()
        if self._token and (now - self._token_ts) < _TOKEN_TTL_S:
            return self._token

        self._ensure_workspace()
        # Force ``instance_name`` resolution first so the endpoint
        # resource path is populated when applicable. ``instance_name``
        # caches; this is cheap on subsequent calls.
        name = self.instance_name

        token = ""
        try:
            if self._endpoint_resource:
                token = self._mint_via_postgres_api(self._endpoint_resource)
            else:
                try:
                    token = self._mint_via_database_instances(name)
                except Exception as exc:  # noqa: BLE001
                    if not _looks_like_instance_not_found(exc):
                        raise
                    # Legacy API doesn't know this project — try the
                    # Postgres API endpoint walk and retry with the
                    # resolved endpoint resource.
                    logger.info(
                        "Legacy generate_database_credential failed for %r "
                        "(%s); falling back to Postgres API endpoint walk",
                        name,
                        exc,
                    )
                    project_id = self._lookup_via_postgres_api(
                        self.host.strip().lower()
                    )
                    if project_id and self._endpoint_resource:
                        # Lock in the project_id discovered via the
                        # Postgres API so future calls bypass the legacy
                        # path entirely.
                        self._instance_name = project_id
                        token = self._mint_via_postgres_api(
                            self._endpoint_resource
                        )
                    else:
                        raise
        except Exception as exc:  # noqa: BLE001
            raise ValidationError(
                f"Failed to mint Lakebase JWT for instance "
                f"{self._instance_name or '?'!r}: {exc}"
            ) from exc

        if not token:
            raise ValidationError("Lakebase JWT was empty")

        self._token = token
        self._token_ts = now
        logger.debug(
            "Minted fresh Lakebase JWT for instance %s via %s",
            self._instance_name,
            "postgres-api" if self._endpoint_resource else "database-api",
        )
        return self._token

    def _mint_via_database_instances(self, name: str) -> str:
        """Mint a JWT via the legacy Database Instance API."""
        cred = self._w.database.generate_database_credential(
            instance_names=[name],
            request_id="ontobricks-registry",
        )
        return cred.token or ""

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
