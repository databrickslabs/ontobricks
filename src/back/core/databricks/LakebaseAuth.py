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

    **Host resolution order** (first non-empty wins):

    1. ``PGHOST`` — auto-injected by Databricks Apps at runtime; or
       set directly in ``.env`` if you already know the endpoint URL.
    2. ``LAKEBASE_PROJECT`` + ``LAKEBASE_BRANCH`` — resolved via
       the Postgres API (``/api/2.0/postgres/projects/<name>/branches``
       → endpoints). Use these in local ``.env`` to select a branch
       without looking up the endpoint hostname manually.

    **Database resolution order** (first non-empty wins):

    1. ``PGDATABASE`` — auto-injected by Databricks Apps.
    2. ``LAKEBASE_DATABASE`` — explicit override for local dev or
       when you want to point at a database that differs from the
       bound default.
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
        # Resolved host cache — avoids re-walking the API on every access
        # when PGHOST is absent and we resolved via branch resolution.
        self._cached_host: Optional[str] = None

    # ------------------------------------------------------------------
    # Connection parameters (read directly from environment)
    # ------------------------------------------------------------------

    @property
    def host(self) -> str:
        host = os.environ.get("PGHOST", "")
        if not host:
            host = self._cached_host or self._resolve_host_from_project_branch() or ""
        if not host:
            raise ValidationError(
                "Cannot determine Lakebase host: set PGHOST (or both "
                "LAKEBASE_PROJECT and LAKEBASE_BRANCH) in .env, "
                "or bind a Lakebase 'database' resource to the Databricks App."
            )
        return host

    @property
    def port(self) -> int:
        return int(os.environ.get("PGPORT", "5432"))

    @property
    def database(self) -> str:
        return (
            os.environ.get("PGDATABASE")
            or os.environ.get("LAKEBASE_DATABASE")
            or "ontobricks_registry"
        )

    @property
    def user(self) -> str:
        user = os.environ.get("PGUSER", "")
        if not user:
            raise ValidationError(
                "PGUSER is not set — bind a Lakebase 'database' resource. "
                "Lakebase is required since v0.4.0."
            )
        return user

    @property
    def branch_name(self) -> str:
        """Return the branch name extracted from the resolved endpoint resource path.

        Forces endpoint resolution via :attr:`instance_name` (cached) so the
        value is available even when the caller only checked ``instance_name``
        previously.  Returns an empty string when the endpoint path cannot be
        parsed (e.g. provisioned legacy instances).

        Endpoint resource path format:
        ``projects/<project_id>/branches/<branch_id>/endpoints/<endpoint_id>``
        """
        # Trigger resolution so _endpoint_resource is populated.
        try:
            _ = self.instance_name
        except Exception:  # noqa: BLE001
            return ""
        if not self._endpoint_resource:
            return ""
        parts = self._endpoint_resource.split("/")
        # Expected: ["projects", "<proj>", "branches", "<branch>", "endpoints", "<ep>"]
        if len(parts) >= 4 and parts[0] == "projects" and parts[2] == "branches":
            return parts[3]
        return ""

    @property
    def branch_path(self) -> str:
        """Return ``projects/<proj>/branches/<branch>`` for the bound Lakebase.

        Triggers endpoint resolution on the first call (same cost as
        :attr:`host`).  Returns an empty string on failure so callers
        can guard with ``if auth.branch_path``.
        """
        try:
            inst = self.instance_name
            branch = self.branch_name
            if inst and branch:
                return f"projects/{inst}/branches/{branch}"
        except Exception:  # noqa: BLE001
            pass
        return ""

    @property
    def is_available(self) -> bool:
        """Return True when the Lakebase connection can be established.

        Accepts either:
        - ``PGHOST`` + ``PGUSER`` (auto-injected by Databricks Apps, or set
          directly in ``.env`` with the raw endpoint URL), or
        - ``LAKEBASE_PROJECT`` + ``LAKEBASE_BRANCH`` + ``PGUSER``
          (local dev with branch-based host resolution).
        """
        has_user = bool(os.environ.get("PGUSER"))
        has_host = bool(os.environ.get("PGHOST"))
        has_branch_coords = bool(
            os.environ.get("LAKEBASE_PROJECT")
            and os.environ.get("LAKEBASE_BRANCH")
        )
        return has_user and (has_host or has_branch_coords)

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

    def _resolve_host_from_project_branch(self) -> Optional[str]:
        """Resolve the Lakebase endpoint hostname from project + branch name.

        Reads ``LAKEBASE_PROJECT`` (project) and ``LAKEBASE_BRANCH``
        (branch) from the environment, then walks
        ``GET /api/2.0/postgres/projects/<project>/branches`` →
        ``GET /api/2.0/postgres/<branch_path>/endpoints`` to find the
        primary endpoint host for that branch.

        On success also populates ``_instance_name`` and
        ``_endpoint_resource`` so subsequent :meth:`password` calls do
        not need to re-walk the API. Returns ``None`` (never raises) on
        any configuration gap or API error — the caller falls back to a
        descriptive ``ValidationError``.
        """
        project = os.environ.get("LAKEBASE_PROJECT", "").strip()
        branch_name = os.environ.get("LAKEBASE_BRANCH", "").strip()
        if not project or not branch_name:
            return None
        try:
            self._ensure_workspace()
            api = getattr(self._w, "api_client", None)
            if api is None or not hasattr(api, "do"):
                return None
            branches = (
                api.do(
                    "GET",
                    f"/api/2.0/postgres/projects/{project}/branches",
                )
                or {}
            ).get("branches") or []
            for branch in branches:
                branch_path = branch.get("name") or ""
                if not branch_path:
                    continue
                if branch_path.rsplit("/", 1)[-1] != branch_name:
                    continue
                endpoints = (
                    api.do("GET", f"/api/2.0/postgres/{branch_path}/endpoints")
                    or {}
                ).get("endpoints") or []
                for endpoint in endpoints:
                    hosts = (endpoint.get("status") or {}).get("hosts") or {}
                    host = (hosts.get("host") or "").strip()
                    if not host:
                        continue
                    endpoint_path = endpoint.get("name") or ""
                    if endpoint_path:
                        self._endpoint_resource = endpoint_path
                    self._instance_name = project
                    self._cached_host = host
                    logger.info(
                        "Resolved Lakebase host %r from project=%r branch=%r",
                        host,
                        project,
                        branch_name,
                    )
                    return host
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "Branch-based Lakebase host resolution failed "
                "(project=%r branch=%r): %s",
                project,
                branch_name,
                exc,
            )
        return None

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
            f"application_name={application_name} "
            # TCP keepalives: detect server-side connection drops in ~25s
            # instead of the OS default ~130s (tcp_retries2). Without these,
            # a borrowed-but-dead pool connection blocks the next query for
            # 2+ minutes, which shows up as a "preview timeout" client side.
            f"keepalives=1 keepalives_idle=10 "
            f"keepalives_interval=5 keepalives_count=3"
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
            # See ``conninfo`` for the rationale on TCP keepalives.
            "keepalives": 1,
            "keepalives_idle": 10,
            "keepalives_interval": 5,
            "keepalives_count": 3,
        }


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


_default: Optional[LakebaseAuth] = None


def get_lakebase_auth() -> LakebaseAuth:
    """Return a process-wide :class:`LakebaseAuth` singleton."""
    global _default
    if _default is None:
        _default = LakebaseAuth()
    return _default
