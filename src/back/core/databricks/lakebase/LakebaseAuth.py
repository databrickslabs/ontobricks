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

Endpoint resolution matches ``status.hosts.host`` /
``status.hosts.read_only_host`` against ``PGHOST`` on the endpoints of
the branch named by ``LAKEBASE_PROJECT`` + ``LAKEBASE_BRANCH``, then of
the app's bound ``postgres`` resource (``GET /api/2.0/apps/<name>``),
and only as a last resort walks ``GET /api/2.0/postgres/projects`` →
branches → endpoints (skipping unreadable projects). The matched
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
from collections.abc import Iterator
from typing import Optional

from databricks.sdk.errors import NotFound, PermissionDenied

from back.core.databricks.lakebase.constants import TOKEN_TTL_S as _TOKEN_TTL_S
from back.core.databricks.lakebase.LakebaseProjectService import (
    LakebaseProjectService,
)
from back.core.errors import ValidationError
from back.core.logging import get_logger

logger = get_logger(__name__)


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
    2. ``LAKEBASE_PROJECT`` + ``LAKEBASE_BRANCH`` — the primary host
       of that branch's endpoints
       (``/api/2.0/postgres/projects/<p>/branches/<b>/endpoints``). Use
       these in local ``.env`` to select a branch without looking up the
       endpoint hostname manually.

    **Endpoint resolution** (for the credential mint) is described in
    :attr:`instance_name`.

    **Database resolution order** (first non-empty wins):

    1. ``PGDATABASE`` — auto-injected by Databricks Apps.
    2. ``LAKEBASE_DATABASE`` — explicit override for local dev or
       when you want to point at a database that differs from the
       bound default.
    """

    #: Seconds before a failed endpoint lookup is tried again.
    _LOOKUP_RETRY_S = 60.0

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
        # Last failed endpoint lookup, re-raised until _LOOKUP_RETRY_S passes.
        self._lookup_error: Optional[ValidationError] = None
        self._lookup_failed_at: float = 0.0

    # ------------------------------------------------------------------
    # Connection parameters (read directly from environment)
    # ------------------------------------------------------------------

    @property
    def host(self) -> str:
        host = (
            os.environ.get("PGHOST", "")
            or self._cached_host
            or self._host_from_env_branch()
        )
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

        Matches ``PGHOST`` against Lakebase endpoints (see
        :meth:`_known_branch_paths` for the lookup order). On a hit
        we cache both the project_id and the matched endpoint
        resource path; the latter is consumed by :meth:`password`.

        Raises :class:`ValidationError` if no endpoint matches —
        that's the canonical "the bundle is binding the wrong
        project" failure and must surface loudly rather than
        silently fall back to a different code path. A failure is
        re-raised without a new lookup for :attr:`_LOOKUP_RETRY_S`,
        because the fallback lists every project in the workspace.

        ``PGAPPNAME`` is intentionally **not** consulted: Databricks
        Apps populates it with the app's name (e.g. ``ontobricks-dev``)
        which is unrelated to the Lakebase project.
        """
        if self._instance_name:
            return self._instance_name
        self._raise_if_backing_off()

        host = self.host.strip().lower()
        if self._instance_name:  # resolved while deriving the host
            return self._instance_name
        try:
            self._ensure_workspace()
            endpoint = LakebaseProjectService.find_endpoint(
                self._api(), host, self._known_branch_paths()
            )
        except Exception as exc:  # noqa: BLE001
            raise self._lookup_failed(
                f"Could not resolve Lakebase Autoscaling project from "
                f"PGHOST={host!r}: {exc}"
            ) from exc
        if not endpoint or not self._remember(endpoint):
            raise self._lookup_failed(
                f"No Lakebase Autoscaling endpoint matched PGHOST={host!r} "
                f"among all listed projects. "
                f"Confirm the Apps ``postgres`` resource binding and endpoint host."
            )

        logger.info(
            "Resolved Lakebase Autoscaling project %r from PGHOST=%s",
            self._instance_name,
            host,
        )
        return self._instance_name

    def _raise_if_backing_off(self) -> None:
        """Re-raise the last failed lookup until :attr:`_LOOKUP_RETRY_S` has passed."""
        if (
            self._lookup_error
            and time.time() - self._lookup_failed_at < self._LOOKUP_RETRY_S
        ):
            raise self._lookup_error

    def _lookup_failed(self, message: str) -> ValidationError:
        """Record a failed lookup so :attr:`instance_name` backs off, and return it."""
        self._lookup_error = ValidationError(message)
        self._lookup_failed_at = time.time()
        return self._lookup_error

    def _remember(self, endpoint: dict) -> bool:
        """Cache the endpoint path and its project_id; False if the path is malformed."""
        parts = (endpoint.get("name") or "").split("/")
        if len(parts) < 6 or parts[0] != "projects":
            return False
        self._endpoint_resource = "/".join(parts)
        self._instance_name = parts[1]
        return True

    @staticmethod
    def _env_branch_path() -> str:
        """Return ``projects/<p>/branches/<b>`` from ``LAKEBASE_PROJECT`` + ``LAKEBASE_BRANCH``, or ``""``."""
        project = os.environ.get("LAKEBASE_PROJECT", "").strip()
        branch = os.environ.get("LAKEBASE_BRANCH", "").strip()
        return f"projects/{project}/branches/{branch}" if project and branch else ""

    def _host_from_env_branch(self) -> str:
        """Return the primary host of the ``LAKEBASE_*`` branch (local dev), or ``""``.

        Also caches the endpoint, so :meth:`password` does not list it again.
        """
        branch_path = self._env_branch_path()
        if not branch_path:
            return ""
        self._raise_if_backing_off()
        try:
            self._ensure_workspace()
            endpoint = LakebaseProjectService.branch_endpoint(
                self._api(), branch_path, ""
            )
        except Exception as exc:  # noqa: BLE001
            raise self._lookup_failed(
                f"Could not resolve the Lakebase host of {branch_path}: {exc}"
            ) from exc
        if not endpoint or not self._remember(endpoint):
            return ""
        self._cached_host = endpoint["status"]["hosts"]["host"].strip()
        logger.info("Resolved Lakebase host %r from %s", self._cached_host, branch_path)
        return self._cached_host

    def _known_branch_paths(self) -> Iterator[str]:
        """Lazily yield the branches to try before walking every project.

        1. ``LAKEBASE_PROJECT`` + ``LAKEBASE_BRANCH`` env vars;
        2. the ``postgres`` resource bound to ``DATABRICKS_APP_NAME``.
        """
        env_branch = self._env_branch_path()
        if env_branch:
            yield env_branch
        app_branch = self._app_branch_path()
        if app_branch:
            yield app_branch

    def _app_branch_path(self) -> str:
        """Return the branch of the app's bound ``postgres`` resource, or ``""``.

        ``""`` when not running as an app, or when its service principal
        may not read the app; the caller then walks every project.
        """
        app_name = os.environ.get("DATABRICKS_APP_NAME", "").strip()
        if not app_name:
            return ""
        try:
            app = self._api().do("GET", f"/api/2.0/apps/{app_name}") or {}
        except (NotFound, PermissionDenied) as exc:
            logger.info(
                "Could not read Databricks App %r to find its Lakebase branch: %s",
                app_name,
                exc,
            )
            return ""
        for resource in app.get("resources") or []:
            branch = ((resource or {}).get("postgres") or {}).get("branch") or ""
            if branch:
                return branch
        logger.warning(
            "Databricks App %r has no postgres resource; finding its Lakebase "
            "endpoint by listing every project",
            app_name,
        )
        return ""

    def _api(self):
        """Return the workspace ``api_client`` used for Postgres API calls."""
        api = getattr(self._w, "api_client", None)
        if api is None or not hasattr(api, "do"):
            raise ValidationError(
                "WorkspaceClient.api_client unavailable; cannot call the "
                "Lakebase Postgres API."
            )
        return api

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
        resp = (
            self._api().do(
                "POST",
                "/api/2.0/postgres/credentials",
                body={"endpoint": endpoint_resource},
            )
            or {}
        )
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


_default: Optional[LakebaseAuth] = None


def get_lakebase_auth() -> LakebaseAuth:
    """Return a process-wide :class:`LakebaseAuth` singleton."""
    global _default
    if _default is None:
        _default = LakebaseAuth()
    return _default
