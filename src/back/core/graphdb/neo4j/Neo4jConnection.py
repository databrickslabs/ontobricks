"""Neo4j driver lifecycle, auth resolution, and Cypher execution helper.

Carved out of :mod:`Neo4jStore` during the PR #47 review split (Benoit
2026-06-18 — "la classe est trop grosse"). This module owns three concerns:

1. **Auth resolution** — `NEO4J_PASSWORD` env var (production, populated by
   a Databricks Apps secret resource bound in ``app.yaml``) takes priority
   over ``engine_config['password']`` (local-dev fallback). When running
   inside the deployed app (``DATABRICKS_APP_PORT`` is set) the env var
   becomes mandatory and a missing value raises
   :class:`InfrastructureError` with a clear remediation pointer.
2. **Driver lifecycle** — lazy creation of the thread-safe ``neo4j.Driver``
   (acts as a connection pool). One driver per ``Neo4jConnection``.
3. **Query execution** — :meth:`run` opens a per-query session, executes
   the Cypher, and emits one INFO log line per call summarising
   ``rows`` + duration + a whitespace-flattened Cypher snippet (per
   Benoit's "log the executed Cypher" review request). Bound ``params``
   are logged at DEBUG only — they never carry credentials (auth lives
   on the driver) but may carry build-pipeline URIs/literals.
"""

import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from back.core.errors import InfrastructureError, ValidationError
from back.core.logging import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
#  Guarded import — neo4j driver is an optional dependency.
# ---------------------------------------------------------------------------
try:
    import neo4j as _neo4j
except ImportError:
    _neo4j = None  # type: ignore[assignment]


DEFAULT_DATABASE = "neo4j"
DEFAULT_AUTH_METHOD = "basic"
SUPPORTED_AUTH_METHODS = ("basic", "databricks_secret")

# Env var fed by a Databricks Apps secret resource bound in app.yaml as
# ``valueFrom: neo4j-password``. When set, the persisted engine_config
# password is ignored (and stripped at save-time) — see
# docs/v0.6-neo4j-demo/secret-configuration.md.
NEO4J_PASSWORD_ENV = "NEO4J_PASSWORD"


def is_neo4j_password_from_secret() -> bool:
    """True when ``NEO4J_PASSWORD`` is set in the environment.

    Module-level helper so the Settings save endpoint and the UI page
    context can ask the question without instantiating a full store.
    """
    return bool(os.environ.get(NEO4J_PASSWORD_ENV, "").strip())


# Cap on the Cypher snippet logged at INFO. Beyond this size we truncate
# (full statement is still available at DEBUG via ``Cypher params``).
_CYPHER_LOG_MAX = 1500
_WHITESPACE_RE = re.compile(r"\s+")


def _normalise_cypher_for_log(cypher: str) -> str:
    """Collapse runs of whitespace into single spaces and truncate.

    Cypher in this module is assembled from multi-line f-strings; flattening
    them keeps each log entry on a single grep-friendly line.
    """
    flat = _WHITESPACE_RE.sub(" ", cypher).strip()
    if len(flat) > _CYPHER_LOG_MAX:
        return flat[:_CYPHER_LOG_MAX] + "… (truncated)"
    return flat


class Neo4jConnection:
    """Owns the Bolt driver and exposes a thin :meth:`run` for Cypher.

    Parameters
    ----------
    uri:
        Bolt URI (validated by the caller).
    database:
        Logical Neo4j database name.
    auth_method:
        ``"basic"`` (username + password) or ``"databricks_secret"``
        (scope/key — reserved for a follow-up PR).
    engine_config:
        The raw ``engine_config`` dict. Used by :meth:`_resolve_auth`
        for the username and the local-dev fallback password.
    encrypted:
        Bolt-level encryption flag (ignored when the URI scheme already
        embeds TLS, e.g. ``neo4j+s://``).
    """

    def __init__(
        self,
        uri: str,
        database: str,
        auth_method: str,
        engine_config: Dict[str, Any],
        encrypted: bool = True,
    ) -> None:
        if _neo4j is None:
            raise ImportError(
                "neo4j is required for the Neo4j backend. "
                "Install it with: pip install 'neo4j>=5.0'"
            )
        self._uri = uri
        self._database = database
        self._auth_method = auth_method
        self._engine_config = engine_config
        self._encrypted = encrypted
        self._driver: Optional[Any] = None

    @property
    def database(self) -> str:
        return self._database

    @property
    def uri(self) -> str:
        return self._uri

    def get_driver(self) -> Any:
        """Return (lazily create) the Neo4j driver.

        Neo4j's Python driver itself is a thread-safe connection pool.
        Sessions are short-lived and created per-query in :meth:`run`.
        """
        if self._driver is not None:
            return self._driver
        auth = self._resolve_auth()
        kwargs: Dict[str, Any] = {"auth": auth}
        # neo4j+s:// embeds TLS — passing encrypted=True is rejected.
        if not self._uri.startswith(("neo4j+s://", "neo4j+ssc://", "bolt+s://", "bolt+ssc://")):
            kwargs["encrypted"] = self._encrypted
        self._driver = _neo4j.GraphDatabase.driver(self._uri, **kwargs)
        logger.info("Neo4j driver opened for %s (database=%s)", self._uri, self._database)
        return self._driver

    def close(self) -> None:
        if self._driver is not None:
            try:
                self._driver.close()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Neo4j driver close failed: %s", exc)
        self._driver = None
        logger.debug("Neo4j driver closed")

    @staticmethod
    def _is_deployed_app() -> bool:
        """True when running inside Databricks Apps (port var is set)."""
        return bool(os.environ.get("DATABRICKS_APP_PORT"))

    def _resolve_auth(self) -> Tuple[str, str]:
        cfg = self._engine_config
        if self._auth_method == "basic":
            user = str(cfg.get("username") or "").strip()
            if not user:
                raise ValidationError(
                    "Neo4jConnection: auth_method=basic requires engine_config['username']"
                )
            pwd_env = os.environ.get(NEO4J_PASSWORD_ENV, "").strip()
            pwd_cfg = str(cfg.get("password") or "")
            if pwd_env:
                logger.info("Neo4j credentials sourced from %s env var", NEO4J_PASSWORD_ENV)
                return (user, pwd_env)
            if self._is_deployed_app():
                raise InfrastructureError(
                    "Neo4jConnection: %s env var is required in the deployed app — "
                    "declare a Databricks Apps secret resource named 'neo4j-password' "
                    "and bind it via app.yaml `valueFrom`. See "
                    "docs/v0.6-neo4j-demo/secret-configuration.md."
                    % NEO4J_PASSWORD_ENV
                )
            if not pwd_cfg:
                raise ValidationError(
                    "Neo4jConnection: auth_method=basic requires either the %s env var "
                    "(production) or engine_config['password'] (local dev)."
                    % NEO4J_PASSWORD_ENV
                )
            logger.info("Neo4j credentials sourced from engine_config (local-dev fallback)")
            return (user, pwd_cfg)
        if self._auth_method == "databricks_secret":
            scope = str(cfg.get("secret_scope") or "").strip()
            key = str(cfg.get("secret_key") or "").strip()
            if not scope or not key:
                raise ValidationError(
                    "Neo4jConnection: auth_method=databricks_secret requires "
                    "engine_config['secret_scope'] and ['secret_key']"
                )
            # TODO(PR3): resolve via Databricks secrets API. The supported
            # production path today is the env-var-via-Apps-secret-resource
            # mechanism handled by the ``basic`` branch above.
            raise NotImplementedError(
                "auth_method=databricks_secret is reserved for a follow-up PR; "
                "use auth_method=basic with the NEO4J_PASSWORD secret resource instead."
            )
        raise ValidationError("Unsupported auth_method: %s" % self._auth_method)

    def run(self, cypher: str, **params: Any) -> List[Dict[str, Any]]:
        """Execute a Cypher statement against the configured database.

        Returns rows as dicts. Wraps the session in a single transaction.
        Emits one INFO log line per call with ``rows`` count, duration, and
        a whitespace-flattened Cypher snippet so reviewers can correlate
        UI actions with the backend query (Benoit's PR #47 2026-06-18
        request). Bound ``params`` are logged at DEBUG only — they never
        contain credentials (auth lives on the driver, not per-session).
        """
        driver = self.get_driver()
        logger.debug("Cypher params: %s", params)
        t0 = time.monotonic()
        with driver.session(database=self._database) as session:
            result = session.run(cypher, **params)
            rows = [dict(record) for record in result]
        duration_ms = (time.monotonic() - t0) * 1000.0
        logger.info(
            "Cypher (%d rows, %.1f ms): %s",
            len(rows),
            duration_ms,
            _normalise_cypher_for_log(cypher),
        )
        return rows
