"""Process-wide psycopg connection pool for Lakebase (Postgres).

This is the single technical connection layer shared by every Lakebase
consumer — the registry store and the graph triple store. Those two remain
**independent databases**: each caller supplies its own ``auth`` object
(``LakebaseAuth`` or ``BranchLakebaseAuth``), ``schema`` and optional
``database`` override, so different projects / databases / schemas never
share a pool.

The pool is a tiny thread-safe LIFO cache of warm connections. It keeps all
the bespoke behaviour the two duplicated pools used to have:

- Cold-start retries with exponential backoff on SQLSTATE ``57P03`` and on
  ``connection refused`` (Lakebase Autoscaling scales-to-zero when idle).
- OAuth/JWT token rotation on auth failure (SQLSTATE ``28P01``): the token is
  invalidated once and the open retried.
- ``search_path`` setup on every fresh connection.
- Connections recycled before the ~1 h JWT expiry so token rotation stays
  invisible to callers.

Consumers wrap the raised :class:`LakebaseConnectionError` into their own
domain error by passing an ``error_factory`` (the registry uses ``StoreError``,
the graph engine uses ``LakebaseGraphPoolError``).
"""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from back.core.databricks.lakebase.constants import (
    AUTH_FAILURE_SQLSTATES,
    COLD_START_SQLSTATES,
    INITIAL_BACKOFF_S,
    MAX_BACKOFF_S,
    MAX_COLD_START_ATTEMPTS,
    POOL_ACQUIRE_TIMEOUT_S,
    POOL_MAX_LIFETIME_S,
    POOL_MAX_SIZE,
)
from back.core.databricks.lakebase.psycopg_gate import require_psycopg
from back.core.logging import get_logger

logger = get_logger(__name__)

ErrorFactory = Callable[[str], Exception]


class LakebaseConnectionError(RuntimeError):
    """Raised when the pool cannot serve a Lakebase connection."""


class LakebaseConnectionPool:
    """Tiny thread-safe LIFO connection pool for a single Lakebase target.

    A single instance is shared by every consumer pointing at the same
    ``host/db/user/schema`` and ``application_name`` (see
    :func:`get_lakebase_pool`).
    """

    def __init__(
        self,
        *,
        auth: Any,
        schema: str,
        database: str = "",
        application_name: str,
        error_factory: ErrorFactory = LakebaseConnectionError,
        max_size: int = POOL_MAX_SIZE,
        max_lifetime: float = POOL_MAX_LIFETIME_S,
    ) -> None:
        self._auth = auth
        self._schema = schema
        # Empty string means "use whatever PGDATABASE is bound to the app".
        # A non-empty value points the pool at a different database on the
        # same Lakebase instance (the JWT scope is per-instance so the cached
        # token still authenticates).
        self._database = database or ""
        self._application_name = application_name
        self._error = error_factory
        self._max_size = max_size
        self._max_lifetime = max_lifetime
        self._cv = threading.Condition()
        self._idle: List[Tuple[Any, float]] = []  # (conn, opened_at)
        self._size = 0  # checked-out + idle
        self._closed = False

    # -- public API --------------------------------------------------

    @contextmanager
    def connection(self):
        """Yield a healthy Lakebase connection from the pool."""
        conn, opened_at = self._acquire()
        try:
            yield conn
        except Exception:
            self._discard(conn)
            raise
        else:
            self._release(conn, opened_at)

    def close(self) -> None:
        """Drain the pool, closing every idle connection."""
        with self._cv:
            self._closed = True
            idle = list(self._idle)
            self._idle.clear()
            self._size = 0
            self._cv.notify_all()
        for conn, _ in idle:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass

    def stats(self) -> Dict[str, int]:
        with self._cv:
            return {
                "size": self._size,
                "idle": len(self._idle),
                "max_size": self._max_size,
            }

    # -- internals ---------------------------------------------------

    def _is_alive(self, conn: Any, opened_at: float) -> bool:
        if (time.monotonic() - opened_at) >= self._max_lifetime:
            return False
        try:
            return not conn.closed
        except Exception:  # noqa: BLE001
            return False

    def _acquire(self, timeout: float = POOL_ACQUIRE_TIMEOUT_S) -> Tuple[Any, float]:
        deadline = time.monotonic() + timeout
        with self._cv:
            while True:
                if self._closed:
                    raise self._error("Lakebase pool is closed")
                # Re-use an idle connection (LIFO keeps the hottest connection
                # on top — friendliest to TCP keep-alive).
                while self._idle:
                    conn, opened_at = self._idle.pop()
                    if self._is_alive(conn, opened_at):
                        return conn, opened_at
                    # Stale or closed: drop and keep looking.
                    self._size -= 1
                    try:
                        conn.close()
                    except Exception:  # noqa: BLE001
                        pass
                # No idle: open a fresh one if we are under cap. We reserve the
                # slot here, then release the lock to do the (potentially slow)
                # open.
                if self._size < self._max_size:
                    self._size += 1
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise self._error(
                        f"Lakebase pool exhausted after waiting "
                        f"{timeout:.1f}s for a connection"
                    )
                self._cv.wait(remaining)
        # Open outside the lock. On failure, give the slot back so other
        # waiters are not starved by a transient outage.
        try:
            conn = self._open_one()
        except Exception:
            with self._cv:
                self._size -= 1
                self._cv.notify()
            raise
        return conn, time.monotonic()

    def _release(self, conn: Any, opened_at: float) -> None:
        with self._cv:
            if self._closed or not self._is_alive(conn, opened_at):
                self._size -= 1
                self._cv.notify()
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass
                return
            self._idle.append((conn, opened_at))
            self._cv.notify()

    def _discard(self, conn: Any) -> None:
        with self._cv:
            self._size -= 1
            self._cv.notify()
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass

    def _open_one(self) -> Any:
        """Open one new psycopg connection, with cold-start + auth retry."""
        psycopg, _ = require_psycopg()
        attempts = 0
        backoff = INITIAL_BACKOFF_S
        retried_auth = False
        while True:
            try:
                kwargs = self._auth.kwargs(application_name=self._application_name)
                if self._database:
                    kwargs["dbname"] = self._database
                conn = psycopg.connect(autocommit=True, **kwargs)
                with conn.cursor() as cur:
                    cur.execute(f'SET search_path TO "{self._schema}", public')
                return conn
            except Exception as exc:  # noqa: BLE001
                sqlstate = getattr(exc, "sqlstate", "") or ""
                msg = str(exc).lower()
                cold = (
                    sqlstate in COLD_START_SQLSTATES
                    or "starting up" in msg
                    or "connection refused" in msg
                )
                auth_failed = (
                    sqlstate in AUTH_FAILURE_SQLSTATES
                    or "authentication failed" in msg
                )
                if auth_failed and not retried_auth:
                    self._auth.invalidate()
                    retried_auth = True
                    logger.info("Lakebase auth failed; rotating token and retrying")
                    continue
                if cold and attempts < MAX_COLD_START_ATTEMPTS:
                    attempts += 1
                    sleep_for = min(backoff, MAX_BACKOFF_S)
                    logger.info(
                        "Lakebase cold start (sqlstate=%s, attempt=%d/%d); "
                        "sleeping %.1fs",
                        sqlstate or "?",
                        attempts,
                        MAX_COLD_START_ATTEMPTS,
                        sleep_for,
                    )
                    time.sleep(sleep_for)
                    backoff *= 2
                    continue
                raise self._error(f"Lakebase connection failed: {exc}") from exc


# Process-wide pool registry. Consumer stores are rebuilt on every request
# (through their factories), so the pool itself must outlive any single store
# instance.
_pools_lock = threading.Lock()
_pools: Dict[Tuple[str, str, str, str, str, str, str, str], LakebaseConnectionPool] = {}


def _safe_attr(obj: Any, name: str) -> str:
    """Read an attribute that may raise ``ValidationError`` lazily."""
    try:
        return str(getattr(obj, name, "") or "")
    except Exception:  # noqa: BLE001
        return ""


def get_lakebase_pool(
    auth: Any,
    schema: str,
    database: str = "",
    *,
    application_name: str,
    error_factory: ErrorFactory = LakebaseConnectionError,
) -> LakebaseConnectionPool:
    """Return (and lazily create) the shared pool for a Lakebase target.

    The pool identity is the full connection tuple plus ``application_name``,
    so two consumers only ever share a pool when they talk to the exact same
    ``(host, port, database, user, instance, schema)`` under the same
    workload label. The registry and the graph engine use different schemas
    (and often different projects), so they never collide.

    ``database`` is the optional override that points the pool at a different
    Postgres database on the same Lakebase instance. The empty string means
    "use the bound PGDATABASE".
    """
    bound_db = _safe_attr(auth, "database")
    effective_db = database or bound_db
    key = (
        _safe_attr(auth, "host"),
        _safe_attr(auth, "port"),
        bound_db,
        effective_db,
        _safe_attr(auth, "user"),
        _safe_attr(auth, "instance_name"),
        schema,
        application_name,
    )
    with _pools_lock:
        pool = _pools.get(key)
        if pool is None:
            pool = LakebaseConnectionPool(
                auth=auth,
                schema=schema,
                database=database,
                application_name=application_name,
                error_factory=error_factory,
            )
            _pools[key] = pool
            logger.info(
                "Created Lakebase connection pool for %s/%s (schema=%s, app=%s, max_size=%d)",
                key[0],
                effective_db,
                schema,
                application_name,
                POOL_MAX_SIZE,
            )
        return pool


@contextmanager
def lakebase_cursor(
    auth: Any,
    schema: str,
    database: str = "",
    *,
    application_name: str,
    error_factory: ErrorFactory = LakebaseConnectionError,
    row_factory: Optional[Any] = None,
) -> Iterator[Any]:
    """Yield a cursor from the shared pool with ``search_path`` already set.

    A convenience wrapper over :func:`get_lakebase_pool` for the common
    "run one query on the right schema" pattern. Pass ``row_factory`` (e.g.
    ``psycopg.rows.dict_row``) to control row mapping.
    """
    pool = get_lakebase_pool(
        auth,
        schema,
        database,
        application_name=application_name,
        error_factory=error_factory,
    )
    with pool.connection() as conn:
        cur_kwargs = {"row_factory": row_factory} if row_factory is not None else {}
        with conn.cursor(**cur_kwargs) as cur:
            cur.execute(f'SET search_path TO "{schema}", public')
            yield cur
