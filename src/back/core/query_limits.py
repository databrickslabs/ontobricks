"""Runtime-tunable bounds for graph *read* queries.

Two knobs are resolved here and consumed by the graph store backends and the
Graph Chat routes so a single broad question can no longer pin a DB connection
or blocking worker indefinitely:

* **statement timeout** — how long a graph read query may run before the
  database cancels it server-side (Lakebase ``SET statement_timeout`` /
  warehouse ``SET STATEMENT_TIMEOUT``).
* **chat result cap** — the hard ceiling on triples returned by the
  session-aware ``/dtwin/triples/find`` route the agent calls.

Resolution order for each knob (first hit wins):

1. **admin override** — persisted in the registry global config and applied via
   :func:`set_graph_query_timeout_override` / :func:`set_graph_chat_result_cap_override`
   (Settings → Graph DB, and re-applied whenever the settings blob is loaded).
2. **environment variable** — ``ONTOBRICKS_GRAPH_QUERY_TIMEOUT_S`` /
   ``ONTOBRICKS_GRAPH_CHAT_RESULT_CAP``.
3. **built-in default**.

These are deliberately independent of the generic (registry / build-pipeline)
SQL paths: only graph reads are bounded, so long-running builds and full-graph
dumps are unaffected.
"""

from __future__ import annotations

import os
import threading
from typing import Optional

from back.core.logging import get_logger

logger = get_logger(__name__)

DEFAULT_GRAPH_QUERY_TIMEOUT_S = 60
DEFAULT_GRAPH_CHAT_RESULT_CAP = 10_000

# Sane bounds so a misconfiguration can't disable the guard entirely or set a
# value that would itself cause problems.
_MIN_TIMEOUT_S = 5
_MAX_TIMEOUT_S = 900
_MIN_RESULT_CAP = 100
_MAX_RESULT_CAP = 100_000

_ENV_TIMEOUT = "ONTOBRICKS_GRAPH_QUERY_TIMEOUT_S"
_ENV_RESULT_CAP = "ONTOBRICKS_GRAPH_CHAT_RESULT_CAP"

_lock = threading.Lock()
_override_timeout_s: Optional[int] = None
_override_result_cap: Optional[int] = None


def _env_int(name: str) -> Optional[int]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError:
        logger.warning("Ignoring non-integer %s=%r", name, raw)
        return None
    return value if value > 0 else None


def _clamp(value: int, lo: int, hi: int) -> int:
    return max(lo, min(int(value), hi))


def get_graph_query_timeout_s() -> int:
    """Return the effective graph-read statement timeout in seconds."""
    with _lock:
        override = _override_timeout_s
    if override is not None:
        return override
    env = _env_int(_ENV_TIMEOUT)
    if env is not None:
        return _clamp(env, _MIN_TIMEOUT_S, _MAX_TIMEOUT_S)
    return DEFAULT_GRAPH_QUERY_TIMEOUT_S


def set_graph_query_timeout_override(seconds: Optional[int]) -> None:
    """Set (or clear, when ``seconds`` is falsy) the admin timeout override."""
    global _override_timeout_s
    with _lock:
        if not seconds:
            _override_timeout_s = None
        else:
            _override_timeout_s = _clamp(int(seconds), _MIN_TIMEOUT_S, _MAX_TIMEOUT_S)


def get_graph_chat_result_cap() -> int:
    """Return the effective hard cap on triples returned to the chat agent."""
    with _lock:
        override = _override_result_cap
    if override is not None:
        return override
    env = _env_int(_ENV_RESULT_CAP)
    if env is not None:
        return _clamp(env, _MIN_RESULT_CAP, _MAX_RESULT_CAP)
    return DEFAULT_GRAPH_CHAT_RESULT_CAP


def set_graph_chat_result_cap_override(count: Optional[int]) -> None:
    """Set (or clear, when ``count`` is falsy) the admin result-cap override."""
    global _override_result_cap
    with _lock:
        if not count:
            _override_result_cap = None
        else:
            _override_result_cap = _clamp(int(count), _MIN_RESULT_CAP, _MAX_RESULT_CAP)
