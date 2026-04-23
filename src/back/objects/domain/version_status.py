"""TTL cache for registry-backed version status (shared across Domain instances).

The cache is encapsulated in :class:`VersionStatusCache`; a module-level
singleton is exposed via free-function accessors to preserve the previous
public surface (see ``__init__.py``).
"""

from __future__ import annotations

import time
from typing import Any, Dict

from back.core.logging import get_logger

logger = get_logger(__name__)

_DEFAULT_TTL_SECONDS = 30


class VersionStatusCache:
    """Process-wide TTL cache for per-domain version status entries.

    Each entry is keyed by an opaque ``cache_key`` (catalog/schema/domain
    tuple) and is considered fresh for :attr:`ttl_seconds` seconds after
    it is written.
    """

    def __init__(self, ttl_seconds: int = _DEFAULT_TTL_SECONDS) -> None:
        self._store: Dict[str, Dict[str, Any]] = {}
        self.ttl_seconds = ttl_seconds

    def clear(self) -> None:
        """Invalidate the whole cache (after UC save/load)."""
        self._store.clear()

    def get(self, cache_key: str) -> Any | None:
        """Return the cached value if present and fresh, else ``None``."""
        cached = self._store.get(cache_key)
        if cached and (time.time() - cached["_ts"]) < self.ttl_seconds:
            return cached["data"]
        return None

    def set(self, cache_key: str, data: Any) -> None:
        """Store ``data`` against ``cache_key`` with the current timestamp."""
        self._store[cache_key] = {"data": data, "_ts": time.time()}

    def snapshot(self) -> dict:
        """Return a serialisable snapshot of the cache state for debugging."""
        now = time.time()
        entries: Dict[str, Any] = {}
        for key, value in self._store.items():
            age = now - value["_ts"]
            entries[key] = {
                "age_seconds": round(age, 1),
                "ttl_remaining": round(max(0, self.ttl_seconds - age), 1),
            }
        return {"ttl_seconds": self.ttl_seconds, "entries": entries}


_cache = VersionStatusCache()

_version_status_cache: Dict[str, Dict[str, Any]] = _cache._store


def clear_version_status_cache() -> None:
    """Invalidate server-side version status cache (after UC save/load)."""
    _cache.clear()


def get_cached_version_status(cache_key: str) -> Any | None:
    """Return the cached value for ``cache_key`` if still fresh."""
    return _cache.get(cache_key)


def set_cached_version_status(cache_key: str, data: Any) -> None:
    """Store ``data`` in the version-status cache under ``cache_key``."""
    _cache.set(cache_key, data)


def get_version_status_cache_snapshot() -> dict:
    """Return a serialisable snapshot of the cache state for debugging."""
    return _cache.snapshot()
