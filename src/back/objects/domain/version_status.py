"""TTL cache for registry-backed version status (shared across Domain instances)."""

from __future__ import annotations

import time
from typing import Any, Dict

_version_status_cache: Dict[str, Any] = {}
_VERSION_STATUS_TTL = 30


def clear_version_status_cache() -> None:
    """Invalidate server-side version status cache (after UC save/load)."""
    _version_status_cache.clear()


def get_cached_version_status(cache_key: str) -> Any | None:
    cached = _version_status_cache.get(cache_key)
    if cached and (time.time() - cached["_ts"]) < _VERSION_STATUS_TTL:
        return cached["data"]
    return None


def set_cached_version_status(cache_key: str, data: Any) -> None:
    _version_status_cache[cache_key] = {"data": data, "_ts": time.time()}


def get_version_status_cache_snapshot() -> dict:
    """Return a serialisable snapshot of the cache state for debugging."""
    now = time.time()
    entries = {}
    for key, value in _version_status_cache.items():
        age = now - value["_ts"]
        entries[key] = {
            "age_seconds": round(age, 1),
            "ttl_remaining": round(max(0, _VERSION_STATUS_TTL - age), 1),
        }
    return {"ttl_seconds": _VERSION_STATUS_TTL, "entries": entries}
