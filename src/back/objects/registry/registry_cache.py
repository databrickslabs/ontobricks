"""Global TTL cache for registry domain listings (shared across sessions)."""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from back.core.logging import get_logger

logger = get_logger(__name__)

_registry_details_cache: Dict[str, Any] = {}
_registry_names_cache: Dict[str, Any] = {}
_DEFAULT_REGISTRY_DOMAINS_TTL = 300
_registry_domains_ttl: int = _DEFAULT_REGISTRY_DOMAINS_TTL


def get_registry_cache_ttl() -> int:
    """Return the current registry cache TTL in seconds."""
    return _registry_domains_ttl


def set_registry_cache_ttl(ttl: int) -> None:
    """Update the registry cache TTL (in seconds). Minimum 10s."""
    global _registry_domains_ttl
    _registry_domains_ttl = max(10, int(ttl))


def registry_cache_key(catalog: str, schema: str, volume: str) -> str:
    """Build a cache key from the registry triplet."""
    return f"{catalog}.{schema}.{volume}"


def get_cached_registry_details(cache_key: str) -> Optional[List[Dict[str, Any]]]:
    """Return cached domain details list, or ``None`` if stale/missing."""
    cached = _registry_details_cache.get(cache_key)
    if cached and (time.time() - cached["_ts"]) < _registry_domains_ttl:
        return cached["data"]
    return None


def set_cached_registry_details(cache_key: str, data: List[Dict[str, Any]]) -> None:
    _registry_details_cache[cache_key] = {"data": data, "_ts": time.time()}


def get_cached_registry_names(cache_key: str) -> Optional[List[str]]:
    """Return cached domain name list, or ``None`` if stale/missing."""
    cached = _registry_names_cache.get(cache_key)
    if cached and (time.time() - cached["_ts"]) < _registry_domains_ttl:
        return cached["data"]
    return None


def set_cached_registry_names(cache_key: str, data: List[str]) -> None:
    _registry_names_cache[cache_key] = {"data": data, "_ts": time.time()}


def invalidate_registry_cache(cache_key: str | None = None) -> None:
    """Clear one key or all entries from both caches."""
    if cache_key:
        _registry_details_cache.pop(cache_key, None)
        _registry_names_cache.pop(cache_key, None)
    else:
        _registry_details_cache.clear()
        _registry_names_cache.clear()


def get_registry_cache_snapshot() -> Dict[str, Any]:
    """Return a serialisable snapshot of the cache state including full data."""
    now = time.time()
    snapshot: Dict[str, Any] = {"ttl_seconds": _registry_domains_ttl}

    for label, store in (("details", _registry_details_cache), ("names", _registry_names_cache)):
        entries = {}
        for key, value in store.items():
            age = now - value["_ts"]
            entries[key] = {
                "age_seconds": round(age, 1),
                "ttl_remaining": round(max(0, _registry_domains_ttl - age), 1),
                "item_count": len(value["data"]) if isinstance(value["data"], list) else "?",
                "data": value["data"],
            }
        snapshot[label] = entries

    return snapshot
