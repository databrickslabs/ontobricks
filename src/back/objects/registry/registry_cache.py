"""Global TTL cache for registry domain listings (shared across sessions)."""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from back.core.logging import get_logger

logger = get_logger(__name__)

_registry_details_cache: Dict[str, Any] = {}
_registry_names_cache: Dict[str, Any] = {}
# Cache of resolved PUBLISHED domain documents keyed by
# ``<cache_key>::<folder>`` → {"version", "data", "_ts"}. Avoids the
# newest→oldest ``read_version`` scan performed by
# ``RegistryService.find_published_version`` on every read-only MCP/API call.
_published_domain_cache: Dict[str, Any] = {}
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


def get_cached_published_domain(cache_key: str, folder: str):
    """Return cached ``(version, data)`` for a PUBLISHED domain, or ``None``."""
    cached = _published_domain_cache.get(f"{cache_key}::{folder}")
    if cached and (time.time() - cached["_ts"]) < _registry_domains_ttl:
        return cached["version"], cached["data"]
    return None


def set_cached_published_domain(
    cache_key: str, folder: str, version: str, data: Dict[str, Any]
) -> None:
    _published_domain_cache[f"{cache_key}::{folder}"] = {
        "version": version,
        "data": data,
        "_ts": time.time(),
    }


def invalidate_registry_cache(cache_key: str | None = None) -> None:
    """Clear one key or all entries from every registry cache."""
    if cache_key:
        _registry_details_cache.pop(cache_key, None)
        _registry_names_cache.pop(cache_key, None)
        # Published-domain entries are namespaced ``<cache_key>::<folder>``;
        # drop every folder belonging to this registry.
        prefix = f"{cache_key}::"
        for k in [k for k in _published_domain_cache if k.startswith(prefix)]:
            _published_domain_cache.pop(k, None)
    else:
        _registry_details_cache.clear()
        _registry_names_cache.clear()
        _published_domain_cache.clear()


def get_registry_cache_snapshot() -> Dict[str, Any]:
    """Return a serialisable snapshot of the cache state including full data."""
    now = time.time()
    snapshot: Dict[str, Any] = {"ttl_seconds": _registry_domains_ttl}

    for label, store in (
        ("details", _registry_details_cache),
        ("names", _registry_names_cache),
    ):
        entries = {}
        for key, value in store.items():
            age = now - value["_ts"]
            entries[key] = {
                "age_seconds": round(age, 1),
                "ttl_remaining": round(max(0, _registry_domains_ttl - age), 1),
                "item_count": (
                    len(value["data"]) if isinstance(value["data"], list) else "?"
                ),
                "data": value["data"],
            }
        snapshot[label] = entries

    return snapshot
