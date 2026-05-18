"""Tests for back.objects.registry.registry_cache – TTL domain cache."""

import time
import pytest

from back.objects.registry.registry_cache import (
    get_registry_cache_ttl,
    set_registry_cache_ttl,
    registry_cache_key,
    get_cached_registry_details,
    set_cached_registry_details,
    get_cached_registry_names,
    set_cached_registry_names,
    invalidate_registry_cache,
    get_registry_cache_snapshot,
    _registry_details_cache,
    _registry_names_cache,
    _DEFAULT_REGISTRY_DOMAINS_TTL,
)


class TestRegistryCache:
    def setup_method(self):
        invalidate_registry_cache()
        set_registry_cache_ttl(_DEFAULT_REGISTRY_DOMAINS_TTL)

    def test_cache_key(self):
        assert registry_cache_key("cat", "sch", "vol") == "cat.sch.vol"

    def test_ttl_get_set(self):
        assert get_registry_cache_ttl() == _DEFAULT_REGISTRY_DOMAINS_TTL
        set_registry_cache_ttl(60)
        assert get_registry_cache_ttl() == 60

    def test_ttl_minimum(self):
        set_registry_cache_ttl(1)
        assert get_registry_cache_ttl() == 10

    def test_details_set_get(self):
        data = [{"name": "domain1"}]
        set_cached_registry_details("key1", data)
        assert get_cached_registry_details("key1") == data

    def test_details_missing(self):
        assert get_cached_registry_details("missing") is None

    def test_details_expired(self):
        set_cached_registry_details("exp", [])
        _registry_details_cache["exp"]["_ts"] = time.time() - 600
        assert get_cached_registry_details("exp") is None

    def test_names_set_get(self):
        set_cached_registry_names("key1", ["a", "b"])
        assert get_cached_registry_names("key1") == ["a", "b"]

    def test_names_missing(self):
        assert get_cached_registry_names("missing") is None

    def test_names_expired(self):
        set_cached_registry_names("exp", ["x"])
        _registry_names_cache["exp"]["_ts"] = time.time() - 600
        assert get_cached_registry_names("exp") is None

    def test_invalidate_specific_key(self):
        set_cached_registry_details("k1", [])
        set_cached_registry_names("k1", [])
        set_cached_registry_details("k2", [])
        invalidate_registry_cache("k1")
        assert get_cached_registry_details("k1") is None
        assert get_cached_registry_details("k2") is not None

    def test_invalidate_all(self):
        set_cached_registry_details("k1", [])
        set_cached_registry_names("k1", [])
        invalidate_registry_cache()
        assert get_cached_registry_details("k1") is None
        assert get_cached_registry_names("k1") is None

    def test_snapshot(self):
        set_cached_registry_details("snap", [{"name": "d"}])
        set_cached_registry_names("snap", ["d"])
        snapshot = get_registry_cache_snapshot()
        assert "ttl_seconds" in snapshot
        assert "details" in snapshot
        assert "names" in snapshot
        assert "snap" in snapshot["details"]
        entry = snapshot["details"]["snap"]
        assert "age_seconds" in entry
        assert "item_count" in entry
        assert entry["item_count"] == 1

    def test_snapshot_empty(self):
        snapshot = get_registry_cache_snapshot()
        assert snapshot["details"] == {}
        assert snapshot["names"] == {}
