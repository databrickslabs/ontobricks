"""Tests for back.objects.domain.version_status – TTL cache."""

import time
import pytest

from back.objects.domain.version_status import (
    clear_version_status_cache,
    get_cached_version_status,
    set_cached_version_status,
    get_version_status_cache_snapshot,
    _version_status_cache,
)


class TestVersionStatusCache:
    def setup_method(self):
        clear_version_status_cache()

    def test_set_and_get(self):
        set_cached_version_status("key1", {"status": "ok"})
        assert get_cached_version_status("key1") == {"status": "ok"}

    def test_get_missing_key(self):
        assert get_cached_version_status("nonexistent") is None

    def test_clear(self):
        set_cached_version_status("k", "v")
        clear_version_status_cache()
        assert get_cached_version_status("k") is None

    def test_expired_entry(self):
        set_cached_version_status("exp", "data")
        _version_status_cache["exp"]["_ts"] = time.time() - 60
        assert get_cached_version_status("exp") is None

    def test_snapshot(self):
        set_cached_version_status("snap", {"x": 1})
        snapshot = get_version_status_cache_snapshot()
        assert "ttl_seconds" in snapshot
        assert "snap" in snapshot["entries"]
        entry = snapshot["entries"]["snap"]
        assert "age_seconds" in entry
        assert "ttl_remaining" in entry

    def test_snapshot_empty(self):
        snapshot = get_version_status_cache_snapshot()
        assert snapshot["entries"] == {}
