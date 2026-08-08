"""Flipping the analytics-job toggle must not leave a stale Analytics banner.

``/dtwin/sync/stats`` carries ``analytics_job_available`` and
``analytics_job_blocked_reason``, and the Analytics page fetches it *without*
``refresh=true`` because the aggregate counts behind it are expensive on a large
graph. That payload is cached on the domain session for
``_TS_STATS_CACHE_TTL_SECONDS``, and its validity check only tests that the
fields are *present*, not that they still reflect the stored toggle.

So an admin who ticked "Compute large-graph metrics on Databricks" and went
straight to Analytics was told, for up to five minutes, to go and enable the
setting they had just enabled — indistinguishable from the save having failed.
Saving the toggle therefore has to drop the cached stats section.
"""

from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.objects.digitaltwin.DigitalTwin import DigitalTwin
from back.objects.domain.SettingsService import SettingsService

pytestmark = pytest.mark.unit

RCFG = {"catalog": "main", "schema": "ob"}


def _domain() -> SimpleNamespace:
    return SimpleNamespace(triplestore={}, save=MagicMock())


class TestClearTsCache:
    def test_a_written_section_is_readable_then_clearable(self):
        domain = _domain()
        dt = DigitalTwin(domain)
        dt.set_ts_cache("stats", {"total_triples": 7})
        assert dt.get_ts_cache("stats") == {"total_triples": 7}

        dt.clear_ts_cache("stats")
        assert dt.get_ts_cache("stats") is None

    def test_other_sections_survive(self):
        """Only the named section goes; dt_existence is unrelated and costly."""
        domain = _domain()
        dt = DigitalTwin(domain)
        dt.set_ts_cache("stats", {"total_triples": 7})
        dt.set_ts_cache("dt_existence", {"view_exists": True})

        dt.clear_ts_cache("stats")
        assert dt.get_ts_cache("dt_existence") == {"view_exists": True}

    def test_clearing_an_absent_section_is_a_noop(self):
        domain = _domain()
        dt = DigitalTwin(domain)
        dt.clear_ts_cache("stats")
        assert dt.get_ts_cache("stats") is None

    def test_the_drop_is_persisted(self):
        """The cache lives on the session document, so it has to be saved."""
        domain = _domain()
        dt = DigitalTwin(domain)
        dt.set_ts_cache("stats", {"total_triples": 7})
        domain.save.reset_mock()
        dt.clear_ts_cache("stats")
        domain.save.assert_called_once()


def _settings(env_default: bool) -> MagicMock:
    s = MagicMock()
    s.analytics_job_enabled = env_default
    return s


def _save(enabled: bool, domain, *, ok: bool = True):
    with patch.object(SettingsService, "require_admin_error"), patch.object(
        SettingsService,
        "_resolve_context",
        return_value=(domain, "https://h", "tok", RCFG),
    ), patch(
        "back.objects.domain.SettingsService.global_config_service"
        ".set_analytics_job_enabled",
        return_value=(ok, "saved" if ok else "disk full"),
    ):
        return SettingsService.save_analytics_job_enabled_result(
            enabled, "u@x", "tok", MagicMock(), _settings(False)
        )


class TestSaveDropsTheStatsCache:
    def test_turning_it_on_invalidates_the_banner_payload(self):
        domain = _domain()
        DigitalTwin(domain).set_ts_cache(
            "stats",
            {"analytics_job_available": False, "analytics_job_blocked_reason": ""},
        )

        _save(True, domain)

        assert DigitalTwin(domain).get_ts_cache("stats") is None

    def test_turning_it_off_invalidates_too(self):
        """The banner has to stop promising Databricks just as promptly."""
        domain = _domain()
        DigitalTwin(domain).set_ts_cache(
            "stats",
            {"analytics_job_available": True, "analytics_job_blocked_reason": ""},
        )

        _save(False, domain)

        assert DigitalTwin(domain).get_ts_cache("stats") is None

    def test_unrelated_cached_sections_are_kept(self):
        domain = _domain()
        dt = DigitalTwin(domain)
        dt.set_ts_cache("stats", {"analytics_job_available": False})
        dt.set_ts_cache("status", {"row_count": 12})

        _save(True, domain)

        assert DigitalTwin(domain).get_ts_cache("status") == {"row_count": 12}

    def test_an_invalidation_failure_does_not_fail_the_save(self):
        """The value is already committed; a cache miss is not worth a 500."""
        domain = SimpleNamespace(triplestore={}, save=MagicMock())
        DigitalTwin(domain).set_ts_cache("stats", {"analytics_job_available": False})
        domain.save.side_effect = RuntimeError("session store down")

        r = _save(True, domain)

        assert r["success"] is True

    def test_nothing_is_dropped_when_the_write_failed(self):
        """A rejected save leaves the stored value — and the payload — as they were."""
        domain = _domain()
        DigitalTwin(domain).set_ts_cache("stats", {"analytics_job_available": False})

        with pytest.raises(Exception):
            _save(True, domain, ok=False)

        assert DigitalTwin(domain).get_ts_cache("stats") is not None


class TestCacheTtlIsWhyThisMatters:
    def test_the_stats_section_would_otherwise_survive_for_minutes(self):
        """Guards the premise: without invalidation the payload is served on."""
        import importlib

        dt_module = importlib.import_module(
            "back.objects.digitaltwin.DigitalTwin"
        )
        assert dt_module._TS_STATS_CACHE_TTL_SECONDS >= 60
        domain = _domain()
        dt = DigitalTwin(domain)
        dt.set_ts_cache("stats", {"analytics_job_available": False})
        # Just inside the window, the stale payload is still what a caller gets.
        domain.triplestore["stats"]["stats"]["_ts"] = time.time() - 30
        assert dt.get_ts_cache("stats") is not None
