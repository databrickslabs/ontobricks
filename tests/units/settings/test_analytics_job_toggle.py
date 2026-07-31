"""Settings → Global toggle for the serverless graph-analytics job.

Covers the storage getter/setter, the admin-gated service methods, and the UI
wiring. The behaviour that matters throughout is the three-state getter: an
unset toggle must fall through to the ``ONTOBRICKS_ANALYTICS_JOB_ENABLED``
deployment default, while a stored ``False`` must override an env var that
enables the job.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import AuthorizationError, InfrastructureError
from back.objects.domain.SettingsService import SettingsService
from back.objects.session.GlobalConfigService import GlobalConfigService

REPO_ROOT = Path(__file__).resolve().parents[3]
SETTINGS_HTML = REPO_ROOT / "src/front/templates/settings.html"
SETTINGS_JS = REPO_ROOT / "src/front/static/config/js/settings.js"

RCFG = {"catalog": "main", "schema": "ob"}


def _svc(stored: dict) -> GlobalConfigService:
    """A service whose ``load`` returns *stored* without touching a registry."""
    svc = GlobalConfigService()
    svc.load = lambda *a, **k: dict(stored)  # type: ignore[assignment]
    return svc


class TestGetAnalyticsJobEnabled:
    def test_absent_key_returns_none_not_false(self):
        # The crux: absent must be distinguishable from an explicit off, or the
        # env-var fallback is unreachable.
        assert _svc({}).get_analytics_job_enabled("", "", RCFG) is None

    def test_empty_string_returns_none(self):
        assert (
            _svc({"analytics_job_enabled": ""}).get_analytics_job_enabled("", "", RCFG)
            is None
        )

    def test_explicit_false_is_preserved(self):
        assert (
            _svc({"analytics_job_enabled": False}).get_analytics_job_enabled(
                "", "", RCFG
            )
            is False
        )

    def test_explicit_true_is_preserved(self):
        assert (
            _svc({"analytics_job_enabled": True}).get_analytics_job_enabled("", "", RCFG)
            is True
        )

    @pytest.mark.parametrize("raw", ["true", "TRUE", " yes ", "on", "1"])
    def test_truthy_strings(self, raw):
        assert (
            _svc({"analytics_job_enabled": raw}).get_analytics_job_enabled("", "", RCFG)
            is True
        )

    @pytest.mark.parametrize("raw", ["false", "FALSE", " no ", "off", "0"])
    def test_falsy_strings(self, raw):
        # JSON round-trips through a text column in some backends, so a stored
        # "false" must not read back as True the way a bare truthiness check
        # would have it.
        assert (
            _svc({"analytics_job_enabled": raw}).get_analytics_job_enabled("", "", RCFG)
            is False
        )

    def test_unparseable_value_returns_none(self):
        assert (
            _svc({"analytics_job_enabled": "maybe"}).get_analytics_job_enabled(
                "", "", RCFG
            )
            is None
        )

    def test_absent_from_the_empty_template(self):
        # Present in _empty() would mean a failed load yields a concrete value
        # and masks the env fallback.
        assert "analytics_job_enabled" not in GlobalConfigService._empty()


class TestSetAnalyticsJobEnabled:
    def test_persists_a_real_bool(self):
        svc = _svc({})
        captured = {}
        svc._save = lambda h, t, r, updates: captured.update(updates) or (True, "ok")
        svc.set_analytics_job_enabled("", "", RCFG, True)
        assert captured == {"analytics_job_enabled": True}

    def test_coerces_truthy_input_to_bool(self):
        svc = _svc({})
        captured = {}
        svc._save = lambda h, t, r, updates: captured.update(updates) or (True, "ok")
        svc.set_analytics_job_enabled("", "", RCFG, "yes")
        assert captured["analytics_job_enabled"] is True


def _settings(env_default: bool) -> MagicMock:
    s = MagicMock()
    s.analytics_job_enabled = env_default
    return s


class TestServiceRead:
    def _result(self, configured, env_default):
        with patch.object(
            SettingsService,
            "_resolve_context",
            return_value=(MagicMock(), "https://h", "tok", RCFG),
        ), patch(
            "back.objects.domain.SettingsService.global_config_service"
            ".get_analytics_job_enabled",
            return_value=configured,
        ):
            return SettingsService.get_analytics_job_enabled_result(
                MagicMock(), _settings(env_default)
            )

    def test_reports_admin_value_and_provenance(self):
        r = self._result(True, False)
        assert r["analytics_job_enabled"] is True
        assert r["source"] == "admin"

    def test_reports_env_default_when_unset(self):
        r = self._result(None, True)
        assert r["analytics_job_enabled"] is True
        assert r["source"] == "default"
        # The UI shows this so an admin knows what they are overriding.
        assert r["env_default"] is True

    def test_admin_off_beats_env_on(self):
        r = self._result(False, True)
        assert r["analytics_job_enabled"] is False
        assert r["source"] == "admin"

    def test_lookup_failure_falls_back_to_env(self):
        with patch.object(
            SettingsService,
            "_resolve_context",
            return_value=(MagicMock(), "https://h", "tok", RCFG),
        ), patch(
            "back.objects.domain.SettingsService.global_config_service"
            ".get_analytics_job_enabled",
            side_effect=RuntimeError("registry down"),
        ):
            r = SettingsService.get_analytics_job_enabled_result(
                MagicMock(), _settings(True)
            )
        assert r["analytics_job_enabled"] is True
        assert r["source"] == "default"


class TestServiceWrite:
    def test_requires_admin_before_writing(self):
        with patch.object(
            SettingsService,
            "require_admin_error",
            side_effect=AuthorizationError("nope"),
        ), patch(
            "back.objects.domain.SettingsService.global_config_service"
            ".set_analytics_job_enabled"
        ) as setter:
            with pytest.raises(AuthorizationError):
                SettingsService.save_analytics_job_enabled_result(
                    True, "u@x", "tok", MagicMock(), _settings(False)
                )
        setter.assert_not_called()

    def test_persists_and_echoes_the_value(self):
        with patch.object(SettingsService, "require_admin_error"), patch.object(
            SettingsService,
            "_resolve_context",
            return_value=(MagicMock(), "https://h", "tok", RCFG),
        ), patch(
            "back.objects.domain.SettingsService.global_config_service"
            ".set_analytics_job_enabled",
            return_value=(True, "saved"),
        ) as setter:
            r = SettingsService.save_analytics_job_enabled_result(
                True, "u@x", "tok", MagicMock(), _settings(False)
            )
        assert r == {"success": True, "analytics_job_enabled": True, "source": "admin"}
        assert setter.call_args[0][3] is True

    def test_persists_an_explicit_off(self):
        with patch.object(SettingsService, "require_admin_error"), patch.object(
            SettingsService,
            "_resolve_context",
            return_value=(MagicMock(), "https://h", "tok", RCFG),
        ), patch(
            "back.objects.domain.SettingsService.global_config_service"
            ".set_analytics_job_enabled",
            return_value=(True, "saved"),
        ) as setter:
            r = SettingsService.save_analytics_job_enabled_result(
                False, "u@x", "tok", MagicMock(), _settings(True)
            )
        assert r["analytics_job_enabled"] is False
        assert setter.call_args[0][3] is False

    def test_store_failure_raises(self):
        with patch.object(SettingsService, "require_admin_error"), patch.object(
            SettingsService,
            "_resolve_context",
            return_value=(MagicMock(), "https://h", "tok", RCFG),
        ), patch(
            "back.objects.domain.SettingsService.global_config_service"
            ".set_analytics_job_enabled",
            return_value=(False, "disk full"),
        ):
            with pytest.raises(InfrastructureError):
                SettingsService.save_analytics_job_enabled_result(
                    True, "u@x", "tok", MagicMock(), _settings(False)
                )


class TestUiWiring:
    @pytest.fixture(scope="class")
    def html(self) -> str:
        return SETTINGS_HTML.read_text()

    @pytest.fixture(scope="class")
    def js(self) -> str:
        return SETTINGS_JS.read_text()

    def test_checkbox_exists_in_the_global_section(self, html):
        assert 'id="analyticsJobEnabled"' in html
        assert 'type="checkbox"' in html.split('id="analyticsJobEnabled"')[0][-120:]

    def test_marked_admin_only(self, html):
        # It changes behaviour for every user of the instance.
        block = html.split("Knowledge-Graph Analytics")[1][:900]
        assert "settings-badge-admin-note" in block

    def test_help_text_states_the_prerequisites(self, html):
        block = html.split("Knowledge-Graph Analytics")[1][:2600]
        assert "bundle to have been deployed" in block
        assert "built at least once" in block
        assert "estimates" in block

    def test_help_text_does_not_promise_a_fallback(self, html):
        """Analytics has one compute path; the copy must not imply otherwise."""
        block = html.split("Knowledge-Graph Analytics")[1][:2600]
        assert "engine-side" not in block
        assert "managed_synced" not in block, (
            "the engine no longer decides whether analytics can run"
        )

    def test_hydrated_on_page_load(self, js):
        assert "loadAnalyticsJobEnabled();" in js
        assert "'/settings/analytics-job-enabled'" in js

    def test_saved_by_the_shared_save_handler(self, js):
        assert "'/settings/save-analytics-job-enabled'" in js

    def test_saved_unconditionally_so_unchecking_persists(self, js):
        """Unchecking must POST too, otherwise "off" can never be stored.

        A ``if (checked)`` guard would make the toggle one-way: an admin could
        turn the job on but never turn it back off against an env var that
        enables it.
        """
        assert "analytics_job_enabled: analyticsJobInput.checked" in js
        assert "if (analyticsJobInput.checked)" not in js


class TestUnhydratedCheckboxIsNeverPersisted:
    """An unread checkbox must not be saved as "off".

    The Save handler is bound to *every* section's Save button and posts this
    checkbox on each one. Its markup default is unchecked, so before this guard a
    hydration GET that failed — or simply had not resolved yet — turned the next
    Save anywhere in Settings into a silent write of ``false`` over an admin's
    ``true``, with a success toast and nobody having touched the control. That is
    the reported "the option is not saved in the registry".
    """

    @pytest.fixture(scope="class")
    def js(self) -> str:
        return SETTINGS_JS.read_text()

    @pytest.fixture(scope="class")
    def hydrate_fn(self, js: str) -> str:
        start = js.index("async function loadAnalyticsJobEnabled")
        return js[start : js.index("\n    }", start)]

    def test_a_hydration_flag_exists(self, js):
        assert "let analyticsJobHydrated = false;" in js

    def test_save_is_gated_on_the_flag(self, js):
        assert "if (analyticsJobInput && analyticsJobHydrated) {" in js

    def test_the_old_ungated_guard_is_gone(self, js):
        """``if (analyticsJobInput) {`` alone is what allowed the clobber."""
        assert "if (analyticsJobInput) {" not in js

    def test_a_failed_read_is_reported_rather_than_saved(self, js):
        assert "could not be read, so it was left" in js

    def test_flag_is_cleared_before_each_read(self, hydrate_fn):
        # Re-entrant: a later failed refresh must not leave a stale "hydrated".
        assert "analyticsJobHydrated = false;" in hydrate_fn

    def test_flag_is_set_only_after_the_value_is_applied(self, hydrate_fn):
        applied = hydrate_fn.index("input.checked = !!result.analytics_job_enabled")
        marked = hydrate_fn.index("analyticsJobHydrated = true")
        assert applied < marked, "the flag must not be set before the value lands"

    def test_success_check_precedes_the_flag(self, hydrate_fn):
        """A ``success: false`` body must not count as hydrated."""
        assert hydrate_fn.index("if (!result.success) return;") < hydrate_fn.index(
            "analyticsJobHydrated = true"
        )

    def test_checkbox_is_disabled_until_hydrated(self, hydrate_fn):
        assert "input.disabled = true;" in hydrate_fn
        assert "input.disabled = false;" in hydrate_fn
        assert hydrate_fn.index("input.disabled = true;") < hydrate_fn.index(
            "input.disabled = false;"
        )

    def test_failure_is_surfaced_in_the_source_note(self, hydrate_fn):
        assert "Could not read the current setting" in hydrate_fn
        assert "finally" in hydrate_fn

    def test_failure_still_caught_so_init_is_not_disrupted(self, hydrate_fn):
        """Hydration runs un-awaited at load; a throw must not reject upward."""
        assert "catch" in hydrate_fn
