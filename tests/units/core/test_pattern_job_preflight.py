"""Pattern discovery is job-only, so every blocked state must name its remedy.

Community and graph analytics may stay silent when the admin toggle is off
(in-app fallbacks exist). Patterns have no in-app path: with the toggle off the
Discover page must say which setting to enable instead of a generic message.
"""

from unittest.mock import patch

import pytest

from back.core.graph_analysis.preflight import pattern_job_configured, pattern_job_status

MODULE = "back.core.graph_analysis.preflight"


class _Settings:
    pattern_job_name = ""
    ontobricks_app_name = ""
    analytics_job_name = "some-graph-analytics"


def _configured(*, enabled=True, job_name="some-pattern-detection", source=("cat.sch.t_data", "")):
    with patch(f"{MODULE}.resolve_analytics_job_enabled", return_value=enabled), patch(
        f"{MODULE}.resolve_pattern_job_name", return_value=job_name
    ), patch(f"{MODULE}.resolve_analytics_source", return_value=source):
        return pattern_job_configured(object(), _Settings())


class TestToggleOffExplainsItself:
    def test_toggle_off_names_the_admin_setting(self):
        available, reason = _configured(enabled=False)
        assert available is False
        assert "Compute large-graph metrics on Databricks" in reason
        assert "Settings" in reason

    def test_toggle_off_never_probes_the_source(self):
        with patch(f"{MODULE}.resolve_analytics_job_enabled", return_value=False), patch(
            f"{MODULE}.resolve_analytics_source"
        ) as source, patch(f"{MODULE}.resolve_pattern_job_name") as name:
            pattern_job_configured(object(), _Settings())
        source.assert_not_called()
        name.assert_not_called()

    def test_status_reports_the_same_reason_without_probing_the_table(self):
        with patch(f"{MODULE}.resolve_analytics_job_enabled", return_value=False), patch(
            f"{MODULE}.probe_data_table"
        ) as probe:
            available, reason = pattern_job_status(object(), _Settings())
        assert available is False
        assert "Compute large-graph metrics on Databricks" in reason
        probe.assert_not_called()


class TestEachCauseIsNamed:
    def test_missing_job_name_names_the_env_var(self):
        available, reason = _configured(job_name="")
        assert available is False
        assert "ONTOBRICKS_PATTERN_JOB_NAME" in reason

    def test_missing_source_is_reported(self):
        available, reason = _configured(source=("", ""))
        assert available is False
        assert reason

    def test_all_prerequisites_met(self):
        assert _configured() == (True, "")


@pytest.mark.parametrize("enabled", [True, False])
def test_reason_is_never_empty_when_unavailable(enabled):
    available, reason = _configured(enabled=enabled, job_name="")
    assert available is False
    assert reason.strip()
