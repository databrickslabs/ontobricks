"""Why job mode is unavailable has to reach the user, not just the logs.

An admin who has already ticked "Compute large-graph metrics on Databricks" and
is still told to go and tick it has been given no information. Each prerequisite
has a different remedy, so ``analytics_job_status`` reports which one is
missing. ``resolve_analytics_source`` already writes those strings for a reader;
the endpoint used to discard them with ``[0]``.

The check is split in two on cost. ``analytics_job_configured`` is the three
free checks and is what the stats payload calls on every page render;
``analytics_job_status`` adds the warehouse probe and is for the caller about to
launch a job.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from back.core.graph_analysis.preflight import (
    analytics_job_configured,
    analytics_job_status,
)

MODULE = "back.core.graph_analysis.preflight"


class _Settings:
    analytics_job_name = "some-job"
    ontobricks_app_name = ""


def _status(
    *,
    enabled=True,
    job_name="some-job",
    spark=("cat.sch.tbl", ""),
    has_rows=True,
):
    with patch(f"{MODULE}.resolve_analytics_job_enabled", return_value=enabled), patch(
        f"{MODULE}.resolve_analytics_job_name", return_value=job_name
    ), patch(f"{MODULE}.resolve_analytics_source", return_value=spark), patch(
        f"{MODULE}.data_table_has_rows", return_value=has_rows
    ):
        return analytics_job_status(object(), _Settings())


class TestAvailable:
    def test_all_prerequisites_met(self):
        assert _status() == (True, "")

    def test_no_reason_is_produced_when_it_is_available(self):
        _available, reason = _status()
        assert reason == ""


class TestToggleOff:
    def test_unavailable_without_a_reason(self):
        """Not using the job is the configured behaviour, not a fault."""
        assert _status(enabled=False) == (False, "")

    def test_nothing_else_is_probed(self):
        """Source resolution can be a remote call; skip it when the toggle is off."""
        with patch(f"{MODULE}.resolve_analytics_job_enabled", return_value=False), patch(
            f"{MODULE}.resolve_analytics_source"
        ) as spark, patch(f"{MODULE}.resolve_analytics_job_name") as name:
            analytics_job_status(object(), _Settings())
        spark.assert_not_called()
        name.assert_not_called()


class TestEachCauseIsNamed:
    def test_missing_job_name_names_the_env_var(self):
        available, reason = _status(job_name="")
        assert available is False
        assert "ONTOBRICKS_ANALYTICS_JOB_NAME" in reason

    def test_spark_reason_is_passed_through_verbatim(self):
        """The store's wording is the useful part — do not paraphrase it."""
        detail = (
            "This Lakebase graph is in app_managed mode, so its triples exist "
            "only in Postgres and are not readable from Spark."
        )
        available, reason = _status(spark=("", detail))
        assert available is False
        assert reason == detail

    def test_a_reasonless_spark_refusal_still_gets_a_message(self):
        available, reason = _status(spark=("", ""))
        assert available is False
        assert reason, "an unavailable job must always explain itself"

    def test_empty_table_names_the_build_remedy(self):
        """A snapshot that exists but is empty should say 'Build first'."""
        available, reason = _status(has_rows=False)
        assert available is False
        assert "Build" in reason

    def test_an_unreachable_warehouse_does_not_blame_the_build(self):
        """Rebuilding a domain does not wake a sleeping warehouse."""
        available, reason = _status(has_rows=None)
        assert available is False
        assert "retry" in reason.lower()
        assert "Build" not in reason

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"job_name": ""},
            {"spark": ("", "some reason")},
            {"spark": ("", "")},
            {"has_rows": False},
        ],
    )
    def test_every_blocked_case_is_reported_as_unavailable(self, kwargs):
        available, reason = _status(**kwargs)
        assert available is False
        assert reason != ""


class TestTheCheapCheckStaysCheap:
    """The stats payload renders on every page; it must not hit the warehouse."""

    def test_the_probe_is_never_run(self):
        with patch(
            f"{MODULE}.resolve_analytics_job_enabled", return_value=True
        ), patch(
            f"{MODULE}.resolve_analytics_job_name", return_value="some-job"
        ), patch(
            f"{MODULE}.resolve_analytics_source", return_value=("cat.sch.tbl", "")
        ), patch(
            f"{MODULE}.data_table_has_rows"
        ) as probe:
            assert analytics_job_configured(object(), _Settings()) == (True, "")
        probe.assert_not_called()

    def test_the_stats_endpoint_uses_it(self):
        assert (
            "analytics_job_configured(domain, settings)" in ROUTER.read_text()
        )


class TestCauseOrdering:
    """The first missing prerequisite is the one worth reporting."""

    def test_job_name_is_reported_before_spark(self):
        _a, reason = _status(job_name="", spark=("", "a spark problem"))
        assert "ONTOBRICKS_ANALYTICS_JOB_NAME" in reason


ROOT = Path(__file__).resolve().parents[3]
PANEL = ROOT / "src/front/static/query/js/query-analytics.js"
ROUTER = ROOT / "src/api/routers/internal/dtwin.py"


class TestTheReasonReachesTheBanner:
    """A reason computed and then dropped on the floor helps nobody."""

    def test_the_endpoint_returns_the_field(self):
        assert '"analytics_job_blocked_reason": job_blocked_reason' in ROUTER.read_text()

    def test_the_panel_reads_it(self):
        assert "data.analytics_job_blocked_reason" in PANEL.read_text()

    def test_the_banner_shows_it_instead_of_the_useless_advice(self):
        panel = PANEL.read_text()
        assert "_jobBlockedReason" in panel
        assert "but the job cannot run" in panel

    def test_the_enable_it_advice_is_conditional_now(self):
        """Only offer 'go and enable it' when it is genuinely not enabled."""
        panel = PANEL.read_text()
        advice = "or enable <strong>Compute large-graph metrics on Databricks</strong>"
        assert advice in panel
        before = panel.split(advice)[0]
        assert "_jobBlockedReason" in before, (
            "the advice must sit on the branch taken when nothing is blocking, "
            "otherwise an admin who already enabled it is told to enable it"
        )


class TestStatsCacheDoesNotHideTheField:
    """A cache entry predating the field would mask a just-changed setting."""

    def test_a_payload_without_the_field_is_treated_as_stale(self):
        router = ROUTER.read_text()
        assert '"analytics_job_blocked_reason" in cached' in router
        assert "has_kind and has_job_reason" in router


class TestStoredResultsBackwardCompat:
    """Cached rows written before the Lakeflow-only change must still render.

    Rows persisted by the old paths carry ``mode="in_memory"`` or
    ``mode="pushdown"``. The guarantee is the *absence* of a gate: the read path
    must not compare ``mode`` against the surviving constant, because that would
    hide every result computed before this change.
    """

    def test_the_read_path_does_not_gate_on_mode(self):
        router = ROUTER.read_text()
        latest = router.split('@router.get("/metrics/latest")')[1].split("@router.")[0]
        assert "**result," in latest, (
            "the stored result should be spread through untouched"
        )
        assert "MODE_JOB" not in latest, (
            "comparing a stored mode against MODE_JOB would drop legacy results"
        )
