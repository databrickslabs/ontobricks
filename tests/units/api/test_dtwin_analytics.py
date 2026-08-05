"""Tests for the analytics preflight check and compute endpoint.

These cover the three-prerequisite preflight (toggle, job name, mapped
snapshot) and the fourth check (snapshot has rows).  The computation path
itself is covered by tests/units/dtwin/test_dtwin_analytics.py.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit


def test_preflight_reports_each_prerequisite_in_order(monkeypatch):
    """Each failure names its own remedy; the toggle being off names none."""
    from back.core.graph_analysis import preflight

    monkeypatch.setattr(preflight, "resolve_analytics_job_enabled", lambda d, s: False)
    assert preflight.analytics_job_status(object(), object()) == (False, "")

    monkeypatch.setattr(preflight, "resolve_analytics_job_enabled", lambda d, s: True)
    monkeypatch.setattr(preflight, "resolve_analytics_job_name", lambda s: "")
    ok, reason = preflight.analytics_job_status(object(), object())
    assert ok is False and "ONTOBRICKS_ANALYTICS_JOB_NAME" in reason

    monkeypatch.setattr(preflight, "resolve_analytics_job_name", lambda s: "job")
    monkeypatch.setattr(
        preflight, "resolve_analytics_source", lambda d, s: ("", "no table")
    )
    ok, reason = preflight.analytics_job_status(object(), object())
    assert ok is False and reason == "no table"


def test_preflight_requires_a_non_empty_data_table(monkeypatch):
    """A resolvable but empty …_data means 'Build first', not 'unsupported'."""
    from back.core.graph_analysis import preflight

    monkeypatch.setattr(preflight, "resolve_analytics_job_enabled", lambda d, s: True)
    monkeypatch.setattr(preflight, "resolve_analytics_job_name", lambda s: "job")
    monkeypatch.setattr(
        preflight, "resolve_analytics_source", lambda d, s: ("cat.sch.t_data", "")
    )
    monkeypatch.setattr(preflight, "data_table_has_rows", lambda d, s, t: False)

    ok, reason = preflight.analytics_job_status(object(), object())
    assert ok is False
    assert "Build" in reason


def test_the_scheduler_gates_on_the_same_preflight(monkeypatch):
    """A scheduled run fails with the actionable reason, not a job error."""
    from types import SimpleNamespace

    import back.core.graph_analysis as graph_analysis
    from back.core.errors import ValidationError
    from back.objects.registry.scheduler_tasks import analytics

    monkeypatch.setattr(
        graph_analysis,
        "analytics_job_status",
        lambda d, s: (False, "The mapped-triples table is missing or empty."),
    )
    ctx = SimpleNamespace(
        domain=object(),
        settings=object(),
        graph_name="Dom_V1",
        tm=None,
        task_id="",
        progress=lambda pct, msg: None,
    )

    with pytest.raises(ValidationError) as exc:
        analytics.run(ctx)
    assert "missing or empty" in str(exc.value)
