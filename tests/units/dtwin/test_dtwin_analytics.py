"""Tests for the analytics entry point on DigitalTwin.

The metric arithmetic lives in ``tests/units/core/test_job_metrics.py``. What is
covered here is the gate in front of it: a domain whose mapped snapshot cannot
be resolved must fail loudly, before a Lakeflow run is paid for.  A failed job
run must propagate rather than silently degrading to a different compute path.
"""

from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import InfrastructureError, OntoBricksError
from back.objects.digitaltwin import DigitalTwin

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Minimal stand-ins
# ---------------------------------------------------------------------------


class _FakeResult:
    def to_dict(self):
        return {"mode": "job"}


class _FakeJobMetrics:
    def compute(self, request, *, on_progress=None):
        return _FakeResult()


class _RaisingJobMetrics:
    def compute(self, request, *, on_progress=None):
        raise OntoBricksError("job run failed")


def _fake_domain(session=None):
    m = MagicMock()
    m.uc_domain_folder = "folder"
    m.current_version = "1"
    return m


def _fake_settings():
    return MagicMock()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def domain(domain_session):
    domain_session._data["domain"]["info"] = {"name": "AcmeConsulting"}
    domain_session._data["domain"]["current_version"] = "1"
    return domain_session


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_an_unresolvable_source_fails_before_any_job_is_launched(domain, monkeypatch):
    """A job run costs money and minutes: refuse before spending either."""
    from back.core.helpers.SQLHelpers import SQLHelpers

    monkeypatch.setattr(
        SQLHelpers,
        "effective_databricks_table",
        staticmethod(lambda domain, settings=None: ""),
    )
    build = MagicMock()
    with patch.object(DigitalTwin, "build_job_metrics", build):
        with pytest.raises(InfrastructureError) as excinfo:
            DigitalTwin(domain).compute_graph_metrics(
                "triples",
                settings=object(),
            )

    # The remedy has to be the one action the user can actually take.
    assert "Build" in str(excinfo.value.detail)
    build.assert_not_called()


def test_compute_always_runs_the_job(monkeypatch):
    """No mode argument, no fallback: one path."""
    calls = []
    monkeypatch.setattr(
        DigitalTwin,
        "build_job_metrics",
        staticmethod(lambda *a, **kw: calls.append(kw) or _FakeJobMetrics()),
    )
    monkeypatch.setattr(
        "back.core.graph_analysis.resolve_analytics_source",
        lambda d, s: ("cat.sch.t_data", ""),
    )
    DigitalTwin(_fake_domain()).compute_graph_metrics(
        "cat.sch.graph", settings=_fake_settings()
    )
    assert calls[0]["source_table"] == "cat.sch.t_data"


def test_compute_raises_when_the_source_is_missing(monkeypatch):
    """Hard fail — a thinner KPI set is what this work removed."""
    monkeypatch.setattr(
        "back.core.graph_analysis.resolve_analytics_source",
        lambda d, s: ("", "Run Knowledge Graph → Build first"),
    )
    with pytest.raises(InfrastructureError) as exc:
        DigitalTwin(_fake_domain()).compute_graph_metrics(
            "cat.sch.graph", settings=_fake_settings()
        )
    assert "Build" in str(exc.value.detail)


def test_failed_job_run_raises_not_degrades(monkeypatch):
    """A raising job_metrics.compute must propagate, not fall back to pushdown."""
    monkeypatch.setattr(
        DigitalTwin,
        "build_job_metrics",
        staticmethod(lambda *a, **kw: _RaisingJobMetrics()),
    )
    monkeypatch.setattr(
        "back.core.graph_analysis.resolve_analytics_source",
        lambda d, s: ("cat.sch.t_data", ""),
    )
    with pytest.raises(OntoBricksError):
        DigitalTwin(_fake_domain()).compute_graph_metrics(
            "cat.sch.graph", settings=_fake_settings()
        )
