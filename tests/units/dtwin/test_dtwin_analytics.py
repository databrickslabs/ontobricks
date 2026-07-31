"""Tests for the analytics entry point on DigitalTwin.

The metric arithmetic lives in ``tests/units/core/test_job_metrics.py``. What is
covered here is the gate in front of it: a domain whose mapped snapshot cannot
be resolved must fail loudly, before a Lakeflow run is paid for.
"""

from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import InfrastructureError
from back.core.graph_analysis import MODE_JOB
from back.objects.digitaltwin import DigitalTwin

pytestmark = pytest.mark.unit


@pytest.fixture
def domain(domain_session):
    domain_session._data["domain"]["info"] = {"name": "AcmeConsulting"}
    domain_session._data["domain"]["current_version"] = "1"
    return domain_session


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
                MagicMock(),
                "triples",
                mode=MODE_JOB,
                settings=object(),
            )

    # The remedy has to be the one action the user can actually take.
    assert "Build" in str(excinfo.value.detail)
    build.assert_not_called()
