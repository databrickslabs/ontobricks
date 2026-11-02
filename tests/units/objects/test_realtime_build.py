"""A ``realtime`` domain builds like ``microbatch`` (continuous slots, self-refreshing view)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from back.core.graphdb import ModeDecision
from back.objects.digitaltwin.build import TwinLakebaseSync
from tests.units.objects import test_build_pipeline_modes as _modes
from tests.units.objects.test_build_pipeline_materialize import _DATA

pytestmark = pytest.mark.unit

REALTIME_SOURCE = {
    "kind": "kafka",
    "bootstrap_servers": "broker-1:9092",
    "topic": "sales.customers",
    "secret_scope": "ontobricks",
    "secret_key": "kafka-sasl",
    "key_format": "json",
}


def realtime_pipeline(*, enabled: bool = True, source: dict | None = None, **kw):
    """A ``realtime`` domain with its topic configured; *enabled* is the deployment setting."""
    pipe = _modes._pipeline("realtime", backend="lakebase", **kw)
    pipe.domain.info["realtime_source"] = dict(REALTIME_SOURCE if source is None else source)
    pipe.settings = SimpleNamespace(
        databricks_triplestore_table="", realtime_enabled=enabled, realtime_job_name=""
    )
    return pipe


@pytest.fixture(autouse=True)
def _no_stream(monkeypatch):
    """The stream job hook records what it would do; ``test_realtime_stream`` covers it."""
    from back.objects.digitaltwin.build import TwinRealtimeStream

    calls: list = []
    monkeypatch.setattr(
        TwinRealtimeStream, "after_build", staticmethod(lambda pipe: calls.append(pipe))
    )
    return calls


class TestRealtimeRunsOnTheMicrobatchPipeline:
    def test_a_configured_and_enabled_domain_runs_realtime_with_a_continuous_pipeline(self):
        pipe = realtime_pipeline()

        log, _, _ = _modes._run(pipe, existing="pipe-1", stored="fp-current")

        assert pipe.mode_decision == ModeDecision("realtime", "realtime", "")
        assert _modes._ensures(log) == [(True, "fp-current")]
        assert _modes._updates(log) == [("start", False), ("wait", True)]

    def test_the_stream_hook_runs_after_the_pipeline_and_the_view(self, _no_stream):
        pipe = realtime_pipeline()

        _modes._run(pipe, existing="pipe-1", stored="fp-current")

        assert _no_stream == [pipe] and pipe.data_relation_mode == "materialized_view"

    def test_its_view_refreshes_itself_between_builds(self):
        pipe = realtime_pipeline(entities={_modes._CUSTOMER: _modes._row_local()})

        _, _, warehouse = _modes._run(pipe)

        data_mv = next(
            s for s in warehouse.calls if s.startswith(f"CREATE OR REPLACE MATERIALIZED VIEW {_DATA}")
        )
        assert "\n  TRIGGER ON UPDATE\n" in data_mv

    @pytest.mark.parametrize(
        ("enabled", "source"),
        [(False, None), (True, {**REALTIME_SOURCE, "topic": ""})],
        ids=["setting-off", "topic-missing"],
    )
    def test_without_its_topic_it_runs_microbatch(self, enabled, source):
        pipe = realtime_pipeline(enabled=enabled, source=source)

        _modes._run(pipe, existing="pipe-1", stored="fp-current")

        assert pipe.mode_decision == ModeDecision("realtime", "microbatch", "realtime_not_available")


def test_the_synced_table_of_a_realtime_domain_streams_continuously():
    plan = TwinLakebaseSync.sync_plan_for(
        view_table="c.s.v",
        data_table=_DATA,
        mode_effective="realtime",
        configured_sync_table_mode="continuous",
    )

    assert plan.sync_mode == "continuous"
