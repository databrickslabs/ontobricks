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


def _slot_sql(log) -> str:
    [ensure] = _modes._events(log, "ensure")
    return ensure[1]["sql_text"]


def _row_tracking_probes(warehouse) -> list:
    return [s for s in warehouse.calls if "delta.enableRowTracking" in s and s.startswith("SHOW")]


class TestTheSlotPipelineReadsTheTopic:
    """An effective ``realtime`` domain's slot pipeline consumes the stream job's topic."""

    def test_an_effective_realtime_domain_compiles_its_slot_pipeline_from_the_topic(self):
        pipe = realtime_pipeline()

        log, _, warehouse = _modes._run(pipe, existing="pipe-1", stored="fp-current")

        sql = _slot_sql(log)
        assert "STREAM read_kafka(" in sql and "sales.customers" in sql
        assert "readChangeFeed" not in sql
        assert pipe.mode_decision == ModeDecision("realtime", "realtime", "")

    def test_untracked_sources_neither_are_probed_nor_hold_it_back(self):
        pipe = realtime_pipeline()
        warehouse = _modes._Warehouse([], tracked=False)

        _, _, warehouse = _modes._run(pipe, warehouse=warehouse, existing="pipe-1", stored="fp-current")

        assert _row_tracking_probes(warehouse) == []
        assert pipe.mode_decision == ModeDecision("realtime", "realtime", "")

    def test_no_source_history_is_read_for_a_full_refresh(self):
        """A source rewrite does not reach the topic: no ``DESCRIBE HISTORY`` of the sources."""
        pipe = realtime_pipeline()

        _, _, warehouse = _modes._run(pipe, existing="pipe-1", stored="fp-current")

        assert not any("main.sales." in s and s.startswith("DESCRIBE HISTORY") for s in warehouse.calls)
        assert pipe.slot_refresh_facts["source_versions"] == {}

    @pytest.mark.parametrize(
        ("enabled", "source"),
        [
            (False, None),
            (True, {**REALTIME_SOURCE, "kind": "kinesis", "bootstrap_servers": "eu-west-1"}),
        ],
        ids=["realtime-not-available", "kinesis"],
    )
    def test_otherwise_it_reads_the_change_feed_and_probes_its_sources(self, enabled, source):
        pipe = realtime_pipeline(enabled=enabled, source=source)

        log, _, warehouse = _modes._run(pipe, existing="pipe-1", stored="fp-current")

        sql = _slot_sql(log)
        assert "readChangeFeed" in sql and "read_kafka" not in sql
        assert _row_tracking_probes(warehouse)

    def test_every_compile_of_the_domain_gives_the_same_slot_sql(self):
        """Build, scheduled tick, readiness and DAB renderer all go through ``compile_for``."""
        from back.objects.digitaltwin.build import TwinIncrementalBuild, TwinSlotFeed

        pipe = realtime_pipeline()

        log, _, _ = _modes._run(pipe, existing="pipe-1", stored="fp-current")
        spec = TwinIncrementalBuild.compile_for(
            pipe.domain,
            pipe.settings,
            pipe.entity_mappings,
            pipe.relationship_mappings,
            base_uri=pipe.base_uri,
        )

        assert spec.to_sql() == _slot_sql(log)
        assert TwinSlotFeed.feed_tables(spec) == []

    def test_the_scheduled_tick_probes_no_table_for_a_topic_fed_pipeline(self):
        from back.objects.digitaltwin.build import TwinIncrementalBuild
        from back.objects.registry.schedule import build as scheduled

        pipe = realtime_pipeline()
        spec = TwinIncrementalBuild.compile_for(
            pipe.domain, pipe.settings, pipe.entity_mappings, [], base_uri=pipe.base_uri
        )

        assert spec.source == "topic"
        assert scheduled._probed_tables(spec) == []
        assert scheduled._probed_tables(None) is None


def test_a_realtime_request_is_never_merged():
    """The MERGE fallback (and its source-version fingerprint) is for ``incremental`` requests only."""
    from back.core.graphdb.MaterializationModeResolver import MaterializationModeResolver
    from back.objects.digitaltwin.build import TwinMergeFallback

    reason = MaterializationModeResolver.REASON_SERVERLESS_UNAVAILABLE

    assert TwinMergeFallback.promotes(ModeDecision("realtime", "full", reason)) is False


class _Described(_modes._Warehouse):
    """The modes warehouse, answering ``DESCRIBE TABLE`` with *columns* (or *describe_error*)."""

    def __init__(self, log, *, columns=None, describe_error=None, **kw):
        super().__init__(log, **kw)
        self._columns, self._describe_error = columns or {}, describe_error

    def execute_query(self, sql: str):
        if sql.startswith("DESCRIBE TABLE"):
            self._record(sql)
            if self._describe_error is not None:
                raise self._describe_error
            return [
                *({"col_name": name, "data_type": kind} for name, kind in self._columns.items()),
                {"col_name": "", "data_type": ""},
                {"col_name": "# Clustering Information", "data_type": ""},
                {"col_name": "id", "data_type": "int"},
            ]
        return super().execute_query(sql)


class TestTheTopicIsTypedWithTheSourceColumns:
    def test_the_build_compiles_with_each_source_s_column_types(self, monkeypatch):
        from back.core.streaming import SlotProjectionCompiler

        seen: list = []
        compile_ = SlotProjectionCompiler.compile
        monkeypatch.setattr(
            SlotProjectionCompiler,
            "compile",
            staticmethod(lambda *a, **kw: seen.append(kw.get("column_types")) or compile_(*a, **kw)),
        )
        pipe = realtime_pipeline(entities={_modes._CUSTOMER: _modes._row_local()})
        warehouse = _Described([], columns={"ID": "bigint", "name": "string"})

        _, _, warehouse = _modes._run(pipe, warehouse=warehouse, existing="pipe-1", stored="fp-current")

        assert seen == [{"main.sales.customers": {"id": "bigint", "name": "string"}}]
        assert [s for s in warehouse.calls if s.startswith("DESCRIBE TABLE")] == [
            "DESCRIBE TABLE main.sales.customers"
        ]

    def test_a_source_that_cannot_be_described_falls_back_without_replaying_the_topic(self):
        pipe = realtime_pipeline()
        warehouse = _Described([], describe_error=RuntimeError("PERMISSION_DENIED"))

        _modes._run(pipe, warehouse=warehouse, existing="pipe-1", stored="fp-current")

        assert pipe.mode_decision.reason == "incremental_failed:InfrastructureError"

    def test_a_change_feed_pipeline_describes_nothing(self):
        pipe = realtime_pipeline(enabled=False)
        warehouse = _Described([], columns={"id": "int"})

        _, _, warehouse = _modes._run(pipe, warehouse=warehouse, existing="pipe-1", stored="fp-current")

        assert not any(s.startswith("DESCRIBE TABLE") for s in warehouse.calls)
