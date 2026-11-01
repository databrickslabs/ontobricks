"""The build dispatches on ``materialization_mode`` and falls back to ``full``.

``full`` keeps today's path (CTAS ``…_data``, reset, stream). ``incremental``
and ``microbatch`` run :class:`TwinIncrementalBuild` instead of the CTAS: the
row-local mappings feed a Lakeflow slot pipeline, ``…_data`` becomes a
materialized view over ``…_slots`` and the batch branch, and the downstream
apply reads that view. Any ``OntoBricksError`` on the way records
``incremental_failed:<Error>`` and runs the full path; a cancel is never
swallowed.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import InfrastructureError, OperationCancelledError
from back.core.graphdb import ModeDecision
from back.core.streaming import SlotPipelineRunner
from back.core.w3c import sparql
from back.objects.digitaltwin.build import TwinIncrementalBuild
from tests.units.objects.test_build_pipeline_materialize import _DATA, _bare_pipeline

pytestmark = pytest.mark.unit

_SLOTS = "cat.sch.triplestore_dom_V3_slots"
_GRAPH = "cat.sch.triplestore_dom_V3_graph"
_INFERRED = "cat.sch.triplestore_dom_V3_inferred"

_RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
_CUSTOMER = "http://ex.org/Customer"
_ORDER = "http://ex.org/Order"


def _row_local() -> dict:
    """A legacy table-based entity mapping: compiled into the slot pipeline."""
    return {
        "table": "main.sales.customers",
        "sql_query": None,
        "uri_template": "http://ex.org/Customer/{id}",
        "id_column": "id",
        "label_column": "name",
        "class_uri": _CUSTOMER,
        "predicates": {_RDFS_LABEL: {"type": "column", "column": "name"}},
    }


def _batch() -> dict:
    """A ``sql_query`` entity mapping: stays on the batch branch of the MV."""
    return {
        "table": None,
        "sql_query": "SELECT id, name FROM main.sales.orders",
        "uri_template": "http://ex.org/Order/{id}",
        "id_column": "id",
        "label_column": "name",
        "class_uri": _ORDER,
        "predicates": {_RDFS_LABEL: {"type": "column", "column": "name"}},
    }


class _Warehouse:
    """Records SQL into a shared *log*; answers counts, row tracking and the event log."""

    def __init__(self, log, *, tracked: bool = True, fail_on: str | None = None, count=5):
        self.log = log
        self.calls: list[str] = []
        self._tracked = tracked
        self._fail_on = fail_on
        self._count = count

    def _record(self, sql: str) -> None:
        self.calls.append(sql)
        self.log.append(("sql", sql))
        if self._fail_on and self._fail_on in sql:
            raise RuntimeError(f"warehouse refused: {self._fail_on}")

    def execute_query(self, sql: str):
        self._record(sql)
        if sql.startswith("SHOW TBLPROPERTIES"):
            return [{"key": "delta.enableRowTracking", "value": str(self._tracked).lower()}]
        if "event_log(" in sql:
            return [{"message": "Flow 'x' has been planned to be executed as ROW_BASED."}]
        if sql.strip().upper().startswith("SELECT COUNT"):
            return [{"cnt": self._count}]
        return []

    def execute_statement(self, sql: str) -> bool:
        self._record(sql)
        return True

    def iter_rows(self, sql: str, batch_size: int = 5000):
        self._record(sql)
        return iter([])


class _Script:
    """What the fake runner answers, and the shared event log it writes to."""

    def __init__(self, log, *, existing=None, stored="", state="IDLE", fail_on=None, error=None):
        self.log = log
        self.existing = existing
        self.stored = stored
        self.state = state
        self.fail_on = fail_on
        self.error = error or InfrastructureError("pipeline update FAILED")
        self.instances = 0


class _FakeRunner(SlotPipelineRunner):
    script: _Script

    def __init__(self) -> None:
        super().__init__(client_factory=lambda: None)
        self.script.instances += 1

    @staticmethod
    def fingerprint(sql_text: str) -> str:
        return "fp-current"

    def _step(self, *event):
        self.script.log.append(event)
        if self.script.fail_on == event[0]:
            raise self.script.error

    def find_pipeline_id(self, name):
        self._step("find", name)
        return self.script.existing

    def stored_fingerprint(self, pipeline_id):
        return self.script.stored

    def get_state(self, pipeline_id):
        return self.script.state

    def ensure(self, **kwargs):
        self._step("ensure", kwargs)
        return "pipe-1"

    def stop(self, pipeline_id):
        self._step("stop", pipeline_id)

    def start_update(self, pipeline_id, *, full_refresh=False):
        self._step("start", pipeline_id, full_refresh)
        return "upd-1"

    def wait_for_update(self, pipeline_id, update_id, **kwargs):
        self._step("wait", update_id, kwargs)
        return "RUNNING" if kwargs.get("until_running") else "COMPLETED"


def _pipeline(mode="incremental", *, backend="databricks", entities=None, relationships=()):
    info = {"graph_backend": backend}
    if mode:
        info["materialization_mode"] = mode
    pipe = _bare_pipeline(info=info)
    pipe.domain_snap = SimpleNamespace(current_version=3, ontology={}, assignment={})
    pipe.entity_mappings = (
        {_CUSTOMER: _row_local(), _ORDER: _batch()} if entities is None else entities
    )
    pipe.relationship_mappings = list(relationships)
    pipe.base_uri = "http://ex.org/"
    return pipe


def _run(pipe, *, warehouse=None, **script_kw):
    """Run *pipe* with a fake runner; return ``(log, script, warehouse)``."""
    log: list = []
    warehouse = warehouse or _Warehouse(log)
    warehouse.log = log
    script = _Script(log, **script_kw)
    runner_cls = type("_BoundRunner", (_FakeRunner,), {"script": script})
    pipe.source_client = warehouse
    pipe._apply_full_rebuild.side_effect = lambda: log.append(("apply_full_rebuild",)) or True
    with patch("back.core.streaming.SlotPipelineRunner", runner_cls):
        pipe.run()
    return log, script, warehouse


def _index(log, predicate) -> int:
    return next(i for i, event in enumerate(log) if predicate(event))


def _sql_index(log, needle: str) -> int:
    return _index(log, lambda e: e[0] == "sql" and needle in e[1])


def _events(log, kind: str) -> list:
    return [event for event in log if event[0] == kind]


class TestFullIsUnchanged:
    def test_a_full_domain_never_builds_a_slot_pipeline(self):
        pipe = _pipeline(mode=None)

        log, script, warehouse = _run(pipe)

        assert script.instances == 0
        assert any(sql.startswith(f"CREATE OR REPLACE TABLE {_DATA} ") for sql in warehouse.calls)
        assert pipe.slots_table is None
        assert pipe.data_relation_mode == "table"
        assert pipe.mode_decision == ModeDecision("full", "full", "")

    def test_the_full_path_drops_leftover_materialized_views_before_the_ctas(self):
        _, _, warehouse = _run(_pipeline(mode=None))

        ctas = next(
            i for i, s in enumerate(warehouse.calls) if s.startswith("CREATE OR REPLACE TABLE")
        )
        assert f"DROP MATERIALIZED VIEW IF EXISTS {_DATA}" in warehouse.calls[:ctas]
        assert f"DROP MATERIALIZED VIEW IF EXISTS {_GRAPH}" in warehouse.calls[:ctas]

    def test_the_full_path_never_probes_the_sources(self):
        _, _, warehouse = _run(_pipeline(mode=None))

        assert not any(s.startswith("SHOW TBLPROPERTIES") for s in warehouse.calls)


class TestIncremental:
    def test_runner_then_views_then_downstream_apply(self):
        pipe = _pipeline("incremental")

        log, _, _ = _run(pipe)

        ensure = _index(log, lambda e: e[0] == "ensure")
        start = _index(log, lambda e: e[0] == "start")
        wait = _index(log, lambda e: e[0] == "wait")
        data_mv = _sql_index(log, f"CREATE OR REPLACE MATERIALIZED VIEW {_DATA}")
        refresh = _sql_index(log, f"REFRESH MATERIALIZED VIEW {_DATA}")
        apply = _index(log, lambda e: e[0] == "apply_full_rebuild")
        assert ensure < start < wait < data_mv < refresh < apply
        assert pipe.tm.failed is False

    def test_the_data_table_is_never_copied(self):
        _, _, warehouse = _run(_pipeline("incremental"))

        assert not any(s.startswith("CREATE OR REPLACE TABLE") for s in warehouse.calls)

    def test_the_downstream_apply_reads_the_data_materialized_view(self):
        pipe = _pipeline("incremental")

        _run(pipe)

        assert pipe.data_relation_mode == "materialized_view"
        assert pipe._triples_source_relation() == _DATA

    def test_the_graph_stays_the_union_view(self):
        _, _, warehouse = _run(_pipeline("incremental"))

        assert not any(
            s.startswith(f"CREATE OR REPLACE MATERIALIZED VIEW {_GRAPH}") for s in warehouse.calls
        )
        assert any(s.startswith(f"CREATE OR REPLACE VIEW {_GRAPH} AS") for s in warehouse.calls)

    def test_an_existing_data_table_is_dropped_on_every_incremental_build(self):
        _, _, warehouse = _run(_pipeline("incremental"))

        data_mv = next(
            i for i, s in enumerate(warehouse.calls)
            if s.startswith(f"CREATE OR REPLACE MATERIALIZED VIEW {_DATA}")
        )
        assert f"DROP TABLE IF EXISTS {_DATA}" in warehouse.calls[:data_mv]

    def test_the_inferred_reset_runs_after_the_views(self):
        log, _, _ = _run(_pipeline("incremental"))

        assert _sql_index(log, f"CREATE OR REPLACE MATERIALIZED VIEW {_DATA}") < _sql_index(
            log, f"TRUNCATE TABLE {_INFERRED}"
        )

    def test_the_pipeline_is_triggered_and_waited_to_completion(self):
        log, _, _ = _run(_pipeline("incremental"))

        ensure = _events(log, "ensure")[0][1]
        wait = _events(log, "wait")[0][2]
        assert ensure["continuous"] is False
        assert ensure["serverless"] is True
        assert ensure["channel"] == "PREVIEW"
        assert ensure["name"] == "ontobricks-slots-dom-v3"
        assert (ensure["catalog"], ensure["schema"]) == ("cat", "sch")
        assert _SLOTS in ensure["sql_text"]
        assert wait["until_running"] is False
        assert wait["timeout_s"] == 3600

    def test_the_build_facts_are_recorded(self):
        pipe = _pipeline("incremental")

        _run(pipe)

        assert pipe.slots_table == _SLOTS
        assert pipe.slot_pipeline_id == "pipe-1"
        assert pipe.refresh_technique == "ROW_BASED"
        assert pipe.mode_decision == ModeDecision("incremental", "incremental", "")


class TestMicrobatch:
    def test_the_pipeline_runs_continuously_and_is_waited_until_running(self):
        pipe = _pipeline("microbatch")

        log, _, _ = _run(pipe)

        assert _events(log, "ensure")[0][1]["continuous"] is True
        assert _events(log, "wait")[0][2]["until_running"] is True
        assert pipe.mode_decision.effective == "microbatch"

    def test_a_pipeline_already_running_the_current_sql_is_left_alone(self):
        pipe = _pipeline("microbatch")

        log, _, _ = _run(pipe, existing="pipe-7", stored="fp-current", state="RUNNING")

        assert _events(log, "ensure") == [] and _events(log, "start") == []
        assert pipe.slot_pipeline_id == "pipe-7"
        assert pipe.data_relation_mode == "materialized_view"


class TestSlotPipelineUpdates:
    def test_a_new_pipeline_gets_a_full_refresh(self):
        log, _, _ = _run(_pipeline("incremental"))

        assert _events(log, "start") == [("start", "pipe-1", True)]

    def test_an_unchanged_definition_refreshes_incrementally(self):
        log, _, _ = _run(_pipeline("incremental"), existing="pipe-1", stored="fp-current")

        assert _events(log, "start") == [("start", "pipe-1", False)]

    def test_a_changed_definition_forces_a_full_refresh(self):
        log, _, _ = _run(_pipeline("incremental"), existing="pipe-1", stored="fp-old")

        assert _events(log, "start") == [("start", "pipe-1", True)]

    def test_an_active_pipeline_is_stopped_before_the_update(self):
        log, _, _ = _run(_pipeline("incremental"), existing="pipe-1", stored="fp-current", state="RUNNING")

        assert _index(log, lambda e: e[0] == "stop") < _index(log, lambda e: e[0] == "start")

    def test_an_idle_pipeline_is_not_stopped(self):
        log, _, _ = _run(_pipeline("incremental"), existing="pipe-1", stored="fp-current")

        assert _events(log, "stop") == []


class TestFallback:
    @pytest.mark.parametrize("step", ["ensure", "start", "wait"])
    def test_a_runner_failure_falls_back_to_full(self, step):
        pipe = _pipeline("incremental")

        log, _, warehouse = _run(pipe, fail_on=step)

        assert pipe.mode_decision == ModeDecision(
            "incremental", "full", "incremental_failed:InfrastructureError"
        )
        assert any(s.startswith(f"CREATE OR REPLACE TABLE {_DATA} ") for s in warehouse.calls)
        assert _events(log, "apply_full_rebuild")
        assert pipe.tm.failed is False
        assert pipe.slots_table is None
        assert pipe.data_relation_mode == "table"

    def test_a_raw_warehouse_error_in_the_views_falls_back(self):
        pipe = _pipeline("incremental")
        warehouse = _Warehouse([], fail_on=f"CREATE OR REPLACE MATERIALIZED VIEW {_DATA}")

        _, _, warehouse = _run(pipe, warehouse=warehouse)

        assert pipe.mode_decision.reason == "incremental_failed:InfrastructureError"
        assert any(s.startswith(f"CREATE OR REPLACE TABLE {_DATA} ") for s in warehouse.calls)
        assert pipe.refresh_technique == ""

    def test_the_fallback_drops_the_half_built_materialized_view(self):
        pipe = _pipeline("incremental")
        warehouse = _Warehouse([], fail_on=f"REFRESH MATERIALIZED VIEW {_DATA}")

        _, _, warehouse = _run(pipe, warehouse=warehouse)

        refresh = warehouse.calls.index(f"REFRESH MATERIALIZED VIEW {_DATA}")
        ctas = next(
            i for i, s in enumerate(warehouse.calls) if s.startswith("CREATE OR REPLACE TABLE")
        )
        assert f"DROP MATERIALIZED VIEW IF EXISTS {_DATA}" in warehouse.calls[refresh:ctas]

    def test_a_cancel_is_never_swallowed(self):
        pipe = _pipeline("incremental")

        log, _, warehouse = _run(
            pipe, fail_on="wait", error=OperationCancelledError("Build cancelled by user")
        )

        assert not any(s.startswith("CREATE OR REPLACE TABLE") for s in warehouse.calls)
        assert _events(log, "apply_full_rebuild") == []
        pipe._record_build_run.assert_any_call("cancelled", message="Build cancelled by user")

    def test_the_build_fails_only_when_full_fails_too(self):
        pipe = _pipeline("incremental")
        warehouse = _Warehouse([], fail_on=f"CREATE OR REPLACE TABLE {_DATA}")

        _run(pipe, warehouse=warehouse, fail_on="ensure")

        assert pipe.tm.failed is True
        assert "Could not build" in pipe.tm.failure_message


class TestBatchBranch:
    def test_without_batch_mappings_the_translator_is_not_called(self):
        pipe = _pipeline("incremental", entities={_CUSTOMER: _row_local()})

        with patch.object(
            sparql, "translate_sparql_to_spark", wraps=sparql.translate_sparql_to_spark
        ) as translate:
            _, _, warehouse = _run(pipe)

        translate.assert_not_called()
        data_mv = next(
            s for s in warehouse.calls
            if s.startswith(f"CREATE OR REPLACE MATERIALIZED VIEW {_DATA}")
        )
        assert "UNION ALL" not in data_mv and f"FROM {_SLOTS}" in data_mv

    def test_without_row_local_mappings_no_slot_pipeline_runs(self):
        pipe = _pipeline("incremental", entities={_ORDER: _batch()})

        log, script, warehouse = _run(pipe)

        assert script.instances == 0
        data_mv = next(
            s for s in warehouse.calls
            if s.startswith(f"CREATE OR REPLACE MATERIALIZED VIEW {_DATA}")
        )
        assert _SLOTS not in data_mv
        assert pipe.slots_table is None
        assert pipe.slot_pipeline_id == ""
        assert pipe.mode_decision.effective == "incremental"

    def test_the_batch_branch_keeps_exactly_the_batch_mappings(self):
        row_local, batch = _row_local(), _batch()
        pipe = _pipeline("incremental", entities={_CUSTOMER: row_local, _ORDER: batch})

        with patch.object(
            sparql, "translate_sparql_to_spark", wraps=sparql.translate_sparql_to_spark
        ) as translate:
            _run(pipe)

        kwargs = translate.call_args.kwargs
        assert kwargs["incremental"] is True
        keep = kwargs["mappings_filter"]
        assert keep(batch) is True
        assert keep(row_local) is False
        assert keep(dict(batch)) is False  # identity, not equality


class TestReadiness:
    def test_untracked_sources_run_full_with_row_tracking_missing(self):
        pipe = _pipeline("incremental")

        _, script, warehouse = _run(pipe, warehouse=_Warehouse([], tracked=False))

        assert pipe.mode_decision == ModeDecision("incremental", "full", "row_tracking_missing")
        assert script.instances == 0
        assert any(s.startswith("SHOW TBLPROPERTIES main.sales.customers") for s in warehouse.calls)

    def test_disabled_serverless_pipelines_skip_the_probe(self):
        pipe = _pipeline("incremental")
        pipe.settings.serverless_pipelines_enabled = False

        _, script, warehouse = _run(pipe)

        assert pipe.mode_decision == ModeDecision("incremental", "full", "serverless_unavailable")
        assert not any(s.startswith("SHOW TBLPROPERTIES") for s in warehouse.calls)
        assert script.instances == 0

    def test_a_failed_probe_falls_back_like_any_incremental_failure(self):
        pipe = _pipeline("incremental")

        _run(pipe, warehouse=_Warehouse([], fail_on="SHOW TBLPROPERTIES"))

        assert pipe.mode_decision.reason == "incremental_failed:InfrastructureError"


class TestDatabricksFullPathDoesNotStream:
    """``_data``/``_graph`` are the Delta engine's graph: streaming into the store
    would route every triple into ``_inferred`` right after its reset."""

    def _pipe(self, backend):
        pipe = _pipeline(mode=None, backend=backend)
        del pipe._apply_full_rebuild
        pipe.store.bulk_load_into_sync = MagicMock(return_value=5)
        return pipe

    def test_the_databricks_backend_counts_but_never_writes_the_store(self):
        pipe = self._pipe("databricks")
        pipe.source_client = _Warehouse([])

        assert pipe._apply_full_rebuild() is True

        assert pipe.triple_count == 5
        assert any("COUNT(*)" in s for s in pipe.source_client.calls)
        assert not any(s.startswith("SELECT subject, predicate") for s in pipe.source_client.calls)
        pipe.store.drop_table.assert_not_called()
        pipe.store.create_table.assert_not_called()
        pipe.store.bulk_load_into_sync.assert_not_called()

    def test_other_backends_still_stream(self):
        pipe = self._pipe("lakebase")
        pipe.source_client = _Warehouse([])

        assert pipe._apply_full_rebuild() is True

        pipe.store.drop_table.assert_called_once()
        pipe.store.bulk_load_into_sync.assert_called_once()


class TestTrace:
    def _entry(self, pipe, **run_kw) -> dict:
        registry = MagicMock()
        del pipe._record_build_run
        with patch(
            "back.objects.registry.RegistryService.RegistryService.from_context",
            return_value=registry,
        ):
            _run(pipe, **run_kw)
        return registry.record_build_run.call_args_list[0][0][1]

    def test_an_incremental_build_traces_its_facts(self):
        entry = self._entry(_pipeline("incremental"))

        assert entry["materialization_mode_requested"] == "incremental"
        assert entry["materialization_mode_effective"] == "incremental"
        assert entry["mode_fallback_reason"] == ""
        assert entry["refresh_technique"] == "ROW_BASED"
        assert entry["slot_pipeline_id"] == "pipe-1"
        assert entry["slots_table"] == _SLOTS
        assert entry["triples_source"] == _DATA

    def test_a_fallback_is_traced_with_its_reason(self):
        entry = self._entry(_pipeline("microbatch"), fail_on="wait")

        assert entry["materialization_mode_requested"] == "microbatch"
        assert entry["materialization_mode_effective"] == "full"
        assert entry["mode_fallback_reason"] == "incremental_failed:InfrastructureError"
        assert entry["refresh_technique"] == ""
        assert entry["slots_table"] == ""

    def test_a_full_build_traces_empty_incremental_facts(self):
        entry = self._entry(_pipeline(mode=None))

        assert (entry["refresh_technique"], entry["slot_pipeline_id"], entry["slots_table"]) == (
            "",
            "",
            "",
        )


class TestSettings:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [(None, 3600), ("", 3600), ("120", 120), ("0", 3600), ("-5", 3600), ("soon", 3600)],
    )
    def test_the_slot_pipeline_timeout_comes_from_the_environment(self, raw, expected, monkeypatch):
        if raw is None:
            monkeypatch.delenv("DTWIN_SLOT_PIPELINE_TIMEOUT_S", raising=False)
        else:
            monkeypatch.setenv("DTWIN_SLOT_PIPELINE_TIMEOUT_S", raw)

        assert TwinIncrementalBuild.slot_pipeline_timeout_s() == expected

    def test_the_channel_defaults_to_preview(self):
        assert TwinIncrementalBuild.slot_pipeline_channel(SimpleNamespace()) == "PREVIEW"
        assert (
            TwinIncrementalBuild.slot_pipeline_channel(
                SimpleNamespace(slot_pipeline_channel="CURRENT")
            )
            == "CURRENT"
        )
