"""The build reads the mapped triples once and resets the inferred companion.

``_BuildPipeline`` keeps one-line hooks; the behaviour lives in
``TwinBuildPostMaterialize``. Three guarantees are pinned here:

* **Compute once.** The R2RML gateway VIEW is expensive to evaluate. Once it
  has been copied into a ``…_data`` table, the count, the stream into the graph
  store and the Lakeflow synced table read that table instead of re-running the
  VIEW (see ``TestLakeflowSource``).
  A pass-through ``view`` ``…_data`` costs the same as the VIEW, so there the
  VIEW stays the source.
* **No stale inferences.** On the Databricks backend the ``…_inferred``
  companion is emptied on every main build; failing to do so fails the build.
* **Traceable.** The build-run entry names the relation read and whether the
  reset ran.
* **Delta is the graph.** On the Databricks backend ``…_data`` / ``…_graph``
  already are the graph, so the full rebuild counts but never streams into
  the store (``DeltaFlatStore.insert_triples`` would refill ``…_inferred``).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.core.graphdb import ModeDecision
from back.core.graphdb.delta import materialize
from back.objects.digitaltwin.build import TwinBuildPostMaterialize, TwinBuildRunHistory
from tests.units.dtwin import test_build_pipeline_streaming as _streaming
from tests.units.objects.test_build_pipeline_materialize import (
    _DATA,
    _VIEW,
    _bare_pipeline,
    _reads,
)

pytestmark = pytest.mark.unit

_INFERRED = "cat.sch.triplestore_dom_V3_inferred"
_GRAPH = "cat.sch.triplestore_dom_V3_graph"

_LAKEBASE = {}
_DATABRICKS = {"graph_backend": "databricks"}
_DATABRICKS_VIEW = {
    "graph_backend": "databricks",
    "lakehouse_materialization": "view",
}


class _Warehouse:
    """Records every SQL string in order; can refuse a statement by substring."""

    def __init__(self, *, count: int = 5, fail_statement_on: str | None = None):
        self.calls: list[str] = []
        self._count = count
        self._fail_on = fail_statement_on

    def execute_query(self, sql: str):
        self.calls.append(sql)
        if sql.strip().upper().startswith("SELECT COUNT"):
            return [{"cnt": self._count}]
        return []

    def execute_statement(self, sql: str) -> bool:
        self.calls.append(sql)
        if self._fail_on and self._fail_on in sql:
            raise RuntimeError(f"warehouse refused: {self._fail_on}")
        return True

    def iter_rows(self, sql: str, batch_size: int = 5000):
        self.calls.append(sql)
        return iter([])


def _build(pipe, client=None, *, real_apply: bool = True) -> list[str]:
    """Run *pipe* through ``run()`` and return the SQL it sent, in order."""
    pipe.source_client = client or _Warehouse()
    if real_apply:
        del pipe._apply_full_rebuild  # run the real count + stream phase
        pipe.store.bulk_load_into_sync = MagicMock(return_value=5)
    pipe.run()
    return pipe.source_client.calls


def _first(calls: list[str], needle: str) -> int:
    return next(i for i, sql in enumerate(calls) if needle in sql)


class TestComputeOnce:
    def test_a_materialized_build_reads_the_data_table_after_building_it(self):
        calls = _build(_bare_pipeline(info=_LAKEBASE))

        counts = [sql for sql in calls if "COUNT(*)" in sql]
        streams = [sql for sql in calls if sql.startswith("SELECT subject, predicate")]
        assert len(counts) == 1 and _reads(counts[0], _DATA)
        assert len(streams) == 1 and _reads(streams[0], _DATA)

    @pytest.mark.parametrize("info", [_DATABRICKS, _DATABRICKS_VIEW], ids=["table", "view"])
    def test_a_databricks_build_counts_and_never_streams_into_the_store(self, info):
        pipe = _bare_pipeline(info=info)

        calls = _build(pipe)

        assert len([sql for sql in calls if "COUNT(*)" in sql]) == 1
        assert not any(sql.startswith("SELECT subject, predicate") for sql in calls)
        pipe.store.drop_table.assert_not_called()
        pipe.store.bulk_load_into_sync.assert_not_called()
        assert pipe.triple_count == 5

    @pytest.mark.parametrize("info", [_LAKEBASE, _DATABRICKS], ids=["lakebase", "databricks"])
    def test_a_materialized_build_evaluates_the_gateway_view_exactly_once(self, info):
        calls = _build(_bare_pipeline(info=info))

        view_reads = [sql for sql in calls if _reads(sql, _VIEW)]
        assert len(view_reads) == 1
        assert view_reads[0].startswith(f"CREATE OR REPLACE TABLE {_DATA} ")

    def test_a_pass_through_build_keeps_reading_the_gateway_view(self):
        pipe = _bare_pipeline(info=_DATABRICKS_VIEW)

        calls = _build(pipe)

        count = next(sql for sql in calls if "COUNT(*)" in sql)
        assert _reads(count, _VIEW) and not _reads(count, _DATA)
        assert pipe._triples_source_relation() == _VIEW

    def test_the_materialized_source_is_the_data_table(self):
        pipe = _bare_pipeline(info=_DATABRICKS)

        _build(pipe)

        assert pipe.data_relation_mode == "table"
        assert pipe._triples_source_relation() == _DATA


class TestTriplesSourceRelation:
    """The relation is decided from ``data_table`` + ``data_relation_mode`` alone."""

    @staticmethod
    def _pipe(**attrs):
        return SimpleNamespace(view_table=_VIEW, **attrs)

    def test_defaults_to_the_gateway_view_before_anything_was_built(self):
        assert TwinBuildPostMaterialize.triples_source_relation(self._pipe()) == _VIEW

    def test_a_materialized_table_is_the_source(self):
        pipe = self._pipe(data_table=_DATA, data_relation_mode="table")
        assert TwinBuildPostMaterialize.triples_source_relation(pipe) == _DATA

    def test_a_pass_through_view_is_not_cheaper_than_the_gateway_view(self):
        pipe = self._pipe(data_table=_DATA, data_relation_mode="view")
        assert TwinBuildPostMaterialize.triples_source_relation(pipe) == _VIEW

    def test_an_unresolved_mode_falls_back_to_the_gateway_view(self):
        pipe = self._pipe(data_table=_DATA, data_relation_mode=None)
        assert TwinBuildPostMaterialize.triples_source_relation(pipe) == _VIEW

    def test_an_empty_data_table_falls_back_to_the_gateway_view(self):
        pipe = self._pipe(data_table="", data_relation_mode="table")
        assert TwinBuildPostMaterialize.triples_source_relation(pipe) == _VIEW


class TestLakeflowSource:
    """Lakeflow syncs the ``…_data`` copy; a pass-through ``…_data`` leaves it on the VIEW.

    The scheduled build (``registry/schedule/build.py``) materializes ``…_data``
    and registers the same source, and ``SyncedTableManager.ensure`` recreates
    a registration whose source changed (``TwinLakebaseSync``).
    """

    @staticmethod
    def _synced(mode: str):
        pipe = _streaming.TestApplyViaSyncedPipeline()._synced_pipe()
        pipe.data_table = f"{pipe.view_table}_data"
        pipe.data_relation_mode = mode
        return pipe

    @pytest.mark.parametrize(
        ("mode", "source"),
        [("table", "cat.sch.v_g_data"), ("view", "cat.sch.v_g")],
        ids=["table", "view"],
    )
    def test_the_sync_source_is_the_data_table_or_the_gateway_view(self, mode, source):
        pipe = self._synced(mode)

        assert pipe._apply_via_synced_pipeline() is True

        assert pipe._mgr.ensure.call_args.kwargs["source_table_full_name"] == source

    def test_the_post_sync_count_reads_the_chosen_source_not_the_alias(self):
        pipe = self._synced("table")

        assert pipe._apply_via_synced_pipeline() is True

        assert pipe.triple_count == 1234
        pipe._count_source_triples.assert_called_once_with()

    @staticmethod
    def _empty_sync_warnings(pipe) -> list[str]:
        pipe._count_source_triples.return_value = 0
        with patch(
            "back.objects.digitaltwin.build._build_pipeline.logger"
        ) as logger:
            assert pipe._apply_via_synced_pipeline() is True
        return [
            call.args[0] % call.args[1:]
            for call in logger.warning.call_args_list
            if "contains 0 triples" in call.args[0]
        ]

    def test_an_empty_post_sync_count_names_the_counted_and_the_synced_relation(self):
        warnings = self._empty_sync_warnings(self._synced("table"))

        assert len(warnings) == 1
        assert (
            "post-sync cat.sch.v_g_data contains 0 triples (Lakeflow synced cat.sch.v_g_data)"
        ) in warnings[0]

    def test_complete_slots_are_the_source_of_a_triggered_sync(self):
        """Slots that carry the whole graph have a change feed every workspace can sync.

        The count still reads ``_data`` (the complete relation); asserted-only
        readers go through the Postgres asserted view, so the slot rows and
        blank objects never reach them.
        """
        pipe = self._synced("materialized_view")
        pipe.mode_decision = ModeDecision("incremental", "incremental", "")
        pipe.store.sync_table_mode = "triggered"
        pipe.slots_table = "cat.sch.v_g_slots"
        pipe.batch_mappings = 0

        warnings = self._empty_sync_warnings(pipe)

        assert (
            "post-sync cat.sch.v_g_data contains 0 triples (Lakeflow synced cat.sch.v_g_slots)"
        ) in warnings[0]
        assert pipe.sync_source_kind == "slots"
        assert pipe._mgr.ensure.call_args.kwargs["primary_key_columns"] == [
            "map_id",
            "source_key",
            "predicate",
        ]
        assert pipe.store.ensure_synced_union_view.call_args.kwargs["source_kind"] == "slots"

    def test_complete_slots_still_sync_the_data_relation_as_a_snapshot(self):
        """The default snapshot schedule has no change feed to gain from the slots."""
        pipe = self._synced("materialized_view")
        pipe.mode_decision = ModeDecision("incremental", "incremental", "")
        pipe.slots_table = "cat.sch.v_g_slots"
        pipe.batch_mappings = 0

        warnings = self._empty_sync_warnings(pipe)

        assert (
            "post-sync cat.sch.v_g_data contains 0 triples (Lakeflow synced cat.sch.v_g_data)"
        ) in warnings[0]
        assert pipe.sync_source_kind == "triples"

    def test_a_synced_data_table_carries_object_hash(self):
        """Lakeflow's primary key includes ``object_hash``; ``_data`` must have it."""
        pipe = _bare_pipeline(info=_LAKEBASE)
        pipe._is_lakebase_synced = True

        calls = _build(pipe, real_apply=False)

        ctas = next(sql for sql in calls if sql.startswith("CREATE OR REPLACE TABLE"))
        assert ctas.endswith("SELECT subject, predicate, object, object_hash FROM " + _VIEW)
        assert "delta.enableRowTracking = true" in ctas

    def test_an_unsynced_data_table_keeps_three_columns(self):
        pipe = _bare_pipeline(info=_LAKEBASE)

        calls = _build(pipe, real_apply=False)

        ctas = next(sql for sql in calls if sql.startswith("CREATE OR REPLACE TABLE"))
        assert ctas.endswith("SELECT subject, predicate, object FROM " + _VIEW)

    def test_a_pass_through_view_never_asks_for_object_hash(self):
        """The VIEW, not ``_data``, is the Lakeflow source in this mode."""
        pipe = _bare_pipeline(info=_DATABRICKS_VIEW)
        pipe._is_lakebase_synced = True

        calls = _build(pipe, real_apply=False)

        data_view = next(sql for sql in calls if sql.startswith("CREATE OR REPLACE VIEW"))
        assert "object_hash" not in data_view


def _logged(logger_mock, level: str) -> list[str]:
    """The fully formatted messages a patched module logger received at *level*."""
    return [
        call.args[0] % call.args[1:]
        for call in getattr(logger_mock, level).call_args_list
    ]


class TestBuildMessagesNameTheRealSource:
    """The messages said "VIEW" while a materialized build reads ``…_data``."""

    @pytest.mark.parametrize(
        ("info", "source"),
        [(_DATABRICKS, _DATA), (_DATABRICKS_VIEW, _VIEW)],
        ids=["table", "view"],
    )
    def test_progress_and_logs_name_the_relation_being_read(self, info, source):
        pipe = _bare_pipeline(info=info)

        with patch(
            "back.objects.digitaltwin.build._build_pipeline.logger"
        ) as logger:
            _build(pipe)

        assert f"Reading all triples from {source}..." in pipe.tm.steps
        assert not any("from VIEW..." in step for step in pipe.tm.steps)
        info_lines = _logged(logger, "info")
        assert any(
            f"reading all triples from {source}" in line for line in info_lines
        )
        assert any(f"{source} reports 5 triples to ingest" in line for line in info_lines)

    @pytest.mark.parametrize(
        ("info", "source"),
        [(_DATABRICKS, _DATA), (_DATABRICKS_VIEW, _VIEW)],
        ids=["table", "view"],
    )
    def test_the_empty_warning_names_the_relation_that_was_empty(self, info, source):
        pipe = _bare_pipeline(info=info)

        with patch(
            "back.objects.digitaltwin.build._build_pipeline.logger"
        ) as logger:
            _build(pipe, _Warehouse(count=0))

        warnings = _logged(logger, "warning")
        assert len(warnings) == 1
        assert warnings[0].startswith(f"[DT-BUILD t-test] {source} returned 0 triples")

    def test_the_full_rebuild_counts_through_the_real_method_not_the_alias(self):
        pipe = _bare_pipeline(info=_DATABRICKS)
        pipe._count_source_triples = MagicMock(return_value=5)

        _build(pipe)

        pipe._count_source_triples.assert_called_once_with()


class TestInferredReset:
    def test_a_databricks_build_resets_the_companion_before_applying(self):
        pipe = _bare_pipeline(info=_DATABRICKS)
        sent_before_apply: list[str] = []
        pipe._announce_apply_step.side_effect = lambda: sent_before_apply.extend(
            pipe.source_client.calls
        )

        _build(pipe)

        ctas = _first(sent_before_apply, f"CREATE OR REPLACE TABLE {_DATA} ")
        ensure = _first(sent_before_apply, f"CREATE TABLE IF NOT EXISTS {_INFERRED}")
        truncate = _first(sent_before_apply, f"TRUNCATE TABLE {_INFERRED}")
        graph = _first(sent_before_apply, f"CREATE OR REPLACE VIEW {_GRAPH}")
        assert ctas < ensure < truncate < graph

    def test_the_graph_view_unions_the_data_and_inferred_relations(self):
        calls = _build(_bare_pipeline(info=_DATABRICKS))

        assert materialize.build_union_view_sql(_GRAPH, _DATA, _INFERRED) in calls

    def test_the_reset_is_reported_as_done(self):
        pipe = _bare_pipeline(info=_DATABRICKS)

        _build(pipe)

        assert pipe.inferred_reset is True

    @pytest.mark.parametrize(
        "refused",
        [
            f"CREATE TABLE IF NOT EXISTS {_INFERRED}",
            f"TRUNCATE TABLE {_INFERRED}",
            f"CREATE OR REPLACE VIEW {_GRAPH}",
        ],
        ids=["ensure_inferred", "truncate", "graph_view"],
    )
    def test_a_failed_reset_fails_the_build(self, refused):
        """Stale inferences are a correctness bug, so this is not a warning."""
        pipe = _bare_pipeline(info=_DATABRICKS)

        _build(pipe, _Warehouse(fail_statement_on=refused), real_apply=False)

        assert pipe.tm.failed is True
        assert "inferred" in pipe.tm.failure_message.lower()
        assert f"warehouse refused: {refused}" in pipe.tm.failure_message
        pipe._apply_full_rebuild.assert_not_called()
        assert getattr(pipe, "inferred_reset", False) is False

    def test_nothing_is_reset_when_the_data_relation_failed(self):
        pipe = _bare_pipeline(info=_DATABRICKS)

        with patch(
            "back.core.graphdb.delta.materialize.apply_data_relation",
            side_effect=RuntimeError("no permission on cat.sch"),
        ):
            calls = _build(pipe, real_apply=False)

        assert pipe.tm.failed is True
        assert not any("TRUNCATE" in sql for sql in calls)

    @pytest.mark.parametrize(
        "info",
        [_LAKEBASE, {"graph_backend": "neo4j"}],
        ids=["lakebase", "neo4j"],
    )
    def test_other_backends_leave_the_companion_alone(self, info):
        """Their full rebuild drops and recreates the graph table itself."""
        pipe = _bare_pipeline(info=info)

        calls = _build(pipe)

        assert not any("_inferred" in sql for sql in calls)
        assert getattr(pipe, "inferred_reset", False) is False
        assert pipe.tm.failed is False

    def test_a_graph_materialized_view_is_refreshed_not_replaced_by_the_view(self):
        """``TwinGraphRelation`` made ``…_graph`` an MV: the reset refreshes it to drop old inferences."""
        pipe = _bare_pipeline(info=_DATABRICKS)
        pipe.source_client = _Warehouse()
        pipe.data_table = _DATA
        pipe.graph_relation = "mv"

        assert TwinBuildPostMaterialize.reset_inferred_companion(pipe) is True

        calls = pipe.source_client.calls
        assert f"TRUNCATE TABLE {_INFERRED}" in calls
        assert calls.index(f"TRUNCATE TABLE {_INFERRED}") < calls.index(f"REFRESH MATERIALIZED VIEW {_GRAPH}")
        assert not any(sql.startswith(f"CREATE OR REPLACE VIEW {_GRAPH}") for sql in calls)

    def test_the_reset_reports_success_for_a_skipped_backend(self):
        pipe = _bare_pipeline(info=_LAKEBASE)

        assert TwinBuildPostMaterialize.reset_inferred_companion(pipe) is True


class TestBuildRunTrace:
    @pytest.fixture(autouse=True)
    def _no_databricks_sdk(self, monkeypatch):
        """Keep the full-path trace tests off the Databricks SDK.

        A requested ``incremental``/``realtime`` build with serverless off
        falls back through ``TwinIncrementalBuild._leave`` →
        ``TwinSlotPipeline.stop_if_active``, which lazily imports
        ``back.core.streaming.SlotPipelineRunner`` and would otherwise build a
        real ``WorkspaceClient`` against the fake test host and wait out the
        90 s API timeout (the resulting ``InfrastructureError`` is swallowed,
        so the test still passed — just 90 s late). Patch the runner with the
        scripted fake from the modes tests, and guard the SDK constructor:
        ``SlotPipelineRunner._w`` wraps any exception into a swallowed
        ``InfrastructureError``, so a raising guard would be invisible — the
        guard records the attempt and the fixture asserts at teardown.
        """
        from tests.units.objects import test_build_pipeline_modes as _modes

        _modes._FakeRunner.script = _modes._Script([])
        monkeypatch.setattr(
            "back.core.streaming.SlotPipelineRunner", _modes._FakeRunner
        )
        attempts: list = []
        monkeypatch.setattr(
            "databricks.sdk.WorkspaceClient",
            lambda *a, **kw: attempts.append((a, kw)) or MagicMock(),
        )
        yield
        assert attempts == [], "unit tests must not build a WorkspaceClient"

    @staticmethod
    def _entry(pipe) -> dict:
        """Run *pipe* with the real trace writer and return the persisted entry."""
        registry = MagicMock()
        del pipe._record_build_run
        pipe.domain_snap = SimpleNamespace(
            current_version=3, ontology={}, assignment={}
        )
        with patch(
            "back.objects.registry.RegistryService.RegistryService.from_context",
            return_value=registry,
        ):
            _build(pipe)
        return registry.record_build_run.call_args_list[0][0][1]

    def test_a_databricks_table_build_names_the_data_table_and_the_reset(self):
        entry = self._entry(_bare_pipeline(info=_DATABRICKS))

        assert entry["triples_source"] == _DATA
        assert entry["inferred_reset"] is True

    def test_a_pass_through_build_names_the_gateway_view(self):
        entry = self._entry(_bare_pipeline(info=_DATABRICKS_VIEW))

        assert entry["triples_source"] == _VIEW
        assert entry["inferred_reset"] is True

    def test_a_lakebase_build_reads_the_table_but_resets_nothing(self):
        entry = self._entry(_bare_pipeline(info=_LAKEBASE))

        assert entry["triples_source"] == _DATA
        assert entry["inferred_reset"] is False

    def test_a_build_that_never_got_that_far_traces_the_view_and_no_reset(self):
        pipe = _bare_pipeline(info=_DATABRICKS)
        pipe._materialize_data_table = MagicMock(return_value=False)

        entry = self._entry(pipe)

        assert entry["triples_source"] == _VIEW
        assert entry["inferred_reset"] is False

    def test_trace_fields_has_exactly_the_twelve_keys(self):
        fields = TwinBuildPostMaterialize.trace_fields(
            SimpleNamespace(view_table=_VIEW)
        )

        assert fields == {
            "triples_source": _VIEW,
            "inferred_reset": False,
            "materialization_mode_requested": "full",
            "materialization_mode_effective": "full",
            "mode_fallback_reason": "",
            "refresh_technique": "",
            "slot_pipeline_id": "",
            "slots_table": "",
            "sync_source": "",
            "sync_source_kind": "",
            "sync_mode_effective": "",
            "sync_fallback_reason": "",
        }

    def test_trace_fields_reports_the_incremental_facts(self):
        fields = TwinBuildPostMaterialize.trace_fields(
            SimpleNamespace(
                view_table=_VIEW,
                refresh_technique="ROW_BASED",
                slot_pipeline_id="pipe-1",
                slots_table="cat.sch.triplestore_dom_V3_slots",
            )
        )

        assert fields["refresh_technique"] == "ROW_BASED"
        assert fields["slot_pipeline_id"] == "pipe-1"
        assert fields["slots_table"] == "cat.sch.triplestore_dom_V3_slots"

    def test_trace_fields_drops_incremental_facts_that_are_not_text(self):
        fields = TwinBuildPostMaterialize.trace_fields(
            MagicMock(view_table=_VIEW, data_table="", data_relation_mode=None, slots_table=None)
        )

        assert (fields["refresh_technique"], fields["slot_pipeline_id"], fields["slots_table"]) == (
            "",
            "",
            "",
        )

    def test_trace_fields_reports_the_mode_decision(self):
        fields = TwinBuildPostMaterialize.trace_fields(
            SimpleNamespace(
                view_table=_VIEW,
                mode_decision=ModeDecision(
                    "incremental", "full", "serverless_unavailable"
                ),
            )
        )

        assert fields["materialization_mode_requested"] == "incremental"
        assert fields["materialization_mode_effective"] == "full"
        assert fields["mode_fallback_reason"] == "serverless_unavailable"

    def test_trace_fields_ignores_a_mode_decision_that_is_not_one(self):
        """A test double's auto-attribute must not leak into the persisted entry."""
        fields = TwinBuildPostMaterialize.trace_fields(
            MagicMock(view_table=_VIEW, data_table="", data_relation_mode=None)
        )

        assert fields["materialization_mode_requested"] == "full"
        assert fields["materialization_mode_effective"] == "full"
        assert fields["mode_fallback_reason"] == ""

    def test_a_default_build_traces_full_with_no_fallback(self):
        entry = self._entry(_bare_pipeline(info=_DATABRICKS))

        assert entry["materialization_mode_requested"] == "full"
        assert entry["materialization_mode_effective"] == "full"
        assert entry["mode_fallback_reason"] == ""

    def test_a_requested_mode_is_traced_with_its_fallback(self):
        pipe = _bare_pipeline(info={**_DATABRICKS, "materialization_mode": "incremental"})
        pipe.settings.serverless_pipelines_enabled = False

        entry = self._entry(pipe)

        assert entry["materialization_mode_requested"] == "incremental"
        assert entry["materialization_mode_effective"] == "incremental"
        assert entry["mode_fallback_reason"] == "serverless_unavailable"
        assert entry["stats"]["materialization"]["strategy"] == "merge"

    def test_realtime_is_traced_as_unavailable(self):
        pipe = _bare_pipeline(info={**_LAKEBASE, "materialization_mode": "realtime"})
        pipe.settings.serverless_pipelines_enabled = False

        entry = self._entry(pipe)

        assert entry["materialization_mode_requested"] == "realtime"
        assert entry["materialization_mode_effective"] == "full"
        assert entry["mode_fallback_reason"] == "realtime_not_available"

    def test_the_mode_is_traced_when_the_build_fails_before_translation(self):
        pipe = _bare_pipeline(info={**_DATABRICKS, "materialization_mode": "microbatch"})
        pipe._prepare_translation = MagicMock(return_value=False)

        entry = self._entry(pipe)

        assert entry["materialization_mode_requested"] == "microbatch"
        assert entry["materialization_mode_effective"] == "full"


class TestPersistLastBuildHook:
    def test_the_pipeline_delegates_to_the_collaborator(self):
        pipe = _bare_pipeline()

        with patch.object(
            TwinBuildRunHistory, "persist_last_build_to_registry"
        ) as persist:
            pipe._persist_last_build_to_registry()

        persist.assert_called_once_with(pipe)
