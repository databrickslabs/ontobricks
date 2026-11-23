"""Streaming + Lakebase-aware behaviour for the Digital Twin build pipeline.

These tests exercise the helpers in :class:`_BuildPipeline` without spinning
up the whole pipeline (the backend load itself is covered in
``test_graph_load.py``). Each test builds a minimal instance via
``object.__new__`` and only sets the attributes the helper needs, which keeps
the test scope tight and fast.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from back.core.graphdb.GraphDBFactory import GraphDBFactory
from back.objects.digitaltwin.build._build_pipeline import (
    _BuildPipeline,
    collect_domain_stats,
    step_times_from_task,
)


def _bare_pipeline(**overrides):
    """Return a ``_BuildPipeline`` instance with only the attributes we patch."""
    pipe = object.__new__(_BuildPipeline)
    pipe.task_id = "t-test"
    pipe.graph_name = "G_V1"
    pipe.view_table = "cat.sch.v_g"
    pipe.parts = pipe.view_table.split(".")
    pipe.source_relation = pipe.view_table
    pipe.source_client = MagicMock()
    pipe.store = MagicMock()
    pipe.tm = MagicMock()
    pipe.is_api = False
    pipe.archive_task_id = None
    pipe.phase_times = {}
    pipe.domain = SimpleNamespace()
    pipe.settings = SimpleNamespace()
    pipe.delta_cfg = {"catalog": "cat", "schema": "sch"}
    pipe._lakebase_engine_config = {}
    pipe._is_lakebase_synced = False
    pipe.triple_count = 0
    for k, v in overrides.items():
        setattr(pipe, k, v)
    return pipe


class TestApplyFullRebuild:
    """The apply phase hands ``source_relation`` to the matching backend load."""

    def test_streamed_load_reads_the_source_relation(self):
        pipe = _bare_pipeline(source_relation="cat.sch.composed_relation")
        pipe._is_cancelled = MagicMock(return_value=False)

        with patch(
            "back.objects.digitaltwin.build.GraphLoad.GraphLoad.run",
            autospec=True,
            return_value="loaded",
        ) as run:
            assert pipe._apply_full_rebuild() is True

        load = run.call_args.args[0]
        assert type(load).__name__ == "GraphLoad"
        assert load.source_relation == "cat.sch.composed_relation"

    def test_synced_store_uses_the_lakeflow_load(self):
        pipe = _bare_pipeline(_is_lakebase_synced=True)
        pipe.store.is_synced = True
        pipe._is_cancelled = MagicMock(return_value=False)
        pipe._ensure_lakeflow_source_view = MagicMock(return_value=True)

        with patch(
            "back.objects.digitaltwin.build.SyncedGraphLoad.SyncedGraphLoad.run",
            autospec=True,
            return_value="loaded",
        ) as run:
            assert pipe._apply_full_rebuild() is True

        pipe._ensure_lakeflow_source_view.assert_called_once_with()
        assert type(run.call_args.args[0]).__name__ == "SyncedGraphLoad"
        pipe.source_client.iter_rows.assert_not_called()

    def test_empty_source_completes_the_build_as_a_success(self):
        pipe = _bare_pipeline(start_time=0.0)
        pipe._is_cancelled = MagicMock(return_value=False)
        pipe._record_build_run = MagicMock()

        with patch(
            "back.objects.digitaltwin.build.GraphLoad.GraphLoad.run",
            autospec=True,
            return_value="empty",
        ):
            assert pipe._apply_full_rebuild() is False

        assert pipe.tm.complete_task.call_args.kwargs["result"]["triple_count"] == 0
        pipe._record_build_run.assert_called_once()
        assert pipe._record_build_run.call_args.args[0] == "success"


class TestResolveLakebaseMode:
    """Mode resolution must run before the store is opened."""

    def test_sets_synced_flag_when_global_config_says_so(self):
        pipe = _bare_pipeline()

        with (
            patch.object(GraphDBFactory, "_resolve_graph_engine", return_value="lakebase"),
            patch.object(GraphDBFactory, "_resolve_graph_engine_config", return_value={"sync_mode": "managed_synced"}),
        ):
            pipe._resolve_lakebase_mode()

        assert pipe._is_lakebase_synced is True
        assert pipe._lakebase_managed_synced() is True

    def test_default_is_app_managed(self):
        pipe = _bare_pipeline()

        with (
            patch.object(GraphDBFactory, "_resolve_graph_engine", return_value="lakebase"),
            patch.object(GraphDBFactory, "_resolve_graph_engine_config", return_value={}),
        ):
            pipe._resolve_lakebase_mode()

        assert pipe._is_lakebase_synced is False

    def test_unknown_engine_is_never_synced(self):
        """Future engines other than Lakebase do not enable synced mode."""
        pipe = _bare_pipeline()

        with (
            patch.object(GraphDBFactory, "_resolve_graph_engine", return_value="other"),
            patch.object(GraphDBFactory, "_resolve_graph_engine_config", return_value={"sync_mode": "managed_synced"}),
        ):
            pipe._resolve_lakebase_mode()

        assert pipe._is_lakebase_synced is False


class TestLakeflowViewWrap:
    """VIEW DDL must expose object_hash whenever the graph store is managed_synced."""

    def test_wrap_follows_opened_store_when_mode_resolution_missed(self):
        pipe = _bare_pipeline(_is_lakebase_synced=False)
        pipe.spark_sql = "SELECT 's' AS subject, 'p' AS predicate, 'o' AS object"
        pipe.store.is_synced = True

        pipe._sync_flags_from_store()

        assert pipe._is_lakebase_synced is True
        assert pipe._wrap_view_sql_for_lakeflow() is True
        wrapped = pipe._view_sql_for_build()
        assert "object_hash" in wrapped

    def test_ensure_lakeflow_source_view_refreshes_view(self):
        pipe = _bare_pipeline(_is_lakebase_synced=True)
        pipe.spark_sql = "SELECT 's' AS subject, 'p' AS predicate, 'o' AS object"
        pipe.parts = ("cat", "sch", "v_g")
        pipe.store.is_synced = True
        pipe.source_client.create_or_replace_view.return_value = (True, "ok")

        assert pipe._ensure_lakeflow_source_view() is True

        args = pipe.source_client.create_or_replace_view.call_args.args
        assert args[:3] == ("cat", "sch", "v_g")
        assert "object_hash" in args[3]


class TestCollectDomainStats:
    """Ontology + mapping stat block recorded with a build run."""

    def test_counts_ontology_and_mapping(self):
        ontology = {
            "classes": [{"uri": "C1"}, {"uri": "C2"}],
            "properties": [
                {"type": "ObjectProperty"},
                {"type": "ObjectProperty"},
                {"type": "DatatypeProperty"},
            ],
            "constraints": [{"x": 1}],
        }
        assignment = {
            "entities": [{"excluded": False}, {"excluded": True}],
            "relationships": [{"excluded": False}],
        }
        stats = collect_domain_stats(
            ontology, assignment, if_then_rules=[{"r": 1}], axioms=[], shacl_shapes=[{}]
        )

        assert stats["ontology"] == {
            "classes": 2,
            "properties": 3,
            "object_properties": 2,
            "attributes": 1,
            "constraints": 1,
            "if_then_rules": 1,
            "axioms": 0,
            "shacl_shapes": 1,
        }
        assert stats["mapping"]["entity_mappings"] == 2
        assert stats["mapping"]["excluded_entities"] == 1
        assert stats["mapping"]["active_entity_mappings"] == 1
        assert stats["mapping"]["relationship_mappings"] == 1
        assert stats["mapping"]["active_relationship_mappings"] == 1

    def test_empty_inputs_yield_zeros(self):
        stats = collect_domain_stats(None, None)
        assert stats["ontology"]["classes"] == 0
        assert stats["mapping"]["entity_mappings"] == 0

    def test_legacy_assignment_keys(self):
        assignment = {
            "data_source_mappings": [{"excluded": False}],
            "relationship_mappings": [{"excluded": False}, {"excluded": False}],
        }
        stats = collect_domain_stats({}, assignment)
        assert stats["mapping"]["entity_mappings"] == 1
        assert stats["mapping"]["relationship_mappings"] == 2


class TestStepTimesFromTask:
    """phase_times mirrors the UI build log (step description -> duration)."""

    def test_computes_per_step_durations(self):
        steps = [
            SimpleNamespace(
                name="prepare",
                description="Generating SQL",
                status="completed",
                started_at="2026-06-03T08:00:00+00:00",
                completed_at="2026-06-03T08:00:02+00:00",
            ),
            SimpleNamespace(
                name="graph",
                description="Populating graph",
                status="completed",
                started_at="2026-06-03T08:00:02+00:00",
                completed_at="2026-06-03T08:00:07.5+00:00",
            ),
        ]
        task = SimpleNamespace(steps=steps)
        out = step_times_from_task(task)
        assert out == {"Generating SQL": 2.0, "Populating graph": 5.5}

    def test_skips_steps_without_start(self):
        steps = [
            SimpleNamespace(
                name="pending",
                description="Not started",
                status="pending",
                started_at=None,
                completed_at=None,
            ),
        ]
        out = step_times_from_task(SimpleNamespace(steps=steps))
        assert out == {}

    def test_no_steps_returns_empty(self):
        assert step_times_from_task(SimpleNamespace(steps=[])) == {}
        assert step_times_from_task(None) == {}
