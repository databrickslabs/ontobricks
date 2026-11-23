"""Original Graph build pipeline: gateway VIEW → ``…_data`` VIEW, every backend."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.core.graphdb.delta.BuildPipeline import (
    BuildPipeline,
    lakehouse_build_steps,
)
from back.core.task_manager.TaskManager import TaskManager


@pytest.mark.unit
def test_original_build_steps() -> None:
    assert [step["name"] for step in lakehouse_build_steps()] == [
        "prepare",
        "view",
        "data",
        "cleanup",
        "adjacency",
    ]


@pytest.mark.unit
def test_prepare_translation_uses_configured_build_transport() -> None:
    pipe = BuildPipeline.__new__(BuildPipeline)
    pipe.tm = MagicMock()
    pipe.task_id = "task-1"
    pipe.domain = SimpleNamespace()
    pipe.settings = SimpleNamespace()
    pipe.host = "host"
    pipe.token = "token"
    pipe.warehouse_id = "wh-build"
    pipe.r2rml_content = "mapping"
    pipe.mapping_config = {}
    pipe.ontology_config = {}
    pipe.base_uri = "https://example.test/"
    pipe.is_api = False

    with (
        patch("back.core.databricks.DatabricksClient") as client_cls,
        patch("back.core.helpers.resolve_build_use_sea", return_value=True),
        patch(
            "back.core.w3c.sparql.extract_r2rml_mappings",
            return_value=([{"entity": "Customer"}], []),
        ),
        patch(
            "back.objects.digitaltwin.DigitalTwin.DigitalTwin."
            "augment_mappings_from_config",
            side_effect=lambda mappings, *_args: mappings,
        ),
        patch(
            "back.objects.digitaltwin.DigitalTwin.DigitalTwin."
            "augment_relationships_from_config",
            side_effect=lambda mappings, *_args: mappings,
        ),
        patch(
            "back.core.w3c.sparql.translate_sparql_to_spark",
            return_value={"success": True, "sql": "SELECT 1"},
        ),
    ):
        assert pipe._prepare_translation() is True

    client_cls.assert_called_once_with(
        host="host",
        token="token",
        warehouse_id="wh-build",
        use_sea=True,
    )


def _minimal_run_pipeline():
    pipe = BuildPipeline.__new__(BuildPipeline)
    pipe.tm = MagicMock()
    pipe.task_id = "task-run"
    pipe.domain = SimpleNamespace(
        info={"name": "dom"},
        current_version=3,
        delta={"catalog": "cat", "schema": "sch"},
    )
    pipe.settings = SimpleNamespace()
    pipe.domain_snap = SimpleNamespace(
        current_version=3, ontology={}, assignment={}
    )
    pipe.domain_name = "dom"
    pipe.view_table = "cat.sch.triplestore_dom_V3"
    pipe.data_table = "cat.sch.triplestore_dom_V3_data"
    pipe.host = "host"
    pipe.token = "token"
    pipe.warehouse_id = "wh"
    pipe.parts = ["cat", "sch", "triplestore_dom_V3"]
    pipe._build_recorded = False
    pipe.start_time = 0.0
    pipe.phase_times = {}
    pipe.is_api = False
    pipe.build_kind = "ui"
    pipe.entity_mappings = []
    pipe.relationship_mappings = []
    pipe.spark_sql = "SELECT 1"
    pipe.triple_count = 5
    pipe.source_client = MagicMock()
    pipe.inferred_table = "cat.sch.triplestore_dom_V3_inferred"
    pipe.graph_view = "cat.sch.triplestore_dom_V3_graph"

    pipe._prepare_translation = MagicMock(return_value=True)
    pipe._create_view = MagicMock(return_value=True)
    pipe._build_data_view = MagicMock(return_value=True)
    pipe._complete_task = MagicMock()
    pipe._is_cancelled = MagicMock(return_value=False)
    pipe._record_build_run = MagicMock()
    pipe._log_phase = MagicMock()
    return pipe


def _statements(client) -> list:
    return [c.args[0] for c in client.execute_statement.call_args_list]


@pytest.mark.unit
def test_run_builds_adjacency_on_the_data_view() -> None:
    pipe = _minimal_run_pipeline()

    with patch("back.core.graphdb.delta.BuildPipeline.MaterializedStore") as store_cls:
        pipe.run()

    store_cls.assert_called_once_with(
        pipe.source_client, domain=pipe.domain, settings=pipe.settings
    )
    store_cls.return_value.rebuild_adjacency.assert_called_once_with(pipe.data_table)


@pytest.mark.unit
def test_run_drops_the_legacy_inferred_objects() -> None:
    pipe = _minimal_run_pipeline()

    with patch("back.core.graphdb.delta.BuildPipeline.MaterializedStore"):
        pipe.run()

    statements = _statements(pipe.source_client)
    assert f"DROP VIEW IF EXISTS {pipe.graph_view}" in statements
    assert f"DROP TABLE IF EXISTS {pipe.inferred_table}" in statements
    assert not any("CREATE" in s and "_inferred" in s for s in statements)


@pytest.mark.unit
def test_run_never_materializes_or_optimizes() -> None:
    pipe = _minimal_run_pipeline()

    with (
        patch("back.core.graphdb.delta.BuildPipeline.MaterializedStore"),
        patch("back.core.graphdb.delta.materialize.optimize_table") as optimize,
    ):
        pipe.run()

    optimize.assert_not_called()
    assert not any("CREATE OR REPLACE TABLE" in s for s in _statements(pipe.source_client))


@pytest.mark.unit
def test_run_fails_when_adjacency_ctas_fails() -> None:
    pipe = _minimal_run_pipeline()

    with patch(
        "back.core.graphdb.delta.BuildPipeline.MaterializedStore"
    ) as store_cls:
        store_cls.return_value.rebuild_adjacency.side_effect = RuntimeError(
            "adjacency failure"
        )
        pipe.run()

    pipe.tm.fail_task.assert_called_once()
    fail_msg = pipe.tm.fail_task.call_args.args[1]
    assert "adjacency failure" in fail_msg


@pytest.mark.unit
def test_data_view_is_built_after_counting_the_gateway_view() -> None:
    pipe = _minimal_run_pipeline()
    del pipe._build_data_view
    order = []
    with (
        patch(
            "back.core.graphdb.delta.materialize.count_triples",
            side_effect=lambda *_a: order.append("count") or 7,
        ),
        patch(
            "back.core.graphdb.delta.materialize.apply_data_relation",
            side_effect=lambda *_a, mode: order.append(("data", mode)),
        ),
    ):
        assert pipe._build_data_view() is True

    assert order == ["count", ("data", "view")]
    assert pipe.triple_count == 7


@pytest.mark.unit
def test_an_empty_mapping_stops_before_the_data_view() -> None:
    pipe = _minimal_run_pipeline()
    del pipe._build_data_view
    pipe._finish_without_triples = MagicMock()
    with (
        patch("back.core.graphdb.delta.materialize.count_triples", return_value=0),
        patch("back.core.graphdb.delta.materialize.apply_data_relation") as apply,
    ):
        assert pipe._build_data_view() is False

    apply.assert_not_called()
    pipe._finish_without_triples.assert_called_once_with()


@pytest.mark.unit
def test_run_sets_terminal_step_statuses_with_real_task_manager() -> None:
    tm = TaskManager()
    task = tm.create_task("original", "original_build", steps=lakehouse_build_steps())
    tm.start_task(task.id, "Preparing mappings...")
    pipe = _minimal_run_pipeline()
    pipe.tm = tm
    pipe.task_id = task.id
    pipe._create_view = lambda: tm.advance_step(  # noqa: E731
        pipe.task_id, f"Creating VIEW {pipe.view_table}..."
    )

    def _build_data_view() -> bool:
        tm.advance_step(pipe.task_id, f"Creating pass-through VIEW {pipe.data_table}...")
        return True

    pipe._build_data_view = _build_data_view

    def _complete_task() -> None:
        tm.complete_task(pipe.task_id, result={"ok": True}, message="done")
        pipe._build_recorded = True

    pipe._complete_task = _complete_task
    try:
        with patch("back.core.graphdb.delta.BuildPipeline.MaterializedStore"):
            pipe.run()

        task_after = tm.get_task(pipe.task_id)
        step_statuses = {step.name: step.status for step in task_after.steps}
        assert step_statuses == {
            "prepare": "completed",
            "view": "completed",
            "data": "completed",
            "cleanup": "completed",
            "adjacency": "completed",
        }
    finally:
        tm.delete_task(task.id)
