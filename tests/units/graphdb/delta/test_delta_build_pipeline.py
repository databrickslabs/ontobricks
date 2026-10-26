"""Regression tests for the Databricks-only triple-store build pipeline."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.core.graphdb.delta.DeltaTripleStoreBuildPipeline import (
    DeltaTripleStoreBuildPipeline,
    lakehouse_build_steps,
)
from back.core.task_manager.TaskManager import TaskManager


@pytest.mark.unit
@pytest.mark.parametrize(
    ("mode", "materialize_description"),
    [
        ("table", "Materializing Delta table in Unity Catalog"),
        ("view", "Exposing pass-through data view"),
    ],
)
def test_lakehouse_build_steps_name_materialization_mode(
    mode: str, materialize_description: str
) -> None:
    steps = lakehouse_build_steps(mode)
    assert [step["name"] for step in steps] == [
        "prepare",
        "view",
        "materialize",
        "inferred",
        "graph_view",
        "optimize",
        "adjacency",
    ]
    assert steps[2]["description"] == materialize_description


@pytest.mark.unit
def test_prepare_translation_uses_configured_build_transport() -> None:
    pipe = DeltaTripleStoreBuildPipeline.__new__(DeltaTripleStoreBuildPipeline)
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


def _minimal_run_pipeline(*, materialization: str = "table"):
    pipe = DeltaTripleStoreBuildPipeline.__new__(DeltaTripleStoreBuildPipeline)
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
    pipe.materialization = materialization
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
    pipe._materialize_data_table = MagicMock(return_value=True)
    pipe._ensure_inferred_companion = MagicMock()
    pipe._truncate_inferred = MagicMock()
    pipe._ensure_graph_view = MagicMock()
    pipe._complete_task = MagicMock()
    pipe._is_cancelled = MagicMock(return_value=False)
    pipe._record_build_run = MagicMock()
    pipe._log_phase = MagicMock()
    return pipe


@pytest.mark.unit
def test_run_builds_adjacency_index_after_graph_view() -> None:
    pipe = _minimal_run_pipeline(materialization="table")

    order = []

    def _mark_graph_view():
        order.append("graph_view")

    pipe._ensure_graph_view = MagicMock(side_effect=_mark_graph_view)

    with patch(
        "back.core.graphdb.delta.DeltaTripleStoreBuildPipeline.DeltaFlatStore"
    ) as store_cls, patch("back.core.graphdb.delta.materialize.optimize_table"):
        store = store_cls.return_value
        store.rebuild_adjacency.side_effect = lambda _table: order.append("adjacency")
        pipe.run()

    store_cls.assert_called_once_with(
        pipe.source_client, domain=pipe.domain, settings=pipe.settings
    )
    store.rebuild_adjacency.assert_called_once_with(pipe.graph_view)
    assert order == ["graph_view", "adjacency"]


@pytest.mark.unit
def test_run_fails_when_adjacency_ctas_fails() -> None:
    pipe = _minimal_run_pipeline(materialization="view")

    with patch(
        "back.core.graphdb.delta.DeltaTripleStoreBuildPipeline.DeltaFlatStore"
    ) as store_cls:
        store_cls.return_value.rebuild_adjacency.side_effect = RuntimeError(
            "adjacency failure"
        )
        pipe.run()

    pipe.tm.fail_task.assert_called_once()
    fail_msg = pipe.tm.fail_task.call_args.args[1]
    assert "adjacency failure" in fail_msg


@pytest.mark.unit
def test_run_in_table_mode_optimizes_data_before_adjacency() -> None:
    pipe = _minimal_run_pipeline(materialization="table")
    order = []

    with patch(
        "back.core.graphdb.delta.DeltaTripleStoreBuildPipeline.DeltaFlatStore"
    ) as store_cls, patch(
        "back.core.graphdb.delta.materialize.optimize_table",
        side_effect=lambda *_args, **_kwargs: order.append("data_optimize"),
    ):
        store = store_cls.return_value
        store.rebuild_adjacency.side_effect = lambda _table: order.append("adjacency")
        pipe.run()

    assert order == ["data_optimize", "adjacency"]


@pytest.mark.unit
def test_run_in_table_mode_advances_all_post_materialization_stages() -> None:
    pipe = _minimal_run_pipeline(materialization="table")

    with (
        patch("back.core.graphdb.delta.DeltaTripleStoreBuildPipeline.DeltaFlatStore"),
        patch("back.core.graphdb.delta.materialize.optimize_table"),
    ):
        pipe.run()

    assert [call.args[1] for call in pipe.tm.advance_step.call_args_list] == [
        "Preparing inferred-triples table...",
        "Creating knowledge graph view...",
        "Optimizing Delta table...",
        "Building adjacency indexes...",
    ]


@pytest.mark.unit
def test_view_build_skips_optimize_stage() -> None:
    pipe = _minimal_run_pipeline(materialization="view")

    with (
        patch("back.core.graphdb.delta.DeltaTripleStoreBuildPipeline.DeltaFlatStore"),
        patch(
            "back.core.graphdb.delta.DeltaTripleStoreBuildPipeline."
            "materialize.optimize_table"
        ) as optimize,
    ):
        pipe.run()

    pipe.tm.skip_step.assert_called_once_with(
        pipe.task_id, "Optimization not needed for pass-through view"
    )
    optimize.assert_not_called()


@pytest.mark.unit
@pytest.mark.parametrize(
    ("materialization", "expected_statuses"),
    [
        (
            "view",
            {"graph_view": "completed", "optimize": "skipped", "adjacency": "completed"},
        ),
        ("table", {"optimize": "completed", "adjacency": "completed"}),
    ],
)
def test_run_sets_terminal_step_statuses_with_real_task_manager(
    materialization: str, expected_statuses: dict[str, str]
) -> None:
    tm = TaskManager()
    task = tm.create_task(
        f"lakehouse-{materialization}",
        "lakehouse_build",
        steps=lakehouse_build_steps(materialization),
    )
    tm.start_task(task.id, "Preparing mappings...")
    pipe = _minimal_run_pipeline(materialization=materialization)
    pipe.tm = tm
    pipe.task_id = task.id
    pipe._create_view = lambda: tm.advance_step(  # noqa: E731
        pipe.task_id, f"Creating VIEW {pipe.view_table}..."
    )

    def _materialize_data_table() -> bool:
        step = (
            f"Creating pass-through VIEW {pipe.data_table}..."
            if materialization == "view"
            else f"Materializing Delta table {pipe.data_table}..."
        )
        tm.advance_step(pipe.task_id, step)
        return True

    pipe._materialize_data_table = _materialize_data_table

    def _complete_task() -> None:
        tm.complete_task(pipe.task_id, result={"ok": True}, message="done")
        pipe._build_recorded = True

    pipe._complete_task = _complete_task
    try:
        with (
            patch("back.core.graphdb.delta.DeltaTripleStoreBuildPipeline.DeltaFlatStore"),
            patch("back.core.graphdb.delta.materialize.optimize_table"),
        ):
            pipe.run()

        task_after = tm.get_task(pipe.task_id)
        assert task_after is not None
        step_statuses = {step.name: step.status for step in task_after.steps}
        for step_name, expected_status in expected_statuses.items():
            assert step_statuses[step_name] == expected_status
    finally:
        tm.delete_task(task.id)
