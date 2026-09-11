"""Tests that every engine's build produces the mapped-triples relation.

``_BuildPipeline._materialize_data_table`` runs unconditionally in ``run()``
between ``_post_create_view_progress`` and ``_apply_full_rebuild``.  Analytics
reads the ``…_data`` relation and nothing else, so a build that skips this step
would leave a domain that looks successful and cannot be analysed.

The *shape* of that relation is a Lakehouse-only choice: a materialized Delta
table by default, or a pass-through view when the domain opted out of copying.
Lakebase and Neo4j always get the table.
"""

from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.objects.digitaltwin._build_pipeline import _BuildPipeline


class _StubTM:
    """Minimal task manager that records the outcome of fail_task."""

    def __init__(self) -> None:
        self.failed = False
        self.failure_message = ""
        self.steps: list[str] = []
        self.results: list[dict] = []

    def advance_step(self, task_id, msg="") -> None:
        self.steps.append(str(msg))

    def update_progress(self, task_id, pct, msg="") -> None:
        self.steps.append(str(msg))

    def fail_task(self, task_id, msg="") -> None:
        self.failed = True
        self.failure_message = str(msg)

    def complete_task(self, task_id, **kw) -> None:
        self.results.append(kw.get("result") or {})

    def start_task(self, task_id, msg="") -> None:
        pass

    def get_task(self, task_id):
        return None

    def is_cancelled(self, task_id) -> bool:
        return False


def _bare_pipeline(
    tm=None,
    *,
    view_table: str = "cat.sch.triplestore_dom_V3",
    info: dict | None = None,
):
    """Return a ``_BuildPipeline`` with only the attributes needed by ``run()``.

    All heavy I/O phases are replaced by ``MagicMock`` so the fixture
    exercises only the ``_materialize_data_table`` step that sits between
    ``_post_create_view_progress`` and ``_apply_full_rebuild``.

    *info* overrides the domain's ``info`` dict, which is where the graph
    backend and the Lakehouse materialization mode are read from.
    """
    pipe = object.__new__(_BuildPipeline)
    pipe.tm = tm if tm is not None else _StubTM()
    pipe.task_id = "t-test"
    # Domain attributes must satisfy _table_naming.data_table_fqn, which
    # calls SQLHelpers.effective_view_table(domain, settings) internally.
    pipe.domain = SimpleNamespace(
        info={"name": "dom", **(info or {})},
        current_version=3,
        delta={"catalog": "cat", "schema": "sch"},
        uc_domain_folder="dom",
    )
    pipe.settings = SimpleNamespace(databricks_triplestore_table="")
    pipe.domain_snap = MagicMock()
    pipe.domain_name = "dom"
    pipe.view_table = view_table
    pipe.graph_name = "G_V3"
    pipe.warehouse_id = "wh-1"
    pipe.source_client = MagicMock()
    pipe.store = MagicMock()
    pipe.parts = view_table.split(".")
    pipe.phase_times = {}
    pipe.start_time = time.time()
    pipe.triple_count = 0
    pipe.entity_mappings = []
    pipe.relationship_mappings = []
    pipe.spark_sql = ""
    pipe.build_kind = "session"
    pipe.is_api = False
    pipe._build_recorded = False
    pipe._lakebase_engine_config = {}
    pipe._graph_engine = "lakebase"
    pipe._is_lakebase_synced = False

    # Replace all phases except _materialize_data_table with no-ops so the
    # test controls exactly what reaches the materialise step.
    pipe._log_start = MagicMock()
    pipe._prepare_translation = MagicMock(return_value=True)
    pipe._resolve_lakebase_mode = MagicMock()
    pipe._open_store = MagicMock(return_value=True)
    pipe._sync_flags_from_store = MagicMock()
    pipe._create_view = MagicMock(return_value=True)
    pipe._post_create_view_progress = MagicMock()
    pipe._announce_apply_step = MagicMock()
    pipe._apply_full_rebuild = MagicMock(return_value=True)
    pipe._populate_session_cache = MagicMock()
    pipe._complete_task = MagicMock()
    pipe._record_build_run = MagicMock()
    pipe._is_cancelled = MagicMock(return_value=False)
    pipe._log_phase = MagicMock()
    return pipe


@pytest.fixture
def lakebase_pipeline():
    """Shared pipeline simulating a Lakebase engine build."""
    pipe = _bare_pipeline()
    pipe._graph_engine = "lakebase"
    return pipe


@pytest.fixture
def delta_pipeline():
    """Shared pipeline simulating a Lakehouse / Delta engine build."""
    pipe = _bare_pipeline(info={"graph_backend": "databricks"})
    pipe._graph_engine = "delta"
    return pipe


@pytest.fixture
def delta_view_only_pipeline():
    """Lakehouse build for a domain that opted out of copying the triples."""
    pipe = _bare_pipeline(
        info={
            "graph_backend": "databricks",
            "lakehouse_materialization": "view",
        }
    )
    pipe._graph_engine = "delta"
    return pipe


def test_every_engine_materialises_the_mapped_snapshot(lakebase_pipeline):
    """Analytics depends on …_data, so Lakebase builds must produce it too."""
    calls = []
    with patch(
        "back.core.graphdb.delta.materialize.materialize_from_view",
        side_effect=lambda client, view, table: calls.append((view, table)),
    ):
        lakebase_pipeline.run()

    assert calls == [(
        "cat.sch.triplestore_dom_V3",
        "cat.sch.triplestore_dom_V3_data",
    )]


def test_a_failed_materialisation_fails_the_build(lakebase_pipeline):
    """A domain that cannot be analysed must not report a green build."""
    with patch(
        "back.core.graphdb.delta.materialize.materialize_from_view",
        side_effect=RuntimeError("no permission on cat.sch"),
    ):
        lakebase_pipeline.run()

    assert lakebase_pipeline.tm.failed is True
    assert "no permission" in lakebase_pipeline.tm.failure_message


def test_the_delta_pipeline_materialises_once(delta_pipeline):
    """The Lakehouse pipeline must call materialize_from_view exactly once."""
    calls = []
    with patch(
        "back.core.graphdb.delta.materialize.materialize_from_view",
        side_effect=lambda client, view, table: calls.append(table),
    ):
        delta_pipeline.run()

    assert len(calls) == 1


def _relation_modes(pipeline):
    """Run *pipeline* and return the mode of every ..._data relation it built."""
    modes = []
    with patch(
        "back.core.graphdb.delta.materialize.apply_data_relation",
        side_effect=lambda client, view, table, *, mode: modes.append((table, mode)),
    ):
        pipeline.run()
    return modes


def test_view_only_lakehouse_copies_nothing(delta_view_only_pipeline):
    """The whole point of the mode: ..._data is a view, not a copy."""
    assert _relation_modes(delta_view_only_pipeline) == [
        ("cat.sch.triplestore_dom_V3_data", "view")
    ]


def test_lakehouse_materialises_by_default(delta_pipeline):
    assert _relation_modes(delta_pipeline) == [
        ("cat.sch.triplestore_dom_V3_data", "table")
    ]


def test_lakebase_ignores_a_view_only_setting():
    """A Lakebase graph is unreadable by the analytics job, so the snapshot stands.

    The setting can linger on a domain that used to run on Lakehouse, and
    honouring it here would silently break analytics for that domain.
    """
    pipe = _bare_pipeline(
        info={"graph_backend": "lakebase", "lakehouse_materialization": "view"}
    )
    pipe._graph_engine = "lakebase"
    assert _relation_modes(pipe) == [("cat.sch.triplestore_dom_V3_data", "table")]


def test_the_databricks_build_endpoint_also_materialises():
    """``/dtwin/databricks-build/start`` never reaches ``_BuildPipeline``.

    It drives ``DeltaTripleStoreBuildPipeline`` directly, so that pipeline has
    to take the snapshot itself. The two are alternative entry points, never
    both run for one build, so this is not a duplicate of the step above.
    """
    pipe = _bare_delta_pipeline()

    calls = []
    with patch(
        "back.core.graphdb.delta.materialize.materialize_from_view",
        side_effect=lambda client, view, table: calls.append((view, table)),
    ), patch("back.core.graphdb.delta.materialize.optimize_table"):
        pipe.run()

    assert calls == [(
        "cat.sch.triplestore_dom_V3",
        "cat.sch.triplestore_dom_V3_data",
    )]


def _bare_delta_pipeline(
    materialization: str = "table",
    *,
    triple_count: int = 5,
    real_reporting: bool = False,
):
    """A ``DeltaTripleStoreBuildPipeline`` reduced to its relation-building step.

    Driven through ``run()`` rather than the method alone, so dropping the call
    from the sequence fails the test as loudly as gutting the method would.

    *triple_count* of 0 takes the early-exit path (mappings produced nothing).
    *real_reporting* keeps ``_complete_task`` and ``_record_build_run`` intact
    for the tests that assert on what a build reports.
    """
    from back.core.graphdb.delta.DeltaTripleStoreBuildPipeline import (
        DeltaTripleStoreBuildPipeline,
    )

    pipe = object.__new__(DeltaTripleStoreBuildPipeline)
    pipe.tm = _StubTM()
    pipe.task_id = "t-test"
    pipe.domain_name = "dom"
    pipe.view_table = "cat.sch.triplestore_dom_V3"
    pipe.data_table = "cat.sch.triplestore_dom_V3_data"
    pipe.materialization = materialization
    pipe.source_client = MagicMock()
    pipe.start_time = time.time()
    pipe.triple_count = 0
    pipe._build_recorded = False
    pipe._count_view_triples = MagicMock(return_value=triple_count)

    pipe._prepare_translation = MagicMock(return_value=True)
    pipe._create_view = MagicMock(return_value=True)
    pipe._ensure_inferred_companion = MagicMock()
    pipe._truncate_inferred = MagicMock()
    pipe._ensure_graph_view = MagicMock()
    pipe._log_phase = MagicMock()

    if not real_reporting:
        pipe._record_build_run = MagicMock()
        pipe._complete_task = MagicMock()
        return pipe

    # _record_build_run reaches the registry; the entry it would persist is
    # what these tests are about, so capture it instead of writing it.
    pipe.domain = SimpleNamespace(
        info={"name": "dom"}, current_version=3, uc_domain_folder="dom"
    )
    pipe.domain_snap = SimpleNamespace(
        current_version=3, ontology={}, assignment={}
    )
    pipe.settings = SimpleNamespace()
    pipe.build_kind = "ui"
    pipe.entity_mappings = []
    pipe.relationship_mappings = []
    pipe.spark_sql = ""
    pipe.phase_times = {}
    return pipe


def _run_capturing_reports(pipe):
    """Run *pipe* and return ``(task_results, build_run_entries)``."""
    registry = MagicMock()
    with patch(
        "back.core.graphdb.delta.materialize.apply_data_relation"
    ), patch("back.core.graphdb.delta.materialize.optimize_table"), patch(
        "back.objects.registry.RegistryService.RegistryService.from_context",
        return_value=registry,
    ):
        pipe.run()
    entries = [call[0][1] for call in registry.record_build_run.call_args_list]
    return pipe.tm.results, entries


def test_the_databricks_build_endpoint_honours_view_only():
    pipe = _bare_delta_pipeline("view")

    modes = []
    with patch(
        "back.core.graphdb.delta.materialize.apply_data_relation",
        side_effect=lambda client, view, table, *, mode: modes.append(mode),
    ), patch("back.core.graphdb.delta.materialize.optimize_table") as optimize:
        pipe.run()

    assert modes == ["view"]
    # OPTIMIZE is a Delta table operation; running it on a view fails the build
    # at the last phase, after everything useful already succeeded.
    optimize.assert_not_called()


def test_the_databricks_build_optimizes_a_materialized_table():
    pipe = _bare_delta_pipeline("table")

    with patch(
        "back.core.graphdb.delta.materialize.apply_data_relation"
    ), patch("back.core.graphdb.delta.materialize.optimize_table") as optimize:
        pipe.run()

    optimize.assert_called_once()
    assert optimize.call_args[0][1] == "cat.sch.triplestore_dom_V3_data"


def test_the_build_step_says_which_object_it_is_creating():
    """"Materializing Delta table" is a lie when nothing is being copied."""
    view_pipe = _bare_delta_pipeline("view")
    with patch("back.core.graphdb.delta.materialize.apply_data_relation"), patch(
        "back.core.graphdb.delta.materialize.optimize_table"
    ):
        view_pipe.run()
    assert any("pass-through VIEW" in s for s in view_pipe.tm.steps)
    assert not any("Materializing Delta table" in s for s in view_pipe.tm.steps)

    table_pipe = _bare_delta_pipeline("table")
    with patch("back.core.graphdb.delta.materialize.apply_data_relation"), patch(
        "back.core.graphdb.delta.materialize.optimize_table"
    ):
        table_pipe.run()
    assert any("Materializing Delta table" in s for s in table_pipe.tm.steps)


class TestWhatABuildReports:
    """The mode has to reach the task result and the audited build run.

    Without it, a Runs page cannot explain why one build of a domain took
    minutes and the next took seconds, and support cannot tell whether a
    domain's triples were copied at all.
    """

    def test_a_view_only_build_reports_its_mode(self):
        results, entries = _run_capturing_reports(
            _bare_delta_pipeline("view", real_reporting=True)
        )
        assert results[0]["build_mode"] == "delta_view"
        assert results[0]["materialization"] == "view"
        assert entries[0]["materialization"] == "view"

    def test_a_materialized_build_reports_its_mode(self):
        results, entries = _run_capturing_reports(
            _bare_delta_pipeline("table", real_reporting=True)
        )
        assert results[0]["build_mode"] == "delta_full"
        assert results[0]["materialization"] == "table"
        assert entries[0]["materialization"] == "table"

    def test_an_empty_mapping_still_reports_the_mode(self):
        """The early exit has its own payload, which used to hardcode the mode."""
        results, entries = _run_capturing_reports(
            _bare_delta_pipeline("view", triple_count=0, real_reporting=True)
        )
        assert results[0]["triple_count"] == 0
        assert results[0]["build_mode"] == "delta_view"
        assert results[0]["materialization"] == "view"
        assert entries[0]["materialization"] == "view"
