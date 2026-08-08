"""Tests that every engine's build materialises the mapped-triples snapshot.

``_BuildPipeline._materialize_data_table`` runs unconditionally in ``run()``
between ``_post_create_view_progress`` and ``_apply_full_rebuild``.  Analytics
reads the ``…_data`` table and nothing else, so a build that skips this step
would leave a domain that looks successful and cannot be analysed.
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

    def advance_step(self, task_id, msg="") -> None:
        pass

    def update_progress(self, task_id, pct, msg="") -> None:
        pass

    def fail_task(self, task_id, msg="") -> None:
        self.failed = True
        self.failure_message = str(msg)

    def complete_task(self, task_id, **kw) -> None:
        pass

    def start_task(self, task_id, msg="") -> None:
        pass

    def get_task(self, task_id):
        return None

    def is_cancelled(self, task_id) -> bool:
        return False


def _bare_pipeline(tm=None, *, view_table: str = "cat.sch.triplestore_dom_V3"):
    """Return a ``_BuildPipeline`` with only the attributes needed by ``run()``.

    All heavy I/O phases are replaced by ``MagicMock`` so the fixture
    exercises only the ``_materialize_data_table`` step that sits between
    ``_post_create_view_progress`` and ``_apply_full_rebuild``.
    """
    pipe = object.__new__(_BuildPipeline)
    pipe.tm = tm if tm is not None else _StubTM()
    pipe.task_id = "t-test"
    # Domain attributes must satisfy _table_naming.data_table_fqn, which
    # calls SQLHelpers.effective_view_table(domain, settings) internally.
    pipe.domain = SimpleNamespace(
        info={"name": "dom"},
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
    pipe = _bare_pipeline()
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


def test_the_databricks_build_endpoint_also_materialises():
    """``/dtwin/databricks-build/start`` never reaches ``_BuildPipeline``.

    It drives ``DeltaTripleStoreBuildPipeline`` directly, so that pipeline has
    to take the snapshot itself. The two are alternative entry points, never
    both run for one build, so this is not a duplicate of the step above.
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
    pipe.source_client = MagicMock()
    pipe.start_time = time.time()
    pipe.triple_count = 0
    pipe._build_recorded = False
    pipe._record_build_run = MagicMock()
    pipe._count_view_triples = MagicMock(return_value=5)

    # Driven through run() rather than the method alone, so dropping the call
    # from the sequence fails the test as loudly as gutting the method would.
    pipe._prepare_translation = MagicMock(return_value=True)
    pipe._create_view = MagicMock(return_value=True)
    pipe._ensure_inferred_companion = MagicMock()
    pipe._truncate_inferred = MagicMock()
    pipe._ensure_graph_view = MagicMock()
    pipe._complete_task = MagicMock()
    pipe._log_phase = MagicMock()

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
