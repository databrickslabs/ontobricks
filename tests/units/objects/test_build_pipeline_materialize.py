"""The Original Graph build exposes the mapped triples as the ``…_data`` VIEW.

Analytics reads ``…_data`` and nothing else, so a build that skips it would
leave a domain that looks successful and cannot be analysed. The Original
Graph is virtual for every backend: nothing is copied.
"""

from __future__ import annotations

import re
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from databricks.sql.exc import RequestError


_VIEW = "cat.sch.triplestore_dom_V3"
_DATA = "cat.sch.triplestore_dom_V3_data"


class _StubTM:
    """Minimal task manager that records the outcome of fail_task."""

    def __init__(self) -> None:
        self.failed = False
        self.failure_message = ""
        self.steps: list[str] = []
        self.results: list[dict] = []
        self.messages: list[str] = []

    def advance_step(self, task_id, msg="") -> None:
        self.steps.append(str(msg))

    def update_progress(self, task_id, pct, msg="") -> None:
        self.steps.append(str(msg))

    def skip_step(self, task_id, msg="") -> None:
        self.steps.append(str(msg))

    def fail_task(self, task_id, msg="") -> None:
        self.failed = True
        self.failure_message = str(msg)

    def complete_task(self, task_id, **kw) -> None:
        self.results.append(kw.get("result") or {})
        self.messages.append(str(kw.get("message", "")))

    def start_task(self, task_id, msg="") -> None:
        pass

    def get_task(self, task_id):
        return None

    def is_cancelled(self, task_id) -> bool:
        return False


class _RecordingClient:
    """Stands in for ``DatabricksClient``, recording every SQL call in order.

    Any ``execute_query`` whose SQL contains *fail_on* raises the connector's
    ``RequestError``: what a status poll sends back once the OAuth token
    behind a long-running statement has expired.
    """

    def __init__(self, *, count: int = 5, fail_on: str | None = None) -> None:
        self.calls: list[str] = []
        self._count = count
        self._fail_on = fail_on

    def execute_query(self, sql: str):
        self.calls.append(sql)
        if self._fail_on and self._fail_on in sql:
            raise RequestError("Error during request to server")
        if sql.strip().upper().startswith("SELECT COUNT"):
            return [{"cnt": self._count}]
        return []

    def execute_statement(self, sql: str) -> bool:
        self.calls.append(sql)
        return True


def _reads(sql: str, relation: str) -> bool:
    """True when *sql* selects from *relation* itself, not a longer name."""
    return re.search(rf"\bFROM\s+{re.escape(relation)}(?![\w.])", sql) is not None


def test_the_original_build_copies_nothing():
    """The Original Graph is virtual for every backend: ``…_data`` is a view."""
    pipe = _bare_delta_pipeline()

    calls = []
    with patch(
        "back.core.graphdb.delta.materialize.materialize_from_view",
        side_effect=lambda client, view, table: calls.append((view, table)),
    ):
        pipe.run()

    assert calls == []
    assert any(
        sql.startswith(f"CREATE OR REPLACE VIEW {_DATA} ")
        for sql in pipe.source_client.calls
    )


def _bare_delta_pipeline(
    *,
    triple_count: int = 5,
    real_reporting: bool = False,
):
    """A ``BuildPipeline`` reduced to its relation-building step.

    Driven through ``run()`` rather than the method alone, so dropping the call
    from the sequence fails the test as loudly as gutting the method would.

    *triple_count* of 0 takes the early-exit path (mappings produced nothing).
    *real_reporting* keeps ``_complete_task`` and ``_record_build_run`` intact
    for the tests that assert on what a build reports.
    """
    from back.core.graphdb.delta.BuildPipeline import (
        BuildPipeline,
    )

    pipe = object.__new__(BuildPipeline)
    pipe.tm = _StubTM()
    pipe.task_id = "t-test"
    pipe.domain_name = "dom"
    pipe.view_table = "cat.sch.triplestore_dom_V3"
    pipe.data_table = "cat.sch.triplestore_dom_V3_data"
    pipe.inferred_table = "cat.sch.triplestore_dom_V3_inferred"
    pipe.graph_view = "cat.sch.triplestore_dom_V3_graph"
    pipe.source_client = _RecordingClient(count=triple_count)
    pipe.start_time = time.time()
    pipe.triple_count = 0
    pipe._build_recorded = False

    pipe._prepare_translation = MagicMock(return_value=True)
    pipe._create_view = MagicMock(return_value=True)
    pipe._rebuild_adjacency_index = MagicMock()
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


def test_the_original_build_exposes_data_as_a_view():
    pipe = _bare_delta_pipeline()

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


def test_the_build_step_says_which_object_it_is_creating():
    """"Materializing Delta table" is a lie when nothing is being copied."""
    view_pipe = _bare_delta_pipeline()
    with patch("back.core.graphdb.delta.materialize.apply_data_relation"):
        view_pipe.run()
    assert any("pass-through VIEW" in s for s in view_pipe.tm.steps)
    assert not any("Materializing Delta table" in s for s in view_pipe.tm.steps)


class TestWhatABuildReports:
    """The mode has to reach the task result and the audited build run.

    Without it, a Runs page cannot explain why one build of a domain took
    minutes and the next took seconds, and support cannot tell whether a
    domain's triples were copied at all.
    """

    def test_a_view_only_build_reports_its_mode(self):
        results, entries = _run_capturing_reports(
            _bare_delta_pipeline(real_reporting=True)
        )
        assert results[0]["build_mode"] == "delta_view"
        assert results[0]["materialization"] == "view"
        assert entries[0]["materialization"] == "view"

    def test_a_successful_build_stamps_last_build_for_the_review_gate(self):
        pipe = _bare_delta_pipeline(real_reporting=True)
        pipe.domain.last_build = "2026-11-23T10:00:00+00:00"
        registry = MagicMock()
        with patch("back.core.graphdb.delta.materialize.apply_data_relation"), patch(
            "back.objects.registry.RegistryService.RegistryService.from_context",
            return_value=registry,
        ):
            pipe.run()

        registry._store.stamp_last_build.assert_called_once_with(
            "dom", "3", "2026-11-23T10:00:00+00:00"
        )

    def test_an_empty_mapping_still_reports_the_mode(self):
        """The early exit has its own payload, which used to hardcode the mode."""
        results, entries = _run_capturing_reports(
            _bare_delta_pipeline(triple_count=0, real_reporting=True)
        )
        assert results[0]["triple_count"] == 0
        assert results[0]["build_mode"] == "delta_view"
        assert results[0]["materialization"] == "view"
        assert entries[0]["materialization"] == "view"


class TestCountingTriples:
    """A Lakehouse build counts what it built and never mistakes an error for 0.

    Reliance's builds ran for an hour, lost the warehouse connection while
    counting the gateway VIEW (the OAuth token expired mid-statement), and
    were reported as successful builds with "no triples generated".
    """

    def test_a_failed_count_fails_the_task_with_the_real_error(self):
        pipe = _bare_delta_pipeline(real_reporting=True)
        pipe.source_client = _RecordingClient(fail_on="COUNT(*)")

        _run_capturing_reports(pipe)

        assert pipe.tm.failed is True
        assert "Error during request to server" in pipe.tm.failure_message

    def test_a_failed_count_is_audited_as_an_error(self):
        pipe = _bare_delta_pipeline(real_reporting=True)
        pipe.source_client = _RecordingClient(fail_on="COUNT(*)")

        _, entries = _run_capturing_reports(pipe)

        assert [entry["status"] for entry in entries] == ["error"]

    def test_the_build_scans_the_gateway_view_once(self):
        pipe = _bare_delta_pipeline()

        pipe.run()

        reads = [
            sql
            for sql in pipe.source_client.calls
            if _reads(sql, _VIEW) and "COUNT(*)" in sql
        ]
        assert len(reads) == 1

    def test_an_empty_build_warns_that_the_mapping_produced_nothing(self):
        pipe = _bare_delta_pipeline(triple_count=0, real_reporting=True)

        results, _ = _run_capturing_reports(pipe)

        assert results[0]["triple_count"] == 0
        assert "no triples generated" in pipe.tm.messages[0]

    def test_an_empty_count_result_fails_the_build(self):
        """COUNT(*) with no row is infrastructure, not an empty mapping."""
        from back.core.errors import InfrastructureError
        from back.core.graphdb.delta import materialize

        class _Empty:
            def execute_query(self, sql: str):
                return []

        with pytest.raises(InfrastructureError, match="returned no row"):
            materialize.count_triples(_Empty(), _VIEW)
