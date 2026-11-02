"""SlotPipelineRunner: create/update the per-version slot pipeline and follow its updates."""

from __future__ import annotations

import hashlib
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from databricks.sdk.service.pipelines import (
    FileLibrary,
    PipelineLibrary,
    PipelineState,
    UpdateInfoState,
)
from databricks.sdk.service.workspace import ImportFormat

from back.core.errors import (
    ConflictError,
    InfrastructureError,
    OperationCancelledError,
    ValidationError,
)
from back.core.streaming import SlotPipelineRunner

pytestmark = pytest.mark.unit

_NAME = "ontobricks-slots-sales-v3"
_SQL = "CREATE OR REFRESH STREAMING TABLE main.ob.t_V3_slots"
_ME = "me@example.com"
_PATH = f"/Workspace/Users/{_ME}/.ontobricks/pipelines/{_NAME}.sql"
_FP_KEY = "ontobricks.slot_sql_sha256"


class _Wait:
    def __init__(self) -> None:
        self.waited = False

    def result(self, timeout=None):
        self.waited = True


class _FakePipelines:
    """In-memory ``w.pipelines``: specs by id, a scripted update state sequence."""

    def __init__(self, *, update_states=("COMPLETED",), state="IDLE", fail_on=None):
        self.specs: dict[str, dict] = {}
        self.created: list[dict] = []
        self.updated: list[tuple[str, dict]] = []
        self.started: list[tuple[str, bool]] = []
        self.stopped: list[str] = []
        self.list_filters: list[str] = []
        self.waits: list[_Wait] = []
        self.state = state
        self._update_states = list(update_states)
        self._fail_on = fail_on

    def _maybe_fail(self, op):
        if self._fail_on == op:
            raise RuntimeError(f"{op} refused: PERMISSION_DENIED")

    def list_pipelines(self, filter=None):
        self._maybe_fail("list")
        self.list_filters.append(filter)
        return iter(
            [SimpleNamespace(name=s["name"], pipeline_id=pid) for pid, s in self.specs.items()]
        )

    def create(self, **kw):
        self._maybe_fail("create")
        pid = f"pipe-{len(self.created) + 1}"
        self.created.append(kw)
        self.specs[pid] = kw
        return SimpleNamespace(pipeline_id=pid)

    def update(self, pipeline_id, **kw):
        self._maybe_fail("update")
        self.updated.append((pipeline_id, kw))
        self.specs[pipeline_id] = kw

    def get(self, pipeline_id):
        self._maybe_fail("get")
        spec = self.specs[pipeline_id]
        return SimpleNamespace(
            state=PipelineState(self.state),
            spec=SimpleNamespace(
                configuration=spec.get("configuration"), deployment=spec.get("deployment")
            ),
        )

    def start_update(self, pipeline_id, full_refresh=None):
        self._maybe_fail("start")
        self.started.append((pipeline_id, full_refresh))
        return SimpleNamespace(update_id="upd-1")

    def get_update(self, pipeline_id, update_id):
        self._maybe_fail("get_update")
        state = self._update_states.pop(0) if len(self._update_states) > 1 else self._update_states[0]
        return SimpleNamespace(update=SimpleNamespace(state=UpdateInfoState(state)))

    def stop(self, pipeline_id):
        self._maybe_fail("stop")
        self.stopped.append(pipeline_id)
        waiter = _Wait()
        self.waits.append(waiter)
        return waiter


class _FakeWorkspace:
    def __init__(self) -> None:
        self.dirs: list[str] = []
        self.uploads: list[dict] = []

    def mkdirs(self, path):
        self.dirs.append(path)

    def upload(self, path, content, *, format=None, language=None, overwrite=False):
        self.uploads.append(
            {"path": path, "content": content, "format": format, "overwrite": overwrite}
        )


def _client(**kw):
    return SimpleNamespace(
        pipelines=_FakePipelines(**kw),
        workspace=_FakeWorkspace(),
        current_user=SimpleNamespace(me=lambda: SimpleNamespace(user_name=_ME)),
    )


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def _runner(client=None, clock=None):
    client = client or _client()
    clock = clock or _Clock()
    return (
        SlotPipelineRunner(client_factory=lambda: client, sleep=clock.sleep, clock=clock),
        client,
    )


def _ensure(runner, **overrides):
    kwargs = dict(
        name=_NAME,
        catalog="main",
        schema="ob",
        sql_text=_SQL,
        continuous=False,
    )
    kwargs.update(overrides)
    return runner.ensure(**kwargs)


class TestPipelineName:
    def test_domain_and_version_are_sanitised(self):
        assert SlotPipelineRunner.pipeline_name("Sales & Ops", 3) == "ontobricks-slots-sales-ops-v3"

    def test_an_empty_domain_still_yields_a_name(self):
        assert SlotPipelineRunner.pipeline_name("", "") == "ontobricks-slots-domain-v"


class TestEnsure:
    def test_the_sql_is_uploaded_as_a_workspace_file_under_the_user(self):
        runner, client = _runner()

        _ensure(runner)

        assert client.workspace.dirs == [f"/Workspace/Users/{_ME}/.ontobricks/pipelines"]
        assert client.workspace.uploads == [
            {
                "path": _PATH,
                "content": _SQL.encode("utf-8"),
                "format": ImportFormat.AUTO,
                "overwrite": True,
            }
        ]

    def test_the_first_call_creates_the_pipeline(self):
        runner, client = _runner()

        pipeline_id = _ensure(runner, continuous=True, channel="CURRENT")

        assert pipeline_id == "pipe-1"
        spec = client.pipelines.created[0]
        assert spec["name"] == _NAME
        assert spec["catalog"] == "main" and spec["schema"] == "ob"
        assert spec["serverless"] is True
        assert spec["continuous"] is True
        assert spec["channel"] == "CURRENT"
        assert spec["development"] is False
        assert spec["libraries"] == [PipelineLibrary(file=FileLibrary(path=_PATH))]

    def test_the_sql_fingerprint_is_stored_in_the_configuration(self):
        runner, client = _runner()

        _ensure(runner)

        expected = hashlib.sha256(_SQL.encode("utf-8")).hexdigest()
        assert client.pipelines.created[0]["configuration"] == {_FP_KEY: expected}
        assert SlotPipelineRunner.fingerprint(_SQL) == expected

    def test_an_explicit_fingerprint_is_stored_instead_of_the_sql_hash(self):
        """While a full refresh is pending the caller keeps the previous fingerprint."""
        runner, client = _runner()

        _ensure(runner, fingerprint="")

        assert client.pipelines.created[0]["configuration"] == {_FP_KEY: ""}

    def test_a_second_call_updates_the_same_pipeline(self):
        runner, client = _runner()
        first = _ensure(runner)

        second = SlotPipelineRunner(client_factory=lambda: client).ensure(
            name=_NAME, catalog="main", schema="ob", sql_text=_SQL + " -- v2", continuous=False
        )

        assert second == first
        assert len(client.pipelines.created) == 1
        assert [pid for pid, _ in client.pipelines.updated] == [first]

    def test_the_lookup_lists_the_name_with_any_prefix(self):
        runner, client = _runner()

        runner.find_pipeline_id(_NAME)

        assert client.pipelines.list_filters == [f"name LIKE '%{_NAME}'"]


    def test_a_missing_pipeline_is_not_found(self):
        runner, _ = _runner()

        assert runner.find_pipeline_id(_NAME) is None

    def test_the_stored_fingerprint_is_read_back(self):
        runner, _ = _runner()
        pipeline_id = _ensure(runner)

        assert runner.stored_fingerprint(pipeline_id) == SlotPipelineRunner.fingerprint(_SQL)

    def test_a_pipeline_the_runner_created_is_not_bundle_managed(self):
        runner, _ = _runner()

        assert runner.bundle_managed(_ensure(runner)) is False

    def test_a_pipeline_a_bundle_deployed_is_bundle_managed(self):
        from databricks.sdk.service.pipelines import DeploymentKind

        runner, client = _runner()
        pipeline_id = _ensure(runner)
        client.pipelines.specs[pipeline_id]["deployment"] = SimpleNamespace(kind=DeploymentKind.BUNDLE)

        assert runner.bundle_managed(pipeline_id) is True

    @pytest.mark.parametrize("name", ["", "../etc", "a name", "x'y"])
    def test_an_unsafe_name_is_refused(self, name):
        runner, client = _runner()

        with pytest.raises(ValidationError):
            _ensure(runner, name=name)
        assert client.workspace.uploads == []

    @pytest.mark.parametrize("op", ["list", "create"])
    def test_an_sdk_failure_is_an_infrastructure_error(self, op):
        runner, _ = _runner(_client(fail_on=op))

        with pytest.raises(InfrastructureError, match="PERMISSION_DENIED"):
            _ensure(runner)

    def test_a_client_that_cannot_be_built_is_an_infrastructure_error(self):
        def _boom():
            raise RuntimeError("no credentials")

        with pytest.raises(InfrastructureError):
            SlotPipelineRunner(client_factory=_boom).find_pipeline_id(_NAME)


class TestUpdates:
    def test_start_update_returns_the_update_id(self):
        runner, client = _runner()

        assert runner.start_update("pipe-1", full_refresh=True) == "upd-1"
        assert client.pipelines.started == [("pipe-1", True)]

    def test_a_continuous_pipeline_that_already_runs_an_update_is_adopted(self):
        """Switching a pipeline to continuous starts its own update (verified live,
        2026-11-02): the next ``start_update`` is refused with "already exists" and
        the runner returns that active update instead of raising."""

        class _Busy(_FakePipelines):
            def start_update(self, pipeline_id, full_refresh=None):
                self.started.append((pipeline_id, full_refresh))
                raise RuntimeError(
                    f"An active update 'u-9' already exists for pipeline '{pipeline_id}'."
                )

            def get(self, pipeline_id):
                return SimpleNamespace(
                    state=PipelineState("RUNNING"),
                    spec=SimpleNamespace(configuration={}, deployment=None),
                    latest_updates=[SimpleNamespace(update_id="u-9")],
                )

        client = _client()
        client.pipelines = _Busy()
        runner = SlotPipelineRunner(client_factory=lambda: client)

        assert runner.start_update("pipe-1") == "u-9"

    def test_a_full_refresh_is_not_satisfied_by_the_running_update(self):
        class _Busy(_FakePipelines):
            def start_update(self, pipeline_id, full_refresh=None):
                raise RuntimeError("An active update 'u-9' already exists for pipeline 'pipe-1'.")

            def get(self, pipeline_id):
                return SimpleNamespace(latest_updates=[SimpleNamespace(update_id="u-9")])

        client = _client()
        client.pipelines = _Busy()
        runner = SlotPipelineRunner(client_factory=lambda: client)

        with pytest.raises(InfrastructureError, match="already exists"):
            runner.start_update("pipe-1", full_refresh=True)

    def test_wait_follows_the_states_to_completion(self):
        runner, _ = _runner(_client(update_states=("QUEUED", "RUNNING", "COMPLETED")))
        seen: list[str] = []

        state = runner.wait_for_update("pipe-1", "upd-1", timeout_s=60, on_state=seen.append)

        assert state == "COMPLETED"
        assert seen == ["QUEUED", "RUNNING", "COMPLETED"]

    def test_until_running_stops_at_the_first_running_state(self):
        runner, _ = _runner(_client(update_states=("INITIALIZING", "RUNNING", "COMPLETED")))

        state = runner.wait_for_update("pipe-1", "upd-1", timeout_s=60, until_running=True)

        assert state == "RUNNING"

    @pytest.mark.parametrize("terminal", ["FAILED", "CANCELED"])
    def test_a_failed_or_cancelled_update_is_an_infrastructure_error(self, terminal):
        runner, _ = _runner(_client(update_states=("RUNNING", terminal)))

        with pytest.raises(InfrastructureError, match=terminal):
            runner.wait_for_update("pipe-1", "upd-1", timeout_s=60)

    def test_a_cancelled_build_stops_waiting(self):
        runner, _ = _runner(_client(update_states=("RUNNING",)))

        with pytest.raises(OperationCancelledError):
            runner.wait_for_update("pipe-1", "upd-1", timeout_s=60, cancel_check=lambda: True)

    def test_a_running_update_times_out(self):
        clock = _Clock()
        runner, _ = _runner(_client(update_states=("RUNNING",)), clock)

        with pytest.raises(InfrastructureError, match="did not finish within 30s"):
            runner.wait_for_update("pipe-1", "upd-1", timeout_s=30, poll_s=10)
        assert clock.now >= 30

    def test_a_lost_update_is_an_infrastructure_error(self):
        runner, _ = _runner(_client(fail_on="get_update"))

        with pytest.raises(InfrastructureError):
            runner.wait_for_update("pipe-1", "upd-1", timeout_s=60)

    def test_a_hung_sdk_call_times_out(self):
        import threading

        release = threading.Event()
        client = _client()
        client.pipelines.start_update = lambda **kw: release.wait(5)
        runner = SlotPipelineRunner(client_factory=lambda: client, api_timeout_s=0.05)

        try:
            with pytest.raises(InfrastructureError, match="timed out"):
                runner.start_update("pipe-1")
        finally:
            release.set()


class TestStateAndStop:
    def test_get_state_names_the_pipeline_state(self):
        runner, client = _runner(_client(state="RUNNING"))
        pipeline_id = _ensure(runner)

        assert runner.get_state(pipeline_id) == "RUNNING"

    def test_stop_waits_for_the_pipeline_to_stop(self):
        runner, client = _runner()

        runner.stop("pipe-1")

        assert client.pipelines.stopped == ["pipe-1"]
        assert client.pipelines.waits[0].waited is True

    def test_a_refused_stop_is_an_infrastructure_error(self):
        runner, _ = _runner(_client(fail_on="stop"))

        with pytest.raises(InfrastructureError):
            runner.stop("pipe-1")


class TestDevelopmentModeNames:
    """DAB development mode deploys ``<name>`` as ``[dev <user>] <name>``."""

    @staticmethod
    def _deployed(client, pipeline_id: str, name: str) -> None:
        client.pipelines.specs[pipeline_id] = {"name": name, "configuration": {}}

    def test_a_prefixed_pipeline_is_found_with_its_deployed_name(self):
        runner, client = _runner()
        self._deployed(client, "pipe-dab", f"[dev ada] {_NAME}")

        assert runner.find_pipeline(_NAME) == ("pipe-dab", f"[dev ada] {_NAME}")
        assert runner.find_pipeline_id(_NAME) == "pipe-dab"

    def test_an_exact_name_wins_over_a_prefixed_one(self):
        runner, client = _runner()
        self._deployed(client, "pipe-dab", f"[dev ada] {_NAME}")
        self._deployed(client, "pipe-app", _NAME)

        assert runner.find_pipeline_id(_NAME) == "pipe-app"

    def test_two_prefixed_pipelines_and_no_exact_one_are_refused(self):
        runner, client = _runner()
        self._deployed(client, "pipe-ada", f"[dev ada] {_NAME}")
        self._deployed(client, "pipe-bob", f"[dev bob] {_NAME}")

        with pytest.raises(ConflictError, match="2 pipelines match"):
            runner.find_pipeline_id(_NAME)

    def test_a_longer_name_with_the_same_ending_does_not_match(self):
        runner, client = _runner()
        self._deployed(client, "pipe-other", f"x{_NAME}")
        self._deployed(client, "pipe-other2", f"[dev ada] x{_NAME}")

        assert runner.find_pipeline_id(_NAME) is None

    def test_updating_a_prefixed_pipeline_keeps_its_deployed_name(self):
        runner, client = _runner()
        self._deployed(client, "pipe-dab", f"[dev ada] {_NAME}")

        assert _ensure(runner) == "pipe-dab"

        [(pipeline_id, spec)] = client.pipelines.updated
        assert pipeline_id == "pipe-dab" and spec["name"] == f"[dev ada] {_NAME}"
        assert client.pipelines.created == []


class _FakeEventsAPI:
    """``w.api_client``: the pipeline events REST endpoint, one scripted page per call."""

    def __init__(self, pages, *, error=None):
        self.pages = list(pages)
        self.calls: list[tuple[str, str, dict]] = []
        self._error = error

    def do(self, method, path, query=None):
        self.calls.append((method, path, dict(query or {})))
        if self._error is not None:
            raise self._error
        return self.pages.pop(0) if self.pages else {"events": []}


def _flow_progress(update_id, **metrics):
    return {
        "event_type": "flow_progress",
        "origin": {"update_id": update_id},
        "details": {"flow_progress": {"status": "COMPLETED", "metrics": metrics}},
    }


def _metrics_runner(pages, **kw):
    client = _client()
    client.api_client = _FakeEventsAPI(pages, **kw)
    return _runner(client)


class TestUpdateMetrics:
    def test_the_update_s_flow_metrics_are_summed(self):
        events = [
            _flow_progress("upd-2", num_output_rows=99),
            _flow_progress("upd-1", num_output_rows=4, num_upserted_rows=3, num_deleted_rows=1),
            {"event_type": "update_progress", "origin": {"update_id": "upd-1"}},
            _flow_progress("upd-1", num_output_rows=6, num_upserted_rows=6, num_deleted_rows=0),
            _flow_progress("upd-0", num_output_rows=500),
        ]
        runner, _ = _metrics_runner([{"events": events}])

        assert runner.update_metrics("pipe-1", "upd-1") == {
            "rows_output": 10,
            "rows_upserted": 9,
            "rows_deleted": 1,
        }

    def test_a_metric_no_event_carries_is_none(self):
        runner, _ = _metrics_runner([{"events": [_flow_progress("upd-1", num_output_rows=7)]}])

        assert runner.update_metrics("pipe-1", "upd-1") == {
            "rows_output": 7,
            "rows_upserted": None,
            "rows_deleted": None,
        }

    def test_the_raw_events_endpoint_is_paged_until_the_update_is_passed(self):
        runner, client = _metrics_runner(
            [
                {"events": [_flow_progress("upd-2", num_output_rows=1)], "next_page_token": "p2"},
                {"events": [_flow_progress("upd-1", num_output_rows=2)], "next_page_token": "p3"},
                {"events": [_flow_progress("upd-0", num_output_rows=3)], "next_page_token": "p4"},
            ]
        )

        assert runner.update_metrics("pipe-1", "upd-1")["rows_output"] == 2
        calls = client.api_client.calls
        assert [c[:2] for c in calls] == [("GET", "/api/2.0/pipelines/pipe-1/events")] * 3
        assert calls[1][2] == {"max_results": 250, "page_token": "p2"}

    def test_an_api_error_is_none_not_raised(self):
        runner, _ = _metrics_runner([], error=RuntimeError("PERMISSION_DENIED"))

        assert runner.update_metrics("pipe-1", "upd-1") is None

    def test_the_live_auto_cdc_shape_counts_its_one_running_event(self):
        """Live (2026-11-02): QUEUED / STARTING / COMPLETED carry no metrics, the first
        RUNNING an empty dict, the second the upserted / deleted counts."""

        def status(name, metrics):
            return {
                "event_type": "flow_progress",
                "origin": {"update_id": "upd-1"},
                "details": {"flow_progress": {"status": name, "metrics": metrics}},
            }

        running = {"backlog_bytes": 0, "num_upserted_rows": 9, "num_deleted_rows": 0}
        events = [
            status("COMPLETED", None),
            status("RUNNING", running),
            status("RUNNING", {}),
            status("STARTING", None),
            status("QUEUED", None),
        ]
        runner, _ = _metrics_runner([{"events": events}])

        assert runner.update_metrics("pipe-1", "upd-1") == {
            "rows_output": None,
            "rows_upserted": 9,
            "rows_deleted": 0,
        }

    def test_an_event_without_an_update_id_does_not_end_the_update(self):
        user_action = {"event_type": "user_action", "origin": {"pipeline_id": "pipe-1"}}
        events = [
            _flow_progress("upd-1", num_output_rows=5),
            user_action,
            _flow_progress("upd-1", num_output_rows=7),
        ]
        runner, _ = _metrics_runner([{"events": events}])

        assert runner.update_metrics("pipe-1", "upd-1")["rows_output"] == 12

    def test_no_metric_at_all_is_none(self):
        events = [
            {"event_type": "flow_progress", "origin": {"update_id": "upd-1"}, "details": {}},
            _flow_progress("upd-1"),
        ]
        runner, _ = _metrics_runner([{"events": events}])

        assert runner.update_metrics("pipe-1", "upd-1") is None

    def test_digit_string_counts_are_read_and_booleans_ignored(self):
        events = [_flow_progress("upd-1", num_upserted_rows="9", num_deleted_rows=True)]
        runner, _ = _metrics_runner([{"events": events}])

        assert runner.update_metrics("pipe-1", "upd-1") == {
            "rows_output": None,
            "rows_upserted": 9,
            "rows_deleted": None,
        }

    def test_the_page_cap_inside_the_update_is_none_not_a_partial_sum(self):
        pages = [
            {"events": [_flow_progress("upd-1", num_output_rows=1)], "next_page_token": f"p{i}"}
            for i in range(30)
        ]
        runner, client = _metrics_runner(pages)

        assert runner.update_metrics("pipe-1", "upd-1") is None
        assert len(client.api_client.calls) == 20

    def test_the_page_cap_before_the_update_is_none(self):
        pages = [
            {"events": [_flow_progress("upd-9", num_output_rows=1)], "next_page_token": f"p{i}"}
            for i in range(30)
        ]
        runner, client = _metrics_runner(pages)

        assert runner.update_metrics("pipe-1", "upd-1") is None
        assert len(client.api_client.calls) == 20
