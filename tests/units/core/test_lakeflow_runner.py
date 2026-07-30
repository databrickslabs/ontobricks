"""Tests for triggering and following the graph-analytics job run.

Everything runs against a fake ``WorkspaceClient`` with an injected ``sleep``,
so the poll loop, the state mapping and the timeout are all exercised without a
workspace or real waiting.
"""

from typing import Any, Dict, List, Optional

import pytest

from back.core.errors import InfrastructureError, NotFoundError
from back.core.graph_analysis.LakeflowRunner import LakeflowRunner

pytestmark = pytest.mark.unit

JOB_NAME = "ontobricks-030-graph-analytics"


class _Settings:
    def __init__(self, name: str) -> None:
        self.name = name


class _Job:
    def __init__(self, job_id: int, name: str) -> None:
        self.job_id = job_id
        self.settings = _Settings(name)


class _State:
    """Mirrors the SDK's run state, whose fields are enums in real life."""

    def __init__(
        self, life_cycle: str, result: str = "", message: str = ""
    ) -> None:
        self.life_cycle_state = life_cycle
        self.result_state = result
        self.state_message = message


class _Run:
    def __init__(self, state: _State, run_page_url: str = "https://example/run/9"):
        self.state = state
        self.run_page_url = run_page_url


class _Started:
    def __init__(self, run_id: int) -> None:
        self.run_id = run_id


class _Jobs:
    def __init__(self, jobs: List[_Job], runs: List[_Run]) -> None:
        self._jobs = jobs
        self._runs = list(runs)
        self.run_now_calls: List[Dict[str, Any]] = []
        self.get_run_calls = 0

    def list(self):
        return iter(self._jobs)

    def run_now(self, **kwargs: Any) -> Any:
        self.run_now_calls.append(kwargs)
        return _Started(77)

    def get_run(self, run_id: int) -> _Run:
        self.get_run_calls += 1
        # Hold the last state once the script runs out.
        idx = min(self.get_run_calls - 1, len(self._runs) - 1)
        return self._runs[idx]


class _Client:
    def __init__(self, jobs: List[_Job], runs: Optional[List[_Run]] = None) -> None:
        self.jobs = _Jobs(jobs, runs or [])


def _runner(client: _Client, *, name: str = JOB_NAME, timeout_s: int = 3600):
    return LakeflowRunner(
        name,
        client_factory=lambda: client,
        sleep=lambda _s: None,
        poll_interval_s=0,
        timeout_s=timeout_s,
    )


class TestResolveJobId:
    def test_exact_name_match(self):
        client = _Client([_Job(1, "other"), _Job(2, JOB_NAME)])
        assert _runner(client).resolve_job_id() == 2

    def test_dev_mode_prefix_is_matched(self):
        # A bundle deployed with mode: development renames the job.
        client = _Client([_Job(5, f"[dev benoit_cayla] {JOB_NAME}")])
        assert _runner(client).resolve_job_id() == 5

    def test_exact_match_wins_over_prefixed(self):
        client = _Client(
            [_Job(5, f"[dev someone] {JOB_NAME}"), _Job(6, JOB_NAME)]
        )
        assert _runner(client).resolve_job_id() == 6

    def test_partial_name_does_not_match(self):
        # A job merely containing the name must not be picked up.
        client = _Client([_Job(9, f"{JOB_NAME}-staging")])
        with pytest.raises(NotFoundError):
            _runner(client).resolve_job_id()

    def test_missing_job_names_the_fix(self):
        client = _Client([_Job(1, "unrelated")])
        with pytest.raises(NotFoundError) as exc:
            _runner(client).resolve_job_id()
        assert "make deploy" in str(exc.value)

    def test_empty_configured_name_is_rejected(self):
        with pytest.raises(NotFoundError):
            _runner(_Client([]), name="").resolve_job_id()

    def test_result_is_cached(self):
        client = _Client([_Job(2, JOB_NAME)])
        runner = _runner(client)
        assert runner.resolve_job_id() == runner.resolve_job_id() == 2

    def test_jobs_without_settings_are_skipped(self):
        broken = _Job(1, "")
        broken.settings = None
        client = _Client([broken, _Job(2, JOB_NAME)])
        assert _runner(client).resolve_job_id() == 2

    def test_sdk_failure_becomes_infrastructure_error(self):
        class _Broken(_Client):
            def __init__(self):
                super().__init__([])
                self.jobs.list = lambda: (_ for _ in ()).throw(RuntimeError("403"))

        with pytest.raises(InfrastructureError):
            _runner(_Broken()).resolve_job_id()

    def test_client_construction_failure_is_wrapped(self):
        runner = LakeflowRunner(
            JOB_NAME,
            client_factory=lambda: (_ for _ in ()).throw(RuntimeError("no creds")),
        )
        with pytest.raises(InfrastructureError):
            runner.resolve_job_id()


class TestSubmit:
    def test_parameters_are_passed_as_strings(self):
        client = _Client([_Job(2, JOB_NAME)])
        run_id = _runner(client).submit(
            source_table="main.onto.g",
            output_table="main.onto.m",
            exclude_predicates=["http://a", "http://b"],
            pagerank_iterations=30,
        )
        assert run_id == 77
        params = client.jobs.run_now_calls[0]["job_parameters"]
        assert params["source_table"] == "main.onto.g"
        assert params["exclude_predicates"] == "http://a,http://b"
        # Job parameters must be strings, not ints.
        assert params["pagerank_iterations"] == "30"

    def test_no_excluded_predicates_sends_empty_string(self):
        client = _Client([_Job(2, JOB_NAME)])
        _runner(client).submit(source_table="a.b.c", output_table="a.b.d")
        assert client.jobs.run_now_calls[0]["job_parameters"]["exclude_predicates"] == ""

    def test_run_now_failure_is_wrapped(self):
        client = _Client([_Job(2, JOB_NAME)])
        client.jobs.run_now = lambda **kw: (_ for _ in ()).throw(RuntimeError("quota"))
        with pytest.raises(InfrastructureError):
            _runner(client).submit(source_table="a.b.c", output_table="a.b.d")

    def test_missing_run_id_is_reported(self):
        client = _Client([_Job(2, JOB_NAME)])
        client.jobs.run_now = lambda **kw: object()
        with pytest.raises(InfrastructureError) as exc:
            _runner(client).submit(source_table="a.b.c", output_table="a.b.d")
        assert "no run id" in str(exc.value)

    def test_waiter_shaped_response_is_unwrapped(self):
        # Some SDK versions return a waiter wrapping the response.
        class _Waiter:
            def __init__(self):
                self.response = _Started(101)

        client = _Client([_Job(2, JOB_NAME)])
        client.jobs.run_now = lambda **kw: _Waiter()
        assert _runner(client).submit(
            source_table="a.b.c", output_table="a.b.d"
        ) == 101


class TestWaitFor:
    def test_successful_run(self):
        client = _Client(
            [_Job(2, JOB_NAME)],
            runs=[
                _Run(_State("PENDING")),
                _Run(_State("RUNNING")),
                _Run(_State("TERMINATED", "SUCCESS")),
            ],
        )
        out = _runner(client).wait_for(77)
        assert out["success"] is True
        assert out["result_state"] == "SUCCESS"
        assert out["run_page_url"] == "https://example/run/9"

    def test_failed_run_is_not_success(self):
        client = _Client(
            [_Job(2, JOB_NAME)],
            runs=[_Run(_State("TERMINATED", "FAILED", "task died"))],
        )
        out = _runner(client).wait_for(77)
        assert out["success"] is False
        assert out["message"] == "task died"

    def test_internal_error_is_terminal(self):
        client = _Client([_Job(2, JOB_NAME)], runs=[_Run(_State("INTERNAL_ERROR"))])
        out = _runner(client).wait_for(77)
        assert out["success"] is False
        assert out["life_cycle_state"] == "INTERNAL_ERROR"

    def test_progress_reported_once_per_state_change(self):
        client = _Client(
            [_Job(2, JOB_NAME)],
            runs=[
                _Run(_State("PENDING")),
                _Run(_State("PENDING")),
                _Run(_State("RUNNING")),
                _Run(_State("RUNNING")),
                _Run(_State("TERMINATED", "SUCCESS")),
            ],
        )
        seen: List[tuple] = []
        _runner(client).wait_for(77, on_progress=lambda p, m: seen.append((p, m)))
        # PENDING, RUNNING, TERMINATED — repeats must not re-notify.
        assert len(seen) == 3
        assert [p for p, _ in seen] == sorted(p for p, _ in seen)

    def test_progress_is_monotonic_and_bounded(self):
        client = _Client(
            [_Job(2, JOB_NAME)],
            runs=[
                _Run(_State("QUEUED")),
                _Run(_State("RUNNING")),
                _Run(_State("TERMINATING")),
                _Run(_State("TERMINATED", "SUCCESS")),
            ],
        )
        seen: List[int] = []
        _runner(client).wait_for(77, on_progress=lambda p, m: seen.append(p))
        assert all(0 < p < 100 for p in seen)

    def test_timeout_raises_and_names_the_run_url(self):
        client = _Client([_Job(2, JOB_NAME)], runs=[_Run(_State("RUNNING"))])
        with pytest.raises(InfrastructureError) as exc:
            _runner(client, timeout_s=1).wait_for(77)
        # Timeout must not imply failure — the run may still be going.
        assert "may still be running" in str(exc.value)

    def test_get_run_failure_is_wrapped(self):
        client = _Client([_Job(2, JOB_NAME)])
        client.jobs.get_run = lambda run_id: (_ for _ in ()).throw(RuntimeError("500"))
        with pytest.raises(InfrastructureError):
            _runner(client).wait_for(77)

    def test_enum_shaped_states_are_normalised(self):
        class _Enum:
            def __init__(self, value):
                self.value = value

            def __str__(self):
                return f"RunLifeCycleState.{self.value}"

        client = _Client(
            [_Job(2, JOB_NAME)],
            runs=[_Run(_State(_Enum("TERMINATED"), _Enum("SUCCESS")))],
        )
        out = _runner(client).wait_for(77)
        assert out["life_cycle_state"] == "TERMINATED"
        assert out["success"] is True


class TestRunAndWait:
    def test_submits_then_polls(self):
        client = _Client(
            [_Job(2, JOB_NAME)],
            runs=[_Run(_State("TERMINATED", "SUCCESS"))],
        )
        out = _runner(client).run_and_wait(
            source_table="main.onto.g", output_table="main.onto.m"
        )
        assert out["success"] is True
        assert out["run_id"] == 77
        assert client.jobs.run_now_calls
