"""JobActionRunner list / run_now / wait_for against a fake WorkspaceClient."""

from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from back.core.databricks.jobs.JobActionRunner import (
    JobActionRunner,
    class_job_entry,
    is_lakehouse_domain,
    job_declares_entity_id,
    parse_job_action_id,
)
from back.core.errors import AuthorizationError, InfrastructureError

pytestmark = pytest.mark.unit


class _Param:
    def __init__(self, name: str) -> None:
        self.name = name


class _Settings:
    def __init__(self, name: str, parameters=None, tasks=None) -> None:
        self.name = name
        self.parameters = parameters or []
        self.tasks = tasks or []


class _Job:
    def __init__(self, job_id: int, name: str, *, entity_id: bool = True) -> None:
        params = [_Param("entity_id")] if entity_id else [_Param("other")]
        self.job_id = job_id
        self.settings = _Settings(name, parameters=params)


class _State:
    def __init__(self, life_cycle: str, result: str = "", message: str = "") -> None:
        self.life_cycle_state = life_cycle
        self.result_state = result
        self.state_message = message


class _Notebook:
    def __init__(self, result: str) -> None:
        self.result = result


class _Task:
    def __init__(self, result: str = "") -> None:
        self.notebook_output = _Notebook(result) if result else None


class _Run:
    def __init__(
        self,
        state: _State,
        run_page_url: str = "https://example/run/9",
        output: str = "",
    ):
        self.state = state
        self.run_page_url = run_page_url
        self.tasks = [_Task(output)] if output else []


class _Started:
    def __init__(self, run_id: int) -> None:
        self.run_id = run_id


class _Jobs:
    def __init__(self, jobs: List[_Job], runs: List[_Run]) -> None:
        self._jobs = jobs
        self._runs = list(runs)
        self.run_now_calls: List[Dict[str, Any]] = []
        self.get_run_calls = 0
        self.list_kwargs: Dict[str, Any] = {}

    def list(self, **kwargs: Any):
        self.list_kwargs = kwargs
        return iter(self._jobs)

    def run_now(self, **kwargs: Any) -> Any:
        self.run_now_calls.append(kwargs)
        return _Started(77)

    def get_run(self, run_id: int) -> _Run:
        self.get_run_calls += 1
        idx = min(self.get_run_calls - 1, len(self._runs) - 1)
        return self._runs[idx]


class _Client:
    def __init__(self, jobs: List[_Job], runs: Optional[List[_Run]] = None) -> None:
        self.jobs = _Jobs(jobs, runs or [])


def _runner(client: _Client, *, timeout_s: int = 3600) -> JobActionRunner:
    return JobActionRunner(
        client_factory=lambda: client,
        sleep=lambda _s: None,
        poll_interval_s=0,
        timeout_s=timeout_s,
    )


def test_parse_job_action_id():
    assert parse_job_action_id("job:12") == 12
    assert parse_job_action_id(12) == 12
    assert parse_job_action_id("0") is None
    assert parse_job_action_id("/Users/me/nb") is None


def test_list_entity_jobs_filters_entity_id_param():
    client = _Client(
        [_Job(1, "skip-me", entity_id=False), _Job(2, "keep-me", entity_id=True)]
    )
    jobs = _runner(client).list_entity_jobs()
    assert jobs == [
        {"job_id": 2, "name": "keep-me", "fullName": "job:2"},
    ]
    assert client.jobs.list_kwargs.get("expand_tasks") is True


def test_job_declares_entity_id_from_spark_python_task_args():
    """jobs.list omits settings.parameters; expand_tasks returns Spark args."""
    spark = MagicMock()
    spark.parameters = [
        "--entity-id",
        "{{job.parameters.entity_id}}",
        "--contract-table",
        "{{job.parameters.contract_table}}",
    ]
    task = MagicMock()
    task.notebook_task = None
    task.spark_python_task = spark
    job = MagicMock()
    job.settings.parameters = []
    job.settings.tasks = [task]
    assert job_declares_entity_id(job) is True


def test_job_declares_entity_id_false_when_spark_args_have_no_entity_id():
    spark = MagicMock()
    spark.parameters = ["--contract-table", "cat.sch.t"]
    task = MagicMock()
    task.notebook_task = None
    task.spark_python_task = spark
    job = MagicMock()
    job.settings.parameters = []
    job.settings.tasks = [task]
    assert job_declares_entity_id(job) is False


def test_run_now_sends_entity_id():
    client = _Client([_Job(2, "keep-me")])
    run_id = _runner(client).run_now(
        2, entity_id="CUST1", entity_uri="u", class_name="Customer", domain="d"
    )
    assert run_id == 77
    params = client.jobs.run_now_calls[0]["job_parameters"]
    assert params["entity_id"] == "CUST1"
    assert params["class_name"] == "Customer"


def test_wait_for_success_and_notebook_exit():
    client = _Client(
        [],
        runs=[
            _Run(_State("RUNNING")),
            _Run(_State("TERMINATED", "SUCCESS"), output='{"ok": true}'),
        ],
    )
    out = _runner(client).wait_for(77)
    assert out["success"] is True
    assert out["output"] == '{"ok": true}'
    assert out["run_page_url"] == "https://example/run/9"


def test_wait_for_timeout():
    client = _Client([], runs=[_Run(_State("RUNNING"))])
    with pytest.raises(InfrastructureError, match="did not finish"):
        _runner(client, timeout_s=1).wait_for(77)


def test_lakehouse_app_mode_without_token_is_fail_closed():
    domain = MagicMock()
    domain.info = {"graph_backend": "databricks"}
    settings = MagicMock()
    settings.databricks_host = "https://example.cloud.databricks.com"
    with patch("back.core.databricks.is_databricks_app", return_value=True):
        with pytest.raises(AuthorizationError):
            JobActionRunner.workspace_client(domain, settings, user_token="")


def test_lakebase_uses_control_plane_client():
    domain = MagicMock()
    domain.info = {"graph_backend": "lakebase"}
    settings = MagicMock()
    dbc = MagicMock()
    dbc.host = "https://example.cloud.databricks.com"
    dbc.token = "pat"
    fake_ws = object()
    with patch("back.core.databricks.is_databricks_app", return_value=True), patch(
        "back.core.helpers.get_databricks_client", return_value=dbc
    ), patch(
        "databricks.sdk.WorkspaceClient", return_value=fake_ws
    ) as ws:
        client = JobActionRunner.workspace_client(domain, settings, user_token="")
    assert client is fake_ws
    ws.assert_called_once()


def test_wait_for_failed_run_is_not_success():
    client = _Client(
        [],
        runs=[_Run(_State("TERMINATED", "FAILED", "spark oops"))],
    )
    out = _runner(client).wait_for(77)
    assert out["success"] is False
    assert out["result_state"] == "FAILED"
    assert out["message"] == "spark oops"


def test_run_state_reads_the_run_once():
    client = _Client(
        [], runs=[_Run(_State("RUNNING"), run_page_url="https://example/run/77")]
    )
    state = _runner(client).run_state(77)
    assert state == {
        "life_cycle_state": "RUNNING",
        "result_state": "",
        "message": "",
        "run_page_url": "https://example/run/77",
    }
    assert client.jobs.get_run_calls == 1


def test_run_state_wraps_sdk_errors():
    client = _Client([])

    def boom(**_kwargs):
        raise RuntimeError("network down")

    client.jobs.get_run = boom
    with pytest.raises(InfrastructureError, match="state of job run 77"):
        _runner(client).run_state(77)


def test_run_now_without_run_id_raises():
    client = _Client([_Job(2, "keep-me")])
    client.jobs.run_now = lambda **_kwargs: object()
    with pytest.raises(InfrastructureError, match="no run id"):
        _runner(client).run_now(2, entity_id="CUST1")


def test_class_job_entry_normalizes_and_rejects():
    assert class_job_entry({"kind": "job", "job_id": 5, "job_name": "n"})[
        "fullName"
    ] == "job:5"
    assert class_job_entry({"fullName": "a.b.c"}) is None
    assert class_job_entry({"kind": "job", "job_id": 3, "notebook_path": "/nb"}) is None
    assert class_job_entry("job:1") is None


def test_is_lakehouse_domain():
    lakehouse = MagicMock()
    lakehouse.info = {"graph_backend": "databricks"}
    lakebase = MagicMock()
    lakebase.info = {"graph_backend": "lakebase"}
    assert is_lakehouse_domain(lakehouse) is True
    assert is_lakehouse_domain(lakebase) is False


def test_lakehouse_app_mode_uses_forwarded_token():
    domain = MagicMock()
    domain.info = {"graph_backend": "databricks"}
    settings = MagicMock()
    settings.databricks_host = "https://example.cloud.databricks.com"
    fake_ws = object()
    with patch("back.core.databricks.is_databricks_app", return_value=True), patch(
        "databricks.sdk.WorkspaceClient", return_value=fake_ws
    ) as ws:
        client = JobActionRunner.workspace_client(
            domain, settings, user_token=" user-obo "
        )
    assert client is fake_ws
    ws.assert_called_once_with(
        host="https://example.cloud.databricks.com", token="user-obo"
    )


def test_job_declares_entity_id_from_notebook_base_parameters():
    notebook = MagicMock()
    notebook.base_parameters = {"entity_id": ""}
    task = MagicMock()
    task.notebook_task = notebook
    job = MagicMock()
    job.settings.parameters = []
    job.settings.tasks = [task]
    assert job_declares_entity_id(job) is True
