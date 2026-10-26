"""JobActionService starts TaskManager tasks and debounce in-flight runs."""

from unittest.mock import MagicMock, patch

import pytest

from back.core.databricks.jobs import TASK_TYPE
from back.core.errors import ConflictError, InfrastructureError, ValidationError
from back.objects.digitaltwin.node.JobActionService import (
    JobActionService,
    _poll_job_task,
)

pytestmark = pytest.mark.unit

_ACTION = {
    "kind": "job",
    "job_id": 42,
    "job_name": "enrich-customer",
    "fullName": "job:42",
}


def _domain():
    domain = MagicMock()
    domain.domain_folder = "cust"
    domain.info = {"name": "cust", "graph_backend": "lakebase"}
    return domain


@pytest.mark.asyncio
async def test_invoke_creates_entity_job_action_task():
    runner = MagicMock()
    runner.run_now.return_value = 99
    tm = MagicMock()
    task = MagicMock()
    task.id = "task-1"
    tm.create_task.return_value = task
    tm.get_active_tasks.return_value = []

    with patch(
        "back.objects.digitaltwin.node.JobActionService.get_task_manager",
        return_value=tm,
    ), patch(
        "back.objects.digitaltwin.node.JobActionService.JobActionRunner.from_domain",
        return_value=runner,
    ), patch(
        "back.objects.digitaltwin.node.JobActionService.run_blocking",
        side_effect=lambda fn, *a, **kw: fn(*a, **kw),
    ), patch(
        "back.objects.digitaltwin.node.JobActionService.threading.Thread"
    ) as thread:
        payload = await JobActionService.invoke(
            _domain(),
            MagicMock(),
            entity_uri="https://example.com/Customer/CUST001",
            action=_ACTION,
            matched_cls={"name": "Customer"},
        )

    assert payload["success"] is True
    assert payload["kind"] == "job"
    assert payload["action"] == "job:42"
    assert payload["task_id"] == "task-1"
    assert payload["run_id"] == 99
    assert payload["message"] == "Job enrich-customer was started"
    tm.create_task.assert_called_once()
    assert tm.create_task.call_args.kwargs["task_type"] == TASK_TYPE
    runner.run_now.assert_called_once()
    assert runner.run_now.call_args.kwargs["entity_id"] == "CUST001"
    thread.assert_called_once()
    assert thread.call_args.kwargs["target"] is _poll_job_task


async def _invoke_with(runner, tm):
    with patch(
        "back.objects.digitaltwin.node.JobActionService.get_task_manager",
        return_value=tm,
    ), patch(
        "back.objects.digitaltwin.node.JobActionService.JobActionRunner.from_domain",
        return_value=runner,
    ), patch(
        "back.objects.digitaltwin.node.JobActionService.run_blocking",
        side_effect=lambda fn, *a, **kw: fn(*a, **kw),
    ), patch("back.objects.digitaltwin.node.JobActionService.threading.Thread"):
        return await JobActionService.invoke(
            _domain(),
            MagicMock(),
            entity_uri="https://example.com/Customer/CUST001",
            action=_ACTION,
            matched_cls={"name": "Customer"},
        )


def _tm_with_task():
    tm = MagicMock()
    task = MagicMock()
    task.id = "task-1"
    tm.create_task.return_value = task
    tm.get_active_tasks.return_value = []
    return tm, task


@pytest.mark.asyncio
async def test_invoke_verifies_the_run_is_running():
    runner = MagicMock()
    runner.run_now.return_value = 99
    runner.run_state.return_value = {
        "life_cycle_state": "RUNNING",
        "result_state": "",
        "message": "",
        "run_page_url": "https://example/run/99",
    }
    tm, task = _tm_with_task()

    payload = await _invoke_with(runner, tm)

    runner.run_state.assert_called_once_with(99)
    assert payload["run_state"] == "RUNNING"
    assert payload["run_page_url"] == "https://example/run/99"
    assert payload["message"] == "Job enrich-customer is running"
    assert task.result["life_cycle_state"] == "RUNNING"
    assert task.result["run_page_url"] == "https://example/run/99"
    assert "running" in tm.start_task.call_args.args[1].lower()


@pytest.mark.asyncio
async def test_invoke_reports_a_queued_run():
    runner = MagicMock()
    runner.run_now.return_value = 99
    runner.run_state.return_value = {
        "life_cycle_state": "PENDING",
        "result_state": "",
        "message": "Waiting for cluster",
        "run_page_url": "https://example/run/99",
    }
    tm, _ = _tm_with_task()

    payload = await _invoke_with(runner, tm)

    assert payload["run_state"] == "PENDING"
    assert payload["message"] == "Job enrich-customer is queued"


@pytest.mark.asyncio
async def test_invoke_still_tracks_the_run_when_the_state_check_fails():
    runner = MagicMock()
    runner.run_now.return_value = 99
    runner.run_state.side_effect = InfrastructureError("Could not read")
    tm, task = _tm_with_task()

    payload = await _invoke_with(runner, tm)

    assert payload["success"] is True
    assert payload["task_id"] == "task-1"
    assert payload["run_state"] == "UNKNOWN"
    assert payload["message"] == "Job enrich-customer was started"
    tm.create_task.assert_called_once()


def test_poll_job_task_records_the_live_state_on_the_task():
    tm = MagicMock()
    task = MagicMock()
    task.result = {"run_id": 99}
    tm.get_task.return_value = task
    runner = MagicMock()

    def wait_for(run_id, on_progress):
        on_progress(50, "RUNNING")
        return {"success": True, "message": "done", "result_state": "SUCCESS"}

    runner.wait_for.side_effect = wait_for
    _poll_job_task(tm, runner, "t1", 42, "CUST001", 99, "job:42", "uri")

    assert task.result["life_cycle_state"] == "RUNNING"
    tm.update_progress.assert_called_once_with("t1", 50, "Databricks run 99: Running")


@pytest.mark.asyncio
async def test_invoke_conflicts_when_same_job_is_already_running():
    tm = MagicMock()
    active = MagicMock()
    active.task_type = TASK_TYPE
    active.result = {"job_id": 42, "entity_id": "CUST001"}
    tm.get_active_tasks.return_value = [active]

    with patch(
        "back.objects.digitaltwin.node.JobActionService.get_task_manager",
        return_value=tm,
    ):
        with pytest.raises(ConflictError, match="already running"):
            await JobActionService.invoke(
                _domain(),
                MagicMock(),
                entity_uri="https://example.com/Customer/CUST001",
                action=_ACTION,
                matched_cls={"name": "Customer"},
            )


@pytest.mark.asyncio
async def test_invoke_rejects_notebook_path_and_invalid_id():
    with pytest.raises(ValidationError, match="job_id"):
        await JobActionService.invoke(
            _domain(),
            MagicMock(),
            entity_uri="https://example.com/Customer/CUST001",
            action={"kind": "job", "job_id": 0},
            matched_cls={"name": "Customer"},
        )
    with pytest.raises(ValidationError, match="Notebook"):
        await JobActionService.invoke(
            _domain(),
            MagicMock(),
            entity_uri="https://example.com/Customer/CUST001",
            action={"kind": "job", "job_id": 7, "notebook_path": "/Users/me/nb"},
            matched_cls={"name": "Customer"},
        )


def test_poll_job_task_completes_on_success():
    tm = MagicMock()
    runner = MagicMock()
    runner.wait_for.return_value = {
        "success": True,
        "message": "done",
        "run_page_url": "https://example/run/99",
        "result_state": "SUCCESS",
        "output": None,
    }
    _poll_job_task(tm, runner, "t1", 42, "CUST001", 99, "job:42", "uri")
    tm.complete_task.assert_called_once()
    assert tm.complete_task.call_args.kwargs["result"]["run_id"] == 99
    tm.fail_task.assert_not_called()


def test_poll_job_task_fails_on_databricks_error():
    tm = MagicMock()
    runner = MagicMock()
    runner.wait_for.return_value = {
        "success": False,
        "message": "spark oops",
        "result_state": "FAILED",
        "run_page_url": "https://example/run/99",
        "output": None,
    }
    _poll_job_task(tm, runner, "t1", 42, "CUST001", 99, "job:42", "uri")
    tm.fail_task.assert_called_once()
    assert tm.fail_task.call_args.args[1] == "spark oops"
    tm.complete_task.assert_not_called()
