"""POST /api/v1/digitaltwin/nodes/action for kind=job."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from shared.fastapi.main import app

pytestmark = pytest.mark.unit


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=False)


_JOB = {
    "kind": "job",
    "job_id": 42,
    "job_name": "enrich-customer",
    "description": "DQ ticket",
}

_CLASSES = [{"name": "Customer", "uri": "https://example.com/Customer", "actions": [_JOB]}]


def _domain():
    domain = MagicMock()
    domain.get_classes.return_value = _CLASSES
    domain.domain_folder = "cust"
    domain.info = {"name": "cust", "graph_backend": "lakebase"}
    return domain


def test_job_invoke_returns_task_id(client):
    runner = MagicMock()
    runner.run_now.return_value = 99
    runner.run_state.return_value = {
        "life_cycle_state": "RUNNING",
        "result_state": "",
        "message": "",
        "run_page_url": "https://example/run/99",
    }
    runner.wait_for.return_value = {
        "success": True,
        "result_state": "SUCCESS",
        "run_page_url": "https://example/run/99",
        "message": "ok",
        "output": None,
    }
    with patch(
        "api.routers.digitaltwin.DigitalTwin.resolve_domain", return_value=_domain()
    ), patch(
        "back.objects.digitaltwin.node.JobActionService.JobActionRunner.from_domain",
        return_value=runner,
    ), patch(
        "back.objects.digitaltwin.node.JobActionService.run_blocking",
        side_effect=lambda fn, *a, **kw: fn(*a, **kw),
    ):
        resp = client.post(
            "/api/v1/digitaltwin/nodes/action",
            json={
                "entity_uri": "https://example.com/Customer/CUST001",
                "action_full_name": "job:42",
                "domain_name": "test",
            },
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["success"] is True
    assert body["kind"] == "job"
    assert body["action"] == "job:42"
    assert body["task_id"]
    assert body["run_id"] == 99
    assert body["run_state"] == "RUNNING"
    assert body["run_page_url"] == "https://example/run/99"
    runner.run_now.assert_called_once()


def test_unknown_job_is_rejected(client):
    with patch(
        "api.routers.digitaltwin.DigitalTwin.resolve_domain", return_value=_domain()
    ):
        resp = client.post(
            "/api/v1/digitaltwin/nodes/action",
            json={
                "entity_uri": "https://example.com/Customer/CUST001",
                "action_full_name": "job:999",
                "domain_name": "test",
            },
        )
    assert resp.status_code == 400


def test_duplicate_running_job_returns_conflict(client):
    active = MagicMock()
    active.task_type = "entity_job_action"
    active.result = {"job_id": 42, "entity_id": "CUST001"}
    tm = MagicMock()
    tm.get_active_tasks.return_value = [active]
    with patch(
        "api.routers.digitaltwin.DigitalTwin.resolve_domain", return_value=_domain()
    ), patch(
        "back.objects.digitaltwin.node.JobActionService.get_task_manager",
        return_value=tm,
    ):
        resp = client.post(
            "/api/v1/digitaltwin/nodes/action",
            json={
                "entity_uri": "https://example.com/Customer/CUST001",
                "action_full_name": "job:42",
                "domain_name": "test",
            },
        )
    assert resp.status_code == 409


def test_context_lists_job_actions(client):
    with patch(
        "api.routers.digitaltwin.DigitalTwin.resolve_domain", return_value=_domain()
    ):
        resp = client.get(
            "/api/v1/digitaltwin/nodes/context",
            params={
                "entity_uri": "https://example.com/Customer/CUST001",
                "domain_name": "test",
            },
        )
    assert resp.status_code == 200, resp.text
    actions = resp.json()["actions"]
    assert actions[0]["kind"] == "job"
    assert actions[0]["fullName"] == "job:42"
    assert actions[0]["job_id"] == 42
    assert actions[0]["job_name"] == "enrich-customer"


@pytest.mark.asyncio
async def test_disabled_actions_policy_blocks_jobs():
    from back.core.errors import ValidationError
    from back.objects.digitaltwin.node.NodeContextService import NodeContextService

    with pytest.raises(ValidationError, match="Actions are disabled"):
        await NodeContextService.invoke_action(
            _domain(),
            MagicMock(),
            entity_uri="https://example.com/Customer/CUST001",
            action_full_name="job:42",
            context_policy={"actions": "disabled"},
        )
