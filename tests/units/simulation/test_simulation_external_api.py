"""External /api/v1/simulations router contracts."""

from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.unit


def _gate(monkeypatch):
    from api.routers import simulation as api

    monkeypatch.setattr(api, "assert_public_graph_read", lambda *a, **k: None)
    monkeypatch.setattr(api, "assert_public_graph_write", lambda *a, **k: None)

    async def _inline(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    monkeypatch.setattr(api, "run_blocking", _inline)
    return api


@pytest.mark.asyncio
async def test_list_strips_diff(monkeypatch):
    api = _gate(monkeypatch)
    fake = MagicMock()
    fake.supported = True
    fake.list_all.return_value = [
        {"id": "s1", "label": "A", "status": "ready", "diff": {"added": []}}
    ]
    monkeypatch.setattr(api, "_resolve_sim", lambda *a, **k: fake)
    out = await api.list_simulations(
        MagicMock(),
        domain_q={},
        session_mgr=MagicMock(),
        settings=MagicMock(),
    )
    assert out["success"] is True
    assert out["count"] == 1
    assert "diff" not in out["simulations"][0]


@pytest.mark.asyncio
async def test_create_and_run(monkeypatch):
    api = _gate(monkeypatch)
    fake = MagicMock()
    fake.create.return_value = {"id": "s9", "label": "A", "status": "draft"}
    fake.start_run.return_value = "task-1"
    monkeypatch.setattr(api, "_resolve_sim", lambda *a, **k: fake)
    created = await api.create_simulation(
        MagicMock(),
        body=api.SimulationWriteBody(
            label="A", scope={"type": "entity", "focus": "http://ex/p"}
        ),
        domain_q={},
        session_mgr=MagicMock(),
        settings=MagicMock(),
    )
    assert created["simulation"]["id"] == "s9"
    ran = await api.run_simulation(
        "s9",
        MagicMock(),
        domain_q={},
        session_mgr=MagicMock(),
        settings=MagicMock(),
    )
    assert ran == {"success": True, "task_id": "task-1"}
    fake.start_run.assert_called_once()


def test_router_is_mounted_on_external_app():
    from api.external_app import create_external_api_app

    paths = {getattr(r, "path", "") for r in create_external_api_app().routes}
    assert "/v1/simulations" in paths
    assert "/v1/simulations/{sim_id}/run" in paths
