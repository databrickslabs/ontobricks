"""Tests for session-aware Graph Chat node-context routes."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from shared.fastapi.main import app
from tests.units.api.test_node_context_endpoint import _CLASSES_WITH_ACTIONS


pytestmark = pytest.mark.unit


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def domain():
    mock_domain = MagicMock()
    mock_domain.info = {"name": "Customer 360"}
    mock_domain.domain_folder = "customer-360"
    mock_domain.get_classes.return_value = _CLASSES_WITH_ACTIONS
    return mock_domain


def test_dtwin_classes_returns_session_actions(client, domain, monkeypatch):
    from api.routers.internal import dtwin

    monkeypatch.setattr(dtwin, "get_domain", lambda _session_mgr: domain)

    response = client.get("/dtwin/classes")

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["domain_name"] == "Customer 360"
    assert body["classes"][0]["dataset"]["fullName"] == "main.crm.customers"
    assert body["classes"][0]["actions"][0]["fullName"] == "main.ops.recompute_risk"


def test_dtwin_nodes_context_returns_metadata_without_rows(client, domain, monkeypatch):
    from api.routers.internal import dtwin

    monkeypatch.setattr(dtwin, "get_domain", lambda _session_mgr: domain)

    response = client.get(
        "/dtwin/nodes/context",
        params={"entity_uri": "https://example.com/Customer/CUST001"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["class_name"] == "Customer"
    assert "rows" not in (body.get("dataset") or {})
    assert body["actions"][0]["function"] == "recompute_risk"


def test_dtwin_nodes_context_clamps_limits(client, domain, monkeypatch):
    from api.routers.internal import dtwin

    resolve_context = AsyncMock(return_value={"success": True})
    monkeypatch.setattr(dtwin, "get_domain", lambda _session_mgr: domain)
    monkeypatch.setattr(dtwin.NodeContextService, "resolve_context", resolve_context)

    response = client.get(
        "/dtwin/nodes/context",
        params={
            "entity_uri": "https://example.com/Customer/CUST001",
            "dataset_row_limit": "99",
            "bridge_depth": "4",
        },
    )

    assert response.status_code == 200
    assert resolve_context.await_args.kwargs["dataset_row_limit"] == 20
    assert resolve_context.await_args.kwargs["bridge_depth"] == 1
