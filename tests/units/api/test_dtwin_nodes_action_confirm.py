"""Tests for the session-aware Action confirmation flow.

``POST /dtwin/nodes/action/request`` mints a one-time pending token without
invoking anything; ``POST /dtwin/nodes/action/confirm`` consumes it and
invokes exactly once; ``POST /dtwin/nodes/action/cancel`` discards it.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from shared.fastapi.main import app
from tests.units.api.test_node_context_endpoint import _CLASSES_WITH_ACTIONS


pytestmark = pytest.mark.unit

_ENTITY_URI = "https://example.com/Customer/CUST001"
_ACTION = "main.ops.recompute_risk"


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


def test_request_mints_token_without_invoke(client, domain, monkeypatch):
    from api.routers.internal import dtwin

    invoke_action = AsyncMock(side_effect=AssertionError("invoke_action must not be called"))
    monkeypatch.setattr(dtwin, "get_domain", lambda _session_mgr: domain)
    monkeypatch.setattr(dtwin.NodeContextService, "invoke_action", invoke_action)

    resp = client.post(
        "/dtwin/nodes/action/request",
        json={"entity_uri": _ENTITY_URI, "action_full_name": _ACTION},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    pending = body["pending_action"]
    assert pending["token"]
    assert pending["entity_uri"] == _ENTITY_URI
    assert pending["entity_label"] == "CUST001"
    assert pending["action"] == _ACTION
    assert pending["description"] == "Recompute the customer risk score"
    assert pending["expires_in_sec"] == 120
    invoke_action.assert_not_called()


def test_confirm_invokes_once_then_rejects_reuse(client, domain, monkeypatch):
    from api.routers.internal import dtwin

    invoke_action = AsyncMock(
        return_value={
            "success": True,
            "entity_uri": _ENTITY_URI,
            "entity_local_id": "CUST001",
            "class_name": "Customer",
            "action": _ACTION,
            "returns_table": False,
            "rows": [{"result": 42}],
        }
    )
    monkeypatch.setattr(dtwin, "get_domain", lambda _session_mgr: domain)
    monkeypatch.setattr(dtwin.NodeContextService, "invoke_action", invoke_action)

    req = client.post(
        "/dtwin/nodes/action/request",
        json={"entity_uri": _ENTITY_URI, "action_full_name": _ACTION},
    )
    token = req.json()["pending_action"]["token"]

    confirm1 = client.post("/dtwin/nodes/action/confirm", json={"token": token})
    assert confirm1.status_code == 200
    body = confirm1.json()
    assert body["success"] is True
    assert body["action"] == _ACTION
    assert body["rows"] == [{"result": 42}]
    invoke_action.assert_called_once()
    assert invoke_action.call_args.kwargs["entity_uri"] == _ENTITY_URI
    assert invoke_action.call_args.kwargs["action_full_name"] == _ACTION

    confirm2 = client.post("/dtwin/nodes/action/confirm", json={"token": token})
    assert confirm2.status_code in (400, 422)
    invoke_action.assert_called_once()  # still only once — no double-invoke


def test_request_rejects_unknown_action(client, domain, monkeypatch):
    from api.routers.internal import dtwin

    monkeypatch.setattr(dtwin, "get_domain", lambda _session_mgr: domain)

    resp = client.post(
        "/dtwin/nodes/action/request",
        json={"entity_uri": _ENTITY_URI, "action_full_name": "evil.fn"},
    )

    assert resp.status_code in (400, 422)


def test_request_rejects_unmatched_entity(client, domain, monkeypatch):
    from api.routers.internal import dtwin

    monkeypatch.setattr(dtwin, "get_domain", lambda _session_mgr: domain)

    resp = client.post(
        "/dtwin/nodes/action/request",
        json={
            "entity_uri": "https://example.com/Invoice/INV001",
            "action_full_name": _ACTION,
        },
    )

    assert resp.status_code in (400, 422)


def test_confirm_rejects_missing_token(client, domain, monkeypatch):
    from api.routers.internal import dtwin

    monkeypatch.setattr(dtwin, "get_domain", lambda _session_mgr: domain)

    resp = client.post("/dtwin/nodes/action/confirm", json={})
    assert resp.status_code in (400, 422)


def test_confirm_rejects_unknown_token(client, domain, monkeypatch):
    from api.routers.internal import dtwin

    monkeypatch.setattr(dtwin, "get_domain", lambda _session_mgr: domain)

    resp = client.post("/dtwin/nodes/action/confirm", json={"token": "not-a-real-token"})
    assert resp.status_code in (400, 422)


def test_confirm_rejects_expired_token(client, domain, monkeypatch):
    from api.routers.internal import dtwin

    monkeypatch.setattr(dtwin, "get_domain", lambda _session_mgr: domain)
    invoke_action = AsyncMock(side_effect=AssertionError("invoke_action must not be called"))
    monkeypatch.setattr(dtwin.NodeContextService, "invoke_action", invoke_action)

    req = client.post(
        "/dtwin/nodes/action/request",
        json={"entity_uri": _ENTITY_URI, "action_full_name": _ACTION},
    )
    token = req.json()["pending_action"]["token"]

    # Fast-forward past the TTL without waiting in real time.
    real_time = dtwin.time.time
    monkeypatch.setattr(dtwin.time, "time", lambda: real_time() + 121)

    resp = client.post("/dtwin/nodes/action/confirm", json={"token": token})
    assert resp.status_code in (400, 422)
    invoke_action.assert_not_called()


def test_confirm_rejects_domain_mismatch(client, domain, monkeypatch):
    from api.routers.internal import dtwin

    other_domain = MagicMock()
    other_domain.info = {"name": "Other Domain"}
    other_domain.domain_folder = "other-domain"
    other_domain.get_classes.return_value = _CLASSES_WITH_ACTIONS

    domains = [domain, other_domain]
    monkeypatch.setattr(dtwin, "get_domain", lambda _session_mgr: domains.pop(0))
    invoke_action = AsyncMock(side_effect=AssertionError("invoke_action must not be called"))
    monkeypatch.setattr(dtwin.NodeContextService, "invoke_action", invoke_action)

    req = client.post(
        "/dtwin/nodes/action/request",
        json={"entity_uri": _ENTITY_URI, "action_full_name": _ACTION},
    )
    token = req.json()["pending_action"]["token"]

    resp = client.post("/dtwin/nodes/action/confirm", json={"token": token})
    assert resp.status_code in (400, 422)
    invoke_action.assert_not_called()


def test_cancel_discards_pending_token(client, domain, monkeypatch):
    from api.routers.internal import dtwin

    monkeypatch.setattr(dtwin, "get_domain", lambda _session_mgr: domain)
    invoke_action = AsyncMock(side_effect=AssertionError("invoke_action must not be called"))
    monkeypatch.setattr(dtwin.NodeContextService, "invoke_action", invoke_action)

    req = client.post(
        "/dtwin/nodes/action/request",
        json={"entity_uri": _ENTITY_URI, "action_full_name": _ACTION},
    )
    token = req.json()["pending_action"]["token"]

    cancel_resp = client.post("/dtwin/nodes/action/cancel", json={"token": token})
    assert cancel_resp.status_code == 200
    assert cancel_resp.json()["success"] is True

    confirm_resp = client.post("/dtwin/nodes/action/confirm", json={"token": token})
    assert confirm_resp.status_code in (400, 422)
    invoke_action.assert_not_called()


def test_cancel_is_always_200_even_without_token(client, domain, monkeypatch):
    from api.routers.internal import dtwin

    monkeypatch.setattr(dtwin, "get_domain", lambda _session_mgr: domain)

    resp = client.post("/dtwin/nodes/action/cancel", json={})
    assert resp.status_code == 200
    assert resp.json()["success"] is True
