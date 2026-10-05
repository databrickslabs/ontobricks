"""Routes for triggering an entity business rule (SWRL) from the Explorer and MCP.

Internal: ``/dtwin/nodes/business-rule/request`` mints a one-time token,
``/confirm`` consumes it and runs the rule once, ``/cancel`` discards it.
Public: ``POST /api/v1/digitaltwin/nodes/business-rule`` (MCP), Builder-gated.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from back.core.errors import AuthorizationError
from shared.fastapi.main import app

pytestmark = pytest.mark.unit

_ENTITY = "https://example.com/Customer/CUST001"
_RULE = {
    "name": "VipCustomer",
    "description": "Customers with an order become VIP",
    "antecedent": "Customer(?c) ^ hasOrder(?c, ?o)",
    "consequent": "VIP(?c)",
}
_CLASSES = [
    {
        "name": "Customer",
        "uri": "https://example.com/Customer",
        "business_rules": [{"name": "VipCustomer"}],
        "actions": [
            {"fullName": "main.ops.recompute_risk", "function": "recompute_risk"}
        ],
    }
]
_RESULT = {
    "success": True,
    "entity_uri": _ENTITY,
    "entity_local_id": "CUST001",
    "class_name": "Customer",
    "rule": "VipCustomer",
    "inferred_count": 1,
    "materialized_count": 1,
    "triples": [{"subject": _ENTITY, "predicate": "p", "object": "o"}],
    "truncated": False,
}


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def domain():
    d = MagicMock()
    d.info = {"name": "Customer 360"}
    d.domain_folder = "customer-360"
    d.swrl_rules = [_RULE]
    d.get_classes.return_value = _CLASSES
    return d


@pytest.fixture
def wired(domain, monkeypatch):
    from api.routers.internal import dtwin

    run = AsyncMock(return_value=_RESULT)
    monkeypatch.setattr(dtwin, "get_domain", lambda _sm: domain)
    monkeypatch.setattr(dtwin.NodeContextService, "run_business_rule", run)
    return run


def test_request_mints_token_without_running(client, wired):
    resp = client.post(
        "/dtwin/nodes/business-rule/request",
        json={"entity_uri": _ENTITY, "rule": "VipCustomer"},
    )
    assert resp.status_code == 200
    pending = resp.json()["pending_business_rule"]
    assert pending["token"]
    assert pending["rule"] == "VipCustomer"
    assert pending["entity_label"] == "CUST001"
    assert pending["consequent"] == "VIP(?c)"
    wired.assert_not_called()


def test_confirm_runs_once_then_rejects_reuse(client, wired):
    token = client.post(
        "/dtwin/nodes/business-rule/request",
        json={"entity_uri": _ENTITY, "rule": "VipCustomer"},
    ).json()["pending_business_rule"]["token"]

    first = client.post("/dtwin/nodes/business-rule/confirm", json={"token": token})
    assert first.status_code == 200
    assert first.json()["materialized_count"] == 1
    wired.assert_called_once()
    assert wired.call_args.kwargs["rule_name"] == "VipCustomer"
    assert wired.call_args.kwargs["entity_uri"] == _ENTITY

    second = client.post("/dtwin/nodes/business-rule/confirm", json={"token": token})
    assert second.status_code == 400
    wired.assert_called_once()


def test_request_rejects_rule_not_on_class(client, wired):
    resp = client.post(
        "/dtwin/nodes/business-rule/request",
        json={"entity_uri": _ENTITY, "rule": "Other"},
    )
    assert resp.status_code == 400


def test_business_rule_token_cannot_confirm_an_action(client, wired, monkeypatch):
    from api.routers.internal import dtwin

    invoke = AsyncMock(side_effect=AssertionError("must not invoke"))
    monkeypatch.setattr(dtwin.NodeContextService, "invoke_action", invoke)
    token = client.post(
        "/dtwin/nodes/business-rule/request",
        json={"entity_uri": _ENTITY, "rule": "VipCustomer"},
    ).json()["pending_business_rule"]["token"]

    resp = client.post("/dtwin/nodes/action/confirm", json={"token": token})
    assert resp.status_code == 400
    invoke.assert_not_called()


def test_action_token_cannot_confirm_a_business_rule(client, wired):
    token = client.post(
        "/dtwin/nodes/action/request",
        json={"entity_uri": _ENTITY, "action_full_name": "main.ops.recompute_risk"},
    ).json()["pending_action"]["token"]

    resp = client.post("/dtwin/nodes/business-rule/confirm", json={"token": token})
    assert resp.status_code == 400
    wired.assert_not_called()


def test_cancel_discards_token(client, wired):
    token = client.post(
        "/dtwin/nodes/business-rule/request",
        json={"entity_uri": _ENTITY, "rule": "VipCustomer"},
    ).json()["pending_business_rule"]["token"]
    assert client.post("/dtwin/nodes/business-rule/cancel", json={"token": token}).status_code == 200
    resp = client.post("/dtwin/nodes/business-rule/confirm", json={"token": token})
    assert resp.status_code == 400


@pytest.mark.parametrize(
    "path", ["/dtwin/nodes/business-rule/request", "/dtwin/nodes/business-rule/confirm"]
)
def test_internal_routes_require_builder(path):
    from api.routers.internal import dtwin

    route = next(r for r in dtwin.router.routes if getattr(r, "path", "") == path)
    names = [d.call.__name__ for d in route.dependant.dependencies]
    assert "require_domain_builder" in names


# ---------------------------------------------------------------------------
# Public route + Builder gate


def _patch_public(monkeypatch, domain, *, role):
    from api.routers import digitaltwin as dt

    monkeypatch.setattr(
        "api.routers.internal._graph_access.is_databricks_app", lambda: True
    )
    monkeypatch.setattr(
        "back.objects.domain.SettingsService.resolve_domain_role",
        lambda request, folder, settings, **kw: role,
    )
    monkeypatch.setattr(
        dt.DigitalTwin, "resolve_domain", classmethod(lambda cls, *a, **k: domain)
    )
    run = AsyncMock(return_value=_RESULT)
    monkeypatch.setattr(dt.NodeContextService, "run_business_rule", run)
    return dt, run


def _pub_req():
    return SimpleNamespace(
        state=SimpleNamespace(user_role="", user_domain_role="", user_email="u@x.com")
    )


async def test_public_route_blocks_viewer(monkeypatch, domain):
    dt, run = _patch_public(monkeypatch, domain, role="viewer")
    with pytest.raises(AuthorizationError):
        await dt.dt_nodes_business_rule(
            payload=dt.NodeBusinessRuleRequest(entity_uri=_ENTITY, rule="VipCustomer"),
            request=_pub_req(),
            session_mgr=MagicMock(),
            settings=MagicMock(),
        )
    run.assert_not_called()


async def test_public_route_runs_for_builder(monkeypatch, domain):
    domain.info = {"name": "Customer 360", "mcp_policy": {}}
    dt, run = _patch_public(monkeypatch, domain, role="builder")
    resp = await dt.dt_nodes_business_rule(
        payload=dt.NodeBusinessRuleRequest(entity_uri=_ENTITY, rule="VipCustomer"),
        request=_pub_req(),
        session_mgr=MagicMock(),
        settings=MagicMock(),
    )
    assert resp.materialized_count == 1
    assert run.call_args.kwargs["rule_name"] == "VipCustomer"


def test_swrl_rename_and_delete_cascade_to_class_refs(client, monkeypatch):
    from api.routers.internal import ontology as ont

    classes = [{"name": "Customer", "business_rules": [{"name": "VipCustomer"}]}]
    d = MagicMock()
    d.swrl_rules = [dict(_RULE)]
    d.get_classes.return_value = classes
    d.diff_meta.return_value = {}
    monkeypatch.setattr(ont, "get_domain", lambda _sm: d)

    renamed = {**_RULE, "name": "VipClient"}
    resp = client.post("/ontology/swrl/save", json={"rule": renamed, "index": 0})
    assert resp.status_code == 200
    assert classes[0]["business_rules"] == [{"name": "VipClient"}]

    resp = client.post("/ontology/swrl/delete", json={"index": 0})
    assert resp.status_code == 200
    assert classes[0]["business_rules"] == []
