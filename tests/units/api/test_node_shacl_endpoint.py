"""Entity SHACL facet endpoints and the shape counts that gate them."""

from __future__ import annotations

from importlib import import_module
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from shared.fastapi.main import app

pytestmark = pytest.mark.unit

ENTITY = "https://example.com/Customer/CUST001"
CUSTOMER = "https://example.com/Customer"

_CLASSES = [
    {"name": "Customer", "uri": CUSTOMER, "dataset": None, "bridges": [], "actions": []},
    {"name": "Order", "uri": "https://example.com/Order"},
]

_SHAPES = [
    {"id": "s_email", "category": "completeness", "target_class": "Customer",
     "target_class_uri": CUSTOMER, "property_uri": "https://example.com/email",
     "shacl_type": "sh:minCount", "parameters": {"sh:minCount": 1}, "enabled": True},
    {"id": "s_label", "category": "structural", "target_class": "",
     "target_class_uri": "", "shacl_type": "sh:sparql",
     "parameters": {"sh:select": "SELECT $this WHERE { FILTER NOT EXISTS { $this rdfs:label ?l } }"},
     "enabled": True},
    {"id": "s_order", "category": "completeness", "target_class": "Order",
     "target_class_uri": "https://example.com/Order", "shacl_type": "sh:minCount",
     "parameters": {"sh:minCount": 1}, "enabled": True},
]

_PAYLOAD = {
    "success": True,
    "entity_uri": ENTITY,
    "entity_local_id": "CUST001",
    "entity_label": "ACME Corp",
    "classes": [{"name": "Customer", "uri": CUSTOMER}],
    "lineage": ["Customer"],
    "summary": {
        "total": 1, "pass": 0, "fail": 1, "not_applicable": 0,
        "not_evaluated": 0, "error": 0,
        "fail_by_severity": {"sh:Violation": 1, "sh:Warning": 0, "sh:Info": 0},
    },
    "facets": [{
        "shape_id": "s_email", "label": "Customer.email must not be empty",
        "category": "completeness", "severity": "sh:Violation",
        "target_class": "Customer", "inherited_from": None,
        "property_path": "email", "property_uri": "https://example.com/email",
        "shacl_type": "sh:minCount", "parameters": {"sh:minCount": 1},
        "constraint": "sh:minCount 1", "status": "fail", "values": [],
        "values_truncated": False, "details": [{"s": ENTITY, "count": 0}],
        "message": "Customer.email must not be empty",
    }],
}


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def domain():
    mock_domain = MagicMock()
    mock_domain.info = {"name": "Customer 360"}
    mock_domain.domain_folder = "customer-360"
    mock_domain.get_classes.return_value = _CLASSES
    mock_domain.shacl_shapes = _SHAPES
    mock_domain.swrl_rules = []
    return mock_domain


@pytest.fixture
def evaluate_calls(monkeypatch):
    """Stub the service so the routes are tested for wiring alone."""
    try:
        svc = import_module("back.objects.digitaltwin.node.NodeShaclService").NodeShaclService
    except ImportError as exc:
        pytest.fail(f"NodeShaclService is not implemented yet: {exc}")
    calls: list[dict] = []

    async def fake_evaluate(domain, settings, *, entity_uri):
        calls.append({"domain": domain, "entity_uri": entity_uri})
        return dict(_PAYLOAD)

    monkeypatch.setattr(svc, "evaluate", staticmethod(fake_evaluate))
    return calls


class TestPublicEndpoint:
    def test_returns_the_facets(self, client, domain, evaluate_calls):
        with patch("api.routers.digitaltwin.DigitalTwin.resolve_domain", return_value=domain):
            resp = client.get(
                "/api/v1/digitaltwin/nodes/shacl",
                params={"entity_uri": ENTITY, "domain_name": "test"},
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["summary"]["fail"] == 1
        assert body["facets"][0]["shape_id"] == "s_email"
        assert body["facets"][0]["details"] == [{"s": ENTITY, "count": 0}]
        assert evaluate_calls == [{"domain": domain, "entity_uri": ENTITY}]

    def test_entity_uri_is_required(self, client, domain, evaluate_calls):
        with patch("api.routers.digitaltwin.DigitalTwin.resolve_domain", return_value=domain):
            resp = client.get("/api/v1/digitaltwin/nodes/shacl", params={"domain_name": "test"})

        assert resp.status_code == 422
        assert evaluate_calls == []

    def test_shapes_are_never_read_from_the_request(self, client, domain, evaluate_calls):
        """Unlike ``/dataquality/execute``, a caller cannot submit a shape."""
        with patch("api.routers.digitaltwin.DigitalTwin.resolve_domain", return_value=domain):
            resp = client.post(
                "/api/v1/digitaltwin/nodes/shacl",
                json={"entity_uri": ENTITY, "shape": {"id": "x"}},
            )

        assert resp.status_code == 405


class TestInternalEndpoint:
    def test_uses_the_session_domain(self, client, domain, evaluate_calls, monkeypatch):
        from api.routers.internal import dtwin

        monkeypatch.setattr(dtwin, "get_domain", lambda _session_mgr: domain)

        resp = client.get("/dtwin/nodes/shacl", params={"entity_uri": ENTITY})

        assert resp.status_code == 200
        assert resp.json()["entity_label"] == "ACME Corp"
        assert evaluate_calls[0]["domain"] is domain

    def test_start_returns_a_task_id(self, client, domain, monkeypatch):
        import threading

        from api.routers.internal import dtwin

        captured = {}

        class _Task:
            id = "shacl1"

        class _Tm:
            def create_task(self, **_kw):
                return _Task()

        class _FakeThread:
            def __init__(self, target=None, **_kw):
                self._target = target

            def start(self):
                self._target()

        def _run(tm, task_id, settings, snap, entity_uri, table):
            captured["entity_uri"] = entity_uri
            captured["table"] = table
            captured["task_id"] = task_id

        monkeypatch.setattr(dtwin, "get_domain", lambda _session_mgr: domain)
        monkeypatch.setattr(dtwin, "DomainSnapshot", lambda _d: object())
        monkeypatch.setattr(dtwin, "_dataquality_table", lambda _d, _s: "c.s.v")
        monkeypatch.setattr(
            dtwin.DigitalTwin, "run_node_shacl_task", staticmethod(_run)
        )
        monkeypatch.setattr(
            "back.core.task_manager.get_task_manager", lambda: _Tm()
        )
        monkeypatch.setattr(threading, "Thread", _FakeThread)

        resp = client.post("/dtwin/nodes/shacl/start", json={"entity_uri": ENTITY})

        assert resp.status_code == 200
        assert resp.json() == {"success": True, "task_id": "shacl1"}
        assert captured["entity_uri"] == ENTITY
        assert captured["task_id"] == "shacl1"

    def test_start_requires_entity_uri(self, client, domain, monkeypatch):
        from api.routers.internal import dtwin

        monkeypatch.setattr(dtwin, "get_domain", lambda _session_mgr: domain)
        resp = client.post("/dtwin/nodes/shacl/start", json={})
        assert resp.status_code == 400


class TestShapeCounts:
    def test_node_context_counts_own_and_global_shapes(self, client, domain):
        with patch("api.routers.digitaltwin.DigitalTwin.resolve_domain", return_value=domain):
            resp = client.get(
                "/api/v1/digitaltwin/nodes/context",
                params={"entity_uri": ENTITY, "domain_name": "test"},
            )

        assert resp.status_code == 200
        assert resp.json()["shacl_shape_count"] == 2

    def test_domain_classes_carry_a_count_per_class(self, client, domain):
        with patch("api.routers.domains.DigitalTwin.resolve_domain", return_value=domain):
            resp = client.get("/api/v1/domain/classes", params={"domain_name": "test"})

        assert resp.status_code == 200
        counts = {c["name"]: c.get("shacl_shape_count") for c in resp.json()["classes"]}
        assert counts == {"Customer": 2, "Order": 2}
