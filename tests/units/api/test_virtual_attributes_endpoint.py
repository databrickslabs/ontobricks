"""Virtual attributes on the node-context and Compute endpoints."""

from importlib import import_module
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from shared.fastapi.main import app

pytestmark = pytest.mark.unit

va_module = import_module("back.objects.digitaltwin.VirtualAttributeService")


_CLASSES_WITH_VIRTUAL = [
    {
        "name": "Customer",
        "uri": "https://example.com/Customer",
        "dataset": None,
        "bridges": [],
        "actions": [],
        "virtualAttributes": [
            {
                "catalog": "main",
                "schema": "kg",
                "function": "customer_risk",
                "fullName": "main.kg.customer_risk",
                "description": "Live credit risk",
                "returns_table": True,
                "attributes": [
                    {"name": "risk_score", "column": "risk_score", "dataType": "DOUBLE"},
                    {"name": "risk_band", "column": "risk_band"},
                ],
            }
        ],
    },
]


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def domain():
    mock_domain = MagicMock()
    mock_domain.info = {"name": "Customer 360"}
    mock_domain.domain_folder = "customer-360"
    mock_domain.get_classes.return_value = _CLASSES_WITH_VIRTUAL
    return mock_domain


@pytest.fixture
def uc_client(monkeypatch):
    """Install a fake Databricks client returning one risk row."""
    fake = MagicMock()
    fake.execute_query.return_value = [{"risk_score": 0.82, "risk_band": "B"}]
    monkeypatch.setattr(va_module, "get_databricks_client", lambda *_a, **_kw: fake)
    return fake


class TestNodeContext:
    def test_declarations_ride_along_without_computing(self, client, domain, uc_client):
        with patch(
            "api.routers.digitaltwin.DigitalTwin.resolve_domain", return_value=domain
        ):
            resp = client.get(
                "/api/v1/digitaltwin/nodes/context",
                params={
                    "entity_uri": "https://example.com/Customer/CUST001",
                    "domain_name": "test",
                },
            )

        assert resp.status_code == 200
        groups = resp.json()["virtual_attributes"]
        assert [a["name"] for a in groups[0]["attributes"]] == [
            "risk_score",
            "risk_band",
        ]
        # The distinction between "not computed" and "computed as null" is
        # load-bearing: no values key at all until asked.
        assert "values" not in groups[0]
        uc_client.execute_query.assert_not_called()

    def test_flag_computes_the_values(self, client, domain, uc_client):
        with patch(
            "api.routers.digitaltwin.DigitalTwin.resolve_domain", return_value=domain
        ):
            resp = client.get(
                "/api/v1/digitaltwin/nodes/context",
                params={
                    "entity_uri": "https://example.com/Customer/CUST001",
                    "domain_name": "test",
                    "compute_virtual_attributes": "true",
                },
            )

        assert resp.status_code == 200
        groups = resp.json()["virtual_attributes"]
        assert groups[0]["values"] == {"risk_score": 0.82, "risk_band": "B"}

    def test_disabled_policy_withholds_and_skips_the_query(
        self, client, domain, uc_client
    ):
        domain.info = {
            "name": "Customer 360",
            "mcp_policy": {"context": {"virtual_attributes": "disabled"}},
        }

        with patch(
            "api.routers.digitaltwin.DigitalTwin.resolve_domain", return_value=domain
        ):
            resp = client.get(
                "/api/v1/digitaltwin/nodes/context",
                params={
                    "entity_uri": "https://example.com/Customer/CUST001",
                    "domain_name": "test",
                    "compute_virtual_attributes": "true",
                },
            )

        assert resp.status_code == 200
        assert "virtual_attributes" not in resp.json()
        uc_client.execute_query.assert_not_called()


class TestComputeEndpoint:
    def test_computes_every_group(self, client, domain, uc_client, monkeypatch):
        from api.routers.internal import dtwin

        monkeypatch.setattr(dtwin, "get_domain", lambda _session_mgr: domain)

        resp = client.get(
            "/dtwin/nodes/virtual-attributes",
            params={"entity_uri": "https://example.com/Customer/CUST001"},
        )

        assert resp.status_code == 200
        body = resp.json()
        assert body["class_name"] == "Customer"
        assert body["virtual_attributes"][0]["values"]["risk_band"] == "B"

    def test_undeclared_function_is_refused(
        self, client, domain, uc_client, monkeypatch
    ):
        from api.routers.internal import dtwin

        monkeypatch.setattr(dtwin, "get_domain", lambda _session_mgr: domain)

        resp = client.get(
            "/dtwin/nodes/virtual-attributes",
            params={
                "entity_uri": "https://example.com/Customer/CUST001",
                "function": "main.kg.salary_lookup",
            },
        )

        assert resp.status_code == 400
        uc_client.execute_query.assert_not_called()

    def test_unmatched_entity_is_not_found(
        self, client, domain, uc_client, monkeypatch
    ):
        from api.routers.internal import dtwin

        monkeypatch.setattr(dtwin, "get_domain", lambda _session_mgr: domain)

        resp = client.get(
            "/dtwin/nodes/virtual-attributes",
            params={"entity_uri": "https://elsewhere.example/Widget/W1"},
        )

        assert resp.status_code == 404


def test_dtwin_classes_exposes_declarations(client, domain, monkeypatch):
    from api.routers.internal import dtwin

    monkeypatch.setattr(dtwin, "get_domain", lambda _session_mgr: domain)

    resp = client.get("/dtwin/classes")

    assert resp.status_code == 200
    groups = resp.json()["classes"][0]["virtualAttributes"]
    assert groups[0]["fullName"] == "main.kg.customer_risk"


def test_domain_classes_honours_the_policy(client, domain):
    domain.info = {
        "name": "Customer 360",
        "mcp_policy": {"context": {"virtual_attributes": "disabled"}},
    }

    with patch(
        "api.routers.domains.DigitalTwin.resolve_domain", return_value=domain
    ):
        resp = client.get("/api/v1/domain/classes", params={"domain_name": "test"})

    assert resp.status_code == 200
    assert resp.json()["classes"][0].get("virtualAttributes", []) == []
