"""Tests for GET /api/v1/domain/classes."""

import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

from shared.fastapi.main import app


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=False)


_MOCK_CLASSES = [
    {
        "name": "Customer",
        "uri": "https://example.com/Customer",
        "dataset": {"catalog": "main", "schema": "crm", "asset": "customers",
                    "type": "TABLE", "fullName": "main.crm.customers", "key_column": "id"},
        "bridges": [{"target_domain": "Finance", "target_class_name": "Contract",
                     "target_class_uri": "https://example.com/Contract", "label": "Owns"}],
        "actions": [{"catalog": "main", "schema": "ops", "function": "recompute_risk",
                     "fullName": "main.ops.recompute_risk",
                     "description": "Recompute the risk score", "returns_table": False}],
    },
    {
        "name": "Order",
        "uri": "https://example.com/Order",
        "dataset": None,
        "bridges": [],
        "actions": [],
    },
]


class TestDomainClassesEndpoint:
    def test_returns_classes_with_actions(self, client):
        mock_domain = MagicMock()
        mock_domain.get_classes.return_value = _MOCK_CLASSES

        with patch("api.routers.domains.DigitalTwin.resolve_domain", return_value=mock_domain):
            resp = client.get("/api/v1/domain/classes", params={"domain_name": "test"})

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        classes = body["classes"]
        assert len(classes) == 2
        customer = next(c for c in classes if c["name"] == "Customer")
        assert customer["dataset"]["key_column"] == "id"
        assert len(customer["bridges"]) == 1
        assert customer["actions"][0]["fullName"] == "main.ops.recompute_risk"
        assert customer["actions"][0]["description"] == "Recompute the risk score"

    def test_empty_actions_class_has_null_dataset_and_no_bridges(self, client):
        mock_domain = MagicMock()
        mock_domain.get_classes.return_value = _MOCK_CLASSES

        with patch("api.routers.domains.DigitalTwin.resolve_domain", return_value=mock_domain):
            resp = client.get("/api/v1/domain/classes", params={"domain_name": "test"})

        body = resp.json()
        order = next(c for c in body["classes"] if c["name"] == "Order")
        # Order has no dataset, bridges or actions — absent or null/empty
        assert not order.get("dataset")
        assert not order.get("bridges")
        assert not order.get("actions")

    def test_missing_domain_name_uses_session(self, client):
        mock_domain = MagicMock()
        mock_domain.get_classes.return_value = []

        with patch("api.routers.domains.DigitalTwin.resolve_domain", return_value=mock_domain):
            resp = client.get("/api/v1/domain/classes")

        assert resp.status_code == 200

    def test_bridges_enriched_with_target_description(self, client):
        mock_domain = MagicMock()
        mock_domain.get_classes.return_value = _MOCK_CLASSES

        with patch(
            "api.routers.domains.DigitalTwin.resolve_domain", return_value=mock_domain
        ), patch(
            "back.objects.digitaltwin.NodeContextService.NodeContextService"
            "._load_mcp_target_descriptions",
            return_value={"Finance": "Finance ontology"},
        ):
            resp = client.get("/api/v1/domain/classes", params={"domain_name": "test"})

        body = resp.json()
        customer = next(c for c in body["classes"] if c["name"] == "Customer")
        assert customer["bridges"][0]["target_domain_description"] == "Finance ontology"
        assert customer["bridges"][0]["target_domain"] == "Finance"

    def test_bridges_omit_non_mcp_visible_target(self, client):
        mock_domain = MagicMock()
        mock_domain.get_classes.return_value = _MOCK_CLASSES

        with patch(
            "api.routers.domains.DigitalTwin.resolve_domain", return_value=mock_domain
        ), patch(
            "back.objects.digitaltwin.NodeContextService.NodeContextService"
            "._load_mcp_target_descriptions",
            return_value={"Other": "Some other domain"},
        ):
            resp = client.get("/api/v1/domain/classes", params={"domain_name": "test"})

        body = resp.json()
        customer = next(c for c in body["classes"] if c["name"] == "Customer")
        assert customer["bridges"] == []  # bridge to Finance dropped: not MCP-visible

    def test_bridges_soft_fail_when_registry_unavailable(self, client):
        mock_domain = MagicMock()
        mock_domain.get_classes.return_value = _MOCK_CLASSES

        with patch(
            "api.routers.domains.DigitalTwin.resolve_domain", return_value=mock_domain
        ), patch(
            "back.objects.digitaltwin.NodeContextService.NodeContextService"
            "._load_mcp_target_descriptions",
            return_value=None,
        ):
            resp = client.get("/api/v1/domain/classes", params={"domain_name": "test"})

        body = resp.json()
        customer = next(c for c in body["classes"] if c["name"] == "Customer")
        # Soft-fail: bridge kept, description empty, no filtering.
        assert len(customer["bridges"]) == 1
        assert customer["bridges"][0]["target_domain_description"] == ""
