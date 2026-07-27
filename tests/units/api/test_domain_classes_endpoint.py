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
    },
    {
        "name": "Order",
        "uri": "https://example.com/Order",
        "dataset": None,
        "bridges": [],
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

    def test_filters_empty_actions(self, client):
        mock_domain = MagicMock()
        mock_domain.get_classes.return_value = _MOCK_CLASSES

        with patch("api.routers.domains.DigitalTwin.resolve_domain", return_value=mock_domain):
            resp = client.get("/api/v1/domain/classes", params={"domain_name": "test"})

        body = resp.json()
        order = next(c for c in body["classes"] if c["name"] == "Order")
        # Order has no dataset and no bridges — they should be absent or null/empty
        assert not order.get("dataset")
        assert not order.get("bridges")

    def test_missing_domain_name_uses_session(self, client):
        mock_domain = MagicMock()
        mock_domain.get_classes.return_value = []

        with patch("api.routers.domains.DigitalTwin.resolve_domain", return_value=mock_domain):
            resp = client.get("/api/v1/domain/classes")

        assert resp.status_code == 200
