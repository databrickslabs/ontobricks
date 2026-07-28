"""Tests for POST /api/v1/digitaltwin/nodes/action."""

import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

from shared.fastapi.main import app


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=False)


_SCALAR_ACTION = {
    "catalog": "main",
    "schema": "ops",
    "function": "recompute_risk",
    "fullName": "main.ops.recompute_risk",
    "description": "Recompute the customer risk score",
    "returns_table": False,
}

_TABLE_ACTION = {
    "catalog": "main",
    "schema": "ops",
    "function": "risk_history",
    "fullName": "main.ops.risk_history",
    "description": "Return the risk score history",
    "returns_table": True,
}

_CLASSES = [
    {
        "name": "Customer",
        "uri": "https://example.com/Customer",
        "actions": [_SCALAR_ACTION, _TABLE_ACTION],
    }
]


def _mock_domain(classes=None):
    domain = MagicMock()
    domain.get_classes.return_value = _CLASSES if classes is None else classes
    return domain


def _invoke(client, action_full_name, mock_client, classes=None):
    with patch(
        "api.routers.digitaltwin.DigitalTwin.resolve_domain",
        return_value=_mock_domain(classes),
    ), patch(
        "api.routers.digitaltwin.get_databricks_client",
        return_value=mock_client,
    ), patch(
        "api.routers.digitaltwin.run_blocking",
        side_effect=lambda fn, *a, **kw: fn(*a, **kw),
    ):
        return client.post(
            "/api/v1/digitaltwin/nodes/action",
            json={
                "entity_uri": "https://example.com/Customer/CUST001",
                "action_full_name": action_full_name,
                "domain_name": "test",
            },
        )


class TestNodeActionEndpoint:
    def test_scalar_action_passes_entity_id_as_single_argument(self, client):
        mock_client = MagicMock()
        mock_client.execute_query.return_value = [{"result": "OK"}]

        resp = _invoke(client, "main.ops.recompute_risk", mock_client)

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["class_name"] == "Customer"
        assert body["action"] == "main.ops.recompute_risk"
        assert body["rows"] == [{"result": "OK"}]
        mock_client.execute_query.assert_called_once_with(
            "SELECT main.ops.recompute_risk('CUST001') AS result"
        )

    def test_table_action_selects_from_the_function(self, client):
        mock_client = MagicMock()
        mock_client.execute_query.return_value = [{"day": "2026-01-01", "score": 42}]

        resp = _invoke(client, "main.ops.risk_history", mock_client)

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["returns_table"] is True
        assert body["rows"] == [{"day": "2026-01-01", "score": 42}]
        mock_client.execute_query.assert_called_once_with(
            "SELECT * FROM main.ops.risk_history('CUST001')"
        )

    def test_unlisted_function_is_rejected(self, client):
        """Only functions declared on the class may run — no arbitrary SQL."""
        mock_client = MagicMock()

        resp = _invoke(client, "main.ops.drop_everything", mock_client)

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is False
        assert "not configured" in body["message"]
        mock_client.execute_query.assert_not_called()

    def test_action_with_invalid_identifier_is_rejected(self, client):
        classes = [
            {
                "name": "Customer",
                "uri": "https://example.com/Customer",
                "actions": [{"fullName": "main.ops.x(); DROP TABLE y"}],
            }
        ]
        mock_client = MagicMock()

        resp = _invoke(
            client, "main.ops.x(); DROP TABLE y", mock_client, classes=classes
        )

        assert resp.status_code == 200
        assert resp.json()["success"] is False
        mock_client.execute_query.assert_not_called()

    def test_unknown_class_is_rejected(self, client):
        mock_client = MagicMock()

        with patch(
            "api.routers.digitaltwin.DigitalTwin.resolve_domain",
            return_value=_mock_domain(),
        ), patch(
            "api.routers.digitaltwin.get_databricks_client",
            return_value=mock_client,
        ):
            resp = client.post(
                "/api/v1/digitaltwin/nodes/action",
                json={
                    "entity_uri": "https://example.com/Invoice/INV001",
                    "action_full_name": "main.ops.recompute_risk",
                    "domain_name": "test",
                },
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is False
        assert "No ontology class" in body["message"]
        mock_client.execute_query.assert_not_called()

    def test_query_failure_is_surfaced(self, client):
        mock_client = MagicMock()
        mock_client.execute_query.side_effect = RuntimeError("warehouse unavailable")

        resp = _invoke(client, "main.ops.recompute_risk", mock_client)

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is False
        assert body["message"] == "warehouse unavailable"
        assert body["rows"] == []

    def test_missing_databricks_client_is_reported(self, client):
        resp = _invoke(client, "main.ops.recompute_risk", None)

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is False
        assert "not configured" in body["message"]

    def test_missing_action_full_name_returns_422(self, client):
        resp = client.post(
            "/api/v1/digitaltwin/nodes/action",
            json={"entity_uri": "https://example.com/Customer/CUST001"},
        )
        assert resp.status_code == 422
