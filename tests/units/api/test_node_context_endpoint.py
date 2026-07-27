"""Tests for GET /api/v1/digitaltwin/nodes/context."""

import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

from shared.fastapi.main import app
from api.routers.digitaltwin import _RDF_TYPE


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=False)


_CLASSES_WITH_ACTIONS = [
    {
        "name": "Customer",
        "uri": "https://example.com/Customer",
        "dataset": {
            "catalog": "main", "schema": "crm", "asset": "customers",
            "type": "TABLE", "fullName": "main.crm.customers", "key_column": "id",
            "description": "Customer master records.",
        },
        "bridges": [
            {
                "target_domain": "Finance",
                "target_class_name": "Contract",
                "target_class_uri": "https://example.com/Contract",
                "label": "Owns contracts",
            }
        ],
    },
]


class TestNodeContextEndpoint:
    def _mock_domain(self):
        mock_domain = MagicMock()
        mock_domain.get_classes.return_value = _CLASSES_WITH_ACTIONS
        return mock_domain

    def test_metadata_only_by_default(self, client):
        """Without flags, returns dataset/bridge metadata with no rows or entities."""
        mock_domain = self._mock_domain()

        with patch("api.routers.digitaltwin.DigitalTwin.resolve_domain", return_value=mock_domain):
            resp = client.get(
                "/api/v1/digitaltwin/nodes/context",
                params={
                    "entity_uri": "https://example.com/Customer/CUST001",
                    "domain_name": "test",
                },
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["entity_local_id"] == "CUST001"
        assert body["class_name"] == "Customer"
        assert body["dataset"]["fullName"] == "main.crm.customers"
        assert body["dataset"]["description"] == "Customer master records."
        assert "rows" not in body["dataset"]
        assert body["bridges"][0]["target_domain"] == "Finance"
        assert "entities" not in body["bridges"][0]

    def test_missing_entity_uri_returns_422(self, client):
        resp = client.get(
            "/api/v1/digitaltwin/nodes/context",
            params={"domain_name": "test"},
        )
        assert resp.status_code == 422

    def test_unknown_class_returns_empty_context(self, client):
        mock_domain = self._mock_domain()
        with patch("api.routers.digitaltwin.DigitalTwin.resolve_domain", return_value=mock_domain):
            resp = client.get(
                "/api/v1/digitaltwin/nodes/context",
                params={
                    "entity_uri": "https://example.com/Invoice/INV001",
                    "domain_name": "test",
                },
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body.get("dataset") is None
        assert body.get("bridges") == [] or body.get("bridges") is None

    def test_hash_class_uri_matches_path_based_entity(self, client):
        """Instance URIs .../ClassName/id must match ontology class ...#ClassName."""
        classes = [
            {
                "name": "Meter",
                "uri": "https://databricks-ontology.com/Cust360Auto#Meter",
                "dataset": {
                    "catalog": "main",
                    "schema": "iot",
                    "asset": "meters",
                    "type": "TABLE",
                    "fullName": "main.iot.meters",
                    "key_column": "meter_id",
                    "description": "Smart meter master.",
                },
                "bridges": [],
            }
        ]
        mock_domain = MagicMock()
        mock_domain.get_classes.return_value = classes

        with patch("api.routers.digitaltwin.DigitalTwin.resolve_domain", return_value=mock_domain):
            resp = client.get(
                "/api/v1/digitaltwin/nodes/context",
                params={
                    "entity_uri": (
                        "https://databricks-ontology.com/Cust360Auto/Meter/MTR0000049"
                    ),
                    "domain_name": "Cust360Auto",
                },
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["class_name"] == "Meter"
        assert body["dataset"]["fullName"] == "main.iot.meters"
        assert body["dataset"]["description"] == "Smart meter master."

    def test_follow_bridges_returns_entities(self, client):
        """follow_bridges=True traverses bridge domain and populates entities."""
        mock_domain = self._mock_domain()
        mock_target_dom = MagicMock()
        mock_target_store = MagicMock()
        mock_target_store.bfs_traversal.return_value = [
            {
                "subject": "http://x/Account/1",
                "predicate": _RDF_TYPE,
                "object": "https://example.com/Contract",
            }
        ]

        resolve_calls = []

        def _resolve_side_effect(*args, **kwargs):
            resolve_calls.append(args)
            return mock_domain if len(resolve_calls) == 1 else mock_target_dom

        with patch(
            "api.routers.digitaltwin.DigitalTwin.resolve_domain",
            side_effect=_resolve_side_effect,
        ), patch(
            "api.routers.digitaltwin.get_graphdb",
            return_value=mock_target_store,
        ), patch(
            "api.routers.digitaltwin.effective_graph_query_table",
            return_value="catalog.schema.graph",
        ):
            resp = client.get(
                "/api/v1/digitaltwin/nodes/context",
                params={
                    "entity_uri": "https://example.com/Customer/CUST001",
                    "domain_name": "test",
                    "follow_bridges": "true",
                },
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        bridges = body.get("bridges", [])
        assert len(bridges) == 1
        assert bridges[0]["entities"] is not None
        assert len(bridges[0]["entities"]) > 0

    def test_dataset_rows_skipped_when_key_column_missing(self, client):
        classes_no_key = [
            {
                "name": "Customer",
                "uri": "https://example.com/Customer",
                "dataset": {
                    "catalog": "main", "schema": "crm", "asset": "customers",
                    "type": "TABLE", "fullName": "main.crm.customers",
                    # key_column absent
                },
                "bridges": [],
            }
        ]
        mock_domain = MagicMock()
        mock_domain.get_classes.return_value = classes_no_key

        with patch("api.routers.digitaltwin.DigitalTwin.resolve_domain", return_value=mock_domain):
            resp = client.get(
                "/api/v1/digitaltwin/nodes/context",
                params={
                    "entity_uri": "https://example.com/Customer/CUST001",
                    "domain_name": "test",
                    "fetch_dataset_rows": "true",
                },
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["dataset"].get("key_column_missing") is True
        assert "rows" not in body["dataset"]

    def test_dataset_row_fetch_passes_domain_and_settings_to_client(self, client):
        mock_domain = self._mock_domain()
        mock_client = MagicMock()
        mock_client.execute_query.return_value = [{"id": "CUST001", "name": "Ada"}]

        with patch(
            "api.routers.digitaltwin.DigitalTwin.resolve_domain",
            return_value=mock_domain,
        ), patch(
            "api.routers.digitaltwin.get_databricks_client",
            return_value=mock_client,
        ) as mock_get_client, patch(
            "api.routers.digitaltwin.run_blocking",
            side_effect=lambda fn, *a, **kw: fn(*a, **kw),
        ):
            resp = client.get(
                "/api/v1/digitaltwin/nodes/context",
                params={
                    "entity_uri": "https://example.com/Customer/CUST001",
                    "domain_name": "test",
                    "fetch_dataset_rows": "true",
                    "dataset_row_limit": "10",
                },
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["dataset"]["rows"] == [{"id": "CUST001", "name": "Ada"}]
        mock_get_client.assert_called_once()
        args = mock_get_client.call_args[0]
        assert args[0] is mock_domain
        assert len(args) == 2
