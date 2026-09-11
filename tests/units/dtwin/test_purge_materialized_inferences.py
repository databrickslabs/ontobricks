"""API contracts for source-safe materialized inference purging."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.unit

MODULE = "api.routers.internal.dtwin"


@pytest.fixture
def api_client():
    from shared.fastapi.main import app

    return TestClient(app, raise_server_exceptions=False)


def test_purge_materialized_inferences_returns_graph_and_count(api_client):
    domain = MagicMock()
    store = MagicMock()
    store.purge_materialized_triples.return_value = 23

    with (
        patch(f"{MODULE}.get_domain", return_value=domain),
        patch(f"{MODULE}.get_graphdb", return_value=store),
        patch(f"{MODULE}.effective_graph_name", return_value="sales_V3"),
    ):
        response = api_client.delete("/dtwin/reasoning/inferred")

    assert response.status_code == 200
    assert response.json() == {
        "success": True,
        "graph_name": "sales_V3",
        "purged_count": 23,
    }
    store.purge_materialized_triples.assert_called_once_with("sales_V3")


def test_purge_materialized_inferences_rejects_unsafe_backend(api_client):
    store = MagicMock()
    store.purge_materialized_triples.side_effect = NotImplementedError(
        "Neo4jStore cannot safely purge generated triples"
    )

    with (
        patch(f"{MODULE}.get_domain", return_value=MagicMock()),
        patch(f"{MODULE}.get_graphdb", return_value=store),
        patch(f"{MODULE}.effective_graph_name", return_value="sales_V3"),
    ):
        response = api_client.delete("/dtwin/reasoning/inferred")

    assert response.status_code == 502
    assert response.json()["error"] == "infrastructure"


def test_materialized_inference_status_returns_live_count(api_client):
    store = MagicMock()
    store.supports_materialized_inference_purge = True
    store.get_inferred_triple_count.return_value = 31

    with (
        patch(f"{MODULE}.get_domain", return_value=MagicMock()),
        patch(f"{MODULE}.get_graphdb", return_value=store),
        patch(f"{MODULE}.effective_graph_name", return_value="sales_V3"),
    ):
        response = api_client.get("/dtwin/reasoning/inferred")

    assert response.status_code == 200
    assert response.json() == {
        "success": True,
        "graph_name": "sales_V3",
        "materialized_inference_count": 31,
        "purge_supported": True,
        "reasoning": {
            "last_run": None,
            "inferred_count": 31,
            "inferred_triples": [],
        },
    }
    store.get_inferred_triple_count.assert_called_once_with("sales_V3")


def test_materialized_inference_status_returns_na_for_unsafe_backend(api_client):
    store = MagicMock()
    store.supports_materialized_inference_purge = False

    with (
        patch(f"{MODULE}.get_domain", return_value=MagicMock()),
        patch(f"{MODULE}.get_graphdb", return_value=store),
        patch(f"{MODULE}.effective_graph_name", return_value="sales_V3"),
    ):
        response = api_client.get("/dtwin/reasoning/inferred")

    assert response.status_code == 200
    assert response.json()["materialized_inference_count"] is None
    assert response.json()["purge_supported"] is False
    store.get_inferred_triple_count.assert_not_called()
