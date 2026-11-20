"""Route tests for GET /dtwin/reasoning/inferred/breakdown."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.unit

MODULE = "api.routers.internal.dtwin"
URL = "/dtwin/reasoning/inferred/breakdown"


@pytest.fixture
def api_client():
    from shared.fastapi.main import app

    return TestClient(app, raise_server_exceptions=False)


def _domain(base_uri=None):
    domain = MagicMock()
    domain.ontology = {"base_uri": base_uri} if base_uri else {}
    return domain


def _rows(reasoning=3, cohorts=5, communities=4):
    return [
        {"category": "reasoning", "label": "Reasoning", "triples": reasoning, "entities": 2, "groups": None},
        {"category": "cohorts", "label": "Cohorts", "triples": cohorts, "entities": 3, "groups": 1},
        {"category": "communities", "label": "Communities", "triples": communities, "entities": 3, "groups": 2},
    ]


def test_breakdown_supported_returns_categories_and_total(api_client):
    store = MagicMock()
    store.supports_inferred_breakdown = True
    store.get_inferred_breakdown.return_value = _rows()

    with (
        patch(f"{MODULE}.get_domain", return_value=_domain("http://x.io/ns#")),
        patch(f"{MODULE}.get_graphdb", return_value=store),
        patch(f"{MODULE}.effective_graph_name", return_value="dom_V2"),
    ):
        response = api_client.get(URL)

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["supported"] is True
    assert body["graph_name"] == "dom_V2"
    assert body["total"] == 12
    assert [c["category"] for c in body["categories"]] == ["reasoning", "cohorts", "communities"]
    store.get_inferred_breakdown.assert_called_once_with("dom_V2", "http://x.io/ns#")


def test_breakdown_falls_back_to_default_base_uri(api_client):
    store = MagicMock()
    store.supports_inferred_breakdown = True
    store.get_inferred_breakdown.return_value = _rows(0, 0, 0)

    with (
        patch(f"{MODULE}.get_domain", return_value=_domain(None)),
        patch(f"{MODULE}.get_graphdb", return_value=store),
        patch(f"{MODULE}.effective_graph_name", return_value="dom_V2"),
        patch(f"{MODULE}.DEFAULT_BASE_URI", "http://default.io/ns#"),
    ):
        response = api_client.get(URL)

    assert response.status_code == 200
    assert response.json()["total"] == 0
    store.get_inferred_breakdown.assert_called_once_with("dom_V2", "http://default.io/ns#")


def test_breakdown_unsupported_backend_skips_store(api_client):
    store = MagicMock()
    store.supports_inferred_breakdown = False

    with (
        patch(f"{MODULE}.get_domain", return_value=_domain()),
        patch(f"{MODULE}.get_graphdb", return_value=store),
        patch(f"{MODULE}.effective_graph_name", return_value="dom_V2"),
    ):
        response = api_client.get(URL)

    assert response.status_code == 200
    assert response.json() == {
        "success": True,
        "graph_name": "dom_V2",
        "supported": False,
        "categories": [],
        "total": None,
    }
    store.get_inferred_breakdown.assert_not_called()
