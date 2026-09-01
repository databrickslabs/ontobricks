"""Task 2 contract tests for graph-metric paginated series."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from back.core.errors import ValidationError
from back.core.graph_analysis import METRIC_SERIES_MAX_LIMIT
from back.objects.digitaltwin import DigitalTwin
from shared.fastapi.main import app

pytestmark = pytest.mark.unit


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=False)


def test_service_serializes_metric_series_page(monkeypatch):
    from back.core import databricks
    from back.core import helpers

    captured = {}

    class _FakeClient:
        def __init__(self, *, host, token, warehouse_id):
            captured["host"] = host
            captured["token"] = token
            captured["warehouse_id"] = warehouse_id

        def execute_query(self, sql):
            captured["sql"] = sql
            return [
                {"node_uri": "urn:a", "label": "A", "score": 0.9, "total_count": 2},
                {"node_uri": "urn:b", "label": "B", "score": 0.8, "total_count": 2},
            ]

    monkeypatch.setattr(
        helpers, "get_databricks_host_and_token", lambda _d, _s: ("https://h", "tok")
    )
    monkeypatch.setattr(helpers, "resolve_delta_warehouse_id", lambda _d, _s: "wh")
    monkeypatch.setattr(databricks, "DatabricksClient", _FakeClient)

    domain = SimpleNamespace(uc_domain_folder="acme", current_version="1")
    settings = SimpleNamespace(analytics_job_output_schema="cat.sch")

    payload = DigitalTwin(domain).load_graph_metric_series(
        graph_name="graph_name", metric="pagerank", offset=0, limit=10, settings=settings
    )

    assert payload == {
        "offset": 0,
        "total": 2,
        "next_offset": None,
        "uris": ["urn:a", "urn:b"],
        "labels": ["A", "B"],
        "scores": [0.9, 0.8],
    }
    assert captured["host"] == "https://h"
    assert captured["token"] == "tok"
    assert captured["warehouse_id"] == "wh"
    assert "FROM cat.sch.graph_metrics_acme_1" in captured["sql"]


def test_route_returns_has_result_false_without_building_series(client, monkeypatch):
    from api.routers.internal import dtwin

    domain = SimpleNamespace(uc_domain_folder="acme", current_version="1")
    service_call = MagicMock()

    monkeypatch.setattr(dtwin, "get_domain", lambda _session_mgr: domain)
    monkeypatch.setattr(dtwin, "_load_stored_metrics", lambda _d, _s: None)
    monkeypatch.setattr(DigitalTwin, "load_graph_metric_series", service_call)

    response = client.get("/dtwin/metrics/series", params={"metric": "pagerank"})

    assert response.status_code == 200
    assert response.json() == {"success": True, "has_result": False}
    service_call.assert_not_called()


def test_route_clamps_limit_and_carries_computed_at(client, monkeypatch):
    from api.routers.internal import dtwin

    domain = SimpleNamespace(uc_domain_folder="acme", current_version="1")
    captured = {}

    def _fake_series(self, graph_name, metric, offset, limit, settings):
        captured["graph_name"] = graph_name
        captured["metric"] = metric
        captured["offset"] = offset
        captured["limit"] = limit
        captured["settings"] = settings
        return {
            "offset": offset,
            "total": 1,
            "next_offset": None,
            "uris": ["urn:a"],
            "labels": ["A"],
            "scores": [0.9],
        }

    monkeypatch.setattr(dtwin, "get_domain", lambda _session_mgr: domain)
    monkeypatch.setattr(
        dtwin,
        "_load_stored_metrics",
        lambda _d, _s: {
            "graph_name": "cat.sch.graph_metrics",
            "computed_at": "2026-09-01T12:00:00Z",
        },
    )
    monkeypatch.setattr(DigitalTwin, "load_graph_metric_series", _fake_series)

    response = client.get(
        "/dtwin/metrics/series",
        params={"metric": "pagerank", "offset": "3", "limit": "999999"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "success": True,
        "has_result": True,
        "metric": "pagerank",
        "computed_at": "2026-09-01T12:00:00Z",
        "offset": 3,
        "total": 1,
        "next_offset": None,
        "uris": ["urn:a"],
        "labels": ["A"],
        "scores": [0.9],
    }
    assert captured["graph_name"] == "cat.sch.graph_metrics"
    assert captured["metric"] == "pagerank"
    assert captured["offset"] == 3
    assert captured["limit"] == METRIC_SERIES_MAX_LIMIT


def test_route_returns_has_result_false_when_graph_name_missing(client, monkeypatch):
    from api.routers.internal import dtwin

    domain = SimpleNamespace(uc_domain_folder="acme", current_version="1")
    service_call = MagicMock()

    monkeypatch.setattr(dtwin, "get_domain", lambda _session_mgr: domain)
    monkeypatch.setattr(
        dtwin,
        "_load_stored_metrics",
        lambda _d, _s: {"graph_name": "", "computed_at": "2026-09-01T12:00:00Z"},
    )
    monkeypatch.setattr(DigitalTwin, "load_graph_metric_series", service_call)

    response = client.get("/dtwin/metrics/series", params={"metric": "pagerank"})

    assert response.status_code == 200
    assert response.json() == {"success": True, "has_result": False}
    service_call.assert_not_called()


def test_route_rejects_unsupported_metric_with_http_400(client, monkeypatch):
    from api.routers.internal import dtwin

    domain = SimpleNamespace(uc_domain_folder="acme", current_version="1")

    monkeypatch.setattr(dtwin, "get_domain", lambda _session_mgr: domain)
    monkeypatch.setattr(
        dtwin,
        "_load_stored_metrics",
        lambda _d, _s: {"graph_name": "cat.sch.graph_metrics", "computed_at": "ts"},
    )
    monkeypatch.setattr(
        DigitalTwin,
        "load_graph_metric_series",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            ValidationError("Unsupported graph metric")
        ),
    )

    response = client.get("/dtwin/metrics/series", params={"metric": "not_a_metric"})

    assert response.status_code == 400
    body = response.json()
    assert body["error"] == "validation"
    assert body["message"] == "Unsupported graph metric"
