"""Behavioral integration tests for external dataset context in MCP tools.

The production FastMCP tools are invoked in-process. HTTP calls are routed
through ``httpx.MockTransport`` so these tests exercise tool state, request
parameters, and output formatting without a live OntoBricks or Databricks
deployment.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import httpx
import pytest


_MCP_SRC = Path(__file__).resolve().parents[3] / "src" / "mcp-server"
if str(_MCP_SRC) not in sys.path:
    sys.path.insert(0, str(_MCP_SRC))


def _text(result: Any) -> str:
    if isinstance(result, str):
        return result
    content = getattr(result, "content", None)
    if content:
        return "\n".join(getattr(item, "text", str(item)) for item in content)
    structured = getattr(result, "structured_content", None)
    if structured is not None:
        return json.dumps(structured)
    return str(result)


@pytest.fixture
def dataset_mcp(monkeypatch):
    try:
        import server.app as app_module  # type: ignore[import-not-found]
    except ImportError as exc:
        pytest.skip(f"MCP server not importable: {exc}")

    routes: dict[tuple[str, str], dict[str, Any]] = {}
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        spec = routes.get((request.method.upper(), request.url.path))
        if spec is None:
            return httpx.Response(
                404,
                json={"error": f"unmocked route: {request.method} {request.url.path}"},
            )
        response_spec = dict(spec)
        status = response_spec.pop("status", 200)
        return httpx.Response(status, **response_spec)

    transport = httpx.MockTransport(handler)
    real_async_client = httpx.AsyncClient

    class PatchedAsyncClient(real_async_client):
        def __init__(self, *args, **kwargs):
            kwargs.setdefault("transport", transport)
            kwargs.setdefault("base_url", "http://test.local")
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(app_module.httpx, "AsyncClient", PatchedAsyncClient)
    monkeypatch.setattr(
        app_module,
        "_get_auth_headers",
        lambda mode: {"Authorization": "Bearer test"},
    )
    monkeypatch.setattr(app_module, "_base_url", lambda mode: "http://test.local")
    monkeypatch.setenv("REGISTRY_CATALOG", "test_cat")
    monkeypatch.setenv("REGISTRY_SCHEMA", "test_schema")
    monkeypatch.setenv("REGISTRY_VOLUME", "test_volume")

    mcp = app_module.create_mcp_server(mode="standalone")

    class Handle:
        async def call(self, tool_name: str, **kwargs):
            return await mcp.call_tool(tool_name, kwargs)

        def route(self, path: str, payload: dict[str, Any]) -> None:
            routes[("GET", path)] = {"json": payload}

        def configure_selected_domain(self) -> None:
            self.route(
                "/api/v1/digitaltwin/status",
                {
                    "success": True,
                    "has_data": True,
                    "count": 42,
                    "graph_name": "sales_graph",
                    "view_table": "main.sales.triples",
                },
            )
            self.route(
                "/api/v1/domain/classes",
                {
                    "success": True,
                    "classes": [
                        {
                            "name": "Customer",
                            "uri": "http://x/Customer",
                            "dataset": {
                                "fullName": "main.crm.customers",
                                "key_column": "customer_id",
                                "description": "Customer master records.",
                            },
                            "bridges": [],
                        }
                    ],
                },
            )

        def requests_to(self, path: str) -> list[httpx.Request]:
            return [request for request in requests if request.url.path == path]

    return Handle()


@pytest.mark.mcp
@pytest.mark.asyncio
async def test_list_entity_types_includes_cached_dataset_description(dataset_mcp):
    dataset_mcp.configure_selected_domain()
    selected = _text(
        await dataset_mcp.call("select_domain", domain_name="sales")
    )
    assert "Domain 'sales' selected" in selected

    dataset_mcp.route(
        "/api/v1/digitaltwin/stats",
        {
            "success": True,
            "total_triples": 100,
            "distinct_subjects": 10,
            "entity_types": [{"uri": "http://x/Customer", "count": 10}],
        },
    )

    text = _text(await dataset_mcp.call("list_entity_types"))
    assert "Dataset: main.crm.customers" in text
    assert "Description: Customer master records." in text


@pytest.mark.mcp
@pytest.mark.asyncio
async def test_describe_entity_includes_dataset_context(dataset_mcp):
    dataset_mcp.configure_selected_domain()
    await dataset_mcp.call("select_domain", domain_name="sales")
    dataset_mcp.route(
        "/api/v1/digitaltwin/triples/find",
        {
            "success": True,
            "seed_count": 1,
            "depth": 1,
            "total": 2,
            "triples": [
                {
                    "subject": "http://x/Customer/CUST001",
                    "predicate": (
                        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                    ),
                    "object": "http://x/Customer",
                },
                {
                    "subject": "http://x/Customer/CUST001",
                    "predicate": "http://x/name",
                    "object": "Ada",
                },
            ],
        },
    )

    text = _text(
        await dataset_mcp.call(
            "describe_entity",
            search="CUST001",
            entity_type="Customer",
            depth=1,
        )
    )
    assert "[Context — class: Customer]" in text
    assert "Dataset: main.crm.customers" in text
    assert "key: customer_id = 'CUST001'" in text
    assert "Description: Customer master records." in text


@pytest.mark.mcp
@pytest.mark.asyncio
async def test_get_entity_context_requests_and_formats_dataset_rows(dataset_mcp):
    dataset_mcp.configure_selected_domain()
    await dataset_mcp.call("select_domain", domain_name="sales")
    dataset_mcp.route(
        "/api/v1/digitaltwin/nodes/context",
        {
            "success": True,
            "entity_uri": "http://x/Customer/CUST001",
            "entity_local_id": "CUST001",
            "class_name": "Customer",
            "dataset": {
                "fullName": "main.crm.customers",
                "key_column": "customer_id",
                "description": "Customer master records.",
                "rows": [
                    {"customer_id": "CUST001", "name": "Ada"},
                    {"customer_id": "CUST001", "name": "Ada (archive)"},
                ],
            },
        },
    )

    text = _text(
        await dataset_mcp.call(
            "get_entity_context",
            entity_uri="http://x/Customer/CUST001",
            fetch_dataset_rows=True,
            dataset_row_limit=10,
        )
    )

    assert "Dataset: main.crm.customers" in text
    assert "Description: Customer master records." in text
    assert "Rows (2):" in text
    assert "customer_id: CUST001" in text
    assert "name: Ada" in text

    requests = dataset_mcp.requests_to("/api/v1/digitaltwin/nodes/context")
    assert len(requests) == 1
    params = requests[0].url.params
    assert params["entity_uri"] == "http://x/Customer/CUST001"
    assert params["fetch_dataset_rows"] == "true"
    assert params["dataset_row_limit"] == "10"
    assert params["follow_bridges"] == "false"
