"""Unit tests for ``agents.agent_dtwin_chat.tools``.

Exercises every tool handler in isolation by swapping in an
``httpx.MockTransport`` so no network I/O happens.  Validates:

- happy-path formatting of each tool's output,
- input-validation guards (empty query, dangerous SPARQL, bad JSON vars),
- HTTP-error surface (status codes are propagated into the tool message,
  5xx payloads are truncated and never re-raised).
"""

from __future__ import annotations

import json

import httpx
import pytest

from agents.agent_dtwin_chat import tools as chat_tools
from agents.tools.context import ToolContext


def _ctx(domain_name: str = "sales_demo") -> ToolContext:
    return ToolContext(
        host="https://test.databricks.com",
        token="test-token",
        dtwin_base_url="http://loopback.invalid",
        dtwin_domain_name=domain_name,
    )


@pytest.fixture
def patch_client(monkeypatch):
    """Yield a factory that installs an httpx MockTransport in chat_tools._client."""

    def _install(handler):
        def _factory(ctx):
            transport = httpx.MockTransport(handler)
            return httpx.Client(
                base_url=ctx.dtwin_base_url or "http://loopback.invalid",
                transport=transport,
                timeout=5,
                follow_redirects=False,
            )

        monkeypatch.setattr(chat_tools, "_client", _factory)

    return _install


# ---------------------------------------------------------------------------
# list_entity_types
# ---------------------------------------------------------------------------


class TestListEntityTypes:
    def test_happy_path(self, patch_client):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/dtwin/sync/stats"
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "total_triples": 12345,
                    "distinct_subjects": 100,
                    "distinct_predicates": 10,
                    "label_count": 100,
                    "type_assertion_count": 100,
                    "relationship_count": 80,
                    "entity_types": [
                        {"uri": "http://example.org/Customer", "count": 42},
                    ],
                    "top_predicates": [
                        {"uri": "http://example.org/hasOrder", "count": 7},
                    ],
                },
            )

        patch_client(handler)
        out = chat_tools.tool_list_entity_types(_ctx())
        assert "sales_demo" in out
        assert "Customer" in out
        assert "12,345" in out  # thousands-formatted
        # ``pretty_predicate`` splits camelCase → "has Order".
        assert "has Order" in out

    def test_backend_returns_failure(self, patch_client):
        patch_client(
            lambda _r: httpx.Response(
                200, json={"success": False, "message": "no domain"}
            )
        )
        assert chat_tools.tool_list_entity_types(_ctx()) == "no domain"

    def test_http_error_is_surfaced(self, patch_client):
        patch_client(lambda _r: httpx.Response(500, text="boom"))
        out = chat_tools.tool_list_entity_types(_ctx())
        assert "500" in out
        assert "boom" in out


# ---------------------------------------------------------------------------
# describe_entity
# ---------------------------------------------------------------------------


class TestDescribeEntity:
    def test_empty_args_returns_error(self, patch_client):
        patch_client(lambda _r: httpx.Response(500))  # should never be called
        out = chat_tools.tool_describe_entity(_ctx())
        assert json.loads(out)["error"].startswith("Please provide")

    def test_depth_is_clamped(self, patch_client):
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["params"] = dict(request.url.params)
            return httpx.Response(200, json={"success": True, "entities": []})

        patch_client(handler)
        chat_tools.tool_describe_entity(_ctx(), search="foo", depth=99)
        assert captured["params"]["depth"] == "10"

    def test_search_query_is_forwarded(self, patch_client):
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["params"] = dict(request.url.params)
            return httpx.Response(200, json={"success": True, "entities": []})

        patch_client(handler)
        chat_tools.tool_describe_entity(_ctx(), search="Jacob Martinez", depth=1)
        assert captured["params"]["search"] == "Jacob Martinez"


# ---------------------------------------------------------------------------
# get_status
# ---------------------------------------------------------------------------


class TestGetStatus:
    def test_happy_path(self, patch_client):
        patch_client(
            lambda _r: httpx.Response(
                200,
                json={
                    "success": True,
                    "view_table": "main.db.customer_view",
                    "graph_name": "customer_graph",
                    "has_data": True,
                    "count": 999,
                },
            )
        )
        out = chat_tools.tool_get_status(_ctx())
        assert "customer_view" in out
        assert "customer_graph" in out
        assert "999" in out

    def test_empty_payload_reports_unknown(self, patch_client):
        patch_client(lambda _r: httpx.Response(200, json={}))
        out = chat_tools.tool_get_status(_ctx())
        assert "unknown" in out.lower()


# ---------------------------------------------------------------------------
# get_graphql_schema
# ---------------------------------------------------------------------------


class TestGetGraphqlSchema:
    def test_no_domain_returns_error(self, patch_client):
        out = chat_tools.tool_get_graphql_schema(_ctx(domain_name=""))
        assert "No domain" in json.loads(out)["error"]

    def test_happy_path(self, patch_client):
        patch_client(
            lambda _r: httpx.Response(
                200,
                json={
                    "success": True,
                    "sdl": "type Query { hello: String }",
                },
            )
        )
        out = chat_tools.tool_get_graphql_schema(_ctx())
        assert "type Query" in out
        assert "sales_demo" in out

    def test_empty_sdl_reports_empty(self, patch_client):
        patch_client(lambda _r: httpx.Response(200, json={"success": True, "sdl": ""}))
        assert "empty" in chat_tools.tool_get_graphql_schema(_ctx()).lower()


# ---------------------------------------------------------------------------
# query_graphql
# ---------------------------------------------------------------------------


class TestQueryGraphql:
    def test_missing_query_returns_error(self, patch_client):
        out = chat_tools.tool_query_graphql(_ctx(), query="")
        assert json.loads(out)["error"].startswith("Missing")

    def test_bad_variables_json_returns_error(self, patch_client):
        out = chat_tools.tool_query_graphql(
            _ctx(), query="{ a }", variables="{not-json"
        )
        assert "Invalid JSON" in json.loads(out)["error"]

    def test_happy_path_forwards_variables_dict(self, patch_client):
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode())
            return httpx.Response(
                200,
                json={"data": {"hello": "world"}, "errors": None},
            )

        patch_client(handler)
        chat_tools.tool_query_graphql(
            _ctx(), query="{ hello }", variables={"x": 1}
        )
        assert captured["body"]["query"] == "{ hello }"
        assert captured["body"]["variables"] == {"x": 1}


# ---------------------------------------------------------------------------
# run_sparql
# ---------------------------------------------------------------------------


class TestRunSparql:
    def test_empty_query_returns_error(self, patch_client):
        out = chat_tools.tool_run_sparql(_ctx(), query="")
        assert json.loads(out)["error"].startswith("Missing")

    @pytest.mark.parametrize(
        "dangerous",
        [
            "DROP GRAPH <g>",
            "DELETE WHERE { ?s ?p ?o }",
            "INSERT DATA { :a :b :c }",
            "CLEAR ALL",
            "create graph <g>",  # case-insensitive
        ],
    )
    def test_mutating_queries_are_refused(self, patch_client, dangerous):
        patch_client(lambda _r: httpx.Response(500))  # never reached
        out = chat_tools.tool_run_sparql(_ctx(), query=dangerous)
        assert "Refusing" in json.loads(out)["error"]

    def test_bad_limit_returns_error(self, patch_client):
        out = chat_tools.tool_run_sparql(
            _ctx(), query="SELECT ?s WHERE { ?s ?p ?o }", limit="many"
        )
        assert "integer" in json.loads(out)["error"]

    def test_happy_path_formats_rows(self, patch_client):
        patch_client(
            lambda _r: httpx.Response(
                200,
                json={
                    "success": True,
                    "columns": ["s", "p"],
                    "rows": [["http://x/a", "http://x/b"]],
                },
            )
        )
        out = chat_tools.tool_run_sparql(
            _ctx(), query="SELECT ?s ?p WHERE { ?s ?p ?o } LIMIT 1"
        )
        assert "SPARQL Result" in out
        assert "Rows: 1" in out

    def test_failure_payload_is_reported(self, patch_client):
        patch_client(
            lambda _r: httpx.Response(
                200, json={"success": False, "message": "query failed"}
            )
        )
        assert chat_tools.tool_run_sparql(_ctx(), query="ASK { ?s ?p ?o }") == (
            "query failed"
        )


# ---------------------------------------------------------------------------
# Static tool-definition sanity (OpenAI chat-completions shape)
# ---------------------------------------------------------------------------


class TestToolDefinitions:
    def test_every_handler_has_a_definition(self):
        defined = {d["function"]["name"] for d in chat_tools.TOOL_DEFINITIONS}
        handled = set(chat_tools.TOOL_HANDLERS.keys())
        assert defined == handled

    def test_definitions_declare_required_fields(self):
        for d in chat_tools.TOOL_DEFINITIONS:
            assert d["type"] == "function"
            fn = d["function"]
            assert "name" in fn and "description" in fn
            params = fn["parameters"]
            assert params["type"] == "object"
            assert "properties" in params
