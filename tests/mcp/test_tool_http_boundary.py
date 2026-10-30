"""Every MCP tool and resource crosses the shared ``http_client`` boundary.

Locks two contracts end to end, through the real ``_get`` / ``_post`` and an
``httpx.MockTransport``:

* the GraphQL tools/resource forward the end-user identity headers and
  URL-encode the domain name into the ``/graphql/{domain}`` path;
* an HTTP failure reaches the model as a short message — never the request
  URL (which carries the registry query params) or a raw response body.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Callable
from pathlib import Path

import httpx
import pytest

_MCP_SRC = Path(__file__).resolve().parents[2] / "src" / "mcp-server"
if str(_MCP_SRC) not in sys.path:
    sys.path.insert(0, str(_MCP_SRC))

from fastmcp import FastMCP  # noqa: E402
from server import http_client as hc  # noqa: E402
from server.resources import register_resources  # noqa: E402
from server.session import MCPServerSession  # noqa: E402
from server.tools import register_tools  # noqa: E402

pytestmark = pytest.mark.mcp

_DOMAIN = "sales/emea q"
_ENCODED = "sales%2Femea%20q"
_LEAKY_BODY = "<html>Internal error at http://test.local/api?registry_catalog=cat</html>"


class _Harness:
    def __init__(self, monkeypatch) -> None:
        self.routes: dict[tuple[str, str], httpx.Response] = {}
        # When set, answers every request (failure-path tests).
        self.responder: Callable[[httpx.Request], httpx.Response] | None = None
        self.requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            self.requests.append(request)
            if self.responder is not None:
                return self.responder(request)
            key = (request.method, request.url.raw_path.split(b"?")[0].decode())
            return self.routes.get(key) or httpx.Response(404, json={"error": "no route"})

        transport = httpx.MockTransport(handler)
        real_client = httpx.AsyncClient

        class _Client(real_client):  # type: ignore[misc, valid-type]
            def __init__(self, *args, **kwargs):
                kwargs["transport"] = transport
                super().__init__(*args, **kwargs)

        monkeypatch.setattr(httpx, "AsyncClient", _Client)
        monkeypatch.setattr(hc, "_base_url", lambda mode: "http://test.local")
        monkeypatch.setattr(hc, "_get_auth_headers", lambda mode: {"Authorization": "Bearer M2M"})
        monkeypatch.setattr(hc, "_RETRY_DELAYS", ())

        self.session = MCPServerSession("standalone")
        self.session.registry.update(catalog="cat", schema="sch", _loaded=True)
        self.session.selected_domain_name = _DOMAIN
        self.mcp = FastMCP("test")
        register_tools(self.mcp, self.session)
        register_resources(self.mcp, self.session)

    async def tool(self, name: str):
        return {t.name: t.fn for t in await self.mcp.list_tools()}[name]

    async def resource(self, uri: str):
        return {str(r.uri): r.fn for r in await self.mcp.list_resources()}[uri]


@pytest.fixture
def harness(monkeypatch):
    hc.set_forwarded_identity(email="user@example.com", user_token="USER-TOKEN")
    yield _Harness(monkeypatch)
    hc.clear_forwarded_identity()


def _assert_forwarded(request: httpx.Request) -> None:
    assert request.headers["x-ontobricks-acting-email"] == "user@example.com"
    assert request.headers["x-ontobricks-acting-token"] == "USER-TOKEN"
    assert request.headers["authorization"] == "Bearer M2M"
    assert "x-forwarded-email" not in request.headers
    assert "x-forwarded-access-token" not in request.headers


def _assert_safe(text: str) -> None:
    assert "http://" not in text
    assert "registry_catalog" not in text
    assert "<html>" not in text


# ---------------------------------------------------------------------------
# GraphQL identity forwarding + path encoding


async def test_get_graphql_schema_forwards_identity_and_encodes_name(harness) -> None:
    harness.routes[("GET", f"/graphql/{_ENCODED}/schema")] = httpx.Response(
        200, json={"sdl": "type Query { a: Int }"}
    )

    text = await (await harness.tool("get_graphql_schema"))()

    assert "type Query { a: Int }" in text
    request = next(r for r in harness.requests if b"/graphql/" in r.url.raw_path)
    _assert_forwarded(request)
    assert request.url.raw_path.startswith(f"/graphql/{_ENCODED}/schema?".encode())
    assert request.url.params["registry_catalog"] == "cat"


async def test_query_graphql_forwards_identity_and_encodes_name(harness) -> None:
    harness.routes[("POST", f"/graphql/{_ENCODED}")] = httpx.Response(
        200, json={"data": {"allCustomer": [{"id": "c1"}]}}
    )

    text = await (await harness.tool("query_graphql"))(query="{ allCustomer { id } }")

    assert "allCustomer" in text
    request = next(r for r in harness.requests if b"/graphql/" in r.url.raw_path)
    _assert_forwarded(request)
    assert request.url.raw_path.startswith(f"/graphql/{_ENCODED}?".encode())
    assert request.url.params["registry_schema"] == "sch"
    assert json.loads(request.content) == {"query": "{ allCustomer { id } }"}


async def test_graphql_schema_resource_forwards_identity_and_encodes_name(harness) -> None:
    harness.routes[("GET", f"/graphql/{_ENCODED}/schema")] = httpx.Response(
        200, json={"sdl": "type Query { a: Int }"}
    )

    payload = json.loads(await (await harness.resource("ontobricks://graphql-schema"))())

    assert payload["sdl"] == "type Query { a: Int }"
    (request,) = harness.requests
    _assert_forwarded(request)
    assert request.url.raw_path.startswith(f"/graphql/{_ENCODED}/schema?".encode())


# ---------------------------------------------------------------------------
# Safe error text


async def test_query_graphql_still_surfaces_query_errors(harness) -> None:
    """A 400 carrying GraphQL ``errors`` is what the model needs to fix its query."""
    harness.routes[("POST", f"/graphql/{_ENCODED}")] = httpx.Response(
        400, json={"errors": [{"message": "Cannot query field 'x'"}]}
    )

    text = await (await harness.tool("query_graphql"))(query="{ x }")

    assert "GraphQL errors" in text
    assert "Cannot query field 'x'" in text


@pytest.mark.parametrize(
    "tool_name,kwargs",
    [
        ("get_graphql_schema", {}),
        ("query_graphql", {"query": "{ a }"}),
        ("list_domains", {}),
        ("list_domain_versions", {"domain_name": "d"}),
        ("get_design_status", {}),
        ("describe_ontology", {}),
        ("list_entity_types", {}),
        ("describe_entity", {"search": "x"}),
        ("get_status", {}),
        ("get_entity_context", {"entity_uri": "https://example.com/C/1"}),
    ],
)
async def test_http_error_yields_a_short_safe_message(harness, tool_name, kwargs) -> None:
    harness.responder = _leaky_500

    text = await (await harness.tool(tool_name))(**kwargs)

    _assert_safe(text)
    assert "(HTTP 500)" in text


async def test_select_domain_http_error_is_safe(harness) -> None:
    harness.responder = _leaky_500

    class _Ctx:
        async def reset_visibility(self) -> None: ...
        async def disable_components(self, **_kw) -> None: ...

    text = await (await harness.tool("select_domain"))(domain_name="d", ctx=_Ctx())

    _assert_safe(text)
    assert "(HTTP 500)" in text


async def test_backend_safe_message_is_passed_through(harness) -> None:
    harness.responder = lambda r: httpx.Response(
        404, json={"success": False, "message": "Domain 'd' not found"}
    )

    text = await (await harness.tool("list_domain_versions"))(domain_name="d")

    assert text == "Domain 'd' not found"


async def test_transport_error_yields_a_short_safe_message(harness) -> None:
    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError(f"cannot reach {request.url}", request=request)

    harness.responder = boom

    text = await (await harness.tool("list_entity_types"))()

    _assert_safe(text)
    assert "unreachable" in text


async def test_graphql_schema_resource_error_is_safe(harness) -> None:
    harness.responder = _leaky_500

    payload = json.loads(await (await harness.resource("ontobricks://graphql-schema"))())

    _assert_safe(payload["error"])
    assert "(HTTP 500)" in payload["error"]


@pytest.mark.parametrize(
    "uri", ["ontobricks://domains", "ontobricks://status", "ontobricks://stats"]
)
async def test_json_resource_error_is_safe(harness, uri) -> None:
    harness.responder = _leaky_500

    payload = json.loads(await (await harness.resource(uri))())

    _assert_safe(payload["error"])
    assert "(HTTP 500)" in payload["error"]


def _leaky_500(request: httpx.Request) -> httpx.Response:
    return httpx.Response(500, text=_LEAKY_BODY)
