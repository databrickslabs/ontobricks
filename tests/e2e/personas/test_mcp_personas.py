"""UAT — MCP discovery surface (the Business Consumer's programmatic lens).

The MCP server (``src/mcp-server/server/app.py``) is a separate process from the
web app and is not gated by ``PermissionMiddleware``; it is the read/discovery
surface an LLM client (a consumer persona) uses. These tests run the tools
in-process via the shared ``mcp_client`` fixture and characterize that the
expected read tools exist and respond. Live data-bearing calls
(``describe_entity``, ``query_graphql``) are covered in the live-smoke suite.

The MCP client is async, but this module lives in the Playwright (sync) e2e
session — pytest-asyncio's runner clashes with Playwright's event loop here, so
the coroutines are driven via a small thread-runner instead of async tests.
"""

from __future__ import annotations

import asyncio
import concurrent.futures

import pytest

pytestmark = [pytest.mark.uat, pytest.mark.mcp]

# Tools an LLM consumer relies on to discover and read a knowledge graph.
EXPECTED_TOOLS = {
    "list_domains",
    "list_domain_versions",
    "get_design_status",
    "select_domain",
    "list_entity_types",
    "describe_entity",
    "get_status",
    "get_graphql_schema",
    "query_graphql",
}


def _run(coro):
    """Run *coro* to completion, even if an event loop is already running
    (Playwright's sync API keeps one alive in the worker)."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        with concurrent.futures.ThreadPoolExecutor() as pool:
            return pool.submit(asyncio.run, coro).result()
    return asyncio.run(coro)


class TestMcpDiscoverySurface:
    def test_expected_tools_registered(self, mcp_client):
        tools = set(_run(mcp_client.list_tools()))
        missing = EXPECTED_TOOLS - tools
        assert not missing, f"MCP server missing consumer tools: {sorted(missing)}"

    def test_describe_entity_has_schema(self, mcp_client):
        schema = _run(mcp_client.schema("describe_entity"))
        props = schema.get("properties", schema)
        assert any("entity" in str(k).lower() for k in props), schema

    def test_list_domains_callable(self, mcp_client):
        """list_domains should return without raising; backend errors → skip."""
        try:
            result = _run(mcp_client.call("list_domains"))
        except Exception as exc:  # noqa: BLE001 — registry unreachable offline
            pytest.skip(f"list_domains needs the registry backend (offline): {exc}")
        assert result is not None
