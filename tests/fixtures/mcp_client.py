"""In-process MCP client fixture — exercise MCP tools without HTTP.

OntoBricks ships an MCP server (`src/mcp-server/server/app.py`) with multiple
tool registrations. For integration tests we want to invoke those tools
directly against the registered handlers rather than spinning up FastMCP HTTP.

FastMCP v2 surface used here:
- `mcp.list_tools()` (async) → list of Tool objects (each has `.name`,
  `.description`, `.parameters` JSON schema, `.fn` handler).
- `mcp.get_tool(name)` (async) → single Tool by name.
- `mcp.call_tool(name, args)` (async) → invokes a tool, returns the result.

If the MCP server module can't be imported (e.g., missing `fastmcp` in env),
the fixtures skip the test rather than erroring.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any
import pytest


# Make src/mcp-server importable as `server.*`.
_REPO_ROOT = Path(__file__).resolve().parents[2]
_MCP_SRC = _REPO_ROOT / "src" / "mcp-server"
if str(_MCP_SRC) not in sys.path:
    sys.path.insert(0, str(_MCP_SRC))


class InProcessMCPClient:
    """Async client that invokes MCP tools registered on a FastMCP app in-process.

    Usage (inside an async test):

        tools = await client.list_tools()
        assert "list_domains" in tools
        schema = await client.schema("list_domains")
        result = await client.call("list_domains")
    """

    def __init__(self, app: Any) -> None:
        self.app = app

    async def list_tools(self) -> list[str]:
        tools = await self.app.list_tools()
        return sorted(getattr(t, "name", str(t)) for t in tools)

    async def get_tool(self, tool_name: str) -> Any:
        return await self.app.get_tool(tool_name)

    async def schema(self, tool_name: str) -> dict[str, Any]:
        tool = await self.app.get_tool(tool_name)
        # FastMCP Tool objects expose `.parameters` as a JSON schema dict.
        params = getattr(tool, "parameters", None)
        if params is None and hasattr(tool, "input_schema"):
            params = tool.input_schema
        return dict(params) if params else {}

    async def call(self, tool_name: str, **kwargs: Any) -> Any:
        """Invoke `tool_name` with `kwargs` as input.

        Returns whatever the tool's handler returned (FastMCP v2 may wrap this
        in a `ToolResult`; callers should accept either shape).
        """
        return await self.app.call_tool(tool_name, kwargs)


@pytest.fixture
def mcp_app():
    """Import and return a configured MCP app instance.

    Skips the test if `fastmcp` or the MCP server module is missing in the env.
    Standalone mode is used (no Databricks Apps wiring).
    """
    try:
        from server.app import create_mcp_server  # type: ignore[import-not-found]
    except ImportError as exc:
        pytest.skip(f"MCP server not importable: {exc}")
    return create_mcp_server(mode="standalone")


@pytest.fixture
def mcp_client(mcp_app):
    """In-process MCP client bound to the production MCP app."""
    return InProcessMCPClient(mcp_app)
