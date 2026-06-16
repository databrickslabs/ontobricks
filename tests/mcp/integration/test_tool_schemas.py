"""MCP tool schema tests (T-M3 under CNS).

These run as part of G1c (the MCP integration gate) — they assert that every
registered tool has a parseable JSON schema with input parameters declared.
This is the backward-compat foundation: future schema changes show up as
snapshot diffs once we add the syrupy-based snapshot test alongside this.
"""

from __future__ import annotations

import pytest


@pytest.mark.mcp
@pytest.mark.asyncio
async def test_mcp_app_imports_and_registers_tools(mcp_client):
    """Sanity: the production MCP app loads and registers at least one tool.

    If this fails, every other MCP test will be skipped — make it the canary.
    """
    tools = await mcp_client.list_tools()
    assert isinstance(tools, list)
    assert len(tools) >= 1, "MCP app registered zero tools"


@pytest.mark.mcp
@pytest.mark.asyncio
async def test_expected_core_tools_registered(mcp_client):
    """The marquee tools from the four-click pipeline are all present.

    These names are part of OntoBricks' public MCP contract — removing one is
    a breaking change for downstream LLM clients (Databricks Playground,
    Cursor, Claude Desktop).
    """
    tools = set(await mcp_client.list_tools())
    expected = {
        "list_domains",
        "list_domain_versions",
        "get_design_status",
        "select_domain",
        "list_entity_types",
        "describe_entity",
        "get_status",
    }
    missing = expected - tools
    assert not missing, f"missing MCP tools: {missing}"


@pytest.mark.mcp
@pytest.mark.asyncio
async def test_every_tool_has_parameter_schema(mcp_client):
    """No tool may ship without a JSON schema for its inputs (even {} is fine)."""
    tools = await mcp_client.list_tools()
    for name in tools:
        schema = await mcp_client.schema(name)
        # FastMCP emits a JSON-Schema-like dict; even no-arg tools get a
        # `properties: {}` shape.
        assert isinstance(schema, dict), f"{name}: schema is not a dict"


@pytest.mark.mcp
@pytest.mark.asyncio
async def test_tool_schemas_have_consistent_shape(mcp_client):
    """Every schema either omits 'type' or declares 'type: object'."""
    tools = await mcp_client.list_tools()
    bad: list[tuple[str, str]] = []
    for name in tools:
        schema = await mcp_client.schema(name)
        t = schema.get("type")
        if t not in (None, "object"):
            bad.append((name, str(t)))
    assert not bad, f"non-object schemas: {bad}"


@pytest.mark.mcp
@pytest.mark.asyncio
async def test_tool_names_are_unique(mcp_client):
    tools = await mcp_client.list_tools()
    assert len(tools) == len(set(tools)), "duplicate tool names registered"
