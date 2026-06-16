"""Parametrized MCP tool tests (T-M3 / M2.P6 expansion).

Runs the same shape-checks across **every** registered MCP tool, so when new
tools are added the suite covers them automatically without per-tool boilerplate.

This complements `test_tool_schemas.py` (which asserts the marquee tools are
present) and `test_smoke_tools.py` (which exercises a few happy paths). Together
they form the "schema + happy + parametrized" coverage shape called for in
CNS §9.5 T-M3.
"""

from __future__ import annotations

import pytest


@pytest.mark.mcp
@pytest.mark.asyncio
class TestEveryToolSchema:
    """Every registered tool — not just the marquee set — has a valid schema."""

    async def test_every_tool_has_a_non_empty_name(self, mcp_client):
        for name in await mcp_client.list_tools():
            assert isinstance(name, str)
            assert len(name) > 0

    async def test_every_tool_name_is_snake_case(self, mcp_client):
        """MCP convention: tool names use snake_case for cross-client compatibility."""
        bad = []
        for name in await mcp_client.list_tools():
            # Allow lowercase ASCII + underscores + digits.
            if not name.replace("_", "").replace(".", "").isalnum():
                bad.append(name)
            if name != name.lower():
                bad.append(name)
        assert not bad, f"non-snake-case tool names: {bad}"

    async def test_every_tool_schema_has_properties_or_no_args(self, mcp_client):
        """A tool either declares input properties or accepts no args."""
        bad = []
        for name in await mcp_client.list_tools():
            schema = await mcp_client.schema(name)
            if not isinstance(schema, dict):
                bad.append((name, "schema not dict"))
                continue
            props = schema.get("properties", {})
            # No-arg tools may omit `properties` entirely.
            if "properties" in schema and not isinstance(props, dict):
                bad.append((name, "properties not dict"))
        assert not bad, f"bad schemas: {bad}"


@pytest.mark.mcp
@pytest.mark.asyncio
class TestToolGroupsRepresented:
    """The 4-click pipeline (CNS §4.5) needs at least one tool per group."""

    async def test_registry_group_has_tools(self, mcp_client):
        tools = await mcp_client.list_tools()
        registry_tools = {"list_domains", "list_domain_versions", "select_domain"}
        present = registry_tools & set(tools)
        assert present, f"no registry-group tools registered. Have: {tools}"

    async def test_entity_group_has_tools(self, mcp_client):
        tools = await mcp_client.list_tools()
        entity_tools = {"list_entity_types", "describe_entity", "search_entities"}
        present = entity_tools & set(tools)
        # Some installs may have just one (describe_entity) — accept any.
        assert present, f"no entity-group tools registered. Have: {tools}"

    async def test_design_status_present(self, mcp_client):
        tools = set(await mcp_client.list_tools())
        # design-status is the gating check for "is the domain ready?".
        assert "get_design_status" in tools or "get_status" in tools, (
            f"neither get_design_status nor get_status registered: {sorted(tools)}"
        )


@pytest.mark.mcp
@pytest.mark.asyncio
class TestSchemaTypes:
    """If a tool declares a `type`, it must be 'object' (MCP convention)."""

    async def test_type_is_object_when_declared(self, mcp_client):
        bad = []
        for name in await mcp_client.list_tools():
            schema = await mcp_client.schema(name)
            t = schema.get("type")
            if t is not None and t != "object":
                bad.append((name, t))
        assert not bad, f"non-object tool input types: {bad}"

    async def test_required_field_is_a_list_when_present(self, mcp_client):
        bad = []
        for name in await mcp_client.list_tools():
            schema = await mcp_client.schema(name)
            if "required" in schema and not isinstance(schema["required"], list):
                bad.append((name, type(schema["required"]).__name__))
        assert not bad, f"required must be a list: {bad}"

    async def test_required_fields_appear_in_properties(self, mcp_client):
        """Required field names must reference a declared property."""
        bad = []
        for name in await mcp_client.list_tools():
            schema = await mcp_client.schema(name)
            required = schema.get("required", []) or []
            props = (schema.get("properties") or {}).keys()
            for r in required:
                if r not in props:
                    bad.append((name, r))
        assert not bad, f"required references missing property: {bad}"
