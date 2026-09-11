"""Contracts for the ontology-only MCP policy (No Backend domains).

A "No Backend" domain exposes only ``describe_ontology`` over MCP: every
graph-dependent tool must be force-disabled in the persisted policy, whatever
the client submitted, so the surface cannot drift back to a graph tool.
"""

from __future__ import annotations

import pytest

from back.core.mcp_tools import (
    MCP_DOMAIN_TOOL_NAMES,
    MCP_DOMAIN_TOOLS,
    MCP_GRAPH_TOOL_NAMES,
    coerce_mcp_policy,
    force_graphless_policy,
)

pytestmark = pytest.mark.unit


def test_graph_tool_names_are_every_domain_tool_but_describe_ontology():
    assert MCP_GRAPH_TOOL_NAMES == MCP_DOMAIN_TOOL_NAMES - {"describe_ontology"}


def test_every_tool_declares_whether_it_needs_a_graph():
    for tool in MCP_DOMAIN_TOOLS:
        assert isinstance(tool["requires_graph"], bool)
    describe = next(t for t in MCP_DOMAIN_TOOLS if t["name"] == "describe_ontology")
    assert describe["requires_graph"] is False


def test_force_graphless_disables_every_graph_tool():
    policy = force_graphless_policy({})
    assert set(policy["disabled_tools"]) == set(MCP_GRAPH_TOOL_NAMES)


def test_force_graphless_keeps_describe_ontology_enabled():
    policy = force_graphless_policy({})
    assert "describe_ontology" not in policy["disabled_tools"]


def test_force_graphless_preserves_other_settings():
    policy = force_graphless_policy(
        {"disabled_tools": ["describe_ontology"], "context": {"actions": "disabled"}}
    )
    assert set(policy["disabled_tools"]) == set(MCP_GRAPH_TOOL_NAMES) | {
        "describe_ontology"
    }
    assert policy["context"] == {"actions": "disabled"}


def test_force_graphless_degrades_malformed_input():
    policy = force_graphless_policy("nonsense")
    assert set(policy["disabled_tools"]) == set(MCP_GRAPH_TOOL_NAMES)


def test_force_graphless_is_idempotent():
    once = force_graphless_policy({})
    assert force_graphless_policy(once) == once


def test_force_graphless_output_survives_recoercion():
    """The forced policy must stay valid through the normal coercion path."""
    policy = force_graphless_policy({})
    assert coerce_mcp_policy(policy) == policy
