"""Per-domain MCP policy catalog and coercion contracts."""

from __future__ import annotations

from back.core.mcp_tools import (
    MCP_CONTEXT_FEATURE_NAMES,
    MCP_CONTEXT_FEATURES,
    MCP_CONTEXT_MODES,
    MCP_DOMAIN_TOOL_NAMES,
    MCP_REGISTRY_TOOLS,
    coerce_mcp_policy,
)


def test_registry_and_domain_tool_sets_are_disjoint() -> None:
    """A tool is either always-on or configurable, never both."""
    assert not (MCP_REGISTRY_TOOLS & MCP_DOMAIN_TOOL_NAMES)


def test_empty_policy_is_the_default_behaviour() -> None:
    """No policy means every tool exposed and every element normal.

    Consumers read a missing key as the default, so the empty blob must carry
    neither a disabled tool nor a context entry.
    """
    policy = coerce_mcp_policy({})
    assert policy == {}
    assert policy.get("disabled_tools", []) == []
    for feature in MCP_CONTEXT_FEATURE_NAMES:
        assert policy.get("context", {}).get(feature, "normal") == "normal"


def test_malformed_policy_degrades_to_empty() -> None:
    """A hand-edited registry row must not break domain loading."""
    for raw in (None, "nonsense", 42, [], {"disabled_tools": "query_graphql"}):
        assert coerce_mcp_policy(raw) == {}


def test_registry_tools_cannot_be_disabled() -> None:
    """They run before a domain is selected, so no policy may hide them."""
    policy = coerce_mcp_policy(
        {"disabled_tools": sorted(MCP_REGISTRY_TOOLS) + ["query_graphql"]}
    )
    assert policy["disabled_tools"] == ["query_graphql"]


def test_unknown_tool_names_are_dropped() -> None:
    policy = coerce_mcp_policy({"disabled_tools": ["query_graphql", "not_a_tool"]})
    assert policy["disabled_tools"] == ["query_graphql"]


def test_context_defaults_are_not_persisted() -> None:
    """Only non-default modes are stored, so a default policy stays empty."""
    policy = coerce_mcp_policy(
        {"context": {"dataset": "normal", "bridges": "preferred"}}
    )
    assert policy == {"context": {"bridges": "preferred"}}


def test_unknown_context_feature_dropped_and_bad_mode_falls_back() -> None:
    policy = coerce_mcp_policy(
        {"context": {"not_a_feature": "disabled", "actions": "banana"}}
    )
    # actions coerces to the default, which is then not persisted, so the
    # consumer's default lookup yields "normal".
    assert policy == {}
    assert policy.get("context", {}).get("actions", "normal") == "normal"


def test_coercion_is_idempotent() -> None:
    raw = {
        "disabled_tools": ["query_graphql", "query_graphql"],
        "context": {"actions": "disabled"},
    }
    once = coerce_mcp_policy(raw)
    assert coerce_mcp_policy(once) == once
    assert once["disabled_tools"] == ["query_graphql"]


def test_context_modes_are_the_three_documented_states() -> None:
    assert set(MCP_CONTEXT_MODES) == {"preferred", "normal", "disabled"}


def test_virtual_attributes_is_a_configurable_context_element() -> None:
    """Computing one costs a warehouse round-trip, so a domain must be able to
    push it (preferred) or take it off the table entirely (disabled)."""
    assert "virtual_attributes" in MCP_CONTEXT_FEATURE_NAMES

    policy = coerce_mcp_policy({"context": {"virtual_attributes": "disabled"}})
    assert policy == {"context": {"virtual_attributes": "disabled"}}
    assert coerce_mcp_policy({"context": {"virtual_attributes": "preferred"}}) == {
        "context": {"virtual_attributes": "preferred"}
    }


def test_every_context_element_is_labelled_and_described() -> None:
    """The MCP settings panel renders the catalog directly, so a new element
    without a label would ship as a blank checkbox."""
    for feature in MCP_CONTEXT_FEATURES:
        assert feature["label"].strip()
        assert feature["description"].strip()
