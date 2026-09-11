"""MCP directive-emphasis contracts for the per-domain context policy.

``preferred`` must change the wording of the follow-up hint the model reads,
and nothing else: the payload, the section order and the neutral wording of
the other elements stay untouched.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
MCP_SRC = REPO_ROOT / "src" / "mcp-server"

if str(MCP_SRC) not in sys.path:
    sys.path.insert(0, str(MCP_SRC))


@pytest.fixture(scope="module")
def formatters():
    """Import formatters; skip if MCP deps (e.g. fastmcp) are unavailable."""
    try:
        from server.app import (  # type: ignore[import-not-found]
            REGISTRY_TOOLS,
            _format_class_context_block,
            _format_node_context_response,
            _preferred,
        )
    except ImportError as exc:
        pytest.skip(f"MCP server not importable: {exc}")
    return (
        _format_class_context_block,
        _format_node_context_response,
        _preferred,
        REGISTRY_TOOLS,
    )


_CLS = {
    "name": "Customer",
    "dataset": {
        "fullName": "main.crm.customers",
        "key_column": "customer_id",
        "description": "Customer master records.",
    },
    "bridges": [
        {
            "target_domain": "finance",
            "target_class_name": "Contract",
            "label": "Owns contracts",
        }
    ],
    "actions": [
        {"fullName": "main.ops.recompute_risk", "description": "Recompute risk"}
    ],
}

_NODE = {
    "success": True,
    "entity_uri": "https://example.com/Customer/CUST001",
    "entity_local_id": "CUST001",
    "class_name": "Customer",
    "dataset": {"fullName": "main.crm.customers", "key_column": "id"},
    "bridges": [{"target_domain": "finance", "target_class_name": "Contract"}],
    "actions": [{"fullName": "main.ops.recompute_risk"}],
}


def test_preferred_helper_only_matches_the_named_feature(formatters) -> None:
    _, _, preferred, _ = formatters
    assert preferred({"bridges": "preferred"}, "bridges")
    assert not preferred({"bridges": "preferred"}, "dataset")
    assert not preferred({"bridges": "normal"}, "bridges")
    assert not preferred(None, "bridges")


def test_registry_tools_mirror_the_app_catalog(formatters) -> None:
    """The MCP process cannot import the app package — keep the lists equal."""
    from back.core.mcp_tools import MCP_REGISTRY_TOOLS

    _, _, _, registry_tools = formatters
    assert set(registry_tools) == set(MCP_REGISTRY_TOOLS)


# ---------------------------------------------------------------------------
# Class context block (describe_entity)


def test_class_block_hints_are_neutral_without_a_policy(formatters) -> None:
    format_class, _, _, _ = formatters
    text = format_class("CUST1", _CLS)
    assert "ALWAYS" not in text
    assert "PREFER" not in text
    assert "→ call get_entity_context(fetch_dataset_rows=True) to retrieve rows" in text


@pytest.mark.parametrize(
    "feature,marker",
    [
        ("dataset", "ALWAYS call get_entity_context(fetch_dataset_rows=True)"),
        ("bridges", "ALWAYS follow these bridges"),
        ("actions", "PREFER invoke_entity_action"),
    ],
)
def test_class_block_hint_becomes_directive_when_preferred(
    formatters, feature, marker
) -> None:
    format_class, _, _, _ = formatters
    text = format_class("CUST1", _CLS, {feature: "preferred"})
    assert marker in text


def test_preferring_one_element_leaves_the_others_neutral(formatters) -> None:
    format_class, _, _, _ = formatters
    text = format_class("CUST1", _CLS, {"bridges": "preferred"})
    assert "ALWAYS follow these bridges" in text
    assert "→ call get_entity_context(fetch_dataset_rows=True) to retrieve rows" in text
    assert "→ call invoke_entity_action(entity_uri, action) to run one" in text


def test_emphasis_does_not_reorder_sections(formatters) -> None:
    """Preferred is wording only — Dataset still precedes Bridges."""
    format_class, _, _, _ = formatters
    text = format_class("CUST1", _CLS, {"bridges": "preferred"})
    assert text.index("Dataset:") < text.index("Bridges:") < text.index("Actions:")


# ---------------------------------------------------------------------------
# Node context response (get_entity_context)


def test_node_context_hints_are_neutral_without_a_policy(formatters) -> None:
    _, format_node, _, _ = formatters
    text = format_node(_NODE)
    assert "ALWAYS" not in text
    assert "PREFER" not in text


@pytest.mark.parametrize(
    "feature,marker",
    [
        ("bridges", "ALWAYS follow these bridges"),
        ("actions", "PREFER running one of these actions"),
    ],
)
def test_node_context_hint_becomes_directive_when_preferred(
    formatters, feature, marker
) -> None:
    _, format_node, _, _ = formatters
    assert marker in format_node(_NODE, {feature: "preferred"})


def test_node_context_omits_blocks_the_server_filtered_out(formatters) -> None:
    """Disabled elements never reach the formatter, so no block renders."""
    _, format_node, _, _ = formatters
    filtered = {k: v for k, v in _NODE.items() if k not in ("actions", "bridges")}
    text = format_node(filtered, {"actions": "disabled", "bridges": "disabled"})
    assert "Actions" not in text
    assert "Bridges" not in text
    assert "Dataset: main.crm.customers" in text
