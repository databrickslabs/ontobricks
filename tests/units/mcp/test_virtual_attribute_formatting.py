"""MCP virtual attribute formatting contracts (behavioral)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
MCP_SRC = REPO_ROOT / "src" / "mcp-server"

if str(MCP_SRC) not in sys.path:
    sys.path.insert(0, str(MCP_SRC))


_DECLARED = [
    {
        "fullName": "main.kg.customer_risk",
        "function": "customer_risk",
        "description": "Live credit risk",
        "returns_table": True,
        "attributes": [
            {"name": "risk_score", "column": "risk_score", "dataType": "DOUBLE"},
            {"name": "risk_band", "column": "risk_band"},
        ],
    }
]


def _computed(values=None, **extra):
    group = dict(_DECLARED[0])
    group["values"] = values if values is not None else {"risk_score": 0.82, "risk_band": "B"}
    group.update(extra)
    return [group]


@pytest.fixture(scope="module")
def formatters():
    """Import formatters; skip if MCP deps (e.g. fastmcp) are unavailable."""
    try:
        from server.app import (  # type: ignore[import-not-found]
            _format_class_context_block,
            _format_node_context_response,
        )
    except ImportError as exc:
        pytest.skip(f"MCP server not importable: {exc}")
    return _format_class_context_block, _format_node_context_response


def _node(groups, **extra):
    payload = {
        "success": True,
        "entity_uri": "http://example.org/Customer/CUST1",
        "entity_local_id": "CUST1",
        "class_name": "Customer",
        "virtual_attributes": groups,
    }
    payload.update(extra)
    return payload


class TestNodeContext:
    def test_declaration_only_names_them_and_asks_for_the_computation(
        self, formatters
    ):
        _, format_node = formatters
        text = format_node(_node(_DECLARED))

        assert "Virtual Attributes (computed on demand):" in text
        assert "risk_score  (DOUBLE)  — not computed" in text
        assert "compute_virtual_attributes=True" in text

    def test_computed_values_replace_the_placeholder_and_drop_the_hint(
        self, formatters
    ):
        _, format_node = formatters
        text = format_node(_node(_computed()))

        assert "risk_score = 0.82  (DOUBLE)" in text
        assert "not computed" not in text
        # Repeating the hint would ask the model to redo work it just did.
        assert "compute_virtual_attributes=True" not in text

    def test_failed_group_surfaces_the_error_without_the_hint(self, formatters):
        _, format_node = formatters
        text = format_node(_node(_computed(values={}, error="PERMISSION_DENIED")))

        assert "computation failed: PERMISSION_DENIED" in text
        assert "compute_virtual_attributes=True" not in text

    def test_preferred_policy_uses_the_directive_hint(self, formatters):
        _, format_node = formatters
        text = format_node(
            _node(_DECLARED), context_policy={"virtual_attributes": "preferred"}
        )

        assert "ALWAYS call get_entity_context" in text

    def test_absent_block_when_nothing_is_declared(self, formatters):
        _, format_node = formatters
        text = format_node(_node(None))

        assert "Virtual Attributes" not in text


class TestClassContext:
    def test_declarations_are_named_with_a_count(self, formatters):
        format_class, _ = formatters
        text = format_class(
            "CUST1", {"name": "Customer", "virtualAttributes": _DECLARED}
        )

        assert "Virtual attributes (2, computed on demand): risk_score, risk_band" in text
        assert "compute_virtual_attributes=True" in text

    def test_preferred_policy_uses_the_directive_hint(self, formatters):
        format_class, _ = formatters
        text = format_class(
            "CUST1",
            {"name": "Customer", "virtualAttributes": _DECLARED},
            context_policy={"virtual_attributes": "preferred"},
        )

        assert "ALWAYS call get_entity_context" in text

    def test_class_with_only_virtual_attributes_still_renders_a_block(self, formatters):
        """A class with no dataset, bridge or action must not fall through the
        early return that used to guard the three original attachments."""
        format_class, _ = formatters
        text = format_class(
            "CUST1",
            {"name": "Customer", "dataset": None, "bridges": [], "actions": [],
             "virtualAttributes": _DECLARED},
        )

        assert "risk_score" in text
