"""MCP class-action formatting contracts (behavioral)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
MCP_SRC = REPO_ROOT / "src" / "mcp-server"
MCP_APP = MCP_SRC / "server" / "app.py"

if str(MCP_SRC) not in sys.path:
    sys.path.insert(0, str(MCP_SRC))


@pytest.fixture(scope="module")
def formatters():
    """Import formatters; skip if MCP deps (e.g. fastmcp) are unavailable."""
    try:
        from server.app import (  # type: ignore[import-not-found]
            _format_class_context_block,
            _format_node_action_response,
            _format_node_context_response,
        )
    except ImportError as exc:
        pytest.skip(f"MCP server not importable: {exc}")
    return (
        _format_class_context_block,
        _format_node_context_response,
        _format_node_action_response,
    )


_ACTIONS = [
    {
        "fullName": "main.ops.recompute_risk",
        "function": "recompute_risk",
        "description": "Recompute the customer risk score",
        "returns_table": False,
    }
]


def test_class_context_block_lists_actions(formatters):
    format_class, _, _ = formatters
    text = format_class(
        "CUST1",
        {"name": "Customer", "dataset": None, "bridges": [], "actions": _ACTIONS},
    )
    assert "Actions:" in text
    assert "main.ops.recompute_risk: Recompute the customer risk score" in text
    assert "invoke_entity_action" in text


def test_node_context_response_lists_actions(formatters):
    _, format_node, _ = formatters
    text = format_node(
        {
            "success": True,
            "entity_uri": "http://example.org/Customer/CUST1",
            "entity_local_id": "CUST1",
            "class_name": "Customer",
            "actions": _ACTIONS,
        }
    )
    assert "Actions (Unity Catalog functions):" in text
    assert "main.ops.recompute_risk" in text
    assert "Description: Recompute the customer risk score" in text
    assert "invoke_entity_action" in text


def test_action_response_formats_scalar_result(formatters):
    _, _, format_action = formatters
    text = format_action(
        {
            "success": True,
            "entity_local_id": "CUST1",
            "class_name": "Customer",
            "action": "main.ops.recompute_risk",
            "rows": [{"result": "72"}],
        }
    )
    assert "Action: main.ops.recompute_risk" in text
    assert "Entity: CUST1  (Customer)" in text
    assert "result: 72" in text


def test_action_response_formats_table_rows(formatters):
    _, _, format_action = formatters
    text = format_action(
        {
            "success": True,
            "entity_local_id": "CUST1",
            "class_name": "Customer",
            "action": "main.ops.risk_history",
            "returns_table": True,
            "rows": [{"day": "2026-01-01", "score": 42}, {"day": "2026-01-02", "score": 43}],
        }
    )
    assert "Result (2 rows):" in text
    assert "day: 2026-01-01" in text
    assert "score: 43" in text


def test_action_response_surfaces_failure_message(formatters):
    _, _, format_action = formatters
    text = format_action({"success": False, "message": "not configured on class"})
    assert text == "not configured on class"


def test_empty_class_context_block_stays_empty(formatters):
    format_class, _, _ = formatters
    assert format_class("CUST1", {"name": "Customer", "dataset": None, "bridges": []}) == ""
