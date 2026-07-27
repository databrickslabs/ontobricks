"""MCP dataset description formatting contracts (behavioral)."""

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
            _format_node_context_response,
        )
    except ImportError as exc:
        pytest.skip(f"MCP server not importable: {exc}")
    return _format_class_context_block, _format_node_context_response


def test_class_context_formatter_includes_description(formatters):
    format_class, _ = formatters
    text = format_class(
        "CUST1",
        {
            "name": "Customer",
            "dataset": {
                "fullName": "main.crm.customers",
                "key_column": "customer_id",
                "description": "Customer master records used for account enrichment.",
            },
            "bridges": [],
        },
    )
    assert "Dataset: main.crm.customers" in text
    assert "Description: Customer master records used for account enrichment." in text


def test_class_context_formatter_omits_blank_description(formatters):
    format_class, _ = formatters
    text = format_class(
        "CUST1",
        {
            "name": "Customer",
            "dataset": {
                "fullName": "main.crm.customers",
                "key_column": "customer_id",
                "description": "   ",
            },
            "bridges": [],
        },
    )
    assert "Description:" not in text


def test_node_context_formatter_includes_description(formatters):
    _, format_node = formatters
    text = format_node(
        {
            "success": True,
            "entity_uri": "http://example.org/Customer/CUST1",
            "entity_local_id": "CUST1",
            "class_name": "Customer",
            "dataset": {
                "fullName": "main.crm.customers",
                "key_column": "customer_id",
                "description": "Customer master records.",
            },
            "bridges": [],
        }
    )
    assert "Dataset: main.crm.customers" in text
    assert "Description: Customer master records." in text


def test_source_still_reads_dataset_description_field():
    """Static guard: formatters must keep reading dataset.description."""
    source = MCP_APP.read_text(encoding="utf-8")
    assert 'dataset.get("description")' in source
    assert 'f"  Description: {purpose}"' in source
    assert 'f"    Description: {desc}"' in source
