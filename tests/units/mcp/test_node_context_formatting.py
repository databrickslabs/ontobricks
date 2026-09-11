"""MCP dataset description formatting contracts (behavioral)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
MCP_SRC = REPO_ROOT / "src" / "mcp-server"
# The response formatters were extracted from ``app.py`` into ``formatting.py``.
MCP_APP = MCP_SRC / "server" / "formatting.py"

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


def test_class_context_formatter_shows_bridge_target_description(formatters):
    """Class-context bridges must render the target-domain description
    and direct the LLM to `select_domain`, not `follow_bridges`."""
    format_class, _ = formatters
    text = format_class(
        "CUST1",
        {
            "name": "Customer",
            "bridges": [
                {
                    "target_domain": "finance",
                    "target_domain_description": "Finance ontology with contracts and payments",
                    "target_class_name": "Contract",
                    "label": "Owns contracts",
                }
            ],
        },
    )
    assert "Bridges:" in text
    assert "finance / Contract" in text
    assert "Target domain: Finance ontology with contracts and payments" in text
    assert "select_domain(<target_domain>)" in text
    # The legacy peek-only hint must be gone: follow_bridges is not the primary hop.
    assert "call get_entity_context(follow_bridges=True) to load cross-domain data" not in text


def test_class_context_formatter_omits_blank_target_description(formatters):
    format_class, _ = formatters
    text = format_class(
        "CUST1",
        {
            "name": "Customer",
            "bridges": [
                {
                    "target_domain": "finance",
                    "target_domain_description": "   ",
                    "target_class_name": "Contract",
                }
            ],
        },
    )
    assert "Bridges:" in text
    assert "Target domain:" not in text


def test_node_context_formatter_shows_bridge_target_description(formatters):
    _, format_node = formatters
    text = format_node(
        {
            "success": True,
            "entity_uri": "http://example.org/Customer/CUST1",
            "entity_local_id": "CUST1",
            "class_name": "Customer",
            "bridges": [
                {
                    "target_domain": "finance",
                    "target_domain_description": "Finance ontology with contracts and payments",
                    "target_class_name": "Contract",
                    "label": "Owns contracts",
                    "entities": None,
                }
            ],
        }
    )
    assert "Cross-domain Bridges:" in text
    assert "finance / Contract" in text
    assert "Target domain: Finance ontology with contracts and payments" in text
    assert "select_domain(<target_domain>)" in text
