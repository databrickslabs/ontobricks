"""MCP ``run_entity_business_rule`` tool and business-rule formatting."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
MCP_SRC = REPO_ROOT / "src" / "mcp-server"
if str(MCP_SRC) not in sys.path:
    sys.path.insert(0, str(MCP_SRC))

from tests.units.mcp.test_domain_policy_gating import (  # noqa: E402,F401
    FakeContext,
    _select,
    mcp_env,
)

_RULES_OFF = {"context": {"business_rules": "disabled"}}
_RULES = [
    {
        "name": "VipCustomer",
        "description": "Customers with an order become VIP",
        "antecedent": "Customer(?c) ^ hasOrder(?c, ?o)",
        "consequent": "VIP(?c)",
    }
]


@pytest.fixture(scope="module")
def fmt():
    try:
        from server import formatting  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - env without fastmcp
        pytest.skip(f"MCP server not importable: {exc}")
    return formatting


def test_node_context_lists_business_rules_with_hint(fmt) -> None:
    text = fmt._format_node_context_response(
        {
            "success": True,
            "entity_uri": "https://example.com/Customer/CUST001",
            "entity_local_id": "CUST001",
            "class_name": "Customer",
            "business_rules": _RULES,
        }
    )
    assert "Business rules (SWRL):" in text
    assert "VipCustomer" in text
    assert "Customer(?c) ^ hasOrder(?c, ?o) -> VIP(?c)" in text
    assert "run_entity_business_rule(entity_uri, rule)" in text


def test_class_context_block_mentions_business_rules(fmt) -> None:
    text = fmt._format_class_context_block(
        "CUST001", {"name": "Customer", "business_rules": _RULES}
    )
    assert "Business rules:" in text
    assert "run_entity_business_rule" in text


def test_business_rule_response_formatting(fmt) -> None:
    text = fmt._format_node_business_rule_response(
        {
            "success": True,
            "entity_local_id": "CUST001",
            "class_name": "Customer",
            "rule": "VipCustomer",
            "inferred_count": 1,
            "materialized_count": 1,
            "triples": [
                {
                    "subject": "https://example.com/Customer/CUST001",
                    "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                    "object": "https://example.com/onto#VIP",
                }
            ],
        }
    )
    assert "Inferred 1 triple, 1 written to the graph" in text
    assert "CUST001  type  VIP" in text


def test_business_rule_response_without_facts(fmt) -> None:
    text = fmt._format_node_business_rule_response(
        {"success": True, "rule": "VipCustomer", "inferred_count": 0}
    )
    assert "no new facts" in text


async def test_tool_posts_to_business_rule_endpoint(mcp_env) -> None:
    from server.constants import API_V1_DT_NODE_BUSINESS_RULE  # type: ignore[import-not-found]

    tools, state = mcp_env
    state["domains"] = [{"name": "d", "description": "", "mcp_policy": {}}]
    await _select(tools, "d")
    state["get_calls"].clear()

    await tools["run_entity_business_rule"](entity_uri="u", rule="VipCustomer")
    assert state["get_calls"] == [API_V1_DT_NODE_BUSINESS_RULE]


async def test_disabled_element_refuses_without_backend_call(mcp_env) -> None:
    tools, state = mcp_env
    state["domains"] = [{"name": "d", "description": "", "mcp_policy": _RULES_OFF}]
    await _select(tools, "d", FakeContext())
    state["get_calls"].clear()

    message = await tools["run_entity_business_rule"](entity_uri="u", rule="r")
    assert "Business rules are disabled" in message
    assert state["get_calls"] == []
