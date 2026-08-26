"""Drift guard between the MCP server's tool mirrors and the canonical catalog.

The MCP server process (``src/mcp-server/``) cannot import the app package, so
it re-declares ``REGISTRY_TOOLS`` and ``GRAPH_TOOLS`` by hand. These must track
``back.core.mcp_tools`` exactly, otherwise a domain-scoped tool could silently
escape the ontology-only restriction (or a registry tool get hidden).
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
def mirrors():
    try:
        from server.app import (  # type: ignore[import-not-found]
            GRAPH_TOOLS,
            REGISTRY_TOOLS,
        )
    except ImportError as exc:
        pytest.skip(f"MCP server not importable: {exc}")
    return REGISTRY_TOOLS, GRAPH_TOOLS


def test_registry_tools_mirror_matches_catalog(mirrors):
    registry_tools, _ = mirrors
    from back.core.mcp_tools import MCP_REGISTRY_TOOLS

    assert set(registry_tools) == set(MCP_REGISTRY_TOOLS)


def test_graph_tools_are_domain_tools_minus_describe_ontology(mirrors):
    _, graph_tools = mirrors
    from back.core.mcp_tools import MCP_DOMAIN_TOOL_NAMES

    assert set(graph_tools) == set(MCP_DOMAIN_TOOL_NAMES) - {"describe_ontology"}


def test_describe_ontology_is_not_a_graph_tool(mirrors):
    _, graph_tools = mirrors

    # It is the sole domain tool an ontology-only domain keeps, so it must never
    # be part of the set hidden when a domain has no built graph.
    assert "describe_ontology" not in graph_tools
