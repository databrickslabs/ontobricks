"""MCP test conftest — re-exports the in-process client fixtures.

Tests under `tests/mcp/` are tagged with the `mcp` marker. They run on the
G1 unit+integration job for every PR, and on the dedicated `mcp-test` job
(G1c) when `src/mcp-server/**` changes.
"""

from __future__ import annotations

# Re-export the canonical fixtures so users don't need to import from
# `tests.fixtures.mcp_client` directly.
from tests.fixtures.mcp_client import mcp_app, mcp_client, InProcessMCPClient  # noqa: F401

__all__ = ["mcp_app", "mcp_client", "InProcessMCPClient"]
