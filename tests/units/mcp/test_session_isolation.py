"""Per-connection isolation of MCP session state.

Regression guard for the shared-session domain bleed: the one
``MCPServerSession`` bound to the process must keep each MCP connection's
selected domain and caches separate, keyed by ``CURRENT_SESSION_ID``.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Make src/mcp-server importable as ``server.*`` (mirrors tests/fixtures/mcp_client).
_MCP_SRC = Path(__file__).resolve().parents[2] / "src" / "mcp-server"
if str(_MCP_SRC) not in sys.path:
    sys.path.insert(0, str(_MCP_SRC))

try:
    from server.session import MCPServerSession, _MAX_TRACKED_SESSIONS
    from server.session_scope import CURRENT_SESSION_ID
except ImportError as exc:  # pragma: no cover - env without fastmcp
    pytest.skip(f"MCP server not importable: {exc}", allow_module_level=True)


def _in_session(sid: str):
    """Context manager entering MCP session *sid*."""
    from contextlib import contextmanager

    @contextmanager
    def _ctx():
        token = CURRENT_SESSION_ID.set(sid)
        try:
            yield
        finally:
            CURRENT_SESSION_ID.reset(token)

    return _ctx()


def test_selected_domain_does_not_leak_between_sessions() -> None:
    """The exact bug: session B must not see session A's selected domain."""
    s = MCPServerSession("standalone")

    with _in_session("sid-A"):
        s.selected_domain_name = "mercuriatrading"
        s.class_actions["https://x/Class"] = {"name": "Class"}
        s.ontology_labels["k"] = "Label"

    with _in_session("sid-B"):
        assert s.selected_domain_name is None
        assert s.class_actions == {}
        assert s.ontology_labels == {}
        s.selected_domain_name = "mercuriamarkets"

    with _in_session("sid-A"):
        assert s.selected_domain_name == "mercuriatrading"
        assert s.class_actions == {"https://x/Class": {"name": "Class"}}
        assert s.ontology_labels == {"k": "Label"}

    with _in_session("sid-B"):
        assert s.selected_domain_name == "mercuriamarkets"


def test_shared_state_is_not_isolated() -> None:
    """Registry + policy are server-wide facts and stay shared across sessions."""
    s = MCPServerSession("standalone")
    with _in_session("sid-A"):
        s.domain_policy["d"] = {"disabled_tools": ["query_graphql"]}
    with _in_session("sid-B"):
        assert s.domain_policy["d"] == {"disabled_tools": ["query_graphql"]}


def test_session_store_is_lru_bounded() -> None:
    """The per-session store cannot grow without limit."""
    s = MCPServerSession("standalone")
    for i in range(_MAX_TRACKED_SESSIONS + 50):
        with _in_session(f"sid-{i}"):
            s.selected_domain_name = "d"
    assert len(s._domain_states) <= _MAX_TRACKED_SESSIONS
