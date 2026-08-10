"""Regression test: a pending Action token minted mid-turn must survive.

Both ``POST /dtwin/assistant/chat`` and ``.../chat/stream`` snapshot the
Graph Chat session cache *before* ``run_agent`` runs, then write it back
*after*. The agent's tool calls loop back into this same process (e.g.
``POST /dtwin/nodes/action/request``) and can mint a ``pending_actions``
token into the shared session while the outer route's pre-agent snapshot
is still in scope. Saving that stale snapshot back would silently discard
the token an instant after it was minted (a lost-update bug).

This test drives the real route functions directly (no mocked cache
helpers) and simulates the nested loopback by calling the real
``dtwin_nodes_action_request`` route *from inside* a patched ``run_agent``,
sharing the same underlying session dict the way two loopback HTTP
requests against the same session would.
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from agents.agent_dtwin_chat.engine import AgentResult
from api.routers.internal import dtwin
from back.objects.session import SessionManager


pytestmark = pytest.mark.unit


class _FakeRequest:
    """Minimal stand-in for FastAPI's ``Request``, just enough for the
    chat / node-action routes: JSON body, cookies/headers, and a
    ``.state.session`` dict shared across "requests" in this test.
    """

    def __init__(self, json_body, session, cookies=None, headers=None):
        self._json_body = json_body
        self.cookies = cookies or {}
        self.headers = headers or {}
        self.state = SimpleNamespace(session=session, session_modified=False)

    async def json(self):
        return self._json_body


def _domain_with_action():
    mock_domain = MagicMock()
    mock_domain.info = {"name": "Customer 360", "llm_endpoint": "test-endpoint"}
    mock_domain.domain_folder = "customer-360"
    mock_domain.get_classes.return_value = [
        {
            "name": "Customer",
            "uri": "https://example.com/Customer",
            "actions": [
                {"fullName": "main.ops.recompute_risk", "returns_table": False}
            ],
        }
    ]
    return mock_domain


def test_chat_does_not_clobber_pending_action_minted_during_run_agent(monkeypatch):
    shared_session: dict = {}
    mock_domain = _domain_with_action()

    monkeypatch.setattr(dtwin, "get_domain", lambda _session_mgr: mock_domain)
    monkeypatch.setattr(
        "back.core.helpers.get_databricks_host_and_token",
        lambda _domain, _settings: ("https://example.com", "tok"),
    )
    monkeypatch.setattr(
        dtwin.DigitalTwin,
        "resolve_registry",
        staticmethod(lambda *a, **k: {"catalog": "", "schema": "", "volume": ""}),
    )

    minted = {}

    def fake_run_agent(**_kwargs):
        # Simulate the agent's tool call looping back into
        # POST /dtwin/nodes/action/request while THIS run_agent call is
        # still executing -- sharing the same session storage the outer
        # chat request uses.
        nested_request = _FakeRequest(
            {
                "entity_uri": "https://example.com/Customer/CUST1",
                "action_full_name": "main.ops.recompute_risk",
            },
            shared_session,
        )
        nested_session_mgr = SessionManager(nested_request)
        result = asyncio.run(
            dtwin.dtwin_nodes_action_request(nested_request, nested_session_mgr)
        )
        minted["token"] = result["pending_action"]["token"]
        return AgentResult(success=True, reply="Done.")

    monkeypatch.setattr("agents.agent_dtwin_chat.run_agent", fake_run_agent)

    # No pre-existing "graph_chat" session key: this is the first Graph
    # Chat turn for this session.
    assert "graph_chat" not in shared_session

    outer_request = _FakeRequest({"message": "hi there"}, shared_session)
    outer_session_mgr = SessionManager(outer_request)
    settings = MagicMock()

    asyncio.run(dtwin.dtwin_assistant_chat(outer_request, outer_session_mgr, settings))

    assert minted, "nested run_agent loopback never minted a pending action"
    final_cache = dtwin._chat_cache(outer_session_mgr)
    assert minted["token"] in final_cache["pending_actions"], (
        "pending_actions token minted mid-turn was clobbered by the "
        "post-agent cache save"
    )
    # The turn itself was still recorded.
    assert final_cache["history"]


def test_chat_stream_does_not_clobber_pending_action_minted_during_run_agent(
    monkeypatch,
):
    shared_session: dict = {}
    mock_domain = _domain_with_action()

    monkeypatch.setattr(dtwin, "get_domain", lambda _session_mgr: mock_domain)
    monkeypatch.setattr(
        "back.core.helpers.get_databricks_host_and_token",
        lambda _domain, _settings: ("https://example.com", "tok"),
    )
    monkeypatch.setattr(
        dtwin.DigitalTwin,
        "resolve_registry",
        staticmethod(lambda *a, **k: {"catalog": "", "schema": "", "volume": ""}),
    )

    minted = {}

    def fake_run_agent(**kwargs):
        nested_request = _FakeRequest(
            {
                "entity_uri": "https://example.com/Customer/CUST1",
                "action_full_name": "main.ops.recompute_risk",
            },
            shared_session,
        )
        nested_session_mgr = SessionManager(nested_request)
        result = asyncio.run(
            dtwin.dtwin_nodes_action_request(nested_request, nested_session_mgr)
        )
        minted["token"] = result["pending_action"]["token"]
        on_event = kwargs.get("on_event")
        if on_event:
            pass  # no intermediate steps needed for this regression test
        return AgentResult(success=True, reply="Done.")

    monkeypatch.setattr("agents.agent_dtwin_chat.run_agent", fake_run_agent)

    assert "graph_chat" not in shared_session

    outer_request = _FakeRequest({"message": "hi there"}, shared_session)
    outer_session_mgr = SessionManager(outer_request)
    settings = MagicMock()

    async def _drive_stream():
        response = await dtwin.dtwin_assistant_chat_stream(
            outer_request, outer_session_mgr, settings
        )
        async for _chunk in response.body_iterator:
            pass

    asyncio.run(_drive_stream())

    assert minted, "nested run_agent loopback never minted a pending action"
    final_cache = dtwin._chat_cache(outer_session_mgr)
    assert minted["token"] in final_cache["pending_actions"], (
        "pending_actions token minted mid-turn was clobbered by the "
        "post-agent cache save"
    )
    assert final_cache["history"]
