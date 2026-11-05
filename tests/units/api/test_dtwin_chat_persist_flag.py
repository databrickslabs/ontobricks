"""``persist`` flag on ``POST /dtwin/assistant/chat``.

Compare → Evaluate fires two parallel, stateless Graph Chat turns. With
``persist: false`` the route must neither read the saved domain history
nor write the exchange back into the session cache. The default
(``persist`` absent or true) keeps the Graph Chat behaviour.
"""

import asyncio
from unittest.mock import MagicMock

import pytest

from agents.agent_dtwin_chat.engine import AgentResult
from api.routers.internal import dtwin
from back.objects.session import SessionManager


pytestmark = pytest.mark.unit


class _FakeRequest:
    def __init__(self, json_body, session):
        self._json_body = json_body
        self.cookies = {}
        self.headers = {}
        self.state = MagicMock(session=session, session_modified=False)

    async def json(self):
        return self._json_body


def _domain():
    domain = MagicMock()
    domain.info = {"name": "Customer 360", "llm_endpoint": "test-endpoint"}
    domain.domain_folder = "customer-360"
    return domain


def _patch_route(monkeypatch, captured):
    monkeypatch.setattr(dtwin, "get_domain", lambda _sm: _domain())
    monkeypatch.setattr(
        "back.core.helpers.get_databricks_host_and_token",
        lambda _domain, _settings: ("https://example.com", "tok"),
    )
    monkeypatch.setattr(
        dtwin.DigitalTwin,
        "resolve_registry",
        staticmethod(lambda *a, **k: {"catalog": "", "schema": "", "volume": ""}),
    )

    def fake_run_agent(**kwargs):
        captured.update(kwargs)
        return AgentResult(success=True, reply="Answer.")

    monkeypatch.setattr("agents.agent_dtwin_chat.run_agent", fake_run_agent)


def _seed_saved_history(session_mgr):
    cache = dtwin._chat_cache(session_mgr)
    key = dtwin._chat_domain_key(_domain())
    cache["history"][key] = [
        {"role": "user", "content": "earlier question"},
        {"role": "assistant", "content": "earlier answer"},
    ]
    dtwin._chat_save_cache(session_mgr, cache)
    return key


def _run_turn(body, session):
    request = _FakeRequest(body, session)
    session_mgr = SessionManager(request)
    return asyncio.run(dtwin.dtwin_assistant_chat(request, session_mgr, MagicMock()))


def test_persist_false_ignores_saved_history_and_does_not_write(monkeypatch):
    session: dict = {}
    captured: dict = {}
    _patch_route(monkeypatch, captured)
    key = _seed_saved_history(SessionManager(_FakeRequest({}, session)))

    payload = _run_turn({"message": "Q?", "persist": False}, session)

    assert payload["reply"] == "Answer."
    assert captured["conversation_history"] == []
    cache = dtwin._chat_cache(SessionManager(_FakeRequest({}, session)))
    assert cache["history"][key] == [
        {"role": "user", "content": "earlier question"},
        {"role": "assistant", "content": "earlier answer"},
    ]


def test_persist_false_does_not_create_history_entry(monkeypatch):
    session: dict = {}
    captured: dict = {}
    _patch_route(monkeypatch, captured)

    _run_turn({"message": "Q?", "persist": False}, session)

    cache = dtwin._chat_cache(SessionManager(_FakeRequest({}, session)))
    assert cache["history"] == {}


def _run_stream_turn(body, session):
    request = _FakeRequest(body, session)
    session_mgr = SessionManager(request)

    async def _drive():
        response = await dtwin.dtwin_assistant_chat_stream(request, session_mgr, MagicMock())
        return [chunk async for chunk in response.body_iterator]

    return asyncio.run(_drive())


def test_stream_persist_false_streams_steps_without_touching_history(monkeypatch):
    from agents.engine_base import AgentStep

    session: dict = {}
    captured: dict = {}
    _patch_route(monkeypatch, captured)

    def fake_run_agent(**kwargs):
        captured.update(kwargs)
        kwargs["on_event"](AgentStep(step_type="tool_call", content="{}", tool_name="list_entity_types"))
        return AgentResult(success=True, reply="Streamed answer.")

    monkeypatch.setattr("agents.agent_dtwin_chat.run_agent", fake_run_agent)
    key = _seed_saved_history(SessionManager(_FakeRequest({}, session)))

    chunks = "".join(
        c.decode() if isinstance(c, bytes) else c
        for c in _run_stream_turn({"message": "Q?", "persist": False}, session)
    )

    assert '"step_type": "tool_call"' in chunks
    assert "Streamed answer." in chunks
    assert captured["conversation_history"] == []
    cache = dtwin._chat_cache(SessionManager(_FakeRequest({}, session)))
    assert len(cache["history"][key]) == 2


def test_default_persists_and_uses_saved_history(monkeypatch):
    session: dict = {}
    captured: dict = {}
    _patch_route(monkeypatch, captured)
    key = _seed_saved_history(SessionManager(_FakeRequest({}, session)))

    _run_turn({"message": "Q?"}, session)

    assert captured["conversation_history"][0]["content"] == "earlier question"
    cache = dtwin._chat_cache(SessionManager(_FakeRequest({}, session)))
    assert cache["history"][key][-2:] == [
        {"role": "user", "content": "Q?"},
        {"role": "assistant", "content": "Answer."},
    ]
