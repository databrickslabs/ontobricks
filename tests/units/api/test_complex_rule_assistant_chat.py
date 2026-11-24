"""Payload tests for ``POST /ontology/business-rules/complex-assistant/chat``."""

import asyncio
from unittest.mock import MagicMock

import pytest

from agents.agent_complex_rule_builder.engine import AgentResult
from api.routers.internal import ontology
from back.core.errors import InfrastructureError, ValidationError
from back.objects.session import SessionManager


pytestmark = pytest.mark.unit

_QUERY = "CONSTRUCT { ?c a :VIP } WHERE { ?c a :Customer . ?c :balance ?b . FILTER(?b > 1000) }"


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
    domain.get_classes.return_value = [{"name": "Customer", "dataProperties": []}]
    domain.get_properties.return_value = []
    domain.ontology = {"base_uri": "http://ex.org/"}
    return domain


def _patch(monkeypatch, captured, result):
    monkeypatch.setattr(ontology, "get_domain", lambda _sm: _domain())
    monkeypatch.setattr(
        ontology,
        "require_domain_llm",
        lambda _domain, _settings: ("https://example.com", "tok", "dbx-llm", ""),
    )

    def fake_run_agent(**kwargs):
        captured.update(kwargs)
        return result

    monkeypatch.setattr("agents.agent_complex_rule_builder.run_agent", fake_run_agent)


def _run(body, monkeypatch, captured, result):
    _patch(monkeypatch, captured, result)
    session: dict = {}
    request = _FakeRequest(body, session)
    session_mgr = SessionManager(request)
    return asyncio.run(
        ontology.complex_rule_assistant_chat(request, session_mgr, MagicMock())
    )


def test_happy_path_passes_through_reply_draft_ready(monkeypatch):
    draft = {"name": "VIP", "description": "balance > 1000", "query": _QUERY}
    result = AgentResult(
        success=True, reply="Review on the right.", draft=draft, ready=True,
        usage={"prompt_tokens": 10},
    )
    captured: dict = {}
    payload = _run(
        {"message": "Flag VIPs", "current_draft": {"query": "old"}},
        monkeypatch, captured, result,
    )

    assert payload == {
        "success": True,
        "reply": "Review on the right.",
        "draft": draft,
        "ready": True,
        "options": None,
        "usage": {"prompt_tokens": 10},
    }
    # current_draft + message forwarded to the agent
    assert captured["current_draft"] == {"query": "old"}
    assert captured["user_message"] == "Flag VIPs"


def test_options_passthrough(monkeypatch):
    options = {"question": "Match 'Active'?", "options": ["Yes", "No"], "multi": False}
    result = AgentResult(success=True, reply="Does 'Active' sound right?", options=options, usage={})
    captured: dict = {}
    payload = _run({"message": "start"}, monkeypatch, captured, result)
    assert payload["options"] == options


def test_empty_message_rejected(monkeypatch):
    with pytest.raises(ValidationError):
        _run({"message": "   "}, monkeypatch, {}, AgentResult(success=True))


def test_agent_failure_raises_infrastructure_error(monkeypatch):
    result = AgentResult(success=False, error="boom")
    with pytest.raises(InfrastructureError):
        _run({"message": "hi"}, monkeypatch, {}, result)
