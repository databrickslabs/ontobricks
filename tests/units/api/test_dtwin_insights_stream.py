"""``POST /dtwin/assistant/insights/stream`` — the Evaluate business-insights route.

The route is stateless and loads the scenario server-side: the browser never
supplies the fact changes. Validation runs before any LLM call.
"""

import asyncio
from unittest.mock import MagicMock

import pytest

# Load the ``back.objects`` package before the agents: a cold import of
# ``agents.agent_dtwin_chat`` hits a pre-existing circular import otherwise.
import back.objects  # noqa: F401

from agents.agent_simulation_insights.engine import AgentResult, AgentStep
from api.routers.internal import dtwin
from back.core.errors import NotFoundError, ValidationError
from back.objects.session import SessionManager

pytestmark = pytest.mark.unit

SCENARIO_SPEC_PATCHES = [
    {
        "op": "set",
        "subject": "http://example.org/sales#C-1",
        "predicate": "http://example.org/sales#status",
        "object": "terminated",
    }
]


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


def _body(**overrides):
    body = {
        "message": "Which customers have an open contract?",
        "live_answer": "CUST00094 has C-1.",
        "sandbox_answer": "Nobody has an open contract.",
        "include_inferred": True,
        "simulation_id": "sReady",
    }
    body.update(overrides)
    return body


def _row(status="ready", **spec_overrides):
    spec = {
        "label": "Terminate C-1",
        "description": "Contract C-1 is terminated.",
        "patches": SCENARIO_SPEC_PATCHES,
    }
    spec.update(spec_overrides)
    return {"id": "sReady", "label": "Terminate C-1", "status": status, "spec": spec}


def _patch_route(monkeypatch, *, row=None, error=None, agent_result=None, captured=None):
    """Stub the domain, LLM target, registry, simulation row and agent."""
    captured = captured if captured is not None else {}
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

    class FakeSimulation:
        def __init__(self, *args, **kwargs):
            pass

        def get(self, sim_id):
            captured["sim_id"] = sim_id
            if error is not None:
                raise error
            return row

    monkeypatch.setattr("back.objects.intelligence.Simulation", FakeSimulation)

    def fake_run(**kwargs):
        captured.update(kwargs)
        return agent_result or AgentResult(success=True, reply="## What changed\nNone.")

    monkeypatch.setattr("agents.agent_simulation_insights.run_agent", fake_run)
    return captured


def _stream(body, session=None):
    session = session if session is not None else {}
    request = _FakeRequest(body, session)
    session_mgr = SessionManager(request)

    async def _drive():
        response = await dtwin.dtwin_assistant_insights_stream(
            request, session_mgr, MagicMock()
        )
        return [c async for c in response.body_iterator]

    chunks = asyncio.run(_drive())
    return "".join(c.decode() if isinstance(c, bytes) else c for c in chunks)


@pytest.mark.parametrize(
    "overrides",
    [
        {"message": "   "},
        {"live_answer": ""},
        {"sandbox_answer": ""},
        {"simulation_id": ""},
    ],
)
def test_missing_inputs_are_rejected_before_any_work(monkeypatch, overrides):
    captured = _patch_route(monkeypatch, row=_row())
    with pytest.raises(ValidationError):
        asyncio.run(
            dtwin.dtwin_assistant_insights_stream(
                _FakeRequest(_body(**overrides), {}),
                SessionManager(_FakeRequest({}, {})),
                MagicMock(),
            )
        )
    assert captured == {}


def test_draft_scenario_is_rejected(monkeypatch):
    captured = _patch_route(monkeypatch, row=_row(status="draft"))
    with pytest.raises(ValidationError):
        _stream(_body())
    assert "scenario" not in captured


def test_unknown_scenario_surfaces_not_found(monkeypatch):
    _patch_route(monkeypatch, error=NotFoundError("Simulation 'sReady' was not found"))
    with pytest.raises(NotFoundError):
        _stream(_body())


def test_happy_path_streams_steps_then_done_and_loads_facts_server_side(monkeypatch):
    def with_step(**kwargs):
        kwargs["on_event"](
            AgentStep(step_type="tool_call", content="{}", tool_name="describe_entity")
        )
        return AgentResult(success=True, reply="## What changed\nC-1 is gone.", iterations=2)

    _patch_route(monkeypatch, row=_row())
    monkeypatch.setattr("agents.agent_simulation_insights.run_agent", with_step)

    forged = [{"op": "set", "subject": "forged", "predicate": "p", "object": "x"}]
    body = _body(scenario={"patches": forged})
    chunks = _stream(body)

    assert '"step_type": "tool_call"' in chunks
    assert '"type": "done"' in chunks
    assert "C-1 is gone." in chunks


def test_run_agent_receives_scenario_from_the_registry_not_the_body(monkeypatch):
    captured = _patch_route(monkeypatch, row=_row())

    forged = [{"op": "set", "subject": "forged", "predicate": "p", "object": "x"}]
    _stream(_body(scenario={"patches": forged}))

    assert captured["simulation_id"] == "sReady"
    assert captured["scenario"] == {
        "label": "Terminate C-1",
        "description": "Contract C-1 is terminated.",
        "patches": SCENARIO_SPEC_PATCHES,
    }
    assert captured["live_answer"] == "CUST00094 has C-1."
    assert captured["sandbox_answer"] == "Nobody has an open contract."
    assert captured["include_inferred"] is True


def test_route_is_stateless_and_leaves_the_chat_history_alone(monkeypatch):
    _patch_route(monkeypatch, row=_row())
    session: dict = {}
    _stream(_body(), session)

    cache = dtwin._chat_cache(SessionManager(_FakeRequest({}, session)))
    assert cache["history"] == {}


def test_agent_failure_streams_an_error_event(monkeypatch):
    _patch_route(
        monkeypatch,
        row=_row(),
        agent_result=AgentResult(success=False, error="LLM request failed: endpoint down"),
    )
    chunks = _stream(_body())

    assert '"type": "error"' in chunks
    assert "endpoint down" in chunks
    assert '"type": "done"' not in chunks
