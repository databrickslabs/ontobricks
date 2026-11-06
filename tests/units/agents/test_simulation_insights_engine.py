"""Engine behaviour for ``agent_simulation_insights`` (LLM mocked)."""

import json

import pytest

# Load the ``back.objects`` package before the agents: a cold import of
# ``agents.agent_dtwin_chat`` hits a pre-existing circular import otherwise.
import back.objects  # noqa: F401

from agents.agent_simulation_insights import engine
from agents.agent_simulation_insights.engine import (
    MAX_ITERATIONS,
    TRUNCATION_MARK,
    build_user_message,
    run_agent,
    truncate_answer,
)

pytestmark = pytest.mark.unit

SCENARIO = {
    "label": "Terminate C-1",
    "description": "Contract C-1 is terminated.",
    "patches": [
        {
            "op": "set",
            "subject": "http://example.org/sales#C-1",
            "predicate": "http://example.org/sales#status",
            "object": "terminated",
        }
    ],
}


def _run(**overrides):
    kwargs = dict(
        host="https://example.com",
        token="tok",
        endpoint_name="ep",
        base_url="http://localhost:8000",
        domain_name="demo",
        registry_params={"registry_catalog": "c"},
        session_cookies={},
        session_headers={},
        message="Which customers have an open contract?",
        live_answer="CUST00094 has C-1.",
        sandbox_answer="Nobody has an open contract.",
        include_inferred=True,
        simulation_id="sABC",
        scenario=SCENARIO,
    )
    kwargs.update(overrides)
    return run_agent(**kwargs)


def _text(content):
    return {
        "choices": [{"message": {"content": content}}],
        "usage": {"prompt_tokens": 5, "completion_tokens": 2},
    }


def _tool_call(name, args, call_id="call-1"):
    return {
        "choices": [
            {
                "message": {
                    "content": "",
                    "tool_calls": [
                        {
                            "id": call_id,
                            "type": "function",
                            "function": {"name": name, "arguments": json.dumps(args)},
                        }
                    ],
                }
            }
        ],
        "usage": {"total_tokens": 7},
    }


def _script_llm(monkeypatch, responses):
    """Replace the LLM with a scripted sequence; records each call's kwargs."""
    calls = []
    queue = list(responses)

    def fake_call(host, token, endpoint_name, messages, **kwargs):
        calls.append({"messages": list(messages), "tools": kwargs.get("tools")})
        return queue.pop(0)

    monkeypatch.setattr(engine, "call_serving_endpoint", fake_call)
    return calls


def test_truncate_answer_keeps_short_text_and_cuts_long_text():
    assert truncate_answer("short", limit=10) == "short"
    text = "x" * 25
    out = truncate_answer(text, limit=10)
    assert out == "x" * 10 + TRUNCATION_MARK
    assert truncate_answer("", limit=10) == ""


def test_user_message_carries_prompt_scenario_facts_and_both_answers():
    msg = build_user_message(
        message="Q?",
        live_answer="LIVE-TEXT",
        sandbox_answer="SANDBOX-TEXT",
        scenario=SCENARIO,
    )
    assert "Q?" in msg
    assert "Terminate C-1" in msg
    assert "terminated" in msg
    assert "LIVE ANSWER" in msg and "LIVE-TEXT" in msg
    assert "SANDBOX ANSWER" in msg and "SANDBOX-TEXT" in msg


def test_user_message_truncates_long_answers():
    msg = build_user_message(
        message="Q?",
        live_answer="a" * (engine.ANSWER_LIMIT + 50),
        sandbox_answer="b",
        scenario=SCENARIO,
    )
    assert TRUNCATION_MARK in msg


def test_happy_path_returns_markdown_reply(monkeypatch):
    calls = _script_llm(monkeypatch, [_text("## What changed\nC-1 is gone.")])
    result = _run()

    assert result.success is True
    assert result.error == ""
    assert result.reply.startswith("## What changed")
    assert result.iterations == 1
    assert result.usage == {"prompt_tokens": 5, "completion_tokens": 2}
    assert result.steps[-1].step_type == "output"
    assert calls[0]["tools"] is engine.TOOL_DEFINITIONS


def test_tool_call_is_routed_to_the_requested_graph(monkeypatch):
    seen = []

    def capture(ctx, **kwargs):
        seen.append((ctx.dtwin_simulation_id, ctx.dtwin_include_inferred, kwargs))
        return json.dumps({"ok": True})

    monkeypatch.setattr("agents.agent_dtwin_chat.tools.tool_describe_entity", capture)
    _script_llm(
        monkeypatch,
        [
            _tool_call("describe_entity", {"graph": "live", "search": "C-1"}, "c1"),
            _tool_call("describe_entity", {"graph": "sandbox", "search": "C-1"}, "c2"),
            _text("## What changed\nDone."),
        ],
    )
    result = _run(include_inferred=False)

    assert result.success is True
    assert seen[0] == ("", False, {"search": "C-1"})
    assert seen[1] == ("sABC", True, {"search": "C-1"})
    tool_steps = [s for s in result.steps if s.step_type == "tool_call"]
    assert [s.tool_name for s in tool_steps] == ["describe_entity", "describe_entity"]


def test_tool_result_is_fed_back_to_the_model(monkeypatch):
    monkeypatch.setattr(
        "agents.agent_dtwin_chat.tools.tool_list_entity_types",
        lambda ctx, **kw: json.dumps({"types": ["Customer"]}),
    )
    calls = _script_llm(
        monkeypatch,
        [
            _tool_call("list_entity_types", {"graph": "sandbox"}, "t9"),
            _text("## What changed\nOK."),
        ],
    )
    _run()

    tool_msgs = [m for m in calls[1]["messages"] if m.get("role") == "tool"]
    assert tool_msgs and tool_msgs[0]["tool_call_id"] == "t9"
    assert "Customer" in tool_msgs[0]["content"]


def test_final_iteration_sends_no_tools(monkeypatch):
    responses = [
        _tool_call("list_entity_types", {"graph": "live"}, f"c{i}")
        for i in range(MAX_ITERATIONS - 1)
    ]
    responses.append(_text("## What changed\nLast try."))
    calls = _script_llm(monkeypatch, responses)
    monkeypatch.setattr(
        "agents.agent_dtwin_chat.tools.tool_list_entity_types",
        lambda ctx, **kw: "{}",
    )

    result = _run()

    assert result.success is True
    assert calls[-1]["tools"] is None
    assert all(c["tools"] is engine.TOOL_DEFINITIONS for c in calls[:-1])


def test_llm_failure_sets_error_and_no_reply(monkeypatch):
    def boom(*args, **kwargs):
        raise RuntimeError("endpoint down")

    monkeypatch.setattr(engine, "call_serving_endpoint", boom)
    result = _run()

    assert result.success is False
    assert result.reply == ""
    assert "endpoint down" in result.error


def test_empty_answer_is_reported_as_error(monkeypatch):
    _script_llm(monkeypatch, [_text("   ")])
    result = _run()

    assert result.success is False
    assert "Empty answer" in result.error


def test_missing_choices_is_reported_as_error(monkeypatch):
    _script_llm(monkeypatch, [{"choices": [], "usage": {}}])
    result = _run()

    assert result.success is False
    assert result.error == "No choices in LLM response"


def test_iteration_cap_stops_a_looping_model(monkeypatch):
    _script_llm(
        monkeypatch,
        [
            _tool_call("list_entity_types", {"graph": "live"}, f"c{i}")
            for i in range(MAX_ITERATIONS + 2)
        ],
    )
    monkeypatch.setattr(
        "agents.agent_dtwin_chat.tools.tool_list_entity_types",
        lambda ctx, **kw: "{}",
    )

    result = _run()

    assert result.success is False
    assert result.error == "Max iterations reached"
    assert result.iterations == MAX_ITERATIONS


def test_on_event_receives_each_step(monkeypatch):
    _script_llm(monkeypatch, [_text("## What changed\nOne.")])
    events = []
    _run(on_event=events.append)
    assert [e.step_type for e in events] == ["output"]
