"""Unit tests for Graph Chat final-answer normalization."""

import json
from unittest.mock import patch

import pytest

from agents.agent_dtwin_chat.engine import AgentResult, normalize_reply_content, run_agent
from agents.tools.context import ToolContext

_PENDING_ACTION = {
    "token": "tok",
    "entity_uri": "https://ex/Customer/CUST1",
    "entity_label": "CUST1",
    "action": "main.ops.recompute_risk",
    "description": "Risk",
    "expires_in_sec": 120,
}


def _tool_call_response():
    return {
        "choices": [
            {
                "message": {
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "tc1",
                            "function": {
                                "name": "request_entity_action",
                                "arguments": json.dumps(
                                    {
                                        "entity_uri": "https://ex/Customer/CUST1",
                                        "action": "main.ops.recompute_risk",
                                    }
                                ),
                            },
                        }
                    ],
                }
            }
        ],
        "usage": {"prompt_tokens": 10, "completion_tokens": 5},
    }


def _fake_dispatch(_handlers, ctx, _tool_name, _arguments, *, trace_name=""):
    ctx.pending_action = _PENDING_ACTION
    return '{"success": true}'


def _run_with_scripted_llm(llm_side_effect):
    with (
        patch(
            "agents.agent_dtwin_chat.engine.call_serving_endpoint",
            side_effect=llm_side_effect,
        ),
        patch(
            "agents.agent_dtwin_chat.engine.dispatch_tool",
            side_effect=_fake_dispatch,
        ),
    ):
        return run_agent(
            host="https://example.invalid",
            token="token",
            endpoint_name="endpoint",
            base_url="http://localhost:8000",
            domain_name="main",
            registry_params={},
            session_cookies={},
            user_message="recompute risk",
        )


def test_run_agent_preserves_pending_action_on_llm_error():
    def llm_side_effect(*_args, **_kwargs):
        if llm_side_effect.calls == 0:
            llm_side_effect.calls += 1
            return _tool_call_response()
        raise RuntimeError("LLM unavailable")

    llm_side_effect.calls = 0
    result = _run_with_scripted_llm(llm_side_effect)

    assert result.success is False
    assert "LLM request failed" in result.error
    assert result.pending_action == _PENDING_ACTION


def test_run_agent_preserves_pending_action_on_empty_choices():
    def llm_side_effect(*_args, **_kwargs):
        if llm_side_effect.calls == 0:
            llm_side_effect.calls += 1
            return _tool_call_response()
        return {"choices": [], "usage": {}}

    llm_side_effect.calls = 0
    result = _run_with_scripted_llm(llm_side_effect)

    assert result.success is False
    assert result.error == "No choices in LLM response"
    assert result.pending_action == _PENDING_ACTION


def test_agent_result_and_context_carry_pending_action():
    ctx = ToolContext(host="h", token="t")
    assert ctx.dtwin_class_actions == {}
    assert ctx.pending_action is None
    result = AgentResult(success=True, pending_action={"token": "abc"})
    assert result.pending_action["token"] == "abc"


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        ("A readable answer.", "A readable answer."),
        (
            [
                {"type": "text", "text": "First paragraph."},
                {"type": "text", "text": "Second paragraph."},
            ],
            "First paragraph.\n\nSecond paragraph.",
        ),
        (
            [
                {"type": "metadata", "data": {"internal": True}},
                {"type": "text", "text": "Visible answer."},
                {"type": "image", "url": "https://example.invalid/image"},
            ],
            "Visible answer.",
        ),
        (
            {"type": "text", "text": {"value": "Nested text."}},
            "Nested text.",
        ),
    ],
)
def test_normalize_reply_content_extracts_readable_text(content, expected):
    assert normalize_reply_content(content) == expected


@pytest.mark.parametrize("content", [None, "", [], {}, [{"type": "metadata"}]])
def test_normalize_reply_content_uses_non_technical_fallback(content):
    assert normalize_reply_content(content) == (
        "I couldn't display that answer. Please try again."
    )
