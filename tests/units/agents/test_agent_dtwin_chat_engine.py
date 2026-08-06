"""Unit tests for Graph Chat final-answer normalization."""

import pytest

from agents.agent_dtwin_chat.engine import AgentResult, normalize_reply_content
from agents.tools.context import ToolContext


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
