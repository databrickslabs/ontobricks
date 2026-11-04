"""Unit tests for ``agents.agent_ontology_assistant.engine`` and ``tools``.

Verifies that SYSTEM_PROMPT is assembled correctly at import time and that
the mutation/read tools remain registered after the pitfalls tool was removed.
"""

from __future__ import annotations


def test_system_prompt_contains_core_sections():
    """SYSTEM_PROMPT must still contain the standard agent sections."""
    from agents.agent_ontology_assistant.engine import SYSTEM_PROMPT

    for section in ("TOOLS", "WORKFLOW", "RULES", "FORMATTING"):
        assert section in SYSTEM_PROMPT, f"SYSTEM_PROMPT is missing section: {section}"


def test_system_prompt_does_not_reference_check_pitfalls():
    from agents.agent_ontology_assistant.engine import SYSTEM_PROMPT

    assert "check_pitfalls" not in SYSTEM_PROMPT


def test_check_pitfalls_tool_is_not_registered():
    from agents.agent_ontology_assistant.tools import TOOL_DEFINITIONS, TOOL_HANDLERS

    names = [td["function"]["name"] for td in TOOL_DEFINITIONS]
    assert "check_pitfalls" not in names
    assert "check_pitfalls" not in TOOL_HANDLERS


def test_run_agent_forwards_domain_endpoint_kind(monkeypatch):
    """The assistant must invoke the exact LLM target selected by the domain."""
    import agents.agent_ontology_assistant.engine as engine_mod

    calls = []

    def _call_llm(*args, **kwargs):
        calls.append(kwargs)
        return {
            "choices": [{"message": {"content": "Done"}}],
            "usage": {},
        }

    monkeypatch.setattr(engine_mod, "call_serving_endpoint", _call_llm)

    result = engine_mod.run_agent(
        host="https://workspace",
        token="token",
        endpoint_name="main.ai.custom",
        endpoint_kind="ai_gateway",
        classes=[],
        properties=[],
        base_uri="https://example.com/",
        user_message="List the ontology",
    )

    assert result.success is True
    assert calls[0]["endpoint_kind"] == "ai_gateway"
