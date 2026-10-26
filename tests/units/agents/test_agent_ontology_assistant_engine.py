"""Unit tests for ``agents.agent_ontology_assistant.engine`` and ``tools``.

Verifies that SYSTEM_PROMPT is assembled correctly at import time, that
the pitfall rules file is loaded and embedded into the prompt, and that
the check_pitfalls tool is registered and degrades gracefully.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest


def test_system_prompt_contains_all_pitfall_ids():
    """SYSTEM_PROMPT must reference every pitfall id P1.1 through P4.7."""
    from agents.agent_ontology_assistant.engine import SYSTEM_PROMPT

    expected_ids = [
        "P1.1", "P1.2", "P1.3",
        "P2.1", "P2.2", "P2.3", "P2.4", "P2.5", "P2.6",
        "P3.1", "P3.2", "P3.3",
        "P4.1", "P4.2", "P4.3", "P4.4", "P4.5", "P4.6", "P4.7",
    ]
    missing = [pid for pid in expected_ids if pid not in SYSTEM_PROMPT]
    assert not missing, f"SYSTEM_PROMPT is missing pitfall IDs: {missing}"


def test_system_prompt_contains_core_sections():
    """SYSTEM_PROMPT must still contain the standard agent sections."""
    from agents.agent_ontology_assistant.engine import SYSTEM_PROMPT

    for section in ("TOOLS", "WORKFLOW", "RULES", "FORMATTING"):
        assert section in SYSTEM_PROMPT, f"SYSTEM_PROMPT is missing section: {section}"


def test_system_prompt_references_check_pitfalls():
    """WORKFLOW must instruct the agent to call check_pitfalls."""
    from agents.agent_ontology_assistant.engine import SYSTEM_PROMPT

    assert "check_pitfalls" in SYSTEM_PROMPT


def test_pitfall_rules_file_exists():
    """PITFALL_RULES.md must be present in the shared agents folder."""
    rules_path = Path(__file__).parents[3] / "src" / "agents" / "PITFALL_RULES.md"
    assert rules_path.exists(), f"PITFALL_RULES.md not found at {rules_path}"


def test_load_pitfall_rules_raises_on_missing_file(tmp_path, monkeypatch):
    """_load_pitfall_rules must raise FileNotFoundError if the MD is absent."""
    import agents.agent_ontology_assistant.engine as engine_mod

    monkeypatch.setattr(engine_mod, "_PITFALL_RULES_PATH", tmp_path / "missing.md")
    with pytest.raises(FileNotFoundError, match="PITFALL_RULES.md"):
        engine_mod._load_pitfall_rules()


def test_check_pitfalls_tool_is_registered():
    """check_pitfalls must appear in both TOOL_DEFINITIONS and TOOL_HANDLERS."""
    from agents.agent_ontology_assistant.tools import TOOL_DEFINITIONS, TOOL_HANDLERS

    names = [td["function"]["name"] for td in TOOL_DEFINITIONS]
    assert "check_pitfalls" in names, "check_pitfalls missing from TOOL_DEFINITIONS"
    assert "check_pitfalls" in TOOL_HANDLERS, "check_pitfalls missing from TOOL_HANDLERS"


def test_check_pitfalls_returns_error_gracefully_when_deps_missing(monkeypatch):
    """tool_check_pitfalls must return a JSON error dict when deps are unavailable."""
    import agents.agent_ontology_assistant.tools as tools_mod

    # Simulate missing pitfalls deps by patching PitfallsService import inside the tool
    original_import = __builtins__.__import__ if hasattr(__builtins__, "__import__") else __import__

    def _mock_import(name, *args, **kwargs):
        if "pitfalls" in name.lower():
            raise ImportError("mocked missing pitfalls dep")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", _mock_import)

    from agents.tools.context import ToolContext

    ctx = ToolContext(host="h", token="t")
    result = json.loads(tools_mod.tool_check_pitfalls(ctx))
    assert "error" in result


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
