"""Tests for agents.serialization – agent step serialization."""

from agents.serialization import serialize_agent_steps
from agents.engine_base import AgentStep


class TestSerializeAgentSteps:
    def test_empty_list(self):
        assert serialize_agent_steps([]) == []

    def test_none(self):
        assert serialize_agent_steps(None) == []

    def test_agent_step_objects(self):
        steps = [
            AgentStep(step_type="tool_call", content="call data", tool_name="get_data", duration_ms=100),
            AgentStep(step_type="output", content="done"),
        ]
        result = serialize_agent_steps(steps)
        assert len(result) == 2
        assert result[0] == {"type": "tool_call", "tool": "get_data", "content": "call data", "ms": 100}
        assert result[1] == {"type": "output", "tool": "", "content": "done", "ms": 0}

    def test_plain_objects_with_attributes(self):
        class FakeStep:
            def __init__(self, st, c, tn="", dm=0):
                self.step_type = st
                self.content = c
                self.tool_name = tn
                self.duration_ms = dm

        steps = [FakeStep("tool_result", "result text", "my_tool", 250)]
        result = serialize_agent_steps(steps)
        assert result == [{"type": "tool_result", "tool": "my_tool", "content": "result text", "ms": 250}]

    def test_missing_attributes_default_to_empty(self):
        class Bare:
            pass

        result = serialize_agent_steps([Bare()])
        assert result == [{"type": "", "tool": "", "content": "", "ms": 0}]

    def test_none_attributes(self):
        class NoneStep:
            step_type = None
            tool_name = None
            content = None
            duration_ms = None

        result = serialize_agent_steps([NoneStep()])
        assert result == [{"type": "", "tool": "", "content": "", "ms": 0}]
