"""In-memory MCP server and LLM endpoint doubles for the example-script tests."""

from __future__ import annotations


class FakeMCP:
    """Records tool calls and answers from a ``{tool_name: text}`` map."""

    def __init__(self, results: dict[str, str] | None = None):
        self.results = results or {}
        self.calls: list[tuple[str, dict]] = []

    def call_tool(self, name: str, arguments: dict) -> str:
        self.calls.append((name, arguments))
        return self.results.get(name, f"{name} ok")


class FakeLLM:
    """Replays canned chat responses and records what it was sent."""

    def __init__(self, responses: list[dict]):
        self.responses = list(responses)
        self.sent: list[dict] = []

    def chat(self, messages, tools=None):
        self.sent.append({"messages": [dict(m) for m in messages], "tools": tools})
        return self.responses.pop(0)


def tool_call_response(name, arguments="{}", *, content=None, call_id="c1", usage=None):
    return {
        "choices": [
            {
                "finish_reason": "tool_calls",
                "message": {
                    "content": content,
                    "tool_calls": [
                        {"id": call_id, "function": {"name": name, "arguments": arguments}}
                    ],
                },
            }
        ],
        "usage": usage or {},
    }


def final_response(content, usage=None):
    return {
        "choices": [{"finish_reason": "stop", "message": {"content": content}}],
        "usage": usage or {},
    }


def raw_tool(name: str, description: str = "", schema: dict | None = None) -> dict:
    tool = {"name": name, "description": description}
    if schema is not None:
        tool["inputSchema"] = schema
    return tool
