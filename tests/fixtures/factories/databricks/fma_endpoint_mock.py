"""Mock Foundation Model API endpoint client.

Stands in for `back.core.agents.AgentClient` / its httpx transport in agent
tests. Supports scripted responses, latency injection, and tool-call recording.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class _Response:
    """One scripted response from the FMA mock."""

    content: str | None = None
    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    usage: dict[str, int] = field(default_factory=lambda: {"input_tokens": 10, "output_tokens": 5})
    error: Exception | None = None


@dataclass
class MockFoundationModelClient:
    """Scripted-response Foundation Model API client.

    Behaviour:
    - `script(*responses)` queues an ordered list of `_Response`-shaped dicts/objects.
    - Each `.invoke(...)` call pops the next response; exhausting the queue raises.
    - All inbound messages are recorded in `.calls` for assertion.

    Example:

        fma = MockFoundationModelClient().script(
            {"tool_calls": [{"name": "list_classes", "args": {}}]},
            {"content": "Done."},
        )
        out1 = fma.invoke([{"role": "user", "content": "find classes"}])
        out2 = fma.invoke([{"role": "user", "content": "ok"}])
    """

    queue: list[_Response] = field(default_factory=list)
    calls: list[dict[str, Any]] = field(default_factory=list)
    default_endpoint: str = "test-endpoint"

    def script(self, *responses: dict[str, Any] | _Response) -> "MockFoundationModelClient":
        for r in responses:
            if isinstance(r, _Response):
                self.queue.append(r)
            else:
                self.queue.append(_Response(**r))
        return self

    def invoke(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        endpoint: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        self.calls.append(
            {
                "messages": messages,
                "tools": tools or [],
                "endpoint": endpoint or self.default_endpoint,
                **kwargs,
            }
        )
        if not self.queue:
            raise RuntimeError(
                "MockFoundationModelClient queue exhausted — script another response or assert call count"
            )
        nxt = self.queue.pop(0)
        if nxt.error is not None:
            raise nxt.error
        return {
            "content": nxt.content,
            "tool_calls": nxt.tool_calls,
            "usage": nxt.usage,
        }

    def assert_called_with_tool(self, tool_name: str) -> None:
        for call in self.calls:
            for t in call.get("tools", []):
                if t.get("name") == tool_name or t.get("function", {}).get("name") == tool_name:
                    return
        raise AssertionError(
            f"Expected at least one .invoke() with tool {tool_name!r}; saw {len(self.calls)} calls"
        )
