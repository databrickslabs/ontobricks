"""Shared httpx.MockTransport factory for agent + REST-client tests.

Generalises the pattern from `tests/test_agent_dtwin_chat.py` so all 5 agents
and any other httpx-using code can mount a fake transport in one line.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable

import httpx
import pytest


Handler = Callable[[httpx.Request], httpx.Response]


@dataclass
class ScriptedTransport:
    """A scripted httpx transport.

    Pass either:
    - `handler`: an `httpx.Request -> httpx.Response` callable; OR
    - `routes`: a dict like `{("GET", "/api/v1/domains"): {"json": [...]}, ...}`
      where the value is forwarded to `httpx.Response(**value)` (status defaults to 200).
    """

    handler: Handler | None = None
    routes: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    requests: list[httpx.Request] = field(default_factory=list)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.handler is not None:
            return self.handler(request)
        key = (request.method.upper(), request.url.path)
        if key in self.routes:
            spec = dict(self.routes[key])
            status = spec.pop("status", 200)
            return httpx.Response(status, **spec)
        return httpx.Response(404, json={"error": f"no route for {key}"})

    def assert_called(self, method: str, path_substring: str) -> None:
        for r in self.requests:
            if r.method.upper() == method.upper() and path_substring in r.url.path:
                return
        urls = ", ".join(f"{r.method} {r.url.path}" for r in self.requests)
        raise AssertionError(f"Expected {method} {path_substring!r}; saw [{urls}]")


@pytest.fixture
def agent_mock_transport():
    """Factory: build a ScriptedTransport and return (transport, install_callable).

    The install callable swaps the global httpx.Client factory for OntoBricks'
    agent tool layer to one that uses `httpx.MockTransport(transport)`. Tests
    receive both so they can pre-script routes and then assert on `.requests`.

    Example:

        def test_dtwin_chat_calls_search(monkeypatch, agent_mock_transport):
            transport, install = agent_mock_transport
            transport.routes[("GET", "/api/v1/search")] = {
                "json": [{"uri": "ex:Alice", "label": "Alice"}],
            }
            install(monkeypatch)
            ... # invoke agent
            transport.assert_called("GET", "/search")
    """
    transport = ScriptedTransport()

    def install(monkeypatch) -> None:
        """Replace the agent layer's httpx.Client builder with our scripted one."""
        try:
            from agents.tools import chat_tools as _chat_tools  # type: ignore[import-not-found]
        except ImportError:
            return

        def _factory(ctx, *args, **kwargs):
            mock_transport = httpx.MockTransport(transport)
            return httpx.Client(base_url=getattr(ctx, "dtwin_base_url", "http://test"), transport=mock_transport)

        if hasattr(_chat_tools, "_client"):
            monkeypatch.setattr(_chat_tools, "_client", _factory)

    return transport, install
