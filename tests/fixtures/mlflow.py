"""In-memory MLflow trace capture for agent + workflow tests.

Why this exists: the `@trace_*` decorators in `src/agents/tracing.py` write to
MLflow if available, no-op otherwise. Tests that want to assert *traces were
emitted* (not just that the decorated function ran) need a sink they can
introspect. This fixture monkeypatches the no-op fallback with an in-memory
sink that records span name + parent + attrs.

This is **not** a real MLflow client. It only mirrors the shape of the calls
the OntoBricks tracing helpers make, so tests can do shape-assertions like:

    assert captured_traces.span_named("owl_generator.generate").parent.name == "agent.run"
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
import pytest


@dataclass
class CapturedSpan:
    name: str
    parent: "CapturedSpan | None" = None
    attrs: dict[str, Any] = field(default_factory=dict)
    children: list["CapturedSpan"] = field(default_factory=list)


class InMemoryTraceSink:
    """Captures spans in memory; queryable by name."""

    def __init__(self) -> None:
        self._spans: list[CapturedSpan] = []
        self._stack: list[CapturedSpan] = []

    def start_span(self, name: str, attrs: dict[str, Any] | None = None) -> CapturedSpan:
        parent = self._stack[-1] if self._stack else None
        span = CapturedSpan(name=name, parent=parent, attrs=dict(attrs or {}))
        if parent is not None:
            parent.children.append(span)
        self._spans.append(span)
        self._stack.append(span)
        return span

    def end_span(self, span: CapturedSpan | None = None) -> None:
        if not self._stack:
            return
        if span is None or span is self._stack[-1]:
            self._stack.pop()
        else:
            # Best-effort: pop until we find the target
            while self._stack and self._stack[-1] is not span:
                self._stack.pop()
            if self._stack:
                self._stack.pop()

    @property
    def span_names(self) -> list[str]:
        return [s.name for s in self._spans]

    def span_named(self, name: str) -> CapturedSpan:
        for s in self._spans:
            if s.name == name:
                return s
        raise AssertionError(f"No span named {name!r}; saw {self.span_names}")

    def __contains__(self, name: str) -> bool:
        return name in self.span_names

    def clear(self) -> None:
        self._spans.clear()
        self._stack.clear()


@pytest.fixture
def captured_traces(monkeypatch):
    """Capture all `@trace_*`-decorated function spans into an in-memory sink.

    Usage:

        def test_agent_emits_spans(captured_traces):
            agent.run(...)
            assert "owl_generator.generate" in captured_traces

    The fixture is a no-op if the tracing module isn't importable (e.g., during
    pure-Python fixture self-tests).
    """
    sink = InMemoryTraceSink()
    try:
        from agents import tracing as _tracing  # type: ignore[import-not-found]
    except ImportError:
        yield sink
        return

    # Replace the public decorators with versions that route into our sink.
    def _make_decorator(span_label: str):
        def decorator(func):
            def wrapper(*args, **kwargs):
                span = sink.start_span(f"{span_label}.{func.__name__}")
                try:
                    return func(*args, **kwargs)
                finally:
                    sink.end_span(span)
            return wrapper
        return decorator

    if hasattr(_tracing, "trace_agent"):
        monkeypatch.setattr(_tracing, "trace_agent", _make_decorator("agent"))
    if hasattr(_tracing, "trace_llm"):
        monkeypatch.setattr(_tracing, "trace_llm", _make_decorator("llm"))
    if hasattr(_tracing, "trace_tool"):
        monkeypatch.setattr(_tracing, "trace_tool", _make_decorator("tool"))

    yield sink
