"""Dual-graph read-only tools for ``agent_simulation_insights``."""

import json

import pytest

# Load the ``back.objects`` package before the agents: a cold import of
# ``agents.agent_dtwin_chat`` hits a pre-existing circular import otherwise.
import back.objects  # noqa: F401

from agents.agent_simulation_insights import tools as insights_tools
from agents.tools.context import ToolContext

pytestmark = pytest.mark.unit

READ_ONLY_TOOLS = {
    "list_entity_types",
    "describe_entity",
    "query_graphql",
    "get_entity_context",
}


def _carrier(include_inferred=False, simulation_id="sABC"):
    return ToolContext(
        host="https://example.com",
        token="tok",
        dtwin_base_url="http://localhost:8000",
        dtwin_include_inferred=include_inferred,
        dtwin_simulation_id=simulation_id,
    )


def test_tool_surface_is_read_only():
    names = {d["function"]["name"] for d in insights_tools.TOOL_DEFINITIONS}
    assert names == READ_ONLY_TOOLS
    assert set(insights_tools.TOOL_HANDLERS) == READ_ONLY_TOOLS
    assert "request_entity_action" not in insights_tools.TOOL_HANDLERS


def test_every_tool_requires_a_graph_argument():
    for definition in insights_tools.TOOL_DEFINITIONS:
        params = definition["function"]["parameters"]
        assert params["properties"]["graph"]["enum"] == ["live", "sandbox"]
        assert "graph" in params["required"]


def test_graph_view_live_drops_the_scenario_and_keeps_request_inference():
    view = insights_tools.graph_view(_carrier(include_inferred=False), "live")
    assert view.dtwin_simulation_id == ""
    assert view.dtwin_include_inferred is False


def test_graph_view_sandbox_forces_inference_on_the_scenario():
    view = insights_tools.graph_view(_carrier(include_inferred=False), "sandbox")
    assert view.dtwin_simulation_id == "sABC"
    assert view.dtwin_include_inferred is True


def test_graph_view_does_not_mutate_the_carrier():
    carrier = _carrier(include_inferred=False)
    insights_tools.graph_view(carrier, "sandbox")
    insights_tools.graph_view(carrier, "live")
    assert carrier.dtwin_simulation_id == "sABC"
    assert carrier.dtwin_include_inferred is False


def test_graph_view_rejects_unknown_graph_and_missing_scenario():
    assert insights_tools.graph_view(_carrier(), "moon") is None
    assert insights_tools.graph_view(_carrier(simulation_id=""), "sandbox") is None


def test_invalid_graph_returns_error_and_never_calls_the_handler(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "agents.agent_dtwin_chat.tools.tool_list_entity_types",
        lambda ctx, **kw: calls.append(ctx) or "ok",
    )
    out = insights_tools.TOOL_HANDLERS["list_entity_types"](_carrier(), graph="moon")
    assert "graph" in json.loads(out)["error"]
    assert calls == []


def test_live_and_sandbox_calls_route_to_distinct_graphs(monkeypatch):
    seen = []

    def capture(ctx, **kwargs):
        seen.append((ctx.dtwin_simulation_id, ctx.dtwin_include_inferred, kwargs))
        return "ok"

    monkeypatch.setattr("agents.agent_dtwin_chat.tools.tool_describe_entity", capture)
    carrier = _carrier(include_inferred=False, simulation_id="sABC")
    handler = insights_tools.TOOL_HANDLERS["describe_entity"]

    handler(carrier, graph="live", search="CUST00094")
    handler(carrier, graph="sandbox", search="CUST00094")

    assert seen[0] == ("", False, {"search": "CUST00094"})
    assert seen[1] == ("sABC", True, {"search": "CUST00094"})
