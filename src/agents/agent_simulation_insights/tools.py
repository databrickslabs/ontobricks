"""Read-only Graph Chat tools for ``agent_simulation_insights``.

The insights agent compares the live graph with a scenario sandbox, so each
tool takes a required ``graph`` argument (``"live"`` or ``"sandbox"``). The
carrier :class:`ToolContext` holds the request's ``include_inferred`` flag and
the scenario id; :func:`graph_view` derives the context for one graph. Only
read-only Graph Chat handlers are exposed: no ``request_entity_action``, no
SPARQL, no write path.
"""

from __future__ import annotations

import copy
import json
from dataclasses import replace
from typing import Callable, Dict, List, Optional

from agents.agent_dtwin_chat import tools as dtwin_tools
from agents.tools.context import ToolContext

GRAPH_LIVE = "live"
GRAPH_SANDBOX = "sandbox"

_GRAPH_PROPERTY = {
    "type": "string",
    "enum": [GRAPH_LIVE, GRAPH_SANDBOX],
    "description": (
        "Graph to query: 'live' (production graph) or 'sandbox' (the scenario)."
    ),
}

# Graph Chat handler name -> function name in ``agents.agent_dtwin_chat.tools``.
# Resolved at call time so the wrappers always reach the current handler.
_UNDERLYING = {
    "list_entity_types": "tool_list_entity_types",
    "describe_entity": "tool_describe_entity",
    "query_graphql": "tool_query_graphql",
    "get_entity_context": "tool_get_entity_context",
}


def graph_view(ctx: ToolContext, graph: str) -> Optional[ToolContext]:
    """Return the context for *graph*, or ``None`` when it cannot be resolved.

    * ``live``: no scenario, the request's inference flag.
    * ``sandbox``: the scenario, always with inferred reasoning.
    """
    key = str(graph or "").strip().lower()
    if key == GRAPH_LIVE:
        return replace(ctx, dtwin_simulation_id="")
    if key == GRAPH_SANDBOX:
        if not ctx.dtwin_simulation_id:
            return None
        return replace(ctx, dtwin_include_inferred=True)
    return None


def _with_graph(definition: dict) -> dict:
    """Copy a Graph Chat tool definition and add the required ``graph`` argument."""
    out = copy.deepcopy(definition)
    fn = out["function"]
    fn["description"] = (
        f"{fn['description']} Set `graph` to 'live' or 'sandbox'."
    )
    params = fn["parameters"]
    params.setdefault("properties", {})["graph"] = dict(_GRAPH_PROPERTY)
    required: List[str] = list(params.get("required") or [])
    if "graph" not in required:
        required.insert(0, "graph")
    params["required"] = required
    return out


def _make_handler(name: str) -> Callable:
    def handler(ctx: ToolContext, graph: str = "", **kwargs) -> str:
        view = graph_view(ctx, graph)
        if view is None:
            return json.dumps(
                {
                    "error": (
                        f"graph must be '{GRAPH_LIVE}' or '{GRAPH_SANDBOX}' "
                        f"(got {graph!r}); the sandbox needs a scenario id."
                    )
                }
            )
        underlying = getattr(dtwin_tools, _UNDERLYING[name])
        return underlying(view, **kwargs)

    handler.__name__ = f"insights_{name}"
    return handler


_DEFINITIONS: Dict[str, dict] = {
    "list_entity_types": dtwin_tools._LIST_ENTITY_TYPES_DEF,
    "describe_entity": dtwin_tools._DESCRIBE_ENTITY_DEF,
    "query_graphql": dtwin_tools._QUERY_GRAPHQL_DEF,
    "get_entity_context": dtwin_tools._GET_ENTITY_CONTEXT_DEF,
}

TOOL_DEFINITIONS: List[dict] = [_with_graph(d) for d in _DEFINITIONS.values()]

TOOL_HANDLERS: Dict[str, Callable] = {name: _make_handler(name) for name in _DEFINITIONS}
