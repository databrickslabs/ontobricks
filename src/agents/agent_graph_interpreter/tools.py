"""
Graph Interpreter Agent — tools module.

Provides a single tool: ``get_entity_details``.  The agent uses it to look up
attributes and relationships of specific entities from the knowledge graph
before writing its insights, so that its output is grounded in real data.
"""

from __future__ import annotations

import json
from typing import Dict

import httpx

from agents.tools.context import ToolContext
from agents.tools.loopback_http import loopback_client
from agents.tools.graph_formatting import format_find_response
from agents.tracing import trace_tool
from back.core.helpers import URIHelpers
from back.core.logging import get_logger

logger = get_logger(__name__)

_HTTP_TIMEOUT = 60
_MAX_DEPTH = 1


def _client(ctx: ToolContext):
    return loopback_client(ctx, timeout=_HTTP_TIMEOUT)


def _error(msg: str) -> str:
    logger.warning("agent_graph_interpreter tool: %s", msg)
    return json.dumps({"error": msg})


@trace_tool(name="get_entity_details")
def tool_get_entity_details(
    ctx: ToolContext,
    *,
    uri: str,
    **_kwargs,
) -> str:
    """Fetch attributes and direct relationships for an entity URI."""
    if not uri:
        return _error("uri is required")

    local_name = URIHelpers.extract_local_name(uri)

    params = {
        "search": local_name,
        "depth": _MAX_DEPTH,
        "limit": 200,
        "offset": 0,
    }

    try:
        with _client(ctx) as c:
            resp = c.get("/dtwin/triples/find", params=params)
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as exc:
        return _error(
            f"triples/find failed ({exc.response.status_code}): "
            f"{exc.response.text[:300]}"
        )
    except Exception as exc:
        return _error(f"triples/find error: {exc}")

    return format_find_response(data, ontology_labels={})


# ── Tool registry ─────────────────────────────────────────────────────────────

TOOL_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": "get_entity_details",
            "description": (
                "Fetch attributes and direct relationships of a specific entity "
                "from the knowledge graph by its full URI. Use this to get richer "
                "context about a top-ranked node before writing your insights."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "uri": {
                        "type": "string",
                        "description": (
                            "Full URI of the entity, e.g. "
                            "https://example.com/Customer/C001"
                        ),
                    }
                },
                "required": ["uri"],
            },
        },
    }
]

TOOL_HANDLERS: Dict[str, callable] = {
    "get_entity_details": tool_get_entity_details,
}
