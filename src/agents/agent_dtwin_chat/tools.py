"""
Graph Chat Agent -- tool definitions and handlers.

Each tool issues an HTTP request over loopback to the running OntoBricks
app, mirroring the tools exposed by the MCP server (see
``src/mcp-server/server/app.py``).  The selected domain is resolved from
the active user session and injected as a query parameter, so the LLM
never has to call ``list_domains`` / ``select_domain``.

Tools:
    * ``list_entity_types``   -- GET  /api/v1/digitaltwin/stats
    * ``describe_entity``     -- GET  /api/v1/digitaltwin/triples/find
    * ``get_status``          -- GET  /api/v1/digitaltwin/status
    * ``get_graphql_schema``  -- GET  /graphql/{domain}/schema
    * ``query_graphql``       -- POST /graphql/{domain}
    * ``run_sparql``          -- POST /dtwin/execute   (internal route;
                                   same session cookies are forwarded)
"""

from __future__ import annotations

import json
import re
from typing import Callable, Dict, List

import httpx

from agents.tools.context import ToolContext
from agents.tools.graph_formatting import (
    format_find_response,
    format_graphql_response,
    format_sparql_rows,
    local_name,
    pretty_predicate,
)
from back.core.logging import get_logger

logger = get_logger(__name__)

_HTTP_TIMEOUT = 120
_MAX_DEPTH = 1
_SPARQL_DANGEROUS = re.compile(
    r"\b(DROP|DELETE|INSERT|CREATE|CLEAR|LOAD|COPY|MOVE|ADD)\b",
    re.IGNORECASE,
)


# =====================================================
# Internal helpers
# =====================================================


def _client(ctx: ToolContext) -> httpx.Client:
    """Build a sync HTTP client bound to the loopback OntoBricks URL.

    Session cookies captured from the user's request are forwarded so
    the internal ``/dtwin/execute`` route resolves the same active
    domain.
    """
    return httpx.Client(
        base_url=ctx.dtwin_base_url or "http://localhost:8000",
        cookies=ctx.dtwin_session_cookies or {},
        timeout=_HTTP_TIMEOUT,
    )


def _registry_params(ctx: ToolContext) -> dict:
    params = {}
    for k, v in (ctx.dtwin_registry_params or {}).items():
        if v:
            params[k] = v
    return params


def _domain_params(ctx: ToolContext, extra: dict | None = None) -> dict:
    params = _registry_params(ctx)
    if extra:
        params.update(extra)
    if ctx.dtwin_domain_name:
        params["domain_name"] = ctx.dtwin_domain_name
    return params


def _error(msg: str) -> str:
    logger.warning("agent_dtwin_chat: %s", msg)
    return json.dumps({"error": msg})


# =====================================================
# Tool handlers
# =====================================================


def tool_list_entity_types(ctx: ToolContext, **_kwargs) -> str:
    """List entity types + counts + aggregate statistics."""
    try:
        with _client(ctx) as c:
            resp = c.get(
                "/api/v1/digitaltwin/stats", params=_domain_params(ctx)
            )
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as exc:
        return _error(
            f"stats request failed ({exc.response.status_code}): "
            f"{exc.response.text[:300]}"
        )
    except Exception as exc:
        return _error(f"stats request error: {exc}")

    if not data.get("success"):
        return data.get("message", "Could not retrieve statistics.")

    lines: list[str] = []
    lines.append(f"Knowledge Graph -- {ctx.dtwin_domain_name}")
    lines.append("=" * 40)
    lines.append(f"Total triples:       {data.get('total_triples', 0):,}")
    lines.append(f"Distinct entities:   {data.get('distinct_subjects', 0):,}")
    lines.append(f"Distinct predicates: {data.get('distinct_predicates', 0):,}")
    lines.append(f"Labels:              {data.get('label_count', 0):,}")
    lines.append(f"Type assertions:     {data.get('type_assertion_count', 0):,}")
    lines.append(f"Relationships:       {data.get('relationship_count', 0):,}")
    lines.append("")

    entity_types = data.get("entity_types", [])
    if entity_types:
        lines.append("Entity Types")
        lines.append("-" * 40)
        for et in entity_types:
            uri = et.get("uri", "")
            count = et.get("count", 0)
            lines.append(f"  - {local_name(uri)}  ({count:,} instances)")
            lines.append(f"    URI: {uri}")
        lines.append("")

    top_predicates = data.get("top_predicates", [])
    if top_predicates:
        lines.append("Predicates (attributes & relationships)")
        lines.append("-" * 40)
        for tp in top_predicates:
            uri = tp.get("uri", "")
            count = tp.get("count", 0)
            lines.append(f"  - {pretty_predicate(uri)}  ({count:,} usages)")

    return "\n".join(lines)


def tool_describe_entity(
    ctx: ToolContext,
    *,
    search: str | None = None,
    entity_type: str | None = None,
    depth: int = _MAX_DEPTH,
    **_kwargs,
) -> str:
    """Search for entities and traverse their relationships."""
    if not search and not entity_type:
        return _error("Please provide at least a search term or an entity type.")

    params = _domain_params(
        ctx,
        {
            "depth": min(max(int(depth or _MAX_DEPTH), 1), 10),
            "limit": 500,
            "offset": 0,
        },
    )
    if search:
        params["search"] = search
    if entity_type:
        params["entity_type"] = entity_type

    try:
        with _client(ctx) as c:
            resp = c.get("/api/v1/digitaltwin/triples/find", params=params)
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as exc:
        return _error(
            f"triples/find failed ({exc.response.status_code}): "
            f"{exc.response.text[:300]}"
        )
    except Exception as exc:
        return _error(f"triples/find error: {exc}")

    return format_find_response(data)


def tool_get_status(ctx: ToolContext, **_kwargs) -> str:
    """Return the selected domain's triple-store status + row count."""
    try:
        with _client(ctx) as c:
            resp = c.get(
                "/api/v1/digitaltwin/status", params=_domain_params(ctx)
            )
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as exc:
        return _error(
            f"status failed ({exc.response.status_code}): "
            f"{exc.response.text[:300]}"
        )
    except Exception as exc:
        return _error(f"status error: {exc}")

    status = data.get("reason") or ("OK" if data.get("success") else "unknown")
    return (
        f"Domain:  {ctx.dtwin_domain_name}\n"
        f"View:    {data.get('view_table', 'N/A')}\n"
        f"Graph:   {data.get('graph_name', 'N/A')}\n"
        f"Status:  {status}\n"
        f"Data:    {'Yes' if data.get('has_data') else 'No'} "
        f"({data.get('count', 0):,} triples)"
    )


def tool_get_graphql_schema(ctx: ToolContext, **_kwargs) -> str:
    """Return the domain's auto-generated GraphQL schema (SDL)."""
    domain = ctx.dtwin_domain_name
    if not domain:
        return _error("No domain selected in the current session.")

    try:
        with _client(ctx) as c:
            resp = c.get(
                f"/graphql/{domain}/schema", params=_registry_params(ctx)
            )
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as exc:
        return _error(
            f"GraphQL schema failed ({exc.response.status_code}): "
            f"{exc.response.text[:300]}"
        )
    except Exception as exc:
        return _error(f"GraphQL schema error: {exc}")

    sdl = data.get("sdl", "")
    if not sdl:
        return "GraphQL schema is empty -- the domain may have no ontology classes."

    return (
        f"GraphQL Schema -- {domain}\n"
        + ("=" * 50)
        + "\n\n"
        + sdl
        + "\n\nUse query_graphql to execute queries against this schema."
    )


def tool_query_graphql(
    ctx: ToolContext,
    *,
    query: str,
    variables: str | None = None,
    **_kwargs,
) -> str:
    """Execute a GraphQL query against the selected domain."""
    domain = ctx.dtwin_domain_name
    if not domain:
        return _error("No domain selected in the current session.")
    if not query:
        return _error("Missing required 'query' argument.")

    body: dict = {"query": query}
    if variables:
        if isinstance(variables, dict):
            body["variables"] = variables
        else:
            try:
                body["variables"] = json.loads(variables)
            except json.JSONDecodeError:
                return _error("Invalid JSON in 'variables' argument.")

    try:
        with _client(ctx) as c:
            resp = c.post(
                f"/graphql/{domain}",
                json=body,
                params=_registry_params(ctx),
            )
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as exc:
        return _error(
            f"GraphQL query failed ({exc.response.status_code}): "
            f"{exc.response.text[:300]}"
        )
    except Exception as exc:
        return _error(f"GraphQL query error: {exc}")

    return format_graphql_response(data, domain)


def tool_run_sparql(
    ctx: ToolContext,
    *,
    query: str,
    limit: int | None = None,
    **_kwargs,
) -> str:
    """Execute a read-only SPARQL SELECT (or ASK) query."""
    if not query or not query.strip():
        return _error("Missing required 'query' argument.")
    if _SPARQL_DANGEROUS.search(query):
        return _error(
            "Refusing to run mutating SPARQL (DROP/DELETE/INSERT/CREATE/...). "
            "Only SELECT / ASK / DESCRIBE queries are allowed."
        )

    payload = {"query": query}
    if limit is not None:
        try:
            payload["limit"] = int(limit)
        except (TypeError, ValueError):
            return _error("'limit' must be an integer.")

    try:
        with _client(ctx) as c:
            resp = c.post("/dtwin/execute", json=payload)
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as exc:
        return _error(
            f"SPARQL execute failed ({exc.response.status_code}): "
            f"{exc.response.text[:300]}"
        )
    except Exception as exc:
        return _error(f"SPARQL execute error: {exc}")

    if not data.get("success"):
        return data.get("message") or "SPARQL query failed."

    columns = data.get("columns") or data.get("headers") or []
    rows = data.get("rows") or data.get("data") or []

    if not columns and rows and isinstance(rows[0], dict):
        columns = list(rows[0].keys())
        rows = [[r.get(c) for c in columns] for r in rows]

    header = (
        f"SPARQL Result -- {ctx.dtwin_domain_name}\n"
        + ("=" * 50)
        + f"\nRows: {len(rows)}\n"
    )
    return header + format_sparql_rows(columns, rows)


# =====================================================
# Tool schema definitions (OpenAI chat-completions format)
# =====================================================


_LIST_ENTITY_TYPES_DEF = {
    "type": "function",
    "function": {
        "name": "list_entity_types",
        "description": (
            "List all entity types (rdf:type) with instance counts and overall "
            "statistics of the selected domain's knowledge graph. Call this to "
            "understand the shape of the graph before describing entities."
        ),
        "parameters": {"type": "object", "properties": {}},
    },
}

_DESCRIBE_ENTITY_DEF = {
    "type": "function",
    "function": {
        "name": "describe_entity",
        "description": (
            "Search the knowledge graph for entities matching a text and/or "
            "type, traverse their relationships, and return a human-readable "
            "description including attributes and relationships."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "search": {
                    "type": "string",
                    "description": "Text to search in labels / names / URIs (e.g. 'Jacob Martinez', 'CUST00094').",
                },
                "entity_type": {
                    "type": "string",
                    "description": "Entity type local name (case-insensitive) e.g. 'Customer'.",
                },
                "depth": {
                    "type": "integer",
                    "description": "Relationship traversal depth (1-10, default 1).",
                },
            },
        },
    },
}

_GET_STATUS_DEF = {
    "type": "function",
    "function": {
        "name": "get_status",
        "description": "Return the triple-store status (view, graph, row count) for the selected domain.",
        "parameters": {"type": "object", "properties": {}},
    },
}

_GET_GRAPHQL_SCHEMA_DEF = {
    "type": "function",
    "function": {
        "name": "get_graphql_schema",
        "description": (
            "Return the auto-generated GraphQL schema (SDL) for the selected "
            "domain. Call this before query_graphql to know which types and "
            "fields are available."
        ),
        "parameters": {"type": "object", "properties": {}},
    },
}

_QUERY_GRAPHQL_DEF = {
    "type": "function",
    "function": {
        "name": "query_graphql",
        "description": (
            "Execute a GraphQL query against the selected domain. Use for "
            "typed lookups and nested relationship traversal. Pair with "
            "get_graphql_schema first."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "GraphQL query string, e.g. '{ allCustomer(limit: 5) { id label } }'.",
                },
                "variables": {
                    "type": "string",
                    "description": "Optional JSON string of query variables.",
                },
            },
            "required": ["query"],
        },
    },
}

_RUN_SPARQL_DEF = {
    "type": "function",
    "function": {
        "name": "run_sparql",
        "description": (
            "Execute a READ-ONLY SPARQL query (SELECT / ASK / DESCRIBE) "
            "against the selected domain's digital twin. Mutating queries "
            "(DROP / DELETE / INSERT / ...) are rejected."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "SPARQL query (SELECT / ASK / DESCRIBE only).",
                },
                "limit": {
                    "type": "integer",
                    "description": "Optional row cap.",
                },
            },
            "required": ["query"],
        },
    },
}


TOOL_DEFINITIONS: List[dict] = [
    _LIST_ENTITY_TYPES_DEF,
    _DESCRIBE_ENTITY_DEF,
    _GET_STATUS_DEF,
    _GET_GRAPHQL_SCHEMA_DEF,
    _QUERY_GRAPHQL_DEF,
    _RUN_SPARQL_DEF,
]

TOOL_HANDLERS: Dict[str, Callable] = {
    "list_entity_types": tool_list_entity_types,
    "describe_entity": tool_describe_entity,
    "get_status": tool_get_status,
    "get_graphql_schema": tool_get_graphql_schema,
    "query_graphql": tool_query_graphql,
    "run_sparql": tool_run_sparql,
}
