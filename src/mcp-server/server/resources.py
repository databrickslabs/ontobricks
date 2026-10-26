"""MCP resource registration for the OntoBricks server.

``register_resources(mcp, session)`` binds the read-only ``ontobricks://``
resources — raw JSON snapshots of the registry, triple-store status/stats and
the GraphQL schema — to a :class:`~server.session.MCPServerSession`.
"""

from __future__ import annotations

import json
import logging

import httpx
from fastmcp import FastMCP

from server import http_client as _http
from server.constants import (
    API_V1_DOMAINS,
    API_V1_DT_STATS,
    API_V1_DT_STATUS,
)
from server.session import MCPServerSession

logger = logging.getLogger(__name__)


async def _fetch_json(
    session: MCPServerSession, path: str, params: dict, what: str
) -> str:
    """GET *path* as pretty JSON, or ``{"error": ...}`` without the request URL."""
    try:
        async with session.client() as client:
            data = await _http._get(client, path, params=params)
    except httpx.HTTPError as exc:
        logger.warning("Resource could not %s: %s", what, exc)
        return json.dumps({"error": _http._http_error_message(exc, what)})
    return json.dumps(data, indent=2)


def register_resources(mcp: FastMCP, session: MCPServerSession) -> None:
    """Register every OntoBricks MCP resource on *mcp*, bound to *session*."""

    @mcp.resource("ontobricks://domains")
    async def resource_domains() -> str:
        """List of domains in the registry (raw JSON from GET /api/v1/domains)."""
        return await _fetch_json(
            session, API_V1_DOMAINS, session.registry_params(), "list the domains"
        )

    @mcp.resource("ontobricks://status")
    async def resource_status() -> str:
        """Current triple store configuration and status."""
        return await _fetch_json(
            session, API_V1_DT_STATUS, session.domain_params(), "read the graph status"
        )

    @mcp.resource("ontobricks://stats")
    async def resource_stats() -> str:
        """Triple store content statistics."""
        return await _fetch_json(
            session, API_V1_DT_STATS, session.domain_params(), "read the graph statistics"
        )

    @mcp.resource("ontobricks://graphql-schema")
    async def resource_graphql_schema() -> str:
        """GraphQL schema (SDL) for the selected domain."""
        domain_name = session.selected_domain_name
        if not domain_name:
            return json.dumps({"error": "No domain selected"})
        return await _fetch_json(
            session,
            _http._graphql_path(domain_name, "/schema"),
            session.registry_params(),
            "retrieve the GraphQL schema",
        )
