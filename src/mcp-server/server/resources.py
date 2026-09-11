"""MCP resource registration for the OntoBricks server.

``register_resources(mcp, session)`` binds the read-only ``ontobricks://``
resources — raw JSON snapshots of the registry, triple-store status/stats and
the GraphQL schema — to a :class:`~server.session.MCPServerSession`.
"""

from __future__ import annotations

import json
import logging

from fastmcp import FastMCP

from server import http_client as _http
from server.constants import (
    API_V1_DOMAINS,
    API_V1_DT_STATS,
    API_V1_DT_STATUS,
)
from server.session import MCPServerSession

logger = logging.getLogger(__name__)


def register_resources(mcp: FastMCP, session: MCPServerSession) -> None:
    """Register every OntoBricks MCP resource on *mcp*, bound to *session*."""

    @mcp.resource("ontobricks://domains")
    async def resource_domains() -> str:
        """List of domains in the registry (raw JSON from GET /api/v1/domains)."""
        async with session.client() as client:
            data = await _http._get(client, API_V1_DOMAINS, params=session.registry_params())
        return json.dumps(data, indent=2)

    @mcp.resource("ontobricks://status")
    async def resource_status() -> str:
        """Current triple store configuration and status."""
        async with session.client() as client:
            data = await _http._get(client, API_V1_DT_STATUS, params=session.domain_params())
        return json.dumps(data, indent=2)

    @mcp.resource("ontobricks://stats")
    async def resource_stats() -> str:
        """Triple store content statistics."""
        async with session.client() as client:
            data = await _http._get(client, API_V1_DT_STATS, params=session.domain_params())
        return json.dumps(data, indent=2)

    @mcp.resource("ontobricks://graphql-schema")
    async def resource_graphql_schema() -> str:
        """GraphQL schema (SDL) for the selected domain."""
        domain_name = session.selected_domain_name
        if not domain_name:
            return json.dumps({"error": "No domain selected"})
        try:
            async with session.client() as client:
                resp = await client.get(
                    f"/graphql/{domain_name}/schema",
                    params=session.registry_params(),
                    timeout=120,
                )
                resp.raise_for_status()
                return json.dumps(resp.json(), indent=2)
        except Exception as exc:
            logger.warning("GraphQL schema resource error: %s", exc)
            return json.dumps({"error": str(exc)})
