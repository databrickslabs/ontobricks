"""Factory functions for the OntoBricks MCP server.

``create_mcp_server`` wires a :class:`~server.session.MCPServerSession` to a
configured :class:`FastMCP` instance (instructions + registered tools and
resources). ``create_databricks_app`` wraps that server in the combined
FastAPI + MCP application served by uvicorn in the Databricks App.
"""

from __future__ import annotations

import logging
import os

from fastmcp import FastMCP

from server import http_client as _http
from server.resources import register_resources
from server.session import MCPServerSession
from server.tools import register_tools

logger = logging.getLogger(__name__)


def create_mcp_server(mode: str = "standalone") -> FastMCP:
    """Build a configured :class:`FastMCP` instance.

    Args:
        mode: ``"databricks"`` | ``"standalone"`` | ``"mounted"``.
    """
    base = _http._base_url(mode)
    logger.info("Creating MCP server — mode=%s, base_url=%s", mode, base)
    logger.info(
        "Env snapshot — REGISTRY_VOLUME_PATH=%r REGISTRY_CATALOG=%r "
        "REGISTRY_SCHEMA=%r REGISTRY_VOLUME=%r DATABRICKS_HOST=%r "
        "DATABRICKS_CLIENT_ID=%s DATABRICKS_CLIENT_SECRET=%s "
        "DATABRICKS_SQL_WAREHOUSE_ID=%r",
        os.getenv("REGISTRY_VOLUME_PATH", ""),
        os.getenv("REGISTRY_CATALOG", ""),
        os.getenv("REGISTRY_SCHEMA", ""),
        os.getenv("REGISTRY_VOLUME", ""),
        os.getenv("DATABRICKS_HOST", ""),
        "set" if os.getenv("DATABRICKS_CLIENT_ID") else "unset",
        "set" if os.getenv("DATABRICKS_CLIENT_SECRET") else "unset",
        os.getenv("DATABRICKS_SQL_WAREHOUSE_ID", ""),
    )

    session = MCPServerSession(mode)

    mcp = FastMCP(
        "OntoBricks",
        instructions=(
            "You are connected to OntoBricks: domain registry + Knowledge Graph "
            "(triple store) over external REST at /api/v1.\n\n"
            "Workflow:\n"
            "1. Call 'list_domains' to see available domains.\n"
            "2. Optionally call 'list_domain_versions' or 'get_design_status' "
            "to inspect versions or design readiness (ontology, mappings, build_ready).\n"
            "3. Call 'select_domain' with the domain name that best matches "
            "the user's question.\n"
            "4. Use 'list_entity_types' and 'describe_entity' for exploration, "
            "or GraphQL tools for typed queries.\n"
            "   Use 'describe_ontology' to read the domain's ontology structure "
            "(classes, attributes, relationships, OWL). It needs no graph, so a "
            "domain published with only an ontology exposes 'describe_ontology' "
            "as its sole domain tool.\n\n"
            "DATA SOURCES — three tools, three different scopes:\n"
            "- 'describe_entity': GROUND TRUTH. Queries the raw triple store "
            "(union of synced data AND inferred/materialised triples). Returns "
            "ALL relationships including those added by reasoning, regardless of "
            "whether their predicate is declared in the ontology schema. "
            "Use this as the PRIMARY tool whenever you need to know what "
            "relationships or attributes an entity has, especially after inference "
            "has been run.\n"
            "- 'query_graphql': Reads the SAME graph store but filtered through "
            "the ontology schema layer. Only predicates declared in the ontology "
            "appear as fields. Inferred/materialised triples whose predicate is "
            "NOT in the ontology schema are silently invisible. Use only for "
            "bulk typed look-ups where you already know the schema covers the data.\n"
            "- 'list_entity_types': Aggregate stats over the full graph store "
            "(union view) — reflects both synced and inferred entity counts.\n\n"
            "DECISION RULE: For any question about a specific entity or its "
            "relationships, always start with 'describe_entity'. Only fall back "
            "to 'query_graphql' for bulk/typed queries after confirming the schema "
            "covers the predicates you need.\n\n"
            "CROSS-DOMAIN BRIDGES: describe_entity and get_entity_context expose "
            "bridges configured on an entity's class. Each bridge carries the "
            "target domain's name AND description, and only MCP-visible targets "
            "are shown. When a bridge is relevant to the user's question, "
            "call select_domain(<target_domain>) to switch — the previously "
            "selected domain is replaced — then describe_entity or GraphQL in "
            "the target. get_entity_context(follow_bridges=True) only PEEKS at "
            "the target graph; it does NOT switch the session, so subsequent "
            "tools stay on the origin domain.\n\n"
            "Always select a domain before entity/triple/GraphQL queries. "
            "If the user's question maps clearly to one domain, select it automatically."
        ),
    )

    register_tools(mcp, session)
    register_resources(mcp, session)

    return mcp


# ── Databricks App (combined FastAPI + MCP) ───────────────────────────────


def create_databricks_app():
    """Build the combined FastAPI application for Databricks deployment."""
    from fastapi import FastAPI

    mcp = create_mcp_server(mode="databricks")
    mcp_app = mcp.http_app()
    ontobricks_url = os.getenv("ONTOBRICKS_URL", "http://localhost:8000")

    app = FastAPI(
        title="mcp-ontobricks",
        description="OntoBricks MCP Server — Knowledge graph tools for "
        "Databricks Playground",
        version="1.0.0",
        lifespan=mcp_app.lifespan,
    )

    @app.get("/", include_in_schema=False)
    async def health():
        vol_path = os.getenv("REGISTRY_VOLUME_PATH", "")
        if vol_path:
            registry_display = vol_path
        else:
            reg_cat = os.getenv("REGISTRY_CATALOG", "")
            reg_sch = os.getenv("REGISTRY_SCHEMA", "")
            reg_vol = os.getenv("REGISTRY_VOLUME", "OntoBricksRegistry")
            registry_display = (
                f"{reg_cat}.{reg_sch}.{reg_vol}"
                if reg_cat and reg_sch
                else "auto-discover"
            )
        return {
            "status": "healthy",
            "service": "mcp-ontobricks",
            "ontobricks_url": ontobricks_url,
            "warehouse_id": os.getenv("DATABRICKS_SQL_WAREHOUSE_ID", ""),
            "registry": registry_display,
        }

    combined = FastAPI(
        title="mcp-ontobricks",
        routes=[*mcp_app.routes, *app.routes],
        lifespan=mcp_app.lifespan,
    )

    return combined
