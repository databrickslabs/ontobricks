"""MCP tools for what-if simulation sandboxes."""

from __future__ import annotations

from typing import Optional

from fastmcp import FastMCP

from server import http_client as _http
from server.constants import API_V1_SIMULATIONS
from server.session import MCPServerSession


def register_simulation_tools(mcp: FastMCP, session: MCPServerSession) -> None:
    """Register list / select / create / run scenario tools on *mcp*."""
    _register_read_tools(mcp, session)
    _register_write_tools(mcp, session)


def _register_read_tools(mcp: FastMCP, session: MCPServerSession) -> None:
    @mcp.tool()
    @_http.http_errors_as_text("list simulations")
    async def list_simulations(domain_name: Optional[str] = None) -> str:
        """List what-if scenarios for the selected domain.

        Ready scenarios can be selected with ``select_simulation`` so later
        graph reads (describe_entity, GraphQL, stats) hit that sandbox.
        Pass ``domain_name`` when the client does not reuse an MCP session.
        """
        blocked = await session.require_domain("list_simulations", domain_name)
        if blocked:
            return blocked
        async with session.client() as client:
            data = await _http._get(
                client, API_V1_SIMULATIONS, params=session.domain_params()
            )
        rows = data.get("simulations") or []
        if not rows:
            return "No scenarios in this domain."
        lines = ["Scenarios", "-" * 40]
        for row in rows:
            lines.append(
                f"  • {row.get('label') or row.get('id')}  "
                f"[{row.get('status')}]  id={row.get('id')}"
            )
        current = session.simulation_id or "live graph"
        lines.append("")
        lines.append(f"Current graph source: {current}")
        return "\n".join(lines)

    @mcp.tool()
    @_http.http_errors_as_text("select a simulation sandbox")
    async def select_simulation(
        simulation_id: str = "",
        domain_name: Optional[str] = None,
    ) -> str:
        """Select a ready scenario sandbox for subsequent graph tools.

        Pass an empty simulation_id to return to the live inferred graph.
        Pass ``live`` for the live asserted graph. A sandbox always reads
        the inferred overlay. Pass ``domain_name`` when the client does
        not reuse an MCP session.
        """
        blocked = await session.require_domain("select_simulation", domain_name)
        if blocked:
            return blocked
        sid = (simulation_id or "").strip()
        if not sid or sid == "live:inferred":
            session.simulation_id = ""
            session.include_inferred = True
            return "Graph source is live graph (inferred)."
        if sid == "live":
            session.simulation_id = ""
            session.include_inferred = False
            return "Graph source is live graph (asserted)."
        async with session.client() as client:
            data = await _http._get(
                client, f"{API_V1_SIMULATIONS}/{sid}", params=session.domain_params()
            )
        row = data.get("simulation") or {}
        if row.get("status") != "ready":
            return (
                f"Scenario '{sid}' is {row.get('status') or 'unknown'}. "
                "Run it before selecting the sandbox."
            )
        session.simulation_id = sid
        session.include_inferred = True
        return f"Graph source is sandbox {row.get('label') or sid}."


def _register_write_tools(mcp: FastMCP, session: MCPServerSession) -> None:
    @mcp.tool()
    @_http.http_errors_as_text("create a simulation")
    async def create_simulation(
        label: str,
        scope_type: str = "entity",
        focus: str = "",
        description: str = "",
        hops: int = 2,
        domain_name: Optional[str] = None,
    ) -> str:
        """Create a draft what-if scenario. Add patches in the UI or API, then run_simulation."""
        blocked = await session.require_domain("create_simulation", domain_name)
        if blocked:
            return blocked
        body = {
            "label": label,
            "description": description,
            "scope": {"type": scope_type, "focus": focus, "hops": hops},
            "patches": [],
        }
        async with session.client() as client:
            data = await _http._post(
                client, API_V1_SIMULATIONS, json=body, params=session.domain_params()
            )
        row = data.get("simulation") or {}
        return (
            f"Created draft scenario {row.get('label')} (id={row.get('id')}). "
            "Run it with run_simulation."
        )

    @mcp.tool()
    @_http.http_errors_as_text("run a simulation")
    async def run_simulation(
        simulation_id: str,
        domain_name: Optional[str] = None,
    ) -> str:
        """Run a scenario (seed, patch, reason). Poll list_simulations until ready."""
        blocked = await session.require_domain("run_simulation", domain_name)
        if blocked:
            return blocked
        sid = (simulation_id or "").strip()
        if not sid:
            return "simulation_id is required."
        async with session.client() as client:
            data = await _http._post(
                client,
                f"{API_V1_SIMULATIONS}/{sid}/run",
                json={},
                params=session.domain_params(),
            )
        return (
            f"Run started. task_id={data.get('task_id')}. "
            "Select the sandbox after status is ready."
        )
