"""Internal API -- what-if simulation sandboxes (Simulation menu)."""

import os
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request

from api.routers.internal._graph_access import assert_domain_graph_read
from api.routers.internal._guards import require
from back.core.errors import InfrastructureError, ValidationError
from back.core.helpers import require_domain_llm, run_blocking
from back.core.logging import get_logger
from back.objects.digitaltwin import DigitalTwin
from back.objects.registry import ROLE_BUILDER
from back.objects.session import SessionManager, get_domain, get_session_manager
from back.objects.simulation import Simulation
from shared.config.settings import Settings, get_settings

logger = get_logger(__name__)

router = APIRouter(prefix="/simulation", tags=["Simulation"])

_builder_only = [Depends(require(ROLE_BUILDER, scope="domain"))]


def _simulation(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
) -> Simulation:
    return Simulation(get_domain(session_mgr), settings)


def _actor(request: Request) -> str:
    return getattr(request.state, "user_email", "") or request.headers.get(
        "x-forwarded-email", ""
    )


async def _json_object(request: Request) -> dict:
    data = await request.json()
    if not isinstance(data, dict):
        raise ValidationError("Body must be a JSON object")
    return data


_MAX_FACT_PROMPT = 4000


def _loopback_base_url() -> str:
    port = os.environ.get("DATABRICKS_APP_PORT") or os.environ.get("PORT") or "8000"
    return f"http://localhost:{port}"


def _forwarded_headers(request: Request) -> dict:
    prefixes = ("x-forwarded-", "x-real-")
    extra = {"x-csrf-token", "referer"}
    return {
        k: v
        for k, v in request.headers.items()
        if k.lower().startswith(prefixes) or k.lower() in extra
    }


def _facts_user_message(
    prompt: str, scope: dict, existing: list, description: str = ""
) -> str:
    parts = [f"REQUEST\n{prompt}"]
    text = str(description or "").strip()
    if text:
        parts.append("CURRENT DESCRIPTION\n" + text[:_MAX_FACT_PROMPT])
    if isinstance(scope, dict) and (scope.get("focus") or scope.get("type")):
        parts.append(
            "SCOPE\n"
            f"type: {scope.get('type') or ''}\n"
            f"focus: {scope.get('focus') or ''}\n"
            f"focus_label: {scope.get('focus_label') or ''}"
        )
    lines = []
    for row in existing[:50] if isinstance(existing, list) else []:
        if not isinstance(row, dict):
            continue
        lines.append(
            f"- {row.get('op')} {row.get('subject')} "
            f"{row.get('predicate')} {row.get('object')}"
        )
    if lines:
        parts.append("EXISTING FACTS\n" + "\n".join(lines))
    return "\n\n".join(parts)


def _fallback_scenario_description(existing: str, prompt: str, scope: dict) -> str:
    prior = str(existing or "").strip()
    label = ""
    if isinstance(scope, dict):
        label = str(scope.get("focus_label") or scope.get("focus") or "").strip()
    heading = f"Hypothetical facts for {label}" if label else "Hypothetical facts"
    body = f"{heading}: {str(prompt or '').strip()}"
    merged = f"{prior}\n\n{body}".strip() if prior else body
    return merged[:_MAX_FACT_PROMPT]


def _scenario_description(result, prompt: str, scope, existing: str) -> str:
    text = str(getattr(result, "proposed_description", "") or "").strip()
    patches = getattr(result, "proposed_patches", None) or []
    if text:
        return text[:_MAX_FACT_PROMPT]
    if patches:
        return _fallback_scenario_description(existing, prompt, scope or {})
    return ""


@router.get("/list")
async def list_simulations(request: Request, sim: Simulation = Depends(_simulation)):
    """Return every simulation of the active domain version (no diff payload)."""
    assert_domain_graph_read(request)
    rows = await run_blocking(sim.list_all)
    return {
        "success": True,
        "supported": sim.supported,
        "simulations": rows,
        "count": len(rows),
    }


@router.get("/runs")
async def list_runs(
    request: Request,
    simulation_id: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    sim: Simulation = Depends(_simulation),
):
    """Newest-first run history, optionally for one simulation."""
    assert_domain_graph_read(request)
    runs = await run_blocking(sim.runs, simulation_id, limit)
    return {"success": True, "runs": runs, "count": len(runs)}


@router.post("/facts/agent", dependencies=_builder_only)
async def propose_facts(
    request: Request,
    body: dict = Depends(_json_object),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Translate a free-text description into hypothetical S/P/O patches."""
    import asyncio

    from agents.agent_simulation_facts import run_agent
    from api.routers.internal._helpers import map_route_errors

    prompt = str(body.get("prompt") or "").strip()
    if not prompt:
        raise ValidationError("Describe the facts to add")
    if len(prompt) > _MAX_FACT_PROMPT:
        raise ValidationError("Fact description is too long")
    domain = get_domain(session_mgr)
    host, token, llm_endpoint, _kind = require_domain_llm(domain, settings)
    reg = DigitalTwin.resolve_registry(session_mgr, settings)
    info = getattr(domain, "info", None) or {}
    with map_route_errors("Simulation facts agent request failed", logger):
        result = await asyncio.to_thread(
            run_agent,
            host=host,
            token=token,
            endpoint_name=llm_endpoint,
            base_url=_loopback_base_url(),
            domain_name=str(info.get("name") or ""),
            registry_params={
                "registry_catalog": reg.get("catalog") or "",
                "registry_schema": reg.get("schema") or "",
                "registry_volume": reg.get("volume") or "",
                "simulation_id": str(body.get("simulation_id") or ""),
            },
            session_cookies=dict(request.cookies or {}),
            session_headers=_forwarded_headers(request),
            user_message=_facts_user_message(
                prompt,
                body.get("scope") or {},
                body.get("existing_patches") or [],
                str(body.get("description") or ""),
            ),
        )
    if not result.success:
        raise InfrastructureError(
            "Could not propose hypothetical facts",
            detail=result.error or None,
        )
    return {
        "success": True,
        "patches": result.proposed_patches or [],
        "description": _scenario_description(
            result, prompt, body.get("scope") or {}, str(body.get("description") or "")
        ),
        "reply": result.reply or "",
        "usage": result.usage,
    }


@router.post("/create", dependencies=_builder_only)
async def create_simulation(
    request: Request,
    body: dict = Depends(_json_object),
    sim: Simulation = Depends(_simulation),
):
    """Create a draft simulation from ``{label, scope, patches, phases}``."""
    row = await run_blocking(sim.create, body, _actor(request))
    return {"success": True, "simulation": row}


@router.get("/{sim_id}")
async def get_simulation(
    sim_id: str, request: Request, sim: Simulation = Depends(_simulation)
):
    """Return one simulation (spec + diff summary; rows come from ``/diff``)."""
    assert_domain_graph_read(request)
    row = await run_blocking(sim.get, sim_id)
    row.pop("diff", None)
    return {"success": True, "simulation": row}


@router.put("/{sim_id}", dependencies=_builder_only)
async def update_simulation(
    sim_id: str,
    body: dict = Depends(_json_object),
    sim: Simulation = Depends(_simulation),
):
    """Replace the simulation spec (resets status to ``draft``)."""
    row = await run_blocking(sim.update, sim_id, body)
    row.pop("diff", None)
    return {"success": True, "simulation": row}


@router.delete("/{sim_id}", dependencies=_builder_only)
async def delete_simulation(sim_id: str, sim: Simulation = Depends(_simulation)):
    """Delete the simulation and drop its sandbox table."""
    deleted = await run_blocking(sim.delete, sim_id)
    return {"success": True, "deleted": bool(deleted)}


@router.post("/{sim_id}/abort", dependencies=_builder_only)
async def abort_simulation(sim_id: str, sim: Simulation = Depends(_simulation)):
    """Stop a stuck or in-flight run so the scenario can be saved again."""
    row = await run_blocking(sim.abort, sim_id)
    row.pop("diff", None)
    return {"success": True, "simulation": row}


@router.post("/{sim_id}/purge", dependencies=_builder_only)
async def purge_simulation(sim_id: str, sim: Simulation = Depends(_simulation)):
    """Drop the sandbox after-picture; keep the scenario as a draft."""
    row = await run_blocking(sim.purge, sim_id)
    row.pop("diff", None)
    return {"success": True, "simulation": row}


@router.post("/{sim_id}/run", dependencies=_builder_only)
async def run_simulation(
    sim_id: str, request: Request, sim: Simulation = Depends(_simulation)
):
    """Start the simulation as a background task; poll ``/tasks/{task_id}``."""
    task_id = await run_blocking(sim.start_run, sim_id, _actor(request))
    return {"success": True, "task_id": task_id}


@router.get("/{sim_id}/diff")
async def get_simulation_diff(
    sim_id: str,
    request: Request,
    kind: str = Query("added"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    search: str = Query(""),
    sim: Simulation = Depends(_simulation),
):
    """One page of added / removed triples from the last run."""
    assert_domain_graph_read(request)
    data = await run_blocking(sim.diff_page, sim_id, kind, page, page_size, search)
    return {"success": True, **data}
