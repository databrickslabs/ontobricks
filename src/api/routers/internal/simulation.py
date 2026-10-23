"""Internal API -- what-if simulation sandboxes (Simulation menu)."""

from typing import Optional

from fastapi import APIRouter, Depends, Query, Request

from api.routers.internal._graph_access import assert_domain_graph_read
from api.routers.internal._guards import require
from back.core.errors import ValidationError
from back.core.helpers import run_blocking
from back.core.logging import get_logger
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
