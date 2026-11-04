"""External REST API — what-if simulation sandboxes.

Mounted at ``/api/v1/simulations``. Same domain resolution as the Knowledge
Graph API. Create / update / run / purge require a graph Builder; list and
read require Viewer.
"""

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Query, Request
from pydantic import AliasChoices, BaseModel, Field

from back.core.helpers import run_blocking
from back.core.logging import get_logger
from back.objects.digitaltwin import DigitalTwin
from back.objects.session import SessionManager, get_session_manager
from back.objects.simulation import Simulation
from shared.config.settings import Settings, get_settings

from api.routers.digitaltwin import TaskProgressResponse, _poll_task, assert_public_graph_read, assert_public_graph_write

logger = get_logger(__name__)

router = APIRouter()


class SimulationWriteBody(BaseModel):
    """Create/update payload — same shape as the Simulation workbench spec."""

    label: str = Field(..., min_length=1, max_length=200)
    description: str = ""
    scope: Dict[str, Any] = Field(default_factory=dict)
    patches: list = Field(default_factory=list)
    phases: Dict[str, Any] = Field(default_factory=dict)
    rule_names: Dict[str, Any] = Field(default_factory=dict)


def _actor(request: Request) -> str:
    return getattr(request.state, "user_email", "") or request.headers.get(
        "x-forwarded-email", ""
    )


def _public_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row or {})
    out.pop("diff", None)
    return out


def _resolve_sim(
    request: Request,
    session_mgr: SessionManager,
    settings: Settings,
    domain_name: Optional[str],
    domain_version: Optional[str],
    registry_catalog: Optional[str],
    registry_schema: Optional[str],
    registry_volume: Optional[str],
    *,
    write: bool,
) -> Simulation:
    domain = DigitalTwin.resolve_domain(
        domain_name,
        session_mgr,
        settings,
        registry_catalog,
        registry_schema,
        registry_volume,
        domain_version,
        read_only=not write,
    )
    if write:
        assert_public_graph_write(request, domain, settings)
    else:
        assert_public_graph_read(request, domain, settings)
    return Simulation(domain, settings)


def _domain_q(
    domain_name: Optional[str] = Query(
        None, validation_alias=AliasChoices("domain_name", "project_name")
    ),
    domain_version: Optional[str] = Query(
        None, validation_alias=AliasChoices("domain_version", "project_version")
    ),
    registry_catalog: Optional[str] = Query(None),
    registry_schema: Optional[str] = Query(None),
    registry_volume: Optional[str] = Query(None),
) -> dict:
    return {
        "domain_name": domain_name,
        "domain_version": domain_version,
        "registry_catalog": registry_catalog,
        "registry_schema": registry_schema,
        "registry_volume": registry_volume,
    }


@router.get("", summary="List scenarios")
async def list_simulations(
    request: Request,
    domain_q: dict = Depends(_domain_q),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """List what-if scenarios for the domain. Ready rows can be queried as sandboxes."""
    sim = _resolve_sim(request, session_mgr, settings, write=False, **domain_q)
    rows = await run_blocking(sim.list_all)
    return {
        "success": True,
        "supported": sim.supported,
        "simulations": [_public_row(r) for r in rows],
        "count": len(rows),
    }


@router.get("/runs", summary="List scenario runs")
async def list_simulation_runs(
    request: Request,
    simulation_id: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    domain_q: dict = Depends(_domain_q),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    sim = _resolve_sim(request, session_mgr, settings, write=False, **domain_q)
    runs = await run_blocking(sim.runs, simulation_id, limit)
    return {"success": True, "runs": runs, "count": len(runs)}


@router.get("/tasks/{task_id}", response_model=TaskProgressResponse, summary="Poll a scenario run")
async def simulation_task_progress(task_id: str):
    return _poll_task(task_id)


@router.post("", summary="Create a scenario")
async def create_simulation(
    request: Request,
    body: SimulationWriteBody,
    domain_q: dict = Depends(_domain_q),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    sim = _resolve_sim(request, session_mgr, settings, write=True, **domain_q)
    row = await run_blocking(sim.create, body.model_dump(), _actor(request))
    return {"success": True, "simulation": _public_row(row)}


@router.get("/{sim_id}", summary="Get a scenario")
async def get_simulation(
    sim_id: str,
    request: Request,
    domain_q: dict = Depends(_domain_q),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    sim = _resolve_sim(request, session_mgr, settings, write=False, **domain_q)
    row = await run_blocking(sim.get, sim_id)
    return {"success": True, "simulation": _public_row(row)}


@router.put("/{sim_id}", summary="Update a scenario")
async def update_simulation(
    sim_id: str,
    request: Request,
    body: SimulationWriteBody,
    domain_q: dict = Depends(_domain_q),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    sim = _resolve_sim(request, session_mgr, settings, write=True, **domain_q)
    row = await run_blocking(sim.update, sim_id, body.model_dump())
    return {"success": True, "simulation": _public_row(row)}


@router.delete("/{sim_id}", summary="Delete a scenario")
async def delete_simulation(
    sim_id: str,
    request: Request,
    domain_q: dict = Depends(_domain_q),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    sim = _resolve_sim(request, session_mgr, settings, write=True, **domain_q)
    deleted = await run_blocking(sim.delete, sim_id)
    return {"success": True, "deleted": bool(deleted)}


@router.post("/{sim_id}/run", summary="Run a scenario")
async def run_simulation(
    sim_id: str,
    request: Request,
    domain_q: dict = Depends(_domain_q),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Seed the sandbox, apply hypothetical facts, reason, then persist the diff.

    Poll ``GET /api/v1/simulations/tasks/{task_id}``. When status is ``ready``,
    pass ``simulation_id`` on GraphQL / triples / SPARQL the same way Explorer
    selects a sandbox.
    """
    sim = _resolve_sim(request, session_mgr, settings, write=True, **domain_q)
    task_id = await run_blocking(sim.start_run, sim_id, _actor(request))
    return {"success": True, "task_id": task_id}


@router.post("/{sim_id}/abort", summary="Abort a running scenario")
async def abort_simulation(
    sim_id: str,
    request: Request,
    domain_q: dict = Depends(_domain_q),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    sim = _resolve_sim(request, session_mgr, settings, write=True, **domain_q)
    row = await run_blocking(sim.abort, sim_id)
    return {"success": True, "simulation": _public_row(row)}


@router.post("/{sim_id}/purge", summary="Drop the sandbox, keep the scenario")
async def purge_simulation(
    sim_id: str,
    request: Request,
    domain_q: dict = Depends(_domain_q),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    sim = _resolve_sim(request, session_mgr, settings, write=True, **domain_q)
    row = await run_blocking(sim.purge, sim_id)
    return {"success": True, "simulation": _public_row(row)}


@router.get("/{sim_id}/diff", summary="Page the last-run triple diff")
async def get_simulation_diff(
    sim_id: str,
    request: Request,
    kind: str = Query("added"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    search: str = Query(""),
    domain_q: dict = Depends(_domain_q),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    sim = _resolve_sim(request, session_mgr, settings, write=False, **domain_q)
    data = await run_blocking(sim.diff_page, sim_id, kind, page, page_size, search)
    return {"success": True, **data}
