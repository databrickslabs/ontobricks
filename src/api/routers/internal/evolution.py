"""Internal API — Ontology › Evolve (question-driven ontology repair).

Every request names its domain folder and parent version explicitly. The
folder must match the context loaded in the session (so the session's role
checks apply to it); anything else is a 409 rather than a silent retarget.
The prefix sits outside the status gate on purpose: Evolve reads a parent
version of any status and never edits it.
"""

import threading
from typing import Callable, Optional

from fastapi import APIRouter, Depends, Query, Request

from api.routers.internal._guards import require
from back.core.errors import ConflictError, ValidationError
from back.core.helpers import run_blocking
from back.core.helpers.DatabricksHelpers import DatabricksHelpers
from back.core.logging import get_logger
from back.objects.ontology.evolution import (
    CandidateBuilder,
    EvaluationService,
    EvolutionPipeline,
    EvolutionService,
    VersionAllocator,
    WarehouseSourceProbe,
)
from back.objects.registry import ROLE_BUILDER, RegistryService
from back.objects.session import SessionManager, get_domain, get_session_manager
from shared.config.settings import Settings, get_settings

logger = get_logger(__name__)

router = APIRouter(prefix="/evolution", tags=["Evolution"])

_builder_only = [Depends(require(ROLE_BUILDER, scope="domain"))]


def _actor(request: Request) -> str:
    return getattr(request.state, "user_email", "") or request.headers.get(
        "x-forwarded-email", ""
    )


async def _json_object(request: Request) -> dict:
    data = await request.json()
    if not isinstance(data, dict):
        raise ValidationError("Body must be a JSON object")
    return data


def _require_enabled(settings: Settings) -> None:
    if not settings.evolution_enabled:
        raise ValidationError(
            "Ontology Evolve is turned off for this deployment",
            detail="Set ONTOBRICKS_EVOLUTION_ENABLED=true",
        )


def _service(
    session_mgr: SessionManager, settings: Settings, folder: str
) -> EvolutionService:
    """EvolutionService for *folder*, which must be the context loaded in the session."""
    _require_enabled(settings)
    domain = get_domain(session_mgr)
    loaded = str(getattr(domain, "domain_folder", "") or "")
    if not folder:
        raise ValidationError("domain is required")
    if folder != loaded:
        raise ConflictError(
            "The loaded context changed — reload the page before continuing",
            detail=f"requested {folder!r}, loaded {loaded!r}",
        )
    return EvolutionService(RegistryService.from_context(domain, settings), folder)


# Per-statement bound for source probes; each is a small aggregate query.
PROBE_STATEMENT_TIMEOUT_S = 60


def _probe(session_mgr: SessionManager, settings: Settings) -> WarehouseSourceProbe:
    """Source probe bound to the caller, created in the request.

    In a Databricks App this uses the forwarded user token (on-behalf-of) and
    fails rather than fall back to the app service principal, so coverage,
    gold lookups and the access checks before evaluate and accept are
    governed by the caller's Unity Catalog grants. The client keeps that
    token, so the evaluation worker's source-version reads run as the
    caller too; only the arm builds and arm graph reads use the app.
    """
    client = DatabricksHelpers.get_data_plane_client(get_domain(session_mgr), settings)
    if client is None:
        raise ValidationError("A SQL warehouse is required to inspect sources")
    return WarehouseSourceProbe(
        lambda sql: client.sql.execute_query(
            sql, statement_timeout_s=PROBE_STATEMENT_TIMEOUT_S
        )
    )


def _pipeline(
    session_mgr: SessionManager, settings: Settings, folder: str
) -> EvolutionPipeline:
    svc = _service(session_mgr, settings, folder)
    return EvolutionPipeline(svc, _probe(session_mgr, settings))


def _candidate_builder(session_mgr: SessionManager, settings: Settings):
    return CandidateBuilder(settings, get_domain(session_mgr).settings)


# Seconds between worker heartbeats; well inside INTERRUPTED_AFTER_S.
HEARTBEAT_EVERY_S = 60


def _spawn(target: Callable[[], None]) -> None:
    threading.Thread(target=target, daemon=True).start()


def _evaluation_worker(evaluator: EvaluationService, svc: EvolutionService, run_id: str):
    """Worker body: heart-beat while the evaluation runs, never leave it RUNNING."""
    stop = threading.Event()

    def beat() -> None:
        while not stop.wait(HEARTBEAT_EVERY_S):
            try:
                svc.heartbeat(run_id)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Evolve heartbeat for %s failed: %s", run_id, exc)

    def work() -> None:
        _spawn(beat)
        try:
            evaluator.execute(run_id)
        finally:
            stop.set()

    return work


@router.get("/capabilities")
async def capabilities(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    domain = get_domain(session_mgr)
    backend = str((getattr(domain, "info", None) or {}).get("graph_backend") or "")
    return {
        "enabled": bool(settings.evolution_enabled),
        "registry": "lakebase",
        "graph_backend": backend,
        "execution_supported": backend in ("", "databricks", "delta"),
        "operations": ["add_relationship", "set_relationship_mapping"],
        "filter_ops": ["eq"],
        "max_edges": 3,
    }


@router.get("/runs")
async def list_runs(
    domain: str = Query(...),
    parent_version: Optional[str] = Query(None),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    svc = _service(session_mgr, settings, domain)
    await run_blocking(svc.recover_interrupted)
    return {"runs": await run_blocking(svc.list_runs, parent_version)}


@router.post("/runs", status_code=202, dependencies=_builder_only)
async def create_run(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    body = await _json_object(request)
    svc = _service(session_mgr, settings, str(body.get("domain") or ""))
    parent_version = str(body.get("parent_version") or "")
    if not parent_version:
        raise ValidationError("parent_version is required")
    run = await run_blocking(
        lambda: svc.create_run(
            parent_version,
            body.get("intent") or {},
            question=str(body.get("question") or ""),
            owner=_actor(request),
            idempotency_key=str(body.get("idempotency_key") or ""),
        )
    )
    return {"run": run}


@router.get("/runs/{run_id}")
async def get_run(
    run_id: str,
    domain: str = Query(...),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    svc = _service(session_mgr, settings, domain)
    await run_blocking(svc.recover_interrupted)
    return {"run": await run_blocking(svc.get_run, run_id)}


@router.patch("/runs/{run_id}/intent", dependencies=_builder_only)
async def update_intent(
    run_id: str,
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    body = await _json_object(request)
    svc = _service(session_mgr, settings, str(body.get("domain") or ""))
    run = await run_blocking(
        svc.update_intent,
        run_id,
        int(body.get("revision") or 0),
        body.get("intent") or {},
    )
    return {"run": run}


@router.post("/runs/{run_id}/cancel", dependencies=_builder_only)
async def cancel_run(
    run_id: str,
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    body = await _json_object(request)
    svc = _service(session_mgr, settings, str(body.get("domain") or ""))
    return {
        "run": await run_blocking(svc.cancel, run_id, int(body.get("revision") or 0))
    }


@router.delete("/runs/{run_id}", dependencies=_builder_only)
async def delete_run(
    run_id: str,
    domain: str = Query(...),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Delete a run and drop the run-owned graph relations it built."""
    svc = _service(session_mgr, settings, domain)
    run = await run_blocking(svc.get_run, run_id)
    builds = (run.get("manifest") or {}).get("builds") or []
    dropped = []
    if builds:
        builder = _candidate_builder(session_mgr, settings)
        dropped = await run_blocking(builder.drop, get_domain(session_mgr), builds)
    return {"deleted": await run_blocking(svc.delete_run, run_id), "dropped": dropped}


@router.post("/runs/{run_id}/assess", dependencies=_builder_only)
async def assess_run(
    run_id: str,
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Run the coverage check for the run's intent against its parent version."""
    body = await _json_object(request)
    pipe = _pipeline(session_mgr, settings, str(body.get("domain") or ""))
    return {
        "run": await run_blocking(pipe.assess, run_id, int(body.get("revision") or 0))
    }


@router.post("/runs/{run_id}/proposal", dependencies=_builder_only)
async def propose(
    run_id: str,
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Build the rule-based proposal, or validate builder-edited ``operations``."""
    body = await _json_object(request)
    pipe = _pipeline(session_mgr, settings, str(body.get("domain") or ""))
    operations = body.get("operations")
    if operations is not None and not isinstance(operations, list):
        raise ValidationError("operations must be a list")
    run = await run_blocking(
        pipe.propose, run_id, int(body.get("revision") or 0), operations
    )
    return {"run": run}


@router.post("/runs/{run_id}/evaluate", status_code=202, dependencies=_builder_only)
async def evaluate(
    run_id: str,
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Build both arms and compare them; returns at once, poll the run.

    Body: ``revision`` plus the expected answer of the run's question —
    ``expected_ids`` inline, or ``gold_table`` + ``target_question_id`` —
    and optional protected ``checks`` (``question_id``, ``intent``,
    ``expected_ids``). Access to every source is checked here, as the
    caller; the builds then run as the app on the Build warehouse.
    """
    body = await _json_object(request)
    svc = _service(session_mgr, settings, str(body.get("domain") or ""))
    probe = _probe(session_mgr, settings)
    evaluator = EvaluationService(
        svc, _candidate_builder(session_mgr, settings), probe, settings=settings
    )
    run = await run_blocking(
        evaluator.prepare, run_id, int(body.get("revision") or 0), body
    )
    _spawn(_evaluation_worker(evaluator, svc, run_id))
    return {"run": run}


@router.get("/runs/{run_id}/results")
async def results(
    run_id: str,
    domain: str = Query(...),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    svc = _service(session_mgr, settings, domain)
    return {"results": await run_blocking(svc.list_results, run_id)}


@router.post("/runs/{run_id}/accept", dependencies=_builder_only)
async def accept(
    run_id: str,
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Create the next DRAFT version from a PASS run (idempotent)."""
    body = await _json_object(request)
    svc = _service(session_mgr, settings, str(body.get("domain") or ""))
    allocator = VersionAllocator(svc, svc.registry, _probe(session_mgr, settings))
    result = await run_blocking(
        allocator.accept, run_id, int(body.get("revision") or 0)
    )
    return result


@router.post("/runs/{run_id}/cleanup", dependencies=_builder_only)
async def cleanup(
    run_id: str,
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Drop the run-owned graph relations but keep the run as evidence.

    For accepted runs, whose record backs the new version's review link.
    Dropping is idempotent (``IF EXISTS``) and limited to ``__evo`` names.
    """
    body = await _json_object(request)
    svc = _service(session_mgr, settings, str(body.get("domain") or ""))
    run = await run_blocking(svc.get_run, run_id)
    if run["status"] == "RUNNING":
        raise ConflictError("Wait for the evaluation to finish or cancel it first")
    builds = (run.get("manifest") or {}).get("builds") or []
    builder = _candidate_builder(session_mgr, settings)
    dropped = await run_blocking(builder.drop, get_domain(session_mgr), builds)
    return {"dropped": dropped}
