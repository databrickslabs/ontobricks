"""Internal API -- what-if simulation sandboxes (Simulation menu)."""

import json
import os
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request

from api.routers.internal._graph_access import assert_domain_graph_read
from api.routers.internal._guards import require
from back.core.errors import InfrastructureError, ValidationError
from back.core.helpers import extract_local_name, require_domain_llm, run_blocking
from back.core.logging import get_logger
from back.objects.digitaltwin import DigitalTwin
from back.objects.registry import ROLE_BUILDER
from back.objects.session import SessionManager, get_domain, get_session_manager
from back.objects.simulation import Patch, Simulation
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


def _facts_user_message(prompt: str, scope: dict, existing: list) -> str:
    parts = [f"REQUEST\n{prompt}"]
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


_TOOL_CALL_LABELS = {
    "list_classes": "Listing ontology classes",
    "list_properties_of": "Looking up class properties",
    "search_entities": "Searching the knowledge graph",
    "list_entity_relationships": "Checking existing relationships",
    "propose_patches": "Validating hypothetical facts",
}
_TOOL_DONE_LABELS = {
    "list_classes": "Listed ontology classes",
    "list_properties_of": "Looked up class properties",
    "search_entities": "Searched the knowledge graph",
    "list_entity_relationships": "Checked existing relationships",
    "propose_patches": "Validated hypothetical facts",
}


def _facts_step_label(step) -> str:
    """Human-readable progress line for one agent step."""
    kind = str(getattr(step, "step_type", "") or "")
    tool = str(getattr(step, "tool_name", "") or "")
    args = {}
    raw = getattr(step, "content", "") or ""
    if isinstance(raw, str) and raw.startswith("{"):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                args = parsed
        except (TypeError, ValueError):
            args = {}
    if kind == "thinking":
        return "Asking the model"
    if kind == "output":
        return "Writing the summary"
    if kind == "tool_call":
        if tool == "search_entities" and args.get("query"):
            return f"Searching the graph for “{str(args.get('query')).strip()[:80]}”"
        if tool == "list_properties_of" and args.get("class_uri"):
            local = extract_local_name(str(args.get("class_uri")))
            if local:
                return f"Looking up properties of {local}"
        return _TOOL_CALL_LABELS.get(tool) or f"Calling {tool or 'a tool'}"
    if kind == "tool_result":
        return _TOOL_DONE_LABELS.get(tool) or f"Finished {tool or 'tool'}"
    return "Working"


def _facts_failure_message(error: Optional[str]) -> str:
    """Client-safe reason for a failed propose-facts turn (detail is stripped)."""
    text = str(error or "")
    low = text.lower()
    if "endpoint_not_found" in low or "endpoint does not exist" in low:
        return (
            "Could not propose hypothetical facts: the selected LLM was not found. "
            "Pick a model in Domain → General → AI."
        )
    if "max iterations" in low:
        return (
            "Could not propose hypothetical facts: the request was too complex. "
            "Try a simpler description."
        )
    if text.startswith("LLM request failed"):
        return "Could not propose hypothetical facts: the LLM request failed."
    return "Could not propose hypothetical facts"


def _facts_success_payload(result, prompt: str, scope, description: str, existing) -> dict:
    return {
        "success": True,
        "patches": Patch.append_all(existing, result.proposed_patches or []),
        "description": _scenario_description(result, prompt, scope or {}, description),
        "reply": result.reply or "",
        "usage": result.usage,
    }


def _facts_agent_kwargs(request: Request, body: dict, session_mgr, settings) -> dict:
    prompt = str(body.get("prompt") or "").strip()
    if not prompt:
        raise ValidationError("Describe the facts to add")
    if len(prompt) > _MAX_FACT_PROMPT:
        raise ValidationError("Fact description is too long")
    domain = get_domain(session_mgr)
    host, token, llm_endpoint, llm_endpoint_kind = require_domain_llm(domain, settings)
    reg = DigitalTwin.resolve_registry(session_mgr, settings)
    info = getattr(domain, "info", None) or {}
    scope = body.get("scope") or {}
    raw_existing = body.get("existing_patches") or []
    existing = raw_existing if isinstance(raw_existing, list) else []
    return {
        "prompt": prompt,
        "scope": scope,
        "existing": existing,
        "agent": {
            "host": host,
            "token": token,
            "endpoint_name": llm_endpoint,
            "endpoint_kind": llm_endpoint_kind,
            "base_url": _loopback_base_url(),
            "domain_name": str(info.get("name") or ""),
            "registry_params": {
                "registry_catalog": reg.get("catalog") or "",
                "registry_schema": reg.get("schema") or "",
                "registry_volume": reg.get("volume") or "",
                "simulation_id": str(body.get("simulation_id") or ""),
            },
            "session_cookies": dict(request.cookies or {}),
            "session_headers": _forwarded_headers(request),
            "user_message": _facts_user_message(prompt, scope, existing),
        },
    }


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

    prep = _facts_agent_kwargs(request, body, session_mgr, settings)
    with map_route_errors("Simulation facts agent request failed", logger):
        result = await asyncio.to_thread(run_agent, **prep["agent"])
    if not result.success:
        raise InfrastructureError(
            _facts_failure_message(result.error),
            detail=result.error or None,
        )
    return _facts_success_payload(
        result, prep["prompt"], prep["scope"], "", prep["existing"]
    )


@router.post("/facts/agent/stream", dependencies=_builder_only)
async def propose_facts_stream(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Stream propose-facts agent steps, then the same payload as ``/facts/agent``."""
    import asyncio
    import json as _json

    from fastapi.responses import StreamingResponse

    from agents.agent_simulation_facts import run_agent
    from agents.engine_base import AgentStep
    from api.routers.internal._helpers import map_route_errors

    body = await _json_object(request)
    prep = _facts_agent_kwargs(request, body, session_mgr, settings)
    loop = asyncio.get_running_loop()
    event_queue: asyncio.Queue = asyncio.Queue()

    def _on_event(step: AgentStep) -> None:
        asyncio.run_coroutine_threadsafe(event_queue.put(step), loop).result(timeout=10)

    async def _run_agent_task() -> None:
        try:
            with map_route_errors("Simulation facts agent request failed", logger):
                result = await asyncio.to_thread(
                    run_agent, **prep["agent"], on_event=_on_event
                )
            await event_queue.put(("done", result))
        except Exception as exc:
            await event_queue.put(("error", str(exc)))

    agent_task = asyncio.create_task(_run_agent_task())

    async def _generate():
        try:
            while True:
                item = await event_queue.get()
                if isinstance(item, tuple):
                    kind, payload = item
                    if kind == "done":
                        result = payload
                        if not result.success:
                            yield "data: " + _json.dumps({
                                "type": "error",
                                "message": _facts_failure_message(result.error),
                            }) + "\n\n"
                            break
                        body = _facts_success_payload(
                            result, prep["prompt"], prep["scope"], "", prep["existing"]
                        )
                        body["type"] = "done"
                        yield "data: " + _json.dumps(body) + "\n\n"
                        break
                    yield "data: " + _json.dumps({
                        "type": "error",
                        "message": _facts_failure_message(str(payload)),
                    }) + "\n\n"
                    break
                if isinstance(item, AgentStep):
                    yield "data: " + _json.dumps({
                        "type": "step",
                        "step_type": item.step_type,
                        "tool_name": item.tool_name,
                        "content": item.content,
                        "duration_ms": item.duration_ms,
                        "label": _facts_step_label(item),
                    }) + "\n\n"
        finally:
            if not agent_task.done():
                agent_task.cancel()

    return StreamingResponse(
        _generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


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
    """One page of added / removed triples from the sandbox before/after companions."""
    assert_domain_graph_read(request)
    data = await run_blocking(sim.diff_page, sim_id, kind, page, page_size, search)
    return {"success": True, **data}


@router.get("/{sim_id}/compare")
async def compare_simulation_to_live(
    sim_id: str,
    request: Request,
    include_inferred: bool = Query(True),
    sim: Simulation = Depends(_simulation),
):
    """Sandbox vs current live graph on the same scope, as a status-tagged graph."""
    assert_domain_graph_read(request)
    data = await run_blocking(
        sim.compare_to_live, sim_id, include_inferred=include_inferred
    )
    return {"success": True, **data}
