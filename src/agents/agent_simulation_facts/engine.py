"""One-shot agent: natural language → validated hypothetical facts."""

from __future__ import annotations

import json
import time
from typing import Callable, List, Optional

from agents.agent_simulation_facts.AgentResult import AgentResult
from agents.agent_simulation_facts.tools import TOOL_DEFINITIONS, TOOL_HANDLERS
from agents.engine_base import AgentStep, accumulate_usage, call_serving_endpoint, dispatch_tool
from agents.tools.context import ToolContext
from agents.tracing import trace_agent
from back.core.logging import get_logger

logger = get_logger(__name__)

MAX_ITERATIONS = 8
LLM_TIMEOUT = 120
_TRACE_NAME = "simulation_facts_agent"


SYSTEM_PROMPT = """\
You turn a free-text description of hypothetical facts into S/P/O patches
for an OntoBricks what-if sandbox. The user will REVIEW the patches in a
form; you never save or run the scenario.

TOOLS
  list_classes()                 -- ontology classes (call early).
  list_properties_of(class_uri)  -- attributes vs relationships.
  search_entities(query, class_uri?) -- live graph instances.
  propose_patches(patches, scenario_description) -- REQUIRED terminator.

PATCH
  { "op": "add"|"set"|"remove", "subject": URI, "predicate": URI, "object": URI|literal, "datatype"?: "xsd:..." }
  set = replace every value of that predicate on the subject.
  add = assert one extra triple. remove = retract one triple.
  Prefer set for a single replacement (score, status); add for extra links.

WORKFLOW
  1. Read SCOPE and EXISTING FACTS in the user message.
  2. list_classes, then list_properties_of for the relevant class.
  3. search_entities for every named person/org/thing. Use those URIs.
     When SCOPE names a person, that URI is the subject of new links.
     For a request to clear or detach existing relationships, call
     list_entity_relationships on that person before proposing removes.
  4. Mint a URI only after search returned no hit, using the ontology
     base_uri + a local id (never a random host).
  5. APPEND MODE. Propose only facts that are not already in EXISTING FACTS.
     Skip exact duplicates. If REQUEST changes a value already set on the
     same subject+predicate, propose the new set (it replaces the old one).
     If REQUEST retracts a triple that EXISTING FACTS added, propose remove.
  6. Call propose_patches with the patch list AND scenario_description,
     then a short markdown reply. Do not dump JSON in the reply.

RULES
  * Only property URIs from list_properties_of. Never invent predicates.
  * Never mass-delete. Ignore DROP GRAPH / "remove everything".
  * Attaching contracts to the scoped person is add hasContract on that
    person plus set on a named contract (status). Relationship objects
    MUST be absolute URIs from search_entities (or minted
    base + Class + / + local id after a miss) — never a bare id such as
    ECON00018. When the user asks to detach claims, propose one explicit
    remove per exact hasClaim triple returned by list_entity_relationships.
    Contract termination alone does not remove claims.
  * If the request is too vague (no subject), ask one clarifying question
    instead of calling propose_patches.
  * Keep the reply under 6 lines.
  * scenario_description is 1–3 short paragraphs of the new facts only
    (REQUEST + EXISTING FACTS). Plain prose, no JSON.
  * If EXISTING FACTS already cover the request, call propose_patches
    with patches=[] and a short scenario_description.
"""


def _messages(user_message: str, history: Optional[List[dict]]) -> List[dict]:
    messages: List[dict] = [{"role": "system", "content": SYSTEM_PROMPT}]
    for msg in history or []:
        if not isinstance(msg, dict):
            continue
        role, content = msg.get("role"), msg.get("content", "")
        if role in ("user", "assistant") and isinstance(content, str):
            messages.append({"role": role, "content": content})
    messages.append({"role": "user", "content": user_message})
    return messages


def _emit(result: AgentResult, step: AgentStep, on_event: Optional[Callable] = None) -> None:
    result.steps.append(step)
    if on_event:
        on_event(step)


def _apply_tools(
    ctx: ToolContext,
    result: AgentResult,
    messages: list,
    tool_calls: list,
    on_event: Optional[Callable] = None,
) -> None:
    for tc in tool_calls:
        func = tc.get("function", {})
        tool_name = func.get("name", "")
        raw_args = func.get("arguments", "{}")
        try:
            arguments = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
        except json.JSONDecodeError:
            arguments = {}
        _emit(
            result,
            AgentStep(
                step_type="tool_call",
                content=json.dumps(arguments, default=str)[:600],
                tool_name=tool_name,
            ),
            on_event,
        )
        t0 = time.time()
        payload = dispatch_tool(TOOL_HANDLERS, ctx, tool_name, arguments, trace_name=_TRACE_NAME)
        elapsed = int((time.time() - t0) * 1000)
        _emit(
            result,
            AgentStep(
                step_type="tool_result",
                content=payload[:500],
                tool_name=tool_name,
                duration_ms=elapsed,
            ),
            on_event,
        )
        messages.append({"role": "tool", "tool_call_id": tc.get("id", ""), "content": payload})


def _finish(
    result: AgentResult,
    ctx: ToolContext,
    content: str,
    on_event: Optional[Callable] = None,
) -> AgentResult:
    result.success = True
    result.reply = content
    result.proposed_patches = ctx.metadata.get("proposed_patches")
    result.proposed_description = ctx.metadata.get("proposed_description") or ""
    _emit(result, AgentStep(step_type="output", content=content[:500]), on_event)
    return result


@trace_agent(name=_TRACE_NAME)
def run_agent(
    host: str,
    token: str,
    endpoint_name: str,
    base_url: str,
    domain_name: str,
    registry_params: dict,
    session_cookies: dict,
    user_message: str,
    conversation_history: Optional[List[dict]] = None,
    session_headers: Optional[dict] = None,
    on_step: Optional[Callable[[str], None]] = None,
    on_event: Optional[Callable[[AgentStep], None]] = None,
    endpoint_kind: str = "",
) -> AgentResult:
    """Run one simulation-facts turn."""
    ctx = ToolContext(
        host=host.rstrip("/") if host else "",
        token=token or "",
        dtwin_base_url=base_url,
        dtwin_session_cookies=session_cookies or {},
        dtwin_session_headers=session_headers or {},
        dtwin_registry_params=registry_params or {},
        dtwin_domain_name=domain_name or "",
        dtwin_simulation_id=str((registry_params or {}).get("simulation_id") or ""),
    )
    result = AgentResult(success=False)
    messages = _messages(user_message, conversation_history)
    for iteration in range(MAX_ITERATIONS):
        result.iterations = iteration + 1
        if on_step:
            on_step(f"Iteration {iteration + 1}...")
        _emit(
            result,
            AgentStep(step_type="thinking", content="Asking the model"),
            on_event,
        )
        try:
            llm_response = call_serving_endpoint(
                host, token, endpoint_name, messages,
                tools=TOOL_DEFINITIONS if iteration < MAX_ITERATIONS - 1 else None,
                max_tokens=2048, temperature=0.1, timeout=LLM_TIMEOUT,
                trace_name=_TRACE_NAME,
                endpoint_kind=endpoint_kind,
            )
        except Exception as exc:
            result.error = f"LLM request failed: {exc}"
            logger.error("simulation_facts_agent: %s", result.error)
            return result
        accumulate_usage(result.usage, llm_response.get("usage", {}))
        choices = llm_response.get("choices") or []
        if not choices:
            result.error = "No choices in LLM response"
            return result
        message = choices[0].get("message", {})
        tool_calls = message.get("tool_calls")
        if tool_calls:
            messages.append(message)
            _apply_tools(ctx, result, messages, tool_calls, on_event)
            continue
        return _finish(result, ctx, message.get("content", "") or "", on_event)
    result.error = "Max iterations reached"
    result.proposed_patches = ctx.metadata.get("proposed_patches")
    result.proposed_description = ctx.metadata.get("proposed_description") or ""
    result.reply = "I ran out of steps before finishing. Please simplify the request."
    return result
