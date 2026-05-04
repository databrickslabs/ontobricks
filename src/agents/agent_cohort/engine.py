"""
Cohort Discovery Agent engine.

One-shot agent: given a natural-language prompt and the active session's
ontology+graph, the LLM iteratively introspects the ontology with
read-only tools, builds a :class:`CohortRule` candidate, validates it via
``propose_rule``, optionally calls ``dry_run`` once, and returns a final
explanation. The proposed rule is captured in
``ToolContext.metadata['proposed_rule']`` so the route can return it as
structured JSON to hydrate the cohort form.

Mirrors :mod:`agents.agent_dtwin_chat.engine` exactly (same retry logic,
trace decorator, OpenAI-compatible schema). The only deltas are the
system prompt, the tool list, and the extra ``proposed_rule`` field on
:class:`AgentResult`.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from agents.agent_cohort.tools import TOOL_DEFINITIONS, TOOL_HANDLERS
from agents.engine_base import (
    AgentStep,
    accumulate_usage,
    call_serving_endpoint,
    dispatch_tool,
)
from agents.tools.context import ToolContext
from agents.tracing import trace_agent
from back.core.logging import get_logger

logger = get_logger(__name__)

MAX_ITERATIONS = 10
LLM_TIMEOUT = 120

_TRACE_NAME = "cohort_agent"


@dataclass
class AgentResult:
    """Outcome of a single cohort-agent turn."""

    success: bool
    reply: str = ""
    proposed_rule: Optional[dict] = None
    steps: List[AgentStep] = field(default_factory=list)
    iterations: int = 0
    error: str = ""
    usage: Dict[str, int] = field(default_factory=dict)


SYSTEM_PROMPT = """\
You are the Cohort Discovery assistant for OntoBricks. The user wants to
group entities in a knowledge graph by linkage and compatibility rules
("cohorts"). Your job is to translate a free-text prompt into a single
validated CohortRule JSON, then return.

CONTEXT
The user has already selected a domain. Every tool operates on that
session's ontology and triple store automatically -- you do not select a
domain. The user will REVIEW your proposed rule in a form before saving;
you never write anything.

TOOLS
  list_classes()                     -- ontology classes (uri, label).
  list_properties_of(class_uri)      -- data + object properties.
  count_class_members(class_uri)     -- live instance count.
  sample_values_of(class_uri,        -- distinct values seen for a property
                   property_uri,        (use this to spell value_equals /
                   limit=20)            value_in literals correctly).
  propose_rule(rule)                 -- validate and register the final rule
                                        (REQUIRED terminating call).
  dry_run(rule)                      -- run the engine, get cluster stats
                                        (call at most ONCE, after propose_rule).

RULE SCHEMA (CohortRule)
{
  "id":         "snake-case-id",
  "label":      "Human title",
  "class_uri":  "<class URI from list_classes>",
  "links": [
    { "shared_class": "<class URI>", "via": "<object property URI>?" }
  ],
  "links_combine": "any" | "all",
  "compatibility": [
    { "type": "same_value",   "property": "<property URI>" }
    | { "type": "value_equals", "property": "<property URI>", "value": <const> }
    | { "type": "value_in",     "property": "<property URI>", "values": [<const>...] }
    | { "type": "value_range",  "property": "<property URI>", "min": <num>?, "max": <num>? }
  ],
  "group_type": "connected" | "strict",
  "min_size":   2,
  "output":     { "graph": true }
}

WORKFLOW (follow exactly)
  1. Call list_classes() to discover the available classes.
  2. Pick the class_uri whose label best matches the user's intent
     (e.g. "people" -> Person, "consultants" -> Consultant).
  3. Call count_class_members on that class to confirm it has data.
  4. Call list_properties_of on that class to find:
       - the OBJECT properties that link members to a shared resource
         (use as `links[].via` and the range as `links[].shared_class`),
       - the DATATYPE properties used in compatibility constraints
         (e.g. status, region, level).
     If the response contains `object_properties_domain_unknown=true`,
     the ontology does not encode rdfs:domain for these properties --
     pick the one whose label/name best matches the user's intent (e.g.
     "staffedOn" for "staffed together"). Use the URIs as-returned even
     when `uri_synthesised=true`.
     If both `data_properties` and `object_properties` are empty, the
     ontology has no properties for that class -- ask the user how they
     identify members instead of guessing.
  5. For each datatype property used in `value_equals` / `value_in`,
     call sample_values_of so the literal you put in the rule matches
     the data's casing/spelling exactly.
  6. Build a complete rule and call propose_rule(rule). If it returns
     valid=false, READ THE ERRORS, fix the rule, and call again.
  7. (Optional) Call dry_run(rule) ONCE to surface cluster_count.
  8. Reply with a short markdown explanation: what the rule does, why
     each link / constraint was chosen, and the dry-run outcome (if any).
     Do NOT dump the full JSON in the reply -- the UI already has it.

RULES
  * Only use class / property URIs returned by the tools. Never invent.
  * Default group_type=connected and min_size=2 unless the user is
    explicit about cliques or larger floors.
  * Default `output.graph = true`. Do not configure UC table output --
    that is a deliberate user choice in the form.
  * If the prompt is too vague to pick a class, ask one short
    clarifying question instead of guessing.
  * Keep the reply short (under 8 lines). The form is the interface.
"""


@trace_agent(name="cohort_agent")
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
) -> AgentResult:
    """Run one turn of the Cohort Discovery agent.

    Same boilerplate as :func:`agents.agent_dtwin_chat.engine.run_agent`
    -- see that module for argument semantics. The only behavioural
    difference is that this engine surfaces the proposed CohortRule via
    :attr:`AgentResult.proposed_rule` (captured by ``propose_rule`` into
    :attr:`ToolContext.metadata`).
    """
    logger.info(
        "===== COHORT AGENT START ===== endpoint=%s, domain=%s, base_url=%s",
        endpoint_name,
        domain_name,
        base_url,
    )

    ctx = ToolContext(
        host=host.rstrip("/") if host else "",
        token=token or "",
        dtwin_base_url=base_url,
        dtwin_session_cookies=session_cookies or {},
        dtwin_session_headers=session_headers or {},
        dtwin_registry_params=registry_params or {},
        dtwin_domain_name=domain_name or "",
    )

    result = AgentResult(success=False)

    messages: List[dict] = [{"role": "system", "content": SYSTEM_PROMPT}]
    if conversation_history:
        for msg in conversation_history:
            if not isinstance(msg, dict):
                continue
            role = msg.get("role")
            content = msg.get("content", "")
            if role in ("user", "assistant") and isinstance(content, str):
                messages.append({"role": role, "content": content})

    messages.append({"role": "user", "content": user_message})

    for iteration in range(MAX_ITERATIONS):
        result.iterations = iteration + 1
        is_last = iteration == MAX_ITERATIONS - 1
        send_tools = TOOL_DEFINITIONS if not is_last else None

        if on_step:
            on_step(f"Iteration {iteration + 1}...")

        try:
            llm_response = call_serving_endpoint(
                host,
                token,
                endpoint_name,
                messages,
                tools=send_tools,
                max_tokens=2048,
                temperature=0.1,
                timeout=LLM_TIMEOUT,
                trace_name=_TRACE_NAME,
            )
        except Exception as exc:
            error_msg = f"LLM request failed: {exc}"
            logger.error(
                "cohort_agent: %s at iteration %d", error_msg, iteration + 1
            )
            result.error = error_msg
            return result

        accumulate_usage(result.usage, llm_response.get("usage", {}))

        choices = llm_response.get("choices", [])
        if not choices:
            logger.warning(
                "cohort_agent: empty choices in LLM response at iteration %d",
                iteration + 1,
            )
            result.error = "No choices in LLM response"
            return result

        message = choices[0].get("message", {})
        content = message.get("content", "") or ""
        tool_calls = message.get("tool_calls")

        if tool_calls:
            messages.append(message)

            for tc in tool_calls:
                func = tc.get("function", {})
                tool_name = func.get("name", "")
                tool_id = tc.get("id", "")
                raw_args = func.get("arguments", "{}")

                try:
                    arguments = (
                        json.loads(raw_args)
                        if isinstance(raw_args, str)
                        else raw_args
                    )
                except json.JSONDecodeError:
                    arguments = {}

                logger.info(
                    "cohort_agent: iteration %d -- tool_call '%s'",
                    iteration + 1,
                    tool_name,
                )

                result.steps.append(
                    AgentStep(
                        step_type="tool_call",
                        content=json.dumps(arguments, default=str)[:600],
                        tool_name=tool_name,
                    )
                )

                tool_t0 = time.time()
                tool_result = dispatch_tool(
                    TOOL_HANDLERS,
                    ctx,
                    tool_name,
                    arguments,
                    trace_name=_TRACE_NAME,
                )
                tool_elapsed = int((time.time() - tool_t0) * 1000)

                result.steps.append(
                    AgentStep(
                        step_type="tool_result",
                        content=tool_result[:500],
                        tool_name=tool_name,
                        duration_ms=tool_elapsed,
                    )
                )

                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tool_id,
                        "content": tool_result,
                    }
                )
        else:
            result.success = True
            result.reply = content
            result.proposed_rule = ctx.metadata.get("proposed_rule")
            result.steps.append(
                AgentStep(step_type="output", content=content[:500])
            )
            logger.info(
                "===== COHORT AGENT DONE ===== iterations=%d, has_rule=%s",
                result.iterations,
                bool(result.proposed_rule),
            )
            return result

    result.error = "Max iterations reached"
    result.proposed_rule = ctx.metadata.get("proposed_rule")
    result.reply = (
        "I ran out of steps before finishing. "
        "Please simplify or split your request and try again."
    )
    return result
