"""Simulation business-insights agent: one-shot loop over Live vs Sandbox answers.

Given the Live and Sandbox answers to the same Graph Chat prompt and the
simulation scenario, the agent explains the business difference in four
Markdown sections. It may verify facts with the read-only dual-graph tools in
:mod:`agents.agent_simulation_insights.tools`. Each turn is stateless.
"""

from __future__ import annotations

import json
import time
from typing import Callable, List, Optional

from agents.agent_simulation_insights.AgentResult import AgentResult
from agents.agent_simulation_insights.tools import TOOL_DEFINITIONS, TOOL_HANDLERS
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

MAX_ITERATIONS = 6
LLM_TIMEOUT = 120
MAX_TOKENS = 1024
ANSWER_LIMIT = 6000
TRUNCATION_MARK = "… [truncated]"
_TRACE_NAME = "simulation_insights_agent"

SYSTEM_PROMPT = """\
You are a business analyst. A what-if scenario was applied to an OntoBricks
knowledge graph. The same prompt was asked twice: once against the LIVE graph
and once against the SANDBOX (the scenario). You receive both answers and the
scenario's FACT CHANGES. Explain the business difference.

TOOLS
  list_entity_types, describe_entity, query_graphql, get_entity_context.
  Every tool REQUIRES graph="live" or graph="sandbox".
  Call tools only to verify an entity or property named in the answers or
  in the FACT CHANGES. Prefer few calls. Never invent differences.

OUTPUT
Markdown, about 200 words, with exactly these four headings in this order:
## What changed
## Why
## Business impact
## Risks / recommendations

RULES
  * Differences come only from the two answers and the tools. Never invent one.
  * In "Why", link each difference to a specific FACT CHANGE when one applies.
  * If the answers are materially the same, say "No material difference" under
    "What changed" and note whether the scenario affected this question.
  * Write for a business reader, not a graph engineer. No JSON, no URIs unless
    they are needed to name an entity.
"""


def truncate_answer(text: str, limit: int = ANSWER_LIMIT) -> str:
    """Cut *text* to *limit* characters, marking the cut."""
    raw = text or ""
    if len(raw) <= limit:
        return raw
    return raw[:limit] + TRUNCATION_MARK


def build_user_message(
    *,
    message: str,
    live_answer: str,
    sandbox_answer: str,
    scenario: dict,
) -> str:
    """Assemble the prompt, scenario facts and both answers for the model."""
    patches = json.dumps(scenario.get("patches") or [], ensure_ascii=False)
    return (
        f"PROMPT\n{message}\n\n"
        f"SCENARIO\nlabel: {scenario.get('label') or ''}\n"
        f"description: {scenario.get('description') or ''}\n\n"
        f"FACT CHANGES (JSON)\n{patches}\n\n"
        f"LIVE ANSWER\n{truncate_answer(live_answer)}\n\n"
        f"SANDBOX ANSWER\n{truncate_answer(sandbox_answer)}\n"
    )


def _tool_context(
    *,
    host: str,
    token: str,
    base_url: str,
    session_cookies: dict,
    session_headers: Optional[dict],
    registry_params: dict,
    domain_name: str,
    include_inferred: bool,
    simulation_id: str,
) -> ToolContext:
    """Carrier context: the request's inference flag and the scenario id."""
    return ToolContext(
        host=host.rstrip("/") if host else "",
        token=token or "",
        dtwin_base_url=base_url,
        dtwin_session_cookies=dict(session_cookies or {}),
        dtwin_session_headers=dict(session_headers or {}),
        dtwin_registry_params=dict(registry_params or {}),
        dtwin_domain_name=domain_name or "",
        dtwin_include_inferred=bool(include_inferred),
        dtwin_simulation_id=str(simulation_id or "").strip(),
    )


def _emit(result: AgentResult, step: AgentStep, on_event) -> None:
    """Record *step* on the result and forward it to the live callback."""
    result.steps.append(step)
    if on_event:
        on_event(step)


def _parse_arguments(raw_args) -> dict:
    """Decode a tool call's JSON arguments; malformed JSON becomes ``{}``."""
    if not isinstance(raw_args, str):
        return raw_args or {}
    try:
        return json.loads(raw_args)
    except json.JSONDecodeError:
        return {}


def _run_tool_calls(
    tool_calls: list, ctx: ToolContext, result: AgentResult, on_event
) -> List[dict]:
    """Dispatch each requested tool and return the ``tool`` messages for the model."""
    tool_messages: List[dict] = []
    for tc in tool_calls:
        func = tc.get("function", {})
        tool_name = func.get("name", "")
        arguments = _parse_arguments(func.get("arguments", "{}"))
        _emit(
            result,
            AgentStep(
                step_type="tool_call",
                content=json.dumps(arguments, default=str),
                tool_name=tool_name,
            ),
            on_event,
        )
        started = time.time()
        tool_result = dispatch_tool(
            TOOL_HANDLERS, ctx, tool_name, arguments, trace_name=_TRACE_NAME
        )
        _emit(
            result,
            AgentStep(
                step_type="tool_result",
                content=tool_result[:500],
                tool_name=tool_name,
                duration_ms=int((time.time() - started) * 1000),
            ),
            on_event,
        )
        tool_messages.append(
            {"role": "tool", "tool_call_id": tc.get("id", ""), "content": tool_result}
        )
    return tool_messages


def _record_reply(message_block: dict, result: AgentResult, on_event) -> None:
    """Store the model's final Markdown answer, or flag an empty one."""
    reply = (message_block.get("content") or "").strip()
    if not reply:
        result.error = "Empty answer from the insights agent"
        return
    result.success = True
    result.reply = reply
    _emit(result, AgentStep(step_type="output", content=reply[:500]), on_event)
    logger.info(
        "===== SIMULATION INSIGHTS DONE ===== iterations=%d, reply_len=%d",
        result.iterations,
        len(reply),
    )


@trace_agent(name="simulation_insights")
def run_agent(
    *,
    host: str,
    token: str,
    endpoint_name: str,
    base_url: str,
    domain_name: str,
    registry_params: dict,
    session_cookies: dict,
    message: str,
    live_answer: str,
    sandbox_answer: str,
    include_inferred: bool,
    simulation_id: str,
    scenario: dict,
    session_headers: Optional[dict] = None,
    endpoint_kind: str = "",
    on_event: Optional[Callable[[AgentStep], None]] = None,
) -> AgentResult:
    """Run the insights agent once and return its Markdown reply."""
    logger.info(
        "===== SIMULATION INSIGHTS START ===== endpoint=%s, simulation=%s",
        endpoint_name,
        simulation_id,
    )
    ctx = _tool_context(
        host=host,
        token=token,
        base_url=base_url,
        session_cookies=session_cookies,
        session_headers=session_headers,
        registry_params=registry_params,
        domain_name=domain_name,
        include_inferred=include_inferred,
        simulation_id=simulation_id,
    )
    result = AgentResult(success=False)
    messages: List[dict] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": build_user_message(
                message=message,
                live_answer=live_answer,
                sandbox_answer=sandbox_answer,
                scenario=scenario,
            ),
        },
    ]

    for iteration in range(MAX_ITERATIONS):
        result.iterations = iteration + 1
        is_last = iteration == MAX_ITERATIONS - 1
        try:
            llm_response = call_serving_endpoint(
                host,
                token,
                endpoint_name,
                messages,
                tools=None if is_last else TOOL_DEFINITIONS,
                max_tokens=MAX_TOKENS,
                temperature=0.1,
                timeout=LLM_TIMEOUT,
                trace_name=_TRACE_NAME,
                endpoint_kind=endpoint_kind,
            )
        except Exception as exc:
            result.error = f"LLM request failed: {exc}"
            logger.error("simulation_insights: %s", result.error)
            return result

        accumulate_usage(result.usage, llm_response.get("usage", {}))
        choices = llm_response.get("choices", [])
        if not choices:
            result.error = "No choices in LLM response"
            return result

        message_block = choices[0].get("message", {})
        tool_calls = message_block.get("tool_calls")
        if tool_calls:
            messages.append(message_block)
            messages.extend(_run_tool_calls(tool_calls, ctx, result, on_event))
            continue

        _record_reply(message_block, result, on_event)
        return result

    result.error = "Max iterations reached"
    return result
