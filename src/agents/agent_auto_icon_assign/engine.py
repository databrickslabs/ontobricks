"""
OntoBricks Auto Icon Assign Agent Engine.

Implements an agentic loop that uses the Databricks Foundation Model API
with function calling to autonomously inspect the ontology and metadata,
then assign appropriate emoji icons to each entity.

Fallback: if the LLM endpoint does not support the ``tools`` parameter the
engine transparently degrades to a single-shot generation (no tool calls).
"""

import json
import re
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

import requests

from back.core.logging import get_logger
from agents.agent_auto_icon_assign.tools import (
    ToolContext,
    TOOL_DEFINITIONS,
    TOOL_HANDLERS,
)
from agents.engine_base import (
    AgentStep,
    call_serving_endpoint,
    dispatch_tool,
    extract_message_content,
    accumulate_usage,
)
from agents.tracing import trace_agent

logger = get_logger(__name__)

MAX_ITERATIONS = 8
LLM_TIMEOUT = 120

_TRACE_NAME = "auto_icon_assign"


# =====================================================
# Data classes
# =====================================================


@dataclass
class AgentResult:
    """Outcome of a full agent run."""

    success: bool
    icons: Dict[str, str] = field(default_factory=dict)
    steps: List[AgentStep] = field(default_factory=list)
    iterations: int = 0
    error: str = ""
    usage: Dict[str, int] = field(default_factory=dict)


# =====================================================
# System prompt
# =====================================================

SYSTEM_PROMPT = """\
You are an expert at choosing visual emoji icons for ontology entities.

TOOLS
You have three tools:
  • get_ontology  – retrieve the ontology entities (classes) and relationships
  • get_metadata  – get database table schemas to understand data context
  • assign_icons  – save your chosen {entity_name: emoji} mapping

WORKFLOW
1. Call get_ontology to see the entity names, their attributes, and relationships.
2. Optionally call get_metadata to understand what each entity represents in data.
3. For EVERY entity, choose a single Unicode emoji that best visually represents
   the concept. Consider the entity name, its attributes, and its relationships.
4. Call assign_icons with a JSON object mapping each entity name to its emoji.

RULES
• Assign exactly ONE emoji per entity.
• Use diverse, meaningful emojis – avoid reusing the same emoji for different entities.
• Prefer concrete, recognizable emojis (🏥 for Hospital, 👤 for Person, 📦 for Product).
• For abstract concepts, pick the closest metaphorical emoji.
• You MUST call assign_icons with ALL entities at once before finishing.
• Do NOT output any text after calling assign_icons."""


# =====================================================
# Internal helpers
# =====================================================


def _parse_icons_from_text(text: str) -> Dict[str, str]:
    """Last-resort: extract a JSON object from free-form LLM text."""
    cleaned = text.strip()
    fence = re.search(r"```(?:json)?\s*([\s\S]*?)```", cleaned)
    if fence:
        cleaned = fence.group(1).strip()
    try:
        obj = json.loads(cleaned)
        if isinstance(obj, dict):
            return {k: v for k, v in obj.items() if isinstance(v, str)}
    except json.JSONDecodeError:
        pass
    brace = re.search(r"\{[^{}]+\}", cleaned)
    if brace:
        try:
            obj = json.loads(brace.group())
            if isinstance(obj, dict):
                return {k: v for k, v in obj.items() if isinstance(v, str)}
        except json.JSONDecodeError:
            pass
    return {}


# =====================================================
# Public entry point
# =====================================================


@trace_agent(name="auto_icon_assign")
def run_agent(
    host: str,
    token: str,
    endpoint_name: str,
    entity_names: List[str],
    metadata: Optional[dict] = None,
    ontology: Optional[dict] = None,
    on_step: Optional[Callable[[str], None]] = None,
) -> AgentResult:
    """Run the icon-mapping agent.

    Returns an AgentResult whose ``icons`` dict maps entity names to emojis.
    """
    logger.info(
        "===== ICON AGENT START ===== endpoint=%s, %d entities",
        endpoint_name,
        len(entity_names),
    )

    ctx = ToolContext(
        host=host.rstrip("/"),
        token=token,
        metadata=metadata or {},
        ontology=ontology,
        icon_results={},
    )

    result = AgentResult(success=False)

    names_str = ", ".join(entity_names)
    user_prompt = (
        f"Assign emoji icons to the following {len(entity_names)} ontology entities: "
        f"{names_str}\n\n"
        "Start by calling get_ontology to understand the entities and their context, "
        "then call assign_icons with your choices."
    )

    messages: List[dict] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt},
    ]

    total_usage: Dict[str, int] = {"prompt_tokens": 0, "completion_tokens": 0}
    tools_supported = True

    def notify(msg: str):
        if on_step:
            on_step(msg)

    notify("Starting icon mapping agent…")

    for iteration in range(MAX_ITERATIONS):
        logger.info(
            "----- Icon Agent Iteration %d/%d — %d messages -----",
            iteration + 1,
            MAX_ITERATIONS,
            len(messages),
        )
        notify(f"Agent thinking… (step {iteration + 1})")

        is_last = iteration >= MAX_ITERATIONS - 1
        send_tools = TOOL_DEFINITIONS if (tools_supported and not is_last) else None

        try:
            llm_response = call_serving_endpoint(
                host,
                token,
                endpoint_name,
                messages,
                tools=send_tools,
                temperature=0.3,
                timeout=LLM_TIMEOUT,
                trace_name=_TRACE_NAME,
            )
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "?"
            if exc.response is not None and status in (400, 422) and tools_supported:
                logger.warning(
                    "Icon Agent: endpoint rejected tools — falling back to direct mode"
                )
                tools_supported = False
                notify("Endpoint does not support tools – using direct generation…")
                try:
                    llm_response = call_serving_endpoint(
                        host,
                        token,
                        endpoint_name,
                        messages,
                        tools=None,
                        temperature=0.3,
                        timeout=LLM_TIMEOUT,
                        trace_name=_TRACE_NAME,
                    )
                except Exception as inner:
                    result.error = f"LLM request failed: {inner}"
                    logger.error("Icon Agent: fallback also failed: %s", inner)
                    return result
            else:
                result.error = f"LLM request failed: {exc}"
                logger.error(
                    "Icon Agent: LLM error at iteration %d: %s", iteration + 1, exc
                )
                return result
        except requests.exceptions.ReadTimeout:
            result.error = f"LLM request timed out after {LLM_TIMEOUT}s"
            logger.error("Icon Agent: timeout at iteration %d", iteration + 1)
            return result
        except requests.exceptions.RequestException as exc:
            result.error = f"LLM request failed: {exc}"
            logger.error(
                "Icon Agent: request error at iteration %d: %s", iteration + 1, exc
            )
            return result

        accumulate_usage(total_usage, llm_response.get("usage", {}))

        choice = llm_response.get("choices", [{}])[0]
        message = choice.get("message", {})
        tool_calls = message.get("tool_calls", [])

        if tool_calls:
            logger.info(
                "Iteration %d: %d tool call(s): [%s]",
                iteration + 1,
                len(tool_calls),
                ", ".join(tc.get("function", {}).get("name", "?") for tc in tool_calls),
            )
            messages.append(message)

            for tc in tool_calls:
                func = tc.get("function", {})
                tool_name = func.get("name", "")
                raw_args = func.get("arguments", "{}")
                tool_id = tc.get("id", "")

                try:
                    arguments = json.loads(raw_args)
                except json.JSONDecodeError:
                    arguments = {}

                notify(f"Calling {tool_name}…")
                result.steps.append(
                    AgentStep(
                        step_type="tool_call",
                        content=json.dumps(arguments)[:200],
                        tool_name=tool_name,
                    )
                )

                t1 = time.time()
                tool_result = dispatch_tool(
                    TOOL_HANDLERS, ctx, tool_name, arguments, trace_name=_TRACE_NAME
                )
                tool_ms = int((time.time() - t1) * 1000)

                result.steps.append(
                    AgentStep(
                        step_type="tool_result",
                        content=(
                            (tool_result[:300] + "…")
                            if len(tool_result) > 300
                            else tool_result
                        ),
                        tool_name=tool_name,
                        duration_ms=tool_ms,
                    )
                )

                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tool_id,
                        "content": tool_result,
                    }
                )

            # Check if assign_icons was called (icons saved in context)
            if ctx.icon_results:
                logger.info(
                    "Icon Agent: assign_icons was called — %d icons saved",
                    len(ctx.icon_results),
                )
                result.success = True
                result.icons = dict(ctx.icon_results)
                result.iterations = iteration + 1
                result.usage = total_usage
                notify(f"Agent assigned {len(result.icons)} icons!")
                logger.info(
                    "===== ICON AGENT COMPLETE ===== iterations=%d, icons=%d",
                    result.iterations,
                    len(result.icons),
                )
                return result
        else:
            # Text response — try to parse icons from it (fallback)
            content = extract_message_content(llm_response)
            logger.info(
                "Iteration %d: text response (%d chars)",
                iteration + 1,
                len(content),
            )

            result.steps.append(
                AgentStep(
                    step_type="output",
                    content=(content[:200] + "…") if len(content) > 200 else content,
                )
            )

            parsed = _parse_icons_from_text(content)
            if parsed:
                logger.info(
                    "Icon Agent: parsed %d icons from text fallback", len(parsed)
                )
                ctx.icon_results.update(parsed)
                result.success = True
                result.icons = dict(ctx.icon_results)
                result.iterations = iteration + 1
                result.usage = total_usage
                notify(f"Agent assigned {len(result.icons)} icons!")
                logger.info(
                    "===== ICON AGENT COMPLETE (fallback) ===== iterations=%d, icons=%d",
                    result.iterations,
                    len(result.icons),
                )
                return result

            # No icons parsed — if we have content, ask the LLM to try again
            if content.strip():
                messages.append(message)
                messages.append(
                    {
                        "role": "user",
                        "content": "Please call the assign_icons tool with the emoji mapping.",
                    }
                )

    result.error = (
        f"Agent reached maximum iterations ({MAX_ITERATIONS}) without assigning icons"
    )
    logger.error("===== ICON AGENT FAILED ===== %s", result.error)
    return result
