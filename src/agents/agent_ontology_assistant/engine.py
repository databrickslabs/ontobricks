"""
OntoBricks Ontology Assistant Agent Engine.

Conversational agent that modifies a loaded ontology via natural language.
Each call processes a single user message and returns the assistant reply
along with the mutated ontology state.
"""

import json
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from back.core.logging import get_logger
from agents.agent_ontology_assistant.tools import (
    TOOL_DEFINITIONS,
    TOOL_HANDLERS,
)
from agents.tools.context import ToolContext
from agents.engine_base import (
    AgentStep,
    call_serving_endpoint,
    dispatch_tool,
    accumulate_usage,
)
from agents.tracing import trace_agent

logger = get_logger(__name__)

MAX_ITERATIONS = 20
LLM_TIMEOUT = 120

_TRACE_NAME = "ontology_assistant"


# =====================================================
# Data classes
# =====================================================

@dataclass
class AgentResult:
    """Outcome of a single assistant turn."""
    success: bool
    reply: str = ""
    classes: list = field(default_factory=list)
    properties: list = field(default_factory=list)
    ontology_changed: bool = False
    steps: List[AgentStep] = field(default_factory=list)
    iterations: int = 0
    error: str = ""
    usage: Dict[str, int] = field(default_factory=dict)


# =====================================================
# System prompt
# =====================================================

SYSTEM_PROMPT = """\
You are an expert ontology assistant for OntoBricks. \
You help users modify their ontology through natural language instructions.

TOOLS
You have tools to read and modify the ontology:
  READ:
  • get_ontology_classes      – list all entity classes with attributes and hierarchy
  • get_ontology_properties   – list all relationships with domain/range

  ENTITIES:
  • add_entity               – add a new entity class
  • remove_entity            – delete an entity (cascade-removes its relationships)
  • rename_entity            – rename and update all references
  • update_entity            – change label, description, parent, or emoji
  • add_attribute            – add a data property to an entity
  • remove_attribute         – remove a data property from an entity

  RELATIONSHIPS:
  • add_relationship         – add a relationship between two entities
  • remove_relationship      – delete a relationship
  • update_relationship      – modify a relationship's fields
  • set_inheritance          – set or change an entity's parent class

WORKFLOW
1. When the user asks to modify the ontology, FIRST call get_ontology_classes \
and/or get_ontology_properties to understand the current state.
2. Apply the requested changes using the appropriate mutation tools.
3. Reply with a concise summary of what was changed.

RULES
• Always inspect the current state before making changes (call the read tools first).
• Entity names should be PascalCase (e.g. SalesOrder, CustomerAddress).
• Relationship names should be camelCase (e.g. belongsTo, hasCustomer).
• When removing an entity, inform the user about cascade-deleted relationships.
• When renaming, all references (relationships, inheritance) are updated automatically.
• Be precise: do not guess names. Use the exact names from get_ontology_classes/properties.
• You may batch multiple independent tool calls in a single response.
• After making changes, provide a brief human-friendly summary.

FORMATTING
• Your replies are rendered as Markdown. Use it for clarity.
• When listing entities or relationships, use a **Markdown table** with columns.
• Use **bold** for entity/relationship names.
• Use bullet lists for short summaries of changes."""


# =====================================================
# Internal helpers
# =====================================================

# =====================================================
# Public entry point
# =====================================================

@trace_agent(name="ontology_assistant")
def run_agent(
    host: str,
    token: str,
    endpoint_name: str,
    classes: list,
    properties: list,
    base_uri: str,
    user_message: str,
    conversation_history: Optional[List[dict]] = None,
    on_step: Optional[Callable[[str], None]] = None,
) -> AgentResult:
    """Run one turn of the ontology assistant conversation.

    Args:
        classes: Mutable list of ontology class dicts (modified in-place).
        properties: Mutable list of ontology property dicts (modified in-place).
        base_uri: Ontology base URI for generating URIs.
        user_message: The user's natural language instruction.
        conversation_history: Previous messages for multi-turn context.
        on_step: Optional progress callback.
    """
    logger.info(
        "===== ONTOLOGY ASSISTANT START ===== endpoint=%s, classes=%d, properties=%d",
        endpoint_name, len(classes), len(properties),
    )

    ctx = ToolContext(
        host=host.rstrip("/"),
        token=token,
        ontology_classes=classes,
        ontology_properties=properties,
        ontology_base_uri=base_uri,
    )

    result = AgentResult(success=False)

    messages = [{"role": "system", "content": SYSTEM_PROMPT}]

    if conversation_history:
        for msg in conversation_history:
            messages.append(msg)

    messages.append({"role": "user", "content": user_message})

    for iteration in range(MAX_ITERATIONS):
        result.iterations = iteration + 1
        is_last = iteration == MAX_ITERATIONS - 1
        send_tools = TOOL_DEFINITIONS if not is_last else None

        if on_step:
            on_step(f"Iteration {iteration + 1}…")

        t0 = time.time()
        try:
            llm_response = call_serving_endpoint(
                host, token, endpoint_name, messages,
                tools=send_tools, max_tokens=2048, temperature=0.2,
                timeout=LLM_TIMEOUT, trace_name=_TRACE_NAME,
            )
        except Exception as exc:
            error_msg = f"LLM request failed: {exc}"
            logger.error("ontology_assistant: %s at iteration %d", error_msg, iteration + 1)
            result.error = error_msg
            return result

        elapsed = int((time.time() - t0) * 1000)

        accumulate_usage(result.usage, llm_response.get("usage", {}))

        choices = llm_response.get("choices", [])
        if not choices:
            logger.warning("ontology_assistant: empty choices in LLM response at iteration %d", iteration + 1)
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
                    arguments = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
                except json.JSONDecodeError:
                    arguments = {}

                logger.info(
                    "ontology_assistant: iteration %d — tool_call '%s'",
                    iteration + 1, tool_name,
                )

                result.steps.append(AgentStep(
                    step_type="tool_call",
                    content=json.dumps(arguments, default=str),
                    tool_name=tool_name,
                ))

                tool_t0 = time.time()
                tool_result = dispatch_tool(TOOL_HANDLERS, ctx, tool_name, arguments, trace_name=_TRACE_NAME)
                tool_elapsed = int((time.time() - tool_t0) * 1000)

                result.steps.append(AgentStep(
                    step_type="tool_result",
                    content=tool_result[:500],
                    tool_name=tool_name,
                    duration_ms=tool_elapsed,
                ))

                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_id,
                    "content": tool_result,
                })
        else:
            result.success = True
            result.reply = content
            result.classes = ctx.ontology_classes
            result.properties = ctx.ontology_properties
            result.ontology_changed = ctx.ontology_dirty

            result.steps.append(AgentStep(
                step_type="output",
                content=content[:500],
            ))

            logger.info(
                "===== ONTOLOGY ASSISTANT DONE ===== iterations=%d, changed=%s, reply_len=%d",
                result.iterations, ctx.ontology_dirty, len(content),
            )
            return result

    result.error = "Max iterations reached"
    result.reply = "I ran out of steps. Please try a simpler request."
    result.classes = ctx.ontology_classes
    result.properties = ctx.ontology_properties
    result.ontology_changed = ctx.ontology_dirty
    return result
