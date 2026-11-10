"""
Complex Rule Builder Agent engine.

Conversational agent that helps a user author a Complex (SPARQL CONSTRUCT)
business rule. Each call processes a single chat turn: the LLM may introspect
the loaded ontology (``get_ontology_overview`` / ``describe_class``) and
(re)write the proposed CONSTRUCT via ``update_draft``, then returns a short
natural-language reply (the next question or a confirmation).

Shares the ``engine_base`` helpers (LLM call, tool dispatch, usage) and the
same tracing as the Ontology Assistant and Graph Chat agents. The ontology is
passed in-process (no HTTP loopback), mirroring ``agent_ontology_assistant``.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from back.core.logging import get_logger
from agents.agent_complex_rule_builder.tools import TOOL_DEFINITIONS, TOOL_HANDLERS
from agents.tools.context import ToolContext
from agents.engine_base import (
    AgentStep,
    accumulate_usage,
    call_serving_endpoint,
    dispatch_tool,
)
from agents.tracing import trace_agent

logger = get_logger(__name__)

MAX_ITERATIONS = 8
LLM_TIMEOUT = 120

_TRACE_NAME = "complex_rule_builder"


@dataclass
class AgentResult:
    """Outcome of a single rule-builder turn."""

    success: bool
    reply: str = ""
    draft: Optional[dict] = None  # {name, description, query} — only when changed
    ready: bool = False
    steps: List[AgentStep] = field(default_factory=list)
    iterations: int = 0
    error: str = ""
    usage: Dict[str, int] = field(default_factory=dict)


SYSTEM_PROMPT = """\
You are the Complex Rule Builder assistant for OntoBricks. You help the user
author a **SPARQL CONSTRUCT** business rule that infers new triples from the
knowledge graph, through a short back-and-forth conversation.

TOOLS
  READ (always ground yourself before asking or writing):
  • get_ontology_overview – class names + their data properties, and all
    object properties with domain/range.
  • describe_class(name)  – one class's data + object properties with ranges.
  WRITE:
  • update_draft(name, description, query, ready) – (re)write the proposed
    CONSTRUCT shown to the user in the code pane. Call it whenever the query
    materially changes. Set ready=true ONLY when the query is complete and
    self-consistent.

WORKFLOW
1. Before your first substantive question, call get_ontology_overview so you
   use real class and property names (never invent them). Use describe_class
   to confirm the exact properties of a class you intend to reference.
2. Ask ONE focused question at a time to pin down: which entities the rule
   matches, the condition(s), and what new triple(s) it should infer.
3. As soon as you can express part of the rule, call update_draft with a
   first CONSTRUCT and keep refining it after each answer so the user watches
   it take shape.
4. When the CONSTRUCT fully captures the user's intent, call update_draft once
   more with a suggested name + description and ready=true. Then, as your final
   message, ALWAYS ask the user to review and, if they like, rename the rule
   Name and Description before they Validate and Save — propose your own Name
   and Description but make clear they can change them.

EDITING AN EXISTING RULE
  If the conversation starts with an existing draft, your FIRST message must
  explain, in plain language (one or two sentences), what the current CONSTRUCT
  matches and what triples it infers — then ask what they want to change. Do
  NOT ask the user to paste the query; it is already in the draft. Build on the
  existing query rather than starting from scratch.

RULES
  • Use the exact class/property names from the tools. Never invent names.
  • CONSTRUCT syntax only: CONSTRUCT { ?s <pred> ?o } WHERE { ... }. Variables
    start with ?. Do not emit mutating SPARQL (INSERT/DELETE).
  • Keep replies short and in Markdown. Put SPARQL only in the draft (via
    update_draft), not duplicated in the chat, unless the user asks to see it.
  • If the ontology is empty or a name is missing, say so and ask the user to
    provide the class/property names.
"""


def _snapshot(draft: Optional[dict]) -> Optional[dict]:
    """Normalised copy for change comparison (ignores missing keys)."""
    if not draft:
        return None
    return {
        "name": draft.get("name", "") or "",
        "description": draft.get("description", "") or "",
        "query": draft.get("query", "") or "",
    }


def _build_system_prompt(initial_draft: Optional[dict]) -> str:
    """System prompt, with the current draft inlined so the model can SEE the
    rule being edited (and any hand-edits made in the code pane). Rebuilt every
    turn, so the agent always works from the latest draft."""
    if not initial_draft or not (initial_draft.get("query") or initial_draft.get("name")):
        return SYSTEM_PROMPT
    return SYSTEM_PROMPT + (
        "\n\nCURRENT DRAFT (already shown to the user in the code pane — "
        "summarise and build on this; do NOT ask the user to paste it):\n"
        f"Name: {initial_draft.get('name', '') or '(none)'}\n"
        f"Description: {initial_draft.get('description', '') or '(none)'}\n"
        "CONSTRUCT query:\n"
        f"```sparql\n{initial_draft.get('query', '') or '(empty)'}\n```"
    )


@trace_agent(name="complex_rule_builder")
def run_agent(
    host: str,
    token: str,
    endpoint_name: str,
    classes: list,
    properties: list,
    base_uri: str,
    user_message: str,
    current_draft: Optional[dict] = None,
    conversation_history: Optional[List[dict]] = None,
    endpoint_kind: str = "",
) -> AgentResult:
    """Run one turn of the Complex Rule Builder conversation.

    Args:
        classes, properties: The loaded ontology lists (read-only here).
        base_uri: Ontology base URI (passed to the tool context).
        user_message: The user's new chat message.
        current_draft: The current right-pane contents
            (``{name, description, query}``) so hand-edits are respected and
            an existing rule can be modified in place.
        conversation_history: Prior ``{role, content}`` messages.
    """
    logger.info(
        "===== COMPLEX RULE BUILDER START ===== endpoint=%s, classes=%d, properties=%d",
        endpoint_name,
        len(classes),
        len(properties),
    )

    initial_draft = _snapshot(current_draft)

    ctx = ToolContext(
        host=host.rstrip("/") if host else "",
        token=token or "",
        ontology_classes=classes,
        ontology_properties=properties,
        ontology_base_uri=base_uri or "",
        rule_draft=initial_draft,
    )

    result = AgentResult(success=False)

    messages = [{"role": "system", "content": _build_system_prompt(initial_draft)}]
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

        try:
            llm_response = call_serving_endpoint(
                host,
                token,
                endpoint_name,
                messages,
                tools=send_tools,
                max_tokens=2048,
                temperature=0.2,
                timeout=LLM_TIMEOUT,
                trace_name=_TRACE_NAME,
                endpoint_kind=endpoint_kind,
            )
        except Exception as exc:  # noqa: BLE001
            error_msg = f"LLM request failed: {exc}"
            logger.error(
                "complex_rule_builder: %s at iteration %d", error_msg, iteration + 1
            )
            result.error = error_msg
            return result

        accumulate_usage(result.usage, llm_response.get("usage", {}))

        choices = llm_response.get("choices", [])
        if not choices:
            logger.warning(
                "complex_rule_builder: empty choices at iteration %d", iteration + 1
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
                        json.loads(raw_args) if isinstance(raw_args, str) else raw_args
                    )
                except json.JSONDecodeError:
                    arguments = {}

                logger.info(
                    "complex_rule_builder: iteration %d — tool_call '%s'",
                    iteration + 1,
                    tool_name,
                )
                result.steps.append(
                    AgentStep(
                        step_type="tool_call",
                        content=json.dumps(arguments, default=str),
                        tool_name=tool_name,
                    )
                )

                tool_t0 = time.time()
                tool_result = dispatch_tool(
                    TOOL_HANDLERS, ctx, tool_name, arguments, trace_name=_TRACE_NAME
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
            # Surface the draft only when it actually changed this turn.
            if ctx.rule_draft is not None and ctx.rule_draft != initial_draft:
                result.draft = ctx.rule_draft
            result.ready = ctx.rule_draft_ready
            result.steps.append(
                AgentStep(step_type="output", content=content[:500])
            )
            logger.info(
                "===== COMPLEX RULE BUILDER DONE ===== iterations=%d, "
                "draft_changed=%s, ready=%s, reply_len=%d",
                result.iterations,
                result.draft is not None,
                result.ready,
                len(content),
            )
            return result

    result.error = "Max iterations reached"
    result.reply = "I ran out of steps. Could you simplify the rule you need?"
    if ctx.rule_draft is not None and ctx.rule_draft != initial_draft:
        result.draft = ctx.rule_draft
    result.ready = ctx.rule_draft_ready
    return result
