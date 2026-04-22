"""
Graph Chat Agent engine.

Runs one turn of a multi-tool conversation: the LLM can iteratively call
the knowledge-graph tools defined in :mod:`agents.agent_dtwin_chat.tools`
until it produces a final natural-language answer.

The engine shares the ``call_serving_endpoint`` / ``dispatch_tool`` /
``accumulate_usage`` helpers defined in :mod:`agents.engine_base`, so it
behaves identically to the Ontology Assistant (same retry logic,
tracing, OpenAI-compatible schema).
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from back.core.logging import get_logger
from agents.agent_dtwin_chat.tools import TOOL_DEFINITIONS, TOOL_HANDLERS
from agents.tools.context import ToolContext
from agents.engine_base import (
    AgentStep,
    accumulate_usage,
    call_serving_endpoint,
    dispatch_tool,
)
from agents.tracing import trace_agent

logger = get_logger(__name__)

MAX_ITERATIONS = 12
LLM_TIMEOUT = 120

_TRACE_NAME = "dtwin_chat"


@dataclass
class AgentResult:
    """Outcome of a single Graph Chat turn."""

    success: bool
    reply: str = ""
    steps: List[AgentStep] = field(default_factory=list)
    iterations: int = 0
    error: str = ""
    usage: Dict[str, int] = field(default_factory=dict)


SYSTEM_PROMPT = """\
You are the Graph Chat assistant for OntoBricks. You help the user
explore a Digital Twin knowledge graph with natural-language questions.

CONTEXT
The user has already selected a domain (knowledge graph). You do NOT
need to list or select domains. Every tool operates on the active
session's domain automatically.

TOOLS
  INSPECT
  - list_entity_types   : overall stats + entity types + counts + top predicates
  - get_status          : triple-store status (view, graph, row count)
  - get_graphql_schema  : auto-generated GraphQL SDL for the domain

  QUERY
  - describe_entity(search, entity_type?, depth?) : full-text description of
      entities matching the search text and/or type, with attributes and
      related entities (BFS traversal up to ``depth`` hops).
  - query_graphql(query, variables?) : execute GraphQL. Use it for typed,
      nested look-ups once you know the schema.
  - run_sparql(query, limit?) : read-only SPARQL (SELECT / ASK / DESCRIBE).
      Use it for aggregations (COUNT, GROUP BY) or joins that GraphQL can
      not express concisely.

WORKFLOW
  1. If you are unsure what the graph contains, first call
     ``list_entity_types`` to see entity types and their counts.
  2. For "tell me about <thing>" questions call ``describe_entity``.
  3. For "how many / group by / aggregate" questions, prefer
     ``run_sparql``.  For typed nested look-ups (e.g. customer + orders
     + products in one payload), prefer ``query_graphql`` after
     inspecting the schema.
  4. When data fetching is done, reply with a concise, well-formatted
     answer using the data you retrieved. Never invent URIs or counts.

RULES
  * Use the exact entity / predicate names returned by the tools.
  * If a tool returns an error, read the error and try a different
    approach (e.g. narrower search, simpler SPARQL, different type).
  * Do NOT try to mutate data; mutating SPARQL is rejected.
  * Keep final answers short and relevant. Include URIs only when the
    user asks for them.

FORMATTING
  * Your replies are rendered as Markdown. Use tables for list-style
    results, bullet lists for short summaries, and ``code`` for
    SPARQL queries, GraphQL snippets, or raw identifiers.

LINKS TO THE KNOWLEDGE GRAPH (VERY IMPORTANT)
  Whenever you mention a concrete entity instance (any subject URI
  returned by a tool, e.g. ``https://ontobricks.com/ontology/Customer/
  CUST00094``), emit it as a Markdown link so the user can jump to
  the knowledge-graph visualization:

      [<display-name>](/resolve?uri=<full-URI>)

  Rules:
    * Use the entity's human-readable name as the link text (its
      ``rdfs:label`` / ``name`` attribute if available, otherwise the
      local name after the last ``/`` or ``#``).
    * The ``<full-URI>`` inside the parentheses MUST be the exact URI
      returned by the tool — do not shorten, prefix, or URL-encode it
      yourself; the app handles encoding.
    * Apply this in prose ("The top customer is
      [Acme Corp](/resolve?uri=https://…/Customer/CUST00094).") AND
      inside Markdown tables (put the link in the relevant cell).
    * Do NOT link entity *types* / classes (e.g. ``Customer``,
      ``Order``) — only concrete instances.
    * Never invent URIs: only link URIs that appeared in a tool
      response.
"""


@trace_agent(name="dtwin_chat")
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
    on_step: Optional[Callable[[str], None]] = None,
) -> AgentResult:
    """Run one turn of the Graph Chat agent.

    Args:
        host, token, endpoint_name: Databricks serving-endpoint target
            used to issue LLM chat completions.
        base_url: Loopback OntoBricks URL, e.g. ``http://localhost:8000``.
        domain_name: Currently selected domain (from session).
        registry_params: Registry query params injected into every call
            (``registry_catalog``, ``registry_schema``, ``registry_volume``).
        session_cookies: Cookies from the user's request; forwarded to
            ``/dtwin/execute`` so that the internal route resolves the
            same session.
        user_message: The user's new natural-language message.
        conversation_history: Prior ``{role, content}`` messages.
        on_step: Optional progress callback (unused but kept for parity).
    """
    logger.info(
        "===== DTWIN CHAT START ===== endpoint=%s, domain=%s, base_url=%s",
        endpoint_name,
        domain_name,
        base_url,
    )

    ctx = ToolContext(
        host=host.rstrip("/") if host else "",
        token=token or "",
        dtwin_base_url=base_url,
        dtwin_session_cookies=session_cookies or {},
        dtwin_registry_params=registry_params or {},
        dtwin_domain_name=domain_name or "",
    )

    result = AgentResult(success=False)

    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
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
                "dtwin_chat: %s at iteration %d", error_msg, iteration + 1
            )
            result.error = error_msg
            return result

        accumulate_usage(result.usage, llm_response.get("usage", {}))

        choices = llm_response.get("choices", [])
        if not choices:
            logger.warning(
                "dtwin_chat: empty choices in LLM response at iteration %d",
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
                    "dtwin_chat: iteration %d -- tool_call '%s'",
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
            result.steps.append(
                AgentStep(step_type="output", content=content[:500])
            )
            logger.info(
                "===== DTWIN CHAT DONE ===== iterations=%d, reply_len=%d",
                result.iterations,
                len(content),
            )
            return result

    result.error = "Max iterations reached"
    result.reply = (
        "I ran out of steps before I could answer. "
        "Could you simplify or narrow down your question?"
    )
    return result
