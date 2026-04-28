"""
OntoBricks Auto-Mapping Agent Engine.

Implements an agentic loop that uses the Databricks Foundation Model API
with function calling to autonomously map ontology entities and relationships
to SQL queries against domain tables.

Fallback: if the LLM endpoint does not support the ``tools`` parameter the
engine transparently degrades to a single-shot generation (no tool calls).
"""

import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

import requests

from back.core.logging import get_logger
from agents.agent_auto_assignment.tools import (
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

MAX_ITERATIONS = 60
LLM_TIMEOUT = 180
_ITERATION_DELAY_SEC = 3

_TRACE_NAME = "auto_assignment"


# =====================================================
# Data classes
# =====================================================


@dataclass
class AgentResult:
    """Outcome of a full auto-mapping agent run."""

    success: bool
    entity_mappings: list = field(default_factory=list)
    relationship_mappings: list = field(default_factory=list)
    steps: List[AgentStep] = field(default_factory=list)
    iterations: int = 0
    error: str = ""
    usage: Dict[str, int] = field(default_factory=dict)
    stats: Dict[str, int] = field(default_factory=dict)


# =====================================================
# System prompt
# =====================================================

SYSTEM_PROMPT = """\
You are an expert data engineer. Your task is to map ontology entities \
and relationships to SQL queries against Databricks tables.

TOOLS
You have six tools:
  • get_metadata           – get imported table schemas (full names, columns, types) — no UC query
  • get_documents_context   – get imported domain documents to enrich domain context — no UC query
  • get_ontology           – get entities (with attributes) and relationships to map
  • execute_sql            – run a SQL query to validate it and see columns + sample data
  • submit_entity_mapping       – record a validated entity mapping
  • submit_relationship_mapping – record a validated relationship mapping

WORKFLOW
1. Call get_ontology AND get_metadata to understand what needs mapping and what data is available.
2. Call get_documents_context to read any imported documents — use them to enrich domain knowledge for better mapping decisions.
3. For EACH entity:
   a. Compose a SELECT query using the table schemas.
   b. Call execute_sql to validate the query works and see the columns.
   c. If the query fails, fix the SQL and try execute_sql again.
   d. Once validated, call submit_entity_mapping with the correct column assignments.
4. For EACH relationship:
   a. Compose a SELECT query returning source and target identifiers.
   b. Call execute_sql to validate the query.
   c. Once validated, call submit_relationship_mapping.
5. After all mappings are submitted, output a brief summary.

SQL RULES FOR ENTITIES (CRITICAL)
• Always use full table names from get_metadata (catalog.schema.table).
• The FIRST column MUST be aliased AS ID (the entity identifier).
• The SECOND column MUST be aliased AS Label (human-readable name).
• Additional columns for each data-property attribute (use original column names).
• If the same column serves as both an alias and an attribute, include it twice: \
once with the alias (AS ID) and once with its original name.
• Add WHERE <id_column> IS NOT NULL to filter null keys.
• Do NOT add LIMIT — the mapping query must return ALL rows.
• Do NOT use ORDER BY, CTEs, or subqueries unless absolutely necessary.
• Write simple, flat SELECT statements.

SQL RULES FOR RELATIONSHIPS (CRITICAL)
• SELECT exactly 2 columns: source identifier AS source_id, target identifier AS target_id.
• If both columns are in the SAME table, query only that table (no joins).
• Do NOT add LIMIT or ORDER BY.

ATTRIBUTE MAPPING
• In submit_entity_mapping, provide attribute_mappings: a JSON object mapping each \
ontology attribute name to the corresponding SQL column name.
• Match by name similarity (e.g. ontology "firstName" → column "first_name").
• Map ALL attributes from the ontology entity to available columns.

GENERAL RULES
• Process ALL entities and ALL relationships — do not skip any.
• If execute_sql fails, read the error and fix the SQL.
• You may batch multiple independent tool calls in a single response.
• Only ever pass row-returning queries (SELECT / WITH …) to execute_sql. \
Never pass DESCRIBE, SHOW, EXPLAIN or other metadata statements — \
use get_metadata for schema introspection instead.
• After submitting all mappings, output ONLY a brief text summary of what was mapped."""


# =====================================================
# Internal helpers
# =====================================================


def _build_user_prompt(entities: List[dict], relationships: List[dict]) -> str:
    parts = []
    parts.append(
        f"Please map {len(entities)} entities and {len(relationships)} relationships "
        "to SQL queries. Start by calling get_ontology, get_metadata, and get_documents_context "
        "(documents enrich domain context for better mapping decisions)."
    )
    if entities:
        names = ", ".join(e.get("name", "?") for e in entities)
        parts.append(f"Entities to map: {names}")
    if relationships:
        names = ", ".join(r.get("name", "?") for r in relationships)
        parts.append(f"Relationships to map: {names}")
    prompt = "\n".join(parts)
    logger.debug("_build_user_prompt (%d chars):\n%s", len(prompt), prompt)
    return prompt


# =====================================================
# Public entry point
# =====================================================


@trace_agent(name="auto_assignment")
def run_agent(
    host: str,
    token: str,
    endpoint_name: str,
    client: Any,
    metadata: dict,
    ontology: dict,
    entity_mappings: Optional[list] = None,
    relationship_mappings: Optional[list] = None,
    documents: Optional[list] = None,
    on_step: Optional[Callable[[str, int], None]] = None,
    max_iterations: Optional[int] = None,
) -> AgentResult:
    """Run the auto-mapping agent.

    The agent autonomously maps ontology entities and relationships to SQL
    queries by composing, validating, and submitting mappings via tools.

    Args:
        max_iterations: Override the default iteration budget.  Use a smaller
            value (e.g. 15) when mapping a single item to keep latency low.
    """
    iteration_limit = max_iterations if max_iterations is not None else MAX_ITERATIONS

    entities = ontology.get("entities", [])
    relationships = ontology.get("relationships", [])
    total_items = len(entities) + len(relationships)

    logger.info(
        "===== AUTO-ASSIGN AGENT START ===== endpoint=%s, entities=%d, relationships=%d, max_iter=%d",
        endpoint_name,
        len(entities),
        len(relationships),
        iteration_limit,
    )
    logger.debug(
        "run_agent: metadata tables=%d", len((metadata or {}).get("tables", []))
    )

    ctx = ToolContext(
        host=host.rstrip("/"),
        token=token,
        client=client,
        metadata=metadata or {},
        ontology=ontology,
        entity_mappings=list(entity_mappings or []),
        relationships=list(relationship_mappings or []),
        documents=list(documents or []),
    )

    result = AgentResult(success=False)

    # Build conversation
    user_prompt = _build_user_prompt(entities, relationships)
    messages: List[dict] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt},
    ]
    logger.info(
        "Agent conversation initialized: system=%d chars, user=%d chars",
        len(SYSTEM_PROMPT),
        len(user_prompt),
    )

    total_usage: Dict[str, int] = {"prompt_tokens": 0, "completion_tokens": 0}
    current_iteration = 0

    def _progress_pct() -> int:
        mapped = len(ctx.entity_mappings) + len(ctx.relationships)
        if total_items <= 0:
            return 5
        return min(5 + int((mapped / total_items) * 90), 95)

    def notify(msg: str, *, pct: Optional[int] = None):
        actual_pct = pct if pct is not None else _progress_pct()
        logger.info("STEP [%d%%] %s", actual_pct, msg)
        if on_step:
            on_step(msg, actual_pct)

    notify("Starting auto-mapping agent…", pct=1)

    # ------------------------------------------------------------------
    # Agent loop
    # ------------------------------------------------------------------
    tools_supported = True

    for iteration in range(iteration_limit):
        # Delay between iterations to avoid "too many requests" rate limits
        if iteration > 0:
            logger.debug(
                "Iteration %d: waiting %ds before LLM call (rate limit mitigation)",
                iteration + 1,
                _ITERATION_DELAY_SEC,
            )
            time.sleep(_ITERATION_DELAY_SEC)

        current_iteration = iteration + 1
        logger.info(
            "----- Iteration %d/%d — %d messages, %d entity mappings, %d rel mappings -----",
            current_iteration,
            iteration_limit,
            len(messages),
            len(ctx.entity_mappings),
            len(ctx.relationships),
        )
        mapped = len(ctx.entity_mappings) + len(ctx.relationships)
        notify(f"Mapped {mapped}/{total_items} — thinking…")

        is_last = iteration >= iteration_limit - 1
        send_tools = TOOL_DEFINITIONS if (tools_supported and not is_last) else None

        t0 = time.time()
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
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "?"
            logger.warning("Iteration %d: HTTPError status=%s", iteration + 1, status)
            logger.debug(
                "Iteration %d: HTTPError body: %.500s",
                iteration + 1,
                exc.response.text if exc.response is not None else "N/A",
            )
            if exc.response is not None and status in (400, 422) and tools_supported:
                logger.warning(
                    "Agent: endpoint rejected tools — falling back to direct mode"
                )
                tools_supported = False
                notify("Endpoint does not support tools – aborting.")
                result.error = "LLM endpoint does not support function calling"
                return result
            result.error = f"LLM request failed: {exc}"
            logger.error(
                "Agent: LLM request failed at iteration %d: %s", iteration + 1, exc
            )
            return result
        except requests.exceptions.ReadTimeout:
            result.error = f"LLM request timed out after {LLM_TIMEOUT}s"
            logger.error("Agent: timeout at iteration %d", iteration + 1)
            return result
        except requests.exceptions.RequestException as exc:
            result.error = f"LLM request failed: {exc}"
            logger.error(
                "Agent: request exception at iteration %d: %s", iteration + 1, exc
            )
            return result

        elapsed_ms = int((time.time() - t0) * 1000)
        logger.info("Iteration %d: LLM responded in %dms", iteration + 1, elapsed_ms)

        accumulate_usage(total_usage, llm_response.get("usage", {}))

        # Parse response
        choice = llm_response.get("choices", [{}])[0]
        finish_reason = choice.get("finish_reason", "?")
        message = choice.get("message", {})
        tool_calls = message.get("tool_calls", [])
        has_content = bool(message.get("content"))
        logger.info(
            "Iteration %d: finish_reason=%s, tool_calls=%d, has_content=%s",
            iteration + 1,
            finish_reason,
            len(tool_calls),
            has_content,
        )

        if tool_calls:
            logger.info(
                "Iteration %d: processing %d tool call(s): [%s]",
                iteration + 1,
                len(tool_calls),
                ", ".join(tc.get("function", {}).get("name", "?") for tc in tool_calls),
            )
            messages.append(message)

            for tc_idx, tc in enumerate(tool_calls, 1):
                func = tc.get("function", {})
                tool_name = func.get("name", "")
                raw_args = func.get("arguments", "{}")
                tool_id = tc.get("id", "")

                try:
                    arguments = json.loads(raw_args)
                except json.JSONDecodeError:
                    arguments = {}

                logger.info(
                    "Iteration %d: calling tool '%s' (%d/%d)",
                    iteration + 1,
                    tool_name,
                    tc_idx,
                    len(tool_calls),
                )

                if tool_name == "submit_entity_mapping":
                    name = arguments.get("class_name", "?")
                    notify(f"Mapping entity: {name}")
                elif tool_name == "submit_relationship_mapping":
                    name = arguments.get("property_name", "?")
                    notify(f"Mapping relationship: {name}")
                elif tool_name == "execute_sql":
                    sql_preview = arguments.get("sql", "")[:80]
                    notify(f"Validating SQL: {sql_preview}…")
                elif tool_name == "get_metadata":
                    notify("Retrieving table metadata…")
                elif tool_name == "get_documents_context":
                    notify("Retrieving imported documents…")
                elif tool_name == "get_ontology":
                    notify("Retrieving ontology to map…")
                else:
                    notify(f"Calling {tool_name}…")

                result.steps.append(
                    AgentStep(
                        step_type="tool_call",
                        content=json.dumps(arguments, default=str)[:500],
                        tool_name=tool_name,
                    )
                )

                t1 = time.time()
                tool_result = dispatch_tool(
                    TOOL_HANDLERS, ctx, tool_name, arguments, trace_name=_TRACE_NAME
                )
                tool_ms = int((time.time() - t1) * 1000)

                logger.info(
                    "Iteration %d: tool '%s' returned %d chars in %dms",
                    iteration + 1,
                    tool_name,
                    len(tool_result),
                    tool_ms,
                )

                result.steps.append(
                    AgentStep(
                        step_type="tool_result",
                        content=(
                            (tool_result[:500] + "…")
                            if len(tool_result) > 500
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

            mapped = len(ctx.entity_mappings) + len(ctx.relationships)
            notify(f"Mapped {mapped}/{total_items} items")
            logger.info(
                "Iteration %d: tool calls done, conversation=%d messages, mappings=%d/%d",
                iteration + 1,
                len(messages),
                mapped,
                total_items,
            )
        else:
            # Agent produced text — should be the final summary
            content = extract_message_content(llm_response)
            logger.info(
                "Iteration %d: agent produced text output — %d chars",
                iteration + 1,
                len(content),
            )

            result.steps.append(
                AgentStep(
                    step_type="output",
                    content=(content[:500] + "…") if len(content) > 500 else content,
                    duration_ms=elapsed_ms,
                )
            )

            result.success = True
            result.entity_mappings = ctx.entity_mappings
            result.relationship_mappings = ctx.relationships
            result.iterations = iteration + 1
            result.usage = total_usage
            result.stats = {
                "total": total_items,
                "entities": len(ctx.entity_mappings),
                "relationships": len(ctx.relationships),
            }

            logger.info(
                "===== AUTO-ASSIGN AGENT COMPLETE ===== iterations=%d, "
                "entity_mappings=%d, rel_mappings=%d, "
                "prompt_tokens=%d, completion_tokens=%d",
                result.iterations,
                len(ctx.entity_mappings),
                len(ctx.relationships),
                total_usage["prompt_tokens"],
                total_usage["completion_tokens"],
            )
            notify("Agent completed!", pct=100)
            return result

    # Exhausted iterations — still return what we have
    result.entity_mappings = ctx.entity_mappings
    result.relationship_mappings = ctx.relationships
    result.iterations = iteration_limit
    result.usage = total_usage
    result.stats = {
        "total": total_items,
        "entities": len(ctx.entity_mappings),
        "relationships": len(ctx.relationships),
    }
    if ctx.entity_mappings or ctx.relationships:
        result.success = True
        result.error = f"Agent used all {iteration_limit} iterations but submitted partial mappings"
        logger.warning(
            "===== AUTO-ASSIGN AGENT PARTIAL ===== %s — entity=%d, rel=%d",
            result.error,
            len(ctx.entity_mappings),
            len(ctx.relationships),
        )
    else:
        result.error = f"Agent reached maximum iterations ({iteration_limit}) without producing mappings"
        logger.error("===== AUTO-ASSIGN AGENT FAILED ===== %s", result.error)

    return result
