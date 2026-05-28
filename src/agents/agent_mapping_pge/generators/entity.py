"""
OntoBricks Mapping-PGE EntityGenerator Agent.

Sprint 4 of the Planner-Generator-Evaluator (PGE) redesign.

The EntityGenerator is a narrow, focused LLM agent that maps **one** ontology
class at a time. The orchestrator (Sprint 7) calls it per item with a
filtered slice of the Planner's :class:`SourceModel`:

* the single ontology class to map, with its full attribute list, and
* a small SourceModel slice — only the candidate tables / canonical IDs /
  joins that are relevant to *this* class.

The Generator does NOT see the full ontology or full metadata. That is the
core design contract: keep its context bounded and each decision cheap.

The loop shape mirrors :mod:`agents.agent_mapping_pge.planner` — same
``call_serving_endpoint`` + ``dispatch_tool`` ReAct cycle, same 3-second
inter-iteration delay, same MLflow trace decorator — with these differences:

* Smaller default budget (12 vs 25): mapping one class is bounded work.
* Different tool set: only ``execute_sql``, ``sample_table``, and the
  terminal ``submit_entity_mapping``. The slice already carries every piece
  of context the Generator needs.
* No single-shot fallback: if the endpoint refuses tools, the Generator
  reports failure — it produces structured output through
  ``submit_entity_mapping`` only.
* The "NO SILENT DROPS" invariant: every ontology attribute must be either
  in ``attribute_mappings`` or in ``unmapped_attributes`` with a one-sentence
  reason. The system prompt enforces this; the tool persists it.
"""

import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

import requests

from back.core.logging import get_logger
from agents.engine_base import (
    call_serving_endpoint,
    dispatch_tool,
    accumulate_usage,
)
from agents.tools.context import ToolContext
from agents.tools.mapping import (
    MAPPING_TOOL_DEFINITIONS_BY_NAME,
    MAPPING_TOOL_HANDLERS,
)
from agents.tools.planner import (
    SAMPLE_TABLE_DEF,
    tool_sample_table,
)
from agents.tools.sql import (
    SQL_TOOL_DEFINITIONS,
    SQL_TOOL_HANDLERS,
)
from agents.tracing import trace_agent

logger = get_logger(__name__)

MAX_ITERATIONS = 12
LLM_TIMEOUT = 180
_ITERATION_DELAY_SEC = 3

_TRACE_NAME = "mapping_pge_entity_generator"


# =====================================================
# Tool aggregation
# =====================================================
#
# The EntityGenerator only needs:
#   * execute_sql        – validate the composed SELECT before submitting.
#   * sample_table       – disambiguate when two candidate tables are equally
#                          plausible (e.g. same confidence in the slice).
#   * submit_entity_mapping – TERMINAL.
#
# We deliberately exclude:
#   * get_ontology / get_metadata / get_documents_context — the Planner's
#     view; the slice already has what's needed.
#   * column_value_overlap / distinct_count — those validate join keys and
#     canonical IDs, which the Planner already locked in.
#   * submit_relationship_mapping / submit_source_model — wrong stage.

# Filter MAPPING_TOOL_DEFINITIONS down to just submit_entity_mapping. We
# look up by name from the by-name index in ``mapping.py`` rather than
# scanning the list inline. Sprint 5 will reuse the same pattern for
# ``submit_relationship_mapping``.
_SUBMIT_ENTITY_DEF: dict = MAPPING_TOOL_DEFINITIONS_BY_NAME["submit_entity_mapping"]

TOOL_DEFINITIONS: List[dict] = (
    SQL_TOOL_DEFINITIONS
    + [SAMPLE_TABLE_DEF]
    + [_SUBMIT_ENTITY_DEF]
)

TOOL_HANDLERS: Dict[str, Callable] = {
    **SQL_TOOL_HANDLERS,
    "sample_table": tool_sample_table,
    "submit_entity_mapping": MAPPING_TOOL_HANDLERS["submit_entity_mapping"],
}


# =====================================================
# Data classes
# =====================================================


@dataclass
class EntityGenStep:
    """One observable step of the EntityGenerator's execution.

    Mirrors :class:`agents.agent_mapping_pge.planner.PlannerStep` but is
    scoped to the Generator so the orchestrator (Sprint 7) can render a
    per-class timeline in the UI.
    """

    step_type: str  # "tool_call" | "tool_result" | "output"
    content: str
    tool_name: str = ""
    duration_ms: int = 0


@dataclass
class EntityGenResult:
    """Outcome of a single EntityGenerator invocation.

    ``mapping`` holds the submitted entity-mapping dict (the same shape the
    handler appends to ``ctx.entity_mappings``) when ``success`` is True.
    """

    success: bool
    mapping: Optional[dict] = None
    steps: List[EntityGenStep] = field(default_factory=list)
    iterations: int = 0
    error: str = ""
    usage: Dict[str, int] = field(default_factory=dict)


# =====================================================
# System prompt
# =====================================================
#
# The ENTITY SQL RULES section is lifted from
# ``agents.agent_auto_assignment.engine`` (the section starting "SQL RULES
# FOR ENTITIES" around line 93) because those rules are correct and load-
# bearing — every mapping query must follow them or downstream SPARQL
# translation breaks.
#
# The PGE-specific additions are the slice-consumption rules: pick the
# best candidate table from the slice, use the canonical ID exactly as
# the Planner specified it, and account for every ontology attribute.

SYSTEM_PROMPT = """\
You are a senior data engineer. Your job is to map ONE ontology class to a \
single SQL SELECT query against a Databricks source table, validated against \
real data via execute_sql, and submitted via submit_entity_mapping.

YOU WILL BE GIVEN
• ontology_class: the class to map (uri, label, comment, attributes list).
• source_model_slice: a small JSON object the Planner already curated for \
this class:
  - candidate_tables[]: {table, confidence, reason} — the tables that could \
realise this class. Pick the one with the highest confidence whose reason \
fits the class semantics. Tie-break with sample_table if two are equally \
plausible.
  - canonical_id.canonical_column_per_table[<table>]: the column that MUST \
be aliased AS ID for the chosen table. Do NOT pick a different ID column.
  - canonical_id.format_note: a one-sentence note about the canonical-ID \
format (may be empty).
  - relevant_joins[]: optional — any joins the Planner thinks may apply. \
Usually entity mappings do not need joins; consult these only if the class \
naturally spans tables.

TOOLS
You have three tools:
  • execute_sql           – Validate the composed SELECT before submitting. \
The tool runs your query with a small LIMIT and returns columns + sample \
rows; the persisted mapping has no LIMIT.
  • sample_table          – Up to N random rows from a table. Use only when \
two candidate tables are equally plausible and you need to peek at real \
values to disambiguate.
  • submit_entity_mapping – TERMINAL. Call exactly once, after execute_sql \
succeeds, with the full mapping payload.

SQL RULES FOR ENTITIES (CRITICAL)
• Always use the full table name from the slice (catalog.schema.table).
• The FIRST column MUST be aliased AS ID — it MUST be the canonical-ID \
column the slice specifies for the chosen table.
• The SECOND column MUST be aliased AS Label — pick the most human-readable \
available column (typically ``name``, ``label``, ``display_name``, or \
similar). If no human-readable column exists, fall back to the canonical \
ID column itself aliased AS Label.
• Add one column per ontology data-property attribute you can satisfy from \
the chosen table. Use the column's original name (no alias).
• If the same column serves as both an alias and an attribute, include it \
twice: once with the alias (AS ID or AS Label) and once with its original \
name so it appears in attribute_mappings.
• Add WHERE <id_column> IS NOT NULL to filter null keys.
• Do NOT add LIMIT — the persisted mapping query must return ALL rows. \
execute_sql adds a small LIMIT internally for validation only.
• Do NOT use ORDER BY, CTEs, or subqueries unless absolutely necessary.
• Write simple, flat SELECT statements.

ATTRIBUTE COVERAGE — NO SILENT DROPS (CRITICAL)
For EACH ontology attribute on the class, you must do ONE of:
  (a) include a SQL column for it in the SELECT, AND add an entry to \
attribute_mappings mapping the ontology attribute name to the SQL column \
name (case-sensitive); OR
  (b) add it to unmapped_attributes with a one-sentence reason, using the \
shape {"name": "<attr>", "reason": "<why>"}.

You may NOT silently drop an attribute. The orchestrator will reject any \
mapping where some ontology attributes appear in neither list. If a column \
genuinely does not exist on the chosen table, that's an honest unmapped — \
say so in the reason.

WORKFLOW
1. Read the ontology class and the source_model_slice carefully.
2. Pick the candidate table — highest confidence whose reason fits the \
class. If tied, call sample_table on the contenders (one tool call each) \
and pick based on actual values.
3. Compose the SELECT following the SQL RULES above. The canonical-ID column \
to alias AS ID comes from canonical_column_per_table[<chosen_table>] in \
the slice — use it.
4. Call execute_sql to validate the SELECT. If it fails, READ the error and \
fix the SQL (typically a typo'd column name or wrong full_name). Retry as \
needed. Never submit an un-validated query.
5. Once execute_sql succeeds, call submit_entity_mapping EXACTLY ONCE with:
     class_uri, class_name, sql_query (no LIMIT), id_column, label_column, \
attribute_mappings, unmapped_attributes.
6. That's the terminal step. Do not emit any free text after submitting.

GENERAL RULES
• Only ever pass row-returning queries (SELECT / WITH …) to execute_sql.
• Do not call get_metadata, get_ontology, or any other tool — they are not \
available to you. The slice carries everything you need.
• If a retry_hint is present at the top of the user message, treat it as \
authoritative — your previous attempt failed for the reason stated and you \
should NOT repeat the same mistake.
"""


# =====================================================
# Internal helpers
# =====================================================


def _build_user_prompt(
    ontology_class: dict,
    source_model_slice: dict,
    retry_hint: Optional[str] = None,
) -> str:
    """Render the per-class user prompt.

    The orchestrator hands us `ontology_class` and a focused
    `source_model_slice`. We emit a structured prompt that:
      * surfaces the retry hint up top if one was provided,
      * lists the class metadata and attribute list explicitly so the LLM
        cannot forget any attribute, and
      * embeds the slice as JSON so the LLM can refer to it precisely.
    """
    parts: List[str] = []

    if retry_hint:
        parts.append(f"RETRY HINT (authoritative): {retry_hint}")
        parts.append("")

    class_uri = ontology_class.get("uri", "")
    class_label = ontology_class.get("label") or ontology_class.get("name", "")
    class_comment = ontology_class.get("comment", "") or ""
    attributes = ontology_class.get("attributes", []) or []

    attr_summary_lines: List[str] = []
    for attr in attributes:
        if isinstance(attr, dict):
            attr_name = attr.get("name") or attr.get("label") or attr.get("uri", "?")
            attr_type = attr.get("type") or attr.get("range") or ""
            attr_summary_lines.append(
                f"  - {attr_name}" + (f" ({attr_type})" if attr_type else "")
            )
        else:
            attr_summary_lines.append(f"  - {attr}")

    parts.append("ONTOLOGY CLASS")
    parts.append(f"  uri:     {class_uri}")
    parts.append(f"  label:   {class_label}")
    if class_comment:
        parts.append(f"  comment: {class_comment}")
    if attr_summary_lines:
        parts.append("  attributes ({} total):".format(len(attributes)))
        parts.extend(attr_summary_lines)
    else:
        parts.append("  attributes: (none — only ID and Label required)")

    parts.append("")
    parts.append("SOURCE MODEL SLICE")
    parts.append(json.dumps(source_model_slice, indent=2, default=str))

    parts.append("")
    parts.append(
        "Pick the best candidate table from the slice, compose a flat SELECT "
        "following the SQL RULES, validate with execute_sql, then call "
        "submit_entity_mapping exactly once. Every ontology attribute must "
        "appear in either attribute_mappings or unmapped_attributes — no "
        "silent drops."
    )

    prompt = "\n".join(parts)
    logger.debug(
        "_build_user_prompt for class=%s (%d chars):\n%s",
        class_uri,
        len(prompt),
        prompt,
    )
    return prompt


# =====================================================
# Public entry point
# =====================================================


@trace_agent(name="mapping_pge_entity_generator")
def run_entity_generator(
    host: str,
    token: str,
    endpoint_name: str,
    client: Any,
    *,
    ontology_class: dict,
    source_model_slice: dict,
    retry_hint: Optional[str] = None,
    on_step: Optional[Callable[[str, int], None]] = None,
    max_iterations: int = MAX_ITERATIONS,
) -> EntityGenResult:
    """Run the EntityGenerator agent for a single ontology class.

    The agent autonomously composes a SQL SELECT for ``ontology_class``
    against the candidate table(s) in ``source_model_slice``, validates the
    SQL with ``execute_sql``, and submits the validated mapping via the
    terminal ``submit_entity_mapping`` tool.

    Args:
        host: Databricks workspace URL.
        token: Bearer token for the serving endpoint.
        endpoint_name: Foundation Model serving endpoint name.
        client: Databricks SQL client (must expose ``execute_query(sql)``).
        ontology_class: Full dict for the SINGLE class to map (uri, label,
            comment, attributes list).
        source_model_slice: Filtered SourceModel slice with candidate_tables,
            canonical_id, and optional relevant_joins.
        retry_hint: Optional one-sentence hint from the orchestrator's
            previous-attempt evaluation. When present, surfaced at the top of
            the user prompt.
        on_step: Optional progress callback ``(msg, pct)`` for UI updates.
        max_iterations: Upper bound on tool-call iterations (default 12 —
            smaller than the Planner because the scope is one class).

    Returns:
        An :class:`EntityGenResult`. ``success`` is True iff a mapping was
        successfully submitted; in that case ``mapping`` holds the submitted
        dict. On failure, ``error`` explains why and ``mapping`` is None.
    """
    iteration_limit = max_iterations if max_iterations is not None else MAX_ITERATIONS

    class_uri = (ontology_class or {}).get("uri", "")
    class_label = (
        (ontology_class or {}).get("label")
        or (ontology_class or {}).get("name", "")
    )
    n_attrs = len(((ontology_class or {}).get("attributes") or []))
    n_candidates = len(((source_model_slice or {}).get("candidate_tables") or []))

    logger.info(
        "===== ENTITY GENERATOR START ===== endpoint=%s, class=%s (%s), "
        "attributes=%d, candidate_tables=%d, retry_hint=%s, max_iter=%d",
        endpoint_name,
        class_label,
        class_uri,
        n_attrs,
        n_candidates,
        "yes" if retry_hint else "no",
        iteration_limit,
    )

    ctx = ToolContext(
        host=host.rstrip("/"),
        token=token,
        client=client,
        # The slice subsumes metadata/ontology for this agent; the unified
        # ToolContext still needs these fields, so we plant the slice into
        # ``metadata`` for completeness even though no handler reads it.
        metadata={},
        ontology={},
        documents=[],
    )

    result = EntityGenResult(success=False)

    user_prompt = _build_user_prompt(
        ontology_class or {}, source_model_slice or {}, retry_hint=retry_hint
    )
    messages: List[dict] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt},
    ]
    logger.info(
        "EntityGenerator conversation initialized: system=%d chars, user=%d chars",
        len(SYSTEM_PROMPT),
        len(user_prompt),
    )

    total_usage: Dict[str, int] = {"prompt_tokens": 0, "completion_tokens": 0}

    def _progress_pct(iteration_idx: int) -> int:
        # Linear ramp 5 → 95 across the iteration budget. submit hits 100.
        ratio = (iteration_idx + 1) / max(iteration_limit, 1)
        return min(5 + int(ratio * 90), 95)

    def notify(msg: str, *, pct: Optional[int] = None) -> None:
        actual_pct = pct if pct is not None else 5
        logger.info("ENTITY GEN STEP [%d%%] %s", actual_pct, msg)
        if on_step:
            on_step(msg, actual_pct)

    notify(f"Generating mapping for {class_label or class_uri}…", pct=1)

    # Snapshot the pre-existing mapping count so we can detect "this run
    # added a mapping" without relying on absolute counters. (The orchestrator
    # in Sprint 7 may reuse a ToolContext across calls; today's `ctx` is
    # fresh, but the assertion is cheap and future-proof.)
    pre_run_mapping_count = len(ctx.entity_mappings)

    # ------------------------------------------------------------------
    # Agent loop
    # ------------------------------------------------------------------
    for iteration in range(iteration_limit):
        if iteration > 0:
            logger.debug(
                "Iteration %d: waiting %ds before LLM call (rate limit mitigation)",
                iteration + 1,
                _ITERATION_DELAY_SEC,
            )
            time.sleep(_ITERATION_DELAY_SEC)

        current_iteration = iteration + 1
        pct = _progress_pct(iteration)
        logger.info(
            "----- EntityGenerator iteration %d/%d — %d messages, mapping=%s -----",
            current_iteration,
            iteration_limit,
            len(messages),
            "set" if len(ctx.entity_mappings) > pre_run_mapping_count else "unset",
        )
        notify(
            f"Mapping iteration {current_iteration}/{iteration_limit}…",
            pct=pct,
        )

        t0 = time.time()
        try:
            llm_response = call_serving_endpoint(
                host,
                token,
                endpoint_name,
                messages,
                tools=TOOL_DEFINITIONS,
                max_tokens=2048,
                temperature=0.1,
                timeout=LLM_TIMEOUT,
                trace_name=_TRACE_NAME,
            )
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "?"
            logger.warning(
                "EntityGenerator iteration %d: HTTPError status=%s",
                current_iteration,
                status,
            )
            logger.debug(
                "EntityGenerator iteration %d: HTTPError body: %.500s",
                current_iteration,
                exc.response.text if exc.response is not None else "N/A",
            )
            if exc.response is not None and status in (400, 422):
                result.error = "LLM endpoint does not support function calling"
                result.iterations = current_iteration
                result.usage = total_usage
                logger.error(
                    "EntityGenerator: endpoint refused tools — cannot produce a mapping"
                )
                return result
            result.error = f"LLM request failed: {exc}"
            result.iterations = current_iteration
            result.usage = total_usage
            logger.error(
                "EntityGenerator: LLM request failed at iteration %d: %s",
                current_iteration,
                exc,
            )
            return result
        except requests.exceptions.ReadTimeout:
            result.error = f"LLM request timed out after {LLM_TIMEOUT}s"
            result.iterations = current_iteration
            result.usage = total_usage
            logger.error("EntityGenerator: timeout at iteration %d", current_iteration)
            return result
        except requests.exceptions.RequestException as exc:
            result.error = f"LLM request failed: {exc}"
            result.iterations = current_iteration
            result.usage = total_usage
            logger.error(
                "EntityGenerator: request exception at iteration %d: %s",
                current_iteration,
                exc,
            )
            return result

        elapsed_ms = int((time.time() - t0) * 1000)
        logger.info(
            "EntityGenerator iteration %d: LLM responded in %dms",
            current_iteration,
            elapsed_ms,
        )

        accumulate_usage(total_usage, llm_response.get("usage", {}))

        choice = llm_response.get("choices", [{}])[0]
        finish_reason = choice.get("finish_reason", "?")
        message = choice.get("message", {})
        tool_calls = message.get("tool_calls", [])
        has_content = bool(message.get("content"))
        logger.info(
            "EntityGenerator iteration %d: finish_reason=%s, tool_calls=%d, has_content=%s",
            current_iteration,
            finish_reason,
            len(tool_calls),
            has_content,
        )

        if not tool_calls:
            # The Generator must terminate via submit_entity_mapping, never
            # via free text.
            content = (message.get("content") or "")[:500]
            logger.warning(
                "EntityGenerator iteration %d: produced text without submitting mapping — %d chars",
                current_iteration,
                len(message.get("content") or ""),
            )
            result.steps.append(
                EntityGenStep(
                    step_type="output",
                    content=content,
                    duration_ms=elapsed_ms,
                )
            )
            result.error = "entity generator produced text without submitting mapping"
            result.iterations = current_iteration
            result.usage = total_usage
            notify(
                "Entity generator produced text without submitting mapping.",
                pct=pct,
            )
            return result

        logger.info(
            "EntityGenerator iteration %d: processing %d tool call(s): [%s]",
            current_iteration,
            len(tool_calls),
            ", ".join(
                tc.get("function", {}).get("name", "?") for tc in tool_calls
            ),
        )
        messages.append(message)

        terminal_success = False
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
                "EntityGenerator iteration %d: calling tool '%s' (%d/%d)",
                current_iteration,
                tool_name,
                tc_idx,
                len(tool_calls),
            )

            # Human-readable progress messages per tool.
            if tool_name == "submit_entity_mapping":
                notify(f"Submitting mapping for {class_label or class_uri}…", pct=pct)
            elif tool_name == "sample_table":
                fn = arguments.get("full_name", "?")
                notify(f"Sampling {fn}…", pct=pct)
            elif tool_name == "execute_sql":
                sql_preview = arguments.get("sql", "")[:80]
                notify(f"Running SQL: {sql_preview}…", pct=pct)
            else:
                notify(f"Calling {tool_name}…", pct=pct)

            result.steps.append(
                EntityGenStep(
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
                "EntityGenerator iteration %d: tool '%s' returned %d chars in %dms",
                current_iteration,
                tool_name,
                len(tool_result),
                tool_ms,
            )

            result.steps.append(
                EntityGenStep(
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

            # Detect terminal success: submit_entity_mapping returned
            # success=True AND a mapping for THIS class_uri is present in
            # ctx.entity_mappings. A submit with a mismatched class_uri (the
            # LLM mapped a different class than requested) is NOT terminal —
            # we coach the LLM via a corrective tool message and let the loop
            # continue so it can resubmit with the right URI.
            if tool_name == "submit_entity_mapping":
                try:
                    parsed = json.loads(tool_result)
                except json.JSONDecodeError:
                    parsed = {}
                if parsed.get("success") is True:
                    matched = any(
                        m.get("ontology_class") == class_uri
                        for m in ctx.entity_mappings
                    )
                    if matched:
                        terminal_success = True
                        logger.info(
                            "EntityGenerator iteration %d: submit_entity_mapping succeeded — terminating",
                            current_iteration,
                        )
                    else:
                        submitted_uri = arguments.get("class_uri", "")
                        mismatch_msg = (
                            f"submitted class_uri '{submitted_uri}' does not "
                            f"match requested class_uri '{class_uri}'; "
                            f"resubmit with class_uri='{class_uri}'"
                        )
                        logger.warning(
                            "EntityGenerator iteration %d: submit_entity_mapping "
                            "class_uri mismatch — submitted=%s, requested=%s",
                            current_iteration,
                            submitted_uri,
                            class_uri,
                        )
                        corrective_payload = json.dumps(
                            {"success": False, "error": mismatch_msg}
                        )
                        # Replace the recorded tool_result step's content so
                        # the UI / trace reflects the corrective signal
                        # rather than the original (misleading) success
                        # response.
                        result.steps[-1] = EntityGenStep(
                            step_type="tool_result",
                            content=corrective_payload,
                            tool_name=tool_name,
                            duration_ms=result.steps[-1].duration_ms,
                        )
                        # Replace the tool message just appended to
                        # ``messages`` so the LLM sees the corrective
                        # payload on the next turn (one tool message per
                        # tool_call_id — keep the protocol clean).
                        messages[-1] = {
                            "role": "tool",
                            "tool_call_id": tool_id,
                            "content": corrective_payload,
                        }

        if terminal_success:
            # Pull the mapping for this class by strict URI match. The
            # terminal-success guard above already verified an entry with
            # this URI exists; if we somehow can't find one here that's an
            # internal invariant violation, not a recoverable failure.
            submitted = next(
                (
                    m
                    for m in reversed(ctx.entity_mappings)
                    if m.get("ontology_class") == class_uri
                ),
                None,
            )
            if submitted is None:
                result.error = (
                    "internal: submit succeeded but mapping not found for class_uri"
                )
                result.iterations = current_iteration
                result.usage = total_usage
                logger.error(
                    "===== ENTITY GENERATOR FAILED ===== %s (class=%s)",
                    result.error,
                    class_uri,
                )
                return result
            result.success = True
            result.mapping = submitted
            result.iterations = current_iteration
            result.usage = total_usage
            logger.info(
                "===== ENTITY GENERATOR COMPLETE ===== class=%s, iterations=%d, "
                "prompt_tokens=%d, completion_tokens=%d",
                class_uri,
                result.iterations,
                total_usage["prompt_tokens"],
                total_usage["completion_tokens"],
            )
            notify(f"Mapping for {class_label or class_uri} complete!", pct=100)
            return result

    # Budget exhausted without a successful submit.
    result.iterations = iteration_limit
    result.usage = total_usage
    result.error = "entity generator exhausted iteration budget"
    logger.error("===== ENTITY GENERATOR FAILED ===== %s", result.error)
    notify(result.error, pct=95)
    return result
