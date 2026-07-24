"""
OntoBricks Mapping-PGE Planner Agent.

Sprint 3 of the Planner-Generator-Evaluator (PGE) redesign.

The Planner is a single-invocation agent (no internal retry loop — re-
invocations come from the orchestrator on Evaluator escalation in Sprint 7).
It consumes the ontology, table metadata, and any imported domain documents,
probes the source data via the planner tools (sample_table, column_value_overlap,
distinct_count) plus the shared tools (get_metadata, get_ontology,
get_documents_context, execute_sql), and emits a validated
:class:`SourceModel` via the ``submit_source_model`` terminal tool.

The loop semantics mirror the prior single-loop mapping agent — same
``call_serving_endpoint`` + ``dispatch_tool`` ReAct cycle, same 3-second
inter-iteration delay, same accumulated usage tracking, same MLflow trace
decorator — with two key differences:

* No fallback to single-shot generation. If the endpoint refuses tools, the
  Planner returns failure (the Planner *needs* tools — it produces structured
  output through ``submit_source_model``).
* Smaller default iteration budget (25 instead of 60) — the Planner is more
  focused than the auto-mapping agent.
"""

import json
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

import requests

if TYPE_CHECKING:
    from agents.agent_mapping_pge.contracts import SourceModel

from back.core.logging import get_logger
from agents.engine_base import (
    call_serving_endpoint,
    dispatch_tool,
    accumulate_usage,
)
from agents.tools.context import ToolContext
from agents.tools.documents import (
    GET_DOCUMENTS_CONTEXT_DEF,
    tool_get_documents_context,
)
from agents.tools.metadata import (
    GET_METADATA_DEF,
    tool_get_metadata,
)
from agents.tools.ontology import (
    ONTOLOGY_TOOL_DEFINITIONS,
    ONTOLOGY_TOOL_HANDLERS,
)
from agents.tools.planner import (
    PLANNER_TOOL_DEFINITIONS,
    PLANNER_TOOL_HANDLERS,
)
from agents.tools.sql import (
    SQL_TOOL_DEFINITIONS,
    SQL_TOOL_HANDLERS,
)
from agents.tracing import trace_agent

logger = get_logger(__name__)

MAX_ITERATIONS = 50
LLM_TIMEOUT = 180
_ITERATION_DELAY_SEC = 1

# The submit_source_model JSON for a real-world ontology can run several KB
# (17+ classes × multiple candidates + canonical_ids + join_keys + plan).
# A small ceiling silently truncates the call (finish_reason=length) and the
# dataclass validation fails with no clue to the LLM as to why. 100k removes
# the practical ceiling for any ontology size; you only pay for tokens
# actually generated, so the cost stays bounded by output complexity.
_MAX_TOKENS = 50000

_TRACE_NAME = "mapping_pge_planner"


# =====================================================
# Tool aggregation
# =====================================================
#
# The Planner uses every read tool the auto-mapping agent has — ontology,
# metadata, documents, execute_sql — *plus* the four planner-specific tools.
# It deliberately does NOT receive ``submit_entity_mapping`` /
# ``submit_relationship_mapping``: those belong to the Generator (Sprints 4
# and 5). The Planner's only terminal tool is ``submit_source_model``.

TOOL_DEFINITIONS: List[dict] = (
    [GET_METADATA_DEF, GET_DOCUMENTS_CONTEXT_DEF]
    + ONTOLOGY_TOOL_DEFINITIONS
    + SQL_TOOL_DEFINITIONS
    + PLANNER_TOOL_DEFINITIONS
)

TOOL_HANDLERS: Dict[str, Callable] = {
    "get_metadata": tool_get_metadata,
    "get_documents_context": tool_get_documents_context,
    **ONTOLOGY_TOOL_HANDLERS,
    **SQL_TOOL_HANDLERS,
    **PLANNER_TOOL_HANDLERS,
}


# =====================================================
# Data classes
# =====================================================


@dataclass
class PlannerStep:
    """One observable step of the Planner's execution.

    Mirrors :class:`agents.engine_base.AgentStep` but is scoped to the Planner
    so the orchestrator (Sprint 7) can present a stage-specific timeline in
    the UI.
    """

    step_type: str  # tool_call | tool_result | output
    content: str
    tool_name: str = ""
    duration_ms: int = 0


@dataclass
class PlannerResult:
    """Outcome of a single Planner invocation.

    ``source_model`` is populated only when the LLM successfully called
    ``submit_source_model`` with a structurally-valid payload. ``error`` is
    the short reason string when ``success`` is ``False``.
    """

    success: bool
    source_model: Optional["SourceModel"] = None
    steps: List[PlannerStep] = field(default_factory=list)
    iterations: int = 0
    error: str = ""
    usage: Dict[str, int] = field(default_factory=dict)


# =====================================================
# System prompt
# =====================================================

SYSTEM_PROMPT = """\
You are a senior data architect. Your job is to build a SourceModel that \
bridges a set of source tables to an OWL ontology, so a downstream Generator \
agent can mechanically emit entity- and relationship-mapping SQL.

TOOLS
You have these tools available:
  • get_ontology           – load classes (with attributes) and object \
properties to be mapped.
  • get_metadata           – load imported table schemas (full names, \
columns, types).
  • get_documents_context  – load any imported domain documents (glossaries, \
schema docs).
  • sample_table           – return up to N random rows so you can see \
actual values, not just column types. Use when a column's role is unclear \
from its name/type alone.
  • column_value_overlap   – measure |distinct(from) ∩ distinct(to)| / \
|distinct(from)| for two bare COLUMNS. Use to VALIDATE a candidate join key \
with real data — never propose a join_key on the strength of name similarity \
alone.
  • normalized_value_overlap – the same overlap metric, but each side is a \
scalar SQL EXPRESSION. This is how you PROVE a canonical-key normalization: \
when two tables for the same class have 0% raw overlap, propose a \
normalization expression per table and confirm overlap_pct > 0 here BEFORE \
you submit. A still-zero result means your expression is wrong — fix it.
  • distinct_count         – row / distinct / null counts plus is_unique \
and is_complete flags. Use to confirm a candidate canonical-ID column is \
actually unique and complete.
  • execute_sql            – escape hatch for any check the four tools above \
do not cover. Use sparingly — prefer the focused tools.
  • submit_source_model    – TERMINAL. Call exactly once, when the \
SourceModel is complete and you are ready to hand off to the Generator.

WORKFLOW
1. Call get_ontology AND get_metadata first to see what needs mapping and \
what data is available.
2. Call get_documents_context to pick up any pre-loaded domain documents — \
they often disambiguate column semantics.
3. For each table, decide which ontology class(es) it could realise — these \
become table_roles[].ontology_class_candidates with a confidence and a one- \
sentence reason.
4. For each ontology class, decide which column serves as its canonical \
identifier in each table — record under canonical_ids[]. When you are \
uncertain, run distinct_count to confirm uniqueness/completeness.
5. For each pair of tables that should join (intra-trust FK or cross-source \
value match), run column_value_overlap and only record join_keys[] when the \
realised overlap_pct supports it. Use kind="same_trust_fk" for FK joins and \
kind="cross_source_value_match" for value-matched joins across sources. \
For any class mapped to 2+ tables, follow CANONICAL-KEY NORMALIZATION below \
and PROVE the chosen keys overlap with normalized_value_overlap.
6. Build mapping_plan.entity_order so that BASE classes come first \
(i.e. classes that are referenced by other classes through object properties \
should be mapped before their referencers). Build \
mapping_plan.relationship_order so that, by the time each relationship is \
attempted, BOTH its domain and range classes have already appeared in \
entity_order. List anything you cannot reasonably map under \
mapping_plan.skip[] with a short reason.
7. Finally, call submit_source_model exactly once with the full JSON. The \
call returns success=true when the model is structurally valid; if it \
returns success=false, fix the indicated problem and call it again.

CANONICAL-KEY NORMALIZATION (CRITICAL — this is the #1 cause of relationship dangling)
For any class whose canonical_id lists MORE THAN ONE table, run \
column_value_overlap on a representative column pair to see whether the raw \
values already share a format:

  • If overlap_pct > 0 → values are in compatible formats. Record bare \
column names in canonical_column_per_table (e.g. ``"MOTHER_NHS_NO"``). \
A UNION across the tables produces a coherent ID universe.

  • If overlap_pct == 0 → DO NOT conclude these are "different" or \
"trust-scoped" entities. When two tables both map to the SAME ontology \
class, 0% overlap almost always means the SAME real-world key wrapped in \
DIFFERENT trust-local encodings (prefixes, suffixes, embedded sub-IDs). \
Leaving them disjoint makes every relationship pointing at this class 100% \
dangle — that is a FAILURE, not an acceptable outcome. You MUST normalize:

    STEP 1 — sample_table BOTH columns and read the raw values. Look for a \
shared embedded substring across the trusts — a stable inner identifier \
(UUID, NHS number, ``...-preg-<n>`` core) that appears in every trust's \
value with only the surrounding prefix/suffix differing.

    STEP 2 — write ONE scalar SQL expression PER TABLE that strips the \
trust-specific wrapping and exposes that shared core in an identical form. \
Prefer extracting the shared core over stripping a single known prefix \
(extraction is robust to multiple prefixes). When matching a hex/UUID core, \
ALWAYS anchor the regex with a leading character class so a preceding dash \
is not captured:
          ✗ WRONG: regexp_extract(EPISODE_ID, '([a-f0-9-]+-preg-[0-9]+)', 1)
                   → returns "-<uuid>-preg-1" (leading dash) — will NOT match
          ✓ RIGHT: regexp_extract(EPISODE_ID, '([a-f0-9][a-f0-9-]+-preg-[0-9]+)', 1)
                   → returns "<uuid>-preg-1"

    STEP 3 — for a DERIVED / child key (e.g. a Delivery, Baby or Apgar that \
hangs off a pregnancy), DO NOT concatenate a suffix onto the RAW prefixed \
local id — that re-introduces the trust prefix and the keys stay disjoint. \
Extract the shared core FIRST, then append the role suffix, so every trust \
yields the identical synthetic key:
          ✗ WRONG: trust_a "CONCAT(EPISODE_ID, '-del')"   (→ STA-<uuid>-preg-1-del)
                   trust_b "delivery_id"                  (→ BUH-DEL-BUH-<uuid>-preg-1)
          ✓ RIGHT: trust_a "CONCAT(regexp_extract(EPISODE_ID, '([a-f0-9][a-f0-9-]+-preg-[0-9]+)', 1), '-del')"
                   trust_b "CONCAT(regexp_extract(delivery_id, '([a-f0-9][a-f0-9-]+-preg-[0-9]+)', 1), '-del')"
                   (both → <uuid>-preg-1-del)

    STEP 4 — PROVE IT. Call normalized_value_overlap with your two \
expressions. It MUST return overlap_pct > 0. If it is still 0, your \
expressions land in different value spaces — go back to STEP 1 and fix them. \
Do NOT call submit_source_model with an unverified normalization.

    (If, after sampling, a table genuinely cannot expose the shared core at \
all, omit that table from canonical_column_per_table and note why — but this \
is rare; exhaust STEP 1–4 first.)

  • Whatever expression you record, the EntityGenerator drops it verbatim \
into the SELECT aliased AS ID. Bare column names and SQL expressions are \
both valid here.

  • Always update format_note to one sentence describing what the canonical \
key looks like (e.g. ``"<NHS-uuid>-preg-<ordinal> core extracted from each \
trust's local pregnancy id"``). Downstream agents read this.

SOURCEMODEL JSON SCHEMA (these key names are LOAD-BEARING — do not improvise)
The `model` argument to submit_source_model has exactly this shape:

{
  "table_roles": [
    {
      "table": "<catalog.schema.table>",                       // STRING — required key is "table"
      "ontology_class_candidates": [
        {"uri": "<class URI>", "confidence": 0.0, "reason": "<one sentence>"}
      ]
    }
  ],
  "canonical_ids": [
    {
      "ontology_class": "<class URI>",                          // STRING — required key is "ontology_class"
      // VALUES may be either a bare column name OR a SQL expression that
      // produces the canonical key for that table. Use a SQL expression
      // when raw column values across the listed tables are in different
      // formats (see CANONICAL-KEY NORMALIZATION below).
      "canonical_column_per_table": {"<catalog.schema.table>": "<column or SQL expression>"},
      "format_note": "<one-sentence description of the canonical-key format>"
    }
  ],
  "join_keys": [
    {
      "from_ref": "<table>.<col>",                              // STRING — required key is "from_ref"
      "to_ref":   "<table>.<col>",                              // STRING — required key is "to_ref"
      "confidence": 0.0,
      "overlap_pct": 0.0,
      "kind": "same_trust_fk"                                   // or "cross_source_value_match"
    }
  ],
  "mapping_plan": {
    "entity_order":       ["<class URI>", "..."],
    "relationship_order": ["<property URI>", "..."],
    "skip": [
      {"item": "<class or property URI, or 'all'>", "reason": "<short reason>"}   // required keys: "item", "reason"
    ]
  }
}

Key-name traps to avoid:
• Use "table" (not "name", "table_name", "uri") in each table_roles[] entry.
• Use "ontology_class" (not "class", "uri") in each canonical_ids[] entry.
• Use "from_ref" / "to_ref" (not "from" / "to" / "source" / "target") in each join_keys[] entry.
• Use "item" (not "uri", "property") in each mapping_plan.skip[] entry.

INVARIANTS (the orchestrator will enforce these)
• Every URI in entity_order MUST exist in the ontology AND have at least one \
candidate in table_roles[].ontology_class_candidates.
• Every URI in relationship_order MUST reference a property whose domain \
class and range class both appear in entity_order at an EARLIER position.
• All confidence values are floats in [0.0, 1.0].
• kind on each join_key is EXACTLY one of: "same_trust_fk", \
"cross_source_value_match".
• Call submit_source_model EXACTLY ONCE, at the end. Do not emit a free-text \
summary afterwards — submit_source_model is the terminal step.

GENERAL RULES
• Prefer the focused tools (sample_table, column_value_overlap, \
normalized_value_overlap, distinct_count) over execute_sql.
• Validate candidate join keys with column_value_overlap before adding them \
to join_keys[].
• You may batch multiple independent tool calls in a single response.
• Only ever pass row-returning queries (SELECT / WITH …) to execute_sql.
"""


# =====================================================
# Internal helpers
# =====================================================


def _build_user_prompt(
    entities: List[dict], relationships: List[dict], n_tables: int
) -> str:
    parts = [
        (
            f"Build a SourceModel for {n_tables} table(s), {len(entities)} ontology "
            f"entity/entities, and {len(relationships)} relationship(s). "
            "Start by calling get_ontology, get_metadata, and get_documents_context."
        )
    ]
    if entities:
        names = ", ".join(e.get("name", "?") for e in entities)
        parts.append(f"Entities in scope: {names}")
    if relationships:
        names = ", ".join(r.get("name", "?") for r in relationships)
        parts.append(f"Relationships in scope: {names}")
    prompt = "\n".join(parts)
    logger.debug("_build_user_prompt (%d chars):\n%s", len(prompt), prompt)
    return prompt


# =====================================================
# Public entry point
# =====================================================


@trace_agent(name="mapping_pge_planner")
def run_planner(
    host: str,
    token: str,
    endpoint_name: str,
    client: Any,
    metadata: dict,
    ontology: dict,
    *,
    documents: Optional[list] = None,
    on_step: Optional[Callable[[str, int], None]] = None,
    max_iterations: int = MAX_ITERATIONS,
) -> PlannerResult:
    """Run the Planner agent.

    The Planner autonomously produces a :class:`SourceModel` by exploring the
    ontology, metadata, documents, and source data via tool calls. It
    terminates as soon as it submits a structurally-valid SourceModel via the
    terminal ``submit_source_model`` tool.

    Args:
        host: Databricks workspace URL.
        token: Bearer token for the serving endpoint.
        endpoint_name: Foundation Model serving endpoint name.
        client: Databricks SQL client (must expose ``execute_query(sql)``).
        metadata: Imported domain metadata (``{"tables": [...]}``).
        ontology: Imported ontology (``{"entities": [...], "relationships": [...]}``).
        documents: Optional pre-loaded domain documents.
        on_step: Optional progress callback ``(msg, pct)`` for UI updates.
        max_iterations: Upper bound on tool-call iterations (default 25).

    Returns:
        A :class:`PlannerResult`. ``success`` is True iff a SourceModel was
        successfully submitted; in that case ``source_model`` holds the
        validated dataclass. On failure, ``error`` explains why and
        ``source_model`` is None.
    """
    iteration_limit = max_iterations if max_iterations is not None else MAX_ITERATIONS

    entities = (ontology or {}).get("entities", [])
    relationships = (ontology or {}).get("relationships", [])
    n_tables = len((metadata or {}).get("tables", []))

    logger.info(
        "===== PLANNER START ===== endpoint=%s, tables=%d, entities=%d, relationships=%d, max_iter=%d",
        endpoint_name,
        n_tables,
        len(entities),
        len(relationships),
        iteration_limit,
    )

    ctx = ToolContext(
        host=host.rstrip("/"),
        token=token,
        client=client,
        metadata=metadata or {},
        ontology=ontology or {},
        documents=list(documents or []),
    )

    result = PlannerResult(success=False)

    user_prompt = _build_user_prompt(entities, relationships, n_tables)
    messages: List[dict] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt},
    ]
    logger.info(
        "Planner conversation initialized: system=%d chars, user=%d chars",
        len(SYSTEM_PROMPT),
        len(user_prompt),
    )

    total_usage: Dict[str, int] = {"prompt_tokens": 0, "completion_tokens": 0}

    def _progress_pct(iteration_idx: int) -> int:
        # Linear ramp from 5 → 95 across the iteration budget. The terminal
        # submit_source_model call is what sets 100.
        ratio = (iteration_idx + 1) / max(iteration_limit, 1)
        return min(5 + int(ratio * 90), 95)

    def notify(msg: str, *, pct: Optional[int] = None):
        actual_pct = pct if pct is not None else 5
        logger.info("PLANNER STEP [%d%%] %s", actual_pct, msg)
        if on_step:
            on_step(msg, actual_pct)

    notify("Starting planner…", pct=1)

    # ------------------------------------------------------------------
    # Agent loop
    # ------------------------------------------------------------------
    for iteration in range(iteration_limit):
        # Rate-limit mitigation — same 3s delay as the legacy mapping agent.
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
            "----- Planner iteration %d/%d — %d messages, source_model=%s -----",
            current_iteration,
            iteration_limit,
            len(messages),
            "set" if ctx.source_model is not None else "unset",
        )
        notify(f"Planning iteration {current_iteration}/{iteration_limit}…", pct=pct)

        t0 = time.time()
        try:
            llm_response = call_serving_endpoint(
                host,
                token,
                endpoint_name,
                messages,
                tools=TOOL_DEFINITIONS,
                max_tokens=_MAX_TOKENS,
                temperature=0.1,
                timeout=LLM_TIMEOUT,
                trace_name=_TRACE_NAME,
            )
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "?"
            logger.warning(
                "Planner iteration %d: HTTPError status=%s", current_iteration, status
            )
            logger.debug(
                "Planner iteration %d: HTTPError body: %.500s",
                current_iteration,
                exc.response.text if exc.response is not None else "N/A",
            )
            # Tools are non-negotiable for the Planner — no single-shot fallback.
            if exc.response is not None and status in (400, 422):
                result.error = "LLM endpoint does not support function calling"
                result.iterations = current_iteration
                result.usage = total_usage
                logger.error(
                    "Planner: endpoint refused tools — cannot produce a SourceModel"
                )
                return result
            result.error = f"LLM request failed: {exc}"
            result.iterations = current_iteration
            result.usage = total_usage
            logger.error(
                "Planner: LLM request failed at iteration %d: %s",
                current_iteration,
                exc,
            )
            return result
        except requests.exceptions.ReadTimeout:
            result.error = f"LLM request timed out after {LLM_TIMEOUT}s"
            result.iterations = current_iteration
            result.usage = total_usage
            logger.error("Planner: timeout at iteration %d", current_iteration)
            return result
        except requests.exceptions.RequestException as exc:
            result.error = f"LLM request failed: {exc}"
            result.iterations = current_iteration
            result.usage = total_usage
            logger.error(
                "Planner: request exception at iteration %d: %s",
                current_iteration,
                exc,
            )
            return result

        elapsed_ms = int((time.time() - t0) * 1000)
        logger.info(
            "Planner iteration %d: LLM responded in %dms", current_iteration, elapsed_ms
        )

        accumulate_usage(total_usage, llm_response.get("usage", {}))

        choice = llm_response.get("choices", [{}])[0]
        finish_reason = choice.get("finish_reason", "?")
        message = choice.get("message", {})
        tool_calls = message.get("tool_calls", [])
        has_content = bool(message.get("content"))
        logger.info(
            "Planner iteration %d: finish_reason=%s, tool_calls=%d, has_content=%s",
            current_iteration,
            finish_reason,
            len(tool_calls),
            has_content,
        )
        # A tool call truncated by the max_tokens ceiling produces malformed
        # arguments and the tool can't recover. Flag it loudly so future runs
        # don't silently waste iterations resubmitting the same broken JSON.
        if finish_reason == "length" and tool_calls:
            logger.error(
                "Planner iteration %d: finish_reason=length on a tool call — "
                "arguments were likely truncated. Consider bumping max_tokens.",
                current_iteration,
            )

        if not tool_calls:
            # The Planner must end with submit_source_model, not free text.
            # If we see text without a terminal call, that's a failure.
            content = (message.get("content") or "")[:500]
            logger.warning(
                "Planner iteration %d: produced text without submitting source model — %d chars",
                current_iteration,
                len(message.get("content") or ""),
            )
            result.steps.append(
                PlannerStep(
                    step_type="output",
                    content=content,
                    duration_ms=elapsed_ms,
                )
            )
            result.error = "planner produced text without submitting source model"
            result.iterations = current_iteration
            result.usage = total_usage
            notify("Planner produced text without submitting source model.", pct=pct)
            return result

        # Tool-call branch — dispatch each call and accumulate steps.
        logger.info(
            "Planner iteration %d: processing %d tool call(s): [%s]",
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
                "Planner iteration %d: calling tool '%s' (%d/%d)",
                current_iteration,
                tool_name,
                tc_idx,
                len(tool_calls),
            )

            # Human-readable progress messages per tool — same pattern as
            # the legacy mapping agent for UI consistency.
            if tool_name == "submit_source_model":
                notify("Submitting source model…", pct=pct)
            elif tool_name == "get_metadata":
                notify("Retrieving table metadata…", pct=pct)
            elif tool_name == "get_ontology":
                notify("Retrieving ontology…", pct=pct)
            elif tool_name == "get_documents_context":
                notify("Retrieving documents…", pct=pct)
            elif tool_name == "sample_table":
                fn = arguments.get("full_name", "?")
                notify(f"Sampling {fn}…", pct=pct)
            elif tool_name == "column_value_overlap":
                notify("Checking column overlap…", pct=pct)
            elif tool_name == "normalized_value_overlap":
                notify("Verifying canonical-key normalization…", pct=pct)
            elif tool_name == "distinct_count":
                notify("Checking distinct count…", pct=pct)
            elif tool_name == "execute_sql":
                sql_preview = arguments.get("sql", "")[:80]
                notify(f"Running SQL: {sql_preview}…", pct=pct)
            else:
                notify(f"Calling {tool_name}…", pct=pct)

            result.steps.append(
                PlannerStep(
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
                "Planner iteration %d: tool '%s' returned %d chars in %dms",
                current_iteration,
                tool_name,
                len(tool_result),
                tool_ms,
            )

            result.steps.append(
                PlannerStep(
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

            # Detect terminal success: submit_source_model returned success=True
            # *and* stamped a SourceModel onto the context. We break *after*
            # appending the tool result so the orchestrator sees a complete
            # message trail in conversation/replay.
            if tool_name == "submit_source_model":
                try:
                    parsed = json.loads(tool_result)
                except json.JSONDecodeError:
                    parsed = {}
                if parsed.get("success") is True and ctx.source_model is not None:
                    terminal_success = True
                    logger.info(
                        "Planner iteration %d: submit_source_model succeeded — terminating",
                        current_iteration,
                    )

        if terminal_success:
            result.success = True
            result.source_model = ctx.source_model
            result.iterations = current_iteration
            result.usage = total_usage
            logger.info(
                "===== PLANNER COMPLETE ===== iterations=%d, "
                "prompt_tokens=%d, completion_tokens=%d",
                result.iterations,
                total_usage["prompt_tokens"],
                total_usage["completion_tokens"],
            )
            notify("Planner completed!", pct=100)
            return result

    # Exhausted the iteration budget without ever calling submit_source_model
    # successfully (or the LLM kept calling other tools forever).
    result.iterations = iteration_limit
    result.usage = total_usage
    result.error = "planner exhausted iteration budget without submitting source model"
    logger.error("===== PLANNER FAILED ===== %s", result.error)
    notify(result.error, pct=95)
    return result
