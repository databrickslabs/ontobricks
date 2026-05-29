"""
OntoBricks Mapping-PGE RelationshipGenerator Agent.

Sprint 5 of the Planner-Generator-Evaluator (PGE) redesign.

The RelationshipGenerator is the sibling of :mod:`.entity` — same ReAct
loop shape and tooling discipline, narrower scope. It maps **one** ontology
property (relationship) at a time, given:

* the property to map (uri, label, comment, domain, range),
* the source and target **entity mappings already produced by the
  EntityGenerator** — crucially, the ``id_column`` each side mapped on, and
* a small SourceModel slice that surfaces the relevant join-key subgraph.

The system prompt FORBIDS picking endpoint columns that do not match the
already-mapped entity IDs: the source/target endpoint columns are GIVEN.
This keeps relationships consistent with the entities they connect — if a
relationship's ``source_id`` doesn't match the source entity's ``id_column``,
the resulting SPARQL graph cannot join.

The loop semantics mirror :mod:`.entity`:

* Same default budget (12).
* Same 3-second inter-iteration delay.
* Same MLflow trace decorator.
* No single-shot fallback (terminate via tool call only).
* Strict ``property_uri`` match on terminal detection — a submit with the
  wrong URI is coached via a corrective tool message, not accepted.
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
# See planner._MAX_TOKENS comment — large UNION ALL queries for cross-source
# relationships can exceed a small ceiling.
_MAX_TOKENS = 100_000

_TRACE_NAME = "mapping_pge_relationship_generator"


# =====================================================
# Tool aggregation
# =====================================================
#
# The RelationshipGenerator only needs:
#   * execute_sql                 – validate the composed two-column SELECT.
#   * sample_table                – peek at endpoint columns when the join is
#                                   ambiguous (rare; usually unnecessary).
#   * submit_relationship_mapping – TERMINAL.
#
# We deliberately exclude:
#   * get_ontology / get_metadata / get_documents_context — wrong stage.
#   * column_value_overlap / distinct_count — already locked by the Planner.
#   * submit_source_model / submit_entity_mapping — wrong stage.

_SUBMIT_RELATIONSHIP_DEF: dict = MAPPING_TOOL_DEFINITIONS_BY_NAME[
    "submit_relationship_mapping"
]

TOOL_DEFINITIONS: List[dict] = (
    SQL_TOOL_DEFINITIONS
    + [SAMPLE_TABLE_DEF]
    + [_SUBMIT_RELATIONSHIP_DEF]
)

TOOL_HANDLERS: Dict[str, Callable] = {
    **SQL_TOOL_HANDLERS,
    "sample_table": tool_sample_table,
    "submit_relationship_mapping": MAPPING_TOOL_HANDLERS[
        "submit_relationship_mapping"
    ],
}


# =====================================================
# Data classes
# =====================================================


@dataclass
class RelationshipGenStep:
    """One observable step of the RelationshipGenerator's execution.

    Mirrors :class:`.entity.EntityGenStep` — scoped to the relationship
    generator so the orchestrator (Sprint 7) can render a per-property
    timeline in the UI.
    """

    step_type: str  # "tool_call" | "tool_result" | "output"
    content: str
    tool_name: str = ""
    duration_ms: int = 0


@dataclass
class RelationshipGenResult:
    """Outcome of a single RelationshipGenerator invocation.

    ``mapping`` holds the submitted relationship-mapping dict (the same
    shape the handler appends to ``ctx.relationships``) when ``success`` is
    True.
    """

    success: bool
    mapping: Optional[dict] = None
    steps: List[RelationshipGenStep] = field(default_factory=list)
    iterations: int = 0
    error: str = ""
    usage: Dict[str, int] = field(default_factory=dict)


# =====================================================
# System prompt
# =====================================================
#
# The RELATIONSHIP SQL RULES section is lifted verbatim from the legacy
# in-house mapping agent (the section starting "SQL RULES FOR
# RELATIONSHIPS"). To those rules we add the Sprint 5 constraints: the
# source and target ID columns are GIVEN by the already-produced entity
# mappings; the LLM may not pick different endpoint columns.

SYSTEM_PROMPT = """\
You are a senior data engineer. Your job is to map ONE ontology property \
(relationship) to a single SQL SELECT query against Databricks source \
table(s), validated against real data via execute_sql, and submitted via \
submit_relationship_mapping.

YOU WILL BE GIVEN
• ontology_property: the property to map (uri, label, comment, domain, range).
• source_entity_mapping: the ALREADY-MAPPED source entity (its class_uri, \
its id_column, and the SQL it uses).
• target_entity_mapping: the ALREADY-MAPPED target entity (same shape).
• source_model_slice: a small JSON object the Planner already curated:
  - relevant_joins[]: {from_ref, to_ref, confidence, overlap_pct, kind} — \
the join keys the Planner believes connect the two endpoints. Prefer \
high-overlap, high-confidence joins.
  - candidate_tables[]: the tables that contain the join, surfaced for \
convenience.

ENDPOINT COLUMNS ARE GIVEN (CRITICAL)
The source and target ID columns are NOT yours to pick. They come from the \
already-mapped entities:
  • source_id values MUST come from the same column the source entity uses \
as id_column (or be directly transformable into it via a join key in the \
slice).
  • target_id values MUST come from the same column the target entity uses \
as id_column (likewise).
Picking different endpoint columns produces a broken graph: the entity \
node IDs and the relationship endpoints will not align.

TOOLS
You have three tools:
  • execute_sql                 – Validate the composed two-column SELECT \
before submitting. The tool runs your query with a small LIMIT and returns \
columns + sample rows; the persisted mapping has no LIMIT.
  • sample_table                – Up to N random rows from a table. Use only \
when the join column is ambiguous and you need to peek at real values.
  • submit_relationship_mapping – TERMINAL. Call EXACTLY ONCE, after \
execute_sql succeeds, with the full mapping payload.

SQL RULES FOR RELATIONSHIPS (CRITICAL)
• SELECT exactly 2 columns: source identifier AS source_id, target \
identifier AS target_id.
• If both columns are in the SAME table, query only that table (no joins).
• Do NOT add LIMIT or ORDER BY.
• Always use full table names (catalog.schema.table).

CHOOSING THE SHAPE OF THE QUERY
• For same-trust FK joins (kind="same_trust_fk"): a simple SELECT from one \
table is usually enough — the foreign key already sits next to the primary \
key on the row.
• For cross-source relationships (kind="cross_source_value_match"): the \
SQL is typically a UNION ALL of single-source SELECTs (one per source that \
contains both endpoint values), or a JOIN through a shared canonical key. \
Pick whichever produces the most rows without duplicating pairs.
• Always prefer joins/columns with the highest confidence and overlap_pct \
in the slice. Low-overlap joins produce sparse and unreliable edges.

WORKFLOW
1. Read the property metadata, the two entity mappings, and the slice. Note \
the source.id_column and target.id_column — those are your endpoints.
2. Compose the two-column SELECT following the SQL RULES above. Use a \
join-key from relevant_joins. The source_id MUST come from (or join to) \
the source entity's id_column; same for target_id.
3. Call execute_sql to validate the SHAPE of the query (it parses, returns \
two columns, returns rows). If it fails, READ the error and fix the SQL. \
Never submit an un-validated query.
4. SELF-VERIFY THE VALUES BEFORE SUBMITTING (CRITICAL — the #1 cause of \
relationship failures). Name-similarity is not enough: a column called \
`infant_id` may hold trust-local keys that do NOT match the Baby entity's \
NHS-derived IDs. Run a dangling-edge probe via execute_sql:

  WITH rel AS (<your two-column SELECT>),
       src AS (<source entity's SQL>),
       tgt AS (<target entity's SQL>)
  SELECT
    (SELECT COUNT(*) FROM rel) AS edges,
    (SELECT COUNT(*) FROM rel r WHERE r.source_id NOT IN (SELECT ID FROM src)) AS dangling_src,
    (SELECT COUNT(*) FROM rel r WHERE r.target_id NOT IN (SELECT ID FROM tgt)) AS dangling_tgt

  If dangling_src or dangling_tgt is high relative to edges, your endpoint \
columns are wrong — STOP and pick different columns or join keys. Repeat \
steps 2–4 until both dangling counts are 0 or a small fraction of edges. \
The evaluator will reject any mapping with >5% dangling on either side \
(unless a cross-source band was explicitly predicted by the Planner).
5. Once self-verify is clean, call submit_relationship_mapping EXACTLY \
ONCE with: property_uri, property_name, sql_query (no LIMIT), \
source_id_column, target_id_column, domain, range_class. The \
source_id_column / target_id_column values you submit MUST match the \
id_column on the corresponding entity mapping.
6. That's the terminal step. Do not emit any free text after submitting.

GENERAL RULES
• Only ever pass row-returning queries (SELECT / WITH …) to execute_sql.
• Do not call get_metadata, get_ontology, column_value_overlap, \
distinct_count, submit_entity_mapping, or submit_source_model — they are \
not available to you. The slice plus the entity mappings carry everything \
you need.
• If a retry_hint is present at the top of the user message, treat it as \
authoritative — your previous attempt failed for the reason stated and you \
should NOT repeat the same mistake.
"""


# =====================================================
# Internal helpers
# =====================================================


def _summarise_entity_mapping(em: dict, side: str) -> List[str]:
    """One-block textual summary of a previously-produced entity mapping.

    Surfaces exactly the fields the LLM needs to constrain its endpoint
    choice: the class_uri, the id_column it locked in, and the SQL it ran.
    Anything else (label_column, attribute_mappings, …) is irrelevant to the
    relationship task and is intentionally omitted to keep the prompt tight.
    """
    em = em or {}
    class_uri = (
        em.get("ontology_class") or em.get("class_uri") or em.get("class") or ""
    )
    id_column = em.get("id_column", "")
    sql_query = em.get("sql_query", "")
    return [
        f"{side.upper()} ENTITY MAPPING",
        f"  class_uri: {class_uri}",
        f"  id_column: {id_column}",
        f"  sql:       {sql_query}",
    ]


def _format_join(j: dict) -> str:
    """Readable one-line rendering of a join entry from the slice.

    Defensive about missing fields — partial joins still render usefully so
    a malformed slice doesn't blow up the prompt build.
    """
    from_ref = j.get("from_ref", "?")
    to_ref = j.get("to_ref", "?")
    kind = j.get("kind", "?")
    conf = j.get("confidence")
    overlap = j.get("overlap_pct")
    extras: List[str] = []
    if conf is not None:
        try:
            extras.append(f"confidence={float(conf):.2f}")
        except (TypeError, ValueError):
            extras.append(f"confidence={conf}")
    if overlap is not None:
        try:
            extras.append(f"overlap_pct={float(overlap):.2f}")
        except (TypeError, ValueError):
            extras.append(f"overlap_pct={overlap}")
    suffix = (" — " + ", ".join(extras)) if extras else ""
    return f"  - {from_ref} -> {to_ref}  [{kind}]{suffix}"


def _build_user_prompt(
    ontology_property: dict,
    source_entity_mapping: dict,
    target_entity_mapping: dict,
    source_model_slice: dict,
    retry_hint: Optional[str] = None,
) -> str:
    """Render the per-property user prompt.

    Structure:
      1. retry_hint (if any) at the very top
      2. ontology property metadata
      3. source entity mapping summary (class_uri / id_column / sql)
      4. target entity mapping summary
      5. relevant joins (one line per join, readable)
      6. candidate_tables (raw JSON — small)
      7. a reminder block reiterating the two-column / endpoint-match rules
    """
    parts: List[str] = []

    if retry_hint:
        parts.append("RETRY HINT (authoritative — your previous attempt FAILED):")
        parts.append(retry_hint)
        parts.append(
            "DO NOT repeat the same column choice. If the hint mentions "
            "'dangling' or 'canonical id': sample BOTH the candidate endpoint "
            "column AND the entity's id_column, compare actual values, and "
            "pick the column whose values overlap. Run the dangling-edge "
            "probe (step 4 of WORKFLOW) BEFORE submitting this time.\n"
        )

    prop_uri = ontology_property.get("uri", "")
    prop_label = (
        ontology_property.get("label") or ontology_property.get("name", "")
    )
    prop_comment = ontology_property.get("comment", "") or ""
    prop_domain = ontology_property.get("domain", "") or ""
    prop_range = ontology_property.get("range", "") or ""

    parts.append("ONTOLOGY PROPERTY")
    parts.append(f"  uri:     {prop_uri}")
    parts.append(f"  label:   {prop_label}")
    if prop_comment:
        parts.append(f"  comment: {prop_comment}")
    parts.append(f"  domain:  {prop_domain}")
    parts.append(f"  range:   {prop_range}")

    parts.append("")
    parts.extend(_summarise_entity_mapping(source_entity_mapping, side="source"))

    parts.append("")
    parts.extend(_summarise_entity_mapping(target_entity_mapping, side="target"))

    slice_obj = source_model_slice or {}
    joins = slice_obj.get("relevant_joins") or []
    candidates = slice_obj.get("candidate_tables") or []

    parts.append("")
    parts.append("RELEVANT JOINS")
    if joins:
        for j in joins:
            parts.append(_format_join(j))
    else:
        parts.append("  (none surfaced by the Planner — fall back to a single-table SELECT if possible)")

    if candidates:
        parts.append("")
        parts.append("CANDIDATE TABLES")
        parts.append(json.dumps(candidates, indent=2, default=str))

    src_id = (source_entity_mapping or {}).get("id_column", "")
    tgt_id = (target_entity_mapping or {}).get("id_column", "")

    parts.append("")
    parts.append("REMINDERS (CRITICAL)")
    parts.append(
        "  • The persisted SQL MUST return EXACTLY two columns aliased "
        "AS source_id and AS target_id."
    )
    parts.append(
        f"  • source_id values MUST come from the column '{src_id}' (the "
        "source entity's id_column) — or be directly transformable into it "
        "via a join key in the slice."
    )
    parts.append(
        f"  • target_id values MUST come from the column '{tgt_id}' (the "
        "target entity's id_column) — same constraint."
    )
    parts.append(
        "  • Validate with execute_sql, then call submit_relationship_mapping "
        "exactly once."
    )

    prompt = "\n".join(parts)
    logger.debug(
        "_build_user_prompt for property=%s (%d chars):\n%s",
        prop_uri,
        len(prompt),
        prompt,
    )
    return prompt


# =====================================================
# Public entry point
# =====================================================


@trace_agent(name="mapping_pge_relationship_generator")
def run_relationship_generator(
    host: str,
    token: str,
    endpoint_name: str,
    client: Any,
    *,
    ontology_property: dict,
    source_entity_mapping: dict,
    target_entity_mapping: dict,
    source_model_slice: dict,
    retry_hint: Optional[str] = None,
    on_step: Optional[Callable[[str, int], None]] = None,
    max_iterations: int = MAX_ITERATIONS,
) -> RelationshipGenResult:
    """Run the RelationshipGenerator agent for a single ontology property.

    The agent composes a two-column SQL SELECT (``source_id`` / ``target_id``)
    that realises the relationship between the source and target entities
    using the join-key subgraph in ``source_model_slice``, validates the
    SQL via ``execute_sql``, and submits the validated mapping via the
    terminal ``submit_relationship_mapping`` tool.

    Args:
        host: Databricks workspace URL.
        token: Bearer token for the serving endpoint.
        endpoint_name: Foundation Model serving endpoint name.
        client: Databricks SQL client (must expose ``execute_query(sql)``).
        ontology_property: Full dict for the SINGLE property to map (uri,
            label, comment, domain, range).
        source_entity_mapping: The ALREADY-MAPPED source entity (carries the
            ``id_column`` the source endpoint must align with).
        target_entity_mapping: The ALREADY-MAPPED target entity (same).
        source_model_slice: Filtered SourceModel slice with relevant_joins
            and optional candidate_tables.
        retry_hint: Optional one-sentence hint from the orchestrator's
            previous-attempt evaluation. When present, surfaced at the top
            of the user prompt.
        on_step: Optional progress callback ``(msg, pct)`` for UI updates.
        max_iterations: Upper bound on tool-call iterations (default 12 —
            same as the EntityGenerator).

    Returns:
        A :class:`RelationshipGenResult`. ``success`` is True iff a mapping
        was successfully submitted with the requested ``property_uri``; in
        that case ``mapping`` holds the submitted dict. On failure, ``error``
        explains why and ``mapping`` is None.
    """
    iteration_limit = max_iterations if max_iterations is not None else MAX_ITERATIONS

    property_uri = (ontology_property or {}).get("uri", "")
    property_label = (
        (ontology_property or {}).get("label")
        or (ontology_property or {}).get("name", "")
    )
    n_joins = len(((source_model_slice or {}).get("relevant_joins") or []))
    n_candidates = len(((source_model_slice or {}).get("candidate_tables") or []))

    logger.info(
        "===== RELATIONSHIP GENERATOR START ===== endpoint=%s, property=%s (%s), "
        "joins=%d, candidate_tables=%d, retry_hint=%s, max_iter=%d",
        endpoint_name,
        property_label,
        property_uri,
        n_joins,
        n_candidates,
        "yes" if retry_hint else "no",
        iteration_limit,
    )

    ctx = ToolContext(
        host=host.rstrip("/"),
        token=token,
        client=client,
        # The slice + entity mappings subsume metadata/ontology for this
        # agent; the unified ToolContext still wants these fields, so we
        # leave them empty.
        metadata={},
        ontology={},
        documents=[],
    )

    result = RelationshipGenResult(success=False)

    user_prompt = _build_user_prompt(
        ontology_property or {},
        source_entity_mapping or {},
        target_entity_mapping or {},
        source_model_slice or {},
        retry_hint=retry_hint,
    )
    messages: List[dict] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt},
    ]
    logger.info(
        "RelationshipGenerator conversation initialized: system=%d chars, user=%d chars",
        len(SYSTEM_PROMPT),
        len(user_prompt),
    )

    total_usage: Dict[str, int] = {"prompt_tokens": 0, "completion_tokens": 0}

    def _progress_pct(iteration_idx: int) -> int:
        ratio = (iteration_idx + 1) / max(iteration_limit, 1)
        return min(5 + int(ratio * 90), 95)

    def notify(msg: str, *, pct: Optional[int] = None) -> None:
        actual_pct = pct if pct is not None else 5
        logger.info("RELATIONSHIP GEN STEP [%d%%] %s", actual_pct, msg)
        if on_step:
            on_step(msg, actual_pct)

    notify(f"Generating mapping for {property_label or property_uri}…", pct=1)

    # Snapshot the pre-existing relationship count so we can detect "this
    # run added a mapping" without relying on absolute counters. Future-proof
    # for an orchestrator that reuses a ToolContext across calls.
    pre_run_count = len(ctx.relationships)

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
            "----- RelationshipGenerator iteration %d/%d — %d messages, mapping=%s -----",
            current_iteration,
            iteration_limit,
            len(messages),
            "set" if len(ctx.relationships) > pre_run_count else "unset",
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
                max_tokens=_MAX_TOKENS,
                temperature=0.1,
                timeout=LLM_TIMEOUT,
                trace_name=_TRACE_NAME,
            )
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "?"
            logger.warning(
                "RelationshipGenerator iteration %d: HTTPError status=%s",
                current_iteration,
                status,
            )
            logger.debug(
                "RelationshipGenerator iteration %d: HTTPError body: %.500s",
                current_iteration,
                exc.response.text if exc.response is not None else "N/A",
            )
            if exc.response is not None and status in (400, 422):
                result.error = "LLM endpoint does not support function calling"
                result.iterations = current_iteration
                result.usage = total_usage
                logger.error(
                    "RelationshipGenerator: endpoint refused tools — cannot produce a mapping"
                )
                return result
            result.error = f"LLM request failed: {exc}"
            result.iterations = current_iteration
            result.usage = total_usage
            logger.error(
                "RelationshipGenerator: LLM request failed at iteration %d: %s",
                current_iteration,
                exc,
            )
            return result
        except requests.exceptions.ReadTimeout:
            result.error = f"LLM request timed out after {LLM_TIMEOUT}s"
            result.iterations = current_iteration
            result.usage = total_usage
            logger.error(
                "RelationshipGenerator: timeout at iteration %d", current_iteration
            )
            return result
        except requests.exceptions.RequestException as exc:
            result.error = f"LLM request failed: {exc}"
            result.iterations = current_iteration
            result.usage = total_usage
            logger.error(
                "RelationshipGenerator: request exception at iteration %d: %s",
                current_iteration,
                exc,
            )
            return result

        elapsed_ms = int((time.time() - t0) * 1000)
        logger.info(
            "RelationshipGenerator iteration %d: LLM responded in %dms",
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
            "RelationshipGenerator iteration %d: finish_reason=%s, tool_calls=%d, has_content=%s",
            current_iteration,
            finish_reason,
            len(tool_calls),
            has_content,
        )

        if not tool_calls:
            # The Generator must terminate via submit_relationship_mapping,
            # never via free text.
            content = (message.get("content") or "")[:500]
            logger.warning(
                "RelationshipGenerator iteration %d: produced text without submitting mapping — %d chars",
                current_iteration,
                len(message.get("content") or ""),
            )
            result.steps.append(
                RelationshipGenStep(
                    step_type="output",
                    content=content,
                    duration_ms=elapsed_ms,
                )
            )
            result.error = "relationship generator produced text without submitting mapping"
            result.iterations = current_iteration
            result.usage = total_usage
            notify(
                "Relationship generator produced text without submitting mapping.",
                pct=pct,
            )
            return result

        logger.info(
            "RelationshipGenerator iteration %d: processing %d tool call(s): [%s]",
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
                "RelationshipGenerator iteration %d: calling tool '%s' (%d/%d)",
                current_iteration,
                tool_name,
                tc_idx,
                len(tool_calls),
            )

            if tool_name == "submit_relationship_mapping":
                notify(
                    f"Submitting mapping for {property_label or property_uri}…",
                    pct=pct,
                )
            elif tool_name == "sample_table":
                fn = arguments.get("full_name", "?")
                notify(f"Sampling {fn}…", pct=pct)
            elif tool_name == "execute_sql":
                sql_preview = arguments.get("sql", "")[:80]
                notify(f"Running SQL: {sql_preview}…", pct=pct)
            else:
                notify(f"Calling {tool_name}…", pct=pct)

            result.steps.append(
                RelationshipGenStep(
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
                "RelationshipGenerator iteration %d: tool '%s' returned %d chars in %dms",
                current_iteration,
                tool_name,
                len(tool_result),
                tool_ms,
            )

            result.steps.append(
                RelationshipGenStep(
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

            # Detect terminal success: submit_relationship_mapping returned
            # success=True AND a mapping for THIS property_uri is present in
            # ctx.relationships. A submit with a mismatched property_uri is
            # NOT terminal — we coach the LLM via a corrective tool message
            # and let the loop continue.
            if tool_name == "submit_relationship_mapping":
                try:
                    parsed = json.loads(tool_result)
                except json.JSONDecodeError:
                    parsed = {}
                if parsed.get("success") is True:
                    matched = any(
                        m.get("property") == property_uri
                        for m in ctx.relationships
                    )
                    if matched:
                        terminal_success = True
                        logger.info(
                            "RelationshipGenerator iteration %d: submit_relationship_mapping succeeded — terminating",
                            current_iteration,
                        )
                    else:
                        submitted_uri = arguments.get("property_uri", "")
                        mismatch_msg = (
                            f"submitted property_uri '{submitted_uri}' does "
                            f"not match requested property_uri "
                            f"'{property_uri}'; resubmit with "
                            f"property_uri='{property_uri}'"
                        )
                        logger.warning(
                            "RelationshipGenerator iteration %d: submit_relationship_mapping "
                            "property_uri mismatch — submitted=%s, requested=%s",
                            current_iteration,
                            submitted_uri,
                            property_uri,
                        )
                        corrective_payload = json.dumps(
                            {"success": False, "error": mismatch_msg}
                        )
                        # Replace the recorded tool_result step's content so
                        # the UI / trace shows the corrective signal.
                        result.steps[-1] = RelationshipGenStep(
                            step_type="tool_result",
                            content=corrective_payload,
                            tool_name=tool_name,
                            duration_ms=result.steps[-1].duration_ms,
                        )
                        # Replace the tool message on the conversation so
                        # the LLM sees the corrective payload next turn.
                        messages[-1] = {
                            "role": "tool",
                            "tool_call_id": tool_id,
                            "content": corrective_payload,
                        }

        if terminal_success:
            # Pull the mapping for this property by strict URI match.
            submitted = next(
                (
                    m
                    for m in reversed(ctx.relationships)
                    if m.get("property") == property_uri
                ),
                None,
            )
            if submitted is None:
                result.error = (
                    "internal: submit succeeded but mapping not found for property_uri"
                )
                result.iterations = current_iteration
                result.usage = total_usage
                logger.error(
                    "===== RELATIONSHIP GENERATOR FAILED ===== %s (property=%s)",
                    result.error,
                    property_uri,
                )
                return result
            result.success = True
            result.mapping = submitted
            result.iterations = current_iteration
            result.usage = total_usage
            logger.info(
                "===== RELATIONSHIP GENERATOR COMPLETE ===== property=%s, iterations=%d, "
                "prompt_tokens=%d, completion_tokens=%d",
                property_uri,
                result.iterations,
                total_usage["prompt_tokens"],
                total_usage["completion_tokens"],
            )
            notify(
                f"Mapping for {property_label or property_uri} complete!", pct=100
            )
            return result

    # Budget exhausted without a successful submit.
    result.iterations = iteration_limit
    result.usage = total_usage
    result.error = "relationship generator exhausted iteration budget"
    logger.error("===== RELATIONSHIP GENERATOR FAILED ===== %s", result.error)
    notify(result.error, pct=95)
    return result
