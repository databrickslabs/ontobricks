"""
OntoBricks Mapping-PGE Semantic Critic Agent.

Sprint 6 of the Planner-Generator-Evaluator (PGE) redesign — stage 2 of the
Evaluator. Runs ONLY after the deterministic (stage-1) evaluator has passed.

The Critic audits ONE submitted mapping for SEMANTIC correctness — issues that
pure structural checks cannot catch:

* the WRONG TABLE was picked (e.g. ``antenatal_visits`` chosen to realise
  the ``Delivery`` class), or
* the wrong COLUMN within the right table (e.g. ``appointment_date`` used
  for ``deliveryDate``).

The Critic's "bubble" signal is sharp: if the wrong TABLE was chosen, the
verdict bubbles to the Planner (which must revise the source model); if just
a wrong column inside the right table, the verdict stays with the Generator
which can retry against the same table.

The loop shape mirrors :mod:`agents.agent_mapping_pge.generators.entity` —
same ``call_serving_endpoint`` + ``dispatch_tool`` ReAct cycle, same 3-second
inter-iteration delay, same MLflow trace decorator. Differences:

* Smaller default budget (6) — auditing is bounded work; if the Critic can't
  conclude in 6 iterations, it defers (PASS with a reasoning note) rather
  than falsely escalates.
* Different tool set: only ``sample_table``, ``execute_sql``,
  ``get_documents_context``, and the terminal ``submit_evaluation``.
* No single-shot fallback — the Critic produces structured output through
  ``submit_evaluation`` only.
"""

import json
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

import requests

if TYPE_CHECKING:
    from agents.agent_mapping_pge.contracts import EvalReport

from back.core.logging import get_logger
from agents.engine_base import (
    accumulate_usage,
    call_serving_endpoint,
    dispatch_tool,
)
from agents.tools.context import ToolContext
from agents.tools.documents import (
    GET_DOCUMENTS_CONTEXT_DEF,
    tool_get_documents_context,
)
from agents.tools.evaluation import (
    EVALUATION_TOOL_DEFINITIONS,
    EVALUATION_TOOL_HANDLERS,
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

MAX_ITERATIONS = 6
LLM_TIMEOUT = 180
_ITERATION_DELAY_SEC = 3
# See planner._MAX_TOKENS comment — same rationale for submit_evaluation.
_MAX_TOKENS = 100_000

_TRACE_NAME = "mapping_pge_critic"


# =====================================================
# Tool aggregation
# =====================================================
#
# The Critic only needs:
#   * sample_table           – peek at actual values to verify the column
#                              picked really represents the ontology concept.
#   * execute_sql            – targeted probes for "is this column really
#                              what it claims" sanity checks.
#   * get_documents_context  – consult any imported domain glossary.
#   * submit_evaluation      – TERMINAL.
#
# We deliberately exclude:
#   * get_metadata / get_ontology — the audit target is supplied in the user
#     prompt; broad re-fetches just inflate context.
#   * column_value_overlap / distinct_count — those are structural, already
#     covered by the deterministic stage.
#   * submit_source_model / submit_entity_mapping / submit_relationship_mapping
#     — wrong stage.

TOOL_DEFINITIONS: List[dict] = (
    [SAMPLE_TABLE_DEF, GET_DOCUMENTS_CONTEXT_DEF]
    + SQL_TOOL_DEFINITIONS
    + EVALUATION_TOOL_DEFINITIONS
)

TOOL_HANDLERS: Dict[str, Callable] = {
    "sample_table": tool_sample_table,
    "get_documents_context": tool_get_documents_context,
    **SQL_TOOL_HANDLERS,
    **EVALUATION_TOOL_HANDLERS,
}


# =====================================================
# Data classes
# =====================================================


@dataclass
class CriticStep:
    """One observable step of the Critic's execution.

    Mirrors :class:`agents.agent_mapping_pge.generators.entity.EntityGenStep`
    so the orchestrator (Sprint 7) can render a per-audit timeline in the UI.
    """

    step_type: str  # "tool_call" | "tool_result" | "output"
    content: str
    tool_name: str = ""
    duration_ms: int = 0


@dataclass
class CriticResult:
    """Outcome of a single Critic invocation.

    ``report`` is populated when the agent terminated by submitting a verdict
    via ``submit_evaluation``. ``success`` here is the agent-level success
    flag — it indicates a *clean termination*, NOT a PASS verdict. A FAIL
    verdict with ``bubble_to_planner=True`` still has ``success=True``.
    ``success=False`` is reserved for budget exhaustion, text-only output,
    and LLM/transport errors.
    """

    success: bool
    report: Optional["EvalReport"] = None
    steps: List[CriticStep] = field(default_factory=list)
    iterations: int = 0
    error: str = ""
    usage: Dict[str, int] = field(default_factory=dict)


# =====================================================
# System prompt
# =====================================================
#
# Kept under 3KB. Frames the Critic as a senior data engineer auditing ONE
# submitted mapping for SEMANTIC correctness — the structural checks have
# already passed. The decision rubric (PASS / FAIL+no-bubble / FAIL+bubble)
# is load-bearing: it determines whether the orchestrator retries the
# Generator or re-invokes the Planner.

SYSTEM_PROMPT = """\
You are a senior data engineer auditing ONE submitted mapping for SEMANTIC \
correctness. The structural checks (row counts, distinct IDs, dangling FKs) \
have ALREADY PASSED — your job is to catch wrong-concept errors that pure \
structural checks cannot see.

WHAT YOU AUDIT
• Did the mapping pick the RIGHT TABLE for the ontology class/property?
• Do sampled values in the chosen column(s) actually represent what the \
ontology attribute means? (e.g. "delivery_date" should be a delivery date, \
not a booking date.)
• Does the column's semantics match the ontology comment / label?

TOOLS
You have these tools:
  • sample_table          – Up to N random rows from a table. Use to peek at \
actual values and check they match the concept.
  • execute_sql           – Targeted SQL for "is this column really what it \
claims" probes (e.g. value ranges, distinct categories, null patterns).
  • get_documents_context – Imported domain glossaries / data dictionaries. \
Check against these when the column's role is non-obvious.
  • submit_evaluation     – TERMINAL. Call EXACTLY ONCE when you have a \
confident verdict.

DECISION RUBRIC
• PASS — sampled values, column semantics, and domain context all support \
the mapping. status="PASS", failures=[], bubble_to_planner=false.
• FAIL with bubble_to_planner=false — the WRONG COLUMN was picked within \
the RIGHT TABLE. The Generator can fix this on retry. Populate failures[] \
with the specific column-level issue and a concrete hint.
• FAIL with bubble_to_planner=true — the WRONG TABLE was chosen entirely. \
The Planner must revise the source model. Populate failures[] and set the \
bubble flag.

HINT DISCIPLINE
• Hints must be CONCRETE, ACTIONABLE, single-sentence corrections.
• Good column-level hint: "Sampled rows show `appointment_date` is the \
booking date, not delivery date. Use `delivery_dttm` instead."
• Good table-level hint: "This mapping uses `antenatal_visits`, but the \
chosen class is Delivery. Switch to the `labour_delivery` table."
• Bad hint (vague): "consider using a different column"
• Bad hint (chatty): "I think there might be an issue here, you should look \
into it more carefully"

HARD RULES
• You are bounded by max_iterations=6. Keep your audit FOCUSED — pick the \
one or two probes that would change your verdict, not exhaustive ones.
• Call submit_evaluation EXACTLY ONCE.
• If you cannot determine a verdict within 6 iterations, submit PASS with a \
reasoning note explaining the uncertainty. Do NOT bubble — better to defer \
than to falsely escalate.
• Do not call get_metadata, get_ontology, column_value_overlap, \
distinct_count, submit_source_model, submit_entity_mapping, or \
submit_relationship_mapping — they are not available to you. The audit \
target and structural metrics are already in the user message.
"""


# =====================================================
# Internal helpers
# =====================================================


def _format_entity_definition(item_definition: dict) -> List[str]:
    """Lines for an entity (ontology class) audit target."""
    parts: List[str] = []
    label = item_definition.get("label") or item_definition.get("name", "")
    comment = item_definition.get("comment", "") or ""
    attributes = item_definition.get("attributes", []) or []

    parts.append(f"  label:   {label}")
    if comment:
        parts.append(f"  comment: {comment}")
    if attributes:
        parts.append(f"  attributes ({len(attributes)}):")
        for attr in attributes:
            if isinstance(attr, dict):
                a_name = attr.get("name") or attr.get("label") or attr.get("uri", "?")
                a_type = attr.get("type") or attr.get("range") or ""
                parts.append(f"    - {a_name}" + (f" ({a_type})" if a_type else ""))
            else:
                parts.append(f"    - {attr}")
    return parts


def _format_relationship_definition(item_definition: dict) -> List[str]:
    """Lines for a relationship (ontology property) audit target.

    Always emits explicit ``domain`` and ``range`` lines — these are what
    differentiate a relationship audit from an entity audit, and the tests
    pin them.
    """
    parts: List[str] = []
    label = item_definition.get("label") or item_definition.get("name", "")
    comment = item_definition.get("comment", "") or ""
    domain = item_definition.get("domain", "") or ""
    range_class = item_definition.get("range", "") or ""

    parts.append(f"  label:   {label}")
    if comment:
        parts.append(f"  comment: {comment}")
    parts.append(f"  domain:  {domain}")
    parts.append(f"  range:   {range_class}")
    return parts


def _format_submitted_entity_mapping(submitted_mapping: dict) -> List[str]:
    """Lines summarising an entity mapping under audit."""
    parts: List[str] = ["SUBMITTED MAPPING (entity)"]
    parts.append(f"  sql_query:       {submitted_mapping.get('sql_query', '')}")
    parts.append(f"  id_column:       {submitted_mapping.get('id_column', '')}")
    parts.append(f"  label_column:    {submitted_mapping.get('label_column', '')}")
    attr_map = submitted_mapping.get("attribute_mappings", {}) or {}
    if attr_map:
        parts.append("  attribute_mappings:")
        for k, v in attr_map.items():
            parts.append(f"    {k} -> {v}")
    unmapped = submitted_mapping.get("unmapped_attributes", []) or []
    if unmapped:
        parts.append("  unmapped_attributes:")
        for u in unmapped:
            if isinstance(u, dict):
                parts.append(
                    f"    - {u.get('name', '?')}: {u.get('reason', '')}"
                )
            else:
                parts.append(f"    - {u}")
    return parts


def _format_submitted_relationship_mapping(submitted_mapping: dict) -> List[str]:
    """Lines summarising a relationship mapping under audit."""
    parts: List[str] = ["SUBMITTED MAPPING (relationship)"]
    parts.append(f"  sql_query:        {submitted_mapping.get('sql_query', '')}")
    parts.append(
        f"  source_id_column: {submitted_mapping.get('source_id_column', '')}"
    )
    parts.append(
        f"  target_id_column: {submitted_mapping.get('target_id_column', '')}"
    )
    parts.append(
        f"  source_class:     {submitted_mapping.get('source_class', '') or submitted_mapping.get('domain', '')}"
    )
    parts.append(
        f"  target_class:     {submitted_mapping.get('target_class', '') or submitted_mapping.get('range_class', '')}"
    )
    return parts


def _build_user_prompt(
    item_kind: str,
    item_uri: str,
    item_definition: dict,
    submitted_mapping: dict,
    source_model_slice: dict,
    stage1_metrics: dict,
) -> str:
    """Render the audit user prompt.

    Structure:
      1. AUDIT TARGET — item_kind, URI, ontology metadata (label/comment,
         attributes for entities; domain/range for relationships).
      2. SUBMITTED MAPPING — the actual mapping under audit.
      3. PLANNER'S PREDICTION — the slice the Planner curated for this item.
      4. STRUCTURAL CHECK METRICS (PASSED) — context from stage 1.
      5. YOUR TASK — short reminder of the rubric.
    """
    parts: List[str] = []

    parts.append("AUDIT TARGET")
    parts.append(f"  kind:    {item_kind}")
    parts.append(f"  uri:     {item_uri}")
    if item_kind == "relationship":
        parts.extend(_format_relationship_definition(item_definition or {}))
    else:
        parts.extend(_format_entity_definition(item_definition or {}))

    parts.append("")
    if item_kind == "relationship":
        parts.extend(_format_submitted_relationship_mapping(submitted_mapping or {}))
    else:
        parts.extend(_format_submitted_entity_mapping(submitted_mapping or {}))

    parts.append("")
    parts.append("PLANNER'S PREDICTION")
    parts.append(json.dumps(source_model_slice or {}, indent=2, default=str))

    parts.append("")
    parts.append("STRUCTURAL CHECK METRICS (PASSED)")
    parts.append(json.dumps(stage1_metrics or {}, indent=2, default=str))

    parts.append("")
    parts.append("YOUR TASK")
    parts.append(
        "Audit the SEMANTIC correctness of the submitted mapping. Use "
        "sample_table / execute_sql / get_documents_context as needed, then "
        "call submit_evaluation EXACTLY ONCE with your verdict. Follow the "
        "PASS / FAIL(no bubble) / FAIL(bubble) rubric in the system prompt."
    )

    prompt = "\n".join(parts)
    logger.debug(
        "_build_user_prompt for %s=%s (%d chars):\n%s",
        item_kind,
        item_uri,
        len(prompt),
        prompt,
    )
    return prompt


# =====================================================
# Public entry point
# =====================================================


@trace_agent(name="mapping_pge_critic")
def run_critic(
    host: str,
    token: str,
    endpoint_name: str,
    client: Any,
    *,
    item_kind: str,
    item_uri: str,
    item_definition: dict,
    submitted_mapping: dict,
    source_model_slice: dict,
    stage1_metrics: dict,
    documents: Optional[list] = None,
    on_step: Optional[Callable[[str, int], None]] = None,
    max_iterations: int = MAX_ITERATIONS,
) -> CriticResult:
    """Run the Semantic Critic agent for one submitted mapping.

    The Critic autonomously audits ``submitted_mapping`` for semantic
    correctness using ``sample_table`` / ``execute_sql`` /
    ``get_documents_context``, then submits a verdict via the terminal
    ``submit_evaluation`` tool. The resulting :class:`EvalReport` (stage
    ``"semantic"``) is stored on ``ctx.semantic_eval_report`` and returned in
    ``CriticResult.report``.

    Args:
        host: Databricks workspace URL.
        token: Bearer token for the serving endpoint.
        endpoint_name: Foundation Model serving endpoint name.
        client: Databricks SQL client (must expose ``execute_query(sql)``).
        item_kind: ``"entity"`` or ``"relationship"``.
        item_uri: The ontology class or property URI under audit.
        item_definition: Full ontology dict for the item (label/comment,
            plus attributes for entities or domain/range for relationships).
        submitted_mapping: The mapping under audit (handler dict shape).
        source_model_slice: The Planner's slice for this item.
        stage1_metrics: Metrics from the deterministic evaluator, for
            context.
        documents: Optional pre-loaded domain documents — surfaced via
            ``get_documents_context``.
        on_step: Optional progress callback ``(msg, pct)`` for UI updates.
        max_iterations: Upper bound on tool-call iterations (default 6 —
            smaller than the Generators because auditing is bounded work).

    Returns:
        A :class:`CriticResult`. ``success`` is True iff the Critic
        terminated by submitting a verdict; in that case ``report`` holds
        the resulting :class:`EvalReport`. On failure (budget exhaustion,
        text-only output, transport error), ``error`` explains why.
    """
    iteration_limit = max_iterations if max_iterations is not None else MAX_ITERATIONS

    logger.info(
        "===== CRITIC START ===== endpoint=%s, kind=%s, uri=%s, max_iter=%d",
        endpoint_name,
        item_kind,
        item_uri,
        iteration_limit,
    )

    ctx = ToolContext(
        host=host.rstrip("/"),
        token=token,
        client=client,
        # The audit target is in the user prompt; metadata/ontology are not
        # needed by the Critic's tools.
        metadata={},
        ontology={},
        documents=list(documents or []),
    )

    result = CriticResult(success=False)

    user_prompt = _build_user_prompt(
        item_kind=item_kind,
        item_uri=item_uri,
        item_definition=item_definition or {},
        submitted_mapping=submitted_mapping or {},
        source_model_slice=source_model_slice or {},
        stage1_metrics=stage1_metrics or {},
    )
    messages: List[dict] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt},
    ]
    logger.info(
        "Critic conversation initialized: system=%d chars, user=%d chars",
        len(SYSTEM_PROMPT),
        len(user_prompt),
    )

    total_usage: Dict[str, int] = {"prompt_tokens": 0, "completion_tokens": 0}

    def _progress_pct(iteration_idx: int) -> int:
        ratio = (iteration_idx + 1) / max(iteration_limit, 1)
        return min(5 + int(ratio * 90), 95)

    def notify(msg: str, *, pct: Optional[int] = None) -> None:
        actual_pct = pct if pct is not None else 5
        logger.info("CRITIC STEP [%d%%] %s", actual_pct, msg)
        if on_step:
            on_step(msg, actual_pct)

    notify(f"Auditing {item_kind} {item_uri}…", pct=1)

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
            "----- Critic iteration %d/%d — %d messages, report=%s -----",
            current_iteration,
            iteration_limit,
            len(messages),
            "set" if ctx.semantic_eval_report is not None else "unset",
        )
        notify(
            f"Critic iteration {current_iteration}/{iteration_limit}…",
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
                "Critic iteration %d: HTTPError status=%s",
                current_iteration,
                status,
            )
            logger.debug(
                "Critic iteration %d: HTTPError body: %.500s",
                current_iteration,
                exc.response.text if exc.response is not None else "N/A",
            )
            if exc.response is not None and status in (400, 422):
                result.error = "LLM endpoint does not support function calling"
                result.iterations = current_iteration
                result.usage = total_usage
                logger.error(
                    "Critic: endpoint refused tools — cannot produce an evaluation"
                )
                return result
            result.error = f"LLM request failed: {exc}"
            result.iterations = current_iteration
            result.usage = total_usage
            logger.error(
                "Critic: LLM request failed at iteration %d: %s",
                current_iteration,
                exc,
            )
            return result
        except requests.exceptions.ReadTimeout:
            result.error = f"LLM request timed out after {LLM_TIMEOUT}s"
            result.iterations = current_iteration
            result.usage = total_usage
            logger.error("Critic: timeout at iteration %d", current_iteration)
            return result
        except requests.exceptions.RequestException as exc:
            result.error = f"LLM request failed: {exc}"
            result.iterations = current_iteration
            result.usage = total_usage
            logger.error(
                "Critic: request exception at iteration %d: %s",
                current_iteration,
                exc,
            )
            return result

        elapsed_ms = int((time.time() - t0) * 1000)
        logger.info(
            "Critic iteration %d: LLM responded in %dms",
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
            "Critic iteration %d: finish_reason=%s, tool_calls=%d, has_content=%s",
            current_iteration,
            finish_reason,
            len(tool_calls),
            has_content,
        )

        if not tool_calls:
            # The Critic must terminate via submit_evaluation, never via
            # free text. Text-only output is a failure.
            content = (message.get("content") or "")[:500]
            logger.warning(
                "Critic iteration %d: produced text without submitting evaluation — %d chars",
                current_iteration,
                len(message.get("content") or ""),
            )
            result.steps.append(
                CriticStep(
                    step_type="output",
                    content=content,
                    duration_ms=elapsed_ms,
                )
            )
            result.error = "critic produced text without submitting evaluation"
            result.iterations = current_iteration
            result.usage = total_usage
            notify(
                "Critic produced text without submitting evaluation.",
                pct=pct,
            )
            return result

        logger.info(
            "Critic iteration %d: processing %d tool call(s): [%s]",
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
                "Critic iteration %d: calling tool '%s' (%d/%d)",
                current_iteration,
                tool_name,
                tc_idx,
                len(tool_calls),
            )

            if tool_name == "submit_evaluation":
                notify(
                    f"Submitting evaluation for {item_uri}…", pct=pct
                )
            elif tool_name == "sample_table":
                fn = arguments.get("full_name", "?")
                notify(f"Sampling {fn}…", pct=pct)
            elif tool_name == "execute_sql":
                sql_preview = arguments.get("sql", "")[:80]
                notify(f"Running SQL: {sql_preview}…", pct=pct)
            elif tool_name == "get_documents_context":
                notify("Retrieving documents…", pct=pct)
            else:
                notify(f"Calling {tool_name}…", pct=pct)

            result.steps.append(
                CriticStep(
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
                "Critic iteration %d: tool '%s' returned %d chars in %dms",
                current_iteration,
                tool_name,
                len(tool_result),
                tool_ms,
            )

            result.steps.append(
                CriticStep(
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

            # Detect terminal success: submit_evaluation returned success=True
            # AND stamped an EvalReport onto the context. An invalid status
            # (the handler returns success=False) does NOT terminate the
            # loop — the agent continues so it can resubmit a valid verdict.
            if tool_name == "submit_evaluation":
                try:
                    parsed = json.loads(tool_result)
                except json.JSONDecodeError:
                    parsed = {}
                if (
                    parsed.get("success") is True
                    and ctx.semantic_eval_report is not None
                ):
                    terminal_success = True
                    logger.info(
                        "Critic iteration %d: submit_evaluation succeeded — terminating",
                        current_iteration,
                    )

        if terminal_success:
            result.success = True
            result.report = ctx.semantic_eval_report
            result.iterations = current_iteration
            result.usage = total_usage
            logger.info(
                "===== CRITIC COMPLETE ===== uri=%s, status=%s, bubble=%s, "
                "iterations=%d, prompt_tokens=%d, completion_tokens=%d",
                item_uri,
                result.report.status if result.report else "?",
                result.report.bubble_to_planner if result.report else "?",
                result.iterations,
                total_usage["prompt_tokens"],
                total_usage["completion_tokens"],
            )
            notify(f"Critic verdict submitted for {item_uri}.", pct=100)
            return result

    # Budget exhausted without a successful submit.
    result.iterations = iteration_limit
    result.usage = total_usage
    result.error = "critic exhausted iteration budget"
    logger.error("===== CRITIC FAILED ===== %s", result.error)
    notify(result.error, pct=95)
    return result
