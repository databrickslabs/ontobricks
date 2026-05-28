"""Terminal tool for the mapping-PGE Semantic Critic (Sprint 6).

The Critic audits ONE submitted mapping for semantic correctness after the
deterministic (stage-1) evaluator has already passed. It submits its verdict
through ``submit_evaluation`` — the terminal tool defined here — which
constructs an :class:`EvalReport` (stage="semantic") and stamps it onto
``ctx.semantic_eval_report``.

This module deliberately mirrors the shape of the other terminal tools
(``submit_source_model``, ``submit_entity_mapping``, …) — pure-Python handler
with a JSON-schema definition for OpenAI function calling, exported via
``EVALUATION_TOOL_DEFINITIONS`` / ``EVALUATION_TOOL_HANDLERS`` aggregates.
"""

import json
from typing import Callable, Dict, List, Optional

from back.core.logging import get_logger
from agents.tools.context import ToolContext

logger = get_logger(__name__)


# =====================================================
# OpenAI function-calling definition
# =====================================================

SUBMIT_EVALUATION_DEF: dict = {
    "type": "function",
    "function": {
        "name": "submit_evaluation",
        "description": (
            "Submit the final semantic evaluation. Terminal tool — call exactly once "
            "when you have a confident verdict. status MUST be 'PASS' or 'FAIL'. "
            "If failing, populate failures[] with at least one entry. "
            "Set bubble_to_planner=true ONLY when the wrong TABLE was chosen "
            "(not just a wrong column within the right table)."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "status": {"type": "string", "enum": ["PASS", "FAIL"]},
                "failures": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "check": {"type": "string"},
                            "expected": {"type": "string"},
                            "observed": {"type": "string"},
                            "hint": {"type": "string"},
                        },
                        "required": ["check", "expected", "observed", "hint"],
                    },
                    "description": "Empty when status is PASS.",
                },
                "bubble_to_planner": {"type": "boolean"},
                "reasoning": {
                    "type": "string",
                    "description": "One-paragraph summary of the audit reasoning.",
                },
            },
            "required": ["status"],
        },
    },
}


# =====================================================
# Handler
# =====================================================


def tool_submit_evaluation(
    ctx: ToolContext,
    *,
    status: str = "",
    failures: Optional[list] = None,
    bubble_to_planner: bool = False,
    reasoning: str = "",
    **_kwargs,
) -> str:
    """Construct an EvalReport from the critic's submission and store on ctx.

    Contract:
      * ``status`` MUST be one of ``"PASS"`` or ``"FAIL"`` — anything else is
        rejected as a JSON error so the agent loop can coach the LLM and
        continue (it does NOT terminate the loop).
      * On ``FAIL`` with an empty ``failures`` list, a generic
        ``semantic_audit`` failure is synthesised so the resulting report is
        coherent (status=FAIL <=> failures non-empty, matching
        :func:`evaluator.report.build_report` semantics).
      * ``bubble_to_planner=True`` is demoted to False when status is PASS —
        same invariant the deterministic evaluator's :func:`build_report`
        enforces (a passing evaluation should not escalate).
    """
    logger.info(
        "tool_submit_evaluation: status=%s, failures=%d, bubble=%s, reasoning=%d chars",
        status,
        len(failures or []),
        bubble_to_planner,
        len(reasoning or ""),
    )

    if status not in ("PASS", "FAIL"):
        logger.warning("tool_submit_evaluation: invalid status=%r", status)
        return json.dumps(
            {
                "success": False,
                "error": f"invalid status: {status!r} (must be PASS or FAIL)",
            }
        )

    # Lazy import — these contracts live in agent_mapping_pge and importing
    # them at module load time would create a cycle through
    # ``agents.tools.context``.
    from agents.agent_mapping_pge.contracts import EvalFailure, EvalReport

    eval_failures: List[EvalFailure] = []
    for f in failures or []:
        if not isinstance(f, dict):
            continue
        eval_failures.append(
            EvalFailure(
                kind="semantic",
                check=str(f.get("check") or ""),
                expected=str(f.get("expected") or ""),
                observed=str(f.get("observed") or ""),
                hint=str(f.get("hint") or ""),
            )
        )

    # status=PASS <=> failures empty. If the LLM submitted both, clamp the
    # failures list and warn — keeping a passing report internally coherent.
    if status == "PASS" and eval_failures:
        logger.warning(
            "tool_submit_evaluation: status=PASS with %d failures — clamping to []",
            len(eval_failures),
        )
        eval_failures = []

    # If status=FAIL but no failures, synthesise a generic one so the report
    # is coherent (status=FAIL <=> failures non-empty).
    if status == "FAIL" and not eval_failures:
        logger.debug(
            "tool_submit_evaluation: synthesising semantic_audit failure for "
            "FAIL with no failures[]"
        )
        eval_failures.append(
            EvalFailure(
                kind="semantic",
                check="semantic_audit",
                expected="PASS",
                observed="FAIL",
                hint=reasoning or "critic returned FAIL without specific failures",
            )
        )

    # If status=PASS but bubble flag is True, demote — matches
    # ``build_report``'s behaviour and the documented invariant: a passing
    # evaluation does not escalate to the Planner.
    if status == "PASS" and bubble_to_planner:
        logger.warning(
            "tool_submit_evaluation: bubble_to_planner=True with status=PASS — "
            "demoting to False"
        )
        bubble_to_planner = False

    metrics: Dict[str, str] = {"reasoning": reasoning} if reasoning else {}

    report = EvalReport(
        status=status,
        stage="semantic",
        metrics=metrics,
        failures=eval_failures,
        bubble_to_planner=bool(bubble_to_planner),
    )
    ctx.semantic_eval_report = report

    logger.info(
        "tool_submit_evaluation: stored EvalReport status=%s, failures=%d, bubble=%s",
        report.status,
        len(report.failures),
        report.bubble_to_planner,
    )

    return json.dumps(
        {
            "success": True,
            "status": status,
            "failures": len(eval_failures),
            "bubble_to_planner": report.bubble_to_planner,
        }
    )


# =====================================================
# Aggregates
# =====================================================

EVALUATION_TOOL_DEFINITIONS: List[dict] = [SUBMIT_EVALUATION_DEF]

EVALUATION_TOOL_HANDLERS: Dict[str, Callable] = {
    "submit_evaluation": tool_submit_evaluation,
}
