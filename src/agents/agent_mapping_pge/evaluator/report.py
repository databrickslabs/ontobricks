"""Small helpers for assembling :class:`EvalReport` objects.

The dataclasses themselves live in
:mod:`agents.agent_mapping_pge.contracts`; this module just centralises the
"compose a report from a list of failures" boilerplate so the deterministic
and (future) semantic evaluators stay short.
"""

from typing import Any, Dict, List

from back.core.logging import get_logger
from agents.agent_mapping_pge.contracts import EvalFailure, EvalReport

logger = get_logger(__name__)


def build_report(
    *,
    stage: str,
    metrics: Dict[str, Any],
    failures: List[EvalFailure],
    bubble_to_planner: bool,
) -> EvalReport:
    """Assemble an :class:`EvalReport`; status is derived from ``failures``."""
    status = "PASS" if not failures else "FAIL"
    if bubble_to_planner and status == "PASS":
        logger.warning(
            "build_report: bubble_to_planner=True but no failures → demoted "
            "to False; check caller logic"
        )
    return EvalReport(
        status=status,
        stage=stage,
        metrics=dict(metrics),
        failures=list(failures),
        bubble_to_planner=bool(bubble_to_planner) and status == "FAIL",
    )
