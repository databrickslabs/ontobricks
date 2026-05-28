"""Tests for ``agents.tools.evaluation`` — the Sprint 6 terminal tool.

These are direct unit tests for ``tool_submit_evaluation``. The agent-level
loop semantics are covered in ``test_critic.py``.
"""

import json

from agents.agent_mapping_pge.contracts import EvalReport
from agents.tools.context import ToolContext
from agents.tools.evaluation import (
    EVALUATION_TOOL_DEFINITIONS,
    EVALUATION_TOOL_HANDLERS,
    SUBMIT_EVALUATION_DEF,
    tool_submit_evaluation,
)


def _ctx() -> ToolContext:
    return ToolContext(host="https://x", token="t")


class TestSubmitEvaluation:
    """Direct handler tests — no LLM, no loop."""

    def test_valid_pass_stores_report(self):
        """status=PASS, no failures → report stored, success=True."""
        ctx = _ctx()
        payload = tool_submit_evaluation(
            ctx,
            status="PASS",
            failures=[],
            bubble_to_planner=False,
            reasoning="Sampled values match the Mother concept.",
        )
        body = json.loads(payload)
        assert body["success"] is True
        assert body["status"] == "PASS"
        assert body["failures"] == 0
        assert body["bubble_to_planner"] is False

        assert isinstance(ctx.semantic_eval_report, EvalReport)
        rep = ctx.semantic_eval_report
        assert rep.status == "PASS"
        assert rep.stage == "semantic"
        assert rep.failures == []
        assert rep.bubble_to_planner is False
        # reasoning is preserved in metrics
        assert rep.metrics.get("reasoning") == "Sampled values match the Mother concept."

    def test_valid_fail_with_failures(self):
        """status=FAIL with failures[] → report stored with semantic-kind failures."""
        ctx = _ctx()
        payload = tool_submit_evaluation(
            ctx,
            status="FAIL",
            failures=[
                {
                    "check": "column_semantics",
                    "expected": "delivery date",
                    "observed": "booking date",
                    "hint": "Use `delivery_dttm` instead of `appointment_date`.",
                }
            ],
            bubble_to_planner=False,
            reasoning="Wrong column within the right table.",
        )
        body = json.loads(payload)
        assert body["success"] is True
        assert body["status"] == "FAIL"
        assert body["failures"] == 1
        assert body["bubble_to_planner"] is False

        rep = ctx.semantic_eval_report
        assert rep is not None
        assert rep.status == "FAIL"
        assert len(rep.failures) == 1
        f = rep.failures[0]
        assert f.kind == "semantic"
        assert f.check == "column_semantics"
        assert f.expected == "delivery date"
        assert f.observed == "booking date"
        assert "delivery_dttm" in f.hint

    def test_invalid_status_rejected_no_report_stored(self):
        """status not in {PASS,FAIL} → handler returns success=False, no
        report is stamped on ctx."""
        ctx = _ctx()
        payload = tool_submit_evaluation(
            ctx,
            status="UNKNOWN",
            failures=[],
        )
        body = json.loads(payload)
        assert body["success"] is False
        assert "invalid status" in body["error"]
        assert ctx.semantic_eval_report is None


class TestExports:
    """Sanity-check the aggregates the Critic agent imports."""

    def test_definitions_include_submit_evaluation(self):
        names = [
            d["function"]["name"] for d in EVALUATION_TOOL_DEFINITIONS
        ]
        assert "submit_evaluation" in names
        assert SUBMIT_EVALUATION_DEF in EVALUATION_TOOL_DEFINITIONS

    def test_handlers_match_definitions(self):
        assert set(EVALUATION_TOOL_HANDLERS.keys()) == {"submit_evaluation"}
        assert EVALUATION_TOOL_HANDLERS["submit_evaluation"] is tool_submit_evaluation
