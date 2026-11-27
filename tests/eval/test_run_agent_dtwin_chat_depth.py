"""Judge contracts for the Depth-setting checks in ``run_agent_dtwin_chat.py``.

Covers the ``depth_capped`` and ``discloses_partial_result`` constraints added
for issue #252. Each judge must reject the bad behaviour it guards against and
accept the clean one; otherwise the regression rows in
``datasets/agent_dtwin_chat/regression.jsonl`` pass vacuously.
"""

from __future__ import annotations

import json

import run_agent_dtwin_chat as runner

CAP_1 = {"constraints": [{"kind": "depth_capped", "value": 1}]}
DISCLOSE = {"constraints": [{"kind": "discloses_partial_result", "value": True}]}


def _describe(requested: int, effective: int, extra: str = "") -> dict:
    return {
        "tool": "describe_entity",
        "args": {"depth": requested},
        "result": f"Found 1 matching entity (3 triples across 2 entities, depth={effective}){extra}",
    }


def test_depth_capped_rejects_traversal_deeper_than_user_setting():
    assert runner.judge_depth_capped(CAP_1, [_describe(3, 3)]) == 0.0


def test_depth_capped_accepts_traversal_within_user_setting():
    assert runner.judge_depth_capped(CAP_1, [_describe(3, 1)]) == 1.0


def test_depth_capped_is_noop_without_constraint():
    assert runner.judge_depth_capped({"constraints": []}, [_describe(3, 3)]) == 1.0


def test_discloses_rejects_confident_answer_on_truncated_result():
    trace = [_describe(1, 1, "\nWARNING: (Showing 1 of 9 triples -- TRUNCATED.)")]
    assert runner.judge_discloses_partial_result(DISCLOSE, "The total is 1,065.", trace) == 0.0


def test_discloses_accepts_hedged_answer_on_truncated_result():
    trace = [_describe(1, 1, "\nWARNING: TRUNCATED")]
    assert runner.judge_discloses_partial_result(DISCLOSE, "The total may be incomplete.", trace) == 1.0


def test_discloses_rejects_confident_answer_when_depth_was_capped():
    # LLM asked for depth 3, user setting capped it to 1: the answer is partial.
    assert runner.judge_discloses_partial_result(DISCLOSE, "Total is 3.", [_describe(3, 1)]) == 0.0


def test_discloses_accepts_confident_answer_when_nothing_was_limited():
    assert runner.judge_discloses_partial_result(DISCLOSE, "The total is 3.", [_describe(1, 1)]) == 1.0


def test_stub_run_regression_rows_pass_their_own_judges():
    with open(runner.REGRESSION_PATH) as f:
        rows = [json.loads(line) for line in f if line.strip()]
    assert len(rows) >= 3
    for row in rows:
        tools, reply, trace = runner._stub_run(row)
        scores = runner.score_example(row, tools, reply, 0.0, trace)
        assert scores["weighted"] == 1.0, row["id"]
