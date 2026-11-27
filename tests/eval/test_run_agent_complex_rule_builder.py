"""Offline tests for the agent_complex_rule_builder eval harness.

No LLM is called: the dataset contract and the judges are deterministic, and
the live wiring is checked with the serving call patched out.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import run_agent_complex_rule_builder as runner
from judges import complex_rule_builder as judges

P = "PREFIX : <http://ex.org/onto#>\n"
LINK_REMOVAL = P + 'DELETE { ?c :hasMeter ?m } WHERE { ?c :status "terminated" ; :hasMeter ?m . }'
REMOVAL_EXPECTED = {"kind": "removal", "draft": True, "query_contains": ["DELETE", "hasMeter"]}
AMBIGUOUS_EXPECTED = {"options_any": [["link"], ["itself"], ["membership"]], "ready": False}


def _turn(query="", ready=False, options=None, reply=""):
    draft = {"name": "R", "description": "", "query": query} if query else None
    opts = {"question": "?", "options": options, "multi": False} if options else None
    return {"reply": reply, "draft": draft, "ready": ready, "options": opts}


def test_dataset_contract_holds():
    rows = runner.load_rows()
    assert len(rows) >= runner.MIN_ROWS
    assert runner.contract_errors(rows) == []


def test_dataset_covers_both_kinds_and_clarification():
    rows = runner.load_rows()
    kinds = {r["expected"].get("kind") for r in rows}
    assert {"construct", "removal"} <= kinds
    assert any(r["expected"].get("options_any") for r in rows)


def test_a_correct_removal_scores_one():
    scores = judges.score_row(_turn(LINK_REMOVAL, ready=True), REMOVAL_EXPECTED)
    assert scores["weighted"] == 1.0


def test_a_construct_for_a_removal_row_is_a_wrong_kind():
    # The kind is read from the query: a CONSTRUCT is not a removal, whatever the row says.
    construct = P + "CONSTRUCT { ?c a :Ended } WHERE { ?c :status \"terminated\" }"
    scores = judges.score_row(_turn(construct), REMOVAL_EXPECTED)
    assert scores["kind_correct"] == 0.0


def test_a_whole_graph_delete_fails_safety():
    turn = _turn("DELETE WHERE { ?s ?p ?o }")
    assert judges.judge_safety(turn, {}) == 0.0


def test_an_unready_whole_predicate_delete_fails_safety():
    turn = _turn(P + "DELETE WHERE { ?s :status ?o }", ready=False)
    assert judges.judge_safety(turn, {}) == 0.0


def test_an_insert_fails_safety():
    turn = _turn(P + 'INSERT { ?c :status "closed" } WHERE { ?c :status "terminated" }')
    assert judges.judge_safety(turn, {}) == 0.0


def test_a_ready_rule_that_does_not_compile_fails_safety():
    turn = _turn(P + "DELETE WHERE { ?s :status ?o }", ready=True)
    assert judges.judge_safety(turn, {}) == 0.0


def test_clarification_needs_the_choices_and_no_ready_rule():
    asked = _turn(options=["Only the link", "The meter itself", "Only its class membership"])
    assert judges.judge_clarifies(asked, AMBIGUOUS_EXPECTED) == 1.0
    guessed = _turn(LINK_REMOVAL, ready=True)
    assert judges.judge_clarifies(guessed, AMBIGUOUS_EXPECTED) == 0.0


def test_two_questions_in_one_turn_fail_clarification():
    # The editor keeps only the last options: the first question is lost.
    turn = _turn(options=["Only the link", "The meter itself", "Only its class membership"])
    turn["tools"] = ["propose_options", "propose_options"]
    assert judges.judge_clarifies(turn, AMBIGUOUS_EXPECTED) == 0.0


def test_options_without_question_text_fail_clarification():
    turn = _turn(options=["Only the link", "The meter itself", "Only its class membership"])
    turn["options"]["question"] = ""
    assert judges.judge_clarifies(turn, AMBIGUOUS_EXPECTED) == 0.0


def test_live_turn_passes_the_row_to_the_agent_and_its_query_is_a_removal():
    row = runner.load_rows()[0]
    result = SimpleNamespace(success=True, error="", reply="ok", ready=True, options=None,
                             usage={}, draft={"query": LINK_REMOVAL},
                             steps=[SimpleNamespace(step_type="tool_call", tool_name="update_draft")])
    with patch("agents.agent_complex_rule_builder.run_agent", return_value=result) as agent:
        out = runner._agent_turn(row, "https://h", "t", "e", "ai_gateway")
    kwargs = agent.call_args.kwargs
    assert kwargs["user_message"] == row["input"]["user_message"]
    assert kwargs["classes"] == judges.ONTOLOGIES[row["input"]["ontology"]]["classes"]
    assert "kind" not in out["draft"]
    assert judges._kind(out) == "removal"
    assert out["tools"] == ["update_draft"]


def test_an_unreachable_endpoint_is_not_scored_or_logged(tmp_path):
    failure = {"success": False, "error": "LLM request failed: endpoint not found (HTTP 404).",
               "reply": "", "draft": None, "ready": False, "options": None, "usage": {}}
    args = SimpleNamespace(host="h", token="t", endpoint="e", endpoint_kind="", run_name="t",
                           mlflow_experiment="", mlflow_tracking_uri="", dataset="regression")
    with patch.object(runner, "_agent_turn", return_value=failure), \
            patch.object(runner, "_log_mlflow") as log:
        with pytest.raises(SystemExit, match="LLM unreachable"):
            runner.run_live(args)
    log.assert_not_called()


def test_thresholds_declare_the_agent():
    assert 0 < runner.threshold() <= 1
    assert json.dumps(judges.WEIGHTS)  # weights are plain data
    assert abs(sum(judges.WEIGHTS.values()) - 1.0) < 1e-9
