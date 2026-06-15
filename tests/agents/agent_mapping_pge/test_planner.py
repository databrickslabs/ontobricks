"""Tests for the mapping-PGE Planner agent (Sprint 3).

The Planner is a tool-calling ReAct loop terminated by ``submit_source_model``.
These tests exercise the loop's control flow with a *fake LLM* — a stub that
replaces ``call_serving_endpoint`` at module level and returns canned tool-
call responses on a per-call basis.

No real HTTP, no real Databricks, no MLflow tracing. The tracing decorator
is a no-op when MLflow isn't configured (see ``_TRACING_READY`` in
``agents.tracing``), so it runs cleanly here.

What we DO exercise:
* The four termination conditions
  — terminal submit_source_model with success=True breaks the loop
  — text content with no tool calls is treated as failure
  — iteration budget exhaustion is treated as failure
  — submit returning success=False is NOT terminal (allows retry)
* Step recording: every tool call produces both tool_call and tool_result
  steps in the right order.
* Iteration counter accuracy.

What we do NOT exercise (covered elsewhere or out of scope):
* The actual content of the SourceModel — that's Sprint 1's contracts tests.
* The four planner tool handlers — that's Sprint 2's test_planner_tools.py.
* MLflow tracing semantics — the decorator is wrapped in an ``if`` guard.
"""

import json
from typing import Any, Callable, Dict, List, Optional

import pytest

from agents.agent_mapping_pge import planner as planner_mod
from agents.agent_mapping_pge.contracts import SourceModel
from agents.agent_mapping_pge.planner import (
    PlannerResult,
    PlannerStep,
    run_planner,
)


# =====================================================
# Fake LLM scaffolding
# =====================================================


def _make_tool_call(name: str, arguments: dict, *, tc_id: str = "tc1") -> dict:
    """Build an OpenAI-style tool_calls entry."""
    return {
        "id": tc_id,
        "type": "function",
        "function": {"name": name, "arguments": json.dumps(arguments)},
    }


def _llm_response(
    *,
    tool_calls: Optional[List[dict]] = None,
    content: Optional[str] = None,
    finish_reason: str = "tool_calls",
    usage: Optional[Dict[str, int]] = None,
) -> dict:
    """Build a minimal OpenAI-style chat-completions response."""
    message: Dict[str, Any] = {"role": "assistant"}
    if tool_calls:
        message["tool_calls"] = tool_calls
    if content is not None:
        message["content"] = content
    return {
        "choices": [{"finish_reason": finish_reason, "message": message}],
        "usage": usage or {"prompt_tokens": 10, "completion_tokens": 5},
    }


class FakeLLM:
    """A stub for ``call_serving_endpoint`` that returns canned responses.

    The list is consumed front-to-back, one response per call. If a test
    exhausts the list, the stub raises — that's almost always a test bug
    (the loop iterated more times than the test author expected).
    """

    def __init__(self, responses: List[dict]):
        self.responses = list(responses)
        self.calls = 0

    def __call__(self, *args, **kwargs) -> dict:
        self.calls += 1
        if not self.responses:
            raise AssertionError(
                f"FakeLLM: ran out of canned responses on call #{self.calls}"
            )
        return self.responses.pop(0)


class CyclingFakeLLM:
    """Like FakeLLM but cycles through a fixed list forever.

    Used for the iteration-budget-exhaustion test, where the LLM is supposed
    to be stuck in an infinite loop until the engine cuts it off.
    """

    def __init__(self, responses: List[dict]):
        self.responses = list(responses)
        self.calls = 0

    def __call__(self, *args, **kwargs) -> dict:
        resp = self.responses[self.calls % len(self.responses)]
        self.calls += 1
        return resp


@pytest.fixture
def no_sleep(monkeypatch):
    """Neutralise the 3-second inter-iteration delay so tests run fast."""
    monkeypatch.setattr(planner_mod.time, "sleep", lambda *_a, **_k: None)


def _patch_llm(monkeypatch, fake: Callable[..., dict]) -> None:
    """Replace the planner's reference to ``call_serving_endpoint``."""
    monkeypatch.setattr(planner_mod, "call_serving_endpoint", fake)


# =====================================================
# Fixtures: a minimal valid SourceModel payload
# =====================================================


def _valid_source_model_dict() -> Dict[str, Any]:
    """Same shape as test_planner_tools._valid_source_model_dict — kept
    independent here so the two test files don't coupling-leak."""
    return {
        "table_roles": [
            {
                "table": "cat.sch.mothers",
                "ontology_class_candidates": [
                    {
                        "uri": "http://ex.org/maternity#Mother",
                        "confidence": 0.9,
                        "reason": "row per NHS",
                    }
                ],
            }
        ],
        "canonical_ids": [
            {
                "ontology_class": "http://ex.org/maternity#Mother",
                "canonical_column_per_table": {"cat.sch.mothers": "nhs_number"},
                "format_note": "",
            }
        ],
        "join_keys": [],
        "mapping_plan": {
            "entity_order": ["http://ex.org/maternity#Mother"],
            "relationship_order": [],
            "skip": [],
        },
    }


def _minimal_metadata() -> dict:
    return {
        "tables": [
            {
                "name": "mothers",
                "full_name": "cat.sch.mothers",
                "columns": [
                    {"name": "nhs_number", "type": "STRING"},
                    {"name": "dob", "type": "DATE"},
                ],
            }
        ]
    }


def _minimal_ontology() -> dict:
    return {
        "entities": [{"name": "Mother", "uri": "http://ex.org/maternity#Mother"}],
        "relationships": [],
    }


# =====================================================
# 1. Single-shot submit terminates immediately
# =====================================================


def test_planner_terminates_on_submit_source_model(monkeypatch, no_sleep):
    """First LLM turn calls submit_source_model with a valid model — Planner
    must return success=True with iterations=1 and source_model populated.
    """
    sm = _valid_source_model_dict()
    fake = FakeLLM(
        [
            _llm_response(
                tool_calls=[_make_tool_call("submit_source_model", {"model": sm})]
            )
        ]
    )
    _patch_llm(monkeypatch, fake)

    result = run_planner(
        host="https://x",
        token="t",
        endpoint_name="ep",
        client=None,  # not used in this scenario
        metadata=_minimal_metadata(),
        ontology=_minimal_ontology(),
    )

    assert isinstance(result, PlannerResult)
    assert result.success is True
    assert result.iterations == 1
    assert isinstance(result.source_model, SourceModel)
    assert len(result.source_model.table_roles) == 1
    assert result.error == ""
    assert result.usage["prompt_tokens"] >= 0
    # Exactly one tool_call + one tool_result step.
    step_kinds = [s.step_type for s in result.steps]
    assert step_kinds == ["tool_call", "tool_result"]
    assert result.steps[0].tool_name == "submit_source_model"
    assert result.steps[1].tool_name == "submit_source_model"


# =====================================================
# 2. Multi-step ReAct trajectory followed by submit
# =====================================================


def test_planner_multi_step_then_submit(monkeypatch, no_sleep):
    """get_metadata → get_ontology → sample_table → submit_source_model."""
    sm = _valid_source_model_dict()
    fake = FakeLLM(
        [
            _llm_response(
                tool_calls=[_make_tool_call("get_metadata", {}, tc_id="a")]
            ),
            _llm_response(
                tool_calls=[_make_tool_call("get_ontology", {}, tc_id="b")]
            ),
            _llm_response(
                tool_calls=[
                    _make_tool_call(
                        "sample_table",
                        {"full_name": "cat.sch.mothers"},
                        tc_id="c",
                    )
                ]
            ),
            _llm_response(
                tool_calls=[
                    _make_tool_call(
                        "submit_source_model", {"model": sm}, tc_id="d"
                    )
                ]
            ),
        ]
    )
    _patch_llm(monkeypatch, fake)

    # sample_table needs a client — return one row.
    class FakeClient:
        def execute_query(self, sql):
            return [{"nhs_number": "1234567890", "dob": "1990-01-01"}]

    result = run_planner(
        host="https://x",
        token="t",
        endpoint_name="ep",
        client=FakeClient(),
        metadata=_minimal_metadata(),
        ontology=_minimal_ontology(),
    )

    assert result.success is True
    assert result.iterations == 4
    assert isinstance(result.source_model, SourceModel)

    # Every iteration produces both a tool_call and a tool_result step.
    assert len(result.steps) == 8
    expected_tool_names = [
        "get_metadata",
        "get_metadata",
        "get_ontology",
        "get_ontology",
        "sample_table",
        "sample_table",
        "submit_source_model",
        "submit_source_model",
    ]
    assert [s.tool_name for s in result.steps] == expected_tool_names
    assert [s.step_type for s in result.steps] == [
        "tool_call",
        "tool_result",
        "tool_call",
        "tool_result",
        "tool_call",
        "tool_result",
        "tool_call",
        "tool_result",
    ]


# =====================================================
# 3. submit returning success=False does NOT terminate
# =====================================================


def test_planner_invalid_source_model_does_not_terminate(monkeypatch, no_sleep):
    """First submit is malformed (missing 'table' on a table_role) — the
    tool returns success=False and the Planner keeps going. Second submit
    is valid and terminates the loop.
    """
    bad = _valid_source_model_dict()
    del bad["table_roles"][0]["table"]  # break it

    good = _valid_source_model_dict()
    # Make the good one visibly different so we can prove which one stuck.
    good["mapping_plan"]["entity_order"] = ["http://ex.org/maternity#Mother"]

    fake = FakeLLM(
        [
            _llm_response(
                tool_calls=[
                    _make_tool_call("submit_source_model", {"model": bad}, tc_id="x")
                ]
            ),
            _llm_response(
                tool_calls=[
                    _make_tool_call(
                        "submit_source_model", {"model": good}, tc_id="y"
                    )
                ]
            ),
        ]
    )
    _patch_llm(monkeypatch, fake)

    result = run_planner(
        host="https://x",
        token="t",
        endpoint_name="ep",
        client=None,
        metadata=_minimal_metadata(),
        ontology=_minimal_ontology(),
    )

    assert result.success is True
    assert result.iterations == 2
    assert isinstance(result.source_model, SourceModel)
    # The valid one is what landed on ctx — pull a field from it.
    assert result.source_model.mapping_plan.entity_order == [
        "http://ex.org/maternity#Mother"
    ]
    # Both submit attempts were recorded; the first tool_result must signal
    # failure so the orchestrator can attribute the retry.
    first_submit_result = result.steps[1]
    assert first_submit_result.step_type == "tool_result"
    assert first_submit_result.tool_name == "submit_source_model"
    payload = json.loads(first_submit_result.content)
    assert payload["success"] is False


# =====================================================
# 4. Free-text output without a terminal tool call → failure
# =====================================================


def test_planner_text_without_terminal_fails(monkeypatch, no_sleep):
    """The Planner must terminate via submit_source_model. A plain-text
    response is treated as failure.
    """
    fake = FakeLLM(
        [_llm_response(content="I think we are done.", finish_reason="stop")]
    )
    _patch_llm(monkeypatch, fake)

    result = run_planner(
        host="https://x",
        token="t",
        endpoint_name="ep",
        client=None,
        metadata=_minimal_metadata(),
        ontology=_minimal_ontology(),
    )

    assert result.success is False
    assert result.iterations == 1
    assert result.source_model is None
    assert "without submitting source model" in result.error
    # The text was recorded as an output step for debuggability.
    assert any(s.step_type == "output" for s in result.steps)


# =====================================================
# 5. Iteration budget exhaustion → failure
# =====================================================


def test_planner_exhausts_iteration_budget(monkeypatch, no_sleep):
    """Fake LLM keeps calling get_metadata forever. With max_iterations=3
    the Planner must give up cleanly and report budget exhaustion.
    """
    fake = CyclingFakeLLM(
        [
            _llm_response(
                tool_calls=[_make_tool_call("get_metadata", {}, tc_id="a")]
            )
        ]
    )
    _patch_llm(monkeypatch, fake)

    result = run_planner(
        host="https://x",
        token="t",
        endpoint_name="ep",
        client=None,
        metadata=_minimal_metadata(),
        ontology=_minimal_ontology(),
        max_iterations=3,
    )

    assert result.success is False
    assert result.iterations == 3
    assert result.source_model is None
    assert "iteration budget" in result.error
    # Three iterations × (tool_call + tool_result) = 6 steps.
    assert len(result.steps) == 6


# =====================================================
# 6. Step recording invariants
# =====================================================


def test_planner_records_steps(monkeypatch, no_sleep):
    """For each tool-calling iteration, the Planner must record exactly one
    ``tool_call`` step (with non-empty arguments-as-content) and one
    ``tool_result`` step (with non-empty content) — in that order, paired by
    ``tool_name``.
    """
    sm = _valid_source_model_dict()
    fake = FakeLLM(
        [
            _llm_response(
                tool_calls=[_make_tool_call("get_metadata", {}, tc_id="a")]
            ),
            _llm_response(
                tool_calls=[
                    _make_tool_call("submit_source_model", {"model": sm}, tc_id="b")
                ]
            ),
        ]
    )
    _patch_llm(monkeypatch, fake)

    result = run_planner(
        host="https://x",
        token="t",
        endpoint_name="ep",
        client=None,
        metadata=_minimal_metadata(),
        ontology=_minimal_ontology(),
    )

    assert result.success is True
    # Verify the pairing: every odd-indexed step (tool_call) is immediately
    # followed by an even-indexed step (tool_result) with the same tool_name.
    assert len(result.steps) % 2 == 0
    for i in range(0, len(result.steps), 2):
        call_step = result.steps[i]
        result_step = result.steps[i + 1]
        assert call_step.step_type == "tool_call"
        assert result_step.step_type == "tool_result"
        assert call_step.tool_name == result_step.tool_name
        assert call_step.content != ""
        assert result_step.content != ""
        # PlannerStep is the right type.
        assert isinstance(call_step, PlannerStep)
        assert isinstance(result_step, PlannerStep)


# =====================================================
# Prompt contract — canonical-key normalization guidance
# =====================================================


class TestCanonicalKeyNormalizationPrompt:
    """Pin the load-bearing canonical-key guidance in the system prompt.

    Issue 2 root cause: the Planner left cross-trust keys disjoint (0%
    overlap rationalized as "trust-scoped"), and when it did normalize it
    copied a non-anchored regex that returns a leading-dash key. These
    assertions keep the corrective guidance from silently regressing.
    """

    def test_offers_expression_overlap_verification_tool(self):
        assert "normalized_value_overlap" in planner_mod.SYSTEM_PROMPT

    def test_zero_overlap_is_not_a_terminal_state(self):
        prompt = planner_mod.SYSTEM_PROMPT
        # The prompt must steer the model AWAY from accepting disjoint keys.
        # (Domain-neutral wording: "source-scoped", not "trust-scoped".)
        assert "source-scoped" in prompt  # names the trap explicitly
        assert "100%" in prompt and "dangle" in prompt

    def test_regex_example_is_anchored(self):
        prompt = planner_mod.SYSTEM_PROMPT
        # The correct, anchored pattern must be present (leading char-class so a
        # preceding dash is not captured) — asserted on the structure, not on a
        # domain-specific token, so the example stays usecase-agnostic.
        assert "[a-f0-9][a-f0-9-]+-ord-" in prompt
        # ...and it must be flagged as the RIGHT one (the WRONG/RIGHT contrast
        # teaches the leading-dash pitfall).
        assert "✓ RIGHT" in prompt and "✗ WRONG" in prompt

    def test_derived_key_extracts_core_before_suffix(self):
        prompt = planner_mod.SYSTEM_PROMPT
        # Derived child keys must extract the shared core, then append suffix —
        # not concat onto the raw prefixed local id.
        assert "regexp_extract" in prompt
        assert "-line" in prompt  # the worked (domain-neutral) child-key example
