"""Tests for the mapping-PGE Semantic Critic agent (Sprint 6).

Mirrors the structure of ``test_relationship_generator.py``. The Critic is a
narrow tool-calling ReAct loop terminated by ``submit_evaluation``. These
tests exercise the loop's control flow with a *fake LLM* — a stub that
replaces ``call_serving_endpoint`` at module level and returns canned
responses on a per-call basis.

No real HTTP, no real Databricks, no MLflow tracing.

What we DO exercise:
* PASS verdict terminates immediately.
* FAIL with bubble_to_planner=False (column-level).
* FAIL with bubble_to_planner=True (table-level) — the bubble flag survives.
* PASS+bubble is demoted (matches build_report behaviour).
* FAIL with empty failures[] synthesises a generic semantic failure.
* Invalid status does NOT terminate — loop continues, accepts a valid retry.
* Text-only response → failure with "without submitting evaluation".
* Iteration-budget exhaustion → failure with "iteration budget".
* User prompt surfaces structural-stage metrics.
* User prompt for relationships includes domain/range sections.
"""

import json
from typing import Any, Callable, Dict, List, Optional

import pytest

from agents.agent_mapping_pge.evaluator import critic as critic_mod
from agents.agent_mapping_pge.evaluator.critic import (
    CriticResult,
    CriticStep,
    run_critic,
)


# =====================================================
# Fake LLM scaffolding
# =====================================================


_ENTITY_URI = "http://ex.org/maternity#Mother"
_REL_URI = "http://ex.org/maternity#motherOf"


def _make_tool_call(name: str, arguments: dict, *, tc_id: str = "tc1") -> dict:
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
    def __init__(self, responses: List[dict]):
        self.responses = list(responses)
        self.calls = 0
        self.last_messages: Optional[List[dict]] = None
        self.first_messages: Optional[List[dict]] = None

    def __call__(self, *args, **kwargs) -> dict:
        self.calls += 1
        msgs: Optional[List[dict]] = None
        if len(args) >= 4 and isinstance(args[3], list):
            msgs = args[3]
        elif "messages" in kwargs:
            msgs = kwargs["messages"]
        if msgs is not None:
            snapshot = [dict(m) for m in msgs]
            if self.first_messages is None:
                self.first_messages = snapshot
            self.last_messages = snapshot

        if not self.responses:
            raise AssertionError(
                f"FakeLLM: ran out of canned responses on call #{self.calls}"
            )
        return self.responses.pop(0)


class CyclingFakeLLM:
    """Like FakeLLM but cycles through a fixed list forever."""

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
    monkeypatch.setattr(critic_mod.time, "sleep", lambda *_a, **_k: None)


def _patch_llm(monkeypatch, fake: Callable[..., dict]) -> None:
    monkeypatch.setattr(critic_mod, "call_serving_endpoint", fake)


# =====================================================
# Fixtures
# =====================================================


def _entity_definition() -> dict:
    return {
        "uri": _ENTITY_URI,
        "label": "Mother",
        "name": "Mother",
        "comment": "A pregnant woman in the maternity dataset.",
        "attributes": [
            {"name": "nhsNumber", "type": "string"},
            {"name": "dateOfBirth", "type": "date"},
        ],
    }


def _relationship_definition() -> dict:
    return {
        "uri": _REL_URI,
        "label": "motherOf",
        "name": "motherOf",
        "comment": "Links a Mother to each of her babies.",
        "domain": _ENTITY_URI,
        "range": "http://ex.org/maternity#Baby",
    }


def _entity_submitted_mapping() -> dict:
    return {
        "ontology_class": _ENTITY_URI,
        "class_name": "Mother",
        "sql_query": "SELECT nhs_number AS ID, nhs_number AS Label FROM cat.sch.mothers WHERE nhs_number IS NOT NULL",
        "id_column": "nhs_number",
        "label_column": "nhs_number",
        "attribute_mappings": {"nhsNumber": "nhs_number"},
        "unmapped_attributes": [
            {"name": "dateOfBirth", "reason": "column absent from this table"}
        ],
    }


def _relationship_submitted_mapping() -> dict:
    return {
        "property": _REL_URI,
        "property_name": "motherOf",
        "sql_query": "SELECT mother_nhs_number AS source_id, baby_id AS target_id FROM cat.sch.babies",
        "source_id_column": "nhs_number",
        "target_id_column": "baby_id",
        "domain": _ENTITY_URI,
        "range_class": "http://ex.org/maternity#Baby",
    }


def _source_model_slice() -> dict:
    return {
        "candidate_tables": [
            {
                "table": "cat.sch.mothers",
                "confidence": 0.9,
                "reason": "row per mother, nhs_number as PK",
            }
        ],
        "canonical_id": {
            "canonical_column_per_table": {"cat.sch.mothers": "nhs_number"},
            "format_note": "10-digit NHS number",
        },
    }


def _stage1_metrics(**overrides) -> dict:
    base = {
        "row_count": 100,
        "distinct_ids": 100,
        "null_ids": 0,
    }
    base.update(overrides)
    return base


def _valid_pass_submit() -> dict:
    return {
        "status": "PASS",
        "failures": [],
        "bubble_to_planner": False,
        "reasoning": "Sampled values match the Mother concept; column semantics OK.",
    }


def _valid_fail_column_submit() -> dict:
    return {
        "status": "FAIL",
        "failures": [
            {
                "check": "column_semantics",
                "expected": "delivery date",
                "observed": "appointment_date is a booking date",
                "hint": "Use `delivery_dttm` instead of `appointment_date`.",
            }
        ],
        "bubble_to_planner": False,
        "reasoning": "Wrong column within the right table.",
    }


def _valid_fail_table_submit() -> dict:
    return {
        "status": "FAIL",
        "failures": [
            {
                "check": "table_selection",
                "expected": "labour_delivery",
                "observed": "antenatal_visits",
                "hint": "Switch to `labour_delivery` table for the Delivery class.",
            }
        ],
        "bubble_to_planner": True,
        "reasoning": "Wrong table chosen — bubble to Planner.",
    }


def _run_entity_critic(
    fake: Callable[..., dict],
    *,
    max_iterations: int = 6,
    item_kind: str = "entity",
    item_uri: str = _ENTITY_URI,
    item_definition: Optional[dict] = None,
    submitted_mapping: Optional[dict] = None,
    stage1_metrics: Optional[dict] = None,
    client: Any = None,
) -> CriticResult:
    return run_critic(
        host="https://x",
        token="t",
        endpoint_name="ep",
        client=client,
        item_kind=item_kind,
        item_uri=item_uri,
        item_definition=item_definition
        if item_definition is not None
        else _entity_definition(),
        submitted_mapping=submitted_mapping
        if submitted_mapping is not None
        else _entity_submitted_mapping(),
        source_model_slice=_source_model_slice(),
        stage1_metrics=stage1_metrics
        if stage1_metrics is not None
        else _stage1_metrics(),
        max_iterations=max_iterations,
    )


# =====================================================
# 1. PASS verdict terminates immediately
# =====================================================


def test_pass_verdict(monkeypatch, no_sleep):
    """First LLM turn submits PASS → success=True, status=PASS, iterations=1."""
    fake = FakeLLM(
        [
            _llm_response(
                tool_calls=[
                    _make_tool_call("submit_evaluation", _valid_pass_submit())
                ]
            )
        ]
    )
    _patch_llm(monkeypatch, fake)

    result = _run_entity_critic(fake)

    assert isinstance(result, CriticResult)
    assert result.success is True
    assert result.iterations == 1
    assert result.report is not None
    assert result.report.status == "PASS"
    assert result.report.stage == "semantic"
    assert result.report.failures == []
    assert result.report.bubble_to_planner is False
    assert result.error == ""
    # Step recording: one tool_call + one tool_result.
    assert [s.step_type for s in result.steps] == ["tool_call", "tool_result"]
    assert result.steps[0].tool_name == "submit_evaluation"


# =====================================================
# 2. FAIL with bubble_to_planner=False (column-level)
# =====================================================


def test_fail_column_level(monkeypatch, no_sleep):
    """status=FAIL, bubble_to_planner=False → column-level failure preserved."""
    fake = FakeLLM(
        [
            _llm_response(
                tool_calls=[
                    _make_tool_call("submit_evaluation", _valid_fail_column_submit())
                ]
            )
        ]
    )
    _patch_llm(monkeypatch, fake)

    result = _run_entity_critic(fake)

    assert result.success is True
    assert result.report is not None
    assert result.report.status == "FAIL"
    assert result.report.bubble_to_planner is False
    assert len(result.report.failures) == 1
    failure = result.report.failures[0]
    assert failure.kind == "semantic"
    assert failure.check == "column_semantics"
    assert "delivery_dttm" in failure.hint


# =====================================================
# 3. FAIL with bubble_to_planner=True (table-level)
# =====================================================


def test_fail_table_level_bubbles(monkeypatch, no_sleep):
    """status=FAIL, bubble_to_planner=True → bubble flag preserved on report."""
    fake = FakeLLM(
        [
            _llm_response(
                tool_calls=[
                    _make_tool_call("submit_evaluation", _valid_fail_table_submit())
                ]
            )
        ]
    )
    _patch_llm(monkeypatch, fake)

    result = _run_entity_critic(fake)

    assert result.success is True
    assert result.report is not None
    assert result.report.status == "FAIL"
    assert result.report.bubble_to_planner is True
    assert len(result.report.failures) == 1
    failure = result.report.failures[0]
    assert failure.check == "table_selection"
    assert "labour_delivery" in failure.hint


# =====================================================
# 4. PASS with bubble_to_planner=True is demoted
# =====================================================


def test_demotes_pass_with_bubble(monkeypatch, no_sleep):
    """A PASS verdict that asks to bubble is demoted to bubble=False."""
    bad_pass = _valid_pass_submit()
    bad_pass["bubble_to_planner"] = True

    fake = FakeLLM(
        [
            _llm_response(
                tool_calls=[_make_tool_call("submit_evaluation", bad_pass)]
            )
        ]
    )
    _patch_llm(monkeypatch, fake)

    result = _run_entity_critic(fake)

    assert result.success is True
    assert result.report is not None
    assert result.report.status == "PASS"
    # The bubble flag must have been demoted.
    assert result.report.bubble_to_planner is False


# =====================================================
# 5. FAIL with empty failures[] synthesises one
# =====================================================


def test_fail_without_failures_synthesises_one(monkeypatch, no_sleep):
    """status=FAIL with empty failures[] gets a generic semantic failure
    synthesised so the report stays coherent."""
    fail_no_failures = {
        "status": "FAIL",
        "failures": [],
        "bubble_to_planner": False,
        "reasoning": "Something is off but I can't pinpoint it.",
    }

    fake = FakeLLM(
        [
            _llm_response(
                tool_calls=[
                    _make_tool_call("submit_evaluation", fail_no_failures)
                ]
            )
        ]
    )
    _patch_llm(monkeypatch, fake)

    result = _run_entity_critic(fake)

    assert result.success is True
    assert result.report is not None
    assert result.report.status == "FAIL"
    assert len(result.report.failures) == 1
    f = result.report.failures[0]
    assert f.kind == "semantic"
    assert f.check == "semantic_audit"
    # The reasoning is folded into the synthetic failure's hint when present.
    assert "Something is off" in f.hint


# =====================================================
# 6. Invalid status does NOT terminate — agent retries
# =====================================================


def test_invalid_status_rejected(monkeypatch, no_sleep):
    """A submit with status='UNKNOWN' must NOT terminate the loop; the
    Critic must keep going and a follow-up submit with a valid status
    should succeed.
    """
    fake = FakeLLM(
        [
            # Turn 1: invalid status → handler returns success=False, loop continues.
            _llm_response(
                tool_calls=[
                    _make_tool_call(
                        "submit_evaluation",
                        {
                            "status": "UNKNOWN",
                            "failures": [],
                            "bubble_to_planner": False,
                            "reasoning": "n/a",
                        },
                        tc_id="bad",
                    )
                ]
            ),
            # Turn 2: valid PASS submit → terminates.
            _llm_response(
                tool_calls=[
                    _make_tool_call(
                        "submit_evaluation",
                        _valid_pass_submit(),
                        tc_id="good",
                    )
                ]
            ),
        ]
    )
    _patch_llm(monkeypatch, fake)

    result = _run_entity_critic(fake)

    assert result.success is True
    assert result.iterations == 2
    assert result.report is not None
    assert result.report.status == "PASS"
    # Both submit attempts left tool_call + tool_result steps (4 total).
    assert len(result.steps) == 4

    # The corrective tool message on the 2nd LLM call must contain the
    # "invalid status" error so the LLM sees why its first attempt failed.
    assert fake.last_messages is not None
    tool_messages = [m for m in fake.last_messages if m.get("role") == "tool"]
    assert tool_messages, "expected at least one tool message on the 2nd call"
    first_tool_msg = tool_messages[0].get("content", "")
    parsed = json.loads(first_tool_msg)
    assert parsed.get("success") is False
    assert "invalid status" in parsed.get("error", "")


# =====================================================
# 7. Text without terminal call → failure
# =====================================================


def test_text_without_terminal_fails(monkeypatch, no_sleep):
    """A plain-text response is treated as failure — the Critic must
    terminate via submit_evaluation.
    """
    fake = FakeLLM(
        [_llm_response(content="I am thinking…", finish_reason="stop")]
    )
    _patch_llm(monkeypatch, fake)

    result = _run_entity_critic(fake)

    assert result.success is False
    assert result.iterations == 1
    assert result.report is None
    assert "without submitting evaluation" in result.error
    assert any(s.step_type == "output" for s in result.steps)


# =====================================================
# 8. Iteration-budget exhaustion → failure
# =====================================================


def test_exhausts_budget(monkeypatch, no_sleep):
    """Endless sample_table calls with max_iterations=3 → fail with
    ``iteration budget`` and three iterations of steps recorded."""
    fake = CyclingFakeLLM(
        [
            _llm_response(
                tool_calls=[
                    _make_tool_call(
                        "sample_table",
                        {"full_name": "cat.sch.mothers"},
                        tc_id="probe",
                    )
                ]
            )
        ]
    )
    _patch_llm(monkeypatch, fake)

    class FakeClient:
        def execute_query(self, sql):
            return [{"nhs_number": "1234567890"}]

    result = _run_entity_critic(fake, max_iterations=3, client=FakeClient())

    assert result.success is False
    assert result.iterations == 3
    assert result.report is None
    assert "iteration budget" in result.error
    # 3 iterations × (tool_call + tool_result) = 6 steps.
    assert len(result.steps) == 6


# =====================================================
# 9. User prompt surfaces stage1 metrics
# =====================================================


def test_user_prompt_includes_stage1_metrics(monkeypatch, no_sleep):
    """The first LLM call's user message must contain stage1 metric values
    so the Critic sees the structural context."""
    fake = FakeLLM(
        [
            _llm_response(
                tool_calls=[
                    _make_tool_call("submit_evaluation", _valid_pass_submit())
                ]
            )
        ]
    )
    _patch_llm(monkeypatch, fake)

    _run_entity_critic(
        fake,
        stage1_metrics={"row_count": 1234, "distinct_ids": 1234, "null_ids": 0},
    )

    assert fake.first_messages is not None
    assert fake.first_messages[0]["role"] == "system"
    assert fake.first_messages[1]["role"] == "user"
    user_content = fake.first_messages[1]["content"]
    assert "1234" in user_content
    assert "STRUCTURAL CHECK METRICS" in user_content


# =====================================================
# 10. Relationship audit surfaces domain/range
# =====================================================


def test_user_prompt_distinguishes_entity_vs_relationship(monkeypatch, no_sleep):
    """When item_kind='relationship', the user prompt must include the
    'domain' and 'range' lines that an entity prompt would not have."""
    fake = FakeLLM(
        [
            _llm_response(
                tool_calls=[
                    _make_tool_call("submit_evaluation", _valid_pass_submit())
                ]
            )
        ]
    )
    _patch_llm(monkeypatch, fake)

    run_critic(
        host="https://x",
        token="t",
        endpoint_name="ep",
        client=None,
        item_kind="relationship",
        item_uri=_REL_URI,
        item_definition=_relationship_definition(),
        submitted_mapping=_relationship_submitted_mapping(),
        source_model_slice=_source_model_slice(),
        stage1_metrics=_stage1_metrics(),
    )

    assert fake.first_messages is not None
    user_content = fake.first_messages[1]["content"]
    # The kind is surfaced explicitly.
    assert "relationship" in user_content
    # The relationship-specific domain/range sections appear.
    assert "domain:" in user_content
    assert "range:" in user_content
    # And it is framed as a relationship submitted mapping.
    assert "SUBMITTED MAPPING (relationship)" in user_content
    # The relationship endpoint columns are surfaced too.
    assert "source_id_column" in user_content
    assert "target_id_column" in user_content


# =====================================================
# 11. Step recording invariants
# =====================================================


def test_records_steps(monkeypatch, no_sleep):
    """Every tool-calling iteration produces one ``tool_call`` step
    immediately followed by one ``tool_result`` step with the same tool_name."""
    fake = FakeLLM(
        [
            _llm_response(
                tool_calls=[
                    _make_tool_call(
                        "sample_table",
                        {"full_name": "cat.sch.mothers"},
                        tc_id="a",
                    )
                ]
            ),
            _llm_response(
                tool_calls=[
                    _make_tool_call(
                        "submit_evaluation",
                        _valid_pass_submit(),
                        tc_id="b",
                    )
                ]
            ),
        ]
    )
    _patch_llm(monkeypatch, fake)

    class FakeClient:
        def execute_query(self, sql):
            return [{"nhs_number": "1234567890"}]

    result = _run_entity_critic(fake, client=FakeClient())

    assert result.success is True
    assert len(result.steps) % 2 == 0
    for i in range(0, len(result.steps), 2):
        call_step = result.steps[i]
        result_step = result.steps[i + 1]
        assert call_step.step_type == "tool_call"
        assert result_step.step_type == "tool_result"
        assert call_step.tool_name == result_step.tool_name
        assert isinstance(call_step, CriticStep)
        assert isinstance(result_step, CriticStep)
