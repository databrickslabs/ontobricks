"""Tests for the mapping-PGE RelationshipGenerator agent (Sprint 5).

Mirrors the structure of ``test_entity_generator.py``. The Generator is a
narrow tool-calling ReAct loop terminated by ``submit_relationship_mapping``.
These tests exercise the loop's control flow with a *fake LLM* — a stub that
replaces ``call_serving_endpoint`` at module level and returns canned
responses on a per-call basis.

No real HTTP, no real Databricks, no MLflow tracing.

What we DO exercise:
* Termination on a single submit call.
* Multi-step trajectory (execute_sql → submit).
* Text-only output is treated as failure (no terminal call).
* Iteration-budget exhaustion is treated as failure.
* ``retry_hint`` surfaces inside the user message.
* Strict ``property_uri`` match — submit with a wrong URI is coached, not
  terminal.
* Step recording invariants.
* The user prompt surfaces the source/target id_columns verbatim — pins the
  Sprint 5 contract that the LLM sees the endpoint columns.
"""

import json
from typing import Any, Callable, Dict, List, Optional

import pytest

from agents.agent_mapping_pge.generators import relationship as rel_mod
from agents.agent_mapping_pge.generators.relationship import (
    RelationshipGenResult,
    RelationshipGenStep,
    run_relationship_generator,
)


# =====================================================
# Fake LLM scaffolding (mirrors test_entity_generator.py)
# =====================================================


_PROP_URI = "http://ex.org/maternity#motherOf"
_SOURCE_CLASS = "http://ex.org/maternity#Mother"
_TARGET_CLASS = "http://ex.org/maternity#Baby"


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
    monkeypatch.setattr(rel_mod.time, "sleep", lambda *_a, **_k: None)


def _patch_llm(monkeypatch, fake: Callable[..., dict]) -> None:
    monkeypatch.setattr(rel_mod, "call_serving_endpoint", fake)


# =====================================================
# Fixtures
# =====================================================


def _ontology_property() -> dict:
    return {
        "uri": _PROP_URI,
        "label": "motherOf",
        "name": "motherOf",
        "comment": "Links a Mother to each of her babies.",
        "domain": _SOURCE_CLASS,
        "range": _TARGET_CLASS,
    }


def _source_entity_mapping() -> dict:
    return {
        "ontology_class": _SOURCE_CLASS,
        "class_name": "Mother",
        "id_column": "nhs_number",
        "label_column": "nhs_number",
        "sql_query": (
            "SELECT nhs_number AS ID, nhs_number AS Label FROM cat.sch.mothers "
            "WHERE nhs_number IS NOT NULL"
        ),
    }


def _target_entity_mapping() -> dict:
    return {
        "ontology_class": _TARGET_CLASS,
        "class_name": "Baby",
        "id_column": "baby_id",
        "label_column": "baby_id",
        "sql_query": (
            "SELECT baby_id AS ID, baby_id AS Label FROM cat.sch.babies "
            "WHERE baby_id IS NOT NULL"
        ),
    }


def _source_model_slice() -> dict:
    return {
        "relevant_joins": [
            {
                "from_ref": "cat.sch.babies.mother_nhs_number",
                "to_ref": "cat.sch.mothers.nhs_number",
                "confidence": 0.95,
                "overlap_pct": 0.98,
                "kind": "same_trust_fk",
            }
        ],
        "candidate_tables": [
            {"table": "cat.sch.babies", "reason": "row per baby, has mother FK"}
        ],
    }


def _valid_submit_args() -> dict:
    return {
        "property_uri": _PROP_URI,
        "property_name": "motherOf",
        "sql_query": (
            "SELECT mother_nhs_number AS source_id, baby_id AS target_id "
            "FROM cat.sch.babies WHERE mother_nhs_number IS NOT NULL"
        ),
        "source_id_column": "nhs_number",
        "target_id_column": "baby_id",
        "domain": _SOURCE_CLASS,
        "range_class": _TARGET_CLASS,
        "direction": "forward",
    }


# =====================================================
# 1. Single-shot submit terminates immediately
# =====================================================


def test_terminates_on_submit(monkeypatch, no_sleep):
    """First LLM turn submits a valid mapping → success, iterations=1."""
    fake = FakeLLM(
        [
            _llm_response(
                tool_calls=[
                    _make_tool_call(
                        "submit_relationship_mapping", _valid_submit_args()
                    )
                ]
            )
        ]
    )
    _patch_llm(monkeypatch, fake)

    result = run_relationship_generator(
        host="https://x",
        token="t",
        endpoint_name="ep",
        client=None,
        ontology_property=_ontology_property(),
        source_entity_mapping=_source_entity_mapping(),
        target_entity_mapping=_target_entity_mapping(),
        source_model_slice=_source_model_slice(),
    )

    assert isinstance(result, RelationshipGenResult)
    assert result.success is True
    assert result.iterations == 1
    assert result.mapping is not None
    assert result.mapping["property"] == _PROP_URI
    assert result.mapping["source_id_column"] == "nhs_number"
    assert result.mapping["target_id_column"] == "baby_id"
    assert result.error == ""
    step_kinds = [s.step_type for s in result.steps]
    assert step_kinds == ["tool_call", "tool_result"]
    assert result.steps[0].tool_name == "submit_relationship_mapping"


# =====================================================
# 2. execute_sql validation, then submit
# =====================================================


def test_validates_sql_then_submits(monkeypatch, no_sleep):
    """execute_sql → submit_relationship_mapping → success, iterations=2."""
    fake = FakeLLM(
        [
            _llm_response(
                tool_calls=[
                    _make_tool_call(
                        "execute_sql",
                        {
                            "sql": (
                                "SELECT mother_nhs_number AS source_id, baby_id "
                                "AS target_id FROM cat.sch.babies "
                                "WHERE mother_nhs_number IS NOT NULL"
                            )
                        },
                        tc_id="a",
                    )
                ]
            ),
            _llm_response(
                tool_calls=[
                    _make_tool_call(
                        "submit_relationship_mapping",
                        _valid_submit_args(),
                        tc_id="b",
                    )
                ]
            ),
        ]
    )
    _patch_llm(monkeypatch, fake)

    class FakeClient:
        def execute_query(self, sql):
            return [{"source_id": "1234567890", "target_id": "b-1"}]

    result = run_relationship_generator(
        host="https://x",
        token="t",
        endpoint_name="ep",
        client=FakeClient(),
        ontology_property=_ontology_property(),
        source_entity_mapping=_source_entity_mapping(),
        target_entity_mapping=_target_entity_mapping(),
        source_model_slice=_source_model_slice(),
    )

    assert result.success is True
    assert result.iterations == 2
    assert result.mapping is not None
    # Sequence: tool_call(execute_sql), tool_result(execute_sql),
    # tool_call(submit), tool_result(submit) — 4 steps.
    assert len(result.steps) == 4
    assert [s.tool_name for s in result.steps] == [
        "execute_sql",
        "execute_sql",
        "submit_relationship_mapping",
        "submit_relationship_mapping",
    ]


# =====================================================
# 3. Text without terminal call → failure
# =====================================================


def test_text_without_terminal_fails(monkeypatch, no_sleep):
    """A plain-text response is treated as failure — the Generator must
    terminate via submit_relationship_mapping.
    """
    fake = FakeLLM(
        [_llm_response(content="I am thinking…", finish_reason="stop")]
    )
    _patch_llm(monkeypatch, fake)

    result = run_relationship_generator(
        host="https://x",
        token="t",
        endpoint_name="ep",
        client=None,
        ontology_property=_ontology_property(),
        source_entity_mapping=_source_entity_mapping(),
        target_entity_mapping=_target_entity_mapping(),
        source_model_slice=_source_model_slice(),
    )

    assert result.success is False
    assert result.iterations == 1
    assert result.mapping is None
    assert "without submitting mapping" in result.error
    assert any(s.step_type == "output" for s in result.steps)


# =====================================================
# 4. Iteration-budget exhaustion → failure
# =====================================================


def test_exhausts_iteration_budget(monkeypatch, no_sleep):
    """Endless sample_table calls with max_iterations=3 → fail with
    ``iteration budget`` and three iterations of steps recorded."""
    fake = CyclingFakeLLM(
        [
            _llm_response(
                tool_calls=[
                    _make_tool_call(
                        "sample_table",
                        {"full_name": "cat.sch.babies"},
                        tc_id="a",
                    )
                ]
            )
        ]
    )
    _patch_llm(monkeypatch, fake)

    class FakeClient:
        def execute_query(self, sql):
            return [{"mother_nhs_number": "1234567890", "baby_id": "b-1"}]

    result = run_relationship_generator(
        host="https://x",
        token="t",
        endpoint_name="ep",
        client=FakeClient(),
        ontology_property=_ontology_property(),
        source_entity_mapping=_source_entity_mapping(),
        target_entity_mapping=_target_entity_mapping(),
        source_model_slice=_source_model_slice(),
        max_iterations=3,
    )

    assert result.success is False
    assert result.iterations == 3
    assert result.mapping is None
    assert "iteration budget" in result.error
    # 3 iterations × (tool_call + tool_result) = 6 steps.
    assert len(result.steps) == 6


# =====================================================
# 5. retry_hint surfaces in the user prompt
# =====================================================


def test_retry_hint_surfaces_in_user_prompt(monkeypatch, no_sleep):
    """If ``retry_hint`` is provided, the FIRST LLM call's user message must
    contain the hint verbatim and the RETRY HINT label."""
    fake = FakeLLM(
        [
            _llm_response(
                tool_calls=[
                    _make_tool_call(
                        "submit_relationship_mapping", _valid_submit_args()
                    )
                ]
            )
        ]
    )
    _patch_llm(monkeypatch, fake)

    result = run_relationship_generator(
        host="https://x",
        token="t",
        endpoint_name="ep",
        client=None,
        ontology_property=_ontology_property(),
        source_entity_mapping=_source_entity_mapping(),
        target_entity_mapping=_target_entity_mapping(),
        source_model_slice=_source_model_slice(),
        retry_hint="Use mother_nhs_number, not patient_id.",
    )

    assert result.success is True
    assert fake.first_messages is not None
    assert fake.first_messages[0]["role"] == "system"
    assert fake.first_messages[1]["role"] == "user"
    user_content = fake.first_messages[1]["content"]
    assert "Use mother_nhs_number, not patient_id." in user_content
    assert "RETRY HINT" in user_content
    # Retry-hint corrective workflow surfaces the dangling-edge probe.
    assert "dangling-edge probe" in user_content
    assert "DO NOT repeat the same column choice" in user_content


def test_system_prompt_mandates_dangling_edge_self_check(monkeypatch, no_sleep):
    """The system prompt must instruct the model to run a dangling-edge
    probe with execute_sql BEFORE submitting — name-similarity alone is
    insufficient and was the root cause of the live smoke failure on
    hasapgarscore."""
    fake = FakeLLM(
        [
            _llm_response(
                tool_calls=[
                    _make_tool_call(
                        "submit_relationship_mapping", _valid_submit_args()
                    )
                ]
            )
        ]
    )
    _patch_llm(monkeypatch, fake)

    run_relationship_generator(
        host="https://x",
        token="t",
        endpoint_name="ep",
        client=None,
        ontology_property=_ontology_property(),
        source_entity_mapping=_source_entity_mapping(),
        target_entity_mapping=_target_entity_mapping(),
        source_model_slice=_source_model_slice(),
    )

    system_content = fake.first_messages[0]["content"]
    assert "SELF-VERIFY THE VALUES BEFORE SUBMITTING" in system_content
    assert "dangling_src" in system_content
    assert "dangling_tgt" in system_content
    # The probe must reference both endpoint universes via the entity SQLs.
    assert "source entity's SQL" in system_content
    assert "target entity's SQL" in system_content


# =====================================================
# 6. Wrong property_uri submission does NOT terminate
# =====================================================


def test_wrong_property_uri_submission_does_not_terminate(monkeypatch, no_sleep):
    """A submit_relationship_mapping call with a property_uri that doesn't
    match the requested one must NOT terminate the loop. The Generator must
    keep going so a follow-up submit (with the correct URI) can succeed, and
    the LLM must see a corrective tool message describing the mismatch.
    """
    requested_uri = _PROP_URI
    other_uri = "http://ex.org/maternity#fatherOf"

    wrong_args = _valid_submit_args()
    wrong_args["property_uri"] = other_uri
    wrong_args["property_name"] = "fatherOf"

    fake = FakeLLM(
        [
            # Turn 1: submit with the WRONG property_uri — must NOT terminate.
            _llm_response(
                tool_calls=[
                    _make_tool_call(
                        "submit_relationship_mapping",
                        wrong_args,
                        tc_id="wrong",
                    )
                ]
            ),
            # Turn 2: submit with the correct property_uri — should terminate.
            _llm_response(
                tool_calls=[
                    _make_tool_call(
                        "submit_relationship_mapping",
                        _valid_submit_args(),
                        tc_id="right",
                    )
                ]
            ),
        ]
    )
    _patch_llm(monkeypatch, fake)

    result = run_relationship_generator(
        host="https://x",
        token="t",
        endpoint_name="ep",
        client=None,
        ontology_property=_ontology_property(),
        source_entity_mapping=_source_entity_mapping(),
        target_entity_mapping=_target_entity_mapping(),
        source_model_slice=_source_model_slice(),
    )

    assert result.success is True
    assert result.iterations == 2
    assert result.mapping is not None
    assert result.mapping["property"] == requested_uri

    # The LLM's second call must have seen a corrective tool message
    # describing the mismatch, surfaced through ``messages``.
    assert fake.last_messages is not None
    tool_messages = [m for m in fake.last_messages if m.get("role") == "tool"]
    assert tool_messages, "expected at least one tool message on the 2nd call"
    corrective = tool_messages[-1]
    corrective_content = corrective.get("content", "")
    assert other_uri in corrective_content
    assert requested_uri in corrective_content
    assert "does not match" in corrective_content
    # Sanity: the corrective payload is a JSON error (not the original
    # success=True response).
    parsed = json.loads(corrective_content)
    assert parsed.get("success") is False
    assert "error" in parsed


# =====================================================
# 7. Step recording invariants
# =====================================================


def test_records_steps(monkeypatch, no_sleep):
    """Every tool-calling iteration produces exactly one ``tool_call`` step
    immediately followed by one ``tool_result`` step with the same tool_name.
    """
    fake = FakeLLM(
        [
            _llm_response(
                tool_calls=[
                    _make_tool_call(
                        "sample_table",
                        {"full_name": "cat.sch.babies"},
                        tc_id="a",
                    )
                ]
            ),
            _llm_response(
                tool_calls=[
                    _make_tool_call(
                        "submit_relationship_mapping",
                        _valid_submit_args(),
                        tc_id="b",
                    )
                ]
            ),
        ]
    )
    _patch_llm(monkeypatch, fake)

    class FakeClient:
        def execute_query(self, sql):
            return [{"mother_nhs_number": "1234567890", "baby_id": "b-1"}]

    result = run_relationship_generator(
        host="https://x",
        token="t",
        endpoint_name="ep",
        client=FakeClient(),
        ontology_property=_ontology_property(),
        source_entity_mapping=_source_entity_mapping(),
        target_entity_mapping=_target_entity_mapping(),
        source_model_slice=_source_model_slice(),
    )

    assert result.success is True
    assert len(result.steps) % 2 == 0
    for i in range(0, len(result.steps), 2):
        call_step = result.steps[i]
        result_step = result.steps[i + 1]
        assert call_step.step_type == "tool_call"
        assert result_step.step_type == "tool_result"
        assert call_step.tool_name == result_step.tool_name
        assert call_step.content != ""
        assert result_step.content != ""
        assert isinstance(call_step, RelationshipGenStep)
        assert isinstance(result_step, RelationshipGenStep)


# =====================================================
# 8. User prompt surfaces source/target id_columns
# =====================================================


def test_user_prompt_includes_source_and_target_id_columns(monkeypatch, no_sleep):
    """The FIRST call's user message must contain both id_column names
    verbatim. This pins the Sprint 5 contract that the Generator surfaces
    the endpoint columns to the LLM, so the LLM cannot silently pick
    different endpoints.
    """
    # Use distinctive id_column names that won't appear anywhere else in
    # the slice (mothers/babies join etc.), to make the assertion strict.
    src_em = {
        "ontology_class": _SOURCE_CLASS,
        "class_name": "Mother",
        "id_column": "weirdly_named_mother_pk",
        "label_column": "weirdly_named_mother_pk",
        "sql_query": "SELECT weirdly_named_mother_pk AS ID FROM cat.sch.mothers",
    }
    tgt_em = {
        "ontology_class": _TARGET_CLASS,
        "class_name": "Baby",
        "id_column": "weirdly_named_baby_pk",
        "label_column": "weirdly_named_baby_pk",
        "sql_query": "SELECT weirdly_named_baby_pk AS ID FROM cat.sch.babies",
    }

    fake = FakeLLM(
        [
            _llm_response(
                tool_calls=[
                    _make_tool_call(
                        "submit_relationship_mapping", _valid_submit_args()
                    )
                ]
            )
        ]
    )
    _patch_llm(monkeypatch, fake)

    run_relationship_generator(
        host="https://x",
        token="t",
        endpoint_name="ep",
        client=None,
        ontology_property=_ontology_property(),
        source_entity_mapping=src_em,
        target_entity_mapping=tgt_em,
        source_model_slice=_source_model_slice(),
    )

    assert fake.first_messages is not None
    user_content = fake.first_messages[1]["content"]
    assert "weirdly_named_mother_pk" in user_content
    assert "weirdly_named_baby_pk" in user_content
