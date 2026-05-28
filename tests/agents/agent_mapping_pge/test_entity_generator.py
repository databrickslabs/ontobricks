"""Tests for the mapping-PGE EntityGenerator agent (Sprint 4).

The Generator is a narrow tool-calling ReAct loop terminated by
``submit_entity_mapping``. These tests exercise the loop's control flow with
a *fake LLM* — a stub that replaces ``call_serving_endpoint`` at module
level and returns canned tool-call responses on a per-call basis.

No real HTTP, no real Databricks, no MLflow tracing.

What we DO exercise:
* Termination on a single submit call.
* Multi-step trajectory (execute_sql → submit).
* ``unmapped_attributes`` round-trips through the tool to the result.
* Text-only output is treated as failure (no terminal call).
* Iteration-budget exhaustion is treated as failure.
* ``retry_hint`` surfaces inside the user message.
* Step recording: every tool call produces both tool_call and tool_result
  steps in the right order.
"""

import json
from typing import Any, Callable, Dict, List, Optional

import pytest

from agents.agent_mapping_pge.generators import entity as entity_mod
from agents.agent_mapping_pge.generators.entity import (
    EntityGenResult,
    EntityGenStep,
    run_entity_generator,
)


# =====================================================
# Fake LLM scaffolding (mirrors test_planner.py)
# =====================================================


_CLASS_URI = "http://ex.org/maternity#Mother"


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
        # Capture the messages list as observed on each call, so tests can
        # introspect what the agent put into the prompt.
        self.last_messages: Optional[List[dict]] = None
        self.first_messages: Optional[List[dict]] = None

    def __call__(self, *args, **kwargs) -> dict:
        self.calls += 1
        # ``call_serving_endpoint(host, token, endpoint, messages, ...)`` —
        # the messages list is positional arg #3 (zero-indexed). Capture
        # defensively in case the call site changes to kwargs.
        msgs: Optional[List[dict]] = None
        if len(args) >= 4 and isinstance(args[3], list):
            msgs = args[3]
        elif "messages" in kwargs:
            msgs = kwargs["messages"]
        if msgs is not None:
            # snapshot so later mutations by the loop do not affect what we
            # captured.
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
    monkeypatch.setattr(entity_mod.time, "sleep", lambda *_a, **_k: None)


def _patch_llm(monkeypatch, fake: Callable[..., dict]) -> None:
    monkeypatch.setattr(entity_mod, "call_serving_endpoint", fake)


# =====================================================
# Fixtures
# =====================================================


def _ontology_class() -> dict:
    return {
        "uri": _CLASS_URI,
        "label": "Mother",
        "name": "Mother",
        "comment": "A mother in the maternity trust dataset.",
        "attributes": [
            {"name": "nhsNumber", "type": "string"},
            {"name": "dateOfBirth", "type": "date"},
            {"name": "ethnicity", "type": "string"},
        ],
    }


def _source_model_slice() -> dict:
    return {
        "candidate_tables": [
            {
                "table": "cat.sch.mothers",
                "confidence": 0.92,
                "reason": "row per NHS — mother demographics",
            }
        ],
        "canonical_id": {
            "canonical_column_per_table": {"cat.sch.mothers": "nhs_number"},
            "format_note": "10-digit NHS",
        },
        "relevant_joins": [],
    }


def _valid_submit_args(
    *,
    unmapped: Optional[list] = None,
) -> dict:
    args: Dict[str, Any] = {
        "class_uri": _CLASS_URI,
        "class_name": "Mother",
        "sql_query": (
            "SELECT nhs_number AS ID, nhs_number AS Label, nhs_number, dob, ethnicity "
            "FROM cat.sch.mothers WHERE nhs_number IS NOT NULL"
        ),
        "id_column": "nhs_number",
        "label_column": "nhs_number",
        "attribute_mappings": {
            "nhsNumber": "nhs_number",
            "dateOfBirth": "dob",
            "ethnicity": "ethnicity",
        },
    }
    if unmapped is not None:
        args["unmapped_attributes"] = unmapped
    return args


# =====================================================
# 1. Single-shot submit terminates immediately
# =====================================================


def test_terminates_on_submit(monkeypatch, no_sleep):
    """First LLM turn submits a valid mapping → success, iterations=1."""
    fake = FakeLLM(
        [
            _llm_response(
                tool_calls=[
                    _make_tool_call("submit_entity_mapping", _valid_submit_args())
                ]
            )
        ]
    )
    _patch_llm(monkeypatch, fake)

    result = run_entity_generator(
        host="https://x",
        token="t",
        endpoint_name="ep",
        client=None,
        ontology_class=_ontology_class(),
        source_model_slice=_source_model_slice(),
    )

    assert isinstance(result, EntityGenResult)
    assert result.success is True
    assert result.iterations == 1
    assert result.mapping is not None
    assert result.mapping["ontology_class"] == _CLASS_URI
    assert result.mapping["id_column"] == "nhs_number"
    assert result.error == ""
    step_kinds = [s.step_type for s in result.steps]
    assert step_kinds == ["tool_call", "tool_result"]
    assert result.steps[0].tool_name == "submit_entity_mapping"


# =====================================================
# 2. execute_sql validation, then submit
# =====================================================


def test_validates_sql_then_submits(monkeypatch, no_sleep):
    """execute_sql → submit_entity_mapping → success, iterations=2."""
    fake = FakeLLM(
        [
            _llm_response(
                tool_calls=[
                    _make_tool_call(
                        "execute_sql",
                        {
                            "sql": (
                                "SELECT nhs_number AS ID, nhs_number AS Label, "
                                "nhs_number, dob, ethnicity FROM cat.sch.mothers "
                                "WHERE nhs_number IS NOT NULL"
                            )
                        },
                        tc_id="a",
                    )
                ]
            ),
            _llm_response(
                tool_calls=[
                    _make_tool_call(
                        "submit_entity_mapping", _valid_submit_args(), tc_id="b"
                    )
                ]
            ),
        ]
    )
    _patch_llm(monkeypatch, fake)

    class FakeClient:
        def execute_query(self, sql):
            return [
                {
                    "ID": "1234567890",
                    "Label": "1234567890",
                    "nhs_number": "1234567890",
                    "dob": "1990-01-01",
                    "ethnicity": "white",
                }
            ]

    result = run_entity_generator(
        host="https://x",
        token="t",
        endpoint_name="ep",
        client=FakeClient(),
        ontology_class=_ontology_class(),
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
        "submit_entity_mapping",
        "submit_entity_mapping",
    ]


# =====================================================
# 3. unmapped_attributes round-trips
# =====================================================


def test_unmapped_attributes_round_trip(monkeypatch, no_sleep):
    """Submit with ``unmapped_attributes`` — the field must appear on the
    resulting mapping dict in the same (normalised) shape."""
    unmapped_payload = [
        {"name": "ethnicity", "reason": "no ethnicity column in this table"}
    ]
    args = _valid_submit_args(unmapped=unmapped_payload)
    # Strip ethnicity from attribute_mappings to make the example coherent.
    args["attribute_mappings"].pop("ethnicity", None)

    fake = FakeLLM(
        [
            _llm_response(
                tool_calls=[_make_tool_call("submit_entity_mapping", args)]
            )
        ]
    )
    _patch_llm(monkeypatch, fake)

    result = run_entity_generator(
        host="https://x",
        token="t",
        endpoint_name="ep",
        client=None,
        ontology_class=_ontology_class(),
        source_model_slice=_source_model_slice(),
    )

    assert result.success is True
    assert result.mapping is not None
    assert result.mapping["unmapped_attributes"] == [
        {"name": "ethnicity", "reason": "no ethnicity column in this table"}
    ]
    # Plain-string form is also documented; make sure it survives too.
    fake2 = FakeLLM(
        [
            _llm_response(
                tool_calls=[
                    _make_tool_call(
                        "submit_entity_mapping",
                        _valid_submit_args(unmapped=["ethnicity"]),
                    )
                ]
            )
        ]
    )
    _patch_llm(monkeypatch, fake2)
    result2 = run_entity_generator(
        host="https://x",
        token="t",
        endpoint_name="ep",
        client=None,
        ontology_class=_ontology_class(),
        source_model_slice=_source_model_slice(),
    )
    assert result2.success is True
    assert result2.mapping["unmapped_attributes"] == ["ethnicity"]


# =====================================================
# 4. Text without terminal call → failure
# =====================================================


def test_text_without_terminal_fails(monkeypatch, no_sleep):
    """A plain-text response is treated as failure — the Generator must
    terminate via submit_entity_mapping.
    """
    fake = FakeLLM(
        [_llm_response(content="I am thinking…", finish_reason="stop")]
    )
    _patch_llm(monkeypatch, fake)

    result = run_entity_generator(
        host="https://x",
        token="t",
        endpoint_name="ep",
        client=None,
        ontology_class=_ontology_class(),
        source_model_slice=_source_model_slice(),
    )

    assert result.success is False
    assert result.iterations == 1
    assert result.mapping is None
    assert "without submitting mapping" in result.error
    assert any(s.step_type == "output" for s in result.steps)


# =====================================================
# 5. Iteration-budget exhaustion → failure
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
                        {"full_name": "cat.sch.mothers"},
                        tc_id="a",
                    )
                ]
            )
        ]
    )
    _patch_llm(monkeypatch, fake)

    class FakeClient:
        def execute_query(self, sql):
            return [{"nhs_number": "1234567890"}]

    result = run_entity_generator(
        host="https://x",
        token="t",
        endpoint_name="ep",
        client=FakeClient(),
        ontology_class=_ontology_class(),
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
# 6. retry_hint surfaces in the user prompt
# =====================================================


def test_retry_hint_surfaces_in_user_prompt(monkeypatch, no_sleep):
    """If ``retry_hint`` is provided, the FIRST LLM call's user message must
    contain the hint verbatim."""
    fake = FakeLLM(
        [
            _llm_response(
                tool_calls=[
                    _make_tool_call("submit_entity_mapping", _valid_submit_args())
                ]
            )
        ]
    )
    _patch_llm(monkeypatch, fake)

    result = run_entity_generator(
        host="https://x",
        token="t",
        endpoint_name="ep",
        client=None,
        ontology_class=_ontology_class(),
        source_model_slice=_source_model_slice(),
        retry_hint="Use NHS column, not patient_id.",
    )

    assert result.success is True
    assert fake.first_messages is not None
    # messages[0] is system, messages[1] is user.
    assert fake.first_messages[0]["role"] == "system"
    assert fake.first_messages[1]["role"] == "user"
    user_content = fake.first_messages[1]["content"]
    assert "Use NHS column, not patient_id." in user_content
    # RETRY HINT label is present so the LLM understands its provenance.
    assert "RETRY HINT" in user_content


# =====================================================
# 7. Step recording invariants
# =====================================================


def test_wrong_class_uri_submission_does_not_terminate(monkeypatch, no_sleep):
    """A submit_entity_mapping call with a class_uri that doesn't match the
    requested one must NOT terminate the loop. The Generator must keep going
    so a follow-up submit (with the correct URI) can succeed, and the LLM
    must see a corrective tool message describing the mismatch.
    """
    requested_uri = _CLASS_URI
    other_uri = "http://ex.org/maternity#Baby"

    wrong_args = _valid_submit_args()
    wrong_args["class_uri"] = other_uri
    wrong_args["class_name"] = "Baby"

    fake = FakeLLM(
        [
            # Turn 1: submit with the WRONG class_uri — must NOT terminate.
            _llm_response(
                tool_calls=[
                    _make_tool_call(
                        "submit_entity_mapping", wrong_args, tc_id="wrong"
                    )
                ]
            ),
            # Turn 2: submit with the correct class_uri — should terminate.
            _llm_response(
                tool_calls=[
                    _make_tool_call(
                        "submit_entity_mapping",
                        _valid_submit_args(),
                        tc_id="right",
                    )
                ]
            ),
        ]
    )
    _patch_llm(monkeypatch, fake)

    result = run_entity_generator(
        host="https://x",
        token="t",
        endpoint_name="ep",
        client=None,
        ontology_class=_ontology_class(),
        source_model_slice=_source_model_slice(),
    )

    assert result.success is True
    assert result.iterations == 2
    assert result.mapping is not None
    assert result.mapping["ontology_class"] == requested_uri

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
                        {"full_name": "cat.sch.mothers"},
                        tc_id="a",
                    )
                ]
            ),
            _llm_response(
                tool_calls=[
                    _make_tool_call(
                        "submit_entity_mapping", _valid_submit_args(), tc_id="b"
                    )
                ]
            ),
        ]
    )
    _patch_llm(monkeypatch, fake)

    class FakeClient:
        def execute_query(self, sql):
            return [{"nhs_number": "1234567890"}]

    result = run_entity_generator(
        host="https://x",
        token="t",
        endpoint_name="ep",
        client=FakeClient(),
        ontology_class=_ontology_class(),
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
        assert isinstance(call_step, EntityGenStep)
        assert isinstance(result_step, EntityGenStep)
