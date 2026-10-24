"""Staged orchestrator contracts for ``agents.agent_owl_generator.staged``.

The staged Generate agent exposes four explicit entry points —
``detect_entities`` (Stage 1) and ``infer_relations`` / ``infer_attributes`` /
``infer_axioms`` (Stage 3, strict order). These tests pin the behavioural
contract from the design
(``docs/superpowers/specs/2026-09-20-three-stage-ontology-generate-design.md``)
and SPEC (``.planning/agents/agent_owl_generator/SPEC.md`` §3/§3a/§6a):

* detection returns bounded structured candidates, defaulted to included,
  deduplicated against locked anchors, using only the no-reparse doc tools;
* completion consumes only the validated draft contract (locked anchors +
  included candidates by stable id), never document tools, never new
  entities; an out-of-closure reference is rejected (reject-only, no
  in-request rewrite);
* strict relations -> attributes -> axioms ordering is enforced at the
  interface;
* every stage carries a distinct trace identity;
* no staged entry point performs one-shot full-ontology generation.

``call_serving_endpoint`` is patched with scripted responses so no live
endpoint is needed.
"""

from __future__ import annotations

import json

from unittest.mock import MagicMock, patch

import pytest
import requests

import agents.tracing as tracing_mod
from back.objects.ontology.generate.GenerateDraft import (
    CHECKPOINT_DONE,
    GenerateDraft,
    GenerateEntity,
    REVIEWING,
)
from agents.agent_owl_generator import staged
from agents.agent_owl_generator import prompts
from agents.agent_owl_generator import schemas


# ---------------------------------------------------------------------------
# Scripted LLM responses
# ---------------------------------------------------------------------------


def _answer(content: str, finish_reason: str = "stop") -> dict:
    return {
        "choices": [{"finish_reason": finish_reason, "message": {"content": content}}],
        "usage": {"prompt_tokens": 100, "completion_tokens": 50},
    }


def _tool_call(name: str, args: str = "{}", call_id: str = "tc-1") -> dict:
    return {
        "choices": [
            {
                "finish_reason": "tool_calls",
                "message": {
                    "content": None,
                    "tool_calls": [
                        {
                            "id": call_id,
                            "function": {"name": name, "arguments": args},
                        }
                    ],
                },
            }
        ],
        "usage": {"prompt_tokens": 50, "completion_tokens": 10},
    }


def _detect(responses):
    with patch.object(staged, "call_serving_endpoint") as mock_llm:
        mock_llm.side_effect = responses
        result = staged.detect_entities(
            host="https://test.databricks.com",
            token="tok",
            endpoint_name="dbx-llm",
            metadata={"tables": []},
            guidelines="Generate a CRM ontology.",
            options={},
            existing_anchors=[
                GenerateEntity.locked_anchor("cls-Customer-a1", "Customer")
            ],
            registry={"catalog": "main", "schema": "ob", "volume": "documents"},
        )
    return result, mock_llm


def _draft(*, candidates=None, anchors=None, relations_done=False, attributes_done=False):
    draft = GenerateDraft.new(
        source_fingerprint="sha256:fp",
        existing_anchors=anchors
        or [GenerateEntity.locked_anchor("cls-Customer-a1", "Customer")],
        candidate_entities=candidates
        or [GenerateEntity.new_candidate("Carrier", entity_id="cand-6")],
        stage=REVIEWING,
    )
    if relations_done:
        draft = draft.with_checkpoint(
            "relations", CHECKPOINT_DONE, result={"relations": []}
        )
    if attributes_done:
        draft = draft.with_checkpoint(
            "attributes", CHECKPOINT_DONE, result={"attributes": []}
        )
    return draft


# ---------------------------------------------------------------------------
# Stage 1: detection
# ---------------------------------------------------------------------------


class TestDetectEntities:
    """Detection is now a bounded tool-gathering phase followed by exactly
    ONE schema-enforced (``response_format``) finalization call — see
    ``TestTwoPhaseDetectionArchitecture`` below for the phase-boundary
    contract itself. Every scripted flow here therefore needs at least TWO
    responses: one gathering turn (tool call(s) and/or a no-tool-call
    "gathering is done" turn, whose content is always discarded) followed
    by the finalization answer that is actually parsed.
    """

    def test_returns_default_included_candidates(self):
        payload = (
            '{"candidate_entities": [{"canonical_label": "Carrier"}, '
            '{"canonical_label": "Invoice"}]}'
        )
        result, _ = _detect([_answer(""), _answer(payload)])
        assert result.success is True
        assert [c.canonical_label for c in result.candidate_entities] == [
            "Carrier",
            "Invoice",
        ]
        assert all(c.included for c in result.candidate_entities)

    def test_deduplicates_against_locked_anchor(self):
        payload = (
            '{"candidate_entities": [{"canonical_label": "Customer"}, '
            '{"canonical_label": "Carrier"}]}'
        )
        result, _ = _detect([_answer(""), _answer(payload)])
        assert [c.canonical_label for c in result.candidate_entities] == ["Carrier"]

    def test_only_uses_no_reparse_tool_surface(self):
        payload = '{"candidate_entities": [{"canonical_label": "Carrier"}]}'
        _, mock_llm = _detect([_answer(""), _answer(payload)])
        sent_tools = mock_llm.call_args_list[0].kwargs["tools"]
        names = {t["function"]["name"] for t in sent_tools}
        assert names <= {
            "list_documents",
            "read_document",
            "get_documents_context",
            "get_metadata",
            "get_table_detail",
        }
        # Never the parser or the pitfall-rewrite tool.
        assert "ai_parse_document" not in names
        assert "check_owl_pitfalls" not in names

    def test_finalization_truncation_fails_without_reprompt(self):
        # Reject-only, per this revision's transport-level fix: a
        # length-truncated FINALIZATION answer fails clearly — it is never
        # re-prompted for a concise re-emission (that "retry" behaviour was
        # removed along with the single-phase loop it belonged to).
        truncated = _answer('{"candidate_entities": [{"canonical', "length")
        result, mock_llm = _detect([_answer(""), truncated])
        assert result.success is False
        assert result.rejected is False
        assert "truncated" in result.error.lower()
        assert mock_llm.call_count == 2  # gathering + ONE finalization, no re-prompt

    def test_gathering_truncation_is_treated_as_a_gather_stop(self):
        # A truncated GATHERING turn's content is never parsed anyway (see
        # TestTwoPhaseDetectionArchitecture), so truncation there is
        # indistinguishable from a normal no-tool-call gather-stop: it just
        # ends gathering and moves on to the one finalization call. Only
        # the finalization call's own truncation is a reportable failure.
        truncated_gather = _answer("some cut off reasoning...", "length")
        payload = '{"candidate_entities": [{"canonical_label": "Carrier"}]}'
        result, mock_llm = _detect([truncated_gather, _answer(payload)])
        assert result.success is True
        assert [c.canonical_label for c in result.candidate_entities] == ["Carrier"]

    def test_malformed_final_output_fails_without_rewrite(self):
        # A non-truncated malformed FINALIZATION answer is rejected
        # (reject-only): exactly one finalization attempt, never a second
        # one asking the LLM to try again.
        result, mock_llm = _detect([_answer(""), _answer("this is not json")])
        assert result.success is False
        assert result.rejected is True
        assert mock_llm.call_count == 2  # gathering + ONE finalization

    def test_trace_identity_is_stage_specific(self):
        payload = '{"candidate_entities": [{"canonical_label": "Carrier"}]}'
        _, mock_llm = _detect([_answer(""), _answer(payload)])
        for call in mock_llm.call_args_list:
            assert call.kwargs["trace_name"] == "owl_generator.detect"

    def test_empty_candidate_list_is_a_successful_result_not_a_rejection(self):
        # Live bug (Stage-1 zero-new-candidate case): every selected table's
        # core entity is already a locked anchor, so the model's ONLY
        # contract-compliant answer is the empty list — that must succeed,
        # not be treated as malformed/rejected.
        payload = '{"candidate_entities": []}'
        result, mock_llm = _detect([_answer(""), _answer(payload)])
        assert result.success is True
        assert result.rejected is False
        assert result.candidate_entities == []
        assert mock_llm.call_count == 2


# ---------------------------------------------------------------------------
# Transport-level structured output: bounded tool-gathering, then exactly
# ONE schema-enforced finalization call (live-reliability fix).
#
# Root cause this fixes: a prompt-only "JSON only, first character must be
# {" instruction cannot force a compliant model to skip a visible reasoning
# preamble — live reproduction against the user's own endpoint
# (benoit_cayla.ontobricks-todrop.monclaudesonnetamoi) still narrated prose
# ahead of the JSON on 4 of 5 calls despite that strengthened prompt. That
# same endpoint was confirmed (by direct user testing) to honour an
# OpenAI/Databricks-style
# ``response_format={"type": "json_schema", "json_schema": {...}}``
# transport directive and return exactly the schema-shaped JSON, while
# rejecting ``response_format`` combined with ``tools`` in the same
# request. Detection is therefore split into a bounded tool-gathering phase
# (``tools=`` set, no ``response_format``) and exactly one finalization
# call (``tools=None``, ``response_format=`` the strict detection schema).
# This is NOT a schema-repair loop: there is one structured finalization
# call, and reject-only validation still runs after it (see
# ``TestDetectEntities`` above).
# ---------------------------------------------------------------------------


class TestTwoPhaseDetectionArchitecture:
    def test_tools_and_response_format_are_never_sent_in_the_same_call(self):
        payload = '{"candidate_entities": [{"canonical_label": "Carrier"}]}'
        _, mock_llm = _detect(
            [_tool_call("get_metadata"), _answer(""), _answer(payload)]
        )
        assert len(mock_llm.call_args_list) >= 2
        for call in mock_llm.call_args_list:
            tools = call.kwargs.get("tools")
            response_format = call.kwargs.get("response_format")
            assert not (tools and response_format)

    def test_gathering_calls_never_carry_response_format(self):
        payload = '{"candidate_entities": [{"canonical_label": "Carrier"}]}'
        _, mock_llm = _detect(
            [_tool_call("get_metadata"), _answer(""), _answer(payload)]
        )
        gathering_calls = mock_llm.call_args_list[:-1]
        assert gathering_calls  # at least one gathering call happened
        for call in gathering_calls:
            assert call.kwargs.get("tools")
            assert not call.kwargs.get("response_format")

    def test_finalization_call_uses_the_strict_detection_schema_with_no_tools(self):
        payload = '{"candidate_entities": [{"canonical_label": "Carrier"}]}'
        _, mock_llm = _detect([_answer(""), _answer(payload)])
        final_call = mock_llm.call_args_list[-1]
        assert final_call.kwargs["response_format"] == schemas.DETECTION_RESPONSE_FORMAT
        assert final_call.kwargs["tools"] is None

    def test_prose_from_a_gather_stop_turn_is_ignored_not_parsed(self):
        # The gathering phase's OWN content-only turn — even if it looks
        # like a (wrong) JSON guess — is never parsed as the answer; only
        # the separate finalization call's content is.
        decoy = '{"candidate_entities": [{"canonical_label": "WrongGuess"}]}'
        real_payload = '{"candidate_entities": [{"canonical_label": "Carrier"}]}'
        result, mock_llm = _detect([_answer(decoy), _answer(real_payload)])
        assert result.success is True
        assert [c.canonical_label for c in result.candidate_entities] == ["Carrier"]
        # It is not even kept in the conversation sent to finalization.
        final_messages = mock_llm.call_args_list[-1].args[3]
        assert not any(m.get("content") == decoy for m in final_messages)

    def test_finalization_receives_accumulated_tool_call_and_result(self):
        payload = '{"candidate_entities": [{"canonical_label": "Carrier"}]}'
        _, mock_llm = _detect(
            [_tool_call("get_metadata"), _answer(""), _answer(payload)]
        )
        final_messages = mock_llm.call_args_list[-1].args[3]
        roles = [m.get("role") for m in final_messages]
        assert "tool" in roles
        # The assistant's tool-calling turn itself (its `tool_calls` field)
        # is also carried forward, alongside the tool result above.
        assert any(m.get("tool_calls") for m in final_messages)

    def test_endpoint_without_tool_support_skips_straight_to_finalization(self):
        payload = '{"candidate_entities": [{"canonical_label": "Carrier"}]}'
        rejection = MagicMock(status_code=400)
        http_error = requests.exceptions.HTTPError(response=rejection)
        with patch.object(staged, "call_serving_endpoint") as mock_llm:
            mock_llm.side_effect = [http_error, _answer(payload)]
            result = staged.detect_entities(
                host="https://test.databricks.com",
                token="tok",
                endpoint_name="dbx-llm",
                metadata={"tables": []},
                guidelines="Generate a CRM ontology.",
                existing_anchors=[],
                registry={"catalog": "main", "schema": "ob", "volume": "documents"},
            )
        assert result.success is True
        assert mock_llm.call_count == 2
        # The only remaining call is the schema-enforced finalization —
        # never a second tools attempt.
        assert mock_llm.call_args_list[1].kwargs["tools"] is None
        assert mock_llm.call_args_list[1].kwargs.get("response_format")

    def test_bounded_gathering_iterations_still_reach_finalization(self):
        # Even if the model keeps requesting tools for the entire gathering
        # budget (never emitting a no-tool-call turn), detection still
        # reaches exactly one finalization call once the budget is spent —
        # never an unbounded gathering loop.
        payload = '{"candidate_entities": [{"canonical_label": "Carrier"}]}'
        gathering_calls = [
            _tool_call("get_metadata") for _ in range(staged._MAX_GATHERING_ITERATIONS)
        ]
        result, mock_llm = _detect([*gathering_calls, _answer(payload)])
        assert result.success is True
        assert [c.canonical_label for c in result.candidate_entities] == ["Carrier"]
        # Exactly budget-many gathering calls + ONE finalization call.
        assert mock_llm.call_count == staged._MAX_GATHERING_ITERATIONS + 1


class TestDetectionResponseFormatFallbackIntegration:
    """End-to-end proof (real ``staged.detect_entities`` +
    ``agents.engine_base.call_serving_endpoint`` — only the HTTP transport
    mocked) that an endpoint rejecting ``response_format`` with a clear 400
    degrades to a plain finalization call transparently, via
    ``engine_base``'s existing per-endpoint unsupported-param ban/retry
    (see ``tests/units/agents/test_agent_engine_base.py`` for the isolated
    unit tests of that mechanism)."""

    def test_endpoint_rejecting_response_format_still_succeeds_end_to_end(self):
        rejection = MagicMock()
        rejection.status_code = 400
        rejection.text = "does not support the response_format parameter"
        http_error = requests.exceptions.HTTPError(response=rejection)

        gather_resp = MagicMock()
        gather_resp.json.return_value = _answer("")
        finalize_resp = MagicMock()
        finalize_resp.json.return_value = _answer(
            '{"candidate_entities": [{"canonical_label": "Carrier"}]}'
        )

        def _retry_side_effect(_url, _headers, payload, timeout=None):
            if payload.get("response_format"):
                raise http_error
            if payload.get("tools"):
                return gather_resp
            return finalize_resp

        with patch(
            "agents.engine_base.call_llm_with_retry", side_effect=_retry_side_effect
        ):
            result = staged.detect_entities(
                host="https://test.databricks.com",
                token="tok",
                endpoint_name="dbx-llm",
                metadata={"tables": []},
                guidelines="Generate a CRM ontology.",
                existing_anchors=[],
                registry={"catalog": "main", "schema": "ob", "volume": "documents"},
            )

        assert result.success is True
        assert [c.canonical_label for c in result.candidate_entities] == ["Carrier"]


# ---------------------------------------------------------------------------
# Stage 3: relations
# ---------------------------------------------------------------------------


class TestInferRelations:
    def _run(self, responses, draft=None):
        draft = draft or _draft()
        with patch.object(staged, "call_serving_endpoint") as mock_llm:
            mock_llm.side_effect = responses
            result = staged.infer_relations(
                host="h", token="t", endpoint_name="e", draft=draft
            )
        return result, mock_llm

    def test_valid_relation_within_closure_succeeds(self):
        payload = (
            '{"relations": [{"label": "shipsTo", '
            '"domain": "cand-6", "range": "cls-Customer-a1"}]}'
        )
        result, mock_llm = self._run([_answer(payload)])
        assert result.success is True
        assert result.substage == "relations"
        assert result.result["relations"][0]["label"] == "shipsTo"

    def test_completion_never_uses_document_tools(self):
        payload = '{"relations": []}'
        _, mock_llm = self._run([_answer(payload)])
        assert mock_llm.call_args_list[0].kwargs["tools"] is None

    def test_reference_outside_closure_rejected_no_rewrite(self):
        payload = (
            '{"relations": [{"label": "shipsTo", '
            '"domain": "cand-6", "range": "cls-Ghost-999"}]}'
        )
        result, mock_llm = self._run([_answer(payload)])
        assert result.success is False
        assert result.rejected is True
        assert "cls-Ghost-999" in result.rejection_reason
        # Reject-only: exactly one LLM call, no rewrite feedback loop.
        assert mock_llm.call_count == 1

    def test_excluded_candidate_reference_rejected(self):
        draft = _draft(
            candidates=[
                GenerateEntity.new_candidate("Carrier", entity_id="cand-6"),
                GenerateEntity.new_candidate(
                    "Invoice", entity_id="cand-4", included=False
                ),
            ]
        )
        payload = (
            '{"relations": [{"label": "billedVia", '
            '"domain": "cand-6", "range": "cand-4"}]}'
        )
        result, mock_llm = self._run([_answer(payload)], draft=draft)
        assert result.rejected is True
        assert "cand-4" in result.rejection_reason
        assert mock_llm.call_count == 1

    def test_existing_relations_are_shown_and_paraphrases_dropped(self):
        draft = _draft(
            anchors=[
                GenerateEntity.locked_anchor("Payment", "Payment"),
                GenerateEntity.locked_anchor("Contract", "Contract"),
            ],
            candidates=[GenerateEntity.new_candidate("Carrier", entity_id="cand-6")],
        )
        existing = [{"label": "settles", "domain": "Payment", "range": "Contract"}]
        payload = (
            '{"relations": ['
            '{"label": "isSettledBy", "domain": "Contract", "range": "Payment"},'
            '{"label": "carries", "domain": "cand-6", "range": "Contract"}]}'
        )
        with patch.object(staged, "call_serving_endpoint") as mock_llm:
            mock_llm.side_effect = [_answer(payload)]
            result = staged.infer_relations(
                host="h",
                token="t",
                endpoint_name="e",
                draft=draft,
                options={"existing_relations": existing},
            )
        user_prompt = mock_llm.call_args_list[0].args[3][1]["content"]
        assert "settles" in user_prompt
        assert [r["label"] for r in result.result["relations"]] == ["carries"]

    def test_trace_identity(self):
        _, mock_llm = self._run([_answer('{"relations": []}')])
        assert mock_llm.call_args_list[0].kwargs["trace_name"] == "owl_generator.relations"

    def test_truncated_then_complete_recovers(self):
        result, mock_llm = self._run(
            [_answer('{"relations": [', "length"), _answer('{"relations": []}')]
        )
        assert result.success is True

    # -----------------------------------------------------------------
    # Live bug fix: id-bracketing. The old catalog rendered ids in
    # brackets (`[cand-6]`) and the closure rule said "reference ids
    # listed above" — the model copied the bracketed token verbatim,
    # which `validate_references` (comparing against the bare id) always
    # rejected. Fix: transport-level enum-constrained `response_format`
    # (structural — a bracketed id becomes impossible on an endpoint
    # that honours it) plus a strengthened bare-id prompt (fallback
    # safety net when the endpoint doesn't honour `response_format`).
    # -----------------------------------------------------------------

    def test_relations_call_uses_enum_constrained_response_format(self):
        draft = _draft()
        payload = '{"relations": []}'
        with patch.object(staged, "call_serving_endpoint") as mock_llm:
            mock_llm.side_effect = [_answer(payload)]
            staged.infer_relations(host="h", token="t", endpoint_name="e", draft=draft)
        call = mock_llm.call_args_list[0]
        assert call.kwargs["tools"] is None
        rf = call.kwargs["response_format"]
        assert rf["type"] == "json_schema"
        item = rf["json_schema"]["schema"]["properties"]["relations"]["items"]
        assert set(item["properties"]["domain"]["enum"]) == draft.closed_entity_ids()
        assert set(item["properties"]["range"]["enum"]) == draft.closed_entity_ids()

    def test_bracketed_id_reference_still_rejected_reject_only(self):
        # Even in the fallback path (no response_format enforcement, or an
        # endpoint that ignores it), a bracketed id must still be rejected
        # outright — never silently stripped/normalized and never
        # resubmitted for a rewrite.
        payload = (
            '{"relations": [{"label": "shipsTo", '
            '"domain": "[cand-6]", "range": "cls-Customer-a1"}]}'
        )
        result, mock_llm = self._run([_answer(payload)])
        assert result.success is False
        assert result.rejected is True
        assert "[cand-6]" in result.rejection_reason
        assert mock_llm.call_count == 1


# ---------------------------------------------------------------------------
# Stage 3: attributes (ordering + closure)
# ---------------------------------------------------------------------------


class TestInferAttributes:
    def _run(self, responses, draft):
        with patch.object(staged, "call_serving_endpoint") as mock_llm:
            mock_llm.side_effect = responses
            result = staged.infer_attributes(
                host="h", token="t", endpoint_name="e", draft=draft
            )
        return result, mock_llm

    def test_blocked_when_relations_not_done(self):
        draft = _draft(relations_done=False)
        result, mock_llm = self._run([_answer('{"attributes": []}')], draft)
        assert result.success is False
        assert result.rejected is True
        # Fails fast at the ordering gate — the LLM is never called.
        assert mock_llm.call_count == 0

    def test_succeeds_when_relations_done(self):
        draft = _draft(relations_done=True)
        payload = (
            '{"attributes": [{"label": "orderDate", '
            '"domain": "cand-6", "datatype": "xsd:date"}]}'
        )
        result, mock_llm = self._run([_answer(payload)], draft)
        assert result.success is True
        assert result.substage == "attributes"

    def test_unknown_domain_rejected(self):
        draft = _draft(relations_done=True)
        payload = (
            '{"attributes": [{"label": "x", '
            '"domain": "cand-nope", "datatype": "xsd:string"}]}'
        )
        result, mock_llm = self._run([_answer(payload)], draft)
        assert result.rejected is True
        assert mock_llm.call_count == 1

    def test_trace_identity(self):
        draft = _draft(relations_done=True)
        _, mock_llm = self._run([_answer('{"attributes": []}')], draft)
        assert (
            mock_llm.call_args_list[0].kwargs["trace_name"]
            == "owl_generator.attributes"
        )

    def test_attributes_call_uses_enum_constrained_response_format(self):
        draft = _draft(relations_done=True)
        _, mock_llm = self._run([_answer('{"attributes": []}')], draft)
        rf = mock_llm.call_args_list[0].kwargs["response_format"]
        item = rf["json_schema"]["schema"]["properties"]["attributes"]["items"]
        assert set(item["properties"]["domain"]["enum"]) == draft.closed_entity_ids()

    # -----------------------------------------------------------------
    # Residual live-reliability bug: JSON double-encoding under
    # `response_format` json_schema (see `schemas._unwrap_double_encoded`).
    # Claude Sonnet endpoints intermittently return a list-typed field's
    # value as a JSON-encoded STRING instead of the raw array. Proves the
    # one-decode tolerance unwraps that shape end-to-end through the real
    # `infer_attributes` orchestrator (schema parse + closure check both
    # still run), and that a shape still wrong after the one tolerated
    # decode is rejected reject-only with exactly one LLM call — no second
    # finalization call either way.
    # -----------------------------------------------------------------

    def test_double_encoded_attributes_field_is_accepted(self):
        draft = _draft(relations_done=True)
        inner = [{"label": "orderDate", "domain": "cand-6", "datatype": "xsd:date"}]
        payload = json.dumps({"attributes": json.dumps(inner)})
        result, mock_llm = self._run([_answer(payload)], draft)
        assert result.success is True
        assert result.result["attributes"][0]["label"] == "orderDate"
        assert mock_llm.call_count == 1

    def test_whole_payload_double_encoded_is_accepted(self):
        draft = _draft(relations_done=True)
        real = {
            "attributes": [
                {"label": "orderDate", "domain": "cand-6", "datatype": "xsd:date"}
            ]
        }
        payload = json.dumps(json.dumps(real))
        result, mock_llm = self._run([_answer(payload)], draft)
        assert result.success is True
        assert result.result["attributes"][0]["label"] == "orderDate"
        assert mock_llm.call_count == 1

    def test_still_invalid_after_one_decode_is_rejected_no_second_call(self):
        # Triple-encoded: one extra decode still yields a string, not a
        # list — rejected reject-only, never a second finalization call.
        draft = _draft(relations_done=True)
        inner = [{"label": "x", "domain": "cand-6", "datatype": "xsd:string"}]
        triple_encoded_field = json.dumps(json.dumps(inner))
        payload = json.dumps({"attributes": triple_encoded_field})
        result, mock_llm = self._run([_answer(payload)], draft)
        assert result.success is False
        assert result.rejected is True
        assert mock_llm.call_count == 1


# ---------------------------------------------------------------------------
# Stage 3: axioms (ordering + no-rewrite-after-reject)
# ---------------------------------------------------------------------------


class TestInferAxioms:
    def _run(self, responses, draft):
        with patch.object(staged, "call_serving_endpoint") as mock_llm:
            mock_llm.side_effect = responses
            result = staged.infer_axioms(
                host="h", token="t", endpoint_name="e", draft=draft
            )
        return result, mock_llm

    def test_blocked_when_attributes_not_done(self):
        draft = _draft(relations_done=True, attributes_done=False)
        result, mock_llm = self._run([_answer('{"axioms": []}')], draft)
        assert result.rejected is True
        assert mock_llm.call_count == 0

    def test_succeeds_when_attributes_done(self):
        draft = _draft(relations_done=True, attributes_done=True)
        payload = (
            '{"axioms": [{"kind": "subClassOf", '
            '"subject": "cand-6", "object": "cls-Customer-a1"}]}'
        )
        result, mock_llm = self._run([_answer(payload)], draft)
        assert result.success is True
        assert result.substage == "axioms"

    def test_orphan_reference_rejected_no_rewrite(self):
        draft = _draft(relations_done=True, attributes_done=True)
        payload = (
            '{"axioms": [{"kind": "disjointWith", '
            '"subject": "cand-6", "object": "cls-UnknownGhost"}]}'
        )
        result, mock_llm = self._run([_answer(payload)], draft)
        assert result.success is False
        assert result.rejected is True
        assert "cls-UnknownGhost" in result.rejection_reason
        assert mock_llm.call_count == 1

    def test_drops_subclassof_for_anchor_that_already_has_parent(self):
        draft = _draft(
            anchors=[
                GenerateEntity.locked_anchor("Party", "Party"),
                GenerateEntity.locked_anchor(
                    "Customer", "Customer", parent="Party"
                ),
            ],
            candidates=[
                GenerateEntity.new_candidate("Agent", entity_id="cand-agent")
            ],
            relations_done=True,
            attributes_done=True,
        )
        payload = json.dumps(
            {
                "axioms": [
                    {
                        "kind": "subClassOf",
                        "subject": "Customer",
                        "object": "cand-agent",
                    }
                ]
            }
        )
        result, mock_llm = self._run([_answer(payload)], draft)
        assert result.success is True
        assert result.result["axioms"] == []
        assert mock_llm.call_count == 1

    def test_axioms_call_uses_enum_constrained_response_format(self):
        draft = _draft(relations_done=True, attributes_done=True)
        _, mock_llm = self._run([_answer('{"axioms": []}')], draft)
        rf = mock_llm.call_args_list[0].kwargs["response_format"]
        item = rf["json_schema"]["schema"]["properties"]["axioms"]["items"]
        assert set(item["properties"]["subject"]["enum"]) == draft.closed_entity_ids()
        assert set(item["properties"]["object"]["enum"]) == draft.closed_entity_ids()
        assert item["properties"]["kind"]["enum"] == [
            "subClassOf",
            "disjointWith",
            "equivalentClass",
        ]


# ---------------------------------------------------------------------------
# Completion response_format degrades transparently when unsupported
# (mirrors TestDetectionResponseFormatFallbackIntegration for Stage 1).
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Paged Stage-3 completion (issue #173)
#
# A single completion call cannot emit more items than fit in the ~8k-token
# output budget — e.g. attributes for a 360-column table, or attributes/
# relations across a 40-entity draft. The substage answer is therefore PAGED:
# the model emits a bounded page and sets ``has_more`` when more items remain;
# the orchestrator asks for the next page until ``has_more`` is false. Pages
# are merged and deduped; schema + entity-closure validation still run on
# every page — reject-only, never a rewrite.
# ---------------------------------------------------------------------------


class TestCompletionPagination:
    def _run_attributes(self, responses, draft=None, options=None):
        draft = draft or _draft(relations_done=True)
        with patch.object(staged, "call_serving_endpoint") as mock_llm:
            mock_llm.side_effect = responses
            result = staged.infer_attributes(
                host="h", token="t", endpoint_name="e", draft=draft, options=options
            )
        return result, mock_llm

    def test_paged_attributes_merge_across_pages(self):
        page1 = (
            '{"attributes": [{"label": "orderDate", "domain": "cand-6", '
            '"datatype": "xsd:date"}], "has_more": true}'
        )
        page2 = (
            '{"attributes": [{"label": "carrierName", "domain": "cand-6", '
            '"datatype": "xsd:string"}], "has_more": false}'
        )
        result, mock_llm = self._run_attributes([_answer(page1), _answer(page2)])
        assert result.success is True
        labels = [a["label"] for a in result.result["attributes"]]
        assert labels == ["orderDate", "carrierName"]
        assert mock_llm.call_count == 2

    def test_paged_attributes_dedupe_repeats_across_pages(self):
        page1 = (
            '{"attributes": [{"label": "orderDate", "domain": "cand-6", '
            '"datatype": "xsd:date"}], "has_more": true}'
        )
        page2 = (
            '{"attributes": [{"label": "orderDate", "domain": "cand-6", '
            '"datatype": "xsd:date"}, {"label": "orderTotal", '
            '"domain": "cand-6", "datatype": "xsd:double"}], "has_more": false}'
        )
        result, _ = self._run_attributes([_answer(page1), _answer(page2)])
        assert result.success is True
        labels = [a["label"] for a in result.result["attributes"]]
        assert labels == ["orderDate", "orderTotal"]

    def test_continuation_message_asks_for_next_page(self):
        page1 = (
            '{"attributes": [{"label": "a", "domain": "cand-6", '
            '"datatype": "xsd:string"}], "has_more": true}'
        )
        page2 = '{"attributes": [], "has_more": false}'
        _, mock_llm = self._run_attributes([_answer(page1), _answer(page2)])
        cont = mock_llm.call_args_list[1].args[3][-1]
        assert cont["role"] == "user"
        assert "next page" in cont["content"].lower()

    def test_has_more_true_with_no_new_items_stops_paging(self):
        # A page that claims has_more but contributes zero NEW items must end
        # the loop — otherwise a stuck model would burn the whole page budget.
        repeats = (
            '{"attributes": [{"label": "orderDate", "domain": "cand-6", '
            '"datatype": "xsd:date"}], "has_more": true}'
        )
        result, mock_llm = self._run_attributes([_answer(repeats)] * 4)
        assert result.success is True
        assert [a["label"] for a in result.result["attributes"]] == ["orderDate"]
        assert mock_llm.call_count == 2

    def test_page_budget_exhausted_fails_clearly(self):
        page = (
            '{"attributes": [{"label": "a%d", "domain": "cand-6", '
            '"datatype": "xsd:string"}], "has_more": true}'
        )
        responses = [_answer(page % i) for i in range(40)]
        result, _ = self._run_attributes(responses)
        assert result.success is False
        assert "page" in result.error.lower()

    def test_truncated_page_retries_with_a_smaller_page(self):
        # A page that ignores the bounded-page contract and still truncates
        # gets ONE retry asking for a smaller page — then fails clearly.
        truncated = _answer('{"attributes": [{"label": "cu', "length")
        page = (
            '{"attributes": [{"label": "orderDate", "domain": "cand-6", '
            '"datatype": "xsd:date"}], "has_more": false}'
        )
        result, mock_llm = self._run_attributes([truncated, _answer(page)])
        assert result.success is True
        assert mock_llm.call_count == 2
        retry_msg = mock_llm.call_args_list[1].args[3][-1]["content"].lower()
        assert "fewer" in retry_msg or "smaller" in retry_msg

    def test_truncated_page_that_never_fits_fails(self):
        truncated = _answer('{"attributes": [{"label": "cu', "length")
        result, _ = self._run_attributes([truncated, truncated])
        assert result.success is False
        assert "truncated" in result.error.lower()

    def test_page_with_out_of_closure_reference_is_rejected(self):
        page1 = (
            '{"attributes": [{"label": "a", "domain": "cand-6", '
            '"datatype": "xsd:string"}], "has_more": true}'
        )
        page2 = (
            '{"attributes": [{"label": "b", "domain": "cand-ghost", '
            '"datatype": "xsd:string"}], "has_more": false}'
        )
        result, mock_llm = self._run_attributes([_answer(page1), _answer(page2)])
        assert result.success is False
        assert result.rejected is True
        assert "cand-ghost" in result.rejection_reason

    def test_paged_relations_merge_and_dedupe_semantic_inverse(self):
        draft = _draft(
            anchors=[
                GenerateEntity.locked_anchor("cls-Customer-a1", "Customer"),
            ],
            candidates=[GenerateEntity.new_candidate("Carrier", entity_id="cand-6")],
        )
        page1 = (
            '{"relations": [{"label": "handles", "domain": "cand-6", '
            '"range": "cls-Customer-a1"}], "has_more": true}'
        )
        # Page 2 restates the same fact as a passive inverse — dropped by the
        # semantic dedupe applied over the merged pages.
        page2 = (
            '{"relations": [{"label": "handled", "domain": '
            '"cls-Customer-a1", "range": "cand-6"}], "has_more": false}'
        )
        with patch.object(staged, "call_serving_endpoint") as mock_llm:
            mock_llm.side_effect = [_answer(page1), _answer(page2)]
            result = staged.infer_relations(
                host="h", token="t", endpoint_name="e", draft=draft
            )
        assert result.success is True
        assert [r["label"] for r in result.result["relations"]] == ["handles"]

    def test_missing_has_more_means_single_page(self):
        # Back-compat: an endpoint/model that ignores the pagination field
        # still completes in one call when has_more is absent.
        payload = (
            '{"attributes": [{"label": "orderDate", "domain": "cand-6", '
            '"datatype": "xsd:date"}]}'
        )
        result, mock_llm = self._run_attributes([_answer(payload)])
        assert result.success is True
        assert mock_llm.call_count == 1

    def test_attributes_schema_requires_has_more(self):
        draft = _draft(relations_done=True)
        _, mock_llm = self._run_attributes([_answer('{"attributes": []}')], draft)
        rf = mock_llm.call_args_list[0].kwargs["response_format"]
        schema = rf["json_schema"]["schema"]
        assert schema["properties"]["has_more"]["type"] == "boolean"
        assert "has_more" in schema["required"]


class TestCompletionResponseFormatFallbackIntegration:
    def test_infer_relations_succeeds_when_response_format_unsupported(self):
        rejection = MagicMock()
        rejection.status_code = 400
        rejection.text = "does not support the response_format parameter"
        http_error = requests.exceptions.HTTPError(response=rejection)

        success_resp = MagicMock()
        success_resp.json.return_value = _answer('{"relations": []}')

        def _retry_side_effect(_url, _headers, payload, timeout=None):
            if payload.get("response_format"):
                raise http_error
            assert "tools" not in payload
            return success_resp

        draft = _draft()
        with patch(
            "agents.engine_base.call_llm_with_retry", side_effect=_retry_side_effect
        ):
            result = staged.infer_relations(
                host="h",
                token="t",
                endpoint_name="dbx-completion-fallback-ep",
                draft=draft,
            )
        assert result.success is True


# ---------------------------------------------------------------------------
# No one-shot default path
# ---------------------------------------------------------------------------


class TestNoOneShotDefault:
    def test_staged_module_does_not_expose_run_agent(self):
        # The one-shot generator must not be reachable via the staged surface.
        assert not hasattr(staged, "run_agent")

    def test_staged_functions_do_not_reference_run_agent(self):
        import inspect

        source = inspect.getsource(staged)
        assert "run_agent" not in source

    def test_staged_functions_do_not_reference_pitfall_rewrite_loop(self):
        # Review fix (item 4): the legacy one-shot bridge's post-generation
        # check_owl_pitfalls -> fix rewrite loop (agent_owl_generator.engine)
        # must never be reachable from the staged path, even indirectly.
        import inspect

        source = inspect.getsource(staged)
        for banned in (
            "tool_check_owl_pitfalls",
            "check_owl_pitfalls",
            "_evaluate_ontology_stage",
            "owl_eval_max_rounds",
            "MAX_OWL_EVAL_ROUNDS",
        ):
            assert banned not in source

    def test_staged_module_never_imports_legacy_engine(self):
        # Checks actual import statements (AST), not prose: the module
        # docstring legitimately *names* agents.agent_owl_generator.engine
        # to document that it is deliberately not imported.
        import ast
        import inspect

        tree = ast.parse(inspect.getsource(staged))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                assert not any(
                    "agent_owl_generator.engine" in alias.name
                    for alias in node.names
                )
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                assert "agent_owl_generator.engine" not in module
                if module == "agents.agent_owl_generator":
                    assert not any(alias.name == "engine" for alias in node.names)

    def test_public_entry_points_exist(self):
        for name in (
            "detect_entities",
            "infer_relations",
            "infer_attributes",
            "infer_axioms",
        ):
            assert callable(getattr(staged, name))


# ---------------------------------------------------------------------------
# Prompt-first pitfalls + lexical alternate labels
# ---------------------------------------------------------------------------


class TestPrompts:
    def test_detection_prompt_lists_anchors_for_dedup(self):
        anchors = [
            GenerateEntity.locked_anchor(
                "cls-Customer-a1", "Customer", alternate_labels=["Client"]
            )
        ]
        text = prompts.build_detection_system_prompt(existing_anchors=anchors)
        assert "Customer" in text
        # Alternate labels are surfaced so the model dedups against synonyms.
        assert "Client" in text

    def test_relations_prompt_includes_alternate_labels_as_lexical_evidence(self):
        draft = _draft(
            candidates=[
                GenerateEntity.new_candidate(
                    "Carrier",
                    entity_id="cand-6",
                    alternate_labels=["Shipper", "Freight Company"],
                )
            ]
        )
        text = prompts.build_relations_user_prompt(draft)
        assert "cand-6" in text
        assert "Shipper" in text

    def test_relations_prompt_lists_existing_relations(self):
        draft = _draft(
            anchors=[
                GenerateEntity.locked_anchor("Payment", "Payment"),
                GenerateEntity.locked_anchor("Contract", "Contract"),
            ]
        )
        text = prompts.build_relations_user_prompt(
            draft,
            existing_relations=[
                {"label": "settles", "domain": "Payment", "range": "Contract"}
            ],
        )
        assert "Payment --settles--> Contract" in text

    def test_relations_prompt_omits_existing_block_when_empty(self):
        text = prompts.build_relations_user_prompt(_draft())
        assert "EXISTING OBJECT PROPERTIES" not in text

    def test_relations_system_prompt_forbids_paraphrases_and_inverses(self):
        lowered = prompts.build_relations_system_prompt().lower()
        assert "paraphrase" in lowered
        assert "active voice" in lowered

    def test_prompts_carry_pitfall_naming_rules_up_front(self):
        # Prompt-first: naming rules live in the stage prompt, not in a
        # post-generation rewrite loop.
        text = prompts.build_relations_system_prompt()
        assert "lowerCamelCase" in text

    def test_completion_prompt_excludes_excluded_candidates(self):
        draft = _draft(
            candidates=[
                GenerateEntity.new_candidate("Carrier", entity_id="cand-6"),
                GenerateEntity.new_candidate(
                    "Invoice", entity_id="cand-4", included=False
                ),
            ]
        )
        text = prompts.build_relations_user_prompt(draft)
        assert "cand-6" in text
        assert "cand-4" not in text

    # -----------------------------------------------------------------
    # Live bug fix: id-bracketing. See TestInferRelations's
    # `test_bracketed_id_reference_still_rejected_reject_only` for the
    # runtime reject-only proof; these pin the prompt-level half of the
    # fix (the strengthened bare-id catalog + closure rule wording that
    # is the fallback safety net when response_format isn't honoured).
    # -----------------------------------------------------------------

    def test_entity_catalog_renders_bare_ids_without_brackets(self):
        draft = _draft(
            anchors=[GenerateEntity.locked_anchor("Agent", "Agent")],
            candidates=[GenerateEntity.new_candidate("Carrier", entity_id="cand-6")],
        )
        text = prompts.build_relations_user_prompt(draft)
        assert "[Agent]" not in text
        assert "[cand-6]" not in text
        assert "id: Agent" in text
        assert "id: cand-6" in text

    def test_closure_rule_forbids_bracketed_or_quoted_ids(self):
        text = prompts.build_relations_system_prompt()
        lowered = text.lower()
        assert "no brackets" in lowered
        assert "no quotes" in lowered
        assert "bare" in lowered
        # The forbidden bracketed form is shown explicitly as a
        # counter-example, so the model can't misinterpret "no brackets"
        # as merely stylistic.
        assert "[agent]" in lowered

    def test_axioms_prompt_forbids_multiple_inheritance(self):
        draft = _draft(
            anchors=[
                GenerateEntity.locked_anchor(
                    "Customer", "Customer", parent="Party"
                ),
                GenerateEntity.locked_anchor("Party", "Party"),
            ],
            relations_done=True,
            attributes_done=True,
        )
        system = prompts.build_axioms_system_prompt()
        user = prompts.build_axioms_user_prompt(draft)
        assert "Single inheritance only" in system
        assert "EXISTING SUBCLASS AXIOMS" in user
        assert "Customer subClassOf Party" in user
        assert "parent: Party" in user

    def test_closure_rule_wording_shared_by_attributes_and_axioms_prompts(self):
        # The strengthened closure rule text is shared via `_CLOSURE_RULE`
        # — every Stage-3 prompt gets the same bare-id requirement.
        for text in (
            prompts.build_attributes_system_prompt(),
            prompts.build_axioms_system_prompt(),
        ):
            lowered = text.lower()
            assert "no brackets" in lowered
            assert "bare" in lowered

    def test_completion_user_prompts_avoid_the_return_json_double_encoding_trigger(
        self,
    ):
        # Live-verification finding (this revision): once completion sends
        # `response_format`, a trailing "Return the <X> JSON." instruction
        # sometimes made `databricks-claude-sonnet-5` double-encode its
        # answer as a JSON *string* nested inside the top-level array key
        # (e.g. `{"relations": "{\"relations\": [...]}"}"`), which fails
        # `parse_relations_payload`. Verified live that "Now infer and
        # provide the X." does not trigger this.
        draft = _draft(relations_done=True, attributes_done=True)
        for text in (
            prompts.build_relations_user_prompt(draft),
            prompts.build_attributes_user_prompt(draft),
            prompts.build_axioms_user_prompt(draft),
        ):
            assert "Return the" not in text
            assert "JSON." not in text
            assert "Now infer and provide the" in text


# ---------------------------------------------------------------------------
# Live Stage-1 detection failure fix: explicit zero-new-candidate contract
# ---------------------------------------------------------------------------
#
# Root cause: a session with every selected table's core entity already a
# locked anchor gives the model no NEW grounded candidate. The prompt said
# "JSON only" but never explicitly defined what to return in that case, so
# the model replied with prose/refusal instead of a structured answer, and
# `detect_entities` correctly (but unhelpfully, from the user's perspective)
# rejected it with "output is not valid JSON". The fix is prompt-only — the
# reject-only architecture and schema (which already accepts an empty list)
# are unchanged.


class TestZeroCandidateContract:
    def test_system_prompt_states_the_exact_empty_json_contract(self):
        anchors = [
            GenerateEntity.locked_anchor("cls-Customer-a1", "Customer"),
            GenerateEntity.locked_anchor("cls-Order-b2", "Order"),
        ]
        text = prompts.build_detection_system_prompt(existing_anchors=anchors)
        # The exact, byte-literal JSON the model must emit when nothing new
        # is grounded — not a paraphrase the model could reinterpret.
        assert '{"candidate_entities": []}' in text

    def test_system_prompt_states_anchors_are_context_not_candidates(self):
        anchors = [GenerateEntity.locked_anchor("cls-Customer-a1", "Customer")]
        text = prompts.build_detection_system_prompt(existing_anchors=anchors)
        lowered = text.lower()
        assert "not candidates" in lowered or "never candidates" in lowered

    def test_system_prompt_forbids_prose_refusal_or_fence_on_empty_result(self):
        anchors = [GenerateEntity.locked_anchor("cls-Customer-a1", "Customer")]
        text = prompts.build_detection_system_prompt(existing_anchors=anchors)
        lowered = text.lower()
        assert "no explanation" in lowered or "no prose" in lowered
        assert "refus" in lowered  # "refusal"/"refuse"

    def test_system_prompt_states_the_zero_candidate_contract_even_with_no_anchors(
        self,
    ):
        # The contract must be stated unconditionally, not only appended to
        # the "existing anchors" branch — a from-scratch domain with zero
        # anchors could still legitimately ground nothing new from a sparse
        # source.
        text = prompts.build_detection_system_prompt(existing_anchors=())
        assert '{"candidate_entities": []}' in text

    def test_system_prompt_forbids_a_reasoning_preamble_before_the_json(self):
        # Live investigation finding: against a real endpoint with a large
        # multi-table source, the model gathered context via tools
        # correctly, then — once forced onto its final (no-tools) turn —
        # wrote a visible step-by-step "Analysis:" preamble before (never
        # reaching) the JSON, which fails to parse. The prompt must forbid
        # this explicitly, not just forbid "explaining an empty result".
        text = prompts.build_detection_system_prompt(existing_anchors=())
        lowered = text.lower()
        assert "first character" in lowered
        assert "analysis" in lowered or "reasoning" in lowered


# ---------------------------------------------------------------------------
# Review fix (item 2): per-tool MLflow tracing for detection tool dispatch
# ---------------------------------------------------------------------------


def _fake_mlflow(spans):
    """Minimal fake ``mlflow`` module recording every span opened/closed,
    across span types (AGENT/LLM/TOOL), so both ``trace_agent`` (already
    wraps ``detect_entities``) and the new per-tool ``trace_tool`` span can
    be exercised in the same scripted run."""

    class _Span:
        def __init__(self, name, span_type):
            self.name = name
            self.span_type = span_type
            self.inputs: dict = {}
            self.outputs: dict = {}

        def set_inputs(self, data):
            self.inputs.update(data)

        def set_outputs(self, data):
            self.outputs.update(data)

        def set_attributes(self, data):
            self.inputs.update(data)

    class _CM:
        def __init__(self, span):
            self._span = span

        def __enter__(self_inner):
            return self_inner._span

        def __exit__(self_inner, *exc):
            spans.append(self_inner._span)
            return False

    def _start_span(name=None, span_type=None):
        return _CM(_Span(name, span_type))

    mock = MagicMock()
    mock.start_span.side_effect = _start_span

    class _SpanType:
        AGENT = "AGENT"
        LLM = "LLM"
        TOOL = "TOOL"

    entities = MagicMock()
    entities.SpanType = _SpanType
    return mock, entities


class TestPerToolTracing:
    """Each Stage-1 tool dispatch gets its own MLflow TOOL span (tool name +
    ok/error status visible on the trace), scoped to the staged detection
    path only — the shared ``dispatch_tool`` helper (used by ~10 other agent
    engines) and the shared tool handlers are untouched."""

    def test_tool_dispatch_gets_a_named_tool_span_with_status(self):
        spans: list = []
        mock_mlflow, mock_entities = _fake_mlflow(spans)
        tracing_mod._TRACING_READY = True
        payload = '{"candidate_entities": [{"canonical_label": "Carrier"}]}'
        try:
            with patch.dict(
                "sys.modules",
                {"mlflow": mock_mlflow, "mlflow.entities": mock_entities},
            ):
                with patch.object(staged, "call_serving_endpoint") as mock_llm:
                    mock_llm.side_effect = [
                        _tool_call("get_metadata"),
                        _answer(""),  # gather-stop (content discarded)
                        _answer(payload),  # schema-enforced finalization
                    ]
                    result = staged.detect_entities(
                        host="https://test.databricks.com",
                        token="tok",
                        endpoint_name="dbx-llm",
                        metadata={"tables": [{"name": "orders", "columns": []}]},
                        guidelines="Generate a CRM ontology.",
                        existing_anchors=[
                            GenerateEntity.locked_anchor("cls-Customer-a1", "Customer")
                        ],
                        registry={"catalog": "main", "schema": "ob", "volume": "documents"},
                    )
        finally:
            tracing_mod._TRACING_READY = False

        assert result.success is True
        tool_spans = [s for s in spans if s.span_type == "TOOL"]
        assert len(tool_spans) == 1
        assert tool_spans[0].name == "tool:get_metadata"
        assert tool_spans[0].inputs["tool_name"] == "get_metadata"
        assert tool_spans[0].outputs["status"] == "ok"

    def test_status_reflects_a_tool_error_result(self):
        spans: list = []
        mock_mlflow, mock_entities = _fake_mlflow(spans)
        tracing_mod._TRACING_READY = True
        payload = '{"candidate_entities": [{"canonical_label": "Carrier"}]}'
        try:
            with patch.dict(
                "sys.modules",
                {"mlflow": mock_mlflow, "mlflow.entities": mock_entities},
            ):
                # get_table_detail with no table_name argument fails cleanly
                # (returns a JSON error), never raises.
                result, _ = _detect(
                    [
                        _tool_call("get_table_detail"),
                        _answer(""),  # gather-stop (content discarded)
                        _answer(payload),  # schema-enforced finalization
                    ]
                )
        finally:
            tracing_mod._TRACING_READY = False

        assert result.success is True
        tool_spans = [s for s in spans if s.span_type == "TOOL"]
        assert len(tool_spans) == 1
        assert tool_spans[0].outputs["status"] == "error"

    def test_no_handler_behaviour_change_when_traced(self):
        """The dispatched tool result is byte-identical whether or not
        tracing is enabled — tracing is observability only."""
        from agents.agent_owl_generator.tools import TOOL_HANDLERS
        from agents.agent_owl_generator.staged import _build_context
        from agents.engine_base import dispatch_tool

        ctx = _build_context(
            host="https://test",
            token="t",
            registry={},
            metadata={"tables": [{"name": "orders", "columns": []}]},
            domain_name=None,
            domain_folder=None,
            domain_version=None,
            warehouse_id=None,
            selected_tables=None,
        )
        direct = dispatch_tool(TOOL_HANDLERS, ctx, "get_metadata", {}, trace_name="x")

        spans: list = []
        mock_mlflow, mock_entities = _fake_mlflow(spans)
        tracing_mod._TRACING_READY = True
        try:
            with patch.dict(
                "sys.modules",
                {"mlflow": mock_mlflow, "mlflow.entities": mock_entities},
            ):
                traced = staged._dispatch_detection_tool(
                    ctx, "get_metadata", {}, trace_name="x"
                )
        finally:
            tracing_mod._TRACING_READY = False

        assert traced == direct
