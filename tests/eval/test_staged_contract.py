"""Focused tests for the staged-contract eval harness.

Review fix (Task 3 findings, item 1): the *detection*-tagged constraint
checks in ``staged_contract.py`` must exercise the real
``staged.detect_entities()`` orchestrator — built from each dataset
example's actual ``input.metadata`` / ``input.corpus`` /
``input.existing_ontology`` — instead of calling
``schemas.parse_detection_payload`` directly with a hand-written literal.
``_c_does_not_parse`` must be based on the tool surface/dispatch actually
observed on that real run, not only a static ``TOOL_DEFINITIONS`` name
check. The deterministic/offline eval stays offline: only
``staged.call_serving_endpoint`` is scripted.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import staged_contract

from agents.agent_owl_generator import staged
from agents.engine_base import AgentStep
from back.objects.ontology.generate.GenerateDraft import GenerateEntity

ROOT = Path(__file__).resolve().parents[2]
DATASET = ROOT / "tests/eval/datasets/agent_owl_generator/baseline.jsonl"


def _load_example(example_id: str) -> dict:
    for line in DATASET.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if row.get("id") == example_id:
            return row
    raise AssertionError(f"example {example_id!r} not found in dataset")


def _constraint(example: dict, kind: str) -> dict:
    return next(c for c in example["expected"]["constraints"] if c["kind"] == kind)


class TestDetectionChecksExerciseRealOrchestrator:
    """Detection-tagged constraints must drive the real staged entry point."""

    def test_excludes_anchor_check_calls_detect_entities(self):
        example = _load_example("staged-detect-new-entities-001")
        constraint = _constraint(example, "excludes_existing_anchor_as_new")
        with patch.object(
            staged, "detect_entities", wraps=staged.detect_entities
        ) as spy:
            assert staged_contract._c_excludes_anchor(example, constraint) is True
        assert spy.call_count == 1

    def test_all_included_check_calls_detect_entities(self):
        example = _load_example("staged-default-inclusion-001")
        with patch.object(
            staged, "detect_entities", wraps=staged.detect_entities
        ) as spy:
            assert staged_contract._c_all_included(example, {}) is True
        assert spy.call_count == 1

    def test_synonyms_check_calls_detect_entities(self):
        example = _load_example("staged-alternate-labels-001")
        constraint = _constraint(example, "synonyms_as_alternate_labels")
        with patch.object(
            staged, "detect_entities", wraps=staged.detect_entities
        ) as spy:
            assert staged_contract._c_synonyms_as_alt(example, constraint) is True
        assert spy.call_count == 1

    def test_min_new_candidates_uses_examples_own_anchors(self):
        """The real orchestrator call must be seeded from the example's
        ``existing_ontology``, not a literal baked into the check."""
        example = _load_example("staged-detect-new-entities-001")
        constraint = _constraint(example, "min_new_candidate_entities")
        with patch.object(
            staged, "detect_entities", wraps=staged.detect_entities
        ) as spy:
            assert staged_contract._c_min_new_candidates(example, constraint) is True
        assert spy.call_count == 1
        _, kwargs = spy.call_args
        anchor_labels = {a.canonical_label for a in kwargs["existing_anchors"]}
        assert anchor_labels == {"Customer"}

    def test_excludes_anchor_alt_check_calls_detect_entities(self):
        example = _load_example("staged-locked-anchor-dedup-001")
        constraint = _constraint(example, "excludes_existing_alternate_label_as_new")
        with patch.object(
            staged, "detect_entities", wraps=staged.detect_entities
        ) as spy:
            assert staged_contract._c_excludes_anchor_alt(example, constraint) is True
        assert spy.call_count == 1


class TestDoesNotParseObservesProductionPath:
    """``does_not_parse`` must react to what actually happened, not only the
    static tool-definition list."""

    def test_detects_a_banned_tool_actually_dispatched(self):
        """Even if a bug caused ``ai_parse_document`` to be dispatched
        despite never appearing in ``TOOL_DEFINITIONS``, the check must
        catch it from the observed run — a purely static name check on
        ``TOOL_DEFINITIONS`` would incorrectly pass here."""
        example = _load_example("staged-detect-new-entities-001")
        constraint = _constraint(example, "does_not_parse")
        fake_result = staged.DetectionResult(
            success=True,
            candidate_entities=[],
            steps=[
                AgentStep(
                    step_type="tool_call", content="{}", tool_name="ai_parse_document"
                )
            ],
        )
        with patch.object(staged, "detect_entities", return_value=fake_result):
            assert staged_contract._c_does_not_parse(example, constraint) is False

    def test_passes_for_the_real_detection_run(self):
        example = _load_example("staged-detect-new-entities-001")
        constraint = _constraint(example, "does_not_parse")
        with patch.object(
            staged, "detect_entities", wraps=staged.detect_entities
        ) as spy:
            assert staged_contract._c_does_not_parse(example, constraint) is True
        assert spy.call_count == 1

    def test_completion_variant_still_checks_tools_is_none(self):
        """The one non-detection ``does_not_parse`` row (completion stage)
        keeps being verified via the real completion orchestrator."""
        example = _load_example("staged-no-reparse-completion-001")
        constraint = _constraint(example, "does_not_parse")
        assert staged_contract._c_does_not_parse(example, constraint) is True


class TestZeroNewCandidatesWhenFullyAnchoredCheck:
    """Live bug regression: a session where every selected table's core
    entity already exists as a locked anchor gives the model no NEW
    grounded candidate, so the only contract-compliant answer is an
    explicit ``{"candidate_entities": []}``. This must be scored by a real
    behavioural check — never fall through the "unmapped constraint kind"
    neutral-1.0 default, which would silently stop catching a future
    regression (e.g. the scripted/real answer drifting back to a
    placeholder candidate or a rejected/malformed response)."""

    def test_kind_is_mapped_to_a_real_behavioural_check(self):
        assert "empty_candidates_when_fully_anchored" in staged_contract._CHECKS

    def test_check_passes_for_the_real_detection_run(self):
        example = _load_example("staged-zero-new-candidates-001")
        constraint = _constraint(example, "empty_candidates_when_fully_anchored")
        check = staged_contract._CHECKS["empty_candidates_when_fully_anchored"]
        with patch.object(
            staged, "detect_entities", wraps=staged.detect_entities
        ) as spy:
            assert check(example, constraint) is True
        assert spy.call_count == 1

    def test_check_fails_when_detection_returns_a_non_empty_list(self):
        """Pins that the check actually inspects the result — a regression
        that starts proposing a candidate again (e.g. a stray "Placeholder")
        must be caught, not masked by a neutral pass."""
        example = _load_example("staged-zero-new-candidates-001")
        constraint = _constraint(example, "empty_candidates_when_fully_anchored")
        fake_result = staged.DetectionResult(
            success=True,
            candidate_entities=[GenerateEntity.new_candidate("Placeholder")],
        )
        check = staged_contract._CHECKS["empty_candidates_when_fully_anchored"]
        with patch.object(staged, "detect_entities", return_value=fake_result):
            assert check(example, constraint) is False

    def test_check_fails_when_detection_is_rejected(self):
        example = _load_example("staged-zero-new-candidates-001")
        constraint = _constraint(example, "empty_candidates_when_fully_anchored")
        fake_result = staged.DetectionResult(
            success=False, error="output is not valid JSON", rejected=True
        )
        check = staged_contract._CHECKS["empty_candidates_when_fully_anchored"]
        with patch.object(staged, "detect_entities", return_value=fake_result):
            assert check(example, constraint) is False

    def test_scripted_builder_produces_the_empty_payload_for_this_row(self):
        """The offline/deterministic scripted LLM answer for this row must
        itself be the empty JSON object — not the generic "Placeholder"
        default the builder falls back to when nothing is `expected.contains`."""
        example = _load_example("staged-zero-new-candidates-001")
        payload = staged_contract._build_scripted_detection_payload(example)
        assert payload == {"candidate_entities": []}

    def test_required_for_dataset_row_floor_coverage(self):
        """The dataset harness (run_agent_owl_generator.py) must require this
        constraint kind to be present at least once, so it cannot silently
        disappear from the dataset in a future edit."""
        import run_agent_owl_generator as runner

        assert (
            "empty_candidates_when_fully_anchored"
            in runner._REQUIRED_STAGED_CONSTRAINT_KINDS
        )


class TestRejectsBracketedIdReferenceCheck:
    """Live bug regression (id-bracketing): the old catalog rendered ids in
    brackets (``[cand-6]``) and the model copied the bracketed token
    verbatim as domain/range, which ``validate_references`` (comparing
    against the *bare* id) always rejected. This must be scored by a real
    behavioural check driving the real ``infer_relations`` orchestrator,
    never fall through the "unmapped constraint kind" neutral-1.0 default."""

    def test_kind_is_mapped_to_a_real_behavioural_check(self):
        assert "rejects_bracketed_id_reference" in staged_contract._CHECKS

    def test_check_passes_for_the_real_completion_run(self):
        example = _load_example("staged-bracketed-id-rejection-001")
        constraint = _constraint(example, "rejects_bracketed_id_reference")
        check = staged_contract._CHECKS["rejects_bracketed_id_reference"]
        with patch.object(
            staged, "infer_relations", wraps=staged.infer_relations
        ) as spy:
            assert check(example, constraint) is True
        assert spy.call_count == 1

    def test_check_fails_when_the_bracketed_id_is_silently_accepted(self):
        """Pins that the check actually inspects the rejection — a
        regression that silently strips brackets (masking the fix) or
        otherwise stops rejecting must be caught, not masked by a neutral
        pass."""
        example = _load_example("staged-bracketed-id-rejection-001")
        constraint = _constraint(example, "rejects_bracketed_id_reference")
        fake_result = staged.CompletionResult(
            success=True, substage="relations", result={"relations": []}
        )
        check = staged_contract._CHECKS["rejects_bracketed_id_reference"]
        with patch.object(staged, "infer_relations", return_value=fake_result):
            assert check(example, constraint) is False

    def test_required_for_dataset_row_floor_coverage(self):
        import run_agent_owl_generator as runner

        assert (
            "rejects_bracketed_id_reference"
            in runner._REQUIRED_STAGED_CONSTRAINT_KINDS
        )
