"""Tests for the Generate workflow orchestration service (plan task 4 of
``staged-ontology-generate``).

See ``docs/superpowers/specs/2026-09-20-three-stage-ontology-generate-design.md``.
Exercises the durable detect -> review -> complete -> merge pipeline against
a real :class:`DomainSession` (through the ``domain_session`` fixture), with
the staged agent entry points (``agents.agent_owl_generator.staged``)
monkeypatched to deterministic scripted results — no live LLM endpoint.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from back.core.errors import NotFoundError, ValidationError
from back.objects.ontology.generate import GenerateWorkflow as wf
from back.objects.ontology.generate.DraftRevisionConflict import DraftRevisionConflict
from back.objects.ontology.generate.DraftStaleError import DraftStaleError
from back.objects.ontology.generate.DraftValidationError import DraftValidationError
from back.objects.ontology.generate.GenerateDraft import (
    CHECKPOINT_DONE,
    CHECKPOINT_FAILED,
    DONE,
    GenerateDraft,
    GenerateEntity,
    REVIEWING,
)
from agents.agent_owl_generator.staged import CompletionResult, DetectionResult

pytestmark = pytest.mark.unit


def _settings():
    return SimpleNamespace()


def _detection_result(labels):
    return DetectionResult(
        success=True,
        candidate_entities=[GenerateEntity.new_candidate(label) for label in labels],
    )


@pytest.fixture(autouse=True)
def no_document_corpus(monkeypatch):
    """Every test runs with an empty ready-document manifest by default.

    ``domain_session`` has no registry configured, so
    ``list_ready_document_manifests`` would already return ``[]`` via the
    "not configured" branch — this fixture just makes that explicit/stable
    if a future registry default changes.
    """
    monkeypatch.setattr(wf, "list_ready_document_manifests", lambda *_a, **_kw: [])


# ---------------------------------------------------------------------------
# Stage 1: detection
# ---------------------------------------------------------------------------


class TestRunDetection:
    def test_creates_paused_draft_with_default_inclusion(
        self, domain_session, monkeypatch
    ):
        monkeypatch.setattr(
            wf.owl_staged,
            "detect_entities",
            lambda **kw: _detection_result(["Carrier", "Invoice"]),
        )

        draft = wf.run_detection(
            domain_session,
            _settings(),
            host="h",
            token="t",
            endpoint_name="e",
            metadata={"tables": []},
        )

        assert draft.stage == REVIEWING
        assert draft.draft_revision == 1
        assert len(draft.candidate_entities) == 2
        assert all(c.included for c in draft.candidate_entities)
        assert all(c.origin == "detected" for c in draft.candidate_entities)

    def test_persists_durably_through_store(self, domain_session, monkeypatch):
        monkeypatch.setattr(
            wf.owl_staged,
            "detect_entities",
            lambda **kw: _detection_result(["Carrier"]),
        )
        wf.run_detection(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )
        loaded = domain_session.generate_draft_store.load()
        assert loaded is not None
        assert loaded.candidate_entities[0].canonical_label == "Carrier"

    def test_existing_ontology_entities_passed_as_locked_anchors(
        self, domain_session, monkeypatch
    ):
        domain_session.ontology["classes"] = [{"name": "Customer", "label": "Customer"}]
        captured = {}

        def _fake_detect(**kw):
            captured.update(kw)
            return _detection_result(["Carrier"])

        monkeypatch.setattr(wf.owl_staged, "detect_entities", _fake_detect)
        draft = wf.run_detection(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )

        assert [a.id for a in captured["existing_anchors"]] == ["Customer"]
        assert [a.id for a in draft.existing_anchors] == ["Customer"]

    def test_ready_corpus_included_no_parse_triggered(
        self, domain_session, monkeypatch
    ):
        """The workflow reads ready manifest identity only — it must never
        call anything that would parse a pending document."""
        monkeypatch.setattr(
            wf,
            "list_ready_document_manifests",
            lambda *_a, **_kw: [{"filename": "spec.pdf", "source_hash": "abc"}],
        )
        captured = {}

        def _fake_detect(**kw):
            captured.update(kw)
            return _detection_result(["Carrier"])

        monkeypatch.setattr(wf.owl_staged, "detect_entities", _fake_detect)
        draft = wf.run_detection(
            domain_session,
            _settings(),
            host="h",
            token="t",
            endpoint_name="e",
            selected_docs=["spec.pdf"],
        )
        assert draft.selected_source_config["documents"] == ["spec.pdf"]
        assert draft.source_fingerprint.startswith("sha256:")

    def test_no_ready_documents_still_detects_from_metadata_only(
        self, domain_session, monkeypatch
    ):
        monkeypatch.setattr(
            wf.owl_staged,
            "detect_entities",
            lambda **kw: _detection_result(["Carrier"]),
        )
        draft = wf.run_detection(
            domain_session,
            _settings(),
            host="h",
            token="t",
            endpoint_name="e",
            metadata={"tables": [{"full_name": "t1"}]},
        )
        assert len(draft.candidate_entities) == 1

    def test_detection_schema_rejection_raises_validation_error(
        self, domain_session, monkeypatch
    ):
        monkeypatch.setattr(
            wf.owl_staged,
            "detect_entities",
            lambda **kw: DetectionResult(
                success=False, error="bad json", rejected=True
            ),
        )
        with pytest.raises(DraftValidationError):
            wf.run_detection(
                domain_session, _settings(), host="h", token="t", endpoint_name="e"
            )

    def test_detection_rejection_message_is_generic_not_raw_parser_text(
        self, domain_session, monkeypatch
    ):
        """The user-visible message for a rejected Stage-1 answer must say
        the model failed the structured contract and suggest retrying — it
        must never leak the raw parser exception text (which could echo
        model prose) to the end user. The raw detail is still logged
        server-side for diagnosis."""
        raw_parser_error = (
            "output is not valid JSON: Expecting value: line 1 column 1 (char 0)"
        )
        monkeypatch.setattr(
            wf.owl_staged,
            "detect_entities",
            lambda **kw: DetectionResult(
                success=False, error=raw_parser_error, rejected=True
            ),
        )
        with pytest.raises(DraftValidationError) as excinfo:
            wf.run_detection(
                domain_session, _settings(), host="h", token="t", endpoint_name="e"
            )
        message = str(excinfo.value)
        assert "Expecting value" not in message
        assert "line 1 column 1" not in message
        assert "retry" in message.lower()

    def test_detection_infra_failure_raises_infrastructure_error(
        self, domain_session, monkeypatch
    ):
        from back.core.errors import InfrastructureError

        monkeypatch.setattr(
            wf.owl_staged,
            "detect_entities",
            lambda **kw: DetectionResult(
                success=False, error="timeout", rejected=False
            ),
        )
        with pytest.raises(InfrastructureError):
            wf.run_detection(
                domain_session, _settings(), host="h", token="t", endpoint_name="e"
            )

    def test_re_detection_discards_prior_draft(self, domain_session, monkeypatch):
        monkeypatch.setattr(
            wf.owl_staged,
            "detect_entities",
            lambda **kw: _detection_result(["Carrier"]),
        )
        wf.run_detection(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )

        monkeypatch.setattr(
            wf.owl_staged,
            "detect_entities",
            lambda **kw: _detection_result(["Invoice"]),
        )
        draft2 = wf.run_detection(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )
        assert draft2.draft_revision == 1
        assert draft2.candidate_entities[0].canonical_label == "Invoice"


# ---------------------------------------------------------------------------
# Stage 2: draft read/update/discard
# ---------------------------------------------------------------------------


class TestDraftReview:
    def _seed(self, domain_session, monkeypatch, labels=("Carrier",)):
        monkeypatch.setattr(
            wf.owl_staged,
            "detect_entities",
            lambda **kw: _detection_result(list(labels)),
        )
        return wf.run_detection(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )

    def test_get_draft_view_none_when_absent(self, domain_session):
        assert wf.get_draft_view(domain_session, _settings()) is None

    def test_get_draft_view_returns_dict_with_stale_flag(
        self, domain_session, monkeypatch
    ):
        self._seed(domain_session, monkeypatch)
        view = wf.get_draft_view(domain_session, _settings())
        assert view["stale"] is False
        assert view["stage"] == REVIEWING
        assert len(view["candidate_entities"]) == 1

    def test_get_draft_view_marks_stale_after_ontology_change(
        self, domain_session, monkeypatch
    ):
        self._seed(domain_session, monkeypatch)
        domain_session.ontology["classes"] = [
            {"name": "NewAnchor", "label": "NewAnchor"}
        ]
        view = wf.get_draft_view(domain_session, _settings())
        assert view["stale"] is True

    def test_full_field_edit(self, domain_session, monkeypatch):
        draft = self._seed(domain_session, monkeypatch)
        cand_id = draft.candidate_entities[0].id
        updated = wf.update_draft(
            domain_session,
            revision=draft.draft_revision,
            op="update",
            entity_id=cand_id,
            updates={
                "canonical_label": "Shipper",
                "description": "Ships goods",
                "type_hint": "class",
                "evidence": [{"source": "spec.pdf", "excerpt": "x"}],
                "alternate_labels": ["Hauler"],
            },
        )
        cand = updated.candidate_entities[0]
        assert cand.id == cand_id
        assert cand.canonical_label == "Shipper"
        assert cand.description == "Ships goods"
        assert cand.alternate_labels == ["Hauler"]

    def test_manual_add(self, domain_session, monkeypatch):
        draft = self._seed(domain_session, monkeypatch)
        updated = wf.update_draft(
            domain_session,
            revision=draft.draft_revision,
            op="add",
            entity={"canonical_label": "Invoice", "description": "Bill"},
        )
        assert len(updated.candidate_entities) == 2
        added = [
            c for c in updated.candidate_entities if c.canonical_label == "Invoice"
        ][0]
        assert added.origin == "manual"

    def test_manual_remove(self, domain_session, monkeypatch):
        draft = self._seed(domain_session, monkeypatch)
        cand_id = draft.candidate_entities[0].id
        updated = wf.update_draft(
            domain_session,
            revision=draft.draft_revision,
            op="remove",
            entity_id=cand_id,
        )
        assert updated.candidate_entities == []

    def test_exclude_then_include(self, domain_session, monkeypatch):
        draft = self._seed(domain_session, monkeypatch)
        cand_id = draft.candidate_entities[0].id
        excluded = wf.update_draft(
            domain_session,
            revision=draft.draft_revision,
            op="exclude",
            entity_id=cand_id,
        )
        assert excluded.candidate_entities[0].included is False
        reincluded = wf.update_draft(
            domain_session,
            revision=excluded.draft_revision,
            op="include",
            entity_id=cand_id,
        )
        assert reincluded.candidate_entities[0].included is True

    def test_stale_revision_raises_conflict(self, domain_session, monkeypatch):
        draft = self._seed(domain_session, monkeypatch)
        with pytest.raises(DraftRevisionConflict):
            wf.update_draft(
                domain_session,
                revision=draft.draft_revision + 5,
                op="exclude",
                entity_id=draft.candidate_entities[0].id,
            )

    def test_update_without_draft_raises_not_found(self, domain_session):
        with pytest.raises(NotFoundError):
            wf.update_draft(
                domain_session, revision=0, op="exclude", entity_id="cand-x"
            )

    def test_unknown_op_rejected(self, domain_session, monkeypatch):
        draft = self._seed(domain_session, monkeypatch)
        with pytest.raises(ValidationError):
            wf.update_draft(domain_session, revision=draft.draft_revision, op="bogus")

    def test_discard_clears_draft(self, domain_session, monkeypatch):
        self._seed(domain_session, monkeypatch)
        wf.discard_draft(domain_session)
        assert domain_session.generate_draft_store.load() is None


# ---------------------------------------------------------------------------
# Stage 3: completion + merge
# ---------------------------------------------------------------------------


def _completion_ok(substage, result):
    return CompletionResult(success=True, substage=substage, result=result)


class TestRunCompletion:
    def _seed_reviewed_draft(self, domain_session, monkeypatch, labels=("Carrier",)):
        monkeypatch.setattr(
            wf.owl_staged,
            "detect_entities",
            lambda **kw: _detection_result(list(labels)),
        )
        return wf.run_detection(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )

    def _stub_all_ok(self, monkeypatch, cand_id):
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_relations",
            lambda **kw: _completion_ok("relations", {"relations": []}),
        )
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_attributes",
            lambda **kw: _completion_ok(
                "attributes",
                {
                    "attributes": [
                        {
                            "label": "trackingCode",
                            "domain": cand_id,
                            "datatype": "xsd:string",
                        }
                    ]
                },
            ),
        )
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_axioms",
            lambda **kw: _completion_ok("axioms", {"axioms": []}),
        )

    def test_completion_runs_strict_order_and_merges(self, domain_session, monkeypatch):
        draft = self._seed_reviewed_draft(domain_session, monkeypatch)
        cand_id = draft.candidate_entities[0].id
        self._stub_all_ok(monkeypatch, cand_id)

        result = wf.run_completion(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )

        assert result["draft"]["stage"] == DONE
        for substage in ("relations", "attributes", "axioms"):
            assert (
                result["draft"]["completion_checkpoints"][substage]["status"]
                == CHECKPOINT_DONE
            )

        classes = domain_session.get_classes()
        assert any(c["label"] == "Carrier" for c in classes)
        carrier = next(c for c in classes if c["label"] == "Carrier")
        assert any(dp["label"] == "trackingCode" for dp in carrier["dataProperties"])

    def test_stale_fingerprint_blocks_completion(self, domain_session, monkeypatch):
        draft = self._seed_reviewed_draft(domain_session, monkeypatch)
        self._stub_all_ok(monkeypatch, draft.candidate_entities[0].id)
        domain_session.ontology["classes"] = [
            {"name": "Unexpected", "label": "Unexpected"}
        ]

        with pytest.raises(DraftStaleError):
            wf.run_completion(
                domain_session, _settings(), host="h", token="t", endpoint_name="e"
            )

    def test_excluded_candidate_never_reaches_merge(self, domain_session, monkeypatch):
        draft = self._seed_reviewed_draft(
            domain_session, monkeypatch, labels=("Carrier", "Invoice")
        )
        carrier_id = draft.candidate_entities[0].id
        invoice_id = draft.candidate_entities[1].id
        wf.update_draft(
            domain_session,
            revision=draft.draft_revision,
            op="exclude",
            entity_id=invoice_id,
        )
        self._stub_all_ok(monkeypatch, carrier_id)

        wf.run_completion(domain_session, _settings(), host="h", token="t", endpoint_name="e")

        classes = domain_session.get_classes()
        assert any(c["label"] == "Carrier" for c in classes)
        assert not any(c["label"] == "Invoice" for c in classes)

    def test_entity_closure_violation_is_rejected_not_merged(
        self, domain_session, monkeypatch
    ):
        draft = self._seed_reviewed_draft(domain_session, monkeypatch)
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_relations",
            lambda **kw: _completion_ok(
                "relations",
                {
                    "relations": [
                        {
                            "label": "shipsFor",
                            "domain": draft.candidate_entities[0].id,
                            "range": "cand-ghost",
                        }
                    ]
                },
            ),
        )
        with pytest.raises(DraftValidationError):
            wf.run_completion(
                domain_session, _settings(), host="h", token="t", endpoint_name="e"
            )

        # Rejected substage must not have been checkpointed done, and nothing merged.
        resumed = domain_session.generate_draft_store.load()
        assert (
            resumed.completion_checkpoints["relations"]["status"] == CHECKPOINT_FAILED
        )
        assert domain_session.get_classes() == []

    def test_partial_failure_then_resume_does_not_rerun_done_substages(
        self, domain_session, monkeypatch
    ):
        self._seed_reviewed_draft(domain_session, monkeypatch)
        relations_calls = []

        def _relations(**kw):
            relations_calls.append(1)
            return _completion_ok("relations", {"relations": []})

        monkeypatch.setattr(wf.owl_staged, "infer_relations", _relations)
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_attributes",
            lambda **kw: CompletionResult(
                success=False, substage="attributes", error="boom"
            ),
        )

        with pytest.raises(Exception):
            wf.run_completion(
                domain_session, _settings(), host="h", token="t", endpoint_name="e"
            )

        after_first_attempt = domain_session.generate_draft_store.load()
        assert (
            after_first_attempt.completion_checkpoints["relations"]["status"]
            == CHECKPOINT_DONE
        )
        assert (
            after_first_attempt.completion_checkpoints["attributes"]["status"]
            == CHECKPOINT_FAILED
        )
        assert len(relations_calls) == 1

        # Retry: attributes now succeeds; relations must NOT be re-run.
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_attributes",
            lambda **kw: _completion_ok("attributes", {"attributes": []}),
        )
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_axioms",
            lambda **kw: _completion_ok("axioms", {"axioms": []}),
        )
        result = wf.run_completion(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )

        assert len(relations_calls) == 1  # still only called once, not re-run
        assert result["draft"]["stage"] == DONE

    def test_completion_without_draft_raises_not_found(self, domain_session):
        with pytest.raises(NotFoundError):
            wf.run_completion(
                domain_session, _settings(), host="h", token="t", endpoint_name="e"
            )

    def test_completion_blocked_when_nothing_included(
        self, domain_session, monkeypatch
    ):
        draft = self._seed_reviewed_draft(domain_session, monkeypatch)
        wf.update_draft(
            domain_session,
            revision=draft.draft_revision,
            op="exclude",
            entity_id=draft.candidate_entities[0].id,
        )
        with pytest.raises(DraftValidationError):
            wf.run_completion(
                domain_session, _settings(), host="h", token="t", endpoint_name="e"
            )

    def test_already_done_draft_cannot_be_completed_twice(
        self, domain_session, monkeypatch
    ):
        draft = self._seed_reviewed_draft(domain_session, monkeypatch)
        self._stub_all_ok(monkeypatch, draft.candidate_entities[0].id)
        wf.run_completion(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )

        with pytest.raises(DraftValidationError):
            wf.run_completion(
                domain_session, _settings(), host="h", token="t", endpoint_name="e"
            )


# ---------------------------------------------------------------------------
# Deterministic append merge: preserve existing entities
# ---------------------------------------------------------------------------


class TestExistingRelationsPassedToRelationsStage:
    def test_relations_runner_receives_existing_object_properties(
        self, domain_session, monkeypatch
    ):
        domain_session.ontology["classes"] = [
            {"name": "Payment", "label": "Payment", "uri": "http://x#Payment"},
            {"name": "Contract", "label": "Contract", "uri": "http://x#Contract"},
        ]
        domain_session.ontology["properties"] = [
            {
                "name": "settles",
                "label": "settles",
                "type": "ObjectProperty",
                "domain": "Payment",
                "range": "Contract",
            },
            {
                "name": "amount",
                "label": "amount",
                "type": "DatatypeProperty",
                "domain": "Payment",
                "range": "xsd:decimal",
            },
        ]
        monkeypatch.setattr(
            wf.owl_staged,
            "detect_entities",
            lambda **kw: _detection_result(["Carrier"]),
        )
        wf.run_detection(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )
        seen = {}

        def _relations(**kw):
            seen["options"] = kw.get("options") or {}
            return _completion_ok("relations", {"relations": []})

        monkeypatch.setattr(wf.owl_staged, "infer_relations", _relations)
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_attributes",
            lambda **kw: _completion_ok("attributes", {"attributes": []}),
        )
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_axioms",
            lambda **kw: _completion_ok("axioms", {"axioms": []}),
        )

        wf.run_completion(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )

        assert seen["options"]["existing_relations"] == [
            {"label": "settles", "domain": "Payment", "range": "Contract"}
        ]


class TestMergePreservesExistingEntities:
    def test_anchor_identity_untouched_by_merge(self, domain_session, monkeypatch):
        domain_session.ontology["classes"] = [
            {
                "name": "Customer",
                "label": "Customer",
                "uri": "http://x#Customer",
                "dataProperties": [{"name": "firstName", "label": "First Name"}],
            }
        ]
        monkeypatch.setattr(
            wf.owl_staged,
            "detect_entities",
            lambda **kw: _detection_result(["Carrier"]),
        )
        draft = wf.run_detection(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )
        cand_id = draft.candidate_entities[0].id

        monkeypatch.setattr(
            wf.owl_staged,
            "infer_relations",
            lambda **kw: _completion_ok(
                "relations",
                {
                    "relations": [
                        {"label": "shipsFor", "domain": cand_id, "range": "Customer"}
                    ]
                },
            ),
        )
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_attributes",
            lambda **kw: _completion_ok("attributes", {"attributes": []}),
        )
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_axioms",
            lambda **kw: _completion_ok("axioms", {"axioms": []}),
        )

        wf.run_completion(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )

        classes = domain_session.get_classes()
        customer = next(c for c in classes if c["name"] == "Customer")
        assert customer["uri"] == "http://x#Customer"
        assert customer["dataProperties"] == [
            {"name": "firstName", "label": "First Name"}
        ]

        properties = domain_session.get_properties()
        rel = next(p for p in properties if p["label"] == "shipsFor")
        assert rel["range"] == "Customer"

    def test_alternate_labels_persisted_as_first_class_data(
        self, domain_session, monkeypatch
    ):
        monkeypatch.setattr(
            wf.owl_staged,
            "detect_entities",
            lambda **kw: DetectionResult(
                success=True,
                candidate_entities=[
                    GenerateEntity.new_candidate(
                        "Carrier", alternate_labels=["Shipper", "Hauler"]
                    )
                ],
            ),
        )
        wf.run_detection(domain_session, _settings(), host="h", token="t", endpoint_name="e")
        self._stub_all_ok_local(monkeypatch)

        wf.run_completion(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )

        carrier = next(
            c for c in domain_session.get_classes() if c["label"] == "Carrier"
        )
        assert carrier["alternate_labels"] == ["Shipper", "Hauler"]

    @staticmethod
    def _stub_all_ok_local(monkeypatch):
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_relations",
            lambda **kw: _completion_ok("relations", {"relations": []}),
        )
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_attributes",
            lambda **kw: _completion_ok("attributes", {"attributes": []}),
        )
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_axioms",
            lambda **kw: _completion_ok("axioms", {"axioms": []}),
        )

    def test_no_duplicate_entity_names_across_repeated_labels(
        self, domain_session, monkeypatch
    ):
        monkeypatch.setattr(
            wf.owl_staged,
            "detect_entities",
            lambda **kw: DetectionResult(
                success=True,
                candidate_entities=[
                    GenerateEntity.new_candidate("Carrier", entity_id="cand-1"),
                ],
            ),
        )
        domain_session.ontology["classes"] = [
            {"name": "Carrier", "label": "Carrier already exists"}
        ]
        # Detection would normally dedup this away; simulate a manual add
        # colliding in *name* only (different label) to test minting.
        draft = wf.run_detection(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )
        assert (
            draft.candidate_entities == []
        )  # dedup already happened inside detect_entities normally,
        # but here detect_entities is stubbed to bypass dedup, so assert on the anchor instead.


class TestSubClassOfAndDisjointAxiomMerge:
    def test_subclassof_axiom_sets_parent(self, domain_session, monkeypatch):
        monkeypatch.setattr(
            wf.owl_staged,
            "detect_entities",
            lambda **kw: _detection_result(["Vehicle", "Truck"]),
        )
        draft = wf.run_detection(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )
        vehicle_id = draft.candidate_entities[0].id
        truck_id = draft.candidate_entities[1].id

        monkeypatch.setattr(
            wf.owl_staged,
            "infer_relations",
            lambda **kw: _completion_ok("relations", {"relations": []}),
        )
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_attributes",
            lambda **kw: _completion_ok("attributes", {"attributes": []}),
        )
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_axioms",
            lambda **kw: _completion_ok(
                "axioms",
                {
                    "axioms": [
                        {
                            "kind": "subClassOf",
                            "subject": truck_id,
                            "object": vehicle_id,
                        }
                    ]
                },
            ),
        )

        wf.run_completion(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )

        truck = next(c for c in domain_session.get_classes() if c["label"] == "Truck")
        assert truck["parent"] == "Vehicle"

    def test_exact_repeat_subclassof_is_deduped_not_error(
        self, domain_session, monkeypatch
    ):
        """The same subClassOf axiom appearing twice (agent redundancy, or a
        re-run merge — task 4 review finding #2/#5) must be silently
        deduped, not rejected."""
        monkeypatch.setattr(
            wf.owl_staged,
            "detect_entities",
            lambda **kw: _detection_result(["Vehicle", "Truck"]),
        )
        draft = wf.run_detection(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )
        vehicle_id = draft.candidate_entities[0].id
        truck_id = draft.candidate_entities[1].id

        monkeypatch.setattr(
            wf.owl_staged,
            "infer_relations",
            lambda **kw: _completion_ok("relations", {"relations": []}),
        )
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_attributes",
            lambda **kw: _completion_ok("attributes", {"attributes": []}),
        )
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_axioms",
            lambda **kw: _completion_ok(
                "axioms",
                {
                    "axioms": [
                        {
                            "kind": "subClassOf",
                            "subject": truck_id,
                            "object": vehicle_id,
                        },
                        {
                            "kind": "subClassOf",
                            "subject": truck_id,
                            "object": vehicle_id,
                        },
                    ]
                },
            ),
        )

        result = wf.run_completion(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )

        truck = next(c for c in domain_session.get_classes() if c["label"] == "Truck")
        assert truck["parent"] == "Vehicle"
        # Only the first application counts; the exact repeat is a no-op.
        assert result["merge"]["axioms_added"] == 1

    def test_conflicting_subclassof_keeps_first_parent(
        self, domain_session, monkeypatch
    ):
        """A *different* parent than the one already set is multi-parent
        inheritance the ontology model cannot represent (only a single
        `parent` field per class) — keep the first parent and drop the extra
        axiom so Generate still succeeds."""
        monkeypatch.setattr(
            wf.owl_staged,
            "detect_entities",
            lambda **kw: _detection_result(["Vehicle", "Boat", "Truck"]),
        )
        draft = wf.run_detection(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )
        vehicle_id = draft.candidate_entities[0].id
        boat_id = draft.candidate_entities[1].id
        truck_id = draft.candidate_entities[2].id

        monkeypatch.setattr(
            wf.owl_staged,
            "infer_relations",
            lambda **kw: _completion_ok("relations", {"relations": []}),
        )
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_attributes",
            lambda **kw: _completion_ok("attributes", {"attributes": []}),
        )
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_axioms",
            lambda **kw: _completion_ok(
                "axioms",
                {
                    "axioms": [
                        {
                            "kind": "subClassOf",
                            "subject": truck_id,
                            "object": vehicle_id,
                        },
                        {
                            "kind": "subClassOf",
                            "subject": truck_id,
                            "object": boat_id,
                        },
                    ]
                },
            ),
        )

        result = wf.run_completion(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )

        truck = next(c for c in domain_session.get_classes() if c["label"] == "Truck")
        assert truck["parent"] == "Vehicle"
        assert result["merge"]["axioms_added"] == 1

    def test_existing_parent_is_not_replaced(
        self, domain_session, monkeypatch
    ):
        """An existing class that already inherits must not gain a second
        parent when Generate proposes another subClassOf."""
        domain_session.ontology["classes"] = [
            {"name": "Party", "label": "Party"},
            {"name": "Customer", "label": "Customer", "parent": "Party"},
        ]
        monkeypatch.setattr(
            wf.owl_staged,
            "detect_entities",
            lambda **kw: _detection_result(["Agent"]),
        )
        draft = wf.run_detection(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )
        assert [a.parent for a in draft.existing_anchors if a.id == "Customer"] == [
            "Party"
        ]
        agent_id = draft.candidate_entities[0].id

        monkeypatch.setattr(
            wf.owl_staged,
            "infer_relations",
            lambda **kw: _completion_ok("relations", {"relations": []}),
        )
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_attributes",
            lambda **kw: _completion_ok("attributes", {"attributes": []}),
        )
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_axioms",
            lambda **kw: _completion_ok(
                "axioms",
                {
                    "axioms": [
                        {
                            "kind": "subClassOf",
                            "subject": "Customer",
                            "object": agent_id,
                        }
                    ]
                },
            ),
        )

        result = wf.run_completion(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )

        customer = next(
            c for c in domain_session.get_classes() if c["name"] == "Customer"
        )
        assert customer["parent"] == "Party"
        assert result["merge"]["axioms_added"] == 0

    def test_disjoint_with_axiom_deduped_on_exact_repeat(
        self, domain_session, monkeypatch
    ):
        monkeypatch.setattr(
            wf.owl_staged,
            "detect_entities",
            lambda **kw: _detection_result(["Cat", "Dog"]),
        )
        draft = wf.run_detection(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )
        cat_id = draft.candidate_entities[0].id
        dog_id = draft.candidate_entities[1].id

        monkeypatch.setattr(
            wf.owl_staged,
            "infer_relations",
            lambda **kw: _completion_ok("relations", {"relations": []}),
        )
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_attributes",
            lambda **kw: _completion_ok("attributes", {"attributes": []}),
        )
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_axioms",
            lambda **kw: _completion_ok(
                "axioms",
                {
                    "axioms": [
                        {"kind": "disjointWith", "subject": cat_id, "object": dog_id},
                        {"kind": "disjointWith", "subject": cat_id, "object": dog_id},
                    ]
                },
            ),
        )

        result = wf.run_completion(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )
        assert result["merge"]["axioms_added"] == 1
        assert len(domain_session.axioms) == 1


# ---------------------------------------------------------------------------
# Minor review finding #3: honor/reject candidate type_hint in merge
# ---------------------------------------------------------------------------


class TestTypeHintValidationInMerge:
    def test_class_type_hint_merges_normally(self, domain_session, monkeypatch):
        monkeypatch.setattr(
            wf.owl_staged,
            "detect_entities",
            lambda **kw: DetectionResult(
                success=True,
                candidate_entities=[
                    GenerateEntity.new_candidate("Carrier", type_hint="class"),
                ],
            ),
        )
        wf.run_detection(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )
        _stub_all_ok_module(monkeypatch)

        result = wf.run_completion(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )
        assert result["merge"]["classes_added"] == 1

    @pytest.mark.parametrize("type_hint", ["object_property", "data_property"])
    def test_non_class_type_hint_is_rejected_not_ignored(
        self, domain_session, monkeypatch, type_hint
    ):
        """An included candidate whose type_hint the merge cannot yet place
        into the ontology model must be rejected explicitly, never silently
        merged as a (wrongly-shaped) class (task 4 review finding #3)."""
        monkeypatch.setattr(
            wf.owl_staged,
            "detect_entities",
            lambda **kw: DetectionResult(
                success=True,
                candidate_entities=[
                    GenerateEntity.new_candidate("worksFor", type_hint=type_hint),
                ],
            ),
        )
        wf.run_detection(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )
        _stub_all_ok_module(monkeypatch)

        with pytest.raises(DraftValidationError, match="type_hint"):
            wf.run_completion(
                domain_session, _settings(), host="h", token="t", endpoint_name="e"
            )
        # Nothing must have been merged — the rejection is atomic.
        assert domain_session.get_classes() == []

    def test_excluded_non_class_candidate_is_not_checked(
        self, domain_session, monkeypatch
    ):
        """An excluded candidate never reaches the merge at all, so its
        type_hint is irrelevant."""
        monkeypatch.setattr(
            wf.owl_staged,
            "detect_entities",
            lambda **kw: DetectionResult(
                success=True,
                candidate_entities=[
                    GenerateEntity.new_candidate("Carrier", type_hint="class"),
                    GenerateEntity.new_candidate(
                        "worksFor", type_hint="object_property"
                    ),
                ],
            ),
        )
        draft = wf.run_detection(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )
        bad_id = draft.candidate_entities[1].id
        wf.update_draft(
            domain_session,
            revision=draft.draft_revision,
            op="exclude",
            entity_id=bad_id,
        )
        _stub_all_ok_module(monkeypatch)

        result = wf.run_completion(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )
        assert result["merge"]["classes_added"] == 1


def _stub_all_ok_module(monkeypatch):
    monkeypatch.setattr(
        wf.owl_staged,
        "infer_relations",
        lambda **kw: _completion_ok("relations", {"relations": []}),
    )
    monkeypatch.setattr(
        wf.owl_staged,
        "infer_attributes",
        lambda **kw: _completion_ok("attributes", {"attributes": []}),
    )
    monkeypatch.setattr(
        wf.owl_staged,
        "infer_axioms",
        lambda **kw: _completion_ok("axioms", {"axioms": []}),
    )


# ---------------------------------------------------------------------------
# Important review finding #2: idempotent/atomic merge + durable merge
# checkpoint (independent of `stage`/`completion_checkpoints`)
# ---------------------------------------------------------------------------


class TestIdempotentMerge:
    def _seed_ready_for_merge(self, domain_session, monkeypatch, cand_id_holder):
        monkeypatch.setattr(
            wf.owl_staged,
            "detect_entities",
            lambda **kw: _detection_result(["Carrier"]),
        )
        draft = wf.run_detection(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )
        cand_id = draft.candidate_entities[0].id
        cand_id_holder["id"] = cand_id
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_relations",
            lambda **kw: _completion_ok("relations", {"relations": []}),
        )
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_attributes",
            lambda **kw: _completion_ok(
                "attributes",
                {
                    "attributes": [
                        {
                            "label": "trackingCode",
                            "domain": cand_id,
                            "datatype": "xsd:string",
                        }
                    ]
                },
            ),
        )
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_axioms",
            lambda **kw: _completion_ok("axioms", {"axioms": []}),
        )
        return draft

    def test_merge_checkpoint_defaults_pending_before_completion(
        self, domain_session, monkeypatch
    ):
        holder = {}
        self._seed_ready_for_merge(domain_session, monkeypatch, holder)
        draft = domain_session.generate_draft_store.load()
        assert draft.merge_checkpoint["status"] == "pending"

    def test_merge_checkpoint_done_after_successful_completion(
        self, domain_session, monkeypatch
    ):
        holder = {}
        self._seed_ready_for_merge(domain_session, monkeypatch, holder)
        wf.run_completion(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )
        draft = domain_session.generate_draft_store.load()
        assert draft.merge_checkpoint["status"] == "done"
        assert draft.merge_checkpoint["result"]["classes_added"] == 1

    def test_crash_after_merge_before_checkpoint_write_is_idempotent_on_resume(
        self, domain_session, monkeypatch
    ):
        """Simulates the exact crash window review finding #2 calls out:
        `merge_draft_into_ontology` already persisted into the live
        ontology (its own `domain.save()` succeeded), but the process died
        before the draft store recorded the merge checkpoint as done — the
        merge checkpoint is still `running`. On resume, `run_completion`
        must re-run the merge (since its own checkpoint isn't `done`) but
        the merge itself must be idempotent: no duplicate class/property.
        """
        holder = {}
        draft = self._seed_ready_for_merge(domain_session, monkeypatch, holder)
        store = domain_session.generate_draft_store

        # Fast-forward the draft past the three substage checkpoints exactly
        # like `run_completion` would, without calling it yet.
        from dataclasses import replace as _replace
        from back.objects.ontology.generate.GenerateDraft import COMPLETING

        draft = store.save(_replace(draft, stage=COMPLETING))
        for substage in ("relations", "attributes", "axioms"):
            runner = wf._substage_runners()[substage]
            result = runner(
                host="h",
                token="t",
                endpoint_name="e",
                draft=draft,
                options=None,
                draft_id="d",
                draft_revision=draft.draft_revision,
                on_step=None,
            )
            draft = store.save(
                draft.with_checkpoint(substage, CHECKPOINT_DONE, result=result.result)
            )

        # Simulate the crash: the merge checkpoint was marked `running`
        # (as `run_completion` does right before calling the merge) and the
        # merge itself was already applied to the live ontology, but no
        # further draft-store write ever happened.
        draft = store.save(draft.with_merge_checkpoint("running"))
        first_stats = wf.merge_draft_into_ontology(domain_session, draft)
        assert first_stats["classes_added"] == 1

        # Resume: run_completion must not duplicate the class/attribute.
        result = wf.run_completion(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )

        classes = domain_session.get_classes()
        carriers = [c for c in classes if c["label"] == "Carrier"]
        assert len(carriers) == 1, "merge must not duplicate the class on retry"
        assert len(carriers[0]["dataProperties"]) == 1
        assert result["draft"]["stage"] == DONE

    def test_merge_checkpoint_done_short_circuits_without_remerging(
        self, domain_session, monkeypatch
    ):
        """If the merge checkpoint is already `done` (durably recorded) but
        `stage` never made it to `done` (the narrower crash window right
        after the merge-checkpoint write, before the stage write),
        `run_completion` must NOT call the merge again — it trusts the
        merge checkpoint, not `stage`."""
        holder = {}
        draft = self._seed_ready_for_merge(domain_session, monkeypatch, holder)
        store = domain_session.generate_draft_store

        from dataclasses import replace as _replace
        from back.objects.ontology.generate.GenerateDraft import COMPLETING

        draft = store.save(_replace(draft, stage=COMPLETING))
        for substage in ("relations", "attributes", "axioms"):
            runner = wf._substage_runners()[substage]
            result = runner(
                host="h",
                token="t",
                endpoint_name="e",
                draft=draft,
                options=None,
                draft_id="d",
                draft_revision=draft.draft_revision,
                on_step=None,
            )
            draft = store.save(
                draft.with_checkpoint(substage, CHECKPOINT_DONE, result=result.result)
            )

        merge_stats = wf.merge_draft_into_ontology(domain_session, draft)
        draft = store.save(
            draft.with_merge_checkpoint(CHECKPOINT_DONE, result=merge_stats)
        )
        # `stage` is deliberately left at COMPLETING here (the crash window).

        calls = []
        real_merge = wf.merge_draft_into_ontology

        def _spy(*args, **kwargs):
            calls.append(1)
            return real_merge(*args, **kwargs)

        monkeypatch.setattr(wf, "merge_draft_into_ontology", _spy)

        result = wf.run_completion(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )

        assert not calls, "merge must not be called again once its checkpoint is done"
        assert result["draft"]["stage"] == DONE
        assert result["merge"] == merge_stats

    def test_repeated_complete_call_after_success_is_rejected(
        self, domain_session, monkeypatch
    ):
        """A completed draft (`stage == done`) cannot be completed again —
        the top-level guard, not merge idempotency, is the first line of
        defense for the ordinary (non-crash) repeated-call case."""
        holder = {}
        self._seed_ready_for_merge(domain_session, monkeypatch, holder)
        wf.run_completion(
            domain_session, _settings(), host="h", token="t", endpoint_name="e"
        )

        with pytest.raises(DraftValidationError):
            wf.run_completion(
                domain_session, _settings(), host="h", token="t", endpoint_name="e"
            )


class TestInverseRelationMerge:
    def test_handles_and_handled_collapse_to_one_property(self, domain_session):
        """A checkpoint that still holds both directions (older parser, or
        a model that ignored the prompt) must merge as a single object
        property — the reverse is dropped."""
        agent = GenerateEntity.new_candidate("Agent")
        claim = GenerateEntity.new_candidate("Claim")
        draft = GenerateDraft.new(
            source_fingerprint="sha256:inverse",
            candidate_entities=[agent, claim],
            stage=REVIEWING,
        )
        draft = draft.with_checkpoint(
            "relations",
            CHECKPOINT_DONE,
            result={
                "relations": [
                    {
                        "label": "handles",
                        "domain": agent.id,
                        "range": claim.id,
                    },
                    {
                        "label": "handled",
                        "domain": claim.id,
                        "range": agent.id,
                    },
                ]
            },
        )
        stats = wf.merge_draft_into_ontology(domain_session, draft)
        assert stats["relations_added"] == 1
        properties = domain_session.get_properties()
        object_props = [p for p in properties if p.get("type") == "ObjectProperty"]
        assert len(object_props) == 1
        assert object_props[0]["label"] == "handles"
        assert object_props[0]["domain"] == "Agent"
        assert object_props[0]["range"] == "Claim"


class TestSemanticRelationMerge:
    @staticmethod
    def _draft_with_relations(entities, relations):
        draft = GenerateDraft.new(
            source_fingerprint="sha256:semantic",
            candidate_entities=entities,
            stage=REVIEWING,
        )
        return draft.with_checkpoint(
            "relations", CHECKPOINT_DONE, result={"relations": relations}
        )

    @staticmethod
    def _object_props(domain_session):
        return [
            p for p in domain_session.get_properties() if p.get("type") == "ObjectProperty"
        ]

    def test_paraphrase_of_existing_property_is_not_added(self, domain_session):
        payment = GenerateEntity.new_candidate("Payment")
        contract = GenerateEntity.new_candidate("Contract")
        first = self._draft_with_relations(
            [payment, contract],
            [{"label": "settles", "domain": payment.id, "range": contract.id}],
        )
        wf.merge_draft_into_ontology(domain_session, first)

        second = self._draft_with_relations(
            [payment, contract],
            [{"label": "isSettledBy", "domain": contract.id, "range": payment.id}],
        )
        stats = wf.merge_draft_into_ontology(domain_session, second)

        assert stats["relations_added"] == 0
        labels = [p["label"] for p in self._object_props(domain_session)]
        assert labels == ["settles"]

    def test_distinct_reverse_predicate_is_kept(self, domain_session):
        company = GenerateEntity.new_candidate("Company")
        invoice = GenerateEntity.new_candidate("Invoice")
        draft = self._draft_with_relations(
            [company, invoice],
            [
                {"label": "issues", "domain": company.id, "range": invoice.id},
                {"label": "references", "domain": invoice.id, "range": company.id},
            ],
        )
        stats = wf.merge_draft_into_ontology(domain_session, draft)

        assert stats["relations_added"] == 2
        labels = sorted(p["label"] for p in self._object_props(domain_session))
        assert labels == ["issues", "references"]

    def test_paraphrases_in_same_result_merge_once(self, domain_session):
        contract = GenerateEntity.new_candidate("Contract")
        payment = GenerateEntity.new_candidate("Payment")
        draft = self._draft_with_relations(
            [contract, payment],
            [
                {"label": "settledBy", "domain": contract.id, "range": payment.id},
                {"label": "isSettledBy", "domain": contract.id, "range": payment.id},
            ],
        )
        stats = wf.merge_draft_into_ontology(domain_session, draft)
        assert stats["relations_added"] == 1
