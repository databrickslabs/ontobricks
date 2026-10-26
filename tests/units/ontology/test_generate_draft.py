"""Tests for the durable Generate draft/contract layer (plan task 2 of
``staged-ontology-generate``).

See ``docs/superpowers/specs/2026-09-20-three-stage-ontology-generate-design.md``.
"""

from dataclasses import replace

import pytest

from back.core.errors import ConflictError, ValidationError
from back.objects.ontology.generate.DraftRevisionConflict import DraftRevisionConflict
from back.objects.ontology.generate.DraftStaleError import DraftStaleError
from back.objects.ontology.generate.DraftValidationError import DraftValidationError
from back.objects.ontology.generate.GenerateDraft import (
    CHECKPOINT_DONE,
    CHECKPOINT_FAILED,
    CHECKPOINT_PENDING,
    CHECKPOINT_RUNNING,
    COMPLETING,
    DETECTING,
    GenerateDraft,
    GenerateEntity,
    ORIGIN_MANUAL,
    REVIEWING,
    build_locked_anchors_from_classes,
    compute_source_fingerprint,
    new_candidate_id,
)
from back.objects.ontology.generate.GenerateDraftStore import GenerateDraftStore


# ---------------------------------------------------------------------------
# GenerateEntity
# ---------------------------------------------------------------------------


class TestGenerateEntityConstruction:
    def test_new_candidate_defaults(self):
        entity = GenerateEntity.new_candidate("Carrier")
        assert entity.canonical_label == "Carrier"
        assert entity.origin == "detected"
        assert entity.included is True
        assert entity.locked is False
        assert entity.id.startswith("cand-")

    def test_new_candidate_manual_origin(self):
        entity = GenerateEntity.new_candidate("Invoice", origin=ORIGIN_MANUAL)
        assert entity.origin == "manual"

    def test_new_candidate_rejects_existing_origin(self):
        with pytest.raises(DraftValidationError):
            GenerateEntity.new_candidate("Carrier", origin="existing")

    def test_locked_anchor_shape(self):
        anchor = GenerateEntity.locked_anchor("Customer", "Customer", alternate_labels=["Client"])
        assert anchor.locked is True
        assert anchor.origin == "existing"
        assert anchor.included is True
        assert anchor.alternate_labels == ["Client"]

    def test_missing_id_rejected(self):
        with pytest.raises(DraftValidationError):
            GenerateEntity(id="", canonical_label="Carrier")

    def test_missing_canonical_label_rejected(self):
        with pytest.raises(DraftValidationError):
            GenerateEntity(id="cand-1", canonical_label="")

    def test_invalid_type_hint_rejected(self):
        with pytest.raises(DraftValidationError):
            GenerateEntity(id="cand-1", canonical_label="Carrier", type_hint="bogus")

    def test_invalid_origin_rejected(self):
        with pytest.raises(DraftValidationError):
            GenerateEntity(id="cand-1", canonical_label="Carrier", origin="bogus")

    def test_locked_requires_existing_origin(self):
        with pytest.raises(DraftValidationError):
            GenerateEntity(
                id="cand-1", canonical_label="Carrier", origin="detected", locked=True
            )

    def test_locked_requires_included(self):
        with pytest.raises(DraftValidationError):
            GenerateEntity(
                id="cls-1",
                canonical_label="Customer",
                origin="existing",
                locked=True,
                included=False,
            )


class TestGenerateEntitySerialization:
    def test_round_trip(self):
        entity = GenerateEntity.new_candidate(
            "Carrier",
            description="Ships orders",
            evidence=[{"source": "spec.pdf", "excerpt": "Order ships via Carrier."}],
            alternate_labels=["Shipper", "Freight Company"],
        )
        restored = GenerateEntity.from_dict(entity.to_dict())
        assert restored == entity

    def test_from_dict_heals_unknown_type_hint(self):
        restored = GenerateEntity.from_dict(
            {"id": "cand-1", "canonical_label": "Carrier", "type_hint": "unknown_future_type"}
        )
        assert restored.type_hint == "class"

    def test_from_dict_heals_unknown_origin_for_unlocked(self):
        restored = GenerateEntity.from_dict(
            {"id": "cand-1", "canonical_label": "Carrier", "origin": "unknown_future_origin"}
        )
        assert restored.origin == "detected"

    def test_from_dict_heals_unknown_origin_for_locked(self):
        restored = GenerateEntity.from_dict(
            {
                "id": "cls-1",
                "canonical_label": "Customer",
                "origin": "unknown_future_origin",
                "locked": True,
            }
        )
        assert restored.origin == "existing"

    def test_from_dict_forces_included_true_for_locked(self):
        restored = GenerateEntity.from_dict(
            {
                "id": "cls-1",
                "canonical_label": "Customer",
                "locked": True,
                "included": False,
            }
        )
        assert restored.included is True

    def test_from_dict_defaults_missing_fields(self):
        restored = GenerateEntity.from_dict({"id": "cand-1", "canonical_label": "Carrier"})
        assert restored.description == ""
        assert restored.evidence == []
        assert restored.alternate_labels == []
        assert restored.origin == "detected"
        assert restored.included is True
        assert restored.locked is False
        assert restored.parent == ""


class TestGenerateEntityEdits:
    def test_with_updates_renames_but_keeps_id(self):
        entity = GenerateEntity.new_candidate("Carrier")
        renamed = entity.with_updates(canonical_label="Shipper")
        assert renamed.id == entity.id
        assert renamed.canonical_label == "Shipper"

    def test_with_updates_rejects_id_change(self):
        entity = GenerateEntity.new_candidate("Carrier")
        with pytest.raises(DraftValidationError):
            entity.with_updates(id="cand-different")

    def test_with_updates_rejects_locked_change(self):
        entity = GenerateEntity.new_candidate("Carrier")
        with pytest.raises(DraftValidationError):
            entity.with_updates(locked=True)

    def test_with_updates_on_locked_anchor_rejected(self):
        anchor = GenerateEntity.locked_anchor("Customer", "Customer")
        with pytest.raises(DraftValidationError):
            anchor.with_updates(canonical_label="Client")

    def test_with_updates_alternate_labels(self):
        entity = GenerateEntity.new_candidate("Carrier")
        updated = entity.with_updates(alternate_labels=["Shipper", "Freight Co"])
        assert updated.alternate_labels == ["Shipper", "Freight Co"]

    def test_with_updates_rejects_invalid_type_hint(self):
        """with_updates is strict construction, not the from_dict migration
        healing boundary: a caller-supplied invalid enum value must raise,
        never be silently coerced back to a default."""
        entity = GenerateEntity.new_candidate("Carrier")
        with pytest.raises(DraftValidationError):
            entity.with_updates(type_hint="not_a_real_type")

    def test_with_updates_rejects_empty_canonical_label(self):
        entity = GenerateEntity.new_candidate("Carrier")
        with pytest.raises(DraftValidationError):
            entity.with_updates(canonical_label="")

    def test_with_candidate_updated_rejects_invalid_type_hint(self):
        """Same strictness through the draft-level review mutation path."""
        cand = GenerateEntity.new_candidate("Carrier", entity_id="cand-1")
        draft = GenerateDraft(
            draft_revision=0, stage=REVIEWING, source_fingerprint="fp", candidate_entities=[cand]
        )
        with pytest.raises(DraftValidationError):
            draft.with_candidate_updated("cand-1", type_hint="bogus_type")

    def test_normalized_labels_includes_alternates_casefolded(self):
        entity = GenerateEntity.new_candidate(
            "Carrier", alternate_labels=["  Shipper  ", "FREIGHT co"]
        )
        assert entity.normalized_labels() == {"carrier", "shipper", "freight co"}


class TestBuildLockedAnchorsFromClasses:
    def test_builds_anchor_per_class_using_name_as_id(self):
        classes = [
            {"name": "Customer", "label": "Customer"},
            {"name": "Order", "label": "Sales Order"},
        ]
        anchors = build_locked_anchors_from_classes(classes)
        assert [a.id for a in anchors] == ["Customer", "Order"]
        assert anchors[1].canonical_label == "Sales Order"
        assert all(a.locked for a in anchors)

    def test_skips_classes_without_name(self):
        anchors = build_locked_anchors_from_classes([{"label": "NoName"}])
        assert anchors == []

    def test_empty_classes(self):
        assert build_locked_anchors_from_classes([]) == []
        assert build_locked_anchors_from_classes(None) == []

    def test_copies_existing_parent(self):
        anchors = build_locked_anchors_from_classes(
            [
                {"name": "Party", "label": "Party"},
                {"name": "Customer", "label": "Customer", "parent": "Party"},
            ]
        )
        by_id = {a.id: a for a in anchors}
        assert by_id["Party"].parent == ""
        assert by_id["Customer"].parent == "Party"


# ---------------------------------------------------------------------------
# Source fingerprinting
# ---------------------------------------------------------------------------


class TestSourceFingerprint:
    def _anchors(self):
        return [GenerateEntity.locked_anchor("Customer", "Customer")]

    def test_deterministic_for_same_inputs(self):
        cfg = {"tables": ["demo.sales.orders"], "documents": ["spec.pdf"]}
        docs = [{"filename": "spec.pdf", "source_hash": "abc123"}]
        fp1 = compute_source_fingerprint(
            selected_source_config=cfg, ready_documents=docs, existing_anchors=self._anchors()
        )
        fp2 = compute_source_fingerprint(
            selected_source_config=cfg, ready_documents=docs, existing_anchors=self._anchors()
        )
        assert fp1 == fp2
        assert fp1.startswith("sha256:")

    def test_order_independent(self):
        cfg_a = {"tables": ["t1", "t2"], "documents": []}
        cfg_b = {"tables": ["t2", "t1"], "documents": []}
        fp_a = compute_source_fingerprint(selected_source_config=cfg_a, existing_anchors=[])
        fp_b = compute_source_fingerprint(selected_source_config=cfg_b, existing_anchors=[])
        assert fp_a == fp_b

    def test_changed_metadata_changes_fingerprint(self):
        fp1 = compute_source_fingerprint(
            selected_source_config={"tables": ["t1"]}, existing_anchors=[]
        )
        fp2 = compute_source_fingerprint(
            selected_source_config={"tables": ["t1", "t2"]}, existing_anchors=[]
        )
        assert fp1 != fp2

    def test_changed_document_hash_changes_fingerprint(self):
        cfg = {"tables": [], "documents": ["spec.pdf"]}
        fp1 = compute_source_fingerprint(
            selected_source_config=cfg,
            ready_documents=[{"filename": "spec.pdf", "source_hash": "aaa"}],
            existing_anchors=[],
        )
        fp2 = compute_source_fingerprint(
            selected_source_config=cfg,
            ready_documents=[{"filename": "spec.pdf", "source_hash": "bbb"}],
            existing_anchors=[],
        )
        assert fp1 != fp2

    def test_changed_ontology_identity_changes_fingerprint(self):
        fp1 = compute_source_fingerprint(
            selected_source_config={}, existing_anchors=self._anchors()
        )
        fp2 = compute_source_fingerprint(
            selected_source_config={},
            existing_anchors=[GenerateEntity.locked_anchor("Order", "Order")],
        )
        assert fp1 != fp2

    def test_is_stale(self):
        draft = GenerateDraft.new(source_fingerprint="sha256:aaa")
        assert draft.is_stale("sha256:bbb") is True
        assert draft.is_stale("sha256:aaa") is False

    def test_ensure_not_stale_raises_on_mismatch(self):
        draft = GenerateDraft.new(source_fingerprint="sha256:aaa")
        with pytest.raises(DraftStaleError):
            draft.ensure_not_stale("sha256:bbb")

    def test_ensure_not_stale_passes_on_match(self):
        draft = GenerateDraft.new(source_fingerprint="sha256:aaa")
        draft.ensure_not_stale("sha256:aaa")  # does not raise

    def test_stale_error_is_conflict(self):
        assert isinstance(DraftStaleError("x"), ConflictError)


# ---------------------------------------------------------------------------
# GenerateDraft construction / validation
# ---------------------------------------------------------------------------


class TestGenerateDraftConstruction:
    def test_new_draft_defaults(self):
        draft = GenerateDraft.new(source_fingerprint="sha256:xyz")
        assert draft.draft_revision == 0
        assert draft.stage == REVIEWING
        assert draft.existing_anchors == []
        assert draft.candidate_entities == []
        for substage in ("relations", "attributes", "axioms"):
            assert draft.completion_checkpoints[substage]["status"] == "pending"
            assert draft.completion_checkpoints[substage]["result"] is None

    def test_invalid_stage_rejected(self):
        with pytest.raises(DraftValidationError):
            GenerateDraft(draft_revision=0, stage="not_a_stage", source_fingerprint="fp")

    def test_negative_revision_rejected(self):
        with pytest.raises(DraftValidationError):
            GenerateDraft(draft_revision=-1, stage=DETECTING, source_fingerprint="fp")

    def test_existing_anchors_must_be_locked(self):
        unlocked = GenerateEntity.new_candidate("Customer")
        with pytest.raises(DraftValidationError):
            GenerateDraft(
                draft_revision=0,
                stage=REVIEWING,
                source_fingerprint="fp",
                existing_anchors=[unlocked],
            )

    def test_candidates_must_not_be_locked(self):
        anchor = GenerateEntity.locked_anchor("Customer", "Customer")
        with pytest.raises(DraftValidationError):
            GenerateDraft(
                draft_revision=0,
                stage=REVIEWING,
                source_fingerprint="fp",
                candidate_entities=[anchor],
            )

    def test_duplicate_ids_rejected(self):
        cand1 = GenerateEntity.new_candidate("Carrier", entity_id="cand-1")
        cand2 = GenerateEntity.new_candidate("Shipper", entity_id="cand-1")
        with pytest.raises(DraftValidationError):
            GenerateDraft(
                draft_revision=0,
                stage=REVIEWING,
                source_fingerprint="fp",
                candidate_entities=[cand1, cand2],
            )


class TestGenerateDraftUniqueLabels:
    def test_duplicate_canonical_labels_rejected(self):
        cand1 = GenerateEntity.new_candidate("Carrier", entity_id="cand-1")
        cand2 = GenerateEntity.new_candidate("carrier", entity_id="cand-2")
        with pytest.raises(DraftValidationError):
            GenerateDraft(
                draft_revision=0,
                stage=REVIEWING,
                source_fingerprint="fp",
                candidate_entities=[cand1, cand2],
            )

    def test_candidate_alternate_label_colliding_with_anchor_label_rejected(self):
        anchor = GenerateEntity.locked_anchor("cls-Customer", "Customer", alternate_labels=["Client"])
        cand = GenerateEntity.new_candidate("Carrier", alternate_labels=["Client"])
        with pytest.raises(DraftValidationError):
            GenerateDraft(
                draft_revision=0,
                stage=REVIEWING,
                source_fingerprint="fp",
                existing_anchors=[anchor],
                candidate_entities=[cand],
            )

    def test_two_candidates_colliding_on_alternate_labels_rejected(self):
        cand1 = GenerateEntity.new_candidate("Carrier", alternate_labels=["Shipper"])
        cand2 = GenerateEntity.new_candidate("Hauler", alternate_labels=["shipper "])
        with pytest.raises(DraftValidationError):
            GenerateDraft(
                draft_revision=0,
                stage=REVIEWING,
                source_fingerprint="fp",
                candidate_entities=[cand1, cand2],
            )

    def test_excluded_candidate_still_counted_for_uniqueness(self):
        cand1 = GenerateEntity.new_candidate("Carrier", included=False)
        cand2 = GenerateEntity.new_candidate("carrier")
        with pytest.raises(DraftValidationError):
            GenerateDraft(
                draft_revision=0,
                stage=REVIEWING,
                source_fingerprint="fp",
                candidate_entities=[cand1, cand2],
            )

    def test_distinct_labels_ok(self):
        anchor = GenerateEntity.locked_anchor("cls-Customer", "Customer")
        cand = GenerateEntity.new_candidate("Carrier")
        draft = GenerateDraft(
            draft_revision=0,
            stage=REVIEWING,
            source_fingerprint="fp",
            existing_anchors=[anchor],
            candidate_entities=[cand],
        )
        assert len(draft.existing_anchors) == 1
        assert len(draft.candidate_entities) == 1


# ---------------------------------------------------------------------------
# GenerateDraft serialization / migration
# ---------------------------------------------------------------------------


class TestGenerateDraftSerialization:
    def _sample(self):
        anchor = GenerateEntity.locked_anchor("cls-Customer", "Customer", alternate_labels=["Client"])
        cand = GenerateEntity.new_candidate("Carrier", entity_id="cand-1", alternate_labels=["Shipper"])
        return GenerateDraft(
            draft_revision=3,
            stage=COMPLETING,
            source_fingerprint="sha256:abc",
            selected_source_config={"tables": ["t1"], "documents": ["d1.pdf"]},
            existing_anchors=[anchor],
            candidate_entities=[cand],
        )

    def test_round_trip(self):
        draft = self._sample()
        restored = GenerateDraft.from_dict(draft.to_dict())
        assert restored == draft

    def test_from_dict_tolerates_missing_keys(self):
        restored = GenerateDraft.from_dict({})
        assert restored.draft_revision == 0
        assert restored.stage == DETECTING
        assert restored.source_fingerprint == ""
        assert restored.existing_anchors == []
        assert restored.candidate_entities == []
        for substage in ("relations", "attributes", "axioms"):
            assert restored.completion_checkpoints[substage]["status"] == "pending"

    def test_from_dict_heals_unknown_stage(self):
        restored = GenerateDraft.from_dict({"stage": "some_future_stage"})
        assert restored.stage == DETECTING

    def test_from_dict_heals_unknown_checkpoint_status(self):
        restored = GenerateDraft.from_dict(
            {"completion_checkpoints": {"relations": {"status": "some_future_status"}}}
        )
        assert restored.completion_checkpoints["relations"]["status"] == "pending"

    def test_from_dict_heals_missing_checkpoint_substage(self):
        restored = GenerateDraft.from_dict(
            {"completion_checkpoints": {"relations": {"status": "done", "result": {"x": 1}}}}
        )
        assert restored.completion_checkpoints["relations"]["status"] == "done"
        assert restored.completion_checkpoints["attributes"]["status"] == "pending"
        assert restored.completion_checkpoints["axioms"]["status"] == "pending"

    def test_from_dict_normalizes_checkpoint_chain_when_later_stage_impossibly_done(self):
        """A persisted draft cannot have 'attributes' done while its
        predecessor 'relations' is pending — that ordering could never have
        happened through with_checkpoint(). Deserialization must repair
        this rather than resurrect an impossible state."""
        restored = GenerateDraft.from_dict(
            {
                "completion_checkpoints": {
                    "relations": {"status": "pending", "result": None},
                    "attributes": {"status": "done", "result": {"attrs": []}},
                    "axioms": {"status": "pending", "result": None},
                }
            }
        )
        assert restored.completion_checkpoints["relations"]["status"] == "pending"
        assert restored.completion_checkpoints["attributes"]["status"] == "pending"
        assert restored.completion_checkpoints["attributes"]["result"] is None
        assert restored.completion_checkpoints["axioms"]["status"] == "pending"

    def test_from_dict_normalizes_checkpoint_chain_cascades_to_axioms(self):
        """relations failed → attributes must not remain 'done', and the
        break must cascade: axioms cannot remain 'done' either."""
        restored = GenerateDraft.from_dict(
            {
                "completion_checkpoints": {
                    "relations": {"status": "failed", "result": None},
                    "attributes": {"status": "done", "result": {"attrs": []}},
                    "axioms": {"status": "done", "result": {"axioms": []}},
                }
            }
        )
        assert restored.completion_checkpoints["relations"]["status"] == "failed"
        assert restored.completion_checkpoints["attributes"]["status"] == "pending"
        assert restored.completion_checkpoints["axioms"]["status"] == "pending"
        assert restored.completion_checkpoints["axioms"]["result"] is None

    def test_from_dict_preserves_valid_in_progress_chain(self):
        """A legitimately valid chain (relations done, attributes running,
        axioms pending) must be preserved as-is, not over-corrected."""
        restored = GenerateDraft.from_dict(
            {
                "completion_checkpoints": {
                    "relations": {"status": "done", "result": {"rel": []}},
                    "attributes": {"status": "running", "result": None},
                    "axioms": {"status": "pending", "result": None},
                }
            }
        )
        assert restored.completion_checkpoints["relations"]["status"] == "done"
        assert restored.completion_checkpoints["relations"]["result"] == {"rel": []}
        assert restored.completion_checkpoints["attributes"]["status"] == "running"
        assert restored.completion_checkpoints["axioms"]["status"] == "pending"

    def test_from_dict_preserves_fully_done_chain(self):
        restored = GenerateDraft.from_dict(
            {
                "completion_checkpoints": {
                    "relations": {"status": "done", "result": {}},
                    "attributes": {"status": "done", "result": {}},
                    "axioms": {"status": "done", "result": {}},
                }
            }
        )
        assert all(
            restored.completion_checkpoints[s]["status"] == "done"
            for s in ("relations", "attributes", "axioms")
        )

    def test_selected_source_config_is_deep_copied(self):
        cfg = {"tables": ["t1"]}
        draft = GenerateDraft.new(source_fingerprint="fp", selected_source_config=cfg)
        cfg["tables"].append("t2")
        assert draft.selected_source_config["tables"] == ["t1"]


# ---------------------------------------------------------------------------
# Rename identity stability + manual add/remove/exclude
# ---------------------------------------------------------------------------


class TestGenerateDraftReviewMutations:
    def test_rename_preserves_id_across_stage(self):
        cand = GenerateEntity.new_candidate("Carrier", entity_id="cand-1")
        draft = GenerateDraft(
            draft_revision=0, stage=REVIEWING, source_fingerprint="fp", candidate_entities=[cand]
        )
        renamed = draft.with_candidate_updated("cand-1", canonical_label="Shipper")
        assert renamed.candidate_entities[0].id == "cand-1"
        assert renamed.candidate_entities[0].canonical_label == "Shipper"

    def test_rename_unknown_candidate_raises(self):
        draft = GenerateDraft.new(source_fingerprint="fp")
        with pytest.raises(DraftValidationError):
            draft.with_candidate_updated("cand-missing", canonical_label="X")

    def test_manual_addition(self):
        draft = GenerateDraft.new(source_fingerprint="fp")
        manual = GenerateEntity.new_candidate("Invoice", origin=ORIGIN_MANUAL)
        updated = draft.with_candidate_added(manual)
        assert len(updated.candidate_entities) == 1
        assert updated.candidate_entities[0].origin == "manual"

    def test_add_duplicate_id_raises(self):
        cand = GenerateEntity.new_candidate("Carrier", entity_id="cand-1")
        draft = GenerateDraft(
            draft_revision=0, stage=REVIEWING, source_fingerprint="fp", candidate_entities=[cand]
        )
        dup = GenerateEntity.new_candidate("Other", entity_id="cand-1")
        with pytest.raises(DraftValidationError):
            draft.with_candidate_added(dup)

    def test_add_locked_entity_as_candidate_raises(self):
        draft = GenerateDraft.new(source_fingerprint="fp")
        anchor = GenerateEntity.locked_anchor("cls-1", "Customer")
        with pytest.raises(DraftValidationError):
            draft.with_candidate_added(anchor)

    def test_removal_deletes_candidate(self):
        cand = GenerateEntity.new_candidate("Carrier", entity_id="cand-1")
        draft = GenerateDraft(
            draft_revision=0, stage=REVIEWING, source_fingerprint="fp", candidate_entities=[cand]
        )
        updated = draft.with_candidate_removed("cand-1")
        assert updated.candidate_entities == []

    def test_remove_unknown_raises(self):
        draft = GenerateDraft.new(source_fingerprint="fp")
        with pytest.raises(DraftValidationError):
            draft.with_candidate_removed("cand-missing")

    def test_exclude_keeps_candidate_but_removes_from_closed_set(self):
        cand = GenerateEntity.new_candidate("Carrier", entity_id="cand-1")
        draft = GenerateDraft(
            draft_revision=0, stage=REVIEWING, source_fingerprint="fp", candidate_entities=[cand]
        )
        excluded = draft.with_candidate_updated("cand-1", included=False)
        assert len(excluded.candidate_entities) == 1
        assert excluded.candidate_entities[0].included is False
        assert "cand-1" not in excluded.closed_entity_ids()

    def test_reincluding_previously_excluded_candidate(self):
        cand = GenerateEntity.new_candidate("Carrier", entity_id="cand-1", included=False)
        draft = GenerateDraft(
            draft_revision=0, stage=REVIEWING, source_fingerprint="fp", candidate_entities=[cand]
        )
        reincluded = draft.with_candidate_updated("cand-1", included=True)
        assert "cand-1" in reincluded.closed_entity_ids()

    def test_locked_anchor_immutable_via_review_mutation(self):
        # Locked anchors are not editable through with_candidate_updated at
        # all (they never live in candidate_entities) — assert directly on
        # the entity-level guard instead.
        anchor = GenerateEntity.locked_anchor("cls-1", "Customer")
        with pytest.raises(DraftValidationError):
            anchor.with_updates(canonical_label="Client")


# ---------------------------------------------------------------------------
# Minimum-set invariant
# ---------------------------------------------------------------------------


class TestEnsureReadyForCompletion:
    def test_raises_when_nothing_included(self):
        draft = GenerateDraft.new(source_fingerprint="fp")
        with pytest.raises(DraftValidationError):
            draft.ensure_ready_for_completion()

    def test_raises_when_all_candidates_excluded_and_no_anchors(self):
        cand = GenerateEntity.new_candidate("Carrier", included=False)
        draft = GenerateDraft(
            draft_revision=0, stage=REVIEWING, source_fingerprint="fp", candidate_entities=[cand]
        )
        with pytest.raises(DraftValidationError):
            draft.ensure_ready_for_completion()

    def test_passes_with_one_anchor_even_if_all_candidates_excluded(self):
        anchor = GenerateEntity.locked_anchor("cls-1", "Customer")
        cand = GenerateEntity.new_candidate("Carrier", included=False)
        draft = GenerateDraft(
            draft_revision=0,
            stage=REVIEWING,
            source_fingerprint="fp",
            existing_anchors=[anchor],
            candidate_entities=[cand],
        )
        draft.ensure_ready_for_completion()  # does not raise

    def test_passes_with_zero_anchors_but_one_included_candidate(self):
        cand = GenerateEntity.new_candidate("Carrier", included=True)
        draft = GenerateDraft(
            draft_revision=0, stage=REVIEWING, source_fingerprint="fp", candidate_entities=[cand]
        )
        draft.ensure_ready_for_completion()  # does not raise


# ---------------------------------------------------------------------------
# Invalid / entity-closure references
# ---------------------------------------------------------------------------


class TestValidateReferences:
    def _draft(self):
        anchor = GenerateEntity.locked_anchor("cls-Customer", "Customer")
        included = GenerateEntity.new_candidate("Carrier", entity_id="cand-1", included=True)
        excluded = GenerateEntity.new_candidate("Invoice", entity_id="cand-2", included=False)
        return GenerateDraft(
            draft_revision=0,
            stage=COMPLETING,
            source_fingerprint="fp",
            existing_anchors=[anchor],
            candidate_entities=[included, excluded],
        )

    def test_allows_anchor_and_included_candidate(self):
        draft = self._draft()
        draft.validate_references(["cls-Customer", "cand-1"])  # does not raise

    def test_rejects_excluded_candidate_reference(self):
        draft = self._draft()
        with pytest.raises(DraftValidationError):
            draft.validate_references(["cand-2"])

    def test_rejects_unknown_reference(self):
        draft = self._draft()
        with pytest.raises(DraftValidationError):
            draft.validate_references(["cand-does-not-exist"])

    def test_error_lists_all_unknown_ids(self):
        draft = self._draft()
        with pytest.raises(DraftValidationError) as exc_info:
            draft.validate_references(["cand-2", "cand-ghost"])
        message = str(exc_info.value)
        assert "cand-2" in message
        assert "cand-ghost" in message


# ---------------------------------------------------------------------------
# Stage 3 checkpoint ordering
# ---------------------------------------------------------------------------


class TestCheckpointOrdering:
    def test_next_pending_substage_starts_at_relations(self):
        draft = GenerateDraft.new(source_fingerprint="fp")
        assert draft.next_pending_substage() == "relations"

    def test_relations_done_advances_next_pending(self):
        draft = GenerateDraft.new(source_fingerprint="fp")
        draft = draft.with_checkpoint("relations", CHECKPOINT_DONE, result={"relations": []})
        assert draft.next_pending_substage() == "attributes"

    def test_all_done_returns_none(self):
        draft = GenerateDraft.new(source_fingerprint="fp")
        draft = draft.with_checkpoint("relations", CHECKPOINT_DONE, result={})
        draft = draft.with_checkpoint("attributes", CHECKPOINT_DONE, result={})
        draft = draft.with_checkpoint("axioms", CHECKPOINT_DONE, result={})
        assert draft.next_pending_substage() is None

    def test_cannot_start_attributes_before_relations_done(self):
        draft = GenerateDraft.new(source_fingerprint="fp")
        with pytest.raises(DraftValidationError):
            draft.with_checkpoint("attributes", CHECKPOINT_RUNNING)

    def test_cannot_start_axioms_before_attributes_done(self):
        draft = GenerateDraft.new(source_fingerprint="fp")
        draft = draft.with_checkpoint("relations", CHECKPOINT_DONE, result={})
        with pytest.raises(DraftValidationError):
            draft.with_checkpoint("axioms", CHECKPOINT_RUNNING)

    def test_cannot_reopen_done_substage(self):
        draft = GenerateDraft.new(source_fingerprint="fp")
        draft = draft.with_checkpoint("relations", CHECKPOINT_DONE, result={})
        with pytest.raises(DraftValidationError):
            draft.with_checkpoint("relations", CHECKPOINT_RUNNING)

    def test_failed_checkpoint_can_be_retried(self):
        draft = GenerateDraft.new(source_fingerprint="fp")
        draft = draft.with_checkpoint("relations", CHECKPOINT_FAILED)
        # A retry re-runs the same (not-done) substage.
        draft = draft.with_checkpoint("relations", CHECKPOINT_RUNNING)
        draft = draft.with_checkpoint("relations", CHECKPOINT_DONE, result={})
        assert draft.next_pending_substage() == "attributes"

    def test_unknown_substage_rejected(self):
        draft = GenerateDraft.new(source_fingerprint="fp")
        with pytest.raises(DraftValidationError):
            draft.with_checkpoint("not_a_substage", CHECKPOINT_RUNNING)

    def test_invalid_status_rejected(self):
        draft = GenerateDraft.new(source_fingerprint="fp")
        with pytest.raises(DraftValidationError):
            draft.with_checkpoint("relations", "not_a_status")


# ---------------------------------------------------------------------------
# Durable merge checkpoint (task 4 review finding #2) — independent of
# `stage`/`completion_checkpoints`, checked before ever re-running the
# append-only merge.
# ---------------------------------------------------------------------------


class TestMergeCheckpoint:
    def test_defaults_to_pending(self):
        draft = GenerateDraft.new(source_fingerprint="fp")
        assert draft.merge_checkpoint == {"status": CHECKPOINT_PENDING, "result": None}

    def test_with_merge_checkpoint_running(self):
        draft = GenerateDraft.new(source_fingerprint="fp")
        draft = draft.with_merge_checkpoint(CHECKPOINT_RUNNING)
        assert draft.merge_checkpoint["status"] == CHECKPOINT_RUNNING

    def test_with_merge_checkpoint_done_stores_result(self):
        draft = GenerateDraft.new(source_fingerprint="fp")
        stats = {"classes_added": 2}
        draft = draft.with_merge_checkpoint(CHECKPOINT_DONE, result=stats)
        assert draft.merge_checkpoint == {"status": CHECKPOINT_DONE, "result": stats}

    def test_with_merge_checkpoint_failed_stores_error(self):
        draft = GenerateDraft.new(source_fingerprint="fp")
        draft = draft.with_merge_checkpoint(CHECKPOINT_FAILED, result={"error": "boom"})
        assert draft.merge_checkpoint["status"] == CHECKPOINT_FAILED
        assert draft.merge_checkpoint["result"] == {"error": "boom"}

    def test_cannot_reopen_done_merge_checkpoint(self):
        draft = GenerateDraft.new(source_fingerprint="fp")
        draft = draft.with_merge_checkpoint(CHECKPOINT_DONE, result={})
        with pytest.raises(DraftValidationError):
            draft.with_merge_checkpoint(CHECKPOINT_RUNNING)

    def test_invalid_status_rejected(self):
        draft = GenerateDraft.new(source_fingerprint="fp")
        with pytest.raises(DraftValidationError):
            draft.with_merge_checkpoint("not_a_status")

    def test_round_trips_through_serialization(self):
        draft = GenerateDraft.new(source_fingerprint="fp")
        draft = draft.with_merge_checkpoint(
            CHECKPOINT_DONE, result={"classes_added": 1}
        )
        restored = GenerateDraft.from_dict(draft.to_dict())
        assert restored.merge_checkpoint == draft.merge_checkpoint

    def test_missing_merge_checkpoint_self_heals_to_pending(self):
        """An older persisted draft (pre-this-field) must resume cleanly."""
        draft = GenerateDraft.new(source_fingerprint="fp")
        data = draft.to_dict()
        del data["merge_checkpoint"]
        restored = GenerateDraft.from_dict(data)
        assert restored.merge_checkpoint == {
            "status": CHECKPOINT_PENDING,
            "result": None,
        }

    def test_invalid_persisted_status_self_heals_to_pending(self):
        draft = GenerateDraft.new(source_fingerprint="fp")
        data = draft.to_dict()
        data["merge_checkpoint"] = {"status": "bogus", "result": None}
        restored = GenerateDraft.from_dict(data)
        assert restored.merge_checkpoint["status"] == CHECKPOINT_PENDING


# ---------------------------------------------------------------------------
# Error hierarchy (used by API-layer error handling in a later task)
# ---------------------------------------------------------------------------


class TestErrorHierarchy:
    def test_validation_error_is_400(self):
        err = DraftValidationError("bad")
        assert isinstance(err, ValidationError)
        assert err.status_code == 400

    def test_revision_conflict_is_409(self):
        err = DraftRevisionConflict("conflict")
        assert isinstance(err, ConflictError)
        assert err.status_code == 409


# ---------------------------------------------------------------------------
# GenerateDraftStore: revision conflicts + durable resume
# ---------------------------------------------------------------------------


class TestGenerateDraftStore:
    def test_load_returns_none_when_no_draft(self, domain_session):
        store = GenerateDraftStore(domain_session)
        assert store.load() is None

    def test_save_first_draft_requires_revision_zero(self, domain_session):
        store = GenerateDraftStore(domain_session)
        draft = GenerateDraft.new(source_fingerprint="fp")
        saved = store.save(draft)
        assert saved.draft_revision == 1

    def test_save_rejects_wrong_initial_revision(self, domain_session):
        store = GenerateDraftStore(domain_session)
        draft = GenerateDraft.new(source_fingerprint="fp")
        stale = replace(draft, draft_revision=5)
        with pytest.raises(DraftRevisionConflict):
            store.save(stale)

    def test_sequential_saves_increment_revision(self, domain_session):
        store = GenerateDraftStore(domain_session)
        draft = GenerateDraft.new(source_fingerprint="fp")
        saved1 = store.save(draft)
        assert saved1.draft_revision == 1
        saved2 = store.save(saved1)
        assert saved2.draft_revision == 2

    def test_stale_write_after_concurrent_save_rejected(self, domain_session):
        store = GenerateDraftStore(domain_session)
        draft = GenerateDraft.new(source_fingerprint="fp")
        saved1 = store.save(draft)  # revision 1
        store.save(saved1)  # revision 2, someone else's concurrent write

        # A second caller still holding the revision-1 copy tries to write.
        with pytest.raises(DraftRevisionConflict):
            store.save(saved1)

    def test_durable_resume_across_new_domain_session(self, mock_session_mgr):
        from back.objects.session.DomainSession import DomainSession

        session1 = DomainSession(mock_session_mgr)
        store1 = GenerateDraftStore(session1)
        anchor = GenerateEntity.locked_anchor("cls-Customer", "Customer")
        draft = GenerateDraft.new(
            source_fingerprint="sha256:abc",
            selected_source_config={"tables": ["t1"]},
            existing_anchors=[anchor],
        )
        store1.save(draft)

        # Simulate a full page reload / backend process restart: a brand
        # new DomainSession instance reading the same underlying session
        # store must resume the same draft.
        session2 = DomainSession(mock_session_mgr)
        store2 = GenerateDraftStore(session2)
        resumed = store2.load()
        assert resumed is not None
        assert resumed.draft_revision == 1
        assert resumed.source_fingerprint == "sha256:abc"
        assert resumed.existing_anchors[0].id == "cls-Customer"

    def test_reset_clears_draft(self, domain_session):
        store = GenerateDraftStore(domain_session)
        draft = GenerateDraft.new(source_fingerprint="fp")
        store.save(draft)
        assert store.load() is not None
        store.reset()
        assert store.load() is None

    def test_load_returns_none_for_structurally_invalid_persisted_draft(
        self, domain_session
    ):
        """A corrupted persisted draft (e.g. two candidates sharing the same
        id — not healable by GenerateEntity.from_dict's per-field coercion)
        must not raise out of load(); it must be treated as no resumable
        draft, forcing re-detection, instead of crashing the caller."""
        domain_session.data["generate_draft"] = {
            "draft_revision": 1,
            "stage": "reviewing",
            "source_fingerprint": "sha256:abc",
            "selected_source_config": {},
            "existing_anchors": [],
            "candidate_entities": [
                {
                    "id": "cand-1",
                    "canonical_label": "Carrier",
                    "type_hint": "class",
                    "origin": "detected",
                    "included": True,
                    "locked": False,
                },
                {
                    "id": "cand-1",
                    "canonical_label": "Shipper",
                    "type_hint": "class",
                    "origin": "detected",
                    "included": True,
                    "locked": False,
                },
            ],
            "completion_checkpoints": {},
        }

        store = GenerateDraftStore(domain_session)
        assert store.load() is None

    def test_load_returns_none_for_invalid_entity_missing_id(self, domain_session):
        domain_session.data["generate_draft"] = {
            "draft_revision": 1,
            "stage": "reviewing",
            "source_fingerprint": "sha256:abc",
            "candidate_entities": [{"canonical_label": "Carrier"}],
        }

        store = GenerateDraftStore(domain_session)
        assert store.load() is None

    def test_load_of_valid_draft_still_works_after_hardening(self, domain_session):
        """Guard against over-broad exception handling breaking the happy path."""
        store = GenerateDraftStore(domain_session)
        store.save(GenerateDraft.new(source_fingerprint="sha256:ok"))
        loaded = store.load()
        assert loaded is not None
        assert loaded.source_fingerprint == "sha256:ok"
