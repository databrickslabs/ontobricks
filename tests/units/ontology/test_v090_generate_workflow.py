"""Staged Generate workflow edge cases (release 0.9.0).

Covers ready-document manifests, naming helpers, review payload validation,
stage guards, rejected substages and the defensive / idempotent branches of
the append-only merge into the live ontology.
"""

from types import SimpleNamespace

import pytest

import back.objects.registry as registry_pkg
from agents.agent_owl_generator.staged import CompletionResult, DetectionResult
from back.objects.ontology.generate import GenerateWorkflow as wf
from back.objects.ontology.generate.DraftValidationError import DraftValidationError
from back.objects.ontology.generate.GenerateDraft import (
    CHECKPOINT_DONE,
    CHECKPOINT_FAILED,
    DETECTING,
    GenerateDraft,
    GenerateEntity,
    REVIEWING,
    build_locked_anchors_from_classes,
)

pytestmark = pytest.mark.unit

wf_module = __import__("sys").modules[wf.run_completion.__module__]


def _settings():
    return SimpleNamespace()


# ---------------------------------------------------------------------------
# Naming helpers
# ---------------------------------------------------------------------------


class TestNamingHelpers:
    @pytest.mark.parametrize(
        "label, expected",
        [("shipping carrier", "ShippingCarrier"), ("", "Entity"), ("!!!", "Entity"),
         ("order-line 2", "OrderLine2")],
    )
    def test_sanitize_pascal(self, label, expected):
        assert wf_module._sanitize_pascal(label) == expected

    def test_sanitize_camel(self):
        assert wf_module._sanitize_camel("Has Carrier") == "hasCarrier"

    def test_unique_name_appends_first_free_suffix(self):
        assert wf_module._unique_name("Carrier", set()) == "Carrier"
        assert wf_module._unique_name("Carrier", {"Carrier", "Carrier2", "Carrier3"}) == "Carrier4"

    @pytest.mark.parametrize(
        "raw, expected", [("xsd:dateTime", "dateTime"), (None, "string"), ("int", "int"), ("xsd:", "string")]
    )
    def test_strip_xsd_prefix(self, raw, expected):
        assert wf_module._strip_xsd_prefix(raw) == expected


# ---------------------------------------------------------------------------
# Ready-document manifests
# ---------------------------------------------------------------------------


class _FakeStore:
    def __init__(self, rows=None, error=None):
        self.rows = rows or []
        self.error = error
        self.calls = []

    def list_documents(self, folder, version):
        self.calls.append((folder, version))
        if self.error:
            raise self.error
        return self.rows


def _patch_registry(monkeypatch, store):
    monkeypatch.setattr(
        registry_pkg,
        "RegistryService",
        SimpleNamespace(from_context=lambda domain, settings: SimpleNamespace(store=store)),
    )


class TestReadyDocumentManifests:
    def test_only_ready_documents_listed(self, domain_session, monkeypatch):
        domain_session.domain_folder = "sales"
        store = _FakeStore(rows=[
            {"status": "ready", "filename": "spec.pdf", "source_hash": "h1"},
            {"status": "pending", "filename": "draft.pdf", "source_hash": "h2"},
            {"status": "ready", "filename": None, "source_hash": None},
        ])
        _patch_registry(monkeypatch, store)
        manifests = wf.list_ready_document_manifests(domain_session, _settings())
        assert manifests == [
            {"filename": "spec.pdf", "source_hash": "h1"},
            {"filename": "", "source_hash": ""},
        ]
        assert store.calls[0][0] == "sales"

    def test_listing_failure_is_best_effort(self, domain_session, monkeypatch):
        domain_session.domain_folder = "sales"
        _patch_registry(monkeypatch, _FakeStore(error=RuntimeError("lakebase down")))
        assert wf.list_ready_document_manifests(domain_session, _settings()) == []

    def test_unsaved_domain_without_folder_returns_empty(self):
        domain = SimpleNamespace(uc_domain_folder="", domain_folder="  ")
        assert wf.list_ready_document_manifests(domain, _settings()) == []

    def test_version_defaults_to_one(self, monkeypatch):
        store = _FakeStore()
        _patch_registry(monkeypatch, store)
        domain = SimpleNamespace(uc_domain_folder="dom", current_version="")
        wf.list_ready_document_manifests(domain, _settings())
        assert store.calls == [("dom", "1")]


# ---------------------------------------------------------------------------
# Review / completion guards
# ---------------------------------------------------------------------------


@pytest.fixture
def no_documents(monkeypatch):
    monkeypatch.setattr(wf_module, "list_ready_document_manifests", lambda *_a, **_kw: [])


def _detect(domain_session, monkeypatch, labels):
    monkeypatch.setattr(
        wf.owl_staged,
        "detect_entities",
        lambda **kw: DetectionResult(
            success=True,
            candidate_entities=[GenerateEntity.new_candidate(label) for label in labels],
        ),
    )
    return wf.run_detection(domain_session, _settings(), host="h", token="t", endpoint_name="e")


class TestUpdateDraftPayloadValidation:
    def test_malformed_nested_field_becomes_validation_error(self, domain_session, monkeypatch, no_documents):
        draft = _detect(domain_session, monkeypatch, ["Carrier"])
        with pytest.raises(DraftValidationError, match="Invalid draft update payload"):
            wf.update_draft(
                domain_session,
                revision=draft.draft_revision,
                op="add",
                entity={"canonical_label": "Invoice", "alternate_labels": 42},
            )
        assert len(domain_session.generate_draft_store.load().candidate_entities) == 1


class TestRunCompletionGuards:
    def test_draft_still_detecting_cannot_complete(self, domain_session, no_documents):
        domain_session.generate_draft_store.save(
            GenerateDraft.new(source_fingerprint="sha256:x", stage=DETECTING)
        )
        with pytest.raises(DraftValidationError, match="not ready for completion"):
            wf.run_completion(domain_session, _settings(), host="h", token="t", endpoint_name="e")

    def test_rejected_substage_checkpointed_failed_with_reason(self, domain_session, monkeypatch, no_documents):
        _detect(domain_session, monkeypatch, ["Carrier"])
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_relations",
            lambda **kw: CompletionResult(
                success=False, substage="relations", rejected=True,
                error="raw", rejection_reason="relation references unknown entity",
            ),
        )
        with pytest.raises(DraftValidationError, match="relation references unknown entity"):
            wf.run_completion(domain_session, _settings(), host="h", token="t", endpoint_name="e")
        stored = domain_session.generate_draft_store.load()
        assert stored.completion_checkpoints["relations"]["status"] == CHECKPOINT_FAILED
        assert stored.completion_checkpoints["relations"]["result"]["rejected"] is True
        assert not any(c.get("generated_from") for c in domain_session.get_classes())

    def test_rejected_substage_falls_back_to_generic_reason(self, domain_session, monkeypatch, no_documents):
        _detect(domain_session, monkeypatch, ["Carrier"])
        monkeypatch.setattr(
            wf.owl_staged,
            "infer_relations",
            lambda **kw: CompletionResult(success=False, substage="relations", rejected=True),
        )
        with pytest.raises(DraftValidationError, match="relations output rejected"):
            wf.run_completion(domain_session, _settings(), host="h", token="t", endpoint_name="e")


# ---------------------------------------------------------------------------
# Merge: defensive skips and idempotency
# ---------------------------------------------------------------------------


def _draft(candidates, *, anchors=(), relations=None, attributes=None, axioms=None):
    draft = GenerateDraft.new(
        source_fingerprint="sha256:test",
        existing_anchors=list(anchors),
        candidate_entities=list(candidates),
        stage=REVIEWING,
    )
    for substage, key, items in (
        ("relations", "relations", relations),
        ("attributes", "attributes", attributes),
        ("axioms", "axioms", axioms),
    ):
        draft = draft.with_checkpoint(substage, CHECKPOINT_DONE, result={key: items or []})
    return draft


def _names(session):
    return [c["name"] for c in session.get_classes()]


class TestMergeDefensiveSkips:
    def test_relation_with_unresolved_endpoint_dropped(self, domain_session):
        carrier = GenerateEntity.new_candidate("Carrier")
        stats = wf.merge_draft_into_ontology(
            domain_session,
            _draft([carrier], relations=[
                {"label": "ships", "domain": carrier.id, "range": "cand-missing"},
                {"label": "ships", "domain": "cand-missing", "range": carrier.id},
            ]),
        )
        assert stats["relations_added"] == 0
        assert domain_session.get_properties() == []

    def test_exact_repeat_relation_in_same_result_merged_once(self, domain_session):
        carrier = GenerateEntity.new_candidate("Carrier")
        parcel = GenerateEntity.new_candidate("Parcel")
        rel = {"label": "transports", "domain": carrier.id, "range": parcel.id}
        stats = wf.merge_draft_into_ontology(
            domain_session, _draft([carrier, parcel], relations=[rel, dict(rel)])
        )
        assert stats["relations_added"] == 1
        prop = domain_session.get_properties()[0]
        assert (prop["name"], prop["domain"], prop["range"]) == ("transports", "Carrier", "Parcel")

    def test_attribute_on_unknown_entity_dropped_and_duplicates_skipped(self, domain_session):
        carrier = GenerateEntity.new_candidate("Carrier")
        stats = wf.merge_draft_into_ontology(
            domain_session,
            _draft([carrier], attributes=[
                {"label": "orphan", "domain": "cand-missing", "datatype": "xsd:string"},
                {"label": "tracking code", "domain": carrier.id, "datatype": "xsd:string"},
                {"label": "Tracking Code", "domain": carrier.id, "datatype": "xsd:int"},
            ]),
        )
        assert stats["attributes_added"] == 1
        dps = domain_session.get_classes()[0]["dataProperties"]
        assert [(d["name"], d["type"]) for d in dps] == [("trackingCode", "string")]

    def test_attribute_on_excluded_candidate_dropped(self, domain_session):
        carrier = GenerateEntity.new_candidate("Carrier", included=False)
        stats = wf.merge_draft_into_ontology(
            domain_session,
            _draft([carrier], attributes=[{"label": "code", "domain": carrier.id}]),
        )
        assert stats == {"classes_added": 0, "relations_added": 0, "attributes_added": 0, "axioms_added": 0}

    def test_unresolved_self_and_unknown_kind_axioms_dropped(self, domain_session):
        a = GenerateEntity.new_candidate("Truck")
        b = GenerateEntity.new_candidate("Vehicle")
        stats = wf.merge_draft_into_ontology(
            domain_session,
            _draft([a, b], axioms=[
                {"kind": "subClassOf", "subject": a.id, "object": "cand-missing"},
                {"kind": "disjointWith", "subject": a.id, "object": a.id},
                {"kind": "inverseOf", "subject": a.id, "object": b.id},
            ]),
        )
        assert stats["axioms_added"] == 0
        assert domain_session.axioms == []
        assert all(not c.get("parent") for c in domain_session.get_classes())

    def test_subclass_axiom_on_anchor_missing_from_ontology_skipped(self, domain_session):
        ghost_anchor = build_locked_anchors_from_classes([{"name": "Ghost"}])
        vehicle = GenerateEntity.new_candidate("Vehicle")
        stats = wf.merge_draft_into_ontology(
            domain_session,
            _draft([vehicle], anchors=ghost_anchor,
                   axioms=[{"kind": "subClassOf", "subject": "Ghost", "object": vehicle.id}]),
        )
        assert stats["axioms_added"] == 0
        assert _names(domain_session) == ["Vehicle"]


class TestMergeIdempotency:
    def test_full_merge_twice_adds_nothing_the_second_time(self, domain_session):
        domain_session.ontology["classes"] = [{"name": "Customer", "label": "Customer"}]
        anchors = build_locked_anchors_from_classes(domain_session.get_classes())
        carrier = GenerateEntity.new_candidate("Carrier")
        parcel = GenerateEntity.new_candidate("Parcel")
        draft = _draft(
            [carrier, parcel],
            anchors=anchors,
            relations=[{"label": "serves", "domain": carrier.id, "range": "Customer"}],
            attributes=[{"label": "code", "domain": carrier.id, "datatype": "xsd:string"}],
            axioms=[
                {"kind": "subClassOf", "subject": parcel.id, "object": "Customer"},
                {"kind": "disjointWith", "subject": carrier.id, "object": parcel.id},
            ],
        )
        first = wf.merge_draft_into_ontology(domain_session, draft)
        assert first == {"classes_added": 2, "relations_added": 1, "attributes_added": 1, "axioms_added": 2}
        snapshot = (
            [dict(c) for c in domain_session.get_classes()],
            list(domain_session.get_properties()),
            list(domain_session.axioms),
        )

        second = wf.merge_draft_into_ontology(domain_session, draft)
        assert second == {"classes_added": 0, "relations_added": 0, "attributes_added": 0, "axioms_added": 0}
        assert _names(domain_session) == ["Customer", "Carrier", "Parcel"]
        assert len(domain_session.get_properties()) == len(snapshot[1])
        assert domain_session.axioms == snapshot[2]
        parcel_cls = next(c for c in domain_session.get_classes() if c["name"] == "Parcel")
        assert parcel_cls["parent"] == "Customer"

    def test_existing_anchor_content_is_never_overwritten(self, domain_session):
        domain_session.ontology["classes"] = [
            {"name": "Customer", "label": "Client", "uri": "http://mine#Customer",
             "dataProperties": [{"name": "vip", "localName": "vip"}]},
        ]
        anchors = build_locked_anchors_from_classes(domain_session.get_classes())
        wf.merge_draft_into_ontology(
            domain_session,
            _draft([], anchors=anchors,
                   attributes=[{"label": "loyalty", "domain": "Customer", "datatype": "xsd:int"}]),
        )
        customer = domain_session.get_classes()[0]
        assert (customer["label"], customer["uri"]) == ("Client", "http://mine#Customer")
        assert [d["name"] for d in customer["dataProperties"]] == ["vip", "loyalty"]

    def test_name_collision_with_existing_class_gets_suffix(self, domain_session):
        domain_session.ontology["classes"] = [{"name": "Carrier", "label": "Haulier"}]
        carrier = GenerateEntity.new_candidate("Carrier")
        wf.merge_draft_into_ontology(domain_session, _draft([carrier]))
        assert _names(domain_session) == ["Carrier", "Carrier2"]
