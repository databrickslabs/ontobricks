"""Workflow orchestration for the three-stage ontology Generate feature.

Plan task 4 of ``staged-ontology-generate``. See
``docs/superpowers/specs/2026-09-20-three-stage-ontology-generate-design.md``
for the full target contract. This module wires together the draft/contract
layer (:mod:`back.objects.ontology.GenerateDraft`, task 2) and the staged
agent entry points (:mod:`agents.agent_owl_generator.staged`, task 3) into
the actual detect -> human review -> checkpointed completion -> append-only
merge pipeline that the API routes (task 4) and the wizard UI (task 5) drive.

No agent or LLM call happens inline in a request handler: every function
here that calls the staged agent (:func:`run_detection`, :func:`run_completion`)
is expected to be invoked from a ``TaskManager`` worker thread by the caller
(the API routes), never from the ``async def`` route body itself.
"""

from __future__ import annotations

import re
from dataclasses import replace
from typing import Any, Callable, Dict, List, Optional, Set

from back.core.errors import InfrastructureError, NotFoundError, ValidationError
from back.core.logging import get_logger
from back.objects.ontology.GenerateDraft import (
    CHECKPOINT_DONE,
    CHECKPOINT_FAILED,
    CHECKPOINT_RUNNING,
    COMPLETING,
    DETECTING,
    DONE,
    REVIEWING,
    SUBSTAGE_ATTRIBUTES,
    SUBSTAGE_AXIOMS,
    SUBSTAGE_RELATIONS,
    DraftValidationError,
    GenerateDraft,
    GenerateEntity,
    ORIGIN_MANUAL,
    TYPE_CLASS,
    build_locked_anchors_from_classes,
    compute_source_fingerprint,
)
from agents.agent_owl_generator import staged as owl_staged
from agents.agent_owl_generator import schemas as owl_schemas
from shared.config.constants import DEFAULT_BASE_URI

logger = get_logger(__name__)

# Editable Stage 2 fields a review "update" op may change on a candidate.
_EDITABLE_FIELDS = frozenset(
    {
        "canonical_label",
        "description",
        "type_hint",
        "evidence",
        "alternate_labels",
        "included",
    }
)

_AXIOM_KINDS_SUBCLASS = "subClassOf"
_AXIOM_KINDS_BINARY = frozenset({"disjointWith", "equivalentClass"})


def _substage_runners() -> Dict[str, Callable[..., Any]]:
    """Looked up dynamically (not bound at import time) so tests can
    monkeypatch ``wf.owl_staged.infer_relations``/etc per-call."""
    return {
        SUBSTAGE_RELATIONS: owl_staged.infer_relations,
        SUBSTAGE_ATTRIBUTES: owl_staged.infer_attributes,
        SUBSTAGE_AXIOMS: owl_staged.infer_axioms,
    }


# Entity-closure reference extractors per substage. ``staged.py`` already
# runs this same check internally before returning a *successful*
# ``CompletionResult`` — but this workflow layer is the actual persistence
# boundary, so it re-checks unconditionally rather than trusting the
# caller-supplied result's ``success`` flag.
_SUBSTAGE_REF_EXTRACTORS: Dict[str, Callable[[Dict[str, Any]], Set[str]]] = {
    SUBSTAGE_RELATIONS: owl_schemas.relations_referenced_ids,
    SUBSTAGE_ATTRIBUTES: owl_schemas.attributes_referenced_ids,
    SUBSTAGE_AXIOMS: owl_schemas.axioms_referenced_ids,
}


# ---------------------------------------------------------------------------
# No-reparse ready-document manifest (identity only, never content)
# ---------------------------------------------------------------------------


def list_ready_document_manifests(domain, settings) -> List[Dict[str, str]]:
    """Identity manifest (``filename`` + ``source_hash``) for every READY
    document in the domain's Knowledge Store (Lakebase corpus).

    Read-only: reads document metadata rows only — never triggers parsing or
    the extractor. A domain with no registry configured yet returns ``[]``
    rather than raising — Stage 1 detection is still possible from selected
    metadata alone.
    """
    folder = _document_folder(domain)
    if not folder:
        return []
    version = str(getattr(domain, "current_version", "") or "1")

    try:
        from back.objects.registry import RegistryService

        store = RegistryService.from_context(domain, settings).store
        rows = store.list_documents(folder, version)
    except Exception:  # noqa: BLE001 — best-effort; detection still proceeds
        logger.warning("list_ready_document_manifests: listing failed", exc_info=True)
        return []

    manifests: List[Dict[str, str]] = []
    for row in rows:
        if row.get("status") == "ready":
            manifests.append(
                {
                    "filename": row.get("filename") or "",
                    "source_hash": row.get("source_hash") or "",
                }
            )
    return manifests


def _document_folder(domain) -> str:
    """Registry folder for the domain's Knowledge Store, or '' when unsaved."""
    return (
        getattr(domain, "uc_domain_folder", "")
        or getattr(domain, "domain_folder", "")
        or ""
    ).strip()


def compute_current_fingerprint(
    domain,
    settings,
    selected_source_config: Optional[Dict[str, Any]],
) -> str:
    """Fingerprint over the *current* source: metadata/doc selection, the
    ready-document manifest, and the live ontology's anchor identities."""
    anchors = build_locked_anchors_from_classes(domain.get_classes())
    ready_docs = list_ready_document_manifests(domain, settings)
    return compute_source_fingerprint(
        selected_source_config=selected_source_config,
        ready_documents=ready_docs,
        existing_anchors=anchors,
    )


# ---------------------------------------------------------------------------
# Stage 1 — detection
# ---------------------------------------------------------------------------


def run_detection(
    domain,
    settings,
    *,
    host: str,
    token: str,
    endpoint_name: str,
    metadata: Optional[dict] = None,
    guidelines: str = "",
    options: Optional[dict] = None,
    selected_tables: Optional[List[str]] = None,
    selected_docs: Optional[List[str]] = None,
    warehouse_id: str = "",
    on_step: Optional[Callable[[str], None]] = None,
) -> GenerateDraft:
    """Stage 1: detect candidates and persist a fresh, paused-for-review draft.

    Any prior draft (at any stage) is discarded — re-running detection is the
    only way to pull in a changed source, and it always starts a new review
    cycle. Raises :class:`DraftValidationError` (400) when the staged agent's
    output was schema-rejected, or :class:`InfrastructureError` (500) for a
    network/endpoint failure. Callers must invoke this from a background
    worker thread — it makes a blocking LLM call.
    """
    anchors = build_locked_anchors_from_classes(domain.get_classes())
    ready_docs = list_ready_document_manifests(domain, settings)
    selected_source_config = {
        "tables": list(selected_tables or []),
        "documents": list(selected_docs or []),
    }
    fingerprint = compute_source_fingerprint(
        selected_source_config=selected_source_config,
        ready_documents=ready_docs,
        existing_anchors=anchors,
    )

    result = owl_staged.detect_entities(
        host=host,
        token=token,
        endpoint_name=endpoint_name,
        metadata=metadata,
        guidelines=guidelines,
        options=options,
        existing_anchors=anchors,
        selected_tables=selected_tables,
        selected_docs=selected_docs,
        registry=dict(domain.registry),
        domain_name=domain.info.get("name", ""),
        domain_folder=domain.domain_folder,
        domain_version=domain.current_version,
        warehouse_id=warehouse_id,
        on_step=on_step,
    )
    if not result.success:
        if result.rejected:
            # Reject-only, user-safe message: the raw parser/schema error
            # (`result.error`) may echo back arbitrary model text and is
            # logged server-side for diagnosis only, never surfaced to the
            # end user — see the live Stage-1 detection failure fix (SPEC
            # §6, "zero-new-candidate" failure mode).
            logger.warning(
                "Stage-1 detection output rejected (not surfaced to the "
                "user): %s",
                result.error,
            )
            raise DraftValidationError(
                "The AI model did not return the expected structured "
                "entity list for this request. Please retry detection."
            )
        raise InfrastructureError(
            "Ontology entity detection failed", detail=result.error
        )

    # Defense-in-depth dedup against locked anchors. ``detect_entities``
    # already dedups internally against the anchors it was given, but this
    # workflow layer is the actual persistence boundary and must not depend
    # on every caller's mock/stub reproducing that behavior:
    # - by normalized canonical/alternate label (a candidate re-proposing an
    #   anchor's synonym), which would otherwise fail ``GenerateDraft``'s
    #   label-uniqueness invariant; and
    # - by the class *name* the merge would mint for it (``id_to_name``'s
    #   ``_sanitize_pascal``) colliding with an existing anchor's raw id —
    #   anchors use the live ontology class's ``name`` as their id (see
    #   :func:`build_locked_anchors_from_classes`), which is independent of
    #   that class's display ``label``.
    anchor_labels: Set[str] = set()
    anchor_ids = {a.id for a in anchors}
    for anchor in anchors:
        anchor_labels |= anchor.normalized_labels()
    candidates = [
        c
        for c in result.candidate_entities
        if not (c.normalized_labels() & anchor_labels)
        and _sanitize_pascal(c.canonical_label) not in anchor_ids
    ]

    draft = GenerateDraft.new(
        source_fingerprint=fingerprint,
        selected_source_config=selected_source_config,
        existing_anchors=anchors,
        candidate_entities=candidates,
        stage=REVIEWING,
    )
    store = domain.generate_draft_store
    store.reset()
    return store.save(draft)


# ---------------------------------------------------------------------------
# Stage 2 — draft read / update / discard
# ---------------------------------------------------------------------------


def get_draft_view(domain, settings) -> Optional[Dict[str, Any]]:
    """Return the persisted draft as a plain dict (plus a ``stale`` flag), or
    ``None`` if no draft is persisted. Synchronous — no LLM call."""
    draft = domain.generate_draft_store.load()
    if draft is None:
        return None
    current_fp = compute_current_fingerprint(
        domain, settings, draft.selected_source_config
    )
    view = draft.to_dict()
    view["stale"] = draft.is_stale(current_fp)
    return view


def update_draft(
    domain,
    *,
    revision: int,
    op: str,
    entity: Optional[Dict[str, Any]] = None,
    entity_id: Optional[str] = None,
    updates: Optional[Dict[str, Any]] = None,
) -> GenerateDraft:
    """Apply one Stage 2 review mutation with optimistic-concurrency checks.

    ``op`` is one of ``add`` (manual new candidate), ``remove`` (delete a
    candidate row), ``update`` (edit canonical_label/description/type_hint/
    evidence/alternate_labels), ``include``, or ``exclude``. Synchronous —
    never calls the LLM. Raises :class:`NotFoundError` if there is no draft,
    :class:`DraftRevisionConflict` (409) on a stale ``revision``, and
    :class:`ValidationError`/:class:`DraftValidationError` (400) for an
    unknown op or an invalid edit (including any attempt to edit a locked
    anchor, which never lives in ``candidate_entities`` to begin with).
    """
    store = domain.generate_draft_store
    draft = store.load()
    if draft is None:
        raise NotFoundError("No Generate draft to update. Run detection first.")

    # Nested field values (e.g. a string where ``evidence``/``alternate_labels``
    # expects a list) can still raise a raw TypeError/ValueError/AttributeError
    # deep inside ``GenerateEntity`` construction even after the route
    # boundary's shape checks — translate those into a 400 too, defense in
    # depth (task 4 review finding #7), rather than letting them surface as
    # an unhandled 500.
    try:
        if op == "add":
            entity = entity or {}
            label = str(entity.get("canonical_label", "") or "")
            new_entity = GenerateEntity.new_candidate(
                label,
                description=str(entity.get("description", "") or ""),
                type_hint=entity.get("type_hint") or TYPE_CLASS,
                evidence=entity.get("evidence"),
                alternate_labels=entity.get("alternate_labels"),
                origin=ORIGIN_MANUAL,
            )
            draft = replace(draft, draft_revision=revision).with_candidate_added(
                new_entity
            )
        elif op == "remove":
            draft = replace(draft, draft_revision=revision).with_candidate_removed(
                entity_id or ""
            )
        elif op == "update":
            field_updates = {
                k: v for k, v in (updates or {}).items() if k in _EDITABLE_FIELDS
            }
            draft = replace(draft, draft_revision=revision).with_candidate_updated(
                entity_id or "", **field_updates
            )
        elif op == "include":
            draft = replace(draft, draft_revision=revision).with_candidate_updated(
                entity_id or "", included=True
            )
        elif op == "exclude":
            draft = replace(draft, draft_revision=revision).with_candidate_updated(
                entity_id or "", included=False
            )
        else:
            raise ValidationError(f"Unknown draft update op: {op!r}")
    except (TypeError, ValueError, AttributeError) as exc:
        raise DraftValidationError(f"Invalid draft update payload: {exc}") from exc

    return store.save(draft)


def discard_draft(domain) -> None:
    """Discard the persisted draft entirely (explicit discard, or forced re-detect)."""
    domain.generate_draft_store.reset()


# ---------------------------------------------------------------------------
# Stage 3 — completion (relations -> attributes -> axioms) + append merge
# ---------------------------------------------------------------------------


def run_completion(
    domain,
    settings,
    *,
    host: str,
    token: str,
    endpoint_name: str,
    options: Optional[dict] = None,
    on_step: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """Stage 3: run relations -> attributes -> axioms in strict order,
    checkpointing each successful substage before the next starts, then
    perform the validated append-only merge into the live ontology.

    Resumable: substages already checkpointed ``done`` are never re-run.
    Callers must invoke this from a background worker thread — it makes up
    to three blocking LLM calls.
    """
    store = domain.generate_draft_store
    draft = store.load()
    if draft is None:
        raise NotFoundError("No Generate draft to complete. Run detection first.")

    if draft.stage == DONE:
        raise DraftValidationError(
            "This Generate draft has already been completed and merged. "
            "Run a new detection to start another Generate cycle."
        )
    if draft.stage == DETECTING:
        raise DraftValidationError(
            "This Generate draft is not ready for completion. Run detection first."
        )

    if draft.stage != COMPLETING:
        # Staleness is only checked on the initial REVIEWING -> COMPLETING
        # transition: once a completion run has started, the append-only
        # merge itself grows the live ontology (new anchors) as substages
        # are checkpointed done, so re-fingerprinting the *current* source
        # on every resume (after a partial-substage failure or a
        # crash-and-retry mid-merge) would see that self-inflicted growth
        # as drift and permanently block resume. The source is frozen for
        # the duration of one completion run by design.
        current_fp = compute_current_fingerprint(
            domain, settings, draft.selected_source_config
        )
        draft.ensure_not_stale(current_fp)
        draft = store.save(replace(draft, stage=COMPLETING))
    draft.ensure_ready_for_completion()

    draft_id = domain.domain_folder or domain.info.get("name", "") or "generate-draft"
    runners = _substage_runners()

    while True:
        substage = draft.next_pending_substage()
        if substage is None:
            break

        draft = store.save(draft.with_checkpoint(substage, CHECKPOINT_RUNNING))
        runner = runners[substage]
        result = runner(
            host=host,
            token=token,
            endpoint_name=endpoint_name,
            draft=draft,
            options=options,
            draft_id=draft_id,
            draft_revision=draft.draft_revision,
            on_step=on_step,
        )

        if not result.success:
            store.save(
                draft.with_checkpoint(
                    substage,
                    CHECKPOINT_FAILED,
                    result={"error": result.error, "rejected": result.rejected},
                )
            )
            if result.rejected:
                raise DraftValidationError(
                    result.rejection_reason
                    or result.error
                    or f"{substage} output rejected"
                )
            raise InfrastructureError(
                f"Ontology {substage} inference failed", detail=result.error
            )

        # Entity-closure re-check at the persistence boundary (see the
        # module-level comment on ``_SUBSTAGE_REF_EXTRACTORS``): a
        # substage's reported ``success`` is never trusted blindly — an
        # out-of-closure reference is rejected here too, before the
        # checkpoint is written as done.
        refs_fn = _SUBSTAGE_REF_EXTRACTORS[substage]
        try:
            draft.validate_references(refs_fn(result.result), context=substage)
        except DraftValidationError as exc:
            store.save(
                draft.with_checkpoint(
                    substage,
                    CHECKPOINT_FAILED,
                    result={"error": str(exc), "rejected": True},
                )
            )
            raise

        draft = store.save(
            draft.with_checkpoint(substage, CHECKPOINT_DONE, result=result.result)
        )

    # Durable merge checkpoint (task 4 review finding #2) — deliberately
    # checked *before* ever calling the merge again, independent of
    # `stage`: a crash between `merge_draft_into_ontology` persisting into
    # the live ontology (its own `domain.save()`) and this checkpoint being
    # written as `done` is the one window a naive `stage`-only check cannot
    # see through. Once this checkpoint is `done`, the merge is trusted to
    # have happened and is never re-run — only the (side-effect-free)
    # `stage` transition is finished off if it didn't make it last time.
    if draft.merge_checkpoint["status"] == CHECKPOINT_DONE:
        merge_stats = draft.merge_checkpoint["result"]
        if draft.stage != DONE:
            draft = store.save(replace(draft, stage=DONE))
        return {"draft": draft.to_dict(), "merge": merge_stats}

    draft = store.save(draft.with_merge_checkpoint(CHECKPOINT_RUNNING))
    try:
        merge_stats = merge_draft_into_ontology(domain, draft)
    except DraftValidationError as exc:
        store.save(
            draft.with_merge_checkpoint(CHECKPOINT_FAILED, result={"error": str(exc)})
        )
        raise
    draft = store.save(draft.with_merge_checkpoint(CHECKPOINT_DONE, result=merge_stats))
    draft = store.save(replace(draft, stage=DONE))
    return {"draft": draft.to_dict(), "merge": merge_stats}


# ---------------------------------------------------------------------------
# Deterministic append-only merge
# ---------------------------------------------------------------------------


def _sanitize_pascal(label: str) -> str:
    """PascalCase identifier from a free-text label (mirrors the class-name
    naming rule enforced by the stage prompts)."""
    words = re.findall(r"[A-Za-z0-9]+", label or "")
    if not words:
        return "Entity"
    return "".join(w[:1].upper() + w[1:] for w in words)


def _sanitize_camel(label: str) -> str:
    """lowerCamelCase identifier from a free-text label (property naming rule)."""
    pascal = _sanitize_pascal(label)
    return pascal[:1].lower() + pascal[1:] if pascal else "property"


def _unique_name(base: str, taken: Set[str]) -> str:
    if base not in taken:
        return base
    i = 2
    while f"{base}{i}" in taken:
        i += 1
    return f"{base}{i}"


def _strip_xsd_prefix(datatype: str) -> str:
    text = str(datatype or "string").strip()
    if ":" in text:
        text = text.rsplit(":", 1)[1]
    return text or "string"


def merge_draft_into_ontology(domain, draft: GenerateDraft) -> Dict[str, Any]:
    """Apply the validated, append-only merge of *draft* into the live ontology.

    Existing entities (locked anchors) keep their ``name``/``uri``/content —
    this function only ever *appends* new classes/properties/dataProperties/
    axioms or sets an unset ``parent`` via a ``subClassOf`` axiom; it never
    deletes or renames anything already present. New candidate entities are
    minted a URI-safe ``name`` from their ``canonical_label`` (their
    detection-time ``id`` stays the join key for this merge only — once
    merged, the new class's ``name`` becomes its stable identity for future
    Generate cycles, exactly like every other existing anchor).

    **Idempotent by construction** (task 4 review finding #2, defense in
    depth on top of the durable ``merge_checkpoint`` in
    :func:`run_completion`): every class this function adds is tagged with
    ``generated_from=<candidate id>`` so a second call for the same draft
    (e.g. a resumed run after a crash between this function's own
    ``domain.save()`` and the checkpoint recording that fact) recognizes
    already-merged candidates and never re-adds them under a suffixed name
    (``Carrier2``). Relations and binary axioms are deduped by their
    resolved identity tuple; ``subClassOf`` is a no-op on an exact repeat.
    """
    from back.objects.ontology.Ontology import Ontology

    # Reject-only, checked before any mutation (task 4 review finding #3):
    # Stage 1/2 candidates may carry a non-class `type_hint`
    # (object_property/data_property) as a hint for a *future* merge
    # target, but the ontology model only supports merging class
    # candidates today. Silently merging a property-hinted candidate as a
    # (wrongly-shaped) class would be worse than rejecting it outright.
    for candidate in draft.candidate_entities:
        if candidate.included and candidate.type_hint != TYPE_CLASS:
            raise DraftValidationError(
                f"Candidate {candidate.canonical_label!r} has "
                f"type_hint={candidate.type_hint!r}, but merge only "
                "supports 'class' candidates today. Edit its type hint "
                "back to 'class' or exclude it before completing."
            )

    classes = list(domain.get_classes())
    properties = list(domain.get_properties())
    base_uri = domain.ontology.get("base_uri") or DEFAULT_BASE_URI

    existing_names = {c.get("name") for c in classes if c.get("name")}
    existing_prop_names = {p.get("name") for p in properties if p.get("name")}
    already_merged_by_candidate_id: Dict[str, str] = {
        c["generated_from"]: c["name"]
        for c in classes
        if c.get("generated_from") and c.get("name")
    }

    # id -> live ontology class "name": anchors keep their id-as-name;
    # newly-included candidates get a freshly-minted, unique class name.
    id_to_name: Dict[str, str] = {a.id: a.id for a in draft.existing_anchors}

    added_classes: List[Dict[str, Any]] = []
    for candidate in draft.candidate_entities:
        if not candidate.included:
            continue
        already_merged_name = already_merged_by_candidate_id.get(candidate.id)
        if already_merged_name:
            # Idempotent short-circuit: a previous (crashed/retried) merge
            # attempt already appended this candidate — reuse its minted
            # name rather than creating a duplicate class.
            id_to_name[candidate.id] = already_merged_name
            continue
        name = _unique_name(_sanitize_pascal(candidate.canonical_label), existing_names)
        existing_names.add(name)
        id_to_name[candidate.id] = name
        new_class = Ontology.build_class_from_data(
            {
                "uri": f"{base_uri}{name}",
                "name": name,
                "label": candidate.canonical_label,
                "description": candidate.description,
                # Defense-in-depth dedup (order-preserving) in case the
                # agent emitted the same synonym twice.
                "alternate_labels": list(dict.fromkeys(candidate.alternate_labels)),
            }
        )
        new_class["generated_from"] = candidate.id
        added_classes.append(new_class)
    classes = classes + added_classes
    class_by_name = {c["name"]: c for c in classes}

    def _result_for(substage: str) -> Dict[str, Any]:
        return (draft.completion_checkpoints.get(substage) or {}).get("result") or {}

    # Idempotent dedup key for object properties: (domain, range, label).
    # `_unique_name` mints a fresh suffixed name on every call regardless of
    # whether the relation was already merged, so identity must be checked
    # on this resolved tuple, not on the property name.
    existing_relation_keys: Set[tuple] = {
        (p.get("domain"), p.get("range"), p.get("label"))
        for p in properties
        if p.get("type") == "ObjectProperty"
    }
    existing_directed_pairs: Set[tuple] = {
        (p.get("domain"), p.get("range"))
        for p in properties
        if p.get("type") == "ObjectProperty" and p.get("domain") and p.get("range")
    }

    added_relations = 0
    for rel in _result_for(SUBSTAGE_RELATIONS).get("relations", []):
        domain_name = id_to_name.get(str(rel.get("domain")))
        range_name = id_to_name.get(str(rel.get("range")))
        if not domain_name or not range_name:
            # Already enforced by the entity-closure check before this
            # substage was checkpointed done; defensive skip only.
            logger.warning(
                "merge: dropping relation with unresolved entity id: %s", rel
            )
            continue
        if (
            domain_name != range_name
            and (range_name, domain_name) in existing_directed_pairs
        ):
            logger.info("merge: dropping inverse relation %s", rel)
            continue
        rel_label = rel.get("label") or ""
        rel_key = (domain_name, range_name, rel_label)
        if rel_key in existing_relation_keys:
            # Idempotent: this relation was already merged (prior attempt
            # or an exact repeat in the same result).
            continue
        prop_name = _unique_name(_sanitize_camel(rel_label), existing_prop_names)
        existing_prop_names.add(prop_name)
        existing_relation_keys.add(rel_key)
        existing_directed_pairs.add((domain_name, range_name))
        properties.append(
            Ontology.build_property_from_data(
                {
                    "uri": f"{base_uri}{prop_name}",
                    "name": prop_name,
                    "label": rel.get("label") or prop_name,
                    "description": rel.get("evidence", ""),
                    "type": "ObjectProperty",
                    "domain": domain_name,
                    "range": range_name,
                    "direction": "forward",
                }
            )
        )
        added_relations += 1

    added_attributes = 0
    for attr in _result_for(SUBSTAGE_ATTRIBUTES).get("attributes", []):
        domain_name = id_to_name.get(str(attr.get("domain")))
        target = class_by_name.get(domain_name) if domain_name else None
        if target is None:
            logger.warning(
                "merge: dropping attribute with unresolved entity id: %s", attr
            )
            continue
        dp_name = _sanitize_camel(attr.get("label", ""))
        existing_dp_names = {dp.get("name") for dp in target.get("dataProperties", [])}
        if dp_name in existing_dp_names:
            continue
        target.setdefault("dataProperties", []).append(
            {
                "name": dp_name,
                "localName": dp_name,
                "label": attr.get("label") or dp_name,
                "type": _strip_xsd_prefix(attr.get("datatype")),
            }
        )
        added_attributes += 1

    axioms = list(domain.axioms)
    # Idempotent dedup key for disjointWith/equivalentClass: (type, subject,
    # sorted objects) — order-independent so a repeat with objects listed
    # in a different order is still recognized as the same axiom.
    existing_binary_axiom_keys: Set[tuple] = {
        (a.get("type"), a.get("subject"), tuple(sorted(a.get("objects") or [])))
        for a in axioms
    }

    added_axioms = 0
    for axiom in _result_for(SUBSTAGE_AXIOMS).get("axioms", []):
        kind = axiom.get("kind")
        subject_name = id_to_name.get(str(axiom.get("subject")))
        object_name = id_to_name.get(str(axiom.get("object")))
        if not subject_name or not object_name or subject_name == object_name:
            logger.warning(
                "merge: dropping axiom with unresolved/self entity id: %s", axiom
            )
            continue
        if kind == _AXIOM_KINDS_SUBCLASS:
            subject_cls = class_by_name.get(subject_name)
            if subject_cls is None:
                continue
            current_parent = subject_cls.get("parent")
            if not current_parent:
                subject_cls["parent"] = object_name
                added_axioms += 1
            elif current_parent == object_name:
                # Idempotent: exact repeat (retried merge, or the agent
                # proposing the same subClassOf twice) — a silent no-op,
                # never an error.
                continue
            else:
                # The ontology model supports only a single `parent` per
                # class (see OntologyGenerator._add_class) — a second,
                # *different* parent is multi-inheritance the model
                # cannot represent. Reject explicitly rather than
                # silently dropping it (task 4 review finding #5).
                raise DraftValidationError(
                    f"Cannot set {subject_name!r} subClassOf {object_name!r}: "
                    f"{subject_name!r} already has a different parent "
                    f"({current_parent!r}); multiple parent classes "
                    "(multi-inheritance) are not supported by the "
                    "ontology model."
                )
        elif kind in _AXIOM_KINDS_BINARY:
            axiom_key = (kind, subject_name, tuple(sorted([object_name])))
            if axiom_key in existing_binary_axiom_keys:
                # Idempotent: exact repeat, silent no-op.
                continue
            existing_binary_axiom_keys.add(axiom_key)
            axioms.append(
                {"type": kind, "subject": subject_name, "objects": [object_name]}
            )
            added_axioms += 1
        else:
            logger.warning("merge: dropping axiom with unrecognized kind: %s", kind)

    domain.ontology["classes"] = classes
    domain.ontology["properties"] = properties
    domain.axioms = axioms
    domain.clear_generated_content()
    domain.record_change(
        "ontology_generated",
        entity_type="ontology",
        entity_ref="generate_wizard",
        summary=(
            f"Generate: +{len(added_classes)} classes, +{added_relations} relations, "
            f"+{added_attributes} attributes, +{added_axioms} axioms"
        ),
    )
    domain.save()

    return {
        "classes_added": len(added_classes),
        "relations_added": added_relations,
        "attributes_added": added_attributes,
        "axioms_added": added_axioms,
    }
