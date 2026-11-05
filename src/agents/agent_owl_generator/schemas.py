"""Structured schema parsing/validation for the staged owl-generator.

This is the **reject-only** boundary of the staged Generate contract (design:
``docs/superpowers/specs/2026-09-20-three-stage-ontology-generate-design.md``
§Structured Schemas; SPEC §3a/§6a). Each staged LLM answer is parsed here into
a bounded, typed structure:

* :func:`parse_detection_payload` — Stage 1 candidate entities, defaulted to
  ``included=true`` / ``origin=detected``, deduplicated against the locked
  anchors (by id, canonical label, and alternate labels) so an existing
  entity is never re-proposed as new;
* :func:`parse_relations_payload` / :func:`parse_attributes_payload` /
  :func:`parse_axioms_payload` — Stage 3 completion outputs, plus the
  ``*_referenced_ids`` extractors that feed the entity-closure check
  (:meth:`GenerateDraft.validate_references`).

A malformed or out-of-schema answer raises :class:`SchemaValidationError`.
The caller reports that as a failure for the user to retry — it is **never**
fed back into another LLM call within the same request (no rewrite loop).
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set

from agents.agent_owl_generator.RelationDeduplicator import RelationDeduplicator
from back.core.logging import get_logger
from back.objects.ontology.generate.DraftValidationError import DraftValidationError
from back.objects.ontology.generate.GenerateDraft import (
    GenerateEntity,
    ORIGIN_DETECTED,
    TYPE_CLASS,
    _VALID_TYPE_HINTS,
)

logger = get_logger(__name__)

# A fenced ```json ... ``` (or bare ``` ... ```) block the model sometimes
# wraps its answer in, despite instructions to emit raw JSON.
_FENCE_RE = re.compile(r"^\s*```(?:json)?\s*(.*?)\s*```\s*$", re.DOTALL)


class SchemaValidationError(DraftValidationError):
    """A staged LLM answer is malformed or violates its structured schema.

    Subclasses :class:`DraftValidationError` so the reject-only closure check
    and the schema check surface through the same 400/validation channel.
    """


# ---------------------------------------------------------------------------
# Transport-level structured output (response_format) for Stage 1
# ---------------------------------------------------------------------------
#
# Live-reliability fix: a prompt-only "JSON only, first character must be {"
# instruction cannot force a compliant model to skip a visible reasoning
# preamble — live reproduction against the user's own endpoint
# (benoit_cayla.ontobricks-todrop.monclaudesonnetamoi) still narrated prose
# ahead of the JSON on 4 of 5 calls despite that strengthened prompt. That
# same endpoint was confirmed (by direct user testing) to honour an
# OpenAI/Databricks-style ``response_format={"type": "json_schema", ...}``
# transport directive and return exactly the schema-shaped JSON, while
# rejecting the simpler ``{"type": "json_object"}`` AND rejecting
# ``response_format`` combined with ``tools`` in the same request. This
# constant is passed as ``response_format`` on Stage 1's single
# schema-enforced finalization call only (never on the tool-gathering
# calls, which need ``tools`` instead — see
# :func:`shared.llm_target.build_llm_request`'s mutual-exclusion guard and
# :mod:`agents.agent_owl_generator.staged`'s two-phase detection flow).
#
# The schema mirrors this module's own parser contract exactly (the fields
# ``parse_detection_payload`` reads) so a strict, additionalProperties=False
# schema can never itself reject a shape the parser would have accepted —
# schemas.py's Python-side validation still runs unconditionally afterwards
# (never trust the wire, even with schema enforcement).
DETECTION_RESPONSE_FORMAT: Dict[str, Any] = {
    "type": "json_schema",
    "json_schema": {
        "name": "detection_candidate_entities",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "candidate_entities": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "canonical_label": {"type": "string"},
                            "description": {"type": "string"},
                            "type_hint": {
                                "type": "string",
                                "enum": [TYPE_CLASS],
                            },
                            "evidence": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "source": {"type": "string"},
                                        "excerpt": {"type": "string"},
                                    },
                                    "required": ["source", "excerpt"],
                                    "additionalProperties": False,
                                },
                            },
                            "alternate_labels": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                        },
                        "required": [
                            "canonical_label",
                            "description",
                            "type_hint",
                            "evidence",
                            "alternate_labels",
                        ],
                        "additionalProperties": False,
                    },
                }
            },
            "required": ["candidate_entities"],
            "additionalProperties": False,
        },
    },
}


def _strip_fences(text: str) -> str:
    match = _FENCE_RE.match(text or "")
    return match.group(1) if match else (text or "")


# ---------------------------------------------------------------------------
# Residual live-reliability bug (JSON double-encoding under
# `response_format` json_schema): Claude Sonnet endpoints
# (`databricks-claude-sonnet-5`, and the user's own
# `benoit_cayla.ontobricks-todrop.monclaudesonnetamoi`) intermittently
# return a JSON-schema-constrained value ENCODED AS A STRING rather than the
# raw JSON value the schema demands — e.g. a list-typed field comes back as
# `"[{...}]"` instead of `[{...}]`, or the entire payload comes back as a
# JSON string holding the real object as its value (`content =
# "{\"attributes\": [...]}"`). This is an endpoint/transport quirk, not a
# free-text/prose answer, so the tolerance below is narrow and
# deterministic: exactly ONE extra `json.loads` is attempted on a `str`
# value, and its result is used ONLY if it is already the expected type.
# No loop, no regex/prose extraction — anything else (not a `str` to begin
# with, a second decode that raises, or a second decode that still isn't
# `expected`) falls through unchanged and is rejected by the caller's own
# existing type check, exactly as before this fix.
# ---------------------------------------------------------------------------


def _unwrap_double_encoded(value: Any, *, expected: type) -> Any:
    """Undo at most one level of JSON-string double-encoding.

    Returns *value* unchanged unless it is a ``str`` that decodes (via a
    single ``json.loads``) into an instance of *expected* — in which case
    the decoded value is returned instead. Never attempts a second decode.
    """
    if not isinstance(value, str):
        return value
    try:
        decoded = json.loads(value)
    except (ValueError, TypeError):
        return value
    return decoded if isinstance(decoded, expected) else value


def parse_json_object(text: str) -> Dict[str, Any]:
    """Parse *text* (fence-tolerant) into a JSON object or reject it."""
    stripped = _strip_fences(text).strip()
    if not stripped:
        raise SchemaValidationError("empty structured output")
    try:
        data = json.loads(stripped)
    except (ValueError, TypeError) as exc:
        raise SchemaValidationError(f"output is not valid JSON: {exc}") from exc
    if isinstance(data, str):
        # Whole-payload double-encoding quirk (see module note above): the
        # wire content was itself a JSON string whose value is the real
        # object. One extra decode; accepted only if it yields a dict.
        data = _unwrap_double_encoded(data, expected=dict)
    if not isinstance(data, dict):
        raise SchemaValidationError("structured output must be a JSON object")
    return data


# ---------------------------------------------------------------------------
# Stage 1 — detection candidates
# ---------------------------------------------------------------------------


def _coerce_evidence(raw: Any) -> List[Dict[str, str]]:
    evidence: List[Dict[str, str]] = []
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                evidence.append(
                    {
                        "source": str(item.get("source", "") or ""),
                        "excerpt": str(item.get("excerpt", "") or ""),
                    }
                )
            elif item:
                evidence.append({"source": "", "excerpt": str(item)})
    return evidence


def _coerce_alt_labels(raw: Any) -> List[str]:
    if isinstance(raw, list):
        return [str(a) for a in raw if a is not None and str(a).strip()]
    return []


def parse_detection_payload(
    text: str,
    *,
    existing_anchors: Sequence[GenerateEntity] = (),
    max_candidates: int | None = None,
) -> List[GenerateEntity]:
    """Parse a Stage-1 detection answer into deduplicated candidate entities.

    Every returned candidate defaults to ``included=true`` / ``origin=detected``
    (the reviewer opts entities out, not in). A candidate is dropped when it
    duplicates a locked anchor — or an earlier candidate — by id or by any
    normalized canonical/alternate label (synonyms are as authoritative as
    the canonical label for dedup). Synonyms stay on the candidate as
    ``alternate_labels`` and are never emitted as separate candidate entities.
    """
    data = parse_json_object(text)
    raw = data.get("candidate_entities")
    if isinstance(raw, str):
        # Field-level double-encoding quirk (see module note by
        # `_unwrap_double_encoded` above): one extra decode, accepted only
        # if it yields a list.
        raw = _unwrap_double_encoded(raw, expected=list)
    if not isinstance(raw, list):
        raise SchemaValidationError(
            "detection output must contain a 'candidate_entities' list"
        )

    anchor_ids: Set[str] = {a.id for a in existing_anchors}
    seen_labels: Set[str] = set()
    for anchor in existing_anchors:
        seen_labels |= anchor.normalized_labels()

    candidates: List[GenerateEntity] = []
    for item in raw:
        if not isinstance(item, dict):
            raise SchemaValidationError(
                "each candidate_entities item must be a JSON object"
            )
        label = str(item.get("canonical_label", "") or "").strip()
        if not label:
            raise SchemaValidationError(
                "candidate entity is missing a 'canonical_label'"
            )
        type_hint = item.get("type_hint") or TYPE_CLASS
        if type_hint not in _VALID_TYPE_HINTS:
            # Stage 1 only ever proposes classes; coerce a stray hint rather
            # than reject an otherwise-usable candidate.
            type_hint = TYPE_CLASS
        try:
            entity = GenerateEntity.new_candidate(
                label,
                description=str(item.get("description", "") or ""),
                type_hint=type_hint,
                evidence=_coerce_evidence(item.get("evidence")),
                alternate_labels=_coerce_alt_labels(item.get("alternate_labels")),
                origin=ORIGIN_DETECTED,
                included=True,
            )
        except DraftValidationError as exc:
            raise SchemaValidationError(str(exc)) from exc

        if entity.id in anchor_ids:
            continue
        labels = entity.normalized_labels()
        if labels & seen_labels:
            # Deduplicated against a locked anchor or an earlier candidate.
            continue
        seen_labels |= labels
        candidates.append(entity)
        if max_candidates and len(candidates) >= max_candidates:
            break
    return candidates


# ---------------------------------------------------------------------------
# Transport-level structured output (response_format) for Stage 3
# ---------------------------------------------------------------------------
#
# Live-bug fix (id-bracketing failure): the old catalog/prompt rendered each
# entity id in brackets (``[<id>]``) and the closure rule said "reference
# entities ONLY by the ids listed above" — the model copied the bracketed
# token verbatim as `domain`/`range`/`subject`/`object`, and
# `GenerateDraft.validate_references` (which compares against the *bare* id
# from `closed_entity_ids()`) rejected every single reference
# (``relations: references unknown or excluded entity id(s): [Agent],
# [Call], ...``). `prompts._entity_catalog`/`_CLOSURE_RULE` were fixed to
# render/require a bare id (see `prompts.py`), but a prompt-only instruction
# cannot *structurally* prevent a model from re-adding brackets — the same
# lesson already learned for Stage 1's reasoning-preamble problem (see
# `DETECTION_RESPONSE_FORMAT` above). These builders make each completion
# substage's id-valued fields (`domain`/`range`/`subject`/`object`) a
# strict-schema ENUM of exactly `GenerateDraft.closed_entity_ids()`, so an
# out-of-closure OR bracketed/quoted id is structurally impossible on any
# endpoint that honours `response_format` — not merely instructed against.
# Completion never sends `tools`, so there is no tools/response_format
# conflict to guard here (unlike Stage 1's two-phase split).
#
# This does NOT replace the reject-only `validate_references` check run
# after parsing (defense in depth — never trust the wire even with schema
# enforcement) — it must simply always pass now. It also does NOT change
# `parse_*_payload`'s own field contract: every field these schemas require
# is exactly the field the parser already reads.
# ---------------------------------------------------------------------------

_AXIOM_KINDS = ("subClassOf", "disjointWith", "equivalentClass")


def _closed_id_enum(closed_ids: Iterable[str]) -> Dict[str, Any]:
    """A strict ``enum`` schema of the closed entity id set, sorted for a
    deterministic/diffable schema. A fresh dict per call (never shared by
    reference) so two fields (e.g. ``domain``/``range``) each get their own
    schema object."""
    return {"type": "string", "enum": sorted({str(i) for i in closed_ids})}


def _has_more_property() -> Dict[str, Any]:
    """The pagination flag every completion page must carry.

    Paged completion (issue #173): a substage's full answer can exceed the
    endpoint's ~8k-token output budget (a 360-column table's attributes, a
    40-entity draft's relations). The model therefore emits one bounded page
    at a time and sets ``has_more`` when items remain; ``staged.py`` keeps
    asking for the next page until it is false. The field is REQUIRED on an
    honoured strict schema so the page boundary is explicit, never implied.
    """
    return {"type": "boolean"}


def build_relations_response_format(closed_ids: Iterable[str]) -> Dict[str, Any]:
    """Strict ``json_schema`` for Stage-3 relations: ``domain``/``range`` are
    each constrained to the exact closed entity id set — see module note
    above. Mirrors :func:`parse_relations_payload`'s field contract
    (``label``, ``domain``, ``range``) plus optional ``evidence``."""
    return {
        "type": "json_schema",
        "json_schema": {
            "name": "relations_output",
            "strict": True,
            "schema": {
                "type": "object",
                "properties": {
                    "relations": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "label": {"type": "string"},
                                "domain": _closed_id_enum(closed_ids),
                                "range": _closed_id_enum(closed_ids),
                                "evidence": {"type": ["string", "null"]},
                            },
                            "required": ["label", "domain", "range", "evidence"],
                            "additionalProperties": False,
                        },
                    },
                    "has_more": _has_more_property(),
                },
                "required": ["relations", "has_more"],
                "additionalProperties": False,
            },
        },
    }


def build_attributes_response_format(closed_ids: Iterable[str]) -> Dict[str, Any]:
    """Strict ``json_schema`` for Stage-3 attributes: ``domain`` is
    constrained to the exact closed entity id set — see module note above.
    Mirrors :func:`parse_attributes_payload`'s field contract (``label``,
    ``domain``, ``datatype``) plus optional ``evidence``."""
    return {
        "type": "json_schema",
        "json_schema": {
            "name": "attributes_output",
            "strict": True,
            "schema": {
                "type": "object",
                "properties": {
                    "attributes": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "label": {"type": "string"},
                                "domain": _closed_id_enum(closed_ids),
                                "datatype": {"type": "string"},
                                "evidence": {"type": ["string", "null"]},
                            },
                            "required": ["label", "domain", "datatype", "evidence"],
                            "additionalProperties": False,
                        },
                    },
                    "has_more": _has_more_property(),
                },
                "required": ["attributes", "has_more"],
                "additionalProperties": False,
            },
        },
    }


def build_axioms_response_format(closed_ids: Iterable[str]) -> Dict[str, Any]:
    """Strict ``json_schema`` for Stage-3 axioms: ``subject``/``object`` are
    each constrained to the exact closed entity id set — see module note
    above. Mirrors :func:`parse_axioms_payload`'s field contract (``kind``,
    ``subject``, ``object``); ``kind`` is constrained to the same 3-value
    enum the axioms prompt documents."""
    return {
        "type": "json_schema",
        "json_schema": {
            "name": "axioms_output",
            "strict": True,
            "schema": {
                "type": "object",
                "properties": {
                    "axioms": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "kind": {"type": "string", "enum": list(_AXIOM_KINDS)},
                                "subject": _closed_id_enum(closed_ids),
                                "object": _closed_id_enum(closed_ids),
                            },
                            "required": ["kind", "subject", "object"],
                            "additionalProperties": False,
                        },
                    },
                    "has_more": _has_more_property(),
                },
                "required": ["axioms", "has_more"],
                "additionalProperties": False,
            },
        },
    }


# ---------------------------------------------------------------------------
# Stage 3 — completion outputs
# ---------------------------------------------------------------------------


def _require_list(data: Dict[str, Any], key: str) -> List[Dict[str, Any]]:
    raw = data.get(key)
    if isinstance(raw, str):
        # Field-level double-encoding quirk (see module note above): the
        # list-typed field came back as a JSON string holding the real
        # array as its value. One extra decode; accepted only if it
        # yields a list.
        raw = _unwrap_double_encoded(raw, expected=list)
    if not isinstance(raw, list):
        raise SchemaValidationError(f"output must contain a '{key}' list")
    for item in raw:
        if not isinstance(item, dict):
            raise SchemaValidationError(f"each '{key}' item must be a JSON object")
    return raw


def _require_fields(item: Dict[str, Any], fields: Sequence[str], kind: str) -> None:
    for field_name in fields:
        value = item.get(field_name)
        if value is None or (isinstance(value, str) and not value.strip()):
            raise SchemaValidationError(f"{kind} is missing required '{field_name}'")


def _coerce_has_more(raw: Any) -> bool:
    """Read the ``has_more`` pagination flag (paged completion, issue #173).

    Tolerant by design: a missing field means "no more pages" (single-page
    back-compat), and a stray ``"true"``/``"false"`` string is coerced —
    endpoints under ``response_format`` occasionally stringify bools the same
    way they double-encode lists (see ``_unwrap_double_encoded``).
    """
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        return raw.strip().casefold() == "true"
    return False


def drop_inverse_relations(
    relations: List[Dict[str, Any]],
    existing: Sequence[Dict[str, Any]] = (),
) -> List[Dict[str, Any]]:
    """Collapse semantic duplicates and inverses (see :class:`RelationDeduplicator`)."""
    return RelationDeduplicator.dedupe(relations, existing=existing)


def parse_relations_payload(
    text: str, existing: Sequence[Dict[str, Any]] = ()
) -> Dict[str, Any]:
    """Parse a Stage-3 relations page: ``{"relations": [...], "has_more": bool}``.

    Paraphrases, inverses, and restatements of ``existing`` relations are
    dropped; distinct predicates on the same entity pair are kept.
    """
    data = parse_json_object(text)
    relations = _require_list(data, "relations")
    for rel in relations:
        _require_fields(rel, ("label", "domain", "range"), "relation")
    return {
        "relations": drop_inverse_relations(relations, existing),
        "has_more": _coerce_has_more(data.get("has_more")),
    }


def parse_attributes_payload(text: str) -> Dict[str, Any]:
    """Parse a Stage-3 attributes page: ``{"attributes": [...], "has_more": bool}``."""
    data = parse_json_object(text)
    attributes = _require_list(data, "attributes")
    for attr in attributes:
        _require_fields(attr, ("label", "domain", "datatype"), "attribute")
    return {
        "attributes": attributes,
        "has_more": _coerce_has_more(data.get("has_more")),
    }


def drop_multi_parent_subclass_axioms(
    axioms: List[Dict[str, Any]],
    existing_parents: Optional[Mapping[str, str]] = None,
) -> List[Dict[str, Any]]:
    """Keep at most one ``subClassOf`` parent per subject.

    Subjects that already have a parent in the live ontology (``existing_parents``)
    lose every extra ``subClassOf`` axiom. Within the payload, the first
    ``subClassOf`` for a subject is kept and later ones are dropped. Other
    axiom kinds are unchanged.
    """
    assigned: Dict[str, str] = {
        str(subject): str(parent)
        for subject, parent in (existing_parents or {}).items()
        if subject and parent
    }
    kept: List[Dict[str, Any]] = []
    for axiom in axioms:
        if axiom.get("kind") != "subClassOf":
            kept.append(axiom)
            continue
        subject = str(axiom.get("subject") or "")
        obj = str(axiom.get("object") or "")
        current = assigned.get(subject)
        if current:
            logger.info(
                "Dropping extra subClassOf %s -> %s (already has parent %s)",
                subject,
                obj,
                current,
            )
            continue
        assigned[subject] = obj
        kept.append(axiom)
    return kept


def parse_axioms_payload(
    text: str, existing_parents: Optional[Mapping[str, str]] = None
) -> Dict[str, Any]:
    """Parse a Stage-3 axioms answer: ``{"axioms": [{kind, subject, object}]}``.

    Extra ``subClassOf`` parents (existing or repeated in the payload) are
    dropped so Generate never proposes multiple inheritance.
    """
    data = parse_json_object(text)
    axioms = _require_list(data, "axioms")
    for axiom in axioms:
        _require_fields(axiom, ("kind", "subject", "object"), "axiom")
    return {
        "axioms": drop_multi_parent_subclass_axioms(axioms, existing_parents),
        "has_more": _coerce_has_more(data.get("has_more")),
    }


def relations_referenced_ids(result: Dict[str, Any]) -> Set[str]:
    ids: Set[str] = set()
    for rel in result.get("relations", []):
        for role in ("domain", "range"):
            value = rel.get(role)
            if value:
                ids.add(str(value))
    return ids


def attributes_referenced_ids(result: Dict[str, Any]) -> Set[str]:
    return {
        str(attr["domain"])
        for attr in result.get("attributes", [])
        if attr.get("domain")
    }


def axioms_referenced_ids(result: Dict[str, Any]) -> Set[str]:
    ids: Set[str] = set()
    for axiom in result.get("axioms", []):
        for role in ("subject", "object"):
            value = axiom.get(role)
            if value:
                ids.add(str(value))
    return ids
