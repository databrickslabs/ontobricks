"""Prompt-first stage prompts for the staged owl-generator.

Per the design's *Prompt-First Pitfall Handling (Rewrite Loop Removed)*
section and SPEC §6a: pitfall constraints (naming rules, orphan avoidance,
domain/range completeness, class-count guidance) are stated **up front** in
each stage's system prompt instead of being corrected after the fact by a
post-generation rewrite loop. Deterministic validation still runs afterwards
but is reject-only (see :mod:`agents.agent_owl_generator.schemas` and
:meth:`GenerateDraft.validate_references`).

Every prompt is JSON-output only — the staged agent produces bounded
structured records addressed by stable entity id, never free-text Turtle.
"""

from __future__ import annotations

from typing import Sequence

from back.objects.ontology.generate.GenerateDraft import GenerateDraft, GenerateEntity

# Explicit zero-new-candidate contract (live-bug fix): stated unconditionally
# and prominently in Stage 1's system prompt, right after the anchors listing.
# Root cause of the live failure this fixes: a session whose every selected
# table's core entity was already a locked anchor gave the model NO new
# grounded candidate. The prompt said "JSON only" but never defined what to
# return in that case, so the model replied with prose/refusal instead of a
# structured answer, and detect_entities correctly rejected it as malformed
# JSON — a confusing failure for a case that should always succeed. This is a
# prompt-only fix: the reject-only architecture and the schema (which already
# accepts `{"candidate_entities": []}`) are unchanged — no post-validation
# rewrite/re-prompt loop is introduced.
_ZERO_CANDIDATE_CONTRACT = """\
# ZERO-NEW-CANDIDATE CONTRACT (CRITICAL — READ BEFORE ANSWERING)
Existing ontology entities (listed above, if any) are valid CONTEXT for your
reasoning — they are NOT candidates and must never be re-emitted. If, after
reviewing the metadata and any ready document, every real-world entity the
domain needs is already one of those locked anchors — or no genuinely NEW
grounded entity exists at all — that is a normal, SUCCESSFUL outcome, not an
error. In that case you MUST return exactly:
{"candidate_entities": []}
Do NOT explain why the list is empty. Do NOT refuse or apologize. Do NOT
write prose, a summary, or a markdown code fence — the empty JSON object
above, verbatim, is the entire (and complete) answer."""

# Shared naming constraints, stated once and injected into every stage prompt.
_NAMING_RULES = """\
# NAMING RULES (CRITICAL — NO EXCEPTIONS)
• Classes: PascalCase (Customer, SalesOrder).
• Object/data properties: lowerCamelCase (placesOrder, orderDate).
• No spaces, underscores, hyphens, or escapes in local names.
• NEVER embed the domain or range class name inside a property name
  (❌ hasPersonName / orderContainsItem — ✅ hasName / contains)."""


# ---------------------------------------------------------------------------
# Stage 1 — detection
# ---------------------------------------------------------------------------


def _anchor_lines(existing_anchors: Sequence[GenerateEntity]) -> str:
    lines = []
    for anchor in existing_anchors:
        alt = ", ".join(anchor.alternate_labels)
        suffix = f" (also known as: {alt})" if alt else ""
        lines.append(f"  • {anchor.canonical_label} [{anchor.id}]{suffix}")
    return "\n".join(lines)


def build_detection_system_prompt(
    *, existing_anchors: Sequence[GenerateEntity] = ()
) -> str:
    """System prompt for Stage 1 candidate-entity detection.

    Lists the locked anchors (canonical + alternate labels) so the model
    deduplicates against them and never re-proposes an existing entity.
    """
    anchors_block = _anchor_lines(existing_anchors)
    if anchors_block:
        anchors_section = (
            "# EXISTING ONTOLOGY ENTITIES (LOCKED ANCHORS — DO NOT RE-PROPOSE)\n"
            "These already exist. Never return any of them (or any of their "
            "alternate labels) as a NEW candidate:\n"
            f"{anchors_block}\n"
        )
    else:
        anchors_section = (
            "# EXISTING ONTOLOGY ENTITIES\nThe ontology is currently empty.\n"
        )

    return f"""\
You are an ontology engineer performing ENTITY DETECTION only.

Read the selected table, view, and Unity Catalog metric-view metadata and
the READY parsed documents (use your tools) and propose the real-world
ENTITIES (classes) the domain needs. You do
NOT design relations, attributes, or axioms — that happens in a later, human-
reviewed stage. Propose one class per real-world entity; never a class per
column or per attribute value.

# UNITY CATALOG METRIC VIEWS
A source with object_kind "metric_view" is a semantic metric view, not a
flat table. Columns carry role "dimension" or "measure":
• Dimensions (region, customer_id, store, month, …) identify real-world
  entities. Propose a candidate class for each distinct business entity a
  dimension represents, unless it is already a locked anchor.
• Measures (revenue, order_count, total_sales, …) are numeric facts ABOUT
  those entities. NEVER propose a class named after a measure.
• Do not skip a metric view or treat it as empty schema. An empty
  candidate_entities list is valid only when every dimension entity is already
  an anchor.

{anchors_section}
{_ZERO_CANDIDATE_CONTRACT}

# SYNONYMS
Any synonym you find in metadata comments or document text (e.g. "Client" for
"Customer") is an ALTERNATE LABEL of a single candidate — put it in
`alternate_labels`, NEVER as a separate candidate entity and NEVER only in the
description prose.

{_NAMING_RULES}

# OUTPUT (JSON ONLY — NO PROSE, NO CODE FENCES)
Your ENTIRE reply is ONLY the JSON object below. The FIRST CHARACTER of your
reply MUST be `{{`. Never precede it with an analysis section, step-by-step
reasoning, a "Here is my answer" preamble, or any other sentence — even
after you finish gathering context with tools, go straight to the JSON.
Return a single JSON object:
{{"candidate_entities": [
  {{"canonical_label": "Carrier",
    "description": "Company that ships an Order to a Customer.",
    "type_hint": "class",
    "evidence": [{{"source": "spec.pdf", "excerpt": "Order ships via Carrier."}}],
    "alternate_labels": ["Shipper", "Freight Company"]}}
]}}
Prefer 8–25 candidates for a typical domain; hard cap 40. Ground every
candidate in the metadata or a ready document. If nothing new is grounded
(see the ZERO-NEW-CANDIDATE CONTRACT above), return
{{"candidate_entities": []}} instead — an empty list is a complete, valid
answer, never an error to explain."""


def build_detection_user_prompt(
    *, guidelines: str, selected_tables: Sequence[str], selected_docs: Sequence[str]
) -> str:
    parts = []
    if selected_tables:
        parts.append(f"Selected tables: {', '.join(selected_tables)}")
    if selected_docs:
        parts.append(f"Selected documents: {', '.join(selected_docs)}")
    parts.append(
        "Guidelines: "
        + (guidelines or "Detect the core entities of the domain.")
    )
    parts.append(
        "Use get_metadata / get_table_detail to inspect tables, views, and "
        "metric views (object_kind + column roles) and "
        "list_documents / read_document to read READY documents, then return "
        "the candidate_entities JSON."
    )
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Stage 3 — completion (relations / attributes / axioms)
# ---------------------------------------------------------------------------


def _entity_catalog(draft: GenerateDraft) -> str:
    """The closed, id-addressed entity set completion may reference.

    Only locked anchors and *included* candidates appear — excluded
    candidates are deliberately absent (they are outside the closure and any
    reference to them is rejected). Alternate labels are surfaced as lexical
    evidence for naming heuristics.

    **Bare-id rendering (live-bug fix).** The id is rendered as a plain
    ``id: <value>`` field, never bracketed (``[<value>]``) or quoted. A
    bracketed id (e.g. ``[Agent]``) previously caused a live failure: the
    old catalog rendered ``  • [Agent] Agent`` and the closure rule said
    "reference entities ONLY by the ids listed above" — the model copied the
    bracketed token verbatim as the `domain`/`range` value, and
    :meth:`GenerateDraft.validate_references` (which compares against the
    bare id from :meth:`GenerateDraft.closed_entity_ids`) rejected every
    single reference. There is nothing left in this rendering a model could
    mistake for part of the id value itself.
    """
    lines = []
    for entity in (*draft.existing_anchors, *draft.candidate_entities):
        if not entity.included:
            continue
        alt = ", ".join(entity.alternate_labels)
        alt_suffix = f" | alt: {alt}" if alt else ""
        desc = f" — {entity.description}" if entity.description else ""
        parent_line = f"\n  parent: {entity.parent}" if entity.parent else ""
        lines.append(
            f"- id: {entity.id}\n  label: {entity.canonical_label}"
            f"{alt_suffix}{desc}{parent_line}"
        )
    return "\n".join(lines)


_CLOSURE_RULE = (
    "You MUST reference entities ONLY by the exact bare id value shown "
    "after `id:` in the closed entity set below — copy that value exactly, "
    "character for character, with NO brackets and NO quotes around it, "
    "and NEVER substitute the label text instead. For example, if the "
    "catalog shows `id: Agent` / `label: Agent`, the value you must use is "
    "Agent — never [Agent], never \"Agent\", and never a paraphrase of the "
    "label. NEVER invent a new entity or a new id. Any output referencing "
    "an id not listed above — including a bracketed or quoted form of a "
    "valid id — will be rejected."
)


# Paged-output contract (issue #173): a substage's full answer can exceed the
# endpoint's ~8k-token output budget (a wide table's attributes, a large
# draft's relations). The model emits one bounded page and sets ``has_more``
# when items remain; the orchestrator asks for the next page until it is
# false. Shared by the three completion system prompts.
_PAGINATION_RULE = """\
# PAGED OUTPUT (BOUNDED — CRITICAL)
The COMPLETE answer may not fit in one response. Emit a bounded page of
COMPLETE items and set "has_more": true when more items remain — you will be
asked for the next page. Never cut an item mid-record. Set "has_more": false
on the last page (or when nothing remains). Do not re-emit items you already
returned on an earlier page."""


def build_continuation_user_prompt(substage: str, emitted_count: int) -> str:
    """Continuation turn asking for the next page of a completion substage."""
    return (
        f"Continue. Emit the NEXT page of {substage} — only NEW items, none "
        f"of the {emitted_count} already emitted. Same JSON shape; set "
        '"has_more": true only if more items remain after this page.'
    )


def build_truncated_page_retry_prompt(substage: str) -> str:
    """Re-ask for the SAME page with fewer items after a length truncation."""
    return (
        "Your previous page was cut off by the output limit. Re-emit THAT "
        f"page of {substage} with FEWER items — a smaller page is fine, set "
        '"has_more": true and continue on the next turn. JSON only.'
    )


def build_relations_system_prompt() -> str:
    return f"""\
You are an ontology engineer inferring OBJECT-PROPERTY RELATIONS between a
fixed, closed set of entities.

{_CLOSURE_RULE}

{_NAMING_RULES}
• One fact = ONE relation. Never emit a relation AND its inverse
  (handles vs handled, settles vs settledBy, owns vs ownedBy, hasX vs isXOf),
  and never a paraphrase of the same fact in either direction (settledBy
  and isSettledBy are the same relation). Use the active voice (settles,
  not isSettledBy).
• Distinct business facts between the same two entities are allowed
  (Customer places Order AND Customer cancels Order).
• Never restate a relation listed under EXISTING OBJECT PROPERTIES, in
  either direction or as a paraphrase — only add genuinely new facts.

{_PAGINATION_RULE}

# OUTPUT (JSON ONLY — NO PROSE, NO CODE FENCES)
{{"relations": [
  {{"label": "placesOrder", "domain": "<entity_id>", "range": "<entity_id>",
    "evidence": "short justification"}}
], "has_more": false}}"""


def build_attributes_system_prompt() -> str:
    return f"""\
You are an ontology engineer inferring DATATYPE ATTRIBUTES for a fixed, closed
set of entities.

{_CLOSURE_RULE}

{_NAMING_RULES}
• Each attribute has an xsd datatype (xsd:string, xsd:integer, xsd:date, …).
• Exclude surrogate keys, audit columns, and foreign keys already carried by
  relations.

{_PAGINATION_RULE}

# OUTPUT (JSON ONLY — NO PROSE, NO CODE FENCES)
{{"attributes": [
  {{"label": "orderDate", "domain": "<entity_id>", "datatype": "xsd:date",
    "evidence": "short justification"}}
], "has_more": false}}"""


def build_axioms_system_prompt() -> str:
    return f"""\
You are an ontology engineer inferring lightweight OWL AXIOMS over a fixed,
closed set of entities.

{_CLOSURE_RULE}

# RULES
• kind ∈ {{subClassOf, disjointWith, equivalentClass}}.
• Prefer OWL 2 EL-like modelling; add an axiom only when justified.
• Never make a class disjoint from its own subclass.
• Single inheritance only: each class may have at most ONE subClassOf parent.
• If an entity already lists a parent in the closed entity set (or under
  EXISTING SUBCLASS AXIOMS), do NOT emit a subClassOf axiom for that
  subject — neither a second parent nor a restatement. The ontology model
  cannot represent multiple inheritance.
• Never emit two subClassOf axioms with the same subject.

{_PAGINATION_RULE}

# OUTPUT (JSON ONLY — NO PROSE, NO CODE FENCES)
{{"axioms": [
  {{"kind": "subClassOf", "subject": "<entity_id>", "object": "<entity_id>"}}
], "has_more": false}}"""


def _prior_results_block(draft: GenerateDraft, substages: Sequence[str]) -> str:
    lines = []
    for substage in substages:
        checkpoint = draft.completion_checkpoints.get(substage) or {}
        if checkpoint.get("status") == "done" and checkpoint.get("result"):
            lines.append(
                f"Already computed {substage}: {checkpoint['result']}"
            )
    return "\n".join(lines)


# Trailing instruction wording (live-verification finding, this revision).
# Live probing of `databricks-claude-sonnet-5` while verifying the
# id-bracketing fix surfaced a SEPARATE quirk this same revision exposes:
# once a completion substage sends `response_format` (it never did before
# this fix), a trailing user-turn instruction phrased "Return the <X>
# JSON." sometimes makes the model double-encode its answer — it puts a
# JSON *string* containing the real answer as the value of the top-level
# array key instead of the array itself (e.g.
# ``{"relations": "{\"relations\": [...]}"}"``), which fails
# `parse_relations_payload`'s `'relations' must be a list` check. Reworded
# to "Now infer and provide the <x>." (verified live to answer correctly,
# repeatably) — a wording-only prompt change, not a parsing workaround: no
# lenient/tolerant parsing was added for the double-encoded shape, and
# `validate_references`/reject-only stay unweakened either way.
def _existing_relations_block(existing_relations: Sequence[dict]) -> str:
    lines = [
        f"- {rel.get('domain')} --{rel.get('label')}--> {rel.get('range')}"
        for rel in existing_relations
    ]
    if not lines:
        return ""
    return (
        "EXISTING OBJECT PROPERTIES (already in the ontology — do not "
        "restate, invert, or paraphrase):\n" + "\n".join(lines) + "\n\n"
    )


def build_relations_user_prompt(
    draft: GenerateDraft, existing_relations: Sequence[dict] = ()
) -> str:
    return (
        "Closed entity set (reference ONLY these ids):\n"
        f"{_entity_catalog(draft)}\n\n"
        f"{_existing_relations_block(existing_relations)}"
        "Now infer and provide the relations."
    )


def build_attributes_user_prompt(draft: GenerateDraft) -> str:
    prior = _prior_results_block(draft, ("relations",))
    prior_section = f"\n{prior}\n" if prior else "\n"
    return (
        "Closed entity set (reference ONLY these ids):\n"
        f"{_entity_catalog(draft)}\n"
        f"{prior_section}"
        "\nNow infer and provide the attributes."
    )


def _existing_subclass_block(draft: GenerateDraft) -> str:
    lines = [
        f"- {subject} subClassOf {parent}"
        for subject, parent in draft.existing_parents().items()
    ]
    if not lines:
        return ""
    return (
        "EXISTING SUBCLASS AXIOMS (already in the ontology — do not add "
        "another parent for these subjects; single inheritance only):\n"
        + "\n".join(lines)
        + "\n\n"
    )


def build_axioms_user_prompt(draft: GenerateDraft) -> str:
    prior = _prior_results_block(draft, ("relations", "attributes"))
    prior_section = f"\n{prior}\n" if prior else "\n"
    return (
        "Closed entity set (reference ONLY these ids):\n"
        f"{_entity_catalog(draft)}\n"
        f"{prior_section}"
        f"{_existing_subclass_block(draft)}"
        "\nNow infer and provide the axioms."
    )
