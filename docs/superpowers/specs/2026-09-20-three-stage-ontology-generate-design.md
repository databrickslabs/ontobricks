# Three-Stage Ontology Generate Design

> Plan: `staged-ontology-generate` (Version 0.9.0).

## Goal

Refactor the ontology Generate wizard from a single one-shot LLM call into a
durable three-stage workflow: detect candidate entities, pause for human
review, then complete relations, attributes, and axioms in checkpointed
append mode. Existing ontology entities are never replaced — they are locked
anchors that the completion stages may relate to or enrich, never rename or
delete.

## Scope

This design covers:

- the three-step Generate wizard (Configure/Detect → Review Entities →
  Complete) and its resumable durable draft;
- Stage 1 detection inputs (selected metadata, ready parsed corpus, current
  ontology) and outputs (bounded candidate entities with provenance);
- Stage 2 human review: add, remove, include/exclude, and edit canonical
  label, description, type hint, evidence, and alternate labels;
- Stage 3 completion order (relations → attributes → axioms), its
  checkpoints, and resume behavior;
- source fingerprinting and invalidation;
- stable entity identity across rename and across stages;
- append-only final merge semantics that preserve existing entity identity;
- removal of the post-generation pitfall rewrite loop in favor of
  prompt-first constraints and reject-only validation;
- the `agent_owl_generator` staged entry-point contract, tracing, and eval
  impact.

It does not cover:

- Mapping, business-rules generation, SHACL/SWRL/axiom editors outside the
  Generate wizard, or any agent other than `agent_owl_generator`;
- semantic retrieval, embeddings, or a document chunk index;
- multi-user concurrent editing of the same draft (last-writer-wins with
  optimistic revision checks is sufficient for this design);
- changes to the parsed-document corpus lifecycle itself (see
  `docs/superpowers/specs/2026-09-18-parsed-document-corpus-design.md`); this
  design only consumes that corpus and never triggers parsing.

## Current State

`src/front/templates/partials/ontology/_ontology_wizard.html` and
`src/front/static/ontology/js/ontology-wizard.js` implement a single-form
wizard: the user picks metadata, documents, and guidelines, then calls
`generateOntologyFromWizard()`, which posts to
`POST /ontology/wizard/generate-async`
(`src/api/routers/internal/ontology.py`). That route runs
`Ontology.generate_with_agent()` (`src/back/objects/ontology/Ontology.py`) in
a background thread, which calls `agent_owl_generator.run_agent()`
(`src/agents/agent_owl_generator/engine.py`) once. The agent iterates
internally (bounded by `MAX_ITERATIONS`), and after each OWL text answer the
engine invokes `tool_check_owl_pitfalls` directly (not exposed to the LLM as
a tool call) and, if warnings are returned, re-prompts the model to rewrite
the Turtle in place — a **post-generation pitfall rewrite loop**. A second,
bounded consolidation/evaluator loop (`MAX_OWL_EVAL_ROUNDS`,
`_evaluate_ontology_stage`) may also ask the model to re-emit the ontology.
The task result is a single OWL document; `applyWizardOntology()` in the
frontend explicitly warns "This will replace your current ontology with the
generated one" and calls the import path, which **replaces** the ontology
rather than appending to it. There is no draft persisted between the request
and the completed task: `wizardCurrentTaskId` lives only in
`sessionStorage`, and a full page/process restart mid-generation loses all
progress and forces starting over.

This has three problems this design fixes:

1. **No review step.** The user cannot see, edit, add, or remove detected
   entities before the ontology is generated and applied.
2. **Replace, not append.** Existing entities and their edits/relationships
   are discarded on every Generate run; there is no way to grow an ontology
   incrementally while keeping prior work.
3. **No durability.** A page reload, browser crash, or app restart during
   generation loses the in-flight work with no resumable checkpoint.

## Considered Approaches

### A. In-place iterative refinement (status quo, extended)

Keep the one-shot call but let the user "regenerate" repeatedly and manually
merge results.

Limitations: the model still owns global structure decisions with no
human-in-the-loop review point; merging is manual and error-prone; provides
no fix for durability or append semantics.

### B. Single staged call with client-side pause

Run detection and completion as one agent invocation, but keep the response
in the browser and let the user edit before "applying" locally.

Limitations: still loses everything on reload — the durable draft
requirement (`Draft.__init__` in
[`docs/superpowers/specs/2026-09-18-parsed-document-corpus-design.md`](2026-09-18-parsed-document-corpus-design.md)'s
sibling durability pattern) is exactly what a page reload cannot honor with
client-only state; browser storage cannot survive "process restart" of the
backend, and multi-tab/session recovery is not possible.

### C. Server-persisted staged workflow (chosen)

Persist a versioned Generate draft on the server (through
`DomainSession.py`, alongside the domain's other durable session state),
expose staged agent entry points, and drive Stage 3 through a
checkpoint-per-substage state machine. The frontend becomes a thin, resumable
client of that server state.

Advantages:

- survives modal close, page reload, and backend process restart because the
  draft lives in the same persisted session storage as the rest of the
  domain, not in memory or `sessionStorage`;
- gives a single source of truth for "what stage are we in" so the UI can
  resume deterministically;
- keeps the append/anchor invariant enforceable server-side instead of
  trusting client state.

Limitations: requires new persisted schema and migration care in
`DomainSession.py`; requires the staged agent contract to be more structured
than free-form OWL text.

## Decision

Adopt Approach C. The existing three-step **Generate** wizard becomes:

1. **Configure/Detect** — reuse the current metadata/document/guideline
   selectors; submitting starts Stage 1 detection instead of full
   generation.
2. **Review Entities** — human-in-the-loop review of the durable draft.
3. **Complete** — Stage 3 runs relations → attributes → axioms with visible
   per-substage progress, then performs the validated append-only merge and
   navigates to the map, as today.

There is no one-shot default: no UI control and no API route silently
performs single-call generation of a full ontology anymore.

## Stage 1: Detection

**Inputs:**

- selected metadata (tables/columns), exactly as gathered by the existing
  metadata selector;
- the ready parsed-document corpus for the domain, read only through the
  existing `list_documents` / `read_document` tools
  (`src/agents/agent_owl_generator/tools.py`) — pending/failed documents are
  disclosed as unavailable and never trigger parsing (see
  [no-reparse contract](#no-reparse-contract) below);
- the current ontology's existing entities, passed as **locked anchors**
  (id, canonical label, alternate labels, type) so the detector can
  deduplicate against them and must not re-propose them as new.

**Output:** a bounded set of `CandidateEntity` records (see
[Structured Schemas](#structured-schemas)) with provenance (which
table/column or which document snippet justified the candidate). Detection
never fabricates relations, attributes, or axioms — those belong to Stage 3
and are computed only from the validated Stage 2 output.

**Default state:** every detected candidate is created with `included=true`.
The user opts entities out in Stage 2; nothing requires an opt-in click to
keep the model's suggestions, but empty results still require the invariant
below.

## Stage 2: Human Review

The Review Entities step renders two groups:

- **Existing entities (locked anchors).** Rendered read-only with a visible
  lock indicator. Their `id`, canonical label, and alternate labels cannot
  be changed from this screen. Stage 3 may add relations/attributes/axioms
  that reference them, or add new alternate labels/evidence discovered
  during completion, but their identity (`id` and canonical label) never
  changes here.
- **Candidate entities (new).** Each has inline controls to:
  - toggle **include/exclude** (default: included);
  - **remove** entirely from the draft (distinct from exclude — removal
    deletes the candidate row; exclude keeps it visible but out of the
    validated set, so a user can re-include it later without re-detecting);
  - **edit** canonical label, description, type hint, evidence (provenance
    text), and a repeatable list of **alternate labels**;
  - **add** a brand-new manual entity not proposed by detection, with the
    same editable fields.

**Invariant:** at least one entity (existing anchor or included new
candidate) must remain part of the validated set before Stage 3 can start.
An empty set (all anchors excluded — which is not possible since anchors are
locked and always counted — and all candidates removed or excluded with no
prior anchors) blocks Continue with an explicit message; this can only
happen on a brand-new ontology with zero detected/kept candidates.

**Synonyms as first-class alternate labels.** Any synonym detected in the
source corpus or metadata (e.g., a column comment or glossary entry naming
"Client" for a "Customer" class) becomes an `alternate_labels` entry on the
candidate, not a separate candidate entity and not prose buried in
`description`. Alternate labels are also carried into Stage 3 as lexical
evidence: relation/attribute/axiom naming heuristics may use alternate
labels to match evidence text, and deduplication logic treats alternate
labels as equally authoritative to the canonical label when detecting
duplicates against locked anchors.

## Stable Identity

Every entity (anchor or candidate) has a stable `id` assigned once (anchors
keep their existing ontology-assigned identity; new candidates get a
detection-time UUID). Renaming the canonical label, editing the description,
or adding/removing alternate labels **never changes `id`**. Stage 3 payloads,
checkpoints, and the final merge all reference entities by `id`, so a
mid-review rename cannot orphan a relation, attribute, or axiom that was
computed (or is being computed) against that entity. Identity is the
join key across detection → review → completion → merge.

## Durable Draft

The Generate draft is a new persisted object stored via `DomainSession.py`
(sibling to the domain's other durable session state — the same mechanism
that survives modal close and page reload today for other Generate/session
data), not in the in-memory `TaskManager` and not only in browser
`sessionStorage`. Browser storage is limited to the currently-active
**task id** for progress polling; every other piece of state (candidates,
edits, stage, checkpoints) is read from the server draft on each screen
load.

**Draft fields** (see [Structured Schemas](#structured-schemas) for exact
shapes):

- `draft_revision` — monotonically increasing integer for optimistic
  concurrency; every write must supply the revision it read, and a stale
  write is rejected with a conflict rather than silently overwritten.
- `source_fingerprint` — a hash over the selected metadata, the ready
  document manifest identities (name + content hash, not content), and the
  existing ontology's entity identity set (ids + canonical labels).
- `selected_source_config` — the metadata/document selection that produced
  the current candidates, kept for display and for fingerprint
  recomputation.
- `existing_anchors` — locked snapshot of current ontology entities at
  detection time (for display only; the live ontology is still the source
  of truth at merge time).
- `candidate_entities` — the editable Stage 2 list.
- `stage` — one of `detecting`, `reviewing`, `completing`, `done`.
- `completion_checkpoints` — per-substage status and result for
  `relations`, `attributes`, `axioms` (each `pending`, `running`, `done`, or
  `failed`, with the validated partial result attached once `done`).

**Survives:** modal close, full page reload, and backend process restart —
because it is read from persisted session storage on load, not
reconstructed from in-memory task state. Only an active in-flight task's
progress bar needs the transient task id; the underlying data does not
depend on that task surviving.

## Source Fingerprint and Invalidation

The fingerprint is recomputed from the *current* selected metadata, the
*current* ready-document manifest identities, and the *current* ontology
identity set, then compared to `source_fingerprint` on the draft. A mismatch
means one of:

- the user changed the metadata/document selection since detecting;
- a previously pending document became ready (or a document's content
  changed, changing its manifest hash);
- the ontology's existing entities changed since detecting (e.g., a
  concurrent edit or a completed prior Generate run).

**On mismatch:** the draft is invalidated. The Review and Complete screens
refuse to resume with a stale fingerprint; the user must re-run Stage 1
detection. Invalidation never silently reuses stale candidates against a
changed source, and it never silently reparses a document to "refresh" the
fingerprint — a fingerprint change is detected from manifest identity alone
(see [no-reparse contract](#no-reparse-contract)).

## Stage 3: Completion (Relations → Attributes → Axioms)

Stage 3 consumes **only the persisted, validated Stage 2 contract**: locked
anchors plus the included candidate set, each addressed by stable `id`. It
never re-reads raw metadata or documents directly — evidence and alternate
labels captured during review are the completion inputs, not a fresh corpus
read. (Re-running Stage 1 detection is the only way to pull in new source
material; Stage 3 cannot expand the entity set.)

**Strict order:** `relations` → `attributes` → `axioms`. Each substage:

1. runs its `agent_owl_generator` entry point against the validated entity
   set (plus, for `attributes` and `axioms`, the relations/attributes
   already produced and checkpointed by prior substages);
2. is validated server-side (schema shape, referenced-id containment — see
   [Validation](#validation-and-the-removed-rewrite-loop));
3. is persisted as a checkpoint **before** the next substage starts.

**Resume:** if the process or request fails partway (e.g., after `relations`
checkpoints but before `attributes` starts, or mid-`attributes`), resuming
Stage 3 re-reads `completion_checkpoints`, skips substages already `done`,
and starts from the first substage that is not `done`. A substage is never
re-run once checkpointed `done`, and substages never run out of order — the
resume logic cannot start `axioms` before `attributes` is `done` even if a
retry request nominally asks to "continue."

**Entity closure invariant:** no Stage 3 substage — relations, attributes,
or axioms — may introduce an entity beyond the locked anchors plus the
validated new set from Stage 2. If a substage's output references an
unknown id, that output is rejected (see Validation) rather than silently
accepted with a dangling or newly-invented reference.

## Final Merge

Once all three substages are `done`, the draft applies a **validated,
append-only merge** into the live ontology:

- existing entities keep their `id`, canonical label, and history; the merge
  may add relations, attributes, or axioms that connect to them, and may
  add newly-discovered alternate labels/evidence, but never renames or
  deletes an existing entity;
- new candidate entities (the included Stage 2 set) are added as new
  ontology entities, using the same stable `id` minted during detection so
  cross-references created during review/completion remain valid after
  merge;
- no entity present before Stage 1 detection is removed by this merge,
  regardless of Stage 3 output; this is the append-mode contract requested
  in the plan (append, not replace).

This replaces the frontend's current explicit warning "This will replace
your current ontology with the generated one" for the wizard path — the
completed staged flow appends and enriches instead.

## No-Reparse Contract

Detection and completion both read documents exclusively through the
existing durable parsed-corpus tools (`list_documents`, `read_document`),
which read only `ready` manifests
(`docs/superpowers/specs/2026-09-18-parsed-document-corpus-design.md`).
Neither stage ever calls `DocumentExtractor` or `ai_parse_document`, and
neither stage triggers parsing of a pending document as a side effect of
detection, review, or completion. A document that becomes ready after
detection is only picked up by *re-running* detection (which changes the
source fingerprint and is therefore an explicit, visible action), never by
an implicit background refresh.

## Prompt-First Pitfall Handling (Rewrite Loop Removed)

The current pitfall/evaluator loop in `engine.py` (`tool_check_owl_pitfalls`
called directly after each OWL answer, plus the bounded
`_evaluate_ontology_stage` consolidation retry) is a **post-generation
rewrite loop**: it feeds warnings back to the model and asks it to re-emit
corrected Turtle. This design removes that rewrite loop. Pitfall
constraints (naming rules, orphan-class avoidance, domain/range
completeness, duplicate-class avoidance, class-count guidance) move into the
**stage prompts themselves** — the model is instructed up front, not
corrected after the fact.

Deterministic validation (the existing `agents.pge_eval.ontology_metrics`
machinery and the new Stage 3 schema/id-closure checks) remains, but is
**reject-only**: a stage output that fails validation is reported as a
failure (visible to the user, retryable by re-running that substage), never
silently rewritten by feeding the violation back into another LLM call
within the same request. This keeps stage outputs auditable — what the model
produced is what gets checkpointed or rejected, with no hidden repair pass
in between.

## Structured Schemas

```json
{
  "candidate_entity": {
    "id": "cand-3f9a1c",
    "canonical_label": "Carrier",
    "description": "Company that ships an Order to a Customer.",
    "type_hint": "class",
    "evidence": [
      {"source": "spec.pdf", "excerpt": "Order ships via Carrier."}
    ],
    "alternate_labels": ["Shipper", "Freight Company"],
    "origin": "detected",
    "included": true,
    "locked": false
  },
  "locked_anchor": {
    "id": "cls-Customer-a1b2",
    "canonical_label": "Customer",
    "alternate_labels": ["Client"],
    "type_hint": "class",
    "origin": "existing",
    "included": true,
    "locked": true
  },
  "generate_draft": {
    "draft_revision": 4,
    "stage": "reviewing",
    "source_fingerprint": "sha256:...",
    "selected_source_config": {
      "tables": ["demo.sales.orders"],
      "documents": ["spec.pdf"]
    },
    "existing_anchors": ["<locked_anchor>", "..."],
    "candidate_entities": ["<candidate_entity>", "..."],
    "completion_checkpoints": {
      "relations": {"status": "pending", "result": null},
      "attributes": {"status": "pending", "result": null},
      "axioms": {"status": "pending", "result": null}
    }
  }
}
```

`origin` is one of `detected` (proposed by Stage 1) or `manual` (added by
the user in Stage 2). `type_hint` is one of `class`, `object_property`, or
`data_property` (Stage 1 only ever proposes `class`; Stage 3 produces the
property-typed output). `locked` is `true` only for existing-ontology
anchors and is never settable by the client.

## Wizard UI Flow

`_ontology_wizard.html` / `ontology-wizard.js` / `ontology-wizard.css` are
restructured into three visible steps that map 1:1 to the stages above:

1. **Configure/Detect** — unchanged selectors; submit starts Stage 1 and
   transitions to Review once the task completes.
2. **Review Entities** — replaces the current auto-apply
   `showWizardResults()` behavior. Locked anchors render first,
   visually distinct (lock icon, disabled controls). Candidates render with
   include/exclude toggles, remove, and inline editors for canonical label,
   description, type hint, evidence, and alternate labels (repeatable chip
   input). Continue is disabled until the at-least-one-entity invariant is
   satisfied.
3. **Complete** — shows relations/attributes/axioms progress in strict
   order (each substage shown as pending/running/done/failed), then performs
   the append merge and navigates to the map, exactly as the wizard does
   today after a successful generate.

On load, the wizard resumes from the durable draft rather than
`sessionStorage`: if a draft exists and its fingerprint is current, the user
returns to the same stage they left; if the fingerprint is stale, the UI
explicitly shows an "outdated — re-detect" state instead of silently
resuming; if a Stage 3 substage previously failed, the UI shows a resumable
checkpoint state with a retry action scoped to the failed substage only.

## Testing Strategy

Unit tests are added at each implementation layer (subsequent plan tasks),
covering:

- draft serialization/migration, durable resume, stale-fingerprint
  invalidation, rename identity stability, manual add/edit/remove, locked
  anchors, and invalid entity references (draft/contract layer);
- staged agent entry points: detection dedup against anchors and
  alternate labels, no-reparse safety, excluded-entity rejection, strict
  substage ordering, entity-closure rejection, and absence of any rewrite
  call after validation failure (agent layer);
- async route contracts for detect/review/complete, stale-revision
  rejection, checkpoint resume, and one-shot-route gating (API layer);
- default inclusion, add/remove/edit, locked-anchor rendering, alternate
  label editing, reload resume, and checkpoint retry (frontend layer).

The mandatory repository test command remains:

```bash
uv run --frozen pytest -q -m "not scenario"
```

Scenario suites (`tests/e2e/scenarios/`) are opt-in and out of scope unless
explicitly requested.

## Acceptance Criteria

The feature is complete when:

1. Generate is a three-step wizard: Configure/Detect → Review Entities →
   Complete; no control silently performs one-shot generation.
2. Stage 1 detects candidates from selected metadata, the ready parsed
   corpus, and current ontology anchors, defaulting every candidate to
   included, and never re-proposes an existing anchor as new.
3. Stage 2 supports add, remove, include/exclude, and editing canonical
   label, description, type hint, evidence, and alternate labels; at least
   one entity remains required to continue; existing entities are locked.
4. The draft is durable: it survives modal close, page reload, and backend
   process restart, and is invalidated (forcing re-detection) when the
   source fingerprint changes.
5. Entity `id` is stable across rename and across all three stages.
6. Stage 3 runs relations → attributes → axioms in strict order, checkpoints
   each successful substage, and resumes from the first incomplete substage
   after failure; no substage can introduce an entity outside the locked
   anchors plus validated new set.
7. The final merge is append-only: existing identities are preserved and may
   be related to or enriched by new output; no pre-existing entity is
   removed or renamed by a Generate run.
8. Synonyms surface as alternate labels (not separate entities or
   description prose) and are usable as lexical evidence in Stage 3.
9. Parsed documents are never reparsed by any stage; a document only enters
   the workflow via an explicit re-detection.
10. The post-generation pitfall rewrite loop is removed; pitfall
    constraints are enforced via stage prompts, and validation may reject a
    stage's output but never rewrites it.
11. `agent_owl_generator`'s SPEC, eval dataset, and MLflow evaluation delta
    are updated per the AI Feature Lifecycle gate before the staged agent
    ships.
12. Unit, API, frontend, and the mandatory repository test command pass.
