# Cohort Discovery

Cohort Discovery turns the question *"which entities travel together?"* into a
deterministic, explainable, idempotent rule that any business user can author,
preview, materialise, and re-run.  It complements — not replaces — the
existing reasoning engines (OWL 2 RL, SWRL, SPARQL CONSTRUCT, Decision
Tables, Aggregate Rules); cohort rules live in their own slot
(`ontology.cohort_rules`) and never interfere with W3C standard reasoning.

This document is the canonical user/developer reference.  The release-
requirement specification that drove the implementation lives at
[`releasereq/cohort_design.md`](../releasereq/cohort_design.md).

---

## 1. Mental model

A cohort is a set of entities that:

1. belong to the same **target class** (e.g. `:Person`),
2. are **linked together** through some bridging entity (e.g. they share a
   `:Project` reachable through `:assignedTo`), and
3. all satisfy the same **compatibility policies** (e.g. their `:status`
   equals `Exempt`, or every member shares the same `:department`).

The user writes this as a **CohortRule** in five small sections.  The engine
runs it, produces deterministic cohort URIs, and writes the result either
into the knowledge graph (as `:inCohort` triples) or into a Unity Catalog
Delta table — or both.

---

## 2. Authoring a rule (UX)

In the Digital Twin, open **Advanced → Cohorts**.  The form has five
sections, each with live feedback:

1. **Identity** — *Rule name* + optional *Description*.  The internal id is
   slug-derived from the name (e.g. `Exempt staffing pool` →
   `exempt_staffing_pool`).
2. **What are we grouping?** — pick a target class from the ontology.  The
   counter to the right shows live instance counts in the graph.
3. **When are two members linked?** — zero-or-more *shared-resource
   paths*.  Each path is an ordered chain of hops starting from the
   source class; the **last hop's target** is the entity two members
   must reach to be considered linked.  Most cohorts only need a 1-hop
   path (e.g. `Person —assignedTo→ Project`), but you can click
   *Add hop* to chain hops — for example
   `Person —assignedTo→ Project —governedBy→ ComplianceType` lets you
   say *"two persons are linked when they work on projects governed by
   the same compliance type"* without materialising an intermediate
   predicate.

   The dropdowns are **dependent at every hop**: each hop's source is
   locked (it's either the rule's source class or the previous hop's
   target).  The `via` list shows only object properties whose
   `domain = <hop source>`, and is further narrowed to those with
   `range = <target_class>` once a target is picked.  The
   `target_class` list is disabled until `via` is picked, then filtered
   to that property's range.  Editing a hop propagates downstream: the
   next hop's source pill updates and its dropdowns re-filter.

   When you add more than one path, choose **ANY** (union) or **ALL**
   (intersection) to combine them.  The link-edge counter shows how
   many candidate edges are produced live.

   Each hop also carries an optional **where filter** (the funnel icon
   next to *shared* shows the count). Use it to constrain a hop's
   *target* node by its own attributes — *"… → ComplianceType where
   complianceTypeId = 'Individual'"* attaches the constraint to the
   compliance type itself, instead of misusing rule-level
   compatibility (which only filters the source class). This is the
   correct fix when a multi-hop preview returns *"0 cohorts"* despite a
   path that would clearly match data: the constraint usually lives on
   a node along the path, not on the source. The same four primitives
   are available — `equals`, `in any`, `between` — minus `same value`,
   which is a pairwise edge constraint and meaningless on a single
   node. Missing values still drop the candidate by default; use the
   filter's `Allow missing` toggle (in the JSON payload, not yet on
   the form) to opt out.
4. **Compatibility policies** — zero-or-more constraints from this menu:
   * **Same value of `<property>`** (every member shares the same value).
   * **`<property>` equals `<value>`** (per-member literal).
   * **`<property>` in any of `<list>`** (per-member set membership).
   * **`<property>` between `<min>` and `<max>`** (per-member numeric range).
   The match-count badge shows surviving members live.  The picker next to
   `value_equals` calls `/dtwin/cohorts/sample-values` so users can pick
   actual values from the graph instead of guessing.
5. **Group type** — *Connected* (transitive: A↔B and B↔C ⇒ {A,B,C}) or
   *Strict* (clique: every pair must be linked directly).  Plus a
   *Minimum cohort size* knob (default 2).

The sticky **action bar** at the bottom of the form lets you:

| Button | What it does |
|---|---|
| **Preview cohorts** | Runs `/dtwin/cohorts/dry-run` and switches to the **Preview** tab (no writes). |
| **Save rule** | `POST /dtwin/cohorts/rules` — versioned with the domain (works in both Volume and Lakebase modes). |
| **Materialise** | Opens a small modal confirming what gets written. Idempotent per rule. |
| **Configure outputs** | Toggle graph triples on/off and configure the optional Unity Catalog Delta target. |
| **View JSON** | Inspect the canonical `CohortRule` payload. |

The Preview tab's **Why? / Why not?** explainer accepts a member URI and
returns a per-stage breakdown — class membership, surviving compatibility
constraints, edge presence, final cohort.  Perfect when a stakeholder
asks *"why isn't Alice in the pool?"*.

The **Trace path** button on the Preview tab is the corresponding
*"why are there 0 cohorts?"* tool. Click it and the engine instruments
each link's path with per-hop counters:

| Column | Meaning |
|---|---|
| `in` | distinct nodes at hop entry (after Stage 3a survivors) |
| `raw` | outbound edges traversed via the hop's `via` predicate |
| `drop` | neighbours rejected — split between `target_class` (type filter) and `where` (hop filter) on hover |
| `out` | distinct surviving neighbours fed into the next hop |

The first hop where `out` collapses to 0 highlights itself in red,
and the diagnostic line below the table reads off the most likely
cause — wrong predicate URI, wrong target class URI, or a misconfigured
hop `where` filter (case-sensitive value, missing `allow_missing`,
etc.). This turns the silent *"0 cohorts, 0 of N members grouped"*
symptom into a one-glance pinpoint.

---

## 3. Output destinations

A cohort run can write to **graph triples**, a **Unity Catalog table**, or
both — the two outputs are independent and idempotent.

### 3.1 Graph triples (always available)

For each cohort `c` produced by rule `r`:

```
<cohort_uri>  rdf:type             :Cohort
<cohort_uri>  rdfs:label           "<rule.label> — cohort #N"
<cohort_uri>  :fromRule            "<rule.id>"
<cohort_uri>  :cohortSize          "<size>"
<member_uri>  :inCohort            <cohort_uri>     # one per member
```

`<cohort_uri>` is `<base_uri>/cohort/<rule_id>/c-<sha256(sorted(members))[:8]>` —
a content-hash URI.  Same membership ⇒ same URI across runs (stable join
key in BI tools, Sigma, GraphQL).  Different membership ⇒ new URI; old
ones are deleted on re-materialise.

### 3.2 Unity Catalog Delta table (optional)

When the rule's `output.uc_table` is set, materialisation creates (if
needed) and populates a Delta table with this schema:

| column | type | notes |
|---|---|---|
| `rule_id` | STRING | partition key |
| `rule_label` | STRING | |
| `cohort_id` | STRING | local fragment, e.g. `c-3f2a91b6` |
| `cohort_uri` | STRING | full URI |
| `cohort_idx` | INT | sequence within the run |
| `cohort_size` | INT | |
| `member_uri` | STRING | |
| `member_id` | STRING | local name |
| `member_label` | STRING | best-effort `rdfs:label` |
| `domain_name` | STRING | |
| `domain_version` | STRING | |
| `materialised_at` | TIMESTAMP | |

Re-runs are idempotent: `DELETE FROM <fq> WHERE rule_id = ?` then
`INSERT`.  The table is partitioned by `rule_id` so multiple rules can
share one table cheaply.

The **Configure outputs** modal exposes two safety nets:

* **Auto-pick** (`/dtwin/cohorts/uc/suggest-target`) — proposes
  catalog/schema/table_name from the domain settings, source-table
  metadata, or registry, falling back to a literal `cohorts` schema.
* **Test write access** (`/dtwin/cohorts/uc/probe-write`) — runs a
  three-step read-only probe (catalog → schema → table) so users find
  out about a missing privilege *before* clicking Materialise.

---

## 4. Persistence (where rules live)

Cohort rules live alongside SWRL/SPARQL/aggregate rules in
`session.ontology.cohort_rules`.  They are versioned and persisted by the
existing registry layer:

* **Volume mode** — written into `versions/V<N>.json` on the Unity Catalog
  Volume next to the rest of the ontology payload.
* **Lakebase mode** — shredded into the `ontology` JSONB column of the
  `<schema>.domain_versions` Postgres table; the registry layer is
  rule-agnostic, so no schema migration was needed.

In-memory access goes through `DomainSession.cohort_rules` (property +
setter, mirroring `aggregate_rules`).  `export_for_save()` includes the
list automatically.  Activating an older domain version reloads its
historical rules transparently.

---

## 5. The algorithm

The engine (`back/core/graph_analysis/CohortBuilder.py`) is **backend-
agnostic**: it talks to the triplestore exclusively through
`store.query_triples(graph_name)` and `store.insert_triples(...)`, which
work on every supported backend (Delta + Spark SQL, LadybugDB + Cypher).
All higher-level filtering, edge construction, and grouping happens in
pure Python — same approach as `CommunityDetector`.

The pipeline has six stages, in order:

```
1. List class members         (subjects with rdf:type = class_uri)
2. Fetch attribute values     (one pass over triples, indexed per property)
3a. Apply node filters         (value_equals / value_in / value_range)
3b. Build candidate edges      (members sharing a bridging entity, per link)
4. Apply edge filters         (same_value)
5. Run NetworkX grouping      (connected_components OR find_cliques)
6. Rank, hash, materialise    (sort by size, content-hash URIs, write)
```

`CohortBuilder` exposes **per-stage helpers** consumed by the live
counters and the Why? explainer:

| Helper | Purpose |
|---|---|
| `count_class_members(class_uri)` | Section 2 counter. |
| `count_link_edges(class_uri, links, combine)` | Section 3 counter. |
| `count_matching_nodes(class_uri, compatibility)` | Section 4 counter. |
| `sample_property_values(class_uri, property_uri, limit)` | `value_equals` picker. |
| `explain_membership(rule, target_uri)` | Why? / Why not? per-stage trace. |

For algorithmic detail (SQL/Cypher dispatch, complexity, schema-drift
handling, worked example), see
[`releasereq/cohort_design.md` §9](../releasereq/cohort_design.md).

---

## 6. Idempotency guarantees

A re-materialise of a saved rule:

1. Wipes the rule's old graph triples via
   `store.delete_cohort_triples(table, prefix, in_cohort)` — the cohort
   URI prefix is `<base_uri>/cohort/<rule_id>/`, and the predicate is
   `:inCohort`.  SQL backends use `DELETE FROM ... WHERE subject LIKE
   'prefix%' OR (predicate = '<inCohort>' AND object LIKE 'prefix%')`;
   LadybugDB overrides with two `MATCH ... DELETE` Cypher passes.
2. Wipes the rule's old Delta-table partition via
   `DELETE FROM <fq> WHERE rule_id = ?`.
3. Re-inserts fresh rows — content-hash URIs are stable, so unchanged
   cohorts keep their identity even though they were deleted/re-inserted.

Multiple concurrent runs of *different* rules are safe (they touch
disjoint URI prefixes / partitions).

---

## 7. Worked examples

### 7.1 Consulting — Exempt staffing pool

> "Find people who can be staffed together: same project AND all
> Exempt." — Acme Consulting

* **Class**: `:Person`.
* **Linked when**: share a `:Project` via `:assignedTo`.
* **Compatibility**: `:status` *equals* `Exempt`.
* **Group type**: Connected; min size 2.

A graph with Alice/Bob (P1, Exempt), Carol (P1, Non-Exempt),
Dave/Eve (P2, Exempt), Frank (P3, Exempt), and Bob bridging P1 and P3
yields **two cohorts**: `{Alice, Bob, Frank}` (3 members, connected via
P1↔P3 through Bob) and `{Dave, Eve}` (2 members).  Carol is dropped at
Stage 3a because her `:status` is `Non-Exempt`.

### 7.2 Healthcare — Co-treated patient cohort

> "Patients seen by the same doctor in the same period, with the same
> primary diagnosis."

* **Class**: `:Patient`.
* **Linked when**: share a `:Doctor` via `:treatedBy` AND share a
  `:Visit` via `:hasVisit` (combine = ALL).
* **Compatibility**: `:primaryDiagnosis` *same value*; `:visitDate`
  *between* (clinic study window).
* **Group type**: Strict; min size 3.

Output to a `cohorts.<study_name>` UC table for downstream BI / cohort
matching analysis.

### 7.3 Manufacturing — Co-located machine cluster

> "Machines on the same shop floor with the same firmware band."

* **Class**: `:Machine`.
* **Linked when**: share a `:ShopFloor` via `:locatedIn`.
* **Compatibility**: `:firmwareVersion` *in any of* `["v3.4", "v3.5"]`.
* **Group type**: Connected; min size 5.

Re-runs nightly into both the graph (so MES dashboards can pivot via
`:inCohort`) and a UC Delta table partitioned by `rule_id`.

### 7.4 Education — Course-cohort recommender

> "Students who can take the same elective track."

* **Class**: `:Student`.
* **Linked when**: share a `:Course` via `:enrolledIn`.
* **Compatibility**: `:program` *same value*; `:yearOfStudy` *value
  range* min 2 max 4.
* **Group type**: Connected; min size 4.

Materialise into the graph only — the recommender simply queries
`:inCohort` to suggest electives.

### 7.5 Compliance — Same-policy people (multi-hop path + hop where)

> "People who work on Individual-compliance projects."

* **Class**: `:Person`.
* **Linked when**: 2-hop path
  `Person —assignedTo→ Project —governedBy→ ComplianceType`
  (terminal = `ComplianceType`), with a **per-hop where** on the
  terminal: `complianceTypeId equal to "Individual"`.
* **Compatibility**: none (the hop where already segments — putting
  `complianceTypeId = "Individual"` in rule-level compatibility would
  silently filter every Person, since `complianceTypeId` lives on
  `ComplianceType`, not `Person`).
* **Group type**: Connected; min size 2.

Two persons land in the same cohort whenever any pair of their projects
is governed by the same Individual-typed `ComplianceType`. Drop the
`where` to get the broader "same compliance type, whatever it is"
variant. Useful for building training-cohort lists or quarterly review
groups without having to materialise a `:hasCompliance` predicate on
`:Person`.

---

## 8. API summary

Endpoints (all under `/dtwin/cohorts/*`, session-scoped):

| Method + path | Purpose |
|---|---|
| `GET  /rules` | List saved rules. |
| `POST /rules` | Validate and upsert a rule (BUILDER role). |
| `DELETE /rules/{rule_id}` | Delete a rule (BUILDER role). |
| `POST /dry-run` | Run engine without writing. |
| `POST /materialize` | Re-run + write outputs (BUILDER role). |
| `GET  /preview/class-stats?class_uri=…` | Live class-instance count. |
| `POST /preview/edge-count` | Live link-edge count. |
| `POST /preview/node-count` | Live matching-member count. |
| `POST /preview/path-trace` | Per-hop frontier diagnostic (powers the *Trace path* button on the Preview tab). |
| `POST /sample-values` | Distinct property values for a `value_equals` picker. |
| `POST /explain` | Why? / Why not? for one member URI. |
| `GET  /uc/suggest-target` | Auto-pick UC catalog/schema/table_name. |
| `POST /uc/probe-write` | Read-only 3-step permission probe. |

The JSON contract is documented inline in
`api/routers/internal/dtwin.py` and tested in
`tests/test_dtwin_cohort.py`.

---

## 9. Stage 2 — natural-language rule generation

A dedicated agent (`agents/agent_cohort/`) translates prompts like
*"find consultants who can be staffed together — exempts only with
exempts"* into a validated `CohortRule` JSON via OpenAI-compatible
tool-calling against the active session's ontology + graph.

### 9.1 Tools (read-only except `propose_rule`, which only validates)

| Tool | Wraps |
|---|---|
| `list_classes()` | `GET /ontology/get-loaded-ontology` (compact: uri, label, n data props) |
| `list_properties_of(class_uri)` | same endpoint, sliced to one class (data + object properties) |
| `count_class_members(class_uri)` | `GET /dtwin/cohorts/preview/class-stats` |
| `sample_values_of(class_uri, property_uri, limit)` | `POST /dtwin/cohorts/sample-values` |
| `propose_rule(rule)` | client-side `CohortRule.validate()`; on success, parks the canonical dict on the engine context |
| `dry_run(rule)` | `POST /dtwin/cohorts/dry-run` (cluster body trimmed to top 5 sizes) |

Stage 2 reuses every Stage 1 endpoint as the agent's toolbox — there is
no parallel pipeline. The agent **never writes**; the user reviews and
edits the proposed rule in the same form before clicking *Save* or
*Materialise*.

### 9.2 Workflow

The system prompt constrains the agent to:

1. `list_classes()` to anchor on a real class URI.
2. `count_class_members(class_uri)` to confirm the class has data.
3. `list_properties_of(class_uri)` to discover datatype properties (for
   compatibility) and object properties (for `links[].via`).
4. `sample_values_of(...)` for each `value_equals` / `value_in` literal
   so the constants match the data's casing/spelling exactly.
5. `propose_rule(rule)` to validate and register the candidate. On
   `valid=false`, the agent reads the errors and re-proposes.
6. (Optional) `dry_run(rule)` exactly once to surface cluster stats.
7. Reply with a short markdown explanation. The form is the interface;
   the JSON is hydrated into it automatically.

If the prompt is too vague to pick a class, the agent asks one short
clarifying question instead of guessing.

### 9.3 API

```
POST /dtwin/cohorts/agent
{
  "prompt":  "find consultants who can be staffed together",
  "history": []
}

→ {
    "success": true,
    "rule":    { ... validated CohortRule ... } | null,
    "reply":   "...short markdown explanation...",
    "tools":   [{"name": "list_classes", "duration_ms": 12}, ...],
    "iterations": 4,
    "usage":   {"prompt_tokens": ..., "completion_tokens": ...}
}
```

When `rule` is `null` the agent could not assemble a valid rule and the
`reply` carries the (likely clarifying) follow-up question.

### 9.4 UX

The Cohorts page exposes a single-line prompt input above the form
(*"Describe the cohort you want"*) plus a *Generate rule* button. On
success:

* the form is hydrated with the proposed rule (class, links,
  compatibility, group type, min size),
* the rule lands as a **draft** (no `activeRuleId`) — saving is an
  explicit user click,
* a collapsible *Agent trace* shows the tool-call order, durations,
  iterations, and token usage so users can audit what the agent did.

### 9.5 Safety

* No write-side tools — the agent cannot save, materialise, or modify
  the ontology/graph.
* All validation runs server-side via `CohortRule.from_dict()` +
  `CohortRule.validate()`. Invalid output never reaches the form.
* The agent reuses the same session cookies + Databricks-Apps headers
  as the request, so loopback tool calls go through
  `PermissionMiddleware` as the same user (no privilege escalation).
* Iteration cap (10) protects against infinite loops; tools that fail
  return a JSON `{error}` payload so the LLM can self-correct.

See `releasereq/cohort_design.md` §12 for the full design and
`agents/agent_cohort/` for the implementation.
