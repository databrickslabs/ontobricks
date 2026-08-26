# SPEC: virtual-attributes

> **Scope:** MCP tool-contract change under `src/mcp-server/**` — a new class
> of ontology attribute that no mapping feeds and no build materializes: a
> Unity Catalog function computes it from the entity's ID, on demand.
>
> Not a new Foundation Model agent. OntoBricks does not call the LLM itself
> here — this SPEC covers the **prompt surface** (a new `get_entity_context`
> parameter and the wording that tells a model when to spend it) and the
> **response contract** that downstream LLMs read. Section 2 is populated
> accordingly.

## 1. Purpose

Every attribute an OntoBricks class exposed was a mapped column: declared in
the ontology, bound to a table by R2RML, materialized into the triple store by
a build. That covers facts the source system stores, and nothing else.

The attributes that matter most in a digital twin are often *not* stored. A
risk score, a live inventory position, a distance to the nearest depot — each
is a function of the entity plus something the graph does not contain. Today
the only way to surface one is to precompute it into a source table and
remap it, which means:

- **The value goes stale between builds.** A score computed at build time is
  the score of a customer who no longer exists in that state.
- **The graph grows for values nobody read.** Materializing a score for every
  entity pays the full cost even when a single entity is inspected.
- **Governance moves out of Unity Catalog.** The logic ends up in an ETL job
  instead of the UC function that already owns it, with its own permissions.

Actions already solved the "call a UC function on this entity" half of the
problem, but an action is a *verb*: the user clicks it, reads a result table,
and nothing about the class says the value exists. A virtual attribute is a
*noun* — declared on the class like any other attribute, listed next to the
mapped ones, and computed when someone actually asks.

This change adds:

1. **Declaration** — `cls["virtualAttributes"]`, edited in **Ontology →
   Designer → Attributes → Virtual Attributes**, one entry per bound UC
   function, one attribute per returned column.
2. **On-demand computation** — a `compute_virtual_attributes` flag on the node
   context surfaces (REST, internal, MCP) plus a dedicated internal endpoint
   for the Graph Explorer's Compute button.
3. **Policy control** — `virtual_attributes` becomes the fourth MCP context
   element, so a domain can mark it `preferred`, `normal` or `disabled`.

A class with no virtual attributes behaves exactly as before: the key is
absent from the ontology blob, the section does not render, and the response
carries no `virtual_attributes` block. No backfill, no migration — the
ontology is already a `jsonb` column.

### 1.1 Deliberate non-goals

Virtual attributes are **not queryable**. They are not in `dataProperties`, so
they never reach R2RML, the build pipeline, SHACL shapes, data-quality checks,
GraphQL or SPARQL. You cannot filter a cohort on one. This is the price of
never materializing them, and it is the point: a value that exists only at
read time cannot be indexed. The designer UI states it inline so nobody
discovers it by writing a query that silently returns nothing.

## 2. Identity

| Field | Value |
|---|---|
| `agent_name` | `virtual_attributes` (tool-contract, not an agent) |
| `module_path` | `src/back/objects/digitaltwin/VirtualAttributeService.py` + `src/mcp-server/server/app.py` + `src/back/core/mcp_tools.py` |
| `model_endpoint` | n/a — consumed by external LLMs (Playground, Cursor, Claude Desktop) |
| `temperature` | n/a |
| `mlflow_experiment` | `/Shared/ontobricks/mcp/virtual_attributes` (created on first eval run) |

## 3. Tool surface

| Tool | Change |
|---|---|
| `get_entity_context` | Declarations always ride along; optional `compute_virtual_attributes=True` still computes inline for backward compatibility. Refused when the element is `disabled` |
| `compute_virtual_attributes` | **New.** Runs the class's virtual attribute UC functions and returns their values. Call this when the user asks about a virtual attribute's value. Refused when the element is `disabled` |
| `describe_entity` | `[Context]` block gains a `Virtual attributes (N, computed on demand)` line naming them, plus the follow-up hint pointing at `compute_virtual_attributes` |
| `list_entity_types` | Unchanged — the per-class declaration lives in the context block, not the type listing |
| every other tool | unchanged |

The dedicated tool mirrors `invoke_entity_action`: a model that sees a virtual
attribute question has an obvious verb to call, instead of discovering a flag
on a broader context tool.

### 3.1 Declaration contract (authoritative)

An ontology class carries:

```json
{
  "name": "Customer",
  "virtualAttributes": [
    {
      "catalog": "main",
      "schema": "kg",
      "function": "customer_risk",
      "fullName": "main.kg.customer_risk",
      "description": "Live credit risk from the scoring model",
      "returns_table": true,
      "attributes": [
        {"name": "risk_score", "column": "risk_score", "label": "Risk score", "dataType": "DOUBLE"},
        {"name": "risk_band", "column": "risk_band", "label": "Risk band", "dataType": "STRING"}
      ]
    }
  ]
}
```

Rules:

- The bound function takes **exactly one parameter**: the entity's local ID,
  passed server-side. Same rule as actions, same picker filter.
- A `RETURNS TABLE` function yields one attribute per result column, read from
  `information_schema.parameters`. A scalar function yields exactly one, whose
  `column` is the reserved alias `result`.
- `name` is the attribute as every consumer displays it; `column` is where to
  read it in the function result. They differ only when a name collided at
  declaration time and the picker suffixed it (`score` → `score_2`).
- Names are unique across the class's mapped attributes (own + inherited) and
  its other virtual attributes: the Graph Explorer and the MCP render both
  families in one namespace, so the collision is resolved when it is declared,
  not when it is displayed.
- `fullName` must match `^[A-Za-z0-9_.]+$` and `name` / `column` must match
  `^[A-Za-z0-9_]+$` (`back/core/helpers.SAFE_SQL_IDENT` / `SAFE_COL_IDENT`).
  A malformed entry is **dropped with a warning**, never raised: one bad
  declaration must not cost the node its whole context.
- Removal is per **group**, not per attribute — an attribute is part of the
  function's signature, so dropping one alone would make the column-to-
  attribute mapping ambiguous.

### 3.2 Response contract

```json
{
  "success": true,
  "entity_uri": "http://…/Customer/C-42",
  "virtual_attributes": [
    {"fullName": "main.kg.customer_risk", "function": "customer_risk",
     "returns_table": true,
     "attributes": [{"name": "risk_score", "column": "risk_score", "label": "Risk score", "dataType": "DOUBLE"}],
     "values": {"risk_score": 0.82},
     "error": null, "message": null}
  ]
}
```

`values` is **absent** until computed — the distinction between "not computed"
and "computed as null" is load-bearing for both the UI and the model, so an
uncomputed group must not present an empty mapping as a result.

A group whose function fails carries `error` and leaves the other groups
untouched: a broken UC function must not deny the user the attributes that
still resolve. Only the first row is used; a function returning several rows
sets `message` rather than aggregating, because a virtual attribute is
single-valued per entity by construction.

### 3.3 Enforcement points

Three layers, matching the `mcp-domain-policy` precedent:

1. **MCP argument guard** — `get_entity_context` refuses
   `compute_virtual_attributes=True` when the element is `disabled`, so the
   model learns the element is off instead of retrying the same call.
2. **Server-side payload filtering** — `NodeContextService.resolve_context`
   withholds declarations *and* skips the computation entirely when disabled;
   `/api/v1/domain/classes` withholds them from the class listing. Internal
   authoring routes pass no policy and keep showing the designer everything.
3. **Allow-list at compute time** — `VirtualAttributeService.compute` rejects
   a `function_full_name` that is not declared on the resolved class, so the
   Compute endpoint cannot be turned into an arbitrary UC function runner.

### 3.4 What `preferred` means

Textual emphasis only, as for the other three elements. The neutral hint
("call `get_entity_context(compute_virtual_attributes=True)` to compute these
values") becomes directive ("ALWAYS call … before answering: these values are
not stored in the graph and only a computation can produce them"). The hint is
emitted **only while at least one group is uncomputed** — repeating it after a
successful computation would ask the model to redo work it just did.

## 4. Success criteria

1. **Declarations are free, values are not**
   - input: entity whose class declares two virtual attributes; client calls
     `get_entity_context(entity_uri)` with no flag.
   - expected: both attributes are named in the response with no values and no
     warehouse query is issued.
2. **Computation happens when asked**
   - input: same entity, `compute_virtual_attributes=True`.
   - expected: each attribute carries its value, and the follow-up hint is
     gone because nothing is left to compute.
3. **Disabled element is withheld and refused**
   - input: domain with `context.virtual_attributes = "disabled"`.
   - expected: no virtual attribute block in `get_entity_context` or
     `describe_entity`, and `compute_virtual_attributes=True` is refused.
4. **Preferred element gets a directive hint**
   - input: domain with `context.virtual_attributes = "preferred"`, entity with
     uncomputed virtual attributes.
   - expected: the directive hint variant, and the model computes before
     answering instead of reporting the attribute as unknown.
5. **A failing function degrades to one group**
   - input: class with two groups, one bound to a function that raises.
   - expected: the healthy group's values are present, the failing one carries
     `error`, and the response is still `success: true`.
6. **An undeclared function is refused**
   - input: `GET /dtwin/nodes/virtual-attributes?function=main.kg.not_declared`.
   - expected: `ValidationError`, no UC call.

## 5. Eval dimensions

The CI gate parses this table — keep it well-formed.

| Dimension | Metric | Threshold | Weight | Judge |
|---|---|---|---|---|
| `declaration_listed` | fraction of rows where every declared virtual attribute is named in the response | `1.00` | `0.25` | rule-based |
| `no_value_without_flag` | fraction of unflagged rows where no value is emitted and no warehouse query is issued | `1.00` | `0.25` | rule-based |
| `disabled_element_absent` | fraction of disabled rows where the element renders nowhere AND the flag is refused | `1.00` | `0.25` | rule-based |
| `computes_before_answering` | fraction of rows where the model computes rather than reporting the attribute as unknown | `0.90` | `0.25` | rule-based |

**Aggregate threshold:** weighted sum ≥ `0.90` to pass G2.

The three contract dimensions (`declaration_listed`, `no_value_without_flag`,
`disabled_element_absent`) are pass/fail (`1.00`) and cannot be masked by the
behavioural dimension.

## 6. Failure modes

| Symptom | Detection | Mitigation |
|---|---|---|
| Model answers "unknown" instead of computing | `computes_before_answering` < 0.90 | The hint is the only lever — mark the element `preferred` for that domain, or sharpen the directive wording |
| Model recomputes on every turn | Trace shows repeated `compute_virtual_attributes=True` | The hint is suppressed once a group has values; check that `values` survives into the formatted block |
| A value silently reads as null | Manual: attribute shows `null` for every entity | `column` does not match the function's result column — the picker derives it from `information_schema`, so a function altered after binding needs rebinding |
| No attribute created when binding a function | Designer notification "exposes no usable return column" | `_fetch_return_columns` soft-failed (warehouse cannot read `information_schema.parameters`) or the function returns an unnamed column. Scalar functions are unaffected |
| Declaration lost on ontology save | OWL round-trip test | `ONTOBRICKS_NS.virtualAttributes` must be emitted by `OntologyGenerator` and read back by `OntologyParser`, like `actions` |
| Compute endpoint used as an arbitrary function runner | Unit test on the allow-list | `VirtualAttributeService.compute` raises `ValidationError` for a `fullName` not declared on the resolved class |
| Name collides with a mapped attribute | Designer notification "renamed" | Resolved at declaration time by suffixing; a class edited outside the UI can still collide, in which case the virtual attribute is displayed twice under one name |
| Values are stale in the Graph Explorer | Manual: value does not change after the source does | Cached per entity for the page's lifetime by design; the Recompute button is the refresh. Cache is not shared with MCP, which never caches |

## 7. Eval dataset

- **Baseline file:** [tests/eval/datasets/mcp/virtual_attributes.jsonl](../../tests/eval/datasets/mcp/virtual_attributes.jsonl)
  — 10 examples covering declaration-only listing, computation on request,
  the element disabled, preferred emphasis, a failing function, an undeclared
  function, a scalar function, and the collision-renamed case. Material-change
  floor per `.cursor/12` (≥ 10).
- **Regression file:** future — every production failure lands under
  `tests/eval/datasets/mcp/regression.jsonl`.

Dataset row shape mirrors [tests/eval/datasets/mcp/domain_policy.jsonl](../../tests/eval/datasets/mcp/domain_policy.jsonl):

```json
{"id": "...", "input": {"domain": "...", "mcp_policy": {...}, "user_message": "..."},
 "expected": {"contains": [...], "constraints": [{"kind": "...", "value": ...}]},
 "tags": ["happy" | "ambiguous" | "adversarial"]}
```

Constraint kinds used by this dataset:

- `context_element_absent` — the named element renders in no block of the
  response (reused from `domain_policy.jsonl`).
- `hint_directive` — the follow-up hint for the named element is the directive
  variant (reused).
- `va_declared` — the named virtual attribute is listed in the response.
- `va_value_absent` — the named virtual attribute is listed **without** a
  value.
- `va_value_present` — the named virtual attribute carries a computed value.
- `va_computed` — the model issued `compute_virtual_attributes(entity_uri)`
  before answering (or `get_entity_context` with
  `compute_virtual_attributes=True` for backward compatibility).
- `va_group_error` — the named function's group carries an `error` while the
  response stays successful.
- `call_refused` — a direct call returns a refusal rather than a result
  (reused).

## 8. MLflow tracing

MCP-side tool handlers stay lightweight (proxy calls to OntoBricks REST + text
formatting), so there is no Foundation Model API call on this path to trace.
Add `@trace_tool` on `_format_virtual_attribute_lines` if the eval harness
needs step-level attribution of the emphasis decision. Downstream LLM traces
come from the client (Cursor, Playground) and are outside this repo.

The eval harness that reads `virtual_attributes.jsonl` must publish one MLflow
run per evaluation with parent experiment
`/Shared/ontobricks/mcp/virtual_attributes`, and the PR body pastes the run
URI under the "MLflow eval run" heading per `.cursor/12`.

## 9. Plan reference

Asana ticket:
[Ontology: virtual attributes — UC-function-computed attributes, resolved on demand](https://app.asana.com/1/8808412813448/project/1217637563677287/task/1217822537534525).

## 10. Sign-off

- [x] Declaration and response contracts authored in §3.1 / §3.2.
- [x] Eval dataset committed at `tests/eval/datasets/mcp/virtual_attributes.jsonl` (10 examples).
- [ ] Baseline eval run URI pasted into PR body.
- [ ] Aggregate threshold ≥ `0.90`.
- [ ] Reviewer waiver (if applicable): _____
