# SPEC: entity-shacl-facets

> **Scope:** a new entity-scoped data-quality check, exposed three ways — the
> Graph Explorer (right-click item opening a popup, a detail-panel section and
> a canvas status ring), a public REST endpoint, and a new MCP tool under
> `src/mcp-server/**`.
>
> Not a new Foundation Model agent. OntoBricks does not call the LLM here —
> this SPEC covers the **tool contract** (a new `check_entity_shacl_facets`
> tool and the hint that tells a model when to call it) and the **response
> contract** both the popup and downstream LLMs read.

## 1. Purpose

SHACL shapes are authored per class in **Ontology → Data Quality** and executed
in bulk by **Digital Twin → Data Quality**, which answers "how healthy is the
`Customer` population?". Nothing answers the question a user actually has when
looking at one node: *"is this customer OK, and if not, what is wrong with it?"*

Today the only route is to run the whole data-quality suite, open each failing
check, and scan its violation sample for the entity — which is capped at
`violation_limit` rows, so the entity may simply not be listed.

This change adds an **entity facet check**: for one entity, collect every
enabled SHACL shape that relates to its type, evaluate each against that entity
only, and return one verdict per shape together with the values the entity
actually carries on the constrained property.

### 1.1 Which shapes relate to the entity type

An enabled shape is selected when its `target_class_uri` is:

1. one of the entity's own `rdf:type` classes, or
2. an ancestor of one of them (`Rules.class_lineage`), or
3. empty — a global shape (`requireLabels`, `noOrphans`, property-level
   `sh:maxCount` from `owl:FunctionalProperty`, …).

The entity's types come from its `rdf:type` triples in the triple store. When
none is found, `NodeContextService.match_ontology_class` (URI-shape matching)
is the fallback, so a node reached through the Graph Explorer always resolves.

### 1.2 Deliberate non-goals

- **SWRL rules, decision tables and aggregate rules** are not SHACL and stay in
  the bulk Data Quality run. The popup does not list them.
- **No remediation.** The popup reports; it does not edit triples or shapes.
- **No change to the bulk run.** The inherited-shape class substitution
  (§3.3) is applied on the entity path only. The bulk run keeps matching
  exact `rdf:type`; aligning it is a separate decision.

## 2. Identity

| Field | Value |
|---|---|
| `agent_name` | `entity_shacl_facets` (tool-contract, not an agent) |
| `module_path` | `src/back/objects/digitaltwin/node/NodeShaclService.py` + `src/mcp-server/server/tools.py` + `src/back/core/mcp_tools.py` |
| `model_endpoint` | n/a — consumed by external LLMs (Playground, Cursor, Claude Desktop) |
| `temperature` | n/a |
| `mlflow_experiment` | `/Shared/ontobricks/mcp/entity_shacl_facets` (created on first eval run) |

## 3. Contracts

### 3.1 Surfaces

| Surface | Change |
|---|---|
| `GET /api/v1/digitaltwin/nodes/shacl` | **New.** Public, `assert_public_graph_read`. Query: `entity_uri`, `domain_name`, `domain_version`, registry overrides |
| `GET /dtwin/nodes/shacl` | **New.** Internal (UI), `assert_domain_graph_read`, session domain |
| `GET …/nodes/context` | Gains `shacl_shape_count` (int, shapes selected per §1.1 using the class already resolved by `match_ontology_class`; no evaluation, no extra warehouse query) |
| `GET /api/v1/domain/classes` | Each class gains `shacl_shape_count`, so MCP `describe_entity` can emit the hint from the cached class listing |
| Graph Explorer node menu | New **Data quality** section with **Data Quality Checks (N)**, rendered only when `shacl_shape_count > 0` — see §3.5 |
| Graph Explorer detail panel | New **Data quality** section with a Check button and, once checked, a one-line summary — see §3.5 |
| Graph Explorer canvas | Checked nodes get a session-only status ring, with a toggle — see §3.5 |
| MCP `check_entity_shacl_facets(entity_uri)` | **New.** Registered in `MCP_DOMAIN_TOOLS` with `requires_graph: True`, so the per-domain tool toggle governs it |
| MCP `get_entity_context` / `describe_entity` | `[Context]` block gains `SHACL shapes (N) — call check_entity_shacl_facets(entity_uri) to evaluate` when N > 0 and the tool is enabled |

### 3.2 Response contract (authoritative)

```json
{
  "success": true,
  "entity_uri": "http://…/Customer/C-42",
  "entity_local_id": "C-42",
  "entity_label": "ACME Corp",
  "classes": [{"name": "Customer", "uri": "http://…#Customer"}],
  "lineage": ["Customer", "Party"],
  "summary": {
    "total": 7, "pass": 4, "fail": 2, "not_applicable": 1,
    "not_evaluated": 0, "error": 0,
    "fail_by_severity": {"sh:Violation": 1, "sh:Warning": 1, "sh:Info": 0}
  },
  "facets": [
    {
      "shape_id": "auto_completeness_Customer_email_1a2b3c4d",
      "label": "Customer.email must not be empty",
      "category": "completeness",
      "severity": "sh:Violation",
      "target_class": "Customer",
      "inherited_from": null,
      "property_path": "email",
      "property_uri": "http://…/email",
      "shacl_type": "sh:minCount",
      "parameters": {"sh:minCount": 1},
      "constraint": "sh:minCount 1",
      "status": "fail",
      "values": [],
      "values_truncated": false,
      "details": [{"s": "http://…/Customer/C-42", "count": 0}],
      "message": "Customer.email must not be empty"
    }
  ]
}
```

Field rules:

- `status` is one of `pass`, `fail`, `not_applicable`, `not_evaluated`,
  `error`.
  - `not_applicable` — the shape carries conditions and the entity does not
    match the guard.
  - `not_evaluated` — `SHACLService.shape_to_sql` returns `None` (e.g.
    `sh:closed`, an unrecognised `sh:sparql`). **Never reported as `pass`.**
  - `error` — the shape's SQL failed; `message` carries the first
    `_SQL_ERROR_MAX_CHARS` of the engine error.
- `inherited_from` names the ancestor class when the shape was selected via
  §1.1 (2); `null` for own-class and global shapes.
- `values` lists the entity's objects on `property_uri` (resolved with
  `SHACLService.resolve_prop_uri`), capped at 20 with `values_truncated`.
  Empty list for shapes without a property.
- `details` holds the violation rows restricted to the entity (each row as the
  shape SQL emitted it: `count`, `val`, `target`, …). Empty on `pass`.
- `facets` order: `fail` (Violation → Warning → Info), `error`,
  `not_evaluated`, `pass`, `not_applicable`; then category, then label.
- An entity whose types match no shape returns `success: true`, empty
  `facets`, all-zero `summary` — not an error.

### 3.3 Evaluation (SQL, entity-scoped)

`NodeShaclService.evaluate(domain, settings, entity_uri)`:

1. `store = get_graphdb(domain, settings, engine="view")`; table from the same
   resolver the Data Quality routes use (`_dataquality_table`, to be lifted
   into the service layer so both share it).
2. One query loads the entity's own triples
   (`SELECT predicate, object FROM t WHERE subject = :uri`) → types, label,
   `values`.
3. Select shapes per §1.1. For an inherited shape, evaluate a **copy** whose
   `target_class_uri` is replaced by the entity's own class URI: the triple
   store only carries the asserted `rdf:type`, so evaluating against the
   ancestor URI would silently pass.
4. Resolve property URIs with `SqlQualityChecks._resolve_shape_uri_for_sql`
   against the predicates seen in step 2 plus the shape's own URI.
5. Build one `UNION ALL` batch. Each branch is
   `SELECT '<shape_id>' AS shape_id, 'v' AS kind, to_json(struct(v.*)) AS detail FROM (<shape_to_sql>) v WHERE v.s = '<uri>'`.
   Shapes with conditions add a `kind = 'g'` branch from
   `ShapeConditions.subject_sql(...)` filtered on the entity: no `g` row ⇒
   `not_applicable`.
6. If the batch fails, rerun shape by shape so one broken shape yields one
   `error` facet instead of failing the whole response.
7. All literals go through `sql_escape`; shape IDs are restricted to
   `[A-Za-z0-9_\-]` before being inlined.

Global uniqueness shapes (`sh:sparql` inverse-functional) scan the whole table
inside the subquery; this is accepted — it is the same query the bulk run
issues, filtered afterwards.

### 3.4 Enforcement points

1. **MCP tool toggle** — `check_entity_shacl_facets` follows the per-domain
   `MCP_DOMAIN_TOOLS` policy; disabled ⇒ not registered for that domain and
   the `[Context]` hint is omitted.
2. **Read-only** — both routes use read guards; nothing is written.
3. **No shape injection** — shapes are read from the domain, never from the
   request body (unlike `/dataquality/execute`).

### 3.5 Graph Explorer rendering

Design system: `.cursor/11-frontend-design.mdc`. Three surfaces share one
client-side result cache, so a check made from any of them shows up in all
three.

#### 3.5.1 Client cache and module

- New `src/front/static/query/js/query-shacl-facets.js` owns the cache
  `{ entityUri: { response, checkedAt } }` for the lifetime of the page (same
  policy as virtual attributes), the fetch to `GET /dtwin/nodes/shacl`, and
  the three renderers below. Exposes `openShaclFacetsModal(entityUri, meta)`,
  `renderShaclFacetSection(entityUri, count)`, `getShaclFacetStatus(entityUri)`.
- New `src/front/static/query/css/query-shacl-facets.css`, built on `ob-*`
  classes and `--db-status-*` tokens; no inline styles.
- A check always replaces the cache entry; **Re-check** is the only way to
  refresh it (no silent re-fetch on reopen).
- Every completed check emits `shacl-facets:updated` (`CustomEvent` with
  `entityUri`); the detail panel and the canvas listen for it instead of being
  called directly.

#### 3.5.2 Node context menu

- New section **Data quality** after **Business rules**, one item
  `<i class="bi bi-shield-check"></i> Data Quality Checks (N)` with
  `data-sg-node-action="shacl-facets"`.
- Rendered only when `meta.shaclShapeCount > 0` and the node is not a
  group/cluster super-node.
- The menu is built from client-side class info (`query-loaders.js`), not
  from `/nodes/context`. The loader fetches `/ontology/dataquality/list`
  alongside the SWRL list and stores `shaclShapeCount` on each class via
  `shaclShapeCountFor(className, classes, shapes)` in
  `query-shacl-facets.js`, which mirrors §1.1 (own + `parent` chain +
  global, enabled only). It only gates the menu item; the server stays
  authoritative for selection and evaluation.
- Click ⇒ `openShaclFacetsModal(uri, meta)`; it reuses the cached result when
  present, otherwise checks.

#### 3.5.3 Popup

Markup lives in a Jinja partial
`src/front/templates/partials/dtwin/_query_shacl_facets_modal.html`,
included from `_query_sigmagraph.html`; opened with
`bootstrap.Modal.getOrCreateInstance(...)`. Classes
`modal-lg modal-dialog-scrollable modal-fullscreen-md-down`. The
dataset-preview pattern (markup built from a JS string, `new bootstrap.Modal`
on each open) is **not** reused — `.cursor/11` forbids both.

```text
┌─────────────────────────────────────────────────────────────────────┐
│ 🛡 Data Quality Checks — ACME Corp   [Customer]  [C-42]          ✕  │
├─────────────────────────────────────────────────────────────────────┤
│ Customer ⊑ Party · 7 shapes · checked 08:32                         │
│  ◠ 50%        ◠ 0%         ◠ 100%       ◠ —                         │
│  Completeness Conformance  Consistency  Structural                  │
│  1 of 2 passed 0 of 1      3 of 3       not checked                 │
│ [✗ 2 failed] [✓ 4 passed] [– 1 not applicable] [? 0 not evaluated]  │
│                                                                     │
│ ▾ Completeness                                        1 failed / 2  │
│   ✗ [Violation]  Customer.email must not be empty                   │
│        email · sh:minCount 1 · found: (none)                  [▸]   │
│   ✓              Customer.name must not be empty                    │
│        name · sh:minCount 1 · found: "ACME Corp"                    │
│ ▾ Conformance                                         1 failed / 1  │
│   ✗ [Warning]    Phone must start with +   ↑ inherited from Party   │
│        phone · sh:pattern ^\+ · found: "06 12 34 56 78"       [▸]   │
│ ▸ Consistency                                         0 failed / 3  │
│ ▸ Structural                                 1 not applicable / 1   │
├─────────────────────────────────────────────────────────────────────┤
│ Open Data Quality ↗                          [↻ Re-check] [Close]   │
└─────────────────────────────────────────────────────────────────────┘
```

- **Header** — entity label, class badge, local-ID badge, filled from the
  menu metadata before the response arrives. The body shows the standard
  `spinner-border` until then.
- **Sub-header** — class lineage, shape count, check time.
- **Summary chips** — one per non-zero status, `--db-radius-chip`, tinted
  with the semantic status vocabulary (`fail` → danger, `pass` → success,
  `not_applicable` → draft, `not_evaluated` → info, `error` → warning), each
  with icon **and** label. Chips are toggle filters; with none active, every
  facet shows.
- **Category groups** — Bootstrap accordion, one item per category in
  `QUALITY_CATEGORIES` order, with the same icon and colour as the Data
  Quality tab (`bi-check2-all` completeness, `bi-123` cardinality,
  `bi-fingerprint` uniqueness, `bi-link-45deg` consistency, `bi-regex`
  conformance, `bi-diagram-3` structural). Header count: `N failed / M`
  (or `N not applicable / M` when nothing failed and some were skipped).
  Every group starts collapsed; activating a status chip expands the groups
  holding a matching row, and clicking a dimension gauge expands and scrolls
  to its group. Empty categories are not rendered.
- **Dimension gauges** — above the chips, one half-gauge per rendered
  category (same order and icon), score = `pass / (pass + fail)` rounded to
  a percentage; `not_applicable`, `not_evaluated` and `error` stay out of the
  score. Colour follows the Data Quality tab thresholds through the status
  tokens (100 % success, ≥ 80 % warning, below danger); a category with no
  checked shape shows `—` in the draft tone. Each gauge carries the
  percentage, the category label and `P of C passed` as text, so the colour
  is never the only signal. Drawn as an inline SVG arc styled in
  `query-shacl-facets.css` — no Chart.js, no hex.
- **Facet row**, in the §3.2 order:
  - line 1: status icon (`bi-x-circle-fill` fail, `bi-check-circle-fill`
    pass, `bi-dash-circle` not applicable, `bi-question-circle`
    not evaluated, `bi-exclamation-triangle-fill` error) with a visually
    hidden status label; severity badge on `fail`/`error` only; shape label;
    `↑ inherited from <Class>` badge when `inherited_from` is set.
  - line 2 (muted, small): property path, constraint in `<code>`, then
    `found:` with up to 5 value chips, `+N more` expander when
    `values.length > 5` or `values_truncated`, `(none)` when empty. Omitted
    for shapes without a property.
  - expander `[▸]` (only when there is something to show): the `details`
    rows as a compact `table-sm` (fail), the engine message (error),
    "This constraint type cannot be checked with SQL" (not evaluated), or the
    shape's condition summary (not applicable).
- **Footer** — `Open Data Quality ↗` (link to Ontology → Data Quality),
  `Re-check` (`btn-outline-primary`, spinner while running), `Close`.
- **Empty state** — no facets: `bi-shield` icon, "No SHACL shape applies to
  *Customer*", link to Ontology → Data Quality.
- **Request failure** — `showNotification(msg, 'error')` and an in-body empty
  state with a **Retry** button; no ad-hoc alert banner.

#### 3.5.4 Detail panel section

- New collapsible `entity-detail-section` **Data quality**
  (`bi-shield-check`), placed after the Virtual attributes section, rendered
  only when `shacl_shape_count > 0`.
- Unchecked: `N SHACL shapes apply` + `Check` button
  (`btn-sm btn-outline-primary w-100`).
- Checked: one line of status chips (same vocabulary as the popup, compact),
  check time, and two buttons — `Details` (opens the popup from the cache)
  and `Re-check`.
- Re-renders on `shacl-facets:updated` for the selected entity, so a check
  made from the context menu fills the panel too.

#### 3.5.5 Canvas status ring

- `_nodeReducer` gains one step: when the **Quality** toggle is on and
  `getShaclFacetStatus(node)` returns a status, set `borderColor` and
  `borderSize = 3`:

  | Node status | Ring |
  |---|---|
  | any `fail` with `sh:Violation` | danger |
  | `fail` with `sh:Warning`/`sh:Info` only, or any `error` | warning |
  | every facet `pass` or `not_applicable` | success |
  | otherwise (some `not_evaluated`, no fail) | no ring |

  Colours are read once from `--db-status-*` via `getComputedStyle` (Sigma
  needs concrete values).
- Precedence: seed highlight and Find-Path highlight are applied **after**
  the ring, so they win; group/cluster super-nodes never get a ring.
- Toggle: inline checkbox **Quality** (`id="sgShowQualityRings"`,
  `data-sg-change="toggleQualityRings"`) in the header toolbar next to
  **Inferred**, checked by default, disabled until at least one node has
  been checked. Toggling calls `refresh` only — no fetch.
- Session-only: rings cover the nodes the user actually checked; nothing is
  persisted and no bulk check is triggered.

#### 3.5.6 Accessibility and responsiveness

- Status is never colour-only: icon + label in the popup and panel; the
  ring is supplementary (the panel states the same status in text).
- Keyboard: menu items and chips are focusable buttons; accordion and
  expanders use Bootstrap's `aria-expanded`.
- Below `md` the popup is full-screen and facet line 2 wraps under line 1.

## 4. Success criteria

1. **Own, inherited and global shapes are all listed**
   - input: `Customer ⊑ Party`; shapes on `Customer.email`, `Party.name`, and
     global `requireLabels`.
   - expected: three facets; `Party.name` has `inherited_from: "Party"`.
2. **Inherited shapes are actually evaluated**
   - input: entity typed `Customer` only, missing `name`; `Party.name minCount 1`.
   - expected: that facet is `fail`, not `pass`.
3. **Verdict parity with the bulk run**
   - input: any entity, all enabled own-class shapes.
   - expected: a shape is `fail` here iff the entity appears in the bulk run's
     uncapped violation set for that shape.
4. **Guarded shapes**
   - input: shape with condition `status = "active"`; entity has `status = "closed"`.
   - expected: `not_applicable`.
5. **Untranslatable shapes are never green**
   - input: `sh:closed` shape on the class.
   - expected: `not_evaluated`.
6. **One broken shape degrades to one facet**
   - input: a shape whose SQL fails (bad regex for `RLIKE`).
   - expected: that facet is `error`, the others carry verdicts, `success: true`.
7. **Popup**
   - right-click a node whose class has shapes ⇒ menu item with the count;
     click ⇒ modal with summary chips and facets grouped by category,
     failures first. No item when the count is 0.
   - clicking the `failed` chip hides every non-failed facet; clicking it
     again restores them.
8. **Detail panel and canvas stay in sync**
   - check from the context menu with the entity selected ⇒ the panel's
     Data quality section shows the summary without a second request, and
     the node gets its ring.
   - **Re-check** after fixing the data ⇒ panel, popup and ring all update.
   - unticking **Quality** removes every ring without a request; seed and
     Find-Path highlights still override a ring.
9. **MCP**
   - "Is customer C-42 compliant with our data-quality rules?" ⇒ the model
     calls `check_entity_shacl_facets` and names the failing constraints and
     their observed values.

## 5. Eval dimensions

The CI gate parses this table — keep it well-formed.

| Dimension | Metric | Threshold | Weight | Judge |
|---|---|---|---|---|
| `facets_complete` | fraction of rows where every shape selected per §1.1 appears in the response | `1.00` | `0.25` | rule-based |
| `verdict_parity` | fraction of rows where each facet's pass/fail matches the bulk run's uncapped violation set for that entity | `1.00` | `0.25` | rule-based |
| `never_false_pass` | fraction of rows where untranslatable or erroring shapes are not reported as `pass` | `1.00` | `0.20` | rule-based |
| `calls_tool_for_quality_question` | fraction of quality-question rows where the model calls `check_entity_shacl_facets` before answering | `0.90` | `0.30` | rule-based |

**Aggregate threshold:** weighted sum ≥ `0.90` to pass G2.

The three contract dimensions are pass/fail (`1.00`) and cannot be masked by
the behavioural one.

## 6. Failure modes

| Symptom | Detection | Mitigation |
|---|---|---|
| Inherited shape always passes | Criterion 2 unit test | Class substitution in §3.3 step 3 |
| Property shape always fails with `count: 0` | `values` empty while the Graph Explorer shows the attribute | `#` vs `/` URI mismatch — resolved via `resolve_prop_uri` against the entity's own predicates |
| Popup slow on large graphs | Latency > 3 s on uniqueness shapes | Inherent to the global subquery; acceptable for an on-demand action. Revisit with a per-shape cache if it becomes the norm |
| Whole popup errors on one bad shape | Criterion 6 test | Per-shape rerun fallback (§3.3 step 6) |
| Entity resolves to no class | `classes` empty, only global facets | `match_ontology_class` fallback; otherwise the entity genuinely has no `rdf:type` and global shapes are all that apply |
| Model answers from `[Context]` without evaluating | `calls_tool_for_quality_question` < 0.90 | Sharpen the hint wording |

## 7. Eval dataset

- **Baseline file:** `tests/eval/datasets/mcp/entity_shacl_facets.jsonl` —
  10 examples (material-change floor per `.cursor/12`): own/inherited/global
  selection, inherited fail, guarded `not_applicable`, `sh:closed`
  `not_evaluated`, broken-regex `error`, entity with no shapes, tool disabled
  for the domain, and three natural-language quality questions.
- Row shape mirrors `tests/eval/datasets/mcp/virtual_attributes.jsonl`.
  New constraint kinds: `facet_listed`, `facet_status`, `facet_inherited_from`,
  `shacl_tool_called`; reused: `call_refused`.

## 8. MLflow tracing

No Foundation Model API call on this path: the MCP handler proxies the REST
endpoint and formats text. The eval harness publishes one MLflow run per
evaluation under `/Shared/ontobricks/mcp/entity_shacl_facets`; the PR body
pastes the run URI under "MLflow eval run" per `.cursor/12`.

## 9. Test plan

Written test-first; all red until implemented.

- `tests/units/dtwin/test_node_shacl_service.py` — shape selection (§1.1),
  class substitution, batch SQL (branches, escaping, hostile shape IDs),
  status mapping, values across the `#`/`/` separator and their cap,
  ordering, summary, batch-failure fallback, `rdf:type` fallback, empty and
  no-store cases. Store faked; `get_graphdb` and
  `NodeShaclService.triplestore_table` are the patch points.
- `tests/units/api/test_node_shacl_endpoint.py` — both endpoints (service
  stubbed), `entity_uri` required, GET-only, and `shacl_shape_count` on
  `/nodes/context` and `/api/v1/domain/classes`.
- `tests/units/mcp/test_shacl_facets_formatting.py` —
  `_format_shacl_facets_response` and the `[Context]` hint in node and class
  blocks.
- `tests/units/mcp/test_domain_policy_gating.py` (extended) — tool count
  15, backend call, hidden on select and refused at call time when
  disabled, hint present / omitted, member of `GRAPH_TOOLS`.
- `tests/units/core/test_mcp_tools_policy.py` (extended) — the tool is in
  `MCP_DOMAIN_TOOL_NAMES` and `MCP_GRAPH_TOOL_NAMES`.
- `tests/units/front/test_shacl_facets_ui.py` — structural contract: the
  modal partial is included once, uses `getOrCreateInstance` and no inline
  styles; the menu item and the panel section are gated on
  `shacl_shape_count`; the status → icon/label/tint and node → ring mappings
  of §3.5.3 / §3.5.5; the **Quality** toggle exists next to **Inferred**.
- Browser check (frontend-design skill step 5): desktop and below-`md`
  widths, keyboard focus through menu → popup → chips → expanders, no
  console or network errors.

## 10. Sign-off

- [ ] Contracts in §3 reviewed.
- [ ] Rendering in §3.5 reviewed.
- [ ] Eval dataset committed (10 examples) — written at
      `tests/eval/datasets/mcp/entity_shacl_facets.jsonl`, not yet committed.
- [ ] Baseline eval run URI pasted into PR body.
- [ ] Aggregate threshold ≥ `0.90`.
