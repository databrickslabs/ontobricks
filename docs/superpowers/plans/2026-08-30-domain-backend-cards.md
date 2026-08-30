# Domain Backend Cards Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Domain Information backend dropdown with accessible
brand-logo cards and hide graph-only details for ontology-only domains.

**Architecture:** Keep the existing hidden `#domainGraphBackend` select as the
single persistence-compatible value source, and synchronize native radio cards
with it in `domain-information.js`. Move the existing panes onto the canonical
`ob-tabs-wrap`/`ob-tab-content` structure and keep card styling local to a new
Domain Information stylesheet.

**Tech Stack:** Jinja2 templates, Bootstrap 5.3, Bootstrap Icons, vanilla
JavaScript, CSS, pytest static UI contracts.

## Global Constraints

- Preserve current `graph_backend` API values and payload construction.
- Use existing Lakebase, Lakehouse, and Neo4j brand assets.
- Use a neutral Bootstrap icon for No Backend.
- Preserve all existing uncommitted changes.
- Do not commit unless the user explicitly requests it.

---

### Task 1: Add failing backend-card UI contracts

**Files:**
- Modify: `tests/units/front/test_no_backend_ui.py`

**Interfaces:**
- Consumes: `_domain_information.html` and `domain-information.js` source.
- Produces: contracts for the card radio values, logo classes, standard
  content surface, synchronization, and graphless visibility.

- [ ] **Step 1: Replace select-only assertions with radio-card contracts**

Assert that the template contains one radio per existing backend value,
`ob-icon-postgresql`, `ob-icon-lakehouse`, and `ob-icon-neo4j`; that
`domainInfoTabContent` uses `ob-tab-content`; and that the graph-only blocks
have stable IDs.

- [ ] **Step 2: Add JavaScript visibility and synchronization contracts**

Assert that card changes update `#domainGraphBackend`, dispatch its existing
change pipeline, and that `applyGraphlessConstraints()` toggles the migration,
Dual Knowledge Graph, and Triple-Store Gateway sections.

- [ ] **Step 3: Run the focused tests and verify RED**

Run:
`uv run --frozen pytest -q tests/units/front/test_no_backend_ui.py tests/units/domain/test_domain_neo4j_connection_picker.py`

Expected: failures for missing card markup, synchronization helpers, and
graphless visibility targets.

### Task 2: Implement the standard tab surface and backend cards

**Files:**
- Modify: `src/front/templates/domain.html`
- Modify: `src/front/templates/partials/domain/_domain_information.html`
- Create: `src/front/static/domain/css/domain-information.css`
- Modify: `src/front/static/domain/js/domain-information.js`

**Interfaces:**
- Consumes: hidden select value `#domainGraphBackend`.
- Produces: `syncGraphBackendCards()` and card-to-select change propagation;
  stable IDs `graphBackendMigrationNotice`, `dualKnowledgeGraphSection`, and
  `tripleStoreGatewaySection`.

- [ ] **Step 1: Load the focused stylesheet**

Add cache-busted `domain/css/domain-information.css` to the Domain page.

- [ ] **Step 2: Convert the page to the shared tab-content structure**

Make `#domainForm` the direct `ob-tabs-wrap` child containing the existing
`ob-tabs` rail and `#domainInfoTabContent.tab-content.ob-tab-content`.

- [ ] **Step 3: Replace the visible select with native radio cards**

Retain `#domainGraphBackend` as a hidden compatibility value source. Add four
`.domain-backend-option` radios and labels with the approved descriptions,
existing brand classes for Lakebase/Lakehouse/Neo4j, and a neutral icon for No
Backend.

- [ ] **Step 4: Add card styling**

Define a four/two/one-column responsive grid, token-based borders and surfaces,
selected and focus-visible states, disabled state, consistent logo sizing, and
no overrides of the shared `.ob-tabs` component.

- [ ] **Step 5: Synchronize cards and graph-only sections**

On radio change, update the hidden select and dispatch `change`. After
server-side paint and late `/domain/info` hydration, update radio checked
states. Extend `applyGraphlessConstraints()` to hide the approved graph-only
sections for `none`.

- [ ] **Step 6: Run focused tests and verify GREEN**

Run:
`uv run --frozen pytest -q tests/units/front/test_no_backend_ui.py tests/units/domain/test_domain_neo4j_connection_picker.py`

Expected: all selected tests pass.

### Task 3: Verify the UI and document the change

**Files:**
- Modify: `changelogs/v0.8.0/benoitcayladbx_2026-08-30.log`

**Interfaces:**
- Consumes: completed backend card UI.
- Produces: browser evidence and the required v0.8.0 changelog entry.

- [ ] **Step 1: Browser-test Domain → Information**

Verify card selection, real logos, backend-specific sections, No Backend
visibility, keyboard focus/selection, responsive layout, read-only behaviour,
and absence of console errors.

- [ ] **Step 2: Run lint diagnostics**

Check the modified template, CSS, JavaScript, and Python test files and resolve
new diagnostics.

- [ ] **Step 3: Run the required non-scenario suite**

Run: `uv run --frozen pytest -q -m "not scenario"`

Expected: the complete non-scenario suite passes.

- [ ] **Step 4: Append the changelog section**

Record the context, changed paths, UI behaviour, and exact final test result in
English under version 0.8.0.
