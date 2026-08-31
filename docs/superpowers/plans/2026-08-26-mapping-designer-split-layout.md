# Mapping Designer Split Layout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Mapping / Designer use Ontology / Designer's two-card split
chrome, with the detail panel always visible at the bottom and a horizontal
resize handle.

**Architecture:** Reuse `ob-split-shell` / `ob-split-pane` /
`.detail-panel-resize-handle` from Ontology. Add
`.ob-split-shell--column` for vertical orientation. Mapping JS stops treating
the panel as a drawer (`height: 0` / `.panel-open`) and instead hydrates or
clears an always-visible pane.

**Tech Stack:** Jinja2, CSS flexbox, vanilla JS, D3, pytest contract tests.

## Global Constraints

- Two framed cards, not one integrated frame and not an overlay.
- Bottom panel always visible; default height **320px**, min **200px**, max **50%**.
- Handle is 8px, `row-resize`, bar 3px, hover/active `var(--db-primary)`.
- Gutter `0.5rem`; pane border `var(--db-border)`; radius `var(--db-radius-card)`.
- No `shadow-sm` on the Mapping Designer canvas wrapper.
- `openMappingPanel()` hydrates; it must not toggle visibility.
- `closeMappingPanel()` clears to placeholder and deselects; it must not collapse to `height: 0`.
- Canvas background click clears the panel (existing `guardedCloseMappingPanel` path).
- Persist height in `sessionStorage` key `mappingDesignerPanelHeight`.
- Manual Mapping bottom panel is out of scope — do not change its open/close CSS.
- Do not restyle Information / Mapping / SQL form internals.
- Do not commit unless the user asks.
- Changelog in English under `changelogs/v0.8.0/benoitcayladbx_2026-08-26.log`.
- Tests: `uv run --frozen pytest -q -m "not scenario"` (`--frozen` mandatory).

---

### Task 1: Contract tests, markup, and column split CSS

**Files:**
- Create: `tests/units/front/test_mapping_designer_split_layout.py`
- Modify: `src/front/templates/partials/mapping/_mapping_design.html`
- Modify: `src/front/templates/mapping.html`
- Modify: `src/front/static/ontology/css/ontology-shared-panels.css`
- Modify: `src/front/static/global/css/mapping.css`
- Test: `tests/units/front/test_mapping_designer_split_layout.py`
- Test: `tests/units/front/test_manual_mapping_panel_host.py` (must still pass)

**Interfaces:**
- Produces CSS class `ob-split-shell--column` and Mapping markup matching the spec tree.
- Consumes existing `ob-split-shell`, `ob-split-pane`, `.detail-panel-resize-handle`.

- [ ] **Step 1: Write failing contract tests** in
  `tests/units/front/test_mapping_designer_split_layout.py` asserting:
  - `_mapping_design.html` has `ob-split-shell--column` on `#mappingDesignerContainer`
  - `#mappingMapCard` and `#mappingRightPanel` have `ob-split-pane`
  - a `.detail-panel-resize-handle` exists between them
  - `#mappingMapCard` does not include `shadow-sm`
  - `#panelBody` contains a `.panel-placeholder` in the template
  - `ontology-shared-panels.css` is linked from `mapping.html`
  - `.ob-split-shell--column` is `flex-direction: column` with `gap: 0.5rem`
  - column handle uses `row-resize` and height `8px`
  - `#mappingRightPanel` / `.mapping-right-panel` in the Designer context is not
    `height: 0`; default height is `320px`, min `200px`, max `50%`
  - `mapping.css` Designer drawer rules `.mapping-designer-container.panel-open`
    no longer split flex 0.55/0.45
  - `.manual-bottom-panel` rules remain (Manual Mapping unchanged)

- [ ] **Step 2: Run the new tests — expect RED**

Run: `uv run --frozen pytest -q tests/units/front/test_mapping_designer_split_layout.py`

- [ ] **Step 3: Implement markup and CSS**

Markup tree:

```
#mappingDesignerContainer.mapping-designer-container.ob-split-shell.ob-split-shell--column
  #mappingMapCard.ob-split-pane  (canvas + loading overlay; no card/shadow-sm)
  .detail-panel-resize-handle (title="Drag to resize panels")
    .resize-handle-bar
  #mappingRightPanel.mapping-right-panel.ob-split-pane
    .panel-header / #panelBody (placeholder) / #panelFooter
```

Placeholder copy, English: "Click on an entity or relationship to configure its mapping"

CSS: add `.ob-split-shell--column` next to existing split styles. Horizontal
handle: `height: 8px; width: 100%; cursor: row-resize;` bar `width: 40px; height: 3px`.
Designer panel: `height: 320px; min-height: 200px; max-height: 50%; width: 100%;`
Remove drawer `height: 0` and `.panel-open` flex split for `#mappingRightPanel`.
Keep Manual Mapping `.manual-bottom-panel` as-is.
Link `ontology-shared-panels.css` in `mapping.html` extra_css.

Match Ontology Designer sidebar padding if Mapping currently uses 0.5rem — use
the same `sidebar-content` padding as Ontology unless a double scrollbar appears.

- [ ] **Step 4: GREEN the contract tests plus**
  `tests/units/front/test_manual_mapping_panel_host.py`

---

### Task 2: Always-open JS, resize persistence, canvas clear

**Files:**
- Modify: `src/front/static/mapping/js/mapping-design.js`
- Modify: `src/front/static/mapping/css/mapping-design.css`
- Modify: `src/front/templates/partials/mapping/_mapping_design.html`
- Modify: `tests/units/front/test_mapping_designer_split_layout.py` (JS assertions)
- Modify: `.cursor/11-frontend-design.mdc` (document Mapping column split)
- Modify: `changelogs/v0.8.0/benoitcayladbx_2026-08-26.log`

**Interfaces:**
- `openMappingPanel()` — show content, remove empty placeholder class, call `resizeMapSvg()`
- `closeMappingPanel()` — `releaseMappingPanel()`, clear selection, restore placeholder, do **not** hide pane
- `setupMappingDesignerResizeHandle()` — drag height, clamp 200px–50% of container, persist `mappingDesignerPanelHeight`
- Restore stored height on init
- `initMappingMapGridToggle()` — default-visible grid persisted as `mappingMapGridVisible`

- [ ] **Step 1: Extend failing JS contract tests** asserting:
  - `openMappingPanel` does not `classList.add('panel-open')`
  - `closeMappingPanel` does not `classList.remove('panel-open')`
  - `closeMappingPanel` writes `.panel-placeholder` back into `#panelBody`
  - JS contains `mappingDesignerPanelHeight` and `row-resize`
  - SVG background click still calls `guardedCloseMappingPanel`
  - `setupMappingDesignerResizeHandle` exists and is invoked on init
  - the grid toggle is active by default, exposes `aria-pressed`, uses the
    Ontology 24px dotted pattern, and persists `mappingMapGridVisible`

- [ ] **Step 2: RED then implement JS**

Init: apply stored height if valid, wire handle, render placeholder if body empty.
On drag: set `#mappingRightPanel` style.height, `resizeMapSvg()` on mouseup.
Do not add document-level mousemove listeners if Ontology's already exist in a
conflicting way — keep Mapping listeners scoped and check `isMappingResizing`.

- [ ] **Step 3: Docs + changelog + focused tests GREEN**

- [ ] **Step 4: Run** `uv run --frozen pytest -q -m "not scenario"`

---

### Task 3: Browser verification

**Files:** none unless a bug is found.

- [ ] Desktop 1600×1000 Mapping/Designer: two cards, panel always visible,
  resize, canvas click clears, SVG fills canvas.
- [ ] Mobile 390×844: no horizontal overflow of the designer chrome.
- [ ] Do not persist mapping data changes; layout-only.

---
