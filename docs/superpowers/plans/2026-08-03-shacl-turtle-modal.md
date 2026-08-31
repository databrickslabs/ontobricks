# SHACL Turtle Modal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (inline) or superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Ontology Data Quality inline SHACL Turtle pane with a Bootstrap `modal-xl`.

**Architecture:** Convert `#dqShaclPanel` into `#dqShaclModal`. Header button opens the modal and refreshes Turtle. Refresh / Export / Import and `#dqShaclEditor` keep the same behaviour and ids. No backend changes.

**Tech Stack:** Bootstrap 5 modals, Jinja HTML partial, vanilla JS (`DataQualityModule`).

**Spec:** `documentation/superpowers/specs/2026-08-03-shacl-turtle-modal-design.md`

## Global Constraints

- Keep `#dqShaclEditor` id and existing turtle/export/import endpoints.
- Import remains `#dqImportModal` stacked on the SHACL modal.
- No Knowledge Graph DQ page or SWRL pane changes.

---

### Task 1: HTML — modal instead of pane

**Files:**
- Modify: `src/front/templates/partials/ontology/_ontology_dataquality.html`
- Modify: `src/front/static/ontology/js/ontology-dataquality.js`
- Modify: `src/front/static/ontology/css/ontology-dataquality.css` (min-height if needed)

- [x] **Step 1:** Change SHACL button to `openShaclModal()`; remove `#dqShaclPanel`; add `#dqShaclModal` (`modal-xl`) with Refresh / Export / Import + `#dqShaclEditor`.
- [x] **Step 2:** Replace `toggleShaclPanel` / `_shaclPanelOpen` with `openShaclModal()` that shows the modal and calls `refreshTurtle()`.
- [x] **Step 3:** Optional CSS min-height on `#dqShaclEditor` in the modal.
- [x] **Step 4:** Changelog + `uv run --frozen pytest -q -m "not scenario"`.

---
