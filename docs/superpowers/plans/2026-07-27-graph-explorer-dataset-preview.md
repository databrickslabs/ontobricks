# Graph Explorer Dataset Preview Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show Dataset metadata + 10-row Preview modal in Graph Explorer Details, matching Dashboard section chrome.

**Architecture:** Shared `openDatasetPreviewModal(entityUri, entityType, entityId)` fetches existing `/nodes/context` with row fetch. Sigma and classic Details both render a Dataset box that invokes it.

**Tech Stack:** Vanilla JS, Bootstrap modal, FastAPI node-context endpoint (unchanged), pytest source contracts.

## Global Constraints

- Preview hard-caps at 10 rows (`dataset_row_limit=10`).
- No new backend endpoints.
- Do not create a git commit unless the user explicitly requests one.

---

### Task 1: Preview modal helper + contracts

**Files:**
- Modify: `src/front/static/query/js/query-dashboard.js` (or add `query-dataset.js` + dtwin.html script tag)
- Create/Modify: `tests/units/front/test_query_dataset_description.py`

- [ ] Write failing contracts for `openDatasetPreviewModal`, nodes/context URL, limit 10
- [ ] Implement modal with loading / table / error states
- [ ] Wire script include if new file
- [ ] Run contracts green

### Task 2: Classic + Sigma Details Dataset box

**Files:**
- Modify: `src/front/static/query/js/query-entity-details.js`
- Modify: `src/front/static/query/js/query-sigmagraph.js`
- Modify: `tests/units/front/test_query_dataset_description.py`

- [ ] Classic: Dataset section with Description label + Preview rows button
- [ ] Sigma: Dataset `_sec` after Dashboard with same content
- [ ] Disable Preview when `key_column` missing
- [ ] Contracts green; full suite `uv run pytest -q -m "not scenario"`
- [ ] Changelog section
