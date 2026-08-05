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

### Task 3: Behavioral addon coverage

**Files:**
- Modify: `tests/units/api/test_node_context_endpoint.py`
- Create: `tests/mcp/integration/test_dataset_context_tools.py`
- Modify: `tests/units/front/test_query_dataset_description.py`

**Interfaces:**
- API: `GET /api/v1/digitaltwin/nodes/context`
- MCP: `select_domain`, `list_entity_types`, `describe_entity`,
  `get_entity_context`
- Frontend: `openDatasetPreviewModal`, Sigma node context menu

- [ ] Assert API emits exact key-filter SQL and returns query rows
- [ ] Assert API distinguishes empty rows from query failures
- [ ] Invoke MCP tools through FastMCP with `httpx.MockTransport`
- [ ] Assert MCP caches and formats dataset name, description, and rows
- [ ] Assert MCP forwards `fetch_dataset_rows=true` and requested row limit
- [ ] Strengthen Graph Explorer contracts for all modal states and context menu
- [ ] Run focused tests and full non-scenario suite
- [ ] Append changelog section
