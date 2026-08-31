# Ontology Dataset Key Column Dropdown Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the ontology entity dataset key-column text input with a dropdown containing every column from the selected Unity Catalog table or view.

**Architecture:** Keep the backend contract unchanged and load columns through `POST /mapping/table-columns`. Add focused client-side helpers in the existing ontology shared-panel module for rendering states, fetching/caching column metadata, rejecting stale responses, preserving missing saved keys, and retrying failures.

**Tech Stack:** Vanilla JavaScript, Bootstrap 5, FastAPI’s existing mapping endpoint, pytest static contract tests.

## Global Constraints

- List all columns; do not filter to ID-like names.
- Persist only `sharedPanelDataset.key_column`; never persist fetched metadata.
- Keep view-only rendering unchanged.
- A failed request must preserve the saved key and must not populate the cache.
- Do not add dependencies or change the backend API.
- Do not create a git commit unless the user explicitly requests one.

---

### Task 1: Add failing dropdown contract tests

**Files:**
- Create: `tests/units/front/test_ontology_dataset_key_dropdown.py`
- Read: `src/front/static/ontology/js/ontology-shared-panels.js`

**Interfaces:**
- Consumes: generated ontology entity panel JavaScript.
- Produces: regression contracts for dropdown markup, endpoint use, cache, stale-response protection, missing-key handling, retry, and dirty-state updates.

- [ ] **Step 1: Create the static contract test**

```python
"""Contracts for ontology external-dataset key-column selection."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SHARED_PANELS_JS = (
    REPO_ROOT / "src/front/static/ontology/js/ontology-shared-panels.js"
)


def _source() -> str:
    return SHARED_PANELS_JS.read_text(encoding="utf-8")


def test_dataset_key_column_uses_select_not_text_input():
    source = _source()
    assert 'id="datasetKeyColumnSelect"' in source
    assert 'id="datasetKeyColumnInput"' not in source
    assert 'onchange="onDatasetKeyColumnChange(this.value)"' in source


def test_dataset_columns_use_existing_mapping_endpoint_and_memory_cache():
    source = _source()
    assert "const _datasetColumnCache = new Map();" in source
    assert "fetch('/mapping/table-columns'" in source
    assert "catalog: dataset.catalog" in source
    assert "schema: dataset.schema" in source
    assert "table: dataset.asset" in source


def test_dataset_column_states_and_missing_saved_key_are_rendered():
    source = _source()
    for label in (
        "Select a key column…",
        "Loading columns…",
        "No columns found",
        "Failed to load columns",
        "(missing)",
    ):
        assert label in source


def test_dataset_column_fetch_has_retry_and_stale_response_guard():
    source = _source()
    assert "function retryDatasetKeyColumns()" in source
    assert "_loadDatasetKeyColumns(sharedPanelDataset, true)" in source
    assert "_isCurrentDataset(datasetKey)" in source
    assert "_datasetColumnCache.set(datasetKey, columns)" in source


def test_key_column_change_keeps_existing_dirty_state_contract():
    source = _source()
    start = source.index("function onDatasetKeyColumnChange")
    body = source[start : start + 250]
    assert "sharedPanelDataset.key_column = value || null;" in body
    assert "markPanelDirty();" in body
```

- [ ] **Step 2: Run the test and verify red state**

Run:

```bash
uv run pytest -q tests/units/front/test_ontology_dataset_key_dropdown.py
```

Expected: failures because `datasetKeyColumnSelect`, column cache, loading helpers, and retry do not exist yet.

---

### Task 2: Implement asynchronous key-column dropdown

**Files:**
- Modify: `src/front/static/ontology/js/ontology-shared-panels.js:944-989`
- Modify: `src/front/static/ontology/js/ontology-shared-panels.js:1000-1003`

**Interfaces:**
- Consumes: `sharedPanelDataset` shaped as `{catalog, schema, asset, key_column}` and `POST /mapping/table-columns` returning `{columns: [{name, type, comment}]}`.
- Produces:
  - `_datasetKey(dataset): string`
  - `_isCurrentDataset(datasetKey): boolean`
  - `_loadDatasetKeyColumns(dataset, force = false): Promise<void>`
  - `_populateDatasetKeyColumnSelect(columns): void`
  - `_setDatasetKeyColumnState(label, retry): void`
  - `retryDatasetKeyColumns(): void`

- [ ] **Step 1: Add page-lifetime column cache near dataset selector state**

Add beside `_datasetAllAssets`:

```javascript
const _datasetColumnCache = new Map();

function _datasetKey(dataset) {
    if (!dataset) return '';
    return `${dataset.catalog || ''}.${dataset.schema || ''}.${dataset.asset || ''}`;
}

function _isCurrentDataset(datasetKey) {
    return Boolean(sharedPanelDataset && _datasetKey(sharedPanelDataset) === datasetKey);
}
```

- [ ] **Step 2: Replace editable key-column input with loading select**

In `renderSharedEntityDataset`, replace `keyColHtml`’s editable branch with:

```javascript
const keyColHtml = !viewOnly
    ? `<div class="mt-2">
         <label class="form-label form-label-sm mb-1" style="font-size:0.8rem;">
           Key column <small class="text-muted">(used to match node ID)</small>
         </label>
         <div class="d-flex gap-2">
           <select class="form-select form-select-sm"
                   id="datasetKeyColumnSelect"
                   onchange="onDatasetKeyColumnChange(this.value)"
                   disabled>
             <option value="">Loading columns…</option>
           </select>
           <button type="button"
                   class="btn btn-sm btn-outline-secondary d-none"
                   id="datasetKeyColumnRetry"
                   onclick="retryDatasetKeyColumns()">Retry</button>
         </div>
       </div>`
    : (ds.key_column
        ? `<div class="mt-1"><small class="text-muted">Key: <code>${escapeHtml(ds.key_column)}</code></small></div>`
        : '');
```

After assigning `container.innerHTML`, start loading only in edit mode:

```javascript
if (!viewOnly) {
    void _loadDatasetKeyColumns(ds);
}
```

- [ ] **Step 3: Add state and option rendering helpers**

Add after `renderSharedEntityDataset`:

```javascript
function _setDatasetKeyColumnState(label, retry = false) {
    const select = panelGetById('datasetKeyColumnSelect');
    const retryButton = panelGetById('datasetKeyColumnRetry');
    if (select) {
        select.disabled = true;
        select.innerHTML = `<option value="">${escapeHtml(label)}</option>`;
    }
    if (retryButton) retryButton.classList.toggle('d-none', !retry);
}

function _populateDatasetKeyColumnSelect(columns) {
    const select = panelGetById('datasetKeyColumnSelect');
    const retryButton = panelGetById('datasetKeyColumnRetry');
    if (!select || !sharedPanelDataset) return;

    const names = columns
        .map(column => String(column?.name || '').trim())
        .filter(Boolean);
    if (!names.length) {
        _setDatasetKeyColumnState('No columns found', true);
        return;
    }

    const current = sharedPanelDataset.key_column || '';
    const options = [new Option('Select a key column…', '')];
    if (current && !names.includes(current)) {
        options.push(new Option(`${current} (missing)`, current, true, true));
    }
    for (const name of names) {
        const selected = name === current;
        options.push(new Option(name, name, selected, selected));
    }
    select.replaceChildren(...options);
    select.disabled = false;
    if (retryButton) retryButton.classList.add('d-none');
}
```

- [ ] **Step 4: Add cached fetch, stale-response guard, and retry**

Add after the option helpers:

```javascript
async function _loadDatasetKeyColumns(dataset, force = false) {
    const datasetKey = _datasetKey(dataset);
    if (!datasetKey || !dataset?.catalog || !dataset?.schema || !dataset?.asset) {
        _setDatasetKeyColumnState('No columns found');
        return;
    }

    if (!force && _datasetColumnCache.has(datasetKey)) {
        _populateDatasetKeyColumnSelect(_datasetColumnCache.get(datasetKey));
        return;
    }

    _setDatasetKeyColumnState('Loading columns…');
    try {
        const response = await fetch('/mapping/table-columns', {
            method: 'POST',
            credentials: 'same-origin',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                catalog: dataset.catalog,
                schema: dataset.schema,
                table: dataset.asset,
            }),
        });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const data = await response.json();
        const columns = Array.isArray(data.columns) ? data.columns : [];
        if (columns.length) {
            _datasetColumnCache.set(datasetKey, columns);
        }
        if (_isCurrentDataset(datasetKey)) {
            _populateDatasetKeyColumnSelect(columns);
        }
    } catch (error) {
        console.error('[Dataset] Error loading columns:', error);
        if (_isCurrentDataset(datasetKey)) {
            _setDatasetKeyColumnState('Failed to load columns', true);
        }
    }
}

function retryDatasetKeyColumns() {
    if (!sharedPanelDataset) return;
    void _loadDatasetKeyColumns(sharedPanelDataset, true);
}
```

The cache write occurs only after an HTTP-successful, JSON-parsed, non-empty
response. Empty responses stay retryable because the backend may degrade a UC
metadata error to an empty successful response. DOM `Option` construction
keeps column names safe in both text and value contexts. The current-dataset
check prevents a late response from repainting a newly selected entity or
asset.

- [ ] **Step 5: Keep key persistence and dirty-state semantics**

Replace the existing handler with:

```javascript
function onDatasetKeyColumnChange(value) {
    if (!sharedPanelDataset) return;
    sharedPanelDataset.key_column = value || null;
    markPanelDirty();
}
```

- [ ] **Step 6: Run focused tests**

Run:

```bash
uv run pytest -q tests/units/front/test_ontology_dataset_key_dropdown.py
```

Expected: all tests pass.

---

### Task 3: Record and verify the change

**Files:**
- Create or append: `changelogs/v0.7.0/benoitcayladbx_2026-07-27.log`
- Verify: `src/front/static/ontology/js/ontology-shared-panels.js`
- Verify: `tests/units/front/test_ontology_dataset_key_dropdown.py`

**Interfaces:**
- Consumes: completed implementation and test results.
- Produces: required v0.7.0 changelog entry and repository-level verification evidence.

- [x] **Step 1: Append the required changelog section**

Use this section, preserving any existing content in the daily file:

```text
Ontology dataset key-column dropdown

Context:
Ontology entities linked to external Unity Catalog datasets required users to type a key column manually, allowing invalid column names.

Changes:
1. src/front/static/ontology/js/ontology-shared-panels.js — replaced the free-text key-column control with an asynchronously populated, cached dropdown with loading, empty, missing-key, failure, and retry states.
2. tests/units/front/test_ontology_dataset_key_dropdown.py — added regression contracts for dropdown rendering, API usage, caching, stale-response protection, retry, and dirty-state persistence.
3. docs/superpowers/specs/2026-07-27-ontology-dataset-key-column-dropdown-design.md — documented the approved design.
4. docs/superpowers/plans/2026-07-27-ontology-dataset-key-column-dropdown.md — documented the implementation plan.

Modified files:
- src/front/static/ontology/js/ontology-shared-panels.js
- tests/units/front/test_ontology_dataset_key_dropdown.py
- docs/superpowers/specs/2026-07-27-ontology-dataset-key-column-dropdown-design.md
- docs/superpowers/plans/2026-07-27-ontology-dataset-key-column-dropdown.md
- changelogs/v0.7.0/benoitcayladbx_2026-07-27.log

Test result:
- uv run pytest -q tests/units/front/test_ontology_dataset_key_dropdown.py
- uv run pytest -q -m "not scenario"
```

Update the test-result lines with pass counts or failure details after running them.

- [x] **Step 2: Check edited-file diagnostics**

Run IDE lint diagnostics for:

- `src/front/static/ontology/js/ontology-shared-panels.js`
- `tests/units/front/test_ontology_dataset_key_dropdown.py`

Expected: no newly introduced diagnostics.

- [x] **Step 3: Run the required repository suite**

Run:

```bash
uv run pytest -q -m "not scenario"
```

Expected: suite passes. If failures pre-exist or are unrelated, record exact failing tests in the changelog and final handoff.

- [x] **Step 4: Manually verify the UI**

With the existing development server:

1. Open an editable ontology entity.
2. Select an external Unity Catalog table or view.
3. Confirm Key column shows all returned columns and saves the selection.
4. Reopen the entity and confirm the key remains selected.
5. Select another dataset and confirm the key resets and new columns load.
6. Simulate an endpoint failure and confirm Retry appears without clearing an existing saved key.

Expected: no free-text key field remains; all loading and recovery states match the design.

> Verified on the running local app with `samples.nyctaxi.trips`: six UC
> columns loaded, selection persisted on reopen, changing dataset cleared the
> key, and an empty response exposed Retry and re-fetched successfully.
