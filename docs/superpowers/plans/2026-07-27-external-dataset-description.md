# External Dataset Description Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist an optional purpose description on class-linked external datasets, default it from UC table comments on first link, and surface it in MCP agent context and Graph Explorer.

**Architecture:** Store `description` on the existing `dataset` JSON object. Ontology panel edits and selection rules own the value. REST `/nodes/context` and MCP formatters pass it through. Graph Explorer loaders retain `dataset` so the entity details panel can render it.

**Tech Stack:** Vanilla JS, FastAPI Pydantic models, MCP text formatters, pytest static/unit contracts.

## Global Constraints

- Persist only `dataset.description`; do not keep live UC sync after first selection.
- First selection defaults from asset `comment`; dataset replacement preserves existing description.
- Removing a dataset clears description; a later selection may default again from UC.
- Empty/missing description remains valid everywhere.
- Do not trigger AI Feature Lifecycle eval gates (deterministic metadata only).
- Do not create a git commit unless the user explicitly requests one.

---

### Task 1: Ontology panel description field

**Files:**
- Modify: `src/front/static/ontology/js/ontology-shared-panels.js`
- Create/Modify: `tests/units/front/test_ontology_dataset_key_dropdown.py` (extend) or create `tests/units/front/test_ontology_dataset_description.py`

**Interfaces:**
- Consumes: `sharedPanelDataset`, UC asset `{name, comment, full_name, table_type}`
- Produces: `sharedPanelDataset.description: string | null`, `onDatasetDescriptionChange(value)`

- [ ] **Step 1: Write failing contracts**

```python
def test_dataset_description_textarea_and_handler_exist():
    source = _source()
    assert 'id="datasetDescriptionInput"' in source
    assert "function onDatasetDescriptionChange" in source
    assert "sharedPanelDataset.description" in source


def test_first_selection_defaults_from_asset_comment_and_switch_preserves():
    source = _source()
    start = source.index("function _datasetSelectAsset")
    body = source[start:start + 700]
    assert "previousDescription" in body or "prevDescription" in body
    assert "asset.comment" in body
    assert "key_column: null" in body
```

- [ ] **Step 2: Run tests red**

`uv run pytest -q tests/units/front/test_ontology_dataset_description.py`

- [ ] **Step 3: Implement UI + selection rules**

In `renderSharedEntityDataset`, after key-column markup, add editable textarea / view-only text:

```javascript
const descHtml = !viewOnly
    ? `<div class="mt-2">
         <label class="form-label form-label-sm mb-1" style="font-size:0.8rem;">
           Description <small class="text-muted">(purpose of this dataset)</small>
         </label>
         <textarea class="form-control form-control-sm" rows="2"
                   id="datasetDescriptionInput"
                   oninput="onDatasetDescriptionChange(this.value)">${escapeHtml(ds.description || '')}</textarea>
       </div>`
    : (ds.description
        ? `<div class="mt-1"><small class="text-muted">${escapeHtml(ds.description)}</small></div>`
        : '');
```

Include `${descHtml}` in the rendered container after `${keyColHtml}`.

Replace `_datasetSelectAsset` with:

```javascript
function _datasetSelectAsset(asset) {
    const previousDescription = Object.prototype.hasOwnProperty.call(
        sharedPanelDataset || {},
        'description'
    )
        ? (sharedPanelDataset.description || '')
        : null;
    const defaultDescription = String(asset.comment || '').trim();
    sharedPanelDataset = {
        catalog: _datasetSelCatalog,
        schema: _datasetSelSchema,
        asset: asset.name,
        type: (asset.table_type || '').toUpperCase() === 'VIEW' ? 'VIEW' : 'TABLE',
        fullName: asset.full_name || `${_datasetSelCatalog}.${_datasetSelSchema}.${asset.name}`,
        key_column: null,
        description: previousDescription !== null ? previousDescription : (defaultDescription || null),
    };
    markPanelDirty();
    renderSharedEntityDataset(false);
    closeDatasetSelectorModal();
    showNotification(`Dataset linked: ${sharedPanelDataset.fullName}`, 'success', 3000);
}
```

Add:

```javascript
function onDatasetDescriptionChange(value) {
    if (!sharedPanelDataset) return;
    sharedPanelDataset.description = value.trim() || null;
    markPanelDirty();
}
```

- [ ] **Step 4: Run focused tests green**

---

### Task 2: REST + MCP exposure

**Files:**
- Modify: `src/api/routers/digitaltwin.py` (`NodeContextDataset`, `dt_nodes_context`)
- Modify: `src/mcp-server/server/app.py` (`_format_class_context_block`, `_format_node_context_response`)
- Modify: `tests/units/api/test_node_context_endpoint.py`
- Create: `tests/units/mcp/test_node_context_formatting.py`

**Interfaces:**
- Consumes: `raw_dataset.description`
- Produces: `NodeContextDataset.description: Optional[str]`, MCP `Purpose: ...` lines

- [ ] **Step 1: Failing tests**

Extend node-context fixture with `"description": "Customer master records."` and assert `body["dataset"]["description"] == "Customer master records."`.

```python
from mcp_server.server.app import (
    _format_class_context_block,
    _format_node_context_response,
)

def test_format_class_context_includes_purpose():
    text = _format_class_context_block("CUST1", {
        "name": "Customer",
        "dataset": {"fullName": "main.crm.customers", "key_column": "id",
                    "description": "Customer master records."},
        "bridges": [],
    })
    assert "Purpose: Customer master records." in text

def test_format_node_context_includes_purpose():
    text = _format_node_context_response({
        "success": True,
        "entity_uri": "https://example.com/Customer/CUST1",
        "entity_local_id": "CUST1",
        "class_name": "Customer",
        "dataset": {"fullName": "main.crm.customers", "description": "Customer master records."},
        "bridges": [],
    })
    assert "Purpose: Customer master records." in text
```

Adjust import path if package layout differs (`from server.app import ...` under `src/mcp-server`). Prefer importing the same module used by existing MCP unit tests.

- [ ] **Step 2: Implement**

```python
class NodeContextDataset(BaseModel):
    fullName: str
    key_column: Optional[str] = None
    key_column_missing: Optional[bool] = None
    description: Optional[str] = None
    rows: Optional[List[Dict[str, Any]]] = None
```

When building `dataset_out`:

```python
desc = (raw_dataset.get("description") or "").strip() or None
dataset_out = NodeContextDataset(
    fullName=raw_dataset["fullName"],
    key_column=key_col,
    key_column_missing=key_col_missing,
    description=desc,
    rows=rows,
)
```

In MCP formatters, after Dataset lines:

```python
purpose = (dataset.get("description") or "").strip()
if purpose:
    lines.append(f"  Purpose: {purpose}")
```

(Use matching indentation for each formatter.)

- [ ] **Step 3: Run focused tests green**

---

### Task 3: Graph Explorer display

**Files:**
- Modify: `src/front/static/query/js/query-loaders.js`
- Modify: `src/front/static/query/js/query-entity-details.js`
- Create: `tests/units/front/test_query_dataset_description.py`

**Interfaces:**
- Consumes: `cls.dataset` from ontology load
- Produces: `entityMapping.dataset` / `classInfo.dataset` rendered in details panel

- [ ] **Step 1: Failing contracts**

```python
def test_loaders_retain_class_dataset():
    js = Path("src/front/static/query/js/query-loaders.js").read_text()
    assert "dataset: cls.dataset" in js or "dataset: cls.dataset || null" in js
    assert "dataset: classInfo?.dataset" in js


def test_entity_details_renders_dataset_section():
    js = Path("src/front/static/query/js/query-entity-details.js").read_text()
    assert "Dataset" in js
    assert "fullName" in js
    assert "key_column" in js
```

- [ ] **Step 2: Implement**

In `loadOntologyClasses` classInfo add `dataset: cls.dataset || null`.
In `loadEntityMappings` mappingInfo add `dataset: classInfo?.dataset || null`.

In `showEntityDetails`, before bridges section:

```javascript
const dataset = entityMapping?.dataset || classInfo?.dataset || null;
if (dataset && (dataset.fullName || dataset.asset)) {
    const fullName = dataset.fullName || [dataset.catalog, dataset.schema, dataset.asset].filter(Boolean).join('.');
    html += `
        <div class="entity-detail-section">
            <h6><i class="bi bi-table"></i> Dataset</h6>
            <div class="entity-detail-item">
                <span class="detail-key">Table</span>
                <span class="detail-value"><code>${escapeHtml(fullName)}</code></span>
            </div>
            ${dataset.key_column ? `
            <div class="entity-detail-item">
                <span class="detail-key">Key</span>
                <span class="detail-value"><code>${escapeHtml(dataset.key_column)}</code></span>
            </div>` : ''}
            ${dataset.description ? `
            <div class="entity-detail-item">
                <span class="detail-key">Purpose</span>
                <span class="detail-value">${escapeHtml(dataset.description)}</span>
            </div>` : ''}
        </div>
    `;
}
```

- [ ] **Step 3: Run focused tests green**

---

### Task 4: Changelog and verification

**Files:**
- Append: `changelogs/v0.7.0/benoitcayladbx_2026-07-27.log`

- [ ] **Step 1: Append changelog section** covering UI, REST/MCP, Graph Explorer, tests, and docs.
- [ ] **Step 2: Run** `uv run pytest -q -m "not scenario"`
- [ ] **Step 3: Record exact test summary in changelog. Do not commit unless asked.
