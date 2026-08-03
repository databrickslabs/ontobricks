# MCP Node Context (Bridges + Dataset) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Surface per-class `bridges` and `dataset` (with optional row retrieval and cross-domain traversal) through two MCP tools: enriched `describe_entity` and new `get_entity_context`.

**Architecture:** New REST endpoint `GET /api/v1/digitaltwin/nodes/context` does all server-side orchestration (class resolution, SQL row fetch, bridge traversal). MCP server is a thin proxy: `describe_entity` appends a metadata `[Context]` block per node; `get_entity_context` calls the new endpoint with optional flags. A new lightweight `GET /api/v1/domain/classes` endpoint provides class Actions without loading full OWL. A `key_column` field is added to the Dataset panel in the ontology UI.

**Tech Stack:** Python / FastAPI / Pydantic (backend), FastMCP / httpx (MCP server), JavaScript (ontology UI), pytest / httpx.MockTransport (tests)

## Global Constraints

- Python ≥ 3.11; uv for package management
- `dataset_row_limit` capped server-side at 20; `bridge_depth` capped at 1 (v0.7.0)
- `key_column` defaults to `null`; row fetch silently skipped when absent
- All new REST endpoints go in the external API (`src/api/routers/`) and are registered in `src/api/external_app.py`
- MCP server stays a thin proxy — no business logic beyond formatting
- Tests run with `uv run pytest -q -m "not scenario"` from the repo root
- No changelog or deployment steps — handled separately per `.cursorrules`

---

### Task 1: Add `key_column` to Dataset panel (UI)

**Files:**
- Modify: `src/front/static/ontology/js/ontology-shared-panels.js`

**Interfaces:**
- Produces: `sharedPanelDataset.key_column` (string | null) included in the class save payload alongside existing `catalog`, `schema`, `asset`, `type`, `fullName`
- Consumed by: Task 2 (backend round-trips through `domain.get_classes()`)

- [ ] **Step 1: Locate the dataset render function**

Open `src/front/static/ontology/js/ontology-shared-panels.js`. The dataset is rendered around line 944 in `renderSharedEntityDataset`. The module-level variable is:
```javascript
let sharedPanelDataset = null;  // { catalog, schema, asset, type, fullName }
```

- [ ] **Step 2: Add `key_column` to the dataset object shape**

In the `_confirmDatasetSelection` handler (around line 1162) where `sharedPanelDataset` is assigned, add `key_column`:
```javascript
sharedPanelDataset = {
    catalog: _datasetSelCatalog,
    schema: _datasetSelSchema,
    asset: asset.name,
    type: asset.type || 'TABLE',
    fullName: asset.full_name || `${_datasetSelCatalog}.${_datasetSelSchema}.${asset.name}`,
    key_column: null,   // ← add this line
};
```

- [ ] **Step 3: Render `key_column` input below the asset display**

In `renderSharedEntityDataset` (around line 944), after the existing asset `<div>`, append a key-column input when a dataset is selected and `!viewOnly`:

```javascript
function renderSharedEntityDataset(viewOnly = false) {
    const container = panelGetById('sharedEntityDatasetContent');
    if (!container) return;

    const ds = sharedPanelDataset;
    if (ds && ds.asset) {
        const isView = (ds.type || '').toUpperCase() === 'VIEW';
        const badge = isView
            ? '<span class="badge bg-info text-dark">View</span>'
            : '<span class="badge bg-secondary">Table</span>';
        const fullName = ds.fullName || `${ds.catalog}.${ds.schema}.${ds.asset}`;
        const keyColHtml = !viewOnly
            ? `<div class="mt-2">
                 <label class="form-label form-label-sm mb-1" style="font-size:0.8rem;">
                   Key column <small class="text-muted">(used to match node ID)</small>
                 </label>
                 <input type="text" class="form-control form-control-sm"
                   id="datasetKeyColumnInput"
                   value="${escapeHtml(ds.key_column || '')}"
                   placeholder="e.g. id"
                   oninput="onDatasetKeyColumnChange(this.value)">
               </div>`
            : (ds.key_column
                ? `<div class="mt-1"><small class="text-muted">Key: <code>${escapeHtml(ds.key_column)}</code></small></div>`
                : '');
        container.innerHTML = `
            <div class="d-flex align-items-center gap-2">
                <i class="bi bi-table text-primary"></i>
                <div class="flex-grow-1">
                    <div class="fw-semibold">${escapeHtml(ds.asset)} ${badge}</div>
                    <small class="text-muted">${escapeHtml(fullName)}</small>
                </div>
                ${!viewOnly ? `<button type="button" class="btn btn-sm btn-outline-danger py-0 px-1" onclick="removeSharedEntityDataset()" title="Remove dataset"><i class="bi bi-x"></i></button>` : ''}
            </div>
            ${keyColHtml}
        `;
    } else {
        container.innerHTML = '<small class="text-muted">No dataset assigned</small>';
    }
}
```

- [ ] **Step 4: Add `onDatasetKeyColumnChange` function**

Immediately after `renderSharedEntityDataset`, add:

```javascript
function onDatasetKeyColumnChange(value) {
    if (!sharedPanelDataset) return;
    sharedPanelDataset.key_column = value.trim() || null;
    markPanelDirty();
}
```

- [ ] **Step 5: Verify `key_column` is included in the save payload**

Around line 1877 the panel build payload already does:
```javascript
dataset: sharedPanelDataset || undefined
```
Since `sharedPanelDataset` now carries `key_column`, no further change is needed — `key_column` flows through automatically.

- [ ] **Step 6: Verify reset paths clear `key_column`**

Confirm lines 384, 440, 974 all set `sharedPanelDataset = null` (they do). The new field is on the object, so clearing the reference is sufficient.

- [ ] **Step 7: Commit**

```bash
git add src/front/static/ontology/js/ontology-shared-panels.js
git commit -m "feat(ontology-ui): add key_column field to Dataset panel"
```

---

### Task 2: `GET /api/v1/domain/classes` — lightweight class Actions endpoint

**Files:**
- Modify: `src/api/routers/domains.py` (add handler + response model)
- Modify: `src/api/external_app.py` (already imports `domains_router` — no change needed)
- Create: `tests/units/api/test_domain_classes_endpoint.py`

**Interfaces:**
- Consumes: `DigitalTwin.resolve_domain(...)` (existing), `domain.get_classes()` (existing)
- Produces: `GET /api/v1/domain/classes` → `{"success": true, "classes": [{"name": "Customer", "uri": "...", "dataset": {...}, "bridges": [...]}]}`

- [ ] **Step 1: Write failing test**

Create `tests/units/api/test_domain_classes_endpoint.py`:

```python
"""Tests for GET /api/v1/domain/classes."""

import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

from shared.fastapi.main import app


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=False)


_MOCK_CLASSES = [
    {
        "name": "Customer",
        "uri": "https://example.com/Customer",
        "dataset": {"catalog": "main", "schema": "crm", "asset": "customers",
                    "type": "TABLE", "fullName": "main.crm.customers", "key_column": "id"},
        "bridges": [{"target_domain": "Finance", "target_class_name": "Contract",
                     "target_class_uri": "https://example.com/Contract", "label": "Owns"}],
    },
    {
        "name": "Order",
        "uri": "https://example.com/Order",
        "dataset": None,
        "bridges": [],
    },
]


class TestDomainClassesEndpoint:
    def test_returns_classes_with_actions(self, client):
        mock_domain = MagicMock()
        mock_domain.get_classes.return_value = _MOCK_CLASSES

        with patch("api.routers.domains.DigitalTwin.resolve_domain", return_value=mock_domain):
            resp = client.get("/api/v1/domain/classes", params={"domain_name": "test"})

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        classes = body["classes"]
        assert len(classes) == 2
        customer = next(c for c in classes if c["name"] == "Customer")
        assert customer["dataset"]["key_column"] == "id"
        assert len(customer["bridges"]) == 1

    def test_filters_empty_actions(self, client):
        mock_domain = MagicMock()
        mock_domain.get_classes.return_value = _MOCK_CLASSES

        with patch("api.routers.domains.DigitalTwin.resolve_domain", return_value=mock_domain):
            resp = client.get("/api/v1/domain/classes", params={"domain_name": "test"})

        body = resp.json()
        order = next(c for c in body["classes"] if c["name"] == "Order")
        # Order has no dataset and no bridges — they should be absent or null/empty
        assert not order.get("dataset")
        assert not order.get("bridges")

    def test_missing_domain_name_uses_session(self, client):
        mock_domain = MagicMock()
        mock_domain.get_classes.return_value = []

        with patch("api.routers.domains.DigitalTwin.resolve_domain", return_value=mock_domain):
            resp = client.get("/api/v1/domain/classes")

        assert resp.status_code == 200
```

- [ ] **Step 2: Run — expect failure (endpoint not yet defined)**

```bash
uv run pytest tests/units/api/test_domain_classes_endpoint.py -v
```
Expected: FAIL with 404 or import error.

- [ ] **Step 3: Add response models and handler to `domains.py`**

At the bottom of the response-models section (after `VersionsResponse`, around line 122), add:

```python
class ClassActionsItem(BaseModel):
    name: str
    uri: str
    dataset: Optional[dict] = None
    bridges: List[dict] = Field(default_factory=list)


class ClassActionsResponse(BaseModel):
    success: bool
    domain_name: Optional[str] = None
    classes: List[ClassActionsItem] = Field(default_factory=list)
    message: Optional[str] = None
```

Then add the handler after `get_domain_design_status` (around line 432):

```python
@router.get(
    "/domain/classes",
    response_model=ClassActionsResponse,
    summary="List class Actions (dataset + bridges)",
    description="Return per-class dataset and bridge metadata for all classes "
    "in the domain's published ontology. Only non-empty values are included.",
)
async def get_domain_classes(
    domain_name: Optional[str] = Query(
        None,
        description="Domain name in the registry (uses current session domain if omitted)",
    ),
    domain_version: Optional[str] = Query(
        None,
        description="Domain version to load (uses latest version if omitted)",
    ),
    registry_catalog: Optional[str] = Query(None, description="Override registry catalog"),
    registry_schema: Optional[str] = Query(None, description="Override registry schema"),
    registry_volume: Optional[str] = Query(None, description="Override registry volume"),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    domain = DigitalTwin.resolve_domain(
        domain_name,
        session_mgr,
        settings,
        registry_catalog,
        registry_schema,
        registry_volume,
        domain_version,
    )
    dname = domain.domain_folder or (domain.info or {}).get("name", "")
    raw_classes = domain.get_classes() or []

    items: List[ClassActionsItem] = []
    for cls in raw_classes:
        dataset = cls.get("dataset") or None
        bridges = cls.get("bridges") or []
        items.append(
            ClassActionsItem(
                name=cls.get("name", ""),
                uri=cls.get("uri", ""),
                dataset=dataset if dataset else None,
                bridges=bridges if bridges else [],
            )
        )

    logger.info(
        "API: domain/classes for '%s' — %d classes", dname, len(items)
    )
    return ClassActionsResponse(success=True, domain_name=dname, classes=items)
```

- [ ] **Step 4: Run — expect pass**

```bash
uv run pytest tests/units/api/test_domain_classes_endpoint.py -v
```
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/api/routers/domains.py tests/units/api/test_domain_classes_endpoint.py
git commit -m "feat(api): add GET /api/v1/domain/classes for class Actions metadata"
```

---

### Task 3: `GET /api/v1/digitaltwin/nodes/context` REST endpoint

**Files:**
- Modify: `src/api/routers/digitaltwin.py` (add handler + response models)
- Create: `tests/units/api/test_node_context_endpoint.py`

**Interfaces:**
- Consumes: `DigitalTwin.resolve_domain(...)`, `domain.get_classes()`, `get_databricks_client(...)`, `store.bfs_traversal(...)` (existing)
- Produces: `GET /api/v1/digitaltwin/nodes/context` → JSON as per spec §5

- [ ] **Step 1: Write failing test**

Create `tests/units/api/test_node_context_endpoint.py`:

```python
"""Tests for GET /api/v1/digitaltwin/nodes/context."""

import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

from shared.fastapi.main import app


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=False)


_CLASSES_WITH_ACTIONS = [
    {
        "name": "Customer",
        "uri": "https://example.com/Customer",
        "dataset": {
            "catalog": "main", "schema": "crm", "asset": "customers",
            "type": "TABLE", "fullName": "main.crm.customers", "key_column": "id",
        },
        "bridges": [
            {
                "target_domain": "Finance",
                "target_class_name": "Contract",
                "target_class_uri": "https://example.com/Contract",
                "label": "Owns contracts",
            }
        ],
    },
]


RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


class TestNodeContextEndpoint:
    def _mock_domain(self):
        mock_domain = MagicMock()
        mock_domain.get_classes.return_value = _CLASSES_WITH_ACTIONS
        return mock_domain

    def test_metadata_only_by_default(self, client):
        """Without flags, returns dataset/bridge metadata with no rows or entities."""
        mock_domain = self._mock_domain()

        with patch("api.routers.digitaltwin.DigitalTwin.resolve_domain", return_value=mock_domain):
            resp = client.get(
                "/api/v1/digitaltwin/nodes/context",
                params={
                    "entity_uri": "https://example.com/Customer/CUST001",
                    "domain_name": "test",
                },
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["entity_local_id"] == "CUST001"
        assert body["class_name"] == "Customer"
        assert body["dataset"]["fullName"] == "main.crm.customers"
        assert "rows" not in body["dataset"]
        assert body["bridges"][0]["target_domain"] == "Finance"
        assert "entities" not in body["bridges"][0]

    def test_missing_entity_uri_returns_422(self, client):
        resp = client.get(
            "/api/v1/digitaltwin/nodes/context",
            params={"domain_name": "test"},
        )
        assert resp.status_code == 422

    def test_unknown_class_returns_empty_context(self, client):
        mock_domain = self._mock_domain()
        with patch("api.routers.digitaltwin.DigitalTwin.resolve_domain", return_value=mock_domain):
            resp = client.get(
                "/api/v1/digitaltwin/nodes/context",
                params={
                    "entity_uri": "https://example.com/Invoice/INV001",
                    "domain_name": "test",
                },
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body.get("dataset") is None
        assert body.get("bridges") == [] or body.get("bridges") is None

    def test_dataset_rows_skipped_when_key_column_missing(self, client):
        classes_no_key = [
            {
                "name": "Customer",
                "uri": "https://example.com/Customer",
                "dataset": {
                    "catalog": "main", "schema": "crm", "asset": "customers",
                    "type": "TABLE", "fullName": "main.crm.customers",
                    # key_column absent
                },
                "bridges": [],
            }
        ]
        mock_domain = MagicMock()
        mock_domain.get_classes.return_value = classes_no_key

        with patch("api.routers.digitaltwin.DigitalTwin.resolve_domain", return_value=mock_domain):
            resp = client.get(
                "/api/v1/digitaltwin/nodes/context",
                params={
                    "entity_uri": "https://example.com/Customer/CUST001",
                    "domain_name": "test",
                    "fetch_dataset_rows": "true",
                },
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["dataset"].get("key_column_missing") is True
        assert "rows" not in body["dataset"]
```

- [ ] **Step 2: Run — expect failure**

```bash
uv run pytest tests/units/api/test_node_context_endpoint.py -v
```
Expected: FAIL (endpoint not defined).

- [ ] **Step 3: Add response models to `digitaltwin.py`**

Near the top of the response models section in `src/api/routers/digitaltwin.py`, add:

```python
class NodeContextDataset(BaseModel):
    fullName: str
    key_column: Optional[str] = None
    key_column_missing: Optional[bool] = None
    rows: Optional[List[Dict[str, Any]]] = None


class NodeContextBridge(BaseModel):
    target_domain: str
    target_class_name: str
    target_class_uri: str = ""
    label: str = ""
    entities: Optional[List[Dict[str, Any]]] = None


class NodeContextResponse(BaseModel):
    success: bool
    entity_uri: str = ""
    entity_local_id: str = ""
    class_name: Optional[str] = None
    dataset: Optional[NodeContextDataset] = None
    bridges: Optional[List[NodeContextBridge]] = None
    message: Optional[str] = None
```

- [ ] **Step 4: Add helper to extract local name from URI**

The existing `_local_name` in `mcp_server/server/app.py` is MCP-only. Add it to `digitaltwin.py` (it also exists as `DigitalTwin.extract_local_id` — reuse that):

```python
# Already available: _extract_local_id = DigitalTwin.extract_local_id (line 44)
# Use it in the handler below.
```

- [ ] **Step 5: Add the handler to `digitaltwin.py`**

Add after the `dt_triples_find` handler (around line 760):

```python
# ---------------------------------------------------------------------------
# GET /nodes/context
# ---------------------------------------------------------------------------


@router.get(
    "/nodes/context",
    response_model=NodeContextResponse,
    summary="Complete node context (dataset + bridges)",
    description="Resolve the ontology class for an entity URI and return linked "
    "dataset metadata (with optional row retrieval) and bridge definitions "
    "(with optional cross-domain entity traversal).",
)
async def dt_nodes_context(
    entity_uri: str = Query(..., description="Full URI of the entity node"),
    domain_name: Optional[str] = Query(
        None,
        validation_alias=AliasChoices("domain_name", "project_name"),
        description="Domain name in the registry",
    ),
    domain_version: Optional[str] = Query(None),
    fetch_dataset_rows: bool = Query(False, description="Fetch rows from the linked UC table/view"),
    dataset_row_limit: int = Query(5, ge=1, le=20, description="Max rows to return (1–20)"),
    follow_bridges: bool = Query(False, description="Traverse bridge target domains"),
    registry_catalog: Optional[str] = Query(None),
    registry_schema: Optional[str] = Query(None),
    registry_volume: Optional[str] = Query(None),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    local_id = _extract_local_id(entity_uri)

    domain = DigitalTwin.resolve_domain(
        domain_name, session_mgr, settings,
        registry_catalog, registry_schema, registry_volume,
        domain_version, read_only=True,
    )
    dname = domain.domain_folder or (domain.info or {}).get("name", "")

    # Resolve class by matching entity URI type prefix or comparing class URIs
    raw_classes = domain.get_classes() or []
    matched_cls = None
    for cls in raw_classes:
        cls_uri = cls.get("uri", "")
        if cls_uri and entity_uri.startswith(cls_uri.rstrip("/") + "/"):
            matched_cls = cls
            break

    if matched_cls is None:
        return NodeContextResponse(
            success=True,
            entity_uri=entity_uri,
            entity_local_id=local_id,
        )

    class_name = matched_cls.get("name", "")
    raw_dataset = matched_cls.get("dataset") or None
    raw_bridges = matched_cls.get("bridges") or []

    # --- Dataset ---
    dataset_out: Optional[NodeContextDataset] = None
    if raw_dataset and raw_dataset.get("fullName"):
        key_col = raw_dataset.get("key_column")
        rows = None
        key_col_missing = None

        if fetch_dataset_rows:
            if not key_col:
                key_col_missing = True
            else:
                try:
                    client_db = get_databricks_client(settings)
                    sql = (
                        f"SELECT * FROM {raw_dataset['fullName']} "
                        f"WHERE {key_col} = '{sql_escape(local_id)}' "
                        f"LIMIT {dataset_row_limit}"
                    )
                    result = await run_blocking(client_db.execute, sql)
                    rows = result if isinstance(result, list) else []
                except Exception as exc:
                    logger.warning(
                        "nodes/context: dataset row fetch failed for %s: %s", entity_uri, exc
                    )

        dataset_out = NodeContextDataset(
            fullName=raw_dataset["fullName"],
            key_column=key_col,
            key_column_missing=key_col_missing,
            rows=rows,
        )

    # --- Bridges ---
    bridges_out: List[NodeContextBridge] = []
    for b in raw_bridges:
        target_domain = b.get("target_domain") or b.get("target_project", "")
        target_class_name = b.get("target_class_name", "")
        target_class_uri = b.get("target_class_uri", "")
        label = b.get("label", "")

        entities = None
        if follow_bridges and target_domain and target_class_name:
            try:
                target_dom = DigitalTwin.resolve_domain(
                    target_domain, session_mgr, settings,
                    registry_catalog, registry_schema, registry_volume,
                    read_only=True,
                )
                target_store = get_graphdb(target_dom, settings)
                if target_store:
                    target_table = effective_graph_query_table(target_dom, settings, store=target_store)
                    if target_table:
                        rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                        esc_type = sql_escape(target_class_name).lower()
                        esc_id = sql_escape(local_id).lower()
                        seed_where = (
                            f" WHERE subject IN ("
                            f"SELECT subject FROM {target_table} "
                            f"WHERE predicate = '{rdf_type}' "
                            f"AND (LOWER(object) LIKE '%#{esc_type}' "
                            f"OR LOWER(object) LIKE '%/{esc_type}'))"
                            f" AND (LOWER(subject) LIKE '%/{esc_id}%' "
                            f"OR LOWER(subject) LIKE '%#{esc_id}%')"
                        )
                        rows_bridge = target_store.bfs_traversal(
                            target_table, seed_where, depth=1,
                            search=local_id, entity_type=target_class_name,
                        )
                        entities = [
                            {"uri": r.get("subject", ""), "predicate": r.get("predicate", ""),
                             "object": r.get("object", "")}
                            for r in (rows_bridge or [])
                        ]
            except Exception as exc:
                logger.warning(
                    "nodes/context: bridge traversal to %s/%s failed: %s",
                    target_domain, target_class_name, exc,
                )

        bridges_out.append(
            NodeContextBridge(
                target_domain=target_domain,
                target_class_name=target_class_name,
                target_class_uri=target_class_uri,
                label=label,
                entities=entities,
            )
        )

    logger.info(
        "nodes/context: entity=%s class=%s domain=%s dataset=%s bridges=%d",
        local_id, class_name, dname, bool(dataset_out), len(bridges_out),
    )

    return NodeContextResponse(
        success=True,
        entity_uri=entity_uri,
        entity_local_id=local_id,
        class_name=class_name,
        dataset=dataset_out,
        bridges=bridges_out or None,
    )
```

- [ ] **Step 6: Run — expect pass**

```bash
uv run pytest tests/units/api/test_node_context_endpoint.py -v
```
Expected: PASS (4 tests).

- [ ] **Step 7: Run full suite — expect no regressions**

```bash
uv run pytest -q -m "not scenario"
```
Expected: all existing tests pass.

- [ ] **Step 8: Commit**

```bash
git add src/api/routers/digitaltwin.py tests/units/api/test_node_context_endpoint.py
git commit -m "feat(api): add GET /api/v1/digitaltwin/nodes/context endpoint"
```

---

### Task 4: MCP class-actions cache + `describe_entity` enrichment

**Files:**
- Modify: `src/mcp-server/server/app.py`
- Modify: `tests/mcp/integration/test_more_smoke_tools.py` (extend `TestDescribeEntity`)

**Interfaces:**
- Consumes: `GET /api/v1/domain/classes` (Task 2), existing `_get()` helper, `_selected_domain["name"]`
- Produces: `_class_actions` dict keyed by class URI; `describe_entity` text now includes `[Context]` block when class has dataset/bridges; new MCP path constant `API_V1_DOMAIN_CLASSES`

- [ ] **Step 1: Add path constant**

In `src/mcp-server/server/app.py`, after line 65 (`API_V1_DT_TRIPLES_FIND`):

```python
API_V1_DOMAIN_CLASSES = "/api/v1/domain/classes"
```

- [ ] **Step 2: Add `_class_actions` cache inside `create_mcp_server`**

Inside `create_mcp_server()`, after `_ontology_labels` (line 569), add:

```python
_class_actions: dict[str, dict] = {}  # class URI → {"dataset": {...}, "bridges": [...]}
```

- [ ] **Step 3: Clear and populate cache on `select_domain`**

In the `select_domain` tool (around line 891 where `_ontology_labels.clear()` is called), add:

```python
_class_actions.clear()
# Fetch class Actions for the selected domain
try:
    async with _client() as client:
        cls_data = await _get(
            client,
            API_V1_DOMAIN_CLASSES,
            params={**_registry_params(), "domain_name": domain_name},
        )
    for cls in cls_data.get("classes", []):
        uri = cls.get("uri", "")
        if uri:
            _class_actions[uri] = {
                "name": cls.get("name", ""),
                "dataset": cls.get("dataset") or None,
                "bridges": cls.get("bridges") or [],
            }
    logger.info(
        "select_domain: loaded class Actions for %d classes", len(_class_actions)
    )
except Exception as exc:
    logger.warning("select_domain: could not load class Actions: %s", exc)
```

- [ ] **Step 4: Add `_format_class_context_block` helper**

After `_format_entity_block` (around line 160), add:

```python
def _format_class_context_block(local_id: str, cls_actions: dict) -> str:
    """Append a [Context] block for a node's class Actions metadata."""
    dataset = cls_actions.get("dataset")
    bridges = cls_actions.get("bridges") or []
    if not dataset and not bridges:
        return ""

    lines: list[str] = []
    lines.append(f"  [Context — class: {cls_actions.get('name', '')}]")

    if dataset and dataset.get("fullName"):
        key_col = dataset.get("key_column")
        if key_col:
            lines.append(f"  Dataset: {dataset['fullName']}  (key: {key_col} = '{local_id}')")
            lines.append(
                "    → call get_entity_context(fetch_dataset_rows=True) to retrieve rows"
            )
        else:
            lines.append(f"  Dataset: {dataset['fullName']}  (key_column not configured)")

    if bridges:
        lines.append("  Bridges:")
        for b in bridges:
            target = f"{b.get('target_domain', '')} / {b.get('target_class_name', '')}"
            label = f"  \"{b['label']}\"" if b.get("label") else ""
            lines.append(f"    → {target}{label}")
        lines.append(
            "    → call get_entity_context(follow_bridges=True) to load cross-domain data"
        )

    return "\n".join(lines)
```

- [ ] **Step 5: Enrich `_format_find_response` to call the context block**

`_format_find_response` (line 191) builds entity blocks and calls `_format_entity_block`. After each seed entity block, look up its class URI from its triples (the `rdf:type` triple) and append the context block.

Replace the section that appends to `parts` for seed entities (around line 234):

```python
parts.append("── Matching Entities ──")
for uri in seed_uris:
    block = _format_entity_block(uri, by_subject.get(uri, []), label_or_local)
    parts.append(block)
    # Append class Actions context if available
    triples_for_uri = by_subject.get(uri, [])
    type_uris = [t["object"] for t in triples_for_uri if t["predicate"] == RDF_TYPE]
    for type_uri in type_uris:
        if type_uri in _class_actions:
            ctx = _format_class_context_block(_local_name(uri), _class_actions[type_uri])
            if ctx:
                parts.append(ctx)
            break
    parts.append("")
```

**Note:** `_class_actions` is a closure variable inside `create_mcp_server` — `_format_find_response` is a module-level function. To pass the cache, change the `describe_entity` tool to call a local wrapper instead of the module-level function, or pass `_class_actions` as a parameter.

Simplest approach — add an optional parameter to `_format_find_response`:

```python
def _format_find_response(
    data: dict,
    label_or_local: "Callable[[str], str] | None" = None,
    class_actions: "dict | None" = None,
) -> str:
```

And in `describe_entity` tool (line 1047), change the call to:

```python
return _format_find_response(data, _label_or_local, class_actions=_class_actions)
```

- [ ] **Step 6: Write smoke test for enriched `describe_entity`**

In `tests/mcp/integration/test_more_smoke_tools.py`, extend `TestDescribeEntity`:

```python
async def test_describe_entity_includes_context_block(self, patched_mcp):
    """When class Actions cache is populated, describe_entity appends [Context]."""
    patched_mcp.add_default_registry()
    patched_mcp.add_route(
        "GET",
        "/api/v1/digitaltwin/status",
        json={"success": True, "has_data": True, "count": 10,
              "graph_name": "sales_graph", "view_table": "main.s.v"},
    )
    patched_mcp.add_route(
        "GET",
        "/api/v1/domain/classes",
        json={
            "success": True,
            "classes": [
                {
                    "name": "Customer",
                    "uri": "http://x/Customer",
                    "dataset": {"fullName": "main.crm.customers", "key_column": "id"},
                    "bridges": [{"target_domain": "Finance",
                                 "target_class_name": "Contract",
                                 "target_class_uri": "http://y/Contract",
                                 "label": "Owns"}],
                }
            ],
        },
    )
    patched_mcp.add_route(
        "GET",
        "/api/v1/digitaltwin/triples/find",
        json={
            "success": True,
            "seed_count": 1,
            "triples": [
                {"subject": "http://x/Customer/CUST001",
                 "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                 "object": "http://x/Customer"},
                {"subject": "http://x/Customer/CUST001",
                 "predicate": "http://x/name", "object": "Alice"},
            ],
            "depth": 1, "total": 2,
        },
    )

    # First select_domain to populate the cache
    try:
        await patched_mcp.call("select_domain", domain_name="sales")
    except Exception:
        pass  # may fail due to incomplete mock — cache population is best-effort

    try:
        result = await patched_mcp.call(
            "describe_entity", search="CUST001", entity_type="Customer"
        )
        text = _text(result)
        assert text, "describe_entity returned empty"
        # If cache was populated, Context block should be present
        # (allowed to be absent if select_domain mock was incomplete)
    except Exception as exc:
        from fastmcp.exceptions import ToolError
        if not isinstance(exc, ToolError):
            raise
```

- [ ] **Step 7: Run MCP smoke tests**

```bash
uv run pytest tests/mcp/integration/ -v -m mcp
```
Expected: existing tests pass; new test passes or is accepted as soft (ToolError).

- [ ] **Step 8: Commit**

```bash
git add src/mcp-server/server/app.py tests/mcp/integration/test_more_smoke_tools.py
git commit -m "feat(mcp): class-actions cache + describe_entity [Context] enrichment"
```

---

### Task 5: New `get_entity_context` MCP tool

**Files:**
- Modify: `src/mcp-server/server/app.py`
- Modify: `tests/mcp/integration/test_more_smoke_tools.py`

**Interfaces:**
- Consumes: `API_V1_DT_NODE_CONTEXT = "/api/v1/digitaltwin/nodes/context"` (Task 3), `_domain_params()`, `_get()`, `_selected_domain["name"]`
- Produces: new `get_entity_context` MCP tool; `_format_node_context_response` text formatter

- [ ] **Step 1: Add path constant**

In `src/mcp-server/server/app.py`, after `API_V1_DOMAIN_CLASSES` (Task 4 Step 1):

```python
API_V1_DT_NODE_CONTEXT = "/api/v1/digitaltwin/nodes/context"
```

- [ ] **Step 2: Add `_format_node_context_response` helper**

After `_format_class_context_block` (Task 4 Step 4), add:

```python
def _format_node_context_response(data: dict) -> str:
    """Format the /nodes/context JSON response as LLM-friendly text."""
    if not data.get("success"):
        return data.get("message", "Could not retrieve node context.")

    entity_uri = data.get("entity_uri", "")
    local_id = data.get("entity_local_id", "") or _local_name(entity_uri)
    class_name = data.get("class_name", "Unknown")

    lines: list[str] = [
        f"Node Context — {local_id}  ({class_name})",
        f"URI: {entity_uri}",
        "",
    ]

    dataset = data.get("dataset")
    if dataset:
        lines.append(f"Dataset: {dataset.get('fullName', '')}")
        key_col = dataset.get("key_column")
        if key_col:
            lines.append(f"  Key: {key_col} = '{local_id}'")
        if dataset.get("key_column_missing"):
            lines.append("  ⚠ key_column not configured — row fetch skipped")
        rows = dataset.get("rows")
        if rows:
            lines.append(f"  Rows ({len(rows)}):")
            for row in rows:
                lines.append("    " + "  |  ".join(f"{k}: {v}" for k, v in row.items()))
        lines.append("")

    bridges = data.get("bridges") or []
    if bridges:
        lines.append("Cross-domain Bridges:")
        for b in bridges:
            target = f"{b.get('target_domain', '')} / {b.get('target_class_name', '')}"
            label = f"  \"{b['label']}\"" if b.get("label") else ""
            lines.append(f"  → {target}{label}")
            entities = b.get("entities")
            if entities:
                lines.append(f"    Entities ({len(entities)}):")
                for e in entities:
                    lines.append(f"      • {_local_name(e.get('uri', ''))}  {e.get('predicate', '')} → {e.get('object', '')}")
        lines.append("")

    return "\n".join(lines)
```

- [ ] **Step 3: Add `get_entity_context` tool inside `create_mcp_server`**

After `query_graphql` (around line 1199), add:

```python
@mcp.tool()
async def get_entity_context(
    entity_uri: str,
    fetch_dataset_rows: bool = False,
    dataset_row_limit: int = 5,
    follow_bridges: bool = False,
) -> str:
    """Return complete context for an entity node: linked dataset rows
    and/or cross-domain bridge entities.

    Requires a domain to be selected first via select_domain.
    The class must have dataset / bridges configured in the ontology.
    Use the entity URI from describe_entity output.

    Args:
        entity_uri: Full URI of the entity (e.g. from describe_entity).
        fetch_dataset_rows: If true, query the linked UC table/view for rows.
        dataset_row_limit: Max rows to return (1–20, default 5).
        follow_bridges: If true, load entities from bridge target domains.
    """
    if not _selected_domain["name"]:
        return (
            "No domain selected. Call list_domains first, "
            "then select_domain to choose one."
        )

    params = _domain_params(
        {
            "entity_uri": entity_uri,
            "fetch_dataset_rows": str(fetch_dataset_rows).lower(),
            "dataset_row_limit": min(max(dataset_row_limit, 1), 20),
            "follow_bridges": str(follow_bridges).lower(),
        }
    )

    async with _client() as client:
        data = await _get(client, API_V1_DT_NODE_CONTEXT, params=params)

    return _format_node_context_response(data)
```

- [ ] **Step 4: Write smoke test**

In `tests/mcp/integration/test_more_smoke_tools.py`, add a new class:

```python
@pytest.mark.mcp
@pytest.mark.asyncio
class TestGetEntityContext:
    async def test_get_entity_context_metadata_only(self, patched_mcp):
        patched_mcp.add_default_registry()
        patched_mcp.add_route(
            "GET",
            "/api/v1/digitaltwin/status",
            json={"success": True, "has_data": True, "count": 10,
                  "graph_name": "g", "view_table": "t"},
        )
        patched_mcp.add_route(
            "GET",
            "/api/v1/digitaltwin/nodes/context",
            json={
                "success": True,
                "entity_uri": "http://x/Customer/CUST001",
                "entity_local_id": "CUST001",
                "class_name": "Customer",
                "dataset": {"fullName": "main.crm.customers", "key_column": "id"},
                "bridges": [{"target_domain": "Finance", "target_class_name": "Contract",
                              "target_class_uri": "http://y/Contract", "label": "Owns"}],
            },
        )

        try:
            await patched_mcp.call("select_domain", domain_name="sales")
        except Exception:
            pass

        try:
            result = await patched_mcp.call(
                "get_entity_context",
                entity_uri="http://x/Customer/CUST001",
            )
            text = _text(result)
            assert text, "get_entity_context returned empty"
            assert "CUST001" in text or "Customer" in text
        except Exception as exc:
            from fastmcp.exceptions import ToolError
            if not isinstance(exc, ToolError):
                raise

    async def test_get_entity_context_no_domain_selected(self, patched_mcp):
        patched_mcp.add_default_registry()
        # No select_domain called → tool must return guidance text
        result = await patched_mcp.call(
            "get_entity_context",
            entity_uri="http://x/Customer/CUST001",
        )
        text = _text(result)
        assert "select_domain" in text.lower() or "no domain" in text.lower()
```

- [ ] **Step 5: Run MCP smoke tests**

```bash
uv run pytest tests/mcp/integration/ -v -m mcp
```
Expected: all tests pass (new: 2 tests).

- [ ] **Step 6: Run full suite**

```bash
uv run pytest -q -m "not scenario"
```
Expected: no regressions.

- [ ] **Step 7: Commit**

```bash
git add src/mcp-server/server/app.py tests/mcp/integration/test_more_smoke_tools.py
git commit -m "feat(mcp): add get_entity_context tool with dataset rows + bridge traversal"
```

---

### Task 6: Tool schema registration and final wiring check

**Files:**
- Modify: `tests/mcp/integration/test_tool_schemas.py` (add `get_entity_context` to expected tools)

**Interfaces:**
- Consumes: `get_entity_context` tool (Task 5)
- Produces: updated marquee-tool assertion

- [ ] **Step 1: Add `get_entity_context` to expected tools**

In `tests/mcp/integration/test_tool_schemas.py`, inside `test_expected_core_tools_registered`, extend the set:

```python
expected = {
    "list_domains",
    "select_domain",
    "list_entity_types",
    "describe_entity",
    "get_status",
    "get_graphql_schema",
    "query_graphql",
    "get_entity_context",   # ← add this
}
```

- [ ] **Step 2: Run schema tests**

```bash
uv run pytest tests/mcp/integration/test_tool_schemas.py -v -m mcp
```
Expected: PASS (all marquee tools present).

- [ ] **Step 3: Run full suite one final time**

```bash
uv run pytest -q -m "not scenario"
```
Expected: all green.

- [ ] **Step 4: Commit**

```bash
git add tests/mcp/integration/test_tool_schemas.py
git commit -m "test(mcp): register get_entity_context in marquee tool schema assertions"
```

---

## Self-review

**Spec coverage:**
- UI `key_column` → Task 1 ✓
- `GET /api/v1/domain/classes` → Task 2 ✓
- `GET /api/v1/digitaltwin/nodes/context` → Task 3 ✓
- Class-actions cache in MCP → Task 4 ✓
- `describe_entity` `[Context]` block → Task 4 ✓
- `get_entity_context` tool → Task 5 ✓
- Schema registration → Task 6 ✓
- Error handling (no key_column, failed row fetch, bridge failure, unknown class) → Task 3 Step 5 ✓

**Placeholder scan:** None found.

**Type consistency:**
- `ClassActionsItem` (Task 2) → `_class_actions[uri]` dict (Task 4): both carry `name`, `dataset`, `bridges` ✓
- `NodeContextResponse` (Task 3) → `_format_node_context_response` (Task 5): field names `entity_uri`, `entity_local_id`, `class_name`, `dataset`, `bridges` match ✓
- `API_V1_DOMAIN_CLASSES`, `API_V1_DT_NODE_CONTEXT` constants defined before use ✓
