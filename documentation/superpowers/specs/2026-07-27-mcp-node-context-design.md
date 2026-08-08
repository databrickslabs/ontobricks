# MCP Node Context (Bridges + Dataset) — Design Spec

**Date:** 2026-07-27  
**Version target:** v0.7.0  
**Status:** Draft — awaiting user review

---

## 1. Goal

Enable MCP tools to return a **complete context** for any matched node: triple-store data (already works) **plus** per-class ontology Actions — specifically `bridges` (cross-domain links) and `dataset` (linked Unity Catalog table/view with row retrieval). Both are returned only when filled in the ontology.

---

## 2. Scope

In scope:
- `dataset` and `bridges` surfaced to MCP clients
- Metadata-only by default; optional row retrieval and bridge traversal via parameters
- Explicit `key_column` per class for dataset row lookup
- Enriched `describe_entity` (metadata summary per matched node)
- New `get_entity_context` MCP tool (full optional retrieval)
- New `GET /api/v1/digitaltwin/nodes/context` REST endpoint (server-side orchestration)
- `key_column` field added to the Dataset panel in the Ontology UI

Out of scope:
- `dashboard` / `dashboardParams` (excluded by user)
- Bridge traversal depth > 1
- Bulk/batch entity context

---

## 3. Data model changes

### 3.1 Class `dataset` field — add `key_column`

Current shape stored in `ontology.classes[].dataset`:

```json
{
  "catalog": "main",
  "schema": "crm",
  "asset": "customers",
  "type": "TABLE",
  "fullName": "main.crm.customers"
}
```

New shape (backward-compatible — `key_column` defaults to `null`):

```json
{
  "catalog": "main",
  "schema": "crm",
  "asset": "customers",
  "type": "TABLE",
  "fullName": "main.crm.customers",
  "key_column": "id"
}
```

`key_column` is the column in the linked table whose value equals the node's **local name** (the last segment of its URI, e.g. `CUST00094`).

### 3.2 Bridges — no schema change

Bridges are already stored per class as:
```json
[
  {
    "target_domain": "FinancialDomain",
    "target_class_name": "Contract",
    "target_class_uri": "https://…/Contract",
    "label": "Owns contracts"
  }
]
```
No change needed.

---

## 4. Architecture

```
LLM client
    │
    ▼
MCP tool: describe_entity          MCP tool: get_entity_context
    │   (enriches output with          │   (full optional retrieval)
    │    class Actions metadata)        │
    └──────────────┬────────────────────┘
                   │ HTTP GET
                   ▼
  GET /api/v1/digitaltwin/nodes/context     ← new endpoint
           │
    ┌──────┴───────────────────────────────┐
    │                                      │
    ▼                                      ▼
Resolve entity rdf:type              Optionally:
→ load class from published          - fetch_dataset_rows=true
  ontology (DigitalTwin.resolve_     →  SELECT * FROM <fullName>
  domain + domain.get_classes())        WHERE <key_column> = '<local_id>'
→ filter classes with                   LIMIT <dataset_row_limit>
  non-empty dataset / bridges        - follow_bridges=true
                                     → for each bridge: call
                                       GET /api/v1/digitaltwin/triples/find
                                       on target domain, entity_type=target_class
                                       search=<local_id>
```

---

## 5. New REST endpoint

```
GET /api/v1/digitaltwin/nodes/context
```

### Query parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `entity_uri` | string | required | Full URI of the entity node |
| `domain_name` | string | required | Domain that owns the entity |
| `fetch_dataset_rows` | bool | `false` | If true and class has dataset + key_column, run SQL row fetch |
| `dataset_row_limit` | int | `5` | Max rows to return (capped at 20) |
| `follow_bridges` | bool | `false` | If true, query each bridge's target domain for the same local ID |
| `bridge_depth` | int | `1` | Max hops (capped at 1 for v0.7.0) |
| `registry_catalog` | string | optional | Registry override |
| `registry_schema` | string | optional | Registry override |
| `registry_volume` | string | optional | Registry override |

### Response shape

```json
{
  "success": true,
  "entity_uri": "https://…/Customer/CUST00094",
  "entity_local_id": "CUST00094",
  "class_name": "Customer",
  "dataset": {
    "fullName": "main.crm.customers",
    "key_column": "id",
    "rows": [
      {"id": "CUST00094", "name": "Jacob Martinez", "segment": "Enterprise", "mrr": 4200}
    ]
  },
  "bridges": [
    {
      "target_domain": "FinancialDomain",
      "target_class_name": "Contract",
      "label": "Owns contracts",
      "entities": [
        {
          "uri": "https://…/Contract/CTR-2024-0089",
          "label": "CTR-2024-0089",
          "attributes": {"startDate": "2024-01-15", "value": "85000", "status": "Active"}
        }
      ]
    }
  ]
}
```

When `fetch_dataset_rows=false`, `dataset.rows` is omitted (just `fullName` + `key_column`).  
When `follow_bridges=false`, `bridges[].entities` is omitted (just target metadata).  
When the class has no dataset or no bridges, those keys are omitted entirely.

---

## 6. Ontology UI change

**File:** `src/front/static/ontology/js/ontology-shared-panels.js`  
**Section:** Unity Catalog Dataset panel (Actions tab)

Add a text input "Key column" below the asset selector. It maps to `sharedPanelDataset.key_column`. Shown only when a dataset is already selected. Saved as part of the class Actions payload.

---

## 7. MCP changes

### 7.1 `describe_entity` enrichment

After formatting each matched entity, resolve its `rdf:type` against the cached class list (loaded from `/api/v1/domain/design-status` or a new `/api/v1/domain/classes` lightweight endpoint). If the matched class has non-empty `dataset` or `bridges`, append a `[Context]` block:

```
[Context — class: Customer]
Dataset:
  Table: main.crm.customers  (key: id = 'CUST00094')
  → call get_entity_context(fetch_dataset_rows=true) to retrieve rows
Bridges:
  → FinancialDomain / Contract  ("Owns contracts")
  → call get_entity_context(follow_bridges=true) to load cross-domain data
```

Class metadata is fetched once per `select_domain` call and cached for the session.

### 7.2 New REST path constant

```python
API_V1_DT_NODE_CONTEXT = "/api/v1/digitaltwin/nodes/context"
```

### 7.3 New `get_entity_context` tool

```python
@mcp.tool()
async def get_entity_context(
    entity_uri: str,
    fetch_dataset_rows: bool = False,
    dataset_row_limit: int = 5,
    follow_bridges: bool = False,
) -> str:
    """Return complete context for an entity node: linked dataset rows
    and/or cross-domain bridge entities, depending on parameters.

    Requires a domain to be selected first via select_domain.
    The class must have dataset / bridges configured in the ontology.

    Args:
        entity_uri: Full URI of the entity (from describe_entity output).
        fetch_dataset_rows: If true, query the linked UC table/view.
        dataset_row_limit: Max rows to return (1–20, default 5).
        follow_bridges: If true, load entities from bridge target domains.
    """
```

Returns formatted text (same style as `describe_entity`).

### 7.4 Class metadata cache in `select_domain`

When `select_domain` is called, fetch `/api/v1/domain/classes` (or reuse design-status classes list) and populate a module-level `_class_actions` dict:

```python
_class_actions: dict[str, dict] = {}
# keyed by class URI → {"dataset": {...}, "bridges": [...]}
```

Cleared on each `select_domain` call, same as `_ontology_labels`.

---

## 8. New lightweight REST endpoint for class Actions

To avoid loading the full OWL on every MCP `select_domain`, add:

```
GET /api/v1/domain/classes
```

Returns only class name, URI, `dataset`, `bridges` (no dataProperties, no constraints). This is a read-only, cheap call.

---

## 9. Error handling

| Situation | Behaviour |
|---|---|
| Class has `dataset` but no `key_column` | Return dataset metadata, skip row fetch, note in response: `key_column not configured` |
| SQL row fetch fails | Return partial context with error message, do not raise |
| Bridge target domain is not accessible | Skip that bridge, log warning, note in response |
| Entity URI resolves to unknown class | Skip `[Context]` block silently |

---

## 10. Testing

- Unit: new endpoint handler with mocked `DigitalTwin.resolve_domain` + `domain.get_classes()`
- Unit: `_format_entity_context_block` text formatter
- Unit: `get_entity_context` MCP tool with mocked `_get` responses
- MCP smoke: `get_entity_context` returns non-empty string
- MCP smoke: `describe_entity` with class-Actions cache populated includes `[Context]` block
- Integration (opt-in scenario): full round-trip against live registry with a domain that has dataset + bridges configured

---

## 11. Files to create or modify

| File | Change |
|---|---|
| `src/api/routers/digitaltwin.py` | Add `GET /nodes/context` handler |
| `src/api/routers/domains.py` | Add `GET /domain/classes` handler |
| `src/api/external_app.py` | Register new routes |
| `src/mcp-server/server/app.py` | Add `get_entity_context` tool; enrich `describe_entity`; class-actions cache; new path constant |
| `src/front/static/ontology/js/ontology-shared-panels.js` | Add `key_column` input to Dataset panel |
| `tests/mcp/integration/test_more_smoke_tools.py` | Smoke tests for new tool |
| `tests/units/api/test_external_api.py` | Unit tests for new endpoints |
