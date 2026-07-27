# Graph Explorer Dataset Preview Box — Design

## Goal

When an entity selected in Graph Explorer Details has a class-linked external
dataset, show a Dataset section styled like Dashboard, with metadata and a
**Preview rows** action that opens a modal showing up to 10 matching UC rows.

## Surfaces

- Sigma Graph Explorer Details (`query-sigmagraph.js`) — add Dataset section
  after Dashboard when `entityMapping.dataset` / `classInfo.dataset` has
  `fullName` or `asset`.
- Classic entity Details (`query-entity-details.js`) — upgrade the existing
  Dataset section to the same box + Preview button pattern.

## UI

### Details section

Collapsible / standard `entity-detail-section` (or `_sec` in Sigma) titled
**Dataset** with icon `bi-table`, containing:

- Table / view full name
- Key column (when configured)
- Description (when non-empty; label **Description**, not Purpose)
- Full-width outline button **Preview rows** (`btn-outline-info`, same weight
  as View Dashboard)

If `key_column` is missing, still show the section and metadata; disable the
Preview button with a title explaining that a key column is required.

### Preview modal

Mirror `openDashboardModal` lifecycle (create → show → destroy on hide):

1. Open immediately with a loading spinner and title
   `{EntityType} Dataset` + optional entity-id badge.
2. `GET /api/v1/digitaltwin/nodes/context` with:
   - `entity_uri` = selected entity URI
   - `fetch_dataset_rows=true`
   - `dataset_row_limit=10`
   - omit `domain_name` (session domain)
3. On success, render:
   - Dataset full name, key, description (from response)
   - Scrollable HTML table of `dataset.rows` (column headers = union of keys
     in returned row objects; empty cell for missing keys)
   - Footer note: “Showing up to 10 rows”
4. Error / edge states (inline in modal body, no toast-only failure):
   - Network / non-OK HTTP
   - `success: false` → `message`
   - `key_column_missing` → clear explanation
   - Zero rows → “No matching rows for this entity”

## Data / API

Reuse existing `NodeContextDataset` — no backend changes required.
`dataset_row_limit` already allows 1–20; UI always requests 10.

## Testing

The addon is covered across its three boundaries:

- **API behavior:** class resolution, dataset metadata and description,
  exact key-filter SQL with `LIMIT 10`, returned rows, empty rows, missing
  key columns, and Databricks query failures.
- **MCP integration:** invoke real FastMCP tools against an
  `httpx.MockTransport`; verify `select_domain` caches the complete dataset,
  `list_entity_types` and `describe_entity` expose its description, and
  `get_entity_context` forwards row-fetch parameters and formats rows.
- **Graph Explorer contracts:** both Details surfaces and the node
  right-click menu invoke the shared modal; the request remains capped at 10;
  loading, missing-key, empty-result, and query-error states remain present.

All tests run without a live Databricks workspace. Live UC validation remains
opt-in because it depends on workspace credentials and mutable external data.

## Out of scope

- Editing dataset from Graph Explorer
- Pagination beyond 10
- Opening UC in Databricks UI
- Bridge row fetch
