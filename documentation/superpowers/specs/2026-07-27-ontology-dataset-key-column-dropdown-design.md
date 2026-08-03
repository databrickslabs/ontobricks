# Ontology Dataset Key Column Dropdown

## Goal

When an ontology entity is linked to an external Unity Catalog table/view,
replace the free-text **Key column** field with a dropdown listing all columns
of that asset, so users pick a valid ID column instead of typing it.

## Scope

In scope:

- Entity panel dataset UI in
  `src/front/static/ontology/js/ontology-shared-panels.js`
  (`renderSharedEntityDataset`, `onDatasetKeyColumnChange`,
  `_datasetSelectAsset`).
- Client-side fetch of column names via the existing
  `POST /mapping/table-columns` endpoint.
- Edit mode only (view mode continues to show the saved key as read-only text).

Out of scope:

- New backend endpoints or changes to UC column APIs.
- Persisting column lists on the ontology class / dataset blob.
- Filtering columns to “ID-like” names (all columns are listed).
- Changing how `key_column` is consumed at query/build time.

## Behaviour

1. After a dataset is assigned (or when an entity with an existing dataset is
   opened for edit), fetch columns for `catalog` / `schema` / `asset`.
2. Render a `<select>` for Key column instead of the text `<input>`:
   - Placeholder option: “Select a key column…” (value empty → `key_column`
     remains `null`).
   - One option per column name from the API response.
   - Pre-select the current `sharedPanelDataset.key_column` when it matches an
     option.
3. If a previously saved `key_column` is not in the fetched list (renamed /
   removed column), still show it as a selected option and mark it as missing
   (e.g. option label `old_col (missing)`), so the user can see and replace it.
4. Changing the select updates `sharedPanelDataset.key_column` and marks the
   panel dirty (same as today’s text input).
5. Cache fetched columns in memory keyed by
   `catalog.schema.asset` for the lifetime of the page session, so reopening
   the same entity does not re-hit UC unnecessarily. Only non-empty column
   lists are cached; an HTTP-successful empty `columns` payload must never be
   written to the cache (the backend can degrade UC errors to an empty
   successful response, so caching `[]` would poison retries).
6. Loading / empty / error states:
   - While fetching: disabled select with “Loading columns…”.
   - Empty column list: disabled select with “No columns found” plus a Retry
     control that re-fetches (bypassing cache). Retry is required here because
     an empty list may represent a degraded UC failure, not a truly empty
     table.
   - Fetch failure: disabled select with “Failed to load columns” plus a
     Retry control that re-fetches (bypassing cache).
7. Selecting a different dataset clears `key_column` and loads columns for the
   new asset (existing `_datasetSelectAsset` behaviour for clearing the key is
   kept).

## Data & API

- Reuse `POST /mapping/table-columns` with body
  `{ catalog, schema, table }` where `table` is the dataset `asset` name.
- Response shape: `{ columns: [{ name, type, comment }, ...] }`.
- Only `name` is used for the dropdown; `type` may optionally appear in the
  option label as muted secondary text if it stays readable (e.g.
  `customer_id (STRING)`). Prefer name-only if labels get noisy.
- Persist only `key_column` on the dataset object (unchanged schema).

## Implementation Notes

Keep the change local to the ontology shared panels JS:

- Replace the key-column text input markup in `renderSharedEntityDataset`.
- Add helpers: fetch columns (with cache), populate select, retry on error.
- Wire `onchange` to the existing `onDatasetKeyColumnChange` (or equivalent).
- No template HTML change required unless a stable container id is useful for
  tests; current markup is generated in JS.

## Error Handling

- Network / API errors never clear an already saved `key_column`.
- Failed fetch leaves the select disabled and offers Retry.
- Cache is not written on failure; Retry always hits the network.
- An HTTP-successful response with an empty `columns` array is treated as a
  recoverable degraded state (not a definitive empty table): do not cache it,
  leave the select disabled with “No columns found”, and offer Retry so the
  user can re-fetch after a transient UC failure.

## Testing

- Unit / static coverage for: dropdown rendered when dataset present; options
  built from column payload; saved key pre-selected; missing saved key still
  listed; dirty flag on change; cache hit avoids second fetch; retry after
  failure.
- Run `uv run pytest -q -m "not scenario"` after implementation.
