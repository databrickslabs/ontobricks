# Build Page Inline Retrieval Spinners

## Goal

Give users visible progress feedback while Lakebase-specific artefact checks
continue after the Build page's initial loading overlay is dismissed.

## Scope

The existing initial Build page overlay remains unchanged. While artefact
retrieval is pending (`dt.pending` / live `/dtwin/sync/dt-existence`), compact
inline Bootstrap spinners appear on the Lakebase architecture cards for:

- Triple Store existence badge (`#dtExistView`);
- Sync existence badge + name (`#dtLakebaseSyncedUcExists`, `#dtLakebaseSyncedUc`);
- Graph DB existence badge + name (`#dtLakebaseTableExists`, `#dtLakebaseFullName`).

No API contract break. `pending_dt_existence` may include cheaply resolved
Lakebase artefact names (no network probes) so cards can show names as soon as
the overlay drops while existence badges keep spinning until the live probe
returns.

## Behaviour

1. When the Build page applies a pending existence skeleton, badges show a
   spinner (never "Unable to check"), and missing names show a compact spinner.
2. Before starting the live existence request, the same loading markup is
   applied so a refresh never flashes a stale "Unable to check" state.
3. As response data is applied, each spinner is replaced by the existing
   success / not-found / error badge or the resolved name.
4. If the request fails, loading states are cleared and the existing
   unavailable/error presentation is shown. A spinner must never remain stuck.
5. Non-Lakebase Build variants are unchanged.
6. Build execution and task-resume progress indicators are unchanged.

## Implementation

Keep the change local to the Build page (+ cheap name fill on the pending
skeleton):

- `src/front/templates/partials/dtwin/_query_sync.html` — stable card ids
  (already present).
- `src/front/static/query/js/query-sync.js` — spinner helpers; pending /
  `_loadDtExistence` loading states; stop treating `null` existence as
  "Unable to check" while pending.
- `src/back/objects/digitaltwin/DigitalTwin.py` — enrich
  `pending_dt_existence` with config-resolved Lakebase names when available.
- Existing Bootstrap spinner and badge styles are reused.

## Error Handling

The loading state is cleared when apply helpers render final content, and also
in a `finally` path on `_loadDtExistence` failure. Existing request error
handling remains authoritative for the final badge content.

## Testing

Add focused UI/static tests that verify:

- spinner helpers exist and are used by `_loadDtExistence` / pending apply paths;
- pending existence does not paint "Unable to check" for unresolved flags;
- successful retrieval replaces loading feedback with resolved statuses;
- failed retrieval clears both spinners and exposes the error/unavailable state;
- `pending_dt_existence` returns `pending: True` and optional Lakebase names
  without requiring live probes.

Run the focused tests first, then the repository's required
`uv run pytest -q -m "not scenario"` suite.
