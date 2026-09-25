# Task 6 Report: Verify Version Cards and Safe Deletion

## Scope

- Added mocked Playwright contracts for the Domain version-card workspace.
- Updated the user guide and feature catalogue to describe cards, lifecycle
  actions, guarded deletion, and the separate API/MCP Active selection.
- Verified the implementation landed through commit `48773ce4`; no Task 1–5
  application fix was required.

## Browser Contracts

`tests/e2e/domain/test_domain_versions_cards.py` covers:

- newest-first card order and visible/enabled actions;
- desktop full-height alignment and internal card-list scrolling;
- mobile natural flow and absence of horizontal overflow;
- lifecycle POST payload and deterministic post-action refresh;
- loaded-domain DELETE endpoint and deterministic post-action refresh;
- focus retention when deletion is cancelled; and
- absence of browser console errors in every flow.

The suite uses route fulfillment for all version-list, lifecycle, and delete
responses. It waits on rendered state, intercepted requests, and an instrumented
refresh completion promise; it uses no arbitrary sleeps.

### Viewport Evidence

- Desktop: Chromium at `1600 × 1000`; the workspace and sidebar bottoms were
  within one CSS pixel, the card list had `overflow-y: auto`, and horizontal
  overflow was at most one CSS pixel.
- Mobile: Chromium at `390 × 844`; the section, workspace, card list, and
  sidebar content all had `overflow-y: visible`, and horizontal overflow was at
  most one CSS pixel.
- All five browser flows completed with an empty captured console-error list.

## Documentation

- `docs/user-guide.md` now documents newest-first cards, lifecycle actions,
  load/delete/create/reload behavior, deletion restrictions, and the distinct
  meanings of Loaded, Latest, lifecycle status, and API/MCP Active.
- `docs/features.md` now describes card-based Version Control, inline lifecycle
  controls, guarded administrator deletion, Knowledge Store cleanup, and the
  separate Registry-managed API/MCP Active selection.
- `changelogs/v0.9.0/benoitcayladbx_2026-09-25.log` receives one final
  consolidated section after the earlier per-task entries.

## Verification

### Targeted Browser Suite

Initial command from the brief:

`uv run --frozen pytest -q tests/e2e/domain/test_domain_versions_cards.py`

Result:

`5 skipped, 1 warning in 0.23s`

Diagnosis with `-rs`: the shared e2e fixture requires real Databricks
credentials by default. These tests mock every version-specific backend call,
so the fixture's documented fake-credential mode is the appropriate execution
path.

Executed browser command:

`ONTOBRICKS_E2E_FAKE_CREDS=1 uv run --frozen pytest -q tests/e2e/domain/test_domain_versions_cards.py`

Result:

`5 passed, 1 warning in 7.54s`

After the final import-formatting fix, the same command was rerun:

`5 passed, 1 warning in 5.69s`

### Focused Regression

Command:

`uv run --frozen pytest -q tests/units/registry/test_version_lifecycle.py tests/units/settings/test_settings_version_deletion.py tests/units/settings/test_settings_version_status.py tests/units/domain/test_version_capabilities.py tests/units/api/test_delete_version_endpoints.py tests/units/api/test_set_version_status_endpoint.py tests/units/front/test_domain_versions_cards.py tests/units/front/test_create_version_ungated.py tests/units/front/test_sidebar_content_stretch_contract.py tests/units/auth/test_permission_middleware.py`

Result:

`542 passed, 1 warning in 2.90s`

### Full Non-Scenario Suite

Command:

`uv run --frozen pytest -q -m "not scenario"`

Result:

`6938 passed, 313 skipped, 6 deselected, 1 xfailed, 32 warnings in 54.98s`

The five new e2e tests account for the skip-count increase in the routine suite;
the shared e2e gate intentionally runs them only when explicitly targeted.

### Diagnostics and Diff Checks

- IDE diagnostics were inspected for every application and test file changed
  across Tasks 1–6; no diagnostics were reported.
- `git diff --check` passed before the final report/changelog write and was
  rerun during final verification.

## Self-Review

- Request contracts assert both method and payload/URL, while refresh contracts
  wait for the actual asynchronous reload to finish.
- The delete test uses `/domain/versions/1`, proving the loaded-domain route is
  used instead of the Registry Settings route.
- Mobile assertions cover every container that participates in the responsive
  overflow reset.
- Console errors are captured independently in each browser flow.
- Documentation no longer conflates Draft/In Review/Published lifecycle state,
  Latest-on-disk status, or API/MCP Active selection.
- Task 6 changes are limited to tests, docs, this report, and the changelog.

## Concerns

- The shared browser fixture still checks Databricks credentials before
  starting a local server, even for fully mocked browser contracts. The
  documented `ONTOBRICKS_E2E_FAKE_CREDS=1` mode is therefore required in an
  environment without a valid integration profile.
- Verification retains one existing Starlette `httpx` deprecation warning in
  the targeted and focused runs and 32 existing warnings in the full suite.
# Task 6 Report: Wire `find_subjects_by_type` to entity-search companion

## Starting HEAD SHA
`d449d811f862db0441d4e51cdeb8905a5e4edf3d`

## Commit Created
`c94ee6682ac60c5e5ad7be63afdf172191b2c916` — feat(graph): read find_subjects_by_type from entity-search companion

## What Was Implemented

### `src/back/core/graphdb/GraphDBBackend.py`
- Added `entity_search_uri_search_sql` to the entity_search import block (alphabetical, before `is_asserted_only_relation`).
- Replaced `find_subjects_by_type` body to:
  1. When `entity_search_ready(table_name)` is True, pick the correct search table (`entity_search_asserted_table_id` for asserted-only relations, `entity_search_table_id` otherwise), build SQL via `entity_search_uri_search_sql`, and return `[r["uri"] for r in rows]`.
  2. On any exception matching `is_missing_relation_error`, log and fall through to SPO path.
  3. SPO fallback path is byte-for-byte identical to the pre-existing code (same conditions list construction, same f-string, same `RDF_TYPE`, same LIMIT/OFFSET coercion, same `r["subject"]` extraction).

### `tests/units/graphdb/test_graphdb_adjacency_contract.py`
Added 3 new tests using the existing `FakeStore`:
- `test_find_subjects_by_type_uses_entity_search_when_ready` — companion path produces SQL with `g_entity_search`, `type_uri = 'http://ex/Customer'`, `LIMIT 10 OFFSET 5`.
- `test_find_subjects_by_type_falls_back_when_entity_search_not_ready` — SPO path produces SQL with `predicate = '`, no `g_entity_search`.
- `test_find_subjects_by_type_falls_back_on_missing_table_error` — first call raises `TABLE_OR_VIEW_NOT_FOUND`, second call (SPO) succeeds; returns `["http://ex/1"]`.

### `tests/units/core/test_lakebase_flat_store.py`
- Updated `test_find_subjects_by_type_delegates` to also patch `table_exists` returning `False`, since `entity_search_ready` now calls `table_exists` before the query (previously this would try to connect to Lakebase in the test environment).
- Added `test_find_subjects_by_type_uses_entity_search_when_ready` — patches `table_exists=True` and `execute_query=[{"uri": "http://ex/1"}]`, asserts `g_v1_entity_search` appears in the SQL.

## TDD Evidence

### RED (before implementation)
```
uv run --frozen pytest tests/units/graphdb/test_graphdb_adjacency_contract.py -k find_subjects_by_type -v

FAILED test_find_subjects_by_type_uses_entity_search_when_ready - assert 'g_entity_search' in "SELECT DISTINCT subject FROM g WHERE predicate = ..."
FAILED test_find_subjects_by_type_falls_back_on_missing_table_error - RuntimeError: TABLE_OR_VIEW_NOT_FOUND
2 failed, 1 passed
```

### GREEN (after implementation)
```
uv run --frozen pytest tests/units/graphdb/test_graphdb_adjacency_contract.py -v
13 passed, 1 warning in 0.94s

uv run --frozen pytest tests/units/core/test_lakebase_flat_store.py -k "find_subjects" -v
6 passed, 49 deselected, 1 warning in 1.46s
```

### Full Suite
```
uv run --frozen pytest -q -m "not scenario"
6872 passed, 308 skipped, 6 deselected, 1 xfailed, 32 warnings in 56.71s
```

## Files Changed
- `src/back/core/graphdb/GraphDBBackend.py` — import + method replacement
- `tests/units/graphdb/test_graphdb_adjacency_contract.py` — +3 new tests
- `tests/units/core/test_lakebase_flat_store.py` — updated delegate test + new companion test

## Self-Review Findings

1. **Import discipline (YAGNI)**: `entity_search_uri_search_sql` added, `entity_search_seed_sql` not added (per brief — Task 8's job). The 5-symbol import block is exactly what the brief specifies.
2. **SPO fallback identity**: The fallback SQL is character-for-character identical to the pre-existing body — same `conditions` list pattern, same f-strings for the subselect, same `ORDER BY subject`, same `LIMIT {int(limit)} OFFSET {int(offset)}`, same `r["subject"]` key. Verified by diffing mentally and by the `_falls_back_when_entity_search_not_ready` test asserting `predicate = '` is present and `g_entity_search` is absent.
3. **Exception handling**: `except Exception as exc` with `# noqa: BLE001` matches the pattern used in `find_preview_seeds` — mirrors existing style exactly.
4. **Lakebase delegate test fix**: The original test patched only `execute_query`. After the change, `entity_search_ready` runs first and calls `table_exists` which would attempt a real connection. Patching `table_exists` to `False` is the correct minimal fix — it doesn't change what the test is asserting (SPO fallback produces `{"subject": ...}` rows).
5. **No concerns.**
