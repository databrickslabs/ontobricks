# Task 6 Report: Verify Version Cards and Safe Deletion

## Scope

- Added mocked Playwright contracts for the Domain version-card workspace.
- Updated the user guide and feature catalogue to describe cards, lifecycle
  actions, guarded deletion, and the separate API/MCP Active selection.
- Verified the implementation landed through commit `48773ce4`, then corrected
  Domain delete-cancellation focus after review exposed a real modal defect.

## Browser Contracts

`tests/e2e/domain/test_domain_versions_cards.py` covers:

- newest-first card order and visible/enabled actions;
- desktop full-height alignment and demonstrated internal card-list scrolling;
- mobile natural flow and absence of horizontal overflow;
- lifecycle POST payload and `?refresh=true` post-action refresh;
- loaded-domain DELETE endpoint and `?refresh=true` post-action refresh;
- focus restoration after cancelling through the real confirmation modal; and
- absence of browser console errors in every flow.

The suite uses route fulfillment for all version-list, lifecycle, and delete
responses. It waits on rendered state, exact forced-refresh requests, and an
instrumented refresh completion promise; it uses no arbitrary sleeps.

### Viewport Evidence

- Desktop: Chromium at `1600 × 1000`; the workspace and sidebar bottoms were
  within one CSS pixel, the card list had `overflow-y: auto`, its scroll height
  exceeded its client height, and horizontal overflow was at most one CSS pixel.
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

After the review fixes, including the real-modal cancellation flow, the final
rerun produced:

`5 passed, 1 warning in 6.88s`

### Focused Regression

Command:

`uv run --frozen pytest -q tests/units/registry/test_version_lifecycle.py tests/units/settings/test_settings_version_deletion.py tests/units/settings/test_settings_version_status.py tests/units/domain/test_version_capabilities.py tests/units/api/test_delete_version_endpoints.py tests/units/api/test_set_version_status_endpoint.py tests/units/front/test_domain_versions_cards.py tests/units/front/test_create_version_ungated.py tests/units/front/test_sidebar_content_stretch_contract.py tests/units/auth/test_permission_middleware.py`

Result:

`543 passed, 1 warning in 2.76s`

### Full Non-Scenario Suite

Command:

`uv run --frozen pytest -q -m "not scenario"`

Result:

`6939 passed, 313 skipped, 6 deselected, 1 xfailed, 32 warnings in 52.93s`

The five new e2e tests account for the skip-count increase in the routine suite;
the shared e2e gate intentionally runs them only when explicitly targeted.

### Diagnostics and Diff Checks

- IDE diagnostics were inspected for every application and test file changed
  across Tasks 1–6; no diagnostics were reported.
- `git diff --check` passed before the final report/changelog write and was
  rerun during final verification.

## Self-Review

- Request contracts assert both method and payload/URL, while refresh contracts
  require `?refresh=true` and wait for the asynchronous reload to finish.
- The delete test uses `/domain/versions/11`, proving the loaded-domain route is
  used instead of the Registry Settings route.
- Twelve newest-first cards prove desktop scrolling through
  `scrollHeight > clientHeight`.
- Cancellation uses the real Bootstrap modal and verifies focus returns to the
  connected, enabled Delete trigger after modal dismissal.
- Mobile assertions cover every container that participates in the responsive
  overflow reset.
- Console errors are captured independently in each browser flow.
- Documentation no longer conflates Draft/In Review/Published lifecycle state,
  Latest-on-disk status, or API/MCP Active selection.
- The review fix is limited to Domain version JavaScript, its unit/browser
  contracts, documentation, this report, and the changelog.

## Concerns

- The shared browser fixture still checks Databricks credentials before
  starting a local server, even for fully mocked browser contracts. The
  documented `ONTOBRICKS_E2E_FAKE_CREDS=1` mode is therefore required in an
  environment without a valid integration profile.
- Verification retains one existing Starlette `httpx` deprecation warning in
  the targeted and focused runs and 32 existing warnings in the full suite.

## Review Fix Evidence

### RED

- Unit command:
  `uv run --frozen pytest -q tests/units/front/test_domain_versions_cards.py`
  → `1 failed, 12 passed, 1 warning in 0.79s`; the trigger received zero focus
  calls after cancellation.
- Browser command:
  `ONTOBRICKS_E2E_FAKE_CREDS=1 uv run --frozen pytest -q tests/e2e/domain/test_domain_versions_cards.py`
  → `1 failed, 4 passed, 1 warning in 12.46s`; after cancelling the real modal,
  Playwright reported the Delete button as inactive.

### GREEN

- Domain frontend unit contracts:
  `13 passed, 1 warning in 0.66s`.
- Mocked browser contracts:
  `5 passed, 1 warning in 6.88s`.
- Focused regression command:
  `543 passed, 1 warning in 2.76s`.
- Full non-scenario suite:
  `6939 passed, 313 skipped, 6 deselected, 1 xfailed, 32 warnings in 52.93s`.
- `node --check src/front/static/domain/js/domain-versions.js`, Ruff, IDE
  diagnostics, and final diff checks passed.
