# Task 5 Report: Harden Registry Version Deletion

## What Was Implemented

- Registry version deletion visibility now comes from
  `delete_control_visible`, and enabled state comes from `can_delete`.
- Blocked controls expose the server's `delete_block_reason` on one
  keyboard-focusable wrapper. Enabled controls leave the wrapper out of the
  tab order so the button is the only tab stop.
- Domain names, version values, and block reasons are assigned through DOM
  properties (`dataset`, `title`, and `textContent`-free icon construction);
  none are interpolated into the delete control's HTML attributes.
- The existing guarded
  `DELETE /settings/registry/domains/{domain}/versions/{version}` call remains.
- HTTP 409 responses show the server message and force
  `loadRegistryDomains(true)` to refresh server capabilities.

## Files Changed

- `src/front/static/registry/js/registry.js`
  - Added the DOM-based delete-control renderer.
  - Replaced loaded-version-derived visibility with server capabilities.
  - Added the 409 notification and forced capability refresh.
- `tests/units/front/test_domain_versions_cards.py`
  - Added executable renderer contracts for capability use, untrusted values,
    and keyboard behavior.
  - Added a source contract for the guarded endpoint's 409 refresh path.
- `changelogs/v0.9.0/benoitcayladbx_2026-09-25.log`
  - Appended the mandatory English changelog entry.
- `.superpowers/sdd/task-5-report.md`
  - Recorded TDD, verification, and self-review evidence.

## TDD Evidence

### RED

Command:

`uv run --frozen pytest -q tests/units/front/test_domain_versions_cards.py`

Result:

`3 failed, 7 passed, 1 warning in 0.69s`

The failures were expected: Registry did not reference the three server
capability fields, the DOM renderer did not exist, and the 409 refresh contract
was absent.

### GREEN

Command:

`uv run --frozen pytest -q tests/units/front/test_domain_versions_cards.py`

Result:

`10 passed, 1 warning in 0.52s`

### Existing Registry Frontend Tests

Command:

`uv run --frozen pytest -q tests/units/front/test_registry_backend_column.py tests/units/front/test_registry_modal_loading.py tests/units/front/test_registry_operational_status.py tests/units/front/test_registry_obx_import_rename.py tests/units/front/test_registry_stacked_obx_modals.py tests/units/front/test_backend_brand_icons.py`

Result:

`26 passed, 1 warning in 0.28s`

### JavaScript and Diff Checks

- `node --check src/front/static/registry/js/registry.js` — passed.
- `git diff --check` — passed.
- IDE lint diagnostics for both edited source/test files — none.

### Full Non-Scenario Suite

Command:

`uv run --frozen pytest -q -m "not scenario"`

Result:

`6936 passed, 308 skipped, 6 deselected, 1 xfailed, 32 warnings in 53.04s`

## Self-Review

- Server authority: delete rendering no longer depends on `isLoaded`; all
  visibility, enabled state, and blocked reason decisions use response fields.
- Endpoint guard: the Settings DELETE URL and method are unchanged.
- Conflict handling: status 409 displays `data.message` when provided and
  forces a domain reload even though deletion failed.
- Injection resistance: malicious quote/markup payloads remain exact DOM
  property values and cannot create `img`, `script`, or `svg` elements.
- Focus behavior: blocked wrappers have `tabIndex = 0`, blocked buttons have
  `tabIndex = -1`, and enabled wrappers stay at `-1` while enabled buttons have
  `tabIndex = 0`.
- Scope: no application files outside the Task 5 Registry JavaScript and
  shared frontend contract test were changed.

## Concerns

No Task 5 blocker found. The broader Registry renderer still contains older
string-built attributes for non-delete controls; this task does not expand that
pre-existing surface, and the new delete control does not reuse it.
