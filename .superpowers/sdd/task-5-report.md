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
- Renderer injection resistance: malicious quote/markup payloads remain exact
  DOM property values and cannot create `img`, `script`, or `svg` elements in
  the delete control. Confirmation-message safety is covered separately below.
- Focus behavior: blocked wrappers have `tabIndex = 0`, blocked buttons have
  `tabIndex = -1`, and enabled wrappers stay at `-1` while enabled buttons have
  `tabIndex = 0`.
- Scope: no application files outside the Task 5 Registry JavaScript and
  shared frontend contract test were changed.

## Blocker Fix: Escape Enabled-Delete Confirmation Values

Follow-up review found that the initial implementation safely rendered the
delete control but passed its untrusted dataset values into
`showConfirmDialog` without escaping. Because that shared dialog intentionally
accepts formatted HTML, an enabled Registry delete action could inject stored
markup into the confirmation modal.

The fix:

- Escapes `domainName` and `version` for the dialog's HTML text-node context
  while preserving the confirmation's normal formatting.
- Exercises the enabled delete control through the real
  `deleteRegistryVersion` confirmation call path with malicious tag payloads.
- Replaces the source-only HTTP 409 assertion with executable behavior coverage
  for the DELETE request, server notification, forced refresh, and absence of
  bridge invalidation.

### Follow-up RED

`uv run --frozen pytest -q tests/units/front/test_domain_versions_cards.py`

`2 failed, 10 passed, 1 warning in 0.77s`

Both failures showed that the deletion function was not executable in the test
harness; this prevented behavioral verification of both confirmation escaping
and conflict refresh.

### Follow-up GREEN

`uv run --frozen pytest -q tests/units/front/test_domain_versions_cards.py`

`12 passed, 1 warning in 0.67s`

### Follow-up Registry Regression

The focused contract plus relevant Registry frontend tests passed together:

`38 passed, 1 warning in 0.74s`

`node --check src/front/static/registry/js/registry.js` and
`git diff --check` also passed.

### Follow-up Full Suite

`uv run --frozen pytest -q -m "not scenario"`

`6938 passed, 308 skipped, 6 deselected, 1 xfailed, 32 warnings in 51.98s`

## Concerns

The initial self-review overstated end-to-end injection resistance by covering
only renderer output and missing the HTML-accepting confirmation boundary. That
gap is now covered behaviorally. The broader Registry renderer still contains
older string-built attributes for non-delete controls; this fix does not expand
that pre-existing surface.
