# Final-fix report — Ontology Studio multi-select (final review)

Commit: `e2fd6980c5287bd20403da3ae3da1c1776d29e58`
Scope: findings from `.superpowers/sdd/review-7b620707..cf86202c.diff`, spec
`docs/superpowers/specs/2026-10-01-ontology-studio-multi-select-design.md`.

> Note: this path previously held a tracked report from an older task
> ("Domain version review blockers"). It was overwritten in the working tree
> only (not committed); the old content is still in `git show HEAD:.superpowers/sdd/final-fix-report.md`.

## Files in the fix commit

- `src/front/static/ontology/js/ontology-map.js`
- `tests/units/front/test_ontology_map_multi_select.py`
- `tests/e2e/ontology/test_studio_multi_select_flows.py` (new)
- `docs/architecture.md` (was clean; new "Ontology Studio multi-select" section)

## Fixes

| # | Finding | Fix |
|---|---------|-----|
| 1 | Browser `click` after Cmd/Ctrl marquee `pointerup` cleared the selection | `_suppressNextMapCanvasClick()` armed in marquee `onUp` (also for empty/cancelled marquees); canvas `click` calls `_consumeMapCanvasClickSuppression()` first. One-shot, 400 ms deadline, reset by the next `pointerdown` (macOS Control-click produces no trailing click) |
| 2 | Multi menu kept stale names / stayed open | `_setMapSelection` calls `hideMapContextMenu()`; Escape hides the menu; menu actions re-read `_liveMapSelectionNames()` and bail if fewer than 2 remain |
| 3 | `initOntologyMap` selection reset closed/saved the panel | `_clearMapSelection({ closePanel: false })` |
| 4 | ~150 duplicated lines in Business View builders | `_createMapBusinessView(spec)` owns name uniqueness, layout, visibility, create/switch/save, navigation. `createBusinessViewFromEntity` (1-hop, centre + ring, same `Auto_<name>`) and `createBusinessViewFromSelection` (selected-only, `Auto_Selection`) only collect names/links and a `positionFor` callback. Generated payloads are unchanged |
| 5 | No browser regression test | `tests/e2e/ontology/test_studio_multi_select_flows.py` (7 tests, Ctrl and Cmd variants) |
| m1 | pointerup must cancel if modifier released | `onUp` returns without selecting if `!(evt.ctrlKey \|\| evt.metaKey)` or the marquee was cancelled mid-drag (mid-drag release keeps listening so the trailing click is still absorbed) |
| m2 | pointercancel/blur cleanup | `pointercancel` + `blur` listeners → `cleanup()` (removes listeners + rect, never touches selection) |
| m3 | Escape vs modal / inactive Studio | Ignored when `.modal.show` exists or `#map-section.active` is absent |
| m4 | Dead marquee target check | Removed the unreachable `closest('.map-node, …')` line |
| m5 | Multi-delete count | Notifies the actual removed class count (`removedCount`); 0 removed → warning, no save |
| m6 | Docs | `docs/architecture.md` section (incl. macOS Ctrl-click and architecture notes) |

Not touched: pre-existing mobile canvas collapse.

## RED evidence (before the JS change)

Unit contracts: `13 failed, 10 passed` (e.g. `test_canvas_click_honours_marquee_suppression`,
`test_selection_change_closes_context_menu`, `test_map_rebuild_does_not_close_the_panel`,
`test_business_view_builders_share_one_helper`, plus updated contracts for helper/removed-count).

Playwright (`ONTOBRICKS_E2E_FAKE_CREDS=1`, server on :18765): `4 failed, 3 passed`
- `test_modifier_marquee_keeps_selection_after_browser_click[cmd]` — `[] == ['Alpha','Beta','Gamma']`
- `test_empty_modifier_marquee_leaves_selection_unchanged[cmd]` — selection wiped
- `test_multi_menu_closes_when_modifier_toggle_changes_selection` — `#mapContextMenu` still present
- `test_multi_menu_closes_on_escape_and_clears_selection` — `#mapContextMenu` still present

## GREEN evidence

- `uv run --frozen pytest -q tests/units/front/test_ontology_map_multi_select.py` → **24 passed**
- Playwright, spawned server on :18765: `ONTOBRICKS_E2E_FAKE_CREDS=1 uv run --frozen pytest tests/e2e/ontology/test_studio_multi_select_flows.py -q --no-cov` → **7 passed**
- Same 7 tests against the running localhost dev app (`http://localhost:8000`, throw-away fresh browser context/session via a temp conftest in `/tmp/lh`) → **7 passed**
- Full suite: `uv run --frozen pytest -q -m "not scenario"` → **7235 passed, 322 skipped, 6 deselected, 1 xfailed**
- `node --check src/front/static/ontology/js/ontology-map.js` → OK

## Self-review

- Single-entity BV behaviour preserved (including the pre-existing self-loop quirk where the selected name can be duplicated in the entity list; payload shape unchanged). Multi BV still contains only selected names + internal links, with all others in `visibility.hidden*`.
- `test_new_pointerdown_resets_leftover_click_suppression` was added after the matching code (the pointerdown reset was discovered while making the plain-click E2E green), so it has no separate RED run.
- Suppression is time-boxed (400 ms) and reset by `pointerdown`; a user who plain-clicks within 400 ms *without* a new pointerdown cannot occur.
- `_setMapSelection` always hides the menu, including when the right-click handler selects an unselected node — the menu is shown after the call, so this is safe.

## Unresolved / caveats

1. **Ctrl variants are not a true RED on macOS**: Chromium on macOS turns Control+left-click into a context-click with no trailing `click`, so the `[ctrl]` marquee tests pass even without the fix; the `[cmd]` variants (and Linux/Windows `[ctrl]`) are the real regression guards. The stale-menu contextmenu path is exercised through a synthetic `MouseEvent('contextmenu', {ctrlKey:true})`.
2. **Default e2e credentials**: with no Databricks CLI profile the suite auto-skips; I ran it with the documented `ONTOBRICKS_E2E_FAKE_CREDS=1` hatch (these pages need no workspace calls).
3. **Changelog not updated**: `.cursorrules` requires a changelog entry, but `changelogs/v0.9.0/benoitcayladbx_2026-10-01.log` is in the protected dirty set, so no entry was added. A follow-up should append a "Studio multi-select final-review fixes" section.
4. `docs/features.md` / `docs/user-guide.md` (dirty, protected) were not touched, so they have no mention of the new Ctrl/Cmd+Esc behaviours beyond what already exists there.
5. Mobile canvas collapse left as is, per instructions.

## Unrelated dirty files — untouched (not modified, staged, or committed)

Still showing as unstaged-modified exactly as before, none in the commit:

- `src/back/objects/mapping/Mapping.py`
- `src/front/static/mapping/js/mapping-diagnostics.js`
- `src/front/templates/partials/mapping/_mapping_diagnostics.html`
- `tests/units/front/test_schema_drift_ui.py`
- `tests/units/mapping/test_mapping_service.py`
- `tests/units/mapping/test_schema_drift.py`
- `docs/features.md`, `docs/user-guide.md`
- `changelogs/v0.9.0/benoitcayladbx_2026-10-01.log`

`.superpowers/` (ignored plan) was not staged; `uv.lock` unchanged (`--frozen` used).

---

# Follow-up — Escape inside Bootstrap confirmation (commit `06f3969db2db9ff8fcce852c037ec6293ba99808`)

Files committed: `src/front/static/ontology/js/ontology-map.js`,
`tests/units/front/test_ontology_map_multi_select.py`,
`tests/e2e/ontology/test_studio_multi_select_flows.py`.

## Changes
- `handleMapSelectionKeyDown` now returns early when `event.target.closest('.modal')` or
  `document.body.classList.contains('modal-open')` (the `.modal.show` probe is kept).
  Bootstrap's element-level keydown handler dismisses the dialog and drops `.show`
  before the document listener runs. Outside-Studio (`#map-section.active`) and normal
  Studio Escape (clear multi-selection, close menu) behaviour unchanged.
- `_createMapBusinessView` JSDoc: `positionFor` documented as
  `function(number, number, number, number)` called as `(index, cx, cy, radius)`.

## RED
- Unit: `2 failed, 24 passed` (`test_escape_ignores_events_from_modal_even_after_show_class_removed`,
  `test_business_view_position_callback_documents_all_arguments`).
- Playwright `test_escape_in_delete_confirmation_keeps_selection`: `assert [] == ['Alpha','Beta','Gamma']`
  (selection cleared by Escape in the delete dialog). First draft of the test passed
  falsely because Escape was pressed before Bootstrap's fade-in finished and moved focus
  into the dialog; the test now waits for focus inside `.modal`, presses Escape, and
  waits for `.modal.show` to detach before asserting.

## GREEN
- Unit: `26 passed`.
- Playwright (`ONTOBRICKS_E2E_FAKE_CREDS=1`, whole module): `10 passed`
  (adds modal-Escape, normal-Studio-Escape-still-clears, outside-Studio-Escape-keeps-selection).
- Full suite `uv run --frozen pytest -q -m "not scenario"`: `7237 passed, 325 skipped, 6 deselected, 1 xfailed`.
- `node --check ontology-map.js`: OK.

## Untouched
`docs/features.md`, `docs/user-guide.md`, `changelogs/v0.9.0/benoitcayladbx_2026-10-01.log`,
mapping diagnostics files and tests, and this report remain unstaged/uncommitted.
Changelog entry still not added (protected file) — carry-over from the previous report.
