# Explorer Search Timing Details Design

## Context

Knowledge Graph Explorer currently shows one end-to-end search duration in the
bottom-left corner of the Sigma canvas. The stopwatch excludes time spent in
the seed-selection dialog, but it does not explain whether time was spent
finding seeds, expanding the graph, or preparing and displaying the result.

The duration indicator is dynamically created as a non-interactive `div` with
inline styles. It cannot expose details by click or keyboard and does not follow
the shared frontend styling rules.

## Scope

Clicking the duration indicator displays the timing breakdown for the latest
successfully rendered search only. No history is retained. Timings are
browser-observed API-phase durations rather than individual warehouse SQL
statement durations.

The breakdown contains:

1. **Preview request** — from submission of the seed-search request until its
   response is parsed.
2. **Expansion request** — from submission of the selected-seed expansion
   request until its response is parsed.
3. **Display** — from the parsed expansion payload through graph construction
   and completion of the Sigma render.
4. **Total** — the active search stopwatch: search click through displayed
   graph. It pauses immediately before the seed-selection popup opens and
   resumes only after Explore closes the popup, excluding the full user dwell.

For a single seed, Preview flows directly into Expansion without a selection
pause. If a phase does not run or does not complete, its value is displayed as
an em dash.

## Interaction and visual design

The bottom-left indicator becomes a real `button` and retains the stopwatch
icon plus compact total duration. It uses area-specific classes in
`query-sigmagraph.css`; no inline visual styles are introduced.

Activating the button opens a compact panel anchored immediately above it. The
panel shows the four labels and durations in a two-column definition-list
layout. It is a lightweight details popover owned by Explorer rather than a
modal or a tab in the entity-details panel.

The control provides:

- `type="button"` and an accessible label describing the latest total;
- `aria-expanded` reflecting panel visibility;
- `aria-controls` pointing at the timing-details panel;
- mouse and keyboard activation through native button behavior;
- Escape and outside-click dismissal;
- no effect on Sigma canvas selection, pan, or zoom.

The panel remains within the canvas at desktop and mobile widths. It uses
existing `--db-*` colors, borders, radii, shadows, and typography tokens.

## State and data flow

`query-sigmagraph.js` owns one in-memory timing record for the active/latest
search:

```text
previewMs: number | null
expansionMs: number | null
displayMs: number | null
totalMs: number | null
```

Starting a new search resets all phase values and closes stale details.
Preview timing is recorded around `_fetchWithTimeout` plus
`_parseJsonResponse`. Expansion timing is recorded around the equivalent
operations in `_expandAndRenderGraph`. Display timing begins after the
expansion payload is parsed and ends after `_render()` completes.

The existing pause/resume stopwatch remains authoritative for Total, preserving
the exclusion of seed-selection dwell. `_showSearchTiming` receives the
completed timing record, updates the button text and accessible label, and
renders the details rows.

Non-search renders and canvas refreshes do not replace the latest timing
record.

## Failure behavior

A failed preview or expansion keeps the current user-facing error behavior and
does not publish a new completed timing record. Starting another search closes
the previous panel and clears stale phase values. A missing timing value is
shown as `—`; the UI never displays `NaN`, negative values, or a partial total.

## Files

- `src/front/static/query/js/query-sigmagraph.js`
  - collect phase timings;
  - create and update the accessible button and details panel;
  - manage open, outside-click, and Escape behavior.
- `src/front/static/query/css/query-sigmagraph.css`
  - own indicator and anchored-panel presentation.
- `tests/units/front/test_explorer_search_timing.py`
  - verify phase collection, latest-only state, accessible interaction, and
    absence of inline styles.
- `docs/get-started.md`
  - describe the clickable Explorer timing indicator.
- `changelogs/v0.9.0/benoitcayladbx_2026-09-10.log`
  - record the user-facing feature and verification.

## Verification

1. Focused structural tests fail before implementation and pass afterward.
2. Browser verification covers:
   - a multi-result search with selection dwell;
   - a single-result search;
   - mouse click, Enter/Space activation, Escape, and outside-click dismissal;
   - desktop and mobile containment;
   - no console or network errors.
3. The complete non-scenario suite passes with:
   `uv run --frozen pytest -q -m "not scenario"`.
