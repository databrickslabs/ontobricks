# Explorer Canvas Loading Overlay Design

## Context

Knowledge Graph Explorer currently reports Filter search progress in a small
status line below the Search button. Other graph operations use either the
existing centered `#sgLoading` spinner or a separate top-right expansion
spinner. This fragments progress feedback and makes the active operation easy
to miss.

## Goal

Use one blocking, centered canvas overlay for every Explorer graph load and
update its text as work moves through distinct phases.

## Scope

- Reuse the existing OntoBricks brand spinner in `#sgLoading`.
- Cover Filter preview search, selected-entity expansion, graph rendering,
  initial graph loading, reload, inferred-data refresh, and neighbor expansion.
- Remove live and completed status text below the Search button.
- Remove the separate top-right neighbor-expansion spinner.
- Preserve the seed-selection modal, search timing details, graph results, and
  backend request contracts.

Changing backend behavior, adding progress streaming, or reporting numeric
percentages is out of scope.

## Interaction and Visual Design

`#sgLoading` becomes a full-canvas overlay within `#sgContainer`. When active,
it covers the graph with a high-opacity warm surface, centers the existing
`.ob-loading-spinner`, and intercepts pointer input to the canvas. The details
panel remains visible. The Search action is disabled until the current load
finishes, preventing overlapping Filter searches.

The overlay uses the existing `.ob-spinner-svg`; no new spinner artwork or
animation is introduced. Its label is a polite live region so assistive
technology announces phase changes without moving keyboard focus. The graph
container exposes `aria-busy="true"` while loading and returns to
`aria-busy="false"` afterward.

Visibility is controlled with a CSS state class rather than inline display
styles. The overlay is hidden by default and remains below Bootstrap modals.

## Loading-State API

`query-sigmagraph.js` owns three small helpers:

1. `_showGraphLoading(label)` displays the overlay, sets the initial label,
   marks the canvas busy, and disables the Search button.
2. `_setGraphLoadingStep(label)` changes only the visible and announced text.
3. `_hideGraphLoading()` hides the overlay, clears the busy state, and
   re-enables the Search button.

All Explorer graph-loading paths use these helpers. Existing direct
`style.display` writes to `#sgLoading` are removed. Cleanup runs for success,
empty results, request failures, parse failures, library failures, and thrown
exceptions so the canvas cannot remain blocked.

## Progress Labels and Flow

The overlay presents concise operation-specific steps:

- Filter preview request: `Searching…`
- A single preview match before expansion:
  `1 entity found — expanding…`
- Selected-entity expansion request: `Expanding N entities…`
- Graph construction and Sigma rendering: `Rendering graph…`
- Neighbor request: `Expanding neighbours (N hop)…`
- Initial load, reload, and inferred-data refresh: `Loading graph data…`

Labels use a typographic ellipsis consistently.

When preview search returns multiple entities, the overlay hides before the
seed-selection modal opens. User selection is not background work and
therefore has no spinner. Choosing **Explore selected** closes the modal and
shows the overlay again at the expansion step.

The overlay hides only after synchronous graph rendering has completed. The
existing search timing chip then appears independently as a post-search result.

## Status and Error Handling

The `#sgGraphFilterInfo` region and all writes to it are removed, so no search
progress or result summary remains below the Search button.

No-result preview behavior keeps the existing **No results** information
dialog. Operational failures hide the overlay and use
`showNotification(message, 'error')`. Empty expansion and neighbor results hide
the overlay and use the existing information or warning feedback as
appropriate. Console logging may remain for diagnostics but is never the only
user-facing failure signal.

## Testing

Frontend contract tests will verify:

- The template contains one centered graph-loading overlay using the shared
  brand spinner and an accessible live label.
- The obsolete Filter status region and top-right expansion spinner are absent.
- Loading helpers toggle a class, `aria-busy`, label text, and Search disabled
  state without inline visual styles.
- Filter preview, expansion, render, neighbor expansion, initial load, reload,
  and inferred refresh use the shared helper and expected labels.
- The overlay hides before the seed-selection modal appears and returns after
  selected entities are submitted.
- Every success, empty, and failure branch releases the blocking state.
- Existing latest-search timing contracts continue to pass.

Browser verification will cover desktop and mobile widths, phase changes,
canvas input blocking, keyboard focus, seed-modal transitions, and console or
network errors. The full non-scenario test suite remains the regression gate.

## Acceptance Criteria

- Every Explorer graph load uses the same centered canvas overlay.
- The displayed text changes to identify the current work phase.
- The canvas cannot be manipulated while the overlay is active.
- No live or completed search status appears below the Search button.
- The seed-selection modal is never obscured by the canvas overlay.
- The overlay always clears after completion, empty results, or failure.
- Existing graph behavior and timing details remain unchanged.
