# Stacked Confirmation Modal Design

## Context

The Registry is displayed in a large Bootstrap modal. Actions such as loading
or deleting a domain call the shared `showConfirmDialog()` helper, which opens
a second Bootstrap modal without accounting for the already-visible modal.
Both surfaces are white and use Bootstrap's default modal stacking, so the
confirmation does not stand out clearly from the Registry beneath it.

## Goal

Make every confirmation opened above an existing modal visually distinct,
while preserving the underlying modal and restoring it unchanged when the
confirmation closes.

## Design

`showConfirmDialog()` will detect the topmost visible modal before it opens its
confirmation. When one exists, the helper will:

1. Mark the existing modal as the underlying modal.
2. Mark the generated confirmation as a stacked modal.
3. Apply a higher stacking level to the confirmation and its backdrop.
4. Dim and lightly blur the underlying modal for the lifetime of the
   confirmation.

The visual treatment will live in shared global component CSS and use existing
`--db-*` tokens where applicable. It will use a short transition and respect
`prefers-reduced-motion`.

When the confirmation is hidden, the helper will remove all temporary classes,
remove the generated modal from the DOM, and return focus to the underlying
modal through Bootstrap's normal modal lifecycle. Confirming and cancelling
will continue to resolve the existing `Promise<boolean>` API unchanged.

## Scope

The behavior applies to any `showConfirmDialog()` invocation made while another
Bootstrap modal is visible. Calls made directly from a page retain their
current appearance. No Registry-specific confirmation API or styling will be
introduced.

Nested confirmations beyond one confirmation over one parent modal are out of
scope.

## Error and Cleanup Behavior

If no visible parent modal exists, the helper follows its current single-modal
path. Cleanup is idempotent so close-button, cancel-button, Escape, and confirm
paths all restore the parent exactly once.

## Testing

Add a focused browser regression test that:

1. Opens the Registry modal.
2. triggers a confirmation from a Registry action.
3. verifies the Registry has the underlying-modal state.
4. verifies the confirmation is visible above it.
5. cancels the confirmation.
6. verifies the temporary state is removed and the Registry remains visible.

Run the focused test first, then the required non-scenario suite:

`uv run pytest -q -m "not scenario"`
