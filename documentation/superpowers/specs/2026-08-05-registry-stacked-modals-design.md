# Registry Stacked Modals Design

## Context

The Registry opens as a centered Bootstrap modal. Its Export and Import
dialogs are nested modals, but their dialog markup is not vertically centered
and their open/close lifecycle does not apply the existing stacked-modal
treatment. As a result, they appear toward the top-right and leave the Registry
content visually active behind them.

## Goal

Center the Registry Export and Import dialogs in the viewport. While either
dialog is open, blur and dim the Registry modal behind it. Closing the child
dialog must restore the Registry modal without leaving stacking classes or
backdrops behind.

## Design

### Dialog layout

Add Bootstrap's `modal-dialog-centered` class to both:

- `src/front/templates/partials/registry/_export_obx_modal.html`
- `src/front/templates/partials/registry/_import_obx_modal.html`

Keep the existing large, scrollable dialog sizing. Render both child modal
includes after the closing `#registryModal` element, rather than inside its
Browse pane. This makes them DOM siblings, preventing the Registry blur and
stacking context from affecting the active child dialog.

### Reusable modal stacking

Extract the existing stacked-modal class lifecycle from
`showConfirmDialog()` in `src/front/static/global/js/utils.js` into a reusable
global helper. The helper will:

1. On child modal display, identify the topmost already-open modal.
2. Mark that modal with `ob-modal-underlying`.
3. Mark the child and its backdrop with the existing stacked classes.
4. On child modal close, remove every class added by that lifecycle.

`showConfirmDialog()` will use the helper so its current behavior remains
unchanged. Registry Export and Import will register the same helper before
showing their Bootstrap modal instances.

The existing rules in `components.css` remain the visual source of truth:
the underlying modal content receives a 2px blur and reduced opacity, while
the child modal and backdrop use the established z-index ordering.

## Testing

- Add a unit-level static contract proving both Registry child dialogs are
  centered, rendered as siblings of Registry, and both open paths register the
  shared stacking helper.
- Extend the stacked-modal helper contract to cover application and cleanup of
  the underlying, child, and backdrop classes.
- Run the required non-scenario test suite:
  `uv run --frozen pytest -q -m "not scenario"`.

## Success criteria

- Export and Import open centered in the viewport.
- Registry is blurred and dimmed while either child dialog is visible.
- Export and Import remain sharp and clickable above the stacked backdrop.
- Closing the child returns Registry to its normal appearance and interaction.
- Existing confirm-dialog stacking behavior is preserved.
- No new modal-specific inline styling or duplicate stacking implementation is
  introduced.
