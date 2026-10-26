# Ontology Icon Remap Confirmation

## Context

Ontology Studio's auto-map icon action currently assigns icons only to entities
that still use the default icon. When every entity already has a custom icon,
the action stops with an informational notification. Users need an explicit,
safe way to replace all existing icon assignments without adding a second
toolbar action.

## User Experience

The existing behavior remains unchanged while at least one entity uses the
default icon: clicking auto-map submits only those entities for icon assignment.

When every entity has a custom icon, clicking auto-map opens the shared
confirmation dialog with:

- Title: `Remap entity icons?`
- Message: `All entities already have custom icons. Remapping will replace every current icon.`
- Cancel action: `Cancel`
- Confirm action: `Remap all`

Cancelling closes the dialog without starting a task or changing icons.
Confirming submits every ontology entity to the existing icon-assignment
background task.

The empty-ontology, inactive-version, and task-already-running guards retain
their current behavior. The running-task guard is evaluated before presenting
the remap confirmation so users are not asked to approve work that cannot
start.

## Implementation

`autoAssignEntityIcons()` in
`src/front/static/ontology/js/ontology-map.js` remains the single entry point.
It determines the normal candidates as it does today. If that candidate set is
empty, it awaits the existing `showConfirmDialog()` helper from
`src/front/static/global/js/utils.js`. A confirmed remap changes the candidate
set to all ontology classes; cancellation returns immediately.

After candidate selection, both normal assignment and confirmed remapping use
the same endpoint, request payload, button loading state, task persistence,
polling, result application, and notifications. No backend or markup change is
required.

## Error Handling

The existing error and notification paths remain authoritative:

- Failure to start the task restores the toolbar button and shows an error.
- Background task failure or cancellation is reported through the Notification
  Center.
- Missing icon results continue to produce the existing warning.
- A missing confirmation helper fails safely by not replacing custom icons.

## Testing

Add a frontend behavior contract covering:

1. Default-icon candidates are submitted without a remap confirmation.
2. An all-custom ontology shows the shared confirmation dialog.
3. Cancelling submits no task.
4. Confirming submits every entity name to `/ontology/auto-assign-icons`.
5. A running task blocks the action before confirmation.

Run the focused contract first, then the repository suite with:

`uv run --frozen pytest -q -m "not scenario"`

## Scope

This change does not add entity selection, a separate force-remap toolbar
action, backend API changes, or confirmation for mixed default/custom icon
sets.
