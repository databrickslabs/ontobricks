# Versions Popup New Version Design

## Goal

Allow users to create a new domain version directly from the global Versions
popup, with the same behavior as Domain → Versions.

## Design

Move the create-version workflow into one globally available function owned by
`navbar.js`. The workflow keeps the existing confirmation dialog, posts to
`/domain/create-version`, reports progress and errors through the Notification
Center, invalidates domain caches, and reloads the page after success.

The Domain → Versions button delegates to this shared function, removing its
duplicate request logic. The Versions popup adds a labeled **New Version**
button with the `bi-plus-circle` icon. It appears in the footer before Cancel
and Switch, leaving Switch as the popup's rightmost primary action.

The action remains available on read-only or incomplete domain versions,
matching the existing version-branching contract.

## Error Handling

Cancellation leaves the popup open and performs no request. API and network
failures use `showNotification(..., "error")`. Successful creation invalidates
cached domain data and reloads the current page.

## Verification

Add structural tests proving that the popup exposes the button, wires it to the
shared action, and that Domain → Versions delegates to the same function.
Retain the existing ungated-version assertions, then run the full non-scenario
test suite and browser-check the popup at desktop and mobile widths.
