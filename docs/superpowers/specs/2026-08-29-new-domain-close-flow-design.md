# New Domain Close-First Flow

## Context

The current New Domain action collects the new domain details before silently
closing the loaded domain. In edit-lock view mode, the loaded domain's
read-only CSS also affects the dynamically inserted New Domain fields.

The action must close any loaded domain first, preserving the existing choice
to save or discard editable changes, and only then collect the new domain
details.

## Behaviour

- With no domain loaded, New Domain opens its details dialog immediately.
- With a domain loaded, New Domain first opens the shared close confirmation.
- An editable domain offers **Save & Close**, **Close without saving**, and
  **Cancel**.
- A domain in view mode offers **Close without saving** and **Cancel** because
  that browser cannot persist domain changes.
- Canceling the close confirmation aborts New Domain and leaves the current
  domain open.
- After a successful close, the New Domain dialog opens without navigating to
  Home.
- Canceling the New Domain dialog after the close leaves no domain loaded.
- The regular Close action keeps its existing post-close navigation to Home.

## Design

Extract the existing close sequence into a shared asynchronous helper in
`src/front/static/global/js/navbar.js`. The helper owns:

1. determining whether saving is available;
2. collecting the close choice;
3. optionally saving the current domain;
4. closing the server session and releasing its edit lock;
5. returning whether the close completed.

`domainClose()` calls the helper and navigates to Home after success.
`domainNew()` calls it without navigation, aborts when the close is canceled or
fails, and opens `showNewDomainDialog()` only after success.

The New Domain fields retain their explicit read-only CSS exemption. This
keeps the modal usable while the close request updates server state and avoids
coupling a cross-domain action to stale page-level presentation classes.

## Error Handling

- A failed save leaves the current domain open and aborts New Domain.
- A failed close reports the error through `showNotification` and does not open
  the New Domain dialog.
- Cancel is not an error and produces no notification.
- No-domain close requests are skipped.

## Testing

Frontend contract tests will verify:

- the three New Domain fields remain exempt from loaded-domain read-only gates;
- New Domain invokes the shared close flow before opening its dialog;
- canceling or failing the close prevents the dialog from opening;
- view mode cannot select the save path;
- the regular Close action still navigates Home after a successful close.

The routine non-scenario pytest suite remains the final regression gate.
