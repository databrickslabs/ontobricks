# Domain Versions Cards and Safe Deletion Design

## Summary

Redesign **Domain → Versions** as a vertically stacked card workspace rather
than a sparse table. Each version card exposes lifecycle state, useful
metadata, permitted lifecycle transitions, loading, and guarded deletion in
the context where the version is managed.

Version deletion remains destructive and admin-only. It is allowed only for an
older `DRAFT` version that is neither loaded in the current session nor the
latest active version. The same server-side policy applies to both
**Domain → Versions** and **Registry → Browse**, closing the current Registry
path that can delete protected versions.

## Goals

- Make version history readable without sending users to Registry for routine
  lifecycle actions.
- Replace the technical table with responsive cards ordered newest first.
- Show description, author, last update, last build, lifecycle status, and
  loaded state for each version.
- Expose only lifecycle transitions permitted by the existing state machine,
  role rules, and readiness preconditions.
- Add a safe, discoverable version-delete action.
- Enforce all lifecycle and deletion rules on the server; frontend state is
  advisory only.
- Keep Domain and Registry behavior consistent.

## Non-Goals

- Bulk deletion.
- Soft deletion, archival, retention periods, or undo.
- Deleting a loaded, latest, `IN-REVIEW`, or `PUBLISHED` version.
- Allowing Builders or Editors to delete versions.
- Changing lifecycle states or transition permissions.
- Redesigning the Publication/Validation workflow.
- Deleting an entire domain from Domain → Versions.

## Current State

`partials/domain/_domain_versions.html` renders a five-column table populated
by `domain-versions.js`. It shows version, description, status, author, and a
load icon. Lifecycle changes are delegated to Registry → Browse through
instructional text.

Deletion already exists in Registry → Browse through:

`DELETE /settings/registry/domains/{domain_name}/versions/{version}`

That endpoint is app-admin-only and delegates to
`RegistryService.delete_version`, which removes the registry row, Knowledge
Store documents, and legacy binary directory. However, neither the endpoint
nor the service validates lifecycle state, loaded state, or whether the
version is the latest. Registry currently hides delete only for the loaded
version, so an unloaded `PUBLISHED`, `IN-REVIEW`, or latest version can be
deleted.

The existing lifecycle source of truth is
`back.objects.registry.version_lifecycle`. Its transition graph remains
unchanged:

- `DRAFT → IN-REVIEW`: Admin or Builder, with ontology or successful build.
- `IN-REVIEW → DRAFT`: Admin or Builder.
- `IN-REVIEW → PUBLISHED`: Admin or Builder.
- `PUBLISHED → DRAFT`: app Admin only.

## UX Design

### Page structure

Keep the existing page header and its two primary controls:

- **Reload Saved**
- **New Version**

Replace the lifecycle instruction paragraph and table with a short subtitle
and a full-height card-list surface. On desktop the list fills the remaining
sidebar workspace and owns vertical scrolling. At `≤768px`, the page returns
to natural document flow.

Cards are ordered by semantic version descending, matching the existing
version-list order.

### Version card anatomy

Each card contains:

1. **Identity row**
   - `v<version>` as the card title.
   - Lifecycle badge: Draft, In Review, or Published.
   - `Loaded` chip when `is_current` is true.
   - `Latest` chip when `is_active` is true.
2. **Description**
   - Version description, or a muted “No description” fallback.
3. **Metadata**
   - Author.
   - Last updated timestamp.
   - Last successful build timestamp, or “Not built”.
4. **Action footer**
   - Lifecycle transitions on the left.
   - Load and Delete on the right.

The loaded version uses a restrained primary border/leading accent rather than
a full primary background. Status remains represented by the shared semantic
badge tokens, not by card color.

### Lifecycle actions

The backend returns transition capabilities for each version. The frontend
renders those capabilities instead of reconstructing role or state-machine
rules.

Examples:

- Draft: **Submit for Review**.
- In Review: **Return to Draft**, **Publish**.
- Published: **Reopen as Draft** for app admins.

An authorized but currently unavailable transition is rendered disabled with
its server-provided reason, for example “Define an ontology or run a Knowledge
Graph build first.” Unauthorized transitions are omitted.

On confirmation, the frontend reuses `POST /domain/set-version-status`, then
refreshes the card list and navbar/domain status caches without a full page
reload. Server errors are shown through the shared notification mechanism.

### Load action

Non-loaded versions expose **Load** with an icon and text. The existing
confirmation remains: loading another version discards unsaved changes. The
loaded card omits Load because **Reload Saved** already owns that operation.

### Delete action

Only app admins see the Delete affordance.

Delete is enabled only when all conditions are true:

- status is `DRAFT`;
- the version is not loaded in the current session;
- the version is not the latest active version (the highest semantic version);
- more than one version exists.

When an admin cannot delete a version, the card shows a disabled trash control
inside a tooltip-capable wrapper with one precise reason:

- “Reopen this version as Draft before deleting it.”
- “Load another version before deleting this one.”
- “The latest version cannot be deleted.”
- “A domain must keep at least one version.”

Non-admin users do not see Delete.

The confirmation names both domain and version, states that associated
Knowledge Store content is permanently removed, and uses the shared danger
dialog. Successful deletion removes the card after a server refresh; failures
leave the list unchanged.

### Empty, loading, and error states

The current semantic states remain, but occupy the card-list surface:

- skeleton/spinner while loading;
- empty state when the domain has no persisted versions;
- retryable warning state on request failure.

No inline `style` visibility toggles are introduced. Shared utility classes
own hidden/visible state.

### Accessibility

- The card list is a labelled region.
- Each card is an `article` labelled by its version heading.
- Status and loaded/latest state use visible text, not color alone.
- All action buttons have visible labels on desktop; icon-only compaction is
  allowed only at the established mobile breakpoint with `aria-label`.
- Disabled-action explanations are keyboard reachable through their wrapper.
- Confirmation focus returns to the originating card/action when cancelled.

## Backend Design

### Capability payload

Extend `GET /domain/versions-list` so each version includes:

```json
{
  "version": "2",
  "description": "Customer domain updates",
  "status": "DRAFT",
  "author": "user@example.com",
  "last_update": "2026-09-25T09:00:00Z",
  "last_build": "",
  "is_current": false,
  "is_active": false,
  "transitions": [
    {
      "target_status": "IN-REVIEW",
      "label": "Submit for Review",
      "enabled": true,
      "blocked_reason": ""
    }
  ],
  "can_delete": true,
  "delete_block_reason": ""
}
```

Capabilities are computed from freshly read version data, request roles, the
existing lifecycle state machine, the loaded session domain/version, and the
sorted version inventory. The client never infers permissions from badge text.

### Shared deletion policy

Add a pure deletion-policy validator alongside the lifecycle state machine. It
accepts:

- app role;
- lifecycle status;
- target version;
- loaded domain/version;
- latest active version;
- version count.

It returns normally when deletion is allowed and raises the existing typed
errors otherwise:

- `AuthorizationError` for non-admin callers;
- `NotFoundError` for a missing target;
- `ConflictError` for lifecycle, loaded, latest, or final-version conflicts.

The capability payload and mutation path call the same policy function. The
mutation re-reads inventory and target status immediately before deletion to
avoid a time-of-check/time-of-use gap.

### Domain delete endpoint

Add:

`DELETE /domain/versions/{version}`

The route resolves the loaded domain folder from the session, passes request
role and current-version context to the shared guarded deletion service, and
returns the standard success envelope. It never accepts a caller-provided
domain folder.

The route remains subject to CSRF protection and the existing domain write
permission middleware. The service-level app-admin check is authoritative
defense in depth.

### Registry compatibility

Keep:

`DELETE /settings/registry/domains/{domain_name}/versions/{version}`

Change its implementation to call the same guarded deletion service. For an
arbitrary Registry target, the service compares loaded state only when the
target domain equals the session domain. Latest version and lifecycle checks
always apply.

Extend the version entries returned by `GET /settings/registry/domains` with
`can_delete` and `delete_block_reason`, computed by the same capability helper
using the caller role and loaded session context. Registry → Browse renders
those fields rather than inferring eligibility. Protected versions can no
longer be deleted through that alternate surface.

### Physical deletion

After policy validation, retain the existing
`RegistryService.delete_version` cleanup:

1. delete the registry version row;
2. delete Knowledge Store documents scoped to `(domain, version)`;
3. best-effort cleanup of the legacy binary version directory;
4. invalidate registry and version-status caches.

Policy rejection performs no cleanup. Partial cleanup continues to use the
existing infrastructure-error reporting and logging behavior.

## Error Handling

- `403`: caller is not an app admin.
- `404`: target version no longer exists.
- `409`: target is loaded, latest, final, or not Draft.
- `5xx`: registry or cleanup infrastructure failure.

The UI refreshes capabilities after any `409`, because another user may have
changed status or created a newer version between render and confirmation.

## Security and Consistency

- UI visibility is not authorization.
- Role and status are re-evaluated server-side for every mutation.
- Domain endpoint derives the target domain from session state.
- Registry endpoint remains admin-only through middleware and service policy.
- Version strings are validated against the actual registry inventory before
  deletion.
- Existing CSRF handling applies to both DELETE routes.

## Testing

### Unit contracts

- Card markup replaces the versions table.
- Required card fields and accessible labels exist.
- Frontend renders server-provided transitions and delete capability.
- Frontend does not duplicate lifecycle or deletion-policy logic.
- Mobile CSS restores natural flow.

### Policy tests

Cover the full deletion matrix:

- admin + old Draft + not loaded → allowed;
- non-admin → denied;
- loaded Draft → conflict;
- latest Draft → conflict;
- only version → conflict;
- old In Review → conflict;
- old Published → conflict;
- missing version → not found.

### API/service tests

- Domain DELETE derives the domain from session state.
- Domain and Settings DELETE routes invoke the same guarded service.
- Registry alternate path cannot bypass lifecycle guards.
- Successful deletion delegates once to physical cleanup and invalidates
  caches.
- Rejected deletion performs no physical cleanup.
- Capability output matches mutation policy for the same context.
- Existing lifecycle transition authorization and readiness rules remain
  unchanged.

### Browser verification

At desktop and mobile widths:

- cards render newest first;
- the desktop list fills the workspace and scrolls internally;
- mobile uses document scrolling without horizontal overflow;
- loaded/latest/status states are understandable without color;
- transitions update the card in place;
- Load preserves its unsaved-change confirmation;
- Delete confirmation, success refresh, blocked reasons, keyboard focus, and
  console/network errors are verified.

## Documentation Impact

Update the user documentation for Domain → Versions to explain inline
lifecycle controls and the conservative deletion policy. Registry
documentation must state that only older Draft versions are deletable.

## Accepted Decisions

- Card stack, not table or timeline.
- Inline lifecycle transitions.
- App-admin-only deletion.
- Only older, unloaded Draft versions are deletable.
- Latest active version is never deletable.
- Domain and Registry use one guarded backend policy.
- Desktop full-height workspace; mobile natural flow.
