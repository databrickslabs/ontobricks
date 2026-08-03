# View-mode write-button gates

**Date:** 2026-07-31  
**Status:** Approved (chat) — UI-only

## Problem

On a non-editable domain version (`read-only-version`: IN-REVIEW / PUBLISHED /
older versions) or for a Viewer (`role-viewer`), a few write actions stay
reachable:

- Mapping **Unmap all** (designer button has no gate; information button is
  only *hidden* via `.ontology-edit-btn`)
- Knowledge Graph **Build** (`#syncStartBtn`) — role-gated at render, but JS
  re-enables it when the graph is "ready"
- Cohorts / Reasoning **Materialise** — cohort button is listed in CSS but JS
  clears `disabled` after Preview; Reasoning `#runMaterializeBtn` has no gate

## Approach

Extend the existing declarative gate in `permissions.css`:

```css
body:is(.read-only-version, .role-viewer, .read-only-locked) #…
```

Disable (visible, greyed, `pointer-events: none`) — do not hide — for:

- `#resetMappingsBtn`, `#resetMappingsDesignBtn`
- `#syncStartBtn`
- `#cohortMaterializeBtn` (already listed; keep)
- `#runMaterializeBtn`

Remove `ontology-edit-btn` from `#resetMappingsBtn` so it is disabled rather
than hidden (matches the TODO wording).

Defense in depth: early-return in the click/start handlers when
`document.body` carries any of those three classes, with a short notification.

Out of scope: backend rejection of build / materialise on non-DRAFT.

## Success criteria

On a PUBLISHED or IN-REVIEW version (or Viewer role): Unmap all, Build, and
both Materialise buttons are greyed and do not fire; on DRAFT they behave as
today.
