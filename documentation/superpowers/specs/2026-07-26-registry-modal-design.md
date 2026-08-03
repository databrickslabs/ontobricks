# Registry Navbar Modal

## Goal

Replace the Registry navbar dropdown + full page with an icon-only control that opens a modal for domain Browse and Bridges.

## Behaviour

1. Navbar: `bi-archive` at ~2× size, no "Registry" label, no dropdown.
2. Click opens a large modal (always available from `base.html`).
3. Modal chrome: **Create a New Domain** top-right (calls existing `domainNew()`). No Load Domain.
4. Tabs: **Browse** (existing domains partial) and **Bridges** (existing bridges partial).
5. `registry.js` keeps working via Bootstrap tab events mapped to the same load triggers as before.
6. `GET /registry/` redirects to `/?open=registry` so bookmarks open the modal on Home.
7. Load Domain / New Domain removed from Registry navbar actions and from the Browse toolbar (Create lives in the modal header).

## Out of scope

- Settings → Registry configuration
- Changing `/settings/registry/*` APIs
