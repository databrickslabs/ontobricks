# OntoBricks — Release Notes V0.1.2

**Release window:** April 20 – April 29, 2026
**Test status:** all changes shipped with the suite green (≥ 1892 passing).

---

## Highlights

- New end-to-end **Permissions model**: app-level perms come from Databricks, domain-level perms from the Teams matrix, with a 4-step refactor (declarative guards, body `data-*` attrs, CSS gating) and a hardened Viewer / read-only role across every ontology and mapping widget.
- **Graph Chat** (formerly *Digital Twin*): natural-language chat with the knowledge graph, now session-aware and stable behind the deployed reverse proxy.
- New in-app **Help Center** accessible from the navbar, including a Starter Guide, Workflow / FAQ accordions, a Data Access / GraphDB engine map (LadybugDB as default), and a refreshed About page.
- **Lakebase registry backend** wired end-to-end.
- Major **domain-switching robustness** improvements (no more stale state, full-page loading overlay everywhere, including cross-domain bridges).
- **Security**: patched two GitPython advisories (GHSA-rpm5-65cw-6hj4 and GHSA-x2qx-6953-8485 / CVE-2026-42284) by pinning `gitpython>=3.1.47` via uv constraint — transitive vuln only, no code-path exposure.

---

## Permissions & multi-tenant access control

- App-level permissions now sourced from Databricks; domain-level permissions handled by the Teams matrix.
- First-deploy bootstrap detects and fixes the app SP self-permission chicken-and-egg situation.
- Viewer / read-only role:
  - Cascaded to all ontology and mapping widgets.
  - OWL preview no longer fails with "Unknown error" in read-only mode.
  - Belt-and-suspenders contextmenu blocker on design surfaces.
  - Gates data-source reset and all ontology / mapping imports.
- Fixed Registry → Teams sub-menu leaking to non-admin users in the top navbar.
- New three-level permission matrix tests + OWL endpoint contract tests.
- 4-step permissions refactor: declarative guards, body `data-*` attributes, CSS-based gating.
- Code-review fix-up: navbar role-badge inline CSS moved into `permissions.css`.

## Graph Chat (renamed from Digital Twin)

- Natural-language chat over the knowledge graph.
- Forwards `X-Forwarded-*` headers on loopback to fix a deployed 302 redirect issue.
- All tools now use session-aware internal routes.
- Code-review hardening pass: clean layering, consistent error handling, deduplication, class-first refactor.

## In-app Help Center

- New navbar Help icon opens a modal with comprehensive documentation.
- Refreshed About page to reflect the current product scope.
- Visual pass:
  - Palette switched from blue to red/black (solid red, no gradients).
  - OntoBricks logo used in title and welcome hero.
  - Modal height locked (no resize when switching menu items).
  - Fixed double vertical scrollbar in tall sections (Starter Guide).
  - Removed horizontal scroll on the Welcome pipeline.
  - Removed grey borders on Workflow / FAQ accordions.
- Starter Guide:
  - Added optional "Import Documents" step.
  - Rewrote the mapping step (manual or Auto-Map).
- Added Data Access engine-map documentation, then generalized it to GraphDB (LadybugDB as default engine).

## Domain switching

- Fixed stale session state leaking between domain switches — `DomainSession.import_from_file` now fully resets ontology, assignment, design layout, domain info, metadata, and triplestore before overlay.
- Full-page "Loading {domain}…" overlay now appears for:
  - Graph switcher modal.
  - Bridge-based switches via URL parameters.
  - Cross-Domain Bridge links going through `/resolve` (server-side redirect).

## UI / UX fixes

- Build sub-menu: fixed unreadable "Mapping" stale-indicator badge.
- Build sub-menu: stopped reporting "Loaded" for the Graph DB digital twin when nothing had actually been built.
- Sidebar: fixed the "Teams" icon misalignment.
- Cockpit: the **Active Version** tile now reflects the version exposed via API/MCP (the one toggled in the Versions table) instead of the latest version on disk, with a `(not loaded)` hint when the loaded version differs. `is_active` keeps its legacy `is_latest` meaning so the read-only body class still gates writes correctly.

## Tasks & Notifications

- Tasks panel now shows only currently running tasks; finished tasks are moved to the Notifications drawer.

## Backend

- Lakebase registry backend wired end-to-end.

## Security

- Patched two GitPython advisories pulled in transitively via
  `mlflow-skinny`:
  - **GHSA-rpm5-65cw-6hj4** — command injection via `upload_pack` /
    `receive_pack` kwargs on `Repo.clone_from`, `Remote.fetch`,
    `Remote.pull`, `Remote.push` (affected `[3.1.30, 3.1.47)`).
  - **GHSA-x2qx-6953-8485 / CVE-2026-42284** — argument injection via
    `multi_options` `shlex.split` bypass in `_clone()` /
    `Submodule.update` (affected `<= 3.1.44`).
- Both fixed by adding `gitpython>=3.1.47` to
  `[tool.uv].constraint-dependencies` in `pyproject.toml`; lockfile
  bumped `gitpython 3.1.46 → 3.1.47`. OntoBricks itself does not import
  `git` anywhere, so there is no code-path exposure — this only closes
  the SCA finding on the lockfile / installed env.

---

## Upgrade notes

- No breaking config changes for existing deployments.
- First-time deploys benefit from the new SP-self-permission bootstrap; existing deployments are unaffected.
- Viewer/Editor/Admin roles are now enforced in both the backend and the UI — verify your Teams matrix after upgrade if you rely on custom role assignments.
