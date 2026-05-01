# OntoBricks — Release Notes V0.2.0

**Release window:** May, 2026
**Test status:** all changes shipped with the suite green (≥ 1892 passing).

---

## Highlights

- New end-to-end **Permissions model**: app-level perms come from Databricks, domain-level perms from the Teams matrix, with a 4-step refactor (declarative guards, body `data-*` attrs, CSS gating) and a hardened Viewer / read-only role across every ontology and mapping widget.
- **Graph Chat** (formerly *Digital Twin*): natural-language chat with the knowledge graph, now session-aware and stable behind the deployed reverse proxy.
- New in-app **Help Center** accessible from the navbar, including a Starter Guide, Workflow / FAQ accordions, a Data Access / GraphDB engine map (LadybugDB as default), and a refreshed About page.
- **Lakebase registry backend** wired end-to-end.
- **Databricks dev sandbox bundle** (`databricks.yml`): deploys **`ontobricks-020`** (main UI) and **`mcp-ontobricks`** (MCP); targets **`dev`** (Volume-only) and **`dev-lakebase`** (Volume + Lakebase Autoscaling `postgres` binding). Lakebase variables include **`lakebase_database_resource_segment`** (the `db-…` suffix from `databricks postgres list-databases … -o json`, not the Postgres `datname`) and **`lakebase_registry_schema`** (keep in sync with **`LAKEBASE_SCHEMA`** in `app.yaml`).
- **Deploy & bootstrap scripts** aligned with the bundle: **`scripts/deploy.sh`** uses **`APP_NAME=ontobricks-020`**; **`make bootstrap-perms`** / **`make bootstrap-lakebase`** and the underlying shell scripts default to **`ontobricks-020`**, **`mcp-ontobricks`**, and the documented Lakebase project / schema-grant flow.
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
- Cockpit: the **Active Version** tile now reflects the version exposed via API/MCP (the one set in **Registry → Browse**), not merely the latest version on disk, with a `(not loaded)` hint when the loaded version differs. `is_active` keeps its legacy `is_latest` meaning so the read-only body class still gates writes correctly.
- Navbar: the **Domain name and version** in the top navbar now refresh reliably after every domain mutation (new domain, load from registry, save / rename, version switch / create / rollback, file import). The `/navbar/state` `sessionStorage` cache (15 s TTL) was previously surviving `window.location.reload()`, so the navbar could display the *previous* domain identity for up to 15 s. Every mutation flow now invalidates the cache before navigating; in-place edits (e.g. saving Domain Information) re-fetch the navbar state immediately.
- Domain → **Versions**: the API/MCP “Active” control is no longer a toggle on this page — it is shown as a **read-only badge**; changing the active version is done only from **Registry → Browse** (consistent with registry-centric operations).
- Domain creation: **Save to UC is now blocked** when the chosen Domain Name already exists in the registry. The duplicate-name check (`/domain/check-name`) was already running on every keystroke of the name field, but its result was only advisory — the navbar's Save action still POSTed and the user only saw the conflict after a round-trip. The Save flow now re-runs the check synchronously and refuses with a clear notification + focuses the offending field.

## Documentation

- **README**, **docs/features.md**, **docs/INFO.md**, **docs/user-guide.md**, **docs/get-started.md**, **docs/README.md**, and **docs/mcp.md** updated so operator-facing text matches the above: Ontology **Designer**, Domain Cockpit **Active Version** vs loaded vs latest, **Registry → Browse** for MCP/API active version, new-domain loading overlay, Digital Twin path refresh on committed name/version changes, duplicate-name guard, and navbar identity refresh.
- **`docs/deployment.md`** rewritten for the current DAB: **`dev` / `dev-lakebase`** targets, correct **`bundle deployment bind`** / **`bundle run`** resource keys and app names, **`scripts/deploy.sh`** flags (no legacy `--all` / `--mcp-only`), **Lakebase** variable summary, **Step 5b** for **`bootstrap-lakebase-perms.sh`**, full deployment checklist, MCP and troubleshooting sections, and **§9 DAB reference** aligned with the **`Makefile`**.
- **README** Lakebase paragraph: documents **`lakebase_database_resource_segment`** and the `list-databases` lookup pattern.

## Tasks & Notifications

- Tasks panel now shows only currently running tasks; finished tasks are moved to the Notifications drawer.

## Backend & Databricks Apps bundle (operator-facing)

- Lakebase **registry** backend wired end-to-end (runtime + optional Volume toggle unchanged).
- **`databricks.yml`**: `ontobricks_dev_app` / `mcp_ontobricks_app` resource keys; workspace app names **`ontobricks-020`** and **`mcp-ontobricks`**; `dev-lakebase` target adds the Apps **`postgres`** resource whose `database` path ends with **`lakebase_database_resource_segment`** (`db-…` from the Postgres API `name` field).
- **`scripts/deploy.sh`**: default target **`dev-lakebase`**; **`APP_NAME`** set to **`ontobricks-020`** so post-deploy **`bootstrap-app-permissions.sh`** and **`bootstrap-lakebase-perms.sh`** resolve the correct service principal.
- **`scripts/bootstrap-lakebase-perms.sh`**: default Lakebase project **`ontobricks-app`**, default Postgres DB **`ontobricks_registry`** (dedicated `datname` aligned with the bundle bind), schema **`ontobricks_registry`**; default grantees **`ontobricks-020`** and **`mcp-ontobricks`**. Use **`-d databricks_postgres`** if the registry schema still lives in the shared default DB. Retarget with **`-i` / `-d` / `-s` / `-a`** when your workspace differs.
- **`scripts/bootstrap-app-permissions.sh`**: default app list **`ontobricks-020`** **`mcp-ontobricks`** (matches the bundle).

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

- **Databricks Apps sandbox name:** if you still point scripts or docs at **`ontobricks-dev`**, switch to **`ontobricks-020`** (the name in `databricks.yml` for `ontobricks_dev_app`) for `databricks apps get`, **`bootstrap-app-permissions.sh`**, and **`bootstrap-lakebase-perms.sh -a …`**, or pass **`-a`** explicitly.
- **Lakebase bundle variables:** the monolithic branch/database path variables are replaced by **`lakebase_project`**, **`lakebase_branch`**, **`lakebase_database_resource_segment`** (must be the **`db-…`** segment from **`databricks postgres list-databases "projects/<id>/branches/<branch>" -o json`** — the Apps API does not accept the Postgres **`datname`** as the path tail), and **`lakebase_registry_schema`**. After each **`dev-lakebase`** deploy, re-run **`scripts/bootstrap-lakebase-perms.sh`** (or **`make bootstrap-lakebase`**) if the postgres resource was rebound, so the app SP keeps **USAGE** on the registry schema.
- **`Makefile`:** **`make bootstrap-perms`** now passes **`ontobricks-020`** and **`mcp-ontobricks`**; **`make bootstrap-lakebase`** runs the Lakebase script with its built-in defaults (override with script flags when needed).
- First-time deploys benefit from the SP-self-permission bootstrap; re-run **`make bootstrap-perms`** if the app name changed or bootstrap was skipped.
- Viewer/Editor/Admin roles are now enforced in both the backend and the UI — verify your Teams matrix after upgrade if you rely on custom role assignments.
