# E2E Tests — `tests/e2e/`

Playwright-based browser tests (Layer 2) that run against a live Uvicorn
server.  Every test interacts with the application exactly as a browser
would — real HTTP, real DOM, real JavaScript execution.

## How it works

`conftest.py` spins up a fresh Uvicorn subprocess on **port 18765** before
the session starts and tears it down afterwards.  The subprocess inherits
mock Databricks credentials so no real cloud connectivity is required:

```
DATABRICKS_HOST=https://test.databricks.com
DATABRICKS_TOKEN=test-token
DATABRICKS_SQL_WAREHOUSE_ID=test-warehouse
```

> **Port conflict** — if port 18765 is already in use (e.g. a stale process
> from a previous run), every test is skipped.  Run
> `lsof -i :18765` and kill the offender before re-running.

### Fixtures

| Fixture | Scope | Purpose |
|---|---|---|
| `_set_env` | session | Injects mock env vars before the server starts |
| `live_server` | session | Starts Uvicorn; yields the base URL `http://localhost:18765` |
| `browser_instance` | session | Launches a headless Chromium browser (Playwright) |
| `page` | function | Creates a fresh browser context + page per test |

## Running

```bash
# Full E2E suite
.venv/bin/python -m pytest tests/e2e/ -v

# Single sub-suite
.venv/bin/python -m pytest tests/e2e/ontology/ -v
.venv/bin/python -m pytest tests/e2e/security/ -v
```

---

## Directory layout

```
tests/e2e/
├── conftest.py          — server fixture, browser fixture, page fixture
├── navigation/          — top-level routing, home page, about, URI resolve
├── settings/            — settings page UI + write / mutation API contracts
├── mapping/             — mapping page sidebar navigation
├── domain/              — domain page sidebar navigation
├── dtwin/               — digital twin sidebar (basic + full section parity)
├── registry/            — registry page smoke tests
├── help/                — help center modal + /api/help/docs endpoint
├── security/            — CSRF enforcement, permission middleware, access-denied
└── ontology/            — ontology feature (sidebar + CRUD + import + OWL export)
```

---

## Sub-suites

### `navigation/` — 15 tests

| File | Class | What it tests |
|---|---|---|
| `test_navigation_flows.py` | `TestNavigation` | All 6 top-level routes load with correct `<title>`; navbar brand goes home; Settings nav-link works |
| | `TestHomePage` | Hero visible, domain panel present, 3 workflow cards, stat counters (`#classCount`, `#propCount`, `#mappingCount`) |
| | `TestAboutPage` | Body contains "OntoBricks" and "R2RML" |
| `test_resolve_flows.py` | `TestResolveRoute` | `/resolve` without URI → 4xx; `?uri=...` → not 5xx; path variant → not 5xx |

### `settings/` — 8 tests

| File | Class | What it tests |
|---|---|---|
| `test_settings_flows.py` | `TestSettingsPage` | Databricks tab visible, host display present, base-URI field non-empty, Save button enabled |
| `test_write_flows.py` | `TestSettingsSaveSessionOnly` | `POST /settings/save` host round-trips through `GET /settings/current` |
| | `TestSettingsSaveRouteContracts` | Route-contract checks (route mounted, CSRF honoured, error JSON-shaped) for `save-base-uri`, `set-default-emoji`, `save-registry-cache-ttl` |

### `mapping/` — 6 tests

| File | Class | What it tests |
|---|---|---|
| `test_mapping_flows.py` | `TestMappingSidebar` | All 6 sections (`information`, `design`, `manual`, `autoassign`, `r2rml`, `sparksql`) become visible via `SidebarNav.switchTo()` |

### `domain/` — 6 tests

| File | Class | What it tests |
|---|---|---|
| `test_domain_flows.py` | `TestDomainSidebar` | All 6 sections (`information`, `metadata`, `documents`, `validation`, `owl-content`, `r2rml`) switch correctly; CSS `pointer-events:none` bypassed via JS |

### `dtwin/` — 9 tests

Merges the two previously separate Digital Twin test files.

| File | Class | What it tests |
|---|---|---|
| `test_dtwin_flows.py` | `TestDigitalTwinSidebar` | Sigma-graph section visible by default; Knowledge Graph sidebar link present |
| | `TestDigitalTwinSidebarParity` | All 6 sidebar sections (`insight`, `dataquality`, `reasoning`, `sigmagraph`, `graphql`, `chat`) reachable via `SidebarNav`; chat panel has an input element |

### `registry/` — 4 tests

| File | Class | What it tests |
|---|---|---|
| `test_registry_flows.py` | `TestRegistryPage` | `/registry/` loads; body non-empty; shared navbar renders; registry link exists on home page |

### `help/` — 8 tests

| File | Class | What it tests |
|---|---|---|
| `test_help_modal_flows.py` | `TestHelpModalPresence` | `#helpCenterToggle` visible; `#helpModal` in DOM; toggle adds `.show`; `#help-welcome` active by default |
| | `TestHelpDocsApi` | `GET /api/help/docs` → JSON `{categories}`; first catalogued slug fetches to 200; unknown slug → 404; path-traversal → 404 |

### `security/` — 9 tests

The E2E server subprocess starts **without** `CSRF_DISABLED` so CSRF is live.

| File | Class | What it tests |
|---|---|---|
| `test_access_denied_flows.py` | `TestAccessDeniedPage` | Default reason → 200; known reasons `app`, `domain`, `bootstrap` → 200; unknown reason `wizardly` → 200 (not 500) |
| `test_permissions_flows.py` | `TestCsrfEnforcement` | POST with wrong `X-CSRF-Token` → 403 `{"error":"csrf"}`; `/api/*` prefix is CSRF-exempt |
| | `TestPermissionMiddlewareShape` | `/access-denied` reachable; `/settings` accessible when no `DATABRICKS_APP_PORT` (admin default) |

### `ontology/` — 102 tests

Deep tests for the Ontology feature.

> **Session setup pattern** — `add_class` / `add_property` generate empty
> URIs when no `base_uri` is configured.  Multi-step tests that need a real
> URI seed the session via a single `POST /ontology/import-owl` call with an
> inline Turtle snippet.

| File | Tests | What it tests |
|---|---|---|
| `test_sidebar_flows.py` | 12 | All 11 sidebar sections switch correctly via click; wizard select-all checkbox present |
| `test_information_flows.py` | 22 | DOM fields; `load` API contract; `save` base_uri round-trip; `reset` clears session |
| `test_class_crud_flows.py` | 15 | Entities DOM; `add_class` → load; `update` / `delete` by imported URI; `get-loaded-ontology` |
| `test_property_crud_flows.py` | 15 | Relationships DOM; `add_property` → load; `update` / `delete` by imported URI |
| `test_import_flows.py` | 22 | Import tabs DOM (5 tabs + file inputs + industry checkboxes); `import-owl` session effect; `parse-owl` response shape; industry catalog APIs |
| `test_owl_export_flows.py` | 20 | OWL section DOM buttons; `export-owl` empty→404/import→Turtle; `generate-owl` inline config; full import→export round-trip |

---

## Summary

| Sub-suite | Tests |
|---|---|
| `navigation/` | 15 |
| `settings/` | 8 |
| `mapping/` | 6 |
| `domain/` | 6 |
| `dtwin/` | 9 |
| `registry/` | 4 |
| `help/` | 8 |
| `security/` | 9 |
| `ontology/` | 102 |
| **Total** | **170** |
