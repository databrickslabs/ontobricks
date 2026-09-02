# Settings Lakehouse Health Permissions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Lakehouse Health validate only the application principal's operational Unity Catalog permissions on the configured Registry schema and standardize the Lakehouse, Lakebase, and Neo4j tab rails.

**Architecture:** Add a principal-filtered Unity Catalog effective-permissions REST client and a pure permission evaluator. Replace the domain-asset health payload and renderer with a schema-permission checklist, then wrap all three Settings tab groups in the canonical card-integrated `ob-tabs` structure.

**Tech Stack:** Python, FastAPI, Databricks Unity Catalog REST API, vanilla JavaScript, Jinja, Bootstrap 5, pytest, Playwright.

## Global Constraints

- Required permissions are exactly `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, `CREATE VIEW`, `SELECT`, and `MODIFY`.
- `ALL PRIVILEGES` satisfies every required permission.
- Inherited effective permissions are accepted.
- The request must be filtered to `DATABRICKS_CLIENT_ID` in Apps or the authenticated current user outside Apps.
- Health must not use the active domain, SQL Warehouse, or any table/view/object probe.
- Lakehouse, Lakebase, and Neo4j use `card h-100` → `card-body p-0 ob-tabs-wrap` → `nav nav-tabs ob-tabs nav-fill` → `tab-content p-3`.
- Use `uv run --frozen` for every pytest command.

---

### Task 1: Effective schema-permission client and evaluator

**Files:**
- Modify: `src/back/core/databricks/uc/UnityCatalog.py`
- Modify: `src/back/core/graphdb/delta/health.py`
- Modify: `tests/units/core/test_unity_catalog.py`
- Modify: `tests/units/graphdb/delta/test_delta_package.py`

**Interfaces:**
- Produces: `UnityCatalog.get_effective_schema_permissions(catalog, schema, principal) -> list[dict[str, str]]`.
- Produces: `schema_permission_summary(catalog, schema, principal, assignments) -> dict`.

- [ ] **Step 1: Add failing client tests**

Test that the REST call uses:

```python
assert request_url.endswith(
    "/api/2.1/unity-catalog/effective-permissions/SCHEMA/main.graph"
)
assert request_params == {"principal": "app-client-id"}
```

Cover privilege objects with `privilege`, `inherited_from_name`, and direct
string privileges. Assert HTTP 404/403 return no grants with a diagnostic and
other request failures propagate.

- [ ] **Step 2: Verify client RED**

```bash
uv run --frozen pytest -q tests/units/core/test_unity_catalog.py -k effective_schema
```

Expected: failure because the method does not exist.

- [ ] **Step 3: Implement the REST client**

Call the effective-permissions endpoint using `self._auth.host`,
`self._auth.get_auth_headers()`, `timeout=10`, and the exact principal query
parameter. Normalize the API response into:

```python
[
    {
        "privilege": "USE CATALOG",
        "inherited_from": "main",
    }
]
```

Return a result object containing `accessible`, `assignments`, and `error`;
404 and 403 are diagnostic results, while other request failures raise.

- [ ] **Step 4: Add failing evaluator tests**

Assert:

```python
summary = schema_permission_summary(
    "main",
    "graph",
    "app-client-id",
    [
        {"privilege": "USE_CATALOG", "inherited_from": "main"},
        {"privilege": "USE_SCHEMA", "inherited_from": ""},
    ],
)
assert summary["permissions"][0] == {
    "name": "USE CATALOG",
    "granted": True,
    "inherited_from": "main",
}
assert summary["operational"] is False
```

Also cover all six grants, `ALL_PRIVILEGES`, underscore/space normalization,
deterministic required-order output, and inherited sources.

- [ ] **Step 5: Verify evaluator RED**

```bash
uv run --frozen pytest -q tests/units/graphdb/delta/test_delta_package.py -k schema_permission
```

- [ ] **Step 6: Implement the pure evaluator**

Declare:

```python
REQUIRED_SCHEMA_PERMISSIONS = (
    "USE CATALOG",
    "USE SCHEMA",
    "CREATE TABLE",
    "CREATE VIEW",
    "SELECT",
    "MODIFY",
)
```

Normalize privilege names, expand `ALL PRIVILEGES`, preserve inherited source,
and return one row in required order plus `operational`.

- [ ] **Step 7: Verify Task 1 GREEN**

Run both Task 1 commands and expect all selected tests to pass.

- [ ] **Step 8: Commit**

```bash
git add src/back/core/databricks/uc/UnityCatalog.py src/back/core/graphdb/delta/health.py tests/units/core/test_unity_catalog.py tests/units/graphdb/delta/test_delta_package.py
git commit -m "feat(settings): evaluate Lakehouse schema permissions"
```

---

### Task 2: Domain-independent Lakehouse Health endpoint

**Files:**
- Modify: `src/back/objects/domain/SettingsService.py`
- Modify: `src/api/routers/internal/settings.py`
- Create: `tests/units/settings/test_delta_health_permissions.py`

**Interfaces:**
- Consumes: `get_effective_schema_permissions(...)` and `schema_permission_summary(...)`.
- Produces: `SettingsService.triple_store_databricks_health_result(...)` with permission-only fields.

- [ ] **Step 1: Write failing service and route tests**

Assert the service:

```python
assert result.keys() >= {
    "success",
    "registry_configured",
    "registry_catalog",
    "registry_schema",
    "storage_location",
    "principal",
    "accessible",
    "operational",
    "permissions",
    "error",
}
assert "active_domain" not in result
assert "view_fqn" not in result
assert "data_table" not in result
assert "warehouse_id" not in result
```

Patch `resolve_app_registry_context`, prove `get_domain`,
`create_databricks_client`, and table-naming helpers are never called, and
assert the effective-permissions client receives the application client ID.
Cover local current-user fallback, missing Registry configuration avoiding the
REST call, missing grants as HTTP 200 diagnostics, and request failures mapped
to `InfrastructureError`.

- [ ] **Step 2: Verify RED**

```bash
uv run --frozen pytest -q tests/units/settings/test_delta_health_permissions.py
```

- [ ] **Step 3: Implement the service**

Use `resolve_app_registry_context(settings)`, instantiate
`DatabricksClient(host=host, token=token)` without a warehouse, resolve:

```python
principal = (
    client.auth.client_id
    or client.workspace.get_current_user_email()
    or ""
).strip()
```

Then call the Task 1 client and evaluator. Return a configured-false diagnostic
without constructing a client when catalog/schema is absent.

- [ ] **Step 4: Align the route**

Update the route docstring to describe Registry schema effective permissions.
Keep `map_route_errors` so propagated network/API failures retain centralized
error handling.

- [ ] **Step 5: Verify GREEN**

Run the Task 2 command and expect all tests to pass.

- [ ] **Step 6: Commit**

```bash
git add src/back/objects/domain/SettingsService.py src/api/routers/internal/settings.py tests/units/settings/test_delta_health_permissions.py
git commit -m "feat(settings): report Lakehouse schema permission health"
```

---

### Task 3: Permission Health UI and canonical tab rails

**Files:**
- Modify: `src/front/templates/settings.html`
- Modify: `src/front/static/config/js/settings.js`
- Create: `tests/units/front/test_settings_lakehouse_health_permissions.py`

**Interfaces:**
- Consumes: the Task 2 permission-only response.
- Produces: permission checklist UI and canonical tab markup.

- [ ] **Step 1: Write failing UI tests**

Assert the Health pane copy mentions effective permissions on Registry
`catalog.schema`, and does not mention an active domain, VIEW, `_data`, assets,
or row counts.

Extract `loadDeltaTripleStoreHealth` and assert it reads `permissions`,
`principal`, `operational`, and `inherited_from`, while it does not read:

```python
for obsolete in (
    "active_domain",
    "view_fqn",
    "data_table_fqn",
    "inferred_table_fqn",
    "materialization",
    "warehouse_id",
):
    assert obsolete not in health_function
```

Parse the three tab groups and assert each has the exact canonical hierarchy
and `nav-fill`, while `ob-tab-content` is absent.

- [ ] **Step 2: Verify RED**

```bash
uv run --frozen pytest -q tests/units/front/test_settings_lakehouse_health_permissions.py
```

- [ ] **Step 3: Update Lakehouse Health markup**

Use the approved copy and preserve the existing Health IDs, Refresh action,
loading spinner, tab IDs, roles, and ARIA attributes.

- [ ] **Step 4: Render permission-only results**

Build an escaped definition list for Registry and Principal, then one status
row per required permission. Use Bootstrap Icons plus success/danger text, and
show inherited source when present. The overall state must include icon and
label and must not rely on color alone.

- [ ] **Step 5: Apply canonical card-integrated tabs**

Wrap Lakehouse, Lakebase, and Neo4j tab groups in:

```html
<div class="card h-100">
    <div class="card-body p-0 ob-tabs-wrap">
        <ul class="nav nav-tabs ob-tabs nav-fill">...</ul>
        <div class="tab-content p-3">...</div>
    </div>
</div>
```

Keep the Lakebase provision modal outside the tab card.

- [ ] **Step 6: Verify GREEN**

Run the Task 3 command and the existing Settings frontend/settings tests:

```bash
uv run --frozen pytest -q tests/units/front/test_settings_lakehouse_health_permissions.py tests/units/settings/test_delta_warehouse_config.py tests/units/settings/test_graph_engine_config.py
```

- [ ] **Step 7: Commit**

```bash
git add src/front/templates/settings.html src/front/static/config/js/settings.js tests/units/front/test_settings_lakehouse_health_permissions.py
git commit -m "feat(settings): focus Lakehouse Health on schema grants"
```

---

### Task 4: Documentation, browser validation, and delivery

**Files:**
- Modify: `docs/user-guide.md`
- Create or modify: `changelogs/v0.8.0/benoitcayladbx_2026-09-02.log`

- [ ] **Step 1: Update user documentation**

Describe the six operational schema permissions, domain-independent Health
behavior, and the distinction between Health permission checks and Objects
asset inventory.

- [ ] **Step 2: Run focused and full tests**

```bash
uv run --frozen pytest -q tests/units/core/test_unity_catalog.py tests/units/graphdb/delta/test_delta_package.py tests/units/settings/test_delta_health_permissions.py tests/units/front/test_settings_lakehouse_health_permissions.py
uv run --frozen pytest -q -m "not scenario"
```

- [ ] **Step 3: Browser-test Settings**

At desktop and 390 px, verify Lakehouse Health displays only Registry schema
permissions, refresh works, no domain assets appear, the three tab rails use
the same underline style and remain keyboard-operable, content stays
contained, and no console/network errors occur.

- [ ] **Step 4: Write the changelog**

Record context, numbered file changes, modified files, focused/full test
results, and browser evidence in English under v0.8.0.

- [ ] **Step 5: Commit delivery files**

```bash
git add docs/user-guide.md changelogs/v0.8.0/benoitcayladbx_2026-09-02.log
git commit -m "docs(settings): document schema permission health"
```

---

### Task 5: Canonical Runs and system Health tab rails

**Files:**
- Modify: `src/front/templates/partials/settings/_settings_runs.html`
- Modify: `src/front/templates/partials/settings/_settings_health.html`
- Modify: `src/front/static/config/js/settings.js`
- Modify: `tests/units/front/test_settings_lakehouse_health_permissions.py`
- Modify: `tests/units/front/test_settings_runs_page.py`

**Interfaces:**
- Consumes: shared `ob-tabs` card-integrated markup and Settings tab
  auto-scroll behavior from Task 3.
- Produces: matching Runs and system Health tab rails.

- [ ] **Step 1: Write failing structural and behavior tests**

Assert both partials use:

```html
<div class="card h-100">
    <div class="card-body p-0 ob-tabs-wrap">
        <ul class="nav nav-tabs ob-tabs nav-fill">
        <div class="tab-content p-3">
```

Assert `ob-tab-content`, `text-primary`, and `text-success` are absent from
their tab rails; existing tab/pane IDs remain; and the Settings auto-scroll
rail list includes `settingsRunsTabs` and `healthTabs`.

- [ ] **Step 2: Verify RED**

```bash
uv run --frozen pytest -q tests/units/front/test_settings_lakehouse_health_permissions.py tests/units/front/test_settings_runs_page.py
```

- [ ] **Step 3: Wrap Runs tabs**

Keep the Domain filter above the card. Wrap the existing Runs rail and body in
`card h-100` and `card-body p-0 ob-tabs-wrap`, add `nav-fill`, change the body
to `tab-content p-3`, and let both tab icons inherit shared colors.

- [ ] **Step 4: Wrap system Health tabs**

Wrap the existing Databricks/Diagnostics rail and body in the same canonical
structure, add `nav-fill`, and change the body to `tab-content p-3`. Preserve
all readiness/diagnostic IDs, lazy loading, refresh actions, and accessibility
attributes.

- [ ] **Step 5: Extend keyboard auto-scroll**

Add `settingsRunsTabs` and `healthTabs` to the existing Settings-only rail list
used by the `shown.bs.tab` and `focusin` nearest-scroll handlers.

- [ ] **Step 6: Verify GREEN and full suite**

```bash
uv run --frozen pytest -q tests/units/front/test_settings_lakehouse_health_permissions.py tests/units/front/test_settings_runs_page.py
uv run --frozen pytest -q -m "not scenario"
```

- [ ] **Step 7: Browser-test**

At desktop and 390 px, verify both rails match Lakehouse/Lakebase/Neo4j,
Bootstrap switching and lazy loading still work, keyboard focus remains
visible, tables/diagnostics stay contained, and no console/network errors
occur.

- [ ] **Step 8: Append changelog and commit**

Append a new section to the existing v0.8.0 daily changelog and commit:

```bash
git add src/front/templates/partials/settings/_settings_runs.html src/front/templates/partials/settings/_settings_health.html src/front/static/config/js/settings.js tests/units/front/test_settings_lakehouse_health_permissions.py tests/units/front/test_settings_runs_page.py changelogs/v0.8.0/benoitcayladbx_2026-09-02.log docs/superpowers/specs/2026-09-02-settings-lakehouse-health-permissions-design.md docs/superpowers/plans/2026-09-02-settings-lakehouse-health-permissions.md
git commit -m "feat(settings): align Runs and Health tabs"
```
