# Build and Query Warehouse Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route Knowledge Graph DDL and materialization through an explicitly
configured non-RT build warehouse while preserving Lakehouse//RT for graph
queries.

**Architecture:** Reuse the existing global `warehouse_id` as the dedicated
build warehouse, add an independent persisted build transport flag, and expose
both in Settings. Build pipelines consume only build settings; graph query
clients continue consuming `graph_engine_config.lakehouse`.

**Tech Stack:** Python 3.10+, FastAPI, Databricks SQL Connector, Jinja2,
vanilla JavaScript, Bootstrap 5, pytest.

## Global Constraints

- Modify only the `0.9.0` worktree.
- Do not route build DDL to a Lakehouse//RT (`REYDEN`) warehouse.
- Do not fall back from a missing build warehouse to the query warehouse.
- Preserve existing saved global warehouse IDs as build warehouse IDs.
- Follow TDD and run `uv run --frozen pytest -q -m "not scenario"`.

---

### Task 1: Persist and resolve build warehouse configuration

**Files:**
- Modify: `src/back/objects/session/GlobalConfigService.py`
- Modify: `src/back/core/helpers/DatabricksHelpers.py`
- Test: `tests/units/settings/test_delta_warehouse_config.py`

**Interfaces:**
- Produces: `GlobalConfigService.get_build_warehouse_use_sea(...) -> bool`
- Produces: `DatabricksHelpers.resolve_build_warehouse_id(...) -> str`
- Produces: `DatabricksHelpers.resolve_build_use_sea(...) -> bool`
- Produces: `DatabricksHelpers.get_build_sql_credentials(...) -> tuple[str, str, str]`

- [ ] Add tests proving the build warehouse uses only the persisted global
  `warehouse_id`, returns empty when absent, and resolves its own transport
  flag independently from `lakehouse.use_sea`.
- [ ] Run the focused tests and confirm they fail because the build-specific
  interfaces do not exist.
- [ ] Implement the minimal getters and resolvers without a query-warehouse
  fallback.
- [ ] Run the focused tests and confirm they pass.

### Task 2: Route both build pipelines through build settings

**Files:**
- Modify: `src/back/objects/digitaltwin/_build_pipeline.py`
- Modify: `src/back/core/graphdb/delta/DeltaTripleStoreBuildPipeline.py`
- Test: `tests/back/core/digitaltwin/test_build_pipeline_units.py`
- Test: `tests/units/graphdb/delta/test_delta_build_pipeline.py`

**Interfaces:**
- Consumes: `resolve_build_use_sea(domain, settings) -> bool`
- Consumes: `get_build_sql_credentials(domain, settings) -> tuple[str, str, str]`

- [ ] Add tests proving both clients receive the build transport rather than
  the Lakehouse query transport.
- [ ] Add tests proving a missing build warehouse produces an actionable
  validation failure before SQL execution.
- [ ] Run the focused tests and confirm the expected failures.
- [ ] Update both build paths to use the dedicated build resolvers.
- [ ] Run the focused tests and confirm they pass.

### Task 3: Expose coordinated Build and Query controls in Settings

**Files:**
- Modify: `src/front/templates/settings.html`
- Modify: `src/front/static/config/js/settings.js`
- Modify: `src/back/objects/domain/SettingsService.py`
- Test: `tests/units/settings/test_delta_warehouse_config.py`
- Test: appropriate Settings frontend contract tests under `tests/units/front/`

**Interfaces:**
- Persists: top-level `warehouse_id`
- Persists: top-level `warehouse_use_sea`
- Preserves: `graph_engine_config.lakehouse.warehouse_id`
- Preserves: `graph_engine_config.lakehouse.use_sea`

- [ ] Add structural tests for separate Build and Lakehouse Query controls.
- [ ] Add service tests rejecting a `REYDEN` build warehouse and accepting a
  standard warehouse.
- [ ] Add tests requiring a distinct query warehouse when RT is enabled and
  clearing the query override when RT is disabled.
- [ ] Run focused tests and confirm they fail.
- [ ] Show an editable non-RT Build dropdown in the Lakehouse panel and persist
  it through Apply; mirror it into a disabled Query dropdown while RT is off.
- [ ] Enable the Query dropdown only when RT is checked, populate it from the
  same workspace list as Build, and require a distinct warehouse.
- [ ] Show the existing Lakehouse configuration spinner while the SQL
  Warehouse tab populates both selectors.
- [ ] Persist build transport separately and prevent RT selections.
- [ ] Clear the saved query override when RT is unchecked and applied.
- [ ] Run focused tests and confirm they pass.

### Task 4: Documentation, changelog, and verification

**Files:**
- Modify: user documentation describing warehouse configuration
- Modify: `changelogs/v0.9.0/benoitcayladbx_2026-09-10.log`

**Interfaces:**
- Documents the build/query warehouse responsibilities and migration behavior.

- [ ] Update relevant user documentation with the two warehouse roles.
- [ ] Run IDE diagnostics for changed files and fix introduced findings.
- [ ] Run `uv run --frozen pytest -q -m "not scenario"` and capture the exact
  result.
- [ ] Add the English changelog section with modified files and test result.
- [ ] Review the final diff for scope and preservation of pre-existing changes.
