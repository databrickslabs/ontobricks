# Editor and Builder Data Sources Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let Editors and Builders browse Unity Catalog locations and manage domain data sources without `CAN_MANAGE`.

**Architecture:** Extend the existing method-aware Settings exception map with catalog and schema read endpoints. Domain metadata mutations continue through the existing domain-role middleware.

**Tech Stack:** Python, FastAPI/Starlette middleware, pytest.

## Global Constraints

- Only `GET` catalog/schema browsing becomes available to non-admin app users.
- Settings writes and SQL Warehouse selection remain admin-only.
- Viewers remain unable to mutate `/domain/metadata/*`.
- Use `uv run --frozen` for every pytest invocation.

---

### Task 1: Permission middleware regression

**Files:**
- Modify: `tests/units/auth/test_permission_middleware.py`
- Modify: `src/shared/fastapi/main.py`

**Interfaces:**
- Consumes: `_PERM_ADMIN_ONLY_EXCEPTIONS`
- Produces: method-aware access to catalog and schema browse routes

- [ ] Add `GET /settings/catalogs`, `GET /settings/schemas`, and
  `GET /settings/schemas/main` to the non-admin read regression cases.
- [ ] Run the focused test and confirm it fails because these routes are
  currently admin-only:
  `uv run --frozen pytest -q tests/units/auth/test_permission_middleware.py -k settings_read_only_exceptions`
- [ ] Add the exact routes plus a method-aware `/settings/schemas/` prefix
  exception in `PermissionMiddleware`.
- [ ] Run the focused middleware tests and confirm they pass.

### Task 2: Documentation and verification

**Files:**
- Modify: `documentation/deployment.md`
- Modify: `changelogs/v0.8.0/benoitcayladbx_2026-08-27.log`

**Interfaces:**
- Consumes: the implemented permission behavior
- Produces: operator guidance and release traceability

- [ ] Document that Editors and Builders manage domain data sources while
  shared Settings remain administrator-owned.
- [ ] Add the required English v0.8.0 changelog section.
- [ ] Run `uv run --frozen pytest -q -m "not scenario"` and record the exact
  result in the changelog.
- [ ] Check diagnostics for all modified source and test files.
