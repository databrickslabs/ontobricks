# OntoBricks — Release Notes V0.5.2

**Release date:** 2026-06-22
**Type:** Patch — two targeted fixes
**Test status:** 2283 passing, 15 skipped (unit tier).

---

## Summary

v0.5.2 is a stability patch with two independent fixes:

1. **Deterministic R2RML serialization** — mapping exports are now stable across runs, making diff-based version control and automated pipelines reliable.
2. **MCP server cold-start resilience** — the MCP server no longer fails on the first tool call when Databricks Apps is waking from idle; it retries transparently.

No schema changes. No configuration changes. No migration scripts required.

---

## Fixes

### 1. Deterministic R2RML serialization

**Symptom:** Two successive exports of the same domain mapping produced R2RML files with attributes in a different order, making version diffs noisy and breaking any pipeline that checksummed the output.

**Root cause:** `R2RMLGenerator._add_entity_mapping()` iterated over `attribute_mappings` (a plain `dict`) without sorting, so insertion order determined output order — non-deterministic across Python versions and dict mutations.

**Fix:** `attribute_mappings.items()` is now wrapped in `sorted()`, producing stable alphabetical order.

**File changed:** `src/back/core/w3c/r2rml/R2RMLGenerator.py`
**Tests updated:** `tests/units/mapping/test_r2rml_generator.py` — assertions updated to match sorted order.

---

### 2. MCP server retry on 502/503 (Databricks Apps cold-start)

**Symptom:** The first MCP tool call after an idle period failed immediately with a 502 or 503 error. The MCP client received a hard failure instead of waiting for the app to wake up.

**Root cause:** `_get` and `_post` in the MCP server called `raise_for_status()` immediately on any non-2xx response, including transient 502/503 responses emitted by the Databricks Apps proxy during cold-start.

**Fix:** Both `_get` and `_post` now retry up to 3 times on 502/503, with progressive back-off (5 s → 10 s → 20 s). Each retry is logged at `WARNING` level. If all three retries are exhausted the final response still raises via `raise_for_status()`.

**File changed:** `src/mcp-server/server/app.py`

---

## Upgrade Notes

### New deploys (v0.5.2 from scratch)

No action required. Both fixes are code-only with no schema or configuration changes.

### Upgrading from v0.5.1

No migration needed. Deploy the new app bundle — both fixes take effect immediately on restart.

### Upgrading from v0.5.0

Apply the v0.5.1 upgrade steps first (no schema migration required), then deploy v0.5.2.

---

## Changes

| Area | File | Change |
|------|------|--------|
| Mapping | `src/back/core/w3c/r2rml/R2RMLGenerator.py` | `sorted()` on `attribute_mappings.items()` for deterministic output |
| Tests | `tests/units/mapping/test_r2rml_generator.py` | Assertions aligned with sorted attribute order |
| MCP server | `src/mcp-server/server/app.py` | Retry loop (3 attempts, 5/10/20 s back-off) on 502/503 in `_get` and `_post` |
| Version | `pyproject.toml` | Bumped to `0.5.2` |

---

## What is NOT changed

- Registry schema — no DDL.
- All v0.5.1 features — fully intact.
- R2RML semantic content — only attribute ordering is affected; all triples and mappings are identical.
- MCP tool contracts — no API surface change; retry is fully transparent to callers.
