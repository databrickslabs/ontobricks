# Lakebase Bulk Loading `sync_mode` Round-Trip — Implementation Plan

> **For agentic workers:** Execute task-by-task. Steps use checkbox syntax.

**Goal:** Add a regression test that `app_managed` ↔ `managed_synced` survive `GlobalConfigService` set → fake Lakebase store → get.

**Architecture:** Patch `_store_for` with an in-memory `global_config` dict merger (mirrors Lakebase JSONB). No production code changes unless the test fails.

**Tech Stack:** pytest, unittest.mock, GlobalConfigService

## Global Constraints

- Tests only under `tests/units/settings/test_graph_engine_config.py`
- No UI or SettingsService changes
- Run with `uv run --frozen pytest`

---

### Task 1: Regression test for sync_mode round-trip

**Files:**
- Modify: `tests/units/settings/test_graph_engine_config.py`
- Spec: `documentation/superpowers/specs/2026-08-04-lakebase-bulk-loading-sync-mode-registry-design.md`

**Steps:**

- [x] Add `_FakeGlobalConfigStore` with `load_global_config` / `save_global_config` (dict merge) and `backend = "lakebase"`
- [x] Add `TestBulkLoadingSyncModeRegistryRoundTrip` with one test that:
  1. sets nested/flat-accepted `managed_synced` + options
  2. asserts `get_graph_engine_config(...).lakebase.sync_mode == "managed_synced"` and options present
  3. sets `app_managed`
  4. asserts get returns `app_managed`
  5. sets `managed_synced` again and asserts get
- [x] Run `uv run --frozen pytest -q tests/units/settings/test_graph_engine_config.py -k sync_mode`
- [x] Changelog under `changelogs/v0.7.0/benoitcayladbx_2026-08-04.log`
- [x] Full suite: `uv run --frozen pytest -q -m "not scenario"`

**Done when:** New test passes; full non-scenario suite green.
