# Lakebase Bulk Loading `sync_mode` — Registry Round-Trip

**Date:** 2026-08-04  
**Status:** Approved  
**Scope:** Regression test only (no product behaviour change)

## Problem

Settings → Lakebase → Bulk loading exposes **App-managed** vs **Managed sync**
(`sync_mode`: `app_managed` | `managed_synced`). Operators need confidence that
the choice is actually persisted in the Lakebase registry `global_config`
JSONB and returned on the next Settings load.

Live probing (2026-08-04) confirmed the path already works. Existing unit
tests assert that `set_graph_engine_config` *writes* nested sync keys into
the `_save` payload, but they mock `_save` and do **not** assert a full
`set` → store → `get` round-trip for mode flips.

## Goal

Lock the contract with a regression test:

1. Persist `managed_synced` (+ managed options) via `GlobalConfigService.set_graph_engine_config`
2. Read it back via `get_graph_engine_config` under `lakebase.sync_mode`
3. Flip to `app_managed`, read back
4. Flip back to `managed_synced`, read back

## Design

- **Where:** `tests/units/settings/test_graph_engine_config.py`
- **How:** In-test fake store with `load_global_config` / `save_global_config`
  that merge into a dict (same merge semantics as Lakebase JSONB). Patch
  `GlobalConfigService._store_for` to return it.
- **Out of scope:** UI/JS changes, live Postgres integration, other Bulk
  loading fields beyond what proves mode persistence.

## Success criteria

- New test passes under `uv run --frozen pytest -q -m "not scenario"`
- No application code changes required unless the test reveals a real gap
