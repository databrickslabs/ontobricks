# OntoBricks — Release Notes V0.5.1

**Release date:** 2026-06-19
**Type:** Patch — single bug fix
**Test status:** 2484 passing, 15 skipped on the unit tier; 6 new regression tests added.

---

## Summary

v0.5.1 is a targeted patch that fixes a blocker introduced with the v0.5.0 review workflow: the **"Submit for Review"** action on the Validation page was permanently blocked even after a successful Digital Twin build, when that build was triggered interactively (via the Build button in the UI or the external REST endpoint `POST /dtwin/build`).

No schema changes. No configuration changes. No migration scripts required for new deploys.

---

## Bug Fixed

### Submit for Review blocked despite a built Digital Twin

**Symptom:** On the Validation page (`/domain/validate`), the banner *"This version has never been built. Run a Digital Twin build first."* appeared and the **Submit for Review** button was disabled — even though the Consistency-checks panel showed a green **"Digital Twin built"** tick.

**Root cause:** The two indicators on the same page read from different sources:

| Indicator | Source |
|-----------|--------|
| Consistency-checks "Digital Twin built" ✅ | Live triple-store state (view exists + has triples) |
| Submit-for-Review gate | `domain_versions.last_build` column in the registry DB |

The interactive build path (`_BuildPipeline._complete_task`) never wrote `domain.last_build` to the `domain_versions` table. Only the **scheduled** build path (`scheduler._persist_domain_metadata`) performed that write. The Submit gate reads the registry column, found it empty, and blocked.

**Fix:** `_BuildPipeline._complete_task()` — the single success exit shared by both the UI build (`build_kind="session"`) and the REST build (`build_kind="api"`) — now calls a new `_persist_last_build_to_registry()` method immediately after recording the build run. This method:

1. Resolves the domain folder and version (same logic already used for build-run tracing).
2. Stamps `domain.last_build` with the current UTC timestamp when it is empty (API path).
3. Calls `RegistryService.from_context(domain, settings)._store.write_version(folder, version, domain_data)` to flush the value to `domain_versions.last_build`.
4. Is **best-effort**: any exception is logged as a warning and never propagates — a registry hiccup cannot break a build that otherwise succeeded.

**File changed:** `src/back/objects/digitaltwin/_build_pipeline.py`
**Tests added:** `tests/back/core/digitaltwin/test_build_pipeline_units.py::TestPersistLastBuildToRegistry` (6 cases)

---

## Upgrade Notes

### New deploys (v0.5.1 from scratch)

No action required. The fix is code-only; the `domain_versions` schema and `last_build` column are unchanged from v0.5.0.

### Upgrading from v0.5.0

No schema migration needed. However, any domain version that was built interactively while running v0.5.0 will have an empty `domain_versions.last_build` and will still be blocked at Submit for Review after the upgrade.

**Two options to unblock existing affected versions:**

**Option A — Re-run the build (recommended, zero SQL)**

On the Validation page for the affected domain + version, click the **Build** button again. The build re-populates the triple-store (idempotent — full rebuild) and now also writes `last_build` to the registry. Once the build completes, the Submit for Review button becomes available immediately.

**Option B — Direct SQL patch (no rebuild needed)**

If you want to unblock Submit for Review without re-running the build (e.g. the triple-store is already healthy and you do not want to re-build), connect to the registry Lakebase database and run:

```sql
-- Replace 'your_schema' with your registry schema (e.g. ontobricks_app_demo).
-- Replace 'supplychain' and '1' with your actual domain folder and version.
UPDATE your_schema.domain_versions dv
SET    last_build  = NOW()::text,
       updated_at  = NOW()
FROM   your_schema.domains d
WHERE  dv.domain_id = d.id
  AND  d.folder     = 'supplychain'   -- domain folder (sanitised name)
  AND  dv.version   = '1'             -- version string
  AND  dv.last_build = '';            -- only patch truly empty rows
```

To patch **all** versions that have a healthy build artifact but a missing `last_build` in one shot:

```sql
-- Patches every version whose last_build is empty but whose status is not DRAFT
-- (i.e. it was previously submitted or published via a scheduled build workaround).
-- Review the SELECT before running the UPDATE.
SELECT d.folder, dv.version, dv.status, dv.last_build
FROM   your_schema.domain_versions dv
JOIN   your_schema.domains         d  ON d.id = dv.domain_id
WHERE  dv.last_build = '';

-- Once satisfied, run:
UPDATE your_schema.domain_versions dv
SET    last_build = NOW()::text,
       updated_at = NOW()
FROM   your_schema.domains d
WHERE  dv.domain_id = d.id
  AND  dv.last_build = '';
```

After the SQL update, reload the Validation page — no app restart is needed.

---

## Changes

| Area | File | Change |
|------|------|--------|
| Core fix | `src/back/objects/digitaltwin/_build_pipeline.py` | Added `_persist_last_build_to_registry()`; called from `_complete_task()` |
| Tests | `tests/back/core/digitaltwin/test_build_pipeline_units.py` | Added `TestPersistLastBuildToRegistry` — 6 regression tests |

---

## What is NOT changed

- `domain_versions` schema — no DDL.
- Scheduled build path — `scheduler._persist_domain_metadata` is untouched and still the authoritative write for scheduled runs.
- Consistency-checks panel — continues to read live triple-store state (unchanged behaviour).
- All other v0.5.0 features — fully intact.
