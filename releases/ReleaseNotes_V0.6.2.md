# OntoBricks — Release Notes V0.6.2

**Release date:** 2026-07-14
**Type:** Patch release (v0.6.1 → v0.6.2)
**Test status:** 3007 passed, 275 skipped, 5 deselected (`uv run pytest -q -m "not scenario"`).

---

## Summary

v0.6.2 is a focused patch on top of v0.6.1. It hardens **Lakebase Knowledge Graph
sync** for long literal values and Lakeflow-managed tables, fixes **Claude serving
endpoint** responses that broke Ontology Wizard and Auto-Map, speeds up **Graph
Explorer / Graph Chat** alias lookups on large graphs, and polishes the **edit-lock
idle UX** (resume control moves into the L2 subnav). Developer ergonomics improve
with **hermetic unit tests** (`.env` Lakebase vars no longer leak into pytest) and
**PAT-based deploy** when the CLI profile is intentionally left empty.

No breaking API changes. One **operational note** for existing Lakebase graphs: run a
**full Knowledge Graph rebuild** after upgrading so companion tables migrate to the
`object_hash` layout (automatic on build — see Upgrade Notes).

---

## Highlights

- **Lakebase `object_hash` KG build fixes** — interactive managed_synced builds open
  the graph store before VIEW creation, refresh the Lakeflow source VIEW with
  `object_hash`, and auto-migrate legacy `__app` companion tables that still keyed
  on full `object` text (fixes `column "object_hash" does not exist` on rebuild).
- **Lakebase long literals ([#108](https://github.com/databrickslabs/ontobricks/issues/108))** —
  composite keys and Lakeflow sync use a generated `object_hash` so literal values
  longer than the Postgres btree index limit no longer abort triple-store sync.
- **Lakeflow graph indexes ([#112](https://github.com/databrickslabs/ontobricks/issues/112))** —
  `ensure_synced_union_view` re-applies `object_hash` lookup indexes idempotently when
  Lakeflow recreates `_sync` tables without secondary indexes.
- **Claude serving endpoints ([#107](https://github.com/databrickslabs/ontobricks/issues/107), PR [#109](https://github.com/databrickslabs/ontobricks/pull/109))** —
  flatten list-style `message.content` blocks from `databricks-claude-*` endpoints so
  Ontology Wizard → Generate and Mapping → Auto-Map no longer crash on `.strip()`.
- **Lakebase alias expansion performance (PR [#115](https://github.com/databrickslabs/ontobricks/pull/115))** —
  group hundreds of `%/<local-id>` LIKE predicates into `RIGHT(subject, k) = ANY(...)`
  clauses for fast Graph Chat / `describe_entity` alias resolution.
- **Edit-lock idle UX** — when a session expires through inactivity, the yellow
  **Resume editing** button sits left of **Save** in the L2 subnav; the full expiry
  message is a hover tooltip (replaces the top-of-page banner).
- **Developer hygiene** — pytest clears developer `LAKEBASE_*` / `PG*` env vars;
  `make deploy` honours an explicitly empty `DEFAULT_DATABRICKS_PROFILE` for PAT auth.

---

## Bug Fixes

### Lakebase sync for long literal objects ([#108](https://github.com/databrickslabs/ontobricks/issues/108))

Knowledge Graph sync to Lakebase Postgres aborted when any mapped literal `object`
exceeded the btree index limit (~2704 bytes). The composite primary key and secondary
indexes on `object` could not index long text columns.

- `src/back/core/graphdb/lakebase/_companion_ddl.py` — `object_hash` generated column
  (`digest(object, 'sha256')`); uniqueness on `(subject, predicate, object_hash)`;
  `pgcrypto`; Lakeflow view wrapper and `LAKEFLOW_SYNC_PRIMARY_KEY`.
- `src/back/core/graphdb/lakebase/LakebaseFlatStore.py` — actionable errors on btree
  limit failures; warn on legacy tables missing `object_hash`.
- `src/back/objects/digitaltwin/_build_pipeline.py` — wrap managed-synced warehouse VIEW
  with `object_hash`; hash PK for Lakeflow.
- `src/back/objects/registry/scheduler.py` — Lakeflow `primary_key_columns` include
  `object_hash`.
- `docs/lakebase-graphdb.md` — document long-literal support and rebuild requirement.

### Interactive managed_synced KG build (`object_hash`)

KG builds on existing domains could fail with `Could not register Lakebase synced
table: column "object_hash" does not exist` even when Lakeflow registration succeeded.
Two root causes:

1. **Warehouse VIEW** — the interactive build pipeline created the UC VIEW before
   opening the graph store, so sync-mode resolution could skip the Lakeflow wrapper
   that adds `object_hash` to the source VIEW.
2. **Legacy companion table** — pre-0.6.2 `__app` tables were left in place by
   `CREATE TABLE IF NOT EXISTS`; index DDL then referenced `object_hash` on a table
   that still had primary key `(subject, predicate, object)`.

- `src/back/objects/digitaltwin/_build_pipeline.py` — open graph store before VIEW
  creation; treat `store.is_synced` as source of truth for wrapping; refresh source
  VIEW with `object_hash` before `SyncedTableManager.ensure`; split companion vs
  synced-table error messages.
- `src/back/core/graphdb/lakebase/_companion_ddl.py` —
  `upgrade_legacy_triple_table_to_object_hash()` migrates existing companions
  (add generated column, drop legacy PK, re-key on `object_hash`) before indexes.
- `tests/units/dtwin/test_build_pipeline_streaming.py`,
  `tests/units/core/test_lakebase_flat_store.py` — regression coverage.

### Scheduled managed_synced VIEW builds ([#108](https://github.com/databrickslabs/ontobricks/issues/108))

Scheduled registry builds created the warehouse VIEW without `object_hash` while Lakeflow
keyed the synced table on that column.

- `src/back/objects/registry/scheduler.py` — resolve graph store before VIEW creation;
  wrap SQL for `managed_synced` via `_view_sql_for_graph_store()`.

### Lakeflow `_sync` table indexes ([#112](https://github.com/databrickslabs/ontobricks/issues/112))

When Lakeflow recreates a `_sync` table, secondary indexes on `object_hash` were lost,
degrading BFS / graph traversal performance.

- `src/back/core/graphdb/lakebase/_companion_ddl.py` / `ensure_synced_union_view` —
  idempotently re-apply graph lookup indexes after managed-synced rebuilds.

### Claude serving endpoint content blocks ([#107](https://github.com/databrickslabs/ontobricks/issues/107))

Databricks Claude serving endpoints return `choices[0].message.content` as a list of
content blocks instead of a plain string.

- `src/agents/engine_base.py` — `extract_message_content` flattens list blocks to a
  single string (OpenAI/Gemini string responses unchanged).
- Cherry-picked from community PR [#109](https://github.com/databrickslabs/ontobricks/pull/109)
  (Andreas Niehaus).

---

## Enhancements

### Lakebase alias expansion for large local-id sets (PR [#115](https://github.com/databrickslabs/ontobricks/pull/115))

`describe_entity` / Graph Chat alias expansion previously generated SQL with 1000+
`OR LIKE '%/<local-id>'` predicates, causing long-running statements and timeouts on
Lakebase.

- `src/back/core/graphdb/lakebase/LakebaseFlatStore.py` —
  `find_subjects_by_patterns()` groups patterns by suffix length into
  `RIGHT(subject, k) = ANY(ARRAY[...])` clauses (single round-trip).

### Edit-lock — resume control in L2 subnav

When an editing session expired through inactivity, a yellow top banner consumed
vertical space above the navbar.

- `src/front/static/global/js/edit-lock.js` — `renderResumeEditingButton()` injects a
  yellow **Resume editing** button left of **Save**; expiry text is a Bootstrap tooltip.
- `src/front/static/global/css/main.css` — `.ob-subnav-resume-btn` styles.

The "another user is editing" viewer banner is unchanged.

---

## Deploy & Operations

### PAT-based deploy (`deploy.config.sh`)

`DEFAULT_DATABRICKS_PROFILE="${DEFAULT_DATABRICKS_PROFILE:-DEFAULT}"` treated an
explicitly empty profile the same as unset, forcing the expired `DEFAULT` CLI profile
even when `DATABRICKS_TOKEN` was exported.

- `scripts/deploy.config.sh` — use `${DEFAULT_DATABRICKS_PROFILE-DEFAULT}` so
  `DEFAULT_DATABRICKS_PROFILE= make deploy` uses PAT auth when the token is in the
  environment.

### Hermetic unit tests (`tests/conftest.py`)

A developer's `.env` could export `LAKEBASE_*` / `PG*` coordinates while pytest's
autouse fixture forced a fake `DATABRICKS_HOST`, causing Registry triplet probes to
block the suite (~1% hang) and two graph-engine config tests to fail.

- `tests/conftest.py` — clear `LAKEBASE_PROJECT`, `LAKEBASE_BRANCH`,
  `LAKEBASE_DATABASE`, `PGHOST`, and `PGDATABASE` in `setup_test_env`.

---

## Upgrade Notes

### Upgrading from v0.6.1

Deploy the new app bundle as usual (`make deploy`). No new Lakebase **registry** schema
tables are required for this release.

**Knowledge Graph graphs on Lakebase:** if your companion triple-store tables were
created before the `object_hash` layout, run a **full Knowledge Graph rebuild** on each
affected domain version. v0.6.2 migrates legacy `__app` companions automatically during
the build (adds `object_hash`, re-keys the PK, recreates indexes). No manual Postgres DDL
is required. See `docs/lakebase-graphdb.md` §6.4.

If you deploy with a Personal Access Token instead of a CLI OAuth profile, you can run:

```bash
export DATABRICKS_HOST=… DATABRICKS_TOKEN=…
DEFAULT_DATABRICKS_PROFILE= make deploy
```

Optionally set `LAKEBASE_DATABASE_RESOURCE_SEGMENT=db-…` when the deploy preflight
cannot auto-resolve the Lakebase database resource id.

### Upgrading from v0.5.x or v0.6.0

Upgrade through v0.6.1 first (see `releases/ReleaseNotes_V0.6.1.md`), then apply v0.6.2.

---

## Changes Summary

| Area | Key files | Change type |
|------|-----------|-------------|
| Lakebase long literal PK | `_companion_ddl.py`, `LakebaseFlatStore.py`, `_build_pipeline.py`, `scheduler.py` | Fix |
| `object_hash` KG build (VIEW + companion migration) | `_build_pipeline.py`, `_companion_ddl.py` | Fix |
| Lakeflow `_sync` indexes | `_companion_ddl.py`, `ensure_synced_union_view` | Fix |
| Claude endpoint content | `agents/engine_base.py` | Fix |
| Lakebase alias expansion perf | `LakebaseFlatStore.py` | Enhancement |
| Edit-lock idle UX | `edit-lock.js`, `main.css` | UX |
| PAT deploy profile | `deploy.config.sh` | Enhancement |
| Hermetic pytest env | `tests/conftest.py` | Fix |
| Version | `pyproject.toml` | Bumped to `0.6.2` |

---

## What is NOT changed

- External API contract — all `/api/v1/` endpoints are backward-compatible.
- MCP tool contracts — no MCP tool signatures changed.
- Registry Lakebase schema — no new collaboration / audit tables.
- v0.6.0 / v0.6.1 features — collaboration, graph analytics, edit-lock lease,
  audit trail, Switch modal fix, and all other capabilities remain intact.
