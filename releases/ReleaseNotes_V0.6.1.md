# OntoBricks — Release Notes V0.6.1

**Release date:** 2026-07-03
**Type:** Patch release (v0.6.0 → v0.6.1)
**Test status:** 2990 passed, 275 skipped, 5 deselected (`uv run pytest -q -m "not scenario"`).

---

## Summary

v0.6.1 is a focused patch on top of v0.6.0. It fixes the **Switch** modal on published /
in-review domains, hardens **Lakebase deploy** diagnostics and database-name resolution,
adds **Databricks CLI profile** support to deploy scripts, and backports a **mapping SQL
column deduplication** fix that prevents `AMBIGUOUS_REFERENCE` errors during digital-twin
builds.

No breaking changes. No schema migrations required.

---

## Highlights

- **Switch modal on read-only versions** — on IN-REVIEW or PUBLISHED domains, the version
  picker is now usable while the "Save my changes before switching" option is correctly
  disabled (fixes [#97](https://github.com/databrickslabs/ontobricks/issues/97)).
- **Lakebase deploy resilience** — shared diagnostics helper, underscore/hyphen slug
  tolerance when resolving `db-…` resource segments vs Postgres `datname`, and clearer
  failure hints with ready-to-run CLI commands.
- **Deploy ergonomics** — `DEFAULT_DATABRICKS_PROFILE` in `scripts/deploy.config.sh`
  pins the CLI profile for all deploy and bootstrap targets.
- **Auto-Map SQL fix** — duplicate `id`/`label` column references in agent-submitted
  mappings are deduplicated before build (backport from PR #98, Eric Poilvet).

---

## Bug Fixes

### Switch modal on published / in-review domains ([#97](https://github.com/databrickslabs/ontobricks/issues/97))

On non-DRAFT lifecycle versions, the L2 **Switch** popup previously kept the version
dropdown disabled and still offered "Save my changes before switching" even though
read-only versions cannot be saved.

- `permissions.css` — exempt `#switchVersionSelect` from the read-only select disable rule
  (navigation, not design editing).
- `navbar.js` — `isSwitchSaveAllowed()`, `configureSwitchSaveOption()`, and a confirm-handler
  guard disable save-before-switch on non-DRAFT versions.
- `docs/user-guide.md` — documents read-only Switch behaviour (version picker available,
  save option disabled).

### Auto-Map SQL column deduplication (PR #98 backport)

When the Auto-Map agent repeats a source column under its original name alongside an
"AS ID" / "AS Label" alias, and that original name is itself `id` or `label`
(case-insensitive), Databricks/Spark resolves both as the same output column, causing
`AMBIGUOUS_REFERENCE` when the digital-twin build creates the triple-store VIEW.

- `src/agents/tools/mapping.py` — call `SQLWizardService._deduplicate_select_columns`
  after LIMIT stripping in `tool_submit_entity_mapping` and
  `tool_submit_relationship_mapping`.
- `src/back/core/sqlwizard/SQLWizardService.py` — `_deduplicate_select_columns` is now a
  `@staticmethod` so agents can reuse it without constructing a full service instance.
- `CONTRIBUTORS.md` — Eric Poilvet ([@epoilvet](https://github.com/epoilvet)) added.

---

## Deploy & Operations

### Databricks CLI profile (`deploy.config.sh`)

Deploy and bootstrap scripts now honour a single source of truth for the CLI profile.
Users with multiple Databricks profiles no longer need to export
`DATABRICKS_CONFIG_PROFILE` manually before every `make` target.

- `scripts/deploy.config.sh` — `DEFAULT_DATABRICKS_PROFILE` (exported as
  `DATABRICKS_CONFIG_PROFILE` when set).
- `scripts/deploy.sh` — active profile shown in the deploy summary; auth hints mention
  `--profile` when configured.
- `docs/deployment.md` and `README.md` — document the new variable.

### Lakebase connection diagnostics

Lakebase misconfiguration errors (wrong project/branch, stale `db-…` id, missing
database) now print actionable hints instead of terse failures.

- `scripts/_lakebase-diag.sh` (new) — shared helper printing common causes and
  ready-to-run `databricks postgres list-databases` / `endpoints` commands (with
  `--profile` when configured).
- `scripts/deploy.sh` — diagnostics on `db-…` resolution failure, resource checks,
  dry-run abort, bootstrap failure, and Lakebase step ERR-trap hints.
- `scripts/bootstrap-lakebase-perms.sh` — diagnostics on endpoint/credentials/psql
  connection failures; distinguishes psql connection errors from missing schema.

### Lakebase underscore vs hyphen naming

Lakebase exposes two names per database — `status.postgres_database` (PG `datname`,
underscores) vs the resource-path `database_id` (RFC-1123, hyphens only). Operators
saw `ontobricks_demo` become `ontobricks-demo` in `list-databases` and deploy failed on
exact string match.

- `scripts/_lakebase-resolve-db.py` (new) — resolve `db` segment + `datname` with
  underscore/hyphen slug tolerance.
- `scripts/deploy.sh` — uses the shared resolver; auto-corrects `LAKEBASE_DATABASE` when
  slug-matched.
- `scripts/_lakebase-diag.sh` — documents the two-name model in failure hints.
- `scripts/deploy.config.sh` — comment that `DEFAULT_LAKEBASE_DATABASE` must come from
  `postgres_database`, not the hyphenated resource id.

---

## Upgrade Notes

### Upgrading from v0.6.0

No schema changes. Deploy the new app bundle as usual (`make deploy`). If you use a
non-default Databricks CLI profile, set `DEFAULT_DATABRICKS_PROFILE` in
`scripts/deploy.config.sh` once instead of exporting it per session.

If Lakebase deploy previously failed because your configured database name used
underscores while the API returned hyphens (or vice versa), v0.6.1 should resolve the
mismatch automatically. Ensure `DEFAULT_LAKEBASE_DATABASE` in `deploy.config.sh` matches
the Postgres `postgres_database` field from `databricks postgres list-databases`, not the
hyphenated resource-path tail.

### Upgrading from v0.5.x

Upgrade to v0.6.0 first (see `releases/ReleaseNotes_V0.6.0.md` for migration steps),
then apply v0.6.1 on top.

---

## Changes Summary

| Area | Key files | Change type |
|------|-----------|-------------|
| Switch modal (read-only versions) | `permissions.css`, `navbar.js`, `test_switch_domain_modal.py`, `docs/user-guide.md` | Fix |
| Auto-Map SQL deduplication | `agents/tools/mapping.py`, `SQLWizardService.py`, `test_mapping_tools.py` | Fix |
| CLI profile support | `deploy.config.sh`, `deploy.sh`, `docs/deployment.md`, `README.md` | Enhancement |
| Lakebase diagnostics | `_lakebase-diag.sh`, `deploy.sh`, `bootstrap-lakebase-perms.sh` | Enhancement |
| Lakebase name resolver | `_lakebase-resolve-db.py`, `deploy.sh`, `test_lakebase_resolve_db.py` | Fix |
| Version | `pyproject.toml` | Bumped to `0.6.1` |

---

## What is NOT changed

- External API contract — all `/api/v1/` endpoints are backward-compatible.
- MCP tool contracts — no MCP tool signatures changed.
- Lakebase schema — no new tables or migrations.
- v0.6.0 features — collaboration, graph analytics, edit-lock, audit trail, and all other
  v0.6.0 capabilities remain intact.
