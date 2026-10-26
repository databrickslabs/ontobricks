# OntoBricks — Release Notes V0.8.2

**Release window:** October 2026<br>
**Type:** Patch release (v0.8.1 → v0.8.2)<br>
**Test status:** `uv run --frozen pytest -q -m "not scenario"`: 5941 passed, 316 skipped, 9 deselected, 32 warnings. The deselected tests are 6 scenario tests and 3 health tests that open a live SQL warehouse connection (`tests/units/api/test_routes.py::TestHealthRoutes::test_health_check` and `tests/units/core/test_health.py::TestRunReadinessChecks`). Without a reachable warehouse they wait in the connector's retry back-off, which is the stall at `test_routes.py` recorded on 2026-10-01 and 2026-10-12.

---

## Highlights

- **Enterprise Initialize no longer 502s on DDL ownership** — when the app service principal is DML-only and `schema.sql` raises `must be owner of table`, Initialize logs a warning, skips DDL, and still upserts the registry identity row (catalog / schema / volume).
- **Ontology generation uses the bound Unity Catalog Volume** — `generate_with_agent` and `generate_rules_with_agent` pass `RegistryCfg.from_domain(...).as_dict()` instead of the seeded session dict (`OntoBricksRegistry`). Document tools no longer look up a non-existent `…ontobricksregistry` volume.
- **Knowledge Graph builds no longer stop at one hour ([#201](https://github.com/databrickslabs/ontobricks/pull/201))** — SQL connections refresh their OAuth token during long statements, a failed triple count fails the build with the real error instead of reporting "no triples generated", and materialized builds read the mapped sources once instead of twice.
- **Table descriptions load without `information_schema` access** — table comments come from the Unity Catalog tables API, which needs only the grants a data source already has.

No registry schema migration. No breaking MCP tool names.

---

## Registry (Lakebase Initialize)

Enterprise workspaces often provision Lakebase with a dedicated owner identity. The Databricks App SP then has DML on `ontobricks_registry` but cannot `ALTER` / `CREATE INDEX` on existing tables such as `domains`.

On 0.8.1, `initialize()` wrapped `_apply_ddl()` and the registry-row DML in one `try/except`. A DDL ownership error returned **502 Registry initialization failed** and never pinned the bound Volume.

On 0.8.2, `_apply_ddl()` is non-fatal:

- DDL failure → warning (`Lakebase DDL skipped (schema already provisioned or caller is not table owner)`) and continue.
- Registry row upsert and column migrations still run.
- A **DML** failure still fails Initialize.

**Check Access** can still report the DDL error; Initialize can succeed and pin the Volume.

---

## Ontology generation (bound Volume)

Agent document reads go through the Unity Catalog Files API on the registry Volume. Generation previously forwarded the seeded session registry (`OntoBricksRegistry`), so tools resolved a volume such as `catalog.schema.ontobricksregistry` instead of the bound volume (`catalog.schema.ontobricks` in the Reliance / Jio Home staging layout).

That produced `FILES_API_RESOURCE_NOT_FOUND`, empty metadata (`0 available table(s)`), then a guidelines-only OWL emit that hit the 10-iteration cap (`finish_reason=length`, output not starting with `@prefix`).

Both ontology and business-rules generation now resolve the Volume the same way as the rest of the domain: `RegistryCfg.from_domain(session, settings)`.

---

## Knowledge Graph build (Lakehouse)

On a large mapping, the Lakehouse build failed at exactly one hour, every time:

- App-mode SQL connections were opened with an M2M OAuth token fixed at connect time. Those tokens last one hour, so the connector's status poll on a long statement failed with `RequestError: Error during request to server`.
- When that happened during the count over the gateway VIEW, the count swallowed the error and returned 0. The build then finished "with warnings: VIEW created but no triples generated (check your mappings)" and was audited as a success, with no `…_data` table.
- Materialized builds counted the gateway VIEW before the CTAS, so every build evaluated the whole mapping twice.

On 0.8.2:

- App-mode SQL connections pass a `credentials_provider` that reads the cached token on every request, so long statements pick up its refresh.
- `count_triples()` raises instead of returning 0. A failed count fails the build with the real error, in both the Lakehouse build and the Lakebase / Neo4j full rebuild. The post-sync count of the managed-synced path stays non-fatal, because it only feeds the progress message.
- Materialized builds run the CTAS first and count the Delta table afterwards: one scan of the mapped sources. View-only builds still count the gateway VIEW first.

---

## Table descriptions (Data Sources)

Table descriptions were read with `SELECT comment FROM <catalog>.information_schema.tables`. When the app's service principal could not read `information_schema`, the error was swallowed and every table showed an empty description.

On 0.8.2, `UnityCatalog.get_table_comment()` reads `comment` from the Unity Catalog tables API (`GET /api/2.1/unity-catalog/tables/{full_name}`), the same API `list_tables_and_views()` already uses. It needs only USE CATALOG, USE SCHEMA and SELECT. Column descriptions are unchanged; they still come from `DESCRIBE`.

---

## Documentation and operator notes

- Package and README version are **0.8.2**. `uv.lock` now records the package as 0.8.2 too (one-line change; every other entry is untouched).
- Changelogs: `changelogs/v0.8.2/benoitcayladbx_2026-10-01.log` (version bump), `changelogs/v0.8.2/benoitcayladbx_2026-10-12.log` (Initialize + bound-volume generation), `changelogs/v0.8.2/dermotsmyth-db_2026-10-19.log` (Knowledge Graph build) and `changelogs/v0.8.2/dermotsmyth-db_2026-10-21.log` (table descriptions and release preparation).

---

## Upgrade notes

- **Lakebase (pre-provisioned schema):** click **Initialize** again after deploy. A DDL owner error is expected and should no longer block pinning catalog / schema / volume. Confirm the bound Volume on Registry → Location matches the UC Volume that holds domain `documents/`.
- **Lakebase (app SP is table owner):** behaviour is unchanged; `schema.sql` still applies.
- **Ontology generation:** re-run Generate after Initialize so document tools hit the bound Volume. Large specs (hundreds of attributes in one Turtle dump) can still truncate at the model token limit; split by functional domain if that happens.
- **Lakebase / MCP policy:** the v0.8.0 `domains.mcp_policy` migration remains a prerequisite if you are still on 0.7.x. This patch does not add columns.
- **Knowledge Graph build:** rebuild after deploy; no setting changes. In materialized mode, a mapping that produces no triples now leaves an empty `…_data` table instead of the previous build's table.
- **Table descriptions:** on the Data Sources page, click **Update from UC** to load descriptions that came back empty before.
- **Builds labelled 0.8.2 before this release** do not contain the Knowledge Graph build or table-description fixes. Redeploy from the `v0.8.2` tag.

---

## Bug Fixes

- Fixed Initialize returning 502 when `schema.sql` raised `InsufficientPrivilege: must be owner of table …` on a pre-provisioned Lakebase schema.
- Fixed Initialize aborting before the registry identity row (and bound Volume) was upserted.
- Fixed ontology / rules generation reading documents from the seeded `OntoBricksRegistry` volume name instead of `RegistryCfg.from_domain`.
- Fixed agent `read_document` 404s (`Volume '….ontobricksregistry' does not exist`) on workspaces whose bound Volume is not named after the Lakebase schema.
- Fixed Lakehouse builds failing one hour in: app-mode SQL connections used an OAuth token fixed at connect time, which expired during long statements.
- Fixed a failed triple count being reported as "VIEW created but no triples generated (check your mappings)" and audited as a successful build; it now fails the build with the error.
- Fixed materialized Lakehouse builds evaluating the mapping twice (count over the gateway VIEW, then CTAS).
- Fixed table descriptions coming back empty when the app's service principal cannot read `information_schema`.
