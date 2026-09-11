# OntoBricks — Release Notes V0.9.0

**Release window:** September 2026<br>
**Test status:** all changes shipped with the non-scenario suite green: 5930 passed, 304 skipped, 6 deselected, 1 xfailed, 32 warnings.

---

## Highlights

- **Lakehouse//RT graph queries** — administrators can point Knowledge Graph reads at a serverless Lakehouse//RT warehouse over the Statement Execution API, while builds keep a classic or serverless warehouse that still accepts `CREATE VIEW`, CTAS, and writes.
- **Faster Explorer first paint** — graph status no longer blocks the Knowledge Graph page on full `COUNT(*)` scans; Explorer preview applies server-side seed limits; graph handlers no longer freeze the event loop.
- **Visible search timing** — Explorer reports Preview, Expansion, Display, and Total duration on the canvas, excluding time spent picking seeds.
- **New Version from the navbar** — branch the open domain from the Versions popup without opening Domain → Versions.
- **Unity Catalog names that start with a digit** — data sources such as `5_g_subscribers` load when backtick-quoted, matching Unity Catalog rules.
- **Safer Databricks Apps result download** — CloudFetch can be turned off globally when Apps cannot reach the CloudFetch storage host.

This release is additive on v0.8.0. Ontology-only domains, MCP policy, virtual attributes, branding, and the Clarity UI remain as documented in `releases/ReleaseNotes_V0.8.0.md`.

---

## Lakehouse compute and Statement Execution API

### Build warehouse vs Query warehouse

Lakehouse//RT warehouses accept reads and reject build DDL (`UNSUPPORTED_FEATURE.CREATE_VIEW`). Settings therefore split the roles:

- **Settings → Lakehouse → SQL Warehouse → Build** — classic or serverless warehouse used for mapping views, materialization, and other writes. RT warehouses are excluded from this selector. An administrator can override the Databricks App `sql-warehouse` resource default.
- **Query** — disabled by default and mirrored from Build. Enable **Use Lakehouse//RT for queries** to pick a distinct query warehouse (standard or RT) from the same workspace list. Turning the option off and applying clears the override so reads fall back to Build.

Builds never fall back to the Lakehouse query warehouse. RT DDL failures map to this guidance instead of a generic source-table error.

### Statement Execution API

Lakehouse//RT rejects the legacy Thrift protocol. When the query (or build) warehouse uses SEA:

- SQL clients connect with `use_sea=True`.
- Transient SEA 5xx / connection-reset failures retry with reconnect; genuine SQL and Thrift-not-supported errors do not.
- `SET STATEMENT_TIMEOUT` is skipped on SEA (the deprecated SEA backend 500s on those statements when CloudFetch is off). Client-side socket timeout still applies. Thrift warehouses keep the server-side bound.

### CloudFetch

**Settings → Databricks → Use CloudFetch** (administrators) controls whether SQL clients download result files via CloudFetch. Leave it on unless Databricks Apps cannot reach the CloudFetch storage host; in that case Explorer searches can time out after SQL has already finished. Saving CloudFetch no longer validates an unloaded Lakehouse Query selector.

---

## Knowledge Graph performance and Explorer

- Async graph handlers (`/sync/stats`, load, BFS find, neighbour expand) run blocking SQL on the sized thread pool so a slow warehouse query does not stall the rest of the page.
- `/sync/stats` runs independent warehouse round-trips concurrently. Lakehouse distinct counts use `approx_count_distinct` for the Insights overview; Lakebase keeps exact counts.
- Graph existence uses `SELECT 1 … LIMIT 1`. `/sync/info` returns a pending skeleton on cache miss so the page paints immediately; the triple-count badge fills in the background.
- Explorer Phase-1 preview applies `LIMIT` in SQL and fetches `rdf:type` plus `rdfs:label` in one scan.
- Knowledge Graph Explorer, GraphQL, and Chat headers no longer repeat Domain / version / status (the navbar already shows it). Graph DB name and Switch domain remain.
- The canvas stopwatch is a button: Preview request, Expansion request, Display, and Total. Seed-selection dwell is excluded. Empty results clear stale times.

---

## Ontology, Mapping, and Versions

- Ontology Designer metadata saves (label, icon, reference, relationship direction) update visible D3 elements in place. Topology changes still rebuild the map.
- The Versions popup includes **New Version**. Domain → Versions uses the same ungated create-version workflow. After confirmation the popup closes and the branded loading overlay stays until the new version loads; failures restore the popup.
- The navbar domain badge restores the last confirmed loaded domain on page start and keeps it across transient `/navbar/state` failures. Navigating to Settings no longer flashes an empty badge. Closing the domain still clears it.
- Unity Catalog catalog, schema, and table identifiers may start with a digit. Injection-sensitive characters (`;`, `--`, quotes, whitespace) remain rejected.

---

## Reliability and CI

- `GET /api/v1/digitaltwin/triples` no longer raises `NameError` when selecting the graph or view table; it uses the documented `backend` parameter.
- Remaining undefined-name defects in ontology evaluation, the pitfalls runner, and Delta warehouse resolution are fixed.
- CI runs a blocking, frozen, repository-wide Ruff **F821** gate.
- Lakebase authentication, Settings discovery, and provisioning follow every Postgres List Projects page, so projects beyond the first API page resolve.
- Default deploy instance identity for this line is **09x** (`scripts/deploy.config.sh`).

---

## Documentation

Operator guides now describe the Build / Query split, the Lakehouse//RT toggle, CloudFetch, Explorer search timing, and Versions-popup creation:

- `docs/get-started.md`
- `docs/deployment.md`
- `docs/architecture.md`
- `docs/features.md`
- `docs/user-guide.md`
- `README.md`

---

## Bug Fixes (selected)

- Fixed Knowledge Graph builds on Lakehouse//RT failing because Thrift was used even when SEA was configured, then failing again when build SQL was sent to a read-only RT warehouse.
- Fixed persistent `Error querying graph` on SEA with CloudFetch off caused by `SET STATEMENT_TIMEOUT`.
- Fixed Explorer timeouts in Databricks Apps after fast RT SQL when CloudFetch could not download result files.
- Fixed Settings → Databricks saves raising a false “Select a Query SQL Warehouse” error before Lakehouse controls had loaded.
- Fixed Data Sources rejecting valid Unity Catalog tables whose names start with a digit.
- Fixed Ontology Designer flashing a full-map spinner on metadata-only saves.
- Fixed the navbar hiding a still-loaded domain after a failed or slow navbar refresh.
- Fixed `GET /api/v1/digitaltwin/triples` crashing with `NameError` on every request.

---

## Upgrade Notes

### No registry schema migration

v0.9.0 adds global-config keys only (`warehouse_use_sea`, Lakehouse query warehouse / `use_sea`, `use_cloud_fetch`). Existing v0.8.0 domains keep working: Query mirrors Build, SEA defaults off, CloudFetch defaults on.

### Enabling Lakehouse//RT reads

1. Keep **Build** on a classic or serverless SQL warehouse (not Lakehouse//RT).
2. Under **Settings → Lakehouse → SQL Warehouse**, enable **Use Lakehouse//RT for queries** and select a distinct query warehouse.
3. If Explorer times out in Databricks Apps after SQL has finished, uncheck **Use CloudFetch** and save.

Rebuilds are not required solely to turn RT queries on. Builds still run on the Build warehouse.

### Instance suffix

Deploy configs and tests for this line use `INSTANCE_ID=09x`. Align `scripts/deploy.config.sh`, app names, and DAB targets before deploying alongside an 08x instance.

### External API

`GET /api/v1/digitaltwin/triples` remains the triples listing endpoint. Callers that already passed `backend` are unchanged; the server now honors that parameter instead of raising.
