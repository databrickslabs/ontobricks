# OntoBricks — Release Notes V0.9.1

**Release window:** October 2026<br>
**Type:** Patch release (v0.9.0 → v0.9.1)<br>
**Test status:** `uv run --frozen pytest -q -m "not scenario"`: 9625 passed, 326 skipped, 8 deselected. Live campaign on `ontobricks-09x` (2026-10-29): `make scenario-campaign` 7 passed in 22m; validation 13 passed, 0 failed.

This release is additive on v0.9.0. Lakehouse//RT queries, staged Ontology
Generate, Knowledge Store, Studio, SPARQL playground, entity quality / SWRL /
Jobs, GitHub mirror, and the 0.9.0 security hardening remain as documented in
`releases/ReleaseNotes_V0.9.0.md`.

---

## Highlights

- **Viewers can search Explorer and use Graph Chat** — those POSTs only read;
  writes (load, node actions) stay blocked. ([#190](https://github.com/databrickslabs/ontobricks/issues/190))
- **SWRL `swrlb:` builtins apply on inference and materialization** — datatype
  filters no longer infer zero triples. ([#171](https://github.com/databrickslabs/ontobricks/issues/171) /
  [#172](https://github.com/databrickslabs/ontobricks/pull/172))
- **Auto-Map emits Unity Catalog Metric View SQL** — `MEASURE(...)` plus
  `GROUP BY` dimensions, with a submit-time guard against `SELECT *`.
- **Faster bulk Data Quality** — the R2RML gateway VIEW is CTAS'd once into a
  task-scoped Delta snapshot; checks run in parallel against the copy, with a
  live activity log that stays visible when the run finishes.
- **Entity Data Quality Checks stream a live log** — Explorer starts a
  background task and polls `log_entries`; MCP and the public GET stay
  synchronous.
- **Lakebase quality SQL stays on the Build warehouse** — quality no longer
  follows a Lakehouse//RT query warehouse (Thrift + Apps OAuth retry).
- **Dependabot floors raised** in `pyproject.toml` (Pillow, certifi, GitPython,
  MLflow, setuptools, fastmcp, OpenTelemetry, filelock, black). **`uv.lock`
  regeneration is still required** before the alerts clear.
- **Planning trees stay local** — `.planning/` is gitignored; the eval gate
  looks at committed datasets under `tests/eval/datasets/`.

No registry schema migration. No breaking MCP tool names.

---

## Permissions (Viewer)

PermissionMiddleware treated every domain-scoped POST as a write. Explorer
search / expand (`/dtwin/sync/filter`) and Graph Chat (`/dtwin/assistant/chat`,
`/chat/stream`, and the agent's `query_graphql` loopback to
`/dtwin/graphql/execute`) take a POST body but only read, so a Viewer could
open Knowledge Graph and could not search or chat.

0.9.1 allowlists those four paths for POST. Graph Chat writes
(`/dtwin/nodes/action/request`, `/confirm`) and `/dtwin/sync/load` stay 403 for
Viewers. Editors are unchanged. The allowlist is documented in
`docs/development.md`.

---

## SWRL builtins

Editor-generated rules with `swrlb:` builtins over datatype properties still
inferred zero triples on 0.9.0. Inference already emitted WHERE filters;
materialization, the `uri_map` `dataProperties` fold, registry prefixes, and
Lakebase `TRY_CAST` were still broken.

On 0.9.1:

- Builtin lookup is prefix- and IRI-tolerant.
- Class `dataProperties` fold into the uri_map (object properties win a
  same-name clash).
- Inference and materialization reuse one atom partition; builtins become
  WHERE filters and negated atoms `NOT EXISTS`.
- Lakebase uses a Postgres `TRY_CAST` / `DOUBLE` rewrite for comparison and
  math builtins (remaining dialect gap documented on the translator).

---

## Auto-Map and Unity Catalog Metric Views

0.9.0 already imported Metric Views as data assets (dimension / measure roles).
Auto-Map still treated them as tables.

0.9.1 adds prompt rules (ID is a dimension; wrap measures in `MEASURE(...)`;
`GROUP BY` dimensions) and rejects `SELECT *`, unwrapped measures, or missing
`GROUP BY` when the SQL hits a metric view. The mapping agent eval dataset
covers happy / ambiguous / adversarial rows.

---

## Data Quality

### Bulk checks (Knowledge Graph → Data Quality)

A multi-rule run used to re-expand the R2RML gateway VIEW on every check.
0.9.1 CTAS that VIEW once into a task-scoped Delta table, runs up to four
workers against the copy, then drops it. If CTAS fails, checks fall back to
the live VIEW (sequential). Runs with fewer than two checks skip the snapshot.

The progress card shows `task.log_entries` in a scrollable activity log,
starts polling immediately, and keeps the log after success or failure.

### Entity checks (Explorer)

The entity popup no longer waits on a single `GET /dtwin/nodes/shacl` with a
spinner. The UI `POST`s `/dtwin/nodes/shacl/start`, polls the task, and
renders the same activity log. MCP and the public API keep the synchronous
GET. Per-shape queries still run after a DQ snapshot when one is available.

### Lakebase + Build warehouse

Quality SQL used `engine="view"`, which followed the Lakehouse//RT query
warehouse even when the graph lived in Lakebase. Thrift then rejected RT, and
the Apps OAuth retry kept `credentials_provider` next to `use_kernel`.

`engine="view"` now binds to the **Build** SQL Warehouse with `use_sea=False`.
On a Thrift-to-Kernel retry the client drops `credentials_provider` and passes
Apps M2M OAuth.

---

## Documentation and operator notes

- Package and README version are **0.9.1**.
- README header is HTML-centered (sized logo, badges, links). Satellite
  guides stay folded: Get Started is `docs/getting-started.md`; Graph DB
  topics live in `docs/backend.md`; import/export and cohort discovery live
  in `docs/advanced_features.md`; the feature inventory is a section of
  `docs/user-guide.md`.
- Eval / AI-feature gate: required PR artifacts are the committed eval
  dataset plus MLflow URI. SPEC.md and `.planning/` stay local.
- Changelogs: `changelogs/v0.9.1/benoitcayladbx_2026-10-22.log` through
  `changelogs/v0.9.1/benoitcayladbx_2026-10-29.log`.

---

## Security (Dependabot)

Raised floors in `pyproject.toml` / `src/mcp-server/pyproject.toml`:

| Package | Floor | Notes |
|---------|-------|--------|
| Python | ≥3.11 | `pillow>=13.0.0` drops 3.10 |
| MLflow | ≥3.17.2 | GHSA-2wqp-q3c6-ccj5, GHSA-wqmh-qccw-pqwf |
| black | ≥26.10.1 | GHSA-cg8m-r9f2-5wm2 |
| fastmcp / fastmcp-slim | ≥3.4.10 | GHSA-7xgj-8392-5q4r |
| GitPython | ≥3.2.1 | GHSA-83vg-56qc-22m7 |
| Pillow | ≥13.0.0 | Dependabot #216–260 |
| setuptools | ≥84.1.0 | GHSA-xmj3-9gf7-h52w |
| certifi | ≥2026.10.19 | GHSA-6q48-6c7w-9gw2 |
| filelock | ≥4.2.1 | GHSA-v4x8-3wr5-3wq8 and related |
| opentelemetry-api | ≥1.45.1 | GHSA-xcq7-p3pf-p88h |

**Lockfile not regenerated in this release.** Dependabot scans `uv.lock`, not
`pyproject.toml`. `--frozen` test runs still exercise the previous pins until
someone with index access runs `uv lock` (root) and `cd src/mcp-server && uv lock`.

**Still blocked upstream (unchanged from 0.9.0):**

- **oauthlib 4.0.0** — `databricks-sql-connector` 4.4–4.6 requires `oauthlib<4.0.0`.
- **NLTK GHSA-8mgp-746c-j5xp** — no patched release; pitfalls extra only.

---

## Upgrade notes

- **No registry schema migration.** Existing 0.9.0 domains keep working.
- **Viewer:** confirm Teams still grant Viewer on domains where search and
  Graph Chat should be available; those POSTs are now allowed, writes are not.
- **Python 3.11:** deploy images and local `uv` environments must be 3.11+
  once `uv.lock` is regenerated against Pillow 13.
- **Dependabot:** do not treat this tag as having closed the lockfile alerts
  until `uv.lock` and `src/mcp-server/uv.lock` are regenerated and the suite
  is re-run `--frozen`.
- **SWRL:** re-run inference / materialization on domains whose rules use
  `swrlb:` builtins over datatype properties; 0.9.0 produced empty results.
- **Metric Views:** existing Auto-Map SQL that `SELECT *` from a metric view
  will fail the new submit-time guard — regenerate those mappings.
- **Data Quality:** bulk runs create a temporary Delta snapshot named from
  the gateway stem and the task id; it is dropped in `finally`. CTAS failure
  falls back to the live VIEW.
- **Entity quality UI:** Explorer uses `POST /dtwin/nodes/shacl/start`. MCP
  and `GET /dtwin/nodes/shacl` are unchanged.
