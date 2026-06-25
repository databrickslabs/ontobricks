# TL;DR

Adds **Neo4j (Bolt / Cypher)** as a fully-functional graph DB engine alongside Lakebase. Opt-in via `Settings → Triple Store → Global → Neo4j (Bolt)`. Lakebase remains the default; existing deployments are unaffected.

**Version bumped to `0.7.0`** for the post-review iteration that addresses every point of Benoit's 2026-06-18 PR review.

**End-to-end demo on `fevm-mjolnir`** using a real PFAS research-paper ontology:

| | |
|---|---|
| AI-generated classes from the paper | **32** |
| AI-generated relations | **13** |
| Entities mapped via Auto-Map | **25 / 25** |
| Relations mapped via Auto-Map | **12 / 12** |
| Triples written to Neo4j over Bolt | **303** in 5.3 s |
| OWL 2 RL T-Box inference | **99 inferred** in 0.102 s |
| SHACL Data Quality (Graph mode against Neo4j) | **92.3 %** Consistency pass on 13 auto-generated rules |
| Aura cleanup after capture | **0 nodes** — paper stays in `fevm-mjolnir` only |

## :sparkles: Post-review iteration — 6 commits (2026-06-22 → 2026-06-25)

Every item of Benoit's PR review punch-list addressed, plus one bonus. Each commit ships its own test/changelog/screenshot artefacts.

| # | Commit | What it does |
|---|---|---|
| 1 | `da9cae9` `feat(graphdb)` | **#68 — Secret resource auth.** Neo4j password sourced from `NEO4J_PASSWORD` env var (Databricks Apps secret bound via `databricks.yml`). Save endpoint strips clear-text from `global_config`. UI badge flips between *From Apps secret* (green) and *Local-dev fallback* (yellow). |
| 2 | `e8b523c` `feat(graphdb)` | **#69 — Cypher logging at INFO.** Every `_run` emits `Cypher (n rows, ms): <flattened>` to the app logs. Bound params at DEBUG only — no credential leak (auth lives on the driver). |
| 3 | `577b70f` `chore(release)` | **#70 — Bump to `0.7.0`.** `pyproject.toml` (single source of truth) + `README.md` banner + `scripts/deploy.config.sh`. |
| 4 | `e63bfce` `fix(front)`  | **#71 — Settings flash fix.** Engine selector server-side rendered via Jinja `{% if graph_engine == "..." %}selected{% endif %}`; sub-panel visibility aligned at `DOMContentLoaded`. |
| 5 | `7a9a625` `refactor(graphdb)` | **#72 — Split `Neo4jStore.py` (1028 LoC → 4 files).** Fowler *Large Class → Extract Class* + façade. Public API unchanged. |
| 6 | `820f607` + `b13dda0` `feat(graphdb)` | **(bonus) — Test connection wired.** Placeholder gone. POST `/settings/graph-engine/neo4j-test` runs `driver.verify_connectivity()` + `RETURN 1 AS probe` through `Neo4jConnection.run`, surfaces handshake latency + Cypher echo + credentials source + typed error category (`config` / `driver-missing` / `auth` / `connectivity`) on failure. |

## :white_check_mark: Live verification on `ontobricks-070` · FEVM-Mjolnir · 2026-06-25

**UI alert** after clicking *Settings → Neo4j → Test connection*:

> :heavy_check_mark: **Connected** to `neo4j+s://b4810af7.databases.neo4j.io` (database `neo4j`) in **1574.4 ms** · credentials from *env var (NEO4J_PASSWORD — Databricks Apps secret)*. · `RETURN 1 AS probe` echoed **1 row(s) — Cypher path live**.

**App logs** captured concurrently via `databricks apps logs ontobricks-070`:

```
INFO   ontobricks.core.graphdb.neo4j.Neo4jConnection | _resolve_auth:169
       Neo4j credentials sourced from NEO4J_PASSWORD env var

INFO   ontobricks.core.graphdb.neo4j.Neo4jConnection | get_driver:141
       Neo4j driver opened for neo4j+s://b4810af7.databases.neo4j.io (database=neo4j)

DEBUG  ontobricks.core.graphdb.neo4j.Neo4jConnection | run:215 | Cypher params: {}

INFO   ontobricks.core.graphdb.neo4j.Neo4jConnection | run:221
       Cypher (1 rows, 706.4 ms): RETURN 1 AS probe

DEBUG  ontobricks.core.graphdb.neo4j.Neo4jConnection | close:151 | Neo4j driver closed
```

Each of the 5 reviewable items leaves a fingerprint in these traces:
- **#68 secret** — `_resolve_auth:169` line confirms the env-var path (no `engine_config.password` lookup).
- **#69 Cypher logging** — `Cypher (1 rows, 706.4 ms): RETURN 1 AS probe` is the new INFO line; the matching DEBUG `Cypher params: {}` shows where bound values would land.
- **#70 v0.7.0** — `header` shows `v0.7.0` (also visible in every Settings screenshot).
- **#71 Settings flash** — first paint of Settings → Global shows `Lakebase` only when that's the persisted engine; switching to `Neo4j` + reload renders `Neo4j` direct on first paint (no flicker).
- **#72 modular split** — every log line carries the `Neo4jConnection` module path, proving the extracted class is the runtime owner of the path (not the legacy `Neo4jStore`).

## :books: Deck + screenshots (committed in this PR)

Full deck (now **27 slides**, was 21) + the source PDF live under [`docs/v0.6-neo4j-demo/`](https://github.com/databrickslabs/ontobricks/tree/feature/neo4j-graphdb-skeleton/docs/v0.6-neo4j-demo):

- :page_facing_up: [`OntoBricks-PR47-Neo4j.pdf`](https://github.com/databrickslabs/ontobricks/blob/feature/neo4j-graphdb-skeleton/docs/v0.6-neo4j-demo/OntoBricks-PR47-Neo4j.pdf) — 5.8 MB, **27 slides** (21 from the v0.6 demo + 6 v0.7 post-review)
- :globe_with_meridians: [`deck.html`](https://github.com/databrickslabs/ontobricks/blob/feature/neo4j-graphdb-skeleton/docs/v0.6-neo4j-demo/deck.html) — same content, single-file HTML
- :framed_picture: [`screenshots/`](https://github.com/databrickslabs/ontobricks/tree/feature/neo4j-graphdb-skeleton/docs/v0.6-neo4j-demo/screenshots) — 13 v0.6 + 3 v0.7 PNGs + 1 v0.7 prod-log text capture
- :page_facing_up: [`secret-configuration.md`](https://github.com/databrickslabs/ontobricks/blob/feature/neo4j-graphdb-skeleton/docs/v0.6-neo4j-demo/secret-configuration.md) — admin guide for the secret-resource flow

### Key proof screenshots (v0.7)

**Settings → Neo4j · password badge `From Apps secret` (v0.7.0)**

![Settings secret-bound](https://raw.githubusercontent.com/databrickslabs/ontobricks/feature/neo4j-graphdb-skeleton/docs/v0.6-neo4j-demo/screenshots/20-settings-neo4j-secret-badge.png)

**Settings → Neo4j · Test connection → green Connected alert with Cypher probe**

![Test connection success](https://raw.githubusercontent.com/databrickslabs/ontobricks/feature/neo4j-graphdb-skeleton/docs/v0.6-neo4j-demo/screenshots/23-settings-neo4j-test-connection-result.png)

### Key proof screenshots (v0.6 — unchanged, runtime behaviour identical post-split)

**Build success · 3-card arch: Triple Store → Bolt (UNWIND·MERGE) → Graph DB (Neo4j) · 303 triples**

![Build success 303 triples](https://raw.githubusercontent.com/databrickslabs/ontobricks/feature/neo4j-graphdb-skeleton/docs/v0.6-neo4j-demo/screenshots/10-build-success-303-triples-neo4j.png)

**Cockpit · same 3-card arch · Digital Twin Active**

![Cockpit Neo4j active](https://raw.githubusercontent.com/databrickslabs/ontobricks/feature/neo4j-graphdb-skeleton/docs/v0.6-neo4j-demo/screenshots/16-cockpit-neo4j-active.png)

**Neo4j Browser · 303 nodes under `:WaterTreatment_V1` label**

![Neo4j Browser](https://raw.githubusercontent.com/databrickslabs/ontobricks/feature/neo4j-graphdb-skeleton/docs/v0.6-neo4j-demo/screenshots/12-neo4j-browser-303-nodes-graph.png)

**Inference · T-Box OWL 2 RL: 99 inferred in 0.102 s**

![Inference 99 inferred](https://raw.githubusercontent.com/databrickslabs/ontobricks/feature/neo4j-graphdb-skeleton/docs/v0.6-neo4j-demo/screenshots/15-inference-99-inferred.png)

**GraphQL Playground · real query against the Neo4j-backed graph**

![GraphQL Playground](https://raw.githubusercontent.com/databrickslabs/ontobricks/feature/neo4j-graphdb-skeleton/docs/v0.6-neo4j-demo/screenshots/17-graphql-playground-watertreatment.png)

**SHACL Data Quality · Graph mode against Neo4j · 92.3 % Consistency pass · 1 rule with 12 violations**

![SHACL on Neo4j](https://raw.githubusercontent.com/databrickslabs/ontobricks/feature/neo4j-graphdb-skeleton/docs/v0.6-neo4j-demo/screenshots/18-data-quality-graph-on-neo4j.png)

## What this PR ships

When a user picks Neo4j in **Settings → Triple store → Global** and configures URI / database / username in **Settings → Triple store → Neo4j** (the password comes from the Apps secret resource), the entire OntoBricks stack works against the Neo4j backend:

- **Build** writes triples via `Bolt` with `UNWIND` + `MERGE` over `:store`-labelled nodes
- **Knowledge Graph view**, **Inference**, **Graph Chat**, **GraphQL** all query Neo4j via Cypher (16+ named-query methods in `Neo4jReadOps`)
- **Reasoning** (SWRL/OWL) wired to `SWRLFlatCypherTranslator` — currently scaffolded (returns `None` + warns), full translation in a follow-up PR. T-Box OWL 2 RL still runs via RDFLib upstream of the store, which produced 99 inferred triples on the demo.
- **Settings UI** exposes a dedicated `Neo4j` sub-page with URI/database/username form, an "Auth method" toggle, the green/yellow secret-source badge, and a working **Test connection** button that runs a real Bolt handshake + `RETURN 1 AS probe` Cypher.
- **Build page + Cockpit** both show a 3-card architecture diagram: `Triple Store → Bolt (UNWIND·MERGE) → Graph DB (Neo4j)`, mirroring the Lakeflow Sync card on Lakebase.

Lakebase remains the default; existing Lakebase deployments are unaffected.

## Architecture decisions

- **Modular split** (v0.7, post-review). `Neo4jStore.py` is now a thin façade composing three focused services: `Neo4jConnection` (driver + auth + run + Cypher logging), `Neo4jWriteOps` (schema + bulk writes), `Neo4jReadOps` (16+ named queries — KG filter, statistics, reasoning helpers). Each file ≤ 700 LoC, single-class, name matches PascalCase.
- **Secret resource for credentials** (v0.7). The `neo4j-password` Apps resource is declared in `app.yaml.template` with `permission: READ` and bound directly via `databricks.yml`'s `dev-lakebase` target overlay (`scope: ontobricks, key: neo4j-password`). At runtime the deployed app refuses to instantiate `Neo4jStore` if `NEO4J_PASSWORD` is missing — raises `InfrastructureError` with a clear remediation pointer. No clear-text password ever lands in `global_config` (stripped at save-time when the env var is present).
- **Single-label-per-store schema** (post-bug-fix). Triples are persisted as `(:`*sanitised_table_name*`) {subject, predicate, object}` nodes. Neo4j 5+ rejects compound labels in `CREATE CONSTRAINT`.
- **No raw Cypher entry point**. `execute_query` raises `NotImplementedError`. All writes go through the ontology-validated build pipeline — preserves the C2 safeguard ("l'entrée se fait par l'ontologie", Benoit 20/05).
- **Cypher logging at INFO**. Every `_run` emits one log line per call (rows + duration + flattened cypher). Bound params at DEBUG only.
- **No UC Volume sync**. Neo4j Aura is remote-only; `sync_to_remote` / `sync_from_remote` / `local_path` are no-ops.
- **`engine_config` keys**: `uri`, `database`, `auth_method` (`basic` / `databricks_secret`), `username`, `encrypted`. The `password` key is local-dev-only and stripped server-side when the Apps secret is in place.
- **Flat triple model** in v1 (`supports_graph_model=False`). The native property-graph mode (typed nodes + typed relationships) lands in a follow-up PR.

## Open questions for @benoitcayladbx

1. `execute_query` → `NotImplementedError`. Aligned with the "l'entrée se fait par l'ontologie" rule from 20/05?
2. Flat-triple model (single label per store) for v1; typed-node graph model deferred. OK?
3. Modular split — Connection / WriteOps / ReadOps + façade pattern. OK for the codebase shape going forward? Or do you prefer a different cut (e.g. single Operations class, or mixins on the Store)?
4. End-user secret flow — today we ship the "admin runs `databricks secrets put-secret` + bind via DAB or Apps UI" path (option A in our 2026-06-25 sync). A follow-up PR could add a Settings-UI wizard that pre-fills the CLI command and deep-links to the Apps UI Resources page (option B). Auto-binding from the app (option C) is blocked by the Apps platform.

## Test plan — all green ✅

- [x] `python3 -m py_compile` on every changed `.py` — OK
- [x] `node --check` on `settings.js` / `query-sync.js` / `domain-validation.js` — OK
- [x] `make bundle-validate` on `dev-lakebase` target — clean
- [x] `make deploy` to `fevm-mjolnir` (with `LAKEBASE_PROJECT / WAREHOUSE_ID / REGISTRY_CATALOG` overrides for the workspace) — `ontobricks-070` RUNNING (2026-06-25)
- [x] Workspace secret `ontobricks/neo4j-password` created via CLI; Apps resource `neo4j-password` bound via DAB (`databricks.yml`)
- [x] Live E2E against Aura — `tests/integration/neo4j_e2e_smoke.py` — 9 / 9 assertions pass
- [x] UI: dropdown + section + auth toggle + Save — verified via Chrome MCP; engine selector no longer flickers from Lakebase to Neo4j on page load
- [x] Settings → Triple store → Neo4j shows the `From Apps secret` (green) badge; password input is disabled with `••••••••` placeholder
- [x] **Settings → Neo4j → Test connection returns green** with handshake latency + Cypher echo + credentials source (verified live 2026-06-25 at 19:19 UTC)
- [x] App logs contain `Cypher (n rows, ms): <flattened>` for every Cypher executed — confirmed for `RETURN 1 AS probe`; bound params absent from INFO lines
- [x] Persistence verified via API: `GET /settings/graph-engine` returns `neo4j`, config has `uri/db/auth/username`, **`password` key absent** (stripped at save-time)
- [x] Neo4j Browser shows 303 nodes with full W3C URI subjects/predicates/objects
- [x] Build pipeline through the OntoBricks UI (Domain → Build) — Sync Result panel: **303 total triples · 5.3 s**
- [x] Inference UI — T-Box OWL 2 RL = **99 inferred**, SWRL skipped (scaffold no-op as designed)
- [x] SHACL Data Quality in Graph mode against Neo4j — 13 auto-generated Consistency rules, **92.3 % pass**
- [x] GraphQL Playground — real query (`pfascompounds + facilities + treatmentprocesses`) returns the expected entities from Neo4j

Smoke-test artefact (committed): `tests/integration/neo4j_e2e_smoke.py` — runnable by any contributor with `neo4j>=5.0` and the Aura creds file.

cc @benoitcayladbx — branch is ready-for-review on the code AND on visible proof. The 6 post-review commits are listed at the top so you can read them in chronological order.

## Effort estimate

For benchmarking future v0.x backend slots. **Honest, triangulated from commit timestamps + session memory — not stopwatch.**

- **~20 – 25 effective hours of focused dev/design** for the original 0.6.0 demo (2026-05-13 → 2026-06-12).
- **~10 – 12 additional effective hours** for the 6 post-review commits (2026-06-22 → 2026-06-25).
- **Phase split (effective hours, cumulative)**:
  - Pre-impl (v0.4 study + v0.6 design proposal + 2026-05-20 sync with Benoit + post-sync writeup): ~4 – 6 h
  - Backend (`Neo4jStore.py` ~580 LoC + factory dispatch + reasoning scaffold + smoke test): ~8 – 11 h
  - Frontend (settings UI, JS wiring, engine-aware Build / Cockpit labels, 3-card arch + Bolt writer card): ~4 – 6 h
  - Bug-hunt (label schema, `--extra neo4j`, `triplestore_page_context` tautology + 4 JS reconciliation fixes): ~2 – 3 h
  - Demo + deck (21-slide HTML/PDF, screenshots, SHACL run, GraphQL → Cypher behind-the-scenes slide): ~3 – 4 h
  - **Post-review iteration v0.7** (secret-resource auth, Cypher logging, modular split, Settings flash fix, Test-connection wire, RETURN-1 probe, deck → 27 slides, smoke test on fresh `ontobricks-070` deploy): ~10 – 12 h
- **Compared to a green-field build** (driver eval + GraphQL schema mapping + R2RML→Cypher patterns + smoke test infra + deck from scratch) this would probably be **80 – 120 hours**.

This pull request and its description were written collaboratively by Hugues and Claude.
