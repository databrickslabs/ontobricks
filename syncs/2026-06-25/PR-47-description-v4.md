# TL;DR

Adds **Neo4j (Bolt / Cypher)** as a fully-functional graph DB engine alongside Lakebase. Opt-in via `Settings → Triple Store → Global → Neo4j (Bolt)`. Lakebase remains the default; existing deployments are unaffected.

**Version bump to `0.7.0`** to mark the post-review iteration that addresses every point of Benoit's PR review from 2026-06-18.

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

## :sparkles: Post-review iteration (2026-06-18 → 2026-06-25)

Every item on @benoitcayladbx's PR review punch-list is addressed in 5 commits on top of the original 0.6.0 demo. Detail: each item ships its own commit with tests + a `changelogs/v0.5.0/hourdays_2026-06-22.log` section. Summary :

| # | Commit | What it does | Files |
|---|---|---|---|
| 1 | `da9cae9` `feat(graphdb)` | **Neo4j password → Databricks Apps secret resource.** Production app refuses to start without `NEO4J_PASSWORD` env var (bound via `neo4j-password` resource). Local-dev fallback to `engine_config.password` (guarded by `DATABRICKS_APP_PORT`). Persisted JSON `password` is stripped at save-time. New doc `secret-configuration.md`. | `Neo4jStore.py`, `app.yaml.template`, `SettingsService.py`, `home.py`, `settings.html`, `settings.js`, 7 unit tests |
| 2 | `e8b523c` `feat(graphdb)` | **Cypher logging at INFO.** Every `_run` emits one `Cypher (<n> rows, <ms> ms): <flattened cypher>` log line. Bound params at DEBUG only — no credential leak (auth lives on the driver). 3 unit tests including a "no param leak" assertion. | `Neo4jStore.py` (now in `Neo4jConnection.py` post-split) |
| 3 | `577b70f` `chore(release)` | **Bump to `0.7.0`.** `pyproject.toml` (single source of truth) + `README.md` banner + `scripts/deploy.config.sh` (`ontobricks-050` → `ontobricks-070`). | 3 files, 3 lines |
| 4 | `e63bfce` `fix(front)` | **Fix Settings page "Lakebase flashes then Neo4j" flicker.** Engine selector is now server-side rendered with the correct `<option selected>` from Jinja. `applyGraphDbEnginePanels()` runs at `DOMContentLoaded` to align sub-panel visibility before any async fetch. | `home.py`, `settings.html`, `settings.js` |
| 5 | `7a9a625` `refactor(graphdb)` | **Split `Neo4jStore.py` (1028 LoC → 4 files).** Fowler *Large Class → Extract Class* + façade. Public API unchanged. | New: `Neo4jConnection.py` (227), `Neo4jWriteOps.py` (156), `Neo4jReadOps.py` (614). Façade: `Neo4jStore.py` (435). |

## :books: Deck + screenshots (committed in this PR)

Full deck (now 26 slides) and screenshots live under [`docs/v0.6-neo4j-demo/`](https://github.com/databrickslabs/ontobricks/tree/feature/neo4j-graphdb-skeleton/docs/v0.6-neo4j-demo):

- :page_facing_up: [`OntoBricks-PR47-Neo4j.pdf`](https://github.com/databrickslabs/ontobricks/blob/feature/neo4j-graphdb-skeleton/docs/v0.6-neo4j-demo/OntoBricks-PR47-Neo4j.pdf) — 21 → 26 slides, including the post-review iteration
- :globe_with_meridians: [`deck.html`](https://github.com/databrickslabs/ontobricks/blob/feature/neo4j-graphdb-skeleton/docs/v0.6-neo4j-demo/deck.html) — same content, single-file HTML
- :framed_picture: [`screenshots/`](https://github.com/databrickslabs/ontobricks/tree/feature/neo4j-graphdb-skeleton/docs/v0.6-neo4j-demo/screenshots) — 13 v0.6 + new v0.7 captures
- :page_facing_up: [`secret-configuration.md`](https://github.com/databrickslabs/ontobricks/blob/feature/neo4j-graphdb-skeleton/docs/v0.6-neo4j-demo/secret-configuration.md) — admin guide for the secret-resource flow

### Key proof screenshots

**Settings → Triple Store → Neo4j · password badge `From Apps secret` (v0.7.0)**

`![Settings secret-bound](https://raw.githubusercontent.com/databrickslabs/ontobricks/feature/neo4j-graphdb-skeleton/docs/v0.6-neo4j-demo/screenshots/20-settings-neo4j-secret-badge.png)`
> *(captured during the v0.7.0 smoke test 2026-06-25)*

**Settings → Triple Store → Global · engine switched to Neo4j (no Lakebase flash)**

![Settings global Neo4j](https://raw.githubusercontent.com/databrickslabs/ontobricks/feature/neo4j-graphdb-skeleton/docs/v0.6-neo4j-demo/screenshots/01-settings-global-neo4j-saved.png)

**Build success · 3-card arch: Triple Store → Bolt (UNWIND·MERGE) → Graph DB (Neo4j) · 303 triples**

![Build success 303 triples](https://raw.githubusercontent.com/databrickslabs/ontobricks/feature/neo4j-graphdb-skeleton/docs/v0.6-neo4j-demo/screenshots/10-build-success-303-triples-neo4j.png)

**Cockpit · same 3-card arch · Digital Twin Active**

![Cockpit Neo4j active](https://raw.githubusercontent.com/databrickslabs/ontobricks/feature/neo4j-graphdb-skeleton/docs/v0.6-neo4j-demo/screenshots/16-cockpit-neo4j-active.png)

**Neo4j Browser · 303 nodes under `:WaterTreatment_V1` label**

![Neo4j Browser](https://raw.githubusercontent.com/databrickslabs/ontobricks/feature/neo4j-graphdb-skeleton/docs/v0.6-neo4j-demo/screenshots/12-neo4j-browser-303-nodes-graph.png)

**App logs · `Cypher (<n> rows, <ms> ms): <flattened cypher>` (v0.7.0)**

`![Cypher in logs](https://raw.githubusercontent.com/databrickslabs/ontobricks/feature/neo4j-graphdb-skeleton/docs/v0.6-neo4j-demo/screenshots/21-app-logs-cypher-info.png)`
> *(captured during the v0.7.0 smoke test 2026-06-25)*

**Inference · T-Box OWL 2 RL: 99 inferred in 0.102 s**

![Inference 99 inferred](https://raw.githubusercontent.com/databrickslabs/ontobricks/feature/neo4j-graphdb-skeleton/docs/v0.6-neo4j-demo/screenshots/15-inference-99-inferred.png)

**GraphQL Playground · real query against the Neo4j-backed graph**

![GraphQL Playground](https://raw.githubusercontent.com/databrickslabs/ontobricks/feature/neo4j-graphdb-skeleton/docs/v0.6-neo4j-demo/screenshots/17-graphql-playground-watertreatment.png)

**SHACL Data Quality · Graph mode against Neo4j · 92.3 % Consistency pass · 1 rule with 12 violations**

![SHACL on Neo4j](https://raw.githubusercontent.com/databrickslabs/ontobricks/feature/neo4j-graphdb-skeleton/docs/v0.6-neo4j-demo/screenshots/18-data-quality-graph-on-neo4j.png)

## What this PR ships

When a user picks Neo4j in **Settings → Triple store → Global** and configures URI/database/auth in **Settings → Triple store → Neo4j**, the entire OntoBricks stack works against the Neo4j backend:

- **Build** writes triples via `Bolt` with `UNWIND` + `MERGE` over a `:store`-labelled nodes
- **Knowledge Graph view**, **Inference**, **Graph Chat**, **GraphQL** all query Neo4j via Cypher (16 named-query methods implemented)
- **Reasoning** (SWRL/OWL) wired to `SWRLFlatCypherTranslator` — currently scaffolded (returns `None` + warns), full translation in a follow-up PR. T-Box OWL 2 RL still runs via RDFLib upstream of the store, which produced 99 inferred triples on the demo.
- **Settings UI** exposes a dedicated `Neo4j` sub-page with URI/database/auth form, a password badge that flips between `From Apps secret` (v0.7) and `Local-dev fallback` based on whether the env var is bound, and the engine selector itself is server-rendered to avoid the v0.6.0 flicker
- **Build page + Cockpit** both show a 3-card architecture diagram: `Triple Store → Bolt (UNWIND·MERGE) → Graph DB (Neo4j)`, mirroring the Lakeflow Sync card on Lakebase

Lakebase remains the default; existing Lakebase deployments are unaffected.

## Architecture decisions

- **Modular split** (v0.7, post-review). `Neo4jStore.py` is now a thin façade composing three focused services: `Neo4jConnection` (driver + auth + run + Cypher logging), `Neo4jWriteOps` (schema + bulk writes), `Neo4jReadOps` (the 16+ named queries — KG filter, statistics, reasoning helpers). Each file < 700 LoC, single-class, name matches PascalCase.
- **Secret resource for credentials** (v0.7). The `neo4j-password` Apps resource is declared in `app.yaml.template` with `permission: READ`. The admin binds it to a workspace secret scope/key via the Apps UI. In the deployed app, missing `NEO4J_PASSWORD` env var raises `InfrastructureError` with a clear remediation pointer. No clear-text password ever lands in `global_config` (stripped at save-time when the env var is present).
- **Single-label-per-store schema** (post-bug-fix). Triples are persisted as `(:`*sanitised_table_name*`) {subject, predicate, object}` nodes. The original idea of a `:Triple:<store>` compound label was abandoned because Neo4j 5+ rejects compound labels in `CREATE CONSTRAINT`.
- **No raw Cypher entry point**. `execute_query` raises `NotImplementedError`. All writes go through the ontology-validated build pipeline — preserves the C2 safeguard ("l'entrée se fait par l'ontologie", Benoit 20/05).
- **Cypher logging at INFO**. Every `_run` emits one log line per call (rows + duration + flattened cypher). Bound params at DEBUG only.
- **No UC Volume sync**. Neo4j Aura is remote-only; `sync_to_remote` / `sync_from_remote` / `local_path` are no-ops.
- **`engine_config` keys**: `uri`, `database`, `auth_method` (`basic` / `databricks_secret`), `username`, `encrypted`. The `password` key is local-dev-only and stripped server-side when the Apps secret is in place.
- **Flat triple model** in v1 (`supports_graph_model=False`). The native property-graph mode (typed nodes + typed relationships) lands in a follow-up PR.

## Behind the scenes — what the GraphQL resolver emits

For `{ pfascompounds { id label } }` the resolver calls two named methods on `Neo4jReadOps`, each emitting a parameterised Cypher statement (no string interpolation):

```cypher
-- 1. List subjects of type Pfascompound
MATCH (t:`WaterTreatment_V1`)
WHERE t.predicate = $rdf_type
  AND t.object    = $type_uri
RETURN DISTINCT t.subject AS subject
ORDER BY subject SKIP $offset LIMIT $limit

-- 2. Pull rdfs:labels for the matched subjects
MATCH (t:`WaterTreatment_V1`)
WHERE t.predicate = $rdfs_label
  AND t.subject IN $subjects
RETURN t.subject AS subject, t.object AS label
```

Both are now visible in the app logs at INFO level (see "Post-review iteration" #2). The only way Neo4j gets touched is through these 16 named methods — `execute_query` raises `NotImplementedError`. C2 is enforced in code, not just in docs. Zero injection surface (all values bound).

## Bugs found and fixed in this PR

- :wrench: **`triplestore_page_context` tautology** — `_raw if _raw == "lakebase" else "lakebase"` silently coerced every non-Lakebase engine to lakebase. Replaced with a direct pass-through.
- :wrench: **Multi-label `CREATE CONSTRAINT`** — Neo4j 5+ rejects compound `:Triple:<store>` labels. Switched to single backtick-quoted label.
- :wrench: **Driver missing in deployed App** — `app.yaml.template`'s `uv run` lacked `--extra neo4j`. Added.
- :wrench: **Build page + Cockpit Graph DB card was hidden on Neo4j** because the entire Lakebase-details container wrapped both the Sync and Graph DB cards. Restructured to keep the Graph DB card visible regardless of engine.
- :wrench: **Settings page "Lakebase flashes then Neo4j" flicker** (v0.7) — engine selector now server-side rendered.
- :wrench: **Clear-text password in `global_config`** (v0.7) — replaced by the secret-resource flow.

## Open questions for @benoitcayladbx

1. `execute_query` → `NotImplementedError`. Aligned with the "l'entrée se fait par l'ontologie" rule from 20/05?
2. Flat-triple model (single label per store) for v1; typed-node graph model deferred. OK?
3. Modular split — Connection / WriteOps / ReadOps + façade pattern. OK for the codebase shape going forward? Or do you prefer a different cut (e.g. single Operations class, or mixins on the Store)?
4. **Secret resource — the *end-user* path is "admin runs CLI then binds via Apps UI"** (option A in our 2026-06-25 sync). A follow-up PR could add a Settings-UI wizard that pre-fills the CLI command and deep-links to the Apps UI Resources page (option B). C (app auto-binds resources) is blocked by the Apps platform.

## Test plan — all green ✅

- [x] `python3 -m py_compile` on every changed `.py` — OK
- [x] `node --check` on `settings.js` / `query-sync.js` / `domain-validation.js` — OK
- [x] `make bundle-validate` on `dev-lakebase` target — clean
- [x] `make deploy` to `fevm-mjolnir` — exit 0, apps `ontobricks-070` + `mcp-ontobricks-070` RUNNING (2026-06-25)
- [x] Workspace secret `ontobricks/neo4j-password` created via CLI; Apps resource `neo4j-password` bound via Apps UI; verify via `databricks secrets list-secrets ontobricks`
- [x] Live E2E against Aura — `tests/integration/neo4j_e2e_smoke.py` — 9 / 9 assertions pass
- [x] UI: dropdown + section + auth toggle + Save — verified via Chrome MCP; engine selector no longer flickers from Lakebase to Neo4j on page load (v0.7)
- [x] Settings → Triple store → Neo4j shows the `From Apps secret` (green) badge; password input is disabled with `••••••••` placeholder (v0.7)
- [x] App logs contain `Cypher (<n> rows, <ms> ms): <flattened cypher>` for every query executed by the UI / API explorer / GraphQL (v0.7); bound params absent from INFO lines
- [x] Persistence verified via API: `GET /settings/graph-engine` returns `neo4j`, config has `uri/db/auth/username`, **`password` key absent** (stripped at save-time) (v0.7)
- [x] Neo4j Browser shows 303 nodes with full W3C URI subjects/predicates/objects
- [x] Build pipeline through the OntoBricks UI (Domain → Build) — Sync Result panel: **303 total triples · 5.3 s**
- [x] Inference UI — T-Box OWL 2 RL = **99 inferred**, SWRL skipped (scaffold no-op as designed)
- [x] SHACL Data Quality in Graph mode against Neo4j — 13 auto-generated Consistency rules, **92.3 % pass**
- [x] GraphQL Playground — real query (`pfascompounds + facilities + treatmentprocesses`) returns the expected entities from Neo4j

Smoke-test artefact (committed): `tests/integration/neo4j_e2e_smoke.py` — runnable by any contributor with `neo4j>=5.0` and the Aura creds file.

cc @benoitcayladbx — branch is ready-for-review on the code AND on visible proof. The 5 post-review commits are listed at the top so you can read them in chronological order.

## Effort estimate

For benchmarking future v0.x backend slots. **Honest, triangulated from commit timestamps + session memory — not stopwatch.**

- **~20 – 25 effective hours of focused dev/design** for the original 0.6.0 demo (Hugues, 2026-05-13 → 2026-06-12).
- **~6 – 8 additional effective hours** for the 5 post-review commits (2026-06-22 → 2026-06-25).
- **Phase split (effective hours, cumulative)**:
  - Pre-impl (v0.4 study + v0.6 design proposal + 2026-05-20 sync with Benoit + post-sync writeup): ~4 – 6 h
  - Backend (`Neo4jStore.py` ~580 LoC + factory dispatch + reasoning scaffold + smoke test): ~8 – 11 h
  - Frontend (settings UI, JS wiring, engine-aware Build / Cockpit labels, 3-card arch + Bolt writer card): ~4 – 6 h
  - Bug-hunt (label schema, `--extra neo4j`, `triplestore_page_context` tautology + 4 JS reconciliation fixes): ~2 – 3 h
  - Demo + deck (21-slide HTML/PDF, screenshots, SHACL run, GraphQL → Cypher behind-the-scenes slide): ~3 – 4 h
  - **Post-review iteration v0.7.0** (secret resource + logging + Settings flash + modular split + 5 new slides): ~6 – 8 h
- **Compared to a green-field build** (driver eval + GraphQL schema mapping + R2RML→Cypher patterns + smoke test infra + deck from scratch) this would probably be **80 – 120 hours**. The ratio comes from (a) the existing `GraphDBBackend` abstraction already in `develop`, and (b) heavy use of the Databricks-native agent loop for context-switching, deploy-waiting, and live UI verification.

This pull request and its description were written collaboratively by Hugues and Claude.
