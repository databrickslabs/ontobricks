# Deck v0.7.0 — new slides

To insert into `docs/v0.6-neo4j-demo/deck.html` AFTER the existing slide 5 ("Backend changes") and BEFORE slide 6 ("Frontend changes"), introducing a new "Post-review iteration" section. Total slides: 21 → 26.

Each slide below is described in markdown for review; the final HTML will mirror the existing `<section class="slide">` block structure of `deck.html`.

---

## Slide 6 (new) — "Post-review iteration — what changed since 0.6.0"

**Section divider slide.** Visual: 5 numbered tiles, one per punch-list item, each with a 1-line description + commit SHA.

| # | Title | Subtitle | Commit |
|---|---|---|---|
| 1 | **Secret resource** | Neo4j password sourced from Databricks Apps secret, no clear-text in `global_config` | `da9cae9` |
| 2 | **Cypher logging** | Every `_run` → INFO log line (rows + ms + flattened cypher) | `e8b523c` |
| 3 | **Version 0.7.0** | `pyproject.toml` + README + `DEFAULT_APP_NAME` | `577b70f` |
| 4 | **Settings UI fix** | No more Lakebase-flashes-then-Neo4j flicker | `e63bfce` |
| 5 | **Modular split** | `Neo4jStore.py` (1028 LoC) → 4 files, façade pattern | `7a9a625` |

Title: **"5/5 of @benoitcayladbx's PR review punch-list addressed"**
Subtitle: *"5 commits, 2026-06-22 → 2026-06-25"*

---

## Slide 7 (new) — "Neo4j password → Databricks Apps secret"

**Visual: data flow diagram (left to right)**

```
┌──────────────────┐    bind via       ┌──────────────────┐    valueFrom    ┌──────────────────┐
│ Workspace secret │ ───── Apps UI ──→ │  Apps resource   │ ──────────────→ │ NEO4J_PASSWORD   │
│ ontobricks/      │                   │  neo4j-password  │  env var inject │ env var in       │
│ neo4j-password   │                   │  (READ permission)│                │ deployed app     │
└──────────────────┘                   └──────────────────┘                 └──────────────────┘
                                                                                       │
                                                                                       ▼
                                                                            ┌──────────────────┐
                                                                            │ Neo4jConnection. │
                                                                            │ _resolve_auth()  │
                                                                            └──────────────────┘
```

**Three "Why this matters" bullets** on the right:

- **No clear-text** in `global_config`. The persisted JSON `password` is stripped at save-time when the env var is present (`SettingsService.set_graph_engine_config_result`).
- **Deployed app refuses to start** Neo4jStore without the env var (`InfrastructureError` with remediation pointer). Local-dev keeps the `engine_config.password` fallback (guarded by `DATABRICKS_APP_PORT`).
- **UI badge** flips between 🟢 `From Apps secret` and 🟡 `Local-dev fallback` based on `is_neo4j_password_from_secret()` — read by `home.py` at render time.

**Screenshot to capture during smoke test**: Settings → Triple store → Neo4j with the green "From Apps secret" badge above the password input (which is disabled with `••••••••` placeholder). Save as `screenshots/20-settings-neo4j-secret-badge.png`.

**Doc**: link to `docs/v0.6-neo4j-demo/secret-configuration.md`.

---

## Slide 8 (new) — "Cypher logging at INFO — every query in the logs"

**Visual: log excerpt screenshot + format spec**

Format:
```
Cypher (<n> rows, <ms> ms): <whitespace-flattened cypher (max 1500 chars, "… (truncated)" beyond)>
```

**Example log line** (taken from app logs during smoke test):
```
2026-06-25 09:14:23 INFO ontobricks.core.graphdb.neo4j.Neo4jConnection
  Cypher (303 rows, 12.4 ms): MATCH (t:`WaterTreatment_V1`) RETURN t.subject AS subject, t.predicate AS predicate, t.object AS object
```

**Three points on the right**:

- **No credential leak.** Bound `params` are logged at DEBUG only; auth lives on the driver (not per-session) so the password never transits through any `session.run` call.
- **Grep-friendly.** Multi-line f-string cypher is collapsed to a single line; long statements are truncated at 1500 chars with a `… (truncated)` marker.
- **One INFO line per call.** Includes row count + duration so operators can spot slow queries in the Databricks log explorer.

**Screenshot to capture**: Databricks app logs page showing a few minutes of INFO output with multiple `Cypher (...)` lines from a Build + KG filter cycle. Save as `screenshots/21-app-logs-cypher-info.png`.

---

## Slide 9 (new) — "Settings UI no longer flickers on load"

**Visual: side-by-side before/after** (animated GIF would be ideal but a 2-pane screenshot works)

| Before (v0.6.0) | After (v0.7.0) |
|---|---|
| Page loads → engine selector shows **Lakebase** (HTML default) → JS fetches `/settings/graph-engine` → snap to **Neo4j**. Visible flicker. | Page loads → engine selector **already shows Neo4j** (Jinja `{% if graph_engine == "neo4j" %}selected{% endif %}`). No JS round-trip needed for the initial paint. |

**Three points**:

- **Server-side resolution**. `home.py` settings route calls `SettingsService.get_graph_engine_result(...)` before render. Failure is non-fatal (logs a warning, falls back to `"lakebase"` — same as before).
- **No regression in degraded mode**. If the global-config service is unreachable, the page still loads with the default selector.
- **JS reconciliation still runs** on first tab-visit (for cross-tab consistency after Save), but it's a no-op when the server-rendered value already matches.

**Screenshot to capture**: Two browser screenshots side by side. (a) Fresh page load on Neo4j-configured app, captured at the first paint — selector shows Neo4j. (b) Same page after the JS lazy-load — still Neo4j. No diff visible. Save as `screenshots/22-settings-no-flash-before-after.png`.

---

## Slide 10 (new) — "Modular split — `Neo4jStore.py` → 4 files"

**Visual: composition diagram**

```
                ┌──────────────────────────────────────┐
                │      Neo4jStore (façade, 435 LoC)    │
                │  • GraphDBBackend interface          │
                │  • Capability flags                  │
                │  • Sync no-ops, execute_query refuse │
                │  • Thin delegators                   │
                └────────────────┬─────────────────────┘
                                 │ composes
                ┌────────────────┼────────────────────────────────┐
                ▼                ▼                                ▼
┌─────────────────────┐  ┌──────────────────┐  ┌──────────────────────────┐
│ Neo4jConnection     │  │ Neo4jWriteOps    │  │ Neo4jReadOps             │
│ (227 LoC)           │  │ (156 LoC)        │  │ (614 LoC)                │
│                     │  │                  │  │                          │
│ • _resolve_auth     │  │ • create_table   │  │ • get_aggregate_stats    │
│ • get_driver/close  │  │ • drop_table     │  │ • find_subjects_by_type  │
│ • run() + INFO log  │  │ • insert_triples │  │ • find_seed_subjects     │
│ • _normalise_cypher │  │ • delete_triples │  │ • bfs_traversal          │
│                     │  │ • optimize_table │  │ • expand_entity_neighbors│
│                     │  │ • delete_cohort  │  │ • get_entity_metadata    │
│                     │  │ • sanitise_label │  │ • get_triples_for_subj.. │
│                     │  │                  │  │ • paginated_*            │
│                     │  │                  │  │ • transitive_closure     │
│                     │  │                  │  │ • symmetric_expand       │
│                     │  │                  │  │ • shortest_path          │
└─────────────────────┘  └──────────────────┘  └──────────────────────────┘
```

**Three "Why this matters" bullets**:

- **Single concern per file**. Auth + driver lifecycle in one place (security-critical). CRUD writes isolated from read queries.
- **Fowler vocab applied**: *Large Class → Extract Class*. Documented in `src/.coding_rules.md §9`.
- **Public API unchanged**. `Neo4jStore(...)` constructor + every `TripleStoreBackend` method unchanged. Factory and existing tests continue to work without modification. Façade pattern preserves the legacy import path.

**Bottom note**: *"Tests still pass: 7 password-sourcing + 3 Cypher-logging + ~30 CRUD/named-query unit tests. Mocking moved from `s._run` to `s._connection.run` with a back-compat alias."*

---

## Insertion plan

1. **Re-number existing slides 6–21 to 11–26.**
2. **Update cover slide** (slide 1):
   - Title: `"OntoBricks v0.7 · Neo4j Integration — PR #47"` (was v0.6)
   - Subtitle: `"E2E green ✅ — 5/5 of Benoit's PR review punch-list addressed"`
   - Date footer: `"2026-06-25"`
3. **Update TL;DR slide** (slide 2): add the "Post-review iteration" highlight (5 commits, 6–8 effective hours).
4. **Update navigation index** in `deck.html` script section if any.

---

## Action items after smoke test

- [ ] Capture `20-settings-neo4j-secret-badge.png`
- [ ] Capture `21-app-logs-cypher-info.png`
- [ ] Capture `22-settings-no-flash-before-after.png` (optional — may be hard to capture the absence-of-flicker statically)
- [ ] Replace placeholder image URLs in `PR-47-description-v4.md` with the real ones
- [ ] HTML-ify these 5 slides into `deck.html` mirroring the existing slide block structure
- [ ] Regenerate `OntoBricks-PR47-Neo4j.pdf` via `npx --yes md-to-pdf` or browser print
- [ ] `gh pr edit 47 --body-file syncs/2026-06-25/PR-47-description-v4.md`
