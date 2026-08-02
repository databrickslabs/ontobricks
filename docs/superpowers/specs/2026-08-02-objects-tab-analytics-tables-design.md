# Analytics Delta tables in the Settings → Objects tabs

Date: 2026-08-02
Status: approved, ready for implementation planning

## Problem

Every Run Analysis writes Unity Catalog Delta tables through the Lakeflow job:
`graph_metrics_<slug>` plus `_summary`, `_type_profiles`, `_type_predicates`,
and — when a run dies before its cleanup — stranded `_work_<stage>` scratch
tables. `DigitalTwin.analytics_output_table` puts them in
`analytics_job_output_schema`, defaulting to the registry `catalog.schema`, so
they sit right next to the triple-store objects.

Neither Objects tab shows them. Settings → Lakehouse → Objects lists only names
starting with `triplestore_` (`group_triplestore_objects`), and Settings →
Lakebase → Objects lists Postgres objects plus the Lakeflow sync sub-block.
An operator cleaning up a domain therefore drops the graph and silently leaves
the analytics tables behind, with no UI that can even see them.

## Decisions

Taken with the user during brainstorming.

1. **Nest analytics under the matching domain-version card**, not in their own
   top-level cards. One card per domain version is the mental model both tabs
   already use.
2. **The card's Delete drops the analytics tables too.** Deleting a domain
   version means every artefact for it, graph and analytics alike.
3. **`_work_*` scratch tables are listed and deleted** with the rest. They are
   pure garbage from failed runs; hiding them is how they accumulated.
4. **On the Lakebase tab, analytics get their own UC block**, separate from the
   existing Lakeflow sync sub-block, still covered by the card's Delete. The
   sync block is about Postgres↔UC replication; analytics output is neither.
5. **Unmatched analytics go to one catch-all "Orphan analytics" card**, never
   hidden. Renames and slug mismatches are exactly when an operator needs to
   find these tables.
6. **One schema scan, client-side nesting.** The existing UC enumeration is
   extended with a second grouping rather than adding a registry join or a
   second endpoint.

## Architecture

```
Load objects
  ├── Lakehouse  GET /settings/triple-store/databricks-objects
  │      one scan of the analytics schema
  │        → domains[]    triplestore_* groups          (existing)
  │        → analytics[]  graph_metrics_* groups        (new)
  │        → orphans[]    analytics with no domain key  (new)
  │
  └── Lakebase   existing Postgres load + sync-slot fill
         + the same databricks-objects payload
           → .lk-analytics-slot per domain card
           → Orphan analytics card when unmatched remain

match key = "<safe>_<version>"
  triplestore_<safe>_V<n>      → "<safe>_<n>"
  graph_metrics_<slug>[...]    → "<slug>"
  Lakebase card base <Domain>_V<n> → "<lower(domain)>_<n>"

Delete (domain card) = existing objects in existing order,
                       then analytics: _work_* → _type_predicates
                       → _type_profiles → _summary → base
```

The analytics schema is `settings.analytics_job_output_schema` when set,
otherwise the registry `catalog.schema` — the same resolution
`DigitalTwin._build_job_metrics` uses, so the tab looks where the job writes.
When that schema differs from the registry schema, the tab performs a second UC
enumeration; when it is the same (the default), the existing single scan is
reused.

### Why the match can miss

`triplestore_<safe>_V<n>` derives `safe` from
`re.sub(r"[^a-z0-9_]", "_", name.lower())` (`SQLHelpers.effective_view_table`),
while the analytics slug derives from `uc_domain_folder`, produced by
`sanitize_domain_folder`, which *removes* offending characters instead of
replacing them. For an ordinary domain name the two agree; for a name with
punctuation they do not (`my.domain` → `my_domain` vs `mydomain`). Rather than
reconciling two naming rules — a change with build-path blast radius — the
mismatch resolves to the Orphan card, which is exactly what decision 5 is for.

## Components

### 1. Grouping helpers — `src/back/core/graphdb/delta/objects.py`

`group_triplestore_objects` and `object_base` are unchanged.

New, alongside them:

- `_ANALYTICS_PREFIX = "graph_metrics_"` and the suffix set
  `_summary`, `_type_profiles`, `_type_predicates`, plus the `_work_` infix.
- `analytics_base(name) -> str` — strips a `_work_<stage>` tail first (it can
  follow any stage name), then a known output suffix, leaving
  `graph_metrics_<slug>`.
- `analytics_match_key(name) -> str` — `analytics_base` minus the prefix, i.e.
  the slug, which is the join key.
- `domain_match_key(name) -> str` — `triplestore_<safe>_V<n>` → `<safe>_<n>`,
  lowercased. Returns `""` for anything that does not parse.
- `_analytics_drop_sort_key(name)` — orders `_work_*` (0), `_type_predicates`
  (1), `_type_profiles` (2), `_summary` (3), base (4). All five are managed
  tables with no dependencies between them, so the order is for a predictable
  progress list, not for correctness.
- `group_analytics_objects(raw_tables, catalog, schema) -> Dict[str, Dict]` —
  same shape as `group_triplestore_objects`: `{key: {base, items,
  sorted_items}}`, each item `{kind, name, full_name, table_type}`.

### 2. Service — `SettingsService.triple_store_databricks_objects_result`

Keeps its current contract and gains two keys:

- `analytics`: `[{key, base, items: [...]}]`, sorted by `base`
- `orphans`: the subset of `analytics` whose `key` matches no entry in
  `domains`, so the front end never has to recompute the difference

`domains` entries gain `key` (their `domain_match_key`) so the front can nest
without re-parsing names.

The response also gains `analytics_location` (`catalog.schema` actually
scanned) so the UI can show where it looked when a card is empty, and
`analytics_message`, empty on success and carrying the reason when the scan
failed.

`registry_configured: false` short-circuits as today, with `analytics` and
`orphans` empty.

### 3. Lakehouse Objects tab — `settings.js` `loadDeltaObjects`

Each domain card body gains an analytics sub-block after the existing table,
styled like the Lakebase sync block (`border-top`, uppercase caption, the
`analytics_location` as a monospace badge). Rows reuse `mkObjectRow`, with a
`work` badge distinguishing scratch tables from outputs.

`_dtDomainRegistry[key]` gains `analyticsItems` (already drop-sorted by the
service), and `dropDeltaDomainObjects` passes
`sortedItems.concat(analyticsItems)` to `_execDropAllDelta`, which needs no
change — analytics items carry the same `{kind, name, full_name}` shape and
`kind` is always `table`.

When `orphans` is non-empty, one extra card renders after the domain cards,
titled "Orphan analytics", with a one-line explanation and its own Delete that
drops every listed table. Its rows carry the full FQN rather than the short
name, since there is no card context to read them against.

### 4. Lakebase Objects tab — `settings.js` `loadLakebaseObjects`

Each domain card body gains `.lk-analytics-slot`, a sibling of the existing
`.lk-sync-slot`, filled from the same `databricks-objects` payload after the
Postgres render. Same visual treatment as the sync block, captioned
"Analytics (Unity Catalog)".

A module-level `_lkAnalyticsRegistry` keyed by domain base mirrors
`_lkUCRegistry`. `dropDomainObjects` includes those items in its confirmation
list, and `_execDropAll` drops them in a third pass after Postgres and UC sync
objects, through the existing `/settings/graph-engine/drop-uc-object` with
`is_sync: false`.

The Lakebase card base is `<Domain>_V<n>`; its match key is
`lower(domain)_<n>`, computed in JS to mirror `domain_match_key`.

The Orphan analytics card renders once, after the domain cards, identical to
the Lakehouse one.

## Error handling

The analytics scan is best-effort and must never blank the tab:

- A failed or unauthorised scan of the analytics schema leaves `analytics` and
  `orphans` empty and sets `analytics_message`; the front shows it as a small
  warning above the cards while still rendering triple-store and Postgres
  objects. This matches how `loadLakebaseSyncObjects` degrades to empty slots.
- Per-object drop failures keep today's behaviour: collected into the error
  list, reported together, the list reloaded afterwards.
- A domain with no analytics tables renders no sub-block at all — not an empty
  one.

## Testing

**Unit — `tests/units/settings/test_delta_objects.py`**

- `analytics_base` strips `_summary` / `_type_profiles` / `_type_predicates`
  and `_work_<stage>`, and leaves a bare `graph_metrics_x_1` alone
- `analytics_match_key` and `domain_match_key` agree for an ordinary domain and
  disagree for a punctuated one (the documented orphan case)
- `group_analytics_objects` groups the five table kinds under one key and
  sorts them `_work_*` → `_type_predicates` → `_type_profiles` → `_summary` →
  base
- non-`graph_metrics_` tables are ignored

**Service — same file**

- the result carries `analytics`, `orphans`, `analytics_location`, and `key` on
  each domain
- an analytics group whose key matches a `triplestore_` group is absent from
  `orphans`; one that matches nothing is present
- a scan failure yields empty `analytics` / `orphans` plus
  `analytics_message`, with `domains` still populated

**Front source contract** — the Lakebase slot is built by JS rather than by a
template, so there is no markup to assert. A source-contract test over
`settings.js`, following the existing front-test pattern, covers the mount
points, both Delete paths including their nested analytics, both orphan cards,
and the JS match key mirroring `domain_match_key`.

## Out of scope

- Changing `analytics_output_table` naming or reconciling `sanitize_domain_folder`
  with the view-name `safe` rule
- Automatic cleanup of `_work_*` outside this tab (the job already drops them on
  its normal path)
- The Runs page analytics table, the Health tab, and `graph_analytics_runs`
  registry rows — dropping a UC output table does not remove run history
- A Neo4j Objects tab. Analytics for a Neo4j-backed domain still lands in the
  same UC schema, so those tables surface on the Lakehouse tab — nested when a
  `triplestore_` group exists for that version, in the Orphan card otherwise
