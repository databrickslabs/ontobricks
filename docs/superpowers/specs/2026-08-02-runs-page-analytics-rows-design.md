# Analytics runs move from the Analytics History tab to the Runs page

Date: 2026-08-02
Status: approved, ready for implementation planning

## Problem

Analytics run history lives in an eighth tab of the Knowledge Graph → Analytics
page, while build history lives in Knowledge Graph → Management → Runs. Two
run-history views in two places, one of them buried behind a tab on a page
about something else.

The Analytics page is about the *current* result: PageRank, betweenness, the
health tabs, AI insights. A ledger of past runs is a different question, and it
is the same question the Runs page already answers for builds.

## Decisions

Taken with the user during brainstorming, recorded here because each one closes
off alternatives an implementer would otherwise reopen.

1. **Two stacked tables, not one merged one.** Build rows and analytics rows
   keep their own columns rather than being interleaved chronologically under a
   shared schema. Their metrics have almost nothing in common — triples versus
   nodes/edges/components/density — so a merged table would either be wide and
   sparse or hide most numbers behind a click.
2. **No version filter.** The existing `runsVersionFilter` dropdown is removed.
   Both tables always show everything, newest first.
3. **Analytics rows get a Details modal**, matching the affordance build rows
   already have.
4. **No pointer from the Analytics page.** The tab is removed silently; no
   "history moved" link or toast.
5. **The dead Runs copy on the Domain page is removed** as part of this work
   (see §6).

## Architecture

Nothing moves between layers. This is a view change plus one widening of an
existing query.

```
Runs page (Knowledge Graph → Management → Runs)
  ├── Build runs table    ← GET /domain/build-runs        (unchanged)
  └── Analytics runs table ← GET /dtwin/metrics/history   (widened: all versions)
                                    ↓
                          RegistryService.load_graph_analytics_runs
                                    ↓
                          graph_analytics_runs  (Lakebase, unchanged DDL)
```

Two independent fetches rather than one merged endpoint. That matches the
stacked layout, isolates failure — an analytics store error must not blank the
build table — and avoids inventing a combined response shape for two row kinds
that share no columns.

## Components

### 1. Runs page — `partials/domain/_domain_runs.html`

Header subtitle becomes "Build and analytics history for this domain (newest
first)". The `runsVersionFilter` `<select>` is deleted. The single Refresh
button reloads both tables.

Two stacked cards, each with **its own** loading / empty / error state. Element
ids must stay distinct per card so one card's failure cannot blank the other.

**Card 1 — Build runs.** The existing table, unchanged:
ID · Date & Time · Version · Status · Triples · Details.

**Card 2 — Analytics runs.** The History tab's columns, plus two:

| Column | Source | Notes |
|---|---|---|
| When | `computed_at` | relative time, full timestamp as tooltip |
| Scope | `class_filter` | local name of first entry, or "All types" |
| **Version** | `version` | **new** — needed now that rows from several versions interleave with no filter |
| Status | `status` | green completed / red failed badge |
| Nodes | `node_count` | dash on failed rows |
| Edges | `edge_count` | dash on failed rows |
| Components | `connected_components` | dash on failed rows |
| Avg Degree | `avg_degree` | 2 dp, dash on failed rows |
| Density | `density` | 6 dp, dash on failed rows |
| Duration | `duration_ms` | |
| **Details** | — | **new** — eye button, opens the modal below |

### 2. Analytics run details modal

A second page-level modal alongside the existing `runDetailsModal`. It must be
rendered at page level in `dtwin.html`, not inside the `.sidebar-section` — the
existing modal carries a comment explaining that a modal inside a hidden
section shows its backdrop but not its dialog.

Built with the same `_kv` / `_statsTable` helpers as the build modal. Contents:
scope (the full `class_filter` list, not just the first entry), every metric,
duration, task ID, and the full `error` text for failed runs.

### 3. Widening the analytics query

`GET /dtwin/metrics/history` gains an optional `version` query parameter.
**Omitting it returns runs across all versions**, replacing today's hard-scoping
to `domain.current_version`.

That pushes one signature change down the stack — `version` becomes optional in
all three implementations:

- `RegistryStore.load_graph_analytics_runs` (base.py:495) — abstract default,
  returns `[]`
- `LakebaseStore.load_graph_analytics_runs` (store.py:2014) — drop
  `AND r.version = %s` from the WHERE clause when `version is None`; the
  `ORDER BY r.computed_at DESC, r.id DESC LIMIT %s` is unchanged and already
  correct for the multi-version case
- the `_InMemoryStore` fake in `tests/units/registry/test_registry_store.py`

`RegistryService.load_graph_analytics_runs` (RegistryService.py:855) passes it
through.

The endpoint's own guard must change with it. Today it reads
`version = domain.current_version` and short-circuits to
`{"success": true, "runs": []}` when **either** folder or version is missing.
Once version is optional that guard must require **folder only** — otherwise a
domain whose `current_version` happens to be empty would silently report no
history even though rows exist for earlier versions.

The write-time cap of `_GRAPH_ANALYTICS_RUNS_CAP = 100` per `(domain, version)`
is unchanged. Note the consequence, which is correct but worth stating: with N
versions the endpoint can now return up to `min(limit, 100 × N)` rows, so the
read `limit` becomes the real bound rather than a formality.

### 4. Removing the History tab — `partials/dtwin/_query_analytics.html`

Four deletions, nothing else references it:

- the tab trigger `<li>`, lines 173–178
- the `<div id="atab-history">` panel, lines 320–354
- `window.analyticsLoadHistory`, lines 755–807
- the auto-refresh call in `_renderAnalyticsData`, line 724

Everything behind the tab **stays**: the endpoint, the store methods, the
`graph_analytics_runs` table and the recording path in
`DigitalTwin.run_metrics_task`. Runs is now their consumer. This is a view
removal only.

### 5. JS — `static/domain/js/domain-runs.js`

Currently owns build rows only. It gains the analytics table and its modal.
Remove `_populateRunsVersions` and `_runsVersionSel` along with the dropdown.

`loadDomainRuns()` fires both fetches and renders each card independently, so
one rejecting still renders the other. Keep the `_esc` / `_fmtTs` /
`_fmtDuration` / `_statusBadge` helpers; the analytics status vocabulary is
`completed` / `failed`, distinct from the build vocabulary
`success` / `error` / `cancelled`, so it needs its own badge helper rather than
an overload of `_statusBadge`.

### 6. Removing the dead Runs copy on the Domain page

`domain.html:83-84` includes the same partial and `domain.js:62` hooks
`section === 'runs'`, but the `domain` menu in `menu_config.json` has no `runs`
item — only `digitaltwin` does. The section is unreachable. Left in place it
would render an analytics-history table on a page where that is out of context.

Delete the `runs-section` div from `domain.html` and the hook from
`domain.js:62`. **Keep** the `domain-runs.js` script tag in `domain.html`:
`domain-audit.js` reuses `showRunDetailsObj` for build entries in the audit
timeline, so the file is still needed there. Keep the page-level
`runDetailsModal` in `domain.html` for the same reason.

## Error handling

Each card is independent. A failed build fetch fills the build card's error
alert; a failed analytics fetch fills the analytics card's. Neither blanks the
other, and neither blanks the page.

`load_graph_analytics_runs` already swallows store errors and returns `[]`, so
an unconfigured or unreachable registry surfaces as the analytics card's empty
state rather than an error — matching how the build card behaves today.

## Testing

**Store contract** — `tests/units/registry/test_registry_store.py`, alongside
the four existing `graph_analytics_runs` cases:
- `version=None` returns runs across all versions, newest-first
- `version="2"` still filters to that version (no regression)
- `limit` is honoured when spanning versions

**Endpoint** — `/dtwin/metrics/history`:
- no `version` param spans versions
- explicit `version` filters
- unsaved domain (no folder) still returns `{"success": true, "runs": []}`
- a domain with a folder but an empty `current_version` still returns its
  history rather than short-circuiting to empty — the guard regression the
  widened signature invites

**Template** — the History tab markup is gone from `_query_analytics.html`
(no `atab-history`, no `analyticsLoadHistory`), and the Runs partial renders
both tables. Extends the existing `runs-section` render check.

**Regression** — `domain.html` no longer contains `runs-section`, but still
loads `domain-runs.js` and still contains `runDetailsModal`, so the audit
trail's reuse of `showRunDetailsObj` keeps working.

## Out of scope

- Merging the two tables into one chronological timeline
- Pagination or sorting controls on either table
- Changing the `graph_analytics_runs` DDL, the 100-row write cap, or the
  recording path
- The `audit_trail_result` timeline, which merges its own streams independently
