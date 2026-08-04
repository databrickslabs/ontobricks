# Settings → Automation → Runs replaces Build Analytics

Date: 2026-08-04
Status: approved, ready for implementation planning

## Problem

Settings → Automation → **Build Analytics** is the older design of a run-history
view: one domain at a time, a version dropdown, four aggregate cards, and a
single build-runs table. Knowledge Graph → Management → **Runs** has since
settled on a better shape for the same question (see
`2026-08-02-runs-page-analytics-rows-design.md`): two tabs, build runs and
analytics runs, no version filter, a Details modal per row, each tab owning its
own loading / empty / error state.

What Settings actually needs that the Knowledge Graph page cannot give is the
*cross-domain* view: every build and every analysis in the registry, not just
those of the domain currently loaded in the session. Build Analytics half-solved
that with a domain picker, but it is stuck on the old layout, shows no analytics
runs at all, and its four cards duplicate numbers that the table already
carries.

So: delete the page, and put the Runs layout in its slot, widened to all
domains.

## Decisions

Taken with the user during brainstorming. Each closes an alternative an
implementer would otherwise reopen.

1. **Mirror Knowledge Graph → Runs, do not invent a new layout.** Two tabs, the
   same columns, the same two Details modals. The only added column is
   **Domain**.
2. **No aggregate cards.** The four Build Analytics cards (total builds, success
   rate, avg duration, active-build triples) are not carried over. The table
   answers the same questions with less indirection.
3. **A Domain dropdown whose first entry is "All domains"**, selected by
   default. This is the one thing the Knowledge Graph page does not have.
4. **No version filter.** Version stays a column, as on the Knowledge Graph
   page.
5. **Admin-only.** The menu item carries `admin_only: true`. Because
   `/settings` is already an admin-gated prefix in `PermissionMiddleware`, the
   new endpoints inherit server-side enforcement with no new guard. This also
   fixes an existing wart: today non-admins see the Build Analytics sidebar
   entry, then its fetches 403.
6. **Real server-side pagination**, not a flat row cap and not client-side
   slicing. Cross-domain history is unbounded in a way per-domain history is
   not.
7. **The two `/settings/build-*` endpoints stay.** `GET
   /settings/build-analytics/{domain}` and `GET /settings/build-runs/{domain}`
   lose their only UI caller but remain available for programmatic use, along
   with `SettingsService.get_build_analytics_result`,
   `get_build_runs_result`, `RegistryService.build_analytics` and the store
   `build_analytics` chain. Their existing tests stay as the thing that keeps
   them honest. Nothing in this work is allowed to change their behaviour.
8. **Triggering an analytics run over HTTP is a separate spec.** Analytics can
   be started today only from the internal, session-scoped `POST
   /dtwin/metrics/compute`; the external `/api` surface has `POST /build` but
   no analytics equivalent. Adding one is a real feature with its own auth and
   task-polling questions — out of scope here, recorded in §Follow-up.
9. **The shared rendering half of `domain-runs.js` is extracted**, rather than
   duplicated into the new page's script. See §5.

## Architecture

```
Settings → Automation → Runs   (admin only)
  ├── Domain dropdown        ← GET /settings/registry/domains      (existing, unchanged)
  ├── Build runs tab         ← GET /settings/runs/build      (new, paginated)
  └── Analytics runs tab     ← GET /settings/runs/analytics  (new, paginated)
                                       ↓
                     SettingsService.get_all_build_runs_result
                     SettingsService.get_all_analytics_runs_result
                                       ↓
                     RegistryService.load_all_build_runs
                     RegistryService.load_all_graph_analytics_runs
                                       ↓
                     RegistryStore.load_all_build_runs            (new)
                     RegistryStore.load_all_graph_analytics_runs   (new)
                                       ↓
                     build_runs / graph_analytics_runs  (Lakebase, unchanged DDL)
```

Two endpoints rather than one merged one, matching the two-tab layout: the row
kinds share no columns, each tab paginates independently, and an analytics store
error must not blank the build tab.

Paging happens in SQL. The alternative — fan out per domain through the existing
per-folder methods and paginate in Python — was rejected: it is N round trips
per page view, and because each per-domain read is itself capped, `total` would
be a lie and deep pages could silently drop rows.

Only one `RegistryStore` implementation exists (`lakebase`), plus the
`_InMemoryStore` fake in `tests/units/registry/test_registry_store.py`.

## Components

### 1. Store — two new cross-registry read methods

On `RegistryStore` (`src/back/objects/registry/store/base.py`), **non-abstract**
with a `([], 0)` default, following the precedent of
`load_graph_analytics_runs`, so a store without the table degrades to "no runs"
instead of failing to instantiate:

```python
def load_all_build_runs(
    self, *, folder: Optional[str] = None, limit: int = 25, offset: int = 0
) -> Tuple[List[BuildRunEntry], int]: ...

def load_all_graph_analytics_runs(
    self, *, folder: Optional[str] = None, limit: int = 25, offset: int = 0
) -> Tuple[List[GraphAnalyticsRun], int]: ...
```

Both return `(page_rows, total_matching_rows)`. `folder=None` spans every domain
in the registry; a folder scopes to one. Naming follows the existing
cross-registry methods `list_all_edit_locks` and `list_all_bridges`.

`BuildRunEntry` and `GraphAnalyticsRun` (both `TypedDict, total=False`) each gain
a `domain: str` key — the additive change that lets a row render its Domain cell.

**Lakebase implementation** (`store/lakebase/store.py`). Each method is two
queries against the same predicate, guarded by the existing
`_ensure_build_runs_table()` / `_ensure_graph_analytics_runs_table()` and
returning `([], 0)` when the guard or the query fails:

- the page query, which is the existing `load_build_runs` /
  `load_graph_analytics_runs` SELECT with `d.folder AS domain` added to the
  projection and `LIMIT %s OFFSET %s` at the end;
- `SELECT COUNT(*)` over the identical FROM/JOIN/WHERE.

The WHERE always carries `d.registry_id = %s` and adds `AND d.folder = %s` only
when `folder is not None`. Ordering is the existing
`ORDER BY b.started_at DESC, b.id DESC` for builds and
`ORDER BY r.computed_at DESC, r.id DESC` for analytics — both already tie-broken
by id, which is what makes `OFFSET` paging stable.

`total` counts rows in the table, so it is unaffected by the per-`(domain,
version)` write cap of 100. The cap bounds what gets *stored*, not what gets
counted, and neither cap changes here.

### 2. Service — two new result builders

`SettingsService` (`src/back/objects/domain/SettingsService.py`), directly
alongside the retained `get_build_runs_result`:

```python
get_all_build_runs_result(session_mgr, settings, *, folder=None, limit=25, offset=0)
get_all_analytics_runs_result(session_mgr, settings, *, folder=None, limit=25, offset=0)
```

Each follows the shape of `get_build_runs_result` exactly: resolve the session
domain, build `RegistryService.from_context`, raise
`ValidationError("Registry not configured")` when `svc.cfg.is_configured` is
false, re-raise `OntoBricksError`, wrap anything else in `InfrastructureError`.

`RegistryService` gains thin pass-throughs `load_all_build_runs` /
`load_all_graph_analytics_runs` that swallow store exceptions and return
`([], 0)`, matching how `load_graph_analytics_runs` already behaves.

### 3. Endpoints

In `src/api/routers/internal/settings.py`:

```
GET /settings/runs/build?domain=&limit=&offset=
GET /settings/runs/analytics?domain=&limit=&offset=
```

`domain` optional (empty or absent means all domains), `limit` default 25 with
`ge=1, le=200`, `offset` default 0 with `ge=0`. Response:

```json
{"success": true, "domain": null, "runs": [...], "total": 137, "limit": 25, "offset": 0}
```

Both are admin-only for free: `_PERM_ADMIN_ONLY_PREFIXES` in
`src/shared/fastapi/main.py` already covers `/settings`, and these paths must
**not** be added to `_PERM_ADMIN_ONLY_EXCEPTIONS`.

### 4. Template — `partials/settings/_settings_runs.html`

Replaces `_settings_build_analytics.html`. Header: `bi-list-check` icon, title
"Runs", subtitle "Build and analytics history across every domain (newest
first)", and a Refresh button. Below it the Domain `<select>`
(`settingsRunsDomain`), whose first option is "All domains" with an empty value,
populated from `GET /settings/registry/domains`.

Then two Bootstrap tabs, structurally the same as `_domain_runs.html`. Element
ids are prefixed `sr` (`srBuildTableBody`, `srAnalyticsRunsLoading`, …) so a
grep for a Runs id is never ambiguous between the two pages. Each tab owns its
own loading spinner, empty state, error alert, table wrapper and pagination
footer.

**Build tab columns:** ID · **Domain** · Date & Time · Version · Status ·
Triples · Details.

**Analytics tab columns:** Date & Time · **Domain** · Scope · Version · Status ·
Nodes · Edges · Components · Avg Degree · Density · Duration · Details.

Empty states name the filter rather than the workflow, since this page is not
scoped to a domain the admin is working in: "No build runs recorded" / "No
analytics runs recorded", followed by "for this domain" when a domain is
selected.

The Domain column is always present, including when a single domain is selected.
A column that appears and disappears would mean conditional header markup and
colspan bookkeeping in the empty state for no real gain.

Each pagination footer holds: a page-size `<select>` (25 / 50 / 100, default
25), a "Showing X–Y of Z" label, and Prev / Next buttons, visually following the
Bootstrap pagination nav that `query-reasoning.js` already uses for inferred
triples. Prev/Next disable at the ends. The footer hides when `total` fits on one
page.

Both Details modals are added at page level in `settings.html`
(`srRunDetailsModal` / `srAnalyticsRunDetailsModal`) — **not** inside the
`.sidebar-section`, for the same reason `dtwin.html` does it: a modal inside a
hidden section shows its backdrop but leaves the dialog invisible.

### 5. JS — extract the shared renderer

`domain-runs.js` is 423 lines, roughly half pure rendering and half fetch/latch
logic bound to hardcoded element ids. The rendering half moves to
**`static/global/js/runs-render.js`** — the existing home for shared frontend
code, next to `utils.js`.

It exposes a single namespaced object, `window.RunsRender`, rather than the bare
globals `domain-runs.js` uses today. That file's own comment on `_runLocalName`
documents the name-clash hazard of those globals; a namespace removes it.

Moving into `RunsRender`: `esc`, `fmtTs`, `fmtDuration`, `fmtMillis`,
`buildStatusBadge` (was `_statusBadge`), `analyticsStatusBadge`, `kindBadge`,
`localName`, `analyticsScope`, `kv`, `statsTable`, `phaseTable`, plus two
modal-body builders that **return HTML strings** instead of writing to the DOM:

```js
RunsRender.buildRunDetailsHtml(run)      // was the body of showRunDetailsObj
RunsRender.analyticsRunDetailsHtml(run)  // was the body of showAnalyticsRunDetails
```

Returning strings is what lets the two pages inject into differently-named modal
bodies. Both builders emit a **Domain** row at the top when `run.domain` is
present, so the same builder serves both pages: rows from
`GET /domain/build-runs` carry no `domain` key and render exactly as they do
today.

`domain-runs.js` keeps its element ids, its fetches, the `_runsLoaded` latch and
the `sidebarSectionChanged` hook, and delegates all formatting to `RunsRender`.
Its own copies of the extracted helpers are deleted. Two things must survive
verbatim: `window.showRunDetailsObj`, which `domain-audit.js` calls for build
entries in the audit timeline, and `window.showAnalyticsRunDetails`, referenced
from inline `onclick` attributes. A check across `src/front/static` confirms
these are the only cross-file consumers — every other `_esc` in the codebase is
a method or an IIFE-local of its own file.

**New `static/config/js/settings-runs.js`**, following `settings-locks.js`: an
IIFE that returns early unless its elements exist, loads on
`sidebarSectionChanged` for `section === 'runs'` and on `DOMContentLoaded` when
`runs-section` is already active.

State: `{ folder, limit, offset }` per tab, and a row cache per tab so the
Details buttons can look up by index. Behaviour:

- both tabs load on section entry, so switching tabs never waits on a fetch;
- each tab renders independently — one rejecting leaves the other's table up;
- changing the Domain dropdown or a page size resets that scope's `offset` to 0;
- Refresh re-fetches both tabs at the **current** domain and offset (it is a
  reload, not a reset);
- re-entering the section re-fetches, so the page is never a stale snapshot —
  matching Locks rather than the Knowledge Graph page's one-shot latch, because
  an admin watching builds land wants current data.

Script order matters: `runs-render.js` must load before `domain-runs.js` in
`dtwin.html` and `domain.html`, and before `settings-runs.js` in `settings.html`.
All three tags carry the `?v={{ asset_version }}` cache-buster.

### 6. Deletions

- `src/front/templates/partials/settings/_settings_build_analytics.html`
- `src/front/static/config/js/build-analytics.js`
- the `build-analytics-section` div and the `build-analytics.js` script tag in
  `settings.html`
- the `build-analytics` item in `menu_config.json`, replaced by
  `{"id": "runs", "label": "Runs", "icon": "bi-list-check", "default": false,
  "requires": null, "admin_only": true}` in the same `settings-automation`
  group, after Scheduler

No backend deletions (decision 7).

## Error handling

Per tab, never per page. A failed build fetch fills the build tab's error alert;
a failed analytics fetch fills the analytics tab's. Neither blanks the other and
neither blanks the page.

Below the HTTP layer, both new service methods swallow store exceptions and
return `([], 0)`, so an unconfigured or unreachable registry surfaces as an empty
tab rather than an error — the same degradation the Knowledge Graph page has
today.

A `domain` that does not exist is not an error: the predicate matches nothing,
so the response is a well-formed empty page with `total: 0`.

## Testing

**Store contract** — `tests/units/registry/test_registry_store.py`, extending
`_InMemoryStore` with the two new methods and testing against the existing
`build_runs` / `graph_analytics_runs` fixtures:

- `folder=None` spans domains, newest-first across all of them
- `folder="demo"` scopes to that domain
- `total` is the full match count, independent of `limit` and `offset`
- consecutive offsets return disjoint pages that reassemble into the full
  newest-first sequence
- `offset` beyond `total` returns `([], total)`, not an error
- every row carries `domain`
- an unknown folder returns `([], 0)`

**Endpoints** — `tests/units/api/`:

- `GET /settings/runs/build` and `/settings/runs/analytics` default to
  `limit=25, offset=0` and echo `limit` / `offset` / `total`
- `?domain=` filters; an unknown domain yields `total: 0` and `success: true`
- `limit` above 200 and `offset` below 0 are rejected by FastAPI validation
- registry not configured surfaces the `ValidationError` shape, not a 500

**Permissions** — `tests/units/auth/test_permission_middleware.py`: both new
paths are admin-only for a non-admin app user, i.e. they are *not* covered by
`_PERM_ADMIN_ONLY_EXCEPTIONS`.

**Regression on the retained endpoints** — the existing
`/settings/build-runs/{domain}` and `/settings/build-analytics/{domain}` tests
must still pass untouched. That is the guard on decision 7.

**Templates** — `tests/units/api/test_ui_rendering.py`: the assertion on
`id="build-analytics-section"` becomes `id="runs-section"`, plus checks that
`settings.html` contains both `sr*` modals and loads `runs-render.js` before
`settings-runs.js`.

**Frontend wiring** — extending `tests/units/front/test_runs_page.py`:

- `settings-runs.js` fetches `/settings/runs/build` and `/settings/runs/analytics`
- `domain-runs.js` still fetches `/domain/build-runs` and
  `/dtwin/metrics/history`, still assigns `window.showRunDetailsObj`, and no
  longer declares the extracted helpers
- `runs-render.js` defines `window.RunsRender` with both modal builders
- `dtwin.html` and `domain.html` load `runs-render.js` before `domain-runs.js`

**Full suite** — `uv run --frozen pytest -q -m "not scenario"`, 0 failures.

## Documentation

- `documentation/development.md:683` — the Settings row of the page table:
  "Automation (Scheduler, Build Analytics)" becomes "Automation (Scheduler,
  Runs)".
- `documentation/user-guide.md`, `### Runs (Sidebar)` (line ~500) — a short
  paragraph pointing admins at Settings → Automation → Runs for the same two
  tables across every domain, with the Domain column and pagination.
- `src/back/objects/registry/store/lakebase/schema.sql:151` — the comment
  "Powers the registry Build Analytics panel." now names Settings → Runs.
- Sphinx: no new module, so no new `.rst`. The new methods on existing classes
  are picked up by `autodoc`; they need Google-style docstrings. Re-run
  `ci/build_docs.sh` and check for new warnings.
- Changelog under `changelogs/v0.7.0/` (the version in `pyproject.toml`).

## Out of scope

- Merging the two tabs into one chronological timeline
- Sorting controls, or filtering by status / date range / build kind
- Reinstating aggregate cards in any form
- Changing the `build_runs` or `graph_analytics_runs` DDL, or the 100-row
  per-`(domain, version)` write cap
- Any behavioural change to `/settings/build-runs/{domain}` or
  `/settings/build-analytics/{domain}`
- Per-domain permission filtering of rows — the page is admin-only, so every
  domain in the registry is visible by construction

## Follow-up

A separate spec should cover triggering a graph-analytics run over the external
API: something like `POST /api/dtwin/analytics` alongside the existing
`POST /api/dtwin/build`, returning a task id that `GET /api/dtwin/...` can poll.
It needs its own answers on authentication, on whether it reuses
`DigitalTwin.run_metrics_task`, and on how it interacts with the
Databricks-job-only constraint that `POST /dtwin/metrics/compute` enforces
today.
