# Analytics Runs on the Runs Page — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the History tab from Knowledge Graph → Analytics and show analytics run history as a second, separate table on the Knowledge Graph → Management → Runs page, below the existing build-runs table.

**Architecture:** Two stacked tables on one page, each fed by its own endpoint and rendered independently so one failing never blanks the other. Build runs keep `GET /domain/build-runs` untouched. Analytics runs come from `GET /dtwin/metrics/history`, widened to span all versions instead of hard-scoping to the current one. No layer moves; this is a view change plus one widened query.

**Tech Stack:** FastAPI, Jinja2 templates, vanilla JS (no framework, no JS test runner), Bootstrap 5, Lakebase Postgres via psycopg, pytest.

**Spec:** `docs/superpowers/specs/2026-08-02-runs-page-analytics-rows-design.md`

## Global Constraints

- Run tests with `env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u DATABRICKS_CONFIG_PROFILE -u DATABRICKS_CLIENT_ID -u DATABRICKS_CLIENT_SECRET -u LAKEBASE_PROJECT -u LAKEBASE_BRANCH -u LAKEBASE_DATABASE -u LAKEBASE_DATABASE_RESOURCE_SEGMENT -u LAKEBASE_SCHEMA .venv/bin/pytest -q -m "not scenario"`. Those variables leak from a sourced `scripts/deploy.config.sh` and make unit tests hit real Databricks endpoints.
- Never commit `uv.lock`. If a command modifies it, `git checkout -- uv.lock` before committing.
- Do not run `uv run pytest` — it rewrites `uv.lock`. Use `.venv/bin/pytest` directly.
- **There is no JavaScript test runner in this repo.** JS is verified by Python tests that assert on rendered HTML (`tests/units/api/test_ui_rendering.py`) and, where behaviour lives only in a `.js` file, by reading the file and asserting on its source text. This is an established pattern here — see `tests/units/settings/test_analytics_job_toggle.py`.
- Analytics status vocabulary is `completed` / `failed`. Build status vocabulary is `success` / `error` / `cancelled`. They are different; do not overload one badge helper for both.
- The `graph_analytics_runs` DDL, its 100-row-per-`(domain, version)` write cap, and the recording path in `DigitalTwin.run_metrics_task` are all **out of scope and must not change**.

---

## File Structure

| File | Change | Responsibility after |
|---|---|---|
| `src/back/objects/registry/store/base.py` | Modify ~495 | Abstract contract: `version` optional |
| `src/back/objects/registry/store/lakebase/store.py` | Modify ~2014 | Drop the version predicate when `version is None` |
| `src/back/objects/registry/RegistryService.py` | Modify ~855 | Pass optional `version` through |
| `src/api/routers/internal/dtwin.py` | Modify 699-726 | Accept `?version=`; require folder only |
| `src/front/templates/partials/domain/_domain_runs.html` | Modify | Two cards; no version filter |
| `src/front/templates/dtwin.html` | Modify | Add page-level analytics-run modal |
| `src/front/static/domain/js/domain-runs.js` | Modify | Fetch + render both tables, both modals |
| `src/front/templates/partials/dtwin/_query_analytics.html` | Modify | History tab deleted |
| `src/front/templates/domain.html` | Modify 83-84 | Dead `runs-section` removed |
| `src/front/static/domain/js/domain.js` | Modify 62-64 | Dead `runs` hook removed |
| `tests/units/registry/test_registry_store.py` | Modify | Fake + cross-version contract tests |
| `tests/units/api/test_ui_rendering.py` | Modify 453-461 | `runs-section` off `/domain`, onto `/dtwin/` |
| `tests/units/dtwin/test_metrics_history.py` | **Create** | Endpoint behaviour |
| `tests/units/front/test_runs_page.py` | **Create** | Runs markup + JS source assertions |

---

### Task 1: Store — `version` becomes optional

**Files:**
- Modify: `src/back/objects/registry/store/base.py:495-502`
- Modify: `src/back/objects/registry/store/lakebase/store.py:2014-2041`
- Modify: `src/back/objects/registry/RegistryService.py:855`
- Test: `tests/units/registry/test_registry_store.py`

**Interfaces:**
- Produces: `load_graph_analytics_runs(self, folder: str, version: Optional[str] = None, *, limit: int = 100) -> List[GraphAnalyticsRun]`. When `version is None`, returns rows for **all** versions of *folder*, newest-first. Task 2 consumes this.

- [ ] **Step 1: Give the in-memory fake a sequence number**

The fake stores runs in a `Dict[(folder, version), List[dict]]` with no timestamps, so a cross-version read has nothing to order by. Add a monotonic counter standing in for `computed_at`.

In `tests/units/registry/test_registry_store.py`, add to `_InMemoryStore.__init__` (after line 56):

```python
        self._graph_analytics_runs: Dict[Tuple[str, str], List[Any]] = {}
        # Stands in for ``computed_at`` so a cross-version read can order
        # newest-first the way the real store's ORDER BY does.
        self._graph_analytics_seq = 0
```

Replace the two fake methods at lines 235-247 with:

```python
    def record_graph_analytics_run(
        self, folder: str, version: str, entry: Dict[str, Any]
    ) -> None:
        # Append-only run history, tagged with a sequence number so reads
        # can order newest-first across versions.
        self._graph_analytics_seq += 1
        row = dict(entry)
        row.setdefault("version", version)
        self._graph_analytics_runs.setdefault((folder, version), []).append(
            (self._graph_analytics_seq, row)
        )

    def load_graph_analytics_runs(
        self, folder: str, version: Optional[str] = None, *, limit: int = 100
    ):
        pairs = [
            (seq, row)
            for (f, v), entries in self._graph_analytics_runs.items()
            if f == folder and (version is None or v == version)
            for seq, row in entries
        ]
        pairs.sort(key=lambda p: p[0], reverse=True)
        return [dict(row) for _, row in pairs[:limit]]
```

Ensure `Optional` is imported at the top of the test file (it imports from `typing` already; add `Optional` if absent).

- [ ] **Step 2: Write the failing tests**

Add to `tests/units/registry/test_registry_store.py` immediately after `test_graph_analytics_runs_scoped_by_version` (line 721):

```python
    def test_graph_analytics_runs_span_versions_when_no_version_given(self, store):
        """The Runs page shows every version at once, so omitting the
        version must return the whole folder's history, newest-first."""
        store.record_graph_analytics_run("demo", "1", {"node_count": 1})
        store.record_graph_analytics_run("demo", "2", {"node_count": 2})
        store.record_graph_analytics_run("demo", "1", {"node_count": 3})

        runs = store.load_graph_analytics_runs("demo")

        assert [r["node_count"] for r in runs] == [3, 2, 1]

    def test_graph_analytics_runs_carry_their_version(self, store):
        """With no filter the table interleaves versions, so each row has
        to say which version it belongs to."""
        store.record_graph_analytics_run("demo", "7", {"node_count": 1})
        assert store.load_graph_analytics_runs("demo")[0]["version"] == "7"

    def test_graph_analytics_runs_limit_applies_across_versions(self, store):
        store.record_graph_analytics_run("demo", "1", {"node_count": 1})
        store.record_graph_analytics_run("demo", "2", {"node_count": 2})
        store.record_graph_analytics_run("demo", "3", {"node_count": 3})

        runs = store.load_graph_analytics_runs("demo", limit=2)

        assert [r["node_count"] for r in runs] == [3, 2]

    def test_graph_analytics_runs_ignores_other_folders(self, store):
        store.record_graph_analytics_run("demo", "1", {"node_count": 1})
        store.record_graph_analytics_run("other", "1", {"node_count": 99})
        assert [r["node_count"] for r in store.load_graph_analytics_runs("demo")] == [1]
```

- [ ] **Step 3: Run the tests to verify they fail**

```bash
env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u DATABRICKS_CONFIG_PROFILE .venv/bin/pytest tests/units/registry/test_registry_store.py -k graph_analytics_runs -q
```

Expected: the four new tests FAIL (`TypeError: load_graph_analytics_runs() missing 1 required positional argument: 'version'`), the four pre-existing ones PASS.

- [ ] **Step 4: Widen the abstract contract**

In `src/back/objects/registry/store/base.py`, replace lines 495-502:

```python
    def load_graph_analytics_runs(
        self, folder: str, version: Optional[str] = None, *, limit: int = 100
    ) -> List[GraphAnalyticsRun]:
        """Newest-first analytics run history for *folder*, capped at *limit*.

        ``version=None`` spans every version of the folder, which is what
        the Runs page asks for; pass a version to scope to one. Empty list
        on any error. Default is an empty list for stores without a
        run-history table.
        """
        return []
```

Confirm `Optional` is already imported in `base.py` (it is, via `from typing import ...`). If not, add it.

- [ ] **Step 5: Widen the Lakebase implementation**

In `src/back/objects/registry/store/lakebase/store.py`, replace the body at lines 2014-2041:

```python
    def load_graph_analytics_runs(
        self, folder: str, version: Optional[str] = None, *, limit: int = 100
    ) -> List[GraphAnalyticsRun]:
        if not self._ensure_graph_analytics_runs_table():
            return []
        try:
            _psycopg, dict_row = _require_psycopg()
            sch = self._q(self._schema)
            where = "WHERE d.registry_id = %s AND d.folder = %s"
            params: List[Any] = [self._registry(), folder]
            if version is not None:
                where += " AND r.version = %s"
                params.append(version)
            params.append(int(limit))
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT r.id, r.version, r.status, r.class_filter, r.node_count,
                           r.edge_count, r.connected_components, r.avg_degree,
                           r.density, r.duration_ms, r.task_id, r.error,
                           r.computed_at
                    FROM {sch}.graph_analytics_runs r
                    JOIN {sch}.domains d ON d.id = r.domain_id
                    {where}
                    ORDER BY r.computed_at DESC, r.id DESC
                    LIMIT %s
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
            return [self._graph_analytics_run_row_to_entry(r) for r in rows]
        except Exception as exc:  # noqa: BLE001
            logger.debug("load_graph_analytics_runs(%s) failed: %s", folder, exc)
            return []
```

The `ORDER BY r.computed_at DESC, r.id DESC` is unchanged and is already correct for the multi-version case. Verify `Any` and `Optional` are imported in this file.

- [ ] **Step 6: Widen the service wrapper**

In `src/back/objects/registry/RegistryService.py` around line 855, change the signature so `version` defaults to `None` and is passed straight through:

```python
    def load_graph_analytics_runs(
        self, folder: str, version: Optional[str] = None, *, limit: int = 100
    ) -> list:
        return self.store.load_graph_analytics_runs(folder, version, limit=limit)
```

Keep whatever docstring and error handling the existing method has; change only the signature and the pass-through. Confirm `Optional` is imported.

- [ ] **Step 7: Run the tests to verify they pass**

```bash
env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u DATABRICKS_CONFIG_PROFILE .venv/bin/pytest tests/units/registry/ -q
```

Expected: PASS, including the four pre-existing `graph_analytics_runs` tests (scoping by explicit version must still work).

- [ ] **Step 8: Commit**

```bash
git add src/back/objects/registry/ tests/units/registry/test_registry_store.py
git commit -m "feat(registry): let analytics run history span versions

The Runs page shows every version at once, so version becomes optional on
load_graph_analytics_runs and the SQL predicate is dropped when it is None."
```

---

### Task 2: Endpoint — `/metrics/history` spans versions

**Files:**
- Modify: `src/api/routers/internal/dtwin.py:699-726`
- Test: `tests/units/dtwin/test_metrics_history.py` (create)

**Interfaces:**
- Consumes: `RegistryService.load_graph_analytics_runs(folder, version=None, *, limit=100)` from Task 1.
- Produces: `GET /dtwin/metrics/history?version=<optional>&limit=<optional>` → `{"success": True, "runs": [...]}`. Task 4's JS calls it with no params.

**A known limit of these tests.** They assert on the endpoint's *source text*
rather than calling it, because exercising it needs a session with a loaded
domain plus a configured registry. That is the same pattern
`tests/units/dtwin/test_analytics_job_status.py` already uses on this router.
The behaviour underneath is covered for real by Task 1's store tests, and the
end-to-end path by manual check 2 at the bottom of this plan. If you find you
can drive the endpoint through the existing `client` fixture without heavy
mocking, prefer a behavioural test and delete the source-text ones.

- [ ] **Step 1: Write the failing tests**

Create `tests/units/dtwin/test_metrics_history.py`:

```python
"""The analytics run-history endpoint behind Knowledge Graph → Runs.

The endpoint used to hard-scope to the domain's current version because it
backed a tab that only ever showed the current one. The Runs page has no
version filter, so it now spans versions unless asked otherwise.
"""

from typing import Any, Dict, List, Optional

import pytest

pytestmark = pytest.mark.unit


class _FakeService:
    """Records how the router called the registry."""

    def __init__(self, runs: Optional[List[Dict[str, Any]]] = None):
        self.runs = runs if runs is not None else []
        self.calls: List[Dict[str, Any]] = []

    def load_graph_analytics_runs(self, folder, version=None, *, limit=100):
        self.calls.append({"folder": folder, "version": version, "limit": limit})
        return self.runs


def _read_router_source() -> str:
    from pathlib import Path

    return Path("src/api/routers/internal/dtwin.py").read_text(encoding="utf-8")


def _history_source() -> str:
    """Just the body of the history endpoint."""
    src = _read_router_source()
    start = src.index('@router.get("/metrics/history")')
    end = src.index("@router.", start + 10)
    return src[start:end]


class TestHistoryEndpointContract:
    def test_accepts_an_optional_version_query_param(self):
        body = _history_source()
        assert "version:" in body and "Query(" in body, (
            "the endpoint must expose ?version= so a caller can still scope "
            "to one version"
        )

    def test_does_not_read_the_current_version_from_the_domain(self):
        """The old implementation defaulted the scope to
        ``domain.current_version``. That default is exactly what made the
        endpoint unable to show older runs."""
        body = _history_source()
        assert "current_version" not in body

    def test_guard_requires_folder_only(self):
        """Guarding on version too would report an empty history for a
        domain whose current_version happens to be blank, even though rows
        exist for earlier versions."""
        body = _history_source()
        assert "if not folder:" in body
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u DATABRICKS_CONFIG_PROFILE .venv/bin/pytest tests/units/dtwin/test_metrics_history.py -q
```

Expected: all three FAIL — the current body reads `current_version` and guards `if not folder or not version:`.

- [ ] **Step 3: Rewrite the endpoint**

In `src/api/routers/internal/dtwin.py`, replace lines 699-726 with:

```python
@router.get("/metrics/history")
async def get_graph_metrics_history(
    version: Optional[str] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return the analytics run history (newest-first) for this domain.

    Spans every version unless ``version`` scopes it. Backs the analytics
    table on Knowledge Graph → Management → Runs, which has no version
    filter. Guarding on a version here would report an empty history for a
    domain whose current version is blank, even with rows on file for
    earlier ones.
    """
    from back.objects.registry.RegistryService import RegistryService

    try:
        domain = get_domain(session_mgr)
        folder = getattr(domain, "uc_domain_folder", "") or ""
        if not folder:
            return {"success": True, "runs": []}

        svc = RegistryService.from_context(domain, settings)
        runs = svc.load_graph_analytics_runs(folder, version, limit=limit)
        return {"success": True, "runs": runs}

    except (ValidationError, InfrastructureError, NotFoundError):
        raise
    except Exception as e:
        logger.exception("Loading graph metrics history failed: %s", e)
        raise InfrastructureError("Loading graph metrics history failed", detail=str(e))
```

Confirm `Query` and `Optional` are imported in `dtwin.py`. If `Query` is not, add it to the existing `from fastapi import ...` line.

The read `limit` default rises from 100 to 200 to match `/domain/build-runs`. This matters now: the write cap is 100 per `(domain, version)`, so spanning N versions can yield up to `min(limit, 100 × N)` rows and `limit` becomes the real bound.

- [ ] **Step 4: Run the tests to verify they pass**

```bash
env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u DATABRICKS_CONFIG_PROFILE .venv/bin/pytest tests/units/dtwin/test_metrics_history.py -q
```

Expected: PASS.

- [ ] **Step 5: Run the surrounding suites for regressions**

```bash
env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u DATABRICKS_CONFIG_PROFILE .venv/bin/pytest tests/units/dtwin/ tests/units/api/ -q
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/api/routers/internal/dtwin.py tests/units/dtwin/test_metrics_history.py
git commit -m "feat(api): /metrics/history spans versions and takes an optional filter

The Runs page has no version filter, so the endpoint no longer defaults its
scope to the domain's current version. The guard now requires a folder only:
guarding on version reported an empty history whenever current_version was
blank, despite rows on file for earlier versions."
```

---

### Task 3: Runs page markup — second card, no version filter

**Files:**
- Modify: `src/front/templates/partials/domain/_domain_runs.html`
- Modify: `src/front/templates/dtwin.html` (add the analytics modal at page level)
- Test: `tests/units/front/test_runs_page.py` (create)

**Interfaces:**
- Produces, for Task 4's JS: element ids `analyticsRunsLoading`, `analyticsRunsEmpty`, `analyticsRunsError`, `analyticsRunsErrorMessage`, `analyticsRunsTableWrapper`, `analyticsRunsTableBody`, and modal ids `analyticsRunDetailsModal` / `analyticsRunDetailsBody`. The build card keeps its existing ids: `runsLoading`, `runsEmpty`, `runsError`, `runsErrorMessage`, `runsTableWrapper`, `runsTableBody`.

- [ ] **Step 1: Write the failing tests**

Create `tests/units/front/test_runs_page.py`:

```python
"""Knowledge Graph → Management → Runs renders two independent tables.

Build runs and analytics runs share no columns, so they are stacked rather
than merged. Each card owns its loading / empty / error elements: a failure
fetching one must not blank the other.
"""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_PARTIAL = Path("src/front/templates/partials/domain/_domain_runs.html")
_DTWIN = Path("src/front/templates/dtwin.html")


def _partial() -> str:
    return _PARTIAL.read_text(encoding="utf-8")


class TestRunsPartial:
    @pytest.mark.parametrize(
        "element_id",
        [
            "runsTableBody",
            "analyticsRunsTableBody",
            "analyticsRunsLoading",
            "analyticsRunsEmpty",
            "analyticsRunsError",
            "analyticsRunsErrorMessage",
            "analyticsRunsTableWrapper",
        ],
    )
    def test_both_cards_have_their_own_elements(self, element_id):
        assert f'id="{element_id}"' in _partial()

    def test_the_version_filter_is_gone(self):
        """Both tables always show every version, so the dropdown that used
        to scope only the build table would now be a half-working control."""
        assert "runsVersionFilter" not in _partial()

    def test_the_analytics_table_names_its_version_column(self):
        """With no filter, rows from several versions interleave, so each
        row has to say which version it came from."""
        html = _partial()
        analytics = html[html.index("analyticsRunsTableWrapper"):]
        for header in ("Scope", "Version", "Nodes", "Edges", "Components", "Density"):
            assert f">{header}<" in analytics


class TestAnalyticsModal:
    def test_modal_is_page_level_not_inside_the_section(self):
        """A modal inside a hidden .sidebar-section shows its backdrop but
        never its dialog — the existing runDetailsModal carries a comment
        saying exactly this."""
        assert 'id="analyticsRunDetailsModal"' in _DTWIN.read_text(encoding="utf-8")
        assert "analyticsRunDetailsModal" not in _partial()

    def test_modal_has_a_body_for_the_script_to_fill(self):
        assert 'id="analyticsRunDetailsBody"' in _DTWIN.read_text(encoding="utf-8")
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u DATABRICKS_CONFIG_PROFILE .venv/bin/pytest tests/units/front/test_runs_page.py -q
```

Expected: FAIL (only `runsTableBody` and `test_the_analytics_table_names_its_version_column`'s absence are satisfied today; `runsVersionFilter` still exists).

Create `tests/units/front/__init__.py` only if the directory needs it to be collected — check whether sibling test dirs have one and match.

- [ ] **Step 3: Rewrite the partial**

Replace the entire contents of `src/front/templates/partials/domain/_domain_runs.html` with:

```html
<!-- JS: /static/domain/js/domain-runs.js -->
<!-- Domain Runs Section: build runs and analytics runs, stacked.
     The two share no columns, so they are separate tables rather than one
     merged timeline, and each owns its loading/empty/error elements so a
     failure fetching one never blanks the other. -->
<div class="content-section">
    <div class="section-header d-flex justify-content-between align-items-center mb-4">
        <div>
            <h4 class="mb-1"><i class="bi bi-list-check me-2"></i>Runs</h4>
            <p class="text-muted mb-0 small">Build and analytics history for this domain (newest first)</p>
        </div>
        <div class="d-flex align-items-center gap-2">
            <button type="button" class="btn btn-sm btn-outline-secondary" id="btnReloadRuns"
                    onclick="loadDomainRuns()" title="Reload runs from the registry">
                <i class="bi bi-arrow-counterclockwise me-1"></i> Refresh
            </button>
        </div>
    </div>

    <!-- ── Build runs ── -->
    <h6 class="text-uppercase text-muted small fw-semibold mb-2">
        <i class="bi bi-hammer me-1"></i>Build runs
    </h6>

    <div id="runsLoading" class="text-center py-4" style="display:none;">
        <div class="spinner-border text-primary" role="status">
            <span class="visually-hidden">Loading...</span>
        </div>
        <p class="text-muted mt-2 small">Loading build runs from registry&hellip;</p>
    </div>

    <div id="runsEmpty" class="text-center py-4" style="display:none;">
        <i class="bi bi-list-check text-muted fs-1"></i>
        <p class="text-muted mt-2 mb-1">No build runs found</p>
        <p class="text-muted small">Build the domain (Management &rarr; Build) to start recording runs.</p>
    </div>

    <div id="runsError" class="alert alert-warning" role="alert" style="display:none;">
        <i class="bi bi-exclamation-triangle me-1"></i>
        <span id="runsErrorMessage"></span>
    </div>

    <div id="runsTableWrapper" style="display:none;">
        <div class="table-responsive">
            <table class="table table-hover align-middle mb-0">
                <thead class="table-light">
                    <tr>
                        <th class="text-end" style="width:8%;">ID</th>
                        <th style="width:24%;">Date &amp; Time</th>
                        <th class="text-center" style="width:16%;">Version</th>
                        <th class="text-center" style="width:18%;">Status</th>
                        <th class="text-end" style="width:18%;">Triples</th>
                        <th class="text-center" style="width:16%;">Details</th>
                    </tr>
                </thead>
                <tbody id="runsTableBody">
                    <!-- Populated by JS -->
                </tbody>
            </table>
        </div>
    </div>

    <!-- ── Analytics runs ── -->
    <h6 class="text-uppercase text-muted small fw-semibold mb-2 mt-5">
        <i class="bi bi-graph-up-arrow me-1"></i>Analytics runs
    </h6>

    <div id="analyticsRunsLoading" class="text-center py-4" style="display:none;">
        <div class="spinner-border text-primary" role="status">
            <span class="visually-hidden">Loading...</span>
        </div>
        <p class="text-muted mt-2 small">Loading analytics runs from registry&hellip;</p>
    </div>

    <div id="analyticsRunsEmpty" class="text-center py-4" style="display:none;">
        <i class="bi bi-graph-up-arrow text-muted fs-1"></i>
        <p class="text-muted mt-2 mb-1">No analytics runs found</p>
        <p class="text-muted small">Run an analysis (Insight &rarr; Analytics) to start recording runs.</p>
    </div>

    <div id="analyticsRunsError" class="alert alert-warning" role="alert" style="display:none;">
        <i class="bi bi-exclamation-triangle me-1"></i>
        <span id="analyticsRunsErrorMessage"></span>
    </div>

    <div id="analyticsRunsTableWrapper" style="display:none;">
        <div class="table-responsive">
            <table class="table table-hover align-middle mb-0" style="font-size:0.85rem">
                <thead class="table-light">
                    <tr>
                        <th>Date &amp; Time</th>
                        <th>Scope</th>
                        <th class="text-center">Version</th>
                        <th class="text-center">Status</th>
                        <th class="text-end">Nodes</th>
                        <th class="text-end">Edges</th>
                        <th class="text-end">Components</th>
                        <th class="text-end">Avg Degree</th>
                        <th class="text-end">Density</th>
                        <th class="text-end">Duration</th>
                        <th class="text-center">Details</th>
                    </tr>
                </thead>
                <tbody id="analyticsRunsTableBody">
                    <!-- Populated by JS -->
                </tbody>
            </table>
        </div>
    </div>
</div>
<!-- NOTE: both run-details modals are rendered at page level in dtwin.html so
     they work from any section (a modal inside a hidden .sidebar-section shows
     its backdrop but leaves the dialog invisible). -->
```

- [ ] **Step 4: Add the analytics modal to `dtwin.html`**

The existing `runDetailsModal` occupies lines 107-125 of `src/front/templates/dtwin.html`, at page level just before the `<script defer ...>` block. Insert this immediately after line 125, at the same nesting level:

```html
<!-- Analytics run details (page level: see the note in _domain_runs.html) -->
<div class="modal fade" id="analyticsRunDetailsModal" tabindex="-1" aria-hidden="true">
    <div class="modal-dialog modal-lg modal-dialog-scrollable">
        <div class="modal-content">
            <div class="modal-header">
                <h5 class="modal-title">
                    <i class="bi bi-graph-up-arrow me-2"></i>Analytics run details
                </h5>
                <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
            </div>
            <div class="modal-body" id="analyticsRunDetailsBody"></div>
            <div class="modal-footer">
                <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
            </div>
        </div>
    </div>
</div>
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u DATABRICKS_CONFIG_PROFILE .venv/bin/pytest tests/units/front/test_runs_page.py -q
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/front/templates/partials/domain/_domain_runs.html src/front/templates/dtwin.html tests/units/front/test_runs_page.py
git commit -m "feat(ui): add an analytics-runs table to the Runs page

Build and analytics runs share no columns, so they are stacked as two tables
with independent loading/empty/error state rather than merged. The version
filter goes: it only ever scoped the build table."
```

---

### Task 4: Runs page JS — fetch and render both tables

**Files:**
- Modify: `src/front/static/domain/js/domain-runs.js`
- Test: `tests/units/front/test_runs_page.py` (extend)

**Interfaces:**
- Consumes: the element ids from Task 3; `GET /dtwin/metrics/history` from Task 2; `GET /domain/build-runs` (unchanged).
- Produces: `window.loadDomainRuns()` reloads both tables; `window.showAnalyticsRunDetails(idx)` opens the analytics modal.

- [ ] **Step 1: Write the failing tests**

Append to `tests/units/front/test_runs_page.py`:

```python
_JS = Path("src/front/static/domain/js/domain-runs.js")


def _js() -> str:
    return _JS.read_text(encoding="utf-8")


class TestRunsScript:
    def test_it_fetches_both_sources(self):
        src = _js()
        assert "/domain/build-runs" in src
        assert "/dtwin/metrics/history" in src

    def test_the_two_fetches_are_independent(self):
        """One endpoint failing must not blank the other table, so the two
        loads cannot share a try block or a Promise.all that rejects."""
        src = _js()
        assert "Promise.all" not in src
        assert src.count("async function _loadBuildRuns") == 1
        assert src.count("async function _loadAnalyticsRuns") == 1

    def test_analytics_status_has_its_own_badge_helper(self):
        """Analytics reports completed/failed; builds report
        success/error/cancelled. Overloading one helper would render every
        analytics row as an unknown-status grey badge."""
        src = _js()
        assert "_analyticsStatusBadge" in src
        assert "'completed'" in src or '"completed"' in src

    def test_the_version_dropdown_wiring_is_gone(self):
        src = _js()
        assert "runsVersionFilter" not in src
        assert "_populateRunsVersions" not in src
        assert "_runsVersionSel" not in src

    def test_failed_analytics_rows_do_not_show_zeroed_metrics(self):
        """A failed run records zeros, and printing them as real values
        would read as a graph with no nodes rather than a run that died."""
        assert "_analyticsRunRow" in _js()
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u DATABRICKS_CONFIG_PROFILE .venv/bin/pytest tests/units/front/test_runs_page.py -q
```

Expected: the five new tests FAIL.

- [ ] **Step 3: Rewrite the script**

In `src/front/static/domain/js/domain-runs.js`:

**(a)** Replace the header comment and module state (lines 1-12) with:

```javascript
/**
 * OntoBricks - domain-runs.js
 * Run history for Knowledge Graph > Management > Runs.
 *
 * Two independent tables: build runs from the registry's build_runs trace
 * (GET /domain/build-runs) and analytics runs from graph_analytics_runs
 * (GET /dtwin/metrics/history). They share no columns, so they are rendered
 * separately, and each load is isolated so one endpoint failing still leaves
 * the other table on screen.
 */

let _runsLoaded = false;
let _runsCache = [];
let _analyticsRunsCache = [];
```

**(b)** Delete `_populateRunsVersions` entirely (old lines 14-31).

**(c)** Replace `_renderRunsTable` (old lines 33-65) with a version that reads the cache directly, with no filter:

```javascript
function _renderRunsTable() {
    const empty = document.getElementById('runsEmpty');
    const wrapper = document.getElementById('runsTableWrapper');
    const tbody = document.getElementById('runsTableBody');
    if (!tbody) return;

    if (_runsCache.length === 0) {
        wrapper.style.display = 'none';
        empty.style.display = '';
        return;
    }
    empty.style.display = 'none';
    tbody.innerHTML = '';
    _runsCache.forEach(function (run, idx) {
        const row = document.createElement('tr');
        row.innerHTML =
            '<td class="text-end text-muted small">' + _esc(run.id || (idx + 1)) + '</td>'
            + '<td class="small">' + _fmtTs(run.started_at) + '</td>'
            + '<td class="text-center"><span class="badge bg-secondary">v' + _esc(run.version || '?') + '</span></td>'
            + '<td class="text-center">' + _statusBadge(run.status) + '</td>'
            + '<td class="text-end">' + _esc((Number(run.triple_count) || 0).toLocaleString()) + '</td>'
            + '<td class="text-center">'
            + '<button class="btn btn-sm btn-outline-primary" onclick="showRunDetails(' + idx + ')" title="View run details">'
            + '<i class="bi bi-eye"></i></button></td>';
        tbody.appendChild(row);
    });
    wrapper.style.display = '';
}
```

**(d)** Add these helpers next to `_statusBadge`:

```javascript
function _analyticsStatusBadge(status) {
    const st = (status || '').toLowerCase();
    if (st === 'completed') return '<span class="badge bg-success"><i class="bi bi-check-circle me-1"></i>Completed</span>';
    if (st === 'failed') return '<span class="badge bg-danger"><i class="bi bi-x-circle me-1"></i>Failed</span>';
    return '<span class="badge bg-secondary">' + _esc(status || 'unknown') + '</span>';
}

// Named _runLocalName, not _localName: this file declares its helpers at
// global scope and query-chat.js — loaded on the same page — has a
// _localName of its own. That one is currently inside an IIFE, so there is
// no clash today, but the prefix means there never can be.
function _runLocalName(uri) {
    const s = String(uri == null ? '' : uri);
    const cut = Math.max(s.lastIndexOf('#'), s.lastIndexOf('/'));
    return cut >= 0 ? s.slice(cut + 1) : s;
}

function _analyticsScope(classFilter) {
    const list = classFilter || [];
    if (!list.length) return '<span class="text-muted">All types</span>';
    const first = _esc(_runLocalName(list[0]));
    if (list.length === 1) return first;
    return first + ' <span class="text-muted">+' + (list.length - 1) + '</span>';
}

function _fmtMillis(ms) {
    const n = Number(ms) || 0;
    if (n < 1000) return n + ' ms';
    return _fmtDuration(n / 1000);
}
```

**(e)** Add the analytics row renderer and table renderer:

```javascript
// A failed run stores zeros for every metric. Printing them would read as a
// graph with no nodes rather than a run that never produced numbers, so
// failed rows dash their metric cells out.
function _analyticsRunRow(run, idx) {
    const failed = (run.status || '').toLowerCase() === 'failed';
    const dash = '<span class="text-muted">&mdash;</span>';
    const num = function (v) { return _esc((Number(v) || 0).toLocaleString()); };

    return '<td class="small">' + _fmtTs(run.computed_at) + '</td>'
        + '<td class="small">' + _analyticsScope(run.class_filter) + '</td>'
        + '<td class="text-center"><span class="badge bg-secondary">v' + _esc(run.version || '?') + '</span></td>'
        + '<td class="text-center">' + _analyticsStatusBadge(run.status) + '</td>'
        + '<td class="text-end">' + (failed ? dash : num(run.node_count)) + '</td>'
        + '<td class="text-end">' + (failed ? dash : num(run.edge_count)) + '</td>'
        + '<td class="text-end">' + (failed ? dash : num(run.connected_components)) + '</td>'
        + '<td class="text-end">' + (failed ? dash : _esc((Number(run.avg_degree) || 0).toFixed(2))) + '</td>'
        + '<td class="text-end font-monospace">' + (failed ? dash : _esc((Number(run.density) || 0).toFixed(6))) + '</td>'
        + '<td class="text-end">' + _esc(_fmtMillis(run.duration_ms)) + '</td>'
        + '<td class="text-center">'
        + '<button class="btn btn-sm btn-outline-primary" onclick="showAnalyticsRunDetails(' + idx + ')" title="View analytics run details">'
        + '<i class="bi bi-eye"></i></button></td>';
}

function _renderAnalyticsRunsTable() {
    const empty = document.getElementById('analyticsRunsEmpty');
    const wrapper = document.getElementById('analyticsRunsTableWrapper');
    const tbody = document.getElementById('analyticsRunsTableBody');
    if (!tbody) return;

    if (_analyticsRunsCache.length === 0) {
        wrapper.style.display = 'none';
        empty.style.display = '';
        return;
    }
    empty.style.display = 'none';
    tbody.innerHTML = '';
    _analyticsRunsCache.forEach(function (run, idx) {
        const row = document.createElement('tr');
        row.innerHTML = _analyticsRunRow(run, idx);
        tbody.appendChild(row);
    });
    wrapper.style.display = '';
}
```

**(f)** Replace `loadDomainRuns` (old lines 109-150) with one entry point delegating to two isolated loaders:

```javascript
async function _loadBuildRuns() {
    const loading = document.getElementById('runsLoading');
    const empty = document.getElementById('runsEmpty');
    const error = document.getElementById('runsError');
    const wrapper = document.getElementById('runsTableWrapper');
    if (!loading) return;

    loading.style.display = '';
    empty.style.display = 'none';
    error.style.display = 'none';
    wrapper.style.display = 'none';

    try {
        const response = await fetch('/domain/build-runs', { credentials: 'same-origin' });
        const data = await response.json();
        loading.style.display = 'none';

        if (!data.success) {
            document.getElementById('runsErrorMessage').textContent =
                data.message || 'Failed to load build runs';
            error.style.display = '';
            return;
        }
        _runsCache = data.runs || [];
        _renderRunsTable();
    } catch (err) {
        loading.style.display = 'none';
        document.getElementById('runsErrorMessage').textContent = err.message;
        error.style.display = '';
    }
}

async function _loadAnalyticsRuns() {
    const loading = document.getElementById('analyticsRunsLoading');
    const empty = document.getElementById('analyticsRunsEmpty');
    const error = document.getElementById('analyticsRunsError');
    const wrapper = document.getElementById('analyticsRunsTableWrapper');
    if (!loading) return;

    loading.style.display = '';
    empty.style.display = 'none';
    error.style.display = 'none';
    wrapper.style.display = 'none';

    try {
        const response = await fetch('/dtwin/metrics/history', { credentials: 'same-origin' });
        const data = await response.json();
        loading.style.display = 'none';

        if (!data.success) {
            document.getElementById('analyticsRunsErrorMessage').textContent =
                data.message || 'Failed to load analytics runs';
            error.style.display = '';
            return;
        }
        _analyticsRunsCache = data.runs || [];
        _renderAnalyticsRunsTable();
    } catch (err) {
        loading.style.display = 'none';
        document.getElementById('analyticsRunsErrorMessage').textContent = err.message;
        error.style.display = '';
    }
}

// Deliberately sequential awaits rather than Promise.all: each loader owns
// its own error handling, and neither is allowed to reject the other.
async function loadDomainRuns() {
    await _loadBuildRuns();
    await _loadAnalyticsRuns();
    _runsLoaded = true;
}
```

**(g)** Add the analytics details modal renderer next to `showRunDetailsObj`:

```javascript
function showAnalyticsRunDetails(idx) {
    const run = _analyticsRunsCache[idx];
    if (!run) return;

    const body = document.getElementById('analyticsRunDetailsBody');
    if (!body) {
        console.error('[domain-runs] #analyticsRunDetailsBody not found — modal missing from page template.');
        return;
    }

    const failed = (run.status || '').toLowerCase() === 'failed';
    const scope = (run.class_filter || []);
    const dash = '<span class="text-muted">&mdash;</span>';
    const num = function (v) { return _esc((Number(v) || 0).toLocaleString()); };

    let html = '<div class="row g-2 mb-3">';
    html += _kv('Status', _analyticsStatusBadge(run.status));
    html += _kv('Version', '<span class="badge bg-secondary">v' + _esc(run.version || '?') + '</span>');
    html += _kv('When', _fmtTs(run.computed_at));
    html += _kv('Duration', _esc(_fmtMillis(run.duration_ms)));
    html += '</div>';

    if (run.error) {
        html += '<div class="alert alert-danger small mb-3"><i class="bi bi-exclamation-octagon me-1"></i>'
            + _esc(run.error) + '</div>';
    }

    html += '<h6 class="mt-2 mb-2"><i class="bi bi-diagram-3 me-1"></i>Scope</h6>';
    if (!scope.length) {
        html += '<p class="text-muted small mb-3">All entity types.</p>';
    } else {
        html += '<ul class="small mb-3">' + scope.map(function (uri) {
            return '<li><span class="fw-semibold">' + _esc(_runLocalName(uri)) + '</span> '
                + '<span class="text-muted font-monospace" style="font-size:0.75rem">' + _esc(uri) + '</span></li>';
        }).join('') + '</ul>';
    }

    html += '<h6 class="mt-2 mb-2"><i class="bi bi-bar-chart me-1"></i>Graph</h6>';
    html += '<div class="row g-2 mb-3">';
    html += _kv('Nodes', failed ? dash : num(run.node_count));
    html += _kv('Edges', failed ? dash : num(run.edge_count));
    html += _kv('Connected Components', failed ? dash : num(run.connected_components));
    html += _kv('Avg Degree', failed ? dash : _esc((Number(run.avg_degree) || 0).toFixed(2)));
    html += _kv('Density', failed ? dash : _esc((Number(run.density) || 0).toFixed(6)));
    html += _kv('Task ID', '<span class="font-monospace small">' + _esc(run.task_id || '—') + '</span>');
    html += '</div>';

    body.innerHTML = html;
    bootstrap.Modal.getOrCreateInstance(
        document.getElementById('analyticsRunDetailsModal')
    ).show();
}

window.showAnalyticsRunDetails = showAnalyticsRunDetails;
```

Leave `showRunDetails`, `showRunDetailsObj`, `_kv`, `_statsTable`, `_phaseTable`, `_kindBadge`, `_esc`, `_fmtTs`, `_fmtDuration` and the two event listeners at the bottom of the file unchanged.

- [ ] **Step 4: Run the tests to verify they pass**

```bash
env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u DATABRICKS_CONFIG_PROFILE .venv/bin/pytest tests/units/front/test_runs_page.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/front/static/domain/js/domain-runs.js tests/units/front/test_runs_page.py
git commit -m "feat(ui): render analytics runs on the Runs page

Two isolated loaders rather than Promise.all so one endpoint failing leaves
the other table on screen. Analytics gets its own status badge because it
reports completed/failed where builds report success/error/cancelled, and
failed rows dash their metrics out rather than printing the stored zeros."
```

---

### Task 5: Remove the History tab from the Analytics page

**Files:**
- Modify: `src/front/templates/partials/dtwin/_query_analytics.html`
- Test: `tests/units/front/test_runs_page.py` (extend)

**Interfaces:**
- Consumes: nothing. Nothing consumes what this removes.

- [ ] **Step 1: Write the failing test**

Append to `tests/units/front/test_runs_page.py`:

```python
_ANALYTICS = Path("src/front/templates/partials/dtwin/_query_analytics.html")


class TestHistoryTabRemoved:
    """Run history lives on the Runs page now, not behind the eighth tab of
    a page about the current result."""

    @pytest.mark.parametrize(
        "marker",
        [
            "atab-btn-history",
            "atab-history",
            "analyticsLoadHistory",
            "analyticsHistoryBody",
            "analyticsHistoryEmpty",
        ],
    )
    def test_no_trace_of_the_history_tab(self, marker):
        assert marker not in _ANALYTICS.read_text(encoding="utf-8")

    def test_helpers_the_other_tabs_still_use_survive(self):
        """_formatComputedAt and _localName were used by the History rows but
        are used by other tabs too — deleting them would be over-reach."""
        src = _ANALYTICS.read_text(encoding="utf-8")
        assert "function _formatComputedAt" in src
        assert "function _localName" in src
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u DATABRICKS_CONFIG_PROFILE .venv/bin/pytest tests/units/front/test_runs_page.py -k History -q
```

Expected: the five parametrized cases FAIL; `test_helpers_the_other_tabs_still_use_survive` PASSES.

- [ ] **Step 3: Delete the four pieces**

In `src/front/templates/partials/dtwin/_query_analytics.html`, delete each of these in turn. Work bottom-up so earlier line numbers stay valid.

**(a)** The `analyticsLoadHistory` function — the whole `window.analyticsLoadHistory = async function () { ... };` block starting at line 755 and running to its closing `};` (approximately line 807).

**(b)** The auto-refresh call at line 724:

```javascript
        if (typeof window.analyticsLoadHistory === 'function') window.analyticsLoadHistory();
```

**(c)** The panel, lines 321-354 — the entire `<div class="tab-pane fade" id="atab-history" role="tabpanel">` element including its `<!-- ── TAB: History ── -->` comment and closing `</div>`.

**(d)** The tab trigger, lines 173-178 — the entire `<li class="nav-item" role="presentation">` containing `atab-btn-history`.

Do **not** delete `_formatComputedAt` (line 640) or `_localName` (line 1510): both are used by other tabs.

- [ ] **Step 4: Run the tests to verify they pass**

```bash
env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u DATABRICKS_CONFIG_PROFILE .venv/bin/pytest tests/units/front/test_runs_page.py -q
```

Expected: PASS.

- [ ] **Step 5: Check nothing else referenced it**

```bash
rg -n "analyticsLoadHistory|atab-history|analyticsHistory" src/ tests/
```

Expected: only the assertions in `tests/units/front/test_runs_page.py`.

- [ ] **Step 6: Commit**

```bash
git add src/front/templates/partials/dtwin/_query_analytics.html tests/units/front/test_runs_page.py
git commit -m "refactor(ui): drop the Analytics History tab

Run history moved to the Runs page. The endpoint, store methods and
recording path all stay — Runs consumes them now. This is a view removal."
```

---

### Task 6: Remove the dead Runs section from the Domain page

**Files:**
- Modify: `src/front/templates/domain.html:83-84`
- Modify: `src/front/static/domain/js/domain.js:61-64`
- Modify: `tests/units/api/test_ui_rendering.py:453-461`

**Interfaces:**
- Consumes: nothing.

Background: `domain.html` includes the Runs partial and `domain.js` hooks `section === 'runs'`, but the `domain` menu in `menu_config.json` has no `runs` item — only `digitaltwin` does. The section is unreachable. Left in place it would render an analytics-history table on a page where that is out of context.

- [ ] **Step 1: Update the render test first**

In `tests/units/api/test_ui_rendering.py`, remove `"runs-section"` from the `TestDomainPage.test_section_div_exists` parametrize list at lines 453-458, so it reads:

```python
    @pytest.mark.parametrize(
        "section_id",
        ["information-section", "metadata-section", "validation-section",
         "audit-section", "mytasks-section", "discussions-section"],
    )
    def test_section_div_exists(self, client, section_id):
        html = _html(client, "/domain")
        assert _find(_tags(html), id_=section_id) is not None
```

Add these two tests to `TestDomainPage`, after `test_audit_section_link_and_script`:

```python
    def test_no_dead_runs_section(self):
        """The Domain menu has no Runs item, so the section was unreachable.
        Left in, it would render analytics history on a page about the
        domain record."""
        from pathlib import Path

        html = Path("src/front/templates/domain.html").read_text(encoding="utf-8")
        assert 'id="runs-section"' not in html

    def test_run_details_modal_and_script_survive(self, client):
        """domain-audit.js reuses showRunDetailsObj to expand build entries
        in the audit timeline, so the script and the modal must stay even
        though the section is gone."""
        html = _html(client, "/domain")
        assert any("domain-runs.js" in src for src in _script_srcs(html))
        assert _find(_tags(html), id_="runDetailsModal") is not None
```

Add to `TestDigitalTwinPage` (after `test_sigmagraph_section_present`):

```python
    def test_runs_section_present(self, client):
        """Runs lives on the Knowledge Graph page, which is the only menu
        that declares it."""
        html = _html(client, "/dtwin/")
        assert _find(_tags(html), id_="runs-section") is not None
```

- [ ] **Step 2: Run the tests to verify the new ones fail**

```bash
env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u DATABRICKS_CONFIG_PROFILE .venv/bin/pytest tests/units/api/test_ui_rendering.py -q
```

Expected: `test_no_dead_runs_section` FAILS; the other two PASS.

- [ ] **Step 3: Remove the dead section**

In `src/front/templates/domain.html`, delete lines 83-84 — the whole block:

```html
            <div id="runs-section" class="sidebar-section">
                {% include "partials/domain/_domain_runs.html" %}
            </div>
```

Keep the `domain-runs.js` `<script>` tag at line 138 and the page-level `runDetailsModal`: `domain-audit.js` reuses `showRunDetailsObj` for build entries in the audit timeline.

- [ ] **Step 4: Remove the dead hook**

In `src/front/static/domain/js/domain.js`, delete lines 61-64:

```javascript
            // Load build runs when switching to runs section
            if (section === 'runs' && typeof loadDomainRuns === 'function') {
                loadDomainRuns();
            }
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u DATABRICKS_CONFIG_PROFILE .venv/bin/pytest tests/units/api/ tests/units/front/ -q
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/front/templates/domain.html src/front/static/domain/js/domain.js tests/units/api/test_ui_rendering.py
git commit -m "refactor(ui): drop the unreachable Runs section from the Domain page

The domain menu never declared a runs item, so the section could not be
opened. The script tag and runDetailsModal stay: the audit timeline reuses
showRunDetailsObj for its build entries."
```

---

### Task 7: Docs, changelog, full suite

**Files:**
- Modify: `docs/user-guide.md`
- Modify: `changelogs/v0.7.0/benoitcayladbx_2026-08-02.log` (create if absent)

- [ ] **Step 1: Find the docs that describe the History tab**

```bash
rg -n -i "history tab|analysis history|analytics history" docs/ README.md
```

- [ ] **Step 2: Update the user guide**

In `docs/user-guide.md`, rewrite each hit so run history is described as living on Knowledge Graph → Management → Runs, as the second of two tables under the build-run table. State that both tables show every version and that analytics rows have a Details button showing scope, metrics, duration, task ID and any error. Do not describe a version filter — there isn't one any more.

- [ ] **Step 3: Confirm the version in `pyproject.toml`**

```bash
rg -n '^version' pyproject.toml
```

Use the reported `vX.Y.Z` for the changelog directory. The steps below assume `v0.7.0`; if it differs, use the real one.

- [ ] **Step 4: Write the changelog**

Append to `changelogs/v0.7.0/benoitcayladbx_2026-08-02.log` (create it, and its directory, if absent) a section with: a title, context explaining that two run-history views existed in two places and one was buried behind the eighth tab of a page about the current result; a numbered list of the changes with file paths; the list of modified files; and the test result.

- [ ] **Step 5: Run the full suite**

```bash
env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u DATABRICKS_CONFIG_PROFILE -u DATABRICKS_CLIENT_ID -u DATABRICKS_CLIENT_SECRET -u LAKEBASE_PROJECT -u LAKEBASE_BRANCH -u LAKEBASE_DATABASE -u LAKEBASE_DATABASE_RESOURCE_SEGMENT -u LAKEBASE_SCHEMA .venv/bin/pytest -q -m "not scenario"
```

Expected: 0 failed. The baseline before this work was 3768 passed, 275 skipped, 1 xfailed.

- [ ] **Step 6: Revert any lockfile churn and commit**

```bash
git checkout -- uv.lock 2>/dev/null || true
git add docs/ changelogs/
git commit -m "docs: run history now lives on the Runs page"
```

---

## Manual verification (after `make deploy`)

The JS has no automated coverage, so these five checks matter:

1. Knowledge Graph → Management → Runs shows two tables, build above analytics.
2. The analytics table lists runs from **more than one version** — this is the widened query working. Confirm the Version badge differs across rows on a domain with history on several versions.
3. A failed analytics run shows a red Failed badge with dashes, not zeros, in the metric columns.
4. The Details eye on an analytics row opens the modal with scope, metrics, task ID and the error text.
5. Knowledge Graph → Analytics has seven tabs and no History tab.
