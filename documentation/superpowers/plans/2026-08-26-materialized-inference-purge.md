# Materialized Inference Purge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a source-safe “Purge Inferences” action to KG/Inference and KG/Cohorts that removes all reasoning and cohort triples from the active graph companion.

**Architecture:** Add one graph-store capability that purges the generated companion and returns its prior row count. Expose it through a Builder-protected `/dtwin/reasoning/inferred` endpoint, then invoke it from one shared frontend module and one shared button partial included by both KG sections.

**Tech Stack:** Python 3.10+, FastAPI, Jinja2, vanilla JavaScript, Bootstrap 5, pytest.

## Global Constraints

- Version: `0.8.0`.
- Purge reasoning and cohort graph triples together.
- Never mutate mapped source relations.
- Never purge standalone Inference Delta targets or cohort UC output tables.
- Neo4j must reject the operation until generated triples have durable provenance.
- Use `OntoBricksError` subclasses for API-visible failures.
- Require the domain Builder role and retain the existing graph-refresh permission behavior.
- Use the standard `showConfirmDialog` and `showNotification` helpers.
- Follow test-first red-green-refactor for every production change.
- Run `uv run --frozen pytest -q -m "not scenario"` before completion.
- Do not create git commits unless the user explicitly requests them.

---

### Task 1: Graph-store purge capability

**Files:**
- Modify: `src/back/core/graphdb/GraphDBBackend.py`
- Modify: `src/back/core/graphdb/delta/DeltaFlatStore.py`
- Modify: `src/back/core/graphdb/lakebase/LakebaseFlatStore.py`
- Create: `tests/units/graphdb/test_materialized_inference_purge.py`

**Interfaces:**
- Produces: `GraphDBBackend.purge_materialized_triples(table_name: str) -> int`
- Semantics: return the number of generated triples present before an idempotent companion truncate.
- Unsupported stores inherit a `NotImplementedError` implementation.

- [ ] **Step 1: Write failing backend tests**

```python
import pytest
from unittest.mock import MagicMock, patch

from back.core.graphdb.GraphDBBackend import GraphDBBackend
from back.core.graphdb.delta.DeltaFlatStore import DeltaFlatStore
from back.core.graphdb.lakebase.LakebaseFlatStore import LakebaseFlatStore


def test_base_backend_rejects_generated_purge():
    with pytest.raises(NotImplementedError, match="generated"):
        GraphDBBackend.purge_materialized_triples(MagicMock(), "sales_V3")


def test_delta_counts_and_truncates_only_inferred_companion():
    client = MagicMock()
    domain = MagicMock()
    settings = MagicMock()
    store = DeltaFlatStore(client, domain=domain, settings=settings)

    with (
        patch.object(store, "_writable_table_fqn", return_value="cat.sch.sales_inferred"),
        patch.object(store, "count_triples", return_value=17) as count,
        patch("back.core.graphdb.delta.DeltaFlatStore.materialize.truncate_table") as truncate,
    ):
        assert store.purge_materialized_triples("sales_V3") == 17

    count.assert_called_once_with("cat.sch.sales_inferred")
    truncate.assert_called_once_with(client, "cat.sch.sales_inferred")


def test_lakebase_counts_and_truncates_only_app_companion():
    store = object.__new__(LakebaseFlatStore)
    store._sync_mode = "app_managed"
    store.count_triples = MagicMock(return_value=9)
    cursor = MagicMock()
    cursor_context = MagicMock()
    cursor_context.__enter__.return_value = cursor
    cursor_context.__exit__.return_value = False
    store._cursor = MagicMock(return_value=cursor_context)

    with (
        patch.object(store, "companion_phy", return_value="g_sales_v3__app"),
        patch(
            "back.core.graphdb.lakebase.LakebaseFlatStore."
            "_companion_ddl.truncate_companion"
        ) as truncate,
    ):
        assert store.purge_materialized_triples("sales_V3") == 9

    store.count_triples.assert_called_once_with("g_sales_v3__app")
    truncate.assert_called_once_with(cursor, "g_sales_v3__app")
```

- [ ] **Step 2: Verify the tests fail for the missing capability**

Run:

```bash
uv run --frozen pytest -q tests/units/graphdb/test_materialized_inference_purge.py
```

Expected: failures report that `purge_materialized_triples` does not exist.

- [ ] **Step 3: Add the default safe rejection**

Add to `GraphDBBackend`:

```python
def purge_materialized_triples(self, table_name: str) -> int:
    """Remove generated triples while preserving mapped source triples."""
    raise NotImplementedError(
        f"{type(self).__name__} cannot safely purge generated triples"
    )
```

- [ ] **Step 4: Implement Delta companion purge**

Add to `DeltaFlatStore`:

```python
def purge_materialized_triples(self, table_name: str) -> int:
    """Truncate the inferred companion and preserve the mapped data table."""
    inferred = self._writable_table_fqn(table_name)
    count = self.count_triples(inferred)
    materialize.truncate_table(self._client, inferred)
    logger.info("Purged %d materialized triples from %s", count, inferred)
    return count
```

- [ ] **Step 5: Implement Lakebase companion purge**

Add to `LakebaseFlatStore` without the current `is_synced` early return:

```python
def purge_materialized_triples(self, table_name: str) -> int:
    """Truncate the writable app companion in either Lakebase layout mode."""
    companion = self.companion_phy(table_name)
    count = self.count_triples(companion)
    with self._cursor() as cur:
        _companion_ddl.truncate_companion(cur, companion)
    logger.info("Purged %d materialized triples from %s", count, companion)
    return count
```

- [ ] **Step 6: Verify graph-store tests pass**

Run:

```bash
uv run --frozen pytest -q tests/units/graphdb/test_materialized_inference_purge.py
```

Expected: all tests pass.

---

### Task 2: Protected purge endpoint

**Files:**
- Modify: `src/api/routers/internal/dtwin.py`
- Create: `tests/units/dtwin/test_purge_materialized_inferences.py`

**Interfaces:**
- Consumes: `GraphDBBackend.purge_materialized_triples(table_name: str) -> int`
- Produces: `DELETE /dtwin/reasoning/inferred`
- Response: `{"success": True, "graph_name": str, "purged_count": int}`

- [ ] **Step 1: Write failing route tests**

Use FastAPI dependency overrides for `get_session_manager` and `get_settings`,
and patch `get_domain`, `get_graphdb`, and `effective_graph_name`:

```python
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient


def test_purge_materialized_inferences_returns_graph_and_count(client):
    domain = MagicMock()
    store = MagicMock()
    store.purge_materialized_triples.return_value = 23

    with (
        patch("api.routers.internal.dtwin.get_domain", return_value=domain),
        patch("api.routers.internal.dtwin.get_graphdb", return_value=store),
        patch(
            "api.routers.internal.dtwin.effective_graph_name",
            return_value="sales_V3",
        ),
    ):
        response = client.delete("/dtwin/reasoning/inferred")

    assert response.status_code == 200
    assert response.json() == {
        "success": True,
        "graph_name": "sales_V3",
        "purged_count": 23,
    }
    store.purge_materialized_triples.assert_called_once_with("sales_V3")


def test_purge_materialized_inferences_rejects_unsafe_backend(client):
    store = MagicMock()
    store.purge_materialized_triples.side_effect = NotImplementedError(
        "Neo4jStore cannot safely purge generated triples"
    )

    with (
        patch("api.routers.internal.dtwin.get_domain", return_value=MagicMock()),
        patch("api.routers.internal.dtwin.get_graphdb", return_value=store),
        patch(
            "api.routers.internal.dtwin.effective_graph_name",
            return_value="sales_V3",
        ),
    ):
        response = client.delete("/dtwin/reasoning/inferred")

    assert response.status_code == 502
    assert response.json()["error"] == "infrastructure_error"
```

Add a permission test using the repository’s existing `require(ROLE_BUILDER,
scope="domain")` test setup and assert a non-Builder receives `403`.

- [ ] **Step 2: Verify route tests fail**

Run:

```bash
uv run --frozen pytest -q tests/units/dtwin/test_purge_materialized_inferences.py
```

Expected: `404` for the missing endpoint.

- [ ] **Step 3: Implement the endpoint**

Add near the existing reasoning materialization route:

```python
@router.delete(
    "/reasoning/inferred",
    dependencies=[Depends(require(ROLE_BUILDER, scope="domain"))],
)
async def purge_materialized_inferences(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Purge generated graph triples without modifying mapped source data."""
    domain = get_domain(session_mgr)
    store = _require_graph_store(domain, settings)
    graph_name = effective_graph_name(domain)
    try:
        purged_count = await run_blocking(
            store.purge_materialized_triples,
            graph_name,
        )
    except NotImplementedError as exc:
        raise InfrastructureError(
            "The active graph backend cannot safely purge materialized inferences",
            detail=str(exc),
        ) from exc
    return {
        "success": True,
        "graph_name": graph_name,
        "purged_count": purged_count,
    }
```

- [ ] **Step 4: Verify route tests pass**

Run:

```bash
uv run --frozen pytest -q tests/units/dtwin/test_purge_materialized_inferences.py
```

Expected: all endpoint and permission tests pass.

---

### Task 3: Shared KG purge control

**Files:**
- Create: `src/front/templates/partials/dtwin/_purge_inferences_button.html`
- Modify: `src/front/templates/partials/dtwin/_query_reasoning.html`
- Modify: `src/front/templates/partials/dtwin/_query_cohorts.html`
- Create: `src/front/static/query/js/query-purge-inferences.js`
- Modify: `src/front/templates/dtwin.html`
- Create: `tests/units/front/test_purge_inferences_ui.py`

**Interfaces:**
- Produces: `.js-purge-inferences-btn` shared button contract.
- Produces: `window.InferencePurgeModule.purge()`.
- Consumes: `DELETE /dtwin/reasoning/inferred`.

- [ ] **Step 1: Write failing frontend contract tests**

```python
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit
ROOT = Path(__file__).resolve().parents[3]


def _read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def test_shared_purge_button_is_in_both_kg_sections():
    include = '{% include "partials/dtwin/_purge_inferences_button.html" %}'
    assert include in _read("src/front/templates/partials/dtwin/_query_reasoning.html")
    assert include in _read("src/front/templates/partials/dtwin/_query_cohorts.html")


def test_shared_button_uses_destructive_style_without_inline_handler():
    html = _read("src/front/templates/partials/dtwin/_purge_inferences_button.html")
    assert "Purge Inferences" in html
    assert "btn-outline-danger" in html
    assert "js-purge-inferences-btn" in html
    assert "onclick=" not in html


def test_shared_action_confirms_checks_permission_and_calls_endpoint():
    js = _read("src/front/static/query/js/query-purge-inferences.js")
    assert "showConfirmDialog" in js
    assert "canRefreshGraph" in js
    assert "'/dtwin/reasoning/inferred'" in js
    assert "method: 'DELETE'" in js
    assert "showNotification" in js
    assert "checkTripleStoreStatus(true)" in js


def test_dtwin_loads_shared_purge_script_once():
    html = _read("src/front/templates/dtwin.html")
    assert html.count("query/js/query-purge-inferences.js") == 1
```

- [ ] **Step 2: Verify frontend tests fail**

Run:

```bash
uv run --frozen pytest -q tests/units/front/test_purge_inferences_ui.py
```

Expected: failures report missing partial, includes, and script.

- [ ] **Step 3: Create the shared button partial**

```html
<button type="button"
        class="btn btn-sm btn-outline-danger js-purge-inferences-btn"
        data-graph-name="{{ graph_name or domain_name }}"
        title="Remove materialized inference and cohort triples">
    <i class="bi bi-trash me-1"></i>Purge Inferences
</button>
```

Include it in the action clusters of `_query_reasoning.html` and
`_query_cohorts.html`.

- [ ] **Step 4: Implement the shared frontend module**

```javascript
const InferencePurgeModule = {
    busy: false,

    async purge(button) {
        if (this.busy) return;
        if (window.OB && typeof window.OB.canRefreshGraph === 'function'
                && !window.OB.canRefreshGraph()) {
            showNotification(
                'Purge is unavailable — builder access and graph refresh permission are required.',
                'warning'
            );
            return;
        }

        const graphName = this._escapeHtml(
            button?.dataset.graphName || 'the active graph'
        );
        const confirmed = await showConfirmDialog({
            title: 'Purge materialized inferences?',
            message: `Remove all materialized inference and cohort triples from ${graphName}?`,
            detailHtml: 'Mapped source triples and external Delta/UC outputs are preserved.',
            confirmText: 'Purge',
            confirmClass: 'btn-danger',
            icon: 'trash',
        });
        if (!confirmed) return;

        this._setBusy(true);
        try {
            const response = await fetch('/dtwin/reasoning/inferred', {
                method: 'DELETE',
                credentials: 'include',
            });
            const data = await response.json();
            if (!response.ok || !data.success) {
                throw new Error(data.message || 'Purge failed');
            }
            this._clearReasoningResult();
            showNotification(
                `Purged ${data.purged_count || 0} materialized triples.`,
                'success'
            );
            if (typeof checkTripleStoreStatus === 'function') {
                await checkTripleStoreStatus(true);
            }
        } catch (error) {
            showNotification(error.message || 'Purge failed.', 'error');
        } finally {
            this._setBusy(false);
        }
    },

    _setBusy(busy) {
        this.busy = busy;
        document.querySelectorAll('.js-purge-inferences-btn').forEach((button) => {
            button.disabled = busy;
        });
    },

    _clearReasoningResult() {
        if (typeof ReasoningModule === 'undefined') return;
        ReasoningModule._inferredData = [];
        ReasoningModule._inferredPage = 0;
        ReasoningModule._updateTabBadges(0);
        ReasoningModule._refreshInferredPane();
        document.getElementById('materializePanel')?.classList.add('d-none');
    },

    _escapeHtml(value) {
        const element = document.createElement('div');
        element.textContent = String(value || '');
        return element.innerHTML;
    },

    init() {
        document.querySelectorAll('.js-purge-inferences-btn').forEach((button) => {
            button.addEventListener('click', () => this.purge(button));
        });
    },
};

window.InferencePurgeModule = InferencePurgeModule;
document.addEventListener('DOMContentLoaded', () => InferencePurgeModule.init());
```

Load `query-purge-inferences.js` once after `query-reasoning.js` and
`query-cohorts.js` in `dtwin.html`.

- [ ] **Step 5: Verify frontend tests pass**

Run:

```bash
uv run --frozen pytest -q tests/units/front/test_purge_inferences_ui.py
```

Expected: all frontend contract tests pass.

---

### Task 4: Changelog and full verification

**Files:**
- Create or append: `changelogs/v0.8.0/benoitcayladbx_2026-08-26.log`
- Verify all files changed by Tasks 1–3.

**Interfaces:**
- No new runtime interface.

- [ ] **Step 1: Run focused regression tests**

Run:

```bash
uv run --frozen pytest -q \
  tests/units/graphdb/test_materialized_inference_purge.py \
  tests/units/dtwin/test_purge_materialized_inferences.py \
  tests/units/front/test_purge_inferences_ui.py \
  tests/units/dtwin/test_run_inference_task.py \
  tests/back/core/digitaltwin/test_cohort_service_units.py
```

Expected: all tests pass.

- [ ] **Step 2: Check diagnostics on modified Python and JavaScript files**

Use the IDE linter diagnostics for:

```text
src/back/core/graphdb/GraphDBBackend.py
src/back/core/graphdb/delta/DeltaFlatStore.py
src/back/core/graphdb/lakebase/LakebaseFlatStore.py
src/api/routers/internal/dtwin.py
src/front/static/query/js/query-purge-inferences.js
```

Expected: no newly introduced diagnostics.

- [ ] **Step 3: Run the mandatory repository suite**

Run:

```bash
uv run --frozen pytest -q -m "not scenario"
```

Expected: all non-scenario tests pass.

- [ ] **Step 4: Write the v0.8.0 changelog entry**

Append an English section containing:

```text
Materialized inference purge

Context:
Builders need a source-safe way to remove reasoning and cohort triples from
the active Knowledge Graph without rebuilding or deleting mapped data.

1. src/back/core/graphdb/... — added generated-companion purge capabilities.
2. src/api/routers/internal/dtwin.py — added the protected purge endpoint.
3. src/front/... — added the shared purge action to Inference and Cohorts.
4. tests/... — added backend, route, permission, and UI contract coverage.

Modified files:
src/back/core/graphdb/GraphDBBackend.py
src/back/core/graphdb/delta/DeltaFlatStore.py
src/back/core/graphdb/lakebase/LakebaseFlatStore.py
src/api/routers/internal/dtwin.py
src/front/templates/partials/dtwin/_purge_inferences_button.html
src/front/templates/partials/dtwin/_query_reasoning.html
src/front/templates/partials/dtwin/_query_cohorts.html
src/front/static/query/js/query-purge-inferences.js
src/front/templates/dtwin.html
tests/units/graphdb/test_materialized_inference_purge.py
tests/units/dtwin/test_purge_materialized_inferences.py
tests/units/front/test_purge_inferences_ui.py

Tests:
uv run --frozen pytest -q -m "not scenario"
```

Follow the command with the observed pass/fail count and duration from the
actual test run.

- [ ] **Step 5: Re-run changelog-sensitive checks if configured**

Run:

```bash
git diff --check
```

Expected: no whitespace errors.

---

### Task 5: Lightweight materialized-inference status

**Files:**
- Modify: `src/back/core/graphdb/GraphDBBackend.py`
- Modify: `src/back/core/graphdb/delta/DeltaFlatStore.py`
- Modify: `src/back/core/graphdb/lakebase/LakebaseFlatStore.py`
- Modify: `src/api/routers/internal/dtwin.py`
- Modify: `tests/units/graphdb/test_materialized_inference_purge.py`
- Modify: `tests/units/dtwin/test_purge_materialized_inferences.py`

**Interfaces:**
- Produces: `GraphDBBackend.supports_materialized_inference_purge: bool`
- Produces: live `GET /dtwin/reasoning/inferred` status with
  `graph_name`, `materialized_inference_count`, and `purge_supported`.
- Preserves: nested `reasoning.inferred_count` and
  `reasoning.inferred_triples` compatibility fields.

- [ ] **Step 1: Write failing capability and GET status tests**

Add backend assertions:

```python
def test_base_backend_reports_source_safe_purge_as_unsupported():
    assert GraphDBBackend.supports_materialized_inference_purge is False


def test_companion_backends_report_source_safe_purge_as_supported():
    assert DeltaFlatStore.supports_materialized_inference_purge is True
    assert LakebaseFlatStore.supports_materialized_inference_purge is True
```

Add route tests:

```python
def test_materialized_inference_status_returns_live_count(api_client):
    store = MagicMock()
    store.supports_materialized_inference_purge = True
    store.get_inferred_triple_count.return_value = 31

    with (
        patch(f"{MODULE}.get_domain", return_value=MagicMock()),
        patch(f"{MODULE}.get_graphdb", return_value=store),
        patch(f"{MODULE}.effective_graph_name", return_value="sales_V3"),
    ):
        response = api_client.get("/dtwin/reasoning/inferred")

    assert response.status_code == 200
    assert response.json() == {
        "success": True,
        "graph_name": "sales_V3",
        "materialized_inference_count": 31,
        "purge_supported": True,
        "reasoning": {
            "last_run": None,
            "inferred_count": 31,
            "inferred_triples": [],
        },
    }
    store.get_inferred_triple_count.assert_called_once_with("sales_V3")


def test_materialized_inference_status_returns_na_for_unsafe_backend(api_client):
    store = MagicMock()
    store.supports_materialized_inference_purge = False

    with (
        patch(f"{MODULE}.get_domain", return_value=MagicMock()),
        patch(f"{MODULE}.get_graphdb", return_value=store),
        patch(f"{MODULE}.effective_graph_name", return_value="sales_V3"),
    ):
        response = api_client.get("/dtwin/reasoning/inferred")

    assert response.status_code == 200
    assert response.json()["materialized_inference_count"] is None
    assert response.json()["purge_supported"] is False
    store.get_inferred_triple_count.assert_not_called()
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
uv run --frozen pytest -q \
  tests/units/graphdb/test_materialized_inference_purge.py \
  tests/units/dtwin/test_purge_materialized_inferences.py
```

Expected: capability assertions and live GET payload assertions fail.

- [ ] **Step 3: Add explicit backend capability**

Add to `GraphDBBackend`:

```python
supports_materialized_inference_purge = False
```

Add to `DeltaFlatStore` and `LakebaseFlatStore`:

```python
supports_materialized_inference_purge = True
```

- [ ] **Step 4: Upgrade the compatibility GET endpoint**

Replace the static zero-count response in `get_inferred_triples` with:

```python
@router.get("/reasoning/inferred")
async def get_inferred_triples(
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Return live materialized-inference status without listing triples."""
    domain = get_domain(session_mgr)
    store = _require_graph_store(domain, settings)
    graph_name = effective_graph_name(domain)
    supported = bool(store.supports_materialized_inference_purge)
    inferred_count = (
        await run_blocking(store.get_inferred_triple_count, graph_name)
        if supported
        else None
    )
    return {
        "success": True,
        "graph_name": graph_name,
        "materialized_inference_count": inferred_count,
        "purge_supported": supported,
        "reasoning": {
            "last_run": None,
            "inferred_count": inferred_count,
            "inferred_triples": [],
        },
    }
```

- [ ] **Step 5: Verify backend and route tests pass**

Run:

```bash
uv run --frozen pytest -q \
  tests/units/graphdb/test_materialized_inference_purge.py \
  tests/units/dtwin/test_purge_materialized_inferences.py
```

Expected: all tests pass.

---

### Task 6: Count-aware modal and Build placement

**Files:**
- Modify: `src/front/templates/partials/dtwin/_query_databricks_build.html`
- Modify: `src/front/static/query/js/query-purge-inferences.js`
- Modify: `tests/units/front/test_purge_inferences_ui.py`

**Interfaces:**
- Consumes: live `GET /dtwin/reasoning/inferred` status.
- Produces: exact count in the confirmation message.
- Adds: existing shared purge button to Knowledge Graph → Build.

- [ ] **Step 1: Extend failing UI contracts**

```python
def test_shared_purge_button_is_in_all_three_kg_sections():
    include = '{% include "partials/dtwin/_purge_inferences_button.html" %}'
    paths = [
        "src/front/templates/partials/dtwin/_query_databricks_build.html",
        "src/front/templates/partials/dtwin/_query_reasoning.html",
        "src/front/templates/partials/dtwin/_query_cohorts.html",
    ]
    for path in paths:
        assert include in _read(path)


def test_confirmation_loads_and_names_combined_materialized_count():
    js = _read("src/front/static/query/js/query-purge-inferences.js")
    assert "method: 'GET'" in js
    assert "materialized_inference_count" in js
    assert "reasoning and cohorts" in js
```

- [ ] **Step 2: Run UI tests and verify they fail**

Run:

```bash
uv run --frozen pytest -q tests/units/front/test_purge_inferences_ui.py
```

Expected: Build include and GET/count copy assertions fail.

- [ ] **Step 3: Add the shared button to Build**

Insert after the Refresh button in
`_query_databricks_build.html`:

```html
{% include "partials/dtwin/_purge_inferences_button.html" %}
```

- [ ] **Step 4: Load status before confirmation**

Add:

```javascript
async _loadStatus() {
    const response = await fetch('/dtwin/reasoning/inferred', {
        method: 'GET',
        credentials: 'include',
    });
    const data = await response.json();
    if (!response.ok || !data.success) {
        throw new Error(data.message || 'Could not load inference count');
    }
    return data;
},
```

In `purge`, call `_loadStatus()` before `showConfirmDialog`. If status loading
fails, show an error notification and do not offer a destructive confirmation
with an unknown count. For supported backends render:

```javascript
const count = Number(status.materialized_inference_count || 0);
const countLabel = count.toLocaleString();
const message = `This will delete ${countLabel} materialized inferences `
    + `(reasoning and cohorts) from ${graphName}.`;
```

If `purge_supported` is false, show a warning and return without opening the
modal. After a successful purge, call `_publishCount(0)`:

```javascript
_publishCount(count) {
    document.querySelectorAll('[data-materialized-inference-count]')
        .forEach((element) => {
            element.textContent = Number(count || 0).toLocaleString();
        });
},
```

- [ ] **Step 5: Verify UI tests pass**

Run:

```bash
uv run --frozen pytest -q tests/units/front/test_purge_inferences_ui.py
```

Expected: all UI contracts pass.

---

### Task 7: Cockpit materialized-inference metric

**Files:**
- Modify: `src/front/templates/partials/domain/_domain_validation.html`
- Modify: `src/front/static/domain/js/domain-validation.js`
- Create: `tests/units/front/test_cockpit_inference_count.py`

**Interfaces:**
- Consumes: live `GET /dtwin/reasoning/inferred` status.
- Produces: `#psDtMaterializedInferenceCount` in the Cockpit Knowledge Graph
  card.

- [ ] **Step 1: Write failing Cockpit contracts**

```python
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit
ROOT = Path(__file__).resolve().parents[3]
HTML = ROOT / "src/front/templates/partials/domain/_domain_validation.html"
JS = ROOT / "src/front/static/domain/js/domain-validation.js"


def test_cockpit_exposes_materialized_inference_metric():
    html = HTML.read_text(encoding="utf-8")
    assert 'id="psDtMaterializedInferenceCount"' in html
    assert "Materialized inferences" in html
    assert "data-materialized-inference-count" in html


def test_cockpit_loads_lightweight_inference_status():
    js = JS.read_text(encoding="utf-8")
    assert "loadMaterializedInferenceCount()" in js
    assert "'/dtwin/reasoning/inferred'" in js
    assert "materialized_inference_count" in js
    assert "'N/A'" in js
```

- [ ] **Step 2: Run Cockpit tests and verify they fail**

Run:

```bash
uv run --frozen pytest -q tests/units/front/test_cockpit_inference_count.py
```

Expected: metric markup and loader assertions fail.

- [ ] **Step 3: Add the metric to the Knowledge Graph card**

Place below the graph triple count:

```html
<div class="small text-muted mt-1"
     title="Generated companion triples from reasoning and cohorts">
    <i class="bi bi-lightning me-1"></i>
    Materialized inferences:
    <strong id="psDtMaterializedInferenceCount"
            data-materialized-inference-count>—</strong>
</div>
```

- [ ] **Step 4: Implement Cockpit count loading**

Add:

```javascript
async function loadMaterializedInferenceCount() {
    const target = document.getElementById('psDtMaterializedInferenceCount');
    if (!target) return;
    try {
        const response = await fetch('/dtwin/reasoning/inferred', {
            credentials: 'same-origin'
        });
        const data = await response.json();
        if (!response.ok || !data.success || !data.purge_supported) {
            target.textContent = 'N/A';
            return;
        }
        const count = Number(data.materialized_inference_count || 0);
        target.textContent = count.toLocaleString();
    } catch (error) {
        target.textContent = 'N/A';
    }
}
```

Call `loadMaterializedInferenceCount()` from `loadValidationDetails()` so
initial page loading and the existing Refresh button both update it.

- [ ] **Step 5: Verify Cockpit tests pass**

Run:

```bash
uv run --frozen pytest -q tests/units/front/test_cockpit_inference_count.py
```

Expected: all Cockpit contracts pass.

---

### Task 8: Extension verification and documentation

**Files:**
- Modify: `documentation/user-guide.md`
- Modify: `documentation/data-access.md`
- Append: `changelogs/v0.8.0/benoitcayladbx_2026-08-26.log`

- [ ] **Step 1: Run focused extension regressions**

Run:

```bash
uv run --frozen pytest -q \
  tests/units/graphdb/test_materialized_inference_purge.py \
  tests/units/dtwin/test_purge_materialized_inferences.py \
  tests/units/front/test_purge_inferences_ui.py \
  tests/units/front/test_cockpit_inference_count.py
```

Expected: all tests pass.

- [ ] **Step 2: Update user and data-access documentation**

Document:

- Build as the third purge entry point;
- the pre-confirmation combined materialized-inference count;
- the Cockpit count and its reasoning-plus-cohorts semantics;
- the live GET status response fields.

- [ ] **Step 3: Run mandatory verification**

Run:

```bash
uv run --frozen pytest -q -m "not scenario"
git diff --check
```

Expected: all non-scenario tests pass and no whitespace errors are reported.

- [ ] **Step 4: Append the observed results to the existing changelog section**

Add the Build placement, live status endpoint, count-aware modal, Cockpit
metric, changed-file list, focused test result, full-suite result, browser
verification result, lint result, and `git diff --check` result in English.
