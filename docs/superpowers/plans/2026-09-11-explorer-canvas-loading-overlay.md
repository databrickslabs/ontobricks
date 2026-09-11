# Explorer Canvas Loading Overlay Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Explorer's fragmented loading indicators with one blocking, centered canvas overlay whose text follows each graph-loading phase.

**Architecture:** Promote the existing `#sgLoading` brand spinner to a class-driven full-canvas overlay and centralize its state in three private helpers in `query-sigmagraph.js`. Route Filter search, expansion, rendering, neighbor expansion, reload, focus loading, and inferred refresh through those helpers; user-facing failures continue through the shared notification/dialog APIs.

**Tech Stack:** Jinja HTML, Bootstrap 5, shared OntoBricks CSS tokens/components, browser JavaScript, pytest source-contract tests.

## Global Constraints

- Reuse `#sgLoading`, `.ob-loading-spinner`, and `.ob-spinner-svg`; add no spinner artwork or animation.
- Use `--db-*` tokens only; add no inline visual styles.
- Use typographic ellipses in progress labels.
- Keep the overlay below Bootstrap modals and hide it before seed selection.
- Remove all live and completed Filter status text below Search.
- Preserve search timing behavior and backend request contracts.
- Use version `0.9.0` and append the changelog to `changelogs/v0.9.0/benoitcayladbx_2026-09-11.log`.
- Run all Python commands through `uv run --frozen`.

---

### Task 1: Shared Overlay Structure and State API

**Files:**
- Create: `tests/units/front/test_explorer_loading_overlay.py`
- Modify: `src/front/templates/partials/dtwin/_query_sigmagraph.html:74-105,320-330`
- Modify: `src/front/static/query/css/query-sigmagraph.css:67-75,152-166`
- Modify: `src/front/static/query/js/query-sigmagraph.js:811-818`

**Interfaces:**
- Consumes: existing `#sgContainer`, `#sgLoading`, `.ob-loading-spinner`, and `[data-sg-action="executeGraphSearch"]`.
- Produces: `_showGraphLoading(label)`, `_setGraphLoadingStep(label)`, and `_hideGraphLoading()`; `#sgLoading.is-active`; `#sgLoadingLabel`; `#sgContainer[aria-busy]`.

- [ ] **Step 1: Write failing structure and helper contracts**

Create `tests/units/front/test_explorer_loading_overlay.py`:

```python
"""Contracts for Explorer's shared blocking canvas loading overlay."""

from pathlib import Path

import pytest


pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
PARTIAL = REPO_ROOT / "src/front/templates/partials/dtwin/_query_sigmagraph.html"
SIGMA_CSS = REPO_ROOT / "src/front/static/query/css/query-sigmagraph.css"
SIGMA_JS = REPO_ROOT / "src/front/static/query/js/query-sigmagraph.js"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_canvas_contains_one_accessible_shared_loading_overlay() -> None:
    html = _read(PARTIAL)
    assert 'id="sgContainer" aria-busy="false"' in html
    assert html.count('id="sgLoading"') == 1
    assert 'class="sg-loading-overlay"' in html
    assert 'id="sgLoadingLabel"' in html
    assert 'class="ob-loading-spinner"' in html
    assert 'class="ob-spinner-svg"' in html
    assert 'aria-live="polite"' in html


def test_obsolete_filter_and_neighbor_indicators_are_removed() -> None:
    html = _read(PARTIAL)
    assert 'id="sgGraphFilterInfo"' not in html
    assert 'id="sgGraphFilterInfoText"' not in html
    assert 'id="sgExpandSpinner"' not in html
    assert 'id="sgExpandSpinnerLabel"' not in html


def test_overlay_css_blocks_the_whole_canvas_with_shared_tokens() -> None:
    css = _read(SIGMA_CSS)
    block = css.split(".sg-loading-overlay", 1)[1].split(".sg-search-timing", 1)[0]
    assert "position: absolute;" in block
    assert "inset: 0;" in block
    assert "display: none;" in block
    assert "pointer-events: auto;" in block
    assert "var(--db-surface-warm)" in block
    assert ".sg-loading-overlay.is-active" in css
    assert ".sg-expand-spinner" not in css


def test_loading_helpers_own_overlay_accessibility_and_search_state() -> None:
    js = _read(SIGMA_JS)
    helper_block = js.split("function _showGraphLoading", 1)[1].split(
        "// -----------------------------------------------------------", 1
    )[0]
    assert "function _setGraphLoadingStep(label)" in js
    assert "function _hideGraphLoading()" in js
    assert "loading.classList.add('is-active');" in helper_block
    assert "loading.classList.remove('is-active');" in helper_block
    assert "container.setAttribute('aria-busy', 'true');" in helper_block
    assert "container.setAttribute('aria-busy', 'false');" in helper_block
    assert "searchButton.disabled = true;" in helper_block
    assert "searchButton.disabled = false;" in helper_block
    assert "loading.style.display" not in helper_block
```

- [ ] **Step 2: Run the new tests and verify RED**

Run:

```bash
uv run --frozen pytest -q tests/units/front/test_explorer_loading_overlay.py
```

Expected: failures for the absent `.sg-loading-overlay`, `#sgLoadingLabel`, and helper functions.

- [ ] **Step 3: Replace the template loading and status markup**

In `_query_sigmagraph.html`, make the canvas start as:

```html
<div id="sgContainer" aria-busy="false">
    <div id="sgLoading" class="sg-loading-overlay">
        <div class="ob-loading-spinner" role="status" aria-live="polite">
            <svg class="ob-spinner-svg" viewBox="0 0 80 80" fill="none" aria-hidden="true">
                <!-- Keep the existing SVG children verbatim. -->
            </svg>
            <span class="ob-spinner-label" id="sgLoadingLabel">Loading graph data…</span>
        </div>
    </div>
</div>
```

Delete the complete `#sgExpandSpinner` block. Add `id="sgGraphSearchBtn"` to the Search button:

```html
<button type="button" class="btn btn-sm btn-primary flex-grow-1"
        id="sgGraphSearchBtn" data-sg-action="executeGraphSearch">
    <i class="bi bi-search"></i> Search
</button>
```

Delete the complete `#sgGraphFilterInfo` block below the buttons.

- [ ] **Step 4: Add class-driven overlay styling**

Insert before `.sg-search-timing` in `query-sigmagraph.css`:

```css
.sg-loading-overlay {
    position: absolute;
    inset: 0;
    z-index: 20;
    display: none;
    align-items: center;
    justify-content: center;
    background: color-mix(in srgb, var(--db-surface-warm) 92%, transparent);
    pointer-events: auto;
}

.sg-loading-overlay.is-active {
    display: flex;
}
```

Delete the complete `.sg-expand-spinner` rule.

- [ ] **Step 5: Replace `_hideLoading` with the state helpers**

Add this implementation in the Render section of `query-sigmagraph.js`:

```javascript
function _setGraphLoadingStep(label) {
    var loadingLabel = document.getElementById('sgLoadingLabel');
    if (loadingLabel) loadingLabel.textContent = label;
}

function _showGraphLoading(label) {
    var loading = document.getElementById('sgLoading');
    var container = document.getElementById('sgContainer');
    var searchButton = document.getElementById('sgGraphSearchBtn');
    _setGraphLoadingStep(label || 'Loading graph data…');
    if (loading) loading.classList.add('is-active');
    if (container) container.setAttribute('aria-busy', 'true');
    if (searchButton) searchButton.disabled = true;
}

function _hideGraphLoading() {
    var loading = document.getElementById('sgLoading');
    var container = document.getElementById('sgContainer');
    var searchButton = document.getElementById('sgGraphSearchBtn');
    if (loading) loading.classList.remove('is-active');
    if (container) container.setAttribute('aria-busy', 'false');
    if (searchButton) searchButton.disabled = false;
}
```

Replace all existing `_hideLoading()` calls with `_hideGraphLoading()` and remove direct `loading.style.display` writes.

- [ ] **Step 6: Run Task 1 tests and existing timing tests**

Run:

```bash
uv run --frozen pytest -q \
  tests/units/front/test_explorer_loading_overlay.py \
  tests/units/front/test_explorer_search_timing.py
```

Expected: all tests pass.

- [ ] **Step 7: Commit Task 1**

```bash
git add \
  tests/units/front/test_explorer_loading_overlay.py \
  src/front/templates/partials/dtwin/_query_sigmagraph.html \
  src/front/static/query/css/query-sigmagraph.css \
  src/front/static/query/js/query-sigmagraph.js
git commit -m "feat(explorer): add shared canvas loading overlay"
```

---

### Task 2: Route Every Graph Load Through the Overlay

**Files:**
- Modify: `tests/units/front/test_explorer_loading_overlay.py`
- Modify: `src/front/static/query/js/query-sigmagraph.js:819-905,2058-2258,2572-2604,2664-2670,2719-2773,2956-3129`

**Interfaces:**
- Consumes: `_showGraphLoading(label)`, `_setGraphLoadingStep(label)`, `_hideGraphLoading()`.
- Produces: deterministic progress transitions and guaranteed cleanup for all Explorer load paths.

- [ ] **Step 1: Add failing progress-flow contracts**

Append:

```python
def test_filter_search_uses_preview_expand_and_render_steps() -> None:
    js = _read(SIGMA_JS)
    search = js.split("async function _executeGraphSearch()", 1)[1].split(
        "async function _expandAndRenderGraph", 1
    )[0]
    expand = js.split("async function _expandAndRenderGraph", 1)[1].split(
        "function _hideSeedPreviewModal", 1
    )[0]
    assert "_showGraphLoading('Searching…');" in search
    assert "_setGraphLoadingStep('1 entity found — expanding…');" in search
    assert "_hideGraphLoading();" in search[: search.index("modal.show();")]
    assert "'Expanding ' + uris.length + ' entities…'" in expand
    assert "_setGraphLoadingStep('Rendering graph…');" in expand


def test_render_reload_focus_and_inferred_refresh_use_overlay() -> None:
    js = _read(SIGMA_JS)
    render = js.split("function _render(", 1)[1].split(
        "// -----------------------------------------------------------", 1
    )[0]
    public_api = js.split("return {", 1)[1]
    assert "_setGraphLoadingStep('Rendering graph…');" in render
    assert "_hideGraphLoading();" in render
    assert "reload: function ()" in public_api
    assert "_showGraphLoading('Loading graph data…');" in public_api
    assert "_showGraphLoading('Loading graph data…');" in js.split(
        "function init(focusUri)", 1
    )[1].split("function _hasData", 1)[0]
    assert "_showGraphLoading('Loading graph data…');" in public_api.split(
        "toggleInferred: async function", 1
    )[1].split("expandHop: async function", 1)[0]


def test_neighbor_expansion_uses_overlay_and_error_notification() -> None:
    js = _read(SIGMA_JS)
    hop = js.split("expandHop: async function", 1)[1].split(
        "// --- Group expand / collapse ---", 1
    )[0]
    assert "'Expanding neighbours (' + depth + ' hop)…'" in hop
    assert "_setGraphLoadingStep('Rendering graph…');" in hop
    assert "finally {" in hop
    assert "_hideGraphLoading();" in hop.split("finally {", 1)[1]
    assert "'danger'" not in hop
    assert "'error'" in hop
    assert "sgExpandSpinner" not in hop
    assert "sgGraphFilterInfo" not in hop


def test_filter_status_dom_is_not_referenced() -> None:
    js = _read(SIGMA_JS)
    assert "sgGraphFilterInfo" not in js
    assert "sgGraphFilterInfoText" not in js
```

- [ ] **Step 2: Run the progress tests and verify RED**

Run:

```bash
uv run --frozen pytest -q tests/units/front/test_explorer_loading_overlay.py
```

Expected: failures because search and load paths still use direct status-region writes and the old neighbor spinner.

- [ ] **Step 3: Wire Filter preview and seed-modal transitions**

At Filter search start call:

```javascript
_showGraphLoading('Searching…');
```

For one result, update before `_expandAndRenderGraph`:

```javascript
_setGraphLoadingStep('1 entity found — expanding…');
```

For multiple results, hide immediately before opening the modal:

```javascript
_searchTimerPause();
_hideGraphLoading();
modal.show();
```

On failed/empty/caught preview responses, call `_hideGraphLoading()` before the existing dialog or a shared error notification. Remove all `info`/`text` variables and Filter status-region writes.

- [ ] **Step 4: Wire expansion and rendering**

At `_expandAndRenderGraph` entry:

```javascript
_showGraphLoading('Expanding ' + uris.length + ' entities…');
```

After graph libraries and `buildGraph` complete, immediately before `_render()`:

```javascript
_setGraphLoadingStep('Rendering graph…');
_render();
```

Use `_hideGraphLoading()` for unsuccessful, empty, library-failure, and exception branches. In `_render`, set `Rendering graph…` before graph construction and hide only after the Sigma renderer and handlers are created. Preserve the deferred-size path by keeping the overlay active until the deferred `_render` succeeds.

- [ ] **Step 5: Wire initial load, reload, focus, and inferred refresh**

Use this public-entry pattern:

```javascript
reload: function () {
    if (_hasData()) {
        _showGraphLoading('Loading graph data…');
        _render();
    } else {
        _showEmptyState();
    }
},
```

In `init(focusUri)`, call `_showGraphLoading('Loading graph data…')` before focus loading or rendering existing data. In `toggleInferred`, show `Loading graph data…` before either `_expandAndRenderGraph` or `loadTripleStore`, and put `_hideGraphLoading()` in a `finally` block so silent backend failures cannot strand the overlay.

- [ ] **Step 6: Replace neighbor expansion status with overlay phases**

At neighbor request start:

```javascript
_showGraphLoading('Expanding neighbours (' + depth + ' hop)…');
```

Before `buildGraph`:

```javascript
_setGraphLoadingStep('Rendering graph…');
```

Remove `info`, `text`, `spinner`, and `spinnerLabel` DOM handling. Keep information notifications for no new neighbors and use:

```javascript
showNotification(
    'Neighbour expansion failed: ' + (e && e.message ? e.message : e),
    'error'
);
```

The `finally` block becomes:

```javascript
} finally {
    _hideGraphLoading();
}
```

- [ ] **Step 7: Run focused tests**

Run:

```bash
uv run --frozen pytest -q \
  tests/units/front/test_explorer_loading_overlay.py \
  tests/units/front/test_explorer_search_timing.py
```

Expected: all tests pass.

- [ ] **Step 8: Commit Task 2**

```bash
git add \
  tests/units/front/test_explorer_loading_overlay.py \
  src/front/static/query/js/query-sigmagraph.js
git commit -m "feat(explorer): report graph loading phases"
```

---

### Task 3: Documentation, Browser Verification, and Regression Gate

**Files:**
- Modify: `docs/user-guide.md:664-690`
- Modify: `changelogs/v0.9.0/benoitcayladbx_2026-09-11.log`

**Interfaces:**
- Consumes: completed overlay behavior and focused test results.
- Produces: user documentation, changelog record, browser evidence, and full regression result.

- [ ] **Step 1: Document the Explorer loading behavior**

Add under `### Explorer (Sidebar)` in `docs/user-guide.md`:

```markdown
While Explorer searches, expands, reloads, or renders graph data, a centered
canvas overlay blocks graph interaction and names the current step. For searches
with multiple matches, the overlay closes while you select seed entities and
returns after you choose **Explore selected**.
```

- [ ] **Step 2: Browser-test desktop and mobile**

With the existing local server, verify `/dtwin/?section=sigmagraph` at desktop and mobile widths:

1. Filter Search shows `Searching…`, then expansion and rendering labels.
2. A multi-result search hides the overlay before the seed modal and restores it after **Explore selected**.
3. Reload, inferred toggle, and neighbor expansion use the same centered spinner.
4. Pointer input cannot reach the graph while active.
5. Search is disabled only while active.
6. Empty/error paths clear the overlay and report through dialogs/notifications.
7. Keyboard focus remains visible and the browser console has no new errors.

Expected: all seven checks pass.

- [ ] **Step 3: Run the full non-scenario suite**

Run:

```bash
uv run --frozen pytest -q -m "not scenario"
```

Expected: all tests pass; record the exact summary.

- [ ] **Step 4: Append the v0.9.0 changelog section**

Append:

```markdown
---

## Centralize Explorer graph loading feedback

Context: Knowledge Graph Explorer displayed Filter progress below the Search
button and neighbor expansion in a separate corner spinner. All graph loads now
use one blocking, centered canvas overlay whose label follows the active phase.

Changes:

1. src/front/templates/partials/dtwin/_query_sigmagraph.html
   Reuse the shared brand spinner as an accessible canvas overlay and remove
   duplicate status regions.
2. src/front/static/query/css/query-sigmagraph.css
   Add token-based blocking overlay geometry and remove the corner spinner.
3. src/front/static/query/js/query-sigmagraph.js
   Centralize loading state and route search, expansion, render, reload,
   inferred refresh, focus load, and neighbor expansion through phase labels.
4. tests/units/front/test_explorer_loading_overlay.py
   Verify overlay structure, accessibility, load-path coverage, and cleanup.
5. docs/user-guide.md
   Document centered phase feedback and the seed-selection transition.

Modified files:
- src/front/templates/partials/dtwin/_query_sigmagraph.html
- src/front/static/query/css/query-sigmagraph.css
- src/front/static/query/js/query-sigmagraph.js
- tests/units/front/test_explorer_loading_overlay.py
- docs/user-guide.md
- changelogs/v0.9.0/benoitcayladbx_2026-09-11.log
```

Immediately after the `Modified files` list, add a `Tests:` line containing
the command `uv run --frozen pytest -q -m "not scenario"` followed by the
exact pass, skip, deselection, xfail, warning, and duration summary emitted in
Step 3.

- [ ] **Step 5: Check lint diagnostics and final diff**

Check diagnostics for all modified HTML, CSS, JavaScript, Python, and Markdown files. Then run:

```bash
git diff --check
git status --short
```

Expected: no whitespace errors; only intended files plus pre-existing unrelated work are listed.

- [ ] **Step 6: Commit documentation and changelog**

```bash
git add \
  docs/user-guide.md \
  changelogs/v0.9.0/benoitcayladbx_2026-09-11.log
git commit -m "docs(explorer): describe canvas loading phases"
```
