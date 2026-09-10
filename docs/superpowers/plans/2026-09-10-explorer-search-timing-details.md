# Explorer Search Timing Details Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Explorer duration indicator clickable and show the latest search's Preview, Expansion, Display, and Total timings.

**Architecture:** Extend the existing browser stopwatch in `query-sigmagraph.js` with one latest-only timing record. Record Preview and Expansion around their API request/parse boundaries, record Display around graph preparation/render, then render the completed record through an accessible canvas-anchored button and details panel styled in `query-sigmagraph.css`.

**Tech Stack:** Vanilla JavaScript, Bootstrap Icons, CSS with OntoBricks `--db-*` tokens, pytest structural contracts.

## Global Constraints

- Work only in the `0.9.0` worktree.
- Retain only the latest successfully displayed search; do not add history or persistence.
- Report browser-observed API phases, not internal SQL statements.
- Total excludes seed-selection dwell using the existing pause/resume stopwatch.
- No inline visual styles, native browser popups, new dependencies, or backend API changes.
- The details panel must support native button activation, Escape, outside-click dismissal, and mobile containment.

---

### Task 1: Collect the latest search timing record

**Files:**
- Create: `tests/units/front/test_explorer_search_timing.py`
- Modify: `src/front/static/query/js/query-sigmagraph.js:37-70`
- Modify: `src/front/static/query/js/query-sigmagraph.js:960-995`
- Modify: `src/front/static/query/js/query-sigmagraph.js:1966-2068`
- Modify: `src/front/static/query/js/query-sigmagraph.js:2071-2159`

**Interfaces:**
- Produces: `_searchTiming`, an object with nullable `previewMs`, `expansionMs`, `displayMs`, and `totalMs`.
- Produces: `_resetSearchTiming()` to clear active phase values and close stale details.
- Consumes: existing `_searchTimerStart`, `_searchTimerPause`, `_searchTimerResume`, and `_searchTimerStop`.

- [ ] **Step 1: Write failing phase-timing contracts**

Create `tests/units/front/test_explorer_search_timing.py`:

```python
"""Contracts for Explorer's latest-search timing breakdown."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SIGMA_JS = REPO_ROOT / "src/front/static/query/js/query-sigmagraph.js"
SIGMA_CSS = REPO_ROOT / "src/front/static/query/css/query-sigmagraph.css"


def _js() -> str:
    return SIGMA_JS.read_text(encoding="utf-8")


def test_latest_timing_record_has_all_phases() -> None:
    js = _js()
    assert "var _searchTiming = {" in js
    assert "previewMs: null" in js
    assert "expansionMs: null" in js
    assert "displayMs: null" in js
    assert "totalMs: null" in js
    assert "function _resetSearchTiming()" in js


def test_preview_and_expansion_measure_request_through_parse() -> None:
    js = _js()
    assert "var previewStartedAt = performance.now();" in js
    assert "_searchTiming.previewMs = performance.now() - previewStartedAt;" in js
    assert "var expansionStartedAt = performance.now();" in js
    assert "_searchTiming.expansionMs = performance.now() - expansionStartedAt;" in js


def test_display_and_total_finish_after_render() -> None:
    js = _js()
    render_call = js.index("_render();", js.index("async function _expandAndRenderGraph"))
    display_done = js.index(
        "_searchTiming.displayMs = performance.now() - displayStartedAt;",
        render_call,
    )
    total_done = js.index("_searchTiming.totalMs = _searchTimerStop();", display_done)
    assert render_call < display_done < total_done
    assert "_showSearchTiming(_searchTiming);" in js[total_done:]


def test_starting_search_resets_latest_details() -> None:
    js = _js()
    start = js.index("async function _executeGraphSearch()")
    timer = js.index("_searchTimerStart();", start)
    assert "_resetSearchTiming();" in js[start:timer]
```

- [ ] **Step 2: Run the focused tests and confirm RED**

Run:

```bash
uv run --frozen pytest -q tests/units/front/test_explorer_search_timing.py
```

Expected: failures for the missing timing record and phase assignments.

- [ ] **Step 3: Add the timing state and phase boundaries**

In `query-sigmagraph.js`, add the latest-only record beside the existing
stopwatch state:

```javascript
var _searchTiming = {
    previewMs: null,
    expansionMs: null,
    displayMs: null,
    totalMs: null
};

function _resetSearchTiming() {
    _searchTiming = {
        previewMs: null,
        expansionMs: null,
        displayMs: null,
        totalMs: null
    };
    _setSearchTimingExpanded(false);
}
```

At the beginning of `_executeGraphSearch`, reset details before starting the
existing total stopwatch:

```javascript
_resetSearchTiming();
_searchTimerStart();
```

Measure Preview from immediately before `_fetchWithTimeout` through the parsed
response:

```javascript
var previewStartedAt = performance.now();
var resp = await _fetchWithTimeout(/* existing arguments */);
var data = await _parseJsonResponse(resp, 'Search request failed');
_searchTiming.previewMs = performance.now() - previewStartedAt;
```

In `_expandAndRenderGraph`, measure Expansion around the existing request and
parse:

```javascript
var expansionStartedAt = performance.now();
var resp = await _fetchWithTimeout(/* existing arguments */);
var data = await _parseJsonResponse(resp, 'Graph expansion failed');
_searchTiming.expansionMs = performance.now() - expansionStartedAt;
```

After validating that expansion returned results, start Display before waiting
for graph libraries/building graph. Finish it immediately after `_render()`:

```javascript
var displayStartedAt = performance.now();
// existing _waitForGraphLibs, buildGraph, state reset
_render();

if (_searchTimerPending) {
    _searchTiming.displayMs = performance.now() - displayStartedAt;
    _searchTiming.totalMs = _searchTimerStop();
    _searchTimerPending = false;
    _showSearchTiming(_searchTiming);
}
```

Remove the old timing-finalization block from `_render()` so non-search renders
cannot publish or replace timing details.

- [ ] **Step 4: Run phase tests and confirm GREEN**

Run:

```bash
uv run --frozen pytest -q tests/units/front/test_explorer_search_timing.py
```

Expected: all phase timing contracts pass.

---

### Task 2: Add the accessible timing button and details panel

**Files:**
- Modify: `tests/units/front/test_explorer_search_timing.py`
- Modify: `src/front/static/query/js/query-sigmagraph.js:970-995`
- Modify: `src/front/static/query/css/query-sigmagraph.css:68-75`

**Interfaces:**
- Produces: `_showSearchTiming(timing)` to render the latest complete record.
- Produces: `_setSearchTimingExpanded(expanded)` to synchronize visibility and `aria-expanded`.
- Produces DOM IDs: `sgSearchTiming` and `sgSearchTimingDetails`.

- [ ] **Step 1: Add failing interaction and styling contracts**

Append:

```python
def test_timing_indicator_is_an_accessible_button() -> None:
    js = _js()
    assert "document.createElement('button')" in js
    assert "el.type = 'button';" in js
    assert "el.setAttribute('aria-controls', 'sgSearchTimingDetails');" in js
    assert "el.setAttribute('aria-expanded', 'false');" in js
    assert "function _setSearchTimingExpanded(expanded)" in js


def test_details_support_click_escape_and_outside_dismissal() -> None:
    js = _js()
    assert "el.addEventListener('click'" in js
    assert "event.key === 'Escape'" in js
    assert "container.contains(event.target)" in js


def test_indicator_uses_classes_instead_of_inline_visual_styles() -> None:
    js = _js()
    css = SIGMA_CSS.read_text(encoding="utf-8")
    show_block = js.split("function _showSearchTiming", 1)[1].split(
        "// -----------------------------------------------------------", 1
    )[0]
    assert "style.cssText" not in show_block
    assert "getComputedStyle" not in show_block
    assert ".sg-search-timing" in css
    assert ".sg-search-timing-details" in css


def test_details_render_latest_phase_labels() -> None:
    js = _js()
    for label in ("Preview request", "Expansion request", "Display", "Total"):
        assert label in js
    assert "aria-label" in js
```

- [ ] **Step 2: Run interaction tests and confirm RED**

Run:

```bash
uv run --frozen pytest -q tests/units/front/test_explorer_search_timing.py
```

Expected: phase tests pass; new DOM/CSS contracts fail.

- [ ] **Step 3: Replace the generated readout with button and panel**

Implement `_showSearchTiming(timing)` so it:

1. creates `button#sgSearchTiming.sg-search-timing`;
2. creates `div#sgSearchTimingDetails.sg-search-timing-details` with
   `role="region"` and `hidden`;
3. formats non-negative finite milliseconds to seconds with two decimals,
   otherwise `—`;
4. updates the button icon/text and accessible label;
5. renders a definition list containing Preview request, Expansion request,
   Display, and Total;
6. toggles through `_setSearchTimingExpanded`;
7. stops button click propagation;
8. closes on document outside-click or Escape.

Use escaped static labels and `textContent`/DOM nodes for timing values; do not
interpolate untrusted data with `innerHTML`.

- [ ] **Step 4: Add token-based canvas styles**

Add to `query-sigmagraph.css`:

```css
.sg-search-timing {
    position: absolute;
    left: 0.5rem;
    bottom: 0.5rem;
    z-index: 5;
    padding: 0.3rem 0.55rem;
    border: 1px solid var(--db-border);
    border-radius: var(--db-radius-control);
    background: var(--db-text);
    color: var(--db-surface-warm);
    font: 600 0.7rem/1 var(--bs-font-monospace);
    white-space: nowrap;
}

.sg-search-timing:focus-visible {
    outline: 2px solid transparent;
    box-shadow: var(--db-focus-ring);
}

.sg-search-timing-details {
    position: absolute;
    left: 0.5rem;
    bottom: 2.6rem;
    z-index: 6;
    width: min(15rem, calc(100% - 1rem));
    padding: 0.75rem;
    border: 1px solid var(--db-border);
    border-radius: var(--db-radius-card);
    background: var(--db-surface-warm);
    color: var(--db-text);
    box-shadow: var(--db-shadow-md);
}

.sg-search-timing-details[hidden] {
    display: none;
}

.sg-search-timing-details dl {
    display: grid;
    grid-template-columns: 1fr auto;
    gap: 0.4rem 0.75rem;
    margin: 0;
    font-size: 0.75rem;
}

.sg-search-timing-details dt,
.sg-search-timing-details dd {
    margin: 0;
}

.sg-search-timing-details dd {
    font-family: var(--bs-font-monospace);
}
```

- [ ] **Step 5: Run focused Explorer contracts**

Run:

```bash
uv run --frozen pytest -q \
  tests/units/front/test_explorer_search_timing.py \
  tests/units/ui/test_home_and_explorer_contracts.py
```

Expected: all tests pass.

---

### Task 3: Document and verify the feature

**Files:**
- Modify: `docs/get-started.md:360-380`
- Modify: `changelogs/v0.9.0/benoitcayladbx_2026-09-10.log`

**Interfaces:**
- Consumes: the completed timing button and details panel from Tasks 1–2.
- Produces: user documentation and verification evidence.

- [ ] **Step 1: Document the timing details**

Add after the Knowledge Graph navigation description:

```markdown
In **Knowledge Graph → Explorer**, the bottom-left stopwatch reports the latest
search's total browser-observed response time. Click it to see Preview request,
Expansion request, Display, and Total. Time spent choosing seeds is excluded
from Total.
```

- [ ] **Step 2: Browser-test desktop and mobile**

Start the existing local application if necessary, then verify Explorer at
desktop and a width below `768px`:

- complete a single-result search and a multi-result search;
- confirm selection dwell does not increase Total;
- open with click and Enter/Space;
- dismiss with Escape and an outside click;
- confirm the panel stays inside the canvas;
- confirm canvas pan/zoom and entity selection still work;
- confirm no console or network errors.

- [ ] **Step 3: Run lints and focused tests**

Read IDE diagnostics for:

```text
src/front/static/query/js/query-sigmagraph.js
src/front/static/query/css/query-sigmagraph.css
tests/units/front/test_explorer_search_timing.py
```

Run:

```bash
uv run --frozen pytest -q \
  tests/units/front/test_explorer_search_timing.py \
  tests/units/ui/test_home_and_explorer_contracts.py
```

Expected: no introduced diagnostics and all focused tests pass.

- [ ] **Step 4: Run the complete non-scenario suite**

Run:

```bash
env -u APP_NAME -u MCP_APP_NAME -u DATABRICKS_CONFIG_PROFILE \
  uv run --frozen pytest -q -m "not scenario"
```

Expected: zero failures.

- [ ] **Step 5: Append the changelog section**

Record:

- the clickable latest-search timing breakdown;
- JavaScript timing state and accessible interaction;
- token-based CSS;
- documentation;
- focused browser verification;
- the exact full-suite summary.
