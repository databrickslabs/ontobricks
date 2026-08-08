# Registry Stacked Modals Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Center the Registry Export and Import dialogs and blur/dim the Registry modal while either child dialog is open.

**Architecture:** Extract the confirmed stacked-modal lifecycle from `showConfirmDialog()` into one global `showStackedModal(modalEl)` helper. The helper owns Bootstrap instance creation, stack class application, backdrop tagging, cleanup, and display. Confirm, Export, and Import call that helper so all nested modals share one implementation and the existing CSS remains unchanged.

**Tech Stack:** JavaScript, Bootstrap 5 modal events, Jinja HTML templates, Python/pytest static frontend contract tests.

## Global Constraints

- Keep the existing `ob-modal-underlying`, `ob-modal-stacked`, and `ob-modal-stacked-backdrop` CSS rules as the visual source of truth.
- Keep Export and Import `modal-lg` and `modal-dialog-scrollable`.
- Use `bootstrap.Modal.getOrCreateInstance`; do not add modal-specific inline styles.
- Cleanup must be idempotent and run on `hidden.bs.modal`.
- Follow TDD: each production change follows a test that was observed failing for the intended reason.
- Run `uv run --frozen pytest -q -m "not scenario"` after all changes.
- Update `changelogs/v0.7.0/benoitcayladbx_2026-08-05.log`.
- Do not create a Git commit unless the user explicitly requests one.

---

## File Map

- `tests/units/front/test_stacked_confirm_modal.py`: shared stacking-helper and confirm-dialog integration contract.
- `src/front/static/global/js/utils.js`: reusable `showStackedModal(modalEl)` implementation and confirm-dialog caller.
- `tests/units/front/test_registry_stacked_obx_modals.py`: Export/Import centering and helper-use contract.
- `src/front/templates/partials/layout/registry_modal.html`: body-level sibling includes for Registry child modals.
- `src/front/templates/partials/registry/_registry_domains.html`: Registry Browse content without nested modal markup.
- `src/front/templates/partials/registry/_export_obx_modal.html`: centered Export dialog markup.
- `src/front/templates/partials/registry/_import_obx_modal.html`: centered Import dialog markup.
- `src/front/static/registry/js/registry.js`: Export/Import calls to the shared helper.
- `changelogs/v0.7.0/benoitcayladbx_2026-08-05.log`: versioned change record and final test result.

### Task 1: Extract the shared stacked-modal lifecycle

**Files:**
- Modify: `tests/units/front/test_stacked_confirm_modal.py`
- Modify: `src/front/static/global/js/utils.js:363-475`

**Interfaces:**
- Produces: `showStackedModal(modalEl: HTMLElement): bootstrap.Modal`
- Behavior: captures the topmost visible parent before showing `modalEl`, applies the three existing stack classes, removes them when hidden, shows the modal, and returns its Bootstrap instance.

- [ ] **Step 1: Rewrite the contract around a shared helper**

In `tests/units/front/test_stacked_confirm_modal.py`, add a named-function extractor:

```python
def _named_function_body(source: str, name: str) -> str:
    match = re.search(
        rf"function\s+{re.escape(name)}\s*\([^)]*\)\s*\{{",
        source,
    )
    if not match:
        return ""
    return _brace_block_after(source, match.end() - 1)


def _stack_helper_body() -> str:
    return _named_function_body(_utils(), "showStackedModal")
```

Replace the three confirm-specific stack lifecycle tests with:

```python
def test_shared_helper_detects_visible_parent_modal():
    body = _stack_helper_body()
    assert body, "showStackedModal helper must exist"
    assert ".modal.show" in body
    assert "ob-modal-underlying" in body


def test_shared_helper_marks_child_and_backdrop_stacked():
    body = _stack_helper_body()
    assert "ob-modal-stacked" in body
    assert "ob-modal-stacked-backdrop" in body


def test_shared_helper_cleans_stack_on_hidden():
    body = _stack_helper_body()
    assert "hidden.bs.modal" in body
    assert re.search(
        r"addEventListener\(\s*['\"]hidden\.bs\.modal['\"]\s*,\s*clearStack",
        body,
    ), "hidden.bs.modal must invoke clearStack"
    clear_body = _function_body(body, "clearStack")
    assert clear_body, "clearStack helper must be defined"
    assert _removals_cover_classes(clear_body, _STACK_CLASSES), (
        "clearStack must remove every stacked-modal class"
    )


def test_confirm_uses_shared_stacked_modal_helper():
    assert "showStackedModal(modalEl)" in _confirm_body()
```

Keep the existing CSS assertions unchanged. Remove `_function_body()` if no test uses it after this rewrite.

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
uv run --frozen pytest -q tests/units/front/test_stacked_confirm_modal.py
```

Expected: FAIL because `showStackedModal` does not exist and `showConfirmDialog` still contains the inline implementation.

- [ ] **Step 3: Add the shared helper**

Immediately before `showConfirmDialog()` in `src/front/static/global/js/utils.js`, add:

```javascript
/**
 * Show a Bootstrap modal above the currently visible modal, if any.
 * @param {HTMLElement} modalEl - Child modal element to display
 * @returns {bootstrap.Modal} Bootstrap modal instance
 */
function showStackedModal(modalEl) {
    const openModals = Array.from(document.querySelectorAll('.modal.show'))
        .filter((el) => el !== modalEl);
    const parentModal = openModals.length
        ? openModals[openModals.length - 1]
        : null;
    let stackApplied = false;

    const applyStack = () => {
        if (!parentModal || stackApplied) return;
        parentModal.classList.add('ob-modal-underlying');
        modalEl.classList.add('ob-modal-stacked');
        const backdrops = document.querySelectorAll('.modal-backdrop');
        const topBackdrop = backdrops.length
            ? backdrops[backdrops.length - 1]
            : null;
        topBackdrop?.classList.add('ob-modal-stacked-backdrop');
        stackApplied = true;
    };

    const clearStack = () => {
        if (!stackApplied) return;
        parentModal?.classList.remove('ob-modal-underlying');
        modalEl.classList.remove('ob-modal-stacked');
        document.querySelectorAll('.modal-backdrop.ob-modal-stacked-backdrop')
            .forEach((el) => el.classList.remove('ob-modal-stacked-backdrop'));
        stackApplied = false;
    };

    modalEl.addEventListener('shown.bs.modal', applyStack, { once: true });
    modalEl.addEventListener('hidden.bs.modal', clearStack, { once: true });

    const modal = bootstrap.Modal.getOrCreateInstance(modalEl);
    modal.show();
    applyStack();
    return modal;
}
```

In `showConfirmDialog()`, delete the inline `parentModal`, `stackApplied`, `applyStack`, and `clearStack` definitions. Delete its `shown.bs.modal` listener and the `clearStack()` call from its existing hidden listener. Replace the final instance creation/show/apply sequence with:

```javascript
        const modal = showStackedModal(modalEl);
```

Keep the confirm/cancel resolution and delayed DOM removal behavior unchanged.

- [ ] **Step 4: Run the focused tests and verify GREEN**

Run:

```bash
uv run --frozen pytest -q tests/units/front/test_stacked_confirm_modal.py
```

Expected: PASS, including the unchanged blur, reduced-motion, and z-index assertions.

### Task 2: Center and stack Registry Export and Import

**Files:**
- Create: `tests/units/front/test_registry_stacked_obx_modals.py`
- Modify: `src/front/templates/partials/layout/registry_modal.html:49`
- Modify: `src/front/templates/partials/registry/_registry_domains.html:31-32`
- Modify: `src/front/templates/partials/registry/_export_obx_modal.html:2`
- Modify: `src/front/templates/partials/registry/_import_obx_modal.html:2`
- Modify: `src/front/static/registry/js/registry.js:592-600,729-737`

**Interfaces:**
- Consumes: `showStackedModal(modalEl: HTMLElement): bootstrap.Modal` from Task 1.
- Produces: centered Export/Import template contracts and both Registry open paths routed through the shared lifecycle.

- [ ] **Step 1: Add failing Registry modal contracts**

Create `tests/units/front/test_registry_stacked_obx_modals.py`:

```python
"""Contracts for centered Registry child modals with stacked blur behavior."""

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
REGISTRY_JS = REPO_ROOT / "src/front/static/registry/js/registry.js"
REGISTRY_LAYOUT = (
    REPO_ROOT / "src/front/templates/partials/layout/registry_modal.html"
)
REGISTRY_DOMAINS = (
    REPO_ROOT / "src/front/templates/partials/registry/_registry_domains.html"
)
EXPORT_MODAL = (
    REPO_ROOT
    / "src/front/templates/partials/registry/_export_obx_modal.html"
)
IMPORT_MODAL = (
    REPO_ROOT
    / "src/front/templates/partials/registry/_import_obx_modal.html"
)


@pytest.mark.parametrize("template", [EXPORT_MODAL, IMPORT_MODAL])
def test_registry_child_modal_is_centered(template: Path):
    markup = template.read_text(encoding="utf-8")
    assert "modal-dialog-centered" in markup


def test_registry_child_modals_are_siblings_not_nested():
    layout = REGISTRY_LAYOUT.read_text(encoding="utf-8")
    domains = REGISTRY_DOMAINS.read_text(encoding="utf-8")
    registry_close = layout.rfind("</div>")
    for modal_name in ("_export_obx_modal.html", "_import_obx_modal.html"):
        assert modal_name not in domains
        assert layout.index(modal_name) > registry_close


def test_export_uses_shared_stacked_modal_helper():
    source = REGISTRY_JS.read_text(encoding="utf-8")
    start = source.index("async function openExportObxModal")
    end = source.index("btnExportObxConfirm", start)
    assert "showStackedModal(modalEl)" in source[start:end]


def test_import_uses_shared_stacked_modal_helper():
    source = REGISTRY_JS.read_text(encoding="utf-8")
    start = source.index("function openImportObxModal")
    end = source.index(
        "document.getElementById('importObxFile')?.addEventListener",
        start,
    )
    assert "showStackedModal(modalEl)" in source[start:end]
```

- [ ] **Step 2: Run the new tests and verify RED**

Run:

```bash
uv run --frozen pytest -q tests/units/front/test_registry_stacked_obx_modals.py
```

Expected: 5 failures: both templates lack `modal-dialog-centered`, both JS
open paths lack `showStackedModal(modalEl)`, and the child modal includes are
nested inside Registry.

- [ ] **Step 3: Render child modals as Registry siblings**

Remove the Export and Import includes from
`src/front/templates/partials/registry/_registry_domains.html`. Add them after
the closing `#registryModal` element in
`src/front/templates/partials/layout/registry_modal.html`:

```jinja
{% include 'partials/registry/_export_obx_modal.html' %}
{% include 'partials/registry/_import_obx_modal.html' %}
```

This keeps the active child outside the blurred parent's DOM and stacking
context.

- [ ] **Step 4: Center both child dialogs**

In both Registry modal templates, change the dialog line to:

```html
  <div class="modal-dialog modal-lg modal-dialog-scrollable modal-dialog-centered">
```

- [ ] **Step 5: Route both open paths through the shared helper**

In `openExportObxModal()`, replace:

```javascript
        const modal = bootstrap.Modal.getOrCreateInstance(modalEl);
        modal.show();
```

with:

```javascript
        showStackedModal(modalEl);
```

In `openImportObxModal()`, replace:

```javascript
        bootstrap.Modal.getOrCreateInstance(modalEl).show();
```

with:

```javascript
        showStackedModal(modalEl);
```

- [ ] **Step 6: Run the Registry and shared modal tests**

Run:

```bash
uv run --frozen pytest -q \
  tests/units/front/test_registry_stacked_obx_modals.py \
  tests/units/front/test_stacked_confirm_modal.py
```

Expected: PASS.

- [ ] **Step 7: Verify the running UI**

Using the already-running local app:

1. Open Registry.
2. Open Export and confirm it is centered.
3. Confirm Registry content is blurred/dimmed and Export remains sharp.
4. Close Export and confirm Registry is restored.
5. Repeat all checks for Import.
6. Reopen each child once to verify one-shot listeners are registered correctly on repeated opens.

Expected: both child dialogs center and stack identically; no stale blur, backdrop, or stacking class remains after either closes.

### Task 3: Record and verify the complete change

**Files:**
- Modify: `changelogs/v0.7.0/benoitcayladbx_2026-08-05.log`
- Verify: all files changed by Tasks 1 and 2

**Interfaces:**
- Consumes: completed implementation and observed test output.
- Produces: required v0.7.0 changelog entry with exact test results.

- [ ] **Step 1: Check diagnostics on edited source and test files**

Read IDE diagnostics for:

```text
src/front/static/global/js/utils.js
src/front/static/registry/js/registry.js
src/front/templates/partials/registry/_export_obx_modal.html
src/front/templates/partials/registry/_import_obx_modal.html
tests/units/front/test_stacked_confirm_modal.py
tests/units/front/test_registry_stacked_obx_modals.py
```

Expected: no newly introduced errors.

- [ ] **Step 2: Run the required full non-scenario suite**

Run:

```bash
uv run --frozen pytest -q -m "not scenario"
```

Expected: PASS. Record the exact passed/skipped/deselected/xfailed counts, warnings, and elapsed time.

- [ ] **Step 3: Append the changelog section**

Append a section titled `## Center and blur Registry child modals` to
`changelogs/v0.7.0/benoitcayladbx_2026-08-05.log` containing:

```text
Context: Registry Export and Import opened off-center and did not visually
separate themselves from the Registry modal behind them.

Changes:

1. `src/front/static/global/js/utils.js` — extracted the existing confirmation
   stacking lifecycle into reusable `showStackedModal`, preserving cleanup.
2. `src/front/static/registry/js/registry.js` — routed Registry Export and
   Import through the shared stacked-modal helper.
3. `src/front/templates/partials/layout/registry_modal.html` and
   `partials/registry/_registry_domains.html` — rendered the child modals as
   siblings of Registry so they remain sharp and clickable.
4. `src/front/templates/partials/registry/_export_obx_modal.html` and
   `_import_obx_modal.html` — centered both child dialogs.
5. `tests/units/front/test_stacked_confirm_modal.py` and
   `test_registry_stacked_obx_modals.py` — added shared lifecycle, centering,
   integration, and cleanup regression contracts.

Modified files:
- `src/front/static/global/js/utils.js`
- `src/front/static/registry/js/registry.js`
- `src/front/templates/partials/layout/registry_modal.html`
- `src/front/templates/partials/registry/_registry_domains.html`
- `src/front/templates/partials/registry/_export_obx_modal.html`
- `src/front/templates/partials/registry/_import_obx_modal.html`
- `tests/units/front/test_stacked_confirm_modal.py`
- `tests/units/front/test_registry_stacked_obx_modals.py` (new)
- `documentation/superpowers/specs/2026-08-05-registry-stacked-modals-design.md` (new)
- `documentation/superpowers/plans/2026-08-05-registry-stacked-modals.md` (new)

```

After the code block, add a `Tests:` line containing the command and the exact
pytest summary observed in Step 2, including counts, warnings, and elapsed time.

- [ ] **Step 4: Re-run the focused contracts after the changelog edit**

Run:

```bash
uv run --frozen pytest -q \
  tests/units/front/test_registry_stacked_obx_modals.py \
  tests/units/front/test_stacked_confirm_modal.py
```

Expected: PASS.
