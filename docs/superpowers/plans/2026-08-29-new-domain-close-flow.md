# New Domain Close-First Flow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close any loaded domain—with save/discard/cancel handling—before opening the New Domain dialog.

**Architecture:** Keep the workflow in `navbar.js`, where both domain actions already live. Extract one close helper that returns a boolean and accepts a navigation option; make both `domainClose()` and `domainNew()` use it so save, lock release, errors, and cancellation cannot drift.

**Tech Stack:** Browser JavaScript, Bootstrap 5 modal APIs, centralized permissions CSS, pytest source-contract tests.

## Global Constraints

- In view mode, closing offers only **Close without saving** and **Cancel**.
- A canceled or failed close must leave the current domain open and abort New Domain.
- The regular Close action must still navigate to `/` after a successful close.
- New Domain must close in place and show its dialog only after the close succeeds.
- All frontend feedback must use `showNotification`.
- Run pytest only through `uv run --frozen`; scenario tests remain excluded.
- Do not create a git commit unless the user explicitly requests one.

---

### Task 1: Protect the New Domain fields from stale read-only presentation

**Files:**
- Modify: `src/front/static/global/js/utils.js:840-970`
- Modify: `src/front/static/global/css/permissions.css:241-302`
- Test: `tests/units/front/test_new_domain_name_validation.py`

**Interfaces:**
- Consumes: the `body.read-only-locked`, `body.read-only-version`, and `body.role-viewer` CSS states.
- Produces: the `.new-domain-field` marker on exactly three dialog controls.

- [ ] **Step 1: Keep the regression contract**

```python
def test_new_domain_fields_remain_editable_while_loaded_domain_is_read_only():
    dialog = _source().split("function showNewDomainDialog", maxsplit=1)[1]
    dialog = dialog.split("window.showNewDomainDialog", maxsplit=1)[0]
    assert dialog.count("new-domain-field") == 3

    css = PERMISSIONS_CSS.read_text(encoding="utf-8")
    rule_start = css.index(
        "body:is(.read-only-version, .role-viewer, .read-only-locked)"
        ':not([data-page="digitaltwin"]):not([data-page="settings"]) input'
    )
    generic_field_rule = css[rule_start : css.index("}", rule_start)]
    assert generic_field_rule.count(":not(.new-domain-field)") == 3
```

- [ ] **Step 2: Verify the focused contract**

Run:

```bash
uv run --frozen pytest -q tests/units/front/test_new_domain_name_validation.py
```

Expected: `2 passed`; this task is already green in the working tree from the
initial bug reproduction.

---

### Task 2: Add close-first workflow contracts

**Files:**
- Create: `tests/units/front/test_new_domain_close_flow.py`
- Test: `tests/units/front/test_new_domain_close_flow.py`

**Interfaces:**
- Consumes: source functions in `src/front/static/global/js/navbar.js`.
- Produces: regression contracts for `hasLoadedDomain()`,
  `canSaveBeforeClose()`, `closeCurrentDomain(options)`,
  `showCloseDomainDialog(options)`, `domainNew()`, and `domainClose()`.

- [ ] **Step 1: Write failing source-contract tests**

```python
"""Contracts for closing the current domain before creating another."""

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
NAVBAR_JS = REPO_ROOT / "src/front/static/global/js/navbar.js"

pytestmark = pytest.mark.unit


def _source() -> str:
    return NAVBAR_JS.read_text(encoding="utf-8")


def _function(name: str, next_name: str) -> str:
    source = _source()
    return source.split(f"async function {name}", maxsplit=1)[1].split(
        f"async function {next_name}", maxsplit=1
    )[0]


def test_new_domain_closes_loaded_domain_before_opening_dialog():
    body = _function("domainNew()", "domainSave()")
    close_pos = body.index("await closeCurrentDomain({ navigate: false })")
    dialog_pos = body.index("await showNewDomainDialog()")
    assert close_pos < dialog_pos


def test_new_domain_aborts_when_close_does_not_complete():
    body = _function("domainNew()", "domainSave()")
    assert "if (!closed) return;" in body


def test_close_helper_saves_only_when_allowed_and_requested():
    body = _function("closeCurrentDomain(options = {})", "domainClose()")
    assert "showCloseDomainDialog({ allowSave })" in body
    assert "choice === 'save' && allowSave" in body
    assert "return closeDomainSession({ navigate });" in body


def test_regular_close_keeps_home_navigation():
    body = _function("domainClose()", "closeDomainSession(options = {})")
    assert "closeCurrentDomain({ navigate: true })" in body


def test_close_dialog_omits_save_action_when_saving_is_unavailable():
    source = _source()
    body = source.split("function showCloseDomainDialog(options = {})", 1)[1]
    body = body.split("// ==========================================", 1)[0]
    assert "allowSave ? " in body
    assert "const saveBtn = document.getElementById('closeSaveBtn')" in body
    assert "if (saveBtn)" in body
```

- [ ] **Step 2: Run the tests and verify RED**

Run:

```bash
uv run --frozen pytest -q tests/units/front/test_new_domain_close_flow.py
```

Expected: failures because the shared close helper and configurable close modal
do not exist and `domainNew()` still opens the details dialog first.

---

### Task 3: Implement the shared close workflow

**Files:**
- Modify: `src/front/static/global/js/navbar.js:218-251`
- Modify: `src/front/static/global/js/navbar.js:416-484`
- Modify: `src/front/static/global/js/navbar.js:733-832`
- Test: `tests/units/front/test_new_domain_close_flow.py`

**Interfaces:**
- Consumes:
  - `fetchCached('/navbar/state', 15000)` for current domain state.
  - `window.OB.canEditOntology()` when available.
  - `isSwitchSaveAllowed()` and `window.editLockMode` as fallback guards.
- Produces:
  - `hasLoadedDomain(): Promise<boolean>`.
  - `canSaveBeforeClose(): boolean`.
  - `closeCurrentDomain({navigate: boolean}): Promise<boolean>`.
  - `closeDomainSession({navigate: boolean}): Promise<boolean>`.

- [ ] **Step 1: Extract the loaded-domain predicate**

Add a pure helper and reuse it from `applyDomainInfo`:

```javascript
function domainIsLoaded(data) {
    const stats = data.stats || {};
    const hasContent = (stats.entities > 0) || (stats.entity_mappings > 0);
    const name = data.info && data.info.name;
    const hasCustomName = name && name !== 'NewProject' && name !== 'NewDomain';
    return Boolean(hasCustomName || hasContent);
}

async function hasLoadedDomain() {
    const state = await fetchCached('/navbar/state', 15000);
    return domainIsLoaded(state.domain || {});
}
```

In `applyDomainInfo(data)`, replace its duplicated `hasContent` /
`hasCustomName` calculation with:

```javascript
const hasDomain = domainIsLoaded(data);
```

- [ ] **Step 2: Make save availability explicit**

```javascript
function canSaveBeforeClose() {
    if (window.OB && typeof window.OB.canEditOntology === 'function') {
        return window.OB.canEditOntology();
    }
    return isSwitchSaveAllowed() && window.editLockMode !== 'view';
}
```

- [ ] **Step 3: Add the shared close helper**

```javascript
async function closeCurrentDomain(options = {}) {
    const { navigate = false } = options;
    let loaded;
    try {
        loaded = await hasLoadedDomain();
    } catch (error) {
        showNotification('Could not determine whether a domain is open.', 'error');
        return false;
    }
    if (!loaded) return true;

    const allowSave = canSaveBeforeClose();
    const choice = await showCloseDomainDialog({ allowSave });
    if (!choice || choice === 'cancel') return false;

    if (choice === 'save' && allowSave) {
        await saveDomainInfoBeforeSave();
        const saved = await doDomainSave({ afterSave: null });
        if (!saved) return false;
    }

    return closeDomainSession({ navigate });
}
```

- [ ] **Step 4: Put close-first ordering in `domainNew()`**

```javascript
async function domainNew() {
    const closed = await closeCurrentDomain({ navigate: false });
    if (!closed) return;

    const input = await showNewDomainDialog();
    if (!input) return;

    try {
        const payload = { name: input.name };
        if (input.description) payload.description = input.description;
        if (input.llm_endpoint) payload.llm_endpoint = input.llm_endpoint;
        await fetch('/domain/info', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
            credentials: 'same-origin'
        });

        invalidateDomainCaches();

        try {
            const nameEl = document.getElementById('domainName');
            if (nameEl) {
                nameEl.value = input.name;
                nameEl.classList.remove('is-invalid');
            }
            const dupHint = document.getElementById('domainNameDuplicateHint');
            if (dupHint) dupHint.remove();
            const descEl = document.getElementById('domainDescription');
            if (descEl) descEl.value = input.description || '';
            const llmEl = document.getElementById('domainLlmEndpoint');
            if (llmEl && input.llm_endpoint) llmEl.value = input.llm_endpoint;
            if (typeof updateAutoBaseUri === 'function') updateAutoBaseUri();
        } catch (error) {
            // Information form is not present on every page.
        }

        const saved = await doDomainSave({ afterSave: '/domain/#information' });
        if (!saved) return;
    } catch (error) {
        console.error('Error creating new domain:', error);
        showNotification('Failed to create new domain: ' + error.message, 'error');
    }
}
```

Delete the old `POST /domain/close` block from inside the `try` because the
shared helper has already completed it before the dialog opens.

- [ ] **Step 5: Rewire the regular Close action**

```javascript
async function domainClose() {
    await closeCurrentDomain({ navigate: true });
}
```

- [ ] **Step 6: Make `closeDomainSession` report success and navigate optionally**

```javascript
async function closeDomainSession(options = {}) {
    const { navigate = true } = options;
    showDomainLoading('Closing domain...');
    try {
        const response = await fetch('/domain/close', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin'
        });
        const data = await response.json();
        if (!response.ok || data.success === false) {
            throw new Error(data.message || 'Could not close domain');
        }
        invalidateDomainCaches();
        if (navigate) {
            window.location.href = '/';
        } else {
            hideDomainLoading();
        }
        return true;
    } catch (error) {
        hideDomainLoading();
        showNotification('Failed to close domain: ' + error.message, 'error');
        return false;
    }
}
```

- [ ] **Step 7: Make the close dialog configurable**

Change the signature and save-button markup:

```javascript
function showCloseDomainDialog(options = {}) {
    const { allowSave = true } = options;
    // ...
    const saveAction = allowSave
        ? `<button type="button" class="btn btn-primary" id="closeSaveBtn">
               <i class="bi bi-cloud-upload"></i> Save &amp; Close
           </button>`
        : '';
    // Insert ${saveAction} after Close without saving.
    const saveBtn = document.getElementById('closeSaveBtn');
    if (saveBtn) {
        saveBtn.addEventListener('click', () => finish('save'));
    }
}
```

When `allowSave` is false, render helper copy stating that the current browser
has read-only access and cannot save.

- [ ] **Step 8: Run the focused tests and verify GREEN**

Run:

```bash
uv run --frozen pytest -q \
  tests/units/front/test_new_domain_close_flow.py \
  tests/units/front/test_new_domain_name_validation.py \
  tests/units/front/test_switch_domain_modal.py
```

Expected: all tests pass.

---

### Task 4: Update records and run the full regression gate

**Files:**
- Modify: `changelogs/v0.8.0/benoitcayladbx_2026-08-29.log`
- Verify: `documentation/superpowers/specs/2026-08-29-new-domain-close-flow-design.md`
- Verify: all files changed by Tasks 1–3.

**Interfaces:**
- Consumes: final test output and modified-file list.
- Produces: the mandatory English changelog entry and verification evidence.

- [ ] **Step 1: Check edited-file diagnostics**

Run the IDE linter check against:

```text
src/front/static/global/js/navbar.js
src/front/static/global/js/utils.js
src/front/static/global/css/permissions.css
tests/units/front/test_new_domain_close_flow.py
tests/units/front/test_new_domain_name_validation.py
```

Expected: no new diagnostics.

- [ ] **Step 2: Run the routine suite**

```bash
uv run --frozen pytest -q -m "not scenario"
```

Expected: zero failures.

- [ ] **Step 3: Write the changelog section**

Use this structure and append the full final pytest summary after the arrow:

```text
## Close the loaded domain before creating another

Context: New Domain previously collected details before silently closing the
loaded domain, and view-mode CSS disabled the dialog fields. The workflow now
resolves save, discard, or cancellation before showing the creation dialog.

Changes:

1. src/front/static/global/js/navbar.js
   Share the close workflow between Close and New Domain, with view-mode save
   gating and optional post-close navigation.
2. src/front/static/global/js/utils.js
   Mark New Domain fields as independent from the loaded domain's read-only state.
3. src/front/static/global/css/permissions.css
   Exempt the New Domain controls from stale loaded-domain field lockdown.
4. tests/units/front/test_new_domain_close_flow.py
   Cover close-first ordering, cancellation, view-mode save gating, and navigation.
5. tests/units/front/test_new_domain_name_validation.py
   Protect the three New Domain field exemptions.
6. documentation/superpowers/specs/2026-08-29-new-domain-close-flow-design.md
   Document the approved close-first workflow and failure behavior.
7. docs/superpowers/plans/2026-08-29-new-domain-close-flow.md
   Record the TDD implementation and verification plan.

Modified files:
- src/front/static/global/js/navbar.js
- src/front/static/global/js/utils.js
- src/front/static/global/css/permissions.css
- tests/units/front/test_new_domain_close_flow.py
- tests/units/front/test_new_domain_name_validation.py
- documentation/superpowers/specs/2026-08-29-new-domain-close-flow-design.md
- docs/superpowers/plans/2026-08-29-new-domain-close-flow.md

Tests: uv run --frozen pytest -q -m "not scenario" → zero failures; record the final pass, skip, deselect, xfail, warning, and duration counts.
```

- [ ] **Step 4: Review the final diff**

Confirm that:

- `domainNew()` contains no second direct `/domain/close` request;
- only the shared helper decides whether saving is offered;
- errors never proceed to the New Domain dialog;
- no scenario suite or deployment was run;
- no commit was created without an explicit user request.
