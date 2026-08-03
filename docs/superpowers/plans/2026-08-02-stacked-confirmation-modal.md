# Stacked Confirmation Modal — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When `showConfirmDialog()` opens above an already-visible Bootstrap modal (e.g. Registry → Load Domain), dim and blur the underlying modal and raise the confirmation so it is readable.

**Architecture:** Keep the existing `Promise<boolean>` API. Before showing the confirmation, detect the topmost `.modal.show`, mark it `ob-modal-underlying`, mark the confirmation `ob-modal-stacked`, and tag its backdrop. Shared CSS in `components.css` owns the blur/dim/z-index. Cleanup on `hidden.bs.modal` is idempotent.

**Tech Stack:** Vanilla JS (`utils.js`), Bootstrap 5.3 modals, `--db-*` CSS tokens in `components.css`, pytest source-contract unit tests, Playwright e2e under `tests/e2e/registry/`.

**Spec:** `docs/superpowers/specs/2026-08-02-stacked-confirmation-modal-design.md`

## Global Constraints

- Run routine tests with `uv run pytest -q -m "not scenario"` (project contract in `.cursorrules` / `.cursor/08-testing-and-deployment.mdc`). Expect 0 failures.
- **There is no JavaScript test runner.** JS behaviour is asserted by reading source text under `tests/units/front/` (established pattern: `tests/units/front/test_new_domain_name_validation.py`). Playwright e2e under `tests/e2e/` is auto-skipped in routine runs unless the path is targeted explicitly — still add the e2e coverage the spec requires, and run it with an explicit path.
- Do not invent a Registry-specific confirmation helper. Fix `showConfirmDialog()` globally.
- Nested confirmations beyond one confirmation over one parent modal are out of scope.
- Class names (locked): `ob-modal-underlying`, `ob-modal-stacked`, `ob-modal-stacked-backdrop`.
- Z-index floors (locked): stacked backdrop `1060`, stacked modal `1065` (above Bootstrap defaults ~1050/1055 and the navbar at 1050).
- Use `--db-*` tokens; no hard-coded brand colours where a token exists. Blur uses `filter: blur(2px)` and dim uses `rgba` of `--db-dark` (`#1B1C1D`).
- After code changes: append changelog under `changelogs/v0.7.0/benoitcayladbx_2026-08-02.log` (version from `pyproject.toml` = `0.7.0`).
- Only create git commits when the user asks (or when executing this plan's commit steps under an execution skill with user approval for commits).

---

## File Structure

| File | Change | Responsibility after |
|---|---|---|
| `src/front/static/global/js/utils.js` | Modify `showConfirmDialog` | Detect parent modal; apply/remove stack classes; keep API |
| `src/front/static/global/css/components.css` | Append stacked-modal section | Blur/dim underlying; raise stacked modal + backdrop; reduced-motion |
| `tests/units/front/test_stacked_confirm_modal.py` | **Create** | Source-contract tests for JS + CSS (routine CI) |
| `tests/e2e/registry/test_registry_flows.py` | Append | Browser regression: Registry + confirm stack/cleanup |
| `changelogs/v0.7.0/benoitcayladbx_2026-08-02.log` | Create/append | Post-change routine |

No changes to `registry.js`, `registry-modal.js`, or `registry-modal.css`.

---

### Task 1: Unit source contracts (failing first)

**Files:**
- Create: `tests/units/front/test_stacked_confirm_modal.py`
- Test: `tests/units/front/test_stacked_confirm_modal.py`

**Interfaces:**
- Produces: failing assertions that `showConfirmDialog` must apply `ob-modal-underlying` / `ob-modal-stacked` / `ob-modal-stacked-backdrop`, clean them up on hide, and that CSS defines those classes with blur + raised z-index. Task 2 and Task 3 make these pass.

- [ ] **Step 1: Write the failing unit tests**

Create `tests/units/front/test_stacked_confirm_modal.py`:

```python
"""Contract: showConfirmDialog stacks above an open Bootstrap modal.

When a confirmation opens while another ``.modal.show`` exists, the parent
gets ``ob-modal-underlying`` (blur/dim), the confirm gets ``ob-modal-stacked``,
and its backdrop gets ``ob-modal-stacked-backdrop``. Cleanup is idempotent on
``hidden.bs.modal``.
"""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
UTILS_JS = REPO_ROOT / "src/front/static/global/js/utils.js"
COMPONENTS_CSS = REPO_ROOT / "src/front/static/global/css/components.css"


def _utils() -> str:
    return UTILS_JS.read_text(encoding="utf-8")


def _confirm_body() -> str:
    source = _utils()
    start = source.index("function showConfirmDialog")
    # Next exported helper after showConfirmDialog
    end = source.index("function showInfoDialog", start)
    return source[start:end]


def _css() -> str:
    return COMPONENTS_CSS.read_text(encoding="utf-8")


def test_confirm_detects_visible_parent_modal():
    body = _confirm_body()
    assert ".modal.show" in body
    assert "ob-modal-underlying" in body


def test_confirm_marks_itself_stacked():
    body = _confirm_body()
    assert "ob-modal-stacked" in body
    assert "ob-modal-stacked-backdrop" in body


def test_confirm_cleans_stack_on_hidden():
    body = _confirm_body()
    assert "hidden.bs.modal" in body
    assert "classList.remove" in body
    assert "ob-modal-underlying" in body
    # Cleanup must not leave the parent marked after close
    assert body.count("ob-modal-underlying") >= 2


def test_css_defines_underlying_blur():
    css = _css()
    assert ".ob-modal-underlying" in css
    assert "blur(" in css
    assert "prefers-reduced-motion" in css


def test_css_raises_stacked_z_index():
    css = _css()
    assert ".ob-modal-stacked" in css
    assert ".ob-modal-stacked-backdrop" in css
    assert "1065" in css
    assert "1060" in css
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest -q tests/units/front/test_stacked_confirm_modal.py
```

Expected: FAIL — assertions miss `ob-modal-underlying` / stacked CSS (current `showConfirmDialog` has none of these strings).

- [ ] **Step 3: Commit (only if user asked for commits / under an approved execution run)**

```bash
git add tests/units/front/test_stacked_confirm_modal.py \
  docs/superpowers/specs/2026-08-02-stacked-confirmation-modal-design.md \
  docs/superpowers/plans/2026-08-02-stacked-confirmation-modal.md
git commit -m "$(cat <<'EOF'
test(front): add failing contracts for stacked confirm dialogs

EOF
)"
```

---

### Task 2: CSS for stacked / underlying modals

**Files:**
- Modify: `src/front/static/global/css/components.css` (append at end)
- Test: `tests/units/front/test_stacked_confirm_modal.py::test_css_defines_underlying_blur`
- Test: `tests/units/front/test_stacked_confirm_modal.py::test_css_raises_stacked_z_index`

**Interfaces:**
- Consumes: class names from Global Constraints.
- Produces: visual contract Task 3's JS classes hang off.

- [ ] **Step 1: Append the stacked-modal CSS block**

Append to the end of `src/front/static/global/css/components.css`:

```css
/* ==========================================================================
   Stacked confirmation over an open modal
   Applied by showConfirmDialog() in global/js/utils.js when another
   .modal.show already exists. Classes: ob-modal-underlying (parent),
   ob-modal-stacked (confirm), ob-modal-stacked-backdrop (its backdrop).
   ========================================================================== */

.modal.ob-modal-underlying .modal-content {
    filter: blur(2px);
    opacity: 0.72;
    transition: filter 0.15s ease, opacity 0.15s ease;
    pointer-events: none;
}

.modal.ob-modal-underlying::after {
    content: "";
    position: absolute;
    inset: 0;
    background: rgba(27, 28, 29, 0.28); /* --db-dark */
    pointer-events: none;
    z-index: 1;
}

.modal.ob-modal-stacked {
    z-index: 1065 !important;
}

.modal-backdrop.ob-modal-stacked-backdrop {
    z-index: 1060 !important;
    background-color: rgba(27, 28, 29, 0.45);
}

@media (prefers-reduced-motion: reduce) {
    .modal.ob-modal-underlying .modal-content {
        filter: none;
        transition: none;
        opacity: 0.55;
    }
}
```

- [ ] **Step 2: Run the CSS contract tests**

Run:

```bash
uv run pytest -q tests/units/front/test_stacked_confirm_modal.py::test_css_defines_underlying_blur tests/units/front/test_stacked_confirm_modal.py::test_css_raises_stacked_z_index
```

Expected: PASS for both CSS tests. JS tests still FAIL.

- [ ] **Step 3: Commit (if committing)**

```bash
git add src/front/static/global/css/components.css
git commit -m "$(cat <<'EOF'
style(front): dim and blur underlying modal under stacked confirms

EOF
)"
```

---

### Task 3: Wire stacking into `showConfirmDialog`

**Files:**
- Modify: `src/front/static/global/js/utils.js` (`showConfirmDialog`, ~376-445)
- Test: `tests/units/front/test_stacked_confirm_modal.py`

**Interfaces:**
- Consumes: CSS classes from Task 2.
- Produces: unchanged `showConfirmDialog(options) -> Promise<boolean>`; callers (`registry.js` Load Domain, deletes, etc.) need no edits.

- [ ] **Step 1: Replace `showConfirmDialog` with stacking-aware implementation**

In `src/front/static/global/js/utils.js`, replace the entire `showConfirmDialog` function (from `function showConfirmDialog` through its closing `}`) with:

```javascript
function showConfirmDialog(options = {}) {
    return new Promise((resolve) => {
        const {
            title = 'Confirm',
            message = 'Are you sure?',
            confirmText = 'Yes',
            cancelText = 'Cancel',
            confirmClass = 'btn-primary',
            icon = 'question-circle'
        } = options;

        const modalId = 'confirmDialog_' + Date.now();

        const modalHtml = `
            <div class="modal fade" id="${modalId}" tabindex="-1" data-bs-backdrop="static">
                <div class="modal-dialog modal-dialog-centered">
                    <div class="modal-content">
                        <div class="modal-header">
                            <h5 class="modal-title">
                                <i class="bi bi-${icon} me-2"></i>${title}
                            </h5>
                            <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                        </div>
                        <div class="modal-body">
                            <p class="mb-0">${message}</p>
                        </div>
                        <div class="modal-footer">
                            <button type="button" class="btn btn-secondary" data-bs-dismiss="modal" id="${modalId}_cancel">
                                ${cancelText}
                            </button>
                            <button type="button" class="btn ${confirmClass}" id="${modalId}_confirm">
                                ${confirmText}
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        `;

        const existing = document.getElementById(modalId);
        if (existing) existing.remove();

        document.body.insertAdjacentHTML('beforeend', modalHtml);

        const modalEl = document.getElementById(modalId);
        const modal = new bootstrap.Modal(modalEl);

        // Topmost already-visible modal becomes the blurred underlay.
        const openModals = document.querySelectorAll('.modal.show');
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
            if (topBackdrop) {
                topBackdrop.classList.add('ob-modal-stacked-backdrop');
            }
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

        let resolved = false;

        document.getElementById(`${modalId}_confirm`).addEventListener('click', () => {
            resolved = true;
            modal.hide();
            resolve(true);
        });

        modalEl.addEventListener('shown.bs.modal', applyStack);

        modalEl.addEventListener('hidden.bs.modal', () => {
            clearStack();
            if (!resolved) {
                resolve(false);
            }
            setTimeout(() => modalEl.remove(), 100);
        });

        modal.show();
        // Backdrop exists immediately after show(); tag it even if shown
        // fires slightly later in some browsers.
        applyStack();
    });
}
```

Keep `window.showConfirmDialog = showConfirmDialog;` at the bottom of the file unchanged.

- [ ] **Step 2: Run all stacked-confirm unit tests**

Run:

```bash
uv run pytest -q tests/units/front/test_stacked_confirm_modal.py
```

Expected: PASS (all 5 tests).

- [ ] **Step 3: Commit (if committing)**

```bash
git add src/front/static/global/js/utils.js
git commit -m "$(cat <<'EOF'
fix(front): blur underlying modal when confirm stacks on top

EOF
)"
```

---

### Task 4: Playwright regression on Registry modal

**Files:**
- Modify: `tests/e2e/registry/test_registry_flows.py`
- Test: `tests/e2e/registry/test_registry_flows.py::TestRegistryModal::test_confirm_stacks_over_registry_modal`

**Interfaces:**
- Consumes: runtime classes from Tasks 2–3. Does not depend on registry domain data — drives `showConfirmDialog` via `page.evaluate` while `#registryModal` is shown.

- [ ] **Step 1: Add the browser regression test**

Append to `tests/e2e/registry/test_registry_flows.py` inside `TestRegistryModal`:

```python
    def test_confirm_stacks_over_registry_modal(self, page, live_server):
        """Confirm over Registry must mark the Registry as underlying and
        clean up after cancel — covers white-on-white stacking."""
        page.goto(live_server)
        page.wait_for_load_state("domcontentloaded")

        page.locator("#registryModalToggle").click()
        page.wait_for_selector("#registryModal.show", state="visible")

        page.evaluate(
            """() => {
                window.__obConfirmPromise = showConfirmDialog({
                    title: 'Load Domain',
                    message: 'Load <strong>fibo</strong> version <strong>v1</strong>?',
                    confirmText: 'Load',
                    cancelText: 'Cancel',
                });
            }"""
        )

        page.wait_for_selector(".modal.ob-modal-stacked.show", state="visible")
        assert page.locator("#registryModal.ob-modal-underlying").count() == 1
        assert page.locator(".modal-backdrop.ob-modal-stacked-backdrop").count() >= 1

        page.locator(".modal.ob-modal-stacked .btn-secondary").click()
        page.wait_for_selector(".modal.ob-modal-stacked", state="detached")

        assert page.locator("#registryModal.show").count() == 1
        assert page.locator("#registryModal.ob-modal-underlying").count() == 0
```

- [ ] **Step 2: Run the e2e test explicitly**

Run (Playwright + live app required; skips if playwright missing):

```bash
uv run pytest -q tests/e2e/registry/test_registry_flows.py::TestRegistryModal::test_confirm_stacks_over_registry_modal
```

Expected: PASS (or SKIP only if playwright is not installed — do not treat skip-for-missing-playwright as a product failure when the unit contracts already pass).

If the app is already running via `./scripts/start.sh`, the e2e conftest should attach; follow existing `tests/e2e` conventions if a live base URL env is required.

- [ ] **Step 3: Commit (if committing)**

```bash
git add tests/e2e/registry/test_registry_flows.py
git commit -m "$(cat <<'EOF'
test(e2e): cover stacked confirm over the Registry modal

EOF
)"
```

---

### Task 5: Changelog + full non-scenario suite

**Files:**
- Create/append: `changelogs/v0.7.0/benoitcayladbx_2026-08-02.log`

**Interfaces:**
- Consumes: all prior tasks' file list and test results.

- [ ] **Step 1: Write the changelog section**

Create or append `changelogs/v0.7.0/benoitcayladbx_2026-08-02.log`:

```
## Fix stacked confirmation visibility over open modals

Context: Opening Load Domain (or other confirms) from the Registry modal
produced a white-on-white confirmation. showConfirmDialog now dims/blurs
any already-open Bootstrap modal and raises the confirmation stacking
level for the lifetime of the dialog.

Changes:

1. src/front/static/global/js/utils.js
   Detect visible parent modal; apply/clear ob-modal-underlying / stacked classes.
2. src/front/static/global/css/components.css
   Blur/dim underlying modal; raise stacked confirm + backdrop; reduced-motion.
3. tests/units/front/test_stacked_confirm_modal.py
   Source contracts for stacking behaviour and CSS.
4. tests/e2e/registry/test_registry_flows.py
   Playwright regression for Registry + confirm stack/cleanup.

Modified files:
- src/front/static/global/js/utils.js
- src/front/static/global/css/components.css
- tests/units/front/test_stacked_confirm_modal.py
- tests/e2e/registry/test_registry_flows.py
- changelogs/v0.7.0/benoitcayladbx_2026-08-02.log

Tests: <paste summary from Step 2>
```

- [ ] **Step 2: Run the required non-scenario suite**

Run:

```bash
uv run pytest -q -m "not scenario"
```

Expected: 0 failures. Paste the summary line into the changelog `Tests:` field.

- [ ] **Step 3: Manual smoke (dev server already running)**

1. Open the app, click the Registry navbar icon.
2. Expand a domain and click Load (or Delete) so a confirm appears.
3. Confirm the Registry content is blurred/dimmed and the confirm text is readable.
4. Cancel — Registry returns to normal (no residual blur).
5. From a non-modal page, trigger any confirm — appearance unchanged (no underlying class).

- [ ] **Step 4: Commit changelog (if committing)**

```bash
git add changelogs/v0.7.0/benoitcayladbx_2026-08-02.log
git commit -m "$(cat <<'EOF'
docs(changelog): record stacked confirmation modal fix

EOF
)"
```

---

## Spec coverage (self-review)

| Spec requirement | Task |
|---|---|
| Detect topmost visible modal before confirm | Task 3 |
| Mark underlying + stacked + raise backdrop | Tasks 2–3 |
| Dim + light blur via shared CSS / `--db-*` | Task 2 |
| Short transition + `prefers-reduced-motion` | Task 2 |
| Cleanup on hide; restore parent; unchanged Promise API | Task 3 |
| Global (not Registry-only); no nested-confirm scope | Tasks 1–3 (no Registry API) |
| Browser regression (open Registry → confirm → assert → cancel → cleanup) | Task 4 |
| `uv run pytest -q -m "not scenario"` | Task 5 |

No placeholders. Class names and z-index values are consistent across Tasks 1–4.
