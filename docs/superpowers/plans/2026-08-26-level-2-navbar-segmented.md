# Level 2 Navbar Segmented Navigation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the full-width L2 strip with a transparent row containing a compact segmented workspace control while preserving inactive dropdowns and all existing domain actions.

**Architecture:** Keep `base.html` as the single owner of L2 markup and use a nested semantic list to group the four workspace targets. Implement the visual change in shared `main.css`; retain the existing `navbar.js` active-target behavior, which already removes dropdown wiring only from the selected workspace.

**Tech Stack:** Jinja2, Bootstrap 5.3.2 dropdowns, Bootstrap Icons, shared `--db-*` CSS tokens, pytest static contract tests.

## Global Constraints

- Preserve Domain, Ontology, Mapping, Knowledge Graph, breadcrumb, Save, Switch Version, and Close order.
- Preserve all routes, menu configuration, permission gates, domain state, and action handlers.
- Only inactive workspace targets expose dropdowns; the selected target uses the page sidebar.
- L2 has no full-width background and no bottom border.
- Save is the only filled primary action.
- Preserve the icon-only mobile contract below `768px` without horizontal scrolling.
- Do not stage or commit unrelated pre-existing working-tree changes.

---

### Task 1: Lock the segmented L2 contract

**Files:**
- Create: `tests/units/front/test_segmented_subnav_contract.py`
- Test: `tests/units/front/test_segmented_subnav_contract.py`

**Interfaces:**
- Consumes: `base.html`, `main.css`, and `navbar.js` as static text.
- Produces: regression coverage for structure, styling, active behavior, and mobile preservation.

- [ ] **Step 1: Write the failing static contract tests**

Add tests that assert:

```python
def test_workspace_targets_are_grouped_before_context_and_actions():
    html = _read(BASE_HTML)
    group = re.search(
        r'<li class="ob-subnav-workspaces">(.*?)</li>\\s*'
        r'<li class="ob-subnav-flex-spacer"',
        html,
        flags=re.DOTALL,
    )
    assert group
    body = group.group(1)
    ids = [
        "subnavDomainDropdown",
        "subnavOntologyDropdown",
        "subnavMappingDropdown",
        "subnavKgDropdown",
    ]
    assert [body.index(element_id) for element_id in ids] == sorted(
        body.index(element_id) for element_id in ids
    )


def test_subnav_surface_is_transparent_and_borderless():
    css = _read(MAIN_CSS)
    block = _rule(css, ".ob-subnav")
    assert re.search(r"background(?:-color)?\\s*:\\s*transparent", block)
    assert re.search(r"border-bottom\\s*:\\s*(?:0|none)", block)


def test_active_target_disables_only_its_dropdown():
    js = _read(NAVBAR_JS)
    assert "disableCurrentSubnavDropdown(link)" in js
    assert "toggle.removeAttribute('data-bs-toggle')" in js
    assert "if (menu) menu.remove()" in js
```

Also assert the workspace group uses a white surface, shared border/radius
tokens, selected primary-soft tokens, a shared focus ring, and that the mobile
block has no `overflow-x: auto`.

- [ ] **Step 2: Run the new test and verify RED**

Run:

```bash
uv run --frozen pytest -q tests/units/front/test_segmented_subnav_contract.py
```

Expected: failures for the missing `.ob-subnav-workspaces` wrapper and the old
warm background/bottom border.

---

### Task 2: Implement the approved L2 treatment

**Files:**
- Modify: `src/front/templates/base.html:262-413`
- Modify: `src/front/static/global/css/main.css:439-748`
- Modify: `.cursor/11-frontend-design.mdc:425-469`
- Test: `tests/units/front/test_segmented_subnav_contract.py`
- Test: `tests/units/front/test_mobile_subnav_contract.py`

**Interfaces:**
- Consumes: existing `.ob-subnav-link`, Bootstrap dropdown markup, `initSubnavActiveState()`, and `disableCurrentSubnavDropdown(toggle)`.
- Produces: `.ob-subnav-workspaces` and `.ob-subnav-workspace-list` as shared L2 structure hooks.

- [ ] **Step 1: Group workspace targets semantically**

Wrap the four existing workspace `<li>` elements and their Domain divider in:

```html
<li class="ob-subnav-workspaces">
    <ul class="ob-subnav-workspace-list">
        <!-- existing Domain, divider, Ontology, Mapping and KG items -->
    </ul>
</li>
```

Keep the spacer, breadcrumb, and domain actions as direct children of
`.ob-subnav-nav`.

- [ ] **Step 2: Apply transparent row and segmented control styles**

Update shared CSS with the following outcomes:

```css
.ob-subnav {
    background: transparent;
    border-bottom: 0;
}

.ob-subnav-workspace-list {
    display: flex;
    align-items: center;
    list-style: none;
    margin: 0;
    padding: 0.2rem;
    gap: 0.125rem;
    background: var(--db-surface-warm);
    border: 1px solid var(--db-border);
    border-radius: var(--db-radius-control);
}

.ob-subnav-link.active {
    color: var(--db-primary-darker);
    background: var(--db-primary-light);
    border-radius: calc(var(--db-radius-control) - 2px);
}
```

Remove the L2 baseline treatment, use indigo-soft hover, add
`:focus-visible { box-shadow: var(--db-focus-ring); }`, fill Save with
`--db-primary`/`--db-on-primary`, and keep Close outlined with danger intent.

- [ ] **Step 3: Preserve compact mobile behavior**

Ensure the nested workspace list remains flex and icon-only below `767.98px`.
Keep labels in the DOM, hide breadcrumb/spacer, compact padding, and do not add
horizontal overflow.

- [ ] **Step 4: Update the canonical frontend design rule**

Change the L2 visual contract and token description to say that L2 is a
transparent row with a white segmented workspace control. Document that only
inactive targets retain dropdowns.

- [ ] **Step 5: Run targeted tests and verify GREEN**

Run:

```bash
uv run --frozen pytest -q \
  tests/units/front/test_segmented_subnav_contract.py \
  tests/units/front/test_mobile_subnav_contract.py \
  tests/units/front/test_clarity_design_contract.py
```

Expected: all selected tests pass.

---

### Task 3: Document and verify the completed change

**Files:**
- Modify: `changelogs/v0.8.0/benoitcayladbx_2026-08-26.log`

**Interfaces:**
- Consumes: completed implementation and test output.
- Produces: required English changelog section and a verified commit.

- [ ] **Step 1: Run the mandatory non-scenario suite**

Run:

```bash
uv run --frozen pytest -q -m "not scenario"
```

Expected: all non-scenario tests pass.

- [ ] **Step 2: Append the versioned changelog section**

Record context, numbered changes, modified files, and the exact test summary in
the existing v0.8.0 daily log.

- [ ] **Step 3: Check edited-file diagnostics**

Read IDE diagnostics for `base.html`, `main.css`, the new test, and the updated
rule. Fix only issues introduced by this change.

- [ ] **Step 4: Commit only this feature**

Stage the new plan/test and clean source files normally. For the already-dirty
frontend rule and changelog, stage only this feature's hunks so unrelated
working-tree edits remain unstaged.

Use:

```bash
git commit -m "feat(ui): segment level 2 navigation"
```
