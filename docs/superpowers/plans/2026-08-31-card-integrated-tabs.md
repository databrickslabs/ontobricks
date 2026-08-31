# Card-Integrated Page Tabs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Align Ontology → Generate with the Data Quality card-integrated tab
surface and codify that pattern for Cursor and Claude.

**Architecture:** Preserve the shared `.ob-tabs` visual component and change
only the page-level container structure. Ontology Generate will use one outer
card containing the tab rail and padded tab body; `.cursor/11` remains the
canonical rule, while a thin Claude skill points to it and sequences browser
verification.

**Tech Stack:** Jinja2 templates, Bootstrap 5.3, project `.ob-tabs` CSS,
Markdown Cursor rules, Claude Agent Skills, pytest static contracts, browser
verification.

## Global Constraints

- Use Ontology → Data Quality as the exact visual reference.
- Preserve every Ontology Generate tab ID, pane ID, control, and JavaScript
  behavior.
- Do not add page-specific tab-link CSS.
- Preserve the independent `ob-tab-content` and compact-panel patterns for
  pages that do not use the integrated card pattern.
- `.cursor/11-frontend-design.mdc` is the single source of truth; Claude skill
  text must not duplicate CSS declarations.
- Preserve the pending Domain Information read-only and tab-surface changes.
- Do not commit unless the user explicitly requests it.

---

### Task 1: Migrate Ontology Generate to the integrated card surface

**Files:**
- Modify: `tests/units/front/test_clarity_design_contract.py`
- Modify: `src/front/templates/partials/ontology/_ontology_wizard.html`

**Interfaces:**
- Consumes: existing `#wizardTabs`, `#wizardTabContent`, and Bootstrap tab
  behavior.
- Produces: Data Quality-compatible card hierarchy without changing IDs or
  controls.

- [ ] **Step 1: Add the failing structural contract**

Add these constants near the other template constants:

```python
ONTOLOGY_WIZARD_TEMPLATE = (
    REPO_ROOT / "src/front/templates/partials/ontology/_ontology_wizard.html"
)
```

Add this test:

```python
def test_ontology_generate_uses_the_card_integrated_tab_pattern():
    template = _read(ONTOLOGY_WIZARD_TEMPLATE)
    assert '<div class="card h-100">' in template
    assert '<div class="card-body p-0 ob-tabs-wrap">' in template
    assert (
        'class="nav nav-tabs ob-tabs nav-fill" id="wizardTabs"'
        in template
    )
    anchor = template.index('id="wizardTabContent"')
    content_tag = template[template.rindex("<div", 0, anchor) : anchor]
    assert "tab-content p-3" in content_tag
    assert "ob-tab-content" not in content_tag
```

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
uv run --frozen pytest -q tests/units/front/test_clarity_design_contract.py::test_ontology_generate_uses_the_card_integrated_tab_pattern
```

Expected: failure because the wizard still uses a standalone rail and
`ob-tab-content`.

- [ ] **Step 3: Apply the Data Quality structure**

Immediately after the Generate page header, wrap the current tab rail, tab
content, and informational note as follows:

```html
<div class="card h-100">
    <div class="card-body p-0 ob-tabs-wrap">
        <ul class="nav nav-tabs ob-tabs nav-fill"
            id="wizardTabs" role="tablist">
            <!-- Keep all four existing li/button elements unchanged. -->
        </ul>

        <div class="tab-content p-3" id="wizardTabContent">
            <!-- Keep all four existing tab panes unchanged. -->

            <div class="text-muted small mb-3 mt-3">
                <i class="bi bi-info-circle me-1"></i>
                Generation uses AI and may take a few seconds. The ontology
                will be applied automatically when ready.
            </div>
        </div>
    </div>
</div>
```

The informational note moves inside `#wizardTabContent` after the panes so the
single card owns the complete workspace. The dynamic progress overlay remains
outside the card.

- [ ] **Step 4: Run focused layout contracts and verify GREEN**

Run:

```bash
uv run --frozen pytest -q \
  tests/units/front/test_clarity_design_contract.py \
  tests/units/front/test_sidebar_content_stretch_contract.py
```

Expected: all selected tests pass, including the existing
`#wizardTabContent` internal-scroll contract.

### Task 2: Codify the pattern in Cursor rules and Claude skills

**Files:**
- Modify: `.cursor/11-frontend-design.mdc`
- Create: `.claude/skills/frontend-design/SKILL.md`
- Modify: `CLAUDE.md`
- Modify: `tests/units/front/test_clarity_design_contract.py`

**Interfaces:**
- Consumes: the canonical Data Quality/Domain/Generate markup pattern.
- Produces: one authoritative Cursor rule and one thin Claude workflow skill.

- [ ] **Step 1: Add failing guidance contracts**

Add these constants:

```python
CLAUDE_GUIDE = REPO_ROOT / "CLAUDE.md"
FRONTEND_SKILL = REPO_ROOT / ".claude/skills/frontend-design/SKILL.md"
```

Add these tests:

```python
def test_frontend_rule_defines_card_integrated_page_tabs():
    rule = _read(FRONTEND_RULE)
    assert "Card-integrated page tabs" in rule
    assert '<div class="card h-100">' in rule
    assert '<div class="card-body p-0 ob-tabs-wrap">' in rule
    assert 'class="nav nav-tabs ob-tabs nav-fill"' in rule
    assert 'class="tab-content p-3"' in rule
    assert "Domain → Information" in rule
    assert "Ontology → Generate" in rule


def test_claude_frontend_skill_points_to_the_canonical_rule():
    skill = _read(FRONTEND_SKILL)
    assert ".cursor/11-frontend-design.mdc" in skill
    assert "browser" in skill.lower()
    assert "desktop" in skill.lower()
    assert "mobile" in skill.lower()
    assert "uv run --frozen pytest -q -m \"not scenario\"" in skill
    assert "frontend-design" in _read(CLAUDE_GUIDE)
```

- [ ] **Step 2: Run the guidance tests and verify RED**

Run:

```bash
uv run --frozen pytest -q \
  tests/units/front/test_clarity_design_contract.py::test_frontend_rule_defines_card_integrated_page_tabs \
  tests/units/front/test_clarity_design_contract.py::test_claude_frontend_skill_points_to_the_canonical_rule
```

Expected: failures because the canonical subsection, skill file, and skill
registration do not yet exist.

- [ ] **Step 3: Update the canonical Cursor rule**

In `.cursor/11-frontend-design.mdc`:

1. Replace the statement that all tab content is an independent surface with
   two explicit choices: card-integrated page tabs for one logical workspace,
   and `ob-tab-content` for genuinely independent surfaces.
2. Add a `#### Card-integrated page tabs` subsection containing this exact
   skeleton:

```html
<div class="card h-100">
    <div class="card-body p-0 ob-tabs-wrap">
        <ul class="nav nav-tabs ob-tabs nav-fill"
            id="<unique>Tabs" role="tablist">
            ...
        </ul>
        <div class="tab-content p-3" id="<unique>TabContent">
            ...
        </div>
    </div>
</div>
```

3. State that an outer `<form>` may replace the card `<div>` when the page
   needs one form owner.
4. Name Domain → Information, Ontology → Generate, Data Quality, Business
   Rules, and Axioms as integrated examples.
5. Keep the independent surface skeleton and compact-panel rules.
6. Update the catalogued combinations so Generate is no longer listed under
   the standalone Wizard pattern.
7. Add an anti-pattern forbidding a standalone `ob-tab-content` directly below
   the rail when both belong to one logical card workspace.

- [ ] **Step 4: Create the thin Claude skill**

Create `.claude/skills/frontend-design/SKILL.md` with:

```markdown
---
name: frontend-design
description: Use when creating or changing OntoBricks templates, page layouts, tabs, cards, forms, or responsive UI behavior.
---

# OntoBricks frontend design

The canonical design system is `.cursor/11-frontend-design.mdc`. Read that
file before proposing or changing frontend markup or CSS. This skill sequences
the work; it does not restate the visual rules.

## Procedure

1. Identify the closest canonical page/component in `.cursor/11`.
2. Inspect its complete markup and shared CSS before editing.
3. Add a failing structural or behavior contract.
4. Reuse shared Bootstrap and `ob-*` components; do not add local visual
   overrides when the shared component already owns the state.
5. Browser-test the changed page against the reference at desktop and mobile
   widths, including keyboard focus, overflow, and console/network errors.
6. Run `uv run --frozen pytest -q -m "not scenario"`.
7. Invoke the `changelog` skill.
```

- [ ] **Step 5: Register the Claude skill**

Add this row to the Claude-only skill table in `CLAUDE.md`:

```markdown
| `frontend-design` | Creating or changing templates, tabs, cards, forms, responsive layout, or other frontend UI |
```

- [ ] **Step 6: Run guidance tests and verify GREEN**

Run:

```bash
uv run --frozen pytest -q tests/units/front/test_clarity_design_contract.py
```

Expected: all Clarity design contracts pass.

### Task 3: Browser-verify and document the complete change

**Files:**
- Modify: `changelogs/v0.8.0/benoitcayladbx_2026-08-31.log`

**Interfaces:**
- Consumes: migrated Generate markup and canonical guidance.
- Produces: browser evidence, lint status, full-suite result, and changelog.

- [ ] **Step 1: Browser-test Ontology Generate**

Compare `http://localhost:8000/ontology/?section=wizard` against
`http://localhost:8000/ontology/?section=dataquality`.

Verify:

- identical outer card border, radius, background, rail-edge geometry, and
  content padding;
- Data Sources, Documents, Guidelines, and Options tabs switch correctly;
- Generate Ontology and tab controls retain their behavior;
- the rail remains pinned while long content scrolls;
- mobile labels stay single-line with horizontal rail scrolling;
- read-only controls remain frozen;
- no new console, page, or failed asset errors.

- [ ] **Step 2: Check diagnostics and diff hygiene**

Run IDE diagnostics on all changed template, Markdown, and Python test files,
then run:

```bash
git diff --check
```

Expected: no new diagnostics and exit code 0.

- [ ] **Step 3: Run the mandatory suite**

Run:

```bash
uv run --frozen pytest -q -m "not scenario"
```

Expected: the complete non-scenario suite passes.

- [ ] **Step 4: Append the changelog section**

Append an English section titled `Standardize card-integrated page tabs` to
`changelogs/v0.8.0/benoitcayladbx_2026-08-31.log`. List the Generate template,
Cursor rule, Claude skill, Claude guide, tests, specification, plan, and
changelog itself. Record the exact focused browser checks and final pytest
summary.
