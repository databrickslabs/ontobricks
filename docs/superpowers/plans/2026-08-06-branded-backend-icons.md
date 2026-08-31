# Branded Backend Icons Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the generic Lakebase, Lakehouse, and Neo4j glyphs with compact, color brand icons sized like existing Bootstrap icons.

**Architecture:** Store the three SVG assets locally under `src/front/static/global/img/`. Reuse a single `ob-brand-icon` CSS component with one background-image modifier per product so existing menu rendering can continue to consume the `icon` string from `menu_config.json`.

**Tech Stack:** Jinja2 templates, Bootstrap 5, Bootstrap Icons, CSS, SVG, pytest.

## Global Constraints

- Render all three icons in their brand colors.
- Keep icon boxes at `1em × 1em`, aligned to the surrounding text baseline.
- Use PostgreSQL for Lakebase, Databricks for Lakehouse, and Neo4j for Neo4j.
- Bundle assets locally; do not add a runtime CDN dependency.

---

### Task 1: Add the branded icon contract

**Files:**
- Create: `tests/units/front/test_backend_brand_icons.py`

**Interfaces:**
- Consumes: `menu_config.json`, `settings.html`, `components.css`, and local SVG assets.
- Produces: Regression coverage for icon mapping, size, color assets, and page-header consistency.

- [ ] **Step 1: Write the failing test**

Assert that each Back end menu item uses `ob-brand-icon` plus its product modifier, each settings header uses the same classes, the shared CSS fixes the icon box to `1em`, and all three local SVG files exist.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --frozen pytest -q tests/units/front/test_backend_brand_icons.py`

Expected: FAIL because the branded classes and two of the local assets do not exist.

### Task 2: Add and render the local color assets

**Files:**
- Modify: `src/front/static/global/img/lakebase-icon.svg`
- Create: `src/front/static/global/img/databricks-icon.svg`
- Create: `src/front/static/global/img/neo4j-icon.svg`
- Modify: `src/front/static/global/css/components.css`
- Modify: `src/front/config/menu_config.json`
- Modify: `src/front/templates/settings.html`

**Interfaces:**
- Consumes: `ob-brand-icon ob-icon-{postgresql,databricks,neo4j}` class strings.
- Produces: Inline-sized, locally served color icons in sidebar/dropdowns and section headings.

- [ ] **Step 1: Add minimal implementation**

Add recognizable brand-color SVG glyphs, define the shared `1em × 1em` background-image component, map menu items to their modifiers, and apply matching classes to the three settings headings.

- [ ] **Step 2: Run focused tests**

Run: `uv run --frozen pytest -q tests/units/front/test_backend_brand_icons.py`

Expected: PASS.

- [ ] **Step 3: Run repository verification**

Run: `uv run --frozen pytest -q -m "not scenario"`

Expected: all selected tests pass.

- [ ] **Step 4: Update the v0.7.0 changelog**

Append the modified files and final test summary to `changelogs/v0.7.0/benoitcayladbx_2026-08-06.log`.
