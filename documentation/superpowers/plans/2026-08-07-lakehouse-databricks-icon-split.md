# Lakehouse and Databricks Icon Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Use the supplied orange lakehouse icon for every Lakehouse backend surface and reserve the official Databricks logo for Settings → Configuration → Databricks.

**Architecture:** Keep the shared `ob-brand-icon` component and split its product modifiers semantically. A new local PNG backs `ob-icon-lakehouse`; the existing local SVG continues to back `ob-icon-databricks`.

**Tech Stack:** Jinja2 templates, JSON menu configuration, CSS, browser JavaScript, PNG/SVG assets, pytest.

## Global Constraints

- Store both assets locally; add no runtime CDN dependency.
- Preserve the shared `1em × 1em` icon box, baseline alignment, and contained background rendering.
- Map `databricks`, `delta`, and `lakehouse` backend aliases to `ob-icon-lakehouse`.
- Use `ob-icon-databricks` only for Settings → Configuration → Databricks.
- Leave Lakebase and Neo4j mappings unchanged.
- Run Python commands through `uv run --frozen`.
- Do not create a git commit unless the user explicitly requests one.

---

### Task 1: Define and implement the semantic icon contract

**Files:**
- Modify: `tests/units/front/test_backend_brand_icons.py`
- Create: `src/front/static/global/img/lakehouse-icon.png`
- Modify: `src/front/static/global/css/components.css`
- Modify: `src/front/config/menu_config.json`
- Modify: `src/front/templates/settings.html`
- Modify: `src/front/static/query/js/query-sync.js`
- Modify: `src/front/static/domain/js/domain-validation.js`
- Modify: `src/front/static/registry/js/registry.js`

**Interfaces:**
- Consumes: supplied asset `/Users/benoit.cayla/.cursor/projects/Users-benoit-cayla-git-labs-ontobricks/assets/primary-icon-orange-data-lakehouse-heavy-76e5a1e5-495c-413c-ae8a-28ba5b66ee6c.png`.
- Produces: `ob-icon-lakehouse` for Lakehouse surfaces and `ob-icon-databricks` exclusively for Databricks settings.

- [ ] **Step 1: Write the failing static contract**

Update the expected backend mapping and separate SVG/PNG validation:

```python
_EXPECTED = {
    "lakebase": ("ob-icon-postgresql", "lakebase-icon.svg"),
    "delta": ("ob-icon-lakehouse", "lakehouse-icon.png"),
    "neo4j": ("ob-icon-neo4j", "neo4j-icon.svg"),
}


def _settings_items(group_id: str) -> dict[str, dict]:
    menus = json.loads(_MENU.read_text(encoding="utf-8"))["menus"]
    settings = next(menu for menu in menus if menu["id"] == "settings")
    group = next(group for group in settings["groups"] if group["id"] == group_id)
    return {item["id"]: item for item in group["items"]}
```

Make `_backend_items()` delegate to `_settings_items("settings-triplestore")`, then update and add these tests:

```python
def test_backend_headers_match_menu_brand_icons():
    template = _SETTINGS.read_text(encoding="utf-8")
    expected_headers = {
        "Lakebase": "ob-icon-postgresql",
        "Lakehouse": "ob-icon-lakehouse",
        "Neo4j": "ob-icon-neo4j",
    }
    for label, modifier in expected_headers.items():
        assert f'<i class="ob-brand-icon {modifier} me-2"></i>{label}</h4>' in template


def test_brand_icon_assets_are_local_and_colored():
    for item_id, (_, filename) in _EXPECTED.items():
        asset = _IMG / filename
        assert asset.is_file()
        if item_id == "delta":
            assert asset.read_bytes().startswith(b"\x89PNG\r\n\x1a\n")
        else:
            svg = asset.read_text(encoding="utf-8")
            assert "<svg" in svg
            assert "#" in svg, f"{filename} must define a brand color"


def test_databricks_settings_uses_official_databricks_icon():
    items = _settings_items("settings-configuration")
    assert items["databricks"]["icon"] == "ob-brand-icon ob-icon-databricks"

    template = _SETTINGS.read_text(encoding="utf-8")
    assert (
        '<i class="ob-brand-icon ob-icon-databricks me-2"></i>Databricks</h4>'
        in template
    )

    css = _CSS.read_text(encoding="utf-8")
    assert (
        '.ob-icon-databricks {\n'
        '    background-image: url("/static/global/img/databricks-icon.svg");\n'
        '}'
    ) in css


def test_lakehouse_dynamic_icons_use_lakehouse_modifier():
    build_js = _BUILD_JS.read_text(encoding="utf-8")
    validation_js = _VALIDATION_JS.read_text(encoding="utf-8")
    registry_js = Path("src/front/static/registry/js/registry.js").read_text(
        encoding="utf-8"
    )

    assert "return 'ob-icon-lakehouse';" in build_js
    assert "iconClass = 'ob-icon-lakehouse';" in validation_js
    assert "databricks: 'ob-icon-lakehouse'," in registry_js
```

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
uv run --frozen pytest -q tests/units/front/test_backend_brand_icons.py
```

Expected: FAIL because `lakehouse-icon.png` and `ob-icon-lakehouse` do not exist, and Lakehouse still maps to `ob-icon-databricks`.

- [ ] **Step 3: Copy the supplied Lakehouse asset**

Run:

```bash
cp "/Users/benoit.cayla/.cursor/projects/Users-benoit-cayla-git-labs-ontobricks/assets/primary-icon-orange-data-lakehouse-heavy-76e5a1e5-495c-413c-ae8a-28ba5b66ee6c.png" "src/front/static/global/img/lakehouse-icon.png"
```

- [ ] **Step 4: Add the Lakehouse CSS modifier**

Insert before `.ob-icon-databricks` in `components.css`:

```css
.ob-icon-lakehouse {
    background-image: url("/static/global/img/lakehouse-icon.png");
}
```

Keep the existing Databricks rule unchanged:

```css
.ob-icon-databricks {
    background-image: url("/static/global/img/databricks-icon.svg");
}
```

- [ ] **Step 5: Split the static menu and settings mappings**

In `menu_config.json`, set the Configuration → Databricks icon to:

```json
"icon": "ob-brand-icon ob-icon-databricks"
```

Set the Back end → Lakehouse icon to:

```json
"icon": "ob-brand-icon ob-icon-lakehouse"
```

In `settings.html`, render:

```html
<h4 class="mb-1"><i class="ob-brand-icon ob-icon-databricks me-2"></i>Databricks</h4>
```

and:

```html
<h4 class="mb-1"><i class="ob-brand-icon ob-icon-lakehouse me-2"></i>Lakehouse</h4>
```

- [ ] **Step 6: Split the dynamic Lakehouse mappings**

In `query-sync.js`, add `ob-icon-lakehouse` to the classes removed by `_setBackendBrandIcon()` and map all Lakehouse aliases to it:

```javascript
element.classList.remove(
    'ob-icon-postgresql',
    'ob-icon-lakehouse',
    'ob-icon-databricks',
    'ob-icon-neo4j',
    'd-none'
);
```

```javascript
if (key === 'databricks' || key === 'delta' || key === 'lakehouse') {
    return 'ob-icon-lakehouse';
}
```

Apply the same cleanup and mapping in `domain-validation.js`:

```javascript
if (key === 'databricks' || key === 'delta' || key === 'lakehouse') {
    iconClass = 'ob-icon-lakehouse';
}
```

```javascript
element.classList.remove(
    'ob-icon-postgresql',
    'ob-icon-lakehouse',
    'ob-icon-databricks',
    'ob-icon-neo4j',
    'd-none'
);
```

In `registry.js`, update only the Lakehouse backend entry:

```javascript
const backendIconClasses = {
    lakebase: 'ob-icon-postgresql',
    databricks: 'ob-icon-lakehouse',
    neo4j: 'ob-icon-neo4j',
};
```

- [ ] **Step 7: Run the focused test and verify GREEN**

Run:

```bash
uv run --frozen pytest -q tests/units/front/test_backend_brand_icons.py
```

Expected: all tests in `test_backend_brand_icons.py` PASS.

### Task 2: Record and verify the completed change

**Files:**
- Modify: `changelogs/v0.7.0/benoitcayladbx_2026-08-07.log`

**Interfaces:**
- Consumes: completed icon split and focused test result from Task 1.
- Produces: required v0.7.0 audit entry and repository-wide non-scenario verification.

- [ ] **Step 1: Append the changelog section**

Append a section with:

```text
Lakehouse and Databricks icon split

Context:
Use the supplied Lakehouse artwork across Lakehouse backend surfaces and reserve the official Databricks logo for Databricks connection settings.

Changes:
1. src/front/static/global/img/lakehouse-icon.png — added the supplied local orange Lakehouse asset.
2. src/front/static/global/css/components.css — added the semantic Lakehouse icon modifier while retaining the Databricks modifier.
3. src/front/config/menu_config.json — split Lakehouse and Databricks menu icon mappings.
4. src/front/templates/settings.html — aligned the Databricks and Lakehouse headers with their menu icons.
5. src/front/static/query/js/query-sync.js — mapped Lakehouse backend aliases to the Lakehouse icon.
6. src/front/static/domain/js/domain-validation.js — mapped validation backend aliases to the Lakehouse icon.
7. src/front/static/registry/js/registry.js — mapped Registry Lakehouse badges to the Lakehouse icon.
8. tests/units/front/test_backend_brand_icons.py — covered local assets and semantic mappings.

Modified files:
- src/front/static/global/img/lakehouse-icon.png
- src/front/static/global/css/components.css
- src/front/config/menu_config.json
- src/front/templates/settings.html
- src/front/static/query/js/query-sync.js
- src/front/static/domain/js/domain-validation.js
- src/front/static/registry/js/registry.js
- tests/units/front/test_backend_brand_icons.py

Test result:
- uv run --frozen pytest -q tests/units/front/test_backend_brand_icons.py: PASS
```

- [ ] **Step 2: Run repository verification**

Run:

```bash
uv run --frozen pytest -q -m "not scenario"
```

Expected: all selected tests PASS.

- [ ] **Step 3: Update the changelog test result**

Add the final command result to the same changelog section, including the
reported pass count from pytest in place of the focused-test-only result:

```text
- uv run --frozen pytest -q -m "not scenario": PASS
```

- [ ] **Step 4: Check edited files for diagnostics**

Read IDE diagnostics for:

- `src/front/static/global/css/components.css`
- `src/front/config/menu_config.json`
- `src/front/templates/settings.html`
- `src/front/static/query/js/query-sync.js`
- `src/front/static/domain/js/domain-validation.js`
- `src/front/static/registry/js/registry.js`
- `tests/units/front/test_backend_brand_icons.py`

Expected: no new diagnostics introduced by this change.
