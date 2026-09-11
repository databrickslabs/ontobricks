# Workspace Map Modal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Clicking the L1 Domain + version badge opens a four-column Bootstrap modal of every L2 workspace menu and its sub-menus, without navigating to `/domain/`.

**Architecture:** One Jinja partial included from `base.html`, driven by `menu_config.json` (same groups as L2). Styles live in `components.css` as `.ob-workspace-map*`. `navbar.js` prevents badge navigation, copies the domain title, highlights the current section, and opens the modal via `bootstrap.Modal.getOrCreateInstance`.

**Tech Stack:** Jinja2, Bootstrap 5.3.2 modal, Bootstrap Icons, `--db-*` tokens, pytest file-contract tests, `uv run --frozen`.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-09-11-workspace-map-modal-design.md`
- Menu source of truth is `src/front/config/menu_config.json` only
- Skip `group.sidebar_only` and Domain group `domain-design`
- Column order: Domain, Ontology, Mapping, Knowledge Graph
- Icons: Domain `bi-box`, Ontology `bi-bezier2`, Mapping `bi-shuffle`, Knowledge Graph `bi-radar`
- No Settings / Registry / Help / Save / Switch / Close in the map
- No inline CSS/JS; no `alert`/`confirm`/`prompt`; `--db-*` tokens only
- L2 segmented control and dropdowns stay unchanged
- Tests: `uv run --frozen pytest -q -m "not scenario"`
- Changelog in `changelogs/v0.9.0/` in English

## File map

- Create: `src/front/templates/partials/layout/_workspace_map_modal.html`
- Create: `tests/units/front/test_workspace_map_modal.py`
- Modify: `src/front/templates/base.html` (include modal)
- Modify: `src/front/static/global/css/components.css` (map styles)
- Modify: `src/front/static/global/js/navbar.js` (trigger, title sync, current highlight)
- Modify: `.cursor/11-frontend-design.mdc` (L1 badge opens the map)
- Modify: `changelogs/v0.9.0/benoitcayladbx_2026-09-11.log`

---

### Task 1: Markup contract and partial

**Files:**
- Create: `tests/units/front/test_workspace_map_modal.py`
- Create: `src/front/templates/partials/layout/_workspace_map_modal.html`
- Modify: `src/front/templates/base.html`

**Interfaces:**
- Consumes: `menu_config` Jinja context already injected for `base.html`
- Produces: `#workspaceMapModal` with four `[data-workspace-map-col]` columns (`domain`, `ontology`, `assignment`, `digitaltwin`), items as `<a data-workspace-map-item href="...">`

- [ ] **Step 1: Write the failing tests**

Create `tests/units/front/test_workspace_map_modal.py`:

```python
"""Workspace map modal: L1 Domain badge overlay of L2 menus."""

import json
import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
BASE_HTML = REPO_ROOT / "src/front/templates/base.html"
PARTIAL = (
    REPO_ROOT / "src/front/templates/partials/layout/_workspace_map_modal.html"
)
NAVBAR_JS = REPO_ROOT / "src/front/static/global/js/navbar.js"
COMPONENTS_CSS = REPO_ROOT / "src/front/static/global/css/components.css"
MENU_CONFIG = REPO_ROOT / "src/front/config/menu_config.json"

_COL_ICONS = {
    "domain": "bi-box",
    "ontology": "bi-bezier2",
    "assignment": "bi-shuffle",
    "digitaltwin": "bi-radar",
}


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _menus():
    return {m["id"]: m for m in json.loads(_read(MENU_CONFIG))["menus"]}


def test_base_includes_workspace_map_modal():
    html = _read(BASE_HTML)
    assert 'partials/layout/_workspace_map_modal.html' in html


def test_modal_root_and_four_columns():
    html = _read(PARTIAL)
    assert 'id="workspaceMapModal"' in html
    assert 'modal-dialog-centered modal-xl' in html
    ids = re.findall(r'data-workspace-map-col="([^"]+)"', html)
    assert ids == ["domain", "ontology", "assignment", "digitaltwin"]


def test_column_icons_match_menu_config():
    html = _read(PARTIAL)
    menus = _menus()
    for col_id, icon in _COL_ICONS.items():
        assert menus[col_id]["icon"] == icon
        assert (
            f'data-workspace-map-col="{col_id}"' in html
            and f'bi {icon}' in html
        )


def test_ontology_designer_and_kg_explorer_routes():
    html = _read(PARTIAL)
    assert "/ontology/?section=map" in html
    assert "/dtwin/?section=sigmagraph" in html
    assert "/domain/?section=information" in html


def test_domain_sidebar_only_w3c_omitted():
    html = _read(PARTIAL)
    assert "owl-content" not in html
    domain_col = html.split('data-workspace-map-col="domain"')[1].split(
        'data-workspace-map-col="ontology"'
    )[0]
    assert "r2rml" not in domain_col.lower() or "/domain/?section=r2rml" not in domain_col


def test_mapping_and_kg_carry_graph_gate():
    html = _read(PARTIAL)
    mapping = html.split('data-workspace-map-col="assignment"')[1].split(
        'data-workspace-map-col="digitaltwin"'
    )[0]
    kg = html.split('data-workspace-map-col="digitaltwin"')[1]
    assert "nav-requires-graph" in mapping
    assert "nav-requires-graph" in kg
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run --frozen pytest -q tests/units/front/test_workspace_map_modal.py -m "not scenario"`

Expected: FAIL — `PARTIAL` missing / `base.html` has no include.

- [ ] **Step 3: Add the partial**

Create `src/front/templates/partials/layout/_workspace_map_modal.html`:

```html
{# Workspace map — opened from #domainL1Link. Driven by menu_config.json. #}
{% set _map_ids = ['domain', 'ontology', 'assignment', 'digitaltwin'] %}
{% set _map_menus = namespace(v={}) %}
{% for m in menu_config.menus %}
    {% if m.id in _map_ids %}
        {% set _ = _map_menus.v.update({m.id: m}) %}
    {% endif %}
{% endfor %}
<div class="modal fade" id="workspaceMapModal" tabindex="-1"
     aria-labelledby="workspaceMapTitle" aria-hidden="true">
    <div class="modal-dialog modal-dialog-centered modal-xl modal-dialog-scrollable">
        <div class="modal-content ob-workspace-map">
            <div class="modal-header">
                <h5 class="modal-title" id="workspaceMapTitle">
                    <i class="bi bi-box me-2"></i>
                    <span id="workspaceMapDomainLabel">Domain</span>
                </h5>
                <button type="button" class="btn-close" data-bs-dismiss="modal"
                        aria-label="Close"></button>
            </div>
            <div class="modal-body p-0">
                <div class="ob-workspace-map-grid">
                {% for col_id in _map_ids %}
                {% set menu = _map_menus.v.get(col_id) %}
                {% if menu %}
                <section class="ob-workspace-map-col" data-workspace-map-col="{{ menu.id }}">
                    <h6 class="ob-workspace-map-col-head">
                        <i class="bi {{ menu.icon }}"></i>
                        <span>{{ menu.label }}</span>
                    </h6>
                    {% for group in menu.groups %}
                    {% if not group.sidebar_only|default(false)
                          and group.id != 'domain-design' %}
                    {% if group.title %}
                    <div class="ob-workspace-map-group">
                        <i class="bi {{ group.icon }}"></i>
                        {{ group.title }}
                    </div>
                    {% endif %}
                    {% for item in group['items'] %}
                    <a class="ob-workspace-map-item{% if item.requires == 'domain_saved' %} dropdown-requires-domain{% endif %}{% if item.hidden_in_view_mode|default(false) %} view-mode-hidden{% endif %}{% if menu.id in ['assignment', 'digitaltwin'] %} nav-requires-graph{% endif %}"
                       data-workspace-map-item
                       {% if item.default|default(false) %}data-workspace-map-default="true"{% endif %}
                       href="{{ item.route if item.route else menu.route ~ '?section=' ~ item.id }}">
                        <i class="bi {{ item.icon }}"></i>
                        {{ item.label }}
                    </a>
                    {% endfor %}
                    {% endif %}
                    {% endfor %}
                </section>
                {% endif %}
                {% endfor %}
                </div>
            </div>
        </div>
    </div>
</div>
```

Jinja note: `{% set _ = _map_menus.v.update(...) %}` may not work on all Jinja. If the dict update pattern is unused in this repo, replace the loop with four `{% set %}` namespaces like `base.html` already does for `ontology_m` / `mapping_m` / `kg_m` / `domain_m`, then iterate a list of those four menu objects. Prefer that existing namespace pattern if `update` fails a template render.

Safer implementation matching `base.html`:

```html
{% set ontology_m = namespace(v=none) %}
{% set mapping_m  = namespace(v=none) %}
{% set kg_m       = namespace(v=none) %}
{% set domain_m   = namespace(v=none) %}
{% for m in menu_config.menus %}
    {% if m.id == 'ontology'   %}{% set ontology_m.v = m %}{% endif %}
    {% if m.id == 'assignment' %}{% set mapping_m.v  = m %}{% endif %}
    {% if m.id == 'digitaltwin'%}{% set kg_m.v       = m %}{% endif %}
    {% if m.id == 'domain'     %}{% set domain_m.v   = m %}{% endif %}
{% endfor %}
```

Then `{% set map_cols = [domain_m.v, ontology_m.v, mapping_m.v, kg_m.v] %}` and `{% for menu in map_cols %}`.

- [ ] **Step 4: Include from `base.html`**

After `{% include "partials/layout/registry_modal.html" %}` add:

```html
    {% include "partials/layout/_workspace_map_modal.html" %}
```

- [ ] **Step 5: Re-run tests**

Run: `uv run --frozen pytest -q tests/units/front/test_workspace_map_modal.py -m "not scenario"`

Expected: the six markup tests PASS. JS/CSS tests are not in this file yet.

- [ ] **Step 6: Commit**

```bash
git add tests/units/front/test_workspace_map_modal.py \
  src/front/templates/partials/layout/_workspace_map_modal.html \
  src/front/templates/base.html
git commit -m "$(cat <<'EOF'
feat(ui): add workspace map modal markup from menu config

EOF
)"
```

---

### Task 2: Map styles

**Files:**
- Modify: `tests/units/front/test_workspace_map_modal.py`
- Modify: `src/front/static/global/css/components.css`

**Interfaces:**
- Consumes: `#workspaceMapModal .ob-workspace-map-grid` markup from Task 1
- Produces: `.ob-workspace-map*` rules using `--db-*` tokens

- [ ] **Step 1: Add failing CSS contract tests** to `test_workspace_map_modal.py`:

```python
def test_workspace_map_css_uses_tokens():
    css = _read(COMPONENTS_CSS)
    assert ".ob-workspace-map-grid" in css
    assert ".ob-workspace-map-item.is-current" in css
    assert "--db-hover-indigo" in css[css.index(".ob-workspace-map") :]
    assert "--db-primary-light" in css[css.index(".ob-workspace-map") :]
    block = css[css.index(".ob-workspace-map") :]
    assert not re.search(r"#[0-9A-Fa-f]{3,8}", block.split("/* ===")[0] if False else block[:2500])
```

Keep the hex assertion local to the new block: slice from `.ob-workspace-map` until the next `/* ===` section header (or EOF). Fail if that slice contains `#` colour literals.

- [ ] **Step 2: Run the new test — expect FAIL** (`components.css` has no `.ob-workspace-map-grid`)

Run: `uv run --frozen pytest -q tests/units/front/test_workspace_map_modal.py::test_workspace_map_css_uses_tokens -m "not scenario"`

- [ ] **Step 3: Append styles to `components.css`**

```css
/* ==========================================================================
   Workspace map modal (.ob-workspace-map)
   Four-column L2 directory opened from #domainL1Link.
   ========================================================================== */

.ob-workspace-map .modal-header {
    border-bottom: 1px solid var(--db-border);
}

.ob-workspace-map-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
}

.ob-workspace-map-col {
    padding: 0.875rem 1rem 1.125rem;
    border-right: 1px solid var(--db-border);
    min-width: 0;
}

.ob-workspace-map-col:last-child {
    border-right: none;
}

.ob-workspace-map-col-head {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    font-size: 0.82rem;
    font-weight: 600;
    color: var(--db-primary-darker);
    padding-bottom: 0.625rem;
    margin: 0 0 0.375rem;
    border-bottom: 1px solid var(--db-border);
}

.ob-workspace-map-col-head i {
    font-size: 0.95rem;
}

.ob-workspace-map-group {
    display: flex;
    align-items: center;
    gap: 0.375rem;
    font-size: 0.65rem;
    letter-spacing: 1px;
    text-transform: uppercase;
    color: var(--db-text-muted);
    font-weight: 600;
    margin: 0.75rem 0 0.25rem;
}

.ob-workspace-map-item {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    font-size: 0.82rem;
    color: var(--db-text);
    text-decoration: none;
    padding: 0.3rem 0.5rem;
    border-radius: 9px;
    line-height: 1.25;
}

.ob-workspace-map-item i {
    width: 16px;
    text-align: center;
    color: var(--db-text-muted);
    font-size: 0.9rem;
}

.ob-workspace-map-item:hover {
    background: var(--db-hover-indigo);
    color: var(--db-primary-darker);
}

.ob-workspace-map-item:hover i {
    color: var(--db-primary);
}

.ob-workspace-map-item.is-current {
    background: var(--db-primary-light);
    color: var(--db-primary-darker);
    font-weight: 600;
}

.ob-workspace-map-item.is-current i {
    color: var(--db-primary);
}

.ob-workspace-map-item:focus-visible {
    outline: none;
    box-shadow: var(--db-focus-ring);
}

@media (max-width: 767.98px) {
    .ob-workspace-map-grid {
        grid-template-columns: 1fr 1fr;
    }

    .ob-workspace-map-col:nth-child(2) {
        border-right: none;
    }
}
```

- [ ] **Step 4: Re-run CSS test — expect PASS**

Run: `uv run --frozen pytest -q tests/units/front/test_workspace_map_modal.py::test_workspace_map_css_uses_tokens -m "not scenario"`

- [ ] **Step 5: Commit**

```bash
git add tests/units/front/test_workspace_map_modal.py \
  src/front/static/global/css/components.css
git commit -m "$(cat <<'EOF'
feat(ui): style workspace map with shell tokens

EOF
)"
```

---

### Task 3: Open from the Domain badge

**Files:**
- Modify: `tests/units/front/test_workspace_map_modal.py`
- Modify: `src/front/static/global/js/navbar.js`

**Interfaces:**
- Consumes: `#workspaceMapModal`, `#workspaceMapDomainLabel`, `#currentDomainName`, `[data-workspace-map-item]`
- Produces:
  - `openWorkspaceMap(event)` — `preventDefault`, sync title, highlight current, `bootstrap.Modal.getOrCreateInstance(el).show()`
  - `highlightWorkspaceMapCurrent()`
  - `syncWorkspaceMapTitle()`
  - `bindWorkspaceMapTrigger()` called from `initNavbar()`

- [ ] **Step 1: Add failing JS contract tests**

```python
def test_navbar_opens_workspace_map_from_domain_badge():
    js = _read(NAVBAR_JS)
    assert "function openWorkspaceMap(" in js
    assert "function bindWorkspaceMapTrigger(" in js
    assert "bindWorkspaceMapTrigger()" in js
    assert "event.preventDefault()" in js[js.index("function openWorkspaceMap(") :][:800]
    assert "Modal.getOrCreateInstance" in js[js.index("function openWorkspaceMap(") :][:800]
    assert "domainL1Link" in js[js.index("function bindWorkspaceMapTrigger(") :][:600]


def test_navbar_highlights_current_workspace_map_item():
    js = _read(NAVBAR_JS)
    assert "function highlightWorkspaceMapCurrent(" in js
    assert "data-workspace-map-item" in js
    assert "is-current" in js
    assert "data-workspace-map-default" in js


def test_navbar_syncs_workspace_map_title():
    js = _read(NAVBAR_JS)
    assert "workspaceMapDomainLabel" in js
    assert "function syncWorkspaceMapTitle(" in js
```

- [ ] **Step 2: Run — expect FAIL**

Run: `uv run --frozen pytest -q tests/units/front/test_workspace_map_modal.py::test_navbar_opens_workspace_map_from_domain_badge tests/units/front/test_workspace_map_modal.py::test_navbar_highlights_current_workspace_map_item tests/units/front/test_workspace_map_modal.py::test_navbar_syncs_workspace_map_title -m "not scenario"`

- [ ] **Step 3: Implement in `navbar.js`**

Add after `initNavbar`'s `initSubnavActiveState();`:

```javascript
    bindWorkspaceMapTrigger();
```

Add these functions (near `initSubnavActiveState`):

```javascript
function bindWorkspaceMapTrigger() {
    const link = document.getElementById('domainL1Link');
    if (!link) return;
    link.addEventListener('click', openWorkspaceMap);
}

function openWorkspaceMap(event) {
    if (event) event.preventDefault();
    const el = document.getElementById('workspaceMapModal');
    if (!el || typeof bootstrap === 'undefined' || !bootstrap.Modal) return;
    syncWorkspaceMapTitle();
    highlightWorkspaceMapCurrent();
    bootstrap.Modal.getOrCreateInstance(el).show();
}

function syncWorkspaceMapTitle() {
    const src = document.getElementById('currentDomainName');
    const dest = document.getElementById('workspaceMapDomainLabel');
    if (!src || !dest) return;
    dest.textContent = src.textContent;
    const srcBadge = src.parentNode && src.parentNode.querySelector('.domain-status-badge');
    applyDomainStatusBadge(dest, srcBadge ? srcBadge.textContent : null);
}

function highlightWorkspaceMapCurrent() {
    const path = window.location.pathname;
    const section = new URLSearchParams(window.location.search).get('section')
        || (window.location.hash || '').replace(/^#/, '');
    document.querySelectorAll('[data-workspace-map-item]').forEach((anchor) => {
        anchor.classList.remove('is-current');
        const href = anchor.getAttribute('href') || '';
        let url;
        try {
            url = new URL(href, window.location.origin);
        } catch (_) {
            return;
        }
        if (!path.startsWith(url.pathname)) return;
        const want = url.searchParams.get('section') || '';
        const isMatch = want
            ? section === want
            : !section;
        const isDefault = !section && anchor.getAttribute('data-workspace-map-default') === 'true';
        if (isMatch || isDefault) {
            anchor.classList.add('is-current');
        }
    });
}

window.openWorkspaceMap = openWorkspaceMap;
```

In `applyDomainInfo`, after updating `currentDomainNameEl`, call `syncWorkspaceMapTitle()` so the modal title stays current if the map is already in the DOM.

`applyDomainStatusBadge` currently maps status enums (`DRAFT`, …). If `syncWorkspaceMapTitle` passes badge *label* text (`Draft`), either:

- pass the raw status from `applyDomainInfo` (`status` variable) into `applyDomainStatusBadge(dest, hasDomain ? status : null)`, or
- skip badge copy in `syncWorkspaceMapTitle` and call `applyDomainStatusBadge(workspaceMapDomainLabel, status)` from `applyDomainInfo` only.

Prefer the second: in `applyDomainInfo`, after the L1 name update:

```javascript
    const mapLabel = document.getElementById('workspaceMapDomainLabel');
    if (mapLabel) {
        mapLabel.textContent = hasDomain ? `${domainName} V${version}` : 'Domain';
        applyDomainStatusBadge(mapLabel, hasDomain ? status : null);
    }
```

Then `syncWorkspaceMapTitle` can just copy `currentDomainNameEl.textContent` and call `applyDomainStatusBadge` with the same `status` only if you thread it through. Simplest: `syncWorkspaceMapTitle` copies text; `applyDomainInfo` owns both labels and both badges. `openWorkspaceMap` still calls `syncWorkspaceMapTitle` as a no-op-safe copy of the L1 text (badge already set by `applyDomainInfo`).

Fix `syncWorkspaceMapTitle` to **only** copy `textContent` (no status parsing):

```javascript
function syncWorkspaceMapTitle() {
    const src = document.getElementById('currentDomainName');
    const dest = document.getElementById('workspaceMapDomainLabel');
    if (src && dest) dest.textContent = src.textContent;
}
```

- [ ] **Step 4: Re-run JS tests — expect PASS**

Run: `uv run --frozen pytest -q tests/units/front/test_workspace_map_modal.py -m "not scenario"`

- [ ] **Step 5: Commit**

```bash
git add tests/units/front/test_workspace_map_modal.py \
  src/front/static/global/js/navbar.js
git commit -m "$(cat <<'EOF'
feat(ui): open workspace map from the domain badge

EOF
)"
```

---

### Task 4: Design-system doc and changelog

**Files:**
- Modify: `.cursor/11-frontend-design.mdc`
- Modify: `changelogs/v0.9.0/benoitcayladbx_2026-09-11.log`

**Interfaces:**
- Consumes: shipped `#workspaceMapModal` behaviour
- Produces: documented L1 badge exception; changelog section

- [ ] **Step 1: Update Two-Level Navigation — L1 Navbar**

In `.cursor/11-frontend-design.mdc`, after the `#domainL1Link` disabled-when-no-domain bullet, add:

- `#domainL1Link` click opens `#workspaceMapModal` (four-column map of Domain / Ontology / Mapping / Knowledge Graph menus from `menu_config.json`). It does not navigate to `/domain/`. Markup: `partials/layout/_workspace_map_modal.html`. Behaviour: `openWorkspaceMap` in `navbar.js`. L2 dropdowns remain.

Also add the modal to the `base.html` ownership table (with Help / Registry).

- [ ] **Step 2: Append changelog**

```
## Add workspace map modal on Domain badge click

Context: The L1 Domain + version badge navigated to /domain/. Users needed
one overlay of all L2 workspace menus and sub-menus.

Changes:

1. src/front/templates/partials/layout/_workspace_map_modal.html
   Four-column directory from menu_config.json (same groups as L2).
2. src/front/templates/base.html
   Include the workspace map modal in the app chrome.
3. src/front/static/global/css/components.css
   `.ob-workspace-map*` layout, hover, and current-item styles.
4. src/front/static/global/js/navbar.js
   Badge click opens the modal; sync title; highlight current section.
5. tests/units/front/test_workspace_map_modal.py
   Markup, icon, gate, CSS token, and navbar wiring contracts.
6. .cursor/11-frontend-design.mdc
   Document the L1 badge → workspace map exception.

Modified files:
- src/front/templates/partials/layout/_workspace_map_modal.html
- src/front/templates/base.html
- src/front/static/global/css/components.css
- src/front/static/global/js/navbar.js
- tests/units/front/test_workspace_map_modal.py
- .cursor/11-frontend-design.mdc
- changelogs/v0.9.0/benoitcayladbx_2026-09-11.log

Tests: (paste `uv run --frozen pytest -q -m "not scenario"` summary)
```

- [ ] **Step 3: Run the full non-scenario suite**

Run: `uv run --frozen pytest -q -m "not scenario"`

Expected: all previous tests still pass; new file included.

- [ ] **Step 4: Commit**

```bash
git add .cursor/11-frontend-design.mdc \
  changelogs/v0.9.0/benoitcayladbx_2026-09-11.log
git commit -m "$(cat <<'EOF'
docs: record workspace map modal in design system

EOF
)"
```

---

## Spec coverage

| Spec requirement | Task |
| --- | --- |
| Badge click opens modal, no `/domain/` navigation | 3 |
| Four columns, L2 groups, icons, skip sidebar_only | 1 |
| `--db-*` hover/current, 2-col mobile | 2 |
| Current section highlight + default fallback | 3 |
| Graph / UC / view-mode classes | 1 (markup) + existing navbar permission scans |
| Title = domain name + version | 3 |
| Tests + 11-frontend-design + changelog | 1–4 |
