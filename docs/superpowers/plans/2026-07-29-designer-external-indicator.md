# Designer Entity External-Link Indicator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show a small badge on the top-right corner of an entity's icon in the Ontology Designer canvas whenever that class already has a Dashboard, Dataset, Actions, or Bridges configured under its entity panel's "External" tab.

**Architecture:** Compute a boolean `hasExternal` flag per class from four already-existing fields (`dashboard`, `dataset`, `actions`, `bridges`), thread it into the `Entity` object at all three places the Designer builds entities from ontology classes, and render a small overlay badge in `_renderEntity` only when the flag is true. Pure frontend, vanilla JS + CSS, no backend changes.

**Tech Stack:** Vanilla JS (`ontoviz.js`, `ontology-design.js`), CSS (`ontoviz-entity.css`), Bootstrap Icons.

## Global Constraints

- Designer canvas only — do not touch `ontology-map.js` or `ontology-entities.js` (spec §Scope).
- No tooltip, no click handler on the badge — visual signal only (spec §Scope).
- OntoViz CSS is a greyscale-only, `--ovz-*`-tokens-only scope — never hardcode a hex colour (`.cursor/11-frontend-design.mdc`).
- Bootstrap Icons only, no emoji-as-icon for any *new* UI element (`.cursor/11-frontend-design.mdc`) — the existing entity emoji icon itself is untouched/out of scope for this rule.
- "Has externals" means: `!!(cls.dashboard || cls.dataset || (cls.actions || []).length || (cls.bridges || []).length)` — exactly this expression, reused verbatim at all three sites.
- There is no JS DOM-rendering test harness for the Designer canvas today — verification for this plan is manual (spec §Testing), backed by the existing `uv run pytest -q -m "not scenario"` regression run.
- After the change, update `changelogs/v0.7.0/benoitcayladbx_<today>.log` per `.cursorrules` (append a new section; the file already exists for today).

---

### Task 1: CSS badge styles

**Files:**
- Modify: `src/front/static/global/ontoviz/css/ontoviz-entity.css:48-57`

**Interfaces:**
- Produces: CSS class `.ovz-entity-external-badge` and `position: relative` on `.ovz-entity-icon`, consumed by Task 2's markup.

- [ ] **Step 1: Add `position: relative` to `.ovz-entity-icon` and a new `.ovz-entity-external-badge` rule**

Current block:

```css
.ovz-entity-icon {
    font-size: 20px;
    cursor: pointer;
    transition: transform var(--ovz-transition);
    line-height: 1;
}

.ovz-entity-icon:hover {
    transform: scale(1.15);
}
```

Replace with:

```css
.ovz-entity-icon {
    position: relative;
    font-size: 20px;
    cursor: pointer;
    transition: transform var(--ovz-transition);
    line-height: 1;
}

.ovz-entity-icon:hover {
    transform: scale(1.15);
}

/* Badge shown on entities that have a Dashboard, Dataset, Actions, or
   Bridges configured under the entity panel's "External" tab. */
.ovz-entity-external-badge {
    position: absolute;
    top: -6px;
    right: -7px;
    width: 16px;
    height: 16px;
    border-radius: 4px;
    background: var(--ovz-accent-purple);
    color: #fff;
    border: 2px solid var(--ovz-entity-header-bg);
    font-size: 9px;
    display: flex;
    align-items: center;
    justify-content: center;
    pointer-events: none;
}

.ovz-entity-external-badge i {
    font-size: 9px;
    line-height: 1;
}
```

- [ ] **Step 2: Verify no linter errors**

Run: read the file back or use the workspace linter on `src/front/static/global/ontoviz/css/ontoviz-entity.css`.
Expected: no errors (plain CSS, existing tokens only).

- [ ] **Step 3: Commit**

```bash
git add src/front/static/global/ontoviz/css/ontoviz-entity.css
git commit -m "feat(ontoviz): add external-badge CSS for entity icons"
```

---

### Task 2: `Entity` field + badge rendering in `ontoviz.js`

**Files:**
- Modify: `src/front/static/global/ontoviz/ontoviz.js:41-60` (constructor)
- Modify: `src/front/static/global/ontoviz/ontoviz.js:1196-1234` (`_renderEntity` icon markup)

**Interfaces:**
- Consumes: `.ovz-entity-external-badge` CSS class from Task 1.
- Produces: `Entity.hasExternal` (boolean, defaults `false`) — consumed by Task 3's `addEntity({..., hasExternal})` calls.

- [ ] **Step 1: Store `hasExternal` on the `Entity` instance**

Current constructor tail:

```javascript
            this.color = options.color || null; // Use default if null
            this.collapsed = options.collapsed || false; // Header-only display when true
            this.element = null;
        }
```

Replace with:

```javascript
            this.color = options.color || null; // Use default if null
            this.collapsed = options.collapsed || false; // Header-only display when true
            // True when the backing class has a Dashboard, Dataset, Actions,
            // or Bridges configured under the entity panel's External tab.
            this.hasExternal = options.hasExternal || false;
            this.element = null;
        }
```

- [ ] **Step 2: Render the badge inside the icon span**

Current code (icon-click attribute + header markup):

```javascript
            // Icon - clickable only in edit mode
            const iconAttr = isViewOnly ? '' : 'data-action="edit-icon" title="Click to change icon"';

            // Collapse / expand toggle (available in both view and edit modes)
            const collapseBtnHTML = `
```

Replace with:

```javascript
            // Icon - clickable only in edit mode
            const iconAttr = isViewOnly ? '' : 'data-action="edit-icon" title="Click to change icon"';

            // Small badge overlay signalling the class has Dashboard/Dataset/
            // Actions/Bridges configured (entity panel's "External" tab).
            const externalBadgeHTML = entity.hasExternal
                ? '<span class="ovz-entity-external-badge"><i class="bi bi-link-45deg"></i></span>'
                : '';

            // Collapse / expand toggle (available in both view and edit modes)
            const collapseBtnHTML = `
```

Then, in the `el.innerHTML` template, current line:

```javascript
                    <span class="ovz-entity-icon" ${iconAttr}>${entity.icon || '📦'}</span>
```

Replace with:

```javascript
                    <span class="ovz-entity-icon" ${iconAttr}>${entity.icon || '📦'}${externalBadgeHTML}</span>
```

- [ ] **Step 3: Verify no linter errors**

Run: read the file back or use the workspace linter on `src/front/static/global/ontoviz/ontoviz.js`.
Expected: no errors introduced by the two edits above.

- [ ] **Step 4: Manual smoke check**

Run: `./scripts/start.sh` (skip if already running — check the terminals folder), open any domain with at least one ontology class in the Design tab, open the browser devtools console, and run:

```javascript
ontologyDesigner.addEntity({ name: 'ManualCheck', icon: '🧪', hasExternal: true, x: 50, y: 50 });
```

Expected: a new "ManualCheck" entity card appears with a small dark badge (link icon) on the top-right of its 🧪 icon. Delete the manual entity afterward (right-click → Delete, or refresh without saving).

- [ ] **Step 5: Commit**

```bash
git add src/front/static/global/ontoviz/ontoviz.js
git commit -m "feat(ontoviz): render external-link badge on entities with hasExternal"
```

---

### Task 3: Compute and thread `hasExternal` from ontology classes

**Files:**
- Modify: `src/front/static/global/js/ontology-design.js:1591-1602` (primary merge path)
- Modify: `src/front/static/global/js/ontology-design.js:1743-1749` (fallback enrich path)
- Modify: `src/front/static/global/js/ontology-design.js:1819-1826` (fresh-layout path)

**Interfaces:**
- Consumes: `Entity.hasExternal` / `addEntity({hasExternal})` from Task 2.
- Produces: nothing further downstream — this is the final wiring task.

- [ ] **Step 1: Primary path — saved layout + ontology classes merge**

Current code:

```javascript
                return {
                    id: savedEntity?.id || undefined,
                    name: cls.name || cls.localName,
                    label: cls.label || cls.name || cls.localName,
                    icon: cls.emoji || cls.icon || '📦',
                    description: cls.description || '',
                    x: posX,
                    y: posY,
                    properties: ownProperties.map(dp => ({
                        name: dp.name || dp.localName || 'attribute'
                    }))
                };
```

Replace with:

```javascript
                return {
                    id: savedEntity?.id || undefined,
                    name: cls.name || cls.localName,
                    label: cls.label || cls.name || cls.localName,
                    icon: cls.emoji || cls.icon || '📦',
                    description: cls.description || '',
                    hasExternal: !!(cls.dashboard || cls.dataset || (cls.actions || []).length || (cls.bridges || []).length),
                    x: posX,
                    y: posY,
                    properties: ownProperties.map(dp => ({
                        name: dp.name || dp.localName || 'attribute'
                    }))
                };
```

- [ ] **Step 2: Fallback path — saved layout only, no ontology classes loaded yet**

Current code:

```javascript
            savedLayout.entities.forEach(entity => {
                const cls = classMap.get(entity.name);
                if (cls) {
                    entity.icon = cls.emoji || cls.icon || entity.icon || '📦';
                    entity.description = cls.description || entity.description || '';
                }
            });
```

Replace with:

```javascript
            savedLayout.entities.forEach(entity => {
                const cls = classMap.get(entity.name);
                if (cls) {
                    entity.icon = cls.emoji || cls.icon || entity.icon || '📦';
                    entity.description = cls.description || entity.description || '';
                    entity.hasExternal = !!(cls.dashboard || cls.dataset || (cls.actions || []).length || (cls.bridges || []).length);
                }
            });
```

- [ ] **Step 3: Fresh-layout path — no saved layout at all**

Current code:

```javascript
            const entity = ontologyDesigner.addEntity({
                name: cls.name || cls.localName || `Class_${index}`,
                icon: cls.emoji || cls.icon || '📦',
                description: cls.description || '',
                x: x,
                y: y,
                properties: dataProps
            });
```

Replace with:

```javascript
            const entity = ontologyDesigner.addEntity({
                name: cls.name || cls.localName || `Class_${index}`,
                icon: cls.emoji || cls.icon || '📦',
                description: cls.description || '',
                hasExternal: !!(cls.dashboard || cls.dataset || (cls.actions || []).length || (cls.bridges || []).length),
                x: x,
                y: y,
                properties: dataProps
            });
```

- [ ] **Step 4: Verify no linter errors**

Run: read the file back or use the workspace linter on `src/front/static/global/js/ontology-design.js`.
Expected: no errors introduced by the three edits above.

- [ ] **Step 5: Manual verification (all three paths + regressions)**

With the dev server running (`./scripts/start.sh`):

1. Open a domain, go to the Entities tab, open a class, go to its External tab, and assign a Dataset (or Dashboard/Action/Bridge) to at least one class. Save.
2. Open the Design tab. Confirm the badge appears only on that class's icon (primary merge path — Step 1).
3. Reload the page and reopen the Design tab. Confirm the badge is still there (still exercises the primary merge path, proving it survives a real reload, not just an in-memory session).
4. Collapse/expand that entity, drag it, add a property, and change its icon. Confirm the badge stays in place through every re-render.
5. In edit mode, click the entity's icon to open the icon picker. Confirm the click still works (the badge has `pointer-events: none`, so it must not intercept the click).
6. Pick a class with none of the four fields set. Confirm it has no badge.
7. If you have a domain with no saved layout yet (or use "Reset Layout" if available), regenerate the layout and confirm the badge still appears correctly on classes that have externals set (fresh-layout path — Step 3).

Expected: badge present exactly where `hasExternal` is true, in every path and through every re-render; no regressions to existing icon-click, collapse, drag, or property editing.

- [ ] **Step 6: Run the full regression suite**

Run: `uv run pytest -q -m "not scenario"`
Expected: same pass count as the prior run in today's changelog (3436 passed, 275 skipped, 5 deselected) or higher — no new failures. This is a safety net only; no backend code changed.

- [ ] **Step 7: Update the changelog**

Append a new section to `changelogs/v0.7.0/benoitcayladbx_<today's date, YYYY-MM-DD>.log` (the file already exists for today — add a new `##` section, do not overwrite prior sections), following the exact structure of the existing sections in that file: title, context, numbered list of changes with file paths and descriptions, modified files list (including the changelog file itself), and the test result line from Step 6.

- [ ] **Step 8: Commit**

```bash
git add src/front/static/global/js/ontology-design.js changelogs/v0.7.0/benoitcayladbx_<today>.log
git commit -m "feat(ontology-design): thread hasExternal into designer entities"
```
