# Designer Entity External-Link Indicator

## Goal

Give a visual signal on entity cards in the Ontology Designer canvas that an
entity already has one or more items configured under its "External" panel
tab (Dashboard, Dataset, Actions, Bridges), without opening the panel.

## Scope

- Designer canvas only (`ontoviz.js` / `ontology-design.js`). Map view
  (`ontology-map.js`) and the Entities tree (`ontology-entities.js`) are out
  of scope for this change.
- This is unrelated to the (unbuilt) vocabulary-reuse feature described in
  `releasereq/optional/vocabulary_reuse.md` (`external` / `source_vocabulary`
  fields for reused ontology terms). That feature, if built later, would need
  its own indicator and is not affected by this spec.
- Icon-only signal: no tooltip and no click handler. The existing entity
  panel (External tab) remains the only way to inspect or edit these values.

## Behaviour

1. A class counts as "has externals" when at least one of these fields
   (already present on class objects, backing the existing entity panel's
   External tab) is non-empty:
   - `dashboard` (truthy string)
   - `dataset` (truthy)
   - `actions` (non-empty array)
   - `bridges` (non-empty array)
2. When true, the Designer renders a small badge overlaid on the top-right
   corner of the entity's emoji icon (Option B from visual review: a dark
   badge with a "link/reuse" glyph). Per `.cursor/11-frontend-design.mdc`,
   OntoViz is a **greyscale-only** token scope (no hardcoded hex) and the app
   forbids emoji-as-icon (Bootstrap Icons only), so Option B's shape/position
   is implemented with the existing `--ovz-accent-purple` token (`#333333`
   grey, not literal purple, per the theme) and the Bootstrap icon
   `bi-link-45deg` — the same glyph the entity panel already uses for
   "Assign" on the Dashboard field, keeping the visual language consistent.
3. When false, no badge is rendered — the icon looks exactly as it does
   today.
4. The badge must survive re-renders that already exist today: collapse /
   expand, drag, add/edit property, icon change. It is computed once per
   entity at designer-load time and stored on the `Entity` instance, so any
   `_renderEntity` call naturally re-paints it consistently.

## Implementation

`ontology-design.js` in `src/front/static/global/js/ontology-design.js`
has **four** distinct entity-construction sites — all four already thread
`icon`/`description` from `cls` the same way, so `hasExternal` follows the
identical pattern at each:

1. **Primary path — saved layout + ontology classes merge** (inside
   `loadOntologyIntoDesigner`, ~line 1578,
   `mergedLayout.entities = classes.map(cls => {...})`): the common case for
   any domain that's been opened before. Add `hasExternal: !!(cls.dashboard
   || cls.dataset || (cls.actions || []).length || (cls.bridges || []).length)`
   to the returned object literal (~line 1591-1602), alongside the existing
   `icon`/`description` fields.
2. **Fallback path — saved layout only, no ontology classes loaded yet**
   (inside `loadOntologyIntoDesigner`, ~line 1735,
   `savedLayout.entities.forEach(entity => { const cls =
   classMap.get(entity.name); if (cls) {...} })`): add
   `entity.hasExternal = !!(cls.dashboard || cls.dataset || (cls.actions ||
   []).length || (cls.bridges || []).length);` next to the existing
   `entity.icon` / `entity.description` assignment (~line 1746-1747).
3. **Fresh-layout path — no saved layout at all** (inside
   `loadOntologyIntoDesigner`, ~line 1819,
   `ontologyDesigner.addEntity({...})` inside `classes.forEach((cls, index)
   => {...})`): compute the same `hasExternal` expression and pass it into
   the `addEntity` call alongside `icon`/`description`.
4. **`_buildFreshDesignLayout`** — builds entity objects the same way but is
   called from `loadFromOntologyFresh()` and the create-business-view flow,
   bypassing `loadOntologyIntoDesigner` entirely. Add the same `hasExternal`
   field to the object returned inside its `classes.map(...)` alongside the
   existing `icon`/`description` fields.

Using the exact same field names (`dashboard`, `dataset`, `actions`,
`bridges`) that `src/front/static/global/js/ontology-design.js:465-469`
already reads/writes when converting the designer's state back to ontology
classes on save — confirming these are the live field names, not the
planned-but-unbuilt vocabulary-reuse fields.

- `src/front/static/global/ontoviz/ontoviz.js`
  - `Entity` constructor (~line 41): store
    `this.hasExternal = options.hasExternal || false`.
  - `_renderEntity` (~line 1222): render
    `<span class="ovz-entity-external-badge"><i class="bi bi-link-45deg"></i></span>`
    inside `.ovz-entity-icon` only when `entity.hasExternal` is true. No
    `title` text needed since there's no tooltip behavior, and no click
    handler is bound to it.
- `src/front/static/global/ontoviz/css/ontoviz-entity.css`
  - Add `position: relative;` to `.ovz-entity-icon` (currently has no
    positioning context).
  - Add `.ovz-entity-external-badge`: `position: absolute; top: -6px;
    right: -7px; width: 16px; height: 16px; border-radius: 4px; background:
    var(--ovz-accent-purple); color: #fff; border: 2px solid
    var(--ovz-entity-header-bg); font-size: 9px; display: flex;
    align-items: center; justify-content: center; pointer-events: none;`
    (`--ovz-accent-purple` and `--ovz-entity-header-bg` are existing tokens
    from `ontoviz-variables.css` — no new hardcoded colours, per the
    project's greyscale-only OntoViz token rule. The border colour matches
    the entity header background it sits on so the badge reads as a clean
    overlay. `pointer-events: none` keeps the existing icon-click-to-
    edit-icon behavior working unchanged).

## Error Handling

No new failure modes: `hasExternal` defaults to `false` via `||` guards, so
missing/undefined class fields never throw and simply omit the badge.

## Testing

There is no existing JS rendering-test harness for the Designer canvas
(confirmed: only a smoke test in `tests/units/api/test_ui_rendering.py` that
`ontoviz.js` is loaded as a script — no DOM assertions). This change stays
manual-verification:

- Load the Designer with a domain containing at least one class that has a
  dashboard, dataset, action, or bridge set, and at least one that has none —
  confirm the badge appears only on the former.
- Reload the page (or switch away from and back to the Design tab) on a
  domain that already has a saved layout, to exercise the primary merge path
  (site 1 above) — confirm the badge still appears, not just on first-ever
  layout generation (site 3).
- Confirm collapse/expand, drag, add-property, and icon-change re-renders
  keep the badge in place.
- Confirm the icon's existing "click to change icon" behavior (edit mode)
  still works with the badge overlaid (`pointer-events: none` on the badge).
- Run `uv run pytest -q -m "not scenario"` to confirm no regressions (no
  backend/Python code is touched by this change, so this is a safety net).
