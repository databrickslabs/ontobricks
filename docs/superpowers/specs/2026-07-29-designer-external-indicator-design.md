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
   corner of the entity's emoji icon (Option B from visual review: a purple
   circular badge with a "link/reuse" glyph, white border for contrast
   against the icon background).
3. When false, no badge is rendered — the icon looks exactly as it does
   today.
4. The badge must survive re-renders that already exist today: collapse /
   expand, drag, add/edit property, icon change. It is computed once per
   entity at designer-load time and stored on the `Entity` instance, so any
   `_renderEntity` call naturally re-paints it consistently.

## Implementation

- `src/front/static/global/js/ontology-design.js`
  (`loadOntologyIntoDesigner`, ~line 1819): compute
  `hasExternal = !!(cls.dashboard || cls.dataset || (cls.actions || []).length || (cls.bridges || []).length)`
  per class and pass it into `ontologyDesigner.addEntity({..., hasExternal})`.
- `src/front/static/global/ontoviz/ontoviz.js`
  - `Entity` constructor (~line 41): store
    `this.hasExternal = options.hasExternal || false`.
  - `_renderEntity` (~line 1222): render
    `<span class="ovz-entity-external-badge" title="">↪</span>` (or an
    inline SVG/Bootstrap-icon glyph) inside `.ovz-entity-icon` only when
    `entity.hasExternal` is true. No `title` text needed since there's no
    tooltip behavior, but keep the span free of click handlers.
- `src/front/static/global/ontoviz/css/ontoviz-entity.css`
  - Add `position: relative;` to `.ovz-entity-icon` (currently has no
    positioning context).
  - Add `.ovz-entity-external-badge`: `position: absolute; top: -6px;
    right: -7px; width: 17px; height: 17px; border-radius: 5px; background:
    #7c3aed; color: #fff; border: 2px solid var(--ovz-entity-header-bg,
    #fff); font-size: 10px; display: flex; align-items: center;
    justify-content: center; pointer-events: none;` (matches the reviewed
    Option B mockup; the border color matches the entity header background
    it sits on so the badge reads as a clean overlay; `pointer-events: none`
    keeps the existing icon-click-to-edit-icon behavior working unchanged).

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
- Confirm collapse/expand, drag, add-property, and icon-change re-renders
  keep the badge in place.
- Confirm the icon's existing "click to change icon" behavior (edit mode)
  still works with the badge overlaid (`pointer-events: none` on the badge).
- Run `uv run pytest -q -m "not scenario"` to confirm no regressions (no
  backend/Python code is touched by this change, so this is a safety net).
