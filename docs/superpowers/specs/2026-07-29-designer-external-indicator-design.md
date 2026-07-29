# Designer Entity External-Link Indicator

> **Correction (same day):** the first version of this spec targeted the
> wrong view. The Ontology app has two visually-similar but distinct
> sections: sidebar item **"Designer"** (`menu_config.json` id `map`) is the
> D3.js force-directed graph (`ontology-map.js` / `_ontology_map.html`,
> content heading "Ontology Designer"); sidebar item **"Business Views"**
> (id `design`) is the OntoViz canvas (`ontoviz.js` / `_ontology_design.html`,
> content heading "Visual Ontology Designer - Business Views"). The original
> request ("Ontology/designer") meant the former. The feature below was
> initially built in OntoViz/Business Views, then reverted and re-implemented
> in the D3 "Designer" view once the mix-up was caught. This spec has been
> rewritten to describe the actual (D3) implementation.

## Goal

Give a visual signal on entity nodes in the Ontology **Designer** (the D3.js
force-directed graph) that an entity already has one or more items
configured under its "External" panel tab (Dashboard, Dataset, Actions,
Bridges), without opening the panel.

## Scope

- Designer view only (`ontology-map.js` / `ontology-map.css`). The OntoViz
  "Business Views" canvas (`ontoviz.js`) and the Entities tree
  (`ontology-entities.js`) are out of scope for this change.
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
   of the node's emoji icon: a dark circle (`#333333`, matching the app's
   neutral badge/dark-accent convention) with a small white "link" glyph
   (Bootstrap Icons `bi-link-45deg`, codepoint `\uf470` — the same glyph the
   entity panel already uses for "Assign" on the Dashboard field). Since this
   view renders nodes as raw SVG (not HTML/CSS cards), the glyph is drawn via
   an SVG `<text>` using the `bootstrap-icons` webfont directly rather than an
   `<i class="bi ...">` element.
3. When false, no badge is rendered — the icon looks exactly as it does
   today.
4. The badge is recomputed on every `initOntologyMap()` call (full SVG
   rebuild from `OntologyState.config.classes`), which already runs on every
   navigation to the Designer section and after every entity-panel save while
   the Designer is the active section (`ontology-shared-panels.js`). There is
   no saved-layout persistence of class fields and no change-detection
   fingerprint to go stale here — unlike the OntoViz canvas, this view has no
   analogous "badge only updates after reload" risk.

## Implementation

- `src/front/static/ontology/js/ontology-map.js`
  - `initOntologyMap()`'s `classes.map(...)` (builds the `nodes` array): add
    `hasExternal: !!(cls.dashboard || cls.dataset || (cls.actions ||
    []).length || (cls.bridges || []).length)` alongside the existing `icon`
    field. This is the *only* entity-construction site in this file (no
    saved-layout entity merge like OntoViz — the map only persists
    `positions`).
  - After the existing `.map-node-icon` / `.map-node-label` `<text>`
    appends: `const externalBadgeNodes = nodeElements.filter(d =>
    d.hasExternal);` then append a `<circle class="map-node-external-badge-bg">`
    (`cx=14, cy=-14, r=7`) and a `<text class="map-node-external-badge-icon">`
    (same `x`/`y`, text content `'\uf470'`) to `externalBadgeNodes` only —
    nodes without the flag get no extra DOM at all. `aria-label="Has external
    configuration"` on the circle plus `aria-hidden="true"` on the text
    (decorative duplicate) for accessibility; no `<title>` (would add a
    native SVG tooltip) and no `.on('click', ...)` handler.
- `src/front/static/ontology/css/ontology-map.css`
  - `.map-node-external-badge-bg { fill: #333333; stroke: #fff; stroke-width:
    2px; pointer-events: none; }` and `.map-node-external-badge-icon {
    font-family: "bootstrap-icons" !important; font-size: 9px; fill: #fff;
    text-anchor: middle; dominant-baseline: central; pointer-events: none; }`.
  - Also fade the badge alongside the icon/label under
    `.map-node.dimmed .map-node-external-badge-bg,
    .map-node.dimmed .map-node-external-badge-icon { opacity: 0.15; }` so it
    behaves consistently with the existing neighborhood-highlight feature.

### Reverted OntoViz implementation

The following were added to `ontoviz.js` / `ontoviz-entity.css` /
`ontology-design.js` (four entity-construction sites there, plus a
change-detection fingerprint fix) and then fully removed once the mix-up was
caught — see the changelog for the exact revert:

- `Entity.hasExternal` field and its badge render in `_renderEntity`
  (`ontoviz.js`).
- `.ovz-entity-external-badge` CSS (`ontoviz-entity.css`).
- `hasExternal` computation threaded through all four
  `loadOntologyIntoDesigner` / `_buildFreshDesignLayout` entity-construction
  sites, and the `_getOntologyVersion()` fingerprint fields
  (`ontology-design.js`).

## Error Handling

No new failure modes: `hasExternal` defaults to `false` via `||` guards, so
missing/undefined class fields never throw and simply omit the badge.

## Testing

`tests/units/front/test_ontology_map_external_badge.py` — contract tests
asserting: the `hasExternal` expression is computed in the node-data builder;
the badge circle/text are appended only to `nodeElements.filter(d =>
d.hasExternal)`; no `<title>` or `.on(...)` handler inside the badge-append
block; `aria-label`/`aria-hidden` are present; CSS rules exist for both badge
elements and for the dimmed state.

Manual/live verification (via `web-devloop-tester`, against the running dev
session `Cust360Auto V5`):

- Designer graph: badge appears only on `Customer` and `Meter` (the two
  classes with External-tab data in that session); all other 13 entities show
  no badge.
- DOM inspection on the `Customer` node confirmed
  `circle.map-node-external-badge-bg` and `text.map-node-external-badge-icon`
  as siblings inside `g.map-node`, no `<title>` on either badge element, and
  `pointer-events: none` (hover produced no native tooltip).
- Business Views (OntoViz) canvas: zero `.ovz-entity-external-badge` elements
  across all 15 entities, confirming the revert was complete.
- No console/JS errors on either page.
