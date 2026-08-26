# Mapping Designer Split Layout — Design Specification

## Purpose

Mapping / Designer must share Ontology / Designer's framed split-panel language.
The only layout difference is orientation: Ontology docks the detail panel on
the **right**; Mapping docks it at the **bottom**.

Today Mapping wraps the canvas in a Bootstrap `shadow-sm` card and treats the
bottom panel as a sliding drawer (`height: 0` until `.panel-open`). That looks
and behaves unlike Ontology's always-visible, resizable, two-card split.

## Decisions

- Two separate framed cards (canvas + panel), not one integrated frame and not
  an overlay drawer.
- The bottom panel is **always visible**, like Ontology's right panel.
- A horizontal drag handle resizes the panel height (Ontology analogue of the
  vertical handle).
- The designer region fills the remaining sidebar height under the section
  header. The canvas occupies leftover space; the panel has a default height
  of **320px**, `min-height` **200px**, `max-height` **50%**.
- Selecting an entity or relationship fills the panel. Clicking the canvas
  outside a node or link **clears** the panel to a placeholder; it does not
  hide the panel.
- Manual Mapping (its own bottom panel) is out of scope.
- Internal Mapping form tabs (Information / Mapping / SQL) and the existing
  vertical panel header stay; this change is layout chrome, not a form redesign.
- No Mapping-specific mobile breakpoint beyond the existing section-header wrap.

## Layout

Under the Mapping Designer section header:

```
.section-header
#mappingDesignerContainer.ob-split-shell.ob-split-shell--column
  #mappingMapCard.ob-split-pane          (canvas)
  .detail-panel-resize-handle            (8px, row-resize)
  #mappingRightPanel.ob-split-pane       (detail; name kept for JS ids)
```

Shared chrome (same tokens as Ontology / Designer and KG Explorer):

- gutter `0.5rem` between panes and handle;
- pane `background: #fff`, `border: 1px solid var(--db-border)`,
  `border-radius: var(--db-radius-card)`;
- handle bar 3px, hover/active `var(--db-primary)`;
- no Bootstrap `shadow-sm` on the canvas card;
- sidebar padding for the Mapping Designer section matches Ontology Designer
  (do not keep the Mapping-only `0.5rem` / overflow-hidden special case unless
  required to prevent a double scrollbar).

`ob-split-shell--column` is `flex-direction: column`. The existing row
`ob-split-shell` used by Ontology is unchanged.

## Interaction

| Event | Result |
|---|---|
| Page load / Designer shown | Panel visible with placeholder. Canvas fills remaining height. |
| Click entity or relationship | Panel shows that object's mapping UI. Selection highlight uses `--db-primary`. |
| Click canvas outside a node/link | Clear selection; panel returns to placeholder. |
| Drag handle | Panel height changes; canvas SVG resizes (`resizeMapSvg`) after the drag and on window resize. |
| Persist size | Last height stored in `sessionStorage` under `mappingDesignerPanelHeight`, same pattern as Ontology panel width. |
| Save mapping in the panel | Content updates; panel stays open. |
| Empty ontology / loading | Existing loading overlay on the canvas; panel stays as placeholder. |

`openMappingPanel()` hydrates content. It must not toggle visibility.
`closeMappingPanel()` (and equivalent cancel/unmap-complete paths) clear
content and selection; they must not collapse the panel to `height: 0`.

## Implementation

Reuse Ontology split chrome rather than duplicating Mapping-only drawer CSS.

1. Markup in `src/front/templates/partials/mapping/_mapping_design.html`:
   drop `card border-0 shadow-sm`; wrap canvas + handle + panel as above;
   add a placeholder node in the panel body for the empty state.
2. CSS:
   - add `.ob-split-shell--column` and horizontal handle rules next to the
     existing split styles in `src/front/static/ontology/css/ontology-shared-panels.css`
     (or a thin Mapping override that only sets orientation and default height);
   - remove Mapping drawer rules that set `.mapping-right-panel { height: 0 }`
     and `.panel-open` flex splits;
   - keep shared Manual Mapping panel styles isolated so that section is
     unchanged.
3. JS in `src/front/static/mapping/js/mapping-design.js`:
   - always-open panel; clear vs hide;
   - canvas background click → clear;
   - horizontal resize observer/drag mirroring Ontology's handle, calling
     `resizeMapSvg()`;
   - sessionStorage for height.
4. Load Mapping Designer CSS/JS so the Ontology shared-panel stylesheet is
   available on the Mapping page if it is not already.

Do not extract a generic orientation-parameterized split component in this
change.

## Testing

Add a front-end contract test (pattern of
`tests/units/front/test_ontology_split_panel_layout.py`):

- Mapping Designer markup uses `ob-split-shell--column`, two `ob-split-pane`
  children, and a `detail-panel-resize-handle`;
- no `shadow-sm` on the canvas wrapper;
- CSS does not hide the Mapping Designer panel with `height: 0`;
- `closeMappingPanel` / canvas-clear paths do not rely on `.panel-open` to
  show the panel;
- handle uses `row-resize`.

Update `tests/units/front/test_manual_mapping_panel_host.py` only if selectors
would otherwise break; Manual Mapping behaviour must still pass.

Browser verification (desktop 1600×1000, spot-check 390×844): Designer chrome
matches Ontology cards; panel always visible; resize works; canvas click
clears; SVG still fills the canvas pane.

## Out of scope

- Manual Mapping bottom panel
- Restyling Information / Mapping / SQL form internals
- Changing mapping data model or save APIs
- Moving the panel to the right
- Overlay / drawer behaviour
