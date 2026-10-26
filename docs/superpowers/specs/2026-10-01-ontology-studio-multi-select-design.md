# Ontology Studio Multi-Select

## Context

Ontology Studio (`Ontology → Studio`) is a D3 force-directed canvas. Today a
click selects one entity, opens the right-hand panel, and highlights the 1-hop
neighbourhood. Drag moves that one node. Right-click acts on that one entity
(delete, Create Business View, relationship, inheritance).

Users need to select several entities at once so they can:

- move them together
- delete them together
- create a Business View that contains exactly the selected set

This change is scoped to the Studio D3 map
(`src/front/static/ontology/js/ontology-map.js` and
`src/front/static/ontology/css/ontology-map.css`). It does not add a new
toolbar mode, does not change OntoViz Business Views editing, and does not
add multi-select for relationships.

## Selection gestures

Plain click, drag-on-node, pan, zoom, and connection mode keep their current
behaviour.

**Ctrl-click or Cmd-click** on an entity toggles that entity in the selection
set. Modifier-click does not open the detail panel.

**Ctrl-drag or Cmd-drag on empty canvas** draws a marquee rectangle. When the
drag ends, the selection becomes every entity whose node position falls inside
the rectangle (replace). A marquee that contains no entities leaves the
current selection unchanged. Plain drag on empty canvas remains pan via
`d3.zoom`.

**Plain click on an entity** replaces the selection with that one entity and
opens its panel as today.

**Plain click on empty canvas** clears the selection, clears neighbourhood
highlights, and guarded-closes the panel as today.

**Escape** clears a multi-selection. If connection mode is active, Escape
still ends connection mode first.

Right-click on an entity that is already selected keeps the selection and
opens the context menu for that set. Right-click on an unselected entity
replaces the selection with that entity and opens the existing single-entity
menu.

`mapConnectionMode` ignores modifier-click and marquee until the connection
ends.

Ctrl and Meta (Cmd) are equivalent everywhere.

## Visual state

Every selected node carries `.map-node.selected` (the existing primary ring
on `.map-node-hitarea`). Neighbourhood dimming runs only when exactly one
entity is selected. A multi-selection does not dim the rest of the graph.

The context-menu header for a multi-selection shows the count, for example
`3 entities`. There is no new toolbar chip or count badge.

The detail panel is bound to a single entity. When the selection size is not
exactly one, the panel is guarded-closed.

## Move

Dragging a node that is part of a multi-selection translates every selected
node by the same delta and keeps their fixed positions (`fx` / `fy`). The
existing `scheduleMapAutoSave` path runs once on drag end.

Dragging an unselected node moves only that node and does not change the
selection.

## Delete

The multi-select context menu offers `Delete N entities`. Confirm once with
`showConfirmDialog`:

- Title: `Delete entities`
- Message: `Delete N entities? Relationships connected to them will also be removed.`
- Confirm: `Delete` (`btn-danger`)

On confirm, apply the same session mutations as `deleteEntityFromMap` for each
selected name (remove class, incident properties, child `parent` references),
then save once, refresh the map once, and notify with the deleted count.
Cancel leaves the selection and ontology unchanged.

Single-entity delete keeps the current dialog and copy.

## Business View

Single-entity **Create Business View** is unchanged: the named entity plus its
1-hop neighbourhood, named `Auto_<entityName>`.

Multi-select **Create Business View** includes:

- exactly the selected entities
- object-property relationships whose both endpoints are selected
- inheritance links whose both endpoints are selected

It does not pull in neighbours. The view is named `Auto_Selection`, with the
existing numeric suffix if that name already exists. Layout reuses the
current circular placement over the selected names. After creation, navigate
to Business Views as today.

Relationship and Inherit-from stay single-entity actions and are omitted from
the multi-select menu. Details / Attributes / References / Constraints are
also omitted while more than one entity is selected.

## Error handling

- Inactive / non-current versions keep the current suppression of write
  actions (no multi-delete, no Business View).
- A missing `showConfirmDialog` helper does not delete.
- Marquee drawing is cancelled if the pointer leaves the canvas or the
  modifier key is released before mouseup; the selection does not change.
- Map rebuilds (`initOntologyMap`) clear the selection.

## Testing

Frontend source and behaviour contracts cover:

1. Ctrl/Cmd-click toggles membership without opening the panel.
2. Plain click replaces the selection with one entity.
3. Ctrl/Cmd-drag on empty canvas marquees; plain drag still pans.
4. Dragging one selected node moves the whole set.
5. Multi-select context menu exposes Delete N and Create Business View, not
   relationship/inherit/panel tabs.
6. Multi Business View contains only selected names plus internal links.
7. Multi-delete confirms once and removes every selected class.

Run `uv run --frozen pytest -q -m "not scenario"`.

## Scope

Out of scope: relationship multi-select, a Select toolbar button, lasso
(freeform) selection, Shift-click range select, and changing the single-entity
1-hop Business View rule.
