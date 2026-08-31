# SHACL Turtle Modal (Data Quality) Design

## Context

Ontology → Data Quality exposes a header **SHACL** button that toggles an
inline Turtle pane (`#dqShaclPanel`) under the dimension tabs. The pane shows
Refresh / Export / Import and a monospace textarea (`#dqShaclEditor`). That
inline layout competes with the rule cards and feels like a bottom drawer
rather than a focused viewer.

Import already uses its own Bootstrap modal (`#dqImportModal`). Add Rule and
related flows also use modals, so a SHACL viewer modal matches the page pattern.

## Goal

Clicking **SHACL** opens a Bootstrap `modal-xl` with the same Turtle content and
actions as today’s pane. The inline bottom pane is removed.

## Design

### UI

In `src/front/templates/partials/ontology/_ontology_dataquality.html`:

1. Change the header **SHACL** button from `toggleShaclPanel()` to
   `openShaclModal()`.
2. Remove `#dqShaclPanel` (the inline `border-top` block under `#dqTabContent`).
3. Add `#dqShaclModal` next to the other Data Quality modals:
   - `modal fade`, dialog `modal-xl`
   - Header: “SHACL Turtle” + close button
   - Body: Refresh / Export / Import button group + `#dqShaclEditor` textarea
     (monospace, tall enough for Turtle; keep the same element id)

`#dqImportModal` stays unchanged. Import still opens on top of the SHACL modal
via Bootstrap stacking.

### JS

In `src/front/static/ontology/js/ontology-dataquality.js`:

- Replace `toggleShaclPanel()` with `openShaclModal()`:
  show `#dqShaclModal` via `bootstrap.Modal.getOrCreateInstance(...)`, then call
  `refreshTurtle()`.
- Remove `_shaclPanelOpen` toggle state.
- Keep `refreshTurtle`, `exportShacl`, `showImportModal`, and `doImport` as-is
  (they already target `#dqShaclEditor` / `#dqImportModal`).

### CSS

Optional only: give `#dqShaclEditor` a min-height inside the modal if the default
`rows` attribute is not tall enough. No other Data Quality CSS changes required.

### Backend

None. Existing `/ontology/dataquality/turtle`, `/export`, and `/import` endpoints
are unchanged.

## Scope

**In scope:** Ontology Data Quality SHACL button → modal conversion only.

**Out of scope:**

- Knowledge Graph → Data Quality execution page
- Business Rules SWRL text pane (separate design)
- Import modal redesign
- Editable save-from-textarea (today’s pane has no save; refresh/export/import
  only — preserve that)

## Testing

Manual smoke:

1. Open Ontology → Data Quality → SHACL → modal appears with Turtle.
2. Refresh reloads Turtle into the textarea.
3. Export downloads as today.
4. Import opens `#dqImportModal` above the SHACL modal; successful import
   refreshes the shape lists; SHACL modal can remain open or be closed normally.

No new unit tests required unless a non-trivial helper is introduced (not
expected). After implementation, run:

`uv run --frozen pytest -q -m "not scenario"`
