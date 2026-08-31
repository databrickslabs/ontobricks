# SWRL Text Modal (Business Rules) Design

## Context

Business Rules exposes a header **SWRL** button that toggles an inline text
pane (`#brSwrlPanel`) under the rule-type tabs. Ontology → Data Quality’s
**SHACL** control was converted to a Bootstrap `modal-xl`; SWRL should match.

## Goal

Clicking **SWRL** opens a Bootstrap `modal-xl` with the same text viewer and
Refresh / Export / Import actions. Remove the inline bottom pane.

## Design

Mirror `documentation/superpowers/specs/2026-08-03-shacl-turtle-modal-design.md`:

1. Header button calls `SwrlModule.openSwrlModal()`.
2. Replace `#brSwrlPanel` with `#brSwrlModal` (`modal-xl`) containing the action
   group and `#brSwrlEditor`.
3. `openSwrlModal()` shows the modal then `refreshSwrlText()`.
4. Drop `_swrlPanelOpen` / `toggleSwrlPanel`. After import, always refresh the
   editor (safe if the modal is closed).
5. `#brSwrlImportModal` unchanged (stacks on the SWRL modal).
6. Optional CSS min-height on `#brSwrlEditor`.
7. Update `tests/units/front/test_swrl_text_pane.py` source contracts.

No backend / API changes.

## Out of scope

Decision Tables, SPARQL, Aggregate text interchange; SWRL visual graph editor.
