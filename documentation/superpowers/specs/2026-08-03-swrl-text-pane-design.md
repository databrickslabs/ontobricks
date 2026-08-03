# SWRL Text Pane (Business Rules) Design

## Context

Ontology → Data Quality already exposes a header **SHACL** button that toggles
an inline Turtle pane (`#dqShaclPanel`) with Refresh / Export / Import. Business
Rules has no equivalent for SWRL: rules are only editable via the visual graph
modal or the card list, with no bulk view or file interchange.

SWRL rules live in the domain session as a list of dicts
(`name`, `description`, `antecedent`, `consequent`). OntoBricks does **not**
round-trip standard SWRL RDF (`swrl:Imp`); OWL export stores them as
OntoBricks annotation resources. The editor already works with human-readable
antecedent / consequent atom strings (`Customer(?c) ^ hasClaim(?c, ?cl)`).

## Goal

Add a **SWRL** button on Business Rules that mirrors the SHACL pane: view all
rules as OntoBricks SWRL text, refresh, export a file, and import (append) text
or an uploaded file.

## Design

### UI

In `src/front/templates/partials/ontology/_ontology_business_rules.html`:

1. Add a `btn-outline-secondary` **SWRL** button (code-slash icon) to the header
   `btn-group`, after Auto-generate — same placement as Data Quality’s SHACL.
2. Below `#brTabContent` (still inside the card body), add `#brSwrlPanel`
   (`display:none` by default) with:
   - Title “SWRL Rules”
   - Refresh / Export / Import button group
   - `#brSwrlEditor` monospace textarea (rows ~12)
3. Add `#brSwrlImportModal` (mirror `#dqImportModal`): paste area + `.swrl` /
   `.txt` file input + Import action.

Toggle only shows/hides the pane and refreshes text when opening. It does not
switch tabs. Decision Tables / SPARQL / Aggregate are out of scope for this pane.

### Text format

Blank-line-separated blocks. Comments optional; implication required.

```
# Rule: Claiming customer must have contract
# Description: Optional free text
Customer(?c) ^ hasClaim(?c, ?cl) -> hasContract(?c, ?ct)

# Rule: Payment with invoices
Payment(?p) ^ hasInvoice(?p, ?i) -> relatedTo(?p, ?i)
```

Rules:

| Line | Meaning |
|------|---------|
| `# Rule: <name>` | Rule name (required for a well-formed block; if missing, synthesize `Imported rule N`) |
| `# Description: <text>` | Optional description |
| `# …` (other) | Ignored |
| `<antecedent> -> <consequent>` | Required; first ` -> ` (spaces optional around arrow) splits IF / THEN |
| blank line | Ends the current rule block |

Empty antecedent or consequent after split → that block is rejected with a
validation error for the whole import (fail closed; no partial append).

### API

Next to existing `/ontology/swrl/*` in
`src/api/routers/internal/ontology.py`:

| Method | Path | Behaviour |
|--------|------|-----------|
| `GET` | `/ontology/swrl/text` | Serialize `domain.swrl_rules` → `{ success, text }` |
| `GET` | `/ontology/swrl/export` | Same text as download (`Content-Disposition`; filename `{domain}_swrl_rules.swrl`) |
| `POST` | `/ontology/swrl/import` | Body `{ text }`; parse; **append** all rules to `domain.swrl_rules`; save; return `{ success, rules, imported_count }` |

Serialization / parsing live in a small helper (prefer
`back/core/reasoning/` or next to existing SWRL validation on `Ontology`), not
inline in the router. Reuse `Ontology.validate_swrl_rule` (or equivalent) on
each parsed dict before append.

Import semantics: **always append**. Duplicate names are allowed (explicit
product choice). No merge-by-name, no replace-all in this change.

### Frontend wiring

Prefer extending `SwrlModule` in `ontology-swrl.js` (owns the SWRL list already)
with `toggleSwrlPanel` / `refreshSwrlText` / `exportSwrl` / `showImportModal` /
`doImport`, parallel to `DataQualityModule`’s SHACL helpers. After a successful
import, refresh the SWRL card list and badge count.

CSS: reuse patterns from `ontology-dataquality.css` (`#dqShaclEditor`) in
`ontology-business-rules.css` or a minimal `#brSwrlEditor` rule.

### Tests

- Unit: serialize round-trip (list → text → list) for 0 / 1 / N rules;
  description optional; missing `# Rule:` synthesizes a name; bad arrow /
  empty side raises; append leaves existing rules intact.
- Front source contract (optional light): template contains the SWRL button,
  `#brSwrlPanel`, and import modal; script exposes the toggle helpers.

### Out of scope

- Standard SWRL RDF / XML interchange
- Decision Table / SPARQL / Aggregate text panes
- Editing-and-saving from the textarea without going through Import
- Merge-by-name or replace-all import modes

## Success criteria

1. Business Rules header shows a **SWRL** button next to Auto-generate.
2. Click toggles an inline pane (not a Bootstrap modal) with the current rules
   as OntoBricks SWRL text.
3. Export downloads a `.swrl` file; Import appends parsed rules and updates the
   SWRL list UI.
4. Existing SWRL editor / list / save / delete paths unchanged.
