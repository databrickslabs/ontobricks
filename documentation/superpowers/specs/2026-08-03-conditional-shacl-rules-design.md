# Conditional SHACL Data Quality Rules Design

## Context

The Ontology Data Quality panel authors SHACL shapes across six dimensions
(completeness, cardinality, uniqueness, consistency, conformance, structural).
Each shape is a flat dict stored in the domain session under
`ontology.shacl_shapes`, authored through `#dqShapeModal` with a fixed set of
per-dimension inputs defined by `CONSTRAINT_FIELDS` in
`src/front/static/ontology/js/ontology-dataquality.js`.

Every shape currently applies to **all** instances of its target class. There is
no way to express "this constraint only applies when some other attribute holds
a given value". The only `sh:condition` in the codebase is on SHACL-AF
`sh:TripleRule` inference shapes (`SHACLService._build_rules_graph`), which is an
inference mechanism, not a validation one, and has no UI.

The Business Rules module already lets users build `property / operator / value`
conditions in two places — the decision-table grid
(`ontology-business-rules.js`) and the SWRL attribute-condition rows
(`ontology-swrl.js`) — but the two share no code and use different operator
vocabularies.

Production execution of data quality checks does **not** go through pyshacl. It
uses two hand-rolled translators, `SHACLService.shape_to_sql` (Delta/Lakebase
backend) and `SHACLService.evaluate_shape_in_memory` (graph backend). Any new
constraint capability must be implemented in both.

## Goal

Let a user attach an optional list of conditions to a conformance or consistency
rule, so the rule reads "IF `<conditions>` THEN `<constraint>` must hold". The
condition editor should feel like the decision-table condition cells the user
already knows.

## Design

### Data model

Two optional keys are added to the shape dict. An absent or empty `conditions`
list means the shape behaves exactly as it does today, so all existing shapes
and all existing code paths are unaffected.

```json
{
  "id": "shape_conformance_Customer_email_ab12",
  "category": "conformance",
  "target_class": "Customer",
  "target_class_uri": "http://example.org/onto#Customer",
  "property_path": "email",
  "property_uri": "http://example.org/onto#email",
  "shacl_type": "sh:pattern",
  "parameters": { "sh:pattern": "^.+@.+$" },
  "severity": "sh:Violation",
  "message": "Active customers must have a valid email",
  "enabled": true,
  "conditions": [
    { "property": "status",   "property_uri": "…#status",   "op": "eq",        "value": "active" },
    { "property": "amount",   "property_uri": "…#amount",   "op": "gt",        "value": "1000" },
    { "property": "shipment", "property_uri": "…#shipment", "op": "notExists", "value": "" }
  ],
  "condition_logic": "and"
}
```

Each condition carries its own property, unlike a decision-table cell whose
property comes from its column position. There is therefore no rectangular
invariant to maintain.

`condition_logic` is `"and"` (default) or `"or"` and combines all rows.

### Operators

Conditions reuse the decision-table operator vocabulary already defined in
`src/back/core/reasoning/constants.py` (`DT_OP_SQL`, `DT_STRING_OPS`,
`DT_NUMERIC_OPS`): `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `startsWith`,
`endsWith`, `contains`. The SHACL SQL builder imports these constants rather
than defining a third operator vocabulary.

Two operators are added for relationships: `exists` and `notExists`. Their
`value` is ignored and the value input is hidden in the UI.

As in the decision table, the value is stored as a string and its type is
inferred at translation time: numeric-looking values use numeric comparison,
everything else is compared case-insensitively as a string.

### Condition scope

A condition may reference:

- any datatype attribute of the target class, resolved through the parent chain
  so inherited attributes are offered (the inheritance-aware lookup that
  `ontology-swrl.js` `_dataPropsForClass` performs, which the decision table
  does not);
- any relationship of the target class, but only with `exists` / `notExists`.

Traversal through a relationship into a related entity's attributes is out of
scope.

### Dimensions

The IF block is available for the **conformance** and **consistency**
dimensions only. Completeness, cardinality, uniqueness and structural rules
keep their current unconditional behaviour.

### UI

A new `#dqConditionBlock` is inserted in `#dqShapeModal`, between the property
select and `#dqConstraintFields`. It is shown only when the modal's category is
conformance or consistency, and contains:

- an "IF (optional)" header with an AND/OR radio group (`dqCondLogic`);
- a rows container `#dqConditionRows`;
- an "Add condition" button.

Each row is a property select, an operator select, and a value input. The value
input is hidden when the operator is `exists` or `notExists`. The property
select lists attributes and relationships; choosing a relationship restricts the
operator list to `exists` / `notExists`.

Events are wired by delegation off `#dqShapeModal`, following the
`_ensureDtEditorDelegation` pattern in `ontology-business-rules.js`, rather than
the inline `onchange` attributes used by `ontology-swrl.js`.

Row rendering and collection live in a new module
`src/front/static/ontology/js/ontology-conditions.js`, exposing a `render(rows,
options)` / `collect(container)` pair plus the shared operator list. It is
loaded in `ontology.html` before `ontology-dataquality.js`. All ontology JS
modules share a single page, so this is reusable across them without
introducing a global JS layer. Migrating the decision table and the SWRL editor
onto this helper is out of scope, but the helper is written so they can move
later.

Three existing behaviours extend to cover conditions:

1. `_renderShapeCard` shows a chip summarising the condition, for example
   `IF status = active AND amount > 1000`.
2. `_autoFillMessage` includes the condition summary in the generated message.
3. `_updateShaclPreview` renders the `sh:target` block described below.

`_collectParams` gains a sibling `_collectConditions`, and the edit path gains
the matching `_fillConditionsFromShape`.

### Turtle generation

A conditional shape is emitted as its own `sh:NodeShape` instead of being merged
into the shared per-class node shape that `SHACLGenerator.generate` builds. Its
focus nodes come from a SHACL-AF SPARQL target; the property shape is generated
exactly as it is today.

```turtle
:shape_conformance_Customer_email_ab12 a sh:NodeShape ;
    sh:target [
        a sh:SPARQLTarget ;
        sh:select """SELECT $this WHERE {
            $this a :Customer ; :status ?c0 ; :amount ?c1 .
            FILTER(STR(?c0) = "active" && xsd:double(?c1) > 1000)
            FILTER NOT EXISTS { $this :shipment ?c2 }
        }""" ;
    ] ;
    sh:property [
        sh:path :email ;
        sh:pattern "^.+@.+$" ;
        sh:severity sh:Violation ;
        sh:message "Active customers must have a valid email" ;
    ] .
```

The example above is shown with prefixed names for readability. The query is
generated with **full IRIs** for classes, predicates and the datatype cast
function, because prefixes inside a `sh:select` string resolve from `sh:prefixes`
rather than from the document's Turtle prefixes, and no `sh:prefixes`
declaration is emitted.

Comparison operators become `FILTER` expressions: numeric values are wrapped in
an `xsd:double(...)` cast, string values are compared through `STR(...)`, and the
string operators map to `STRSTARTS`, `STRENDS` and `CONTAINS`. `exists` and
`notExists` become `FILTER EXISTS` / `FILTER NOT EXISTS` blocks and bind no
variable. `condition_logic` selects `&&` or `||` between the filter expressions;
with `"or"`, attribute bindings use `OPTIONAL` so an unbound attribute does not
eliminate the row.

### Execution

Both executors treat the conditions as a filter layered on top of the existing
per-constraint-type logic. No existing constraint branch changes.

**SQL.** `shape_to_sql` builds the base query exactly as it does today, then
wraps it when conditions are present:

```sql
SELECT v.* FROM ( <existing shape sql> ) v
WHERE v.s IN ( <condition subject query> )
```

The condition subject query selects the target class instances and applies the
conditions:

```sql
SELECT DISTINCT t0.subject AS s
FROM <table> t0
LEFT JOIN <table> c0 ON c0.subject = t0.subject AND c0.predicate = '<status_uri>'
LEFT JOIN <table> c1 ON c1.subject = t0.subject AND c1.predicate = '<amount_uri>'
WHERE t0.predicate = '<rdf:type>' AND t0.object = '<Customer_uri>'
  AND ( LOWER(c0.object) = 'active'
        AND CAST(c1.object AS DOUBLE) > 1000
        AND NOT EXISTS ( SELECT 1 FROM <table> x
                         WHERE x.subject = t0.subject
                           AND x.predicate = '<shipment_uri>' ) )
```

Attribute conditions use a `LEFT JOIN`, not an inner join: under
`condition_logic = "or"` an inner join on a missing property would drop rows
that the other branch should match, while under `"and"` a NULL object fails its
comparison anyway. One join form is therefore correct for both. `exists` /
`notExists` compile to `EXISTS` / `NOT EXISTS` subqueries and take no join.

Values are escaped with the same `esc` helper already used throughout
`shape_to_sql`. Property URIs go through `SHACLService._normalize_prop_uri`, as
the constrained property already does.

If a shape has conditions but its base constraint returns `None` (untranslatable
constraint type), `shape_to_sql` still returns `None`.

**In memory.** `evaluate_shape_in_memory` computes the matching subject set from
the same structured rows, using the `subj_by_pred` and `type_map` indexes it
already builds, and filters the resulting violation list on `v["s"]`. Property
URIs go through the existing `resolve_prop_uri` so the `#`/slash mismatch
handling applies to condition properties too.

### Validation

`Ontology.validate_shape` rejects a shape when:

- a condition has no `property` or no `op`;
- a comparison operator has an empty `value`;
- `op` is not in the known operator set;
- `condition_logic` is neither `"and"` nor `"or"`;
- `conditions` is non-empty on a category other than conformance or consistency;
- `conditions` is non-empty while `target_class_uri` is empty, since a condition
  needs a target class to select focus nodes from.

### Import and export

Export is one-way for conditional shapes. `SHACLParser` ignores `sh:target`, so
re-importing an exported conditional shape yields the constraint without its
condition. Because that silently widens a rule, the parser counts shapes that
carried a `sh:target` and the `/ontology/dataquality/import` response reports
how many shapes were imported without their conditions; the UI surfaces this as
a warning in the import result.

Duplicate detection and the fingerprint used by
`DomainSession.deduplicate_shacl_shapes` are left unchanged: two shapes that
differ only by their conditions are still treated as duplicates. Making the
fingerprint condition-aware is out of scope.

## Scope

Out of scope:

- conditions on completeness, cardinality, uniqueness and structural rules;
- traversal through a relationship to a related entity's attributes;
- `in` / `notIn` operators;
- migrating the decision-table grid and the SWRL condition rows onto the shared
  helper;
- a pyshacl advanced-mode (`sh:SPARQLTarget`-aware) validation path — the
  generated Turtle is for export and preview, and execution runs through the two
  translators;
- condition-aware duplicate detection;
- AI suggestion of conditional rules by the business rules generator agent.

## Testing

Add to `tests/units/ontology/test_dataquality.py`:

1. `shape_to_sql` on a conditional conformance shape wraps the base query and
   references the condition property URI and value.
2. A `notExists` condition produces `NOT EXISTS` and no join for that property.
3. `condition_logic = "or"` joins the predicates with `OR` and uses `LEFT JOIN`.
4. Regression: an unconditional shape produces the same SQL as before the
   change.
5. `evaluate_shape_in_memory` returns violations only for subjects matching the
   conditions, and returns the unfiltered set when `conditions` is empty.

Add to `tests/back/core/w3c/shacl/test_shacl_generator.py`:

6. A conditional shape emits its own node shape with an `sh:SPARQLTarget` whose
   `sh:select` contains the expected `FILTER`, and the property shape is
   unchanged.
7. Unconditional shapes are still grouped into one node shape per class.

Add unit tests for `Ontology.validate_shape` covering each rejection case.

Add `tests/units/front/test_dq_condition_builder.py`, a source-introspection
test in the style of `tests/units/front/test_swrl_attribute_conditions.py`,
asserting that the condition module exposes `render` / `collect`, that the value
input is suppressed for `exists` / `notExists`, and that the data quality module
only renders the block for the conformance and consistency categories.

Run the focused tests first, then the required suite:

`uv run pytest -q -m "not scenario"`

Finally, add the changelog entry under `changelogs/v0.7.0/` and update the
Sphinx documentation covering the Data Quality panel.
