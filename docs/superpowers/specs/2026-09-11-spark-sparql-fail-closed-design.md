# Spark SPARQL Fail-Closed Translation Design

**Date:** 2026-09-11  
**Status:** Approved for implementation planning

## Problem

The Explorer translates SPARQL to Spark SQL with a regex-based translator.
Valid SPARQL constructs outside its supported subset can currently disappear
from the generated SQL without an error. Confirmed examples include `GROUP BY`,
numeric or complex `FILTER` expressions, and property paths. The resulting SQL
is valid but has different semantics from the submitted query.

Relationship `OPTIONAL` patterns have a separate correctness defect. The
translator emits `LEFT JOIN` clauses, then adds relationship-presence checks to
the top-level `WHERE` clause. A single check makes the optional relationship
mandatory; multiple checks joined with `OR` require at least one optional
relationship to match.

The local RDFLib query path is not an equivalent fallback: it queries the R2RML
mapping graph rather than warehouse data. The Spark path must therefore reject
queries it cannot translate faithfully.

## Goals

1. Reject valid-but-unsupported Spark SPARQL before SQL generation.
2. Preserve the existing supported subset.
3. Restore SPARQL `OPTIONAL` row-preservation semantics for relationships.
4. Return actionable `ValidationError` messages for unsupported constructs.
5. Add regression tests that fail if query semantics are silently discarded.

## Non-goals

- Implement full SPARQL 1.1 translation.
- Add support for numeric filters, aggregates, ordering, property paths, or
  subqueries.
- Replace the current SQL generator with an algebra-to-SQL visitor.
- Add an automatic RDFLib fallback.
- Change SWRL or T-Box/A-Box reasoning.

## Architecture

Add `SparqlCapabilityValidator` under
`src/back/core/w3c/sparql/SparqlCapabilityValidator.py`. It is a dedicated
preflight boundary between accepted SPARQL and the existing SQL generator.

`SparqlTranslator.translate_sparql_to_spark()` invokes the validator before
normalizing or parsing the query with its current helpers. The validator:

1. parses the query with RDFLib, rejecting malformed syntax;
2. walks the parsed SPARQL algebra;
3. allows only algebra nodes and expression shapes the SQL generator supports;
4. raises `ValidationError` for every unsupported node or expression.

The existing translator remains responsible for SQL generation. It may assume
that its input belongs to the validated subset, but its current defensive
mapping errors remain in place.

## Supported subset

The capability gate allows:

- `SELECT` projections and `SELECT *`;
- `DISTINCT`;
- `LIMIT`;
- basic graph patterns with simple variables, IRIs, prefixed names, and
  currently supported literals;
- class and property patterns already handled by the mapping translator;
- `OPTIONAL` around supported basic patterns;
- the existing supported string filters: `CONTAINS`, string equality,
  `STRSTARTS`, and `STRENDS`;
- the existing predicate `IN` filter shape;
- literal `BIND`;
- the existing specialized relationship-filter `UNION` shape, only when every
  branch matches that shape.

The capability gate rejects:

- aggregate projections, `GROUP BY`, and `HAVING`;
- `ORDER BY`, `OFFSET`, and unsupported solution modifiers;
- numeric and complex filter expressions;
- property paths;
- subqueries;
- `MINUS`, `VALUES`, `SERVICE`, and `GRAPH`;
- arbitrary `UNION`;
- non-literal or otherwise unsupported `BIND`;
- any unknown algebra node or expression.

Unknown constructs are rejected by default. Adding support later requires
updating both the validator whitelist and translator tests.

## OPTIONAL correction

The existing relationship `LEFT JOIN` generation remains. The translator stops
collecting relationship-presence predicates and stops appending their
`IS NOT NULL` disjunction to the top-level `WHERE` clause.

For:

```sparql
SELECT ?customer ?order
WHERE {
  ?customer a ex:Customer .
  OPTIONAL { ?customer ex:hasOrder ?order }
}
```

the SQL retains every customer row. `?order` is `NULL` when no relationship
matches. Multiple OPTIONAL blocks remain independent and do not introduce an
"at least one matched" condition.

## Error handling

All syntax and capability failures raise `back.core.errors.ValidationError`.
Messages identify the unsupported feature without exposing internal parser
objects, for example:

```text
Spark SPARQL does not support GROUP BY.
```

The current route-level error mapping is unchanged. Error messages do not
recommend the local engine because it does not query equivalent warehouse data.
Unexpected parser failures are wrapped as a validation failure with exception
chaining for diagnostics.

## Tests

### Capability validator

Add direct unit tests for:

- accepted basic SELECT/BGP queries;
- accepted `DISTINCT`, `LIMIT`, supported string filters, literal `BIND`,
  OPTIONAL patterns, and the specialized UNION;
- each rejected operator family;
- unsupported constructs inside nested OPTIONAL or UNION blocks;
- malformed SPARQL versus syntactically valid but unsupported SPARQL;
- unknown algebra nodes failing closed.

### Translator regressions

Add tests proving:

- `GROUP BY`, complex/numeric FILTER, and property paths raise
  `ValidationError` instead of returning incomplete SQL;
- a single relationship OPTIONAL emits `LEFT JOIN` without a top-level
  relationship-presence predicate;
- multiple relationship OPTIONALs preserve rows when none match;
- column-valued OPTIONAL behavior remains unchanged;
- supported queries retain their current SQL shape.

Replace the nine `if result.get("success")` guards in
`tests/back/core/w3c/sparql/test_sparql_translator_units.py` with mandatory
success assertions followed by unconditional assertions.

### Verification

Run focused SPARQL tests first, then:

```bash
uv run --frozen pytest -q -m "not scenario"
```

## Documentation

Document the supported Spark SPARQL subset and fail-closed behavior in the user
guide and architecture documentation. Update the advanced SPARQL example that
currently combines OPTIONAL and GROUP BY without identifying which execution
engine supports it. Explicitly state that the local RDFLib path is not a
warehouse-data fallback.

## Delivery

Deliver this as one focused Spark SPARQL safety change:

1. capability validator and rejection tests;
2. translator integration;
3. OPTIONAL correction and regressions;
4. conditional-test hardening;
5. documentation and changelog;
6. focused and full-suite verification.

The change must not alter SWRL reasoning or attempt broader SPARQL support.
