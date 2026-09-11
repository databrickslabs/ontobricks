# Spark SPARQL Fail-Closed Translation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Explorer's SPARQL-to-SQL path reject unsupported semantics and preserve relationship `OPTIONAL` rows, first on `0.8.1`, then on `0.9.0`.

**Architecture:** Add an RDFLib-algebra capability validator ahead of the existing regex SQL generator. Keep the generator's supported subset, remove the top-level relationship-presence filter that defeats `LEFT JOIN`, and port the verified change from the clean `0.8.1` worktree into the active `0.9.0` worktree without overwriting unrelated edits.

**Tech Stack:** Python 3.10+, RDFLib SPARQL algebra, pytest, Ruff, Git worktrees.

## Global Constraints

- Implement and fully verify branch `0.8.1` before modifying branch `0.9.0`.
- Follow test-driven development: every behavior test must fail for the expected reason before production code changes.
- Keep the current supported Spark SPARQL subset; do not implement additional SPARQL 1.1 features.
- Raise `back.core.errors.ValidationError` for malformed or unsupported queries.
- Do not provide an RDFLib fallback because it queries the R2RML mapping graph, not warehouse data.
- Preserve unrelated uncommitted changes in the `0.9.0` worktree.
- Use `uv run --frozen pytest`; never run bare `uv run`.
- Track GitHub issue [#167](https://github.com/databrickslabs/ontobricks/issues/167).

---

### Task 1: Add the capability validator on `0.8.1`

**Files:**
- Create: `src/back/core/w3c/sparql/SparqlCapabilityValidator.py`
- Create: `tests/back/core/w3c/sparql/test_sparql_capability_validator.py`

**Interfaces:**
- Produces: `SparqlCapabilityValidator.validate(query: str) -> None`
- Raises: `ValidationError` with `Spark SPARQL does not support <FEATURE>.`
- Consumes: `rdflib.plugins.sparql.prepareQuery`, `CompValue`, RDF terms, and `rdflib.paths.Path`

- [ ] **Step 1: Write failing syntax and operator tests**

Add tests for a basic accepted `SELECT`, malformed syntax, `GROUP BY`, `ORDER BY`, numeric/complex `FILTER`, property paths, subqueries, `MINUS`, `VALUES`, `SERVICE`, `GRAPH`, arbitrary `UNION`, non-literal `BIND`, and `OFFSET`.

```python
@pytest.mark.parametrize(
    ("query", "feature"),
    [
        ("SELECT ?s (COUNT(?o) AS ?n) WHERE { ?s <http://ex/p> ?o } GROUP BY ?s", "GROUP BY"),
        ("SELECT ?s WHERE { ?s <http://ex/p> ?o } ORDER BY ?s", "ORDER BY"),
        ("SELECT ?s WHERE { ?s <http://ex/p> ?o FILTER(?o > 5) }", "numeric FILTER"),
        ("SELECT ?s WHERE { ?s <http://ex/p>+ ?o }", "property paths"),
    ],
)
def test_rejects_unsupported_construct(query, feature):
    with pytest.raises(ValidationError, match=feature):
        SparqlCapabilityValidator.validate(query)
```

- [ ] **Step 2: Run validator tests and verify RED**

Run:

```bash
uv run --frozen pytest -q tests/back/core/w3c/sparql/test_sparql_capability_validator.py
```

Expected: collection/import failure because `SparqlCapabilityValidator` does not exist.

- [ ] **Step 3: Implement algebra traversal**

Create a class-first validator with these rules:

```python
class SparqlCapabilityValidator:
    @classmethod
    def validate(cls, query: str) -> None:
        try:
            algebra = prepareQuery(query).algebra
        except Exception as exc:
            raise ValidationError("Invalid SPARQL query.", detail=str(exc)) from exc
        cls._validate_select(algebra)
```

The recursive visitor must:

- allow `SelectQuery`, `Project`, `Distinct`, `Slice` with `start` unset/zero, `BGP`, `LeftJoin` with `TrueFilter`, and known filter/extend/union shapes;
- reject any unknown `CompValue.name`;
- reject `Path` values in BGP predicates;
- allow BGP terms only when the current regex parser can consume them;
- allow only `CONTAINS(LCASE(STR(?v)), literal)`, `STR(?v) = literal`, `STRSTARTS(LCASE(STR(?v)), literal)`, `STRENDS(LCASE(STR(?v)), literal)`, and predicate-variable `IN` URI lists;
- allow ordinary `Extend` only when its expression is a `Literal`;
- allow `Union` only when every branch is an `Extend` that binds a relationship predicate URI to `?predicate` over one matching relationship BGP;
- map unsupported algebra names to stable feature labels, with an `unsupported SPARQL construct` fallback.

- [ ] **Step 4: Run validator tests and verify GREEN**

Run the command from Step 2.

Expected: all validator tests pass.

- [ ] **Step 5: Commit the validator**

```bash
git add src/back/core/w3c/sparql/SparqlCapabilityValidator.py \
  tests/back/core/w3c/sparql/test_sparql_capability_validator.py
git commit -m "fix(sparql): reject unsupported Spark query algebra"
```

---

### Task 2: Integrate fail-closed validation on `0.8.1`

**Files:**
- Modify: `src/back/core/w3c/sparql/SparqlTranslator.py:150-170`
- Modify: `tests/back/core/w3c/sparql/test_sparql_translator_units.py`

**Interfaces:**
- Consumes: `SparqlCapabilityValidator.validate(query: str) -> None`
- Preserves: `SparqlTranslator.translate_sparql_to_spark(...) -> dict`

- [ ] **Step 1: Add translator-level regression tests**

Add parametrized tests that invoke the public translator and require `ValidationError` for `GROUP BY`, numeric/complex filters, and property paths. Add positive tests for basic BGP, supported string filters, literal `BIND`, and the existing relationship-filter UNION.

- [ ] **Step 2: Run the new translator tests and verify RED**

Run:

```bash
uv run --frozen pytest -q tests/back/core/w3c/sparql/test_sparql_translator_units.py
```

Expected: unsupported-query cases return successful incomplete SQL instead of raising.

- [ ] **Step 3: Call the validator before normalization**

At the start of `translate_sparql_to_spark`, import and call:

```python
from back.core.w3c.sparql.SparqlCapabilityValidator import SparqlCapabilityValidator

SparqlCapabilityValidator.validate(sparql_query)
```

Keep current defensive mapping errors after this boundary.

- [ ] **Step 4: Run translator and validator tests**

```bash
uv run --frozen pytest -q tests/back/core/w3c/sparql/test_sparql_capability_validator.py \
  tests/back/core/w3c/sparql/test_sparql_translator_units.py
```

Expected: all tests pass.

- [ ] **Step 5: Commit integration**

```bash
git add src/back/core/w3c/sparql/SparqlTranslator.py \
  tests/back/core/w3c/sparql/test_sparql_translator_units.py
git commit -m "fix(sparql): fail closed before SQL translation"
```

---

### Task 3: Restore relationship OPTIONAL semantics on `0.8.1`

**Files:**
- Modify: `src/back/core/w3c/sparql/SparqlTranslator.py:1813-2042,2262-2407`
- Modify: `tests/back/core/w3c/sparql/test_sparql_translator_units.py`

**Interfaces:**
- Preserves: existing relationship `LEFT JOIN` SQL
- Removes: `optional_rel_conditions` collection and top-level OR predicate

- [ ] **Step 1: Add failing OPTIONAL regressions**

Use explicit entity and relationship mappings and assert:

```python
assert "LEFT JOIN" in sql
assert "IS NOT NULL" not in sql
assert " OR " not in sql
```

Cover one relationship OPTIONAL, two relationship OPTIONALs, no-match row preservation by SQL shape, and a column-valued OPTIONAL.

- [ ] **Step 2: Run OPTIONAL tests and verify RED**

Expected: current SQL contains `WHERE (rel_o.customer_id IS NOT NULL)`.

- [ ] **Step 3: Remove presence-filter plumbing**

Remove `optional_rel_conditions` parameters, appends, return values, and the `_spark_finalize_query_sql` block that adds their OR expression. Use a monotonic alias counter based on the existing `from_tables`/join state rather than `len(optional_rel_conditions)`.

- [ ] **Step 4: Run focused SPARQL tests and verify GREEN**

```bash
uv run --frozen pytest -q tests/back/core/w3c/sparql
```

- [ ] **Step 5: Commit OPTIONAL correction**

```bash
git add src/back/core/w3c/sparql/SparqlTranslator.py \
  tests/back/core/w3c/sparql/test_sparql_translator_units.py
git commit -m "fix(sparql): preserve rows for optional relationships"
```

---

### Task 4: Harden tests and document `0.8.1`

**Files:**
- Modify: `tests/back/core/w3c/sparql/test_sparql_translator_units.py`
- Modify: `docs/architecture.md`
- Modify: `docs/user-guide.md`
- Create: `changelogs/v0.8.1/benoitcayladbx_2026-09-11.log`

- [ ] **Step 1: Replace vacuous assertions**

Replace all nine `if result.get("success"):` guards with:

```python
assert result["success"], result
```

Then execute the existing assertions unconditionally. Replace tests that swallow arbitrary exceptions with an explicit `ValidationError` expectation.

- [ ] **Step 2: Run the hardened tests**

```bash
uv run --frozen pytest -q tests/back/core/w3c/sparql
```

- [ ] **Step 3: Document the supported subset**

Document fail-closed Spark behavior, supported operators, relationship OPTIONAL semantics, and the fact that local RDFLib is not a warehouse-data fallback. Do not claim full SPARQL 1.1 support.

- [ ] **Step 4: Add the required v0.8.1 changelog**

Record context, numbered file-level changes, modified files, focused test results, and full-suite result in English.

- [ ] **Step 5: Run lint and full verification**

```bash
uv run --frozen ruff check \
  src/back/core/w3c/sparql/SparqlCapabilityValidator.py \
  src/back/core/w3c/sparql/SparqlTranslator.py \
  tests/back/core/w3c/sparql
uv run --frozen pytest -q -m "not scenario"
```

- [ ] **Step 6: Commit the completed `0.8.1` fix**

```bash
git add docs/architecture.md docs/user-guide.md \
  tests/back/core/w3c/sparql/test_sparql_translator_units.py \
  changelogs/v0.8.1/benoitcayladbx_2026-09-11.log
git commit -m "docs(sparql): describe fail-closed Spark subset"
```

Record the ordered commit hashes for the `0.9.0` port.

---

### Task 5: Port and verify on `0.9.0`

**Files:**
- Create: `src/back/core/w3c/sparql/SparqlCapabilityValidator.py`
- Modify: `src/back/core/w3c/sparql/SparqlTranslator.py`
- Create: `tests/back/core/w3c/sparql/test_sparql_capability_validator.py`
- Modify: `tests/back/core/w3c/sparql/test_sparql_translator_units.py`
- Modify: `docs/architecture.md`
- Modify: `docs/user-guide.md`
- Modify: `changelogs/v0.9.0/benoitcayladbx_2026-09-11.log`

- [ ] **Step 1: Re-check the active `0.9.0` tree**

Capture `git status --short` and preserve every pre-existing change. Compare the SPARQL files with the verified `0.8.1` versions.

- [ ] **Step 2: Apply the verified production and test changes**

Apply the `0.8.1` diffs in commit order. Cherry-pick only when it cannot overwrite local edits; otherwise apply the relevant hunks manually. Do not stage or rewrite unrelated user changes.

- [ ] **Step 3: Verify the port with focused tests**

```bash
uv run --frozen pytest -q tests/back/core/w3c/sparql
```

Expected: the same focused suite passes on `0.9.0`.

- [ ] **Step 4: Merge documentation and changelog safely**

Add the same capability documentation while retaining existing `0.9.0` edits. Append a new English changelog section referencing issue #167 and both focused/full test results.

- [ ] **Step 5: Run lint and the mandatory full suite**

```bash
uv run --frozen ruff check \
  src/back/core/w3c/sparql/SparqlCapabilityValidator.py \
  src/back/core/w3c/sparql/SparqlTranslator.py \
  tests/back/core/w3c/sparql
uv run --frozen pytest -q -m "not scenario"
```

- [ ] **Step 6: Verify branch order and diffs**

Confirm `0.8.1` contains and passed the fix before `0.9.0`; inspect both branch diffs; verify issue #167 acceptance criteria line by line. Commit only isolated `0.9.0` files/hunks that do not absorb unrelated edits.

