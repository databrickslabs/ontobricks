# Spark SQL TRY_CAST Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every Spark/Databricks SQL fragment OntoBricks emits for stringification, typed literals, and typed nulls uses `TRY_CAST` instead of bare `CAST`, so a value that fails to convert yields NULL instead of aborting the statement under ANSI mode.

**Architecture:** Add a single `SQLHelpers.sql_cast(expr, sql_type)` helper that always emits `TRY_CAST`. Route the three remaining bare-`CAST` Spark emitters (`SparqlTranslator._cast_str`, `agent_mapping_pge/coverage.py`'s abstract-union NULL columns, `lakebase/_companion_ddl.py`'s Lakeflow view wrap) through it or through an inline `TRY_CAST`. Update the two stale test assertions that still expect bare `CAST`. Add a small guard-test module that pins the "no bare CAST" invariant across all three modules.

**Tech Stack:** Python 3.11, pytest, no new dependencies.

## Global Constraints

- Run tests with `uv run --frozen pytest -q -m "not scenario"` (the `--frozen` flag is mandatory).
- `sql_numeric` (existing rule/DQ helper) must keep its exact current signature and behavior — other callers (`DecisionTableEngine`, `SPARQLRuleEngine`, `ShapeConditions`, `AggregateRuleEngine`, `SHACLService`) must not need any change.
- Do **not** touch `src/jobs/graph_analytics_job.py` or `src/back/core/graph_analysis/JobMetrics.py` — their `CAST` calls cast already-numeric data and their SQL executes literally against SQLite in tests plus a Postgres sqlglot dialect check; `TRY_CAST` is not valid in either. This is a deliberate exclusion agreed with the user, documented in `documentation/superpowers/specs/2026-08-05-spark-try-cast-design.md` under "Amendment".
- Do **not** touch SWRL **Cypher** templates in `SWRLBuiltinRegistry.py` (Neo4j dialect, separate from the `sql_template` field which already uses `TRY_CAST`).
- After the code changes, update `/changelogs/v0.7.0/benoitcayladbx_<today>.log` per the workspace changelog convention (append if the file already exists for today).

---

### Task 1: `SQLHelpers.sql_cast` helper

**Files:**
- Modify: `src/back/core/helpers/SQLHelpers.py:26-38`
- Modify: `src/back/core/helpers/__init__.py` (re-export)
- Test: `tests/units/core/test_uri_sql_helpers.py` (existing `TestSQLHelpers` class — add tests there, do not create a new file)

**Interfaces:**
- Produces: `SQLHelpers.sql_cast(expr: str, sql_type: str) -> str` returning `f"TRY_CAST({expr} AS {sql_type})"`. Re-exported as `back.core.helpers.sql_cast`.
- `SQLHelpers.sql_numeric(expr, sql_type="DOUBLE")` becomes a one-line wrapper calling `sql_cast` — same signature, same output, so no caller changes anywhere else in the codebase.

- [ ] **Step 1: Write the failing tests**

In `tests/units/core/test_uri_sql_helpers.py`, add these methods to the existing `TestSQLHelpers` class (after `test_sql_escape_normal`, before `test_validate_table_name_valid`):

```python
    def test_sql_cast_emits_try_cast(self):
        assert SQLHelpers.sql_cast("t2.object", "DOUBLE") == "TRY_CAST(t2.object AS DOUBLE)"

    def test_sql_numeric_delegates_to_sql_cast(self):
        assert SQLHelpers.sql_numeric("t2.object") == SQLHelpers.sql_cast("t2.object", "DOUBLE")

    def test_sql_numeric_custom_type_delegates_to_sql_cast(self):
        assert SQLHelpers.sql_numeric("t2.object", "BIGINT") == SQLHelpers.sql_cast(
            "t2.object", "BIGINT"
        )
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen pytest tests/units/core/test_uri_sql_helpers.py::TestSQLHelpers -v`
Expected: the three new tests FAIL with `AttributeError: type object 'SQLHelpers' has no attribute 'sql_cast'`.

- [ ] **Step 3: Implement `sql_cast` and rewrite `sql_numeric` as a wrapper**

In `src/back/core/helpers/SQLHelpers.py`, replace:

```python
    @staticmethod
    def sql_numeric(expr: str, sql_type: str = "DOUBLE") -> str:
        """Cast a triple-store value to a number without failing the query.

        Every object in the triple store is stored as a string, so a numeric
        comparison has to cast. A plain ``CAST`` aborts the whole statement on
        the first value that is not a number, and Databricks warehouses run
        with ANSI mode on, so one ``"free"`` in a fee column takes the entire
        rule down. ``TRY_CAST`` yields NULL instead, which makes the
        comparison false — the value does not satisfy the condition, which is
        what the reader means.
        """
        return f"TRY_CAST({expr} AS {sql_type})"
```

with:

```python
    @staticmethod
    def sql_cast(expr: str, sql_type: str) -> str:
        """Cast a value to *sql_type* without failing the query.

        Databricks warehouses run with ANSI mode on, so a plain ``CAST``
        aborts the whole statement on the first value that will not convert.
        ``TRY_CAST`` yields NULL instead. Every Spark SQL fragment OntoBricks
        emits — stringification, typed literals, typed NULLs, numeric
        coercion — must use this helper (or inline ``TRY_CAST``) instead of a
        bare ``CAST``.
        """
        return f"TRY_CAST({expr} AS {sql_type})"

    @staticmethod
    def sql_numeric(expr: str, sql_type: str = "DOUBLE") -> str:
        """Cast a triple-store value to a number without failing the query.

        Every object in the triple store is stored as a string, so a numeric
        comparison has to cast. See :meth:`sql_cast` for why this is a
        ``TRY_CAST`` rather than a bare ``CAST``.
        """
        return SQLHelpers.sql_cast(expr, sql_type)
```

- [ ] **Step 4: Re-export `sql_cast` from the helpers package**

In `src/back/core/helpers/__init__.py`, add next to the existing `sql_numeric = SQLHelpers.sql_numeric` line (currently line 15):

```python
sql_escape = SQLHelpers.sql_escape
sql_numeric = SQLHelpers.sql_numeric
sql_cast = SQLHelpers.sql_cast
```

And add `"sql_cast"` to the `__all__` list next to `"sql_numeric"`:

```python
    "sql_escape",
    "sql_numeric",
    "sql_cast",
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run --frozen pytest tests/units/core/test_uri_sql_helpers.py -v`
Expected: PASS (all tests in the file, including the 3 new ones).

- [ ] **Step 6: Run the full existing rule/DQ suite to confirm no regression**

Run: `uv run --frozen pytest tests/units/ontology/test_business_rules.py tests/units/ontology/test_dataquality.py tests/units/ontology/test_swrl_engine.py tests/units/ontology/test_shape_conditions.py -q`
Expected: PASS, same counts as before (these all call `sql_numeric` transitively; behavior is unchanged).

- [ ] **Step 7: Commit**

```bash
git add src/back/core/helpers/SQLHelpers.py src/back/core/helpers/__init__.py tests/units/core/test_uri_sql_helpers.py
git commit -m "feat: add SQLHelpers.sql_cast as the single TRY_CAST emitter"
```

---

### Task 2: `SparqlTranslator._cast_str` → `TRY_CAST`

**Files:**
- Modify: `src/back/core/w3c/sparql/SparqlTranslator.py:27-30`
- Modify: `tests/units/ontology/test_sparql_service.py:57-59`

**Interfaces:**
- Consumes: `back.core.helpers.sql_cast` from Task 1 (import already present in this file for `sql_escape` / `extract_local_name` — extend the same import block).
- Produces: `SparqlTranslator._cast_str(expr, dialect=DIALECT_SPARK) -> str` now returns `TRY_CAST(<expr> AS STRING)`. Every caller of `_cast_str` (`_coalesce_cast_str`, `_subject_expr_from_template`, and the four other call sites at lines 736, 877, 889, 1147, 1160, 1733, 1927) is unaffected code-wise since they all go through this one method — only the emitted string changes.

- [ ] **Step 1: Update the failing test first**

In `tests/units/ontology/test_sparql_service.py`, replace:

```python
    def test_cast_str_spark(self):
        result = _cast_str("col", DIALECT_SPARK)
        assert "CAST(col AS STRING)" == result
```

with:

```python
    def test_cast_str_spark(self):
        result = _cast_str("col", DIALECT_SPARK)
        assert "TRY_CAST(col AS STRING)" == result
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --frozen pytest tests/units/ontology/test_sparql_service.py::TestDialectHelpers::test_cast_str_spark -v`
Expected: FAIL — `AssertionError: assert 'CAST(col AS STRING)' == 'TRY_CAST(col AS STRING)'` (current code still emits bare `CAST`).

- [ ] **Step 3: Implement the change**

In `src/back/core/w3c/sparql/SparqlTranslator.py`, update the import block (lines 8-11):

```python
from back.core.helpers import (
    sql_escape as _escape_sql,
    extract_local_name as _extract_local,
    sql_cast as _sql_cast,
)
```

Then replace `_cast_str` (lines 27-30):

```python
    @staticmethod
    def _cast_str(expr: str, dialect: str = DIALECT_SPARK) -> str:
        """CAST(<expr> AS STRING) for Spark SQL."""
        return f"CAST({expr} AS {SparqlTranslator._string_type(dialect)})"
```

with:

```python
    @staticmethod
    def _cast_str(expr: str, dialect: str = DIALECT_SPARK) -> str:
        """TRY_CAST(<expr> AS STRING) for Spark SQL (ANSI-safe stringification)."""
        return _sql_cast(expr, SparqlTranslator._string_type(dialect))
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run --frozen pytest tests/units/ontology/test_sparql_service.py -v`
Expected: PASS (all tests in the file, including the docstring update at `_coalesce_cast_str` which still reads correctly since it composes `_cast_str`).

- [ ] **Step 5: Update the stale docstring on `_coalesce_cast_str`**

In the same file, `_coalesce_cast_str` (currently lines 32-35):

```python
    @staticmethod
    def _coalesce_cast_str(expr: str, dialect: str = DIALECT_SPARK) -> str:
        """COALESCE(CAST(<expr> AS STRING), '')."""
        return f"COALESCE({SparqlTranslator._cast_str(expr, dialect)}, '')"
```

Update only the docstring (the body already delegates to `_cast_str` and needs no code change):

```python
    @staticmethod
    def _coalesce_cast_str(expr: str, dialect: str = DIALECT_SPARK) -> str:
        """COALESCE(TRY_CAST(<expr> AS STRING), '')."""
        return f"COALESCE({SparqlTranslator._cast_str(expr, dialect)}, '')"
```

Also update the docstring of `_subject_expr_from_template` (around line 71), which says "like `CONCAT(base, CAST(col AS STRING))`" — change to "like `CONCAT(base, TRY_CAST(col AS STRING))`".

- [ ] **Step 6: Run the broader SPARQL test suite**

Run: `uv run --frozen pytest tests/units/ontology/test_sparql_service.py tests/units/ontology/ -k sparql -q`
Expected: PASS. If any test elsewhere asserts a literal `"CAST(...AS STRING)"` string on SPARQL-generated SQL, update it the same way (bare `CAST` → `TRY_CAST`); search first with the command in Step 7.

- [ ] **Step 7: Search for any other literal SPARQL-generated SQL assertions**

Run: `grep -rn "CAST(.*AS STRING)" tests/ --include=*.py`
Expected: only the line fixed in Step 1 previously matched; if this turns up other SPARQL-related asserts still expecting bare `CAST`, update them the same way and re-run their test file.

- [ ] **Step 8: Commit**

```bash
git add src/back/core/w3c/sparql/SparqlTranslator.py tests/units/ontology/test_sparql_service.py
git commit -m "fix: SPARQL-to-Spark stringification uses TRY_CAST"
```

---

### Task 3: `agent_mapping_pge/coverage.py` abstract-union NULL columns

**Files:**
- Modify: `src/agents/agent_mapping_pge/coverage.py:270-298`
- Test: `tests/agents/agent_mapping_pge/test_coverage.py`

**Interfaces:**
- Consumes: `back.core.helpers.sql_cast` from Task 1.
- Produces: `build_abstract_union_mapping(abstract_uri, abstract_entity, subclass_mappings)` unchanged in shape/behavior; only the generated `sql_query` string's NULL-column placeholder changes from `CAST(NULL AS STRING)` to `TRY_CAST(NULL AS STRING)`.

- [ ] **Step 1: Write the failing test**

Add to `tests/agents/agent_mapping_pge/test_coverage.py` (after `test_build_abstract_union_mapping_reuses_subclass_sql`):

```python
def test_build_abstract_union_mapping_null_column_uses_try_cast():
    baby_em = {
        "ontology_class": BABY, "id_column": "ID",
        "sql_query": "SELECT nhs AS ID FROM c.s.baby",
        "attribute_mappings": {},  # no postcode -> NULL column for Baby
    }
    patient = next(e for e in _ontology()["entities"] if e["uri"] == PATIENT)
    patient["attributes"] = [{"name": "nhsnumber"}, {"name": "postcode"}]
    m = cov.build_abstract_union_mapping(PATIENT, patient, [baby_em])
    assert "TRY_CAST(NULL AS STRING) AS postcode" in m["sql_query"]
    assert "CAST(" not in m["sql_query"].replace("TRY_CAST(", "")
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --frozen pytest tests/agents/agent_mapping_pge/test_coverage.py::test_build_abstract_union_mapping_null_column_uses_try_cast -v`
Expected: FAIL — `AssertionError: assert 'TRY_CAST(NULL AS STRING) AS postcode' in '...CAST(NULL AS STRING) AS postcode...'`.

- [ ] **Step 3: Implement the change**

In `src/agents/agent_mapping_pge/coverage.py`, add the import near the top of the file (check existing imports first — file currently imports `List`, `Optional` from `typing`; add the helper import alongside):

```python
from back.core.helpers import sql_cast
```

Then in `build_abstract_union_mapping` (around line 297), replace:

```python
            else:
                cols.append(f"CAST(NULL AS STRING) AS {attr}")
```

with:

```python
            else:
                cols.append(f"{sql_cast('NULL', 'STRING')} AS {attr}")
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run --frozen pytest tests/agents/agent_mapping_pge/test_coverage.py -v`
Expected: PASS (all tests in the file, including the pre-existing ones — `test_build_abstract_union_mapping_reuses_subclass_sql` does not assert on the NULL-column text so it is unaffected).

- [ ] **Step 5: Update the module docstring reference**

The docstring of `build_abstract_union_mapping` says "subclasses that do not carry an attribute contribute ``NULL`` for it" — this is still accurate, no change needed. Leave as-is.

- [ ] **Step 6: Commit**

```bash
git add src/agents/agent_mapping_pge/coverage.py tests/agents/agent_mapping_pge/test_coverage.py
git commit -m "fix: abstract-union NULL columns use TRY_CAST"
```

---

### Task 4: `lakebase/_companion_ddl.py` Lakeflow view wrap

**Files:**
- Modify: `src/back/core/graphdb/lakebase/_companion_ddl.py:85-92`
- Modify: `tests/units/core/test_lakebase_flat_store.py:742-748`

**Interfaces:**
- Consumes: `back.core.helpers.sql_cast` from Task 1.
- Produces: `wrap_triple_view_sql_for_lakeflow(spark_sql: str) -> str` unchanged signature; the `object_hash` column expression changes from `sha2(cast(object AS string), 256)` to `sha2(TRY_CAST(object AS STRING), 256)`.

- [ ] **Step 1: Update the failing test first**

In `tests/units/core/test_lakebase_flat_store.py`, replace (lines 742-748):

```python
def test_wrap_triple_view_sql_for_lakeflow_adds_object_hash():
    from back.core.graphdb.lakebase._companion_ddl import wrap_triple_view_sql_for_lakeflow

    inner = "SELECT 's' AS subject, 'p' AS predicate, 'o' AS object"
    wrapped = wrap_triple_view_sql_for_lakeflow(inner)
    assert "sha2(cast(object AS string), 256) AS object_hash" in wrapped
    assert wrapped.endswith("FROM (SELECT 's' AS subject, 'p' AS predicate, 'o' AS object) AS _ob_triples")
```

with:

```python
def test_wrap_triple_view_sql_for_lakeflow_adds_object_hash():
    from back.core.graphdb.lakebase._companion_ddl import wrap_triple_view_sql_for_lakeflow

    inner = "SELECT 's' AS subject, 'p' AS predicate, 'o' AS object"
    wrapped = wrap_triple_view_sql_for_lakeflow(inner)
    assert "sha2(TRY_CAST(object AS STRING), 256) AS object_hash" in wrapped
    assert "CAST(" not in wrapped.replace("TRY_CAST(", "")
    assert wrapped.endswith("FROM (SELECT 's' AS subject, 'p' AS predicate, 'o' AS object) AS _ob_triples")
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --frozen pytest tests/units/core/test_lakebase_flat_store.py::test_wrap_triple_view_sql_for_lakeflow_adds_object_hash -v`
Expected: FAIL — the current code emits `sha2(cast(object AS string), 256)`.

- [ ] **Step 3: Implement the change**

In `src/back/core/graphdb/lakebase/_companion_ddl.py`, add the import (near the existing `from back.core.helpers import safe_identifier` at the top of the file):

```python
from back.core.helpers import safe_identifier, sql_cast
```

Then replace `wrap_triple_view_sql_for_lakeflow` (lines 85-92):

```python
def wrap_triple_view_sql_for_lakeflow(spark_sql: str) -> str:
    """Wrap translated Spark SQL so the view exposes ``object_hash`` for Lakeflow."""
    inner = spark_sql.strip().rstrip(";")
    return (
        "SELECT subject, predicate, object, "
        "sha2(cast(object AS string), 256) AS object_hash "
        f"FROM ({inner}) AS _ob_triples"
    )
```

with:

```python
def wrap_triple_view_sql_for_lakeflow(spark_sql: str) -> str:
    """Wrap translated Spark SQL so the view exposes ``object_hash`` for Lakeflow."""
    inner = spark_sql.strip().rstrip(";")
    return (
        "SELECT subject, predicate, object, "
        f"sha2({sql_cast('object', 'STRING')}, 256) AS object_hash "
        f"FROM ({inner}) AS _ob_triples"
    )
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run --frozen pytest tests/units/core/test_lakebase_flat_store.py -v`
Expected: PASS (all tests in the file — this is a large shared file, confirm the count matches pre-change minus the one now-updated assertion).

- [ ] **Step 5: Commit**

```bash
git add src/back/core/graphdb/lakebase/_companion_ddl.py tests/units/core/test_lakebase_flat_store.py
git commit -m "fix: Lakeflow object_hash view wrap uses TRY_CAST"
```

---

### Task 5: Cross-module guard test

**Files:**
- Create: `tests/units/core/test_try_cast_policy.py`

**Interfaces:**
- Consumes: `SparqlTranslator._cast_str` / `_coalesce_cast_str` / `_subject_expr_from_template` (Task 2), `agents.agent_mapping_pge.coverage.build_abstract_union_mapping` (Task 3), `back.core.graphdb.lakebase._companion_ddl.wrap_triple_view_sql_for_lakeflow` (Task 4), `SQLHelpers.sql_cast` (Task 1).
- Produces: nothing consumed elsewhere — this is a terminal regression guard.

- [ ] **Step 1: Write the guard test module**

Create `tests/units/core/test_try_cast_policy.py`:

```python
"""Guard: the Spark SQL emitters this project controls must never emit a bare
``CAST(``. Databricks warehouses run with ANSI mode on, where a bare ``CAST``
aborts the whole statement on the first value that will not convert;
``TRY_CAST`` yields NULL instead. See
``documentation/superpowers/specs/2026-08-05-spark-try-cast-design.md``.

Deliberately excluded (see the design doc's "Amendment" section):
``src/jobs/graph_analytics_job.py`` and
``src/back/core/graph_analysis/JobMetrics.py`` — their SQL runs literally
against SQLite in tests and is parse-checked under Postgres, neither of which
understands ``TRY_CAST``, and every cast there is on already-numeric data
(counts, literals, typed NULLs), never raw triple-store strings.
"""

import pytest

pytestmark = pytest.mark.unit


def _no_bare_cast(sql: str) -> bool:
    return "CAST(" not in sql.replace("TRY_CAST(", "")


class TestSqlHelpers:
    def test_sql_cast_has_no_bare_cast(self):
        from back.core.helpers.SQLHelpers import SQLHelpers

        assert _no_bare_cast(SQLHelpers.sql_cast("x", "DOUBLE"))


class TestSparqlTranslator:
    def test_cast_str_has_no_bare_cast(self):
        from back.core.w3c.sparql.SparqlTranslator import SparqlTranslator

        assert _no_bare_cast(SparqlTranslator._cast_str("col"))

    def test_coalesce_cast_str_has_no_bare_cast(self):
        from back.core.w3c.sparql.SparqlTranslator import SparqlTranslator

        assert _no_bare_cast(SparqlTranslator._coalesce_cast_str("col"))

    def test_subject_expr_from_template_has_no_bare_cast(self):
        from back.core.w3c.sparql.SparqlTranslator import SparqlTranslator

        result = SparqlTranslator._subject_expr_from_template(
            "http://ex.org/Customer/{id}", "customer_id", alias="c"
        )
        assert "TRY_CAST(c.customer_id AS STRING)" in result
        assert _no_bare_cast(result)


class TestAbstractUnionMapping:
    def test_null_column_has_no_bare_cast(self):
        from agents.agent_mapping_pge import coverage as cov

        abstract_entity = {"name": "Patient", "attributes": [{"name": "postcode"}]}
        subclass_mapping = {
            "ontology_class": "u#Baby",
            "id_column": "ID",
            "sql_query": "SELECT nhs AS ID FROM c.s.baby",
            "attribute_mappings": {},
        }
        m = cov.build_abstract_union_mapping("u#Patient", abstract_entity, [subclass_mapping])
        assert _no_bare_cast(m["sql_query"])


class TestLakeflowWrap:
    def test_object_hash_expression_has_no_bare_cast(self):
        from back.core.graphdb.lakebase._companion_ddl import (
            wrap_triple_view_sql_for_lakeflow,
        )

        wrapped = wrap_triple_view_sql_for_lakeflow("SELECT 1 AS object")
        assert _no_bare_cast(wrapped)
```

- [ ] **Step 2: Run the new guard test file**

Run: `uv run --frozen pytest tests/units/core/test_try_cast_policy.py -v`
Expected: PASS (7 tests) — Tasks 1-4 are already applied at this point, so every assertion should already hold.

- [ ] **Step 3: Confirm the guard actually catches a regression**

Temporarily edit `src/back/core/w3c/sparql/SparqlTranslator.py`'s `_cast_str` to return a bare `CAST(...)` again, re-run `uv run --frozen pytest tests/units/core/test_try_cast_policy.py::TestSparqlTranslator -v`, confirm it FAILS, then revert the temporary edit (`git checkout -- src/back/core/w3c/sparql/SparqlTranslator.py`) and re-run to confirm PASS again. This is a manual sanity check, not a committed step.

- [ ] **Step 4: Commit**

```bash
git add tests/units/core/test_try_cast_policy.py
git commit -m "test: guard that Spark SQL emitters never emit a bare CAST"
```

---

### Task 6: Full test run, changelog, docs

**Files:**
- Modify: `/changelogs/v0.7.0/benoitcayladbx_<today's date, YYYY-MM-DD>.log` (append a new section if the file already exists for today; otherwise create it)

**Interfaces:** None — this task wraps up the change set.

- [ ] **Step 1: Run the full unit test suite**

Run: `uv run --frozen pytest -q -m "not scenario"`
Expected: PASS, same total count as the pre-change baseline plus the new tests added in Tasks 1-5 (4 + 1 + 1 + 5 or 7 new tests depending on how many were folded together).

- [ ] **Step 2: Grep-verify no bare CAST remains in the three call-site files**

Run: `grep -n "CAST(" src/back/core/w3c/sparql/SparqlTranslator.py src/agents/agent_mapping_pge/coverage.py src/back/core/graphdb/lakebase/_companion_ddl.py | grep -v TRY_CAST`
Expected: no output (empty).

- [ ] **Step 3: Write the changelog entry**

Determine today's date and check whether `/changelogs/v0.7.0/benoitcayladbx_<date>.log` already exists:

Run: `ls changelogs/v0.7.0/ | grep benoitcayladbx`

If a file for today exists, append a new `##`-titled section to it. If not, create the file. Use this content (fill in the actual date and actual test count from Step 1's output):

```markdown
## Use TRY_CAST for remaining Spark SQL emitters

### Context
Databricks warehouses run with ANSI mode on, where a bare `CAST` aborts the
whole statement on the first value that will not convert. Rule/DQ SQL
(SWRL, decision tables, aggregates, SHACL guards) already used `TRY_CAST` via
`SQLHelpers.sql_numeric`. Three more Spark SQL emitters still used a bare
`CAST` for stringification and typed NULLs.

### Changes
1. `src/back/core/helpers/SQLHelpers.py` — added `sql_cast(expr, sql_type)`
   emitting `TRY_CAST`; `sql_numeric` is now a thin wrapper over it (same
   signature, same behavior, no caller changes).
2. `src/back/core/helpers/__init__.py` — re-exported `sql_cast`.
3. `src/back/core/w3c/sparql/SparqlTranslator.py` — `_cast_str` (used by
   every SPARQL-to-Spark stringification/subject-URI path) now emits
   `TRY_CAST(... AS STRING)`.
4. `src/agents/agent_mapping_pge/coverage.py` — abstract-union NULL
   placeholder columns now use `TRY_CAST(NULL AS STRING)`.
5. `src/back/core/graphdb/lakebase/_companion_ddl.py` —
   `wrap_triple_view_sql_for_lakeflow`'s `object_hash` expression now uses
   `TRY_CAST(object AS STRING)`.

Deliberately excluded: `src/jobs/graph_analytics_job.py` and
`src/back/core/graph_analysis/JobMetrics.py`. Their generated SQL executes
literally against SQLite in tests and is parse-checked against Postgres;
`TRY_CAST` is valid in neither. Every cast in those two files is also on
already-numeric data (counts, literals, typed NULLs) rather than raw
triple-store strings, so the ANSI-abort risk `TRY_CAST` guards against does
not apply there. See
`documentation/superpowers/specs/2026-08-05-spark-try-cast-design.md`
("Amendment" section) for the full analysis.

### Modified files
- `src/back/core/helpers/SQLHelpers.py`
- `src/back/core/helpers/__init__.py`
- `src/back/core/w3c/sparql/SparqlTranslator.py`
- `src/agents/agent_mapping_pge/coverage.py`
- `src/back/core/graphdb/lakebase/_companion_ddl.py`
- `tests/units/core/test_sql_helpers.py` (new)
- `tests/units/ontology/test_sparql_service.py`
- `tests/agents/agent_mapping_pge/test_coverage.py`
- `tests/units/core/test_lakebase_flat_store.py`
- `tests/units/core/test_try_cast_policy.py` (new)

### Test result
`uv run --frozen pytest -q -m "not scenario"` — <PASTE ACTUAL RESULT COUNT HERE>
```

- [ ] **Step 4: Commit the changelog**

```bash
git add changelogs/v0.7.0/
git commit -m "docs: changelog for TRY_CAST Spark SQL policy"
```

---

## Self-Review Notes (already applied above)

- **Spec coverage:** every remaining call site from the design doc's "Call sites" table (SparqlTranslator, coverage.py, `_companion_ddl.py`) has a task; the SQLHelpers helper is Task 1; the guard test is Task 5; the excluded files are called out explicitly in Global Constraints and in the guard test's own docstring so a future contributor does not "fix" them by mistake.
- **Placeholder scan:** no TBD/TODO; every step has literal code.
- **Type consistency:** `sql_cast(expr: str, sql_type: str) -> str` is the same signature used in every task that calls it (Tasks 2, 3, 4, 5).
