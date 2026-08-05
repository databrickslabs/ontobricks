# Spark SQL TRY_CAST Policy Design

## Context

Databricks SQL warehouses run with ANSI mode on. A bare `CAST` of a value that
cannot convert aborts the entire statement. Triple-store objects are stored as
strings, so numeric / date casts in rule and DQ SQL are especially fragile: one
malformed cell (`"free"` in a fee column) fails the whole rule.

Part of this was already fixed (2026-08-03): SWRL SQL templates, decision
tables, aggregate rules, SPARQL FILTER numerics, and SHACL numeric guards go
through `SQLHelpers.sql_numeric` → `TRY_CAST`. Remaining Spark emitters still
use bare `CAST` for stringification, analytics literals, typed nulls, and one
Lakebase Lakeflow wrap.

## Goal

Every Spark / Databricks SQL fragment OntoBricks emits uses `TRY_CAST`, never
bare `CAST`. Failed casts become NULL instead of aborting the query.

## Policy

| Case | Emission |
|------|----------|
| Any typed cast in Spark SQL | `TRY_CAST(<expr> AS <type>)` |
| Stringification | `TRY_CAST(<expr> AS STRING)` |
| Literals | `TRY_CAST(1.0 AS DOUBLE)`, etc. |
| Typed nulls | `TRY_CAST(NULL AS BIGINT)`, etc. |

Out of scope (do not change):

- SWRL **Cypher** templates (Neo4j; no Spark `TRY_CAST`)
- Agent prompt / evaluator prose that *mentions* CAST as guidance
- Historical docs under `documentation/superpowers/plans/` (optional cleanup)
- `src/jobs/graph_analytics_job.py` and `src/back/core/graph_analysis/JobMetrics.py`
  — see **Amendment** below

### Amendment (found during plan-writing): graph_analytics_job.py / JobMetrics.py excluded

Both files generate SQL that is executed **literally against SQLite** in their
test suites (`test_graph_analytics_job_sql.py`, `test_job_metrics.py`, each an
oracle comparing results to NetworkX) and parse-checked under a **Postgres**
sqlglot dialect (`TestJobSqlDialects`, `test_read_back_sql_parses_in_both_dialects`),
in addition to running on Spark/Databricks in production. `TRY_CAST` is not
valid SQLite syntax (confirmed: `near "AS": syntax error`) and is not a
standard Postgres function, so switching these files would break both the
SQLite execution oracle and the Postgres portability guarantee.

Every `CAST(` in these two files also casts **already-numeric, non-adversarial
data** — `COUNT(*)`-derived columns (`d.d`, `cl.reached`), Python-interpolated
integer literals (`{pivot_count}`), float literals (`1.0`, `0.0`), and typed
`NULL`s. None of them ever cast a raw triple-store `object` string, so the
ANSI-abort failure mode `TRY_CAST` protects against cannot occur here. These
two files are therefore excluded from this change; their portable `CAST` stays
as-is.

## Design

### Helper

Extend `SQLHelpers` in `src/back/core/helpers/SQLHelpers.py`:

```python
@staticmethod
def sql_cast(expr: str, sql_type: str) -> str:
    """Emit TRY_CAST for Spark / Databricks SQL (ANSI-safe)."""
    return f"TRY_CAST({expr} AS {sql_type})"
```

`sql_numeric(expr, sql_type="DOUBLE")` becomes a thin wrapper over `sql_cast`
so existing rule/DQ callers keep working without churn.

Re-export `sql_cast` from `back.core.helpers` (`__init__.py` + `__all__`).

### Call sites

| File | Change |
|------|--------|
| `src/back/core/w3c/sparql/SparqlTranslator.py` | `_cast_str` uses `sql_cast` / `TRY_CAST(… AS STRING)` |
| `src/agents/agent_mapping_pge/coverage.py` | `CAST(NULL AS …)` → `TRY_CAST(NULL AS …)` |
| `src/back/core/graphdb/lakebase/_companion_ddl.py` | Lakeflow wrap `cast(object AS string)` → `TRY_CAST(object AS STRING)` |

~~`src/jobs/graph_analytics_job.py`~~ and ~~`src/back/core/graph_analysis/JobMetrics.py`~~
— excluded, see Amendment above.

Prefer the helper where a single expression is built (`SparqlTranslator`,
typed null helpers). Inline `TRY_CAST` is fine in large f-string SQL builders
(`graph_analytics_job`) for readability, as long as no bare `CAST(` remains.

### Tests

1. Fix stale asserts:
   - `tests/units/ontology/test_dataquality.py` — expect `TRY_CAST(c0.object AS DOUBLE)`
   - `tests/units/ontology/test_sparql_service.py` — expect `TRY_CAST(col AS STRING)`
2. Add / extend a guard: Spark SQL emitters under the listed modules (the
   three in the Call sites table above) must not contain bare `CAST(` after
   stripping `TRY_CAST(`. Exclude Cypher templates, non-SQL agent prompt
   strings, and `graph_analytics_job.py` / `JobMetrics.py` (Amendment above).
3. Unit test `sql_cast("x", "DOUBLE") == "TRY_CAST(x AS DOUBLE)"` and that
   `sql_numeric` delegates to the same form.

### Behaviour notes

- `TRY_CAST(NULL AS T)` and `TRY_CAST(<literal> AS T)` are valid Spark SQL and
  preserve current result shapes when conversion succeeds.
- When conversion fails, NULL propagates; comparisons become unknown/false and
  URI concatenations may yield NULL subjects — same trade-off already accepted
  for rule SQL.
- Postgres companion DDL that is not Spark (e.g. `pg_class` queries) is
  untouched; only the Spark fragment in `wrap_triple_view_sql_for_lakeflow`
  changes.

## Success criteria

- Grep of the three call-site files above shows no bare `CAST(` in emitted SQL.
- Existing rule/DQ TRY_CAST tests still pass.
- `graph_analytics_job.py` / `JobMetrics.py` SQL and their SQLite/Postgres
  dialect tests are unchanged.
- Stale CAST asserts updated and green.
- Guard test fails if a listed emitter reintroduces bare `CAST(`.
