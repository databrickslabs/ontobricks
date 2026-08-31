# Analytics: Lakeflow-only, R2RML-sourced — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make graph analytics compute *every* metric in the Lakeflow job, always reading the R2RML-derived `…_data` snapshot, so the same domain yields the same KPIs on Lakehouse, Lakebase and Neo4j.

**Architecture:** Build unconditionally materialises `…_data` from the R2RML VIEW for every engine. Analytics resolves that table (never the engine store), runs the Lakeflow job with `class_filter` / `exclude_predicates` / `pivots` / `max_depth`, and reads four output tables back. The NetworkX and SQL-pushdown compute paths are deleted; the app assembles the job's output and applies only the pure-Python "flat dataset" labelling.

**Tech Stack:** Python 3.11, FastAPI, Databricks SDK (Lakeflow `run_now`), Databricks SQL warehouse, Delta / Unity Catalog, portable SQL executed on Spark in production and on SQLite in tests, pytest, NetworkX as a test-only oracle.

**Spec:** `documentation/superpowers/specs/2026-07-31-analytics-lakeflow-only-design.md`

## Global Constraints

- Job SQL must stay portable: `tests/units/core/test_graph_analytics_job_sql.py` executes the exact same statements against SQLite. No `collect_set`, no `FILTER (WHERE …)`, no Spark-only functions. `least` / `greatest` / `md5` / window functions are already proven to work in both.
- Every table the job writes is created with `GraphAnalyticsSQL._recreate` (`DROP TABLE IF EXISTS` + `CREATE TABLE … AS`), never `CREATE OR REPLACE TABLE`.
- Table names are interpolated into SQL unquoted. Any new identifier goes through `validate_identifier`; any new string literal goes through `sql_escape` / `_in_list`.
- `DEFAULT_EXCLUDED_PREDICATES` in `src/jobs/graph_analytics_job.py` must stay byte-identical in content to `GraphBuilder._DEFAULT_EXCLUDED_PREDICATES`: `rdf:type`, `rdfs:label`, `rdfs:comment`, `rdfs:seeAlso`.
- `MetricsRequest.predicate_filter` is an **exclusion** list and maps onto the job's existing `--exclude-predicates`. It is not a new parameter.
- Analytics KPIs are defined on the **mapped graph only**. Inferred and cohort triples are out of scope by decision.
- `networkx` stays a runtime dependency — community detection and cohort discovery still use it. Only the analytics NetworkX path is deleted.
- `Settings.analytics_max_triples` stays (used by `POST /clusters/detect`). `Settings.analytics_pushdown_enabled` is deleted.
- Run tests with the Databricks and Lakebase env vars unset. Without this the SDK
  tries to reach a real workspace and the suite hangs, and three unrelated tests
  fail on leaked local config (`test_health.py` reads the real CLI profile,
  `test_graph_engine_config.py` reads the real Lakebase database name):

```bash
env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u DATABRICKS_CONFIG_PROFILE \
    -u LAKEBASE_PROJECT -u LAKEBASE_BRANCH -u LAKEBASE_DATABASE \
    -u LAKEBASE_DATABASE_RESOURCE_SEGMENT -u LAKEBASE_SCHEMA \
    uv run pytest -q -m "not scenario"
```

  Baseline with the full list, as of Task 5: **3798 passed, 275 skipped**. Any
  failure beyond that is yours.
- Changelog is mandatory after every task: `changelogs/v0.7.0/benoitcayladbx_2026-07-31.log` (append a section; do not create a second file for the same day).

## File Structure

| File | Responsibility after this plan |
|---|---|
| `src/jobs/graph_analytics_job.py` | The **only** metric computation. Adds `class_filter`, per-node `type_uri`/`label`, `_type_profiles`, `_type_predicates`, `total_node_count`. |
| `src/back/core/graph_analysis/JobMetrics.py` | Trigger the job, then *assemble* `MetricsResult` from its four output tables. No graph computation, no store. |
| `src/back/core/graph_analysis/LakeflowRunner.py` | Parameter plumbing only. Gains `class_filter`. |
| `src/back/core/graph_analysis/profiles.py` | Pure-Python flat/temporal labelling of job output. Unchanged logic, corrected docstring. |
| `src/back/core/graph_analysis/GraphMetrics.py` | **Deleted.** |
| `src/back/core/graph_analysis/PushdownMetrics.py` | **Deleted.** |
| `src/back/objects/digitaltwin/_build_pipeline.py` | Guarantees `…_data` exists after every Build, for every engine. |
| `src/back/objects/digitaltwin/DigitalTwin.py` | Single analytics path: resolve source → `JobMetrics`. No mode argument. |
| `src/api/routers/internal/dtwin.py` | Three-step preflight, no mode selection, thinner stats payload. |
| `src/front/templates/partials/dtwin/_query_analytics.html` | Job-only wording; Run Analysis gated on `analytics_job_available`. |
| `resources/graph_analytics.job.yml` | Declares `class_filter`. |

Task order is deliberate: the job grows its new outputs first (Tasks 1–3), then the plumbing that carries the new parameter (Task 4), then the app-side rewrite that consumes them (Tasks 5–6), then Build (Task 7), then the collapse and deletions (Tasks 8–9), then UI (Task 10), then docs and deploy (Task 11). Deletions land late so the suite stays green throughout.

---

### Task 1: Job — per-node `type_uri` and `label`

The job's per-node output table gains the two columns the app used to get from
`PushdownMetrics.get_top_nodes_by_degree`. A node with several `rdf:type`
triples resolves to `MIN(object)` so the choice is deterministic across runs.

**Files:**
- Modify: `src/jobs/graph_analytics_job.py` (`GraphAnalyticsSQL.write_output`, currently lines 537-605)
- Test: `tests/units/core/test_graph_analytics_job_sql.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: the output table `<output_table>` gains columns `type_uri TEXT NULL` and `label TEXT NULL`, in addition to the existing `node_uri, degree_raw, degree, pagerank, component_id, clustering, betweenness, closeness`. Tasks 2 and 6 both rely on `type_uri` being present on this table.

- [ ] **Step 1: Write the failing test**

Read `tests/units/core/test_graph_analytics_job_sql.py` first: it builds a
SQLite database holding a `triples` table, runs `run_analysis` against it, and
compares the result with NetworkX. Reuse its existing seeding harness — do not
add a second one.

Add one shared reader next to the existing fixtures. Task 2 uses it too, so it
takes the table suffix as an argument:

```python
def _table_rows(tmp_path, triples, *, suffix=""):
    """Run the job over *triples*, return one output table as a list of dicts.

    ``suffix`` selects which output table to read: "" is the per-node table,
    "_summary" / "_type_profiles" / "_type_predicates" the others.
    """
    conn, builder = _run_job(tmp_path, triples)
    cur = conn.execute(f"SELECT * FROM {builder.output_table}{suffix}")
    names = [d[0] for d in cur.description]
    return [dict(zip(names, r)) for r in cur.fetchall()]
```

`_run_job` is whatever the file already calls to build the SQLite connection and
the `GraphAnalyticsSQL` builder and drive `run_analysis`. If it does not return
both the connection and the builder, widen its return value rather than writing
a parallel harness. (Task 3 adds a `class_filter` argument to both helpers when
it adds the field they would forward it to — do not add the parameter now, while
nothing can consume it.)

Now the test:

```python
def test_output_carries_rdf_type_and_label(tmp_path):
    """The per-node output resolves one type and one label per node."""
    triples = [
        ("http://ex/a", "http://ex/knows", "http://ex/b"),
        ("http://ex/a", RDF_TYPE, "http://ex/Person"),
        ("http://ex/a", RDFS_LABEL, "Alice"),
        ("http://ex/b", RDF_TYPE, "http://ex/Person"),
        # A second type must not duplicate the node's output row.
        ("http://ex/b", RDF_TYPE, "http://ex/Agent"),
    ]
    rows = _table_rows(tmp_path, triples)

    by_uri = {r["node_uri"]: r for r in rows}
    assert len(rows) == 2
    assert by_uri["http://ex/a"]["type_uri"] == "http://ex/Person"
    assert by_uri["http://ex/a"]["label"] == "Alice"
    # MIN over {Agent, Person} — deterministic, not arbitrary.
    assert by_uri["http://ex/b"]["type_uri"] == "http://ex/Agent"
    assert by_uri["http://ex/b"]["label"] is None
```

- [ ] **Step 2: Run the test and watch it fail**

Run: `env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u LAKEBASE_PROJECT -u LAKEBASE_BRANCH uv run pytest tests/units/core/test_graph_analytics_job_sql.py::test_output_carries_rdf_type_and_label -v`

Expected: FAIL with `KeyError: 'type_uri'` (the column does not exist yet).

- [ ] **Step 3: Add the two columns to `write_output`**

In `src/jobs/graph_analytics_job.py`, `write_output` currently builds a `joins`
string only for the pivot case. Restructure so the metadata joins are always
present. Replace the tail of the method (from `if pivot_count > 0 and node_count > 2:`
to the final `return`) with:

```python
        if pivot_count > 0 and node_count > 2:
            scale = (1.0 / ((node_count - 1) * (node_count - 2))) * (
                float(node_count) / float(pivot_count)
            )
            betweenness = (
                f"  COALESCE(bc.raw, 0.0) * {scale} AS betweenness,\n"
                f"  CASE\n"
                f"    WHEN cl.dist_sum IS NULL OR cl.dist_sum <= 0 THEN 0.0\n"
                f"    ELSE (CAST(cl.reached AS DOUBLE) * cl.reached)\n"
                f"         / ((CAST({pivot_count} AS DOUBLE)"
                f" - CASE WHEN pv.n IS NULL THEN 0 ELSE 1 END) * cl.dist_sum)\n"
                f"  END AS closeness,\n"
            )
            pivot_joins = (
                f"\nLEFT JOIN {self.betweenness_table} bc ON bc.node = d.n"
                f"\nLEFT JOIN {self.closeness_table} cl ON cl.node = d.n"
                f"\nLEFT JOIN {self.pivots} pv ON pv.n = d.n"
            )
        else:
            betweenness = "  0.0 AS betweenness,\n  0.0 AS closeness,\n"
            pivot_joins = ""

        return self._recreate(
            self.output_table,
            f"SELECT\n"
            f"  d.n AS node_uri,\n"
            f"  d.d AS degree_raw,\n"
            f"  CAST(d.d AS DOUBLE) / {divisor} AS degree,\n"
            f"  p.rank AS pagerank,\n"
            f"  cc.component_id AS component_id,\n"
            f"  CASE WHEN d.d < 2 THEN 0.0\n"
            f"       ELSE 2.0 * COALESCE(t.t, 0) / (CAST(d.d AS DOUBLE) * (d.d - 1))\n"
            f"  END AS clustering,\n"
            f"{betweenness}"
            f"  ty.type_uri AS type_uri,\n"
            f"  lb.label AS label\n"
            f"FROM {self.deg} d\n"
            f"JOIN {pr} p ON p.n = d.n\n"
            f"JOIN {cc} cc ON cc.n = d.n\n"
            f"LEFT JOIN {self.triangle_counts} t ON t.n = d.n\n"
            f"LEFT JOIN ({self.node_type_select()}) ty ON ty.n = d.n\n"
            f"LEFT JOIN ({self.node_label_select()}) lb ON lb.n = d.n"
            + pivot_joins,
        )
```

Note the moved trailing comma: `betweenness` now always ends with `,` because
`type_uri` follows it.

Add the two subquery builders just above `write_output`, so Task 2 can reuse
`node_type_select()` rather than repeating the SQL:

```python
    def node_type_select(self) -> str:
        """One ``rdf:type`` per node.

        ``MIN`` rather than an arbitrary pick: a node typed both ``Person`` and
        ``Agent`` must land in the same profile on every run, or the per-type
        rollups move between runs for no reason.
        """
        return (
            f"SELECT subject AS n, MIN(object) AS type_uri\n"
            f"FROM {self.source_table}\n"
            f"WHERE predicate = '{sql_escape(RDF_TYPE)}' AND object <> ''\n"
            f"GROUP BY subject"
        )

    def node_label_select(self) -> str:
        """One ``rdfs:label`` per node, chosen the same way as the type."""
        return (
            f"SELECT subject AS n, MIN(object) AS label\n"
            f"FROM {self.source_table}\n"
            f"WHERE predicate = '{sql_escape(RDFS_LABEL)}' AND object <> ''\n"
            f"GROUP BY subject"
        )
```

- [ ] **Step 4: Run the new test plus the whole job-SQL suite**

Run: `env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u LAKEBASE_PROJECT -u LAKEBASE_BRANCH uv run pytest tests/units/core/test_graph_analytics_job_sql.py -v`

Expected: PASS, including the pre-existing NetworkX parity tests. The parity
tests select named columns, so two extra columns must not disturb them. If one
of them does `SELECT *` and unpacks positionally, fix that test to select by
name rather than reordering the output columns.

- [ ] **Step 5: Commit**

```bash
git add src/jobs/graph_analytics_job.py tests/units/core/test_graph_analytics_job_sql.py
git commit -m "feat(analytics): job resolves rdf:type and rdfs:label per node"
```

---

### Task 2: Job — `_type_profiles` and `_type_predicates` output tables

Per-entity-type aggregation moves from `PushdownMetrics` into the job. The job
writes two extra tables plus one extra summary column, and the app will later
just `SELECT` from them.

**Files:**
- Modify: `src/jobs/graph_analytics_job.py` (`GraphAnalyticsSQL` table-name properties, new `type_profiles` / `type_predicates` builders, `write_summary`, `run_analysis`)
- Test: `tests/units/core/test_graph_analytics_job_sql.py`

**Interfaces:**
- Consumes: `GraphAnalyticsSQL.node_type_select()` and the `type_uri` column on `<output_table>` from Task 1.
- Produces:
  - `<output_table>_type_profiles(type_uri TEXT, instance_count BIGINT, connected_count BIGINT, degree_sum BIGINT, avg_clustering DOUBLE, avg_betweenness DOUBLE)`
  - `<output_table>_type_predicates(type_uri TEXT, predicate TEXT)` — distinct rows
  - `<output_table>_summary` gains `total_node_count BIGINT`
  - `GraphAnalyticsSQL.type_profiles_table` / `.type_predicates_table` properties
  - `run_analysis` returns `stats["total_node_count"]`

  Task 6 reads all of these.

- [ ] **Step 1: Write the failing tests**

```python
def test_type_profiles_cover_isolated_and_connected_instances(tmp_path):
    """instance_count is the full population; the rest covers scored nodes."""
    triples = [
        ("http://ex/a", "http://ex/knows", "http://ex/b"),
        ("http://ex/a", RDF_TYPE, "http://ex/Person"),
        ("http://ex/b", RDF_TYPE, "http://ex/Person"),
        # Typed but with no entity-entity edge: counted, never scored.
        ("http://ex/c", RDF_TYPE, "http://ex/Person"),
        ("http://ex/c", RDFS_LABEL, "Carol"),
    ]
    profiles = _table_rows(tmp_path, triples, suffix="_type_profiles")

    assert len(profiles) == 1
    row = profiles[0]
    assert row["type_uri"] == "http://ex/Person"
    assert row["instance_count"] == 3     # a, b, c
    assert row["connected_count"] == 2    # a, b
    assert row["degree_sum"] == 2         # one undirected edge, two endpoints
    assert row["avg_clustering"] == 0.0


def test_type_predicates_are_distinct_and_exclude_metadata(tmp_path):
    """rdf:type and rdfs:label never count as relationship predicates."""
    triples = [
        ("http://ex/a", "http://ex/knows", "http://ex/b"),
        ("http://ex/a", "http://ex/knows", "http://ex/c"),
        ("http://ex/a", "http://ex/owns", "http://ex/b"),
        ("http://ex/a", RDF_TYPE, "http://ex/Person"),
        ("http://ex/a", RDFS_LABEL, "Alice"),
        # A literal object is not a relationship.
        ("http://ex/a", "http://ex/age", "41"),
    ]
    rows = _table_rows(tmp_path, triples, suffix="_type_predicates")

    pairs = {(r["type_uri"], r["predicate"]) for r in rows}
    assert pairs == {
        ("http://ex/Person", "http://ex/knows"),
        ("http://ex/Person", "http://ex/owns"),
    }


def test_summary_reports_total_and_connected_node_counts(tmp_path):
    """node_count is the scored graph; total_node_count is every subject."""
    triples = [
        ("http://ex/a", "http://ex/knows", "http://ex/b"),
        ("http://ex/c", RDF_TYPE, "http://ex/Person"),  # isolated subject
    ]
    summary = _table_rows(tmp_path, triples, suffix="_summary")[0]

    assert summary["node_count"] == 2        # a, b
    assert summary["total_node_count"] == 3  # a, b, c
```

`_table_rows` is the helper Task 1 added; no new fixture is needed.

- [ ] **Step 2: Run the tests and watch them fail**

Run: `env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u LAKEBASE_PROJECT -u LAKEBASE_BRANCH uv run pytest tests/units/core/test_graph_analytics_job_sql.py -k "type_profiles or type_predicates or total_and_connected" -v`

Expected: FAIL — `sqlite3.OperationalError: no such table: …_type_profiles` for
the first two, and `KeyError: 'total_node_count'` for the third.

- [ ] **Step 3: Add the table names**

In `GraphAnalyticsSQL`, next to `summary_table`:

```python
    @property
    def type_profiles_table(self) -> str:
        return f"{self.output_table}_type_profiles"

    @property
    def type_predicates_table(self) -> str:
        return f"{self.output_table}_type_predicates"
```

These are outputs, not work tables, so they must **not** be added to
`work_tables()` — that list is dropped at the end of every run.

- [ ] **Step 4: Add the total node count query**

Next to `node_count_query`:

```python
    def total_node_count_query(self) -> str:
        """Every distinct entity URI in the source, connected or not.

        ``node_count`` counts only nodes that survived edge construction, so a
        domain of fully isolated instances would otherwise report zero nodes and
        look broken rather than flat.

        Read straight from the source rather than from the degree table: this is
        a population total, and a total that moved when the user narrowed the
        run would be indefensible. Neither per-run filter may reach it — not the
        class filter, and not ``excluded_predicates`` either, which the app sets
        per request.

        An entity is anything appearing as a subject, or as a URI object of a
        predicate that can carry one. The predicate test uses the fixed
        metadata list rather than ``self.excluded_predicates`` for exactly that
        reason, and applies to the object side only: a subject is an entity
        whatever it carries, while ``rdf:type``'s object is a class and
        ``rdfs:label``'s is a literal.
        """
        return (
            f"SELECT COUNT(*) AS n FROM (\n"
            f"  SELECT DISTINCT subject AS n FROM {self.source_table}\n"
            f"  WHERE subject <> ''\n"
            f"  UNION\n"
            f"  SELECT DISTINCT object AS n FROM {self.source_table}\n"
            f"  WHERE object <> ''\n"
            f"    AND (object LIKE 'http://%' OR object LIKE 'https://%')\n"
            f"    AND predicate NOT IN "
            f"({_in_list(list(DEFAULT_EXCLUDED_PREDICATES))})\n"
            f") s"
        )
```

`UNION` (not `UNION ALL`) does the deduplication across the two sides. The
`LIKE` pair is the same literal-object test `build_edges` uses, so
`(a, ex:age, "41")` contributes no phantom entity.

The regression test must prove the *property*, not just the number: seed a
source where one entity appears **only as an object**, run the job twice with
different `excluded_predicates` (one of them excluding the predicate that links
to that entity), and assert `total_node_count` is identical both times. A test
whose entities are all subjects would pass against the degree-table
implementation too, and so proves nothing.

- [ ] **Step 5: Add the two output-table builders**

Add a new stage section after `write_output`:

```python
    # -- stage 7: per-type rollups --------------------------------------
    def type_profiles(self) -> List[str]:
        """One row per entity type, joining the population to the scored nodes.

        ``instance_count`` comes from the source so isolated instances are
        counted, which is what makes the "flat dataset" heuristic meaningful:
        a type with 5,000 instances and no relationships must be visible.
        ``connected_count`` and the averages come from the output table, so
        they describe exactly the graph that was scored.
        """
        return self._recreate(
            self.type_profiles_table,
            f"SELECT\n"
            f"  pop.type_uri AS type_uri,\n"
            f"  pop.instance_count AS instance_count,\n"
            f"  COALESCE(m.connected_count, 0) AS connected_count,\n"
            f"  COALESCE(m.degree_sum, 0) AS degree_sum,\n"
            f"  COALESCE(m.avg_clustering, 0.0) AS avg_clustering,\n"
            f"  COALESCE(m.avg_betweenness, 0.0) AS avg_betweenness\n"
            f"FROM ({self.type_population_select()}) pop\n"
            f"LEFT JOIN (\n"
            f"  SELECT type_uri,\n"
            f"         COUNT(*) AS connected_count,\n"
            f"         SUM(degree_raw) AS degree_sum,\n"
            f"         AVG(clustering) AS avg_clustering,\n"
            f"         AVG(betweenness) AS avg_betweenness\n"
            f"  FROM {self.output_table}\n"
            f"  WHERE type_uri IS NOT NULL\n"
            f"  GROUP BY type_uri\n"
            f") m ON m.type_uri = pop.type_uri",
        )

    def type_predicates(self) -> List[str]:
        """Distinct ``(type, predicate)`` pairs over entity-entity edges.

        Rows rather than an array column: ``collect_set`` has no SQLite
        equivalent, and the test harness runs this same SQL.

        The predicate filter mirrors ``build_edges`` exactly, so a predicate
        that forms no edge never shows up as a type's relationship.
        """
        return self._recreate(
            self.type_predicates_table,
            f"SELECT DISTINCT ty.type_uri AS type_uri, s.predicate AS predicate\n"
            f"FROM {self.source_table} s\n"
            f"JOIN ({self.node_type_select()}) ty ON ty.n = s.subject\n"
            f"WHERE s.object <> ''\n"
            f"  AND s.subject <> s.object\n"
            f"  AND (s.object LIKE 'http://%' OR s.object LIKE 'https://%')\n"
            f"  AND s.predicate NOT IN ({_in_list(self.excluded_predicates)})",
        )
```

And the population subquery, next to `node_type_select`:

```python
    def type_population_select(self) -> str:
        """Instance count per type over the whole source, isolated included."""
        return (
            f"SELECT object AS type_uri, COUNT(DISTINCT subject) AS instance_count\n"
            f"FROM {self.source_table}\n"
            f"WHERE predicate = '{sql_escape(RDF_TYPE)}' AND object <> ''\n"
            f"GROUP BY object"
        )
```

- [ ] **Step 6: Add `total_node_count` to the summary**

In `write_summary`, insert after the `node_count` column:

```python
            f"{int(stats.get('total_node_count', 0) or 0)} AS total_node_count",
```

- [ ] **Step 7: Wire the new stages into `run_analysis`**

Three edits in `run_analysis`:

1. After `edge_count = …` (line ~658), add:

```python
    total_node_count = int(scalar(builder.total_node_count_query()) or 0)
```

2. In the `node_count == 0` early-return branch, add
   `"total_node_count": total_node_count,` to that branch's `stats` dict, and
   emit an empty per-node output plus the two type tables before writing the
   summary, so a domain with no entity-entity edges still reports its types:

```python
        for stmt in builder.write_empty_output():
            execute(stmt)
        for stmt in builder.type_profiles() + builder.type_predicates():
            execute(stmt)
```

   The empty shell is needed because `type_profiles` joins `self.output_table`,
   and on this branch the PageRank and component tables it normally joins were
   never built. Add the shell builder next to `write_output`:

```python
    def write_empty_output(self) -> List[str]:
        """An empty per-node output, for a source with no entity-entity edges.

        The read-back then never has to distinguish "no edges" from "the job
        did not get that far", and ``type_profiles`` can join unconditionally.
        """
        return self._recreate(
            self.output_table,
            f"SELECT\n"
            f"  d.n AS node_uri,\n"
            f"  d.d AS degree_raw,\n"
            f"  0.0 AS degree,\n"
            f"  0.0 AS pagerank,\n"
            f"  CAST(NULL AS BIGINT) AS component_id,\n"
            f"  0.0 AS clustering,\n"
            f"  0.0 AS betweenness,\n"
            f"  0.0 AS closeness,\n"
            f"  CAST(NULL AS VARCHAR) AS type_uri,\n"
            f"  CAST(NULL AS VARCHAR) AS label\n"
            f"FROM {self.deg} d\n"
            f"WHERE 1 = 0",
        )
```

`VARCHAR`, not `STRING`: `STRING` is Spark-specific and the dialect parse test
rejects it on Postgres. `VARCHAR` parses on all three dialects and on SQLite.

3. On the normal path, after the `write_output` loop and before `write_summary`:

```python
    for stmt in builder.type_profiles() + builder.type_predicates():
        execute(stmt)
```

   and add `"total_node_count": total_node_count,` to the main `stats` dict.

Order matters: `type_profiles` reads `self.output_table`, so it must run after
`write_output`.

- [ ] **Step 8: Register the new SQL with the dialect parse test**

`tests/units/core/test_graph_analytics_job_sql.py` has a `TestJobSqlDialects`
class whose `_all_statements` helper collects every statement the builder can
generate and parses each against the Spark, Databricks and Postgres dialects.
New builders are invisible to it until they are listed there, so add
`write_empty_output()`, `type_profiles()`, `type_predicates()` and
`total_node_count_query()`.

This is the guard that catches accidentally Spark-only SQL, so a new builder
missing from it is a silent hole rather than a missing nicety.

- [ ] **Step 9: Run the tests**

Run: `env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u LAKEBASE_PROJECT -u LAKEBASE_BRANCH uv run pytest tests/units/core/test_graph_analytics_job_sql.py -v`

Expected: PASS, all tests including the pre-existing NetworkX parity ones.

- [ ] **Step 10: Add an empty-graph regression test**

```python
def test_flat_source_still_gets_profiles(tmp_path):
    """A source with no entity-entity edges still reports its types."""
    triples = [
        ("http://ex/a", RDF_TYPE, "http://ex/Reading"),
        ("http://ex/a", "http://ex/value", "41"),
        ("http://ex/b", RDF_TYPE, "http://ex/Reading"),
    ]
    profiles = _table_rows(tmp_path, triples, suffix="_type_profiles")

    assert len(profiles) == 1
    assert profiles[0]["instance_count"] == 2
    assert profiles[0]["connected_count"] == 0
    assert _table_rows(tmp_path, triples, suffix="_type_predicates") == []
```

Run the same pytest command. Expected: PASS.

- [ ] **Step 11: Commit**

```bash
git add src/jobs/graph_analytics_job.py tests/units/core/test_graph_analytics_job_sql.py
git commit -m "feat(analytics): job writes per-type profiles and predicate pairs"
```

---

### Task 3: Job — `class_filter`

The entity-type filter becomes a job parameter. Filtering happens once, where
the bidirectional edge list is built, so PageRank, components, clustering and
the pivot BFS all see the induced subgraph.

**Files:**
- Modify: `src/jobs/graph_analytics_job.py` (`GraphAnalyticsSQL` dataclass field, `build_edges`, `type_population_select`, `node_type_select`, `parse_args`, `main`)
- Modify: `resources/graph_analytics.job.yml`
- Test: `tests/units/core/test_graph_analytics_job_sql.py`

**Interfaces:**
- Consumes: `type_population_select()` / `node_type_select()` from Task 2.
- Produces:
  - `GraphAnalyticsSQL(class_filter: List[str] = [])` — empty means no filter
  - CLI flag `--class-filter` (comma-separated URIs)
  - job parameter `class_filter` (default `""`)

  Task 4 passes the parameter; Task 6 supplies the value.

- [ ] **Step 1: Write the failing parity test**

The oracle is NetworkX on the induced subgraph — the same shape the deleted
in-memory path produced, which is the whole point of keeping parity.

```python
def test_class_filter_matches_networkx_induced_subgraph(tmp_path):
    """A class-filtered run equals NetworkX on graph.subgraph(typed nodes)."""
    triples = [
        ("http://ex/p1", "http://ex/knows", "http://ex/p2"),
        ("http://ex/p2", "http://ex/knows", "http://ex/p3"),
        ("http://ex/p1", "http://ex/knows", "http://ex/p3"),
        # An Order attached to a Person must vanish with the filter, taking
        # its edge with it.
        ("http://ex/p1", "http://ex/ordered", "http://ex/o1"),
        ("http://ex/p1", RDF_TYPE, "http://ex/Person"),
        ("http://ex/p2", RDF_TYPE, "http://ex/Person"),
        ("http://ex/p3", RDF_TYPE, "http://ex/Person"),
        ("http://ex/o1", RDF_TYPE, "http://ex/Order"),
    ]
    rows = _run_job_on_triples(
        tmp_path, triples, class_filter=["http://ex/Person"]
    )

    g = nx.Graph()
    g.add_edges_from([
        ("http://ex/p1", "http://ex/p2"),
        ("http://ex/p2", "http://ex/p3"),
        ("http://ex/p1", "http://ex/p3"),
    ])
    expected_degree = nx.degree_centrality(g)
    expected_clustering = nx.clustering(g)

    assert {r["node_uri"] for r in rows} == set(g.nodes)
    for row in rows:
        uri = row["node_uri"]
        assert row["degree"] == pytest.approx(expected_degree[uri], abs=1e-6)
        assert row["clustering"] == pytest.approx(expected_clustering[uri], abs=1e-6)


def test_class_filter_narrows_type_profiles(tmp_path):
    """Only the selected types get a profile."""
    triples = [
        ("http://ex/p1", "http://ex/knows", "http://ex/p2"),
        ("http://ex/p1", RDF_TYPE, "http://ex/Person"),
        ("http://ex/p2", RDF_TYPE, "http://ex/Person"),
        ("http://ex/o1", RDF_TYPE, "http://ex/Order"),
    ]
    profiles = _table_rows(
        tmp_path, triples, suffix="_type_profiles",
        class_filter=["http://ex/Person"],
    )
    assert [r["type_uri"] for r in profiles] == ["http://ex/Person"]
```

Extend the file's harness helpers to forward `class_filter=None` into
`GraphAnalyticsSQL`. Do not duplicate the harness.

- [ ] **Step 2: Run the tests and watch them fail**

Run: `env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u LAKEBASE_PROJECT -u LAKEBASE_BRANCH uv run pytest tests/units/core/test_graph_analytics_job_sql.py -k class_filter -v`

Expected: FAIL with `TypeError: __init__() got an unexpected keyword argument 'class_filter'`.

- [ ] **Step 3: Add the dataclass field**

In `GraphAnalyticsSQL`, after `excluded_predicates`:

```python
    #: Entity types to restrict the analysis to. Empty means the whole graph.
    class_filter: List[str] = field(default_factory=list)
```

- [ ] **Step 4: Filter the edge list**

Add a helper next to `node_type_select`:

```python
    def typed_nodes_select(self) -> str:
        """Nodes carrying one of ``class_filter``'s types.

        Only called when a filter is set, so it never widens the graph.
        """
        return (
            f"SELECT DISTINCT subject AS n\n"
            f"FROM {self.source_table}\n"
            f"WHERE predicate = '{sql_escape(RDF_TYPE)}'\n"
            f"  AND object IN ({_in_list(self.class_filter)})"
        )
```

Then in `build_edges`, filter both endpoints. Replace the first `_recreate`
call's SELECT with a version that keeps the existing predicate and
literal-object conditions and adds the endpoint restriction:

```python
        keep = ""
        if self.class_filter:
            # Both endpoints must survive, which is exactly an induced
            # subgraph — the same graph the entity-type filter produced when
            # the analysis ran in memory.
            keep = (
                f"  AND subject IN ({self.typed_nodes_select()})\n"
                f"  AND object IN ({self.typed_nodes_select()})\n"
            )

        statements = self._recreate(
            self.edges,
            f"SELECT DISTINCT\n"
            f"  least(subject, object) AS src,\n"
            f"  greatest(subject, object) AS dst\n"
            f"FROM {self.source_table}\n"
            f"WHERE subject <> ''\n"
            f"  AND object <> ''\n"
            f"  AND subject <> object\n"
            f"  AND (object LIKE 'http://%' OR object LIKE 'https://%')\n"
            f"  AND predicate NOT IN ({_in_list(self.excluded_predicates)})\n"
            f"{keep}",
        )
```

- [ ] **Step 5: Apply the filter to the per-type rollups**

The profiles must describe the scored subgraph, so restrict the population and
the predicate pairs too. In `type_population_select` and `typed_nodes_select`'s
sibling `node_type_select`, append a type restriction when a filter is set. Add
one shared helper:

```python
    def _class_clause(self, column: str) -> str:
        """``AND <column> IN (…)`` when a class filter is set, else empty."""
        if not self.class_filter:
            return ""
        return f"\n  AND {column} IN ({_in_list(self.class_filter)})"
```

Then:
- in `node_type_select`, change the `WHERE` line to
  `f"WHERE predicate = '{sql_escape(RDF_TYPE)}' AND object <> ''{self._class_clause('object')}\n"`
- in `type_population_select`, make the same change.

`node_label_select` is untouched: a label is not a type.

- [ ] **Step 6: Add the CLI flag**

In `parse_args`, next to the existing `--exclude-predicates`:

```python
    parser.add_argument(
        "--class-filter",
        default="",
        help=(
            "Comma-separated entity type URIs. When set, only nodes carrying "
            "one of these rdf:type values are scored, and only edges whose "
            "both endpoints survive."
        ),
    )
```

In `main`, where `GraphAnalyticsSQL` is constructed, parse it the same way
`--exclude-predicates` is parsed (find that line and mirror it exactly):

```python
    class_filter = [
        v.strip() for v in (args.class_filter or "").split(",") if v.strip()
    ]
```

and pass `class_filter=class_filter` to the constructor. Log it at INFO next
to the existing source/output logging so a run's scope is visible in the job
output:

```python
    if class_filter:
        logger.info("class filter: %d type(s) — %s", len(class_filter), ", ".join(class_filter))
```

- [ ] **Step 7: Declare the job parameter**

In `resources/graph_analytics.job.yml`, add to the `parameters:` block, mirroring
how `max_depth` is declared:

```yaml
      - name: class_filter
        default: ""
```

and add to the task's `parameters:` list, after the `--max-depth` pair:

```yaml
            - "--class-filter"
            - "{{job.parameters.class_filter}}"
```

- [ ] **Step 8: Run the full job suite**

Run: `env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u LAKEBASE_PROJECT -u LAKEBASE_BRANCH uv run pytest tests/units/core/test_graph_analytics_job_sql.py tests/units/core/test_graph_analytics_job_exit.py -v`

Expected: PASS. The unfiltered tests prove the filter is inert when empty.

- [ ] **Step 9: Commit**

```bash
git add src/jobs/graph_analytics_job.py resources/graph_analytics.job.yml tests/units/core/test_graph_analytics_job_sql.py
git commit -m "feat(analytics): job accepts a class filter and scores the induced subgraph"
```

---

### Task 4: `LakeflowRunner` — carry `class_filter`

Pure plumbing, in the same shape as the `max_depth` work already committed.

**Files:**
- Modify: `src/back/core/graph_analysis/LakeflowRunner.py` (`submit`, `run_and_wait`)
- Test: `tests/units/core/test_lakeflow_runner.py`

**Interfaces:**
- Consumes: the `class_filter` job parameter from Task 3.
- Produces: `submit(..., class_filter: Optional[List[str]] = None)` and the same on `run_and_wait`. A non-empty list is sent as a comma-joined string; `None` or `[]` sends `""`.

- [ ] **Step 1: Write the failing test**

Model it on the existing `test_max_depth_reaches_the_job` in the same file.

```python
def test_class_filter_reaches_the_job():
    """The filter is sent comma-joined, as the job's CLI expects."""
    runner, fake = _runner_with_fake_jobs()
    runner.submit(
        source_table="cat.sch.data",
        output_table="cat.sch.out",
        class_filter=["http://ex/Person", "http://ex/Order"],
    )
    params = fake.last_run_now_params["job_parameters"]
    assert params["class_filter"] == "http://ex/Person,http://ex/Order"


def test_absent_class_filter_sends_an_empty_string():
    """An unfiltered run must not send 'None' as a type URI."""
    runner, fake = _runner_with_fake_jobs()
    runner.submit(source_table="cat.sch.data", output_table="cat.sch.out")
    assert fake.last_run_now_params["job_parameters"]["class_filter"] == ""
```

Reuse the fake/helper the existing tests use rather than adding a second one.

- [ ] **Step 2: Run and watch it fail**

Run: `env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u LAKEBASE_PROJECT -u LAKEBASE_BRANCH uv run pytest tests/units/core/test_lakeflow_runner.py -k class_filter -v`

Expected: FAIL with `TypeError: submit() got an unexpected keyword argument 'class_filter'`.

- [ ] **Step 3: Implement**

Add `class_filter: Optional[List[str]] = None` to both `submit` and
`run_and_wait` signatures, forward it from `run_and_wait` to `submit`, and in
`submit`'s `job_parameters` dict add:

```python
            "class_filter": ",".join(class_filter or []),
```

- [ ] **Step 4: Run the tests**

Run: `env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u LAKEBASE_PROJECT -u LAKEBASE_BRANCH uv run pytest tests/units/core/test_lakeflow_runner.py -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/back/core/graph_analysis/LakeflowRunner.py tests/units/core/test_lakeflow_runner.py
git commit -m "feat(analytics): thread the class filter through the Lakeflow runner"
```

---

### Task 5: `resolve_analytics_source` replaces `resolve_spark_source`

Analytics stops asking the engine where its data lives. The source is always
the R2RML-derived `…_data` table.

**Files:**
- Modify: `src/back/core/graph_analysis/JobMetrics.py` (delete `resolve_spark_source` and `_lakebase_uc_source`, lines 62-136; add `resolve_analytics_source`)
- Modify: `src/back/core/graph_analysis/__init__.py` (export swap)
- Test: `tests/units/core/test_job_metrics.py`

**Interfaces:**
- Consumes: `SQLHelpers.effective_databricks_table(domain, settings)` → the `…_data` FQN (already exists; delegates to `back.core.graphdb.delta._table_naming.data_table_fqn`).
- Produces: `resolve_analytics_source(domain, settings) -> Tuple[str, str]` returning `(table, "")` or `("", reason)`. Tasks 6 and 8 both call it.

- [ ] **Step 1: Write the failing test**

```python
def test_analytics_source_is_the_data_table_whatever_the_engine(monkeypatch):
    """No engine branch: every backend resolves the same mapped snapshot."""
    from back.core.graph_analysis import JobMetrics as jm

    monkeypatch.setattr(
        jm.SQLHelpers, "effective_databricks_table",
        staticmethod(lambda domain, settings=None: "cat.sch.triplestore_dom_V3_data"),
    )
    table, reason = jm.resolve_analytics_source(object(), object())
    assert table == "cat.sch.triplestore_dom_V3_data"
    assert reason == ""


def test_unqualified_source_is_refused_with_a_remedy(monkeypatch):
    """A half-configured domain must say what to fix, not fail obscurely."""
    from back.core.graph_analysis import JobMetrics as jm

    monkeypatch.setattr(
        jm.SQLHelpers, "effective_databricks_table",
        staticmethod(lambda domain, settings=None: "triplestore_dom_V3_data"),
    )
    table, reason = jm.resolve_analytics_source(object(), object())
    assert table == ""
    assert "catalog.schema.table" in reason
```

- [ ] **Step 2: Run and watch it fail**

Run: `env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u LAKEBASE_PROJECT -u LAKEBASE_BRANCH uv run pytest tests/units/core/test_job_metrics.py -k analytics_source -v`

Expected: FAIL with `AttributeError: module … has no attribute 'resolve_analytics_source'`.

- [ ] **Step 3: Implement, and delete the old resolver**

In `JobMetrics.py`, replace the whole `resolve_spark_source` /
`_lakebase_uc_source` block (lines 62-136) with:

```python
def resolve_analytics_source(domain: Any, settings: Any) -> Tuple[str, str]:
    """Resolve the Unity Catalog table analytics reads.

    Always the ``…_data`` snapshot the Build materialises from the R2RML VIEW,
    never the engine's own graph relation. That is what makes a KPI identical
    on Lakehouse, Lakebase and Neo4j: the engines differ, the mapped snapshot
    does not.

    Returns ``(table, "")`` or ``("", reason)``, where the reason is written for
    whoever reads it in the UI.
    """
    table = (SQLHelpers.effective_databricks_table(domain, settings) or "").strip()
    table = table.replace("`", "")
    if not table:
        return "", (
            "No mapped-triples table could be resolved for this domain. Run "
            "Knowledge Graph → Build to materialise it."
        )
    if table.count(".") != 2:
        return "", (
            f"The mapped-triples table resolves to {table!r}, which is not a "
            f"catalog.schema.table name the Databricks job can read."
        )
    return table, ""
```

Add the import at the top of the module:

```python
from back.core.helpers.SQLHelpers import SQLHelpers
```

Check for a circular import: `SQLHelpers` imports `_table_naming` lazily inside
the method, so a module-level import here is safe. If the suite reports a cycle,
move the import inside the function and keep the `monkeypatch` target working by
patching `back.core.helpers.SQLHelpers.SQLHelpers.effective_databricks_table`
in the tests instead.

In `src/back/core/graph_analysis/__init__.py`, replace the
`resolve_spark_source` import and `__all__` entry with `resolve_analytics_source`.

- [ ] **Step 4: Find and fix every remaining reference**

Run: `rg -n "resolve_spark_source|_lakebase_uc_source" src tests`

Expected after the fix: only `src/api/routers/internal/dtwin.py:508` remains,
which Task 8 rewrites. Leave it importing the old name for now **only if** the
suite still passes; if the import breaks, point it at
`resolve_analytics_source(domain, settings)` immediately and adjust the call at
line 508 to drop `store, graph_name`.

- [ ] **Step 5: Run the tests**

Run: `env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u LAKEBASE_PROJECT -u LAKEBASE_BRANCH uv run pytest tests/units -q`

Expected: PASS. Tests asserting the old Neo4j/`app_managed` refusal messages
should be deleted — that refusal no longer exists by design.

- [ ] **Step 6: Commit**

```bash
git add src/back/core/graph_analysis/JobMetrics.py src/back/core/graph_analysis/__init__.py tests/units/core/test_job_metrics.py
git commit -m "refactor(analytics): resolve the source as the R2RML data snapshot, not the engine graph"
```

---

### Task 6: `JobMetrics` becomes a read-back assembler

The biggest task. `JobMetrics` stops calling `PushdownMetrics`, stops taking a
`store`, and builds `MetricsResult` purely from the job's four output tables.

**Files:**
- Modify: `src/back/core/graph_analysis/JobMetrics.py` (module docstring, read-back queries, the whole `JobMetrics` class)
- Modify: `src/back/core/graph_analysis/profiles.py` (docstring only)
- Modify: `src/back/objects/digitaltwin/DigitalTwin.py` (`build_job_metrics`, lines 3482-3536)
- Test: `tests/units/core/test_job_metrics.py`

**Interfaces:**
- Consumes: `resolve_analytics_source` (Task 5); the job's `<out>`, `<out>_summary`, `<out>_type_profiles`, `<out>_type_predicates` (Tasks 1-2); `LakeflowRunner.run_and_wait(..., class_filter=…)` (Task 4).
- Produces:
  - `JobMetrics(source_table: str, *, runner, query, output_table, top_n=100, pagerank_iterations=20, pivots=64, max_depth=32)` — note: **no** `store`, **no** `graph_name`; the first positional is now the source table.
  - `summary_query(output_table)`, `top_nodes_query(output_table, top_n)`, `type_profiles_query(output_table)`, `type_predicates_query(output_table)`
  - `DigitalTwin.build_job_metrics(domain, settings, *, source_table, graph_name, top_n=100)`

  Task 8 calls `build_job_metrics` with that signature.

- [ ] **Step 1: Write the failing tests**

The existing `_job_metrics` helper in `tests/units/core/test_job_metrics.py`
builds a `JobMetrics` over a fake store and a fake query function. Rework it to
the new constructor and drive the assembly from canned rows.

```python
def _query_for(rows_by_table):
    """A fake warehouse query that dispatches on the table being read."""
    def query(sql):
        for suffix, rows in rows_by_table.items():
            if suffix and f"_{suffix}" in sql:
                return rows
        return rows_by_table.get("", [])
    return query


def test_stats_come_from_the_job_summary():
    """node_count and graph_node_count are distinct numbers, both from SQL."""
    query = _query_for({
        "summary": [{
            "node_count": 4, "total_node_count": 7, "edge_count": 3,
            "component_count": 2, "components_converged": True,
            "pivot_count": 64, "bfs_complete": True,
        }],
        "type_profiles": [],
        "type_predicates": [],
        "": [],
    })
    metrics = _job_metrics(query=query)
    result = metrics.compute(MetricsRequest())

    assert result.mode == MODE_JOB
    assert result.stats.node_count == 7
    assert result.stats.graph_node_count == 4
    assert result.stats.edge_count == 3
    assert result.stats.connected_components == 2
    # 2 * 3 / 4
    assert result.stats.avg_degree == pytest.approx(1.5)
    # 2 * 3 / (4 * 3)
    assert result.stats.density == pytest.approx(0.5)


def test_type_profiles_are_assembled_and_labelled():
    """The job supplies the numbers; profiles.py supplies the 'flat' verdict."""
    query = _query_for({
        "summary": [{"node_count": 2, "total_node_count": 30, "edge_count": 1,
                     "component_count": 1, "components_converged": True,
                     "pivot_count": 0, "bfs_complete": True}],
        "type_profiles": [{
            "type_uri": "http://ex/Reading", "instance_count": 25,
            "connected_count": 25, "degree_sum": 25,
            "avg_clustering": 0.0, "avg_betweenness": 0.0,
        }],
        "type_predicates": [
            {"type_uri": "http://ex/Reading", "predicate": "http://ex/sensor"},
        ],
        "": [],
    })
    result = _job_metrics(query=query).compute(MetricsRequest())

    profile = result.entity_type_profiles["http://ex/Reading"]
    assert profile.count == 25
    assert profile.distinct_predicates == 1
    # One predicate across 25 instances is the flat-dataset signal.
    assert profile.is_flat is True
    assert "only 1 distinct relationship predicate" in profile.flat_reasons[0]


def test_nodes_carry_type_and_label_from_the_job():
    query = _query_for({
        "summary": [{"node_count": 1, "total_node_count": 1, "edge_count": 0,
                     "component_count": 1, "components_converged": True,
                     "pivot_count": 8, "bfs_complete": True}],
        "type_profiles": [],
        "type_predicates": [],
        "": [{
            "node_uri": "http://ex/a", "degree": 0.5, "pagerank": 0.25,
            "clustering": 0.0, "betweenness": 0.1, "closeness": 0.4,
            "component_id": 1, "type_uri": "http://ex/Person", "label": "Alice",
        }],
    })
    result = _job_metrics(query=query).compute(MetricsRequest())

    assert result.node_types == {"http://ex/a": "http://ex/Person"}
    assert result.node_labels == {"http://ex/a": "Alice"}
    assert result.nodes["http://ex/a"].degree == 0.5
    assert result.nodes["http://ex/a"].betweenness == 0.1


def test_class_and_predicate_filters_reach_the_runner():
    """Both filters are the job's business now, not the app's."""
    metrics, runner = _job_metrics_with_runner()
    metrics.compute(MetricsRequest(
        class_filter=["http://ex/Person"],
        predicate_filter=["http://ex/noisy"],
    ))
    assert runner.last_kwargs["class_filter"] == ["http://ex/Person"]
    assert runner.last_kwargs["exclude_predicates"] == ["http://ex/noisy"]
```

Keep the existing `test_max_depth_reaches_the_runner` and the
truncated-BFS/no-pivot tests — they still describe required behaviour. Update
them to the new constructor.

- [ ] **Step 2: Run and watch them fail**

Run: `env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u LAKEBASE_PROJECT -u LAKEBASE_BRANCH uv run pytest tests/units/core/test_job_metrics.py -v`

Expected: FAIL — the constructor still requires `store` and `graph_name`.

- [ ] **Step 3: Rewrite the module docstring**

Replace lines 1-26 of `JobMetrics.py` with:

```python
"""Graph metrics, computed entirely by the Databricks graph analytics job.

This is the *only* analytics compute path. The job reads the ``…_data``
snapshot the Build materialises from the R2RML VIEW, writes four tables, and
this module assembles them into a :class:`MetricsResult`:

* ``<out>``                  one row per scored node, with ``type_uri`` and ``label``
* ``<out>_summary``          node / edge / component counts, pivot and BFS flags
* ``<out>_type_profiles``    per-entity-type counts, degree sum, mean clustering
* ``<out>_type_predicates``  the distinct relationship predicates per type

Nothing here computes a metric. The one piece of logic the app still applies is
the flat-dataset heuristic in :mod:`back.core.graph_analysis.profiles`, which
turns ``(instance_count, distinct_predicates)`` into a human-readable verdict —
string matching on predicate names, not graph computation.

Betweenness and closeness are Brandes-Pich *estimates* sampled from pivots, so
they are flagged as approximate. They drop to unavailable when the pivot BFS was
truncated by its depth cap, since the distance sums would be biased — raise
``analytics_job_max_depth`` and re-run when that happens.

The read-back keeps the bounded-payload contract: only the union of the top-N
per metric is returned, never a row per node.
"""
```

- [ ] **Step 4: Rewrite the read-back queries**

Replace `summary_query`, `top_nodes_query`, `metrics_for_nodes_query` and
`type_clustering_query` with:

```python
def summary_query(output_table: str) -> str:
    """One-row run summary written by the job."""
    return (
        "SELECT node_count, total_node_count, edge_count, component_count, "
        "components_converged, pivot_count, bfs_complete "
        f"FROM {output_table}_summary"
    )


def top_nodes_query(output_table: str, top_n: int) -> str:
    """Union of the top *top_n* by each ranked metric.

    Degree is ranked here too: with the pushdown path gone, this query is the
    only source of the node payload. Ties break on ``node_uri`` so the result
    is deterministic across runs.
    """
    k = max(1, int(top_n))
    return (
        "WITH ranked AS (\n"
        "  SELECT node_uri, degree, pagerank, clustering, betweenness, closeness,\n"
        "         component_id, type_uri, label,\n"
        "         ROW_NUMBER() OVER (ORDER BY degree DESC, node_uri) AS rn_dg,\n"
        "         ROW_NUMBER() OVER (ORDER BY pagerank DESC, node_uri) AS rn_pr,\n"
        "         ROW_NUMBER() OVER (ORDER BY clustering DESC, node_uri) AS rn_cl,\n"
        "         ROW_NUMBER() OVER (ORDER BY betweenness DESC, node_uri) AS rn_bc,\n"
        "         ROW_NUMBER() OVER (ORDER BY closeness DESC, node_uri) AS rn_cn\n"
        f"  FROM {output_table}\n"
        ")\n"
        "SELECT node_uri, degree, pagerank, clustering, betweenness, closeness,\n"
        "       component_id, type_uri, label\n"
        "FROM ranked\n"
        f"WHERE rn_dg <= {k} OR rn_pr <= {k} OR rn_cl <= {k}\n"
        f"   OR rn_bc <= {k} OR rn_cn <= {k}\n"
        "ORDER BY pagerank DESC, node_uri"
    )


def type_profiles_query(output_table: str) -> str:
    """Per-entity-type rollups, as written by the job."""
    return (
        "SELECT type_uri, instance_count, connected_count, degree_sum,\n"
        "       avg_clustering, avg_betweenness\n"
        f"FROM {output_table}_type_profiles"
    )


def type_predicates_query(output_table: str) -> str:
    """Distinct ``(type, predicate)`` pairs, as written by the job."""
    return f"SELECT type_uri, predicate FROM {output_table}_type_predicates"
```

`RDF_TYPE` is no longer used in this module — remove the constant.

- [ ] **Step 5: Rewrite the class**

Replace the whole `JobMetrics` class with:

```python
class JobMetrics:
    """Run the Lakeflow job, then assemble its output into a MetricsResult."""

    def __init__(
        self,
        source_table: str,
        *,
        runner: Any,
        query: Callable[[str], List[Dict[str, Any]]],
        output_table: str,
        top_n: int = 100,
        pagerank_iterations: int = 20,
        pivots: int = 64,
        max_depth: int = 32,
    ) -> None:
        self._source_table = source_table
        self._runner = runner
        self._query = query
        self._output_table = output_table
        self._top_n = max(1, int(top_n))
        self._pagerank_iterations = max(1, int(pagerank_iterations))
        self._pivots = max(0, int(pivots))
        self._max_depth = max(1, int(max_depth))

    def compute(
        self,
        request: MetricsRequest,
        on_progress: Optional[Callable[[int, str], None]] = None,
    ) -> MetricsResult:
        """Trigger the job and read every metric back."""
        t0 = time.time()

        outcome = self._runner.run_and_wait(
            source_table=self._source_table,
            output_table=self._output_table,
            exclude_predicates=list(request.predicate_filter or []) or None,
            class_filter=list(request.class_filter or []) or None,
            pagerank_iterations=self._pagerank_iterations,
            pivots=self._pivots,
            max_depth=self._max_depth,
            on_progress=on_progress,
        )
        if not outcome.get("success"):
            raise InfrastructureError(
                "The graph analytics job did not complete successfully",
                detail=(
                    f"{outcome.get('life_cycle_state', '')} "
                    f"{outcome.get('result_state', '')} "
                    f"{outcome.get('message', '')}".strip()
                    + (
                        f" — {outcome['run_page_url']}"
                        if outcome.get("run_page_url")
                        else ""
                    )
                ),
            )

        if on_progress:
            on_progress(85, "Reading results back")

        result = MetricsResult(mode=MODE_JOB)
        self._read_summary(result)
        self._read_nodes(result)
        self._read_type_profiles(result)
        result.stats.elapsed_ms = int((time.time() - t0) * 1000)

        logger.info(
            "JobMetrics: %s nodes / %s edges, %s components, "
            "%d profiles, %d nodes returned in %dms",
            f"{result.stats.graph_node_count:,}",
            f"{result.stats.edge_count:,}",
            result.stats.connected_components,
            len(result.entity_type_profiles),
            len(result.nodes),
            result.stats.elapsed_ms,
        )
        return result

    # ------------------------------------------------------------------

    def _read_summary(self, result: MetricsResult) -> None:
        """Fill the structure counts and decide on the sampled metrics."""
        rows = self._query(summary_query(self._output_table)) or []
        if not rows:
            # No summary means no run to trust: withhold the sampled metrics
            # rather than charting zeros as real centrality values.
            result.approximate_metrics = []
            result.unavailable_metrics = list(UNAVAILABLE_METRICS)
            result.pivot_count = 0
            return

        row = rows[0]
        graph_node_count = int(row.get("node_count", 0) or 0)
        edge_count = int(row.get("edge_count", 0) or 0)
        divisor = float(graph_node_count - 1) if graph_node_count > 1 else 1.0

        result.stats.node_count = int(row.get("total_node_count", 0) or 0)
        result.stats.graph_node_count = graph_node_count
        result.stats.edge_count = edge_count
        result.stats.connected_components = int(row.get("component_count", 0) or 0)
        if graph_node_count:
            result.stats.avg_degree = round(2.0 * edge_count / graph_node_count, 4)
            result.stats.density = round(
                2.0 * edge_count / (graph_node_count * divisor), 6
            )

        if not row.get("components_converged", True):
            # Surfaced rather than silently trusted: an unconverged label
            # propagation over-counts components.
            logger.warning(
                "Component labelling did not converge for %s — the component "
                "count is a lower bound",
                self._output_table,
            )

        pivot_count = int(row.get("pivot_count", 0) or 0)
        bfs_complete = bool(row.get("bfs_complete", True))
        if pivot_count > 0 and bfs_complete:
            result.approximate_metrics = list(APPROXIMATE_METRICS)
            result.unavailable_metrics = []
            result.pivot_count = pivot_count
        else:
            result.approximate_metrics = []
            result.unavailable_metrics = list(UNAVAILABLE_METRICS)
            result.pivot_count = 0
            if pivot_count > 0 and not bfs_complete:
                logger.warning(
                    "The pivot BFS for %s hit its depth cap; betweenness and "
                    "closeness are reported as unavailable rather than truncated",
                    self._output_table,
                )

    def _read_nodes(self, result: MetricsResult) -> None:
        """Fill the bounded node payload from the top-N union."""
        rows = self._query(top_nodes_query(self._output_table, self._top_n)) or []
        for row in rows:
            uri = row.get("node_uri") or ""
            if not uri:
                continue
            node = NodeMetrics(
                degree=round(float(row.get("degree", 0.0) or 0.0), 6),
                pagerank=round(float(row.get("pagerank", 0.0) or 0.0), 8),
                clustering=round(float(row.get("clustering", 0.0) or 0.0), 6),
            )
            if result.approximate_metrics:
                node.betweenness = round(float(row.get("betweenness", 0.0) or 0.0), 8)
                node.closeness = round(float(row.get("closeness", 0.0) or 0.0), 6)
            result.nodes[uri] = node
            if row.get("type_uri"):
                result.node_types[uri] = row["type_uri"]
            if row.get("label"):
                result.node_labels[uri] = row["label"]

        result.top_pagerank = [
            uri
            for uri, _ in sorted(
                result.nodes.items(), key=lambda kv: (-kv[1].pagerank, kv[0])
            )
        ][: self._top_n]

    def _read_type_profiles(self, result: MetricsResult) -> None:
        """Assemble the per-type profiles and label the flat ones."""
        profiles = self._query(type_profiles_query(self._output_table)) or []
        pairs = self._query(type_predicates_query(self._output_table)) or []

        predicates_by_type: Dict[str, Set[str]] = {}
        for row in pairs:
            type_uri = row.get("type_uri") or ""
            predicate = row.get("predicate") or ""
            if type_uri and predicate:
                predicates_by_type.setdefault(type_uri, set()).add(predicate)

        graph_node_count = result.stats.graph_node_count or 0
        divisor = float(graph_node_count - 1) if graph_node_count > 1 else 1.0

        for row in profiles:
            type_uri = row.get("type_uri") or ""
            if not type_uri:
                continue
            count = int(row.get("instance_count", 0) or 0)
            connected = int(row.get("connected_count", 0) or 0)
            degree_sum = int(row.get("degree_sum", 0) or 0)
            preds = predicates_by_type.get(type_uri, set())
            reasons = flat_reasons(count, len(preds))

            result.entity_type_profiles[type_uri] = EntityTypeProfile(
                uri=type_uri,
                count=count,
                # Normalised the same way ``nx.degree_centrality`` does, so a
                # type's average degree is comparable with a node's.
                avg_degree=(
                    round((degree_sum / connected) / divisor, 6) if connected else 0.0
                ),
                avg_clustering=round(float(row.get("avg_clustering", 0.0) or 0.0), 6),
                avg_betweenness=round(float(row.get("avg_betweenness", 0.0) or 0.0), 8),
                distinct_predicates=len(preds),
                has_temporal_predicates=has_temporal_predicates(preds),
                is_flat=bool(reasons),
                flat_reasons=reasons,
            )
```

Fix the imports at the top of the module: drop `PushdownMetrics`, add
`EntityTypeProfile` to the `models` import (`MetricsStats` is not needed —
`MetricsResult.stats` has a `default_factory`, so `MetricsResult(mode=MODE_JOB)`
already carries a zeroed `MetricsStats`), add
`from back.core.graph_analysis.profiles import flat_reasons, has_temporal_predicates`,
and add `Set` to the `typing` import.

- [ ] **Step 6: Update `build_job_metrics`**

In `src/back/objects/digitaltwin/DigitalTwin.py`, change the signature and the
construction:

```python
    @staticmethod
    def build_job_metrics(
        domain: Any,
        settings: Any,
        *,
        source_table: str,
        graph_name: str,
        top_n: int = 100,
    ) -> Any:
        """Wire a :class:`JobMetrics` from domain credentials and settings.

        *graph_name* is used only to name the output table, not to read data:
        the job reads *source_table*, which is always the mapped snapshot.
        """
```

Keep the body as-is down to the `return`, then:

```python
        return JobMetrics(
            source_table,
            runner=runner,
            query=client.execute_query,
            output_table=DigitalTwin.analytics_output_table(
                output_schema, domain, graph_name
            ),
            top_n=top_n,
            pagerank_iterations=int(
                getattr(settings, "analytics_job_pagerank_iterations", 20) or 20
            ),
            pivots=int(getattr(settings, "analytics_job_pivots", 64) or 0),
            max_depth=int(getattr(settings, "analytics_job_max_depth", 32) or 32),
        )
```

Its caller at `DigitalTwin.compute_graph_metrics` line ~3447 must be updated in
the same commit or the suite breaks; Task 8 rewrites that method properly, so
for now make the minimal edit:

```python
            source_table, reason = resolve_analytics_source(domain, settings)
            if not source_table:
                raise InfrastructureError(
                    "The graph analytics job cannot read this domain", detail=reason
                )
            job_metrics = DigitalTwin.build_job_metrics(
                domain, settings, source_table=source_table,
                graph_name=graph_name, top_n=top_n,
            )
```

- [ ] **Step 7: Fix `profiles.py`'s docstring**

It currently claims two callers that are about to disappear. Replace lines 1-7:

```python
"""Shared entity-type profiling helpers.

The temporal-predicate detection and the flat-dataset heuristic label the
per-type rollups the Databricks analytics job computes. They are pure string
heuristics over predicate names and instance counts, which is why they live in
the app rather than in the job's SQL — keeping one copy means the wording of
"flat" cannot drift.
"""
```

- [ ] **Step 8: Run the tests**

Run: `env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u LAKEBASE_PROJECT -u LAKEBASE_BRANCH uv run pytest tests/units/core/test_job_metrics.py -v`

Expected: PASS.

Then the whole suite: `env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u LAKEBASE_PROJECT -u LAKEBASE_BRANCH uv run pytest -q -m "not scenario"`

Expected: failures only in tests that assert the old pushdown/in-memory
behaviour. Note them for Tasks 8-10; do not paper over them here.

- [ ] **Step 9: Commit**

```bash
git add src/back/core/graph_analysis/JobMetrics.py src/back/core/graph_analysis/profiles.py src/back/objects/digitaltwin/DigitalTwin.py tests/units/core/test_job_metrics.py
git commit -m "refactor(analytics): JobMetrics assembles every metric from the job output"
```

---

### Task 7: Build always materialises `…_data`

`…_data` is the contract between Build and Analytics. Every engine must honour
it, and a failure to materialise fails the build.

**Files:**
- Modify: `src/back/objects/digitaltwin/_build_pipeline.py` (`run`, around line 386)
- ~~Modify: `src/back/core/graphdb/delta/DeltaTripleStoreBuildPipeline.py` (remove the now-duplicate materialisation at lines 255-258)~~ — **the plan was wrong here.** That materialisation is not a duplicate. `DeltaTripleStoreBuildPipeline` is a second, independent entry point, reached only from `POST /dtwin/databricks-build/start` via `_databricks_triplestore_build.run_databricks_triplestore_build`; it never runs inside `_BuildPipeline`, so the two never both fire for one build. Removing it silently left that endpoint producing a domain with no `…_data` and therefore no analytics. The step stays in both pipelines. See Step 4.
- Test: `tests/units/objects/test_build_pipeline_materialize.py` (create)

**Interfaces:**
- Consumes: `materialize.materialize_from_view(client, view_fqn, table_fqn)` and `_table_naming.data_table_fqn(domain, settings)`, both already present.
- Produces: `_BuildPipeline._materialize_data_table() -> bool` — `True` on success, `False` after recording the build as failed.

- [ ] **Step 1: Write the failing test**

```python
def test_every_engine_materialises_the_mapped_snapshot(lakebase_pipeline):
    """Analytics depends on …_data, so Lakebase builds must produce it too."""
    calls = []
    with patch(
        "back.core.graphdb.delta.materialize.materialize_from_view",
        side_effect=lambda client, view, table: calls.append((view, table)),
    ):
        lakebase_pipeline.run()

    assert calls == [(
        "cat.sch.triplestore_dom_V3",
        "cat.sch.triplestore_dom_V3_data",
    )]


def test_a_failed_materialisation_fails_the_build(lakebase_pipeline):
    """A domain that cannot be analysed must not report a green build."""
    with patch(
        "back.core.graphdb.delta.materialize.materialize_from_view",
        side_effect=RuntimeError("no permission on cat.sch"),
    ):
        lakebase_pipeline.run()

    assert lakebase_pipeline.tm.failed is True
    assert "no permission" in lakebase_pipeline.tm.failure_message


def test_the_delta_pipeline_materialises_once(delta_pipeline):
    """The Lakehouse pipeline used to do this itself — not twice."""
    calls = []
    with patch(
        "back.core.graphdb.delta.materialize.materialize_from_view",
        side_effect=lambda client, view, table: calls.append(table),
    ):
        delta_pipeline.run()

    assert len(calls) == 1
```

Look at the existing build-pipeline tests under `tests/units/objects/` first and
reuse their fixtures for a fake `tm` / fake `source_client`. If no such fixture
exists, build the two pipeline fixtures in this new file with a stub task
manager exposing `advance_step`, `update_progress`, `fail_task`, and record
`failed` / `failure_message`.

- [ ] **Step 2: Run and watch it fail**

Run: `env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u LAKEBASE_PROJECT -u LAKEBASE_BRANCH uv run pytest tests/units/objects/test_build_pipeline_materialize.py -v`

Expected: FAIL — the Lakebase build never calls `materialize_from_view`, so
`calls == []`.

- [ ] **Step 3: Add the phase to the shared pipeline**

In `src/back/objects/digitaltwin/_build_pipeline.py`, in `run()`, between
`self._post_create_view_progress()` and `if not self._apply_full_rebuild():`:

```python
            if not self._materialize_data_table():
                return
```

Then add the method next to `_post_create_view_progress`:

```python
    def _materialize_data_table(self) -> bool:
        """Snapshot the R2RML VIEW into ``…_data`` for every engine.

        Analytics reads this table and nothing else, which is what makes the
        KPIs identical across Lakehouse, Lakebase and Neo4j. It is therefore
        not optional: a build that skipped it would leave a domain that looks
        fine and cannot be analysed.
        """
        from back.core.graphdb.delta import _table_naming, materialize

        data_table = _table_naming.data_table_fqn(self.domain, self.settings)
        if not data_table:
            self._record_build_run(
                "error", message="Could not resolve the mapped-triples table name"
            )
            self.tm.fail_task(
                self.task_id, "Could not resolve the mapped-triples table name"
            )
            return False

        self.tm.update_progress(
            self.task_id, 30, f"Materializing mapped triples into {data_table}..."
        )
        try:
            materialize.materialize_from_view(
                self.source_client, self.view_table, data_table
            )
        except Exception as exc:  # noqa: BLE001
            msg = f"Could not materialize {data_table}: {exc}"
            logger.error("[DT-BUILD %s] %s", self.task_id, msg)
            self._record_build_run("error", message=msg)
            self.tm.fail_task(self.task_id, msg)
            return False

        self.data_table = data_table
        logger.info("[DT-BUILD %s] materialized %s", self.task_id, data_table)
        return True
```

Match the exact `_record_build_run` / `fail_task` call shape the neighbouring
failure paths in that file use — read `_create_view`'s failure branch (around
line 560-600) and copy its conventions rather than the sketch above if they
differ.

- [ ] **Step 4: Remove the duplicate from the Delta pipeline**

**Superseded — do not do this.** The step was deleted from
`DeltaTripleStoreBuildPipeline` as originally written and then restored, because
it was never a duplicate: that class is only ever constructed by
`_databricks_triplestore_build.run_databricks_triplestore_build`, which serves
`POST /dtwin/databricks-build/start` and does not go through `_BuildPipeline`.
Both pipelines materialise, each for the builds it owns. `_BuildPipeline` calling
it once per build is asserted by `test_the_delta_pipeline_materialises_once`; the
endpoint's own pipeline is covered by
`test_the_databricks_build_endpoint_also_materialises`.

- [ ] **Step 5: Run the tests**

Run: `env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u LAKEBASE_PROJECT -u LAKEBASE_BRANCH uv run pytest tests/units/objects -v`

Expected: PASS. Existing Delta build tests that asserted the materialisation
happened inside `_apply_full_rebuild` need their assertion moved, not deleted —
the behaviour still happens, one phase earlier.

- [ ] **Step 6: Commit**

```bash
git add src/back/objects/digitaltwin/_build_pipeline.py src/back/core/graphdb/delta/DeltaTripleStoreBuildPipeline.py tests/units/objects/test_build_pipeline_materialize.py
git commit -m "feat(build): materialize the mapped snapshot on every engine"
```

---

### Task 8: Collapse the compute path and the preflight

One path, three prerequisites, hard failure. This is where the mode selection
disappears.

**Carried over from the Task 6 review — this task must not ship without it:**
Task 6 left the `except OntoBricksError` fallback inside the `MODE_JOB` branch of
`compute_graph_metrics` (currently lines 3457-3471), which silently answers a
failed job run with `PushdownMetrics` output. That is the "no other paths"
requirement violated in the one place it matters most, so deleting it is a
requirement of this task, not a side effect. A failed job run must raise. Add a
test that a raising `job_metrics.compute` propagates rather than degrading.
The guard in front of it — an unresolvable source failing before a run is
launched — is already covered by
`tests/units/dtwin/test_dtwin_analytics.py::test_an_unresolvable_source_fails_before_any_job_is_launched`;
keep that test passing through the signature change.

**Files:**
- Modify: `src/back/objects/digitaltwin/DigitalTwin.py` (`compute_graph_metrics` lines 3392-3479, `run_metrics_task` lines 2685-2747)
- Modify: `src/api/routers/internal/dtwin.py` (`_analytics_job_status` lines 473-514, `compute_graph_metrics` endpoint lines 538-657, stats endpoint lines 1519-1552)
- Test: `tests/units/api/test_dtwin_analytics.py` (extend or create), `tests/units/objects/test_digitaltwin_metrics.py` (extend)

**Interfaces:**
- Consumes: `resolve_analytics_source` (Task 5), `build_job_metrics(domain, settings, source_table=…, graph_name=…, top_n=…)` (Task 6).
- Produces:
  - `DigitalTwin.compute_graph_metrics(graph_name, *, predicate_filter=None, class_filter=None, top_n=100, settings=None, on_progress=None) -> Dict[str, Any]` — no `store`, no `mode`, no `max_triples`, no `allow_pushdown_fallback`
  - `DigitalTwin.run_metrics_task(tm, task_id, domain, settings, graph_name, *, predicate_filter=None, class_filter=None, top_n=100)`
  - `_analytics_job_status(domain, settings) -> Tuple[bool, str]`

- [ ] **Step 1: Write the failing tests**

```python
def test_preflight_reports_each_prerequisite_in_order(monkeypatch):
    """Each failure names its own remedy; the toggle being off names none."""
    from api.routers.internal import dtwin

    monkeypatch.setattr(dtwin, "resolve_analytics_job_enabled", lambda d, s: False)
    assert dtwin._analytics_job_status(object(), object()) == (False, "")

    monkeypatch.setattr(dtwin, "resolve_analytics_job_enabled", lambda d, s: True)
    monkeypatch.setattr(dtwin, "resolve_analytics_job_name", lambda s: "")
    ok, reason = dtwin._analytics_job_status(object(), object())
    assert ok is False and "ONTOBRICKS_ANALYTICS_JOB_NAME" in reason

    monkeypatch.setattr(dtwin, "resolve_analytics_job_name", lambda s: "job")
    monkeypatch.setattr(
        dtwin, "resolve_analytics_source", lambda d, s: ("", "no table")
    )
    ok, reason = dtwin._analytics_job_status(object(), object())
    assert ok is False and reason == "no table"


def test_preflight_requires_a_non_empty_data_table(monkeypatch):
    """A resolvable but empty …_data means 'Build first', not 'unsupported'."""
    from api.routers.internal import dtwin

    monkeypatch.setattr(dtwin, "resolve_analytics_job_enabled", lambda d, s: True)
    monkeypatch.setattr(dtwin, "resolve_analytics_job_name", lambda s: "job")
    monkeypatch.setattr(
        dtwin, "resolve_analytics_source", lambda d, s: ("cat.sch.t_data", "")
    )
    monkeypatch.setattr(dtwin, "_data_table_has_rows", lambda d, s, t: False)

    ok, reason = dtwin._analytics_job_status(object(), object())
    assert ok is False
    assert "Build" in reason


def test_compute_always_runs_the_job(monkeypatch):
    """No mode argument, no fallback: one path."""
    calls = []
    monkeypatch.setattr(
        DigitalTwin, "build_job_metrics",
        staticmethod(lambda *a, **kw: calls.append(kw) or _FakeJobMetrics()),
    )
    monkeypatch.setattr(
        "back.core.graph_analysis.resolve_analytics_source",
        lambda d, s: ("cat.sch.t_data", ""),
    )
    DigitalTwin(_fake_domain()).compute_graph_metrics(
        "cat.sch.graph", settings=_fake_settings()
    )
    assert calls[0]["source_table"] == "cat.sch.t_data"


def test_compute_raises_when_the_source_is_missing(monkeypatch):
    """Hard fail — a thinner KPI set is what this work removed."""
    monkeypatch.setattr(
        "back.core.graph_analysis.resolve_analytics_source",
        lambda d, s: ("", "Run Knowledge Graph → Build first"),
    )
    with pytest.raises(InfrastructureError) as exc:
        DigitalTwin(_fake_domain()).compute_graph_metrics(
            "cat.sch.graph", settings=_fake_settings()
        )
    assert "Build" in str(exc.value.detail)
```

- [ ] **Step 2: Run and watch them fail**

Run: `env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u LAKEBASE_PROJECT -u LAKEBASE_BRANCH uv run pytest tests/units/api/test_dtwin_analytics.py tests/units/objects/test_digitaltwin_metrics.py -v`

Expected: FAIL — `_analytics_job_status` still requires `store`, `graph_name`
and `pushdown_ok`.

- [ ] **Step 3: Rewrite `compute_graph_metrics`**

Replace lines 3392-3479 of `DigitalTwin.py` with:

```python
    def compute_graph_metrics(
        self,
        graph_name: str,
        predicate_filter: Optional[List[str]] = None,
        class_filter: Optional[List[str]] = None,
        top_n: int = 100,
        settings: Any = None,
        on_progress: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Compute centrality and structural metrics on the mapped graph.

        There is one compute path: the Databricks analytics job, reading the
        ``…_data`` snapshot the Build materialises from the R2RML VIEW. That is
        deliberate — the same domain must produce the same KPIs whatever engine
        holds its graph, and at any size.

        Raises rather than degrading when the job cannot run: a run that
        silently returns fewer metrics is exactly what this path replaced.

        *graph_name* names the output table only; the data comes from the
        resolved source. *on_progress* receives ``(percent, message)``.

        Returns a JSON-serializable dict matching the API contract.
        """
        from back.core.graph_analysis import MetricsRequest, resolve_analytics_source

        source_table, reason = resolve_analytics_source(self._domain, settings)
        if not source_table:
            raise InfrastructureError(
                "The graph analytics job cannot read this domain", detail=reason
            )

        job_metrics = DigitalTwin.build_job_metrics(
            self._domain,
            settings,
            source_table=source_table,
            graph_name=graph_name,
            top_n=top_n,
        )
        request = MetricsRequest(
            predicate_filter=predicate_filter, class_filter=class_filter
        )
        return job_metrics.compute(request, on_progress=on_progress).to_dict()
```

- [ ] **Step 4: Simplify `run_metrics_task`**

Drop `store`, `max_triples`, `max_nodes_betweenness`, `mode` and
`allow_pushdown_fallback` from the signature, and replace the mode-dependent
progress message with the single one that applies:

```python
            tm.update_progress(
                task_id, 20, "Starting the Databricks graph analytics job"
            )
```

Update the `dt.compute_graph_metrics(...)` call to the new signature (no
`store`, no `mode`).

- [ ] **Step 5: Rewrite the preflight**

Replace lines 473-514 of `dtwin.py` with:

```python
def _data_table_has_rows(domain, settings, table: str) -> bool:
    """Whether the mapped snapshot exists and holds at least one triple.

    A ``…_data`` that is absent and one that is empty have the same remedy —
    build the domain — so they are one check.
    """
    from back.core.databricks import DatabricksClient
    from back.core.helpers import (
        get_databricks_host_and_token,
        resolve_delta_warehouse_id,
    )

    try:
        host, token = get_databricks_host_and_token(domain, settings)
        client = DatabricksClient(
            host=host,
            token=token,
            warehouse_id=resolve_delta_warehouse_id(domain, settings),
        )
        rows = client.execute_query(f"SELECT 1 AS ok FROM {table} LIMIT 1")
        return bool(rows)
    except Exception as exc:  # noqa: BLE001
        logger.debug("mapped-snapshot probe failed for %s: %s", table, exc)
        return False


def _analytics_job_status(domain, settings) -> Tuple[bool, str]:
    """Return ``(analytics_available, reason_it_is_not)`` for this domain.

    Analytics needs three things: the admin toggle, a resolvable job name, and
    the mapped snapshot the job reads. Each has a different remedy, so the
    caller gets the specific one rather than a bare ``False``.

    The reason is empty whenever the toggle is off, because then nothing is
    broken: not using the job is the configured behaviour.
    """
    if not resolve_analytics_job_enabled(domain, settings):
        return False, ""

    if not resolve_analytics_job_name(settings):
        return False, (
            "No Databricks job name could be determined. Set "
            "ONTOBRICKS_ANALYTICS_JOB_NAME, or deploy the bundle so the name can "
            "be derived from the app name."
        )

    source, reason = resolve_analytics_source(domain, settings)
    if not source:
        return False, reason or (
            "No mapped-triples table could be resolved for this domain."
        )

    if not _data_table_has_rows(domain, settings, source):
        return False, (
            f"The mapped-triples table {source} is missing or empty. Run "
            f"Knowledge Graph → Build to materialise it, then retry."
        )

    return True, ""
```

- [ ] **Step 6: Rewrite the endpoint**

In the `POST /metrics/compute` handler, delete the triple-count probe, the
`oversized` branch, `pushdown_available`, `mode` and `allow_pushdown_fallback`.
Replace lines 560-648 with:

```python
        predicate_filter = data.get("predicate_filter")
        class_filter = data.get("class_filter")

        domain = get_domain(session_mgr)
        store = _require_graph_store(domain, settings)
        graph_name = _graph_query_table(domain, settings, store)
        if not graph_name:
            raise ValidationError("Graph name is not configured")

        # One compute path. When it cannot run, say why instead of quietly
        # returning a thinner metric set.
        job_available, blocked_reason = _analytics_job_status(domain, settings)
        if not job_available:
            raise ValidationError(
                blocked_reason
                or (
                    "Graph analytics runs on Databricks, which is not enabled "
                    "for this workspace. Enable 'Compute large-graph metrics on "
                    "Databricks' in Settings."
                )
            )

        tm = get_task_manager()
        task = tm.create_task(
            name="Graph Analytics",
            task_type="graph_analytics",
            steps=[
                {"name": "compute", "description": "Computing graph metrics"},
                {"name": "store", "description": "Storing analytics result"},
            ],
        )

        def run_metrics():
            DigitalTwin.run_metrics_task(
                tm,
                task.id,
                domain,
                settings,
                graph_name,
                predicate_filter=predicate_filter,
                class_filter=class_filter,
                top_n=settings.analytics_top_n,
            )

        thread = threading.Thread(target=run_metrics, daemon=True)
        thread.start()

        return {
            "success": True,
            "task_id": task.id,
            "mode": MODE_JOB,
            "message": "Analysis started (running on Databricks)",
        }
```

`store` is still needed for `_graph_query_table` (the output table name is
derived from the graph name), so keep those two lines.

- [ ] **Step 7: Thin the stats payload**

At lines 1519-1552, replace the preflight call and the two payload keys:

```python
            job_available, job_blocked_reason = _analytics_job_status(domain, settings)
```

and in the returned dict drop `analytics_max_triples` and
`analytics_pushdown_available`, keeping `analytics_job_available` and
`analytics_job_blocked_reason`.

Fix the module imports: drop `MODE_IN_MEMORY`, `MODE_PUSHDOWN`,
`supports_pushdown` and `resolve_spark_source`; add `resolve_analytics_source`.

- [ ] **Step 8: Run the tests**

Run: `env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u LAKEBASE_PROJECT -u LAKEBASE_BRANCH uv run pytest tests/units -q`

Expected: PASS except front-end/UI-wording tests, which Task 10 fixes.

- [ ] **Step 9: Commit**

```bash
git add src/back/objects/digitaltwin/DigitalTwin.py src/api/routers/internal/dtwin.py tests/units
git commit -m "refactor(analytics): one compute path, three prerequisites, hard failure"
```

---

### Task 9: Delete the dead compute paths

Now that nothing calls them, remove them. A deletion-only task so a reviewer
can check "is anything still referenced?" without reading feature logic.

**Files:**
- Delete: `src/back/core/graph_analysis/GraphMetrics.py`
- Delete: `src/back/core/graph_analysis/PushdownMetrics.py`
- Delete: `tests/units/core/test_graph_metrics.py`, `tests/units/core/test_pushdown_metrics.py`
- Modify: `src/back/core/graph_analysis/__init__.py`, `src/back/core/graph_analysis/models.py`, `src/shared/config/settings.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `MODE_JOB` is the only mode constant. `MetricsResult.mode` defaults to `MODE_JOB`.

- [ ] **Step 1: Prove nothing references them**

Run: `rg -n "GraphMetrics|PushdownMetrics|supports_pushdown|MODE_IN_MEMORY|MODE_PUSHDOWN|analytics_pushdown_enabled" src tests`

Every hit outside the files being deleted must be fixed in this task. Expect
hits in `src/back/core/graph_analysis/__init__.py`, `models.py`,
`settings.py`, and possibly `.env.example` and the docs.

- [ ] **Step 2: Delete the modules and their tests**

```bash
git rm src/back/core/graph_analysis/GraphMetrics.py \
       src/back/core/graph_analysis/PushdownMetrics.py \
       tests/units/core/test_graph_metrics.py \
       tests/units/core/test_pushdown_metrics.py
```

- [ ] **Step 3: Clean the package exports**

In `src/back/core/graph_analysis/__init__.py`, remove the `GraphMetrics`,
`PushdownMetrics`, `supports_pushdown`, `MODE_IN_MEMORY` and `MODE_PUSHDOWN`
imports and their `__all__` entries. `resolve_analytics_source` must be
exported (Task 5 added it).

- [ ] **Step 4: Collapse the mode constants**

In `src/back/core/graph_analysis/models.py`, delete `MODE_IN_MEMORY` and
`MODE_PUSHDOWN` (lines 141-142) and change line 177:

```python
    mode: str = MODE_JOB
```

Add a short note above the remaining constant so the next reader knows why
there is only one:

```python
#: The only compute mode. Analytics runs in the Databricks job and nowhere
#: else, so a result carries this for the API contract's sake rather than as a
#: choice. Stored results from earlier versions may carry "in_memory" or
#: "pushdown"; the read path must tolerate an unknown string.
MODE_JOB = "job"
```

The `MetricsResult` class docstring (lines 148-169) explains `nodes` in terms of
`in_memory` versus `pushdown` mode, and `approximate_metrics` in terms of "``job``
mode" as one option among several. Rewrite both paragraphs for a single mode:
`nodes` is always the bounded top-N slice (`Settings.analytics_top_n`), and
`stats.node_count` remains the true total, so a node count must never be derived
from `len(nodes)`.

- [ ] **Step 5: Delete the pushdown setting**

In `src/shared/config/settings.py`, delete `analytics_pushdown_enabled` (lines
104-111) and the sentence in `analytics_max_triples`'s comment that refers to
it. Keep `analytics_max_triples` itself — `POST /clusters/detect` uses it.

Remove `ONTOBRICKS_ANALYTICS_PUSHDOWN_ENABLED` from `.env.example` and from
`scripts/deploy.config.sh` if either mentions it.

- [ ] **Step 6: Run the whole suite**

Run: `env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u LAKEBASE_PROJECT -u LAKEBASE_BRANCH uv run pytest -q -m "not scenario"`

Expected: PASS except UI-wording tests (Task 10).

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "refactor(analytics): delete the NetworkX and pushdown compute paths"
```

---

### Task 10: UI and API contract

The panel stops explaining which subset of metrics the user got, because there
is only one set now.

**Files:**
- Modify: `src/front/templates/partials/dtwin/_query_analytics.html`
- Test: `tests/units/front/test_analytics_unavailable_metrics.py` (existing), plus whichever front tests assert the panel's copy

**Interfaces:**
- Consumes: `/dtwin/sync/stats` → `analytics_job_available`, `analytics_job_blocked_reason`; `/dtwin/metrics/compute` → `mode: "job"`.
- Produces: no new interface.

- [ ] **Step 1: Update the failing UI tests first**

Find the assertions that name the removed modes:

Run: `rg -n "in_memory|pushdown|analytics_max_triples|in memory|engine-side" tests/units/front src/front/templates/partials/dtwin/_query_analytics.html`

Rewrite each assertion to the job-only wording. The "not computed" notice for
betweenness/closeness must stop offering the in-memory escape hatch — that
sentence ("Pick an Entity Type above to analyse a subgraph in memory and get
this metric.") is now false. Replace it with the depth-cap remedy:

```
Betweenness is estimated from a sample of source nodes, and this run could not
produce a sample it can stand behind — either no pivots were sampled or the
breadth-first search hit its depth cap. Raise the analytics job's max depth in
Settings and re-run.
```

- [ ] **Step 2: Run and watch them fail**

Run: `env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u LAKEBASE_PROJECT -u LAKEBASE_BRANCH uv run pytest tests/units/front -v`

Expected: FAIL on the new expected copy.

- [ ] **Step 3: Edit the template**

Four changes in `_query_analytics.html`:

1. Delete `_maxTriples` and everything that reads `data.analytics_max_triples`
   (around line 659), plus the size-limit banner it drove.
2. Delete the `analytics_pushdown_available` branch and the "engine-side
   aggregation" copy.
3. Gate Run Analysis on `analytics_job_available`; when false, disable it and
   render `analytics_job_blocked_reason` as the tooltip and inline notice.
4. Replace the unavailable-metric copy per Step 1. Keep the clustering
   genuine-zero explanation: a clustering of 0 across a bipartite knowledge
   graph is a real result, not a missing metric.

- [ ] **Step 4: Run the front tests**

Run: `env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u LAKEBASE_PROJECT -u LAKEBASE_BRANCH uv run pytest tests/units/front -v`

Expected: PASS.

- [ ] **Step 5: Tolerate stored results from earlier versions**

A cached `graph_analytics` row written before this change carries
`mode: "in_memory"` or `"pushdown"`. Nothing may branch on those strings any
more, and nothing may assume the stored mode is `"job"`.

Run: `rg -n "mode" src/front/templates/partials/dtwin/_query_analytics.html src/api/routers/internal/dtwin.py`

Any comparison against a mode string in the render path becomes a plain
display, and the "not computed" copy is driven by `unavailable_metrics` — which
old rows also carry — rather than by the mode. Add the regression test:

```python
def test_a_stored_in_memory_result_still_renders():
    """Cached results predate the Lakeflow-only path; they must not crash."""
    stored = {
        "mode": "in_memory",
        "stats": {"node_count": 10, "graph_node_count": 10, "edge_count": 9},
        "nodes": {},
        "unavailable_metrics": [],
    }
    payload = _render_latest_metrics(stored)
    assert payload["mode"] == "in_memory"
```

- [ ] **Step 6: Verify in the browser**

The dev server is already running (`./scripts/start.sh`). Open the Digital Twin
→ Analytics panel and check three states by hand:

1. Toggle off → Run Analysis disabled, no scary reason text.
2. Toggle on, domain built → Run Analysis enabled, a run completes and the
   type-profile table shows non-zero `avg_clustering`.
3. Entity-type filter set → the run scores only that type.

- [ ] **Step 7: Commit**

```bash
git add src/front/templates/partials/dtwin/_query_analytics.html tests/units/front
git commit -m "feat(analytics): job-only wording and availability gating in the panel"
```

---

### Task 11: Docs, changelog, deploy

**Files:**
- Modify: `documentation/` (the analytics page and any architecture page naming the three modes), `README.md`
- Modify: `changelogs/v0.7.0/benoitcayladbx_2026-07-31.log`

- [ ] **Step 1: Find every doc that describes the old modes**

Run: `rg -ln "in_memory|pushdown|NetworkX|analytics mode" docs README.md`

Update each: analytics is Lakeflow-only, sourced from the R2RML `…_data`
snapshot, and needs a Build before it can run. Keep the NetworkX mentions that
refer to community detection and cohort discovery — those are still true.

- [ ] **Step 2: Run the docs build**

Run: `cd docs && uv run make html`

Expected: no new warnings about broken references to deleted modules.

- [ ] **Step 3: Append the changelog section**

Append to `changelogs/v0.7.0/benoitcayladbx_2026-07-31.log` with a title,
context, a numbered list of changes with file paths, the list of modified files,
and the test result — the format the existing sections in that file use.

- [ ] **Step 4: Run the full suite one last time**

Run: `env -u DATABRICKS_HOST -u DATABRICKS_TOKEN -u LAKEBASE_PROJECT -u LAKEBASE_BRANCH uv run pytest -q -m "not scenario"`

Expected: PASS, no skips beyond the usual scenario markers.

- [ ] **Step 5: Deploy**

The job gained a parameter, and `run_now` rejects parameters the job does not
declare, so the bundle **must** be redeployed before analytics works:

```bash
make deploy
```

- [ ] **Step 6: Verify end to end on the deployed app**

Rebuild one Lakebase domain and one Lakehouse domain, then run analytics on
both. Confirm from `<out>_summary` that `bfs_complete` is true and
`pivot_count` is non-zero, and that betweenness, closeness and clustering all
show values:

```sql
SELECT * FROM benoit_cayla.ontobricks.graph_metrics_<domain>_<version>_summary;
SELECT * FROM benoit_cayla.ontobricks.graph_metrics_<domain>_<version>_type_profiles;
```

- [ ] **Step 7: Commit**

```bash
git add docs README.md changelogs
git commit -m "docs(analytics): describe the Lakeflow-only, R2RML-sourced path"
```

---

## Post-plan notes

**Deliberately out of scope**

- `get_graph_structure_stats`, `get_top_nodes_by_degree`, `get_type_edge_stats`
  and `get_type_predicate_pairs` lose their only caller in Task 9 but stay on
  every store: they are part of the `TripleStoreBackend` contract, and removing
  them touches Delta, Lakebase, Neo4j and the starter kit. Separate cleanup.
  `get_type_distribution` keeps real callers (the entity-type dropdown).
- Backfilling `…_data` for domains built before this change. The remedy is a
  rebuild, and the preflight says so.

**Behaviour changes worth calling out in the changelog**

- `EntityTypeProfile.count` is now the full instance population for every run,
  not only the connected instances on unfiltered runs. Numbers in the type table
  will go up for types with isolated instances. This is the more truthful number
  and it is what makes the flat-dataset heuristic meaningful.
- `avg_clustering` and `avg_betweenness` per type are real values rather than
  the zeros the pushdown path reported.
- Analytics now scores the mapped graph only. A domain where reasoning has run
  will report different KPIs than before, by design.
