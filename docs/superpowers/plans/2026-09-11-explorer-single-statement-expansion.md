# Explorer Single-Statement Expansion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Delta/Lakehouse Explorer's iterative BFS and batched triple retrieval with one bounded SQL statement.

**Architecture:** `DeltaFlatStore` exposes an optional `expand_and_fetch_subgraph` capability that generates a fixed-depth CTE chain and parses bounded results. `DigitalTwin.filter_expand` dispatches to that capability when present and retains the current iterative implementation for every other graph store.

**Tech Stack:** Python 3.10+, Databricks SQL/Lakehouse//RT, Delta Lake, pytest.

## Global Constraints

- Optimize only the Delta/Lakehouse SQL path.
- Keep depth capped at three, Databricks Apps entities capped at 3,000, and returned triples capped at 100,000.
- Preserve forward and reverse traversal, typed-neighbor filtering, inferred-table routing, API payloads, and graph statement timeouts.
- Do not retry iteratively after a Delta SQL failure.
- Run all Python commands through `uv run --frozen`.

---

### Task 1: Generate and execute one bounded Delta expansion query

**Files:**
- Modify: `src/back/core/graphdb/delta/DeltaFlatStore.py`
- Modify: `tests/units/graphdb/delta/test_delta_flat_store.py`

**Interfaces:**
- Consumes: `DeltaFlatStore._sql_relation(table_name: str) -> str`, `_sql_escape(value: str) -> str`, and `execute_query(query: str) -> list[dict]`.
- Produces: `DeltaFlatStore.expand_and_fetch_subgraph(table_name: str, selected_uris: list[str], depth: int, max_entities: int, max_triples: int) -> dict[str, Any]`.

- [ ] **Step 1: Write failing SQL-generation tests**

Append a `TestDeltaSingleStatementExpansion` class. Capture SQL through
`store.execute_query`, return representative rows carrying
`_ob_expanded_count`, and assert:

```python
class TestDeltaSingleStatementExpansion:
    RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"

    def _store(self, rows=None):
        store = DeltaFlatStore(MagicMock())
        store.execute_query = MagicMock(return_value=rows or [])
        return store

    def test_depth_three_uses_one_statement_and_three_levels(self):
        store = self._store(
            [{"subject": "s", "predicate": self.RDF_TYPE, "object": "T",
              "_ob_expanded_count": 4}]
        )
        result = store.expand_and_fetch_subgraph(
            "cat.sch.graph", ["http://ex/a"], 3, 3000, 100000
        )
        store.execute_query.assert_called_once()
        sql = store.execute_query.call_args.args[0]
        assert all(f"level_{n}" in sql for n in range(4))
        assert "LEFT ANTI JOIN visited_3" in sql
        assert "LIMIT 3001" in sql
        assert "LIMIT 100001" in sql
        assert result["expanded_count"] == 4

    def test_depth_zero_has_no_neighbor_level(self):
        store = self._store()
        store.expand_and_fetch_subgraph(
            "cat.sch.graph", ["http://ex/a"], 0, 100, 200
        )
        sql = store.execute_query.call_args.args[0]
        assert "level_0" in sql
        assert "level_1_candidates" not in sql

    def test_query_traverses_both_directions_and_only_typed_neighbors(self):
        store = self._store()
        store.expand_and_fetch_subgraph(
            "cat.sch.graph", ["http://ex/a"], 1, 100, 200
        )
        sql = store.execute_query.call_args.args[0]
        assert "t.subject = frontier.entity" in sql
        assert "t.object = frontier.entity" in sql
        assert f"typed.predicate = '{self.RDF_TYPE}'" in sql
        assert self.RDFS_LABEL in sql

    def test_seed_uris_are_escaped(self):
        store = self._store()
        store.expand_and_fetch_subgraph(
            "cat.sch.graph", ["http://ex/O'Brien"], 1, 100, 200
        )
        assert "O''Brien" in store.execute_query.call_args.args[0]
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
uv run --frozen pytest -q \
  tests/units/graphdb/delta/test_delta_flat_store.py::TestDeltaSingleStatementExpansion
```

Expected: FAIL because `DeltaFlatStore.expand_and_fetch_subgraph` does not exist.

- [ ] **Step 3: Implement fixed-depth SQL generation**

Add private helpers to build:

```python
def _expansion_level_ctes(
    self, relation: str, depth: int, max_entities: int
) -> List[str]:
    ctes: List[str] = []
    for level in range(1, depth + 1):
        previous = f"level_{level - 1}"
        visited = f"visited_{level}"
        visited_union = " UNION ALL ".join(
            f"SELECT entity FROM level_{n}" for n in range(level)
        )
        ctes.extend(
            [
                f"{visited} AS ({visited_union})",
                (
                    f"level_{level}_candidates AS ("
                    f"SELECT t.object AS entity FROM {relation} t "
                    f"JOIN {previous} frontier ON t.subject = frontier.entity "
                    f"WHERE t.object LIKE 'http%' "
                    f"AND t.predicate != '{RDF_TYPE}' "
                    f"AND t.predicate != '{RDFS_LABEL}' "
                    f"UNION ALL "
                    f"SELECT t.subject AS entity FROM {relation} t "
                    f"JOIN {previous} frontier ON t.object = frontier.entity "
                    f"WHERE t.predicate != '{RDF_TYPE}' "
                    f"AND t.predicate != '{RDFS_LABEL}')"
                ),
                (
                    f"level_{level} AS ("
                    f"SELECT DISTINCT candidate.entity "
                    f"FROM level_{level}_candidates candidate "
                    f"JOIN {relation} typed "
                    f"ON typed.subject = candidate.entity "
                    f"AND typed.predicate = '{RDF_TYPE}' "
                    f"LEFT ANTI JOIN {visited} "
                    f"ON {visited}.entity = candidate.entity "
                    f"LIMIT {max_entities})"
                ),
            ]
        )
    return ctes
```

Add `expand_and_fetch_subgraph` to validate integer bounds, create `level_0`
from escaped seed `VALUES`, append generated level CTEs, build an
`entity_probe` limited to `max_entities + 1`, a bounded `entities` CTE, and one
final subject join limited to `max_triples + 1`.

Parse `_ob_expanded_count` from returned rows, remove it from each triple,
truncate the extra row, and return:

```python
{
    "results": triples,
    "count": len(triples),
    "expanded_count": min(discovered_count, max_entities),
    "capped": discovered_count > max_entities or triple_capped,
    "timeout_capped": False,
}
```

- [ ] **Step 4: Add result-boundary tests**

Cover exact-limit and extra-row cases:

```python
def test_extra_triple_row_marks_result_capped(self):
    rows = [
        {"subject": f"s{i}", "predicate": "p", "object": "o",
         "_ob_expanded_count": 3}
        for i in range(3)
    ]
    store = self._store(rows)
    result = store.expand_and_fetch_subgraph("g", ["s0"], 0, 10, 2)
    assert len(result["results"]) == 2
    assert result["capped"] is True
    assert all("_ob_expanded_count" not in row for row in result["results"])

def test_entity_probe_marks_result_capped(self):
    store = self._store(
        [{"subject": "s", "predicate": "p", "object": "o",
          "_ob_expanded_count": 11}]
    )
    result = store.expand_and_fetch_subgraph("g", ["s"], 1, 10, 20)
    assert result["expanded_count"] == 10
    assert result["capped"] is True
```

- [ ] **Step 5: Run Task 1 tests**

Run:

```bash
uv run --frozen pytest -q \
  tests/units/graphdb/delta/test_delta_flat_store.py \
  tests/units/core/test_graph_query_bounds.py
```

Expected: all selected tests pass.

- [ ] **Step 6: Commit Task 1**

```bash
git add src/back/core/graphdb/delta/DeltaFlatStore.py \
  tests/units/graphdb/delta/test_delta_flat_store.py
git commit -m "perf(kg): query expanded Delta subgraph once"
```

---

### Task 2: Dispatch Delta expansion without changing other backends

**Files:**
- Modify: `src/back/objects/digitaltwin/DigitalTwin.py`
- Create: `tests/units/dtwin/test_filter_expand.py`

**Interfaces:**
- Consumes: optional store method `expand_and_fetch_subgraph(table_name, selected_uris, depth, max_entities, max_triples)`.
- Produces: unchanged `DigitalTwin.filter_expand(...) -> dict[str, Any]` API payload.

- [ ] **Step 1: Write failing capability-dispatch tests**

Create fake optimized and iterative stores:

```python
class OptimizedStore:
    def __init__(self):
        self.calls = []

    def expand_and_fetch_subgraph(self, *args):
        self.calls.append(args)
        return {
            "results": [{"subject": "s", "predicate": "p", "object": "o"}],
            "count": 1,
            "expanded_count": 2,
            "capped": False,
            "timeout_capped": False,
        }

    def expand_entity_neighbors(self, *_):
        raise AssertionError("iterative expansion must not run")

    def get_triples_for_subjects(self, *_):
        raise AssertionError("batched fetch must not run")


def test_filter_expand_uses_single_statement_capability():
    store = OptimizedStore()
    result = DigitalTwin.filter_expand(
        store, "graph", ["seed"], depth=2, max_entities=50,
        max_triples=100, batch_size=10
    )
    assert len(store.calls) == 1
    assert result["initial_count"] == 1
    assert result["expanded_count"] == 2


def test_filter_expand_keeps_iterative_fallback():
    store = MagicMock(spec=["expand_entity_neighbors", "get_triples_for_subjects"])
    store.expand_entity_neighbors.return_value = {"neighbor"}
    store.get_triples_for_subjects.return_value = [
        {"subject": "seed", "predicate": "p", "object": "neighbor"}
    ]
    result = DigitalTwin.filter_expand(
        store, "graph", ["seed"], depth=1, max_entities=50,
        max_triples=100, batch_size=10
    )
    store.expand_entity_neighbors.assert_called_once()
    store.get_triples_for_subjects.assert_called_once()
    assert result["initial_count"] == 1
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
uv run --frozen pytest -q tests/units/dtwin/test_filter_expand.py
```

Expected: optimized test fails because `filter_expand` still calls the
iterative methods.

- [ ] **Step 3: Add capability dispatch**

At the start of `DigitalTwin.filter_expand`, after `initial_count`:

```python
single_statement_expand = getattr(store, "expand_and_fetch_subgraph", None)
if callable(single_statement_expand):
    payload = single_statement_expand(
        graph_name,
        list(entity_set),
        depth,
        max_entities,
        max_triples,
    )
    return {
        "phase": "expand",
        **payload,
        "initial_count": initial_count,
    }
```

Do not catch capability exceptions. Leave the existing iterative body
unchanged below this branch.

- [ ] **Step 4: Add SQL-error propagation test**

```python
def test_filter_expand_does_not_retry_after_single_statement_failure():
    store = OptimizedStore()
    store.expand_and_fetch_subgraph = MagicMock(side_effect=RuntimeError("timeout"))
    with pytest.raises(RuntimeError, match="timeout"):
        DigitalTwin.filter_expand(
            store, "graph", ["seed"], depth=2, max_entities=50,
            max_triples=100, batch_size=10
        )
    store.expand_and_fetch_subgraph.assert_called_once()
```

- [ ] **Step 5: Run Task 2 tests**

Run:

```bash
uv run --frozen pytest -q \
  tests/units/dtwin/test_filter_expand.py \
  tests/units/graphdb/delta/test_delta_flat_store.py
```

Expected: all selected tests pass.

- [ ] **Step 6: Commit Task 2**

```bash
git add src/back/objects/digitaltwin/DigitalTwin.py \
  tests/units/dtwin/test_filter_expand.py
git commit -m "perf(kg): dispatch Delta expansion in one statement"
```

---

### Task 3: Document, verify, and benchmark

**Files:**
- Modify: `docs/get-started.md`
- Modify: `docs/features.md`
- Modify: `changelogs/v0.9.0/benoitcayladbx_2026-09-11.log`

**Interfaces:**
- Consumes: completed Delta capability and unchanged Explorer API.
- Produces: user documentation, changelog evidence, and benchmark result.

- [ ] **Step 1: Update user documentation**

In the existing Explorer timing sections, state that Delta/Lakehouse expansion
uses one bounded RT statement while other backends retain native or iterative
traversal. Do not promise a fixed latency.

- [ ] **Step 2: Run focused static checks**

```bash
uv run --frozen pytest -q \
  tests/units/graphdb/delta/test_delta_flat_store.py \
  tests/units/dtwin/test_filter_expand.py \
  tests/units/front/test_explorer_search_timing.py
uv run --frozen ruff check \
  src/back/core/graphdb/delta/DeltaFlatStore.py \
  src/back/objects/digitaltwin/DigitalTwin.py \
  tests/units/graphdb/delta/test_delta_flat_store.py \
  tests/units/dtwin/test_filter_expand.py
```

Expected: all focused tests and Ruff checks pass.

- [ ] **Step 3: Run the full regression suite**

```bash
uv run --frozen pytest -q -m "not scenario"
```

Expected: zero failures.

- [ ] **Step 4: Browser-benchmark Expansion**

Use `BIGCustomers_V1`, a warm Lakehouse//RT warehouse, identical selected
seeds, depth, and limits. Run at least three before/after Expansion requests,
record the median, and verify:

- exactly one SQL statement per optimized Expansion request;
- graph entity/triple output remains compatible;
- no browser console or failed network requests;
- the entity-selection dwell remains excluded from Total.

- [ ] **Step 5: Append the mandatory changelog section**

Record context, numbered file changes, all modified files, focused/full test
summaries, and the before/after median Expansion duration in
`changelogs/v0.9.0/benoitcayladbx_2026-09-11.log`.

- [ ] **Step 6: Commit documentation and verification evidence**

```bash
git add docs/get-started.md docs/features.md \
  changelogs/v0.9.0/benoitcayladbx_2026-09-11.log
git commit -m "docs(kg): document bounded RT expansion"
```
