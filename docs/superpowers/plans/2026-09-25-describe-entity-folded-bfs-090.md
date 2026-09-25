# Folded `describe_entity` BFS 0.9.0 Forward-Port Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Forward-port PR 182 to `0.9.0` by moving triple de-duplication, exact counting, stable ordering, and pagination into `_props`/SPO SQL while retaining the existing companion-aware BFS and URI aliases.

**Architecture:** `DigitalTwin.find_triples_bfs` continues to call the existing `_entity_search` plus `_adj_out/_adj_in` traversal and alias expansion. A new `GraphDBBackend.get_triples_page_for_subjects` sends only entity URIs to a `_props`-aware SQL helper, which returns one ordered page plus the exact distinct total and falls back to SPO through the existing missing-table mechanism.

**Tech Stack:** Python 3.10+, Databricks SQL, PostgreSQL/Lakebase, Neo4j Cypher, FastAPI/Pydantic, pytest.

## Global Constraints

- Base the new forward-port PR on `0.9.0`; do not merge or enable auto-merge.
- Preserve Laurent Prat's authorship on the original performance commit and credit him in the PR.
- Reuse `bfs_traversal`, `entity_search_seed_sql`, `seeded_bfs_sql`, `expand_uri_aliases`, and existing companion fallbacks.
- Preserve exact `total`, exact alias-expanded `entity_count`, and add `has_more`.
- Python may materialize entity URIs but not the complete SQL-backend triple neighborhood.
- Stable page order is `(subject, predicate, object)`.
- Use `uv run --frozen`; never run bare `uv run`.
- Comments, docs, logs, changelog, and PR text are English.
- A reviewer waiver or linked MLflow eval is required for MCP/agent-facing changes.

---

### Task 1: Add `_props`-aware page and exact-count SQL

**Files:**
- Modify: `src/back/core/graphdb/props.py`
- Modify: `src/back/core/graphdb/GraphDBBackend.py`
- Test: `tests/units/graphdb/test_adjacency_sql.py`
- Test: `tests/units/graphdb/test_graphdb_adjacency_contract.py`
- Test: `tests/units/core/test_lakebase_flat_store.py`

**Interfaces:**
- Produces: `props_page_sql(payload_relation, uris, limit, offset, escape) -> str`.
- Produces: `GraphDBBackend.get_triples_page_for_subjects(...) -> {"rows": list[dict], "total": int}`.
- Reuses: `execute_expand_with_props_fallback` for `_props` to SPO fallback.

- [ ] **Step 1: Write failing SQL-builder tests**

Add tests that call:

```python
sql = props_page_sql(
    payload_relation="g_props",
    uris=["http://ex/a", "http://ex/O'Brien"],
    limit=2,
    offset=3,
    escape=lambda value: value.replace("'", "''"),
)
```

Assert it contains:

```python
assert "SELECT DISTINCT subject, predicate, object" in sql
assert "COUNT(*) AS _ob_total" in sql
assert "ORDER BY page.subject, page.predicate, page.object" in sql
assert "LIMIT 2 OFFSET 3" in sql
assert "O''Brien" in sql
```

Add empty-subject behavior asserting the builder is not called and the backend
returns `{"rows": [], "total": 0}`.

- [ ] **Step 2: Run builder tests and verify RED**

```bash
uv run --frozen pytest -q tests/units/graphdb/test_adjacency_sql.py \
  -k props_page
```

Expected: import/function failures because `props_page_sql` does not exist.

- [ ] **Step 3: Implement the page SQL builder**

Generate:

```sql
WITH base AS (
  SELECT DISTINCT subject, predicate, object
  FROM <payload_relation>
  WHERE subject IN (<escaped URIs>)
), stats AS (
  SELECT COUNT(*) AS _ob_total FROM base
), page AS (
  SELECT subject, predicate, object FROM base
  ORDER BY subject, predicate, object
  LIMIT <int(limit)> OFFSET <int(offset)>
)
SELECT page.subject, page.predicate, page.object, stats._ob_total
FROM stats LEFT JOIN page ON TRUE
ORDER BY page.subject, page.predicate, page.object
```

The left join guarantees one metadata row for an empty page.

- [ ] **Step 4: Write failing backend fallback tests**

For `get_triples_page_for_subjects`, cover:

1. `_props` relation used when supported;
2. known-missing `_props` immediately uses SPO;
3. missing-table exception marks `_props` missing and retries SPO;
4. metadata keys are stripped from returned triples;
5. empty subjects execute no query.

Use result rows shaped as:

```python
[{
    "subject": "s",
    "predicate": "p",
    "object": "o",
    "_ob_total": 7,
}]
```

- [ ] **Step 5: Run fallback tests and verify RED**

```bash
uv run --frozen pytest -q \
  tests/units/graphdb/test_graphdb_adjacency_contract.py \
  tests/units/core/test_lakebase_flat_store.py \
  -k triples_page
```

Expected: failures because the backend method is absent.

- [ ] **Step 6: Implement the backend method**

Add:

```python
def get_triples_page_for_subjects(
    self,
    table_name: str,
    subjects: List[str],
    *,
    limit: int,
    offset: int = 0,
) -> Dict[str, Any]:
```

Build `_props` and SPO versions with `props_page_sql`, execute through
`execute_expand_with_props_fallback`, read `_ob_total` from the first row,
discard a null placeholder row, and return triples without `_ob_total`.

- [ ] **Step 7: Run focused tests and verify GREEN**

```bash
uv run --frozen pytest -q \
  tests/units/graphdb/test_adjacency_sql.py \
  tests/units/graphdb/test_graphdb_adjacency_contract.py \
  tests/units/core/test_lakebase_flat_store.py
```

Expected: all selected tests pass.

- [ ] **Step 8: Commit**

```bash
git add src/back/core/graphdb/props.py \
  src/back/core/graphdb/GraphDBBackend.py \
  tests/units/graphdb/test_adjacency_sql.py \
  tests/units/graphdb/test_graphdb_adjacency_contract.py \
  tests/units/core/test_lakebase_flat_store.py
git commit -m "perf(graphdb): page BFS payloads server-side"
```

---

### Task 2: Use paged payloads from `DigitalTwin`

**Files:**
- Modify: `src/back/objects/digitaltwin/DigitalTwin.py`
- Test: `tests/back/core/digitaltwin/test_digitaltwin_units.py`
- Test: `tests/units/dtwin/test_digitaltwin_api.py`

**Interfaces:**
- Consumes: `get_triples_page_for_subjects` from Task 1.
- Produces: legacy result fields plus additive `has_more`.

- [ ] **Step 1: Write failing orchestration tests**

Use a fake store that records:

- `bfs_traversal` returning one seed plus neighbors;
- `find_subjects_by_patterns` adding an alias URI;
- `get_triples_page_for_subjects` returning a page and exact total.

Assert:

```python
assert result["seed_count"] == 1
assert result["entity_count"] == 3
assert result["total"] == 7
assert result["count"] == 2
assert result["has_more"] is True
assert alias_uri in paged_call_subjects
```

Add final-page and empty-BFS tests; the empty case must not call paged fetch.

- [ ] **Step 2: Run orchestration tests and verify RED**

```bash
uv run --frozen pytest -q \
  tests/back/core/digitaltwin/test_digitaltwin_units.py \
  tests/units/dtwin/test_digitaltwin_api.py \
  -k "find_triples_bfs or alias"
```

Expected: failures because the method still fetches and slices all triples.

- [ ] **Step 3: Replace Python triple materialization**

Keep companion-aware traversal and aliases unchanged:

```python
bfs_rows = store.bfs_traversal(...)
all_entities = {r["entity"] for r in bfs_rows}
seed_count = sum(1 for r in bfs_rows if int(r.get("min_lvl", 0)) == 0)
all_entities = DigitalTwin.expand_uri_aliases(store, table, all_entities)
page = store.get_triples_page_for_subjects(
    table,
    list(all_entities),
    limit=limit,
    offset=offset,
)
triples = page["rows"]
total = page["total"]
has_more = offset + len(triples) < total
```

Return exact legacy fields plus `has_more`; remove the Python dedup loop and
slice.

- [ ] **Step 4: Run focused tests and verify GREEN**

```bash
uv run --frozen pytest -q \
  tests/back/core/digitaltwin/test_digitaltwin_units.py \
  tests/units/dtwin/test_digitaltwin_api.py
```

Expected: all selected tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/back/objects/digitaltwin/DigitalTwin.py \
  tests/back/core/digitaltwin/test_digitaltwin_units.py \
  tests/units/dtwin/test_digitaltwin_api.py
git commit -m "perf(digitaltwin): avoid BFS triple materialization"
```

---

### Task 3: Add Neo4j page parity

**Files:**
- Modify: `src/back/core/graphdb/neo4j/Neo4jReadOps.py`
- Modify: `src/back/core/graphdb/neo4j/Neo4jStore.py`
- Test: `tests/units/graphdb/test_neo4j_roundtrip.py`

**Interfaces:**
- Produces: `get_triples_page_for_subjects` with the SQL-backend contract.

- [ ] **Step 1: Write a failing parity test**

Stub `get_triples_for_subjects` with duplicates and unsorted rows. Assert the
new method returns sorted distinct rows for the requested offset/limit and an
exact total.

- [ ] **Step 2: Run the parity test and verify RED**

```bash
uv run --frozen pytest -q \
  tests/units/graphdb/test_neo4j_roundtrip.py -k triples_page
```

Expected: failure because the method/delegation is absent.

- [ ] **Step 3: Implement the Neo4j override**

Use `get_triples_for_subjects`, de-duplicate by `(subject, predicate, object)`,
sort, and return:

```python
{"rows": dedup[offset:offset + limit], "total": len(dedup)}
```

Add the corresponding `Neo4jStore` delegation. This secondary backend may
materialize its triple neighborhood for parity.

- [ ] **Step 4: Run Neo4j tests and verify GREEN**

```bash
uv run --frozen pytest -q tests/units/graphdb/test_neo4j_roundtrip.py
```

Expected: all selected tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/back/core/graphdb/neo4j/Neo4jReadOps.py \
  src/back/core/graphdb/neo4j/Neo4jStore.py \
  tests/units/graphdb/test_neo4j_roundtrip.py
git commit -m "fix(neo4j): add BFS payload paging parity"
```

---

### Task 4: Thread `has_more`, document, verify, and open the PR

**Files:**
- Modify: `src/api/routers/digitaltwin.py`
- Modify: `src/api/routers/internal/dtwin.py`
- Modify: `src/agents/tools/graph_formatting.py`
- Modify: `src/mcp-server/server/formatting.py`
- Modify: `src/mcp-server/server/tools.py`
- Modify: `src/front/templates/partials/dtwin/_query_api.html`
- Modify: `docs/api.md`
- Modify: `docs/optimizations.md`
- Create/append: `changelogs/v0.9.0/benoitcayladbx_2026-09-25.log`

**Interfaces:**
- Consumes: Task 2's additive `has_more`.
- Produces: linked, unmerged PR targeting `0.9.0`.

- [ ] **Step 1: Write failing response/formatter tests**

Assert `FindResponse.has_more is False` by default, both routes propagate the
domain result, and both formatters use explicit `has_more` for the pagination
hint while retaining exact-total output.

- [ ] **Step 2: Run focused tests and verify RED**

```bash
uv run --frozen pytest -q \
  tests/units/dtwin/test_digitaltwin_api.py \
  tests/units/agents/test_agent_dtwin_chat.py \
  tests/mcp
```

Expected: response/formatter assertions fail.

- [ ] **Step 3: Implement additive response and formatter behavior**

Add:

```python
has_more: bool = Field(False, description="Whether more triples exist beyond this page")
```

Map it in both routes. Formatters keep exact `total` and use `has_more` for
the truncation hint. Keep `%`-style logs.

- [ ] **Step 4: Update docs and changelog**

Document:

- deterministic `(subject, predicate, object)` page ordering;
- exact `total` and alias-expanded `entity_count`;
- additive `has_more`;
- reuse of `_entity_search`, `_adj_out/_adj_in`, and `_props`;
- SPO/missing-table fallback;
- `depth=1` guidance for type-wide scans.

The changelog must list every modified file and exact test result.

- [ ] **Step 5: Run diagnostics and mandatory tests**

Check modified-file lint diagnostics, then run:

```bash
uv run --frozen pytest -q -m "not scenario"
```

Expected: zero failures. Update the changelog with the exact summary and commit.

- [ ] **Step 6: Verify attribution**

The branch must include Laurent's original performance commit or a
cherry-picked derivative retaining:

```text
Author: Laurent Prat <laurent.prat@databricks.com>
Co-authored-by: Isaac <no-reply@databricks.com>
```

Do not squash it into maintainer-authored commits.

- [ ] **Step 7: Push and open the linked PR**

Push a branch based on `0.9.0`, then open a PR authored by the authenticated
maintainer and targeting `0.9.0`. Its body must:

- credit Laurent as original contributor;
- link PR 182;
- explain why 0.9.0 folds only payload fetch/paging;
- report exact tests and benchmark provenance;
- request the `.cursor/12` reviewer eval waiver or link an MLflow run;
- state that it is intentionally unmerged pending confirmation.

Post reciprocal links on PR 182 and the new PR. Do not merge, auto-merge, or
queue either PR.
