# Folded `describe_entity` BFS 0.8.1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Update PR 182 for `0.8.1` so BFS search remains server-side and fast while preserving URI aliases and the existing exact response metadata.

**Architecture:** `GraphDBBackend.find_triples_bfs_page` performs seed selection, bidirectional traversal, alias expansion, triple de-duplication, exact metadata calculation, ordering, and pagination in one SQL operation. `DigitalTwin` and both routers only normalize that result. Neo4j keeps a native/Python parity implementation because it does not consume SQL fragments.

**Tech Stack:** Python 3.10+, FastAPI/Pydantic, Databricks SQL, PostgreSQL/Lakebase, Neo4j Cypher, pytest.

## Global Constraints

- Target PR 182 at branch `0.8.1`; do not merge or enable auto-merge.
- Preserve Laurent Prat as PR author and original commit author; add fix commits without squashing.
- Preserve legacy URI-alias inclusion and exact `total` / `entity_count`; add `has_more`.
- Do not materialize the complete triple neighborhood in Python on SQL backends.
- Cast SQL pagination and traversal integers before interpolation.
- Comments, docs, logs, changelog, and PR text are English.
- Use `uv run --frozen`; never run bare `uv run`.
- A reviewer waiver or linked MLflow eval is required because MCP/agent-facing text changes.

---

### Task 1: Fold aliases and exact metadata into SQL

**Files:**
- Modify: `src/back/core/graphdb/GraphDBBackend.py`
- Modify: `src/back/objects/digitaltwin/DigitalTwin.py`
- Test: `tests/units/dtwin/test_digitaltwin_api.py`
- Test: `tests/back/core/digitaltwin/test_digitaltwin_units.py`

**Interfaces:**
- Produces: `GraphDBBackend.find_triples_bfs_page(...) -> {"seed_count": int, "triples": list[dict], "total": int, "entity_count": int, "has_more": bool}`.
- Produces: `DigitalTwin.find_triples_bfs(...)` with the same metadata plus `depth`, `count`, `limit`, and `offset`.
- Removes: the separate `count_seeds` round trip introduced by the original PR.

- [ ] **Step 1: Write failing SQL contract tests**

Add tests to `TestBfsTraversalSql` using its real-helper `MagicMock` harness:

```python
class _Depth:
    def __int__(self):
        return 2

    def __str__(self):
        return "unsafe-depth"


def test_depth_is_cast_before_sql_interpolation(self):
    sql = self._capture_sql("tbl", " WHERE 1=1", _Depth())
    assert "b.lvl < 2" in sql
    assert "unsafe-depth" not in sql


def test_find_page_folds_aliases_and_exact_metadata(self):
    captured = {}

    def fake_execute(sql):
        captured["sql"] = sql
        return [{
            "subject": "s",
            "predicate": "p",
            "object": "o",
            "seed_count": 3,
            "total": 7,
            "entity_count": 5,
        }]

    store = self._bfs_store(fake_execute)
    out = GraphDBBackend.find_triples_bfs_page(
        store, "tbl", " WHERE 1=1", 2, limit=1, offset=0
    )

    assert "alias_ids AS" in captured["sql"]
    assert "regexp_replace(" in captured["sql"]
    assert "all_ents AS" in captured["sql"]
    assert "distinct_triples AS" in captured["sql"]
    assert "stats AS" in captured["sql"]
    assert out == {
        "seed_count": 3,
        "triples": [{"subject": "s", "predicate": "p", "object": "o"}],
        "total": 7,
        "entity_count": 5,
        "has_more": True,
    }
```

Update existing page tests so mocked rows include metadata, and replace the
`limit + 1` assertion with exact-total pagination assertions.

- [ ] **Step 2: Run the SQL tests and verify RED**

Run:

```bash
uv run --frozen pytest -q \
  tests/units/dtwin/test_digitaltwin_api.py::TestBfsTraversalSql
```

Expected: failures because aliases and exact metadata are absent and depth is
interpolated through `str()`.

- [ ] **Step 3: Implement the one-operation SQL result**

In `_bfs_walk_cte`, cast depth:

```python
safe_depth = max(0, int(depth))
```

Build the page query around these CTE responsibilities:

```python
sql = (
    f"{self._bfs_walk_cte(rel, seed_where, depth)}, ents AS (\n"
    f"  SELECT DISTINCT entity FROM bfs\n"
    f"), alias_ids AS (\n"
    f"  SELECT DISTINCT regexp_replace(entity, '^.*[#/]', '') AS local_id\n"
    f"  FROM ents\n"
    f"  WHERE regexp_replace(entity, '^.*[#/]', '') != ''\n"
    f"), alias_ents AS (\n"
    f"  SELECT DISTINCT candidate.subject AS entity\n"
    f"  FROM {rel} candidate\n"
    f"  JOIN alias_ids alias\n"
    f"    ON candidate.subject LIKE CONCAT('%/', alias.local_id)\n"
    f"), all_ents AS (\n"
    f"  SELECT entity FROM ents UNION SELECT entity FROM alias_ents\n"
    f"), distinct_triples AS (\n"
    f"  SELECT DISTINCT t.subject, t.predicate, t.object\n"
    f"  FROM {rel} t JOIN all_ents e ON t.subject = e.entity\n"
    f"), stats AS (\n"
    f"  SELECT\n"
    f"    (SELECT COUNT(*) FROM seeds) AS seed_count,\n"
    f"    (SELECT COUNT(*) FROM distinct_triples) AS total,\n"
    f"    (SELECT COUNT(*) FROM all_ents) AS entity_count\n"
    f"), page AS (\n"
    f"  SELECT subject, predicate, object FROM distinct_triples\n"
    f"  ORDER BY subject, predicate, object\n"
    f"  LIMIT {int(limit)} OFFSET {int(offset)}\n"
    f")\n"
    f"SELECT page.subject, page.predicate, page.object,\n"
    f"       stats.seed_count, stats.total, stats.entity_count\n"
    f"FROM stats LEFT JOIN page ON TRUE\n"
    f"ORDER BY page.subject, page.predicate, page.object"
)
```

Parse the always-present stats row, discard the null page placeholder, strip
metadata keys from triples, and compute:

```python
has_more = offset + len(triples) < total
```

Delete `GraphDBBackend.count_seeds`; it is no longer part of this path.

- [ ] **Step 4: Write failing domain orchestration tests**

Change `_FakeStore` so its page result includes all metadata and assert the
domain method delegates exactly once:

```python
def test_returns_compatible_metadata_from_one_store_call(self):
    store = _FakeStore(page={
        "seed_count": 3,
        "triples": [{"subject": "s", "predicate": "p", "object": "o"}],
        "total": 7,
        "entity_count": 5,
        "has_more": True,
    })
    out = DigitalTwin.find_triples_bfs(
        store, "tbl", entity_type="Counterparty", depth=2, limit=1
    )
    assert out["seed_count"] == 3
    assert out["total"] == 7
    assert out["entity_count"] == 5
    assert out["has_more"] is True
    assert [call[0] for call in store.calls] == ["page"]
```

Add an empty-result test where the page operation returns zero metadata and no
triples; assert the existing message and all zero/false compatibility fields.

- [ ] **Step 5: Run domain tests and verify RED**

Run:

```bash
uv run --frozen pytest -q \
  tests/back/core/digitaltwin/test_digitaltwin_units.py::TestFindTriplesBfs
```

Expected: failures because `DigitalTwin` still calls `count_seeds` and does not
thread `total` / `entity_count`.

- [ ] **Step 6: Implement one-call domain orchestration**

Call `store.find_triples_bfs_page(...)` first, use its `seed_count` for the
empty-match branch, and return all metadata without Python deduplication or
alias expansion.

- [ ] **Step 7: Run focused tests and verify GREEN**

Run:

```bash
uv run --frozen pytest -q \
  tests/units/dtwin/test_digitaltwin_api.py \
  tests/back/core/digitaltwin/test_digitaltwin_units.py
```

Expected: all selected tests pass.

- [ ] **Step 8: Commit**

```bash
git add src/back/core/graphdb/GraphDBBackend.py \
  src/back/objects/digitaltwin/DigitalTwin.py \
  tests/units/dtwin/test_digitaltwin_api.py \
  tests/back/core/digitaltwin/test_digitaltwin_units.py
git commit -m "fix(graphdb): preserve folded BFS result semantics"
```

---

### Task 2: Restore Neo4j parity

**Files:**
- Modify: `src/back/core/graphdb/neo4j/Neo4jReadOps.py`
- Modify: `src/back/core/graphdb/neo4j/Neo4jStore.py`
- Test: `tests/units/graphdb/test_neo4j_roundtrip.py`

**Interfaces:**
- Consumes: the result contract from Task 1.
- Produces: the same result contract for Neo4j, with alias expansion and exact
  metadata; Python materialization is allowed for this secondary backend.

- [ ] **Step 1: Write a failing Neo4j parity test**

Use the existing Neo4j read/store fixture and stub traversal, alias lookup, and
triple fetch so two BFS entities expand to a third alias entity. Assert:

```python
assert result["seed_count"] == 1
assert result["entity_count"] == 3
assert result["total"] == 4
assert result["has_more"] is True
assert len(result["triples"]) == 2
```

Also assert the page is sorted and the alias subject is represented.

- [ ] **Step 2: Run the Neo4j test and verify RED**

Run:

```bash
uv run --frozen pytest -q \
  tests/units/graphdb/test_neo4j_roundtrip.py -k bfs_page
```

Expected: failure because aliases and exact metadata are absent.

- [ ] **Step 3: Implement Neo4j metadata and aliases**

In `Neo4jReadOps.find_triples_bfs_page`:

```python
seed_count = sum(int(r.get("min_lvl", 0)) == 0 for r in bfs_rows)
local_ids = {
    str(entity).rsplit("#", 1)[-1].rsplit("/", 1)[-1]
    for entity in entities
}
patterns = [f"%/{local_id}" for local_id in local_ids if local_id]
entities.update(self.find_subjects_by_patterns(table_name, patterns))
```

Deduplicate and sort as today, then return `seed_count`, `total`,
`entity_count`, `has_more`, and the page. Remove `count_seeds` from
`Neo4jReadOps` and its `Neo4jStore` pass-through.

- [ ] **Step 4: Run Neo4j tests and verify GREEN**

Run:

```bash
uv run --frozen pytest -q tests/units/graphdb/test_neo4j_roundtrip.py
```

Expected: all selected tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/back/core/graphdb/neo4j/Neo4jReadOps.py \
  src/back/core/graphdb/neo4j/Neo4jStore.py \
  tests/units/graphdb/test_neo4j_roundtrip.py
git commit -m "fix(neo4j): align folded BFS metadata"
```

---

### Task 3: Restore the API contract and documentation

**Files:**
- Modify: `src/api/routers/digitaltwin.py`
- Modify: `src/api/routers/internal/dtwin.py`
- Modify: `src/agents/tools/graph_formatting.py`
- Modify: `src/mcp-server/server/formatting.py`
- Modify: `src/mcp-server/server/tools.py`
- Modify: `src/front/templates/partials/dtwin/_query_api.html`
- Modify: `docs/api.md`
- Test: `tests/units/dtwin/test_digitaltwin_api.py`
- Test: formatter tests located by `rg "format_find_response" tests`

**Interfaces:**
- Consumes: Task 1's complete result metadata.
- Produces: additive API response fields (`total`, `entity_count`, `has_more`)
  and exact-total agent/MCP formatting.

- [ ] **Step 1: Write failing contract and formatter tests**

Extend `test_find_response_defaults`:

```python
assert r.total == 0
assert r.entity_count == 0
assert r.has_more is False
```

For both formatter implementations, add data with `count=2`, `total=7`,
`entity_count=3`, and `has_more=True`; assert the output includes
`"2 of 7 triples"` and `"more exist"`.

- [ ] **Step 2: Run contract/formatter tests and verify RED**

Run the exact files found above plus:

```bash
uv run --frozen pytest -q tests/units/dtwin/test_digitaltwin_api.py
```

Expected: failures because exact fields are absent from the response and
formatters.

- [ ] **Step 3: Restore additive response fields and mappings**

Add to `FindResponse`:

```python
total: int = Field(0, description="Total distinct triples across all pages")
entity_count: int = Field(0, description="Entities included after alias expansion")
has_more: bool = Field(False, description="Whether more triples exist beyond this page")
```

Map all three fields in public and internal routes. Keep `%`-style logging and
log page count, exact total, and `has_more`.

Format exact totals while using `has_more` for the pagination hint:

```python
total = data.get("total", len(triples))
has_more = data.get("has_more", total > len(triples))
```

- [ ] **Step 4: Update docs and samples**

Update `_query_api.html` and both `/triples/find` sections in `docs/api.md` to
show all three fields, explain exact totals, and recommend `depth=1` for
type-wide scans and `search=` for deeper entity-scoped scans.

Keep the MCP docstring guidance and state that the backend reports exact
`total` plus `has_more`.

- [ ] **Step 5: Run focused tests and verify GREEN**

Run:

```bash
uv run --frozen pytest -q \
  tests/units/dtwin/test_digitaltwin_api.py \
  tests/units/agents/test_agent_dtwin_chat.py \
  tests/mcp
```

Expected: all selected tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/api/routers/digitaltwin.py \
  src/api/routers/internal/dtwin.py \
  src/agents/tools/graph_formatting.py \
  src/mcp-server/server/formatting.py \
  src/mcp-server/server/tools.py \
  src/front/templates/partials/dtwin/_query_api.html \
  docs/api.md tests
git commit -m "fix(api): preserve BFS pagination contract"
```

---

### Task 4: Rebase, document, verify, and update PR 182

**Files:**
- Modify: `changelogs/v0.8.1/<github-user>_2026-09-25.log`
- Modify: PR 182 title/body/base branch and add a technical comment.

**Interfaces:**
- Consumes: Tasks 1–3.
- Produces: reviewable PR 182 targeting `0.8.1`, with test evidence,
  attribution, benchmark provenance, and AI-eval waiver request.

- [ ] **Step 1: Integrate current `0.8.1` without rewriting Laurent's commit**

Fetch `origin/0.8.1` and merge it into the contributor branch. Resolve only
branch-integration conflicts; keep Laurent's original `3cf1ff98` author.

- [ ] **Step 2: Add the versioned changelog**

Use the GitHub username prefix and include context, numbered file changes,
modified files, and exact test results. Do not edit Laurent's existing
2026-09-24 entry.

- [ ] **Step 3: Run lint diagnostics on modified files**

Run repository lint/type checks applicable to the changed files and inspect
IDE diagnostics. Fix only introduced findings.

- [ ] **Step 4: Run the mandatory suite**

Run:

```bash
uv run --frozen pytest -q -m "not scenario"
```

Expected: zero failures. Record the exact summary in the changelog and PR.

- [ ] **Step 5: Commit and push**

Commit the changelog/test-result update, then push to
`LaurentPRAT-DB:perf/describe-entity-folded-bfs`. Confirm `3cf1ff98` still
shows Laurent Prat as author.

- [ ] **Step 6: Update GitHub without merging**

Retarget PR 182 to `0.8.1`. Rewrite its description with:

- root cause and final architecture;
- alias/API compatibility decisions;
- consistent benchmark provenance (do not mix the 34.5s single-run and 7.1s
  concurrency figures without labeling both);
- exact tests;
- links to the design and forthcoming `0.9.0` PR;
- `MLflow eval run: reviewer waiver requested` and the waiver rationale.

Post a concise technical comment explaining the review fixes and asking a
reviewer for the `.cursor/12` eval waiver. Do not approve, merge, auto-merge,
or queue the PR.
