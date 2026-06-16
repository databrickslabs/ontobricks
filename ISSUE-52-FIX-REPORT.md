# Bug Fix Report — Issue #52

**Title:** Expanded instances in knowledge graph view are grouped under
identifier instead of entity type
**Issue:** [databrickslabs/ontobricks#52](https://github.com/databrickslabs/ontobricks/issues/52)
**Reported by:** @ulsmo · **Affected version:** 0.4.0 · **Fixed in:** 0.5.0
**Date:** 2026-06-12

---

## 1. Summary

When using the knowledge graph viewer's **right-click → "Expand neighbours"**
action, the newly added nodes showed up in the *View* pane as brand-new entity
types named after their **identifiers** (with freshly randomised colours)
instead of being folded into the entity type they actually belong to. This
made type-based filtering and the colour coding effectively unusable after any
expansion.

The root cause was a **backend filter dropping `rdf:type` triples**, not a
front-end rendering bug. A one-line condition in the neighbour-expansion
endpoint is responsible.

---

## 2. Reproduction

1. Run a query to load a graph in the Digital Twin viewer.
2. Right-click any node → **Expand neighbours**.
3. Observe the *View* pane: the expanded nodes appear under new groups keyed by
   their identifier rather than under their existing entity type, each with a
   new random colour.

---

## 3. Root Cause Analysis

The expansion calls `GET /dtwin/neighbors`
(`src/api/routers/internal/dtwin.py`). The endpoint BFS-traverses from the seed
URI, collects the set of `visited` **instance** URIs, then fetches every triple
whose subject is in that set and returns the induced subgraph.

To avoid rendering edges that point at nodes outside the returned subgraph
("ghost endpoints"), it discarded any triple whose object is a URI **not** in
`visited`:

```python
is_uri_obj = o.startswith("http://") or o.startswith("https://")
if is_uri_obj and o not in visited:
    continue          # <-- also drops rdf:type triples
```

The problem: an `rdf:type` triple's object is the **class** URI
(e.g. `http://…/onto/Person`). A class is part of the ontology schema, never an
*instance*, so it is **never** in the `visited` instance set. As a result every
`rdf:type` triple for the freshly expanded nodes was silently filtered out.

Downstream, the front-end `buildGraph()` (`query-d3graph.js`) assigns each
node's entity type from its `rdf:type` triple. With that triple missing it
falls back to `inferTypeFromUri()`, which derives a pseudo-type from the URI's
local part — i.e. the node's **identifier**. That pseudo-type becomes a new
group with its own colour, exactly matching the reported symptom.

The originally-queried nodes were unaffected because the initial query result
*does* include their `rdf:type` triples.

---

## 4. The Fix

Preserve `rdf:type` triples through neighbour filtering even when the class URI
is not part of the visited subgraph. The inline loop was extracted into a small,
unit-testable helper.

`src/api/routers/internal/dtwin.py`:

```python
_RDF_TYPE_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


def _is_type_predicate(predicate: str) -> bool:
    if not predicate:
        return False
    return (
        predicate == _RDF_TYPE_URI
        or predicate.endswith("#type")
        or predicate.endswith("/type")
    )


def _filter_neighbor_triples(rows, visited, limit):
    triples, seen = [], set()
    for r in rows:
        s, p, o = r.get("subject", ""), r.get("predicate", ""), r.get("object", "")
        key = (s, p, o)
        if key in seen:
            continue
        is_uri_obj = o.startswith("http://") or o.startswith("https://")
        if is_uri_obj and o not in visited and not _is_type_predicate(p):
            continue            # keep literals, in-subgraph URIs, and rdf:type
        seen.add(key)
        triples.append({"subject": s, "predicate": p, "object": o})
        if len(triples) >= limit:
            break
    return triples
```

The endpoint now simply calls `triples = _filter_neighbor_triples(rows, visited, limit)`.

**Why this is safe:** keeping type triples does **not** introduce ghost class
nodes — `buildGraph()`'s pre-pass already detects type/class URIs and excludes
them from the entity set, using them only to assign types. The type-predicate
detection (`#type` / `/type` / full RDF URI) mirrors the logic the front-end
already uses, so backend and front-end stay consistent.

---

## 5. Tests

Added `TestNeighborTripleFilter` in `tests/units/api/test_routes.py` (6 cases):

| Test | Asserts |
|------|---------|
| `test_keeps_rdf_type_triple_even_when_class_uri_not_visited` | The #52 regression — type triple survives |
| `test_keeps_short_type_predicate` | `#type` suffix predicates also survive |
| `test_drops_non_type_uri_object_outside_visited` | Ghost-endpoint guard still works |
| `test_keeps_uri_object_within_visited` | In-subgraph edges preserved |
| `test_keeps_literal_object` | Literal attributes preserved |
| `test_dedups_and_respects_limit` | De-dup + `limit` cap intact |

**Result:** `python -m pytest tests/units` → **2328 passed, 11 skipped**.
(`tests/property/*` is skipped because the optional `hypothesis` package is not
installed in this environment — unrelated to this change.)

---

## 6. Files Changed

- `src/api/routers/internal/dtwin.py` — added `_RDF_TYPE_URI`,
  `_is_type_predicate()`, `_filter_neighbor_triples()`; endpoint now delegates
  filtering to the helper.
- `tests/units/api/test_routes.py` — added `TestNeighborTripleFilter`.
- `changelogs/v0.5.0/benoitcayladbx_2026-06-12.log` — changelog entry.
