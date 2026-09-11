# Explorer Single-Statement Expansion Design

## Context

Knowledge Graph Explorer currently expands selected entities one breadth-first
level at a time and then fetches triples in batches. On Databricks Apps this
can issue up to three neighbor queries plus twelve 250-entity fetch queries.
Lakehouse//RT reduces each query's execution latency, but repeated statement
submission and result transport still dominate the Expansion request.

## Goal

Execute bounded entity expansion and triple retrieval in one Lakehouse//RT SQL
statement while preserving Explorer's current request and response contract.

## Scope

- Optimize only the Delta/Lakehouse SQL path.
- Keep Neo4j, Lakebase, and other graph stores on the existing iterative path.
- Preserve forward and reverse relationship traversal, typed-neighbor
  filtering, depth, entity, triple, inferred-data, and statement-timeout limits.
- Preserve the `/dtwin/sync/filter` response shape and UI behavior.

Changing Explorer controls, graph rendering, preview search, table clustering,
and warehouse configuration is out of scope.

## Architecture

Add an optional graph-store capability named `expand_and_fetch_subgraph`.
`DeltaFlatStore` implements it with one generated SQL statement.
`DigitalTwin.filter_expand` uses the capability when present and otherwise
executes the current iterative algorithm unchanged.

The optimized method accepts selected URIs, depth, maximum entities, and
maximum triples. It returns triple rows plus expansion metadata. SQL execution
continues through `DeltaFlatStore.execute_query`, retaining the configured
graph statement timeout and Lakehouse//RT routing.

## SQL Shape

Python generates a non-recursive CTE chain because Explorer depth is capped at
three:

1. `level_0` contains the selected seed URIs.
2. Each subsequent `level_N_candidates` traverses both outgoing and incoming
   edges from `level_N-1`, excludes `rdf:type` and `rdfs:label`, accepts only
   HTTP(S) object URIs, and joins the candidate subject to an `rdf:type`
   assertion.
3. Each `level_N` excludes entities already present in earlier levels, applies
   `DISTINCT`, and limits candidates to `max_entities`.
4. `all_entities` unions all levels, deduplicates them, and applies the final
   `max_entities` bound.
5. The final query joins `all_entities` to the graph relation by subject and
   returns at most `max_triples + 1` rows. The extra row detects triple
   truncation without a separate count query.

The CTE chain has at most `(depth + 1) * max_entities` intermediate entity rows.
It avoids recursive `UNION ALL` path multiplication and Databricks' one-million
recursive-row failure mode.

The query projects the bounded entity count with each triple row. The Python
adapter removes metadata columns before returning results. Every selected or
expanded entity is typed and therefore has at least one subject triple, so a
successful non-empty entity set always carries metadata.

All table names use the existing validated SQL relation helper. Every URI uses
the existing SQL escaping helper. Depth and limits are validated integers and
rendered as literals.

## Bounds and Semantics

- Depth remains capped at three in Databricks Apps.
- Entities remain capped at 3,000 in Databricks Apps.
- Returned triples remain capped at 100,000.
- The optimized query uses the same inferred or asserted graph relation already
  selected by the route.
- A completed query reports `capped` when entity or triple limits truncate the
  result and reports `timeout_capped=false`.
- A statement timeout fails through the existing infrastructure error path.
  Unlike the iterative implementation, a single SQL statement cannot return a
  partially fetched graph after cancellation.

## Error Handling and Fallback

Capability dispatch, not exception recovery, selects the implementation.
Non-Delta stores use the existing iterative path. A Delta query error is
reported normally; it does not trigger a second, slower traversal after a
timeout or infrastructure failure.

Depth zero uses the same single statement with only `level_0` and the final
triple join. Empty selected URI lists remain rejected by the API before store
dispatch.

## Testing

Unit tests will verify:

- Delta capability dispatch and iterative fallback for stores without it.
- Generated depth-zero through depth-three CTE chains.
- Forward and reverse edge traversal.
- Exclusion of metadata predicates and previously visited levels.
- Typed-neighbor, entity, triple, escaping, and inferred-table contracts.
- Extra-row truncation detection and response payload parity.
- SQL errors propagate without retrying the iterative path.

The full non-scenario test suite remains the regression gate. A browser
benchmark on the same built graph will compare Expansion request duration
before and after, using identical seeds, depth, limits, and a warm RT warehouse.

## Acceptance Criteria

- Delta/Lakehouse expansion executes exactly one SQL statement.
- Other graph backends preserve current behavior.
- Existing Explorer results and metadata remain compatible.
- All configured safety bounds remain effective.
- Warm-warehouse Expansion duration improves for the representative
  `BIGCustomers_V1` search without console or network errors.
