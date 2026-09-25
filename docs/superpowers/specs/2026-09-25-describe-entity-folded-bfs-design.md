# Folded `describe_entity` BFS backports

## Goal

Deliver PR 182's server-side BFS pagination safely to the maintained `0.8.1`
and `0.9.0` branches without merging either pull request, while preserving
Laurent Prat as the original contributor and commit author.

## Pull-request structure

GitHub pull requests have one base branch. The work therefore uses two linked
pull requests:

1. Retarget PR 182, whose author remains `LaurentPRAT-DB`, from `master` to
   `0.8.1` and update its contributor branch through maintainer access.
2. Open a separate forward-port pull request to `0.9.0`. Its commits preserve
   Laurent's original authorship and co-author credit, while its body links
   PR 182 and explains the branch-specific adaptation.

Neither pull request is merged without explicit user confirmation.

## `0.8.1` design

Keep the folded server-side path introduced by PR 182: seed selection, graph
walk, triple selection, de-duplication, ordering, and pagination execute in the
database. The recursive fallback uses bidirectional edges joined through one
equality instead of the non-sargable subject/object `OR`.

The revised query must also preserve behavior and API compatibility:

- Include URI aliases that the prior `DigitalTwin.expand_uri_aliases` step
  added. A regression test compares alias-bearing results with the legacy
  semantics.
- Keep exact `total` and `entity_count` response fields for existing clients,
  add `has_more`, and derive all three in the folded server-side operation.
  Python must not materialize the complete triple neighborhood.
- Convert traversal depth to an integer before SQL interpolation.
- Keep Neo4j behavior aligned and covered.
- Update the interactive API sample and Sphinx/API documentation.
- Use one consistent benchmark statement, including query shape, warehouse
  size, and concurrency.

Because the pull request changes MCP guidance and agent-facing formatting, its
description must link an evaluation result or contain an explicit reviewer
waiver under `.cursor/12-ai-feature-lifecycle.mdc`.

## `0.9.0` adaptation

Do not copy the `0.8.1` SQL path unchanged. `0.9.0` has materialized
`_entity_search`, `_adj_out`, `_adj_in`, and `_props` companions.

Keep the existing companion-aware `bfs_traversal` and URI-alias expansion.
They already seed from `_entity_search`, walk `_adj_out`/`_adj_in`, and fall
back to SPO if companions disappear. Replacing that path would duplicate its
fallback logic and make alias expansion non-sargable.

Fold the remaining bottleneck only: fetch, de-duplicate, exactly count, order,
and paginate payload triples in `_props` where supported, with the existing SPO
missing-table fallback. This leaves only entity URIs—not the complete triple
neighborhood—materialized in Python and preserves exact `total`,
`entity_count`, and URI-alias behavior.

Tests cover companion traversal reuse, `_props` paging, SPO and missing-table
fallbacks, pagination metadata, URI aliases, and Neo4j parity.

## Validation

Use test-driven development for each behavior change. Run focused tests during
red/green cycles, then run:

```text
uv run --frozen pytest -q -m "not scenario"
```

on each target branch. Update the versioned changelog for each branch and
report exact results in the corresponding pull-request description/comment.

## Git and attribution

- Do not squash or rewrite Laurent's original commit author.
- Add review-fix commits separately.
- Preserve Laurent as PR 182's GitHub author by updating its existing head
  branch.
- Credit Laurent in the `0.9.0` forward-port PR and retain his authorship on
  cherry-picked or derived commits.
- Do not merge, auto-merge, or enable a merge queue.
