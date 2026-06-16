<!--
PR Template — Cursor-Native Superpowers (CNS) methodology
See docs/PR_REVIEW_CHECKLIST.md for the reviewer's pass.
-->

## Summary

<!-- One paragraph: what changes, why now. Avoid restating the diff. -->

## Linked Issue / milestone

Closes #<issue-number>. Part of `M<n>.<phase>` in `.planning/ROADMAP.md`.

## Plan

`.planning/<slug>/PLAN.md`

<!-- For T4 (agent feature) work, link the SPEC too: -->
<!-- `.planning/<slug>/SPEC.md` -->

## Type of change

- [ ] feat — new feature
- [ ] fix — bug fix
- [ ] docs — documentation only
- [ ] refactor — no behaviour change
- [ ] test — tests only
- [ ] perf — perf improvement
- [ ] ci / build / chore — tooling

## Author checklist

- [ ] Conventional Commit PR title (`<type>(<scope>): <subject>`).
- [ ] `changelogs/<today>.log` updated with title + context + numbered changes + files + test result.
- [ ] Tests added or modified for every behaviour change.
- [ ] `uv run pytest tests/<scope>/` green locally.
- [ ] `pre-commit run --all-files` clean (or skipped hooks documented).
- [ ] If `src/agents/**` or any MLflow-traced LLM path changed:
  - [ ] `SPEC.md` present in `.planning/<slug>/`.
  - [ ] `tests/eval/datasets/<agent>/dataset.jsonl` has ≥20 examples (new) or ≥10 (change).
  - [ ] MLflow eval run URI in PR body below.
  - [ ] Judge score ≥ baseline + delta, or explicit waiver here.
- [ ] If `src/mcp-server/**` changed: `uv run pytest tests/mcp/ -m mcp` green.
- [ ] No `gsd-*` references re-introduced.

## MLflow eval run

<!-- For agent PRs only. Paste the run URI: -->
<!-- https://<workspace>.databricks.com/ml/experiments/<id>/runs/<run-id> -->

## Test plan

- [ ] `uv run pytest tests/ -m "not e2e and not property and not eval" --cov-fail-under=90`
- [ ] Per-package coverage thresholds (`scripts/check_coverage.py`) green for touched packages.
- [ ] If new MCP tool: `uv run pytest tests/mcp/integration/test_tool_schemas.py` includes it.

## Reviewer hint

See `docs/PR_REVIEW_CHECKLIST.md`. Numbered items map 1:1 to comments — `#3: missing OntoBricksError subclass for new condition` is more useful than "fix error handling".
