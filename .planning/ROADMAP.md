# OntoBricks Roadmap — Cursor-Native Superpowers (CNS)

> Single source of truth for active and upcoming workstreams. Mirrors GitHub Milestones.
> See `/Users/dermot.smyth/.claude/plans/ultrathink-perform-a-detailed-whimsical-token.md` for the full methodology + analysis.

## How to use this file

- **Each H2** is a GitHub Milestone (`M1: Foundation`, `M2: AI Discipline`, etc.).
- **Each `- [ ]` bullet** is a GitHub Issue + a `.planning/<slug>/PLAN.md` directory in this repo.
- **`<slug>`** is the lowercase-hyphen form of the Issue title (e.g., `ai-feature-skill`, `digitaltwin-split-p1`).
- Update this file in the same PR that closes an Issue. `code-review` skill checks for the update.

## Cross-references

- The full methodology lives in the plan file (above).
- Per-task atomic plans live in `.planning/<slug>/PLAN.md` (produced by `superpowers:writing-plans`).
- AI-feature work additionally has `.planning/<slug>/SPEC.md` (gated by `.cursor/12-ai-feature-lifecycle.mdc`).
- Audit trail is `changelogs/<YYYY-MM-DD>.log`.

---

## M1 — Foundation (close missing artifacts)

Theme: stand up the substrate that the rest of CNS depends on. None of these change product behaviour; all close gaps from §2 of the plan.

- [x] **M1.P1** Bootstrap `.planning/ROADMAP.md` (this file). *Slug: `bootstrap-roadmap`.*
- [x] **M1.P2** Recreate `src/.coding_rules.md` (gap #1). *Slug: `coding-rules-bootstrap`.*
- [x] **M1.P3** Create `changelogs/` + seed entry (gap #2). *Landed 2026-05-12.*
- [x] **M1.P4** `.pre-commit-config.yaml` (gap #8). *Slug: `pre-commit-hooks`.*
- [x] **M1.P5** PR review checklist + PR template (gap #12). *Slug: `pr-review-checklist`.*
- [x] **M1.P6** Conventional Commits + commitlint (gap #10). *Slug: `commitlint`.*
- [x] **M1.P7** Worktree multi-agent guide (gap #13). *Slug: `worktree-guide`.*

## M2 — AI Discipline (critical path) — closes gap #4, #5

Theme: the rule + skill + eval gate that prevents 5-agents-zero-eval regressions from shipping. Critical-path because every later refactor of agent-touching code needs the safety net.

- [x] **M2.P1** `.cursor/12-ai-feature-lifecycle.mdc` — the rule (gap #4 enforcement). *Landed 2026-05-14 (45c60aa).*
- [x] **M2.P2** `.claude/skills/ai-feature/SKILL.md` — the orchestrator. *Landed 2026-05-14 (45c60aa).*
- [x] **M2.P3** SPEC.md template + 5 retroactive SPEC scaffolds. *Landed 2026-05-14 (45c60aa).*
- [ ] **M2.P4** Baseline eval datasets per agent (≥20 examples each). **Partial — 3-example seeds landed for all 5 agents on 2026-05-14 (ddf07c4); team must expand each to ≥20.** *Slug: `agent-eval-datasets`.*
- [x] **M2.P5** `.github/workflows/eval-gate.yml` — G2 CI gate. *Landed 2026-05-14 (45c60aa); calibration mode for 2 weeks.*
- [ ] **M2.P6** MCP integration test harness — full 40+ tool coverage. **Partial — schema tests + 9 happy-paths + 9 parametrized landed on 2026-05-14 (round-4); remaining tools per-tool coverage open.** *Slug: `mcp-harness-full`.*
- [ ] **M2.P7** Eval drift cron + `mcp-ontobricks` smoke probe. **Scaffolded on 2026-05-14 (round-4) — `.github/workflows/eval-drift.yml`; gated behind `ONTOBRICKS_EVAL_RUNNERS_READY` and `ONTOBRICKS_INT_MCP_REACHABLE` repo variables; flip on once M2.P4 lands real runners.** *Slug: `eval-drift-cron`.*

## M3 — Quality enforcement in CI

Theme: turn the discipline into automation so it can't decay.

- [x] **M3.P1** ruff + mypy in CI with baseline. *Landed 2026-05-14 (ddf07c4) — 160-error mypy baseline, `check-mypy-diff.py` gate on new errors only; ruff advisory on changed-files-only until ~3000 findings are burned down.*
- [x] **M3.P2** Changelog presence gate (gap #9). *Landed 2026-05-14 (45c60aa).*
- [ ] **M3.P3** E2E in nightly CI. **Scaffolded — `nightly.yml` exists from T-M0.P6; needs `ONTOBRICKS_INT_*` GitHub secrets + staging DAB target to flip on.** *Slug: `nightly-e2e`.*

## M4 — Monolith splits (architecture cleanup)

**Hard precondition:** M2 must be done (eval datasets + G2 gate). Refactoring agent-touching code without eval is reckless.

- [ ] **M4.P1** Decompose `DigitalTwin.py` (3525 LOC). *Slug: `digitaltwin-split`.*
- [ ] **M4.P2** Decompose `SparqlTranslator.py` (2407 LOC). *Slug: `sparql-translator-split`.*
- [ ] **M4.P3** Decompose `SettingsService.py` (2148 LOC). *Slug: `settings-service-split`.*

## T-M — Testing milestones (from Section 9)

Built on top of M1–M4. Each phase fills coverage gaps surfaced in §9.0.

- [x] **T-M0** Test foundations (P1–P6). *Landed 2026-05-12 (1db8647).*
- [x] **T-M1.P1** SHACL parser/generator/service unit tests. *Landed 2026-05-12 (1db8647) — 25 tests covering parser, generator, service. Expandable to ~80 with more constraint variants.*
- [x] **T-M1.P2** SparqlTranslator direct unit tests. *Partial — 21 tests landed in round-5 covering the single public method (`translate_sparql_to_spark`): return-shape, single-variable SELECT, LIMIT propagation, multi-variable SELECT, entity-mapping respected (catalog/schema/table in output), SQL safety (no statement terminator, no IRI injection), missing-mapping/malformed-input raising `ValidationError`, non-SELECT rejection. Per-visitor expansion (~100 more tests for BGP, FILTER, OPTIONAL, UNION, GROUP BY, ORDER BY, property paths) deferred — file is 2407 LOC, one public entry point.*
- [x] **T-M1.P3** DigitalTwin direct unit tests. *Partial — 25 tests landed on 2026-05-14 (round-4) covering pure-function surface (is_datatype_range, extract_local_id, build_quality_sql, diagnose_view_error, compute_dtwin_indicator). Behaviour-rich paths (build_task, materialize) deferred to T-M2 + M4 split.*
- [x] **T-M1.P4** `src/back/core/logging/` unit tests. *Landed 2026-05-14 (ddf07c4) — 17 tests.*
- [x] **T-M1.P5** `src/back/core/errors/` direct unit tests. *Landed 2026-05-14 (ddf07c4) — 33 tests.*
- [ ] **T-M2** Integration tier. **Partial — `tests/contract/test_openapi_contract.py` (10 tests, ddf07c4) + `tests/contract/test_graphql_schema.py` (10 tests, round-4) cover P4 + P5. Remaining: P1 Delta sync, P2 Lakebase via testcontainers, P3 R2RML complex joins, P6 error propagation.**
- [ ] **T-M3 (full)** MCP integration — all 40+ tools × 4 tests each. **Partial — 25 tests landed (10 schema + 5 smoke + 9 parametrized + 6 more smoke); remaining tools per-tool coverage open.** *Slug: `mcp-harness-full` (shared with M2.P6).*
- [ ] **T-M4** Agent eval harness — drives M2.P4 + thresholds. *Slug: `agent-evals`. **OPEN — needs the runners (`tests/eval/run_<agent>.py`) + judges (`tests/eval/judges/`); datasets seed landed (ddf07c4).***
- [ ] **T-M5** E2E nightly user journeys (7 marquee flows). *Slug: `e2e-journeys`. **`nightly.yml` exists; need to write the Playwright scenarios.***
- [x] **T-M6** Hypothesis property tests for W3C translators. *Partial — OWL roundtrip (3 tests, ddf07c4) + SHACL conformance (4 tests, round-4) + R2RML idempotency (5 tests, round-4) all landed. Remaining: SPARQL property tests, R2RML W3C conformance suite import.*

---

## Out-of-scope (deferred)

- Mutation testing (mutmut) — needs CI budget; revisit after M4.
- Performance benchmarks (pytest-benchmark) — useful but not blocking.
- Real-Databricks `external` tier in CI (https://fevm-ontobricks-int.cloud.databricks.com) — workflow scaffolded, needs `ONTOBRICKS_INT_*` GitHub secrets to enable.

## Conventions

- **One PR per phase.** No mega-PRs spanning milestones.
- **Branch name = slug.** `git checkout -b ai-feature-skill` for `M2.P2`.
- **PR title prefix = milestone.** `feat(M2.P2): ai-feature skill orchestrator`. Enforced by commitlint (M1.P6).
- **PR body links the Issue + plan file.** `.planning/<slug>/PLAN.md` is the resumption substrate.
- **Closing the loop:** when the PR merges, mark the bullet `[x]` and add a one-line note (`Landed YYYY-MM-DD on <branch-or-PR>`).

## Last updated

2026-05-14 (round 4) — added T-M2.P5 GraphQL contract, T-M6 SHACL+R2RML property tests, T-M3 expansion smoke tests, T-M1.P3 DigitalTwin unit-test sample, M2.P7 eval-drift workflow scaffold. Status table reflects which phases are fully done vs partial vs open.
