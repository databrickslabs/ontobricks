---
name: ai-feature
description: Use when the user adds, changes, or refactors an LLM agent under src/agents/ — or anything that goes through Foundation Model API or an MLflow-traced LLM call. Mandatory under CNS §3.5 and .cursor/12-ai-feature-lifecycle.mdc. Walks the SPEC → dataset → eval-harness → impl → re-eval sequence.
---

# OntoBricks ai-feature

Triggered automatically for any change under `src/agents/**`, `src/back/core/agents/**`, or an MCP tool that wraps an agent. The companion gate is `.cursor/12-ai-feature-lifecycle.mdc`. **This skill is the path of least resistance to passing that gate.** Skip this skill, and the CI gate (G2 — `.github/workflows/eval-gate.yml`) will reject the PR.

## Why this exists

OntoBricks ships **5 production agents** (`agent_owl_generator`, `agent_ontology_assistant`, `agent_auto_assignment`, `agent_auto_icon_assign`, `agent_dtwin_chat`) with **zero eval coverage today**. A prompt regression or a tool-handler bug ships green because the existing tests only mock the LLM. This skill enforces a SPEC-first + eval-gated lifecycle so quality is observable.

## Procedure

7 steps. The first 4 happen **before** code changes (the gate is on the artifact, not the runtime). Steps 5–7 are the implementation loop.

### 1. Brainstorm (≤ 10 min)

Invoke `superpowers:brainstorming`. Surface:

- **Purpose** in one sentence.
- **Target users**: who calls this agent? (LLM client via MCP? Internal UI? Another agent?)
- **In/out shape**: input format, output format.
- **Success criteria**: what does "right" look like in three examples?
- **Failure modes**: what's the worst output? How would a user notice?

Capture decisions at the top of `.planning/<slug>/PLAN.md`.

### 2. Fill SPEC.md

Copy `.claude/skills/ai-feature/SPEC.template.md` to `.planning/<slug>/SPEC.md`. Fill every section. **Do not skip the eval-dimensions table** — that's what the CI gate parses.

Required fields:

- `agent_name`, `module_path` (e.g., `src/agents/agent_fact_checker/`).
- `model_endpoint` — the Databricks Foundation Model API endpoint name.
- `tools[]` — tool name, JSON schema, intended LLM use.
- `eval_dimensions[]` — name, metric, threshold, weight.
- `failure_modes[]` — symptom, detection (which judge / trace tag), mitigation.

### 3. Build the eval dataset

Output: `.planning/<slug>/eval/dataset.jsonl` AND a mirror at `tests/eval/datasets/<agent_name>/baseline.jsonl`.

Minimum sizes:

| Change type | Min examples |
|---|---|
| New agent | 20 (15 hand-curated + 5 synthetic) |
| Material change (prompt, tool surface) | 10 |
| Hotfix / regression test | 3 (the failing cases) → `regression.jsonl` |

Row shape (JSON-Lines, one example per line):

```json
{"input": {...}, "expected": {"contains": [...], "schema": {...}, "constraints": [...]}, "tags": ["happy" | "ambiguous" | "adversarial"]}
```

Sources:

- **Hand-curated**: from product team intuition, real user requests, existing screenshots.
- **Synthetic**: use the `databricks-synthetic-data-generation` skill to bootstrap. Tag as `tags: ["synthetic"]` so reviewers know.
- **Regression**: every production-trace failure that you fix gets added here. Never deleted.

### 4. Wire the eval harness

Output: `tests/eval/run_<agent_name>.py`.

Pattern:

```python
import mlflow
from databricks_mlflow_evaluation import evaluate
# ...
result = evaluate(
    data="tests/eval/datasets/<agent_name>/baseline.jsonl",
    model=<your agent invocation callable>,
    judges=[...]  # see tests/eval/judges/
)
mlflow.log_metric("judge_score", result.aggregate_score)
```

Run a **baseline** before any code change. Record the MLflow run URI in the PR body (template `.github/PULL_REQUEST_TEMPLATE.md` has a slot).

### 5. Plan and implement

Invoke `superpowers:writing-plans` referencing SPEC.md. The plan lives at `.planning/<slug>/PLAN.md` and lists:

- Files to add / change under `src/agents/<agent_name>/` (follow `src/agents/engine_base.py` loop pattern).
- Tool definitions (`TOOL_DEFINITIONS`) and handlers (`TOOL_HANDLERS`).
- Tracing wiring: `@trace_agent`, `@trace_llm`, `@trace_tool` on every code path (`src/agents/tracing.py`).
- Unit tests with `httpx.MockTransport` (see `tests/fixtures/http.py`).

Run `superpowers:test-driven-development`. Red → Green → Refactor.

### 6. Re-run the eval

After the implementation lands locally, re-run `tests/eval/run_<agent_name>.py` against the same dataset. **Two outcomes:**

- **Judge score ≥ baseline + delta:** great, push the PR with the new MLflow run URI.
- **Judge score < threshold:** iterate. Look at failed examples (the judge writes per-example pass/fail to MLflow). If the failure is the eval being wrong, edit the dataset and document why in `.planning/<slug>/PLAN.md`.

Borderline cases get the `superpowers:requesting-code-review` invocation, then a reviewer's judgment via a waiver comment in the PR.

### 7. Ship

- `superpowers:verification-before-completion` — tests + eval green.
- `code-review` (project skill).
- `changelog` (project skill).
- Conventional Commit: `feat(agents): add agent_<name>` or `fix(agents): tune <agent> threshold`.
- PR; CI G1 + G2 both pass.

## What this skill **does not** do

- It doesn't deploy the agent. Use `deploy` once the PR merges.
- It doesn't run the LLM in CI. CI runs only the unit tests + the eval comparison against the **committed** dataset; the LLM call happens locally or against a configured serving endpoint, with the result captured in MLflow.
- It doesn't replace `superpowers:brainstorming` or `superpowers:writing-plans`. It **sequences** them.

## Cross-references

- `.cursor/12-ai-feature-lifecycle.mdc` — the rule that gates this work.
- §3.5 of `/Users/dermot.smyth/.claude/plans/ultrathink-perform-a-detailed-whimsical-token.md` — methodology context.
- `src/agents/engine_base.py` — the runtime pattern to follow.
- `tests/fixtures/http.py` — `agent_mock_transport` for unit tests.
- `tests/fixtures/mlflow.py` — `captured_traces` for span-shape assertions.
- `databricks-mlflow-evaluation` (plugin skill) — the harness.
- `databricks-synthetic-data-generation` (plugin skill) — dataset cold-start.
- `agent-evaluation` (plugin skill) — umbrella for the eval flow.
