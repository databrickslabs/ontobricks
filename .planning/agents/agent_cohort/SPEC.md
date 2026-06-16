# SPEC: agent_cohort

> **Scaffold status:** Skeleton only. Fill sections 4, 5, 6, 7 before merging any change to `src/agents/agent_cohort/`.
> Required by `.cursor/12-ai-feature-lifecycle.mdc`.
>
> **Origin:** This agent landed via upstream merge on 2026-05-26 (commit `229012a`). The retroactive SPEC scaffold below was added under the CNS M2.P3 pattern so the G2 CI gate recognises the agent.

## 1. Purpose

`agent_cohort` is the Cohort Discovery assistant. Given a natural-language prompt and the active session's ontology + graph, the LLM iteratively introspects the ontology via read-only tools, builds a `CohortRule` candidate, validates it with `propose_rule`, optionally invokes `dry_run` once, and returns the final explanation. The proposed rule is captured in `ToolContext.metadata['proposed_rule']` so the route can return it as structured JSON to hydrate the cohort form.

The agent is **one-shot** — a single natural-language turn yields a single proposed rule. There is no multi-turn dialogue today; the route resets context per call.

## 2. Identity

| Field | Value |
|---|---|
| `agent_name` | `agent_cohort` |
| `module_path` | `src/agents/agent_cohort/` |
| `model_endpoint` | _TBD — currently configured per workspace (see `call_serving_endpoint`)_ |
| `temperature` | `0.0` (for eval; tool-driven workflow benefits from determinism) |
| `mlflow_experiment` | `/Shared/ontobricks/agents/cohort` |
| `trace_name` | `cohort_agent` (see `_TRACE_NAME` in `engine.py`) |
| `max_iterations` | `10` |
| `llm_timeout_seconds` | `120` |

## 3. Tool surface

Five read-only ontology-introspection tools plus two rule-handling tools. All defined in `src/agents/agent_cohort/tools.py`.

| Tool name | Input schema | Output type | Purpose |
|---|---|---|---|
| `list_classes` | `{}` | JSON: array of `{uri, label, count?}` | Enumerate classes in the active ontology. |
| `list_properties_of` | `{class_uri: str}` | JSON: array of `{uri, label, range, domain}` | Properties whose domain matches the class. |
| `count_class_members` | `{class_uri: str}` | JSON: `{class_uri, count}` | Member cardinality for sizing decisions. |
| `sample_values_of` | `{property_uri: str, limit?: int}` | JSON: `{property_uri, samples: list[str]}` | Inspect literal/IRI shape on a property. |
| `propose_rule` | `{rule: CohortRuleDict}` | JSON: `{ok, errors?}` | Validate a candidate rule against the ontology. Sets `ctx.metadata['proposed_rule']` on success. |
| `dry_run` | `{rule: CohortRuleDict}` | JSON: `{cohorts: [...], stats: {...}}` | Preview the cohorts the rule would produce. At most one call per agent turn (the agent prompt enforces this). |

## 4. Success criteria

_TBD — three concrete prompt → rule examples covering (a) single-linkage simple rule, (b) multi-property compatibility, (c) ambiguous prompt requiring clarification._

## 5. Eval dimensions

_To fill in M2.P4. Below is the proposed table; calibrate after baseline run._

| Dimension | Metric | Threshold | Weight | Judge |
|---|---|---|---|---|
| `rule_validity` | `propose_rule` returns `ok: true` on the agent's final proposal | `0.95` | `0.30` | rule-based (run `propose_rule` post-hoc) |
| `tool_selection` | exact-match on first tool invoked for canonical inputs | `0.85` | `0.15` | rule-based |
| `cohort_quality` | LLM-judge: does the proposed rule semantically match the prompt? | `0.80` | `0.25` | `tests/eval/judges/cohort_judge.py` (to build) |
| `latency_p95` | seconds | `<= 30.0` | `0.10` | wall-clock |
| `cost_per_call` | USD | `<= 0.05` | `0.10` | MLflow usage |
| `dry_run_calls` | proportion of turns that invoke `dry_run` at most once | `0.98` | `0.10` | rule-based |

**Aggregate threshold:** ≥ `0.82` to pass G2 (proposed).

## 6. Failure modes

_TBD._

| Symptom | Detection | Mitigation |
|---|---|---|
| Tool argument is malformed JSON (LLM hallucination) | `dispatch_tool` returns an error string | `engine.py` retry loop falls back; flagged if `error_rate > 0.05` in eval |
| Proposed rule references URIs not present in the loaded ontology | `propose_rule` returns `ok: false` with a class/property mismatch error | system prompt: only use URIs returned from `list_classes` / `list_properties_of` |
| Agent never calls `propose_rule` and hits `MAX_ITERATIONS` | `iterations == MAX_ITERATIONS` and `proposed_rule is None` | dataset includes adversarial "too vague" prompts; system prompt instructs the agent to ask for clarification rather than guess |
| Drift after prompt edit | nightly drift cron (M2.P7) opens a JIRA tagged `eval-drift` | revert + add regression examples |

## 7. Eval dataset

- **Baseline:** `tests/eval/datasets/agent_cohort/baseline.jsonl` (seed of 3 examples landed; needs ≥ 20 — mix of single-linkage, multi-property, adversarial vague prompts).
- **Synthetic:** Use `databricks-synthetic-data-generation` against a sample ontology (e.g., the `benoit_cayla.ontobricks.default_triplestore` schema).
- **Regression:** `tests/eval/datasets/agent_cohort/regression.jsonl` (empty until first production failure).

## 8. MLflow tracing

`@trace_agent("cohort_agent")` on `run_cohort_agent`. The shared `call_serving_endpoint` + `dispatch_tool` helpers add `@trace_llm` and `@trace_tool` spans for free. Verify `proposed_rule` appears as a span attribute on the final agent span.

## 9. Plan reference

`.planning/agent_cohort-spec/PLAN.md` (to create when the team picks this up — M2.P4).

## 10. Sign-off

- [ ] Author has filled sections 4, 5, 6, 7.
- [ ] Baseline eval run URI pasted into PR body.
- [ ] Aggregate threshold ≥ declared value in §5.
- [ ] Reviewer waiver (if applicable): _____
