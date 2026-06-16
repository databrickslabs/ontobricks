# SPEC: agent_auto_icon_assign

> **Scaffold status:** Skeleton only. Fill sections 4, 5, 6, 7 before merging any change to `src/agents/agent_auto_icon_assign/`.
>
> **Recommended first agent to fully spec** — smallest surface, deterministic output (icon ID classification), trivial exact-match judge.

## 1. Purpose

`agent_auto_icon_assign` is the icon-only variant of `agent_auto_assignment`. Given a class IRI + label + optional comment, it returns the best-match icon ID from the OntoBricks icon library. Stateless, single-step.

## 2. Identity

| Field | Value |
|---|---|
| `agent_name` | `agent_auto_icon_assign` |
| `module_path` | `src/agents/agent_auto_icon_assign/` |
| `model_endpoint` | _TBD_ |
| `temperature` | `0.0` (classification) |
| `mlflow_experiment` | `/Shared/ontobricks/agents/auto_icon_assign` |

## 3. Tool surface

| Tool name | Input schema | Output type | Purpose |
|---|---|---|---|
| _TBD — likely a single icon-library lookup_ | _TBD_ | `string (icon_id)` | classification |

## 4. Success criteria

_TBD — three example class-name → icon mappings the team agrees on._

## 5. Eval dimensions

Simplest of the 5. **Top-K accuracy** is the dominant signal.

| Dimension | Metric | Threshold | Weight | Judge |
|---|---|---|---|---|
| `top1_accuracy` | exact match on icon ID | `0.85` | `0.50` | rule-based |
| `top3_accuracy` | gold ID in top-3 returned | `0.95` | `0.25` | rule-based |
| `latency_p95` | seconds | `<= 2.5` | `0.10` | wall-clock |
| `cost_per_call` | USD | `<= 0.005` | `0.15` | MLflow usage |

**Aggregate threshold:** ≥ `0.85`.

## 6. Failure modes

| Symptom | Detection | Mitigation |
|---|---|---|
| Wrong icon for "ID-column" classes | regression — happens after a prompt edit; add 3 examples to `regression.jsonl` | tighter heuristic in the prompt; explicit examples in system message |
| Picks an icon that isn't in the library | sanity check post-LLM: `icon_id in library_set` | wrap the response in a validator; retry on invalid |
| _TBD_ | _TBD_ | _TBD_ |

## 7. Eval dataset

- **Baseline:** `tests/eval/datasets/agent_auto_icon_assign/baseline.jsonl` — ≥ 20 examples covering common entity types (Customer, Order, Product, …) and rare/ambiguous ones (Address, LineItem, Notification).
- **Synthetic:** Names drawn from the W3C-style sample ontologies.
- **Regression:** `tests/eval/datasets/agent_auto_icon_assign/regression.jsonl` — start with the production failure described in CNS §4.6 worked example T6 (wrong icon for ~12% of UC tables since yesterday's deploy).

## 8. MLflow tracing

`@trace_agent` on entry; `@trace_tool` on the icon-library lookup.

## 9. Plan reference

`.planning/agent_auto_icon_assign-spec/PLAN.md` (to create at M2.P4).

## 10. Sign-off

- [ ] Author has filled sections 4, 5, 6, 7.
- [ ] Baseline eval run URI pasted into PR body.
- [ ] Aggregate threshold ≥ declared value in §5.
- [ ] Reviewer waiver (if applicable): _____
