# SPEC: agent_auto_assignment

> **Scaffold status:** Skeleton only. Fill sections 4, 5, 6, 7 before merging any change to `src/agents/agent_auto_assignment/`.

## 1. Purpose

`agent_auto_assignment` assigns icons and visual layout coordinates to ontology entities. Given a list of class IRIs and (optionally) a layout area, it produces `{class_iri: {icon, x, y}}` for the OntoViz canvas.

## 2. Identity

| Field | Value |
|---|---|
| `agent_name` | `agent_auto_assignment` |
| `module_path` | `src/agents/agent_auto_assignment/` |
| `model_endpoint` | _TBD_ |
| `temperature` | `0.0` (deterministic ground truth) |
| `mlflow_experiment` | `/Shared/ontobricks/agents/auto_assignment` |

## 3. Tool surface

| Tool name | Input schema | Output type | Purpose |
|---|---|---|---|
| _TBD_ | _TBD_ | _TBD_ | Icon library lookup + layout placement |

## 4. Success criteria

_TBD._

## 5. Eval dimensions

| Dimension | Metric | Threshold | Weight | Judge |
|---|---|---|---|---|
| `icon_exact_match` | exact icon ID match against gold-standard | `0.92` | `0.40` | rule-based |
| `layout_no_overlap` | proportion of pairwise non-overlapping bounding boxes | `0.98` | `0.20` | rule-based |
| `f1_class_coverage` | F1 over assignments vs gold | `0.95` | `0.20` | rule-based |
| `latency_p95` | seconds | `<= 4.0` | `0.10` | wall-clock |
| `cost_per_call` | USD | `<= 0.01` | `0.10` | MLflow usage |

**Aggregate threshold:** ≥ `0.90`.

## 6. Failure modes

| Symptom | Detection | Mitigation |
|---|---|---|
| Assigns same icon to two semantically different classes | `icon_exact_match` drops below 0.85 on a tag-specific subset | tighter system prompt; add tag-stratified examples |
| Overlapping bounding boxes | `layout_no_overlap` < 0.95 | post-hoc layout adjustment in code, not in the prompt |
| _TBD_ | _TBD_ | _TBD_ |

## 7. Eval dataset

- **Baseline:** `tests/eval/datasets/agent_auto_assignment/baseline.jsonl` — ≥ 20 examples covering small, medium, and large ontologies.
- **Regression:** `tests/eval/datasets/agent_auto_assignment/regression.jsonl`.

## 8. MLflow tracing

`@trace_agent`, `@trace_tool`.

## 9. Plan reference

`.planning/agent_auto_assignment-spec/PLAN.md` (to create at M2.P4).

## 10. Sign-off

- [ ] Author has filled sections 4, 5, 6, 7.
- [ ] Baseline eval run URI pasted into PR body.
- [ ] Aggregate threshold ≥ declared value in §5.
- [ ] Reviewer waiver (if applicable): _____
