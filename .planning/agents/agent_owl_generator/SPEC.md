# SPEC: agent_owl_generator

> **Scaffold status:** Skeleton only. Fill sections 4, 5, 6, 7 before merging any change to `src/agents/agent_owl_generator/`.
> Required by `.cursor/12-ai-feature-lifecycle.mdc`.

## 1. Purpose

`agent_owl_generator` auto-designs an OWL ontology from UC metadata. Given a catalog/schema/table set, it proposes classes, properties, and relationships in a single LLM-driven step, returning a structure that conforms to the OntoBricks ontology JSON format consumed by `back/objects/ontology/OntologyService`.

## 2. Identity

| Field | Value |
|---|---|
| `agent_name` | `agent_owl_generator` |
| `module_path` | `src/agents/agent_owl_generator/` |
| `model_endpoint` | _TBD — currently configured per workspace_ |
| `temperature` | `0.0` (for eval) |
| `mlflow_experiment` | `/Shared/ontobricks/agents/owl_generator` |

## 3. Tool surface

(Existing tools — see `src/agents/agent_owl_generator/` and `agents/tools/`. To be enumerated when SPEC is filled.)

| Tool name | Input schema | Output type | Purpose |
|---|---|---|---|
| _TBD_ | _TBD_ | _TBD_ | _TBD_ |

## 4. Success criteria

_TBD — three concrete examples._

## 5. Eval dimensions

_To fill in M2.P4. Below is the proposed table; calibrate after baseline run._

| Dimension | Metric | Threshold | Weight | Judge |
|---|---|---|---|---|
| `schema_validity` | RDFLib `parse(serialize())` succeeds | `0.95` | `0.30` | rule-based |
| `class_coverage` | proportion of input tables mapped to a class | `0.80` | `0.20` | rule-based |
| `property_quality` | LLM-judge on property naming + domain/range correctness | `0.80` | `0.25` | `tests/eval/judges/owl_property_judge.py` (to build) |
| `latency_p95` | seconds | `<= 30.0` | `0.10` | wall-clock |
| `cost_per_call` | USD | `<= 0.05` | `0.15` | MLflow usage |

**Aggregate threshold:** ≥ `0.82` to pass G2 (proposed).

## 6. Failure modes

_TBD._

| Symptom | Detection | Mitigation |
|---|---|---|
| _TBD_ | _TBD_ | _TBD_ |

## 7. Eval dataset

- **Baseline:** `tests/eval/datasets/agent_owl_generator/baseline.jsonl` (not built; needs ≥ 20 examples; mix of single-table, multi-table, and degenerate inputs).
- **Synthetic:** Use `databricks-synthetic-data-generation` against UC sample data.
- **Regression:** `tests/eval/datasets/agent_owl_generator/regression.jsonl` (empty until first production failure).

## 8. MLflow tracing

Existing: `@trace_agent` on the entry point in `src/agents/agent_owl_generator/`. Verify `@trace_tool` is on each tool handler.

## 9. Plan reference

`.planning/agent_owl_generator-spec/PLAN.md` (to create when the team picks this up — M2.P4).

## 10. Sign-off

- [ ] Author has filled sections 4, 5, 6, 7.
- [ ] Baseline eval run URI pasted into PR body.
- [ ] Aggregate threshold ≥ declared value in §5.
- [ ] Reviewer waiver (if applicable): _____
