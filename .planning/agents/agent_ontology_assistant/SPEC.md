# SPEC: agent_ontology_assistant

> **Scaffold status:** Skeleton only. Fill sections 4, 5, 6, 7 before merging any change to `src/agents/agent_ontology_assistant/`.

## 1. Purpose

`agent_ontology_assistant` is the conversational canvas modifier — given a user instruction in natural language ("add a Customer class with name + email"), it mutates the in-session ontology by emitting tool calls that add/remove/update classes, properties, and relationships.

## 2. Identity

| Field | Value |
|---|---|
| `agent_name` | `agent_ontology_assistant` |
| `module_path` | `src/agents/agent_ontology_assistant/` |
| `model_endpoint` | _TBD_ |
| `temperature` | `0.2` (some creativity in naming; lower for eval) |
| `mlflow_experiment` | `/Shared/ontobricks/agents/ontology_assistant` |

## 3. Tool surface

| Tool name | Input schema | Output type | Purpose |
|---|---|---|---|
| _TBD_ | _TBD_ | _TBD_ | add/remove/update classes, properties, relationships |

## 4. Success criteria

_TBD — three concrete chat-style examples._

## 5. Eval dimensions

_Proposed. Calibrate at M2.P4._

| Dimension | Metric | Threshold | Weight | Judge |
|---|---|---|---|---|
| `relevance` | LLM-judge: did the response address the user's instruction? | `0.85` | `0.30` | `tests/eval/judges/relevance_judge.py` (to build) |
| `groundedness` | LLM-judge: are the proposed changes consistent with the current ontology state? | `0.80` | `0.30` | `tests/eval/judges/grounded_judge.py` |
| `tool_selection` | exact-match on first tool called for canonical inputs | `0.90` | `0.20` | rule-based |
| `latency_p95` | seconds | `<= 8.0` | `0.10` | wall-clock |
| `cost_per_call` | USD | `<= 0.02` | `0.10` | MLflow usage |

**Aggregate threshold:** ≥ `0.82`.

## 6. Failure modes

| Symptom | Detection | Mitigation |
|---|---|---|
| Suggests a class that already exists | `groundedness` < 0.7 with `tags: ["duplicate"]` | system prompt: enumerate existing classes |
| Renames an unrelated class | `relevance` < 0.6 with `tags: ["scope-creep"]` | tighter tool input shape; reject implicit mutations |
| _TBD_ | _TBD_ | _TBD_ |

## 7. Eval dataset

- **Baseline:** `tests/eval/datasets/agent_ontology_assistant/baseline.jsonl` — ≥ 20 examples covering add/remove/update, ambiguous instructions, and adversarial inputs (e.g., "delete everything").
- **Regression:** `tests/eval/datasets/agent_ontology_assistant/regression.jsonl`.

## 8. MLflow tracing

`@trace_agent` on the run loop; `@trace_tool` on each handler.

## 9. Plan reference

`.planning/agent_ontology_assistant-spec/PLAN.md` (to create at M2.P4).

## 10. Sign-off

- [ ] Author has filled sections 4, 5, 6, 7.
- [ ] Baseline eval run URI pasted into PR body.
- [ ] Aggregate threshold ≥ declared value in §5.
- [ ] Reviewer waiver (if applicable): _____
