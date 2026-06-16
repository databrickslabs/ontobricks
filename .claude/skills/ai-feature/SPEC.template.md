# SPEC: <agent-name>

> Copy this template to `.planning/<slug>/SPEC.md` and fill every section.
> Required by `.cursor/12-ai-feature-lifecycle.mdc` (CI gate `.github/workflows/eval-gate.yml` checks for presence + non-empty `eval_dimensions`).
> Use one SPEC per agent. Material changes to an existing agent update the existing SPEC in-place; CI inspects the diff for an eval-dimensions update.

---

## 1. Purpose

<!-- One paragraph: what does this agent do, for whom, and why now? Avoid jargon. -->

## 2. Identity

| Field | Value |
|---|---|
| `agent_name` | `agent_<snake_case_name>` |
| `module_path` | `src/agents/<agent_name>/` |
| `model_endpoint` | `<Databricks Foundation Model API endpoint, e.g., databricks-meta-llama-3-3-70b>` |
| `temperature` | `0.0` (eval-deterministic; production may use higher) |
| `mlflow_experiment` | `/Shared/ontobricks/agents/<agent_name>` |

## 3. Tool surface

| Tool name | Input schema | Output type | Purpose |
|---|---|---|---|
| `<tool_name_1>` | `{"x": "string", "y": "int"}` | `dict` | <one-liner> |
| `<tool_name_2>` | … | … | … |

For each tool, paste the JSON schema below.

<details>
<summary><code>&lt;tool_name_1&gt;</code> schema</summary>

```json
{
  "type": "object",
  "properties": {
    "x": {"type": "string"},
    "y": {"type": "integer"}
  },
  "required": ["x"]
}
```
</details>

## 4. Success criteria

Three concrete examples (input → expected output shape) that an LLM consumer should see succeed:

1. **<scenario name>**
   - input: `…`
   - expected: `…`
2. **<scenario name>**
   - input: `…`
   - expected: `…`
3. **<scenario name>**
   - input: `…`
   - expected: `…`

## 5. Eval dimensions

The CI gate parses this table — keep it well-formed.

| Dimension | Metric | Threshold | Weight | Judge |
|---|---|---|---|---|
| `correctness` | <metric, e.g., schema_valid, exact_match, top-K accuracy> | `0.90` | `0.40` | <judge name from `tests/eval/judges/`> |
| `faithfulness` | <metric> | `0.85` | `0.25` | <judge> |
| `latency_p95` | seconds | `<= 5.0` | `0.15` | wall-clock |
| `cost_per_call` | USD | `<= 0.01` | `0.10` | MLflow usage record |
| `tool_selection` | exact-match on first tool called | `0.95` | `0.10` | rule-based |

**Aggregate threshold:** weighted sum ≥ <e.g., 0.85> to pass G2.

## 6. Failure modes

For each known failure mode, declare how it's detected and how it's mitigated.

| Symptom | Detection | Mitigation |
|---|---|---|
| <e.g., hallucinated URI> | Judge `faithfulness` < 0.6 on any example | Stricter system prompt; retry with smaller context |
| <e.g., wrong tool chosen> | `tool_selection` < 0.9 over 10-call window | Add example to `regression.jsonl`; tune tool descriptions |
| <e.g., latency spike> | P95 > 8s | Cache common queries; lower max_tokens |

## 7. Eval dataset

- **Baseline file:** `tests/eval/datasets/<agent_name>/baseline.jsonl` — ≥ 20 examples for new agents, ≥ 10 for changes.
- **Synthetic file:** `tests/eval/datasets/<agent_name>/synthetic.jsonl` — generated via `databricks-synthetic-data-generation`. Tag examples with `tags: ["synthetic"]`.
- **Regression file:** `tests/eval/datasets/<agent_name>/regression.jsonl` — every production failure we fix lands here. Never retired.

Dataset row shape:

```json
{"input": {...}, "expected": {"contains": [...], "schema": {...}, "constraints": [...]}, "tags": ["happy" | "ambiguous" | "adversarial" | "synthetic"]}
```

## 8. MLflow tracing

Every code path that calls Foundation Model API must be decorated:

```python
from agents.tracing import trace_agent, trace_llm, trace_tool

@trace_agent
def run(...): ...

@trace_llm
def _call_model(...): ...

@trace_tool
def my_tool_handler(...): ...
```

Trace assertions in unit tests use the `captured_traces` fixture (`tests/fixtures/mlflow.py`).

## 9. Plan reference

Implementation plan: `.planning/<slug>/PLAN.md` (produced by `superpowers:writing-plans`).

## 10. Sign-off

- [ ] Author has filled every section.
- [ ] Baseline eval run URI pasted into PR body.
- [ ] Aggregate threshold ≥ declared value in §5.
- [ ] Reviewer waiver (if applicable): _____
