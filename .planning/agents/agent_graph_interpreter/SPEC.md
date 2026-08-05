# SPEC: agent_graph_interpreter

## 1. Purpose

Receives pre-computed graph centrality metrics (PageRank, betweenness, degree,
closeness, clustering) for a knowledge-graph domain and produces structured,
actionable insights for the end user.  The agent may look up additional entity
details via the `get_entity_details` tool to ground its observations before
writing its final answer.  Called by the Analytics page after the user clicks
"Interpret".

## 2. Identity

| Field | Value |
|---|---|
| `agent_name` | `agent_graph_interpreter` |
| `module_path` | `src/agents/agent_graph_interpreter/` |
| `model_endpoint` | domain-configured `llm_endpoint` (auto-discovered) |
| `temperature` | `0.1` |
| `mlflow_experiment` | `/Shared/ontobricks/agents/agent_graph_interpreter` |

## 3. Tool surface

| Tool name | Input schema | Output type | Purpose |
|---|---|---|---|
| `get_entity_details` | `{"uri": "string"}` | `string (JSON)` | Fetch attributes + direct relationships for a specific entity URI from the live triple-store |

<details>
<summary><code>get_entity_details</code> schema</summary>

```json
{
  "type": "object",
  "properties": {
    "uri": {
      "type": "string",
      "description": "Full URI of the entity to inspect, e.g. https://example.com/Customer/C001"
    }
  },
  "required": ["uri"]
}
```
</details>

## 4. Success criteria

1. **Hub entity identified** — Given a graph where one entity has PageRank 10×
   higher than the median, the agent names it in "Notable Entities" and explains
   why it is central.
2. **Isolated nodes flagged** — When `zero_metrics: ["betweenness","closeness","clustering"]`
   is set, the agent surfaces this in "Key Findings" and recommends data
   enrichment.
3. **Filtered analysis** — When `class_filter_applied: true` and `entity_type:
   "Customer"`, the agent scopes all observations to Customer entities and does
   not mention unrelated types.

## 5. Eval dimensions

| Dimension | Metric | Threshold | Weight | Judge |
|---|---|---|---|---|
| `section_completeness` | all 3 sections present in output | `1.00` | `0.30` | rule-based |
| `groundedness` | notable entity labels match top-pagerank input | `0.85` | `0.30` | rule-based |
| `tool_selection` | `get_entity_details` called for at least 1 top node when nodes > 0 | `0.70` | `0.20` | rule-based |
| `latency_p95` | seconds | `<= 20.0` | `0.10` | wall-clock |
| `cost_per_call` | USD | `<= 0.02` | `0.10` | MLflow usage record |

**Aggregate threshold:** weighted sum ≥ 0.82 to pass G2.

## 6. Failure modes

| Symptom | Detection | Mitigation |
|---|---|---|
| Hallucinated entity name | `groundedness` < 0.85 | System prompt strictly requires only labels from the input payload |
| Missing section | `section_completeness` = 0 | Stricter JSON-only system prompt; add failing case to `regression.jsonl` |
| No tool call | `tool_selection` = 0 | Prompt instructs agent to call `get_entity_details` for the top node |
| Latency spike | P95 > 20s | Lower `max_tokens`; reduce `MAX_ITERATIONS` |

## 7. Eval dataset

- **Baseline:** `tests/eval/datasets/agent_graph_interpreter/baseline.jsonl` — 20 examples
- **Regression:** `tests/eval/datasets/agent_graph_interpreter/regression.jsonl` — populated as failures occur

## 8. MLflow tracing

`engine.py` is decorated with `@trace_agent(name="graph_interpreter")`.
`call_serving_endpoint` carries `trace_name="graph_interpreter"`.
Tool handler decorated with `@trace_tool`.

## 9. Plan reference

Implementation plan: `.planning/agents/agent_graph_interpreter/PLAN.md`

## 10. Sign-off

- [x] Author has filled every section.
- [ ] Baseline eval run URI pasted into PR body.
- [ ] Aggregate threshold ≥ 0.82.
- [ ] Reviewer waiver (if applicable): _____
