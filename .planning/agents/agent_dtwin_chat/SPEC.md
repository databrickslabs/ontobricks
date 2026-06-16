# SPEC: agent_dtwin_chat

> **Scaffold status:** Skeleton only. Fill sections 4, 5, 6, 7 before merging any change to `src/agents/agent_dtwin_chat/`.
>
> **Hardest of the 5 to spec** — RAG-style, multi-turn, output is free-form text grounded in the digital twin triplestore.

## 1. Purpose

`agent_dtwin_chat` is the conversational interface to a materialised digital twin. Given a natural-language question about an ontology + its triple store, it picks the right tool calls (search entities, find triples, traverse the graph, translate SPARQL to SQL) and produces a grounded answer.

## 2. Identity

| Field | Value |
|---|---|
| `agent_name` | `agent_dtwin_chat` |
| `module_path` | `src/agents/agent_dtwin_chat/` |
| `model_endpoint` | _TBD_ |
| `temperature` | `0.1` (small for grounded answers; lower for eval) |
| `mlflow_experiment` | `/Shared/ontobricks/agents/dtwin_chat` |

## 3. Tool surface

| Tool name | Input schema | Output type | Purpose |
|---|---|---|---|
| _TBD — see `src/agents/agent_dtwin_chat/` and `agents/tools/`_ | _TBD_ | _TBD_ | entity search, relationship traversal, SPARQL→SQL |

## 4. Success criteria

_TBD — three concrete chat-style examples (e.g., "How many customers placed orders last month?")._

## 5. Eval dimensions

Hardest to calibrate. **Groundedness** is the most important signal.

| Dimension | Metric | Threshold | Weight | Judge |
|---|---|---|---|---|
| `groundedness` | LLM-judge: every factual claim is supported by a tool result | `0.85` | `0.30` | `tests/eval/judges/grounded_judge.py` |
| `factuality` | LLM-judge: claims that are gold-standard correct vs the triplestore | `0.90` | `0.25` | `tests/eval/judges/factual_judge.py` (queries triplestore directly) |
| `tool_selection` | exact-match on first tool called for canonical inputs | `0.85` | `0.15` | rule-based |
| `relevance` | LLM-judge: answer addresses the user's question | `0.90` | `0.10` | `tests/eval/judges/relevance_judge.py` |
| `latency_p95` | seconds | `<= 15.0` | `0.10` | wall-clock |
| `cost_per_call` | USD | `<= 0.04` | `0.10` | MLflow usage |

**Aggregate threshold:** ≥ `0.85`.

## 6. Failure modes

| Symptom | Detection | Mitigation |
|---|---|---|
| Tool-call failures (the production incident in CNS §4.6 T6 worked example) | Latency P95 + tool-call success rate dashboard | size-guard on SPARQL queries returning > 10k rows; structured error responses |
| Hallucinated entity URIs not present in the triplestore | `factuality` < 0.7 on `tags: ["adversarial"]` examples | system prompt: only reference URIs returned by tools |
| Drift after a prompt edit | nightly drift cron (M2.P7) opens a JIRA tagged `eval-drift` | revert + add regression examples |
| _TBD_ | _TBD_ | _TBD_ |

## 7. Eval dataset

- **Baseline:** `tests/eval/datasets/agent_dtwin_chat/baseline.jsonl` — ≥ 20 examples spanning aggregate queries, single-entity lookups, multi-hop traversals, and adversarial (out-of-scope) prompts.
- **Synthetic:** Use `databricks-synthetic-data-generation` against a sample ontology.
- **Regression:** `tests/eval/datasets/agent_dtwin_chat/regression.jsonl` — seed with the failing-SPARQL-tool-call cases from the production incident.

## 8. MLflow tracing

`@trace_agent` on the run loop; `@trace_llm` on each model call; `@trace_tool` on each tool handler. Spans should expose `tool_call_count`, `tokens_in`, `tokens_out` as attributes for the drift cron.

## 9. Plan reference

`.planning/agent_dtwin_chat-spec/PLAN.md` (to create at M2.P4).

## 10. Sign-off

- [ ] Author has filled sections 4, 5, 6, 7.
- [ ] Baseline eval run URI pasted into PR body.
- [ ] Aggregate threshold ≥ declared value in §5.
- [ ] Reviewer waiver (if applicable): _____
