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
| `max_tokens` | `8192` per call (`_GEN_MAX_TOKENS`; the endpoint output cap — the full ontology Turtle is emitted in one message) |
| `max_classes` | `40` (`_DEFAULT_MAX_CLASSES`; over-generation guard — accepted ontology is asked to consolidate above this. Overridable via `options["max_classes"]`, `<=0` disables) |
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

| Symptom | Detection | Mitigation |
|---|---|---|
| **Truncated ontology → empty result.** The final Turtle answer is cut off at the output-token cap (`finish_reason == "length"`); the salvaged remainder fails to parse in every RDF syntax, so `/ontology/parse-owl` lands 0 classes and the Generate wizard polls until timeout. | `OntologyParser` logs `Content appeared truncated`; `rdf_utils.parse_rdf_flexible` fails all formats; session saved with 0 classes. In tests: `finish_reason == "length"` on the text answer. | `max_tokens=8192` (was 4096) + a truncation guard in `engine.run_agent`: a length-capped answer is not accepted — the agent is asked to re-emit the ontology concisely (within `MAX_ITERATIONS`), or the run fails with an explicit "output truncated" error instead of a silent empty ontology. Regression: `tests/eval/datasets/agent_owl_generator/regression.jsonl` + `tests/units/agents/test_agent_owl_generator_truncation.py`. |
| **Over-generation / class explosion.** The model over-decomposes — one class per column or per attribute value (e.g. `VatAmount`, `MeterReading`, `Payment`, `Call`) — emitting ~110 classes for a ~5-entity guideline. The ontology parses fine but downstream **auto-mapping** chunks ~5 classes/chunk with cool-downs, so ~22 chunks overrun the scenario `AUTOMAP_TIMEOUT` (600s) → "Auto-Map produced no entity SQL". | Auto-assign log shows `Chunk N/22` (vs the healthy `N/4`); accepted ontology `owl:Class` count ≫ input entity count. In tests: `_count_owl_classes(content) > max_classes`. | Prompt: replaced the "30–60 classes" size limit with "prefer 8–25, one class per real-world entity, never a class per column/value, hard limit 40". Guard: a class-count check in `engine.run_agent` asks the model (bounded by `_MAX_CONSOLIDATE_ROUNDS=2`) to consolidate above `max_classes` (default 40) before accepting. Regression: `tests/eval/datasets/agent_owl_generator/regression.jsonl` + `tests/units/agents/test_agent_owl_generator_class_cap.py`. |

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
