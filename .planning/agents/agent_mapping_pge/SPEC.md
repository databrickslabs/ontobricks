# SPEC: agent_mapping_pge

> Required by `.cursor/12-ai-feature-lifecycle.mdc`.

## 1. Purpose

`agent_mapping_pge` generates entity and relationship SQL mappings for a domain
via a Planner→Generator→Evaluator (PGE) loop. Given source metadata + an ontology
it plans a source model, generates SQL per ontology item, and gates each mapping
with a deterministic evaluator plus an independent semantic critic. It is an
**additive** alternative to the single-agent `agent_auto_assignment` mapping flow:
the legacy engine remains the default in `Mapping.auto_assign_with_agent`; this
engine is reachable via `Mapping.auto_assign_with_pge_agent` and
`AgentClient.run_mapping_pge`. Coverage is enforced from the ontology rather than
LLM discretion.

## 2. Identity

| Field | Value |
|---|---|
| `agent_name` | `agent_mapping_pge` |
| `module_path` | `src/agents/agent_mapping_pge/` |
| `model_endpoint` | _configured per workspace_ |
| `temperature` | `0.0`–`0.2` |
| `mlflow_experiment` | `/Shared/ontobricks/agents/mapping_pge` |

## 3. Tool surface

| Tool name | Input | Output | Purpose |
|---|---|---|---|
| `submit_source_model` | planner source-model | `SourceModel` | Terminal planner tool |
| `submit_entity_mapping` | entity SQL + id expr | mapping dict | Record an entity mapping |
| `submit_relationship_mapping` | rel SQL + endpoints | mapping dict | Record a relationship mapping |
| `normalized_value_overlap` | two columns | overlap ratio | Verify join-key overlap |
| `submit_evaluation` | critic verdict | `EvalReport` | Terminal critic tool |

## 4. Success criteria

1. Every mappable ontology class/relationship is covered (engine-enforced, not
   LLM-discretionary).
2. Relationship endpoints reproduce the entity's canonical id expression →
   0% dangling on a valid domain.
3. A failed hub entity does not cascade to drop all its relationships (synthetic
   endpoint fallback).

## 5. Eval dimensions

| Dimension | Metric | Threshold | Weight | Judge |
|---|---|---|---|---|
| `entity_coverage` | mapped entities / mappable classes | `1.00` | `0.25` | rule-based (`coverage.py`) |
| `relationship_coverage` | mapped rels / ontology object-properties | `1.00` | `0.20` | rule-based |
| `dangling_rate` | proportion of relationship edges with a resolvable endpoint | `1.00` | `0.25` | rule-based (deterministic evaluator) |
| `sql_executes` | generated SQL parses + runs | `0.98` | `0.15` | rule-based |
| `semantic_correctness` | critic agreement that the mapping matches intent | `0.85` | `0.15` | LLM critic (`evaluator/critic.py`) |

**Aggregate threshold:** ≥ `0.90`.

## 6. Failure modes

| Symptom | Detection | Mitigation |
|---|---|---|
| Class silently skipped | `entity_coverage` < 1.0 | coverage is computed from the ontology; `skip[]` is advisory and never removes an item |
| Relationship dangles | `dangling_rate` < 1.0 | relationship generator reproduces the endpoint's canonical id expression |
| One failed hub drops all rels | rel coverage collapse | synthetic-endpoint fallback from `canonical_ids` |
| Abstract superclass unmapped | missing union | abstract classes derived as UNION-ALL of concrete subclass SQL |

## 7. Eval dataset

- **Baseline:** `tests/eval/datasets/agent_mapping_pge/baseline.jsonl` — ≥ 20 examples
  spanning single-source, multi-source cross-trust, and degenerate inputs.
- **Regression:** added on first production mis-mapping.

## 8. MLflow tracing

The engine traces planner / generator / evaluator / critic stages; per-item
`mapping_evaluations` + `mapping_run_log` are surfaced on the result.

## 9. Plan reference

PGE design notes tracked in session memory; loop pattern per Anthropic's
harness-design (planner/generator/evaluator separation).

## 10. Sign-off

- [x] Sections 4, 5, 6, 7 filled.
- [ ] Baseline eval run URI pasted into PR body (waiver: calibration grace period per `.cursor/12-ai-feature-lifecycle.mdc`; unit + agent tests cover cold-start).
- [x] Aggregate threshold declared in §5.
