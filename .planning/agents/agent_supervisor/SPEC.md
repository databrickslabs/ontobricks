# SPEC: agent_supervisor

> Required by `.cursor/12-ai-feature-lifecycle.mdc`.

## 1. Purpose

`agent_supervisor` is a Databricks Agent Bricks Multi-Agent Supervisor (MAS) that
orchestrates OntoBricks entity/relationship mapping. It deterministically scores a
domain's complexity (from source metadata + ontology) and routes the mapping task
to either the heavyweight PGE engine (`agent_mapping_pge`) or the original simple
single-agent engine (`agent_auto_assignment`). The routing decision is computed by
a Unity Catalog function (`assess_domain_complexity`) and acted on via the
supervisor's natural-language instructions.

## 2. Identity

| Field | Value |
|---|---|
| `agent_name` | `agent_supervisor` |
| `module_path` | `src/agents/agent_supervisor/` |
| `model_endpoint` | Agent Bricks MAS endpoint (provisioned via `mas.py`) |
| `temperature` | `0.0` (assessment is deterministic; routing is rule-driven) |
| `mlflow_experiment` | `/Shared/ontobricks/agents/supervisor` |

## 3. Tool surface

| Tool name | Input | Output | Purpose |
|---|---|---|---|
| `assess_domain_complexity` (UC fn) | `metadata_json`, `ontology_json` | JSON `{score, tier, recommended_engine, signals, rationale}` | Deterministic engine recommendation |
| `pge_mapping` (endpoint) | mapping `custom_inputs` | mapping result + PGE extras | Run `agent_mapping_pge` |
| `simple_mapping` (endpoint) | mapping `custom_inputs` | mapping result | Run `agent_auto_assignment` |

## 4. Success criteria

1. A 3-source domain sharing an NHS-number key with ~17 classes is routed to `pge`.
2. A single-table, 2-class domain is routed to `simple`.
3. The supervisor always calls `assess_domain_complexity` before routing and never
   overrides its `recommended_engine`.

## 5. Eval dimensions

| Dimension | Metric | Threshold | Weight | Judge |
|---|---|---|---|---|
| `routing_accuracy` | predicted engine == expected engine over the baseline set | `0.95` | `0.50` | rule-based (`complexity.assess`) |
| `determinism` | identical input yields identical recommendation across runs | `1.00` | `0.20` | rule-based |
| `assessor_called_first` | supervisor calls `assess_domain_complexity` before any engine | `1.00` | `0.20` | trace inspection |
| `latency_p95` | assessment seconds (excludes the engine run) | `<= 2.0` | `0.10` | wall-clock |

**Aggregate threshold:** ≥ `0.90` to pass.

## 6. Failure modes

| Symptom | Detection | Mitigation |
|---|---|---|
| Supervisor skips the assessor and guesses | trace shows no `assess_domain_complexity` call | strengthen instructions; the assessor verdict is authoritative |
| Complex domain routed to simple engine | `routing_accuracy` drop on cross-source cases | re-tune weights/threshold in `complexity.py` + `uc_function.sql` (keep in sync) |
| UC function / Python drift | `test_uc_function_parity` shared-constant check | edit both files together |

## 7. Eval dataset

- **Baseline:** `tests/eval/datasets/agent_supervisor/baseline.jsonl` (≥20 examples;
  mix of single-source/simple and multi-source/complex domains with the expected
  engine).
- **Regression:** added on first production mis-route.

## 8. MLflow tracing

The mapping-engine ResponsesAgents (`responses_agent.py`) trace via the shared
MLflow `ResponsesAgent` plumbing; the assessment is logged at INFO. The MAS
endpoint is traced by Agent Bricks.

## 9. Plan reference

`docs/plans/2026-06-25-goal-loop-and-pge-eval-design.md` (PGE family) + the PR-split
plan tracked in session memory.

## 10. Sign-off

- [x] Sections 4, 5, 6, 7 filled.
- [ ] Baseline eval run URI pasted into PR body (waiver: calibration grace period; routing unit tests + 20-example baseline cover cold-start).
- [x] Aggregate threshold declared in §5.
