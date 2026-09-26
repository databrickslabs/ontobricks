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
| `get_documents_context` | `{}` | Ready documents plus `unavailable_documents` | Read the durable parsed corpus; never starts parsing |
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
4. A ready PDF sidecar is available to planner/critic as mapping evidence
   without an `ai_parse_document` call.
5. Pending and failed documents are disclosed as unavailable and are never
   treated as mapping evidence.

## 5. Eval dimensions

| Dimension | Metric | Threshold | Weight | Judge |
|---|---|---|---|---|
| `entity_coverage` | mapped entities / mappable classes | `1.00` | `0.25` | rule-based (`coverage.py`) |
| `relationship_coverage` | mapped rels / ontology object-properties | `1.00` | `0.20` | rule-based |
| `dangling_rate` | proportion of relationship edges with a resolvable endpoint | `1.00` | `0.25` | rule-based (deterministic evaluator) |
| `sql_executes` | generated SQL parses + runs | `0.98` | `0.15` | rule-based |
| `semantic_correctness` | critic agreement that the mapping matches intent | `0.85` | `0.15` | LLM critic (`evaluator/critic.py`) |
| `ready_corpus_use` | ready document context is consumed | `0.90` | contract | `tests/eval/run_agent_mapping_pge.py` |
| `no_parse_safety` | no extractor/parse tool in observed trace | `1.00` | contract | `tests/eval/run_agent_mapping_pge.py` |
| `status_disclosure` | unavailable documents are disclosed | `0.90` | contract | `tests/eval/run_agent_mapping_pge.py` |
| `sidecar_hiding` | `_parsed` never appears as a source | `1.00` | contract | `tests/eval/run_agent_mapping_pge.py` |
| `metric_view_sql_correctness` | metric-view mappings use `MEASURE()` + `GROUP BY` and reference only declared columns | `1.00` | contract | rule-based (`evaluator/deterministic.py::check_metric_view_sql`) |

**Mapping-quality aggregate threshold:** ≥ `0.90`.
**Parsed-corpus contract threshold:** ≥ `0.90`.

## 6. Failure modes

| Symptom | Detection | Mitigation |
|---|---|---|
| Class silently skipped | `entity_coverage` < 1.0 | coverage is computed from the ontology; `skip[]` is advisory and never removes an item |
| Relationship dangles | `dangling_rate` < 1.0 | relationship generator reproduces the endpoint's canonical id expression |
| One failed hub drops all rels | rel coverage collapse | synthetic-endpoint fallback from `canonical_ids` |
| Abstract superclass unmapped | missing union | abstract classes derived as UNION-ALL of concrete subclass SQL |
| Repeated warehouse parsing | trace contains `ai_parse_document` during Mapping | document preload has no extractor dependency; fail the corpus eval |
| Corpus not ready | pending/failed document is cited as evidence | return it under `unavailable_documents` and require status disclosure |
| Internal sidecar exposed | `_parsed` appears in document context | filter internal directories before preloading |
| Flat SELECT on a metric view | metric-view SQL missing `MEASURE()`/`GROUP BY` (deterministic `metric_view_sql` check + runtime `sql_execution`) | generator prompt rule + evaluator rejection |

### Metric-view sources (added 2026-09)

A source with `object_kind='metric_view'` is queried as
`SELECT <dimensions>, MEASURE(<measure>) AS <measure> ... FROM <fqn> GROUP BY <dimensions>`.
The planner slice carries `object_kind` per table (`TableRole.object_kind`) and
`role` (dimension|measure) per column; the Entity/Relationship generators MUST
wrap measures in `MEASURE()` and `GROUP BY` the projected dimensions, and MUST
NOT `SELECT *` a metric view. Measures are never used as an entity id or a
relationship endpoint. The deterministic evaluator enforces this statically via
`check_metric_view_sql` when a mapping carries `source_object_kind='metric_view'`
and `source_columns`.

## 7. Eval dataset

- **Baseline:** `tests/eval/datasets/agent_mapping_pge/baseline.jsonl` — 10
  material-change cases covering ready, pending, failed, mixed, empty,
  boundary, and adversarial corpus states.
- **Planning mirror:** `.planning/agents/agent_mapping_pge/eval/dataset.jsonl`.
- **Metric-view set:** `tests/eval/datasets/agent_mapping_pge/metric_view.jsonl`
  (mirrored at `.planning/agents/agent_mapping_pge/eval/metric_view.jsonl`) — 10
  cases (4 happy / 3 ambiguous / 3 adversarial) asserting `MEASURE()` + `GROUP
  BY`, no `SELECT *`, and measures never used as ids. Kept separate from the
  parsed-corpus `baseline.jsonl` because the current
  `run_agent_mapping_pge.py` runner is a document-corpus contract (needs
  `input.documents`); a SQL-generation runner over a live metric view is the
  remaining harness to wire. `check_metric_view_sql` already enforces the
  contract deterministically in unit tests.
- **Regression:** added on first production mis-mapping.

## 8. MLflow tracing

The engine traces planner / generator / evaluator / critic stages; per-item
`mapping_evaluations` + `mapping_run_log` are surfaced on the result.

## 9. Plan reference

`docs/superpowers/plans/2026-09-18-parsed-document-corpus.md`.

## 10. Sign-off

- [x] Sections 4, 5, 6, 7 filled.
- [x] Baseline eval: `https://fe-vm-bcayla-demos.cloud.databricks.com/ml/experiments/1426639566663819/runs/ab43e8f6c175472c8c166112e31d73ce` (`judge_score=0.985`).
- [x] Post-change eval: `https://fe-vm-bcayla-demos.cloud.databricks.com/ml/experiments/1426639566663819/runs/55125984c92e4466a3a20ea8ebe4137d` (`judge_score=1.000`).
- [ ] Baseline eval run URI pasted into PR body.
- [x] Aggregate threshold declared in §5.

### Metric-view change (2026-09-26)

- [x] `metric_view_sql_correctness` dimension declared in §5.
- [x] Dataset extended with metric-view rows (§7).
- [ ] Baseline eval run (pre-change) URI — run `tests/eval/run_agent_mapping_pge.py`.
- [ ] Post-change eval run URI — must show `metric_view_sql_correctness = 1.00`.
