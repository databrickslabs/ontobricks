# OntoBricks Agent Evals

Eval harness lives in `tests/eval/`. Each agent has:

- `tests/eval/datasets/<agent>/baseline.jsonl` — frozen, hand-curated examples.
- `tests/eval/datasets/<agent>/synthetic.jsonl` — `databricks-synthetic-data-generation` output (optional).
- `tests/eval/datasets/<agent>/regression.jsonl` — production failures we've fixed; never retired.
- `tests/eval/run_<agent>.py` — runner that loads the dataset, calls the agent (locally or against a serving endpoint), evaluates against `tests/eval/judges/`, and writes the run to MLflow.
- `tests/eval/judges/<judge>.py` — per-judge implementations (rule-based, schema validators, LLM-judges).
- `tests/eval/thresholds.yaml` — per-agent thresholds (judge score, top-K accuracy, latency, cost).

## Status (CNS T-M4)

| Agent | Baseline | Runner | Judge | Threshold |
|---|---|---|---|---|
| `agent_owl_generator` | 🟡 seed (3 examples) | ❌ | ❌ | (proposed in SPEC) |
| `agent_ontology_assistant` | 🟡 seed (3 examples) | ❌ | ❌ | (proposed in SPEC) |
| `agent_auto_assignment` | 🟡 seed (3 examples) | ❌ | ❌ | (proposed in SPEC) |
| `agent_auto_icon_assign` | 🟡 seed (3 examples) | ❌ | ❌ | (proposed in SPEC) |
| `agent_dtwin_chat` | 🟡 seed (3 examples) | ❌ | ❌ | (proposed in SPEC) |
| `agent_simulation_facts` | 🟢 27 examples | 🟡 dry-run schema | 🟡 rule-based | SPEC §5 |

**Recommended first to fully build:** `agent_auto_icon_assign` (deterministic top-K classification; trivial judge). The SPEC scaffold flags this.

## Row schema

```json
{
  "id": "<unique stable id>",
  "input": {...},
  "expected": {
    "contains": ["substring or URI must appear in output"],
    "schema": {"...": "JSON schema fragment the output must satisfy"},
    "constraints": [{"kind": "exact_match", "field": "icon_id", "value": "..."}]
  },
  "tags": ["happy" | "ambiguous" | "adversarial" | "synthetic" | "regression"]
}
```

Per-agent the `expected` shape may use different keys — see each agent's SPEC.md.

## Min sizes (gated by `eval-gate.yml`)

- New agent: ≥ 20 examples in `baseline.jsonl`.
- Material change to existing agent: ≥ 10 examples.
- Hotfix / regression test: ≥ 3 examples in `regression.jsonl`.

## How to fill an empty agent (M2.P4 workflow)

1. Invoke the `ai-feature` skill — it walks the brainstorming → SPEC → dataset → runner → eval flow.
2. Expand the 3-example seed in `baseline.jsonl` to ≥ 20.
3. Implement the judge(s) in `tests/eval/judges/`.
4. Implement the runner in `tests/eval/run_<agent>.py`.
5. Pin a threshold in `tests/eval/thresholds.yaml`.
6. Run the baseline locally — paste the MLflow URI into your PR.
7. When G2 (`eval-gate.yml`) flips out of calibration mode, the gate becomes hard.

## Large-schema stress run (owl_generator, issue #173)

`large_schema_stress.py` runs the real staged Generate chain
(`detect_entities` → `infer_relations` → `infer_attributes` → `infer_axioms`)
on synthetic schemas shaped like the #173 reports (one 164- or 360-column
table, 100 tables × 18 columns) and reports, per stage, items produced, LLM
calls, truncated calls, largest request size and token usage — plus how many
tables/columns the Stage-1 metadata overview actually shows the model.

```bash
uv run --frozen python tests/eval/large_schema_stress.py \
  --host "$DATABRICKS_HOST" --token "$DATABRICKS_TOKEN" \
  --endpoint <foundation-model-endpoint> \
  --mlflow-experiment /Shared/ontobricks/agents/owl_generator
```

Exit code is non-zero if any scenario fails; with `--mlflow-experiment` the
metrics and the JSON report are logged to one MLflow run whose URI is printed
(paste it into the PR). Offline coverage of the harness itself:
`tests/eval/test_large_schema_stress.py`.

## CI integration

- `.github/workflows/eval-gate.yml` — G2 gate (currently in 2-week calibration: reports without blocking).
- `.github/workflows/nightly.yml` → eval-drift detector (planned at M2.P7).
