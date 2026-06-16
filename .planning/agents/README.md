# Retroactive Agent SPECs

This directory holds the SPEC.md scaffold for every agent that existed **before** the CNS AI-feature lifecycle (`.cursor/12-ai-feature-lifecycle.mdc`) was introduced.

## Status

| Agent | SPEC.md | Eval dataset | First eval run |
|---|---|---|---|
| `agent_owl_generator` | 🟡 scaffold | ❌ not built | ❌ |
| `agent_ontology_assistant` | 🟡 scaffold | ❌ not built | ❌ |
| `agent_auto_assignment` | 🟡 scaffold | ❌ not built | ❌ |
| `agent_auto_icon_assign` | 🟡 scaffold | ❌ not built | ❌ |
| `agent_dtwin_chat` | 🟡 scaffold | ❌ not built | ❌ |
| `agent_cohort` | 🟡 scaffold | ❌ not built | ❌ |

**Legend:** 🟢 complete, 🟡 scaffold present, ❌ missing.

## Why scaffolds (not full SPECs)

Filling the eval-dimensions table for an existing agent requires:

- Running the agent against production-like inputs.
- Choosing a judge model.
- Calibrating thresholds against current behaviour (the baseline).

That's the work M2.P4 + M2.P5 do, in order. **The scaffolds here unblock the CI gate to recognise the agents as known**, and they encode the eight required headings so the team only has to fill the sections that need real data.

## How to fill one

1. Pick an agent.
2. Open its SPEC.md scaffold in `.planning/agents/<agent_name>/SPEC.md`.
3. Invoke `ai-feature` skill — it walks you through brainstorming + the table.
4. Build the eval dataset under `tests/eval/datasets/<agent_name>/baseline.jsonl`.
5. Wire the eval harness (`tests/eval/run_<agent_name>.py`).
6. Run baseline → record MLflow URI in the SPEC.
7. Open a PR with the filled SPEC + dataset + harness.

The G2 CI gate (`.github/workflows/eval-gate.yml`) will recognise the agent as gated once the SPEC's eval-dimensions table is non-empty.

## Order of work (recommended)

Start with **`agent_auto_icon_assign`** — it's the smallest, has deterministic output (icon ID classification), and an exact-match judge is trivial. That gives the team a worked example before the harder agents (`dtwin_chat` — RAG-style; `owl_generator` — generative).
