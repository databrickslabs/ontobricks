# Auto Mapping Live Trace + Audit Report Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** While auto-mapping runs, the Mapping overlay shows the agent's per-entity/relationship steps as they happen; after the run (success, failure, or cancellation), Domain → Audit trail carries a durable report of everything the agent executed.

**Architecture:** No new transport. `Mapping.run_auto_assign_task` already assigns `task.result` per chunk, and `Task.to_dict()` serializes `result` while the task is `running`, so republishing `serialize_agent_steps(all_steps)` per chunk makes the steps visible to the existing 1.5 s poll in `mapping-autoassign.js`. The frontend renderer already exists (`renderAgentStepsLogHtml`); it is only gated behind `status === 'completed'`. Cancellation becomes cooperative via `TaskManager.is_cancelled`. The durable report is one `agent_auto_map_run` change-audit event appended to the session buffer (`domain_data.change_log`) and flushed to `domain_change_events` by the existing save-to-registry path.

**Tech Stack:** Python / FastAPI, in-memory `TaskManager`, `agents.serialization.serialize_agent_steps`, JSON session files, Lakebase `domain_change_events.meta` (jsonb), vanilla JS overlay + audit timeline, pytest.

**Spec:** `.planning/agents/agent_auto_assignment/SPEC.md` §3.1 (observability contract) — authoritative for event and report shapes.

**Asana:** OntoBricks-Product → Automation & Exploration Performance (v0.8.0 freeze).

## Global Constraints

- Python ≥ 3.10; tests with `uv run --frozen pytest …` only (never bare `uv run`, it poisons `uv.lock`)
- Work directly on `develop`
- v0.8.0 freeze only — no parallel auto-map, no Graph Explorer parallelization
- Step JSON keys are exactly `type` / `tool` / `content` / `ms` (from `serialize_agent_steps`); never invent a second step schema
- Audit action name is exactly `agent_auto_map_run`, `source="agent"`
- Reuse `domain_change_events.meta` (jsonb) — no new table, no new migration
- Audit writes are best-effort: an audit failure must never fail or roll back a mapping save
- The report reaches the Audit Trail on the next save-to-registry, exactly like today's `mapping_entity_added` events. No immediate registry write.
- Changelog under `changelogs/v0.8.0/` (version from `pyproject.toml`)
- Comments, changelog, docs, audit-trail strings: English only

## Why no SSE

The earlier draft of this plan assumed a new SSE route mirroring Graph Chat. Three findings removed the need:

1. `Task.to_dict()` includes `result` unconditionally (`src/back/core/task_manager/models.py`), so anything written to `task.result` mid-run is already exposed by `GET /tasks/{id}`.
2. `src/front/static/global/js/task-progress-ui.js` already renders agent steps (`renderAgentStepsLogHtml`, `mountAgentStepsLog`); the only blockers are the `task.status === 'completed'` guard in `updateFromTask` and the mount early-return in `mountAgentStepsLog`.
3. `monitorTask` in `src/front/static/mapping/js/mapping-autoassign.js` polls every 1500 ms and calls `updateProgressFromTask` on every tick, including while running.

Perceived granularity is 1.5 s against LLM steps that take seconds each, which is adequate. If a future demo needs sub-second updates, adding SSE is a self-contained follow-up that does not change the step or report contracts.

## File map

| File | Role |
|---|---|
| `.planning/agents/agent_auto_assignment/SPEC.md` | Observability contract (§3.1) |
| `.planning/agents/agent_auto_assignment/eval/dataset.jsonl` | Golden sequences (happy / error / cancel) |
| `tests/eval/datasets/agent_auto_assignment/observability.jsonl` | Mirror of the same dataset |
| `tests/eval/thresholds.yaml` | `trace_step_order`, `audit_report_parity` |
| `src/back/objects/mapping/Mapping.py` | Per-chunk publish, cooperative cancel, audit event on all terminal paths |
| `src/front/static/global/js/task-progress-ui.js` | Render steps while running, refresh the mount |
| `src/front/static/domain/js/domain-audit.js` | `agent_auto_map_run` label + expandable step report |
| `tests/units/mapping/test_auto_assign_observability.py` | Publish, cancel, and audit-event tests |
| `documentation/user-guide.md` | Audit Trail paragraph |
| `changelogs/v0.8.0/benoitcayladbx_2026-08-24.log` | Post-change log |

---

### Task 1: SPEC + eval dataset

**Files:**
- Modify: `.planning/agents/agent_auto_assignment/SPEC.md`
- Create: `.planning/agents/agent_auto_assignment/eval/dataset.jsonl`
- Create: `tests/eval/datasets/agent_auto_assignment/observability.jsonl`
- Modify: `tests/eval/thresholds.yaml`

The repo convention is one SPEC per agent under `.planning/agents/<agent>/`, updated in place for a material change — not a per-slug folder. The pre-existing SPEC described icons and layout (that is `agent_auto_icon_assign`); correct section 1 and leave the icon/layout eval dimensions and their `thresholds.yaml` keys untouched so this change does not re-baseline an unrelated gate.

- [ ] **Step 1:** Add SPEC §3.1 with the live surface, cancellation semantics, and the audit report field table.
- [ ] **Step 2:** Add `trace_step_order` and `audit_report_parity` as pass/fail dimensions with no aggregate weight.
- [ ] **Step 3:** Write three dataset rows (happy, error, cancel) using the existing `{id, input, expected.constraints[{kind, value}], tags}` shape from `baseline.jsonl`.

---

### Task 2: Publish steps per chunk + cooperative cancellation

**Files:**
- Modify: `src/back/objects/mapping/Mapping.py` (`run_auto_assign_task`)
- Test: `tests/units/mapping/test_auto_assign_observability.py`

**Interfaces:**
- Produces: `task.result["agent_steps"]` (cumulative, append-only, `serialize_agent_steps` shape) during the run
- Produces: early loop exit on cancellation, with mappings persisted and the audit report written

The chunk loop already ends with a `task.result = {...}` assignment carrying `live_stats` and the four counters. Extend that dict with `agent_steps`; do not introduce a `TaskManager` setter, since direct assignment is the established pattern in this function.

- [ ] **Step 1: Write the failing test** — a fake agent result with two steps, two chunks; assert `task.result["agent_steps"]` has entries after chunk 1 (before completion) and that keys are exactly `type` / `tool` / `content` / `ms`.
- [ ] **Step 2:** Add `"agent_steps": serialize_agent_steps(all_steps)` to the per-chunk `task.result` assignment, keeping `live_stats` and the counters.
- [ ] **Step 3: Write the failing cancel test** — mark the task cancelled after chunk 1; assert the second chunk never calls the agent and that mappings from chunk 1 are saved.
- [ ] **Step 4:** At the top of the chunk loop, `if tm.is_cancelled(task.id): break`. Place the check before the cooldown `time.sleep`, so a cancel during cooldown is honoured promptly. Track that the loop was cut short so the terminal status is `cancelled`, not `completed`.
- [ ] **Step 5:** Run the new tests, then commit.

---

### Task 3: Live rendering in the overlay

**Files:**
- Modify: `src/front/static/global/js/task-progress-ui.js`

- [ ] **Step 1:** In `updateFromTask`, mount agent steps whenever `task.result.agent_steps` is non-empty, not only on `completed`.
- [ ] **Step 2:** Make `mountAgentStepsLog` refresh instead of bailing when the mount exists: replace the inner HTML and preserve the `<details>` open/closed state so a 1.5 s tick cannot collapse the panel under the user's cursor.
- [ ] **Step 3:** Keep `clearPanels` removing the mount, so a new run starts clean.

---

### Task 4: Persist the agent-run report

**Files:**
- Modify: `src/back/objects/mapping/Mapping.py`
- Test: `tests/units/mapping/test_auto_assign_observability.py`

The background thread does not hold a live `DomainSession`: it writes the session file directly through the static `save_mappings_to_session`. The change-audit buffer lives in the same bucket (`DomainSession.SESSION_KEY = "domain_data"`, `record_change` appends to `self._data["change_log"]`), so the event is appended to `data["domain_data"]["change_log"]` in that same write, using the exact shape `record_change` produces: `ts`, `action`, `entity_type`, `entity_ref`, `summary`, `source`, `meta`.

- [ ] **Step 1: Write the failing test** for a pure builder that returns the event dict; assert `action`, `source`, and `meta.steps` parity with the published payload.
- [ ] **Step 2:** Implement the builder and a best-effort session-append helper (log and continue on any exception).
- [ ] **Step 3:** Call it on all three terminal paths: `complete_task`, the `fail_task` no-mappings path and the outer `except`, and the new cancelled path.
- [ ] **Step 4:** Ensure the failure paths still write a report even when no mappings were produced (no session mapping write to piggyback on).

---

### Task 5: Audit Trail UI

**Files:**
- Modify: `src/front/static/domain/js/domain-audit.js`
- Modify: `documentation/user-guide.md`

- [ ] **Step 1:** Add `agent_auto_map_run: { icon: 'robot', cls: 'text-info', label: 'Auto-mapping agent run' }` to `CHANGE_META`.
- [ ] **Step 2:** In `changeItem`, special-case that action: status chip from `meta.status`, counts from `meta.stats`, and a `<details>` listing `meta.steps` in order. Escape every value with `esc()`; never inject raw model text as HTML.
- [ ] **Step 3:** Leave it in the existing `changes` filter and version dropdown; do not add a fourth stream.
- [ ] **Step 4:** Document the new entry in the Audit Trail section of the user guide, including that it appears after the domain is saved.

---

### Task 6: Tests, changelog, Asana

- [ ] **Step 1:** `uv run --frozen pytest -q -m "not scenario"`
- [ ] **Step 2:** Append an English section to `changelogs/v0.8.0/benoitcayladbx_2026-08-24.log`: title, context, numbered changes with paths, modified-file list, test result.
- [ ] **Step 3:** Manual check with `./scripts/start.sh`: run Auto-map, watch steps appear mid-run, cancel a run, save the domain, open Domain → Audit trail and confirm one report per run.
- [ ] **Step 4:** Re-sync Asana — the subtasks still describe an SSE approach and 16 h for the live trace. Update to the incremental-publish approach, revise effort down, and add cooperative cancellation to the scope.

## Spec coverage

| Requirement | Task |
|---|---|
| Live per-entity/relationship steps in Mapping | 2, 3 |
| No new streaming route | 2 (design decision) |
| Cancel actually stops the agent | 2 |
| Full agent report in Audit Trail | 4, 5 |
| Success / failure / cancellation all recorded | 4 |
| SPEC + eval delta (AI-feature gate) | 1 |
| No parallel map, no explorer perf | Global Constraints |
| Existing mapping chips unchanged | 4 |
