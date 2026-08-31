# Release Notes v0.8.0 and September 2026 Roadmap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish accurate v0.8.0 release notes and a complete Asana-first product roadmap for September 2026.

**Architecture:** Build the release notes from repository evidence and all v0.8.0 changelogs, then rebuild the roadmap from the active `OntoBricks-Product` Asana project. Asana controls planned scope and relative priority, while repository evidence controls delivered status; Asana dates are deliberately excluded.

**Tech Stack:** Markdown, Git, Asana MCP, repository changelogs.

## Global Constraints

- Write both deliverables in English.
- Create `releases/ReleaseNotes_V0.8.0.md` and `releases/Roadmap_2026-09.md`.
- Use `releases/ReleaseNotes_V0.7.0.md` as the release-note format reference.
- Treat the active `OntoBricks-Product` Asana project as the primary source for planned scope and priority.
- Do not copy Asana dates into the roadmap and do not publish forecast dates.
- Use repository state and `changelogs/v0.8.0/` to determine what is delivered.
- Mark items without an Asana priority as needing triage.
- Do not commit unless the user explicitly requests a commit.

---

### Task 1: Build the v0.8.0 release evidence inventory

**Files:**
- Read: `changelogs/v0.8.0/*.log`
- Read: `releases/ReleaseNotes_V0.7.0.md`
- Read: `pyproject.toml`
- Create: `releases/ReleaseNotes_V0.8.0.md`

**Interfaces:**
- Consumes: v0.8.0 changelog sections and existing release-note conventions.
- Produces: a release document containing only shipped v0.8.0 behavior.

- [ ] **Step 1: List every tracked v0.8.0 changelog**

Run:

```bash
git ls-files 'changelogs/v0.8.0/*.log'
```

Expected: the complete set of tracked v0.8.0 contributor logs, including the
latest `benoitcayladbx_2026-08-31.log`.

- [ ] **Step 2: Extract a capability inventory**

Read every listed log and group the entries into:

```text
Highlights
Ontology and Mapping
Knowledge Graph and Backends
Registry and Workflow
External API and MCP
UI and Settings
Security and Reliability
Deployment and Documentation
Selected Fixes
Upgrade Notes
```

For each candidate claim, retain the originating changelog path. Exclude design
documents, planned work, and incomplete Asana tasks.

- [ ] **Step 3: Resolve duplicate and superseded entries**

When multiple changelog entries describe iterations of the same feature,
describe only the final user-visible state. In particular, avoid narrating
intermediate implementations that were replaced before release.

- [ ] **Step 4: Write the release notes**

Create `releases/ReleaseNotes_V0.8.0.md` with this structure:

```markdown
# OntoBricks — Release Notes V0.8.0

**Release window:** August 2026
**Test status:** all changes shipped with the latest recorded non-scenario suite green.

---

## Highlights

## Ontology and Mapping

## Knowledge Graph and Backends

## Registry and Workflow

## External API and MCP

## UI and Settings

## Security and Reliability

## Deployment and Documentation

## Bug Fixes (selected)

---

## Upgrade Notes
```

Replace the generic test sentence with the exact latest full-suite result from
the v0.8.0 changelog. Keep highlights concise and move operational detail into
the relevant sections.

- [ ] **Step 5: Verify every major claim**

For every highlight and upgrade note, confirm at least one corresponding
v0.8.0 changelog entry. Remove any claim supported only by the old roadmap or
an incomplete Asana task.

---

### Task 2: Refresh the Asana product inventory

**Files:**
- Read: `releases/Roadmap_2026-08.md`
- Create: `releases/Roadmap_2026-09.md`

**Interfaces:**
- Consumes: active tasks, sections, priority/status fields from
  `OntoBricks-Product`, plus delivered-state evidence from Task 1.
- Produces: a normalized ordered inventory for the September roadmap.

- [ ] **Step 1: Read the Asana project structure**

Fetch the active `OntoBricks-Product` project with sections and custom fields.
Confirm these fields before drafting:

```text
Priority: High, Medium, Low
Status: Not started, In progress, Completed, Analyzing
Sections: Version 0.8.0, Version 0.9.0, Version 1.0.0, Requests - To evaluate
```

- [ ] **Step 2: Fetch active tasks from every section**

Retrieve incomplete tasks from each project section with:

```text
name
section
Priority
Status
Git Working Branch
description or notes
subtasks
```

Dates may be read to understand stale records, but must not be copied into the
roadmap.

- [ ] **Step 3: Normalize relative priority**

Order tasks within each release using:

```text
1. Explicit Asana priority: High > Medium > Low > unset
2. Status at equal priority: In progress > Analyzing > Not started > unset
3. Product dependency and user value
4. Repository evidence about delivered prerequisites
```

Label unset priority as `Needs triage`. Keep requests in their evaluation
section unless Asana already assigns them to a release.

- [ ] **Step 4: Reconcile delivered work**

Compare each active task with Task 1 and the current repository:

```text
Delivered -> describe in release notes; remove from future roadmap scope
Partially delivered -> retain only the remaining scope
Not delivered -> retain with Asana priority/status
Unclear -> mark Needs validation rather than guessing
```

---

### Task 3: Write the complete September roadmap

**Files:**
- Read: `releases/Roadmap_2026-08.md`
- Create: `releases/Roadmap_2026-09.md`

**Interfaces:**
- Consumes: normalized Asana inventory from Task 2 and delivered capabilities
  from Task 1.
- Produces: the complete forward-looking roadmap replacing the August edition.

- [ ] **Step 1: Write the roadmap header and policy**

Use this opening structure:

```markdown
# OntoBricks — Product Roadmap

> **Version:** 0.8.0 → beyond
> **Last updated:** 2026-09-01
> **Status:** Living document — Asana-first prioritization
> **Scheduling policy:** priorities and dependencies are directional; this roadmap contains no delivery-date commitments.
```

Retain the existing legal disclaimer.

- [ ] **Step 2: Update current state**

Replace the v0.7.x current-state section with a concise v0.8.0 capability
baseline derived from Task 1. Do not repeat the full release notes.

- [ ] **Step 3: Write release sections**

Use this order:

```markdown
## Prioritization and Status Legend
## Current State — v0.8.0
## Roadmap
### v0.8.x — Release Readiness and Follow-up
### v0.9.0 — Product and Workflow Priorities
### v1.0.0 — GA and Ecosystem Priorities
### Requests — To Evaluate
## Cross-Release Dependencies
## Open Decisions
## How to Contribute
```

Each planned item must show `Priority` and `Status`. Use `Needs triage` where
Asana leaves priority unset. Do not include forecast dates, freeze dates, or
effort windows.

- [ ] **Step 4: Preserve useful strategic context**

Retain still-current market positioning, graph-engine comparison, and
contribution guidance from `Roadmap_2026-08.md`, but remove:

```text
stale delivered limitations
obsolete capacity calculations
old calendar windows
claims that v0.7.0 is current
features absent from both Asana and the active strategic direction
```

- [ ] **Step 5: Represent all active Asana items**

Confirm every active task is either:

```text
included under its Asana release section
listed under Requests — To Evaluate
explicitly identified as delivered, duplicate, or superseded
```

Do not silently omit an active task.

---

### Task 4: Validate both documents

**Files:**
- Verify: `releases/ReleaseNotes_V0.8.0.md`
- Verify: `releases/Roadmap_2026-09.md`
- Verify: `docs/superpowers/specs/2026-08-31-release-notes-roadmap-september-design.md`

**Interfaces:**
- Consumes: both completed documents and the approved design.
- Produces: internally consistent release documentation ready for user review.

- [ ] **Step 1: Scan for placeholders and stale version claims**

Search:

```bash
rg -n 'TBD|TODO|v0\.7\.0 is the current|freeze [0-9]|release [0-9]{1,2} ' \
  releases/ReleaseNotes_V0.8.0.md releases/Roadmap_2026-09.md
```

Expected: no placeholder, stale-current-version, or calendar-commitment match.

- [ ] **Step 2: Check release-note evidence**

Compare the release-note headings and highlights against all
`changelogs/v0.8.0/*.log`. Correct unsupported or duplicated claims.

- [ ] **Step 3: Check roadmap coverage**

Compare the roadmap against the latest active Asana task inventory. Confirm
that all active tasks are represented and all unset priorities are labelled
`Needs triage`.

- [ ] **Step 4: Review the diff**

Run:

```bash
git diff --check
git diff --stat
git status --short
```

Expected: no whitespace errors; only the approved specification, plan, release
notes, and September roadmap are new or modified by this work.

- [ ] **Step 5: Report verification**

Summarize:

```text
release-note changelog coverage
active Asana task coverage
placeholder/stale-claim scan
files changed
```

Do not run the application test suite because this task changes product
documentation only.
