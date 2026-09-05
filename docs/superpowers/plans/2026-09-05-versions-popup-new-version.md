# Versions Popup New Version Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the existing domain-version creation workflow to the global Versions popup without duplicating its behavior.

**Architecture:** `navbar.js` owns one global `createNewDomainVersion(options)` workflow because the popup is available across all domain workspaces. Domain → Versions delegates without popup context; the popup supplies close/restore callbacks so the shared workflow can manage it around the existing full-page loading overlay without coupling the action to Bootstrap internals.

**Tech Stack:** Jinja HTML, browser JavaScript, Bootstrap 5, pytest structural contracts.

## Global Constraints

- Keep version creation available for read-only and incomplete versions.
- Use `showConfirmDialog` and `showNotification`; do not use native browser dialogs.
- Keep Switch as the popup footer's rightmost primary action.
- Close the Switch Version popup only after confirmation, keep the loading overlay visible through reload, and restore the popup after failure.

---

### Task 1: Shared create-version action

**Files:**
- Modify: `tests/units/front/test_create_version_ungated.py`
- Modify: `src/front/static/global/js/navbar.js`
- Modify: `src/front/static/domain/js/domain-versions.js`
- Modify: `src/front/templates/partials/domain/_domain_versions.html`

**Interfaces:**
- Produces: `window.createNewDomainVersion(options?: { closeSourceModal?: Function, restoreSourceModal?: Function }): Promise<void>`
- Consumes: `/domain/create-version`, `showConfirmDialog`, `showNotification`

- [ ] Add assertions that the global function owns the API workflow, accepts optional popup context, and Domain → Versions calls it without that context.
- [ ] Run `uv run --frozen pytest -q tests/units/front/test_create_version_ungated.py` and verify the new assertions fail.
- [ ] Implement `createNewDomainVersion(options)` in `navbar.js`, close the source modal after confirmation, show the shared overlay, and hide/reopen it on failure.
- [ ] Re-run the focused test and verify it passes.

### Task 2: Versions popup button

**Files:**
- Modify: `tests/units/front/test_switch_domain_modal.py`
- Modify: `src/front/static/global/js/navbar.js`

**Interfaces:**
- Consumes: `createNewDomainVersion({ closeSourceModal, restoreSourceModal }): Promise<void>`

- [ ] Add assertions that the popup click binding passes its Bootstrap modal to the shared action.
- [ ] Run `uv run --frozen pytest -q tests/units/front/test_switch_domain_modal.py` and verify failure.
- [ ] Bind the existing New Version button with callbacks that hide the current modal and recreate it after failure.
- [ ] Re-run both focused frontend tests and verify they pass.

### Task 3: Verification and changelog

**Files:**
- Modify: `changelogs/v0.9.0/benoitcayladbx_2026-09-05.log`

**Interfaces:**
- No new runtime interface.

- [ ] Browser-check cancellation, popup close + branded spinner after confirmation, and popup restoration on failure.
- [ ] Run `uv run --frozen pytest -q -m "not scenario"`.
- [ ] Check diagnostics for all modified source and test files.
- [ ] Record modified files and exact test results in the v0.9.0 changelog.
