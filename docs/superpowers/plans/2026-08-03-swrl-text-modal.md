# SWRL Text Modal Implementation Plan

> **For agentic workers:** Inline execution (same change as SHACL Turtle modal).

**Goal:** Replace Business Rules inline SWRL text pane with a Bootstrap `modal-xl`.

**Architecture:** Same as SHACL Turtle modal — `#brSwrlModal`, `openSwrlModal()`, keep editor id and import modal.

**Tech Stack:** Bootstrap 5, Jinja partial, `SwrlModule` JS.

**Spec:** `documentation/superpowers/specs/2026-08-03-swrl-text-modal-design.md`

---

### Task 1

- [x] HTML / JS / CSS convert pane → modal
- [x] Update front source-contract test
- [x] Changelog + pytest
