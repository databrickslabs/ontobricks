---
name: frontend-design
description: Use when creating or changing OntoBricks templates, page layouts, tabs, cards, forms, or responsive UI behavior.
---

# OntoBricks frontend design

The canonical design system is `.cursor/11-frontend-design.mdc`. Read that
file before proposing or changing frontend markup or CSS. This skill sequences
the work; it does not restate the visual rules.

## Procedure

1. Identify the closest canonical page or component in `.cursor/11`.
2. Inspect its complete markup and shared CSS before editing.
3. Add a failing structural or behavior contract.
4. Reuse shared Bootstrap and `ob-*` components; do not add local visual
   overrides when the shared component already owns the state.
5. Keep the sidebar section layout: the active section is a white bordered
   page panel; `.section-header` first, then all body content inside one
   `.card` (`.card-body` when needed).
   Reference: Domain → Versions (`.cursor/11` → *Section Body Surface*).
   Never leave tiles, tables, forms, or empty states bare on the canvas.
6. Browser-test the changed page against the reference at desktop and mobile
   widths, including keyboard focus, overflow, and console or network errors.
7. Run `uv run --frozen pytest -q -m "not scenario"`.
8. Invoke the `changelog` skill.
