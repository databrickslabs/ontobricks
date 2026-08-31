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
5. Browser-test the changed page against the reference at desktop and mobile
   widths, including keyboard focus, overflow, and console or network errors.
6. Run `uv run --frozen pytest -q -m "not scenario"`.
7. Invoke the `changelog` skill.
