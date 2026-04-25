---
name: refactoring
description: Use when the user asks to "refactor", restructure, clean up, simplify, deduplicate, extract, or reorganize code. Enforces the Martin Fowler discipline defined in src/.coding_rules.md and .cursor/08-testing-and-deployment.mdc.
---

# OntoBricks refactoring

"Refactor" = the **Martin Fowler** definition. The canonical guides are
`src/.coding_rules.md` (long-form) and `.cursor/08-testing-and-deployment.mdc
§Working Style for Refactoring` (the 5-step procedure). Read those first;
this skill only sequences the work.

## Procedure

For every refactor request, follow the 5 steps from `.cursor/08`:

1. Summarize the relevant module/function (1–3 sentences).
2. List code smells, mapping each to a named refactoring (Fowler catalog).
3. Propose an ordered plan (2–5 steps).
4. Apply in **small, reviewable chunks** — one route, one method, one file at a time.
5. After each chunk: name the refactorings applied, explain the structural improvement, list remaining smells.

Run `uv run pytest -q` between chunks for non-trivial sequences. Behavior
must stay identical — see `src/.coding_rules.md §1.1`.

## Smell → refactoring quick lookup

The full table lives in `.cursor/05-code-style-and-structure §Code Smells`
and `src/.coding_rules.md §2`. The few I reach for most often:

| Smell | Refactoring |
|-------|-------------|
| Long route handler | Extract Method into the matching `back/objects/` class |
| Legacy `service.py` | Move Function + delete `service.py` once empty |
| Repeated try/except → JSON envelope | Extract Function or rely on the global handler |
| Long parameter list / data clump | Introduce Parameter Object (Pydantic / `@dataclass`) |
| Magic strings/codes | Replace Magic Number with Symbolic Constant in `constants.py` |
| God service | Extract Class by domain + Move Function |
| Module-level functions sharing state | Convert to a class (one public class per file, PascalCase) |

## Don't

- Don't bundle behavioural changes with a refactor — separate commits.
- Don't rename a public class/method without a backward-compat wrapper in `__init__.py` (`.cursor/07 §__init__.py Conventions`).
- Don't skip the changelog (`changelog` skill).
