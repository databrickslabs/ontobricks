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

Run `uv run --frozen pytest -q` between chunks for non-trivial sequences. Behavior
must stay identical — see `src/.coding_rules.md §1.1`.

Target shape (`.cursor/07 §Class-First Policy`, `src/.coding_rules.md §2`):
one public class per file, one purpose per class, class ≤ 400 LOC, method
≤ 50 LOC, records grouped by aggregate root, inheritance only where the
"when inheritance is the right tool" table applies.

**The simplest code is always the best** (`.cursor/01 §Key Principles`). Before
each extraction ask: does this remove more concepts than it adds? If a split
creates a class with one caller and no independent purpose, or a base class
with one subclass, don't do it.

After the last chunk: `uv run --frozen pytest -q tests/units/architecture`, then
tighten the shrink-only baseline with
`uv run --frozen python -m tests.units.architecture.ratchet` and commit the
smaller `baseline.json`.

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
| Several public classes in one file | Move Class to its own file (records: group by aggregate root) |
| Crowded folder (> ~12 modules, or ≥ 3 sharing a topic) | Move the topic's modules into a sub-package (`.cursor/07 §Package Organisation 5`) |
| Facade whose methods only forward | Remove Middle Man — callers import the real class |
| Siblings copying the same loop/flow | Form Template Method in a base class |
| Subclass overriding most of its base | Extract Superclass (intermediate base) or Replace Subclass with Delegate |
| `if/elif` ladder on a kind | Replace Conditional with Polymorphism |
| Hook, parameter, or layer "for later" | Inline Class / Remove Parameter |

## Don't

- Don't bundle behavioural changes with a refactor — separate commits.
- Don't leave backward-compat wrappers, aliases, or shim modules after a move or rename: update every caller (src and tests, including `patch("...")` targets) in the same change and delete the old module. Only external contracts (REST `v1`, MCP tools, string-referenced entry points) keep their names (`.cursor/07 §Package Organisation`).
- Don't add entries to `tests/units/architecture/baseline.json` — it may only shrink.
- Don't add abstraction the code doesn't need yet; the simplest version wins.
- Don't skip the changelog (`changelog` skill).
- Don't add or leave non-English comments, logs, changelog text, audit-trail
  strings, or traces (`.cursor/05 §English-only artifacts`). Translate in place
  when you touch them.
