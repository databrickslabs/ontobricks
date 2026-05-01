---
name: changelog
description: Use after any code change (feature, fix, refactor, review fixup) to update /changelogs/YYYY-MM-DD.log and run the test suite. Mandatory post-change routine — see .cursorrules.
---

# Post-change routine

The requirement is canonical in **`.cursorrules`** ("After ANY code
change…"). This skill only gives the section template, since `.cursorrules`
specifies *what* must be present but not the exact layout.

## Procedure

1. **Get today's date** — `Shell`: `date +%F` → filename `changelogs/YYYY-MM-DD.log`.
2. **Append** if the file exists, **create** if not. One file per day, multiple sections allowed.
3. **Write the section** in the layout below.
4. **Run tests** — `uv run pytest -q`. Paste the final summary line into `Tests:`.
5. If tests fail: do **not** mark the change complete. Fix and re-run, or surface the failures explicitly.
6. **Sphinx** — see `.cursor/08-testing-and-deployment §Sphinx API Documentation` if you added/removed/renamed public Python symbols.
7. **README / docs** — update if user-visible behaviour changed.

## Section layout

```
## <Title — short, imperative>

Context: <2–6 lines: what triggered the change, what it solves. Reference
user-visible symptoms or technical drivers.>

Changes:

1. <relative/file/path.py>
   <one-line description of the change in this file>
2. <relative/file/path.js>
   <one-line description>
…

Modified files:
- <relative/file/path.py>
- <relative/file/path.js>
- …

Tests: <e.g. "uv run pytest -q → 312 passed, 0 failed in 18.4s">
```

## Style

- Short titles. Detail goes in `Context:` and per-file lines.
- Cite Fowler refactorings by name when applicable.
- Cite the canonical rule when enforcing one (e.g. "complies with `.cursor/10 §Error Handling`").
- No diffs in the log — paths + one-liners only.
