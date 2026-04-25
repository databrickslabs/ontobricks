---
name: code-review
description: Use when the user asks for a code review, asks to "review the code", or requests review of a feature/PR/branch. Runs the OntoBricks review checklist defined in .cursorrules.
---

# OntoBricks code review

The review steps are canonical in **`.cursorrules`** ("When asking for a code
review, do all these tasks (in this order)…"). Read it first; this skill only
sequences the work and adds Claude-Code-specific tooling notes.

## Procedure

Create a TodoWrite with one item per `.cursorrules` review step, in order:

1. **Rule compliance** — walk changed files, flag every violation. Cite the
   specific canonical file and section (e.g. "violates `.cursor/05 §Error
   Handling`"). The most common violations are listed at the bottom of this
   skill as a quick-reference checklist; the rules themselves live in the
   canonical files, not here.
2. **Duplication** — `Grep` for similar function names, repeated SQL strings,
   recurring try/except patterns. Propose Extract Function/Class/Move Function
   per Fowler vocabulary (see `src/.coding_rules.md`).
3. **Dead code** — propose, do not delete blindly. List candidates with a
   one-line rationale.
4. **Session-data audit** — open `src/back/objects/session/DomainSession.py`
   and any other session storage. For each field output `field | used? |
   derivable? | recommendation`.
5. **Tests** — `uv run pytest -q`. Report PASS/FAIL counts.
6. **Documentation** — Sphinx + README per `.cursor/08-testing-and-deployment §Documentation Rules`.

End with a summary block:

```
Review summary
- Rule violations: <count> (fixed: <count>, plan: <count>)
- Duplication clusters: <count>
- Dead code candidates: <count>
- Session fields removed: <count>
- Tests: <passed>/<total>
- Docs updated: <yes/no>
```

## Quick-reference: the violations I see most often

This is a memory aid for grep-style spotting. Authoritative wording lives in
the canonical files, not here.

- Bare `HTTPException` or `{'success': False, ...}` → `.cursor/10`
- `print()` or f-string in `logger.*(...)` → `.cursor/10 §Logging`
- Business logic in routes → `.cursor/05`, `.cursor/07`
- HTTP types in `back/core/` or `Request`/`Response` in `back/objects/` → `.cursor/07`
- Inline CSS/JS in templates → `.cursor/05 §Frontend`
- Secrets or query results on a domain object → `.cursor/02`, `.cursor/07 §Domain Security`
- Multiple public classes per file, or filename not PascalCase → `.cursor/01`, `.cursor/07 §Class-First Policy`

## Don't

- Don't refactor and review in the same pass without explicit approval — propose first.
- Don't paraphrase the rules in your review output — cite the canonical file.
- Don't claim "all good" without running the tests.
