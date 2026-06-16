# Changelogs

Per `.cursorrules`: every code change appends a section to `changelogs/<user>_<YYYY-MM-DD>.log`. One file per day and per user; multiple sections per file when multiple changes land same-day.

## Format

```markdown
## <Title — short imperative>

**Context:** Why this change was needed.

**Changes:**
1. `path/to/file.py` — short description
2. `path/to/another.py` — short description

**Files modified:**
- `path/to/file.py`
- `path/to/another.py`

**Tests:** `uv run pytest tests/<scope>/` — N passed, M failed
```

## When to append vs create

- If `changelogs/<user>_<today>.log` exists: append a new `##` section.
- If not: create the file with today's date and a single section.
- Date format: ISO `YYYY-MM-DD` (UTC of the developer's machine).

## Multi-agent collision protocol

When two agents work the same day, each appends its own `##` section. Section titles must be unique within a day; suffix with the worktree slug if needed: `## SHACL filter (worktree shacl-severity-a1b2c3d4)`.

## CI enforcement (M3.P2)

A planned CI gate (`.github/workflows/changelog-presence.yml`) will fail PRs that change `src/` or `tests/` without a matching diff under `changelogs/`. Bypass with the `no-changelog` label for trivial PRs.
