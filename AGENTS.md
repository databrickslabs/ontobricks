# AGENTS.md

Pointer file for AI coding agents (Codex, Gemini CLI, OpenAI Agents, etc.).

The single source of truth for OntoBricks project conventions is the trio:

- `.cursorrules` — workflow contract (post-change routine, code-review order, deploy)
- `.cursor/*.mdc` — modular, scoped rules
- `src/.coding_rules.md` — long-form refactoring guide

Cursor reads them natively. Claude Code reads them via `@`-imports in
`CLAUDE.md`. Other agents should read those files directly — start with
`CLAUDE.md`, which lists the full set.

**Do not edit `CLAUDE.md` or `AGENTS.md` to add rules. Edit the canonical
files above.**
