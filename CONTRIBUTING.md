# Contributing to OntoBricks

The complete contributor guide is **[docs/contributing.md](docs/contributing.md)**.

That document covers local setup, where code goes, style, testing, the AI/agent eval gate, changelog paths, Conventional Commits, branches, PRs, docs/Sphinx, releases, and supply-chain rules.

Quick start:

```bash
uv sync --frozen --extra lakebase
uv run --frozen python run.py
uv run --frozen pytest -q -m "not scenario"
```

Related:

- [docs/development.md](docs/development.md) — dependencies and the full test campaign
- [docs/code_organization.md](docs/code_organization.md) — code map and layering
- [docs/PR_REVIEW_CHECKLIST.md](docs/PR_REVIEW_CHECKLIST.md) — reviewer gates
- [src/.coding_rules.md](src/.coding_rules.md) — long-form coding rules
