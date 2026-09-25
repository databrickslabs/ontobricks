# Contributor Guide

How to set up a local environment, change OntoBricks, and land a pull request.

This is the **workflow** guide. Deep dives live next to it:

| Need | Document |
|------|----------|
| Dependencies, full test campaign, permissions / SDK | [development.md](development.md) |
| Code map, layering, logging, error hierarchy | [code_organization.md](code_organization.md) |
| Long-form coding rules and Fowler catalog | [src/.coding_rules.md](../src/.coding_rules.md) |
| Reviewer pass (numbered gates) | [PR_REVIEW_CHECKLIST.md](PR_REVIEW_CHECKLIST.md) |
| Deploy / Databricks Apps | [deployment.md](deployment.md), [DEPLOY_CHECKLIST.md](DEPLOY_CHECKLIST.md) |
| First-run operator setup | [get-started.md](get-started.md) |
| Changelog format | [changelogs/README.md](../changelogs/README.md) |

GitHub also looks at the repo-root [CONTRIBUTING.md](../CONTRIBUTING.md), which points here.

---

## Table of contents

1. [Code of conduct](#1-code-of-conduct)
2. [Prerequisites](#2-prerequisites)
3. [Fork and clone](#3-fork-and-clone)
4. [Development setup](#4-development-setup)
5. [Day-to-day loop](#5-day-to-day-loop)
6. [Where new code goes](#6-where-new-code-goes)
7. [Code style](#7-code-style)
8. [Frontend](#8-frontend)
9. [Testing](#9-testing)
10. [AI / agent features](#10-ai--agent-features)
11. [Changelog](#11-changelog)
12. [Commits](#12-commits)
13. [Branches](#13-branches)
14. [Pull requests](#14-pull-requests)
15. [Documentation](#15-documentation)
16. [Versioning and releases](#16-versioning-and-releases)
17. [Supply chain](#17-supply-chain)
18. [License](#18-license)
19. [Questions](#19-questions)

---

## 1. Code of conduct

Be respectful and professional. OntoBricks is a Databricks Labs project: exploratory, AS-IS, no SLA. File bugs as GitHub issues; do not open Databricks support tickets for Labs software.

---

## 2. Prerequisites

- **Python 3.10+** (`requires-python` in `pyproject.toml`)
- **[uv](https://docs.astral.sh/uv/)** for deps and the lockfile
- **Git**
- Optional for local Databricks calls: [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) (`databricks auth login`)
- Optional for browser e2e: Playwright Chromium (`uv run --frozen playwright install chromium`)

```bash
pip install uv
```

---

## 3. Fork and clone

1. Fork [databrickslabs/ontobricks](https://github.com/databrickslabs/ontobricks).
2. Clone your fork and add upstream:

```bash
git clone https://github.com/YOUR_USERNAME/ontobricks.git
cd ontobricks
git remote add upstream https://github.com/databrickslabs/ontobricks.git
```

Do not commit on `main`. One concern per branch; delete the branch after merge.

---

## 4. Development setup

### Install

```bash
uv sync --frozen --extra lakebase
# or
make install
```

Always pass **`--frozen`**. A bare `uv run` / `uv sync` re-resolves against the internal PyPI proxy, rewrites `uv.lock` artifact URLs, and **breaks the next Databricks Apps deploy**. After any `uv` command, `git status` must show `uv.lock` clean unless you intentionally re-locked. See [development.md](development.md) and `.cursor/09-package-management.mdc`.

### Environment

```bash
cp .env.example .env
```

If the workspace blocks PAT generation, leave `DATABRICKS_TOKEN` empty and use CLI auth (`databricks auth login`). The app reads `~/.databrickscfg`; set `DATABRICKS_CONFIG_PROFILE` for a non-default profile.

Operator-facing Databricks, Lakebase, warehouse, and volume setup is in [get-started.md](get-started.md).

### Run locally

```bash
uv run --frozen python run.py
# or
make dev
```

- App: http://localhost:8000
- Swagger: http://localhost:8000/docs

Entry point: `run.py` → `create_app` in `src/shared/fastapi/main.py`.

---

## 5. Day-to-day loop

1. Branch from up-to-date `main` (`feat/…`, `fix/…`, …).
2. Implement in the right layer (routes → domain → core).
3. Add or update tests next to the behaviour.
4. Run the **routine suite**: `uv run --frozen pytest -q -m "not scenario"`.
5. Append a changelog section under `changelogs/vX.X.X/` (version from `pyproject.toml`).
6. Update `/docs` when behaviour or structure changed.
7. Open a PR with a Conventional Commit title.

English-only for comments, changelogs, logger templates, audit-trail strings, traces, and agent notes — even if the conversation is in another language. Product UI copy may stay localised.

---

## 6. Where new code goes

Three-layer backend. Routes stay thin.

```
src/
├── front/      HTML routes, Jinja2, static assets
├── api/        External REST + internal JSON
├── back/
│   ├── objects/   Domain classes (business logic). No Request/Response.
│   └── core/      Infrastructure (Databricks, W3C, graph DB). No FastAPI types.
├── shared/     App factory, middleware, settings
├── agents/     LLM engines (SPEC + eval required)
└── mcp-server/ Separate MCP Databricks App
```

| New thing | Goes in |
|-----------|---------|
| HTML page | `front/routes/<area>/` + `front/templates/<area>/` |
| Internal JSON | `api/routers/internal/` |
| External REST | `api/routers/v1.py` (stateless) |
| Domain method | `back/objects/<entity>/` |
| Databricks / W3C / graph engine | `back/core/` |
| LLM agent | `src/agents/<name>/` + SPEC + eval dataset |
| Shared pure helper | `back/core/helpers/` |

`service.py` files are legacy. Prefer routes → domain class. New subpackages: follow the checklist in [code_organization.md](code_organization.md) §11.3 (and the `adding-subpackage` skill).

---

## 7. Code style

Canonical rules: [src/.coding_rules.md](../src/.coding_rules.md) and `.cursor/05-code-style-and-structure.mdc`.

**Python**

- Type hints; `async def` for I/O (FastAPI handlers, SDK).
- **Class-first**: one public class per file, PascalCase filename matching the class (`OWLRLReasoner.py`).
- Raise `OntoBricksError` subclasses (`ValidationError`, `NotFoundError`, `AuthorizationError`, `ConflictError`, `InfrastructureError`). Never `{'success': False, …}` or bare `HTTPException` from domain/core.
- Logging: `from back.core.logging import get_logger`; **%-style** only (`logger.error("Error for user %s", email, exc_info=True)`). Never `print()`, never f-strings in log calls.
- Re-export public symbols from package `__init__.py`; consumers import the package, not the module file.
- Format: `black`. Lint: `flake8` (see CI). `make format` / `make lint`.

```python
@router.post("/entity/add")
async def add_entity_mapping(request: Request, session_mgr=Depends(get_session_manager)):
    data = await request.json()
    domain = get_domain(session_mgr)
    _, new_mapping = Mapping(domain).add_or_update_entity_mapping(data)
    return {"success": True, "mapping": new_mapping}
```

**Naming**

- Python class files: `PascalCase.py`. Modules without a primary class: `snake_case.py`.
- Templates: `snake_case.html`, partials `_prefix.html`.
- Tests: `test_*.py`.

---

## 8. Frontend

Details: `.cursor/11-frontend-design.mdc`.

- No inline CSS or JavaScript in templates. Assets under `src/front/static/<area>/{js,css}/`.
- HTML routes return `TemplateResponse` only — no business logic, no JSON.
- Design tokens: `--db-*` in `main.css` for the app shell; `--ovz-*` only inside OntoViz. Do not hard-code colours that have a token.
- Never `alert()` / `confirm()` / `prompt()`. Use `showConfirmDialog` / `showDeleteConfirm` / `showNotification` in `front/static/global/js/utils.js`.
- Cache busting: `?v={{ asset_version }}` on `<script>` and `<link>`. CSRF: state-changing requests send `X-CSRF-Token`; tests set `CSRF_DISABLED=1`.

UI changes: exercise the flow (not a single screenshot). Check shared state on other pages.

---

## 9. Testing

Routine command (post-change, review, iteration):

```bash
uv run --frozen pytest -q -m "not scenario"
```

Expect **0 failures**. `--frozen` is mandatory.

| Suite | When | Command |
|-------|------|---------|
| Routine (default) | Every change | `uv run --frozen pytest -q -m "not scenario"` |
| One file | Debugging | `uv run --frozen pytest tests/path/test_foo.py -q` |
| Browser e2e | Explicitly targeting `tests/e2e` | `uv run --frozen pytest tests/e2e` |
| Scenarios | **Only if asked** | `uv run --frozen pytest -m scenario` |
| MCP | MCP server changes | `uv run --frozen pytest tests/mcp/ -m mcp` |
| Live HTTP smoke | Deployed app | `tests/live_integration/` with `ONTOBRICKS_LIVE_BASE` |

Do **not** run `tests/e2e/scenarios/` in the default loop. Live scenario campaigns need a running app + warehouse + LLM (`make scenario-campaign`, `ONTOBRICKS_SCENARIO_LIVE=1`). They mutate a shared registry — treat as billable and coordinated.

**Writing tests**

- New behaviour in `src/` needs a matching `tests/` diff. Name tests after behaviour, not implementation.
- Use factories in `tests/fixtures/factories/` instead of inline sample dicts.
- Mark tests (`unit`, `integration`, `mcp`, `db`, `e2e`, `eval`, `property`, `scenario`).
- Full campaign layout, markers, and coverage notes: [development.md](development.md) (OntoBricks Test Campaign).

**Live integration** (optional; deployed Databricks App):

```bash
databricks auth login --profile <profile> --host https://<workspace>.cloud.databricks.com
export ONTOBRICKS_LIVE_BASE=https://<app>.databricksapps.com
export DATABRICKS_CONFIG_PROFILE=<profile>
uv run --frozen pytest tests/live_integration/ -v -m live_integration --no-cov
```

Mutating live e2e: `ONTOBRICKS_LIVE_ALLOW_MUTATING=1` — the int workspace is shared.

---

## 10. AI / agent features

Any change under `src/agents/**`, a new MLflow-traced LLM call, or an MCP tool wrapping an agent follows `.cursor/12-ai-feature-lifecycle.mdc`.

**Required in the PR**

1. `.planning/<slug>/SPEC.md` (prompt, tools, success/failure, metrics).
2. Eval dataset: ≥ **20** examples for a new agent, ≥ **10** for a material change (`tests/eval/datasets/<agent>/` and/or `.planning/<slug>/eval/dataset.jsonl` as the gate currently expects).
3. Linked **MLflow eval run URI** in the PR body.
4. Passing eval (judge ≥ baseline + delta) **or** an explicit reviewer waiver.

No new agent without SPEC. No prompt/tool tweak without an eval delta. Trace every Foundation Model API path (`@trace_agent` / `@trace_llm` / `@trace_tool` in `src/agents/tracing.py`). Judge model must not be the same endpoint as the agent.

---

## 11. Changelog

Mandatory after **any** code change (docs included). Source of truth for the version is `pyproject.toml`.

Path:

```text
changelogs/vX.X.X/<github-user>_YYYY-MM-DD.log
```

Example: version `0.8.1`, user `benoitcayladbx`, 25 Sep 2026 → `changelogs/v0.8.1/benoitcayladbx_2026-09-25.log`.

If the file exists, **append** a new `##` section. One file per user per day.

```markdown
## Short imperative title

Context: Why this change was needed (2–6 lines).

Changes:

1. path/to/file.py
   One-line description
2. path/to/other.md
   One-line description

Modified files:
- path/to/file.py
- path/to/other.md

Tests: uv run --frozen pytest -q -m "not scenario" → N passed, 0 failed in Xs
```

Write the **entire** section in English. CI (`.github/workflows/changelog-presence.yml`) fails PRs that touch `src/` or `tests/` without a `changelogs/` diff unless the PR has the `no-changelog` label.

---

## 12. Commits

[Conventional Commits](https://www.conventionalcommits.org/). Enforced on PR titles (`lint-pr-title.yml`, `commitlint.config.js`).

```
<type>(<scope>): <description>
```

| Type | Use |
|------|-----|
| `feat` | New feature |
| `fix` | Bug fix |
| `docs` | Documentation only |
| `style` | Formatting |
| `refactor` | No behaviour change |
| `perf` | Performance |
| `test` | Tests |
| `build` | Build / deps |
| `ci` | CI |
| `chore` | Neither src nor tests |
| `revert` | Revert |

Scopes (examples): `backend`, `frontend`, `api`, `ontology`, `mapping`, `dtwin`, `domain`, `graphdb`, `triplestore`, `reasoning`, `agents`, `mcp`, `ci`, `tests`.

Breaking change: `feat(api)!: …` or a `BREAKING CHANGE:` footer.

```
feat(ontology): add SHACL validation on import
fix(mapping): correct R2RML generation for nested properties
docs: add contributor guide under docs/
```

---

## 13. Branches

```
<type>/<scope>-<short-description>
```

Lowercase, hyphens, no spaces. Short-lived. Special: `release/<version>`, `hotfix/<description>`.

| Intent | Branch |
|--------|--------|
| SHACL on import | `feat/ontology-shacl-validation` |
| R2RML nested props | `fix/mapping-r2rml-nested-props` |
| This guide | `docs/contributor-guide` |
| v0.8.1 prep | `release/0.8.1` |

---

## 14. Pull requests

Template: [`.github/PULL_REQUEST_TEMPLATE.md`](../.github/PULL_REQUEST_TEMPLATE.md). Reviewer list: [PR_REVIEW_CHECKLIST.md](PR_REVIEW_CHECKLIST.md). CODEOWNERS: `@databrickslabs/ontobricks-maintain`.

### Before you open

```bash
git fetch upstream
git rebase upstream/main
uv run --frozen pytest -q -m "not scenario"
```

Confirm `uv.lock` is untouched.

### PR bar

- **Title**: Conventional Commit (`feat(ontology): add OWL import from URL`).
- **Body**: what / why / how to test; screenshots for UI; plan path (`.planning/<slug>/PLAN.md`) when used.
- **Size**: one concern. Split large features.
- **Author checklist** (from the template): tests for behaviour changes, changelog, agents SPEC/eval if applicable, no `gsd-*` references.
- **Reviews**: at least one approval; address comments. Reviewers cite checklist numbers (`#3: missing OntoBricksError subclass`).

After merge: delete the branch; pull `main`.

---

## 15. Documentation

- User-facing behaviour → [user-guide.md](user-guide.md), [features.md](features.md), examples.
- Structure / APIs → [architecture.md](architecture.md), [api.md](api.md), [code_organization.md](code_organization.md).
- New or dropped Python deps / CDN libs → table in [development.md](development.md).
- Public Python symbols added/removed/renamed → Sphinx under `docs/sphinx/` (gitignored sources; build with `scripts/build_docs.sh`). New subpackages need a matching `.rst` and toctree entry.
- Docstrings: Google or NumPy style, **English**.

Help Center in the app serves the `/docs` markdown set. Keep this file accurate; do not duplicate large tables that already live in `development.md`.

---

## 16. Versioning and releases

[SemVer](https://semver.org/). Version field in `pyproject.toml` is the source of truth.

Typical release flow:

1. `release/<version>` from up-to-date `main`.
2. Bump `pyproject.toml`.
3. Release notes from `changelogs/vX.X.X/` using `releases/ReleaseNotes_V0.2.0.md` as template → `releases/ReleaseNotes_Vx.x.x.md`.
4. Tag `vX.X.X`, GitHub Release, then deploy (`make deploy` / DAB — [deployment.md](deployment.md)).

Operators on 0.7.x → 0.8: [update_to_0.8.md](../update_to_0.8.md).

---

## 17. Supply chain

Required for `databrickslabs` repos.

**GitHub Actions** — pin to full SHA with a version comment:

```yaml
uses: actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5 # v4
```

Dependabot: `.github/dependabot.yml`. Workflows declare a minimal `permissions:` block (`contents: read` unless more is required).

**Python** — declare in `pyproject.toml`; lock with uv on the public CDN (`files.pythonhosted.org` URLs in `uv.lock`).

---

## 18. License

Contributions are licensed under the project [Databricks License](../LICENSE.txt).

---

## 19. Questions

- Bugs and features: GitHub Issues (search first).
- Design / “how should this work”: GitHub Discussions if enabled, otherwise an issue.
- In-repo architecture questions: [code_organization.md](code_organization.md) and [architecture.md](architecture.md).

Thank you for contributing to OntoBricks.
