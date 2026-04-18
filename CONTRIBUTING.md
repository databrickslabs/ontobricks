# Contributing to OntoBricks

Thank you for your interest in contributing to OntoBricks! This document provides guidelines and instructions for contributing to the project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Commit Guidelines](#commit-guidelines)
- [Versioning](#versioning)
- [Release Process](#release-process)
- [Pull Request Process](#pull-request-process)
- [Code Style](#code-style)
- [Testing](#testing)
- [License](#license)

---

## Code of Conduct

Please be respectful and professional in all interactions. We're building a collaborative community around ontology management and knowledge graph engineering on Databricks.

---

## Getting Started

### Prerequisites

- **Python 3.10+** (as defined in `pyproject.toml`)
- **[uv](https://docs.astral.sh/uv/)** (Python dependency management):
  ```bash
  pip install uv
  ```

### Fork and Clone

1. Fork the repository on GitHub
2. Clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/ontobricks.git
   cd ontobricks
   ```
3. Add the upstream remote:
   ```bash
   git remote add upstream https://github.com/databrickslabs/ontobricks.git
   ```

---

## Development Setup

### 1. Install Dependencies

```bash
# Install all Python dependencies (managed by uv via pyproject.toml)
uv sync
```

### 2. Configure Environment

Create a `.env` file in the project root:

```bash
cp .env.example .env
# Edit .env with your configuration
```

### 3. Start the Development Server

```bash
# Run the app locally with auto-reload
uv run python run.py
```

- App: http://localhost:8000
- API Docs (Swagger): http://localhost:8000/docs

---

## Commit Guidelines

We use [Conventional Commits](https://www.conventionalcommits.org/) for all commit messages. This enables automatic changelog generation and semantic versioning.

### Commit Message Format

```
<type>(<scope>): <description>

[optional body]

[optional footer(s)]
```

### Types

| Type | Description |
|------|-------------|
| `feat` | A new feature |
| `fix` | A bug fix |
| `docs` | Documentation only changes |
| `style` | Code style changes (formatting, missing semicolons, etc.) |
| `refactor` | Code change that neither fixes a bug nor adds a feature |
| `perf` | Performance improvements |
| `test` | Adding or updating tests |
| `build` | Changes to build system or dependencies |
| `ci` | Changes to CI configuration |
| `chore` | Other changes that don't modify src or test files |
| `revert` | Reverts a previous commit |

### Scope (optional)

The scope provides additional context. Common scopes include:

- `backend` - Python/FastAPI backend changes
- `frontend` - Jinja2 templates, JS, CSS changes
- `api` - API endpoint changes (internal or external)
- `ontology` - Ontology management feature
- `mapping` - Entity/relationship mapping feature
- `dtwin` - Digital Twin / knowledge graph feature
- `domain` - Domain management (UC save/load, metadata)
- `graphdb` - Graph database engine (LadybugDB)
- `triplestore` - Triple store / Delta views
- `reasoning` - OWL 2 RL, SWRL, SHACL reasoning
- `agents` - LLM agent engines
- `mcp` - MCP server integration

### Examples

```bash
# Feature
feat(ontology): add SHACL validation on import

# Bug fix
fix(mapping): correct R2RML generation for nested properties

# Documentation
docs: update API documentation for digital twin endpoints

# Refactoring
refactor(backend): extract SPARQL translation into dedicated service

# Breaking change (use ! or BREAKING CHANGE footer)
feat(api)!: change digital twin query response format

# With body and footer
feat(dtwin): add community detection via NetworkX

Implements Louvain-based community detection on the materialized
knowledge graph. Results are exposed through the internal API.

Closes #42
```

### Pre-commit Checks

Before committing, ensure:

1. **Tests pass**:
   ```bash
   uv run pytest -q
   ```

2. **Commit message follows convention**

---

## Versioning

We use [Semantic Versioning](https://semver.org/) (SemVer):

- **MAJOR** (X.0.0): Breaking changes
- **MINOR** (0.X.0): New features (backward compatible)
- **PATCH** (0.0.X): Bug fixes (backward compatible)

### Version File

The project version is tracked in `pyproject.toml` (source of truth).

---

## Release Process

### 1. Prepare Release

```bash
# Ensure you're on main and up to date
git checkout main
git pull upstream main

# Create release branch (optional for larger releases)
git checkout -b release/0.2.0
```

### 2. Update Version

Edit the `version` field in `pyproject.toml`.

### 3. Update Changelog

Add release notes to the relevant changelog file under `/changelogs/`.

### 4. Commit and Tag

```bash
git add -A
git commit -m "chore: bump version to 0.2.0"
git tag v0.2.0
```

### 5. Push

```bash
git push origin main
git push origin v0.2.0
# Or if on release branch:
# git push origin release/0.2.0
# Then create PR and merge
```

### 6. Create GitHub Release

1. Go to GitHub Releases
2. Click "Draft a new release"
3. Select the tag `v0.2.0`
4. Add release notes (can be auto-generated from commits)
5. Publish

### 7. Deploy

```bash
databricks bundle deploy
databricks apps deploy <app-name>
```

---

## Pull Request Process

### Before Submitting

1. **Sync with upstream**:
   ```bash
   git fetch upstream
   git rebase upstream/main
   ```

2. **Run tests**:
   ```bash
   uv run pytest -q
   ```

### PR Guidelines

1. **Title**: Use conventional commit format
   - `feat(ontology): add OWL import from URL`
   
2. **Description**: Include:
   - What changes were made
   - Why the changes were needed
   - How to test the changes
   - Screenshots for UI changes

3. **Size**: Keep PRs focused and reasonably sized
   - Large features should be broken into smaller PRs

4. **Reviews**: 
   - At least one approval required
   - Address all review comments

### After Merge

- Delete your feature branch
- Pull latest main to your local

---

## Code Style

### Python (Backend)

- Follow a layered architecture: routes (HTTP) -> domain classes (`back/objects`) -> core infrastructure (`back/core`).
- Use type hints extensively.
- Use `async def` for asynchronous operations (FastAPI route handlers, SDK calls).
- Follow the **class-first policy**: encapsulate behaviour in classes, one public class per file.
- Raise from the `OntoBricksError` hierarchy, never return `{'success': False, ...}` or bare `HTTPException`.
- Use %-style formatting in logging statements, never f-strings with dynamic variables.
- Use `from back.core.logging import get_logger`, never `print()`.

```python
async def get_ontology_classes(
    request: Request,
    session_mgr=Depends(get_session_manager),
) -> dict:
    """Retrieve all classes for the current ontology."""
    domain = get_domain(session_mgr)
    classes = Ontology(domain).get_classes()
    return {"success": True, "classes": classes}
```

### Frontend (HTML/CSS/JS)

- HTML templates use Jinja2 with `base.html` and partial templates for modularity.
- Templates must NOT contain inline CSS or JavaScript.
- JavaScript files live in `src/front/static/<area>/js/`.
- CSS files live in `src/front/static/<area>/css/`.

### File Naming

- **Python**: `PascalCase.py` for class files (e.g., `SparqlTranslator.py`), `snake_case.py` for modules
- **Templates**: `snake_case.html`, partials prefixed with `_` (e.g., `_ontology_wizard.html`)
- **Tests**: `test_*.py`

---

## Testing

### Running Tests

```bash
# Run all tests
uv run pytest -q

# Run a specific test file
uv run pytest tests/test_home_service.py -q

# Run with verbose output
uv run pytest -v
```

### Writing Tests

- Place tests in the `tests/` directory
- Name tests descriptively: `test_sparql_translator_handles_optional_clauses`
- Use fixtures for common setup
- Aim for good coverage on new code

---

## Project Structure Overview

```
src/
├── back/           # Backend: domain classes (objects/), core infra (core/)
├── front/          # Frontend: Jinja2 templates, routes, static assets
├── shared/         # Shared infrastructure: app factory, middleware, config
├── api/            # API layer: external REST API, internal JSON API
└── agents/         # LLM agent engines (OWL generator, auto-assign, etc.)
```

Entry point: `run.py` imports `create_app` from `shared.fastapi.main`.

---

## License

By contributing to OntoBricks, you agree that your contributions will be licensed under the project's Databricks License (see LICENSE.txt).

---

## Supply Chain Security

This project follows GitHub Actions supply chain security best practices as required for `databrickslabs` repos.

### Action Pinning

All GitHub Actions in `.github/workflows/` must be pinned to full SHA commits with a version comment:

```yaml
# Correct
uses: actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5 # v4

# Incorrect
uses: actions/checkout@v4
```

Dependabot (`.github/dependabot.yml`) will automatically open PRs when new action versions are available.

### Python Dependency Pinning

Python dependencies are declared in `pyproject.toml` and managed by [uv](https://github.com/astral-sh/uv).

### Workflow Permissions

All workflows must declare a minimal `permissions` block at the workflow level:

```yaml
permissions:
  contents: read
```

Only add additional permissions (e.g., `issues: write`) if the workflow genuinely requires them.

---

## Questions?

- Open an issue for bugs or feature requests
- Start a discussion for questions or ideas
- Check existing issues before creating new ones

Thank you for contributing to OntoBricks!
