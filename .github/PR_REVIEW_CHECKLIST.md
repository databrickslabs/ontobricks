# PR Review Checklist

Standalone reviewer reference (not product documentation). Cross-linked from
`.github/PULL_REQUEST_TEMPLATE.md`.

Reviewers should walk the items in order. Items 1–10 are hard gates; CI enforces most but a reviewer's job is to catch what CI misses (intent, naming, taste).

> **Convention:** when you flag an item, cite the number and a one-line reason. `#3: missing changelog for src/back/objects/digitaltwin/DigitalTwin.py` is more useful than "no changelog".

---

## 1. Layering (§ `src/.coding_rules.md` §1)

- `back/core/` imports anything from `fastapi`? → **block**.
- `back/objects/` imports `Request` or `Response`? → **block**.
- Route file does more than 10 lines of business logic? → **block**.

## 2. Class-first policy (§ `src/.coding_rules.md` §2)

- New `.py` in `back/objects/` or `back/core/` with no public class? → **request changes**.
- Public class name doesn't match filename? → **request changes**.
- Module-level functions doing what a class should? → **request changes**.

## 3. Error handling (§ `src/.coding_rules.md` §4)

- `return {"success": False, ...}` anywhere? → **block**.
- Bare `HTTPException` in `back/core/` or `back/objects/`? → **block**.
- Broad `except Exception:` swallow? → **request changes** (unless explicitly justified).
- New error condition without a matching `OntoBricksError` subclass? → **request changes**.

## 4. Logging (§ `src/.coding_rules.md` §6)

- f-string or `.format()` in `logger.*`? → **request changes**.
- Token, password, JWT, or PII in a log line? → **block**.
- `print(...)` left in `src/`? → **request changes**.

## 5. Async + I/O (§ `src/.coding_rules.md` §5)

- `databricks-sql-connector` call inside `async def` without `to_thread`? → **request changes**.
- `asyncio.create_task(...)` ad-hoc instead of `TaskManager`? → **request changes**.

## 6. Public API & re-exports (§ `src/.coding_rules.md` §7)

- New public class in `back/objects/<subpackage>/` not re-exported from `__init__.py`? → **request changes**.
- Caller imports from the file path instead of the package? → **request changes**.

## 7. Tests (§ Section 9 of methodology plan)

- New behaviour in `src/` without a matching test diff in `tests/`? → **block**.
- Test name describes implementation, not behaviour? → **request changes**.
- Inline sample dict where a factory exists? → **request changes** (factories live in `tests/fixtures/factories/`).
- Missing pytest marker (`unit` / `integration` / `mcp` / `db` / `e2e` / `eval` / `property`)? → **request changes**.
- New code lowers package coverage below threshold in `ci/coverage_thresholds.yaml`? → **block** (CI will too).

## 8. Changelog (§ `changelogs/README.md`)

- `changelogs/<today>.log` not updated? → **block** (CI will too).
- Changelog entry has no "Tests" line? → **request changes**.
- Title isn't imperative ("add X" / "fix Y" / "refactor Z")? → **request changes**.

## 9. Conventional Commits (§ `commitlint.config.js`)

- PR title doesn't match `<type>(<scope>): <subject>`? → **block** (CI will too).
- Allowed types: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `revert`.
- Scope is the package (`dtwin`, `ontology`, `mapping`, `agents`, `mcp`, `ci`, `tests`) or a milestone tag (`M2.P1`).

## 10. AI features (§ `.cursor/12-ai-feature-lifecycle.mdc`)

If the PR touches `src/agents/**` or adds an MLflow-traced LLM call:

- `.planning/<slug>/SPEC.md` present? → **block** (CI G2 gate will too).
- `.planning/<slug>/eval/dataset.jsonl` present with ≥10 (changes) or ≥20 (new agent) examples? → **block**.
- MLflow eval run URI linked in the PR body? → **block**.
- Judge score ≥ baseline + delta or explicit waiver comment? → **block**.

## 11. Soft signals (reviewer judgment)

- Could a Fowler refactoring make this clearer? Suggest it (cite the smell + name).
- Is the diff bigger than ~400 LOC? Ask whether it should be split.
- Does the PLAN.md in `.planning/<slug>/` exist and match the actual diff? Mismatch = either the plan changed or the implementation drifted. Either way, update.
- Could this be a one-line fix instead of N? Suggest the smaller version.

## 12. Anti-patterns specific to CNS (§ §3.12 of methodology plan)

- Using Claude Code for a one-file edit when Cursor would do? → comment, don't block.
- Cursor Agent walking a 20-file refactor without a parallel-agent sweep? → comment.
- Re-introducing `gsd-*` references? → **block**. (Pre-commit hook should have caught it.)
- Updating `.cursor/*.mdc` priority without bumping the comment? → **request changes**.
- Resuming work by re-reading chat history (visible in commit messages or PR description)? → comment ("re-derive from `PLAN.md` + `git status` next time").

---

## After approval

The author runs `superpowers:finishing-a-development-branch`, then merges. The reviewer checks the merge is clean and the milestone in `.planning/ROADMAP.md` is updated with the landing date.
