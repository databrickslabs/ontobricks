# OntoBricks on Databricks Marketplace — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make OntoBricks installable as a Databricks-authored (1P) app from the Databricks Marketplace, satisfying the Marketplace SOP so the Labs owner can publish it.

**Architecture:** Marketplace installs an app from a **subfolder** of a **tagged Git commit** and runs it as-is (no render or build step). We keep the dev repo layout unchanged. A release-time script copies the runtime file set into `dist/marketplace/ontobricks/` and drops in a **static, committed** `marketplace/app.yaml` + `marketplace/manifest.yaml`. A workflow commits that folder to a `marketplace` branch and tags it `marketplace-vX.Y.Z`. Separately, we close the two API paths that accept a caller-supplied PAT in Apps mode, so the FEIP self-certification is truthful.

**Tech Stack:** Python 3.11, FastAPI, PyYAML, pytest, GitHub Actions, Databricks Apps + Lakebase Autoscaling.

**Spec:** [Databricks-authored App on Marketplace SOP (Sept 2026)](https://docs.google.com/document/d/1g1lx7dD5AaTTH-j9nm7H4eeNRmb_gzhchZQs4DmStWs) + [Manifest.yaml for your Databricks App](https://databricks.atlassian.net/wiki/spaces/UN/pages/5101748287/Manifest.yaml+for+your+Databricks+App). FEIP: [FEIP-2484](https://databricks.atlassian.net/browse/FEIP-2484).

**Context / history:** Benoit asked Dermot to own this (Aug 2026, #ontrobricks-contributors). Jianyu Zhou (Marketplace team) pointed to the SOP in #marketplace (Oct 2026). Customer pull: Romy Li, whose customer's data modeller can't drive a DAB + CLI deploy without the platform team.

## Global Constraints

- Manifest `version: 1` (hardcoded schema version).
- `manifest.yaml` and `app.yaml` live at the **root of the app folder**; the app folder is **not** the repo root.
- Every manifest `resource_specs[].name` must have a matching `valueFrom` in `app.yaml`. Exception: the Lakebase `postgres` resource, whose `PG*` env vars the Apps platform injects automatically.
- The app **MUST NOT** ask for or store user PAT tokens.
- No extra build steps before the app is deployed: `app.yaml` contains no `${...}` placeholders.
- Open-source only (Apache-licensed repo in `github.com/databrickslabs`).
- Run tests with `uv run --frozen pytest -q -m "not scenario"`. Never bare `uv run`: it rewrites `uv.lock`.
- Changelog goes to `changelogs/v0.7.0/<github-user>_YYYY-MM-DD.log` (version from `pyproject.toml`).
- Logging is `%`-style only; errors are raised from the `OntoBricksError` hierarchy (`src/.coding_rules.md` §4, §6).

## Decisions & open questions — resolve BEFORE Task 1

| # | Question | Owner | Blocks |
|---|---|---|---|
| **Q1 (blocker)** | Lakebase has been the only registry backend since v0.4.0 (`src/back/objects/registry/store/__init__.py`). The manifest guide documents `secret_spec`, `sql_warehouse_spec`, `serving_endpoint_spec`, `job_spec` and `uc_securable_spec`, but **no Postgres/Lakebase spec**. (a) Does `manifest.yaml` support a Lakebase Autoscaling `postgres` resource (exact key + permission name)? (b) If so, can the installer *create* a new Lakebase project from the install screen, or only pick an existing one? If (a) is "no", **Task 7 is the way out**: install with warehouse + Volume only, and a first-run setup screen creates or attaches Lakebase. | Marketplace team (Jianyu Zhou / Tia Chang) | Task 1; decides whether Task 7 is required or a nice-to-have |
| Q2 | Is it acceptable for the publish tag to point at a commit on a release-only `marketplace` branch (assembled folder) rather than on `main`? Who can push branches/tags in `databrickslabs/ontobricks` from CI (`contents: write`)? If the tag must be on `main`, pull the **`/app` milestone** forward and drop Tasks 2 + 4. | Marketplace team + Jason Pohl / Alex Ott | Task 4 |
| Q3 | `app.yaml.template` declares user scopes `iam.current-user:read` + `iam.access-control:read`, which are **not** in the manifest's supported `user_api_scopes` list. Can we omit `user_api_scopes` and get the platform defaults (the same two scopes)? | Marketplace team / #databricks-apps | Task 1 |
| **Q4 (blocker for usability)** | `scripts/bootstrap/app-permissions.sh` grants the app's SP `CAN_MANAGE` on its own app. Without it, `PermissionMiddleware` (`src/shared/fastapi/main.py:383-395`) redirects to `/access-denied?reason=bootstrap`. Per its comment, that can lock out *everyone*. The `CAN_MANAGE` installer probably still gets in via their own token (`PermissionService.is_admin` tries the user token first), but every other user is locked out. Does a Marketplace install grant this automatically? If not: a documented post-install step, plus Task 7 spike S4 (can the first-run screen grant it?). | Verify in dogfood (Task 6) + spike S4 | Task 5 wording |
| Q5 | v1 listing scope: **excludes** the MCP companion app (`src/mcp-server`; one manifest = one app), the Neo4j secret, and the graph-analytics job (it's bundle-deployed; it's already off by default and the Analytics page names the missing prerequisite). OK? | Benoit | — |
| Q6 | Agree the **`/app` layout milestone** (end of this plan) as the release after the first listing, including dropping the generated `app.yaml` for `make deploy` too? | Benoit | `/app` milestone |
| **Q7 (blocker, 0.10.x)** | Since `8fad03ff` (0.10.0) the registry catalog/schema come only from `REGISTRY_CATALOG`/`REGISTRY_SCHEMA`, which are deploy-time values. A fixed Marketplace `app.yaml` can't carry them: Apps binds only CONNECTION/FUNCTION/TABLE/VOLUME securables, and Settings has no input for them. `RegistryCfg.is_configured` requires them before Initialize. Options: (a) a Settings input persisted in the Lakebase registry row (fits Task 7's first-run screen); (b) bind a Volume again, only to carry the location. | Benoit + Dermot | Initialize on a Marketplace install |

**Status (2026-11-24, later):** Q4 and Q7 have in-app fixes on this branch, verified on `ontobricks-mp-test-013`.
- Q4: the Marketplace manifest adds the `apps` user scope, so the admin gets in before any grant.
- Q7: **Settings → Registry → Set up the registry** runs the schema DDL and grants as the admin (`sql` scope), grants the service principal Can Manage (`apps` scope), then runs Initialize.
- `LISTING.md` is down to two steps after install.
- The `apps` scope needs a security note on FEIP-2484: it lets the app call the Apps API as the signed-in user.
- Task 7's create-a-Lakebase-project screen is no longer needed, because Q1 = yes.

**Status on 0.10.0 (2026-11-24):** Lakebase is back in the manifest, and the project-lookup fix is on this branch. Live re-test on `ontobricks-mp-test-011`:
- Lakebase is bound at install, and the project resolves from the app's own `postgres` resource.
- The registry location survives a plain redeploy.
- Q7 additionally needs the installer to create the schema and grant the service principal UC privileges on it (`marketplace/README.md` › Results 2026-11-24). Task 7's setup screen should run that DDL as the admin (OBO `sql` scope).

**Status on 0.10.0 (2026-11-12, branch `gvocode/marketplace-listing-0.10.0`):**
- **Q1 answered "yes" (corrected 2026-11-24).** The 2026-11-12 "no" was based on the SDK's `AppManifestAppResourceSpec` (0.82), which lacks a postgres spec. The model is just behind: Ontos, Lakemeter and the app-templates ship `postgres_spec` (`permission: CAN_CONNECT_AND_CREATE`). Lakebase is an install-time resource again. Q1(b) stands: the installer picks an existing project, so LISTING.md asks them to create one first. Task 7 becomes the nice-to-have that removes that step.
- **Q7 opened** (registry catalog/schema), see the table.
- **Startup bug found:** `httpx` was imported at runtime but was a dev-only dependency. The fix is on this branch.
- **Already in 0.10.0 upstream:** the PAT write in Settings save is gone, and the assistant route always overwrites caller credentials. Only the session-host rejection and `/api/v1` needed porting.
- **Live test in E2 dogfood (`ontobricks-mp-test-010`, see `marketplace/README.md` › Results):**
  - Q4 reproduces.
  - Q7 is confirmed, and it's the only thing blocking Initialize: with catalog/schema supplied, Initialize succeeds (it also creates a UC Volume, `OntoBricksRegistry`).
- **Found:** `LakebaseAuth.instance_name` walks every Lakebase project to match `PGHOST` and aborts on the first one it can't read. So without `LAKEBASE_PROJECT` (always the case on Marketplace), the registry row is never read back in a large workspace. Q7's fix must not depend on it. **Fix (2026-11-24):** resolve the endpoint from `LAKEBASE_PROJECT`/`LAKEBASE_BRANCH`, then from the app's own `postgres` resource (`DATABRICKS_APP_NAME`), and only then a full walk that skips unreadable projects.

**Status after the E2 dogfood test install (2026-11-03, `ontobricks-mp-test`, 0.7 base, see `marketplace/README.md` › Results):**
- **Q3 answered.** Effective user scopes default to both `iam.*` scopes with none declared, so the manifest can omit `user_api_scopes`.
- **Q4 answered: the grant is not automatic.** Without the service principal holding CAN_MANAGE on its own app, *everyone* is locked out, including the installer. That's now post-install step 1 in `LISTING.md`. Ask the Marketplace team whether the platform can grant it, or whether the app should stop depending on it.
- **Lakebase project CAN_USE is not needed.** The bound `postgres` resource was enough to connect.
- Tasks 1–3 are verified live.

**Deferred, not rejected:** a permanent `app/` subfolder. It's the better end state (tag on `main`, no assembly step, deploy and Marketplace ship the same folder). But it forces one fixed `app.yaml` for both deploy paths, which means a runtime `LAKEBASE_SCHEMA` + a migration for existing instances. So it's sequenced after the dogfood install (Task 6) proves the fixed `app.yaml` works. See **Follow-up milestone: `/app` layout**.

## File map

| File | Status | Responsibility |
|---|---|---|
| `marketplace/manifest.yaml` | create | Marketplace manifest: name, description, resource specs |
| `marketplace/app.yaml` | create | Static Apps config for Marketplace installs (no placeholders) |
| `marketplace/LISTING.md` | create | Listing description Markdown (SOP Phase 2) |
| `marketplace/README.md` | create | Maintainer runbook: how a Marketplace release is cut |
| `scripts/marketplace/assemble.py` | create | Copy runtime file set + static configs into `dist/marketplace/ontobricks/` |
| `.github/workflows/marketplace-publish.yml` | create | Assemble → commit to `marketplace` branch → tag |
| `Makefile` | modify | `marketplace-bundle` target + help line |
| `src/back/objects/domain/SettingsService.py:210-212` | modify | Reject `token` in `apply_config_save` under Apps mode |
| `src/back/core/helpers/DatabricksHelpers.py:30-43` | modify | `_domain_databricks` hides any session token in Apps mode; add `strip_caller_credentials` |
| `src/api/routers/internal/ontology.py:~2187` | modify | Assistant route drops caller `host`/`token` in Apps mode |
| `tests/units/scripts/test_marketplace_package.py` | create | Manifest ↔ app.yaml ↔ template consistency |
| `tests/units/scripts/test_marketplace_assemble.py` | create | Assembly behaviour |
| `tests/units/settings/test_no_pat_in_app_mode.py` | create | PAT hardening |
| `README.md` | modify | "Install from Databricks Marketplace" section |
| `src/back/core/databricks/lakebase/LakebaseProjectProvisioner.py` | create (Task 7) | Create/adopt a Lakebase Autoscaling project; resolve its default database path |
| `src/back/core/databricks/AppResourceBinder.py` | create (Task 7) | Add a `postgres` binding to the running app without dropping other resources |
| `src/back/objects/registry/LakebaseSetupService.py` | create (Task 7) | First-run orchestration via `TaskManager` (project → database → bind → apply) |
| `src/api/routers/internal/settings.py` | modify (Task 7) | `GET/POST /settings/registry/lakebase-setup` (admin) |
| `src/front/templates/partials/registry/_lakebase_setup.html`, `src/front/static/config/js/settings-lakebase-setup.js` | create (Task 7) | "No Lakebase bound" card on Settings → Registry |
| `tests/units/registry/test_lakebase_first_run.py` | create (Task 7) | Provisioner, binder, service |

## Review Focus

1. **Template drift.** Someone adds a required env var to `app.yaml.template`, and Marketplace installs silently lack it. Expected: a test fails until the var is added to `marketplace/app.yaml` or explicitly allowlisted as deploy-only. → Task 1, `test_every_template_env_is_shipped_or_allowlisted`.
2. **Renamed or missing runtime file** (e.g. `run.py` moved). Expected: assembly exits non-zero, names the missing paths, and leaves no half-built folder. → Task 2, `test_missing_root_file_fails_before_writing`.
3. **Stale files from a previous assembly** (a deleted module still shipped). Expected: the target folder is rebuilt from scratch. → Task 2, `test_reassembly_removes_stale_files`.
4. **Self-bind drops the app's other resources.** `update_mask=resources` replaces the whole array, so a naive bind silently unbinds the warehouse and Volume. A second run could also repoint an already-bound app at a new, empty registry. Expected: the other resources are preserved, and an existing `postgres` binding is refused, not replaced. → Task 7, `test_merge_preserves_other_resources`, `test_merge_refuses_existing_postgres`. (The `--out` inside a copied tree case is still pinned by Task 2's `test_out_dir_inside_copied_tree_is_refused`.)
5. **A token already in the session** before the upgrade (or from an imported domain), read by a helper in Apps mode. Expected: it is ignored and the SP OAuth token is used. → Task 3, `test_session_token_ignored_in_app_mode`.

---

### Task 1: Static Marketplace `app.yaml` + `manifest.yaml` with drift guards

**Gate:** Q1 and Q3 answered. If the Marketplace team gives a different key than `postgres_spec`, substitute it in Step 3 and in `AUTO_INJECTED_RESOURCES`.

**Files:**
- Create: `marketplace/manifest.yaml`, `marketplace/app.yaml`
- Test: `tests/units/scripts/test_marketplace_package.py`

**Interfaces:**
- Produces: `marketplace/app.yaml` and `marketplace/manifest.yaml` at fixed paths, consumed by Task 2 (`MARKETPLACE_FILES`).

- [ ] **Step 1: Write the failing tests**

```python
"""Contract tests for the Marketplace app package (marketplace/*.yaml).

Marketplace runs ``app.yaml`` verbatim — no render step — so it must be
static, must agree with ``manifest.yaml`` on resource names, and must not
drift from ``app.yaml.template`` (the DAB deploy path).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
MARKETPLACE = REPO_ROOT / "marketplace"
TEMPLATE = REPO_ROOT / "app.yaml.template"

#: Resources whose env vars the Apps platform injects (PGHOST, PGDATABASE, …)
#: so they have no ``valueFrom`` in app.yaml.
AUTO_INJECTED_RESOURCES = {"postgres"}

#: Template env vars intentionally NOT shipped to Marketplace installs.
#: Each is a deploy-time fallback that a bound resource or a Settings default
#: replaces. Adding a var here is a deliberate decision — review it.
DEPLOY_ONLY_ENV = {
    "DATABRICKS_SQL_WAREHOUSE_ID_DEFAULT",  # MCP/session-less fallback
    "DATABRICKS_TRIPLESTORE_TABLE",  # session-less fallback
    "REGISTRY_CATALOG",  # superseded by REGISTRY_VOLUME_PATH
    "REGISTRY_SCHEMA",
    "REGISTRY_VOLUME",
    "LAKEBASE_PROJECT",  # only read when PGHOST is absent
    "LAKEBASE_DATABASE",
    "LAKEBASE_BRANCH",
    "ONTOBRICKS_SYNC_UC_CATALOG",  # optional, managed-synced only
    "NEO4J_PASSWORD",  # Neo4j out of scope for v1 (Q5)
}


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


def _env(doc: dict) -> dict[str, dict]:
    return {e["name"]: e for e in doc.get("env", [])}


def test_manifest_schema_version_is_1():
    assert _load(MARKETPLACE / "manifest.yaml")["version"] == 1


def test_app_yaml_has_no_template_placeholders():
    assert "${" not in (MARKETPLACE / "app.yaml").read_text()


def test_every_value_from_is_declared_in_manifest():
    app = _load(MARKETPLACE / "app.yaml")
    declared = {r["name"] for r in _load(MARKETPLACE / "manifest.yaml")["resource_specs"]}
    used = {e["valueFrom"] for e in _env(app).values() if "valueFrom" in e}
    assert used <= declared, f"valueFrom without manifest resource: {used - declared}"


def test_every_manifest_resource_is_consumed():
    app = _load(MARKETPLACE / "app.yaml")
    declared = {r["name"] for r in _load(MARKETPLACE / "manifest.yaml")["resource_specs"]}
    used = {e["valueFrom"] for e in _env(app).values() if "valueFrom" in e}
    assert declared - used <= AUTO_INJECTED_RESOURCES


def test_command_matches_deploy_template():
    assert _load(MARKETPLACE / "app.yaml")["command"] == _load(TEMPLATE)["command"]


def test_marketplace_env_names_exist_in_template():
    unknown = set(_env(_load(MARKETPLACE / "app.yaml"))) - set(_env(_load(TEMPLATE)))
    assert not unknown, f"typo or undocumented var: {unknown}"


def test_every_template_env_is_shipped_or_allowlisted():
    template_env = set(_env(_load(TEMPLATE)))
    shipped = set(_env(_load(MARKETPLACE / "app.yaml")))
    missing = template_env - shipped - DEPLOY_ONLY_ENV
    assert not missing, f"add to marketplace/app.yaml or DEPLOY_ONLY_ENV: {missing}"


def test_literal_values_agree_with_template():
    """Vars the template hardcodes (no ${...}) must ship the same value."""
    template_env = _env(_load(TEMPLATE))
    for name, entry in _env(_load(MARKETPLACE / "app.yaml")).items():
        tmpl = template_env[name]
        if "value" in tmpl and not re.search(r"\$\{", str(tmpl["value"])):
            assert entry.get("value") == tmpl["value"], name
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run --frozen pytest tests/units/scripts/test_marketplace_package.py -q`
Expected: FAIL with `FileNotFoundError: .../marketplace/manifest.yaml`

- [ ] **Step 3: Create `marketplace/manifest.yaml`**

```yaml
# Databricks Marketplace manifest for OntoBricks.
# Spec: https://databricks.atlassian.net/wiki/spaces/UN/pages/5101748287
# Resource names MUST match the valueFrom keys in ./app.yaml
# (tests/units/scripts/test_marketplace_package.py enforces this).
version: 1
name: "ontobricks"
description: "Design OWL ontologies, map them to Unity Catalog tables, and build and query knowledge graphs on Databricks."

# user_api_scopes intentionally omitted: the platform defaults
# (iam.current-user:read, iam.access-control:read) are what the app uses
# for SCIM group lookup + App ACL read. See PLAN.md Q3.

resource_specs:
  - name: "sql-warehouse"
    description: "SQL Warehouse for executing queries and metadata operations"
    sql_warehouse_spec:
      permission: "CAN_USE"

  - name: "volume"
    description: "Unity Catalog Volume for OntoBricks documents and binary artefacts"
    uc_securable_spec:
      securable_type: "VOLUME"
      permission: "WRITE_VOLUME"

  # Lakebase Autoscaling database holding the OntoBricks registry (the
  # only registry backend since v0.4.0). Apps injects PG* env vars, so
  # there is no valueFrom for it in app.yaml. Key confirmed under Q1.
  - name: "postgres"
    description: "Lakebase Autoscaling Postgres database for the OntoBricks registry"
    postgres_spec:
      permission: "CAN_CONNECT_AND_CREATE"
```

- [ ] **Step 4: Create `marketplace/app.yaml`**

```yaml
# OntoBricks — Databricks Apps config for MARKETPLACE installs.
#
# Static: Marketplace runs this file verbatim (no render step). The DAB
# deploy path uses ../app.yaml.template instead; the two are kept in sync
# by tests/units/scripts/test_marketplace_package.py.
#
# Resources are declared in ./manifest.yaml and bound by the installer.

command:
  - "uv"
  - "run"
  - "--frozen"
  - "--no-dev"
  - "--extra"
  - "lakebase"
  - "python"
  - "run.py"

env:
  - name: DATABRICKS_SQL_WAREHOUSE_ID
    valueFrom: sql-warehouse
  - name: REGISTRY_VOLUME_PATH
    valueFrom: volume
  # PGHOST/PGPORT/PGDATABASE/PGUSER are injected from the bound postgres
  # resource. The schema is not injected, so it is pinned to the Settings
  # default.
  - name: LAKEBASE_SCHEMA
    value: "ontobricks_registry"
  - name: MLFLOW_TRACKING_URI
    value: "databricks"
  - name: ONTOBRICKS_ANALYTICS_MAX_TRIPLES
    value: "500000"
  - name: ONTOBRICKS_ANALYTICS_TOP_N
    value: "100"
  # The graph-analytics job is bundle-deployed and not part of a Marketplace
  # install, so the Analytics page stays disabled (it names the missing job).
  - name: ONTOBRICKS_ANALYTICS_JOB_ENABLED
    value: "false"
  - name: ONTOBRICKS_ANALYTICS_JOB_NAME
    value: ""
  - name: ONTOBRICKS_ANALYTICS_JOB_PAGERANK_ITERATIONS
    value: "20"
  - name: ONTOBRICKS_ANALYTICS_JOB_PIVOTS
    value: "64"
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run --frozen pytest tests/units/scripts/test_marketplace_package.py tests/units/settings/test_app_yaml_template.py -q`
Expected: all PASS. If `test_literal_values_agree_with_template` fails, copy the template's literal value; don't edit the template.

- [ ] **Step 6: Commit**

```bash
git add marketplace/manifest.yaml marketplace/app.yaml tests/units/scripts/test_marketplace_package.py
git commit -m "feat(marketplace): add static app.yaml + manifest.yaml with drift guards"
```

---

### Task 2: Assembly script + `make marketplace-bundle`

**Files:**
- Create: `scripts/marketplace/assemble.py`
- Modify: `Makefile` (`.PHONY`, `help`, new target)
- Test: `tests/units/scripts/test_marketplace_assemble.py`

**Interfaces:**
- Consumes: `marketplace/app.yaml`, `marketplace/manifest.yaml` (Task 1).
- Produces: `assemble(repo_root: Path, out_dir: Path) -> Path` (returns `<out_dir>/ontobricks`); CLI `python3 scripts/marketplace/assemble.py [--out DIR]`, exit 0 on success, 1 on missing inputs or a bad out dir. Constants `APP_FOLDER = "ontobricks"`, `ROOT_FILES`, `TREES`. Used by Task 4.

- [ ] **Step 1: Write the failing tests**

```python
"""Unit tests for scripts/marketplace/assemble.py."""

from __future__ import annotations

import importlib.util
import pathlib

import pytest

pytestmark = pytest.mark.unit

_ROOT = pathlib.Path(__file__).resolve().parents[3]
_SPEC = importlib.util.spec_from_file_location(
    "marketplace_assemble", _ROOT / "scripts" / "marketplace" / "assemble.py"
)
mod = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(mod)


@pytest.fixture
def fake_repo(tmp_path: pathlib.Path) -> pathlib.Path:
    repo = tmp_path / "repo"
    for name in mod.ROOT_FILES:
        (repo / name).parent.mkdir(parents=True, exist_ok=True)
        (repo / name).write_text(name)
    (repo / "src/back").mkdir(parents=True)
    (repo / "src/back/x.py").write_text("x = 1\n")
    (repo / "src/back/__pycache__").mkdir()
    (repo / "src/back/__pycache__/x.cpython-311.pyc").write_bytes(b"\0")
    (repo / "src/mcp-server").mkdir()
    (repo / "src/mcp-server/app.yaml").write_text("mcp")
    (repo / "documentation").mkdir()
    (repo / "documentation/user-guide.md").write_text("# guide")
    (repo / "documentation/sphinx/_build").mkdir(parents=True)
    (repo / "documentation/sphinx/_build/index.html").write_text("built")
    (repo / "tests").mkdir()
    (repo / "tests/test_x.py").write_text("")
    (repo / "marketplace").mkdir()
    (repo / "marketplace/app.yaml").write_text("command: []\n")
    (repo / "marketplace/manifest.yaml").write_text("version: 1\n")
    return repo


def test_app_folder_contains_runtime_files(fake_repo, tmp_path):
    target = mod.assemble(fake_repo, tmp_path / "out")
    for name in (*mod.ROOT_FILES, "app.yaml", "manifest.yaml", "src/back/x.py", "documentation/user-guide.md"):
        assert (target / name).is_file(), name


def test_app_folder_is_a_subfolder(fake_repo, tmp_path):
    target = mod.assemble(fake_repo, tmp_path / "out")
    assert target == tmp_path / "out" / "ontobricks"


def test_dev_only_content_is_excluded(fake_repo, tmp_path):
    target = mod.assemble(fake_repo, tmp_path / "out")
    assert not (target / "tests").exists()
    assert not (target / "src/mcp-server").exists()
    assert not (target / "documentation/sphinx/_build").exists()
    assert not list(target.rglob("*.pyc"))


def test_static_configs_come_from_marketplace_dir(fake_repo, tmp_path):
    target = mod.assemble(fake_repo, tmp_path / "out")
    assert (target / "app.yaml").read_text() == "command: []\n"


def test_reassembly_removes_stale_files(fake_repo, tmp_path):
    target = mod.assemble(fake_repo, tmp_path / "out")
    (target / "src/back/stale.py").write_text("old")
    target = mod.assemble(fake_repo, tmp_path / "out")
    assert not (target / "src/back/stale.py").exists()


def test_missing_root_file_fails_before_writing(fake_repo, tmp_path):
    (fake_repo / "run.py").unlink()
    with pytest.raises(FileNotFoundError, match="run.py"):
        mod.assemble(fake_repo, tmp_path / "out")
    assert not (tmp_path / "out" / "ontobricks").exists()


def test_out_dir_inside_copied_tree_is_refused(fake_repo):
    with pytest.raises(ValueError, match="inside"):
        mod.assemble(fake_repo, fake_repo / "src" / "dist")


def test_real_repo_has_every_input():
    assert mod.missing_inputs(_ROOT) == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run --frozen pytest tests/units/scripts/test_marketplace_assemble.py -q`
Expected: FAIL — `FileNotFoundError` loading `scripts/marketplace/assemble.py`

- [ ] **Step 3: Write `scripts/marketplace/assemble.py`**

```python
#!/usr/bin/env python3
"""Assemble the Databricks Marketplace app folder.

Marketplace installs a Databricks App from a *subfolder* of a tagged Git
commit and runs it verbatim — no render step, no build step. This script
copies the runtime file set into ``<out>/ontobricks/`` and adds the static
``marketplace/app.yaml`` + ``marketplace/manifest.yaml`` at its root.

The relative layout (``run.py`` beside ``src/`` and ``documentation/``) is
preserved, so the app behaves exactly as it does under ``make deploy``.

Usage::

    python3 scripts/marketplace/assemble.py               # -> dist/marketplace/ontobricks
    python3 scripts/marketplace/assemble.py --out /tmp/mp
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
APP_FOLDER = "ontobricks"

ROOT_FILES = (
    "run.py",
    "pyproject.toml",
    "uv.lock",
    "requirements.txt",
    "LICENSE.txt",
    "NOTICE.txt",
)
TREES = ("src", "documentation")
MARKETPLACE_FILES = ("app.yaml", "manifest.yaml")

_EXCLUDED_DIRS = {"__pycache__", ".pytest_cache", "_build", ".doctrees", "mcp-server"}
_EXCLUDED_NAMES = {".DS_Store", ".env"}
_EXCLUDED_SUFFIXES = {".pyc", ".pyo"}


def _ignore(_directory: str, names: list[str]) -> set[str]:
    return {
        n
        for n in names
        if n in _EXCLUDED_DIRS or n in _EXCLUDED_NAMES or Path(n).suffix in _EXCLUDED_SUFFIXES
    }


def missing_inputs(repo_root: Path) -> list[str]:
    """Return every required input path that does not exist under *repo_root*."""
    wanted = [*ROOT_FILES, *TREES, *(f"marketplace/{n}" for n in MARKETPLACE_FILES)]
    return [p for p in wanted if not (repo_root / p).exists()]


def assemble(repo_root: Path, out_dir: Path) -> Path:
    """Build ``<out_dir>/ontobricks`` from *repo_root*; return that path.

    Raises ``FileNotFoundError`` (nothing written) when an input is missing,
    and ``ValueError`` when *out_dir* sits inside a tree being copied.
    """
    repo_root = repo_root.resolve()
    out_dir = out_dir.resolve()
    for tree in TREES:
        if out_dir.is_relative_to(repo_root / tree):
            raise ValueError(f"--out {out_dir} is inside copied tree {tree}/")

    missing = missing_inputs(repo_root)
    if missing:
        raise FileNotFoundError(f"missing Marketplace inputs: {', '.join(missing)}")

    target = out_dir / APP_FOLDER
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)

    for name in ROOT_FILES:
        shutil.copy2(repo_root / name, target / name)
    for tree in TREES:
        shutil.copytree(repo_root / tree, target / tree, ignore=_ignore)
    for name in MARKETPLACE_FILES:
        shutil.copy2(repo_root / "marketplace" / name, target / name)
    return target


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=REPO_ROOT / "dist" / "marketplace")
    args = parser.parse_args(argv)
    try:
        target = assemble(REPO_ROOT, args.out)
    except (FileNotFoundError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(f"Marketplace app folder ready: {target}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run --frozen pytest tests/units/scripts/test_marketplace_assemble.py -q`
Expected: 8 passed

- [ ] **Step 5: Add the Makefile target**

Append `marketplace-bundle` to the `.PHONY` line (line 14), add the help line under the deploy section of `help:`, and add the target after `render-app-yaml:`:

```make
	@echo "    make marketplace-bundle - Assemble dist/marketplace/ontobricks for a Marketplace release"
```

```make
marketplace-bundle:
	@python3 scripts/marketplace/assemble.py
```

Run: `make marketplace-bundle && ls dist/marketplace/ontobricks`
Expected: `app.yaml  documentation  LICENSE.txt  manifest.yaml  NOTICE.txt  pyproject.toml  requirements.txt  run.py  src  uv.lock`

- [ ] **Step 6: Commit**

```bash
git add scripts/marketplace/assemble.py tests/units/scripts/test_marketplace_assemble.py Makefile
git commit -m "feat(marketplace): assemble Marketplace app subfolder (make marketplace-bundle)"
```

---

### Task 3: Never accept or use a caller-supplied PAT in Apps mode

**Why:** SOP Phase 1 says the app MUST NOT ask for or store user PATs. The UI has no token field, but `POST /settings/save` writes `data["token"]` into the session (`SettingsService.py:211`). And `/ontology/assistant/invoke` uses `custom_inputs.token` from the request body (`ontology.py:2187`). Local dev keeps PAT support.

**Files:**
- Modify: `src/back/objects/domain/SettingsService.py:210-212`
- Modify: `src/back/core/helpers/DatabricksHelpers.py:30-43` (+ new static method)
- Modify: `src/api/routers/internal/ontology.py` (assistant invoke, `custom_inputs = data.get(...)` line)
- Test: `tests/units/settings/test_no_pat_in_app_mode.py`

**Interfaces:**
- Produces: `DatabricksHelpers.strip_caller_credentials(custom_inputs: dict) -> dict`, which returns a copy without `host`/`token` when `is_databricks_app()`, else the input unchanged.

- [ ] **Step 1: Write the failing tests**

```python
"""Databricks Apps mode must never accept or use a caller-supplied PAT.

Marketplace SOP (Phase 1): "The app MUST NOT ask for or store user PAT tokens."
Local dev (no DATABRICKS_APP_PORT) keeps PAT support.
"""

import importlib
from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import ValidationError
from back.core.helpers.DatabricksHelpers import DatabricksHelpers
from back.objects.domain.SettingsService import SettingsService

pytestmark = pytest.mark.unit

# ``back.objects.domain`` re-exports the *class* SettingsService, so patch the
# module object explicitly (same convention as test_settings_domain_permissions.py).
_svc_module = importlib.import_module("back.objects.domain.SettingsService")


def _domain(databricks=None):
    domain = MagicMock()
    domain.databricks = dict(databricks or {})
    return domain


def _save(domain, body, app_mode):
    with patch.object(_svc_module, "get_domain", return_value=domain), patch.object(
        _svc_module, "is_databricks_app", return_value=app_mode
    ):
        return SettingsService.apply_config_save(body, "a@b.com", "", MagicMock(), MagicMock())


class TestSettingsSave:
    def test_token_rejected_in_app_mode(self):
        with pytest.raises(ValidationError, match="Personal access tokens"):
            _save(_domain(), {"token": "dapi-secret"}, app_mode=True)

    def test_rejected_token_is_not_stored(self):
        domain = _domain()
        with pytest.raises(ValidationError):
            _save(domain, {"token": "dapi-secret"}, app_mode=True)
        assert "token" not in domain.databricks

    def test_token_still_accepted_in_local_mode(self):
        domain = _domain()
        _save(domain, {"token": "dapi-local"}, app_mode=False)
        assert domain.databricks["token"] == "dapi-local"


class TestHostAndTokenResolution:
    def test_session_token_ignored_in_app_mode(self):
        domain = _domain({"host": "https://h.example", "token": "dapi-stale"})
        settings = MagicMock(databricks_host="", databricks_token="")
        client = MagicMock()
        client.get_oauth_token.return_value = "oauth-sp"
        with patch("back.core.databricks.is_databricks_app", return_value=True), patch(
            "back.core.databricks.DatabricksClient", return_value=client
        ):
            _, token = DatabricksHelpers.get_databricks_host_and_token(domain, settings)
        assert token == "oauth-sp"

    def test_session_token_used_in_local_mode(self):
        domain = _domain({"host": "https://h.example", "token": "dapi-local"})
        settings = MagicMock(databricks_host="", databricks_token="")
        with patch("back.core.databricks.is_databricks_app", return_value=False):
            _, token = DatabricksHelpers.get_databricks_host_and_token(domain, settings)
        assert token == "dapi-local"


class TestStripCallerCredentials:
    def test_strips_host_and_token_in_app_mode(self):
        with patch("back.core.databricks.is_databricks_app", return_value=True):
            out = DatabricksHelpers.strip_caller_credentials(
                {"host": "https://evil", "token": "dapi-x", "endpoint_name": "e"}
            )
        assert out == {"endpoint_name": "e"}

    def test_passthrough_in_local_mode(self):
        inputs = {"host": "https://h", "token": "dapi-x"}
        with patch("back.core.databricks.is_databricks_app", return_value=False):
            assert DatabricksHelpers.strip_caller_credentials(inputs) == inputs
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run --frozen pytest tests/units/settings/test_no_pat_in_app_mode.py -q`
Expected: FAIL — `test_token_rejected_in_app_mode` (no raise), `test_session_token_ignored_in_app_mode` (returns `dapi-stale`), `AttributeError: strip_caller_credentials`

- [ ] **Step 3: Reject the token in `SettingsService.apply_config_save`**

Replace lines 210-212:

```python
        if data.get("host"):
            domain.databricks["host"] = data["host"]
        if data.get("token"):
            if is_databricks_app():
                raise ValidationError(
                    "Personal access tokens are not accepted when OntoBricks runs "
                    "as a Databricks App; it authenticates with its service principal.",
                )
            domain.databricks["token"] = data["token"]
```

- [ ] **Step 4: Hide session tokens in `_domain_databricks` and add `strip_caller_credentials`**

In `src/back/core/helpers/DatabricksHelpers.py`, replace the body tail of `_domain_databricks` (lines 41-43):

```python
    if domain is None:
        return {}
    dbcfg = getattr(domain, "databricks", None) or {}
    if "token" in dbcfg and _databricks.is_databricks_app():
        # Apps mode authenticates as the SP; a token left in the session
        # (pre-upgrade session, imported domain) must never be used.
        return {k: v for k, v in dbcfg.items() if k != "token"}
    return dbcfg
```

Add to `class DatabricksHelpers` (next to `get_databricks_host_and_token`):

```python
    @staticmethod
    def strip_caller_credentials(custom_inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Drop caller-supplied ``host``/``token`` overrides in Databricks Apps mode."""
        if not _databricks.is_databricks_app():
            return custom_inputs
        return {k: v for k, v in custom_inputs.items() if k not in ("host", "token")}
```

- [ ] **Step 5: Use it in the assistant route**

In `src/api/routers/internal/ontology.py`, `ontology_assistant_invoke`, replace
`custom_inputs = data.get("custom_inputs", {})` with:

```python
    custom_inputs = DatabricksHelpers.strip_caller_credentials(
        data.get("custom_inputs", {})
    )
```

and add `from back.core.helpers.DatabricksHelpers import DatabricksHelpers` to the imports block (line 20 area). Update the route docstring: under `"custom_inputs"`, note "host/token overrides are ignored when running as a Databricks App".

- [ ] **Step 6: Run the new tests plus the settings/api suites**

Run: `uv run --frozen pytest tests/units/settings tests/units/api -q`
Expected: all PASS

- [ ] **Step 7: Commit**

```bash
git add src/back/objects/domain/SettingsService.py src/back/core/helpers/DatabricksHelpers.py src/api/routers/internal/ontology.py tests/units/settings/test_no_pat_in_app_mode.py
git commit -m "fix(security): never accept or use caller-supplied PATs in Databricks Apps mode"
```

---

### Task 4: Publish workflow (`marketplace` branch + tag)

**Gate:** Q2 answered "yes", and CI has `contents: write` in `databrickslabs/ontobricks` (or Jason/Alex run it).

**Files:**
- Create: `.github/workflows/marketplace-publish.yml`

**Interfaces:**
- Consumes: `scripts/marketplace/assemble.py --out DIR` (Task 2).
- Produces: branch `marketplace` whose only content is `ontobricks/` plus a short `README.md`; tag `marketplace-v<version>`.

- [ ] **Step 1: Write the workflow**

```yaml
name: Marketplace publish

on:
  workflow_dispatch:
    inputs:
      version:
        description: "Release tag to package (e.g. v0.9.2) — must already exist"
        required: true
      dry_run:
        description: "Assemble and show the diff, but do not push"
        type: boolean
        default: true

permissions:
  contents: write

jobs:
  publish:
    runs-on: ubuntu-latest
    timeout-minutes: 15
    steps:
      - name: Checkout release tag
        uses: actions/checkout@v7
        with:
          ref: ${{ inputs.version }}
          path: src-repo

      - name: Assemble app folder
        run: python3 src-repo/scripts/marketplace/assemble.py --out "$RUNNER_TEMP/mp"

      - name: Checkout marketplace branch
        uses: actions/checkout@v7
        with:
          ref: marketplace
          path: mp-branch

      - name: Replace app folder and commit
        working-directory: mp-branch
        env:
          VERSION: ${{ inputs.version }}
        run: |
          rm -rf ontobricks
          cp -R "$RUNNER_TEMP/mp/ontobricks" ontobricks
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add -A
          git commit -m "chore(marketplace): package ${VERSION}" || echo "no changes"
          git tag "marketplace-${VERSION}"
          git --no-pager show --stat HEAD

      - name: Push
        if: ${{ !inputs.dry_run }}
        working-directory: mp-branch
        env:
          VERSION: ${{ inputs.version }}
        run: git push origin marketplace "marketplace-${VERSION}"
```

- [ ] **Step 2: One-time branch bootstrap (maintainer, locally)**

```bash
git switch --orphan marketplace
git rm -rf --cached . >/dev/null; rm -rf -- * .[!.]* 2>/dev/null || true
printf '# OntoBricks — Marketplace packaging branch\n\nGenerated by `.github/workflows/marketplace-publish.yml`. Do not edit; app source lives on `main`.\n' > README.md
git add README.md && git commit -m "chore(marketplace): bootstrap packaging branch"
git push origin marketplace
git switch -
```

- [ ] **Step 3: Verify with a dry run**

Run the workflow from the Actions tab with `version=v0.9.1`, `dry_run=true`.
Expected: the job is green, and the `git show --stat` output lists `ontobricks/app.yaml`, `ontobricks/manifest.yaml`, `ontobricks/run.py` and nothing outside `ontobricks/` except `README.md`.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/marketplace-publish.yml
git commit -m "ci(marketplace): add publish workflow (marketplace branch + tag)"
```

---

### Task 5: Listing description, maintainer runbook, README, changelog

**Files:**
- Create: `marketplace/LISTING.md`, `marketplace/README.md`
- Modify: `README.md` (new "Install from Databricks Marketplace" section, after the existing install section)
- Create/append: `changelogs/v0.7.0/<github-user>_2026-11-03.log`

- [ ] **Step 1: Write `marketplace/LISTING.md`** (pasted into the listing's description field)

```markdown
# OntoBricks

Turn your Unity Catalog tables into a governed knowledge graph. OntoBricks lets you design an OWL ontology, map it to your lakehouse tables with R2RML, build the graph, and query and explore it, all from a Databricks App.

## Features
- 🧭 Visual ontology designer (OWL), with industry starters (FHIR and more)
- 🔗 Table-to-ontology mapping with AI-assisted auto-mapping
- 🕸️ Knowledge-graph build into Delta, with SPARQL querying
- ✅ SHACL data-quality shapes and validation
- 🤖 Ontology assistant powered by Databricks Foundation Model APIs
- 🔒 Runs as the app service principal; access governed by Databricks Apps permissions

## What you need at install time
| Resource | Used for |
|---|---|
| SQL Warehouse (`CAN_USE`) | Queries and metadata |
| Unity Catalog Volume (`WRITE_VOLUME`) | Documents and binary artefacts |
| Lakebase Autoscaling database (`CAN_CONNECT_AND_CREATE`) | The OntoBricks registry |

## After installing
1. Open the app and go to **Settings → Registry → Initialize**.
2. Grant colleagues `CAN_USE` on the app (Apps → Permissions). Users with `CAN_MANAGE` are OntoBricks admins.

## Learn more
- Source and docs: https://github.com/databrickslabs/ontobricks
- Databricks Labs projects are provided as-is, without formal Databricks support.
```

If Q4 comes back "not automatic", insert as step 2: "Grant the app's service principal `CAN_MANAGE` on the app itself (Apps → Permissions)", and renumber.

- [ ] **Step 2: Write `marketplace/README.md`** (maintainers)

```markdown
# Marketplace packaging

Marketplace installs OntoBricks from the `ontobricks/` folder on the
`marketplace` branch at a `marketplace-vX.Y.Z` tag. That folder is generated;
never edit the branch by hand.

| File | Purpose |
|---|---|
| `app.yaml` | Static Apps config shipped to installs (no `${…}`) |
| `manifest.yaml` | Marketplace manifest: name, description, resource specs |
| `LISTING.md` | Listing description (paste into the provider console) |

## Cutting a Marketplace release
1. Cut the normal GitHub release `vX.Y.Z` on `main`.
2. Actions → **Marketplace publish** → `version=vX.Y.Z`, `dry_run=true`; check the diff.
3. Re-run with `dry_run=false`.
4. Update the listing to the new tag in the provider workspace
   (SOP step 6, https://databricks.atlassian.net/wiki/spaces/UN/pages/3190490011).

## Changing app config
Edit `app.yaml.template` **and** `marketplace/app.yaml`.
`tests/units/scripts/test_marketplace_package.py` fails until they agree, or
until the var is added to `DEPLOY_ONLY_ENV` with a reason.

Local check: `make marketplace-bundle` → `dist/marketplace/ontobricks/`.
```

- [ ] **Step 3: Add the README section**

```markdown
## Install from Databricks Marketplace

Search **OntoBricks** in Databricks Marketplace and click **Install**. At install
time, bind a SQL Warehouse, a Unity Catalog Volume, and a Lakebase Autoscaling
database. Then open the app and run **Settings → Registry → Initialize**.

Marketplace installs don't include the MCP companion server, the Neo4j engine,
or the graph-analytics job. For those, deploy with `make deploy` (below).
```

- [ ] **Step 4: Changelog** — use the `changelog` skill. One section for Tasks 1–5: title, context, numbered changes with paths, modified files, test result.

- [ ] **Step 5: Full test run**

Run: `uv run --frozen pytest -q -m "not scenario"`
Expected: green; record the pass count in the changelog.

- [ ] **Step 6: Commit**

```bash
git add marketplace/LISTING.md marketplace/README.md README.md changelogs/v0.7.0/
git commit -m "docs(marketplace): listing description, maintainer runbook, README install section"
```

---

### Task 6: Dogfood validation + SOP process (non-code)

Do these in order; each depends on the one before. Record links in FEIP-2484.

- [ ] **Step 1: Running demo in E2 dogfood.** Deploy `dist/marketplace/ontobricks/` as an app, with a SQL warehouse, a Volume and a Lakebase database bound by hand:
  `databricks apps deploy <name> --source-code-path /Workspace/Users/<me>/ontobricks-mp` (after `databricks sync dist/marketplace/ontobricks /Workspace/Users/<me>/ontobricks-mp`). Verify:
  - The app starts. If `/health` reports `status: error`, read the startup trace before continuing.
  - Settings → Registry → Initialize succeeds as the SP (this is the Lakebase schema-grant question).
  - A **second, non-admin** user with `CAN_USE` can open the app. This answers **Q4**.
  - `POST /settings/save` with `{"token": "x"}` returns 400 (Task 3).
  - If Task 7 shipped: deploy a second copy with **no** postgres resource bound, open Settings → Registry, click **Create one for me**, and confirm the app comes back with the registry initialised.
- [ ] **Step 2: Phase 1 sign-off.** Post the app link in the #marketplace thread with Jianyu Zhou, plus answers or asks for Q1–Q3.
- [ ] **Step 3: Self-certify on FEIP-2484.** Tick the attestation boxes, citing Task 3 and the SOP's security best-practices doc.
- [ ] **Step 4: Tag.** Run the Task 4 workflow for the current release (or ask Jason Pohl / Alex Ott).
- [ ] **Step 5: Test listing in E2 dogfood.** Get the Marketplace Admin role (send your dogfood user ID: `WorkspaceClient().current_user.me().id`), use the "Databricks" provider profile, point it at the `marketplace-vX.Y.Z` tag, subfolder `ontobricks`, description = `marketplace/LISTING.md`. Do a successful install from the listing.
- [ ] **Step 6: Provider workspace access.** Opal group `app.databricks.marketplace-provider-account`; VPN set to US-west; workspace `marketplace-data-providers.cloud.databricks.com`.
- [ ] **Step 7: Labs checklist review + publish.** Jason Pohl walks through the checklist and clicks publish on the FEIP, coordinating with the Marketplace team.
- [ ] **Step 8: Telemetry + usage plan.** Confirm Jason has added the app to the telemetry ETL. Tell #ontobricks and Romy Li's account team; add the Marketplace link to the GitHub README and the docs site.

---

### Task 7: First-run Lakebase setup (create or attach when none is bound)

**Why:** this is required if Q1(a) is "the manifest can't declare Postgres". Even if it can, it removes the "find or create a Lakebase project first" step that sends a data modeller to the platform team. When the app starts with no `postgres` resource, an admin sees a card on Settings → Registry: **Create one for me**, or instructions to bind an existing one. The app's SP creates the project, so it owns it and every schema it creates. That makes `scripts/bootstrap/lakebase-perms.sh` unnecessary for these installs.

**Design: self-bind, not a side channel.** With no binding, `PGUSER` is not injected and `LakebaseAuth.user` raises (`LakebaseAuth.py:129-136`). So rather than teaching `LakebaseAuth` a second config source, the app **binds the new project to itself** (`postgres` resource) and redeploys. After the restart, the normal injected-`PG*` path runs unchanged. Precedent: the in-app *graph* provisioner (`back/core/graphdb/lakebase/provisioner.py`) already runs as the SP and reports through `TaskManager`. This one uses the **Autoscaling** API (`w.postgres.create_project`); the graph provisioner still uses the retiring `/api/2.0/database/instances` API, which should move separately.

**Verified locally (SDK 0.82.0 in `uv.lock`):** `PostgresAPI.create_project(project: Project, project_id: str) -> CreateProjectOperation` (`.wait() -> Project`); `ProjectSpec(display_name=…)`; `AppsAPI.deploy(app_name, app_deployment: AppDeployment)`; `AppDeployment` has `source_code_path` + `git_source`; `App.active_deployment` exists. **Not available in the SDK:** `AppResource` has no `postgres` field, and `PostgresAPI` has no `list_databases`. Both go through `w.api_client.do(...)`.

**Files:**
- Create: `src/back/core/databricks/lakebase/LakebaseProjectProvisioner.py`
- Create: `src/back/core/databricks/AppResourceBinder.py`
- Create: `src/back/objects/registry/LakebaseSetupService.py` (+ re-export in `src/back/objects/registry/__init__.py`)
- Modify: `src/api/routers/internal/settings.py` (two routes next to `/graph-engine/lakebase-provision`)
- Create: `src/front/templates/partials/registry/_lakebase_setup.html`; include it at the top of `partials/registry/_registry_configuration.html`
- Create: `src/front/static/config/js/settings-lakebase-setup.js`; add its `<script>` after `settings-registry-configuration.js` in `settings.html` (line ~1115)
- Test: `tests/units/registry/test_lakebase_first_run.py`

**Interfaces:**
- Produces: `LakebaseProjectProvisioner(w).ensure_project(project_id: str, display_name: str) -> str` (returns `projects/<id>`); `.database_path(project_path: str, branch: str = "production", postgres_database: str = "databricks_postgres") -> str`.
- Produces: `AppResourceBinder.merge_postgres(resources: list[dict], branch_path: str, database_path: str) -> list[dict]` (raises `ConflictError` if a `postgres` resource exists); `AppResourceBinder(w).bind_postgres(app_name: str, branch_path: str, database_path: str) -> None`.
- Produces: `LakebaseSetupService.status(app_name: str) -> {"bound": bool, "suggested_project_id": str}`; `LakebaseSetupService().start(project_id: str, app_name: str) -> {"success": True, "task_id": str, "message": str}`.
- Consumes: `get_lakebase_auth().is_available`, `get_task_manager()` (`create_task` / `start_task` / `advance_step` / `complete_task` / `fail_task`).

- [ ] **Step 1: Spike in E2 dogfood (≈1–2 h, answers S1–S4; record results in FEIP-2484)**

| # | Question | How | If "no" |
|---|---|---|---|
| S1 | Can an app SP create a Lakebase project? | Deploy any throwaway app; from its SP run `w.postgres.create_project(project=Project(spec=ProjectSpec(display_name="spike")), project_id="spike-sp").wait()` (or as that SP via the CLI) | The card shows only "ask an admin to bind one" (keep Steps 6–8; skip the Create button) |
| S2 | Exact REST shape of `apps create-update` with a `postgres` resource, and of `list-databases` | `databricks apps create-update <app> --json @u.json --debug 2>&1 \| grep -E '^> (GET\|POST\|PATCH)'` and `databricks postgres list-databases projects/<p>/branches/production -o json --debug` | Put the captured path in `APPS_UPDATE_PATH` / `_DATABASES_PATH` (Steps 4–5) |
| S3 | After create-update adds `postgres`, does the running app see `PG*` without a new deployment? | Bind on a running app; check `/settings/permissions/diag` or the logs for `PGHOST`; then try `w.apps.deploy(...)` with the active deployment's `source_code_path` / `git_source` | If no redeploy is needed, delete `_apply_binding` and its step |
| S4 | Who may call create-update on the app: its own SP (needs `CAN_MANAGE` on itself, the Q4 grant), or only a human `CAN_MANAGE`? Same for `PATCH /api/2.0/permissions/apps/<app>` (the Q4 grant itself). | Try each as the SP | If only humans can: add an `apps` user scope (in the manifest's supported list; needs a security note on the FEIP) and pass the admin's `x-forwarded-access-token` client to `AppResourceBinder` |

- [ ] **Step 2: Write the failing tests**

```python
"""First-run Lakebase setup: create/adopt project, bind it to the app, redeploy."""

import importlib
from unittest.mock import MagicMock, patch

import pytest
from databricks.sdk.errors import NotFound, PermissionDenied

from back.core.databricks.AppResourceBinder import AppResourceBinder
from back.core.databricks.lakebase.LakebaseProjectProvisioner import (
    LakebaseProjectProvisioner,
)
from back.core.errors import (
    AuthorizationError,
    ConflictError,
    InfrastructureError,
    ValidationError,
)
from back.objects.registry.LakebaseSetupService import LakebaseSetupService

pytestmark = pytest.mark.unit

# Re-exported class shadows the module name — patch the module object.
_setup_module = importlib.import_module("back.objects.registry.LakebaseSetupService")

_DBS = {
    "databases": [
        {"name": "projects/p/branches/production/databases/db-other",
         "status": {"postgres_database": "other"}},
        {"name": "projects/p/branches/production/databases/db-main",
         "status": {"postgres_database": "databricks_postgres"}},
    ]
}
_WAREHOUSE = {"name": "sql-warehouse", "sql_warehouse": {"id": "w1", "permission": "CAN_USE"}}
_VOLUME = {"name": "volume", "uc_securable": {"securable_type": "VOLUME"}}


class TestLakebaseProjectProvisioner:
    def test_adopts_existing_project(self):
        w = MagicMock()
        assert LakebaseProjectProvisioner(w).ensure_project("p", "P") == "projects/p"
        w.postgres.create_project.assert_not_called()

    def test_creates_missing_project(self):
        w = MagicMock()
        w.postgres.get_project.side_effect = NotFound("missing")
        LakebaseProjectProvisioner(w).ensure_project("p", "P")
        assert w.postgres.create_project.call_args.kwargs["project_id"] == "p"

    def test_permission_denied_becomes_authorization_error(self):
        w = MagicMock()
        w.postgres.get_project.side_effect = NotFound("missing")
        w.postgres.create_project.side_effect = PermissionDenied("nope")
        with pytest.raises(AuthorizationError, match="workspace admin"):
            LakebaseProjectProvisioner(w).ensure_project("p", "P")

    def test_database_path_picks_default_database(self):
        w = MagicMock()
        w.api_client.do.return_value = _DBS
        path = LakebaseProjectProvisioner(w).database_path("projects/p")
        assert path == "projects/p/branches/production/databases/db-main"

    def test_database_path_missing_raises(self):
        w = MagicMock()
        w.api_client.do.return_value = {"databases": []}
        with pytest.raises(InfrastructureError):
            LakebaseProjectProvisioner(w).database_path("projects/p")


class TestAppResourceBinder:
    def test_merge_preserves_other_resources(self):
        merged = AppResourceBinder.merge_postgres([_WAREHOUSE, _VOLUME], "b", "d")
        assert [r["name"] for r in merged] == ["sql-warehouse", "volume", "postgres"]

    def test_merge_refuses_existing_postgres(self):
        with pytest.raises(ConflictError):
            AppResourceBinder.merge_postgres([{"name": "postgres"}], "b", "d")

    def test_bind_sends_merged_resources_with_mask(self):
        w = MagicMock()
        w.api_client.do.side_effect = [{"resources": [_WAREHOUSE]}, {}]
        AppResourceBinder(w).bind_postgres("ontobricks", "b", "d")
        body = w.api_client.do.call_args_list[1].kwargs["body"]
        assert body["update_mask"] == "resources"
        assert [r["name"] for r in body["app"]["resources"]] == ["sql-warehouse", "postgres"]


class TestLakebaseSetupService:
    def _bound(self, value):
        auth = MagicMock(is_available=value)
        return patch.object(_setup_module, "get_lakebase_auth", return_value=auth)

    def test_status_suggests_project_from_app_name(self):
        with self._bound(False):
            out = LakebaseSetupService.status("OntoBricks_Prod")
        assert out == {"bound": False, "suggested_project_id": "ontobricks-prod"}

    def test_start_refuses_when_already_bound(self):
        with self._bound(True), pytest.raises(ConflictError):
            LakebaseSetupService().start("ontobricks", "ontobricks")

    @pytest.mark.parametrize("bad", ["", "My Project", "1abc", "a" * 64])
    def test_start_rejects_invalid_project_id(self, bad):
        with self._bound(False), pytest.raises(ValidationError):
            LakebaseSetupService().start(bad, "ontobricks")

    def test_run_executes_steps_in_order_and_completes(self):
        w = MagicMock()
        w.postgres.get_project.return_value = MagicMock()
        w.api_client.do.side_effect = [_DBS, {"resources": [_WAREHOUSE]}, {}]
        tm = MagicMock()
        LakebaseSetupService(lambda: w).run(tm, "t1", "p", "ontobricks")
        assert tm.advance_step.call_count == 3
        tm.complete_task.assert_called_once()
        w.apps.deploy.assert_called_once()
        tm.fail_task.assert_not_called()

    def test_run_reports_failure_to_task(self):
        w = MagicMock()
        w.postgres.get_project.side_effect = NotFound("missing")
        w.postgres.create_project.side_effect = PermissionDenied("nope")
        tm = MagicMock()
        LakebaseSetupService(lambda: w).run(tm, "t1", "p", "ontobricks")
        assert "workspace admin" in tm.fail_task.call_args.args[1]
        w.apps.deploy.assert_not_called()
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run --frozen pytest tests/units/registry/test_lakebase_first_run.py -q`
Expected: FAIL — `ModuleNotFoundError: back.core.databricks.AppResourceBinder`

- [ ] **Step 4: Write `LakebaseProjectProvisioner.py`**

```python
"""Create (or adopt) a Lakebase Autoscaling project for the OntoBricks registry.

Used by the first-run setup when an install has no ``postgres`` resource
bound. Runs as whichever identity owns *workspace_client* — the app's
service principal by default — so that identity owns the project and
every schema it later creates.
"""

from __future__ import annotations

from typing import Any

from databricks.sdk.errors import NotFound, PermissionDenied
from databricks.sdk.service import postgres

from back.core.errors import AuthorizationError, InfrastructureError
from back.core.logging import get_logger

logger = get_logger(__name__)

DEFAULT_BRANCH = "production"
DEFAULT_DATABASE = "databricks_postgres"
#: Confirmed in Task 7 spike S2 (no ``list_databases`` in SDK 0.82).
_DATABASES_PATH = "/api/2.0/postgres/{branch_path}/databases"


class LakebaseProjectProvisioner:
    """Thin wrapper over the Lakebase Autoscaling control plane."""

    def __init__(self, workspace_client: Any) -> None:
        self._w = workspace_client

    def ensure_project(self, project_id: str, display_name: str) -> str:
        """Return ``projects/<project_id>``, creating the project if missing."""
        path = f"projects/{project_id}"
        try:
            self._w.postgres.get_project(name=path)
            logger.info("Lakebase project %s already exists; adopting it", path)
            return path
        except NotFound:
            pass
        try:
            self._w.postgres.create_project(
                project=postgres.Project(
                    spec=postgres.ProjectSpec(display_name=display_name)
                ),
                project_id=project_id,
            ).wait()
        except PermissionDenied as exc:
            raise AuthorizationError(
                "This app's service principal is not allowed to create Lakebase "
                "projects here. Ask a workspace admin to create one and bind it "
                "to the app as a 'postgres' resource."
            ) from exc
        logger.info("Created Lakebase project %s", path)
        return path

    def database_path(
        self,
        project_path: str,
        branch: str = DEFAULT_BRANCH,
        postgres_database: str = DEFAULT_DATABASE,
    ) -> str:
        """Return the ``…/databases/<db-id>`` path whose datname is *postgres_database*."""
        branch_path = f"{project_path}/branches/{branch}"
        raw = self._w.api_client.do(
            "GET", _DATABASES_PATH.format(branch_path=branch_path)
        ) or {}
        for row in raw.get("databases", []):
            if (row.get("status") or {}).get("postgres_database") == postgres_database:
                return row["name"]
        raise InfrastructureError(
            f"Database {postgres_database!r} not found on {branch_path}"
        )
```

- [ ] **Step 5: Write `AppResourceBinder.py`**

```python
"""Add a Lakebase ``postgres`` binding to a running Databricks App.

The SDK's typed ``AppResource`` (0.82) has no ``postgres`` field, so this
uses the raw REST client. ``update_mask=resources`` REPLACES the whole
resources array: the current resources are read and merged first, and an
existing ``postgres`` binding is never replaced (that would repoint the
registry at an empty database).
"""

from __future__ import annotations

from typing import Any, Dict, List

from back.core.errors import ConflictError
from back.core.logging import get_logger

logger = get_logger(__name__)

POSTGRES_RESOURCE = "postgres"
#: Confirmed in Task 7 spike S2 (``databricks apps create-update --debug``).
APPS_UPDATE_PATH = "/api/2.0/apps/{app_name}/update"


class AppResourceBinder:
    """Merge-and-update helper for a Databricks App's resources."""

    def __init__(self, workspace_client: Any) -> None:
        self._w = workspace_client

    @staticmethod
    def merge_postgres(
        resources: List[Dict[str, Any]], branch_path: str, database_path: str
    ) -> List[Dict[str, Any]]:
        """Return *resources* plus a ``postgres`` binding; others untouched."""
        if any(r.get("name") == POSTGRES_RESOURCE for r in resources):
            raise ConflictError(
                "This app already has a 'postgres' resource bound; refusing to replace it."
            )
        return [
            *resources,
            {
                "name": POSTGRES_RESOURCE,
                "description": "Lakebase Autoscaling Postgres database for the OntoBricks registry",
                "postgres": {
                    "branch": branch_path,
                    "database": database_path,
                    "permission": "CAN_CONNECT_AND_CREATE",
                },
            },
        ]

    def bind_postgres(self, app_name: str, branch_path: str, database_path: str) -> None:
        """Bind *database_path* to *app_name*, preserving existing resources."""
        app = self._w.api_client.do("GET", f"/api/2.0/apps/{app_name}") or {}
        resources = self.merge_postgres(
            app.get("resources", []), branch_path, database_path
        )
        self._w.api_client.do(
            "POST",
            APPS_UPDATE_PATH.format(app_name=app_name),
            body={"update_mask": "resources", "app": {"resources": resources}},
        )
        logger.info("Bound postgres resource %s to app %s", database_path, app_name)
```

- [ ] **Step 6: Write `LakebaseSetupService.py`** and add it to `back/objects/registry/__init__.py`'s imports and `__all__`

```python
"""First-run Lakebase setup for installs with no ``postgres`` resource bound."""

from __future__ import annotations

import re
import threading
from typing import Any, Callable, Dict, Optional

from back.core.databricks.AppResourceBinder import AppResourceBinder
from back.core.databricks.lakebase import get_lakebase_auth
from back.core.databricks.lakebase.LakebaseProjectProvisioner import (
    DEFAULT_BRANCH,
    LakebaseProjectProvisioner,
)
from back.core.errors import ConflictError, ValidationError
from back.core.logging import get_logger
from back.core.task_manager import get_task_manager

logger = get_logger(__name__)

_PROJECT_ID_RE = re.compile(r"^[a-z][a-z0-9-]{0,62}$")

SETUP_STEPS = [
    {"name": "project", "description": "Creating Lakebase project"},
    {"name": "database", "description": "Resolving database"},
    {"name": "bind", "description": "Binding Lakebase to this app"},
    {"name": "apply", "description": "Restarting the app with the new binding"},
]


class LakebaseSetupService:
    """Create or adopt a Lakebase project, bind it to this app, redeploy."""

    def __init__(self, workspace_client_factory: Optional[Callable[[], Any]] = None) -> None:
        self._client_factory = workspace_client_factory or self._default_client

    @staticmethod
    def _default_client() -> Any:
        from databricks.sdk import WorkspaceClient

        return WorkspaceClient()

    @staticmethod
    def status(app_name: str) -> Dict[str, Any]:
        slug = re.sub(r"[^a-z0-9-]+", "-", (app_name or "").lower()).strip("-")[:63]
        if not _PROJECT_ID_RE.match(slug):
            slug = "ontobricks"
        return {"bound": bool(get_lakebase_auth().is_available), "suggested_project_id": slug}

    def start(self, project_id: str, app_name: str) -> Dict[str, Any]:
        project_id = (project_id or "").strip()
        if not _PROJECT_ID_RE.match(project_id):
            raise ValidationError(
                "Project ID must start with a lowercase letter and use only "
                "lowercase letters, digits and hyphens (max 63)."
            )
        if not app_name:
            raise ValidationError("Could not determine this app's name (DATABRICKS_APP_NAME).")
        if get_lakebase_auth().is_available:
            raise ConflictError("A Lakebase database is already bound to this app.")

        tm = get_task_manager()
        task = tm.create_task(
            name="Lakebase first-run setup",
            task_type="lakebase_setup",
            steps=SETUP_STEPS,
        )
        threading.Thread(
            target=self.run, args=(tm, task.id, project_id, app_name), daemon=True
        ).start()
        return {"success": True, "task_id": task.id, "message": "Lakebase setup started"}

    def run(self, tm: Any, task_id: str, project_id: str, app_name: str) -> None:
        try:
            w = self._client_factory()
            provisioner = LakebaseProjectProvisioner(w)
            tm.start_task(task_id, "Creating Lakebase project...")
            project_path = provisioner.ensure_project(
                project_id, display_name=f"OntoBricks ({app_name})"
            )
            tm.advance_step(task_id)
            database_path = provisioner.database_path(project_path)
            tm.advance_step(task_id)
            AppResourceBinder(w).bind_postgres(
                app_name, f"{project_path}/branches/{DEFAULT_BRANCH}", database_path
            )
            tm.advance_step(task_id)
            # Redeploying restarts THIS process, so start it last and don't
            # wait on it; TaskManager state is in-memory and dies with us.
            self._apply_binding(w, app_name)
            tm.complete_task(
                task_id,
                result={"project": project_path, "database": database_path},
                message=(
                    "Lakebase bound. The app is restarting: reload in about a minute, "
                    "then run Settings → Registry → Initialize."
                ),
            )
        except Exception as exc:  # noqa: BLE001 — surfaced to the admin via the task
            logger.exception("Lakebase first-run setup failed for %s", app_name)
            tm.fail_task(task_id, str(exc))

    @staticmethod
    def _apply_binding(w: Any, app_name: str) -> None:
        """Redeploy the active deployment so the platform injects ``PG*`` (spike S3)."""
        from databricks.sdk.service import apps

        active = w.apps.get(name=app_name).active_deployment
        w.apps.deploy(
            app_name=app_name,
            app_deployment=apps.AppDeployment(
                source_code_path=active.source_code_path,
                git_source=active.git_source,
            ),
        )
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `uv run --frozen pytest tests/units/registry/test_lakebase_first_run.py -q`
Expected: all PASS

- [ ] **Step 8: Routes** (`src/api/routers/internal/settings.py`, after `post_graph_engine_lakebase_provision`; add `from back.objects.registry import LakebaseSetupService` to the imports)

```python
@router.get(
    "/registry/lakebase-setup",
    dependencies=[Depends(require(ROLE_ADMIN))],
)
async def get_registry_lakebase_setup(settings: Settings = Depends(get_settings)):
    """Whether a Lakebase ``postgres`` resource is bound (admin only)."""
    return LakebaseSetupService.status(settings.ontobricks_app_name)


@router.post(
    "/registry/lakebase-setup",
    dependencies=[Depends(require(ROLE_ADMIN))],
)
async def post_registry_lakebase_setup(
    request: Request, settings: Settings = Depends(get_settings)
):
    """Create/adopt a Lakebase project, bind it to this app and redeploy (async).

    Body: ``{ "project_id": "<rfc1123 id>" }``. Returns a ``task_id`` the UI
    polls via ``GET /tasks/{id}``.
    """
    data = await request.json()
    with map_route_errors("start Lakebase first-run setup", logger):
        return LakebaseSetupService().start(
            data.get("project_id", ""), settings.ontobricks_app_name
        )
```

- [ ] **Step 9: UI.** Create `partials/registry/_lakebase_setup.html` (no inline JS/CSS; Bootstrap 5.3 only):

```html
{# First-run card: settings-lakebase-setup.js un-hides it when no postgres resource is bound. #}
<div id="lakebaseSetupCard" class="card border-warning mb-4 d-none">
    <div class="card-body">
        <h5 class="card-title"><i class="bi bi-database-exclamation me-2"></i>No Lakebase database is bound</h5>
        <p class="card-text small text-muted mb-3">
            OntoBricks keeps its registry in Lakebase. Create a dedicated project now, or bind an
            existing one: <em>Compute → Apps → this app → Edit → Resources</em>, add a
            <code>postgres</code> resource named <code>postgres</code>, then redeploy.
        </p>
        <div class="input-group mb-2">
            <span class="input-group-text">Project ID</span>
            <input type="text" class="form-control" id="lakebaseSetupProjectId"
                   pattern="[a-z][a-z0-9-]{0,62}" aria-describedby="lakebaseSetupStatus">
            <button class="btn btn-primary" id="lakebaseSetupBtn" type="button">Create one for me</button>
        </div>
        <div id="lakebaseSetupStatus" class="small text-muted" role="status" aria-live="polite"></div>
    </div>
</div>
```

`settings-lakebase-setup.js`:

```js
// Settings → Registry: first-run Lakebase setup card (admin-only endpoints).
(function () {
    'use strict';
    const card = document.getElementById('lakebaseSetupCard');
    if (!card) return;
    const input = document.getElementById('lakebaseSetupProjectId');
    const btn = document.getElementById('lakebaseSetupBtn');
    const statusEl = document.getElementById('lakebaseSetupStatus');

    function setStatus(text, tone) {
        statusEl.className = 'small text-' + (tone || 'muted');
        statusEl.textContent = text;
    }

    async function refresh() {
        const resp = await fetch('/settings/registry/lakebase-setup', { credentials: 'same-origin' });
        if (!resp.ok) return; // non-admins get 403: card stays hidden
        const data = await resp.json();
        card.classList.toggle('d-none', Boolean(data.bound));
        if (!input.value) input.value = data.suggested_project_id || '';
    }

    async function monitor(taskId) {
        while (true) {
            await new Promise((r) => setTimeout(r, 1500));
            let task;
            try {
                const resp = await fetch('/tasks/' + encodeURIComponent(taskId), { credentials: 'same-origin' });
                task = (await resp.json()).task;
            } catch (_) {
                // The app restarts at the end of a successful run; keep polling the card.
                setStatus('App restarting… reload this page in a minute.', 'success');
                return;
            }
            if (!task) return;
            setStatus(task.message || task.status);
            if (task.status === 'completed') { setStatus(task.message, 'success'); return; }
            if (task.status === 'failed') { setStatus(task.error || 'Setup failed', 'danger'); btn.disabled = false; return; }
        }
    }

    btn.addEventListener('click', async () => {
        btn.disabled = true;
        setStatus('Starting…');
        const headers = { 'Content-Type': 'application/json' };
        const csrf = typeof _getCSRFToken === 'function' ? _getCSRFToken() : '';
        if (csrf) headers['X-CSRF-Token'] = csrf;
        const resp = await fetch('/settings/registry/lakebase-setup', {
            method: 'POST',
            credentials: 'same-origin',
            headers,
            body: JSON.stringify({ project_id: input.value.trim() }),
        });
        const data = await resp.json();
        if (!resp.ok || !data.success) {
            btn.disabled = false;
            setStatus(data.message || data.detail || 'Could not start setup', 'danger');
            return;
        }
        monitor(data.task_id);
    });

    refresh();
})();
```

Manual check (local, `make run`): Settings → Registry. With `PGHOST`/`PGUSER` unset the card shows with a suggested ID. With them set it's hidden.

- [ ] **Step 10: Manifest follow-through.** If Q1(a) = "no": remove the `postgres` entry from `marketplace/manifest.yaml`, drop `"postgres"` from `AUTO_INJECTED_RESOURCES`, and change `LISTING.md`'s "What you need" table and "After installing" step 1 to "open the app → Settings → Registry → **Create one for me** (or bind an existing database), then **Initialize**".

- [ ] **Step 11: Full suite + commit**

Run: `uv run --frozen pytest -q -m "not scenario"` → green.

```bash
git add src/back/core/databricks/lakebase/LakebaseProjectProvisioner.py src/back/core/databricks/AppResourceBinder.py src/back/objects/registry/LakebaseSetupService.py src/back/objects/registry/__init__.py src/api/routers/internal/settings.py src/front/templates/partials/registry/ src/front/static/config/js/settings-lakebase-setup.js src/front/templates/settings.html tests/units/registry/test_lakebase_first_run.py
git commit -m "feat(registry): first-run Lakebase setup — create or attach and self-bind"
```

---

## Follow-up milestone: `/app` layout (next release after the first listing)

**Gate:** Q6 agreed with Benoit, and the Task 6 dogfood install proved the fixed `marketplace/app.yaml` runs the app with no generated values. **Expand this into its own `PLAN.md` before executing**: it touches every contributor's workflow and needs a merge window with no long-lived branches open.

**End state:** `app/` holds exactly what ships: `run.py`, `src/` (minus the MCP server), `pyproject.toml`, `uv.lock`, `requirements.txt`, `app.yaml`, `manifest.yaml`, `documentation/` (Help Center markdown only). `mcp-server/` is top-level. Tests, scripts, CI, Sphinx and the GitHub Pages site stay at the root. The Marketplace listing points at `app/` on the normal `vX.Y.Z` tag; Tasks 2 + 4 and the `marketplace` branch are deleted.

**What moves cheaply (measured on `main` @ `2491c065`):**
- The five root-relative lookups in `src/` (`help.py:142`, `shared/fastapi/main.py:624`, `front/fastapi/dependencies.py:26`, both `agents/*/log_model.py`) keep working, because `run.py`, `src/` and `documentation/` move together.
- Tests stay at the root (otherwise they'd ship). 61 test files reference `src/` paths and 58 find the repo root via `parents[N]`: introduce `tests/fixtures/paths.py` with `REPO_ROOT` / `APP_ROOT` and rewrite mechanically.
- `databricks.yml`: `source_code_path: ./app` and `./mcp-server`; most of the `sync.exclude` list goes; the job's `python_file` becomes `${workspace.file_path}/app/src/jobs/graph_analytics_job.py`.
- Scripts: 10 files reference `src/`, 5 reference `run.py` / `app.yaml`. Docs: ~75 files mention `src/…` paths. That's doc rot, not breakage: fix them in the same PR with a scripted rewrite.

**What's expensive (the real work):**
1. **uv project root moves.** `pyproject.toml` + `uv.lock` must be inside `app/`: the container runs `uv run --frozen` there, and the lock pins `files.pythonhosted.org` URLs to avoid the cold internal PyPI proxy (see the `app.yaml.template` comment). A root uv workspace doesn't work: its lock would sit outside the shipped folder. So the mandated test command becomes `uv run --frozen --project app pytest -q -m "not scenario"`. Update `.cursorrules`, `.cursor/08-testing-and-deployment.mdc`, the `changelog` / `deploy` / `code-review` skills, the `Makefile`, and the four workflows that run uv (`ci.yml`, `release.yml`, `nightly.yml`, `eval-gate.yml`). Move `[tool.ruff]` / `[tool.mypy]` / `[tool.coverage]` to root `ruff.toml` / `mypy.ini` / `.coveragerc` (root `pytest.ini` already overrides `[tool.pytest]`).
2. **Docs weight.** The SOP shows no exclude mechanism for the Marketplace folder, so `app/` must contain only the Help Center files (`_DOC_INDEX` in `help.py`). Sphinx (46 MB), `pr47-neo4j-demo` (13 MB) and `superpowers/` stay at the root. Add a test asserting every `_DOC_INDEX` file exists under `app/documentation/` and nothing else does.
3. **One `app.yaml` slot for both deploy paths.** `app/app.yaml` must be committed and fixed for Marketplace, so `make deploy` can no longer generate one there. Retire `_render-app-yaml.py` for the main app. The blocker is `LAKEBASE_SCHEMA`: instances share a database and are kept apart by schema (`ontobricks_demo`, `ontobricks_registry`, …).
   - Resolve the schema at runtime (explicit env > `DATABRICKS_APP_NAME` slug > `ontobricks_registry`).
   - Migration: before upgrading, `scripts/deploy.sh` reads each live app's current `LAKEBASE_SCHEMA` (`scripts/update-deployed-app.sh` already reads live values back) and refuses to deploy if the runtime-derived name would differ, unless an override is pinned. Get this wrong and an instance quietly starts on an empty schema.
   - The MCP server keeps its generated `app.yaml` (it needs `ONTOBRICKS_URL`).

---

## Self-review notes

- SOP coverage: open source + labs repo ✅ (already); security / PAT → Task 3; no build steps + subfolder + app.yaml / manifest → Tasks 1–2; tag → Task 4; dogfood demo + listing + description → Tasks 5–6; usage plan → Task 6 Step 8; self-cert / publish / telemetry → Task 6.
- Q1 = "no" is now covered by Task 7 (first-run create/attach + self-bind) instead of a non-Postgres registry backend.
- Lakebase permission bootstrap is covered by Task 7: the SP-created project and schema are SP-owned.
- Q4 (SP self-grant) is covered by the Task 6 dogfood check plus spike S4. If the platform doesn't grant it and the SP can't, it stays a documented post-install step.
- The `/app` layout is a gated follow-up milestone, with blast radius measured, not a task in this plan.
- Not covered on purpose: the MCP companion listing (Q5), Neo4j / analytics in Marketplace installs (Q5), and moving the graph provisioner off the retiring `/database/instances` API (separate change).
- Unverified until spike S2: `APPS_UPDATE_PATH` and `_DATABASES_PATH`. They're constants, so the fix is one line each.
