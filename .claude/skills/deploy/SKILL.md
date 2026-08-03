---
name: deploy
description: Use when the user asks to deploy, ship, release, or push OntoBricks to Databricks. Wraps the Databricks Asset Bundle deploy for the FastAPI app and the MCP server, with the bootstrap-perms safety net described in README.md.
---

# Deploy OntoBricks

The canonical deploy steps are in **`README.md §Deploying / Installing`** and
**`.cursorrules` §"When asking to deploy"**. The Make targets live in
`Makefile`. This skill only sequences pre-flight and post-deploy checks.

## Pre-flight (verify before invoking make)

1. `databricks --version` — CLI present.
2. `databricks auth describe` — authenticated against the right workspace. If not, use the `databricks-authentication` skill.
3. `git status` — clean tree (or user has acknowledged uncommitted changes).
4. `uv run --frozen pytest -q -m "not scenario"` — tests pass. **Do not deploy on red.**
   **Never drop `--frozen`**: a bare `uv run` performs an implicit re-resolve
   against the internal proxy and rewrites every URL in `uv.lock`, so running the
   tests is itself enough to poison the lock *after* you have already checked it.
5. **`uv.lock` is deploy-safe** — the container runs `uv run --frozen` and
   installs verbatim from the lock, so verify (all read-only — none of these
   rewrite the file):
   - **All URLs on the public CDN:** `rg -c 'pypi-proxy\.dev\.databricks\.com' uv.lock`
     → must be **no matches**. `rg -o 'url = "https?://[^/"]+' uv.lock | sort -u`
     → only `https://files.pythonhosted.org`. (The container's default index,
     `pypi-proxy.dev.databricks.com`, cold-caches fresh wheels and crashes the
     deploy; the committed lock is pinned to the public CDN to avoid this.)
   - **In sync with `pyproject.toml`:** `UV_INDEX_URL=https://pypi.org/simple uv lock --check`
     → exit 0. **Check against the public index**, not the shell default —
     `UV_INDEX_URL`/`PIP_INDEX_URL` usually point at the proxy, which yields a
     false "lockfile needs to be updated" warning.
   - **Re-verify immediately before `make deploy`**, after the test run — the
     lock can be poisoned between pre-flight and deploy. `git status` must show
     `uv.lock` clean at the moment you deploy.
   - If the lock is dirty/proxied, fix it before deploying:
     `git checkout -- uv.lock` (to discard accidental churn) or re-lock against
     the public index: `UV_INDEX_URL=https://pypi.org/simple uv lock`. See
     `.cursor/09-package-management.mdc` for the full lockfile policy.

## Deploy

| Command | When |
|---------|------|
| `make deploy` | dev — deploy + start FastAPI app |
| `make deploy-all` | dev — deploy + start both apps (FastAPI + MCP) |
| `make deploy-mcp` | only MCP server changed |
| `make deploy-prod` | production target |
| `make deploy-no-run` | deploy artifacts without starting apps |
| `make bundle-validate` | validate `databricks.yml` only |
| `make bundle-summary` | preview what will deploy |

`make deploy` runs `scripts/bootstrap/app-permissions.sh` automatically on
the first deploy (see README.md). If the user runs `databricks bundle
deploy` directly, run `make bootstrap-perms` once afterwards (idempotent).

## Post-deploy

1. **Compute > Apps > ontobricks > Resources** — confirm `sql-warehouse` and `volume` are bound. If missing, the user must bind them manually (binding cannot be done via DAB).
2. **Logs** — confirm startup completed cleanly.
3. If the registry volume is empty, tell the user to open **Settings > Registry > Initialize** in the app UI.
4. Hit `/healthz` to confirm the FastAPI app is responsive.
5. **Re-check `app_status.state` about a minute after the deploy reports success.**
   A dependency-download failure surfaces as a crash ~45s *after* "App started
   successfully", so a single immediate `/healthz` 200 does not prove the deploy
   is healthy: `databricks apps get <app> --output json`.

## Release flow

When the user says "release vX.Y.Z" rather than just "deploy":

1. `make test`
2. Bump version in `pyproject.toml`
3. `git add -A && git commit -m "Release vX.Y.Z" && git tag vX.Y.Z && git push origin main --tags`
4. `make deploy` (or `make deploy-prod`)
5. Update the changelog (`changelog` skill).

## Don't

- Don't deploy without running tests.
- Don't `databricks bundle deploy` directly without remembering `make bootstrap-perms` for first-time deploys.
- Don't claim deploy is done until the app status is confirmed (or the user accepts `deploy-no-run`).
- Don't run a bare `uv lock` / `uv sync` / `uv run` (no `--frozen`) in the dev
  shell — it re-resolves against `pypi-proxy.dev.databricks.com` and rewrites
  every `uv.lock` URL to the proxy, which breaks the next `--frozen` deploy.
  The container then fails to download any wheel the proxy hasn't cached and the
  app crashes shortly after reporting "started successfully". Use
  `--frozen` for everyday work; only re-lock via
  `UV_INDEX_URL=https://pypi.org/simple uv lock` after a real dependency change.
- Don't commit a `uv.lock` containing `pypi-proxy.dev.databricks.com` URLs.
