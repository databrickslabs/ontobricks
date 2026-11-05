# OntoBricks — Release Notes V0.9.2

**Release window:** October – November 2026<br>
**Type:** Patch release (v0.9.1 → v0.9.2)<br>
**Test status:** `uv run --frozen pytest -q -m "not scenario"`: 9653 passed, 326 skipped, 8 deselected.

This release is a patch on v0.9.1. It fixes MCP identity and domain resolution, SPARQL value filters, Apps deployment scopes, local setup on machines whose default `python3` is too old, and dependency security floors. No new user-facing feature.

---

## Highlights

- **MCP graph tools are authorised as the end user again** — Builders no longer get `You are not a member of this domain's team` through the MCP server. ([#208](https://github.com/databrickslabs/ontobricks/issues/208))
- **MCP tools work without a sticky session** — AI Playground opens a new session per tool call, so domain-scoped tools accept an explicit `domain_name`. ([#209](https://github.com/databrickslabs/ontobricks/issues/209))
- **SPARQL value FILTERs are applied** — typed BGP filters such as `FILTER(STR(?v) = "…")` and `CONTAINS(LCASE(STR(?v)), …)` compile to SQL instead of producing `WHERE 1=1`. Unmapped filters now fail closed. ([#210](https://github.com/databrickslabs/ontobricks/issues/210))
- **Databricks Apps deploy fixed** — `user_api_scopes` lists only `sql`; the identity scopes are platform defaults and are rejected when declared explicitly.
- **Dependency security floors raised and lockfiles regenerated** — Authlib, dnspython, msgpack, nltk, pyasn1, graphql-core, joserfc, and others, with `uv.lock` and `src/mcp-server/uv.lock` regenerated.

---

## MCP identity and domain resolution

### End-user identity forwarded past the Apps proxy

Graph tools called through the MCP server were authorised as the MCP app service principal, even for domain Builders. Databricks Apps overwrites `x-forwarded-*` on the MCP → UI hop with the M2M bearer, so the header-forwarding design never reached `PermissionMiddleware`.

- The MCP server sends `X-OntoBricks-Acting-*` headers on outbound M2M calls instead of `x-forwarded-*`.
- `PermissionMiddleware` swaps to the acting email and token only when the proxy identity is the allowlisted MCP service principal (`MCP_SP_CLIENT_ID`). The swap applies to `/api` and `/graphql` as well as HTML routes.
- Domain filtering and Team/group lookup prefer the user token after the swap.
- Both apps declare the `sql` user API scope so the inbound MCP call carries a UC-capable user token.

### Domain resolution without a sticky session

Each MCP tool call can arrive in a new StreamableHTTP session, so `select_domain` keyed by `mcp-session-id` was lost between calls. Per-session isolation is kept.

- Domain-scoped tools accept an optional `domain_name` argument.
- Resolution order: `domain_name` argument, then `MCP_DEFAULT_DOMAIN`, then the only published MCP domain.
- Factory instructions tell clients to pass `domain_name` when the session is not reused.

---

## SPARQL

- `SparqlValueFilterSql` extracts the SQL compilation of string `FILTER`s and applies them on typed basic graph patterns.
- `SparqlTranslator` calls the compiler from the typed SQL path and reuses it on generic triples.
- Accepted shapes: `STR(?v) = "…"` and `CONTAINS(LCASE(STR(?v)), …)`. Any filter that cannot be mapped to a column raises an error rather than returning unfiltered results.

---

## Deployment and local setup

### Apps deploy scopes

`make deploy` failed at app update with `INVALID_PARAMETER_VALUE: The specified scope iam.current-user:read is not a valid scope`. Databricks Apps now auto-assigns `iam.current-user:read` and `iam.access-control:read`, and rejects them when listed.

- `databricks.yml`: `user_api_scopes` reduced to `[sql]` on `ontobricks_dev_app` and `mcp_ontobricks_app`.
- `app.yaml.template`: `authorization.user.scopes` reduced to `[sql]`.

### Local setup on machines with an old `python3`

`./scripts/start.sh` aborted on macOS, where `/usr/bin/python3` is 3.9. The venv was never created even though `uv` could provision CPython ≥ 3.11.

- Local preflight accepts `uv` as the interpreter source when it is on `PATH`, even if the default `python3` is 3.9.
- `scripts/setup.sh` installs `uv` before the `--local` preflight, then runs `uv venv`.

### MCP_SP_CLIENT_ID on redeploy

`scripts/deploy.sh` and `scripts/deploy.config.sh` inject `MCP_SP_CLIENT_ID` from `databricks apps get` so the identity swap is configured on every redeploy.

---

## Security (Dependabot)

Constraint floors raised in `pyproject.toml` and `src/mcp-server/pyproject.toml`:

| Package | Floor | Notes |
|---------|-------|-------|
| authlib | ≥ 1.9.1 | Also in the MCP server |
| dnspython | ≥ 2.9.0 | Requires Python ≥ 3.11, so the MCP server now requires Python ≥ 3.11 |
| msgpack | ≥ 1.2.3 | |
| nltk | ≥ 3.10.4 | Still has an open advisory (see below) |
| pyasn1 | ≥ 0.6.5 | Quadratic BER/CER/DER decoding DoS |
| graphql-core | ≥ 3.3.1 | Memory leak on aborted validations (via strawberry-graphql) |
| joserfc | ≥ 1.7.6 | Unbounded JWE `recipients` array; compact-JWS size-guard allocation |

Previously pinned packages that were never resolved into the lock (Pillow, filelock, GitPython, MLflow, certifi, FastMCP, and others) are now resolved.

`uv.lock` and `src/mcp-server/uv.lock` were regenerated against the Databricks PyPI proxy. The proxy URLs were then rewritten to `pypi.org/simple` and `files.pythonhosted.org`, which mirror the same layout. `uv lock --check` passes with the public index.

**Still open (blocked upstream):**

- **oauthlib 4.0.0** — `databricks-sql-connector` 4.4–4.6 pins `oauthlib<4.0.0`. Accepted risk.
- **nltk GHSA-8mgp-746c-j5xp** — no patched release yet.

---

## Documentation

- **`docs/deployment.md`**: acting headers vs Apps `x-forwarded-*` overwrite, identity scopes as platform defaults, Apple `python3` 3.9 accepted when `uv` is on `PATH`.
- **`docs/architecture.md`**: acting-identity flow; typed BGP string `FILTER`s compile to SQL.
- **`docs/user-guide.md`** and **`docs/mcp.md`**: AI Playground one-shot sessions, `domain_name`, `MCP_DEFAULT_DOMAIN`.
- **`docs/getting-started.md`**: `uv`-provisioned CPython and the setup order.
- **`docs/development.md`**: current security floors and the MCP Python 3.11 floor.

---

## Upgrade notes

- **Apps deploy:** if `databricks.yml` or `app.yaml` still lists `iam.current-user:read` or `iam.access-control:read` in `user_api_scopes`, remove them. Only `sql` may be declared.
- **MCP clients:** AI Playground and any client that does not reuse the MCP session must pass `domain_name` on domain-scoped tools, or set `MCP_DEFAULT_DOMAIN`.
- **Redeploy:** `MCP_SP_CLIENT_ID` is injected by `scripts/deploy.sh`. Use the deploy script rather than a raw bundle deploy so the identity swap stays configured.
- **Lockfiles:** the MCP server now requires Python ≥ 3.11, matching the app.
- **Known open items:** oauthlib 4.0.0 and the nltk advisory above remain open.
