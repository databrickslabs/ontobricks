# Refactor: src/mcp-server/server/app.py

## Summary

`server/app.py` (2131 LOC) is the entire MCP server: URI/text helpers, an HTTP
client layer (auth, retry, GET/POST), a 1170-line `create_mcp_server` factory
closure holding all session state + policy gating + every `@mcp.tool` /
`@mcp.resource` handler, and the Databricks app builder. Everything lives in one
module. This refactor splits it by topic into a `server/` package, keeping
`server/app.py` as a thin re-export facade so every existing import path and
entry point (`server.app:combined_app`, `from server.app import …`) is unchanged.

## Code smells observed

- **Large Class / God Module** — `app.py` at 2131 LOC mixes six unrelated concerns.
- **Long Method** — `create_mcp_server` is ~1170 LOC.
- **Module-level functions sharing state** — the factory closure threads mutable
  dicts (`_selected_domain`, `_domain_policy`, `_class_actions`, `_registry`,
  `_shared_client`) through ~15 nested helpers → convert to a class (`.coding_rules §2`).

## Plan (Fowler refactorings), applied in order, tests green between each

1. **Extract Module** `constants.py` — tool sets, `API_V1_*`, RDF constants,
   `MAX_DEPTH`, `_USER_AGENT`.
2. **Extract Module** `uri_helpers.py` — `_local_name`, `_pretty_predicate`,
   `_is_uri`, `_is_label_predicate`.
3. **Extract Module** `formatting.py` — `_preferred`, `_hint`, all `_format_*`,
   `_merge_uri_aliases`.
4. **Extract Module** `http_client.py` — `_base_url`, `_get_auth_headers`,
   `_get`, `_post`, retry helpers, `_oauth_cache`.
5. **Extract Class** `MCPServerSession` (`session.py`) — mutable state + `_client`,
   `_ensure_registry`, `_registry_params`, `_domain_params`, `_label_or_local`,
   and the policy/gating helpers.
6. **Move Method** the `@mcp.tool` / `@mcp.resource` handlers into
   `register_tools` / `register_resources` (`tools.py`, `resources.py`) that take
   `(mcp, session)`.
7. **Move Function** `create_mcp_server` + `create_databricks_app` → `factory.py`.
8. Rewrite `app.py` as a re-export facade; keep `combined_app = create_databricks_app()`.

## Public API preservation

- `app.py` re-exports every name the tests + entry points import:
  `create_mcp_server`, `create_databricks_app`, `combined_app`, `REGISTRY_TOOLS`,
  `GRAPH_TOOLS`, `API_V1_*`, `_format_*`, `_local_name`, `_preferred`.
- Entry points unchanged: `server.app:combined_app` (main.py / uvicorn),
  `from server.app import create_mcp_server` (mcp_server.py, fixtures).

## Test impact (mechanical, non-behavioural)

Tools resolve `_get`/`_post`/`_get_auth_headers`/`_base_url` via late binding on
their new home module (`http_client`). Four tests monkeypatch those on `server.app`
today; their patch **target module path** moves to `server.http_client`
(assertions unchanged):
- `tests/units/mcp/test_domain_policy_gating.py` — `_get`, `_post`.
- `tests/mcp/integration/test_{smoke,more_smoke,dataset_context}_tools.py` —
  `_get_auth_headers`, `_base_url`.

## Verification

- `uv run --frozen pytest -q tests/units/mcp tests/mcp` green at every step
  (baseline: 104 passed).
- `wc -l server/*.py` shows each file well under 800 LOC.
