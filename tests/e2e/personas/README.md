# Persona-based UAT suite

A job-function **persona** layer on top of the feature-organized `tests/e2e`
Playwright suite. Each persona follows a real archetype through their goals
**and** proves the permission boundaries that separate them.

## Personas

| Persona | App role | Domain role | Owns |
|---|---|---|---|
| **Priya** — Platform Admin | `admin` | `admin` | Settings, app permissions, Teams matrix, registry config, health, publish/revert |
| **Olu** — Ontology Engineer | `app_user` | `builder` | Ontology design (classes/props/SWRL/SHACL/axioms), OWL gen/import, build, submit-for-review |
| **Dana** — Data Engineer | `app_user` | `builder` | Data sources, mapping/R2RML (manual/auto-map/diagnostics), build/sync |
| **Sam** — Data Steward | `app_user` | `editor` | Edit ontology/mappings, data quality, review + sign-off (no build/publish) |
| **Cory** — Business Consumer | `app_user` | `viewer` | Read-only: browse, KG explore (SPARQL/GraphQL), cohorts/DQ/inference, API/MCP |
| *Nina* — No-Domain (edge) | `app_user` | `none` | drives `/access-denied?reason=domain` |
| *Nora* — No-App (edge) | `none` | `none` | drives `/access-denied?reason=app` |

Defined once in [`personas.py`](./personas.py).

## How a persona is simulated — the test-auth seam

Locally every request is `admin` (the middleware short-circuits when not a
Databricks App). To drive non-admin personas **offline and deterministically**,
`PermissionMiddleware` has a prod-safe seam:

```
ONTOBRICKS_TEST_AUTH=1  +  not a real Databricks App  +  x-ontobricks-test-role header
        → role/domain-role taken from headers, then the REAL gates run
```

The triple gate makes it **inert in production** (a deployed app is always
`is_databricks_app()`), and inert without the header (so the 257 existing e2e
tests still see `admin`). `tests/e2e/conftest.py` sets `ONTOBRICKS_TEST_AUTH=1`
on the local subprocess automatically; persona contexts attach the headers via
`extra_http_headers` (see [`conftest.py`](./conftest.py)).

> The deployed app does **not** enable the seam, so the offline persona tests
> skip in live mode — use the live-smoke file for live acceptance.

## Coverage tiers

* **Tier 1 — offline (CI nightly, `-m "uat and not live_integration"`):** UI
  render, navigation, **permission gating (positive + negative)**, session-local
  CRUD, pure-compute (OWL/SHACL/SWRL/R2RML generation, SPARQL validation,
  GraphQL schema), and graceful-degradation of Databricks-backed actions
  (request authorized / clean error). Databricks-dependent *success* is asserted
  only at the authorization boundary and skips cleanly without creds.
* **Tier 2 — live smoke (`-m "uat and live_integration"`):** the real
  Databricks journeys (registry discovery, real SPARQL, build/sync,
  status/stats) against a deployed app, plus the JS-only UI indicators
  (`body.role-viewer`, role pill) that require app mode.

## Files

| File | What it proves |
|---|---|
| `test_permission_matrix.py` | persona × endpoint authorization (the exhaustive negative coverage) |
| `test_ui_gating.py` | server-rendered body roles + `data-requires-app="admin"` visibility |
| `test_access_denied_personas.py` | edge personas → `/access-denied?reason=domain|app` |
| `test_lifecycle_gate_personas.py` | DRAFT-editable; locked-version edit-gate (best-effort) |
| `test_admin_journeys.py` | Priya: settings/teams/warehouse + authorized everywhere |
| `test_ontology_engineer_journeys.py` | Olu: import → CRUD → DQ → generate OWL → build |
| `test_data_engineer_journeys.py` | Dana: mapping save/add/R2RML/diagnostics → build |
| `test_data_steward_journeys.py` | Sam: edits allowed; build/cohorts/settings blocked |
| `test_consumer_journeys.py` | Cory: browse allowed; every write 403 |
| `test_api_personas.py` | REST `/api/v1` is an open (un-gated) programmatic surface |
| `test_graphql_personas.py` | `/dtwin/graphql` schema read vs execute (POST) gating |
| `test_mcp_personas.py` | MCP discovery tools present and callable |
| `test_live_smoke.py` | Tier-2 real Databricks journeys (live mode only) |

## Running

```bash
# Offline (Tier-1) — deterministic, no Databricks needed
make test-uat
#   ≡ ONTOBRICKS_E2E_FAKE_CREDS=1 uv run pytest tests/e2e/personas \
#       -m "uat and not live_integration" --no-cov

# A single persona / file
ONTOBRICKS_E2E_FAKE_CREDS=1 uv run pytest \
  tests/e2e/personas/test_consumer_journeys.py -m uat --no-cov

# Tier-2 live smoke (deployed app + seeded .domain_permissions.json)
export ONTOBRICKS_LIVE_BASE=https://<app-host>
export DATABRICKS_CONFIG_PROFILE=fevm-ontobricks-int
make test-uat-live
```

To run the **Databricks-backed Tier-1 steps for real** (instead of skipping),
run without `ONTOBRICKS_E2E_FAKE_CREDS` and with a working
`fevm-ontobricks-int` CLI profile — the conftest mints a workspace token and the
local subprocess talks to the int workspace.

## Conventions

Mirror the rest of `tests/e2e`: Playwright **sync** API, `live_server` +
`persona_page(persona)` fixtures, CSRF primed via a GET then `X-CSRF-Token`
header, stable IDs / `data-section` / `data-requires*` selectors (no
`data-testid`). Helpers live in [`_helpers.py`](./_helpers.py).
