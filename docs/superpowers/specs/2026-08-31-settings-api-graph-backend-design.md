# Settings API Graph Backend Design

## Goal

Keep Settings → API as a curated, interactive external API reference while
making its domain selector, examples, controls, and Swagger schema reflect the
published-domain and graph-backend contracts.

## Current problem

Settings → API renders a hand-authored endpoint catalog. Its domain selector
loads `/settings/registry/domains`, which can include domains and versions that
the external API cannot serve. The `/api/v1/domains` example omits
`graph_backend`, `has_graph`, and `mcp_policy`. GraphQL copy still describes the
removed Active toggle and does not require a materialized graph.

Swagger already exposes the typed `GET /api/v1/domains` response after the
external API update, but stateless `POST /api/v1/domain/info` still uses the
generic `SuccessResponse`, so its `graph_backend` payload is invisible in the
generated schema.

## Approved approach

Retain the curated cards and Try-it workflow. Synchronize their data and
availability with the external API instead of replacing the page with a second
Swagger renderer.

## Settings → API behavior

### Domain and version selection

- Load the domain selector from `GET /api/v1/domains`.
- Show only domains exposed by the external API.
- Store `graph_backend` and `has_graph` on each option.
- Annotate option labels with the configured backend.
- Load versions from `GET /api/v1/domain/versions`.
- Offer only versions with `is_published: true`, because external domain data
  endpoints reject DRAFT and IN-REVIEW versions.
- Label the empty version choice as the numeric-latest PUBLISHED version.

### Selected-domain status

Add a compact status line below the selectors:

- `No Backend · Ontology only` when `graph_backend == "none"`.
- `<Backend> · Graph ready` when `has_graph == true`.
- `<Backend> · Awaiting first build` otherwise.

The status is informational and uses existing Bootstrap badges and project
tokens; no new card or page-level visual language is introduced.

### Endpoint availability

- Domain discovery, versions, design status, and OWL remain callable for every
  listed domain.
- R2RML and Spark SQL remain callable and return their normal API validation
  response when mappings are absent.
- For `graph_backend == "none"`, disable graph build, graph query, traversal,
  data-quality execution, inference, cohort execution, GraphiQL, GraphQL query,
  and GraphQL schema Try-it controls.
- For a configured backend with `has_graph == false`, keep build controls
  enabled but disable controls that require existing graph data.
- Registry/status discovery controls remain enabled.
- Disabled controls expose a concise title explaining whether the domain is
  ontology-only or awaits its first build.
- Changing the domain immediately recomputes availability and clears stale
  Try-it responses.

### Curated content

- Update the `/api/v1/domains` description and sample to include
  `graph_backend`, `has_graph`, and `mcp_policy`.
- Replace Active-toggle wording in the GraphQL section with PUBLISHED,
  API-exposed, materialized-graph wording.
- Explain the difference between configured backend and graph availability.
- Keep direct links to `/api/docs` and `/api/redoc`.

## Swagger/OpenAPI

- Keep the external docs at `/api/docs`, `/api/redoc`, and
  `/api/openapi.json`.
- Preserve the `DomainInfo.graph_backend` enum and description.
- Add a dedicated typed response model for `POST /api/v1/domain/info` so
  Swagger documents `graph_backend` and the existing statistics fields instead
  of showing opaque `Any`.
- Update external API and GraphQL tag descriptions to state that `none` is
  ontology-only and GraphQL lists materialized graph domains only.
- Preserve tag, contact, and license metadata when the custom external OpenAPI
  schema rewrites mounted paths; the current custom builder drops those fields.
- Do not change endpoint paths or request compatibility aliases.

## Files

- `src/front/templates/partials/dtwin/_query_api.html`
- `src/front/static/query/js/query-api.js`
- `src/front/static/query/css/query-api.css` only if a small status-line layout
  rule cannot be expressed with existing utilities
- `src/api/routers/v1.py`
- `src/api/constants.py`
- `src/api/external_app.py`
- relevant frontend, API, and OpenAPI contract tests
- `documentation/api.md`, `documentation/user-guide.md`, and the versioned
  changelog when implementation changes their documented behavior

## Testing

- Static frontend contracts pin the selector source, status element, current
  response samples, and graph-required markers.
- JavaScript contracts cover ontology-only, unbuilt graph-backed, and
  graph-ready states.
- API tests pin the typed stateless domain-info response.
- OpenAPI tests assert the `POST /api/v1/domain/info` response schema includes
  `graph_backend`.
- Browser verification covers Settings → API at desktop and mobile widths,
  selector/version loading, status badges, disabled graph controls, enabled
  ontology controls, card expansion, Try-it behavior, and console/network
  errors.
- Run `uv run --frozen pytest -q -m "not scenario"`.

## Non-goals

- Generating the curated cards dynamically from OpenAPI.
- Replacing Swagger or ReDoc.
- Adding or renaming API endpoints.
- Changing backend build behavior or MCP policy.
- Redesigning the full Settings page.
