"""GraphQL schema contract tests (T-M2.P5 under CNS).

Locks the GraphQL public surface so consumers (notably the MCP server's
`query_graphql` tool and the front-end dtwin canvas) don't silently drift.

The schema is exposed at three places that all matter:
- `/dtwin/graphql/schema` — the canonical schema document for the current dtwin.
- `/graphql/settings/depth` — the configured max query depth.
- `/graphql/{domain_name}` and `/graphql/{domain_name}/schema` — per-domain.

This test asserts the routes exist and the schema document is well-formed.
The MCP server's `get_graphql_schema` tool expects these endpoints to be
shaped this way; if they drift, the dogfooding loop breaks.
"""

from __future__ import annotations

import pytest


@pytest.mark.contract
@pytest.mark.integration
class TestGraphQLEndpointsDeclared:
    """The four canonical GraphQL routes must be declared on the app."""

    EXPECTED_ROUTES = [
        ("/dtwin/graphql/schema", {"GET"}),
        ("/dtwin/graphql/execute", {"POST"}),
        ("/graphql/settings/depth", {"GET"}),
        # /graphql/{domain_name} is registered as two distinct APIRoutes
        # (one GET, one POST) — collected as a union for this test.
        ("/graphql/{domain_name}", {"GET", "POST"}),
        ("/graphql/{domain_name}/schema", {"GET"}),
    ]

    @pytest.mark.parametrize("path,methods", EXPECTED_ROUTES)
    def test_route_registered(self, path, methods):
        """The path is registered on the main FastAPI app with the expected methods.

        FastAPI may register one APIRoute per method, so we collect the union
        of method sets across all routes matching the path.
        """
        from shared.fastapi.main import app

        found_methods: set[str] = set()
        for route in app.routes:
            if getattr(route, "path", None) == path:
                found_methods |= set(getattr(route, "methods", set()))
        assert found_methods, f"GraphQL route {path!r} not registered on the app"
        missing = methods - found_methods
        assert not missing, (
            f"{path} declared but missing methods {missing} "
            f"(has {found_methods})"
        )


@pytest.mark.contract
@pytest.mark.integration
class TestGraphQLSchemaEndpoint:
    """The /dtwin/graphql/schema endpoint must be reachable and obey the contract.

    Empty-ontology behaviour is part of the contract: when there are zero
    classes, the endpoint returns 400 with a `ValidationError` JSON body
    (per `back/core/errors`). When there are classes, it returns 200 with
    SDL. Both are valid contract outcomes — what's NOT acceptable is a 500
    or a missing route.
    """

    def test_schema_endpoint_reachable(self, client):
        """Either 200 (with classes) or 400 (empty ontology); never 5xx or 404."""
        resp = client.get("/dtwin/graphql/schema")
        assert resp.status_code in (200, 400), (
            f"expected 200 (with classes) or 400 (empty ontology); got {resp.status_code}"
        )

    def test_schema_content_shape(self, client):
        """200 → SDL string; 400 → JSON error body per OntoBricksError contract."""
        resp = client.get("/dtwin/graphql/schema")
        if resp.status_code == 200:
            body = resp.text
            assert "type Query" in body or "type " in body or "schema {" in body, (
                f"200 response from schema endpoint is not SDL-shaped: {body[:200]!r}"
            )
        elif resp.status_code == 400:
            data = resp.json()
            # OntoBricksError -> ErrorResponse: {error, message, detail?, request_id?}
            assert "error" in data and "message" in data, (
                f"400 response missing OntoBricksError shape: {data!r}"
            )

    def test_schema_endpoint_idempotent(self, client):
        r1 = client.get("/dtwin/graphql/schema")
        r2 = client.get("/dtwin/graphql/schema")
        # Two calls without state mutation in between should return the same
        # status. Body equality is too strict — Strawberry may reorder types.
        assert r1.status_code == r2.status_code


@pytest.mark.contract
@pytest.mark.integration
class TestGraphQLDepthSetting:
    """The configured max query depth must be exposed and be sane."""

    def test_depth_endpoint_returns_200(self, client):
        resp = client.get("/graphql/settings/depth")
        assert resp.status_code == 200

    def test_depth_value_is_a_positive_int(self, client):
        data = client.get("/graphql/settings/depth").json()
        # Permit either {"depth": N} or just N at top level — accept both.
        depth = data.get("depth", data) if isinstance(data, dict) else data
        if isinstance(depth, dict):
            # Look for any numeric value in the dict.
            numbers = [v for v in depth.values() if isinstance(v, int) and v > 0]
            assert numbers, f"no positive int depth in {depth}"
            return
        assert isinstance(depth, int) and depth > 0, f"depth not a positive int: {depth!r}"
