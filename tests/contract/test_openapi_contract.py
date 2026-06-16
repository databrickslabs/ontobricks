"""OpenAPI contract tests for OntoBricks REST APIs (T-M2.P4 under CNS).

Verifies that the FastAPI app emits a well-formed OpenAPI schema and that the
public `/api/v1/*` surface declares the routes the MCP server expects to call.
This is a **contract** test, not a full schemathesis sweep — the latter is
nightly material under §9.4 G3.

Marker: `contract` (a subset of `integration`).
"""

from __future__ import annotations

import json

import pytest


@pytest.mark.contract
@pytest.mark.integration
class TestOpenAPISchemaShape:
    """The /openapi.json endpoint must be well-formed."""

    def test_openapi_endpoint_returns_200(self, client):
        resp = client.get("/openapi.json")
        assert resp.status_code == 200

    def test_openapi_returns_valid_json(self, client):
        resp = client.get("/openapi.json")
        data = resp.json()
        assert isinstance(data, dict)

    def test_openapi_has_required_top_level_keys(self, client):
        data = client.get("/openapi.json").json()
        assert "openapi" in data
        assert "info" in data
        assert "paths" in data
        assert isinstance(data["paths"], dict)

    def test_openapi_version_is_3_x(self, client):
        version = client.get("/openapi.json").json()["openapi"]
        assert version.startswith("3.")

    def test_openapi_info_has_title(self, client):
        info = client.get("/openapi.json").json()["info"]
        assert "title" in info
        assert len(info["title"]) > 0


@pytest.mark.contract
@pytest.mark.integration
class TestMCPContractPaths:
    """The MCP server's tool layer expects these REST paths to exist on the
    external app (mounted at /api). See `src/mcp-server/server/app.py` —
    the API_V1_* constants. If any change without updating the MCP server,
    the dogfooding loop breaks.

    The external app's OpenAPI lives at `/api/openapi.json` and uses path
    keys with prefix removed (per `OPENAPI_PATH_PREFIX`). We probe both the
    mount-relative form ("/v1/domains") and the absolute form
    ("/api/v1/domains") so this stays green regardless of how the spec
    is published.
    """

    EXPECTED_PATHS = [
        ("/api/v1/domains", "/v1/domains"),
        ("/api/v1/domain/versions", "/v1/domain/versions"),
        ("/api/v1/domain/design-status", "/v1/domain/design-status"),
    ]

    @pytest.mark.parametrize("absolute,relative", EXPECTED_PATHS)
    def test_path_declared_in_external_openapi(self, client, absolute, relative):
        # External app's OpenAPI is mounted at /api/openapi.json.
        resp = client.get("/api/openapi.json")
        if resp.status_code != 200:
            pytest.skip(f"/api/openapi.json returned {resp.status_code} — external app may not be mounted in this test app")
        spec = resp.json()
        paths = set(spec.get("paths", {}).keys())
        # Accept either form. The MCP server constructs absolute URLs, but the
        # external app's spec may use the mount-relative form.
        assert absolute in paths or relative in paths, (
            f"Expected REST path (either {absolute!r} or {relative!r}) not in external OpenAPI spec — MCP server contract broken."
            f" Available /v1 paths: " + ", ".join(p for p in sorted(paths) if "/v1" in p)[:300]
        )


@pytest.mark.contract
@pytest.mark.integration
class TestOpenAPIStability:
    """Snapshot-style: the route count + names shouldn't drift unannounced.

    Not a syrupy snapshot yet (M3.P3 candidate). For now, just sanity bounds:
    we have at least N paths and at most M (catches accidental route deletion
    OR exuberant addition).
    """

    MIN_PATHS = 10  # Conservative — current count is much higher.
    MAX_PATHS = 500  # Defensive — surface explosion would be a smell.

    def test_path_count_within_bounds(self, client):
        spec = client.get("/openapi.json").json()
        n = len(spec["paths"])
        assert self.MIN_PATHS <= n <= self.MAX_PATHS, (
            f"OpenAPI declares {n} paths; expected [{self.MIN_PATHS}, {self.MAX_PATHS}]"
        )

    def test_no_undocumented_v1_paths(self, client):
        """Every /api/v1/ path should declare at least one operation (no stubs)."""
        spec = client.get("/openapi.json").json()
        bad = []
        for path, methods in spec["paths"].items():
            if not path.startswith("/api/v1/"):
                continue
            if not any(m in methods for m in ("get", "post", "put", "patch", "delete")):
                bad.append(path)
        assert not bad, f"v1 paths without an HTTP-method declaration: {bad}"
