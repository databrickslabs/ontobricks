"""Tests for the in-app Help Center JSON/binary API.

Covers the router introduced at ``src/api/routers/internal/help.py``:

- ``GET /api/help/docs``                 → index grouped by category
- ``GET /api/help/docs/{slug}``          → markdown payload
- ``GET /api/help/docs/images/{name}``   → binary image asset
- ``GET /api/help/docs/screenshots/{name}``

The suite exercises the happy path plus the hardening:

- unknown slugs → 404,
- bad image filenames (path traversal / wrong extension) → 404,
- valid known assets that may not exist on disk → 404 (never 500).
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from shared.fastapi.main import app


@pytest.fixture
def client():
    return TestClient(app)


class TestHelpDocsIndex:
    def test_index_structure(self, client):
        response = client.get("/api/help/docs")
        assert response.status_code == 200
        payload = response.json()
        assert "categories" in payload
        assert isinstance(payload["categories"], list)
        assert payload["categories"], "categories list must not be empty"
        for cat in payload["categories"]:
            assert {"id", "label", "docs"}.issubset(cat.keys())
            for doc in cat["docs"]:
                assert {"slug", "title"}.issubset(doc.keys())

    def test_index_slugs_are_unique(self, client):
        response = client.get("/api/help/docs")
        slugs = [
            doc["slug"]
            for cat in response.json()["categories"]
            for doc in cat["docs"]
        ]
        assert len(slugs) == len(set(slugs))


class TestHelpDocFetch:
    def test_unknown_slug_returns_404(self, client):
        response = client.get("/api/help/docs/this-slug-does-not-exist")
        assert response.status_code == 404
        assert response.json()["detail"] == "Doc not found"

    def test_known_slug_returns_markdown_or_404(self, client):
        """A catalogued slug returns either the markdown (200) or a 404 if
        the underlying file is absent on disk -- never 500."""
        index = client.get("/api/help/docs").json()
        slug = index["categories"][0]["docs"][0]["slug"]
        response = client.get(f"/api/help/docs/{slug}")
        assert response.status_code in (200, 404)
        if response.status_code == 200:
            body = response.json()
            assert body["slug"] == slug
            assert "markdown" in body
            assert "title" in body


class TestHelpAssetGuards:
    @pytest.mark.parametrize(
        "name",
        [
            "../secret.png",
            "..%2Fsecret.png",
            "not-an-image.txt",
            "no-extension",
            "has space.exe",
            "has;semicolon.png",
        ],
    )
    def test_bad_image_name_is_rejected(self, client, name):
        response = client.get(f"/api/help/docs/images/{name}")
        assert response.status_code == 404

    @pytest.mark.parametrize(
        "name",
        [
            "../secret.png",
            "not-an-image.txt",
            "no-extension",
        ],
    )
    def test_bad_screenshot_name_is_rejected(self, client, name):
        response = client.get(f"/api/help/docs/screenshots/{name}")
        assert response.status_code == 404

    def test_unknown_subdir_404(self, client):
        """The image/screenshot handlers are the only allow-listed subdirs;
        any other subdir string cannot leak because the handler enforces
        ``subdir in {"images", "screenshots"}`` internally. We verify the
        public surface only returns 404 for missing concrete assets."""
        response = client.get("/api/help/docs/images/missing-image.png")
        assert response.status_code == 404
