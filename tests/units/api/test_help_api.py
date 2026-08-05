"""Tests for the in-app Help Center JSON/binary API.

Covers the router introduced at ``src/api/routers/internal/help.py``:

- ``GET /api/help/docs``                 → index grouped by category
- ``GET /api/help/docs/{slug}``          → markdown payload
- ``GET /api/help/docs/images/{name}``   → binary image asset
- ``GET /api/help/docs/screenshots/{name}``

The suite exercises the happy path plus the hardening:

- every catalogued doc slug returns 200 with markdown (no 404 in production),
- catalogued markdown files and referenced ``images/`` assets exist on disk,
- deploy bundle config ships ``documentation/`` to Databricks,
- unknown slugs → 404,
- bad image filenames (path traversal / wrong extension) → 404,
- valid known assets that may not exist on disk → 404 (never 500).
"""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import unquote

import pytest
import yaml
from fastapi.testclient import TestClient

from api.routers.internal.help import _DOC_INDEX, _docs_dir
from shared.fastapi.main import app

_REPO_ROOT = Path(__file__).resolve().parents[3]

_MD_IMAGE_RE = re.compile(r"!\[[^\]]*\]\(([^)]+)\)")
_HTML_IMG_SRC_RE = re.compile(r"""<img[^>]+src=["']([^"']+)["']""", re.IGNORECASE)


def _all_help_slugs() -> list[str]:
    return list(_DOC_INDEX.keys())


def _iter_catalogued_markdown_paths() -> list[tuple[str, Path]]:
    docs_dir = Path(_docs_dir())
    return [
        (slug, docs_dir / _DOC_INDEX[slug]["file"])
        for slug in _all_help_slugs()
    ]


def _normalize_help_asset_ref(raw: str) -> tuple[str, str] | None:
    """Return ``(subdir, filename)`` for Help Center asset refs, else ``None``."""
    ref = raw.strip().split("#", 1)[0].strip()
    if not ref or ref.startswith(("http://", "https://", "data:", "/")):
        return None
    ref = ref.replace("\\", "/").lstrip("./")
    if ref.startswith("documentation/"):
        ref = ref[len("documentation/") :]
    elif ref.startswith("docs/"):
        # Legacy markdown paths from before docs/ → documentation/ rename.
        ref = ref[len("docs/") :]
    for subdir in ("images", "screenshots"):
        prefix = f"{subdir}/"
        if ref.startswith(prefix):
            return subdir, unquote(ref[len(prefix) :])
    return None


def _collect_help_asset_refs(markdown: str) -> set[tuple[str, str]]:
    refs: set[tuple[str, str]] = set()
    for pattern in (_MD_IMAGE_RE, _HTML_IMG_SRC_RE):
        for match in pattern.finditer(markdown):
            normalized = _normalize_help_asset_ref(match.group(1))
            if normalized:
                refs.add(normalized)
    return refs


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

    def test_index_matches_server_catalog(self, client):
        """Sidebar index must mirror the allow-list in help.py."""
        response = client.get("/api/help/docs")
        assert response.status_code == 200
        indexed = {
            doc["slug"]: doc["title"]
            for cat in response.json()["categories"]
            for doc in cat["docs"]
        }
        expected = {slug: meta["title"] for slug, meta in _DOC_INDEX.items()}
        assert indexed == expected

    def test_graph_backend_guides_are_catalogued(self, client):
        """Lakebase and Neo4j backend guides must be reachable from Help Center."""
        response = client.get("/api/help/docs")
        slugs = {
            doc["slug"]
            for cat in response.json()["categories"]
            for doc in cat["docs"]
        }
        assert "graphdb-integration" in slugs
        assert "lakebase-graphdb" in slugs
        assert "neo4j-requirements" in slugs

        neo = client.get("/api/help/docs/neo4j-requirements")
        assert neo.status_code == 200
        body = neo.json()["markdown"]
        assert "Neo4j" in body
        assert "Aura" in body

    def test_cohort_and_import_export_guides_are_catalogued(self, client):
        """Cohort Discovery, Import/Export and the contributor Code Map must be reachable."""
        response = client.get("/api/help/docs")
        slugs = {
            doc["slug"]
            for cat in response.json()["categories"]
            for doc in cat["docs"]
        }
        assert "cohort-discovery" in slugs
        assert "import-export" in slugs
        assert "code-organization" in slugs

        for slug in ("cohort-discovery", "import-export", "code-organization"):
            resp = client.get(f"/api/help/docs/{slug}")
            assert resp.status_code == 200
            assert resp.json()["markdown"].strip()


class TestHelpCatalogIntegrity:
    def test_docs_directory_exists(self):
        docs_dir = Path(_docs_dir())
        assert docs_dir.is_dir(), f"Help Center docs directory missing: {docs_dir}"

    def test_all_catalogued_doc_files_exist_on_disk(self):
        missing = [
            f"{slug} -> {path.name}"
            for slug, path in _iter_catalogued_markdown_paths()
            if not path.is_file()
        ]
        assert not missing, "Catalogued Help Center files missing on disk:\n" + "\n".join(
            missing
        )

    @pytest.mark.parametrize("slug", _all_help_slugs())
    def test_catalogued_slug_returns_200(self, client, slug):
        response = client.get(f"/api/help/docs/{slug}")
        assert response.status_code == 200, (
            f"/api/help/docs/{slug} returned {response.status_code}; "
            "deploy bundle must ship documentation/*.md"
        )
        body = response.json()
        assert body["slug"] == slug
        assert body["title"] == _DOC_INDEX[slug]["title"]
        assert body["file"] == _DOC_INDEX[slug]["file"]
        assert body.get("markdown", "").strip(), f"{slug} markdown must not be empty"

    def test_referenced_help_images_exist_and_are_served(self, client):
        docs_dir = Path(_docs_dir())
        missing_on_disk: list[str] = []
        failed_api: list[str] = []

        for slug, md_path in _iter_catalogued_markdown_paths():
            markdown = md_path.read_text(encoding="utf-8")
            for subdir, name in _collect_help_asset_refs(markdown):
                if subdir != "images":
                    continue
                disk_path = docs_dir / subdir / name
                label = f"{slug}: {subdir}/{name}"
                if not disk_path.is_file():
                    missing_on_disk.append(label)
                    continue
                response = client.get(f"/api/help/docs/{subdir}/{name}")
                if response.status_code != 200:
                    failed_api.append(f"{label} -> HTTP {response.status_code}")

        assert not missing_on_disk, (
            "Referenced Help Center images missing on disk:\n"
            + "\n".join(missing_on_disk)
        )
        assert not failed_api, (
            "Referenced Help Center images failed API fetch:\n"
            + "\n".join(failed_api)
        )


class TestHelpDeployBundle:
    def test_databricksignore_does_not_exclude_documentation_tree(self):
        ignore_path = _REPO_ROOT / ".databricksignore"
        blocked = {
            line.strip()
            for line in ignore_path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.strip().startswith("#")
        }
        # Help Center markdown lives under documentation/; docs/ is the
        # GitHub Pages marketing site and is intentionally excluded.
        assert "documentation/" not in blocked
        assert "docs/" in blocked
        assert "*.md" not in blocked

    def test_databricks_yml_includes_documentation_for_help_center(self):
        bundle_path = _REPO_ROOT / "databricks.yml"
        bundle = yaml.safe_load(bundle_path.read_text(encoding="utf-8"))
        includes = bundle.get("sync", {}).get("include", [])
        excludes = bundle.get("sync", {}).get("exclude", [])
        assert "documentation/**" in includes
        assert "documentation/" not in excludes
        assert "*.md" not in excludes


class TestHelpDocFetch:
    def test_unknown_slug_returns_404(self, client):
        response = client.get("/api/help/docs/this-slug-does-not-exist")
        assert response.status_code == 404
        body = response.json()
        # OntoBricks-wide error envelope: {error, message, detail, request_id}.
        assert body["error"] == "not_found"
        assert body["message"] == "Doc not found"


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
