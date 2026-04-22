"""In-app Help Center documentation endpoints.

Serves the Markdown files from the repository's ``docs/`` folder and their
referenced images so the Help Center modal can render them client-side with
marked.js. Markdown remains the single source of truth.

Endpoints (all read-only, JSON/binary):

- ``GET /api/help/docs``                → list of available docs, grouped by category
- ``GET /api/help/docs/{slug}``         → ``{slug, title, markdown}``
- ``GET /api/help/docs/images/{name}``  → raw image file from ``docs/images/``

Access is restricted by a hard-coded allow-list (slug → relative path + title
+ category) and a strict filename regex on image names, to prevent any path
traversal attempt.
"""

from __future__ import annotations

import os
import re
from typing import Dict, List

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from back.core.logging import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["Help"], prefix="/api/help")


# ---------------------------------------------------------------------------
# Docs allow-list
# ---------------------------------------------------------------------------
# Order inside each category defines display order in the sidebar.
# ``slug`` is the public identifier used in URLs and deep-links.

_DOC_CATEGORIES: List[Dict] = [
    {
        "id": "getting-started",
        "label": "Getting Started",
        "docs": [
            {"slug": "readme", "file": "README.md", "title": "Overview"},
            {
                "slug": "get-started",
                "file": "get-started.md",
                "title": "Get Started",
            },
            {"slug": "info", "file": "INFO.md", "title": "Project Info"},
        ],
    },
    {
        "id": "guides",
        "label": "Guides",
        "docs": [
            {"slug": "user-guide", "file": "user-guide.md", "title": "User Guide"},
            {"slug": "examples", "file": "examples.md", "title": "Examples"},
            {"slug": "features", "file": "features.md", "title": "Features"},
            {"slug": "product", "file": "product.md", "title": "Product"},
            {
                "slug": "graphdb-integration",
                "file": "graphdb-integration.md",
                "title": "GraphDB Integration",
            },
            {"slug": "mcp", "file": "mcp.md", "title": "MCP"},
        ],
    },
    {
        "id": "reference-ops",
        "label": "Reference & Ops",
        "docs": [
            {
                "slug": "architecture",
                "file": "architecture.md",
                "title": "Architecture",
            },
            {"slug": "api", "file": "api.md", "title": "API Reference"},
            {"slug": "development", "file": "development.md", "title": "Development"},
            {"slug": "deployment", "file": "deployment.md", "title": "Deployment"},
        ],
    },
]

# Flat index for O(1) slug lookup.
_DOC_INDEX: Dict[str, Dict] = {
    doc["slug"]: {**doc, "category": cat["id"]}
    for cat in _DOC_CATEGORIES
    for doc in cat["docs"]
}

# Strict filename pattern for images (no path separators, no traversal).
# Allows spaces since some screenshots use them in their filenames.
_IMAGE_NAME_RE = re.compile(r"^[A-Za-z0-9_ .-]+\.(svg|png|jpg|jpeg|gif|webp)$")


def _docs_dir() -> str:
    """Resolve the repository's ``docs/`` directory.

    Uses the same relative-to-package trick as ``static_dir`` in
    ``src/shared/fastapi/main.py`` (3 levels up from ``src/front/routes/``
    lands on the repo root).
    """
    routes_dir = os.path.dirname(os.path.abspath(__file__))
    # routes_dir = <repo>/src/front/routes
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(routes_dir)))
    return os.path.join(repo_root, "docs")


def _audit_docs_on_disk() -> None:
    """Log a loud warning at boot if any catalogued doc is missing on disk.

    Prevents the class of bug where ``.databricksignore`` (or any other
    deploy-time filter) silently drops the ``docs/`` folder and every
    Help Center entry starts 404'ing in production.
    """
    docs_dir = _docs_dir()
    if not os.path.isdir(docs_dir):
        logger.warning(
            "Help Center docs directory is missing at %s — the deployed "
            "bundle is likely excluding docs/*.md (check .databricksignore). "
            "All /api/help/docs/* endpoints will return 404.",
            docs_dir,
        )
        return

    missing = [
        doc["file"]
        for doc in _DOC_INDEX.values()
        if not os.path.isfile(os.path.join(docs_dir, doc["file"]))
    ]
    if missing:
        logger.warning(
            "Help Center: %d catalogued doc file(s) missing on disk under %s: %s",
            len(missing),
            docs_dir,
            ", ".join(sorted(missing)),
        )


_audit_docs_on_disk()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/docs")
async def list_docs():
    """Return the catalogued list of docs grouped by category."""
    return {
        "categories": [
            {
                "id": cat["id"],
                "label": cat["label"],
                "docs": [
                    {"slug": d["slug"], "title": d["title"]} for d in cat["docs"]
                ],
            }
            for cat in _DOC_CATEGORIES
        ]
    }


_ASSET_SUBDIRS = {"images", "screenshots"}


def _serve_doc_asset(subdir: str, name: str) -> FileResponse:
    """Serve a whitelisted asset file from ``docs/<subdir>/``."""
    if subdir not in _ASSET_SUBDIRS:
        raise HTTPException(status_code=404, detail="Asset not found")
    if not _IMAGE_NAME_RE.match(name):
        raise HTTPException(status_code=404, detail="Asset not found")

    assets_root = os.path.realpath(os.path.join(_docs_dir(), subdir))
    path = os.path.join(_docs_dir(), subdir, name)
    real_path = os.path.realpath(path)
    if not real_path.startswith(assets_root + os.sep) and real_path != assets_root:
        raise HTTPException(status_code=404, detail="Asset not found")
    if not os.path.isfile(real_path):
        raise HTTPException(status_code=404, detail="Asset not found")

    return FileResponse(real_path)


@router.get("/docs/images/{name}")
async def get_doc_image(name: str):
    """Serve a whitelisted image file from ``docs/images/``."""
    return _serve_doc_asset("images", name)


@router.get("/docs/screenshots/{name}")
async def get_doc_screenshot(name: str):
    """Serve a whitelisted screenshot file from ``docs/screenshots/``."""
    return _serve_doc_asset("screenshots", name)


@router.get("/docs/{slug}")
async def get_doc(slug: str):
    """Return the raw Markdown text for a given doc slug."""
    doc = _DOC_INDEX.get(slug)
    if not doc:
        raise HTTPException(status_code=404, detail="Doc not found")

    path = os.path.join(_docs_dir(), doc["file"])
    if not os.path.isfile(path):
        logger.warning("Help doc file missing on disk: %s", path)
        raise HTTPException(status_code=404, detail="Doc file missing")

    try:
        with open(path, "r", encoding="utf-8") as fh:
            markdown = fh.read()
    except OSError as exc:
        logger.error("Failed to read help doc %s: %s", path, exc)
        raise HTTPException(status_code=500, detail="Failed to read doc") from exc

    return {
        "slug": doc["slug"],
        "title": doc["title"],
        "category": doc["category"],
        "file": doc["file"],
        "markdown": markdown,
    }
