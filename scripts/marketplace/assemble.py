#!/usr/bin/env python3
"""Assemble the Databricks Marketplace app folder.

Marketplace installs a Databricks App from a *subfolder* of a tagged Git
commit and runs it verbatim — no render step, no build step. This script
copies the runtime file set into ``<out>/ontobricks/`` and adds the static
``marketplace/app.yaml`` + ``marketplace/manifest.yaml`` at its root.

The relative layout (``run.py`` beside ``src/`` and ``docs/``) is preserved,
so the app behaves exactly as it does under ``make deploy``. The file set
mirrors the runtime payload listed in ``databricks.yml`` ``sync`` and
``.databricksignore``: ``src/`` (minus the MCP companion, which is not part of
the Marketplace listing) and the Help Center set ``docs/*.md`` +
``docs/images/`` + ``docs/screenshots/``.

Usage::

    python3 scripts/marketplace/assemble.py               # -> dist/marketplace/ontobricks
    python3 scripts/marketplace/assemble.py --out /tmp/mp
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
APP_FOLDER = "ontobricks"

ROOT_FILES = (
    "run.py",
    "pyproject.toml",
    "uv.lock",
    "requirements.txt",
    "LICENSE.txt",
    "NOTICE.txt",
)
TREES = ("src",)
#: Help Center payload served at /api/help/docs/* (see help.py ``_docs_dir``).
DOCS_DIR = "docs"
DOCS_SUBDIRS = ("images", "screenshots")
MARKETPLACE_FILES = ("app.yaml", "manifest.yaml")

_EXCLUDED_DIRS = {"__pycache__", ".pytest_cache", "_build", ".doctrees", "mcp-server", "logs"}
_EXCLUDED_NAMES = {".DS_Store", ".coding_rules.md"}
_EXCLUDED_SUFFIXES = {".pyc", ".pyo", ".log"}
#: Files every previous bundle has at its root. An existing target folder is
#: only deleted when it carries all of them, so --out can never wipe anything else.
_BUNDLE_SIGNATURE = ("run.py", *MARKETPLACE_FILES)


def _ignore(_directory: str, names: list[str]) -> set[str]:
    return {
        n
        for n in names
        if n in _EXCLUDED_DIRS
        or n in _EXCLUDED_NAMES
        or n.startswith(".env")  # .env, .env.local, ... can hold live PATs
        or Path(n).suffix in _EXCLUDED_SUFFIXES
    }


def missing_inputs(repo_root: Path) -> list[str]:
    """Return every required input path that does not exist under *repo_root*."""
    wanted = [*ROOT_FILES, *TREES, DOCS_DIR, *(f"marketplace/{n}" for n in MARKETPLACE_FILES)]
    return [p for p in wanted if not (repo_root / p).exists()]


def assemble(repo_root: Path, out_dir: Path) -> Path:
    """Build ``<out_dir>/ontobricks`` from *repo_root*; return that path.

    Raises ``FileNotFoundError`` (nothing written) when an input is missing,
    and ``ValueError`` (nothing written or deleted) when *out_dir* is inside a
    copied tree, contains the repo, or already holds a folder that is not a
    previous bundle.
    """
    repo_root = repo_root.resolve()
    out_dir = out_dir.resolve()
    for tree in (*TREES, DOCS_DIR):
        if out_dir.is_relative_to(repo_root / tree):
            raise ValueError(f"--out {out_dir} is inside copied tree {tree}/")
    target = out_dir / APP_FOLDER
    if repo_root.is_relative_to(out_dir):
        raise ValueError(f"--out {out_dir} contains the repo; pick a separate output folder")
    if target.exists() and not all((target / f).is_file() for f in _BUNDLE_SIGNATURE):
        raise ValueError(f"{target} exists and is not a previous bundle; refusing to delete it")

    missing = missing_inputs(repo_root)
    if missing:
        raise FileNotFoundError(f"missing Marketplace inputs: {', '.join(missing)}")

    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)

    for name in ROOT_FILES:
        shutil.copy2(repo_root / name, target / name)
    for tree in TREES:
        shutil.copytree(repo_root / tree, target / tree, ignore=_ignore)
    docs = repo_root / DOCS_DIR
    (target / DOCS_DIR).mkdir()
    for md in sorted(docs.glob("*.md")):
        shutil.copy2(md, target / DOCS_DIR / md.name)
    for sub in DOCS_SUBDIRS:
        if (docs / sub).is_dir():
            shutil.copytree(docs / sub, target / DOCS_DIR / sub, ignore=_ignore)
    for name in MARKETPLACE_FILES:
        shutil.copy2(repo_root / "marketplace" / name, target / name)
    return target


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=REPO_ROOT / "dist" / "marketplace")
    args = parser.parse_args(argv)
    try:
        target = assemble(REPO_ROOT, args.out)
    except (FileNotFoundError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(f"Marketplace app folder ready: {target}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
