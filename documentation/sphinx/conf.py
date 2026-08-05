"""Sphinx configuration for OntoBricks documentation."""

import os
import sys
import tomllib
from pathlib import Path

from pygments.lexers.special import TextLexer
from sphinx.highlighting import lexers

sys.path.insert(0, os.path.abspath("../../src"))

# -- Project information -----------------------------------------------------

project = "OntoBricks"
copyright = "2024-2026, OntoBricks Contributors"
author = "OntoBricks Team"

# Single source of truth: never let the docs drift from the packaged version.
with (Path(__file__).resolve().parents[2] / "pyproject.toml").open("rb") as _f:
    release = tomllib.load(_f)["project"]["version"]
version = release

# -- General configuration ---------------------------------------------------

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx.ext.todo",
    "sphinx.ext.coverage",
    "sphinx.ext.graphviz",
    "myst_parser",
]

# Markdown topic guides (sources in ../../ relative to guides/*.md wrappers)
source_suffix = {
    ".rst": "restructuredtext",
    ".md": "myst",
}

# Generate slug anchors for headings down to level 4 so the cross-document
# ``file.md#some-heading`` links used throughout /docs resolve. Without this
# MyST emits no heading anchors and every such link warns.
myst_heading_anchors = 4

myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "html_image",
    "replacements",
    "smartquotes",
    "substitution",
    "tasklist",
]

autosummary_generate = True
autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
    "member-order": "bysource",
}
autodoc_typehints = "description"
# Real FastAPI / Starlette / Jinja2 / Pydantic are required so autodoc can import
# ``front.fastapi.dependencies`` (Jinja2 ``globals``) and HTML routers.
autodoc_mock_imports = [
    "databricks",
    "databricks.sdk",
    "databricks.sql",
    "mlflow",
    "psycopg",
    "psycopg_pool",
    "strawberry",
    "pyshacl",
    "owlrl",
    "rdflib",
    # pyarrow must NOT be mocked: pandas (pulled in transitively by neo4j)
    # evaluates ``Version(pyarrow.__version__)`` at import time, which raises
    # TypeError on a mock and silently emptied ~30 modules of API reference.
    "apscheduler",
    "uvicorn",
    "aiofiles",
    "itsdangerous",
    "dotenv",
]

napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = True
napoleon_include_private_with_doc = False

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# Pygments has no lexer for these fence languages, which the guides use for
# diagrams and tabular samples. Render them verbatim instead of warning.
for _lang in ("mermaid", "csv"):
    lexers[_lang] = TextLexer()

# The Turtle/SPARQL lexers reject prefixed names containing '/' (e.g.
# ``ex:person/p1``), which is legal RDF. Pygments falls back to relaxed mode and
# still renders correctly, but emits a warning per block — ~100 of them, which
# buries the warnings that matter. Rewriting the examples to appease the lexer
# would make them less faithful, so the category is suppressed instead.
suppress_warnings = ["misc.highlighting_failure"]

# -- Options for HTML output -------------------------------------------------

html_theme = "alabaster"
html_static_path = ["_static"]
html_title = "OntoBricks Documentation"
html_short_title = "OntoBricks"

html_theme_options = {
    "description": "Graph Viewer Builder for Databricks",
    "github_user": "",
    "github_repo": "OntoBricks",
    "fixed_sidebar": True,
    "sidebar_collapse": True,
    "show_powered_by": False,
    "page_width": "1100px",
    "sidebar_width": "260px",
}

html_sidebars = {
    "**": [
        "about.html",
        "searchbox.html",
        "navigation.html",
        "relations.html",
    ]
}

# -- Options for todo extension ----------------------------------------------

todo_include_todos = True
