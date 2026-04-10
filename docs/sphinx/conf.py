"""Sphinx configuration for OntoBricks documentation."""

import os
import sys

sys.path.insert(0, os.path.abspath("../../src"))

# -- Project information -----------------------------------------------------

project = "OntoBricks"
copyright = "2024-2026, OntoBricks Contributors"
author = "OntoBricks Team"
release = "0.1.0"
version = "0.1"

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
    "real_ladybug",
    "strawberry",
    "pyshacl",
    "owlrl",
    "rdflib",
    "pyarrow",
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

# -- Options for HTML output -------------------------------------------------

html_theme = "alabaster"
html_static_path = ["_static"]
html_title = "OntoBricks Documentation"
html_short_title = "OntoBricks"

html_theme_options = {
    "description": "Knowledge Graph Builder for Databricks",
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
