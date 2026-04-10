Getting Started
===============

.. seealso::

   Full installation, environment variables, and troubleshooting:
   :doc:`../guides/get-started` (from ``docs/get-started.md``).

Prerequisites
-------------

- Python 3.9+
- `uv <https://docs.astral.sh/uv/>`_ package manager (recommended)
- Access to a Databricks workspace

Installation
------------

.. code-block:: bash

   # Clone the repository
   git clone <repo-url> && cd OntoBricks

   # Install dependencies via uv
   uv sync

   # Or via pip
   pip install -e .

Running the Application
-----------------------

.. code-block:: bash

   # Development mode
   python run.py

   # Or with uvicorn directly
   uvicorn shared.fastapi.main:app --reload --port 8000

The web UI is available at ``http://localhost:8000``.

Running Tests
-------------

.. code-block:: bash

   pytest tests/

Building This Documentation
----------------------------

.. code-block:: bash

   # From the project root
   ./build_docs.sh

   # Or manually
   cd sphinx && make html

The built HTML is written to ``docs/sphinx/_build/html/``.

The Markdown sources under ``docs/*.md`` are also pulled into this site under
**Topic guides** (see the toctree on the home page).
