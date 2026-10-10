OntoBricks Documentation
========================

**Graph Viewer Builder for Databricks**

OntoBricks is a web-based application that turns Databricks tables into a
graph viewer. Design ontologies using OWL or import industry standards
(FIBO, CDISC, IOF), map them to tables via R2RML, materialize triples into a
Delta-backed triple store mirrored on Lakebase Postgres, query them through a typed GraphQL API, and
explore your graph viewer visually.

**Topic guides** (Markdown in ``docs/``, included here via MyST) are the
canonical narrative documentation. Categories match the docs hub and the
in-app Help Center. **Developer overviews** below are short RST summaries.

.. toctree::
   :maxdepth: 1
   :caption: Start here

   guides/documentation
   guides/getting-started
   guides/examples

.. toctree::
   :maxdepth: 1
   :caption: Using OntoBricks

   guides/user-guide
   guides/advanced_features
   guides/mcp

.. toctree::
   :maxdepth: 1
   :caption: Platform

   guides/backend
   guides/architecture
   guides/api

.. toctree::
   :maxdepth: 1
   :caption: Deploy & develop

   guides/deployment
   guides/development

.. toctree::
   :maxdepth: 1
   :caption: About

   guides/product

.. toctree::
   :maxdepth: 2
   :caption: Developer overview

   overview/architecture
   overview/getting-started

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   api/app
   api/app.fastapi
   api/api_external
   api/app.core
   api/app.core.databricks
   api/app.core.external
   api/app.core.external.pitfalls
   api/app.core.graphdb
   api/app.core.graphdb.lakebase
   api/app.core.graphdb.neo4j
   api/app.core.graphql
   api/app.core.industry
   api/app.core.reasoning
   api/app.objects
   api/app.core.sqlwizard
   api/app.core.w3c
   api/app.frontend
   api/app.config
   api/agents

.. toctree::
   :maxdepth: 1
   :caption: Additional

   changelog


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
