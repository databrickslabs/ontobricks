OntoBricks Documentation
========================

**Knowledge Graph Builder for Databricks**

OntoBricks is a web-based application that turns Databricks tables into a
knowledge graph. Design ontologies using OWL or import industry standards
(FIBO, CDISC, IOF), map them to tables via R2RML, materialize triples into a
Delta or LadybugDB triple store, query them through a typed GraphQL API, and
explore your knowledge graph visually.

**Topic guides** (Markdown in ``docs/``, included here via MyST) are the
canonical narrative documentation. **Developer overviews** below are short
RST summaries; see the architecture guide for the full design document.

.. toctree::
   :maxdepth: 2
   :caption: Topic guides

   guides/documentation
   guides/get-started
   guides/deployment
   guides/architecture
   guides/user-guide
   guides/import-export
   guides/api
   guides/mcp
   guides/development
   guides/product
   guides/examples

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
   api/app.core.graphdb
   api/app.core.graphql
   api/app.core.industry
   api/app.core.reasoning
   api/app.objects
   api/app.core.sqlwizard
   api/app.core.triplestore
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
