Architecture Overview
=====================

.. seealso::

   Full architecture, OntoViz, agentic stack, and incremental sync design:
   :doc:`../guides/architecture` (from ``docs/architecture.md``).

OntoBricks follows a layered FastAPI architecture with clear separation of
concerns.

Layers
------

.. code-block:: text

   ┌──────────────────────────────────────────────────────┐
   │  UI Layer  (front.routes.*)                        │
   │  HTML routes + Jinja2 templates + static assets    │
   ├──────────────────────────────────────────────────────┤
   │  API Layer  (shared.fastapi / front.fastapi /       │
   │             back.fastapi + api)                    │
   │  REST endpoints, GraphQL mount, health checks      │
   ├──────────────────────────────────────────────────────┤
   │  Core Domain  (back.core)                          │
   │  Databricks · W3C · TripleStore · Reasoning ·       │
   │  Registry · Session · GraphQL · SQL Wizard          │
   ├──────────────────────────────────────────────────────┤
   │  Agents  (agents)                                  │
   │  LLM-powered assistants for ontology tasks         │
   └──────────────────────────────────────────────────────┘

Key Design Decisions
--------------------

- **Class-first policy** -- behaviour is encapsulated in service classes, not
  loose module-level functions.
- **Strategy pattern** for triple stores -- ``TripleStoreFactory`` instantiates
  the Delta view client and ``GraphDBFactory`` instantiates the active Graph DB
  engine (Lakebase Postgres today).
- **Facade pattern** for Databricks APIs -- ``DatabricksClient`` composes
  per-surface service classes.
- **W3C standards** -- OWL, RDFS, R2RML, SPARQL, and SHACL each have their
  own subpackage under ``back.core.w3c``.

Package Map
-----------

==========================================  ============================================
Package                                     Responsibility
==========================================  ============================================
``shared.fastapi``                          FastAPI app factory, health, shared wiring
``front.fastapi``                           UI-specific FastAPI dependencies
``back.fastapi``                            GraphQL routes and domain HTTP helpers
``api``                                     External REST v1 + Knowledge Graph (mounted at ``/api``)
``back.core.databricks``                    Typed facades for Databricks SDK & SQL
``back.core.triplestore``                   Abstract base + Delta view backend
``back.core.graphdb``                       Pluggable Graph DB engines (Lakebase Postgres, …)
``back.core.w3c``                           OWL, RDFS, R2RML, SPARQL, SHACL services
``back.core.reasoning``                     OWL 2 RL inference + SWRL engine
``back.core.graphql``                       Strawberry GraphQL schema from ontology
``back.objects.registry``                   UC Volume domain registry + permissions
``back.objects.session``                    HTTP session + in-memory domain state
``back.objects.domain``                     Session-scoped domain management (UC, metadata, layout)
``back.core.industry``                      FIBO, CDISC, IOF importers
``back.core.sqlwizard``                     LLM-assisted SQL generation
``front.routes.*``                          UI areas (home, ontology, mapping, etc.)
``agents``                                  LLM agent engines and tool functions
==========================================  ============================================
