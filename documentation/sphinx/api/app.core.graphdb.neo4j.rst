``back.core.graphdb.neo4j`` — Neo4j (Cypher) graph engine
=========================================================

Cypher-based, remote-only backend (Neo4j Aura or self-hosted Neo4j) over the
Bolt protocol via the official ``neo4j`` Python driver. Selected per domain
(``graph_backend = "neo4j"``) and configured under **Settings → Back end →
Neo4j**. The Bolt password is sourced from the ``NEO4J_PASSWORD`` env var
(Databricks Apps secret resource) in the deployed app, with an
``engine_config`` fallback for local development.

Package
-------

.. automodule:: back.core.graphdb.neo4j
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:

Store (façade)
--------------

Thin :class:`~back.core.graphdb.GraphDBBackend` implementation composing the
three services below.

.. automodule:: back.core.graphdb.neo4j.Neo4jStore
   :members:
   :undoc-members:
   :show-inheritance:

Connection
----------

Driver lifecycle, auth resolution, and the single Cypher execution entry point.

.. automodule:: back.core.graphdb.neo4j.Neo4jConnection
   :members:
   :undoc-members:
   :show-inheritance:

Write operations
----------------

Schema (constraint create/drop) and bulk writes (``UNWIND`` + ``MERGE`` /
``DETACH DELETE``).

.. automodule:: back.core.graphdb.neo4j.Neo4jWriteOps
   :members:
   :undoc-members:
   :show-inheritance:

Read operations
---------------

Statistics, entity lookup, pagination, KG-filter primitives, and reasoning
helpers.

.. automodule:: back.core.graphdb.neo4j.Neo4jReadOps
   :members:
   :undoc-members:
   :show-inheritance:
