``back.core.graphdb`` -- Pluggable Graph Database Backends
===========================================================

Package
-------

.. automodule:: back.core.graphdb
   :members:
   :undoc-members:
   :show-inheritance:
   :exclude-members: GraphDBBackend, GraphDBFactory

Constants
---------

.. automodule:: back.core.graphdb.constants
   :members:
   :undoc-members:
   :show-inheritance:

Abstract Base
-------------

The single triple-store / graph DB abstraction. Every backend — Lakebase
Postgres (SQL), Delta / Unity Catalog (SQL), and Neo4j (Cypher) — subclasses
:class:`~back.core.graphdb.GraphDBBackend`. The concrete engine is selected
**per domain** via ``graph_backend`` (``lakebase`` | ``databricks`` |
``neo4j``); see :class:`back.core.graphdb.GraphDBFactory`.

.. automodule:: back.core.graphdb.GraphDBBackend
   :members:
   :undoc-members:
   :show-inheritance:

Factory
-------

.. automodule:: back.core.graphdb.GraphDBFactory
   :members:
   :undoc-members:
   :show-inheritance:

Delta (Unity Catalog) backend
-----------------------------

.. automodule:: back.core.graphdb.delta.DeltaFlatStore
   :members:
   :undoc-members:
   :show-inheritance:

Delta relation lifecycle
------------------------

The SQL that builds a domain's Unity Catalog objects. The ``…_data`` relation
is either a materialized Delta table or a pass-through view over the R2RML
gateway view, per the domain's Lakehouse materialization setting — see
:func:`back.core.graphdb.GraphDBFactory.GraphDBFactory.resolve_lakehouse_materialization`.

.. automodule:: back.core.graphdb.delta.materialize
   :members:
   :undoc-members:
   :show-inheritance:

Lakebase (Postgres) subpackage
------------------------------

See :doc:`app.core.graphdb.lakebase` for ``back.core.graphdb.lakebase`` (flat triple
tables on the App-bound Lakebase Postgres instance).

Neo4j (Cypher) subpackage
-------------------------

See :doc:`app.core.graphdb.neo4j` for ``back.core.graphdb.neo4j`` (Bolt-based
Cypher engine on Neo4j Aura or self-hosted Neo4j).

Adding a new engine
-------------------

A copy-paste template for new engines lives at
``src/back/core/graphdb/_starter_kit/ExampleStore.py``. Implement the
:class:`back.core.graphdb.GraphDBBackend` contract, register the engine in
:class:`back.core.graphdb.GraphDBFactory`, and add it to
``ALLOWED_GRAPH_ENGINES`` in
:mod:`back.objects.session.GlobalConfigService`. See ``docs/graphdb-integration.md``
for the full integration walkthrough.
